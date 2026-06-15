package aggregation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/remotedialer"
	"github.com/rancher/steve/pkg/auth"
	"github.com/sirupsen/logrus"
)

const (
	HandshakeTimeOut = 10 * time.Second
)

func ListenAndServe(ctx context.Context, url string, caCert []byte, token string, handler http.Handler) {
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: HandshakeTimeOut,
	}

	if caCert != nil && len(caCert) == 0 {
		dialer.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	} else if len(caCert) > 0 {
		if _, err := http.Get(url); err != nil {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)
			// The aggregation Secret URL written by Rancher targets the
			// cattle-cluster-agent ClusterIP (https://<ClusterIP>/v3/connect).
			// In a downstream cluster that IP is usually absent from the
			// serving certificate's SANs, so the default TLS verification fails
			// with "x509: ... doesn't contain any IP SANs". We still verify the
			// certificate chain against the provided CA (signature + validity),
			// but skip the hostname/IP SAN check because the connection target
			// is an in-cluster ClusterIP we trust via the CA.
			dialer.TLSClientConfig = &tls.Config{
				InsecureSkipVerify:    true,
				VerifyPeerCertificate: verifyChainSkipHostname(pool),
			}
		}
	}

	handler = auth.ToMiddleware(auth.AuthenticatorFunc(auth.Impersonation))(handler)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)

	for {
		err := serve(ctx, dialer, url, headers, handler)
		if err != nil {
			logrus.Errorf("Failed to dial steve aggregation server: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func serve(ctx context.Context, dialer websocket.Dialer, url string, headers http.Header, handler http.Handler) error {
	url = strings.Replace(url, "http://", "ws://", 1)
	url = strings.Replace(url, "https://", "wss://", 1)

	// ensure we clean up everything on exit
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()
	conn, _, err := dialer.DialContext(dialCtx, url, headers)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx = context.WithValue(ctx, remotedialer.ContextKeyCaller, fmt.Sprintf("steve server: url:%s", url))

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	listener := NewListener("steve")
	server := http.Server{
		Handler: handler,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}
	go server.Serve(listener)
	defer server.Shutdown(context.Background())

	session := remotedialer.NewClientSessionWithDialer(allowAll, conn, listener.Dial)
	defer session.Close()

	_, err = session.Serve(ctx)
	return err
}

func allowAll(_, _ string) bool {
	return true
}

// verifyChainSkipHostname returns a tls.Config.VerifyPeerCertificate callback
// that verifies the presented certificate chain against roots (signature,
// validity and chain of trust) while deliberately skipping the DNS/IP SAN
// check. It is used for the Steve aggregation tunnel, whose target is an
// in-cluster ClusterIP that is trusted via the CA but typically not present in
// the certificate's SAN list.
func verifyChainSkipHostname(roots *x509.CertPool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no server certificate presented")
		}

		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("failed to parse server certificate: %w", err)
			}
			certs = append(certs, cert)
		}

		intermediates := x509.NewCertPool()
		for _, cert := range certs[1:] {
			intermediates.AddCert(cert)
		}

		// DNSName is intentionally left empty to skip hostname/IP SAN matching.
		_, err := certs[0].Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
		})
		return err
	}
}
