package client

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/api"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type SPDKServiceContext struct {
	cc      *grpc.ClientConn
	service rpc.SPDKServiceClient
}

func (c *SPDKServiceClient) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *SPDKServiceClient) getControllerServiceClient() rpc.SPDKServiceClient {
	return c.service
}

type SPDKServiceClient struct {
	serviceURL string
	tlsConfig  *tls.Config
	SPDKServiceContext
}

func NewSPDKServiceClient(serviceURL string, tlsConfig *tls.Config) (*SPDKServiceClient, error) {
	getSPDKServiceContext := func(serviceUrl string, tlsConfig *tls.Config) (SPDKServiceContext, error) {
		connection, err := util.Connect(serviceUrl, tlsConfig)
		if err != nil {
			return SPDKServiceContext{}, errors.Wrapf(err, "cannot connect to SPDK Service %v", serviceUrl)
		}

		return SPDKServiceContext{
			cc:      connection,
			service: rpc.NewSPDKServiceClient(connection),
		}, nil
	}

	serviceContext, err := getSPDKServiceContext(serviceURL, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &SPDKServiceClient{
		serviceURL:         serviceURL,
		tlsConfig:          tlsConfig,
		SPDKServiceContext: serviceContext,
	}, nil
}

func NewSPDKServiceClientWithTLS(serviceURL, caFile, certFile, keyFile, peerName string) (*SPDKServiceClient, error) {
	tlsConfig, err := util.LoadClientTLS(caFile, certFile, keyFile, peerName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load tls key pair from file")
	}

	return NewSPDKServiceClient(serviceURL, tlsConfig)
}

func (c *SPDKServiceClient) ReplicaCreate(name, lvsName, lvsUUID string, size uint64, exposeRequired bool) (*rpc.Replica, error) {
	if name == "" || lvsUUID == "" || size == 0 {
		return nil, fmt.Errorf("failed to create replica: missing required parameter name=%v, lvsUUID=%v, size=%v", name, lvsUUID, size)
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaCreate(ctx, &rpc.ReplicaCreateRequest{
		Name:           name,
		LvsName:        lvsName,
		LvsUuid:        lvsUUID,
		SpecSize:       size,
		ExposeRequired: exposeRequired,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *SPDKServiceClient) ReplicaDelete(name string, cleanupRequired bool) error {
	if name == "" {
		return fmt.Errorf("failed to delete replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaDelete(ctx, &rpc.ReplicaDeleteRequest{
		Name:            name,
		CleanupRequired: cleanupRequired,
	})
	return err
}

func (c *SPDKServiceClient) ReplicaGet(name string) (*rpc.Replica, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaGet(ctx, &rpc.ReplicaGetRequest{
		Name: name,
	})
}

func (c *SPDKServiceClient) ReplicaList() (map[string]*rpc.Replica, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	return resp.Replicas, nil
}

func (c *SPDKServiceClient) EngineCreate(name string, size uint64, replicaAddresses map[string]string) (*rpc.Engine, error) {
	if name == "" || replicaAddresses == nil {
		return nil, fmt.Errorf("failed to create engine: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineCreate(ctx, &rpc.EngineCreateRequest{
		Name:              name,
		SpecSize:          size,
		ReplicaAddressMap: replicaAddresses,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *SPDKServiceClient) EngineDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete engine: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineDelete(ctx, &rpc.EngineDeleteRequest{
		Name: name,
	})
	return err
}

func (c *SPDKServiceClient) EngineGet(name string) (*rpc.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get engine: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	return client.EngineGet(ctx, &rpc.EngineGetRequest{
		Name: name,
	})
}

func (c *SPDKServiceClient) EngineList() (map[string]*rpc.Engine, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineList(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	return resp.Engines, nil
}

func (c *SPDKServiceClient) ReplicaWatch(ctx context.Context) (*api.ReplicaStream, error) {
	client := c.getControllerServiceClient()
	stream, err := client.ReplicaWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open replica update stream")
	}

	return api.NewReplicaStream(stream), nil
}

func (c *SPDKServiceClient) EngineWatch(ctx context.Context) (*api.EngineStream, error) {
	client := c.getControllerServiceClient()
	stream, err := client.EngineWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open engine update stream")
	}

	return api.NewEngineStream(stream), nil
}

func (c *SPDKServiceClient) VersionGet() (*meta.VersionOutput, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.VersionGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get SPDK service version")
	}

	return &meta.VersionOutput{
		Version:   resp.Version,
		GitCommit: resp.GitCommit,
		BuildDate: resp.BuildDate,

		InstanceManagerAPIVersion:    int(resp.InstanceManagerAPIVersion),
		InstanceManagerAPIMinVersion: int(resp.InstanceManagerAPIMinVersion),

		InstanceManagerProxyAPIVersion:    int(resp.InstanceManagerProxyAPIVersion),
		InstanceManagerProxyAPIMinVersion: int(resp.InstanceManagerProxyAPIMinVersion),

		InstanceManagerDiskServiceAPIVersion:    int(resp.InstanceManagerDiskServiceAPIVersion),
		InstanceManagerDiskServiceAPIMinVersion: int(resp.InstanceManagerDiskServiceAPIMinVersion),
	}, nil
}
