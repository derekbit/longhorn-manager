package client

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/longhorn/longhorn-instance-manager/pkg/api"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type InstanceServiceContext struct {
	cc *grpc.ClientConn

	ctx  context.Context
	quit context.CancelFunc

	service rpc.InstanceServiceClient
}

func (c InstanceServiceContext) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *InstanceServiceClient) getControllerServiceClient() rpc.InstanceServiceClient {
	return c.service
}

type InstanceServiceClient struct {
	serviceURL string
	InstanceServiceContext
}

func NewInstanceServiceClient(ctx context.Context, ctxCancel context.CancelFunc, address string, port int) (*InstanceServiceClient, error) {
	getServiceCtx := func(serviceUrl string) (InstanceServiceContext, error) {
		dialOptions := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Second * 10,
				PermitWithoutStream: true,
			}),
		}
		connection, err := grpc.Dial(serviceUrl, dialOptions...)
		if err != nil {
			return InstanceServiceContext{}, errors.Wrapf(err, "cannot connect to Instance Service %v", serviceUrl)
		}
		return InstanceServiceContext{
			cc:      connection,
			ctx:     ctx,
			quit:    ctxCancel,
			service: rpc.NewInstanceServiceClient(connection),
		}, nil
	}

	serviceURL := util.GetURL(address, port)
	serviceCtx, err := getServiceCtx(serviceURL)
	if err != nil {
		return nil, err
	}
	logrus.Tracef("Connected to instance service on %v", serviceURL)

	return &InstanceServiceClient{
		serviceURL:             serviceURL,
		InstanceServiceContext: serviceCtx,
	}, nil
}

func (c *InstanceServiceClient) InstanceCreate(name, instanceType, backendStoreDriver, diskUUID string, size int64,
	binary string, args []string, portCount int, portArgs []string) (*api.Instance, error) {
	if name == "" || instanceType == "" || backendStoreDriver == "" {
		return nil, fmt.Errorf("failed to create instance: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.InstanceCreate(ctx, &rpc.InstanceCreateRequest{
		Spec: &rpc.InstanceSpec{
			Name:               name,
			Type:               instanceType,
			BackendStoreDriver: backendStoreDriver,
			Size:               size,
			DiskUuid:           diskUUID,

			PortCount: int32(portCount),
			PortArgs:  portArgs,

			Process: &rpc.Process{
				Binary: binary,
				Args:   args,
			},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create instance")
	}

	return api.RPCToInstance(p), nil
}

func (c *InstanceServiceClient) InstanceDelete(name, instanceType, backendStoreDriver string) (*api.Instance, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to delete instance: missing required parameter name")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.InstanceDelete(ctx, &rpc.InstanceDeleteRequest{
		Name:               name,
		Type:               instanceType,
		BackendStoreDriver: backendStoreDriver,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to delete instance %v", name)
	}
	return api.RPCToInstance(p), nil
}

func (c *InstanceServiceClient) InstanceGet(name, instanceType, backendStoreDriver string) (*api.Instance, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get instance: missing required parameter name")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.InstanceGet(ctx, &rpc.InstanceGetRequest{
		Name:               name,
		Type:               instanceType,
		BackendStoreDriver: backendStoreDriver,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get instance %v", name)
	}
	return api.RPCToInstance(p), nil
}

func (c *InstanceServiceClient) InstanceList() (map[string]*api.Instance, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	instances, err := client.InstanceList(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list instances")
	}
	return api.RPCToInstanceList(instances), nil
}

func (c *InstanceServiceClient) InstanceLog(ctx context.Context, name, instanceType, backendStoreDriver string) (*api.LogStream, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get instance: missing required parameter name")
	}

	client := c.getControllerServiceClient()
	stream, err := client.InstanceLog(ctx, &rpc.InstanceLogRequest{
		Name:               name,
		Type:               instanceType,
		BackendStoreDriver: backendStoreDriver,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get instance log of %v", name)
	}
	return api.NewLogStream(stream), nil
}

func (c *InstanceServiceClient) InstanceWatch(ctx context.Context) (*api.InstanceStream, error) {
	client := c.getControllerServiceClient()
	stream, err := client.InstanceWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open instance update stream")
	}

	return api.NewInstanceStream(stream), nil
}

func (c *InstanceServiceClient) InstanceReplace(name, instanceType, backendStoreDriver, binary string, portCount int, args, portArgs []string, terminateSignal string) (*api.Instance, error) {
	if name == "" || binary == "" {
		return nil, fmt.Errorf("failed to replace instance: missing required parameter")
	}
	if terminateSignal != "SIGHUP" {
		return nil, fmt.Errorf("unsupported terminate signal %v", terminateSignal)
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.InstanceReplace(ctx, &rpc.InstanceReplaceRequest{
		Spec: &rpc.InstanceSpec{
			Name:               name,
			Type:               instanceType,
			BackendStoreDriver: backendStoreDriver,
			Process: &rpc.Process{
				Binary: binary,
				Args:   args,
			},
			PortCount: int32(portCount),
			PortArgs:  portArgs,
		},
		TerminateSignal: terminateSignal,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to replace instance")
	}
	return api.RPCToInstance(p), nil
}

func (c *InstanceServiceClient) VersionGet() (*meta.VersionOutput, error) {

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.VersionGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get version")
	}

	return &meta.VersionOutput{
		Version:   resp.Version,
		GitCommit: resp.GitCommit,
		BuildDate: resp.BuildDate,

		InstanceManagerAPIVersion:    int(resp.InstanceManagerAPIVersion),
		InstanceManagerAPIMinVersion: int(resp.InstanceManagerAPIMinVersion),

		InstanceManagerProxyAPIVersion:    int(resp.InstanceManagerProxyAPIVersion),
		InstanceManagerProxyAPIMinVersion: int(resp.InstanceManagerProxyAPIMinVersion),
	}, nil
}
