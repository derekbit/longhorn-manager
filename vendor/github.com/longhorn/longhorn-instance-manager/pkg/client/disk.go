package client

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type DiskServiceContext struct {
	cc      *grpc.ClientConn
	service rpc.DiskServiceClient
}

func (c *DiskServiceClient) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *DiskServiceClient) getControllerServiceClient() rpc.DiskServiceClient {
	return c.service
}

type DiskServiceClient struct {
	serviceURL string
	tlsConfig  *tls.Config
	DiskServiceContext
}

func NewDiskServiceClient(serviceURL string, tlsConfig *tls.Config) (*DiskServiceClient, error) {
	getDiskServiceContext := func(serviceUrl string, tlsConfig *tls.Config) (DiskServiceContext, error) {
		connection, err := util.Connect(serviceUrl, tlsConfig)
		if err != nil {
			return DiskServiceContext{}, errors.Wrapf(err, "cannot connect to Disk Service %v", serviceUrl)
		}

		return DiskServiceContext{
			cc:      connection,
			service: rpc.NewDiskServiceClient(connection),
		}, nil
	}

	serviceContext, err := getDiskServiceContext(serviceURL, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &DiskServiceClient{
		serviceURL:         serviceURL,
		tlsConfig:          tlsConfig,
		DiskServiceContext: serviceContext,
	}, nil
}

func NewDiskServiceClientWithTLS(serviceURL, caFile, certFile, keyFile, peerName string) (*DiskServiceClient, error) {
	tlsConfig, err := util.LoadClientTLS(caFile, certFile, keyFile, peerName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load tls key pair from file")
	}

	return NewDiskServiceClient(serviceURL, tlsConfig)
}

func (c *DiskServiceClient) DiskCreate(diskName, diskPath string) (*DiskInfo, error) {
	if diskName == "" || diskPath == "" {
		return nil, fmt.Errorf("failed to create disk: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.DiskCreate(ctx, &rpc.DiskCreateRequest{
		DiskName: diskName,
		DiskPath: diskPath,
	})
	if err != nil {
		return nil, err
	}

	return &DiskInfo{
		ID:          resp.GetId(),
		UUID:        resp.GetUuid(),
		Path:        resp.GetPath(),
		Type:        resp.GetType(),
		TotalSize:   resp.GetTotalSize(),
		FreeSize:    resp.GetFreeSize(),
		TotalBlocks: resp.GetTotalBlocks(),
		FreeBlocks:  resp.GetFreeBlocks(),
		BlockSize:   resp.GetBlockSize(),
		ClusterSize: resp.GetClusterSize(),
		Readonly:    resp.GetReadonly(),
	}, nil
}

func (c *DiskServiceClient) DiskGet(diskPath string) (*DiskInfo, error) {
	if diskPath == "" {
		return nil, fmt.Errorf("failed to get disk info: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.DiskGet(ctx, &rpc.DiskGetRequest{
		DiskPath: diskPath,
	})
	if err != nil {
		return nil, err
	}

	return &DiskInfo{
		ID:          resp.GetId(),
		UUID:        resp.GetUuid(),
		Path:        resp.GetPath(),
		Type:        resp.GetType(),
		TotalSize:   resp.GetTotalSize(),
		FreeSize:    resp.GetFreeSize(),
		TotalBlocks: resp.GetTotalBlocks(),
		FreeBlocks:  resp.GetFreeBlocks(),
		BlockSize:   resp.GetBlockSize(),
		ClusterSize: resp.GetClusterSize(),
		Readonly:    resp.GetReadonly(),
	}, nil
}

func (c *DiskServiceClient) ReplicaCreate(name, lvstoreUUID string, size int64, address string) (*rpc.Replica, error) {
	if name == "" || lvstoreUUID == "" || size == 0 {
		return nil, fmt.Errorf("failed to create replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaCreate(ctx, &rpc.ReplicaCreateRequest{
		Name:        name,
		LvstoreUuid: lvstoreUUID,
		Size:        size,
		Address:     address,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *DiskServiceClient) ReplicaDelete(name, lvstoreUUID string) error {
	if name == "" || lvstoreUUID == "" {
		return fmt.Errorf("failed to delete replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaDelete(ctx, &rpc.ReplicaDeleteRequest{
		Name:        name,
		LvstoreUuid: lvstoreUUID,
	})
	return err
}

func (c *DiskServiceClient) ReplicaGet(name, lvstoreUUID string) (*rpc.Replica, error) {
	if name == "" || lvstoreUUID == "" {
		return nil, fmt.Errorf("failed to get replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaGet(ctx, &rpc.ReplicaGetRequest{
		Name:        name,
		LvstoreUuid: lvstoreUUID,
	})
}

func (c *DiskServiceClient) ReplicaList() (map[string]*rpc.Replica, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	return resp.Replicas, nil
}

func (c *DiskServiceClient) EngineCreate(name, frontend, address string, replicaAddresses map[string]string) (*rpc.Engine, error) {
	if name == "" || replicaAddresses == nil {
		return nil, fmt.Errorf("failed to create engine: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineCreate(ctx, &rpc.EngineCreateRequest{
		Name:              name,
		Address:           address,
		ReplicaAddressMap: replicaAddresses,
		Frontend:          frontend,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *DiskServiceClient) EngineDelete() error {
	return nil
}

func (c *DiskServiceClient) EngineList() (map[string]*rpc.Engine, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineList(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	return resp.Engines, nil
}

func (c *DiskServiceClient) VersionGet() (*meta.VersionOutput, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.VersionGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get disk service version")
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
