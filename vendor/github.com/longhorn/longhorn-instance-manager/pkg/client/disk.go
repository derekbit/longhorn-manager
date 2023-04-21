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

	diskInfo := resp.GetDiskInfo()

	return &DiskInfo{
		ID:          diskInfo.GetId(),
		UUID:        diskInfo.GetUuid(),
		Path:        diskInfo.GetPath(),
		Type:        diskInfo.GetType(),
		TotalSize:   diskInfo.GetTotalSize(),
		FreeSize:    diskInfo.GetFreeSize(),
		TotalBlocks: diskInfo.GetTotalBlocks(),
		FreeBlocks:  diskInfo.GetFreeBlocks(),
		BlockSize:   diskInfo.GetBlockSize(),
		ClusterSize: diskInfo.GetClusterSize(),
		Readonly:    diskInfo.GetReadonly(),
	}, nil
}

func (c *DiskServiceClient) DiskInfo(diskPath string) (*DiskInfo, error) {
	if diskPath == "" {
		return nil, fmt.Errorf("failed to get disk info: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.DiskInfo(ctx, &rpc.DiskInfoRequest{
		DiskPath: diskPath,
	})
	if err != nil {
		return nil, err
	}

	diskInfo := resp.GetDiskInfo()

	return &DiskInfo{
		ID:          diskInfo.GetId(),
		UUID:        diskInfo.GetUuid(),
		Path:        diskInfo.GetPath(),
		Type:        diskInfo.GetType(),
		TotalSize:   diskInfo.GetTotalSize(),
		FreeSize:    diskInfo.GetFreeSize(),
		TotalBlocks: diskInfo.GetTotalBlocks(),
		FreeBlocks:  diskInfo.GetFreeBlocks(),
		BlockSize:   diskInfo.GetBlockSize(),
		ClusterSize: diskInfo.GetClusterSize(),
		Readonly:    diskInfo.GetReadonly(),
	}, nil
}

func (c *DiskServiceClient) ReplicaCreate(name, lvstoreUUID string, size int64) (*ReplicaInfo, error) {
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
	})
	if err != nil {
		return nil, err
	}

	replicaInfo := resp.GetReplicaInfo()

	return &ReplicaInfo{
		Name:          replicaInfo.GetName(),
		UUID:          replicaInfo.GetUuid(),
		BdevName:      replicaInfo.GetBdevName(),
		LvstoreUUID:   replicaInfo.GetLvstoreUuid(),
		TotalSize:     replicaInfo.GetTotalSize(),
		TotalBlocks:   replicaInfo.GetTotalBlocks(),
		ThinProvision: replicaInfo.GetThinProvision(),
		State:         replicaInfo.GetState(),
	}, nil
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

func (c *DiskServiceClient) ReplicaInfo(name, lvstoreUUID string) (*ReplicaInfo, error) {
	if name == "" || lvstoreUUID == "" {
		return nil, fmt.Errorf("failed to get replica: missing required parameter")
	}

	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaInfo(ctx, &rpc.ReplicaInfoRequest{
		Name:        name,
		LvstoreUuid: lvstoreUUID,
	})
	if err != nil {
		return nil, err
	}

	replicaInfo := resp.GetReplicaInfo()

	return &ReplicaInfo{
		Name:          replicaInfo.GetName(),
		UUID:          replicaInfo.GetUuid(),
		BdevName:      replicaInfo.GetBdevName(),
		LvstoreUUID:   replicaInfo.GetLvstoreUuid(),
		TotalSize:     replicaInfo.GetTotalSize(),
		TotalBlocks:   replicaInfo.GetTotalBlocks(),
		ThinProvision: replicaInfo.GetThinProvision(),
		State:         replicaInfo.GetState(),
	}, nil
}

func (c *DiskServiceClient) ReplicaList() (map[string]*ReplicaInfo, error) {
	client := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	replicaInfos := map[string]*ReplicaInfo{}

	for _, replicaInfo := range resp.GetReplicaInfos() {
		replicaInfos[replicaInfo.GetName()] = &ReplicaInfo{
			Name:          replicaInfo.GetName(),
			UUID:          replicaInfo.GetUuid(),
			BdevName:      replicaInfo.GetBdevName(),
			LvstoreUUID:   replicaInfo.GetLvstoreUuid(),
			TotalSize:     replicaInfo.GetTotalSize(),
			TotalBlocks:   replicaInfo.GetTotalBlocks(),
			ThinProvision: replicaInfo.GetThinProvision(),
			State:         replicaInfo.GetState(),
		}
	}
	return replicaInfos, nil
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
