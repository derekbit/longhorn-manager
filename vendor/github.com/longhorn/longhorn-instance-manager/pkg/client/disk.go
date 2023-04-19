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

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type DiskServiceContext struct {
	cc *grpc.ClientConn

	ctx  context.Context
	quit context.CancelFunc

	service rpc.DiskServiceClient
}

func (c *DiskServiceClient) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *DiskServiceClient) getDiskServiceClient() rpc.DiskServiceClient {
	return c.service
}

type DiskServiceClient struct {
	ServiceURL string
	DiskServiceContext
}

func NewDiskServiceClient(ctx context.Context, ctxCancel context.CancelFunc, address string, port int) (*DiskServiceClient, error) {
	getServiceCtx := func(serviceUrl string) (DiskServiceContext, error) {
		dialOptions := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Second * 10,
				PermitWithoutStream: true,
			}),
		}
		connection, err := grpc.Dial(serviceUrl, dialOptions...)
		if err != nil {
			return DiskServiceContext{}, errors.Wrapf(err, "cannot connect to DiskService %v", serviceUrl)
		}
		return DiskServiceContext{
			cc:      connection,
			ctx:     ctx,
			quit:    ctxCancel,
			service: rpc.NewDiskServiceClient(connection),
		}, nil
	}

	serviceURL := util.GetURL(address, port)
	serviceCtx, err := getServiceCtx(serviceURL)
	if err != nil {
		return nil, err
	}
	logrus.Tracef("Connected to disk service on %v", serviceURL)

	return &DiskServiceClient{
		ServiceURL:         serviceURL,
		DiskServiceContext: serviceCtx,
	}, nil
}

func (c *DiskServiceClient) DiskCreate(diskName, diskPath string) (*DiskInfo, error) {
	if diskName == "" || diskPath == "" {
		return nil, fmt.Errorf("failed to create disk: missing required parameter")
	}

	client := c.getDiskServiceClient()
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

	client := c.getDiskServiceClient()
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

	client := c.getDiskServiceClient()
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

	client := c.getDiskServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaDelete(ctx, &rpc.ReplicaDeleteRequest{
		Name:        name,
		LvstoreUuid: lvstoreUUID,
	})
	return err
}

func (c *DiskServiceClient) ReplicaInfo(uuid string) (*ReplicaInfo, error) {
	if uuid == "" {
		return nil, fmt.Errorf("failed to get replica: missing required parameter")
	}

	client := c.getDiskServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaInfo(ctx, &rpc.ReplicaInfoRequest{
		Uuid: uuid,
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
	client := c.getDiskServiceClient()
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
