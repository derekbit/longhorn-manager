package monitor

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	TestDiskID1 = "fsid"

	TestOrphanedReplicaDirectoryName = "test-volume-r-000000000"
)

func NewFakeNodeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*NodeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, NodeMonitorSyncPeriod),

		nodeName:        nodeName,
		checkVolumeMeta: false,

		collectedDataLock: sync.RWMutex{},
		collectedData:     make(map[string]*CollectedDiskInfo, 0),

		syncCallback: syncCallback,

		getDiskStatHandler:                      fakeGetDiskStat,
		getDiskConfigHandler:                    fakeGetDiskConfig,
		generateDiskConfigHandler:               fakeGenerateDiskConfig,
		getPossibleReplicaDirectoryNamesHandler: fakeGetPossibleReplicaDirectoryNames,
	}

	return m, nil
}

func fakeGetPossibleReplicaDirectoryNames(node *longhorn.Node, diskName, diskUUID, diskPath string) map[string]string {
	return map[string]string{
		TestOrphanedReplicaDirectoryName: "",
	}
}

func fakeGetDiskStat(diskType longhorn.DiskType, directory string, client engineapi.DiskServiceClient) (*util.DiskStat, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return &util.DiskStat{
			DiskID:      "fsid",
			Path:        directory,
			Type:        "ext4",
			FreeBlocks:  0,
			TotalBlocks: 0,
			BlockSize:   0,

			StorageMaximum:   0,
			StorageAvailable: 0,
		}, nil
	case longhorn.DiskTypeBlock:
		return &util.DiskStat{
			DiskID:      "block",
			Path:        directory,
			Type:        "ext4",
			FreeBlocks:  0,
			TotalBlocks: 0,
			BlockSize:   0,

			StorageMaximum:   0,
			StorageAvailable: 0,
		}, nil
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func fakeGetDiskConfig(diskType longhorn.DiskType, path string, client engineapi.DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return &util.DiskConfig{
			DiskUUID: TestDiskID1,
		}, nil
	case longhorn.DiskTypeBlock:
		return &util.DiskConfig{
			DiskUUID: TestDiskID1,
		}, nil
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func fakeGenerateDiskConfig(diskType longhorn.DiskType, name, path string, client engineapi.DiskServiceClient) (*util.DiskConfig, error) {
	return &util.DiskConfig{
		DiskUUID: TestDiskID1,
	}, nil
}
