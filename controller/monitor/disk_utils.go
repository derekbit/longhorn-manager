package monitor

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

// GetDiskStat returns the disk stat of the given directory
func GetDiskStat(diskType longhorn.DiskType, path string, client engineapi.DiskServiceClient) (stat *util.DiskStat, err error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskStat(path)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskStat(client, path)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getFilesystemTypeDiskStat(path string) (stat *util.DiskStat, err error) {
	return util.GetDiskStat(path)
}

func getBlockTypeDiskStat(client engineapi.DiskServiceClient, path string) (stat *util.DiskStat, err error) {
	info, err := client.DiskInfo(path)
	if err != nil {
		return nil, err
	}
	return &util.DiskStat{
		DiskID:           info.ID,
		Path:             info.Path,
		Type:             info.Type,
		TotalBlocks:      info.TotalBlocks,
		FreeBlocks:       info.FreeBlocks,
		BlockSize:        info.BlockSize,
		StorageMaximum:   info.TotalSize,
		StorageAvailable: info.FreeSize,
	}, nil
}

// GetDiskConfig returns the disk config of the given directory
func GetDiskConfig(diskType longhorn.DiskType, path string, client engineapi.DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskConfig(client, path)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getFilesystemTypeDiskConfig(path string) (*util.DiskConfig, error) {
	nsPath := iscsiutil.GetHostNamespacePath(util.HostProcPath)
	nsExec, err := iscsiutil.NewNamespaceExecutor(nsPath)
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(path, util.DiskConfigFile)
	output, err := nsExec.Execute("cat", []string{filePath})
	if err != nil {
		return nil, fmt.Errorf("cannot find config file %v on host: %v", filePath, err)
	}

	cfg := &util.DiskConfig{}
	if err := json.Unmarshal([]byte(output), cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %v content %v on host: %v", filePath, output, err)
	}
	return cfg, nil
}

func getBlockTypeDiskConfig(client engineapi.DiskServiceClient, path string) (config *util.DiskConfig, err error) {
	info, err := client.DiskInfo(path)
	if err != nil {
		if grpcstatus.Code(err) == grpccodes.NotFound {
			return nil, errors.Wrapf(err, "cannot find disk info")
		}
		return nil, err
	}
	return &util.DiskConfig{
		DiskUUID: info.UUID,
	}, nil
}

// GenerateDiskConfig generates a disk config for the given directory
func GenerateDiskConfig(diskType longhorn.DiskType, name, path string, client engineapi.DiskServiceClient) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return generateFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return generateBlockTypeDiskConfig(client, name, path)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func generateFilesystemTypeDiskConfig(path string) (*util.DiskConfig, error) {
	cfg := &util.DiskConfig{
		DiskUUID: util.UUID(),
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("BUG: Cannot marshal %+v: %v", cfg, err)
	}

	nsPath := iscsiutil.GetHostNamespacePath(util.HostProcPath)
	nsExec, err := iscsiutil.NewNamespaceExecutor(nsPath)
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(path, util.DiskConfigFile)
	if _, err := nsExec.Execute("ls", []string{filePath}); err == nil {
		return nil, fmt.Errorf("disk cfg on %v exists, cannot override", filePath)
	}

	defer func() {
		if err != nil {
			if derr := util.DeleteDiskPathReplicaSubdirectoryAndDiskCfgFile(nsExec, path); derr != nil {
				err = errors.Wrapf(err, "cleaning up disk config path %v failed with error: %v", path, derr)
			}

		}
	}()

	if _, err := nsExec.ExecuteWithStdin("dd", []string{"of=" + filePath}, string(encoded)); err != nil {
		return nil, fmt.Errorf("cannot write to disk cfg on %v: %v", filePath, err)
	}
	if err := util.CreateDiskPathReplicaSubdirectory(path); err != nil {
		return nil, err
	}
	if _, err := nsExec.Execute("sync", []string{filePath}); err != nil {
		return nil, fmt.Errorf("cannot sync disk cfg on %v: %v", filePath, err)
	}

	return cfg, nil
}

func generateBlockTypeDiskConfig(client engineapi.DiskServiceClient, name, path string) (*util.DiskConfig, error) {
	info, err := client.DiskCreate(name, path)
	if err != nil {
		return nil, err
	}
	return &util.DiskConfig{
		DiskUUID: info.UUID,
	}, nil
}
