package monitor

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"

	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

// GetDiskStat returns the disk stat of the given directory
func GetDiskStat(diskType longhorn.DiskType, directory string) (stat *util.DiskStat, err error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskStat(directory)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskStat(directory)
	default:
		return nil, fmt.Errorf("unknown disk type %v", diskType)
	}
}

func getFilesystemTypeDiskStat(directory string) (stat *util.DiskStat, err error) {
	return util.GetDiskStat(directory)
}

func getBlockTypeDiskStat(directory string) (stat *util.DiskStat, err error) {
	return &util.DiskStat{}, nil
}

// GetDiskConfig returns the disk config of the given directory
func GetDiskConfig(diskType longhorn.DiskType, path string) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return getFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return getBlockTypeDiskConfig(path)
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

func getBlockTypeDiskConfig(path string) (config *util.DiskConfig, err error) {
	return &util.DiskConfig{}, nil
}

// GenerateDiskConfig generates a disk config for the given directory
func GenerateDiskConfig(diskType longhorn.DiskType, path string) (*util.DiskConfig, error) {
	switch diskType {
	case longhorn.DiskTypeFilesystem:
		return generateFilesystemTypeDiskConfig(path)
	case longhorn.DiskTypeBlock:
		return generateBlockTypeDiskConfig(path)
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

func generateBlockTypeDiskConfig(path string) (*util.DiskConfig, error) {
	// Create a lvstore on the given block device
	return &util.DiskConfig{}, nil
}
