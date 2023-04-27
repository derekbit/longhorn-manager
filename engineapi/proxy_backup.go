package engineapi

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/longhorn/backupstore"
	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) SnapshotBackup(e *longhorn.Engine, snapshotName, backupName, backupTarget,
	backingImageName, backingImageChecksum, compressionMethod string, concurrentLimit int,
	labels, credential map[string]string) (string, string, error) {
	if snapshotName == etypes.VolumeHeadName {
		return "", "", fmt.Errorf("invalid operation: cannot backup %v", etypes.VolumeHeadName)
	}

	if e == nil {
		return "", "", errors.Wrapf(errors.Errorf("missing engine"), "failed to backup %v", snapshotName)
	}

	snap, err := p.SnapshotGet(e, snapshotName)
	if err != nil {
		return "", "", errors.Wrapf(err, "error getting snapshot '%s', engine '%s'", snapshotName, e.Name)
	}

	if snap == nil {
		return "", "", errors.Errorf("could not find snapshot '%s' to backup, engine '%s'", snapshotName, e.Name)
	}

	// get environment variables if backup for s3
	credentialEnv, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return "", "", err
	}

	backupID, replicaAddress, err := p.grpcClient.SnapshotBackup(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver),
		backupName, snapshotName, backupTarget, backingImageName, backingImageChecksum,
		compressionMethod, concurrentLimit, labels, credentialEnv,
	)
	if err != nil {
		return "", "", err
	}

	return backupID, replicaAddress, nil
}

func (p *Proxy) SnapshotBackupStatus(e *longhorn.Engine, backupName, replicaAddress string) (status *longhorn.EngineBackupStatus, err error) {
	recv, err := p.grpcClient.SnapshotBackupStatus(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver), backupName, replicaAddress)
	if err != nil {
		return nil, err
	}

	return (*longhorn.EngineBackupStatus)(recv), nil
}

func (p *Proxy) BackupRestore(e *longhorn.Engine, backupTarget, backupName, backupVolumeName, lastRestored string,
	credential map[string]string, concurrentLimit int) error {
	backupURL := backupstore.EncodeBackupURL(backupName, backupVolumeName, backupTarget)

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return err
	}

	return p.grpcClient.BackupRestore(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver), backupURL, backupTarget, backupVolumeName, envs, concurrentLimit)
}

func (p *Proxy) BackupRestoreStatus(e *longhorn.Engine) (status map[string]*longhorn.RestoreStatus, err error) {
	recv, err := p.grpcClient.BackupRestoreStatus(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver))
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.RestoreStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.RestoreStatus)(v)
	}
	return status, nil
}
