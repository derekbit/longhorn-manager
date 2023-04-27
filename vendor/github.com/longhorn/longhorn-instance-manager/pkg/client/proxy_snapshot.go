package client

import (
	"github.com/pkg/errors"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

func (c *ProxyClient) VolumeSnapshot(engineName, serviceAddress, backendStoreDriver, volumeSnapshotName string, labels map[string]string) (snapshotName string, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return "", errors.Wrap(err, "failed to snapshot volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to snapshot volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	for key, value := range labels {
		if errList := eutil.IsQualifiedName(key); len(errList) > 0 {
			err = errors.Errorf("invalid key %v for label: %v", key, errList[0])
			return "", err
		}

		// We don't need to validate the Label value since we're allowing for any form of data to be stored, similar
		// to Kubernetes Annotations. Of course, we should make sure it isn't empty.
		if value == "" {
			err = errors.Errorf("invalid empty value for label with key %v", key)
			return "", err
		}
	}

	req := &rpc.EngineVolumeSnapshotRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		SnapshotVolume: &eptypes.VolumeSnapshotRequest{
			Name:   volumeSnapshotName,
			Labels: labels,
		},
	}
	recv, err := c.service.VolumeSnapshot(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return "", err
	}

	return recv.Snapshot.Name, nil
}

func (c *ProxyClient) SnapshotList(engineName, serviceAddress, backendStoreDriver string) (snapshotDiskInfo map[string]*etypes.DiskInfo, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to list snapshots")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to list snapshots", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: backendStoreDriver,
	}
	resp, err := c.service.SnapshotList(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	snapshotDiskInfo = map[string]*etypes.DiskInfo{}
	for k, v := range resp.Disks {
		if v.Children == nil {
			v.Children = map[string]bool{}
		}
		if v.Labels == nil {
			v.Labels = map[string]string{}
		}
		snapshotDiskInfo[k] = &etypes.DiskInfo{
			Name:        v.Name,
			Parent:      v.Parent,
			Children:    v.Children,
			Removed:     v.Removed,
			UserCreated: v.UserCreated,
			Created:     v.Created,
			Size:        v.Size,
			Labels:      v.Labels,
		}
	}
	return snapshotDiskInfo, nil
}

func (c *ProxyClient) SnapshotClone(engineName, serviceAddress, backendStoreDriver, snapshotName, fromController string, fileSyncHTTPClientTimeout int) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
		"snapshotName":   snapshotName,
		"fromController": fromController,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to clone snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to clone snapshot %v from %v", c.getProxyErrorPrefix(serviceAddress), snapshotName, fromController)
	}()

	req := &rpc.EngineSnapshotCloneRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		FromController:            fromController,
		SnapshotName:              snapshotName,
		ExportBackingImageIfExist: false,
		FileSyncHttpClientTimeout: int32(fileSyncHTTPClientTimeout),
	}
	_, err = c.service.SnapshotClone(getContextWithGRPCLongTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) SnapshotCloneStatus(engineName, serviceAddress, backendStoreDriver string) (status map[string]*SnapshotCloneStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot clone status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot clone status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: backendStoreDriver,
	}
	recv, err := c.service.SnapshotCloneStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = map[string]*SnapshotCloneStatus{}
	for k, v := range recv.Status {
		status[k] = &SnapshotCloneStatus{
			IsCloning:          v.IsCloning,
			Error:              v.Error,
			Progress:           int(v.Progress),
			State:              v.State,
			FromReplicaAddress: v.FromReplicaAddress,
			SnapshotName:       v.SnapshotName,
		}
	}
	return status, nil
}

func (c *ProxyClient) SnapshotRevert(engineName, serviceAddress, backendStoreDriver string, name string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
		"name":           name,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to revert volume to snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to revert volume to snapshot %v", c.getProxyErrorPrefix(serviceAddress), name)
	}()

	if name == etypes.VolumeHeadName {
		err = errors.Errorf("invalid operation: cannot revert to %v", etypes.VolumeHeadName)
		return err
	}

	req := &rpc.EngineSnapshotRevertRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		Name: name,
	}
	_, err = c.service.SnapshotRevert(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) SnapshotPurge(engineName, serviceAddress, backendStoreDriver string, skipIfInProgress bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to purge snapshots")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to purge snapshots", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotPurgeRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		SkipIfInProgress: skipIfInProgress,
	}
	_, err = c.service.SnapshotPurge(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) SnapshotPurgeStatus(engineName, serviceAddress, backendStoreDriver string) (status map[string]*SnapshotPurgeStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot purge status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot purge status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: backendStoreDriver,
	}

	recv, err := c.service.SnapshotPurgeStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = make(map[string]*SnapshotPurgeStatus)
	for k, v := range recv.Status {
		status[k] = &SnapshotPurgeStatus{
			Error:     v.Error,
			IsPurging: v.IsPurging,
			Progress:  int(v.Progress),
			State:     v.State,
		}
	}
	return status, nil
}

func (c *ProxyClient) SnapshotRemove(engineName, serviceAddress, backendStoreDriver string, names []string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrapf(err, "failed to remove snapshot %v", names)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to remove snapshot %v", c.getProxyErrorPrefix(serviceAddress), names)
	}()

	req := &rpc.EngineSnapshotRemoveRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		Names: names,
	}
	_, err = c.service.SnapshotRemove(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) SnapshotHash(engineName, serviceAddress, backendStoreDriver string, snapshotName string, rehash bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to hash snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to hash snapshot", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotHashRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		SnapshotName: snapshotName,
		Rehash:       rehash,
	}
	_, err = c.service.SnapshotHash(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) SnapshotHashStatus(engineName, serviceAddress, backendStoreDriver, snapshotName string) (status map[string]*SnapshotHashStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot hash status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot hash status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotHashStatusRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		SnapshotName: snapshotName,
	}

	recv, err := c.service.SnapshotHashStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = make(map[string]*SnapshotHashStatus)
	for k, v := range recv.Status {
		status[k] = &SnapshotHashStatus{
			State:             v.State,
			Checksum:          v.Checksum,
			Error:             v.Error,
			SilentlyCorrupted: v.SilentlyCorrupted,
		}
	}

	return status, nil
}
