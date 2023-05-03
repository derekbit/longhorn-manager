package client

import (
	"strconv"

	"github.com/pkg/errors"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (c *ProxyClient) VolumeGet(engineName, serviceAddress, backendStoreDriver string) (info *etypes.VolumeInfo, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: backendStoreDriver,
	}
	resp, err := c.service.VolumeGet(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	info = &etypes.VolumeInfo{
		Name:                      resp.Volume.Name,
		Size:                      resp.Volume.Size,
		ReplicaCount:              int(resp.Volume.ReplicaCount),
		Endpoint:                  resp.Volume.Endpoint,
		Frontend:                  resp.Volume.Frontend,
		FrontendState:             resp.Volume.FrontendState,
		IsExpanding:               resp.Volume.IsExpanding,
		LastExpansionError:        resp.Volume.LastExpansionError,
		LastExpansionFailedAt:     resp.Volume.LastExpansionFailedAt,
		UnmapMarkSnapChainRemoved: resp.Volume.UnmapMarkSnapChainRemoved,
	}
	return info, nil
}

func (c *ProxyClient) VolumeExpand(engineName, serviceAddress, backendStoreDriver string, size int64) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to expand volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to expand volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineVolumeExpandRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		Expand: &eptypes.VolumeExpandRequest{
			Size: size,
		},
	}
	_, err = c.service.VolumeExpand(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) VolumeFrontendStart(engineName, serviceAddress, backendStoreDriver, frontendName string) (err error) {
	input := map[string]string{
		"serviceAddress": serviceAddress,
		"engineName":     engineName,
		"backendStore":   backendStoreDriver,
		"frontendName":   frontendName,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to start volume frontend")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to start volume frontend %v", c.getProxyErrorPrefix(serviceAddress), frontendName)
	}()

	req := &rpc.EngineVolumeFrontendStartRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		FrontendStart: &eptypes.VolumeFrontendStartRequest{
			Frontend: frontendName,
		},
	}
	_, err = c.service.VolumeFrontendStart(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) VolumeFrontendShutdown(engineName, serviceAddress, backendStoreDriver string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to shutdown volume frontend")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to shutdown volume frontend", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: backendStoreDriver,
	}
	_, err = c.service.VolumeFrontendShutdown(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) VolumeUnmapMarkSnapChainRemovedSet(engineName, serviceAddress, backendStoreDriver string, enabled bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"serviceAddress": serviceAddress,
		"backendStore":   backendStoreDriver,
		"enabled":        strconv.FormatBool(enabled),
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to set volume flag UnmapMarkSnapChainRemoved")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to set UnmapMarkSnapChainRemoved", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: backendStoreDriver,
		},
		UnmapMarkSnap: &eptypes.VolumeUnmapMarkSnapChainRemovedSetRequest{Enabled: enabled},
	}
	_, err = c.service.VolumeUnmapMarkSnapChainRemovedSet(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}
