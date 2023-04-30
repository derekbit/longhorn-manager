package engineapi

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) VolumeGet(e *longhorn.Engine) (volume *Volume, err error) {
	recv, err := p.grpcClient.VolumeGet(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver))
	if err != nil {
		return nil, err
	}

	return (*Volume)(recv), nil
}

func (p *Proxy) VolumeExpand(e *longhorn.Engine) (err error) {
	return p.grpcClient.VolumeExpand(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver), e.Spec.VolumeSize)
}

func (p *Proxy) VolumeFrontendStart(e *longhorn.Engine) (err error) {
	frontendName, err := GetEngineProcessFrontend(e.Spec.Frontend)
	if err != nil {
		return err
	}

	if frontendName == "" {
		return fmt.Errorf("cannot start empty frontend")
	}

	return p.grpcClient.VolumeFrontendStart(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver), frontendName)
}

func (p *Proxy) VolumeFrontendShutdown(e *longhorn.Engine) (err error) {
	return p.grpcClient.VolumeFrontendShutdown(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver))
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeUnmapMarkSnapChainRemovedSet(e.Name, p.DirectToURL(e), string(e.Spec.BackendStoreDriver), e.Spec.UnmapMarkSnapChainRemovedEnabled)
}
