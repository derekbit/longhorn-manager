package engineapi

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	smclient "github.com/longhorn/longhorn-share-manager/pkg/client"
	v1 "k8s.io/api/core/v1"
)

type ShareManagerClient struct {
	grpcClient *smclient.ShareManagerClient
}

func NewShareManagerClient(sm *longhorn.ShareManager, pod *v1.Pod) (*ShareManagerClient, error) {
	if sm.Status.State != longhorn.ShareManagerStateRunning {
		return nil, fmt.Errorf("invalid Share Manager %v, state: %v", sm.Name, sm.Status.State)
	}

	return &ShareManagerClient{
		grpcClient: smclient.NewShareManagerClient(fmt.Sprintf("%s:%d", pod.Status.PodIP, ShareManagerDefaultPort)),
	}, nil
}

func (c *ShareManagerClient) FilesystemTrim(isEncryptedDevice bool) error {
	return c.grpcClient.FilesystemTrim(isEncryptedDevice)
}

func (c *ShareManagerClient) FilesystemMount() error {
	return c.grpcClient.FilesystemMount()
}

func (c *ShareManagerClient) FilesystemMountStatus() (*longhorn.ShareManagerMountStatus, error) {
	resp, err := c.grpcClient.FilesystemMountStatus()
	if err != nil {
		return nil, err
	}

	return &longhorn.ShareManagerMountStatus{
		State: resp.State,
		Error: resp.Error,
	}, nil
}
