package v14xto150

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.4.x to v1.5.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeNodes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the node upgrade")
	}

	for _, n := range nodeMap {
		if n.Spec.Disks == nil {
			continue
		}

		for name, disk := range n.Spec.Disks {
			if disk.Type == "" {
				disk.Type = longhorn.DiskTypeFilesystem
				n.Spec.Disks[name] = disk
			}
		}
	}

	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumeMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumeMap {
		if v.Spec.BackupCompressionMethod == "" {
			v.Spec.BackupCompressionMethod = longhorn.BackupCompressionMethodGzip
		}
		if v.Spec.DataLocality == longhorn.DataLocalityStrictLocal {
			v.Spec.RevisionCounterDisabled = true
		}
		if v.Spec.BackendStoreDriver == "" {
			v.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}
