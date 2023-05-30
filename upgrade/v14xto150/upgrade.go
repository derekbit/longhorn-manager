package v14xto150

import (
	"context"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.4.x to v1.5.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeWebhookAndRecoveryService(namespace, kubeClient); err != nil {
		return err
	}

	if err := upgradeNodes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeInstanceManagers(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeEngines(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackups(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeSnapshots(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeReplicas(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeOrphans(namespace, lhClient, resourceMaps)
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
		if n.Spec.Disks != nil {
			for name, disk := range n.Spec.Disks {
				if disk.Type == "" {
					disk.Type = longhorn.DiskTypeFilesystem
					n.Spec.Disks[name] = disk
				}
			}
		}

		if n.Status.DiskStatus != nil {
			for name, disk := range n.Status.DiskStatus {
				if disk.Type == "" {
					disk.Type = longhorn.DiskTypeFilesystem
					n.Status.DiskStatus[name] = disk
				}
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
		if v.Spec.ReplicaSoftAntiAffinity == "" {
			v.Spec.ReplicaSoftAntiAffinity = longhorn.ReplicaSoftAntiAffinityDefault
		}
		if v.Spec.ReplicaZoneSoftAntiAffinity == "" {
			v.Spec.ReplicaZoneSoftAntiAffinity = longhorn.ReplicaZoneSoftAntiAffinityDefault
		}
		if v.Spec.BackendStoreDriver == "" {
			v.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}

func upgradeWebhookAndRecoveryService(namespace string, kubeClient *clientset.Clientset) error {
	selectors := []string{"app=longhorn-conversion-webhook", "app=longhorn-admission-webhook", "app=longhorn-recovery-backend"}

	for _, selector := range selectors {
		deployments, err := kubeClient.AppsV1().Deployments("").List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return errors.Wrapf(err, upgradeLogPrefix+"failed to get deployment with label %v during the upgrade", selector)
		}
		for _, deployment := range deployments.Items {
			err := kubeClient.AppsV1().Deployments(deployment.Namespace).Delete(context.TODO(), deployment.Name, metav1.DeleteOptions{})
			if err != nil {
				return errors.Wrapf(err, upgradeLogPrefix+"failed to delete the deployment with label %v during the upgrade", selector)
			}
		}
	}
	return nil
}

func upgradeInstanceManagers(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade InstanceManagers failed")
	}()

	instanceManagerMap, err := upgradeutil.ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to list all existing Longhorn InstanceManagers")
	}

	for _, im := range instanceManagerMap {
		for name, instance := range im.Status.Instances {
			if instance.Spec.BackendStoreDriver == "" {
				instance.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
				im.Status.Instances[name] = instance
			}
		}
	}

	engineList, err := lhClient.LonghornV1beta2().Engines(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn Engines")
	}
	for _, engine := range engineList.Items {
		for _, instanceManager := range instanceManagerMap {
			if engine.Status.InstanceManagerName != instanceManager.Name {
				continue
			}
			instanceManager.Status.InstanceEngines = instanceManager.Status.Instances
			instanceManager.Status.Instances = nil
			break
		}
	}

	replicaList, err := lhClient.LonghornV1beta2().Replicas(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn Replicas")
	}
	for _, engine := range replicaList.Items {
		for _, instanceManager := range instanceManagerMap {
			if engine.Status.InstanceManagerName != instanceManager.Name {
				continue
			}
			instanceManager.Status.InstanceReplicas = instanceManager.Status.Instances
			instanceManager.Status.Instances = nil
			break
		}
	}

	return nil
}

func upgradeReplicas(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade replica failed")
	}()

	replicaMap, err := upgradeutil.ListAndUpdateReplicasInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn replicas during the replica upgrade")
	}

	for _, r := range replicaMap {
		if r.Spec.BackendStoreDriver == "" {
			r.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}

func upgradeEngines(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engine failed")
	}()

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the engine upgrade")
	}

	for _, e := range engineMap {
		if e.Spec.BackendStoreDriver == "" {
			e.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup failed")
	}()

	backupMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backups during the backup upgrade")
	}

	for _, b := range backupMap {
		if b.Spec.BackendStoreDriver == "" {
			b.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}

func upgradeSnapshots(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade snapshot failed")
	}()

	snapshotMap, err := upgradeutil.ListAndUpdateSnapshotsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn snapshots during the snapshot upgrade")
	}

	for _, s := range snapshotMap {
		if s.Spec.BackendStoreDriver == "" {
			s.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeLonghorn
		}
	}

	return nil
}

func upgradeOrphans(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade orphan failed")
	}()

	orphanMap, err := upgradeutil.ListAndUpdateOrphansInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn orphans during the orphan upgrade")
	}

	for _, o := range orphanMap {
		if o.Spec.Parameters == nil {
			continue
		}

		if _, ok := o.Spec.Parameters[longhorn.OrphanDiskType]; !ok {
			o.Spec.Parameters[longhorn.OrphanDiskType] = string(longhorn.DiskTypeFilesystem)
		}
	}

	return nil
}
