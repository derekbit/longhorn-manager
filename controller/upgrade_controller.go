package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type UpgradeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *UpgradeController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &UpgradeController{
		baseController: newBaseController("longhorn-upgrade", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-upgrade-controller"}),
	}

	ds.UpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueUpgrade(cur) },
		DeleteFunc: uc.enqueueUpgrade,
	})
	uc.cacheSyncs = append(uc.cacheSyncs, ds.UpgradeInformer.HasSynced)

	return uc
}

func (uc *UpgradeController) enqueueUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *UpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn Upgrade controller")
	defer uc.logger.Info("Shut down Longhorn Upgrade controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *UpgradeController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *UpgradeController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncUpgrade(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *UpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("upgrade", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn upgrade")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn upgrade out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForUpgrade(logger logrus.FieldLogger, upgrade *longhorn.Upgrade) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"upgrade": upgrade.Name,
		},
	)
}

func (uc *UpgradeController) isResponsibleFor(upgrade *longhorn.Upgrade) bool {
	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, "", upgrade.Status.OwnerID)
}

func (uc *UpgradeController) syncUpgrade(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync upgrade %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != uc.namespace {
		return nil
	}

	logrus.Infof("Debug ===> syncUpgrade %v", name)
	return uc.reconcile(name)
}

func (uc *UpgradeController) reconcile(upgradeName string) (err error) {
	upgrade, err := uc.ds.GetUpgrade(upgradeName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForUpgrade(uc.logger, upgrade)

	if !uc.isResponsibleFor(upgrade) {
		return nil
	}

	if upgrade.Status.OwnerID != uc.controllerID {
		upgrade.Status.OwnerID = uc.controllerID
		upgrade, err = uc.ds.UpdateUpgradeStatus(upgrade)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Upgrade got new owner %v", uc.controllerID)
	}

	if !upgrade.DeletionTimestamp.IsZero() {
		return uc.ds.RemoveFinalizerForUpgrade(upgrade)
	}

	existingUpgrade := upgrade.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingUpgrade.Status, upgrade.Status) {
			return
		}
		if _, err := uc.ds.UpdateUpgradeStatus(upgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
			uc.enqueueUpgrade(upgrade)
		}
	}()

	switch upgrade.Status.State {
	case longhorn.UpgradeStateUndefined:
		upgrade.Status.State = longhorn.UpgradeStateUpgrading
		return nil
	case longhorn.UpgradeStateUpgrading:
		if err := uc.upgrade(upgrade); err != nil {
			upgrade.Status.State = longhorn.UpgradeStateError
		}
		return nil
	case longhorn.UpgradeStateCompleted:
		fallthrough
	case longhorn.UpgradeStateError:
		return nil
	default:
		return fmt.Errorf("unknown upgrade state: %v", upgrade.Status.State)
	}
}

func (uc *UpgradeController) upgrade(upgrade *longhorn.Upgrade) error {
	switch upgrade.Spec.BackendStoreDriver {
	case longhorn.BackendStoreDriverTypeV2:
		return uc.upgradeInstanceManagers(upgrade)
	default:
		return fmt.Errorf("unsupported backend store driver: %v", upgrade.Spec.BackendStoreDriver)
	}
}

func (uc *UpgradeController) upgradeInstanceManagers(upgrade *longhorn.Upgrade) error {
	logrus.Infof("Debug ===> UpgradingNode=%v, NodeUpgradeState=%v", upgrade.Status.UpgradingNode, upgrade.Status.NodeUpgradeState)

	if upgrade.Status.UpgradingNode == "" {
		node, err := uc.pickNodeForUpgrade()
		if err != nil {
			return err
		}
		existingNode := node.DeepCopy()

		logrus.Infof("Debug[v2] upgrading node %v", node.Name)

		upgrade.Status.UpgradingNode = node.Name
		upgrade.Status.NodeUpgradeState = longhorn.NodeUpgradeStatePending

		defer func() {
			if !reflect.DeepEqual(node.Spec, existingNode.Spec) {
				_, err := uc.ds.UpdateNode(node)
				if err != nil {
					uc.logger.WithError(err).Errorf("Failed to update node %v", node.Name)
				}
			}
		}()
		node.Spec.AllowScheduling = false
		return nil
	}

	log := uc.logger.WithFields(logrus.Fields{
		"upgradingNode": upgrade.Status.UpgradingNode,
	})

	_, err := uc.ds.GetNodeRO(upgrade.Status.UpgradingNode)
	if err != nil {
		return errors.Wrapf(err, "failed to get node %v", upgrade.Status.UpgradingNode)
	}

	if len(upgrade.Status.UpgradingVolumes) == 0 {
		// Find the associated volumes and add to upgrade.Status.UpgradingVolumes
		// Update the volume.spec.image to default one
		volumes := map[string]*longhorn.UpgradeVolumeInfo{}

		engines, err := uc.ds.ListEnginesByNodeRO(upgrade.Status.UpgradingNode)
		if err != nil {
			return errors.Wrapf(err, "failed to list engines for node %v", upgrade.Status.UpgradingNode)
		}
		for _, engine := range engines {
			volumes[engine.Spec.VolumeName] = &longhorn.UpgradeVolumeInfo{
				NodeID: engine.Status.OwnerID,
			}
		}

		replicas, err := uc.ds.ListReplicasByNodeRO(upgrade.Status.UpgradingNode)
		if err != nil {
			return errors.Wrapf(err, "failed to list volumes for node %v", upgrade.Status.UpgradingNode)
		}
		for _, replica := range replicas {
			volumes[replica.Spec.VolumeName] = &longhorn.UpgradeVolumeInfo{
				NodeID: replica.Status.OwnerID,
			}
		}

		log.Infof("Updating upgrade %v with volumes %+v", upgrade.Name, volumes)
		upgrade.Status.UpgradingVolumes = volumes
		upgrade.Status.NodeUpgradeState = longhorn.NodeUpgradeStateUpgrading
		return nil
	}

	defaultInstanceManagerImage, err := uc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return err
	}

	// Update volume.spec.image and volume.spec.suspendRequested
	for name := range upgrade.Status.UpgradingVolumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v", name)
		}
		existingVolume := volume.DeepCopy()

		volume.Spec.SuspendRequested = true
		if volume.Status.OwnerID == upgrade.Status.UpgradingNode {
			volume.Spec.Image = defaultInstanceManagerImage
		}

		if !reflect.DeepEqual(volume.Spec, existingVolume.Spec) {
			_, err := uc.ds.UpdateVolume(volume)
			if err != nil {
				uc.logger.WithError(err).Warnf("Failed to update volume %v", volume.Name)
			}
		}
	}

	// Wait for all the volumes finish the upgrade
	allVolumesSuspended := true
	for name := range upgrade.Status.UpgradingVolumes {
		engine, err := uc.ds.GetVolumeCurrentEngine(name)
		if err != nil {
			return errors.Wrapf(err, "failed to get engine for volumex %v", name)
		}

		if engine.Status.OwnerID == upgrade.Status.UpgradingNode {
			if engine.Status.CurrentState != longhorn.InstanceStateReconnected {
				allVolumesSuspended = false
				break
			}
		} else {
			if engine.Status.CurrentState != longhorn.InstanceStateSuspended {
				allVolumesSuspended = false
				break
			}
		}
	}

	if allVolumesSuspended {
		log.Info("All volumes are suspended, start to upgrade instance manager")
	}

	return nil
}

func (uc *UpgradeController) pickNodeForUpgrade() (*longhorn.Node, error) {
	nodes, err := uc.ds.ListNodesRO()
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		if !node.Spec.AllowScheduling {
			return node.DeepCopy(), nil
		}

		ims, err := uc.ds.ListInstanceManagersBySelectorRO(node.Name, "", longhorn.InstanceManagerTypeAllInOne, longhorn.BackendStoreDriverTypeV2)
		if err != nil {
			return nil, err
		}
		if len(ims) == 2 {
			return node.DeepCopy(), nil
		}
	}

	return nil, fmt.Errorf("failed to find node for upgrade")
}
