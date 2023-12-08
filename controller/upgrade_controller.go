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
		log.Infof("Upgrade %v got new owner %v", upgrade.Name, uc.controllerID)
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

	return uc.upgrade(upgrade)
}

func (uc *UpgradeController) upgrade(upgrade *longhorn.Upgrade) error {
	switch upgrade.Spec.BackendStoreDriver {
	case longhorn.BackendStoreDriverTypeV2:
		return uc.upgradeInstanceManagers(upgrade)
	default:
		return fmt.Errorf("unsupported backend store driver: %v", upgrade.Spec.BackendStoreDriver)
	}
}

func (uc *UpgradeController) upgradeInstanceManagers(u *longhorn.Upgrade) (err error) {
	if u.Status.State == longhorn.UpgradedNodeStateCompleted ||
		u.Status.State == longhorn.UpgradedNodeStateError {
		return nil
	}

	defer func() {
		if err != nil {
			u.Status.State = longhorn.UpgradedNodeStateError
		}

	}()

	if u.Spec.UpgradedNode == "" {
		u.Status.State = longhorn.UpgradedNodeStateError
		return fmt.Errorf("UpgradeNode is not set for upgrade %v", u.Name)
	}

	node, err := uc.ds.GetNode(u.Spec.UpgradedNode)
	if err != nil {
		return errors.Wrapf(err, "failed to get node %v", u.Spec.UpgradedNode)
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		return fmt.Errorf("node %v is not ready", node.Name)
	}

	if !node.Spec.UpgradeRequested {
		node.Spec.UpgradeRequested = true

		node, err = uc.ds.UpdateNode(node)
		if err != nil {
			return errors.Wrapf(err, "failed to update node %v for upgrade %v", node.Name, u.Name)
		}
	}

	log := uc.logger.WithFields(logrus.Fields{
		"upgradingNode": u.Spec.UpgradedNode,
	})

	if u.Status.Volumes == nil {
		u.Status.Volumes = map[string]*longhorn.UpgradedVolumeInfo{}
	}

	switch u.Status.State {
	case longhorn.UpgradedNodeStateUndefined:
		log.Infof("Switching upgrade %v to pending state", u.Name)
		u.Status.State = longhorn.UpgradedNodeStatePending

	case longhorn.UpgradedNodeStatePending:
		if err := uc.updateVolumesForUpgrade(u, log); err != nil {
			return err
		}
	case longhorn.UpgradedNodeStateUpgrading:
		if err := uc.handleVolumeSuspension(u, log); err != nil {
			return err
		}
	}

	return nil
}

func (uc *UpgradeController) updateVolumesForUpgrade(u *longhorn.Upgrade, log *logrus.Entry) error {
	upgradeVolumes := map[string]*longhorn.UpgradedVolumeInfo{}

	engines, err := uc.ds.ListEnginesByNodeRO(u.Spec.UpgradedNode)
	if err != nil {
		return errors.Wrapf(err, "failed to list engines for node %v", u.Spec.UpgradedNode)
	}

	for _, e := range engines {
		v, err := uc.ds.GetVolumeRO(e.Spec.VolumeName)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v for engine %v", e.Spec.VolumeName, e.Name)
		}

		if v.Status.State != longhorn.VolumeStateAttached &&
			v.Status.State != longhorn.VolumeStateDetached {
			return fmt.Errorf("volume %v is not attached or detached", v.Name)
		}

		upgradeVolumes[v.Name] = &longhorn.UpgradedVolumeInfo{
			NodeID: v.Status.OwnerID,
		}
	}

	log.Infof("Switching upgrade %v to upgrading state", u.Name)
	u.Status.Volumes = upgradeVolumes
	u.Status.State = longhorn.UpgradedNodeStateUpgrading

	return nil
}

func (uc *UpgradeController) handleVolumeSuspension(u *longhorn.Upgrade, log *logrus.Entry) error {
	defaultInstanceManagerImage, err := uc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return errors.Wrapf(err, "failed to get setting %v for upgrade %v", types.SettingNameDefaultInstanceManagerImage, u.Name)
	}

	allVolumeRequestedToUpgrade := true
	for name := range u.Status.Volumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v for upgrade %v", name, u.Name)
		}

		if volume.Spec.Image != defaultInstanceManagerImage {
			volume.Spec.Image = defaultInstanceManagerImage

			_, err := uc.ds.UpdateVolume(volume)
			if err != nil {
				uc.logger.WithError(err).Warnf("Failed to update volume %v", volume.Name)
				allVolumeRequestedToUpgrade = false
			}
		}
	}

	if !allVolumeRequestedToUpgrade {
		return fmt.Errorf("failed to update all volumes to use default instance manager image")
	}

	// Check if all volumes are upgraded
	for name := range u.Status.Volumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v for upgrade %v", name, u.Name)
		}

		if volume.Status.CurrentImage != defaultInstanceManagerImage {
			return fmt.Errorf("volume %v is still upgrading", volume.Name)
		}
	}

	log.Infof("Switching upgrade %v to completed state", u.Name)
	u.Status.State = longhorn.UpgradedNodeStateCompleted

	return nil
}
