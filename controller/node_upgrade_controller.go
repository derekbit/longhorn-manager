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

const (
	nodeUpgradeControllerResyncPeriod = 5 * time.Second
)

type NodeUpgradeController struct {
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

func NewNodeUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*NodeUpgradeController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &NodeUpgradeController{
		baseController: newBaseController("longhorn-node-upgrade", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-node-upgrade-controller"}),
	}

	ds.NodeUpgradeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueNodeUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueNodeUpgrade(cur) },
		DeleteFunc: uc.enqueueNodeUpgrade,
	}, nodeUpgradeControllerResyncPeriod)
	uc.cacheSyncs = append(uc.cacheSyncs, ds.NodeUpgradeInformer.HasSynced)

	return uc, nil
}

func (uc *NodeUpgradeController) enqueueNodeUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *NodeUpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn NodeUpgrade controller")
	defer uc.logger.Info("Shut down Longhorn NodeUpgrade controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *NodeUpgradeController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *NodeUpgradeController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncNodeUpgrade(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *NodeUpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("nodeUpgrade", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn nodeUpgrade resource")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn nodeUpgrade out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForNodeUpgrade(logger logrus.FieldLogger, nodeUpgrade *longhorn.NodeUpgrade) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"nodeUpgrade": nodeUpgrade.Name,
		},
	)
}

func (uc *NodeUpgradeController) isResponsibleFor(upgrade *longhorn.NodeUpgrade) bool {
	preferredOwnerID := upgrade.Spec.NodeID

	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, preferredOwnerID, upgrade.Status.OwnerID)
}

func (uc *NodeUpgradeController) syncNodeUpgrade(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync nodeUpgrade %v", key)
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

func (uc *NodeUpgradeController) reconcile(upgradeName string) (err error) {
	upgrade, err := uc.ds.GetNodeUpgrade(upgradeName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForNodeUpgrade(uc.logger, upgrade)

	if !uc.isResponsibleFor(upgrade) {
		return nil
	}

	if upgrade.Status.OwnerID != uc.controllerID {
		upgrade.Status.OwnerID = uc.controllerID
		upgrade, err = uc.ds.UpdateNodeUpgradeStatus(upgrade)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("NodeUpgrade resource %v got new owner %v", upgrade.Name, uc.controllerID)
	}

	if !upgrade.DeletionTimestamp.IsZero() {
		return uc.ds.RemoveFinalizerForNodeUpgrade(upgrade)
	}

	existingNodeUpgrade := upgrade.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingNodeUpgrade.Status, upgrade.Status) {
			if _, err := uc.ds.UpdateNodeUpgradeStatus(upgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
				uc.enqueueNodeUpgrade(upgradeName)
			}
		}
	}()

	return uc.handleNodeUpgrade(upgrade)
}

func (uc *NodeUpgradeController) handleNodeUpgrade(upgrade *longhorn.NodeUpgrade) (err error) {
	if !types.IsDataEngineV2(upgrade.Spec.DataEngine) {
		return fmt.Errorf("unsupported data engine %v for nodeUpgrade resource %v", upgrade.Spec.DataEngine, upgrade.Name)
	}

	log := getLoggerForNodeUpgrade(uc.logger, upgrade)

	defer func() {
		if err != nil {
			log.WithError(err).Errorf("Failed to handle nodeUpgrade resource %v", upgrade.Name)
			upgrade.Status.State = longhorn.UpgradeStateError
			upgrade.Status.ErrorMessage = err.Error()
		}
	}()

	switch upgrade.Status.State {
	case longhorn.UpgradeStateUndefined:
		upgrade.Status.State = longhorn.UpgradeStateInitializing
		return nil
	case longhorn.UpgradeStateInitializing:
		log.Infof("Initializing nodeUpgrade resource %v", upgrade.Name)

		volumes, err := uc.collectVolumes(upgrade)
		if err != nil {
			return err
		}

		upgrade.Status.Volumes = volumes
		upgrade.Status.State = longhorn.UpgradeStateUpgrading

		return nil
	case longhorn.UpgradeStateUpgrading:
		log.Infof("Upgrading nodeUpgrade resource %v", upgrade.Name)

		if upgradeCompleted := uc.reconcileVolumes(upgrade); upgradeCompleted {
			node, err := uc.ds.GetNode(upgrade.Status.OwnerID)
			if err != nil {
				return errors.Wrapf(err, "failed to get node %v for nodeUpgrade resource %v", upgrade.Status.OwnerID, upgrade.Name)
			}

			if node.Spec.UpgradeRequested {
				log.Infof("Setting upgradeRequested to false for node %v since the node upgrade is completed", upgrade.Status.OwnerID)

				node.Spec.UpgradeRequested = false
				_, err = uc.ds.UpdateNode(node)
				if err != nil {
					return errors.Wrapf(err, "failed to update node %v for updating upgrade requested to true", upgrade.Status.OwnerID)
				}
			}

			upgrade.Status.State = longhorn.UpgradeStateCompleted
		}

		return nil
	case longhorn.UpgradeStateError, longhorn.UpgradeStateCompleted:
		return nil
	default:
		return fmt.Errorf("unknown state %v for nodeUpgrade resource %v", upgrade.Status.State, upgrade.Name)
	}
}

func (uc *NodeUpgradeController) collectVolumes(upgrade *longhorn.NodeUpgrade) (map[string]*longhorn.VolumeUpgradeStatus, error) {
	engines, err := uc.ds.ListEnginesByNodeRO(upgrade.Status.OwnerID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list engines on node %v", upgrade.Status.OwnerID)
	}

	volumes := make(map[string]*longhorn.VolumeUpgradeStatus)
	for _, engine := range engines {
		if engine.Spec.DataEngine != upgrade.Spec.DataEngine {
			continue
		}

		volume, err := uc.ds.GetVolumeRO(engine.Spec.VolumeName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get volume %v for engine %v", engine.Spec.VolumeName, engine.Name)
		}

		volumes[volume.Name] = &longhorn.VolumeUpgradeStatus{
			State: longhorn.UpgradeStateInitializing,
		}
	}

	return volumes, nil
}

func (uc *NodeUpgradeController) reconcileVolumes(upgrade *longhorn.NodeUpgrade) (upgradeCompleted bool) {
	uc.updateVolumes(upgrade)

	return uc.AreAllVolumesUpgraded(upgrade)
}

func (uc *NodeUpgradeController) updateVolumes(upgrade *longhorn.NodeUpgrade) {
	for name, volume := range upgrade.Status.Volumes {
		if volume.State == longhorn.UpgradeStateInitializing {
			if err := uc.updateVolumeImage(name, upgrade.Spec.InstanceManagerImage); err != nil {
				uc.logger.WithError(err).Warnf("Failed to update volume %v", name)
				volume.State = longhorn.UpgradeStateError
				volume.ErrorMessage = err.Error()
			} else {
				volume.State = longhorn.UpgradeStateUpgrading
			}
		}
	}
}

func (uc *NodeUpgradeController) updateVolumeImage(volumeName, image string) error {
	volume, err := uc.ds.GetVolume(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v for upgrade", volumeName)
	}

	if volume.Spec.Image != image {
		volume.Spec.Image = image
		_, err := uc.ds.UpdateVolume(volume)
		if err != nil {
			return errors.Wrapf(err, "failed to update volume %v for upgrade", volumeName)
		}
	}
	return nil
}

func (uc *NodeUpgradeController) AreAllVolumesUpgraded(upgrade *longhorn.NodeUpgrade) bool {
	allUpgraded := true

	for name := range upgrade.Status.Volumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			upgrade.Status.Volumes[name].State = longhorn.UpgradeStateError
			upgrade.Status.Volumes[name].ErrorMessage = err.Error()
			continue
		}

		if volume.Status.CurrentImage == upgrade.Spec.InstanceManagerImage {
			upgrade.Status.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			upgrade.Status.Volumes[name].State = longhorn.UpgradeStateUpgrading
			allUpgraded = false
		}
	}
	return allUpgraded
}
