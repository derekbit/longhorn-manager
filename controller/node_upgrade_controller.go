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

	"github.com/longhorn/longhorn-manager/controller/monitor"
	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type NodeUpgradeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	nodeUpgradeMonitor monitor.Monitor

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

	if _, err := ds.NodeUpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueNodeUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueNodeUpgrade(cur) },
		DeleteFunc: uc.enqueueNodeUpgrade,
	}); err != nil {
		return nil, err
	}
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
	nodeUpgrade, err := uc.ds.GetNodeUpgrade(upgradeName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForNodeUpgrade(uc.logger, nodeUpgrade)

	if !uc.isResponsibleFor(nodeUpgrade) {
		log.Debugf("Skip nodeUpgrade %v as it's not owned by this controller", nodeUpgrade.Name)
		return nil
	}

	if nodeUpgrade.Status.OwnerID != uc.controllerID {
		nodeUpgrade.Status.OwnerID = uc.controllerID
		nodeUpgrade, err = uc.ds.UpdateNodeUpgradeStatus(nodeUpgrade)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("NodeUpgrade resource %v got new owner %v", nodeUpgrade.Name, uc.controllerID)
	}

	if !nodeUpgrade.DeletionTimestamp.IsZero() {
		if uc.nodeUpgradeMonitor != nil {
			uc.nodeUpgradeMonitor.Close()
			uc.nodeUpgradeMonitor = nil
		}

		return uc.ds.RemoveFinalizerForNodeUpgrade(nodeUpgrade)
	}

	if nodeUpgrade.Status.State == longhorn.UpgradeStateCompleted ||
		nodeUpgrade.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	existingNodeUpgrade := nodeUpgrade.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingNodeUpgrade.Status, nodeUpgrade.Status) {
			if _, err := uc.ds.UpdateNodeUpgradeStatus(nodeUpgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
				uc.enqueueNodeUpgrade(upgradeName)
			}
		}
	}()

	if _, err := uc.createNodeUpgradeMonitor(nodeUpgrade); err != nil {
		return err
	}

	if uc.nodeUpgradeMonitor != nil {
		data, _ := uc.nodeUpgradeMonitor.GetCollectedData()
		status, ok := data.(*longhorn.NodeUpgradeStatus)
		if !ok {
			log.Errorf("Failed to assert value from nodeUpgrade monitor: %v", data)
		} else {
			nodeUpgrade.Status.State = status.State
			nodeUpgrade.Status.ErrorMessage = status.ErrorMessage
			nodeUpgrade.Status.Volumes = make(map[string]*longhorn.VolumeUpgradeStatus)
			for k, v := range status.Volumes {
				nodeUpgrade.Status.Volumes[k] = &longhorn.VolumeUpgradeStatus{
					State:        v.State,
					ErrorMessage: v.ErrorMessage,
				}
			}
		}
	}

	if nodeUpgrade.Status.State == longhorn.UpgradeStateCompleted ||
		nodeUpgrade.Status.State == longhorn.UpgradeStateError {
		uc.nodeUpgradeMonitor.Close()
		uc.nodeUpgradeMonitor = nil
	}

	return nil
}

func (uc *NodeUpgradeController) createNodeUpgradeMonitor(nodeUpgrade *longhorn.NodeUpgrade) (monitor.Monitor, error) {
	if uc.nodeUpgradeMonitor != nil {
		return uc.nodeUpgradeMonitor, nil
	}

	monitor, err := monitor.NewNodeUpgradeMonitor(uc.logger, uc.ds, nodeUpgrade.Name, nodeUpgrade.Status.OwnerID, uc.enqueueNodeUpgradeForMonitor)
	if err != nil {
		return nil, err
	}

	uc.nodeUpgradeMonitor = monitor

	return monitor, nil
}

func (uc *NodeUpgradeController) enqueueNodeUpgradeForMonitor(key string) {
	uc.queue.Add(key)
}
