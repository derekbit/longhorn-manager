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

type UpgradeManagerController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	upgradeManagerMonitor monitor.Monitor

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewUpgradeManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*UpgradeManagerController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &UpgradeManagerController{
		baseController: newBaseController("longhorn-upgrade-manager", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-upgrade-manager-controller"}),
	}

	if _, err := ds.UpgradeManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueUpgradeManager,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueUpgradeManager(cur) },
		DeleteFunc: uc.enqueueUpgradeManager,
	}); err != nil {
		return nil, err
	}
	uc.cacheSyncs = append(uc.cacheSyncs, ds.UpgradeManagerInformer.HasSynced)

	return uc, nil
}

func (uc *UpgradeManagerController) enqueueUpgradeManager(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *UpgradeManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn UpgradeManager controller")
	defer uc.logger.Info("Shut down Longhorn UpgradeManager controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *UpgradeManagerController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *UpgradeManagerController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncUpgradeManager(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *UpgradeManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("upgradeManager", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn upgradeManager resource")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn upgradeManager out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForUpgradeManager(logger logrus.FieldLogger, upgradeManager *longhorn.UpgradeManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"upgradeManager": upgradeManager.Name,
		},
	)
}

func (uc *UpgradeManagerController) isResponsibleFor(upgrade *longhorn.UpgradeManager) bool {
	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, "", upgrade.Status.OwnerID)
}

func (uc *UpgradeManagerController) syncUpgradeManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync upgradeManager %v", key)
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

func (uc *UpgradeManagerController) reconcile(upgradeManagerName string) (err error) {
	upgradeManager, err := uc.ds.GetUpgradeManager(upgradeManagerName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForUpgradeManager(uc.logger, upgradeManager)

	if !uc.isResponsibleFor(upgradeManager) {
		return nil
	}

	if upgradeManager.Status.OwnerID != uc.controllerID {
		upgradeManager.Status.OwnerID = uc.controllerID
		upgradeManager, err = uc.ds.UpdateUpgradeManagerStatus(upgradeManager)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("UpgradeManager resource %v got new owner %v", upgradeManager.Name, uc.controllerID)
	}

	if !upgradeManager.DeletionTimestamp.IsZero() {
		if uc.upgradeManagerMonitor != nil {
			uc.upgradeManagerMonitor.Close()
			uc.upgradeManagerMonitor = nil
		}

		return uc.ds.RemoveFinalizerForUpgradeManager(upgradeManager)
	}

	if upgradeManager.Status.State == longhorn.UpgradeStateCompleted ||
		upgradeManager.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	existingUpgradeManager := upgradeManager.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingUpgradeManager.Status, upgradeManager.Status) {
			if _, err := uc.ds.UpdateUpgradeManagerStatus(upgradeManager); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeManagerName)
				uc.enqueueUpgradeManager(upgradeManager)
			}
		}
	}()

	if _, err := uc.createUpgradeManagerMonitor(upgradeManager); err != nil {
		return err
	}

	if uc.upgradeManagerMonitor != nil {
		data, _ := uc.upgradeManagerMonitor.GetCollectedData()
		status, ok := data.(*longhorn.UpgradeManagerStatus)
		if !ok {
			log.Errorf("Failed to assert value from upgradeManager monitor: %v", data)
		} else {
			upgradeManager.Status.InstanceManagerImage = status.InstanceManagerImage
			upgradeManager.Status.State = status.State
			upgradeManager.Status.ErrorMessage = status.ErrorMessage
			upgradeManager.Status.UpgradingNode = status.UpgradingNode
			upgradeManager.Status.UpgradeNodes = make(map[string]*longhorn.UpgradeNodeStatus)
			for k, v := range status.UpgradeNodes {
				upgradeManager.Status.UpgradeNodes[k] = &longhorn.UpgradeNodeStatus{
					State:        v.State,
					ErrorMessage: v.ErrorMessage,
				}
			}
		}
	}

	if upgradeManager.Status.State == longhorn.UpgradeStateCompleted ||
		upgradeManager.Status.State == longhorn.UpgradeStateError {
		uc.upgradeManagerMonitor.Close()
		uc.upgradeManagerMonitor = nil
	}

	return nil
}

func (uc *UpgradeManagerController) createUpgradeManagerMonitor(upgradeManager *longhorn.UpgradeManager) (monitor.Monitor, error) {
	if uc.upgradeManagerMonitor != nil {
		return uc.upgradeManagerMonitor, nil
	}

	monitor, err := monitor.NewUpgradeManagerMonitor(uc.logger, uc.ds, upgradeManager.Name, upgradeManager.Status.OwnerID, uc.enqueueUpgradeManagerForMonitor)
	if err != nil {
		return nil, err
	}

	uc.upgradeManagerMonitor = monitor

	return monitor, nil
}

func (uc *UpgradeManagerController) enqueueUpgradeManagerForMonitor(key string) {
	uc.queue.Add(key)
}
