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

	return nil
}
