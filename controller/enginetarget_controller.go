package controller

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// EngineTargetController is a placeholder controller for the separated engine target lifecycle.
type EngineTargetController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	instanceHandler *InstanceHandler

	cacheSyncs []cache.InformerSynced
}

func NewEngineTargetController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string, controllerID string) (*EngineTargetController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	etc := &EngineTargetController{
		baseController: newBaseController("longhorn-engine-target", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-engine-target-controller"}),

		ds: ds,
	}
	etc.instanceHandler = NewInstanceHandler(ds, etc, etc.eventRecorder)

	var err error
	if _, err = ds.EngineTargetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    etc.enqueueEngineTarget,
		UpdateFunc: func(old, cur interface{}) { etc.enqueueEngineTarget(cur) },
		DeleteFunc: etc.enqueueEngineTarget,
	}); err != nil {
		return nil, err
	}
	etc.cacheSyncs = append(etc.cacheSyncs, ds.EngineTargetInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    etc.enqueueInstanceManagerChange,
		UpdateFunc: func(old, cur interface{}) { etc.enqueueInstanceManagerChange(cur) },
		DeleteFunc: etc.enqueueInstanceManagerChange,
	}, 0); err != nil {
		return nil, err
	}
	etc.cacheSyncs = append(etc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	return etc, nil
}

func (etc *EngineTargetController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer etc.queue.ShutDown()

	etc.logger.Info("Starting Longhorn engine target controller")
	defer etc.logger.Info("Shut down Longhorn engine target controller")

	if !cache.WaitForNamedCacheSync("longhorn engine targets", stopCh, etc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(etc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (etc *EngineTargetController) worker() {
	for etc.processNextWorkItem() {
	}
}

func (etc *EngineTargetController) processNextWorkItem() bool {
	key, quit := etc.queue.Get()
	if quit {
		return false
	}
	defer etc.queue.Done(key)

	err := etc.syncEngineTarget(key.(string))
	etc.handleErr(err, key)
	return true
}

func (etc *EngineTargetController) handleErr(err error, key interface{}) {
	if err == nil {
		etc.queue.Forget(key)
		return
	}

	log := etc.logger.WithField("EngineTarget", key)
	if etc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn engine target")
		etc.queue.AddRateLimited(key)
		return
	}

	handleReconcileErrorLogging(log, err, "Dropping Longhorn engine target out of the queue")
	etc.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (etc *EngineTargetController) enqueueEngineTarget(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}
	etc.queue.Add(key)
}

func (etc *EngineTargetController) enqueueInstanceManagerChange(obj interface{}) {
	im, isInstanceManager := obj.(*longhorn.InstanceManager)
	if !isInstanceManager {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		im, ok = deletedState.Obj.(*longhorn.InstanceManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("cannot convert DeletedFinalStateUnknown to InstanceManager object: %#v", deletedState.Obj))
			return
		}
	}

	engineTargets, err := etc.ds.ListEngineTargetsRO()
	if err != nil {
		etc.logger.WithError(err).Warn("Failed to list engine targets")
		return
	}

	engineTargetMap := map[string]*longhorn.EngineTarget{}
	for _, et := range engineTargets {
		if et.Spec.NodeID == im.Spec.NodeID || et.Status.InstanceManagerName == im.Name {
			engineTargetMap[et.Name] = et
		}
	}

	for _, et := range engineTargetMap {
		etc.enqueueEngineTarget(et)
	}
}

func (etc *EngineTargetController) syncEngineTarget(key string) (err error) {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	engineTarget, err := etc.ds.GetEngineTarget(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	log := getLoggerForEngineTarget(etc.logger, engineTarget)

	if !types.IsDataEngineV2(engineTarget.Spec.DataEngine) {
		log.Warn("Engine target only supports v2 data engine")
		return nil
	}

	if engineTarget.DeletionTimestamp != nil {
		if err := etc.DeleteInstance(engineTarget); err != nil {
			return err
		}
		return nil
	}

	if engineTarget.Status.OwnerID != etc.controllerID {
		engineTarget.Status.OwnerID = etc.controllerID
		engineTarget, err = etc.ds.UpdateEngineTargetStatus(engineTarget)
		if err != nil {
			if apierrors.IsConflict(err) {
				return nil
			}
			return err
		}
		log.Infof("EngineTarget got new owner %v", etc.controllerID)
	}

	existingEngineTarget := engineTarget.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingEngineTarget.Status, engineTarget.Status) {
			_, err = etc.ds.UpdateEngineTargetStatus(engineTarget)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debug("Requeue engine target due to conflict")
			etc.enqueueEngineTarget(engineTarget)
			err = nil
		}
	}()

	if !reflect.DeepEqual(engineTarget.Status.CurrentReplicaAddressMap, engineTarget.Spec.ReplicaAddressMap) {
		engineTarget.Status.CurrentReplicaAddressMap = engineTarget.Spec.ReplicaAddressMap
		return nil
	}

	if err := etc.instanceHandler.ReconcileInstanceState(engineTarget, &engineTarget.Spec.InstanceSpec, &engineTarget.Status.InstanceStatus); err != nil {
		return err
	}
	return nil
}

func (etc *EngineTargetController) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	et, ok := obj.(*longhorn.EngineTarget)
	if !ok {
		return nil, fmt.Errorf("invalid object for engine target instance creation: %v", obj)
	}
	if et.Spec.VolumeName == "" || et.Spec.NodeID == "" {
		return nil, fmt.Errorf("missing parameters for engine target instance creation: %v", et)
	}

	im, err := etc.ds.GetInstanceManagerByInstanceRO(obj)
	if err != nil {
		return nil, err
	}

	if et.Status.InstanceManagerName == "" {
		et.Status.InstanceManagerName = im.Name
	}
	if et.Status.InstanceManagerName != im.Name {
		return nil, fmt.Errorf("found instance manager name conflict %s vs %s during engine target instance creation", et.Status.InstanceManagerName, im.Name)
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			etc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	instanceManagerPod, err := etc.ds.GetPod(im.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get pod for instance manager %v", im.Name)
	}

	instanceManagerStorageIP := etc.ds.GetIPFromPodByCNISetting(instanceManagerPod, types.SettingNameStorageNetwork)

	et.Status.Starting = true
	engineTargetName := et.Name
	if et, err = etc.ds.UpdateEngineTargetStatus(et); err != nil {
		return nil, errors.Wrapf(err, "failed to update engine target %v status.starting to true before sending instance create request", engineTargetName)
	}

	return c.EngineTargetInstanceCreate(&engineapi.EngineTargetInstanceCreateRequest{
		EngineTarget: et,
		Address:      instanceManagerStorageIP,
	})
}

func (etc *EngineTargetController) DeleteInstance(obj interface{}) (err error) {
	et, ok := obj.(*longhorn.EngineTarget)
	if !ok {
		return fmt.Errorf("invalid object for engine target process deletion: %v", obj)
	}

	log := getLoggerForEngineTarget(etc.logger, et)
	var im *longhorn.InstanceManager

	if et.Status.InstanceManagerName == "" {
		if et.Spec.NodeID == "" {
			log.Warn("EngineTarget does not set instance manager name and node ID, will skip the actual instance deletion")
			return nil
		}
		im, err = etc.ds.GetInstanceManagerByInstance(obj)
		if err != nil {
			log.WithError(err).Warn("Failed to detect instance manager for engine target, will skip the actual instance deletion")
			return nil
		}
		log.Infof("Cleaning up the process for engine target in instance manager %v", im.Name)
	} else {
		im, err = etc.ds.GetInstanceManager(et.Status.InstanceManagerName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			log.Warnf("The engine target instance manager %v is gone during the engine target instance %v deletion. Will do nothing for the deletion", et.Status.InstanceManagerName, et.Name)
			return nil
		}
	}

	log.Info("Deleting engine target instance")

	c, err := engineapi.NewInstanceManagerClient(im, true)
	if err != nil {
		return err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			etc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	err = c.InstanceDelete(et.Spec.DataEngine, et.Name, "", string(longhorn.InstanceManagerTypeEngine), "", true)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	return nil
}

func (etc *EngineTargetController) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	et, ok := obj.(*longhorn.EngineTarget)
	if !ok {
		return nil, fmt.Errorf("invalid object for engine target instance get: %v", obj)
	}

	var (
		im  *longhorn.InstanceManager
		err error
	)
	if et.Status.InstanceManagerName == "" {
		im, err = etc.ds.GetInstanceManagerByInstanceRO(obj)
		if err != nil {
			return nil, err
		}
	} else {
		im, err = etc.ds.GetInstanceManagerRO(et.Status.InstanceManagerName)
		if err != nil {
			return nil, err
		}
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			etc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	return c.InstanceGet(et.Spec.DataEngine, et.Name, string(longhorn.InstanceManagerTypeEngine))
}

func (etc *EngineTargetController) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	et, ok := obj.(*longhorn.EngineTarget)
	if !ok {
		return nil, nil, fmt.Errorf("invalid object for engine target instance log: %v", obj)
	}

	im, err := etc.ds.GetInstanceManagerRO(et.Status.InstanceManagerName)
	if err != nil {
		return nil, nil, err
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, nil, err
	}

	stream, err := c.InstanceLog(ctx, et.Spec.DataEngine, et.Name, string(longhorn.InstanceManagerTypeEngine))
	return c, stream, err
}

func getLoggerForEngineTarget(logger logrus.FieldLogger, et *longhorn.EngineTarget) *logrus.Entry {
	return logger.WithField("engineTarget", et.Name)
}
