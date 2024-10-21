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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	upgradeManagerControllerResyncPeriod = 5 * time.Second
)

type UpgradeManagerController struct {
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

	ds.UpgradeManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueUpgradeManager,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueUpgradeManager(cur) },
		DeleteFunc: uc.enqueueUpgradeManager,
	}, upgradeManagerControllerResyncPeriod)
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
		return uc.ds.RemoveFinalizerForUpgradeManager(upgradeManager)
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

	return uc.handleUpgradeManager(upgradeManager)
}

func (uc *UpgradeManagerController) handleUpgradeManager(upgradeManager *longhorn.UpgradeManager) (err error) {
	if !types.IsDataEngineV2(upgradeManager.Spec.DataEngine) {
		return fmt.Errorf("unsupported data engine %v for upgradeManager resource %v", upgradeManager.Spec.DataEngine, upgradeManager.Name)
	}

	log := uc.logger.WithFields(logrus.Fields{"upgradeManager": upgradeManager.Name})

	if upgradeManager.Status.State == longhorn.UpgradeStateCompleted ||
		upgradeManager.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	defer func() {
		if err != nil {
			upgradeManager.Status.State = longhorn.UpgradeStateError
		}
	}()

	if upgradeManager.Status.UpgradeNodes == nil {
		upgradeManager.Status.UpgradeNodes = map[string]*longhorn.UpgradeNodeStatus{}
	}

	switch upgradeManager.Status.State {
	case longhorn.UpgradeStateUndefined:
		defaultInstanceManagerImage, err := uc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
		if err != nil {
			return errors.Wrapf(err, "failed to get setting %v", types.SettingNameDefaultInstanceManagerImage)
		}

		upgradeManager.Status.InstanceManagerImage = defaultInstanceManagerImage
		upgradeManager.Status.State = longhorn.UpgradeStateInitializing
		return nil
	case longhorn.UpgradeStateInitializing:
		if len(upgradeManager.Spec.Nodes) == 0 {
			nodes, err := uc.ds.ListNodes()
			if err != nil {
				return errors.Wrapf(err, "failed to list nodes for upgradeManager resource %v", upgradeManager.Name)
			}
			for _, node := range nodes {
				upgradeManager.Status.UpgradeNodes[node.Name] = &longhorn.UpgradeNodeStatus{
					State: longhorn.UpgradeStatePending,
				}
			}
		} else {
			for _, nodeName := range upgradeManager.Spec.Nodes {
				if _, err := uc.ds.GetNode(nodeName); err != nil {
					return errors.Wrapf(err, "failed to get node %v for upgradeManager resource %v", nodeName, upgradeManager.Name)
				}
				upgradeManager.Status.UpgradeNodes[nodeName] = &longhorn.UpgradeNodeStatus{
					State: longhorn.UpgradeStatePending,
				}
			}
		}
		if len(upgradeManager.Status.UpgradeNodes) == 0 {
			upgradeManager.Status.State = longhorn.UpgradeStateCompleted
		} else {
			upgradeManager.Status.State = longhorn.UpgradeStateUpgrading
		}
	case longhorn.UpgradeStateUpgrading:
		// Check if the active nodeUpgrade is matching the upgradeManager.Status.UpgradingNode
		nodeUpgrades, err := uc.ds.ListNodeUpgrades()
		if err != nil {
			upgradeManager.Status.State = longhorn.UpgradeStateError
			return errors.Wrapf(err, "failed to list nodeUpgrades for upgradeManager resource %v", upgradeManager.Name)
		}

		if upgradeManager.Status.UpgradingNode != "" {
			foundNodeUpgrade := false
			nodeUpgrade := &longhorn.NodeUpgrade{}
			for _, nodeUpgrade = range nodeUpgrades {
				if nodeUpgrade.Spec.NodeID != upgradeManager.Status.UpgradingNode {
					continue
				}
				if nodeUpgrade.Status.State != longhorn.UpgradeStateCompleted &&
					nodeUpgrade.Status.State != longhorn.UpgradeStateError {
					return nil
				}
				foundNodeUpgrade = true
				break
			}
			if foundNodeUpgrade {
				upgradeManager.Status.UpgradeNodes[upgradeManager.Status.UpgradingNode].State = nodeUpgrade.Status.State
				upgradeManager.Status.UpgradeNodes[upgradeManager.Status.UpgradingNode].ErrorMessage = nodeUpgrade.Status.ErrorMessage
				upgradeManager.Status.UpgradingNode = ""
			} else {
				upgradeManager.Status.UpgradeNodes[upgradeManager.Status.UpgradingNode].State = longhorn.UpgradeStateError
				upgradeManager.Status.UpgradeNodes[upgradeManager.Status.UpgradingNode].ErrorMessage = "NodeUpgrade resource not found"
				upgradeManager.Status.UpgradingNode = ""
			}
			return nil
		}

		// TODO: Check if there is any nodeUpgrade in progress but not tracked by upgradeManager.Status.UpgradingNode

		// Pick a node to upgrade
		for nodeName, nodeStatus := range upgradeManager.Status.UpgradeNodes {
			if nodeStatus.State == longhorn.UpgradeStateCompleted ||
				nodeStatus.State == longhorn.UpgradeStateError {
				continue
			}

			// Create a new upgrade resource for the node
			log.Infof("Creating NodeUpgrade resource for node %v", nodeName)
			_, err := uc.ds.GetNode(nodeName)
			if err != nil {
				nodeStatus.State = longhorn.UpgradeStateError
				nodeStatus.ErrorMessage = err.Error()
				continue
			}

			_, err = uc.ds.CreateNodeUpgrade(&longhorn.NodeUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					// TODO:
					// 1. Use "nodeName+random" string as the name
					// 2. Add labels
					Name: nodeName,
				},
				Spec: longhorn.NodeUpgradeSpec{
					NodeID:               nodeName,
					DataEngine:           longhorn.DataEngineTypeV2,
					InstanceManagerImage: upgradeManager.Status.InstanceManagerImage,
				},
			})
			if err != nil {
				nodeStatus.State = longhorn.UpgradeStateError
				nodeStatus.ErrorMessage = err.Error()
				continue
			}
			log.Infof("Created NodeUpgrade resource for node %v", nodeName)
			upgradeManager.Status.UpgradingNode = nodeName
			break
		}

		return nil
	case longhorn.UpgradeStateCompleted:
		return nil
	default:
		return fmt.Errorf("unknown state %v for upgradeManager resource %v", upgradeManager.Status.State, upgradeManager.Name)
	}

	return nil
}
