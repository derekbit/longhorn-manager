package monitor

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	NodeDataEngineUpgradeMonitorSyncPeriod = 3 * time.Second
)

type NodeDataEngineUpgradeMonitor struct {
	sync.RWMutex
	*baseMonitor

	upgradeManagerName string
	nodeUpgradeName    string
	syncCallback       func(key string)

	collectedData     *longhorn.NodeDataEngineUpgradeStatus
	nodeUpgradeStatus *longhorn.NodeDataEngineUpgradeStatus

	proxyConnCounter util.Counter
}

func NewNodeDataEngineUpgradeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeUpgradeName, nodeID string, syncCallback func(key string)) (*NodeDataEngineUpgradeMonitor, error) {
	nodeUpgrade, err := ds.GetNodeDataEngineUpgradeRO(nodeUpgradeName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get longhorn nodeDataEngineUpgrade %v", nodeUpgradeName)
	}

	if !types.IsDataEngineV2(nodeUpgrade.Spec.DataEngine) {
		return nil, errors.Errorf("unsupported data engine %v", nodeUpgrade.Spec.DataEngine)
	}

	ctx, quit := context.WithCancel(context.Background())

	m := &NodeDataEngineUpgradeMonitor{
		baseMonitor:        newBaseMonitor(ctx, quit, logger, ds, NodeDataEngineUpgradeMonitorSyncPeriod),
		upgradeManagerName: nodeUpgrade.Spec.DataEngineUpgradeManager,
		nodeUpgradeName:    nodeUpgradeName,
		syncCallback:       syncCallback,
		collectedData:      &longhorn.NodeDataEngineUpgradeStatus{},
		nodeUpgradeStatus: &longhorn.NodeDataEngineUpgradeStatus{
			OwnerID: nodeID,
			Volumes: map[string]*longhorn.VolumeUpgradeStatus{},
		},
		proxyConnCounter: util.NewAtomicCounter(),
	}

	go m.Start()

	return m, nil
}

func (m *NodeDataEngineUpgradeMonitor) Start() {
	m.logger.Infof("Start monitoring nodeDataEngineUpgrade %v with sync period %v", m.nodeUpgradeName, m.syncPeriod)

	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring upgrade monitor")
		}
		return false, nil
	}); err != nil {
		if errors.Cause(err) == context.Canceled {
			m.logger.Infof("Stopped monitoring nodeDataEngineUpgrade %v due to context cancellation", m.nodeUpgradeName)
		} else {
			m.logger.WithError(err).Error("Failed to start nodeDataEngineUpgrade monitor")
		}
	}

	m.logger.Infof("Stopped monitoring nodeDataEngineUpgrade %v", m.nodeUpgradeName)
}

func (m *NodeDataEngineUpgradeMonitor) Close() {
	m.quit()
}

func (m *NodeDataEngineUpgradeMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *NodeDataEngineUpgradeMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *NodeDataEngineUpgradeMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.collectedData.DeepCopy(), nil
}

func (m *NodeDataEngineUpgradeMonitor) run(value interface{}) error {
	nodeUpgrade, err := m.ds.GetNodeDataEngineUpgrade(m.nodeUpgradeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn nodeDataEngineUpgrade %v", m.nodeUpgradeName)
	}

	existingNodeUpgradeStatus := m.nodeUpgradeStatus.DeepCopy()

	m.handleNodeUpgrade(nodeUpgrade)
	if !reflect.DeepEqual(existingNodeUpgradeStatus, m.nodeUpgradeStatus) {
		func() {
			m.Lock()
			defer m.Unlock()

			m.collectedData.State = m.nodeUpgradeStatus.State
			m.collectedData.Message = m.nodeUpgradeStatus.Message
			m.collectedData.Volumes = map[string]*longhorn.VolumeUpgradeStatus{}
			for k, v := range m.nodeUpgradeStatus.Volumes {
				m.collectedData.Volumes[k] = &longhorn.VolumeUpgradeStatus{
					State:   v.State,
					Message: v.Message,
				}
			}
		}()

		key := nodeUpgrade.Namespace + "/" + m.nodeUpgradeName
		m.syncCallback(key)
	}
	return nil
}

func (m *NodeDataEngineUpgradeMonitor) handleNodeUpgrade(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name}).Infof("Handling nodeDataEngineUpgrade %v state %v",
		nodeUpgrade.Name, m.nodeUpgradeStatus.State)

	switch m.nodeUpgradeStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined()
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(nodeUpgrade)
	case longhorn.UpgradeStateFailingReplicas:
		m.handleUpgradeStateFailingReplicas(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingOver:
		m.handleUpgradeStateSwitchingOver(nodeUpgrade)
	case longhorn.UpgradeStateUpgradingInstanceManager:
		m.handleUpgradeStateUpgradingInstanceManager(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingBack:
		m.handleUpgradeStateSwitchingBack(nodeUpgrade)
	case longhorn.UpgradeStateRebuildingReplica:
		m.handleUpgradeStateRebuildingReplica(nodeUpgrade)
	case longhorn.UpgradeStateFinalizing:
		m.handleUpgradeStateFinalizing(nodeUpgrade)
	case longhorn.UpgradeStateCompleted, longhorn.UpgradeStateError:
		return
	default:
		m.handleUpgradeStateUnknown()
	}
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUndefined() {
	m.nodeUpgradeStatus.State = longhorn.UpgradeStateInitializing
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateInitializing(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	// Check if the node is existing and ready
	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v for nodeDataEngineUpgrade %v", nodeUpgrade.Status.OwnerID, nodeUpgrade.Name)
		return
	}
	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		err = errors.Errorf("node %v is not ready", node.Name)
		return
	}

	if !node.Spec.DataEngineUpgradeRequested {
		node.Spec.DataEngineUpgradeRequested = true
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v to set DataEngineUpgradeRequested to true", nodeUpgrade.Status.OwnerID)
		}
		return
	}

	var volumes map[string]*longhorn.VolumeUpgradeStatus

	condition = types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable)
	if condition.Status == longhorn.ConditionStatusTrue {
		// Return here and check again in the next reconciliation
		err = errors.Errorf("spec.dataEngineUpgradeRequested of node %v is set to true, but the node is still schedulable", nodeUpgrade.Status.OwnerID)
		return
	}

	err = m.areIndirectVolumesReadyForUpgrade(nodeUpgrade)
	if err != nil {
		err = errors.Wrap(err, "failed to check if associated volumes are ready for upgrade")
		return
	}

	volumes, err = m.listDirectVolumes(nodeUpgrade)
	if err != nil {
		err = errors.Wrapf(err, "failed to list volumes on node %v", nodeUpgrade.Status.OwnerID)
		return
	}
	m.nodeUpgradeStatus.Volumes = volumes

	// TODO: make this optional
	err = m.snapshotVolumes(volumes)
	if err != nil {
		err = errors.Wrap(err, "failed to snapshot volumes")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateFailingReplicas
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateFailingReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	m.failReplicas(nodeUpgrade)

	allStoppedOrFailed, err := m.areAllReplicasStoppedOrFailed(nodeUpgrade)
	if !allStoppedOrFailed {
		err = fmt.Errorf("not all replicas are stopped or failed")
		return
	}

	m.deleteAllStoppedOrFailedReplicas(nodeUpgrade)
	allDeleted, err := m.areAllStoppedOrFailedReplicasDeleted(nodeUpgrade)
	if !allDeleted {
		if err != nil {
			err = errors.Wrapf(err, "not all stopped or failed replicas are deleted")
		} else {
			err = fmt.Errorf("not all stopped or failed replicas are deleted")
		}
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingOver
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) areIndirectVolumesReadyForUpgrade(nodeUpgrade *longhorn.NodeDataEngineUpgrade) error {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return errors.Wrapf(err, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID)
	}

	for _, r := range replicas {
		if r.Spec.DataEngine != nodeUpgrade.Spec.DataEngine {
			continue
		}

		volume, err := m.ds.GetVolumeRO(r.Spec.VolumeName)
		if err != nil {
			return errors.Wrapf(err, "failed to get volume %v for replica %v", r.Spec.VolumeName, r.Name)
		}

		// No need to care about the detached volumes
		if volume.Status.State == longhorn.VolumeStateDetached {
			continue
		}

		if volume.Spec.NumberOfReplicas == 1 {
			return fmt.Errorf("volume %v has only 1 replica, which is not supported for live upgrade", volume.Name)
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			return fmt.Errorf("volume %v is not healthy, which is not supported for live upgrade", volume.Name)
		}
	}

	return nil
}

func (m *NodeDataEngineUpgradeMonitor) snapshotVolumes(volumes map[string]*longhorn.VolumeUpgradeStatus) error {
	for name := range volumes {
		engine, err := m.ds.GetVolumeCurrentEngine(name)
		if err != nil {
			return fmt.Errorf("failed to get volume %v for snapshot creation", name)
		}

		freezeFilesystem, err := m.ds.GetFreezeFilesystemForSnapshotSetting(engine)
		if err != nil {
			return err
		}

		if engine == nil {
			return fmt.Errorf("failed to get engine for volume %v", name)
		}

		if engine.Status.CurrentState == longhorn.InstanceStateRunning {
			engineCliClient, err := engineapi.GetEngineBinaryClient(m.ds, engine.Spec.VolumeName, m.nodeUpgradeStatus.OwnerID)
			if err != nil {
				return err
			}

			engineClientProxy, err := engineapi.GetCompatibleClient(engine, engineCliClient, m.ds, m.logger, m.proxyConnCounter)
			if err != nil {
				return err
			}
			defer engineClientProxy.Close()

			snapLabels := map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotForDataEngineLiveUpgrade): m.nodeUpgradeName}
			_, err = engineClientProxy.SnapshotCreate(engine, m.upgradeManagerName+"-"+util.RandomID(), snapLabels, freezeFilesystem)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateSwitchingOver(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	targetNode, err := m.findAvailableNodeForTargetInstanceReplacement(nodeUpgrade)
	if err != nil {
		err = errors.Wrapf(err, "failed to find available node for target instance replacement")
		return
	}

	for name := range nodeUpgrade.Status.Volumes {
		m.updateVolumeForSwitchOver(name, nodeUpgrade.Spec.InstanceManagerImage, targetNode)
	}

	if !m.areAllVolumesSwitchedOver(nodeUpgrade) {
		err = fmt.Errorf("not all volumes are switched over")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateUpgradingInstanceManager
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateSwitchingBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	for name := range nodeUpgrade.Status.Volumes {
		m.updateVolumeForSwitchBack(name, nodeUpgrade.Status.OwnerID)
	}

	if m.areAllVolumesSwitchedBack(nodeUpgrade) {
		if !m.clearVolumesTargetNode() {
			return
		}

		m.nodeUpgradeStatus.State = longhorn.UpgradeStateRebuildingReplica
		m.nodeUpgradeStatus.Message = ""
	}
}

func (m *NodeDataEngineUpgradeMonitor) clearVolumesTargetNode() bool {
	allCleared := true
	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			if !datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
				allCleared = false
			} else {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			}
			continue
		}

		if volume.Spec.TargetNodeID != "" {
			volume.Spec.TargetNodeID = ""

			_, err := m.ds.UpdateVolume(volume)
			if err != nil {
				m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Message = errors.Wrap(err, "failed to empty spec.targetNodeID").Error()
				allCleared = false
			}
		}
	}

	return allCleared
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateRebuildingReplica(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return
	}

	if node.Spec.DataEngineUpgradeRequested {
		node.Spec.DataEngineUpgradeRequested = false
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v for updating upgrade requested to false", nodeUpgrade.Status.OwnerID)
			return
		}
	}

	allHealthy, err := m.areAllVolumeHealthy(nodeUpgrade)
	if !allHealthy {
		if err != nil {
			err = errors.Wrap(err, "not all volumes are healthy")
		} else {
			err = fmt.Errorf("not all volumes are healthy")
		}
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateFinalizing
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateFinalizing(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	volumes, err := m.ds.ListVolumes()
	if err != nil {
		return
	}

	allUpdated := true
	for _, volume := range volumes {
		_, ok := m.nodeUpgradeStatus.Volumes[volume.Name]
		if !ok {
			m.nodeUpgradeStatus.Volumes[volume.Name] = &longhorn.VolumeUpgradeStatus{}
		}

		if volume.Spec.NodeID == "" && volume.Status.OwnerID == nodeUpgrade.Status.OwnerID {
			if volume.Spec.Image == nodeUpgrade.Spec.InstanceManagerImage {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
				continue
			}
			volume.Spec.Image = nodeUpgrade.Spec.InstanceManagerImage

			if _, err := m.ds.UpdateVolume(volume); err != nil {
				m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Volumes[volume.Name].Message = errors.Wrapf(err, "failed to update volume %v to image %v", volume.Name, nodeUpgrade.Spec.InstanceManagerImage).Error()
				allUpdated = false
			}
		}
	}
	if allUpdated {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateCompleted
		m.nodeUpgradeStatus.Message = ""
	}
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUnknown() {
	m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
	m.nodeUpgradeStatus.Message = fmt.Sprintf("Unknown state %v", m.nodeUpgradeStatus.State)
}

func (m *NodeDataEngineUpgradeMonitor) areAllVolumeHealthy(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (bool, error) {
	volumes, err := m.ds.ListVolumesRO()
	if err != nil {
		return false, errors.Wrapf(err, "failed to list volumes")
	}

	for _, volume := range volumes {
		if volume.Status.State == longhorn.VolumeStateDetached {
			continue
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			return false, fmt.Errorf("need to make sure all volumes are healthy before proceeding to the next state")
		}
	}

	return true, nil
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUpgradingInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	// m.removeRunningReplicasFromEngines(nodeUpgrade)

	// allStoppedOrFailed, err := m.areAllReplicasStoppedOrFailed(nodeUpgrade)
	// if !allStoppedOrFailed {
	// 	m.nodeUpgradeStatus.Message = err.Error()
	// 	return
	// }

	if !m.upgradeInstanceManager(nodeUpgrade) {
		return
	}

	if !m.areAllInitiatorInstancesRecreated(nodeUpgrade) {
		err = fmt.Errorf("not all initiator instances are recreated")
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingBack
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) areAllInitiatorInstancesRecreated(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID).Error()
		return false
	}

	allRecreated := true
	for name := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateCompleted {
			continue
		}

		// TODO: should we introduce max retry count here?
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateError {
			allRecreated = false
			continue
		}

		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			if !datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
				allRecreated = false
			}
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			continue
		}

		if volume.Status.State == longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[name].Message = "Volume is detached"
			continue
		}

		engine, err := m.ds.GetVolumeCurrentEngine(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		_, ok := im.Status.InstanceEngines[engine.Name]
		if !ok {
			allRecreated = false
		}
	}

	return allRecreated
}

func (m *NodeDataEngineUpgradeMonitor) listDirectVolumes(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (map[string]*longhorn.VolumeUpgradeStatus, error) {
	volumes, err := m.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	volumeUpgradeStatus := make(map[string]*longhorn.VolumeUpgradeStatus)
	for _, volume := range volumes {
		if volume.Status.OwnerID != nodeUpgrade.Status.OwnerID {
			continue
		}

		if volume.Spec.DataEngine != nodeUpgrade.Spec.DataEngine {
			continue
		}

		volumeUpgradeStatus[volume.Name] = &longhorn.VolumeUpgradeStatus{
			State: longhorn.UpgradeStateInitializing,
		}
	}

	return volumeUpgradeStatus, nil
}

func (m *NodeDataEngineUpgradeMonitor) areAllStoppedOrFailedReplicasDeleted(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (bool, error) {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return false, err
	}

	return len(replicas) == 0, nil
}

func (m *NodeDataEngineUpgradeMonitor) areAllReplicasStoppedOrFailed(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (bool, error) {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return false, err
	}

	for _, r := range replicas {
		if r.Status.CurrentState != longhorn.InstanceStateStopped && r.Spec.DesireState != longhorn.InstanceStateError {
			return false, fmt.Errorf("not all replicas are stopped or failed")
		}
	}

	return true, nil
}

func (m *NodeDataEngineUpgradeMonitor) deleteAllStoppedOrFailedReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) error {
	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Status.CurrentState == longhorn.InstanceStateStopped || r.Spec.DesireState == longhorn.InstanceStateError {
			if err := m.ds.DeleteReplica(r.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

func (m *NodeDataEngineUpgradeMonitor) upgradeInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (upgradeCompleted bool) {

	m.deleteNonDefaultInstanceManager(nodeUpgrade)

	return m.isDefaultInstanceManagerRunning(nodeUpgrade)
}

func (m *NodeDataEngineUpgradeMonitor) isDefaultInstanceManagerRunning(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID).Error()
		return false
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		m.nodeUpgradeStatus.Message = fmt.Sprintf("instance manager %v is not running and in state %v", im.Name, im.Status.CurrentState)
		return false
	}

	return true
}

func (m *NodeDataEngineUpgradeMonitor) failReplicas(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})

	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID).Error()
		return
	}

	for _, r := range replicas {
		if r.Spec.DesireState == longhorn.InstanceStateStopped {
			continue
		}

		log.Infof("Failing replica %v on node %v", r.Name, nodeUpgrade.Status.OwnerID)

		setReplicaFailedAt(r, util.Now())
		r.Spec.DesireState = longhorn.InstanceStateStopped

		_, err = m.ds.UpdateReplica(r)
		if err != nil {
			log.WithError(err).Warnf("Failed to fail replica %v on node %v", r.Name, nodeUpgrade.Status.OwnerID)
			continue
		}
	}
}

// r.Spec.FailedAt and r.Spec.LastFailedAt should both be set when a replica failure occurs.
// r.Spec.FailedAt may be cleared (before rebuilding), but r.Spec.LastFailedAt must not be.
func setReplicaFailedAt(r *longhorn.Replica, timestamp string) {
	r.Spec.FailedAt = timestamp
	if timestamp != "" {
		r.Spec.LastFailedAt = timestamp
	}
}

func (m *NodeDataEngineUpgradeMonitor) findAvailableNodeForTargetInstanceReplacement(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (string, error) {
	upgradeManager, err := m.ds.GetDataEngineUpgradeManager(nodeUpgrade.Spec.DataEngineUpgradeManager)
	if err != nil {
		return "", err
	}

	ims, err := m.ds.ListInstanceManagersBySelectorRO("", "", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return "", err
	}

	availableNode := ""
	for _, im := range ims {
		if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
			continue
		}

		if im.Spec.NodeID == nodeUpgrade.Status.OwnerID {
			continue
		}

		if availableNode == "" {
			availableNode = im.Spec.NodeID
		}

		upgradeNodeStatus, ok := upgradeManager.Status.UpgradeNodes[im.Spec.NodeID]
		if !ok {
			continue
		}

		// Prefer the node that has completed the upgrade
		if upgradeNodeStatus.State == longhorn.UpgradeStateCompleted {
			availableNode = im.Spec.NodeID
			break
		}
	}

	if availableNode == "" {
		return "", fmt.Errorf("failed to find available node for target")
	}

	return availableNode, nil
}

func (m *NodeDataEngineUpgradeMonitor) updateVolumeForSwitchOver(volumeName, image, tempTargetNode string) {
	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				// If the volume is not found, we don't need to switch over it and therefore consider it as completed.
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
		}
	}()

	volume, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return
	}

	// If a volume is detached, no need to switch over,
	// because it will use the default instance manager after being attached.
	if volume.Status.State == longhorn.VolumeStateDetached {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
		m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached, so no need to switch over"
		return
	}

	if volume.Status.State != longhorn.VolumeStateAttached {
		err = errors.Errorf("volume %v is in neither detached nor attached state", volume.Name)
		return
	}

	// If a volume's targetNodeID is not empty, it means the volume is being switched over
	if volume.Spec.TargetNodeID != "" {
		if volume.Spec.TargetNodeID != volume.Spec.NodeID {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
			return
		}
	}

	if volume.Spec.Image != image || volume.Spec.TargetNodeID != tempTargetNode {
		volume.Spec.Image = image
		volume.Spec.TargetNodeID = tempTargetNode

		_, err = m.ds.UpdateVolume(volume)
		if err != nil {
			err = errors.Wrapf(err, "failed to update volume %v to image %v and target node %v for switch over", volumeName, image, tempTargetNode)
			return
		}

		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
	}
}

func (m *NodeDataEngineUpgradeMonitor) updateVolumeForSwitchBack(volumeName, targetNode string) {
	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
			}
		}
	}()

	volume, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return
	}

	if volume.Status.State == longhorn.VolumeStateDetached {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
		m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached"
		return
	}

	if volume.Status.State != longhorn.VolumeStateAttached {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
		err = errors.Errorf("volume %v is in neither detached nor attached state", volume.Name)
		return
	}

	if volume.Spec.TargetNodeID == "" {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
		return
	}

	if volume.Spec.TargetNodeID == volume.Spec.NodeID {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingBack
		return
	}

	if volume.Spec.TargetNodeID != targetNode {
		volume.Spec.TargetNodeID = targetNode
		_, err := m.ds.UpdateVolume(volume)
		if err != nil {
			return
		}
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingBack
	}
}

func (m *NodeDataEngineUpgradeMonitor) areAllVolumesSwitchedOver(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allSwitched := true
	for name := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateCompleted {
			continue
		}

		// TODO: should we introduce max retry count here?
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateError {
			allSwitched = false
			continue
		}

		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			if !datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
				allSwitched = false
			}
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			continue
		}

		if volume.Spec.TargetNodeID != volume.Status.CurrentTargetNodeID ||
			volume.Spec.Image != volume.Status.CurrentImage ||
			volume.Status.CurrentImage != nodeUpgrade.Spec.InstanceManagerImage {
			allSwitched = false
			continue
		}

		m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateSwitchedOver
		m.nodeUpgradeStatus.Volumes[name].Message = ""
	}
	return allSwitched
}

func (m *NodeDataEngineUpgradeMonitor) areAllVolumesSwitchedBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allSwitched := true

	for name := range m.nodeUpgradeStatus.Volumes {
		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateCompleted {
			continue
		}

		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateError {
			continue
		}

		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			if !datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
				allSwitched = false
			}
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			continue
		}

		if volume.Spec.TargetNodeID != volume.Spec.NodeID ||
			volume.Spec.TargetNodeID != volume.Status.CurrentTargetNodeID ||
			volume.Spec.Image != volume.Status.CurrentImage ||
			volume.Status.CurrentImage != nodeUpgrade.Spec.InstanceManagerImage {
			allSwitched = false
			continue
		}

		m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateSwitchedBack
		m.nodeUpgradeStatus.Volumes[name].Message = ""
	}
	return allSwitched
}

func (m *NodeDataEngineUpgradeMonitor) AreAllVolumesUpgraded(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allUpgraded := true

	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		if volume.Status.CurrentImage == nodeUpgrade.Spec.InstanceManagerImage {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateUpgradingInstanceManager
			allUpgraded = false
		}
	}
	return allUpgraded
}

func (m *NodeDataEngineUpgradeMonitor) deleteNonDefaultInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})

	var err error

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	ims, err := m.ds.ListInstanceManagersByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		err = errors.Wrapf(err, "failed to list instance managers for node %v for deleting non-default instance managers", nodeUpgrade.Status.OwnerID)
		return
	}

	for _, im := range ims {
		if im.Spec.Image == nodeUpgrade.Spec.InstanceManagerImage {
			continue
		}

		log.Infof("Deleting non-default instance manager %v", im.Name)
		err = m.ds.DeleteInstanceManager(im.Name)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			err = errors.Wrapf(err, "failed to delete non-default instance manager %v", im.Name)
		}
	}
}
