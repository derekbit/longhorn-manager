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

	apierrors "k8s.io/apimachinery/pkg/api/errors"

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
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})
	var err error

	defer func() {
		if err != nil {
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

	defer func() {
		if err != nil {
			node, errGet := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
			if errGet != nil {
				log.WithError(errGet).Warnf("Failed to get node %v", nodeUpgrade.Status.OwnerID)
				return
			}

			node.Spec.DataEngineUpgradeRequested = false
			if _, errUpdate := m.ds.UpdateNode(node); errUpdate != nil {
				log.WithError(errUpdate).Warnf("Failed to update node %v to set DataEngineUpgradeRequested to false", nodeUpgrade.Status.OwnerID)
				return
			}
		}
	}()

	// Mark the node as upgrade requested
	var volumes map[string]*longhorn.VolumeUpgradeStatus
	if node.Spec.DataEngineUpgradeRequested {
		condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable)
		if condition.Status == longhorn.ConditionStatusTrue {
			log.Infof("DataEngineUpgradeRequested of node %v is set to true, but it is still schedulable", nodeUpgrade.Status.OwnerID)
			// Return here and check again in the next reconciliation
			return
		}

		var im *longhorn.InstanceManager
		im, err = m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
		if err != nil {
			return
		}

		for name, engine := range im.Status.InstanceEngines {
			for _, path := range engine.Status.NvmeSubsystem.Paths {
				if path.State != "live" {
					m.nodeUpgradeStatus.Message = fmt.Sprintf("NVMe subsystem path for engine %v is in state %v", name, path.State)
				}
			}
		}

		replicas, errList := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
		if errList != nil {
			err = errors.Wrapf(errList, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID)
			return
		}

		for _, r := range replicas {
			volume, errGet := m.ds.GetVolumeRO(r.Spec.VolumeName)
			if errGet != nil {
				err = errors.Wrapf(errGet, "failed to get volume %v for replica %v", r.Spec.VolumeName, r.Name)
				return
			}
			if volume.Spec.NodeID != "" {
				if volume.Spec.NumberOfReplicas == 1 {
					err = errors.Errorf("volume %v has only 1 replica", volume.Name)
					return
				}
				if volume.Status.State != longhorn.VolumeStateAttached {
					if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
						err = errors.Errorf("volume %v is not healthy", volume.Name)
						return
					}
				}
			}
		}

		volumes, err = m.listVolumes(nodeUpgrade)
		if err != nil {
			err = errors.Wrapf(err, "failed to list volumes on node %v", nodeUpgrade.Status.OwnerID)
			return
		}

		err = m.snapshotVolumes(volumes)
		if err != nil {
			err = errors.Wrap(err, "failed to snapshot volumes")
			return
		}

		m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingOver
		m.nodeUpgradeStatus.Message = ""
		m.nodeUpgradeStatus.Volumes = volumes
	} else {
		node.Spec.DataEngineUpgradeRequested = true
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v to set DataEngineUpgradeRequested to true", nodeUpgrade.Status.OwnerID)
			return
		}
	}
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
	m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name}).Info("Switching over target instances of volumes")

	targetNode, err := m.findAvailableNodeForTargetInstanceReplacement(nodeUpgrade)
	if err != nil {
		// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to find available node for target instance replacement").Error()
		return
	}

	for name := range nodeUpgrade.Status.Volumes {
		m.updateVolumeForSwitchOver(name, nodeUpgrade.Spec.InstanceManagerImage, targetNode)
	}

	if m.areAllVolumesSwitchedOver(nodeUpgrade) {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateUpgradingInstanceManager
		m.nodeUpgradeStatus.Message = ""
	}
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateSwitchingBack(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name}).Infof("Switching back targets of volumes to the original node %v", nodeUpgrade.Status.OwnerID)
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
		m.clearVolumesTargetNode()

		m.nodeUpgradeStatus.State = longhorn.UpgradeStateRebuildingReplica
		m.nodeUpgradeStatus.Message = ""
	}
}

func (m *NodeDataEngineUpgradeMonitor) clearVolumesTargetNode() {
	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		if volume.Spec.TargetNodeID != "" {
			volume.Spec.TargetNodeID = ""
			if _, err := m.ds.UpdateVolume(volume); err != nil {
				m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
				m.nodeUpgradeStatus.Message = errors.Wrap(err, "failed to empty spec.targetNodeID").Error()
			}
		}
	}
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateRebuildingReplica(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	var err error
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})

	defer func() {
		if err != nil {
			// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
			m.nodeUpgradeStatus.Message = err.Error()
		}
	}()

	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v for nodeDataEngineUpgrade resource %v", nodeUpgrade.Status.OwnerID, nodeUpgrade.Name)
		return
	}

	if node.Spec.DataEngineUpgradeRequested {
		log.Infof("Setting DataEngineUpgradeRequested to false for node %v since the node upgrade is completed", nodeUpgrade.Status.OwnerID)

		node.Spec.DataEngineUpgradeRequested = false
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v for updating upgrade requested to false", nodeUpgrade.Status.OwnerID)
			return
		}
	}

	if m.areAllVolumeHealthy(nodeUpgrade) {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateFinalizing
		m.nodeUpgradeStatus.Message = ""
	}
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

func (m *NodeDataEngineUpgradeMonitor) areAllVolumeHealthy(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	allHealthy := true

	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		if volume.Status.OwnerID != nodeUpgrade.Status.OwnerID {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = fmt.Sprintf("Volume %v is not owned by node %v", name, nodeUpgrade.Status.OwnerID)
			continue
		}

		if volume.Status.State != longhorn.VolumeStateAttached && volume.Status.State != longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = fmt.Sprintf("Volume %v is in unexpected state %v", name, volume.Status.State)
			continue
		}

		if volume.Status.State == longhorn.VolumeStateDetached {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[name].Message = "Volume is detached"
			continue
		}

		if volume.Status.Robustness == longhorn.VolumeRobustnessHealthy {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateRebuildingReplica
			allHealthy = false
		}
	}

	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID).Error()
		return false
	}

	for _, r := range replicas {
		volume, err := m.ds.GetVolumeRO(r.Spec.VolumeName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to get volume %v for replica %v", r.Spec.VolumeName, r.Name).Error()
			continue
		}
		if volume.Spec.NodeID != "" {
			if volume.Spec.NumberOfReplicas == 1 {
				allHealthy = false
				break
			}
			if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
				allHealthy = false
				break
			}
		}
	}

	if allHealthy {
		im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
		if err != nil {
			m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID).Error()
			return false
		}

		for name, engine := range im.Status.InstanceEngines {
			for _, path := range engine.Status.NvmeSubsystem.Paths {
				if path.State != "live" {
					m.nodeUpgradeStatus.Message = fmt.Sprintf("NVMe subsystem path for engine %v is in state %v", name, path.State)
					allHealthy = false
				}
			}
		}
	}

	return allHealthy
}

func (m *NodeDataEngineUpgradeMonitor) handleUpgradeStateUpgradingInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	if !m.upgradeInstanceManager(nodeUpgrade) {
		return
	}

	if !m.areAllEngineInitiatorInstancesRecreated(nodeUpgrade) {
		return
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingBack
	m.nodeUpgradeStatus.Message = ""
}

func (m *NodeDataEngineUpgradeMonitor) areAllEngineInitiatorInstancesRecreated(nodeUpgrade *longhorn.NodeDataEngineUpgrade) bool {
	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		// Don't set nodeUpgradeStatus state to error here, so that the monitor will retry
		m.nodeUpgradeStatus.Message = errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID).Error()
		return false
	}

	allRecreated := true
	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolumeRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		if !types.IsDataEngineV2(volume.Spec.DataEngine) {
			continue
		}

		if volume.Spec.NodeID == "" {
			continue
		}

		engines, err := m.ds.ListVolumeEnginesRO(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = err.Error()
			continue
		}

		if len(engines) == 0 {
			continue
		}

		if len(engines) > 1 {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].Message = fmt.Sprintf("multiple engines found for volume %v", name)
			continue
		}

		var engine *longhorn.Engine
		for _, e := range engines {
			engine = e
		}

		_, ok := im.Status.InstanceEngines[engine.Name]
		if !ok {
			allRecreated = false
		}
	}

	return allRecreated
}

func (m *NodeDataEngineUpgradeMonitor) listVolumes(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (map[string]*longhorn.VolumeUpgradeStatus, error) {
	volumeUpgradeStatus := make(map[string]*longhorn.VolumeUpgradeStatus)

	volumes, err := m.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	for _, volume := range volumes {
		if volume.Status.OwnerID != nodeUpgrade.Status.OwnerID {
			continue
		}

		volumeUpgradeStatus[volume.Name] = &longhorn.VolumeUpgradeStatus{
			State: longhorn.UpgradeStateInitializing,
		}
	}

	return volumeUpgradeStatus, nil
}

func (m *NodeDataEngineUpgradeMonitor) upgradeInstanceManager(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (upgradeCompleted bool) {
	log := m.logger.WithFields(logrus.Fields{"nodeDataEngineUpgrade": nodeUpgrade.Name})
	log.Info("Upgrading instance manager")

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

func (m *NodeDataEngineUpgradeMonitor) updateVolumeForSwitchOver(volumeName, image, targetNode string) {
	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
				m.logger.WithError(err).Warnf("Failed to update volume %v for switch over", volumeName)
			}
		}
	}()

	volume, err := m.ds.GetVolume(volumeName)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v for switch over", volumeName)
		return
	}

	if volume.Status.State == longhorn.VolumeStateDetached {
		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
		m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached"
		return
	}
	if volume.Status.State != longhorn.VolumeStateAttached {
		err = errors.Errorf("volume %v is in neither detached nor attached state", volume.Name)
		return
	}

	if volume.Spec.TargetNodeID != "" {
		if volume.Spec.TargetNodeID != volume.Spec.NodeID {
			// The volume is being switched over
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
			return
		}
	}

	if volume.Spec.Image != image || volume.Spec.TargetNodeID != targetNode {
		volume.Spec.Image = image
		volume.Spec.TargetNodeID = targetNode

		_, err = m.ds.UpdateVolume(volume)
		if err != nil {
			err = errors.Wrapf(err, "failed to update volume %v to image %v and target node %v for switch over", volumeName, image, targetNode)
			return
		}

		m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingOver
	}
}

func (m *NodeDataEngineUpgradeMonitor) updateVolumeForSwitchBack(volumeName, targetNode string) {
	log := m.logger.WithFields(logrus.Fields{"volume": volumeName, "targetNode": targetNode})

	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.Volumes[volumeName].Message = err.Error()
			if datastore.ErrorIsNotFound(err) {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateCompleted
			} else {
				m.nodeUpgradeStatus.Volumes[volumeName].State = longhorn.UpgradeStateError
				m.logger.WithError(err).Warnf("Failed to update volume %v for switch over", volumeName)
			}
		}
	}()

	volume, err := m.ds.GetVolume(volumeName)
	if err != nil {
		err = errors.Wrapf(err, "failed to get volume %v for switch over", volumeName)
		return
	}

	if volume.Spec.NodeID == "" {
		if volume.Status.OwnerID == "" {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateCompleted
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is detached"
		} else {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is being detached"
		}
		return
	}

	if volume.Spec.TargetNodeID != "" {
		if volume.Spec.TargetNodeID == volume.Spec.NodeID {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateSwitchingBack
		} else {
			m.nodeUpgradeStatus.Volumes[volume.Name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Unexpected target node that is different from the attached node"
		}
	} else {
		m.nodeUpgradeStatus.Volumes[volume.Name].Message = "Volume is switched over"
		return
	}

	if volume.Spec.TargetNodeID != targetNode {
		volume.Spec.TargetNodeID = targetNode

		log.Infof("Updating volume %v to target node %v for switch back", volumeName, targetNode)
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

		if m.nodeUpgradeStatus.Volumes[name].State == longhorn.UpgradeStateError {
			continue
		}

		volume, err := m.ds.GetVolume(name)
		if err != nil {
			// Error state will be caught in the next reconciliation
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
			// Error state will be caught in the next reconciliation
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
