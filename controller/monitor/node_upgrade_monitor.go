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
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	NodeUpgradeMonitorSyncPeriod = 5 * time.Second
)

type NodeUpgradeMonitor struct {
	sync.RWMutex
	*baseMonitor

	nodeUpgradeName string
	syncCallback    func(key string)

	collectedData     *longhorn.NodeUpgradeStatus
	nodeUpgradeStatus *longhorn.NodeUpgradeStatus
}

func NewNodeUpgradeMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeUpgradeName, nodeID string, syncCallback func(key string)) (*NodeUpgradeMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &NodeUpgradeMonitor{
		baseMonitor:     newBaseMonitor(ctx, quit, logger, ds, UpgradeMonitorMonitorSyncPeriod),
		nodeUpgradeName: nodeUpgradeName,
		syncCallback:    syncCallback,
		collectedData:   &longhorn.NodeUpgradeStatus{},
		nodeUpgradeStatus: &longhorn.NodeUpgradeStatus{
			OwnerID: nodeID,
		},
	}

	go m.Start()

	return m, nil
}

func (m *NodeUpgradeMonitor) Start() {
	m.logger.Infof("Start monitoring nodeUpgrade %v with sync period %v", m.nodeUpgradeName, m.syncPeriod)

	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring upgrade monitor")
		}
		return false, nil
	}); err != nil {
		if errors.Cause(err) == context.Canceled {
			m.logger.Infof("Stopped monitoring nodeUpgrade %v due to context cancellation", m.nodeUpgradeName)
		} else {
			m.logger.WithError(err).Error("Failed to start nodeUpgrade monitor")
		}
	}

	m.logger.Infof("Stopped monitoring nodeUpgrade %v", m.nodeUpgradeName)
}

func (m *NodeUpgradeMonitor) Close() {
	m.quit()
}

func (m *NodeUpgradeMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *NodeUpgradeMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *NodeUpgradeMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.collectedData.DeepCopy(), nil
}

func (m *NodeUpgradeMonitor) run(value interface{}) error {
	nodeUpgrade, err := m.ds.GetNodeUpgrade(m.nodeUpgradeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn nodeUpgrade %v", m.nodeUpgradeName)
	}

	existingNodeUpgradeStatus := m.nodeUpgradeStatus.DeepCopy()

	m.handleNodeUpgrade(nodeUpgrade)
	if !reflect.DeepEqual(existingNodeUpgradeStatus, m.nodeUpgradeStatus) {
		func() {
			m.Lock()
			defer m.Unlock()

			m.collectedData.State = m.nodeUpgradeStatus.State
			m.collectedData.ErrorMessage = m.nodeUpgradeStatus.ErrorMessage
			m.collectedData.Volumes = map[string]*longhorn.VolumeUpgradeStatus{}
			for k, v := range m.nodeUpgradeStatus.Volumes {
				m.collectedData.Volumes[k] = &longhorn.VolumeUpgradeStatus{
					State:        v.State,
					ErrorMessage: v.ErrorMessage,
				}
			}
		}()

		key := nodeUpgrade.Namespace + "/" + m.nodeUpgradeName
		m.syncCallback(key)
	}
	return nil
}

func (m *NodeUpgradeMonitor) handleNodeUpgrade(nodeUpgrade *longhorn.NodeUpgrade) {
	m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name}).Infof("Handling nodeUpgrade %v state %v",
		nodeUpgrade.Name, m.nodeUpgradeStatus.State)

	switch m.nodeUpgradeStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined(nodeUpgrade)
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingOver:
		m.handleUpgradeStateSwitchingOver(nodeUpgrade)
	case longhorn.UpgradeStateUpgrading:
		m.handleUpgradeStateUpgrading(nodeUpgrade)
	case longhorn.UpgradeStateSwitchingBack:
		m.handleUpgradeStateSwitchingBack(nodeUpgrade)
	case longhorn.UpgradeStateFinalizing:
		m.handleUpgradeStateFinalizing(nodeUpgrade)
	case longhorn.UpgradeStateCompleted, longhorn.UpgradeStateError:
		return
	default:
		m.handleUpgradeStateUnknown()
	}
}

func (m *NodeUpgradeMonitor) handleUpgradeStateUndefined(nodeUpgrade *longhorn.NodeUpgrade) {
	if !types.IsDataEngineV2(nodeUpgrade.Spec.DataEngine) {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
		m.nodeUpgradeStatus.ErrorMessage = fmt.Sprintf("Unsupported data engine %v", nodeUpgrade.Spec.DataEngine)
		return
	}

	if m.nodeUpgradeStatus.Volumes == nil {
		m.nodeUpgradeStatus.Volumes = map[string]*longhorn.VolumeUpgradeStatus{}
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateInitializing
}

func (m *NodeUpgradeMonitor) handleUpgradeStateInitializing(nodeUpgrade *longhorn.NodeUpgrade) {
	var err error
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = err.Error()
		}
	}()

	volumes, err := m.collectVolumes(nodeUpgrade)
	if err != nil {
		return
	}

	// Check if the node is existing and ready
	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v for nodeUpgrade resource %v", nodeUpgrade.Status.OwnerID, nodeUpgrade.Name)
		return
	}
	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		err = errors.Errorf("node %v is not ready", node.Name)
		return
	}

	// Mark the node as upgrade requested
	if node.Spec.UpgradeRequested {
		condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable)
		if condition.Status == longhorn.ConditionStatusTrue {
			log.Infof("UpgradeRequested of node %v is set to true, but it is still schedulable", nodeUpgrade.Status.OwnerID)
			return
		}
	} else {
		node.Spec.UpgradeRequested = true
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v for setting upgrade requested to true", nodeUpgrade.Status.OwnerID)
			return
		}
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingOver
	m.nodeUpgradeStatus.Volumes = volumes
}

func (m *NodeUpgradeMonitor) handleUpgradeStateSwitchingOver(nodeUpgrade *longhorn.NodeUpgrade) {
	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = err.Error()
		}
	}()

	if completed := m.switchOverVolumes(nodeUpgrade); completed {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateUpgrading
	}
}

func (m *NodeUpgradeMonitor) handleUpgradeStateUpgrading(nodeUpgrade *longhorn.NodeUpgrade) {
	var err error
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = err.Error()
		}
	}()

	if upgradeCompleted := m.upgradeInstanceManager(nodeUpgrade); upgradeCompleted {
		log.Infof("Upgrade of instance manager for nodeUpgrade %v is completed", nodeUpgrade.Name)
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateSwitchingBack
	}
}

func (m *NodeUpgradeMonitor) handleUpgradeStateSwitchingBack(nodeUpgrade *longhorn.NodeUpgrade) {
	var err error

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = err.Error()
		}
	}()

	if completed := m.switchBackVolumes(nodeUpgrade); completed {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateFinalizing
	}
}

func (m *NodeUpgradeMonitor) handleUpgradeStateFinalizing(nodeUpgrade *longhorn.NodeUpgrade) {
	var err error
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})

	defer func() {
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = err.Error()
		}
	}()

	node, err := m.ds.GetNode(nodeUpgrade.Status.OwnerID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v for nodeUpgrade resource %v", nodeUpgrade.Status.OwnerID, nodeUpgrade.Name)
		return
	}

	if node.Spec.UpgradeRequested {
		log.Infof("Setting upgradeRequested to false for node %v since the node upgrade is completed", nodeUpgrade.Status.OwnerID)

		node.Spec.UpgradeRequested = false
		_, err = m.ds.UpdateNode(node)
		if err != nil {
			err = errors.Wrapf(err, "failed to update node %v for updating upgrade requested to false", nodeUpgrade.Status.OwnerID)
			return
		}
	}

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateCompleted
}

func (m *NodeUpgradeMonitor) handleUpgradeStateUnknown() {
	m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
	m.nodeUpgradeStatus.ErrorMessage = fmt.Sprintf("Unknown state %v", m.nodeUpgradeStatus.State)
}

func (m *NodeUpgradeMonitor) collectVolumes(nodeUpgrade *longhorn.NodeUpgrade) (map[string]*longhorn.VolumeUpgradeStatus, error) {
	engines, err := m.ds.ListEnginesByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list engines on node %v", nodeUpgrade.Status.OwnerID)
	}

	volumes := make(map[string]*longhorn.VolumeUpgradeStatus)
	for _, engine := range engines {
		if engine.Spec.DataEngine != nodeUpgrade.Spec.DataEngine {
			continue
		}

		volume, err := m.ds.GetVolumeRO(engine.Spec.VolumeName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get volume %v for engine %v", engine.Spec.VolumeName, engine.Name)
		}

		volumes[volume.Name] = &longhorn.VolumeUpgradeStatus{
			State: longhorn.UpgradeStateInitializing,
		}
	}

	return volumes, nil
}

func (m *NodeUpgradeMonitor) switchOverVolumes(nodeUpgrade *longhorn.NodeUpgrade) (completed bool) {
	m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name}).Info("Switching over volumes")

	nodeForNVMeTarget, err := m.findAvailableNodeForNVMeTarget(nodeUpgrade)
	if err != nil {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
		m.nodeUpgradeStatus.ErrorMessage = fmt.Errorf("failed to find available node for NVMe target: %v", err).Error()

		return false
	}

	m.updateVolumes(nodeUpgrade, nodeForNVMeTarget)

	return m.AreAllVolumesSwitchedOver(nodeUpgrade)
}

func (m *NodeUpgradeMonitor) switchBackVolumes(nodeUpgrade *longhorn.NodeUpgrade) (completed bool) {
	m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name}).Infof("Switching back targets of volumes to the original node %v", nodeUpgrade.Status.OwnerID)

	m.updateVolumes(nodeUpgrade, nodeUpgrade.Status.OwnerID)

	return m.AreAllVolumesSwitchedBack(nodeUpgrade)
}

func (m *NodeUpgradeMonitor) upgradeInstanceManager(nodeUpgrade *longhorn.NodeUpgrade) (upgradeCompleted bool) {
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})
	log.Info("Upgrading instance manager")

	m.failReplicas(nodeUpgrade)

	err := m.deleteNonDefaultInstanceManager(nodeUpgrade)
	if err != nil {
		log.WithError(err).Warnf("Failed to delete non-default instance manager")
		return false
	}

	im, err := m.ds.GetDefaultInstanceManagerByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.DataEngineTypeV2)
	if err != nil {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
		m.nodeUpgradeStatus.ErrorMessage = errors.Wrapf(err, "failed to get default instance manager for node %v", nodeUpgrade.Status.OwnerID).Error()
		return
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return
	}

	upgradeCompleted = true
	for volumeName := range m.nodeUpgradeStatus.Volumes {
		engines, err := m.ds.ListVolumeEngines(volumeName)
		if err != nil {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = errors.Wrapf(err, "failed to list engines for volume %v", volumeName).Error()
			continue
		}

		if len(engines) == 0 {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = fmt.Sprintf("no engine found for volume %v", volumeName)
			continue
		}

		if len(engines) > 1 {
			m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.ErrorMessage = fmt.Sprintf("multiple engines found for volume %v", volumeName)
			continue
		}

		var engine *longhorn.Engine
		for e := range engines {
			engine = engines[e]
		}

		_, ok := im.Status.InstanceEngines[engine.Name]
		if !ok {
			upgradeCompleted = false
			continue
		}
	}

	return upgradeCompleted
}

func (m *NodeUpgradeMonitor) updateVolumes(nodeUpgrade *longhorn.NodeUpgrade, nodeName string) {
	m.logger.WithField("nodeUpgrade", nodeUpgrade.Name).Infof("Updating volumes to node %v", nodeName)

	for name := range nodeUpgrade.Status.Volumes {
		if err := m.updateVolume(name, nodeUpgrade.Spec.InstanceManagerImage, nodeName); err != nil {
			m.logger.WithError(err).Warnf("Failed to update volume %v", name)
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].ErrorMessage = err.Error()
		}
	}
}

func (m *NodeUpgradeMonitor) failReplicas(nodeUpgrade *longhorn.NodeUpgrade) {
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})

	replicas, err := m.ds.ListReplicasByNodeRO(nodeUpgrade.Status.OwnerID)
	if err != nil {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
		m.nodeUpgradeStatus.ErrorMessage = errors.Wrapf(err, "failed to list replicas on node %v", nodeUpgrade.Status.OwnerID).Error()
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

func (m *NodeUpgradeMonitor) findAvailableNodeForNVMeTarget(nodeUpgrade *longhorn.NodeUpgrade) (string, error) {
	upgradeManager, err := m.ds.GetUpgradeManager(nodeUpgrade.Spec.UpgradeManager)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get upgradeManager %v", nodeUpgrade.Name)
	}

	ims, err := m.ds.ListInstanceManagersBySelectorRO("", "", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return "", errors.Wrapf(err, "failed to list instance managers for v2 data engine")
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
		return "", fmt.Errorf("failed to find available node for NVMe target")
	}

	return availableNode, nil
}

func (m *NodeUpgradeMonitor) updateVolume(volumeName, image, nodeForNVMeTarget string) error {
	volume, err := m.ds.GetVolume(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v for upgrade", volumeName)
	}

	if volume.Spec.Image != image || volume.Spec.TargetNodeID != nodeForNVMeTarget {
		volume.Spec.Image = image
		volume.Spec.TargetNodeID = nodeForNVMeTarget

		_, err := m.ds.UpdateVolume(volume)
		if err != nil {
			return errors.Wrapf(err, "failed to update volume %v for upgrade", volumeName)
		}
	}
	return nil
}

func (m *NodeUpgradeMonitor) AreAllVolumesSwitchedOver(nodeUpgrade *longhorn.NodeUpgrade) bool {
	allSwitchedOver := true

	for name := range m.nodeUpgradeStatus.Volumes {
		logrus.Infof("Debug -------> AreAllVolumesSwitchedOver volume=%v", name)
		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].ErrorMessage = err.Error()
			continue
		}

		if volume.Status.CurrentImage == nodeUpgrade.Spec.InstanceManagerImage &&
			volume.Spec.Image == volume.Status.CurrentImage &&
			volume.Spec.TargetNodeID == volume.Status.CurrentTargetNodeID {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateUpgrading
		} else {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateSwitchingOver
			allSwitchedOver = false
		}
	}
	return allSwitchedOver
}

func (m *NodeUpgradeMonitor) AreAllVolumesSwitchedBack(nodeUpgrade *longhorn.NodeUpgrade) bool {
	allSwitchedOver := true

	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].ErrorMessage = err.Error()
			continue
		}

		if volume.Status.CurrentImage == nodeUpgrade.Spec.InstanceManagerImage &&
			volume.Spec.Image == volume.Status.CurrentImage &&
			volume.Spec.TargetNodeID == volume.Spec.NodeID &&
			volume.Spec.TargetNodeID == volume.Status.CurrentTargetNodeID {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateSwitchingBack
			allSwitchedOver = false
		}
	}
	return allSwitchedOver
}

func (m *NodeUpgradeMonitor) AreAllVolumesUpgraded(nodeUpgrade *longhorn.NodeUpgrade) bool {
	allUpgraded := true

	for name := range m.nodeUpgradeStatus.Volumes {
		volume, err := m.ds.GetVolume(name)
		if err != nil {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateError
			m.nodeUpgradeStatus.Volumes[name].ErrorMessage = err.Error()
			continue
		}

		if volume.Status.CurrentImage == nodeUpgrade.Spec.InstanceManagerImage {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			m.nodeUpgradeStatus.Volumes[name].State = longhorn.UpgradeStateUpgrading
			allUpgraded = false
		}
	}
	return allUpgraded
}

func (m *NodeUpgradeMonitor) deleteNonDefaultInstanceManager(nodeUpgrade *longhorn.NodeUpgrade) error {
	ims, err := m.ds.ListInstanceManagersByNodeRO(nodeUpgrade.Status.OwnerID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return errors.Wrapf(err, "failed to list instance managers for node %v", nodeUpgrade.Status.OwnerID)
	}

	for _, im := range ims {
		if im.Spec.Image == nodeUpgrade.Spec.InstanceManagerImage {
			continue
		}
		if len(im.Status.InstanceReplicas) != 0 {
			return errors.Errorf("instance manager %v has instance replicas", im.Name)
		}

		if err := m.ds.DeleteInstanceManager(im.Name); err != nil {
			return errors.Wrapf(err, "failed to delete instance manager %v", im.Name)
		}
	}

	return nil
}
