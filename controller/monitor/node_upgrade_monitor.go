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
		m.logger.WithError(err).Error("Failed to start nodeUpgrade monitor")
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
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})
	log.Infof("Handling nodeUpgrade %v state %v", nodeUpgrade.Name, m.nodeUpgradeStatus.State)

	switch m.nodeUpgradeStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined(nodeUpgrade)
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(nodeUpgrade)
	case longhorn.UpgradeStateUpgrading:
		m.handleUpgradeStateUpgrading(nodeUpgrade)
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

	m.nodeUpgradeStatus.State = longhorn.UpgradeStateUpgrading
	m.nodeUpgradeStatus.Volumes = volumes
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

	if upgradeCompleted := m.reconcileVolumes(nodeUpgrade); upgradeCompleted {
		var node *longhorn.Node

		node, err = m.ds.GetNode(nodeUpgrade.Status.OwnerID)
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

func (m *NodeUpgradeMonitor) reconcileVolumes(nodeUpgrade *longhorn.NodeUpgrade) (upgradeCompleted bool) {
	log := m.logger.WithFields(logrus.Fields{"nodeUpgrade": nodeUpgrade.Name})
	log.Infof("Reconciling volumes for nodeUpgrade %v", nodeUpgrade.Name)

	m.updateVolumes(nodeUpgrade)

	return m.AreAllVolumesUpgraded(nodeUpgrade)
}

func (m *NodeUpgradeMonitor) updateVolumes(nodeUpgrade *longhorn.NodeUpgrade) {
	nodeForNVMeTarget, err := m.findAvailableNodeForNVMeTarget(nodeUpgrade)
	if err != nil {
		m.nodeUpgradeStatus.State = longhorn.UpgradeStateError
		m.nodeUpgradeStatus.ErrorMessage = fmt.Errorf("failed to find available node for NVMe target: %v", err).Error()
		return
	}

	m.logger.Infof("Got available node %v for NVMe target for nodeUpgrade %v", nodeForNVMeTarget, nodeUpgrade.Name)

	for name, volume := range nodeUpgrade.Status.Volumes {
		if volume.State == longhorn.UpgradeStateInitializing {
			if err := m.updateVolume(name, nodeUpgrade.Spec.InstanceManagerImage, nodeForNVMeTarget); err != nil {
				m.logger.WithError(err).Warnf("Failed to update volume %v", name)
				volume.State = longhorn.UpgradeStateError
				volume.ErrorMessage = err.Error()
			} else {
				volume.State = longhorn.UpgradeStateUpgrading
			}
		}
	}
}

func (m *NodeUpgradeMonitor) findAvailableNodeForNVMeTarget(nodeUpgrade *longhorn.NodeUpgrade) (string, error) {
	upgradeManager, err := m.ds.GetUpgradeManager(nodeUpgrade.Spec.UpgradeManager)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get upgradeManager %v", nodeUpgrade.Name)
	}

	availableNode := ""
	for nodeID, upgradeNodeStatus := range upgradeManager.Status.UpgradeNodes {
		if nodeID == nodeUpgrade.Status.OwnerID {
			continue
		}
		if upgradeNodeStatus.State == longhorn.UpgradeStateError {
			continue
		}

		if availableNode == "" {
			availableNode = nodeID
		}

		// Prefer the node that has completed the upgrade
		if upgradeNodeStatus.State == longhorn.UpgradeStateCompleted {
			availableNode = nodeID
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

	if volume.Spec.Image != image {
		volume.Spec.Image = image
		volume.Spec.TargetNodeID = nodeForNVMeTarget

		_, err := m.ds.UpdateVolume(volume)
		if err != nil {
			return errors.Wrapf(err, "failed to update volume %v for upgrade", volumeName)
		}
	}
	return nil
}

func (m *NodeUpgradeMonitor) AreAllVolumesUpgraded(nodeUpgrade *longhorn.NodeUpgrade) bool {
	allUpgraded := true

	for name := range nodeUpgrade.Status.Volumes {
		volume, err := m.ds.GetVolume(name)
		if err != nil {
			nodeUpgrade.Status.Volumes[name].State = longhorn.UpgradeStateError
			nodeUpgrade.Status.Volumes[name].ErrorMessage = err.Error()
			continue
		}

		if volume.Status.CurrentImage == nodeUpgrade.Spec.InstanceManagerImage {
			nodeUpgrade.Status.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			nodeUpgrade.Status.Volumes[name].State = longhorn.UpgradeStateUpgrading
			allUpgraded = false
		}
	}
	return allUpgraded
}