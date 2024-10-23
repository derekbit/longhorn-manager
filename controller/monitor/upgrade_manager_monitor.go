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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	UpgradeMonitorMonitorSyncPeriod = 5 * time.Second
)

type UpgradeManagerMonitor struct {
	sync.RWMutex
	*baseMonitor

	upgradeManagerName string
	syncCallback       func(key string)

	collectedData        *longhorn.UpgradeManagerStatus
	upgradeManagerStatus *longhorn.UpgradeManagerStatus
}

func NewUpgradeManagerMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, upgradeManagerName, nodeID string, syncCallback func(key string)) (*UpgradeManagerMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &UpgradeManagerMonitor{
		baseMonitor:        newBaseMonitor(ctx, quit, logger, ds, UpgradeMonitorMonitorSyncPeriod),
		upgradeManagerName: upgradeManagerName,
		syncCallback:       syncCallback,
		collectedData:      &longhorn.UpgradeManagerStatus{},
		upgradeManagerStatus: &longhorn.UpgradeManagerStatus{
			OwnerID: nodeID,
		},
	}

	go m.Start()

	return m, nil
}

func (m *UpgradeManagerMonitor) Start() {
	m.logger.Infof("Start monitoring upgradeManager %v with sync period %v", m.upgradeManagerName, m.syncPeriod)

	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, true, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring upgrade monitor")
		}
		return false, nil
	}); err != nil {
		m.logger.WithError(err).Error("Failed to start upgradeManager monitor")
	}
}

func (m *UpgradeManagerMonitor) Close() {
	m.quit()
}

func (m *UpgradeManagerMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *UpgradeManagerMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *UpgradeManagerMonitor) GetCollectedData() (interface{}, error) {
	m.RLock()
	defer m.RUnlock()
	return m.collectedData.DeepCopy(), nil
}

func (m *UpgradeManagerMonitor) run(value interface{}) error {
	upgradeManager, err := m.ds.GetUpgradeManager(m.upgradeManagerName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn upgradeManager %v", m.upgradeManagerName)
	}

	existingUpgradeManagerStatus := m.upgradeManagerStatus.DeepCopy()

	m.handleUpgradeManager(upgradeManager)
	if !reflect.DeepEqual(existingUpgradeManagerStatus, m.upgradeManagerStatus) {
		func() {
			m.Lock()
			defer m.Unlock()

			m.collectedData.InstanceManagerImage = m.upgradeManagerStatus.InstanceManagerImage
			m.collectedData.State = m.upgradeManagerStatus.State
			m.collectedData.ErrorMessage = m.upgradeManagerStatus.ErrorMessage
			m.collectedData.UpgradingNode = m.upgradeManagerStatus.UpgradingNode
			m.collectedData.UpgradeNodes = map[string]*longhorn.UpgradeNodeStatus{}
			for k, v := range m.upgradeManagerStatus.UpgradeNodes {
				m.collectedData.UpgradeNodes[k] = &longhorn.UpgradeNodeStatus{
					State:        v.State,
					ErrorMessage: v.ErrorMessage,
				}
			}
		}()

		key := upgradeManager.Namespace + "/" + m.upgradeManagerName
		m.syncCallback(key)
	}
	return nil
}

func (m *UpgradeManagerMonitor) handleUpgradeManager(upgradeManager *longhorn.UpgradeManager) {
	log := m.logger.WithFields(logrus.Fields{"upgradeManager": upgradeManager.Name})
	log.Infof("Handling upgradeManager %v state %v", upgradeManager.Name, m.upgradeManagerStatus.State)

	switch m.upgradeManagerStatus.State {
	case longhorn.UpgradeStateUndefined:
		m.handleUpgradeStateUndefined(upgradeManager)
	case longhorn.UpgradeStateInitializing:
		m.handleUpgradeStateInitializing(upgradeManager)
	case longhorn.UpgradeStateUpgrading:
		m.handleUpgradeStateUpgrading(upgradeManager)
	case longhorn.UpgradeStateCompleted, longhorn.UpgradeStateError:
		return
	default:
		m.handleUpgradeStateUnknown()
	}
}

func (m *UpgradeManagerMonitor) handleUpgradeStateUndefined(upgradeManager *longhorn.UpgradeManager) {
	if !types.IsDataEngineV2(upgradeManager.Spec.DataEngine) {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateError
		m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Unsupported data engine %v", upgradeManager.Spec.DataEngine)
		return
	}

	if m.upgradeManagerStatus.UpgradeNodes == nil {
		m.upgradeManagerStatus.UpgradeNodes = map[string]*longhorn.UpgradeNodeStatus{}
	}

	defaultInstanceManagerImage, err := m.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateError
		m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Failed to get default instance manager image: %v", err.Error())
		return
	}
	m.upgradeManagerStatus.State = longhorn.UpgradeStateInitializing
	m.upgradeManagerStatus.InstanceManagerImage = defaultInstanceManagerImage
}

func (m *UpgradeManagerMonitor) handleUpgradeStateInitializing(upgradeManager *longhorn.UpgradeManager) {
	log := m.logger.WithFields(logrus.Fields{
		"upgradeManager": upgradeManager.Name,
		"state":          m.upgradeManagerStatus.State,
	})

	if len(upgradeManager.Spec.Nodes) == 0 {
		ims, err := m.ds.ListInstanceManagersBySelectorRO("", m.upgradeManagerStatus.InstanceManagerImage, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
		if err != nil {
			m.upgradeManagerStatus.State = longhorn.UpgradeStateError
			m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Failed to list instanceManagers for upgradeManager resource %v: %v", upgradeManager.Name, err.Error())
			return
		}
		for _, im := range ims {
			if string(im.Status.CurrentState) == string(longhorn.InstanceManagerStateRunning) {
				log.Infof("Skipping instanceManager %v for upgradeManager %v since it's already using the instanceManager image", im.Name, upgradeManager.Name)
				m.upgradeManagerStatus.UpgradeNodes[im.Spec.NodeID] = &longhorn.UpgradeNodeStatus{
					State: longhorn.UpgradeStateCompleted,
				}
				continue
			}

			m.upgradeManagerStatus.UpgradeNodes[im.Spec.NodeID] = &longhorn.UpgradeNodeStatus{
				State: longhorn.UpgradeStatePending,
			}
		}
	} else {
		for _, nodeName := range upgradeManager.Spec.Nodes {
			if _, err := m.ds.GetNode(nodeName); err != nil {
				m.upgradeManagerStatus.State = longhorn.UpgradeStateError
				m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Failed to get node %v for upgradeManager resource %v: %v", nodeName, upgradeManager.Name, err.Error())
				return
			}
			m.upgradeManagerStatus.UpgradeNodes[nodeName] = &longhorn.UpgradeNodeStatus{
				State: longhorn.UpgradeStatePending,
			}
		}
	}
	if len(m.upgradeManagerStatus.UpgradeNodes) == 0 {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateCompleted
	} else {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateUpgrading
	}
}

func (m *UpgradeManagerMonitor) handleUpgradeStateUpgrading(upgradeManager *longhorn.UpgradeManager) {
	log := m.logger.WithFields(logrus.Fields{"upgradeManager": upgradeManager.Name})

	// Check if the active nodeUpgrade is matching the m.upgradeManagerStatus.UpgradingNode
	nodeUpgrades, err := m.ds.ListNodeUpgrades()
	if err != nil {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateError
		m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Failed to list nodeUpgrades for upgradeManager resource %v: %v", upgradeManager.Name, err.Error())
		return
	}

	if m.upgradeManagerStatus.UpgradingNode != "" {
		foundNodeUpgrade := false
		nodeUpgrade := &longhorn.NodeUpgrade{}
		for _, nodeUpgrade = range nodeUpgrades {
			if nodeUpgrade.Spec.NodeID != m.upgradeManagerStatus.UpgradingNode {
				continue
			}
			if nodeUpgrade.Status.State != longhorn.UpgradeStateCompleted &&
				nodeUpgrade.Status.State != longhorn.UpgradeStateError {
				return
			}
			foundNodeUpgrade = true
			break
		}
		if foundNodeUpgrade {
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].State = nodeUpgrade.Status.State
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].ErrorMessage = nodeUpgrade.Status.ErrorMessage
			m.upgradeManagerStatus.UpgradingNode = ""
		} else {
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].State = longhorn.UpgradeStateError
			m.upgradeManagerStatus.UpgradeNodes[m.upgradeManagerStatus.UpgradingNode].ErrorMessage = "NodeUpgrade resource not found"
			m.upgradeManagerStatus.UpgradingNode = ""
		}
		return
	}

	// TODO: Check if there is any nodeUpgrade in progress but not tracked by m.upgradeManagerStatus.UpgradingNode

	// Pick a node to upgrade
	for nodeName, nodeStatus := range m.upgradeManagerStatus.UpgradeNodes {
		if nodeStatus.State == longhorn.UpgradeStateCompleted ||
			nodeStatus.State == longhorn.UpgradeStateError {
			continue
		}

		// Create a new upgrade resource for the node
		log.Infof("Creating NodeUpgrade resource for node %v", nodeName)
		_, err := m.ds.GetNode(nodeName)
		if err != nil {
			nodeStatus.State = longhorn.UpgradeStateError
			nodeStatus.ErrorMessage = err.Error()
			continue
		}

		_, err = m.ds.CreateNodeUpgrade(&longhorn.NodeUpgrade{
			ObjectMeta: metav1.ObjectMeta{
				// TODO:
				// 1. Use "nodeName+random" string as the name
				// 2. Add labels
				Name: nodeName,
			},
			Spec: longhorn.NodeUpgradeSpec{
				NodeID:               nodeName,
				DataEngine:           longhorn.DataEngineTypeV2,
				InstanceManagerImage: m.upgradeManagerStatus.InstanceManagerImage,
				UpgradeManager:       upgradeManager.Name,
			},
		})
		if err != nil {
			nodeStatus.State = longhorn.UpgradeStateError
			nodeStatus.ErrorMessage = err.Error()
			continue
		}
		log.Infof("Created NodeUpgrade resource for node %v", nodeName)
		m.upgradeManagerStatus.UpgradingNode = nodeName
		break
	}

	if m.upgradeManagerStatus.UpgradingNode == "" {
		m.upgradeManagerStatus.State = longhorn.UpgradeStateCompleted
	}
}

func (m *UpgradeManagerMonitor) handleUpgradeStateUnknown() {
	m.upgradeManagerStatus.State = longhorn.UpgradeStateError
	m.upgradeManagerStatus.ErrorMessage = fmt.Sprintf("Unknown upgradeManager state %v", m.upgradeManagerStatus.State)
}
