package controller

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Spec.EvictionRequested {
			return true
		}
	}

	return false
}

func isVolumeMigrating(v *longhorn.Volume) bool {
	return v.Spec.MigrationNodeID != "" || v.Status.CurrentMigrationNodeID != ""
}

func (vc *VolumeController) isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.Image
}

// isTargetVolumeOfCloning checks if the input volume is the target volume of an on-going cloning process
func isTargetVolumeOfCloning(v *longhorn.Volume) bool {
	isCloningDesired := types.IsDataFromVolume(v.Spec.DataSource)
	isCloningDone := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed
	return isCloningDesired && !isCloningDone
}

func isVolumeFullyDetached(vol *longhorn.Volume) bool {
	return vol.Spec.NodeID == "" &&
		vol.Spec.MigrationNodeID == "" &&
		vol.Status.PendingNodeID == "" &&
		vol.Status.State == longhorn.VolumeStateDetached
}

func createOrUpdateAttachmentTicket(va *longhorn.VolumeAttachment, ticketID, nodeID, disableFrontend string, attacherType longhorn.AttacherType) {
	attachmentTicket, ok := va.Spec.AttachmentTickets[ticketID]
	if !ok {
		// Create new one
		attachmentTicket = &longhorn.AttachmentTicket{
			ID:     ticketID,
			Type:   attacherType,
			NodeID: nodeID,
			Parameters: map[string]string{
				longhorn.AttachmentParameterDisableFrontend: disableFrontend,
			},
		}
	}
	if attachmentTicket.NodeID != nodeID {
		attachmentTicket.NodeID = nodeID
	}
	va.Spec.AttachmentTickets[attachmentTicket.ID] = attachmentTicket
}

func handleReconcileErrorLogging(logger logrus.FieldLogger, err error, mesg string) {
	if types.ErrorIsInvalidState(err) {
		logger.WithError(err).Trace(mesg)
		return
	}

	if apierrors.IsConflict(err) {
		logger.WithError(err).Warn(mesg)
	} else {
		logger.WithError(err).Error(mesg)
	}
}

func isV2Replica(r *longhorn.Replica) bool {
	return r.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2
}

func isV2Engine(e *longhorn.Engine) bool {
	return e.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2
}

func isV2Volume(v *longhorn.Volume) bool {
	return v.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2
}

func isV1Volume(v *longhorn.Volume) bool {
	return v.Spec.BackendStoreDriver != longhorn.BackendStoreDriverTypeV2
}

func checkDiskReadiness(ds *datastore.DataStore, r *longhorn.Replica, im *longhorn.InstanceManager) (bool, error) {
	node, err := ds.GetNodeRO(r.Status.OwnerID)
	if err != nil {
		return false, err
	}

	for _, diskStatus := range node.Status.DiskStatus {
		if diskStatus.DiskUUID == r.Spec.DiskID {
			if diskStatus.InstanceManagerName == im.Name &&
				types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeReady).Status == longhorn.ConditionStatusTrue {
				return true, nil
			}
			return false, nil
		}
	}

	return false, fmt.Errorf("failed to find disk %v on node %v", r.Spec.DiskID, node.Name)
}
