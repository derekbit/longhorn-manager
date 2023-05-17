package snapshot

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type snapshotValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &snapshotValidator{ds: ds}
}

func (o *snapshotValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "snapshots",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Snapshot{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (o *snapshotValidator) Create(request *admission.Request, newObj runtime.Object) error {
	snapshot, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
	}

	if snapshot.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeSPDK {
		spdkEnabled, err := o.ds.GetSettingAsBool(types.SettingNameSpdk)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !spdkEnabled {
			return werror.NewInvalidError("SPDK feature is not enabled", "")
		}
	}

	return nil
}

func (o *snapshotValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldSnapshot, ok := oldObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", oldObj), "")
	}
	newSnapshot, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
	}

	if newSnapshot.Spec.Volume != oldSnapshot.Spec.Volume {
		return werror.NewInvalidError("spec.volume field is immutable", "spec.volume")
	}

	if len(oldSnapshot.OwnerReferences) != 0 && !reflect.DeepEqual(newSnapshot.OwnerReferences, oldSnapshot.OwnerReferences) {
		return werror.NewInvalidError("snapshot OwnerReferences field is immutable", "metadata.ownerReferences")
	}

	if _, ok := oldSnapshot.Labels[types.LonghornLabelVolume]; ok && newSnapshot.Labels[types.LonghornLabelVolume] != oldSnapshot.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	if oldSnapshot.Spec.BackendStoreDriver != "" {
		if oldSnapshot.Spec.BackendStoreDriver != newSnapshot.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for snapshot %v is not supported", oldSnapshot.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}
