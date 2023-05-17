package backup

import (
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupValidator{ds: ds}
}

func (b *backupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	if backup.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeSPDK {
		spdkEnabled, err := b.ds.GetSettingAsBool(types.SettingNameSpdk)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !spdkEnabled {
			return werror.NewInvalidError("SPDK feature is not enabled", "")
		}
	}

	if backup.Spec.Labels != nil {
		volumeName, ok := backup.Labels[types.LonghornLabelBackupVolume]
		if !ok {
			err := fmt.Errorf("cannot find the backup volume label for backup %v", backup.Name)
			return werror.NewInvalidError(err.Error(), "")
		}

		volume, err := b.ds.GetVolumeRO(volumeName)
		if err != nil {
			err := errors.Wrapf(err, "cannot find the backup volume %v for backup %v", volumeName, backup.Name)
			return werror.NewInvalidError(err.Error(), "")
		}

		if volume.Spec.BackendStoreDriver != longhorn.BackendStoreDriverTypeLonghorn {
			err := fmt.Errorf("cannot create backup for volume %v with backend store driver %v", volumeName, volume.Spec.BackendStoreDriver)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}
