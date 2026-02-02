package enginetarget

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineTargetMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &engineTargetMutator{ds: ds}
}

func (e *engineTargetMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "enginetargets",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineTarget{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineTargetMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (e *engineTargetMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	engineTarget, ok := newObj.(*longhorn.EngineTarget)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineTarget", newObj), "")
	}

	var patchOps admission.PatchOps

	if engineTarget.Spec.ReplicaAddressMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/replicaAddressMap", "value": {}}`)
	}

	labels := engineTarget.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	patchOp, err := common.GetLonghornLabelsPatchOp(engineTarget, labels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for engineTarget %v", engineTarget.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(engineTarget)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for engineTarget %v", engineTarget.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	if string(engineTarget.Spec.DataEngine) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataEngine", "value": "%s"}`, longhorn.DataEngineTypeV1))
	}

	return patchOps, nil
}
