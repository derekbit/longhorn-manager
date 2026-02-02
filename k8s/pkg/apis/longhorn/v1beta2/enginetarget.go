package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EngineTargetSpec defines the desired state of the Longhorn engine target
// EngineTarget is responsible for target-side I/O serving in a separated engine design.
type EngineTargetSpec struct {
	InstanceSpec `json:""`
	// +optional
	ReplicaAddressMap map[string]string `json:"replicaAddressMap"`
	// +optional
	SnapshotMaxCount int `json:"snapshotMaxCount"`
	// +kubebuilder:validation:Type=string
	// +optional
	SnapshotMaxSize int64 `json:"snapshotMaxSize,string"`
}

// EngineTargetStatus defines the observed state of the Longhorn engine target
// NOTE: This is a placeholder for the target-side status until instance-manager support is added.
type EngineTargetStatus struct {
	InstanceStatus `json:""`
	// +optional
	// +nullable
	CurrentReplicaAddressMap map[string]string `json:"currentReplicaAddressMap"`
	// +optional
	// +nullable
	ReplicaModeMap map[string]ReplicaMode `json:"replicaModeMap"`
	// +optional
	Endpoint string `json:"endpoint"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhet
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine of the engine target"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.currentState`,description="The current state of the engine target"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node that the engine target is on"
// +kubebuilder:printcolumn:name="InstanceManager",type=string,JSONPath=`.status.instanceManagerName`,description="The instance manager of the engine target"
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.status.currentImage`,description="The current image of the engine target"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EngineTarget is where Longhorn stores engine target object.
type EngineTarget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngineTargetSpec   `json:"spec,omitempty"`
	Status EngineTargetStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EngineTargetList is a list of EngineTargets.
type EngineTargetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EngineTarget `json:"items"`
}
