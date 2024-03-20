package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UpgradeState string

const (
	UpgradeStateUndefined = UpgradeState("")
	UpgradeStatePending   = UpgradeState("pending")
	UpgradeStateUpgrading = UpgradeState("upgrading")
	UpgradeStateCompleted = UpgradeState("completed")
	UpgradeStateError     = UpgradeState("error")
)

type UpgradedVolume struct {
	NodeID string `json:"nodeID"`
}

// UpgradeSpec defines the desired state of the upgrade resource
type UpgradeSpec struct {
	// +optional
	NodeID string `json:"nodeID"`
	// +optional
	DataEngine DataEngineType `json:"dataEngine"`
}

// UpgradeStatus defines the observed state of the upgrade resource
type UpgradeStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	Volumes map[string]*UpgradedVolume `json:"volumes"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhu
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.ownerID`,description="The node that the upgrade is being performed on"
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine targeted for upgrade"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The current state of the upgrade process"
// Upgrade is where Longhorn stores upgrade object.
type Upgrade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UpgradeSpec   `json:"spec,omitempty"`
	Status UpgradeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpgradeList is a list of upgrades.
type UpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Upgrade `json:"items"`
}
