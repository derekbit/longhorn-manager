package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UpgradeState string

const (
	UpgradeStateUpgrading = "upgrading"
	UpgradeStateError     = "error"
	UpgradeStateCompleted = "completed"
)

type NodeUpgradeState string

const (
	NodeUpgradeStateUndefined      = ""
	NodeUpgradeStatePending        = "pending"
	NodeUpgradeStateSuspending     = "suspending"
	NodeUpgradeStateSuspended      = "suspended"
	NodeUpgradeStateReconstructing = "reconstructing"
	NodeUpgradeStateResuming       = "resuming"
	NodeUpgradeStateCompleted      = "completed"
)

// UpgradeSpec defines the desired state of the upgrade
type UpgradeSpec struct {
	// The backend store driver of the upgrade
	// +optional
	BackendStoreDriver BackendStoreDriverType `json:"backendStoreDriver"`
}

// UpgradeStatus defines the observed state of the Longhorn upgrade data
type UpgradeStatus struct {
	// The node ID on which the controller is responsible to reconcile this backup target CR.
	// +optional
	OwnerID string `json:"ownerID"`
	// The state of the upgrade
	// +optional
	State UpgradeState `json:"state"`
	// The node ID where the upgrade is running.
	// +optional
	UpgradingNode string `json:"upgradingNode"`
	// The state of the upgrade on the node.
	// +optional
	NodeUpgradeState NodeUpgradeState `json:"nodeUpgradeState"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhu
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.upgradeType`,description="The type of the upgrade"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node that the upgrade is on"
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
