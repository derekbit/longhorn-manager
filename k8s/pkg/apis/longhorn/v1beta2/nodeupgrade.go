package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UpgradeState string

const (
	UpgradeStateUndefined     = UpgradeState("")
	UpgradeStatePending       = UpgradeState("pending")
	UpgradeStateInitializing  = UpgradeState("initializing")
	UpgradeStateSwitchingOver = UpgradeState("switching-over")
	UpgradeStateUpgrading     = UpgradeState("upgrading")
	UpgradeStateSwitchingBack = UpgradeState("switching-back")
	UpgradeStateCompleted     = UpgradeState("completed")
	UpgradeStateError         = UpgradeState("error")
)

// NodeUpgradeSpec defines the desired state of the node upgrade resource
type NodeUpgradeSpec struct {
	// +optional
	NodeID string `json:"nodeID"`
	// +optional
	DataEngine DataEngineType `json:"dataEngine"`
	// +optional
	InstanceManagerImage string `json:"instanceManagerImage"`
	// +optional
	UpgradeManager string `json:"upgradeManager"`
}

type VolumeUpgradeStatus struct {
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	ErrorMessage string `json:"errorMessage"`
}

// NodeUpgradeStatus defines the observed state of the node upgrade resource
type NodeUpgradeStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	Volumes map[string]*VolumeUpgradeStatus `json:"volumes"`
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	ErrorMessage string `json:"errorMessage"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhnu
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.ownerID`,description="The node that the upgrade is being performed on"
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine targeted for node upgrade"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The current state of the node upgrade process"
// NodeUpgrade is where Longhorn stores node upgrade object.
type NodeUpgrade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeUpgradeSpec   `json:"spec,omitempty"`
	Status NodeUpgradeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeUpgradeList is a list of NodeUpgrades.
type NodeUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeUpgrade `json:"items"`
}
