package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// UpgradeManagerSpec defines the desired state of the upgrade manager resource
type UpgradeManagerSpec struct {
	// +optional
	DataEngine DataEngineType `json:"dataEngine"`
	// +optional
	Nodes []string `json:"nodes"`
}

// UpgradeState defines the state of the node upgrade process
type UpgradeNodeStatus struct {
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	ErrorMessage string `json:"errorMessage"`
}

// UpgradeManagerStatus defines the observed state of the upgrade manager resource
type UpgradeManagerStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	InstanceManagerImage string `json:"instanceManagerImage"`
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	ErrorMessage string `json:"errorMessage"`
	// +optional
	UpgradingNode string `json:"upgradingNode"`
	// +optional
	UpgradeNodes map[string]*UpgradeNodeStatus `json:"upgradeNodes"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhum
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.ownerID`,description="The node that the upgrade manager is being performed on"
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine targeted for upgrade"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The current state of the upgrade process"
// UpgradeManager is where Longhorn stores upgrade manager object.
type UpgradeManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UpgradeManagerSpec   `json:"spec,omitempty"`
	Status UpgradeManagerStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpgradeManagerList is a list of UpgradeManagers.
type UpgradeManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UpgradeManager `json:"items"`
}
