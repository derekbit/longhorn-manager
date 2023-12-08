package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	NodeConditionTypeReady            = "Ready"
	NodeConditionTypeMountPropagation = "MountPropagation"
	NodeConditionTypeSchedulable      = "Schedulable"
)

const (
	NodeConditionReasonManagerPodDown            = "ManagerPodDown"
	NodeConditionReasonManagerPodMissing         = "ManagerPodMissing"
	NodeConditionReasonKubernetesNodeGone        = "KubernetesNodeGone"
	NodeConditionReasonKubernetesNodeNotReady    = "KubernetesNodeNotReady"
	NodeConditionReasonKubernetesNodePressure    = "KubernetesNodePressure"
	NodeConditionReasonUnknownNodeConditionTrue  = "UnknownNodeConditionTrue"
	NodeConditionReasonNoMountPropagationSupport = "NoMountPropagationSupport"
	NodeConditionReasonKubernetesNodeCordoned    = "KubernetesNodeCordoned"
)

const (
	DiskConditionTypeSchedulable = "Schedulable"
	DiskConditionTypeReady       = "Ready"
	DiskConditionTypeError       = "Error"
)

const (
	DiskConditionReasonDiskPressure           = "DiskPressure"
	DiskConditionReasonDiskFilesystemChanged  = "DiskFilesystemChanged"
	DiskConditionReasonNoDiskInfo             = "NoDiskInfo"
	DiskConditionReasonDiskNotReady           = "DiskNotReady"
	DiskConditionReasonDiskServiceUnreachable = "DiskServiceUnreachable"
)

const (
	ErrorReplicaScheduleInsufficientStorage              = "insufficient storage"
	ErrorReplicaScheduleDiskNotFound                     = "disk not found"
	ErrorReplicaScheduleDiskUnavailable                  = "disks are unavailable"
	ErrorReplicaScheduleSchedulingSettingsRetrieveFailed = "failed to retrieve scheduling settings failed to retrieve"
	ErrorReplicaScheduleTagsNotFulfilled                 = "tags not fulfilled"
	ErrorReplicaScheduleNodeNotFound                     = "node not found"
	ErrorReplicaScheduleNodeUnavailable                  = "nodes are unavailable"
	ErrorReplicaScheduleEngineImageNotReady              = "none of the node candidates contains a ready engine image"
	ErrorReplicaScheduleHardNodeAffinityNotSatisfied     = "hard affinity cannot be satisfied"
	ErrorReplicaScheduleSchedulingFailed                 = "replica scheduling failed"
)

type DiskType string

const (
	DiskTypeFilesystem = DiskType("filesystem")
	DiskTypeBlock      = DiskType("block")
)

type SnapshotCheckStatus struct {
	// +optional
	LastPeriodicCheckedAt metav1.Time `json:"lastPeriodicCheckedAt"`
}

type DiskSpec struct {
	// +kubebuilder:validation:Enum=filesystem;block
	// +optional
	Type DiskType `json:"diskType"`
	// +optional
	Path string `json:"path"`
	// +optional
	AllowScheduling bool `json:"allowScheduling"`
	// +optional
	EvictionRequested bool `json:"evictionRequested"`
	// +optional
	StorageReserved int64 `json:"storageReserved"`
	// +optional
	Tags []string `json:"tags"`
}

type DiskStatus struct {
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
	// +optional
	StorageAvailable int64 `json:"storageAvailable"`
	// +optional
	StorageScheduled int64 `json:"storageScheduled"`
	// +optional
	StorageMaximum int64 `json:"storageMaximum"`
	// +optional
	// +nullable
	ScheduledReplica map[string]int64 `json:"scheduledReplica"`
	// +optional
	DiskUUID string `json:"diskUUID"`
	// +optional
	Type DiskType `json:"diskType"`
	// +optional
	InstanceManagerName string `json:"instanceManagerName"`
}

// NodeSpec defines the desired state of the Longhorn node
type NodeSpec struct {
	// The name of the node.
	// +optional
	Name string `json:"name"`
	// The disks of the node.
	// +optional
	Disks map[string]DiskSpec `json:"disks"`
	// Allow scheduling replicas on the node.
	// +optional
	AllowScheduling bool `json:"allowScheduling"`
	// Request to evict all replicas on the node.
	// +optional
	EvictionRequested bool `json:"evictionRequested"`
	// The tags of the node.
	// +optional
	Tags []string `json:"tags"`
	// The CPU request of the instance manager.
	// +optional
	InstanceManagerCPURequest int `json:"instanceManagerCPURequest"`
	// Request to upgrade the instance manager for v2 volumes on the node.
	// +optional
	UpgradeRequested bool `json:"upgradeRequested"`
}

// NodeStatus defines the observed state of the Longhorn node
type NodeStatus struct {
	// The condition of the node.
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
	// The status of the disks on the node.
	// +optional
	// +nullable
	DiskStatus map[string]*DiskStatus `json:"diskStatus"`
	// The Region of the node.
	// +optional
	Region string `json:"region"`
	// The Zone of the node.
	// +optional
	Zone string `json:"zone"`
	// The status of the snapshot integrity check.
	// +optional
	SnapshotCheckStatus SnapshotCheckStatus `json:"snapshotCheckStatus"`
	// Indicate whether the node is auto-evicting.
	// +optional
	AutoEvicting bool `json:"autoEvicting"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhn
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].status`,description="Indicate whether the node is ready"
// +kubebuilder:printcolumn:name="AllowScheduling",type=boolean,JSONPath=`.spec.allowScheduling`,description="Indicate whether the user disabled/enabled replica scheduling for the node"
// +kubebuilder:printcolumn:name="Schedulable",type=string,JSONPath=`.status.conditions[?(@.type=='Schedulable')].status`,description="Indicate whether Longhorn can schedule replicas on the node"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Node is where Longhorn stores Longhorn node object.
type Node struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeSpec   `json:"spec,omitempty"`
	Status NodeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeList is a list of Nodes.
type NodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Node `json:"items"`
}

// Hub defines the current version (v1beta2) is the storage version
// so mark this as Hub
func (n *Node) Hub() {}
