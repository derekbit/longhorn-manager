package types

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	LonghornKindNode                = "Node"
	LonghornKindVolume              = "Volume"
	LonghornKindEngine              = "Engine"
	LonghornKindReplica             = "Replica"
	LonghornKindBackup              = "Backup"
	LonghornKindEngineImage         = "EngineImage"
	LonghornKindInstanceManager     = "InstanceManager"
	LonghornKindShareManager        = "ShareManager"
	LonghornKindBackingImage        = "BackingImage"
	LonghornKindBackingImageManager = "BackingImageManager"
	LonghornKindRecurringJob        = "RecurringJob"
	LonghornKindSetting             = "Setting"
	LonghornKindSupportBundle       = "SupportBundle"
	LonghornKindSystemRestore       = "SystemRestore"

	LonghornKindBackingImageDataSource = "BackingImageDataSource"

	LonghornKindEngineImageList  = "EngineImageList"
	LonghornKindRecurringJobList = "RecurringJobList"
	LonghornKindSettingList      = "SettingList"
	LonghornKindVolumeList       = "VolumeList"

	KubernetesKindClusterRole           = "ClusterRole"
	KubernetesKindClusterRoleBinding    = "ClusterRoleBinding"
	KubernetesKindConfigMap             = "ConfigMap"
	KubernetesKindDaemonSet             = "DaemonSet"
	KubernetesKindDeployment            = "Deployment"
	KubernetesKindJob                   = "Job"
	KubernetesKindPersistentVolume      = "PersistentVolume"
	KubernetesKindPersistentVolumeClaim = "PersistentVolumeClaim"
	KubernetesKindPodSecurityPolicy     = "PodSecurityPolicy"
	KubernetesKindRole                  = "Role"
	KubernetesKindRoleBinding           = "RoleBinding"
	KubernetesKindService               = "Service"
	KubernetesKindServiceAccount        = "ServiceAccount"
	KubernetesKindStorageClass          = "StorageClass"

	KubernetesKindClusterRoleList           = "ClusterRoleList"
	KubernetesKindClusterRoleBindingList    = "ClusterRoleBindingList"
	KubernetesKindConfigMapList             = "ConfigMapList"
	KubernetesKindDaemonSetList             = "DaemonSetList"
	KubernetesKindDeploymentList            = "DeploymentList"
	KubernetesKindPersistentVolumeList      = "PersistentVolumeList"
	KubernetesKindPersistentVolumeClaimList = "PersistentVolumeClaimList"
	KubernetesKindPodSecurityPolicyList     = "PodSecurityPolicyList"
	KubernetesKindRoleList                  = "RoleList"
	KubernetesKindRoleBindingList           = "RoleBindingList"
	KubernetesKindServiceList               = "ServiceList"
	KubernetesKindServiceAccountList        = "ServiceAccountList"
	KubernetesKindStorageClassList          = "StorageClassList"

	APIExtensionsKindCustomResourceDefinition = "CustomResourceDefinition"

	APIExtensionsKindCustomResourceDefinitionList = "CustomResourceDefinitionList"

	CRDAPIVersionV1alpha1 = "longhorn.rancher.io/v1alpha1"
	CRDAPIVersionV1beta1  = "longhorn.io/v1beta1"
	CRDAPIVersionV1beta2  = "longhorn.io/v1beta2"
	CurrentCRDAPIVersion  = CRDAPIVersionV1beta2
)

const (
	DefaultAPIPort           = 9500
	DefaultWebhookServerPort = 9443

	WebhookTypeConversion = "conversion"
	WebhookTypeAdmission  = "admission"

	ValidatingWebhookName = "longhorn-webhook-validator"
	MutatingWebhookName   = "longhorn-webhook-mutator"

	DefaultRecoveryBackendServerPort = 9600

	EngineBinaryDirectoryInContainer = "/engine-binaries/"
	EngineBinaryDirectoryOnHost      = "/var/lib/longhorn/engine-binaries/"
	ReplicaHostPrefix                = "/host"
	EngineBinaryName                 = "longhorn"

	UnixDomainSocketDirectoryInContainer = "/host/var/lib/longhorn/unix-domain-socket/"
	UnixDomainSocketDirectoryOnHost      = "/var/lib/longhorn/unix-domain-socket/"

	BackingImageManagerDirectory = "/backing-images/"
	BackingImageFileName         = "backing"

	TLSDirectoryInContainer = "/tls-files/"
	TLSSecretName           = "longhorn-grpc-tls"
	TLSCAFile               = "ca.crt"
	TLSCertFile             = "tls.crt"
	TLSKeyFile              = "tls.key"

	DefaultBackupTargetName = "default"

	LonghornNodeKey     = "longhornnode"
	LonghornDiskUUIDKey = "longhorndiskuuid"

	NodeCreateDefaultDiskLabelKey             = "node.longhorn.io/create-default-disk"
	NodeCreateDefaultDiskLabelValueTrue       = "true"
	NodeCreateDefaultDiskLabelValueConfig     = "config"
	KubeNodeDefaultDiskConfigAnnotationKey    = "node.longhorn.io/default-disks-config"
	KubeNodeDefaultNodeTagConfigAnnotationKey = "node.longhorn.io/default-node-tags"

	LastAppliedTolerationAnnotationKeySuffix = "last-applied-tolerations"

	ConfigMapResourceVersionKey = "configmap-resource-version"

	KubernetesStatusLabel = "KubernetesStatus"
	KubernetesReplicaSet  = "ReplicaSet"
	KubernetesStatefulSet = "StatefulSet"
	RecurringJobLabel     = "RecurringJob"

	VolumeRecurringJobInfoLabel     = "VolumeRecurringJobInfo"
	VolumeRecurringJobRestorePrefix = "restored-recurring-job-"

	LonghornLabelKeyPrefix = "longhorn.io"

	LonghornLabelRecurringJobKeyPrefixFmt = "recurring-%s.longhorn.io"
	LonghornLabelVolumeSettingKeyPrefix   = "setting.longhorn.io"

	LonghornLabelEngineImage                = "engine-image"
	LonghornLabelInstanceManager            = "instance-manager"
	LonghornLabelNode                       = "node"
	LonghornLabelDiskUUID                   = "disk-uuid"
	LonghornLabelInstanceManagerType        = "instance-manager-type"
	LonghornLabelInstanceManagerImage       = "instance-manager-image"
	LonghornLabelVolume                     = "longhornvolume"
	LonghornLabelShareManager               = "share-manager"
	LonghornLabelShareManagerImage          = "share-manager-image"
	LonghornLabelShareManagerConfigMap      = "share-manager-configmap"
	LonghornLabelBackingImage               = "backing-image"
	LonghornLabelBackingImageManager        = "backing-image-manager"
	LonghornLabelManagedBy                  = "managed-by"
	LonghornLabelSnapshotForCloningVolume   = "for-cloning-volume"
	LonghornLabelBackingImageDataSource     = "backing-image-data-source"
	LonghornLabelBackupVolume               = "backup-volume"
	LonghornLabelRecurringJob               = "job"
	LonghornLabelRecurringJobGroup          = "job-group"
	LonghornLabelOrphan                     = "orphan"
	LonghornLabelOrphanType                 = "orphan-type"
	LonghornLabelRecoveryBackend            = "recovery-backend"
	LonghornLabelCRDAPIVersion              = "crd-api-version"
	LonghornLabelVolumeAccessMode           = "volume-access-mode"
	LonghornLabelFollowGlobalSetting        = "follow-global-setting"
	LonghornLabelSystemRestore              = "system-restore"
	LonghornLabelLastSkippedSystemRestore   = "last-skipped-system-restored"
	LonghornLabelLastSkippedSystemRestoreAt = "last-skipped-system-restored-at"
	LonghornLabelLastSystemRestore          = "last-system-restored"
	LonghornLabelLastSystemRestoreAt        = "last-system-restored-at"
	LonghornLabelLastSystemRestoreBackup    = "last-system-restored-backup"
	LonghornLabelVersion                    = "version"

	LonghornLabelValueEnabled = "enabled"
	LonghornLabelValueIgnored = "ignored"

	LonghornLabelExportFromVolume                 = "export-from-volume"
	LonghornLabelSnapshotForExportingBackingImage = "for-exporting-backing-image"

	KubernetesFailureDomainRegionLabelKey = "failure-domain.beta.kubernetes.io/region"
	KubernetesFailureDomainZoneLabelKey   = "failure-domain.beta.kubernetes.io/zone"
	KubernetesTopologyRegionLabelKey      = "topology.kubernetes.io/region"
	KubernetesTopologyZoneLabelKey        = "topology.kubernetes.io/zone"

	KubernetesClusterAutoscalerSafeToEvictKey = "cluster-autoscaler.kubernetes.io/safe-to-evict"

	LonghornDriverName = "driver.longhorn.io"

	DefaultDiskPrefix = "default-disk-"

	DeprecatedProvisionerName          = "rancher.io/longhorn"
	DepracatedDriverName               = "io.rancher.longhorn"
	DefaultStorageClassConfigMapName   = "longhorn-storageclass"
	DefaultDefaultSettingConfigMapName = "longhorn-default-setting"
	DefaultStorageClassName            = "longhorn"
	ControlPlaneName                   = "longhorn-manager"

	DefaultRecurringJobConcurrency = 10

	PVAnnotationLonghornVolumeSchedulingError = "longhorn.io/volume-scheduling-error"

	CniNetworkNone          = ""
	StorageNetworkInterface = "lhnet1"
)

const (
	SupportBundleNameFmt = "support-bundle-%v"

	SupportBundleManagerApp      = "support-bundle-manager"
	SupportBundleManagerLabelKey = "rancher/supportbundle"

	SupportBundleURLPort        = 8080
	SupportBundleURLStatusFmt   = "http://%s:%v/status"
	SupportBundleURLDownloadFmt = "http://%s:%v/bundle"

	SupportBundleDownloadTimeout = 5 * time.Minute
)

const (
	KubernetesMinVersion = "v1.18.0"
)

const (
	EnvNodeName       = "NODE_NAME"
	EnvPodNamespace   = "POD_NAMESPACE"
	EnvPodIP          = "POD_IP"
	EnvServiceAccount = "SERVICE_ACCOUNT"

	BackupStoreTypeS3   = "s3"
	BackupStoreTypeCIFS = "cifs"

	AWSIAMRoleAnnotation = "iam.amazonaws.com/role"
	AWSIAMRoleArn        = "AWS_IAM_ROLE_ARN"
	AWSAccessKey         = "AWS_ACCESS_KEY_ID"
	AWSSecretKey         = "AWS_SECRET_ACCESS_KEY"
	AWSEndPoint          = "AWS_ENDPOINTS"
	AWSCert              = "AWS_CERT"

	CIFSUsername = "CIFS_USERNAME"
	CIFSPassword = "CIFS_PASSWORD"

	HTTPSProxy = "HTTPS_PROXY"
	HTTPProxy  = "HTTP_PROXY"
	NOProxy    = "NO_PROXY"

	VirtualHostedStyle = "VIRTUAL_HOSTED_STYLE"

	OptionFromBackup          = "fromBackup"
	OptionNumberOfReplicas    = "numberOfReplicas"
	OptionStaleReplicaTimeout = "staleReplicaTimeout"
	OptionBaseImage           = "baseImage"
	OptionFrontend            = "frontend"
	OptionDiskSelector        = "diskSelector"
	OptionNodeSelector        = "nodeSelector"

	// DefaultStaleReplicaTimeout in minutes. 48h by default
	DefaultStaleReplicaTimeout = "2880"

	ImageChecksumNameLength             = 8
	InstanceManagerSuffixChecksumLength = 32
)

// SettingsRelatedToVolume should match the items in datastore.GetLabelsForVolumesFollowsGlobalSettings
//
//	TODO: May need to add the data locality check
var SettingsRelatedToVolume = map[string]string{
	string(SettingNameReplicaAutoBalance):                  LonghornLabelValueIgnored,
	string(SettingNameSnapshotDataIntegrity):               LonghornLabelValueIgnored,
	string(SettingNameRemoveSnapshotsDuringFilesystemTrim): LonghornLabelValueIgnored,
}

type NotFoundError struct {
	Name string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("cannot find %v", e.Name)
}

const (
	engineSuffix    = "-e"
	replicaSuffix   = "-r"
	recurringSuffix = "-c"

	engineImagePrefix          = "ei-"
	instanceManagerImagePrefix = "imi-"
	shareManagerImagePrefix    = "smi-"
	orphanPrefix               = "orphan-"

	BackingImageDataSourcePodNamePrefix = "backing-image-ds-"

	shareManagerPrefix    = "share-manager-"
	recoveryBackendPrefix = "recovery-backend-"
	instanceManagerPrefix = "instance-manager-"
	engineManagerPrefix   = instanceManagerPrefix + "e-"
	replicaManagerPrefix  = instanceManagerPrefix + "r-"
)

func GenerateEngineNameForVolume(vName string) string {
	return vName + engineSuffix + "-" + util.RandomID()
}

func GenerateReplicaNameForVolume(vName string) string {
	return vName + replicaSuffix + "-" + util.RandomID()
}

func GetCronJobNameForRecurringJob(name string) string {
	return name + recurringSuffix
}

func GetCronJobNameForVolumeAndJob(vName, job string) string {
	return vName + "-" + job + recurringSuffix
}

func GetAPIServerAddressFromIP(ip string) string {
	return net.JoinHostPort(ip, strconv.Itoa(DefaultAPIPort))
}

func GetDefaultManagerURL() string {
	return "http://longhorn-backend:" + strconv.Itoa(DefaultAPIPort) + "/v1"
}

func GetImageCanonicalName(image string) string {
	return strings.Replace(strings.Replace(image, ":", "-", -1), "/", "-", -1)
}

func GetEngineBinaryDirectoryOnHostForImage(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(EngineBinaryDirectoryOnHost, cname)
}

func GetEngineBinaryDirectoryForEngineManagerContainer(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(EngineBinaryDirectoryInContainer, cname)
}

func GetEngineBinaryDirectoryForReplicaManagerContainer(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(filepath.Join(ReplicaHostPrefix, EngineBinaryDirectoryOnHost), cname)
}

func EngineBinaryExistOnHostForImage(image string) bool {
	st, err := os.Stat(filepath.Join(GetEngineBinaryDirectoryOnHostForImage(image), "longhorn"))
	return err == nil && !st.IsDir()
}

func GetBackingImageManagerName(image, diskUUID string) string {
	return fmt.Sprintf("backing-image-manager-%s-%s", util.GetStringChecksum(image)[:4], diskUUID[:4])
}

func GetBackingImageDirectoryName(backingImageName, backingImageUUID string) string {
	return fmt.Sprintf("%s-%s", backingImageName, backingImageUUID)
}

func GetBackingImageManagerDirectoryOnHost(diskPath string) string {
	return filepath.Join(diskPath, BackingImageManagerDirectory)
}

func GetBackingImageDirectoryOnHost(diskPath, backingImageName, backingImageUUID string) string {
	return filepath.Join(GetBackingImageManagerDirectoryOnHost(diskPath), GetBackingImageDirectoryName(backingImageName, backingImageUUID))
}

func GetBackingImagePathForReplicaManagerContainer(diskPath, backingImageName, backingImageUUID string) string {
	return filepath.Join(ReplicaHostPrefix, GetBackingImageDirectoryOnHost(diskPath, backingImageName, backingImageUUID), BackingImageFileName)
}

var (
	LonghornSystemKey = "longhorn"
)

func GetLonghornLabelKey(name string) string {
	return fmt.Sprintf("%s/%s", LonghornLabelKeyPrefix, name)
}

func GetBaseLabelsForSystemManagedComponent() map[string]string {
	return map[string]string{GetLonghornLabelKey(LonghornLabelManagedBy): ControlPlaneName}
}

func GetLonghornLabelComponentKey() string {
	return GetLonghornLabelKey("component")
}

func GetLonghornLabelCRDAPIVersionKey() string {
	return GetLonghornLabelKey(LonghornLabelCRDAPIVersion)
}

func GetEngineImageLabels(engineImageName string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelEngineImage
	labels[GetLonghornLabelKey(LonghornLabelEngineImage)] = engineImageName
	return labels
}

// GetEIDaemonSetLabelSelector returns labels for engine image daemonset's Spec.Selector.MatchLabels
func GetEIDaemonSetLabelSelector(engineImageName string) map[string]string {
	labels := make(map[string]string)
	labels[GetLonghornLabelComponentKey()] = LonghornLabelEngineImage
	labels[GetLonghornLabelKey(LonghornLabelEngineImage)] = engineImageName
	return labels
}

func GetEngineImageComponentLabel() map[string]string {
	return map[string]string{
		GetLonghornLabelComponentKey(): LonghornLabelEngineImage,
	}
}

func GetInstanceManagerLabels(node, instanceManagerImage string, managerType longhorn.InstanceManagerType) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelInstanceManager
	labels[GetLonghornLabelKey(LonghornLabelInstanceManagerType)] = string(managerType)
	if node != "" {
		labels[GetLonghornLabelKey(LonghornLabelNode)] = node
	}
	if instanceManagerImage != "" {
		labels[GetLonghornLabelKey(LonghornLabelInstanceManagerImage)] = GetInstanceManagerImageChecksumName(GetImageCanonicalName(instanceManagerImage))
	}

	return labels
}

func GetInstanceManagerComponentLabel() map[string]string {
	return map[string]string{
		GetLonghornLabelComponentKey(): LonghornLabelInstanceManager,
	}
}

func GetShareManagerComponentLabel() map[string]string {
	return map[string]string{
		GetLonghornLabelComponentKey(): LonghornLabelShareManager,
	}
}

func GetShareManagerInstanceLabel(name string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelKey(LonghornLabelShareManager)] = name
	return labels
}

func GetShareManagerLabels(name, image string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelShareManager

	if name != "" {
		labels[GetLonghornLabelKey(LonghornLabelShareManager)] = name
	}

	if image != "" {
		labels[GetLonghornLabelKey(LonghornLabelShareManagerImage)] = GetShareManagerImageChecksumName(GetImageCanonicalName(image))
	}

	return labels
}

func GetShareManagerConfigMapLabels(name string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelKey(LonghornLabelShareManager)] = name
	labels[GetLonghornLabelComponentKey()] = LonghornLabelShareManagerConfigMap
	return labels
}

func GetCronJobLabels(job *longhorn.RecurringJobSpec) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[fmt.Sprintf(LonghornLabelRecurringJobKeyPrefixFmt, LonghornLabelRecurringJob)] = job.Name
	return labels
}

func GetBackingImageLabels() map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelBackingImage
	return labels
}

func GetBackingImageManagerLabels(nodeID, diskUUID string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelBackingImageManager
	if diskUUID != "" {
		labels[GetLonghornLabelKey(LonghornLabelDiskUUID)] = diskUUID
	}
	if nodeID != "" {
		labels[GetLonghornLabelKey(LonghornLabelNode)] = nodeID
	}
	return labels
}

func GetBackingImageDataSourceLabels(name, nodeID, diskUUID string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelBackingImageDataSource
	if name != "" {
		labels[GetLonghornLabelKey(LonghornLabelBackingImageDataSource)] = name
	}
	if diskUUID != "" {
		labels[GetLonghornLabelKey(LonghornLabelDiskUUID)] = diskUUID
	}
	if nodeID != "" {
		labels[GetLonghornLabelKey(LonghornLabelNode)] = nodeID
	}
	return labels
}

func GetBackupVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		LonghornLabelBackupVolume: volumeName,
	}
}

func GetVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		LonghornLabelVolume: volumeName,
	}
}

func GetRecurringJobLabelKey(labelType, recurringJobName string) string {
	prefix := fmt.Sprintf(LonghornLabelRecurringJobKeyPrefixFmt, labelType)
	return fmt.Sprintf("%s/%s", prefix, recurringJobName)
}

func GetRecurringJobLabelValueMap(labelType, recurringJobName string) map[string]string {
	return map[string]string{
		GetRecurringJobLabelKey(labelType, recurringJobName): LonghornLabelValueEnabled,
	}
}

func GetOrphanLabelsForOrphanedDirectory(nodeID, diskUUID string) map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelOrphan
	labels[LonghornNodeKey] = nodeID
	labels[GetLonghornLabelKey(LonghornLabelOrphanType)] = string(longhorn.OrphanTypeReplica)
	return labels
}

func GetRecoveryBackendConfigMapLabels() map[string]string {
	labels := GetBaseLabelsForSystemManagedComponent()
	labels[GetLonghornLabelComponentKey()] = LonghornLabelRecoveryBackend
	return labels
}

func GetSystemRestoreInProgressLabel() map[string]string {
	return map[string]string{
		GetSystemRestoreLabelKey(): string(longhorn.SystemRestoreStateInProgress),
	}
}

func GetSystemRestoreLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelSystemRestore)
}

func GetLastSystemRestoreLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelLastSystemRestore)
}

func GetLastSystemRestoreAtLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelLastSystemRestoreAt)
}

func GetLastSkippedSystemRestoreLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelLastSkippedSystemRestore)
}

func GetLastSkippedSystemRestoreAtLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelLastSkippedSystemRestoreAt)
}

func GetLastSystemRestoreBackupLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelLastSystemRestoreBackup)
}

func GetVersionLabelKey() string {
	return GetLonghornLabelKey(LonghornLabelVersion)
}

func GetRegionAndZone(labels map[string]string) (string, string) {
	region := ""
	zone := ""
	if v, ok := labels[KubernetesTopologyRegionLabelKey]; ok {
		region = v
	}
	if v, ok := labels[KubernetesTopologyZoneLabelKey]; ok {
		zone = v
	}
	return region, zone
}

func GetEngineImageChecksumName(image string) string {
	return engineImagePrefix + util.GetStringChecksum(strings.TrimSpace(image))[:ImageChecksumNameLength]
}

func GetInstanceManagerImageChecksumName(image string) string {
	return instanceManagerImagePrefix + util.GetStringChecksum(strings.TrimSpace(image))[:ImageChecksumNameLength]
}

func GetShareManagerImageChecksumName(image string) string {
	return shareManagerImagePrefix + util.GetStringChecksum(strings.TrimSpace(image))[:ImageChecksumNameLength]
}

func GetOrphanChecksumNameForOrphanedDirectory(nodeID, diskName, diskPath, diskUUID, dirName string) string {
	return orphanPrefix + util.GetStringChecksumSHA256(strings.TrimSpace(fmt.Sprintf("%s-%s-%s-%s-%s", nodeID, diskName, diskPath, diskUUID, dirName)))
}

func GetShareManagerPodNameFromShareManagerName(smName string) string {
	return shareManagerPrefix + smName
}

func GetConfigMapNameFromShareManagerName(smName string) string {
	return recoveryBackendPrefix + shareManagerPrefix + smName
}

func GetConfigMapNameFromHostname(hostname string) string {
	return recoveryBackendPrefix + hostname
}

func GetShareManagerNameFromShareManagerPodName(podName string) string {
	return strings.TrimPrefix(podName, shareManagerPrefix)
}

func ValidateEngineImageChecksumName(name string) bool {
	matched, _ := regexp.MatchString(fmt.Sprintf("^%s[a-fA-F0-9]{%d}$", engineImagePrefix, ImageChecksumNameLength), name)
	return matched
}

func GetInstanceManagerName(imType longhorn.InstanceManagerType, nodeName, image string) (string, error) {
	hashedSuffix := util.GetStringChecksum(nodeName + image)[:InstanceManagerSuffixChecksumLength]
	switch imType {
	case longhorn.InstanceManagerTypeEngine:
		return engineManagerPrefix + hashedSuffix, nil
	case longhorn.InstanceManagerTypeReplica:
		return replicaManagerPrefix + hashedSuffix, nil
	case longhorn.InstanceManagerTypeAllInOne:
		return instanceManagerPrefix + hashedSuffix, nil
	}
	return "", fmt.Errorf("cannot generate name for unknown instance manager type %v", imType)
}

func GetInstanceManagerPrefix(imType longhorn.InstanceManagerType) string {
	switch imType {
	case longhorn.InstanceManagerTypeAllInOne:
		return instanceManagerPrefix
	case longhorn.InstanceManagerTypeEngine:
		return engineManagerPrefix
	case longhorn.InstanceManagerTypeReplica:
		return replicaManagerPrefix
	}
	return ""
}

func GetBackingImageDataSourcePodName(bidsName string) string {
	return fmt.Sprintf("%s%s", BackingImageDataSourcePodNamePrefix, bidsName)
}

func GetReplicaDataPath(diskPath, dataDirectoryName string) string {
	return filepath.Join(diskPath, "replicas", dataDirectoryName)
}

func GetReplicaMountedDataPath(dataPath string) string {
	if !strings.HasPrefix(dataPath, ReplicaHostPrefix) {
		return filepath.Join(ReplicaHostPrefix, dataPath)
	}
	return dataPath
}

func ErrorIsNotFound(err error) bool {
	return strings.Contains(err.Error(), "cannot find")
}

func ErrorIsNotSupport(err error) bool {
	return strings.Contains(err.Error(), "not support")
}

func ErrorAlreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

func ValidateReplicaCount(count int) error {
	if count < 1 || count > 20 {
		return fmt.Errorf("replica count value must between 1 to 20")
	}
	return nil
}

func ValidateDataLocalityAndReplicaCount(mode longhorn.DataLocality, count int) error {
	if mode == longhorn.DataLocalityStrictLocal && count != 1 {
		return fmt.Errorf("replica count should be 1 in data locality %v mode", longhorn.DataLocalityStrictLocal)
	}
	return nil
}

func ValidateReplicaAutoBalance(option longhorn.ReplicaAutoBalance) error {
	switch option {
	case longhorn.ReplicaAutoBalanceIgnored,
		longhorn.ReplicaAutoBalanceDisabled,
		longhorn.ReplicaAutoBalanceLeastEffort,
		longhorn.ReplicaAutoBalanceBestEffort:
		return nil
	default:
		return fmt.Errorf("invalid replica auto-balance option: %v", option)
	}
}

func ValidateDataLocality(mode longhorn.DataLocality) error {
	if mode != longhorn.DataLocalityDisabled && mode != longhorn.DataLocalityBestEffort && mode != longhorn.DataLocalityStrictLocal {
		return fmt.Errorf("invalid data locality mode: %v", mode)
	}
	return nil
}

func ValidateAccessMode(mode longhorn.AccessMode) error {
	if mode != longhorn.AccessModeReadWriteMany && mode != longhorn.AccessModeReadWriteOnce {
		return fmt.Errorf("invalid access mode: %v", mode)
	}
	return nil
}

func ValidateStorageNetwork(value string) (err error) {
	if value == CniNetworkNone {
		return nil
	}

	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return errors.Errorf("storage network must be in <NAMESPACE>/<NETWORK-ATTACHMENT-DEFINITION> format: %v", value)
	}
	return nil
}

func ValidateSnapshotDataIntegrity(mode string) error {
	if mode != string(longhorn.SnapshotDataIntegrityDisabled) &&
		mode != string(longhorn.SnapshotDataIntegrityEnabled) &&
		mode != string(longhorn.SnapshotDataIntegrityFastCheck) {
		return fmt.Errorf("invalid snapshot data integrity mode: %v", mode)
	}
	return nil
}

func ValidateBackupCompressionMethod(method string) error {
	if method != string(longhorn.BackupCompressionMethodNone) &&
		method != string(longhorn.BackupCompressionMethodLz4) &&
		method != string(longhorn.BackupCompressionMethodGzip) {
		return fmt.Errorf("invalid backup compression method: %v", method)
	}
	return nil
}

func ValidateUnmapMarkSnapChainRemoved(unmapValue longhorn.UnmapMarkSnapChainRemoved) error {
	if unmapValue != longhorn.UnmapMarkSnapChainRemovedIgnored && unmapValue != longhorn.UnmapMarkSnapChainRemovedEnabled && unmapValue != longhorn.UnmapMarkSnapChainRemovedDisabled {
		return fmt.Errorf("invalid UnmapMarkSnapChainRemoved setting: %v", unmapValue)
	}
	return nil
}

func GetDaemonSetNameFromEngineImageName(engineImageName string) string {
	return "engine-image-" + engineImageName
}

func GetEngineImageNameFromDaemonSetName(dsName string) string {
	return strings.TrimPrefix(dsName, "engine-image-")
}

func GetVolumeSettingLabelKey(settingName string) string {
	return fmt.Sprintf("%s/%s", LonghornLabelVolumeSettingKeyPrefix, settingName)
}

func LabelsToString(labels map[string]string) string {
	res := ""
	for k, v := range labels {
		res += fmt.Sprintf("%s=%s,", k, v)
	}
	res = strings.TrimSuffix(res, ",")
	return res
}

func CreateDisksFromAnnotation(annotation string) (map[string]longhorn.DiskSpec, error) {
	validDisks := map[string]longhorn.DiskSpec{}
	existFsid := map[string]string{}

	disks, err := UnmarshalToDisks(annotation)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal the default disks annotation")
	}
	for _, disk := range disks {
		if disk.Path == "" {
			return nil, fmt.Errorf("invalid disk %+v", disk)
		}
		diskStat, err := util.GetDiskStat(disk.Path)
		if err != nil {
			return nil, err
		}
		for _, vDisk := range validDisks {
			if vDisk.Path == disk.Path {
				return nil, fmt.Errorf("duplicate disk path %v", disk.Path)
			}
		}

		// Set to default disk name
		if disk.Name == "" {
			disk.Name = DefaultDiskPrefix + diskStat.Fsid
		}

		if _, exist := existFsid[diskStat.Fsid]; exist {
			return nil, fmt.Errorf(
				"the disk %v is the same"+
					"file system with %v, fsid %v",
				disk.Path, existFsid[diskStat.Fsid],
				diskStat.Fsid)
		}

		existFsid[diskStat.Fsid] = disk.Path

		if disk.StorageReserved < 0 || disk.StorageReserved > diskStat.StorageMaximum {
			return nil, fmt.Errorf("the storageReserved setting of disk %v is not valid, should be positive and no more than storageMaximum and storageAvailable", disk.Path)
		}
		tags, err := util.ValidateTags(disk.Tags)
		if err != nil {
			return nil, err
		}
		disk.Tags = tags
		_, exists := validDisks[disk.Name]
		if exists {
			return nil, fmt.Errorf("the disk name %v has duplicated", disk.Name)
		}
		validDisks[disk.Name] = disk.DiskSpec
	}

	return validDisks, nil
}

func GetNodeTagsFromAnnotation(annotation string) ([]string, error) {
	nodeTags, err := UnmarshalToNodeTags(annotation)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal the node tag annotation")
	}
	validNodeTags, err := util.ValidateTags(nodeTags)
	if err != nil {
		return nil, err
	}

	return validNodeTags, nil
}

type DiskSpecWithName struct {
	longhorn.DiskSpec
	Name string `json:"name"`
}

// UnmarshalToDisks input format should be:
// `[{"path":"/mnt/disk1","allowScheduling":false},
//
//	{"path":"/mnt/disk2","allowScheduling":false,"storageReserved":1024,"tags":["ssd","fast"]}]`
func UnmarshalToDisks(s string) (ret []DiskSpecWithName, err error) {
	if err := json.Unmarshal([]byte(s), &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// UnmarshalToNodeTags input format should be:
// `["worker1","enabled"]`
func UnmarshalToNodeTags(s string) ([]string, error) {
	var res []string
	if err := json.Unmarshal([]byte(s), &res); err != nil {
		return nil, err
	}
	return res, nil
}

func CreateDefaultDisk(dataPath string, storageReservedPercentage int64) (map[string]longhorn.DiskSpec, error) {
	if err := util.CreateDiskPathReplicaSubdirectory(dataPath); err != nil {
		return nil, err
	}
	diskStat, err := util.GetDiskStat(dataPath)
	if err != nil {
		return nil, err
	}
	return map[string]longhorn.DiskSpec{
		DefaultDiskPrefix + diskStat.Fsid: {
			Type:              longhorn.DiskTypeFilesystem,
			Path:              diskStat.Path,
			AllowScheduling:   true,
			EvictionRequested: false,
			StorageReserved:   diskStat.StorageMaximum * storageReservedPercentage / 100,
			Tags:              []string{},
		},
	}, nil
}

func ValidateCPUReservationValues(engineManagerCPUStr, replicaManagerCPUStr, instanceManagerCPUStr string) error {
	engineManagerCPU, err := strconv.Atoi(engineManagerCPUStr)
	if err != nil {
		return fmt.Errorf("guaranteed/requested engine manager CPU value %v is not int: %v", engineManagerCPUStr, err)
	}
	replicaManagerCPU, err := strconv.Atoi(replicaManagerCPUStr)
	if err != nil {
		return fmt.Errorf("guaranteed/requested replica manager CPU value %v is not int: %v", replicaManagerCPUStr, err)
	}
	instanceManagerCPU, err := strconv.Atoi(instanceManagerCPUStr)
	if err != nil {
		return fmt.Errorf("guaranteed/requested instance manager CPU value %v is not int: %v", replicaManagerCPUStr, err)
	}
	isUnderLimit := engineManagerCPU+replicaManagerCPU < 0 || instanceManagerCPU < 0
	isOverLimit := engineManagerCPU+replicaManagerCPU > 40 || instanceManagerCPU > 40
	if isUnderLimit || isOverLimit {
		return fmt.Errorf("invalid requested instance manager CPUs. Valid instance manager CPU range between 0%% - 40%%")
	}
	return nil
}

type CniNetwork struct {
	Name string   `json:"name"`
	IPs  []string `json:"ips,omitempty"`
}

func CreateCniAnnotationFromSetting(storageNetwork *longhorn.Setting) string {
	if storageNetwork.Value == "" {
		return ""
	}

	storageNetworkSplit := strings.Split(storageNetwork.Value, "/")
	return fmt.Sprintf("[{\"namespace\": \"%s\", \"name\": \"%s\", \"interface\": \"%s\"}]", storageNetworkSplit[0], storageNetworkSplit[1], StorageNetworkInterface)
}

func BackupStoreRequireCredential(backupType string) bool {
	return backupType == BackupStoreTypeS3 || backupType == BackupStoreTypeCIFS
}

func ConsolidateInstances(typeInstances ...map[string]longhorn.InstanceProcess) map[string]longhorn.InstanceProcess {
	consolidated := make(map[string]longhorn.InstanceProcess)
	for _, instances := range typeInstances {
		for name, instance := range instances {
			consolidated[name] = instance
		}
	}
	return consolidated
}

func ConsolidateInstanceManagers(typeInstanceManagers ...map[string]*longhorn.InstanceManager) map[string]*longhorn.InstanceManager {
	consolidated := make(map[string]*longhorn.InstanceManager)
	for _, instanceManagers := range typeInstanceManagers {
		for name, instanceManager := range instanceManagers {
			consolidated[name] = instanceManager
		}
	}
	return consolidated
}
