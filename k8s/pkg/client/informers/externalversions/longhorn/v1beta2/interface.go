/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by informer-gen. DO NOT EDIT.

package v1beta2

import (
	internalinterfaces "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/internalinterfaces"
)

// Interface provides access to all the informers in this group version.
type Interface interface {
	// BackingImages returns a BackingImageInformer.
	BackingImages() BackingImageInformer
	// BackingImageDataSources returns a BackingImageDataSourceInformer.
	BackingImageDataSources() BackingImageDataSourceInformer
	// BackingImageManagers returns a BackingImageManagerInformer.
	BackingImageManagers() BackingImageManagerInformer
	// Backups returns a BackupInformer.
	Backups() BackupInformer
	// BackupTargets returns a BackupTargetInformer.
	BackupTargets() BackupTargetInformer
	// BackupVolumes returns a BackupVolumeInformer.
	BackupVolumes() BackupVolumeInformer
	// Engines returns a EngineInformer.
	Engines() EngineInformer
	// EngineImages returns a EngineImageInformer.
	EngineImages() EngineImageInformer
	// InstanceManagers returns a InstanceManagerInformer.
	InstanceManagers() InstanceManagerInformer
	// Nodes returns a NodeInformer.
	Nodes() NodeInformer
	// Orphans returns a OrphanInformer.
	Orphans() OrphanInformer
	// RecurringJobs returns a RecurringJobInformer.
	RecurringJobs() RecurringJobInformer
	// Replicas returns a ReplicaInformer.
	Replicas() ReplicaInformer
	// Settings returns a SettingInformer.
	Settings() SettingInformer
	// ShareManagers returns a ShareManagerInformer.
	ShareManagers() ShareManagerInformer
	// Snapshots returns a SnapshotInformer.
	Snapshots() SnapshotInformer
	// SupportBundles returns a SupportBundleInformer.
	SupportBundles() SupportBundleInformer
	// SystemBackups returns a SystemBackupInformer.
	SystemBackups() SystemBackupInformer
	// SystemRestores returns a SystemRestoreInformer.
	SystemRestores() SystemRestoreInformer
	// Upgrades returns a UpgradeInformer.
	Upgrades() UpgradeInformer
	// Volumes returns a VolumeInformer.
	Volumes() VolumeInformer
	// VolumeAttachments returns a VolumeAttachmentInformer.
	VolumeAttachments() VolumeAttachmentInformer
}

type version struct {
	factory          internalinterfaces.SharedInformerFactory
	namespace        string
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// New returns a new Interface.
func New(f internalinterfaces.SharedInformerFactory, namespace string, tweakListOptions internalinterfaces.TweakListOptionsFunc) Interface {
	return &version{factory: f, namespace: namespace, tweakListOptions: tweakListOptions}
}

// BackingImages returns a BackingImageInformer.
func (v *version) BackingImages() BackingImageInformer {
	return &backingImageInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// BackingImageDataSources returns a BackingImageDataSourceInformer.
func (v *version) BackingImageDataSources() BackingImageDataSourceInformer {
	return &backingImageDataSourceInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// BackingImageManagers returns a BackingImageManagerInformer.
func (v *version) BackingImageManagers() BackingImageManagerInformer {
	return &backingImageManagerInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Backups returns a BackupInformer.
func (v *version) Backups() BackupInformer {
	return &backupInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// BackupTargets returns a BackupTargetInformer.
func (v *version) BackupTargets() BackupTargetInformer {
	return &backupTargetInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// BackupVolumes returns a BackupVolumeInformer.
func (v *version) BackupVolumes() BackupVolumeInformer {
	return &backupVolumeInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Engines returns a EngineInformer.
func (v *version) Engines() EngineInformer {
	return &engineInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// EngineImages returns a EngineImageInformer.
func (v *version) EngineImages() EngineImageInformer {
	return &engineImageInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// InstanceManagers returns a InstanceManagerInformer.
func (v *version) InstanceManagers() InstanceManagerInformer {
	return &instanceManagerInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Nodes returns a NodeInformer.
func (v *version) Nodes() NodeInformer {
	return &nodeInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Orphans returns a OrphanInformer.
func (v *version) Orphans() OrphanInformer {
	return &orphanInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// RecurringJobs returns a RecurringJobInformer.
func (v *version) RecurringJobs() RecurringJobInformer {
	return &recurringJobInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Replicas returns a ReplicaInformer.
func (v *version) Replicas() ReplicaInformer {
	return &replicaInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Settings returns a SettingInformer.
func (v *version) Settings() SettingInformer {
	return &settingInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// ShareManagers returns a ShareManagerInformer.
func (v *version) ShareManagers() ShareManagerInformer {
	return &shareManagerInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Snapshots returns a SnapshotInformer.
func (v *version) Snapshots() SnapshotInformer {
	return &snapshotInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// SupportBundles returns a SupportBundleInformer.
func (v *version) SupportBundles() SupportBundleInformer {
	return &supportBundleInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// SystemBackups returns a SystemBackupInformer.
func (v *version) SystemBackups() SystemBackupInformer {
	return &systemBackupInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// SystemRestores returns a SystemRestoreInformer.
func (v *version) SystemRestores() SystemRestoreInformer {
	return &systemRestoreInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Upgrades returns a UpgradeInformer.
func (v *version) Upgrades() UpgradeInformer {
	return &upgradeInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Volumes returns a VolumeInformer.
func (v *version) Volumes() VolumeInformer {
	return &volumeInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// VolumeAttachments returns a VolumeAttachmentInformer.
func (v *version) VolumeAttachments() VolumeAttachmentInformer {
	return &volumeAttachmentInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}
