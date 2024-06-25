/*
Copyright 2019, 2020 the Velero contributors.

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

package backup

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/catalogic"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
)

// PVCBackupItemAction is a backup item action plugin for Velero.
type PVCBackupItemAction struct {
	Log logrus.FieldLogger
}

// liveCopyDrivers is a list of drivers for which we will skip creating the snapshot and will copy data live
// Must match liveCopyDrivers in amdslib/utils/utils.go
var liveCopyDrivers = []string{"nfs.csi.k8s.io", "efs.csi.aws.com", "driver.longhorn.io"}

// AppliesTo returns information indicating that the PVCBackupItemAction should be invoked to backup PVCs.
func (p *PVCBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("PVCBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
	}, nil
}

// Execute recognizes PVCs backed by volumes provisioned by CSI drivers with volumesnapshotting capability and creates snapshots of the
// underlying PVs by creating volumesnapshot CSI API objects that will trigger the CSI driver to perform the snapshot operation on the volume.
func (p *PVCBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	p.Log.Info("Starting PVCBackupItemAction")
	var err error
	// Do nothing if volume snapshots have not been requested in this backup
	if boolptr.IsSetToFalse(backup.Spec.SnapshotVolumes) {
		p.Log.Infof("Volume snapshotting not requested for backup %s/%s", backup.Namespace, backup.Name)
		return item, nil, nil
	}

	var pvc corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pvc); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	client, snapshotClient, err := util.GetClients()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	p.Log.Debugf("Fetching underlying PV for PVC %s", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
	// Do nothing if this is not a CSI provisioned volume
	pv, err := util.GetPVForPVC(&pvc, client.CoreV1())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if pv.Spec.PersistentVolumeSource.CSI == nil {
		p.Log.Infof("Skipping PVC %s/%s, associated PV %s is not a CSI volume", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, nil
	}

	// Do nothing if FS uploader is used to backup this PV
	isFSUploaderUsed, err := util.IsPVCDefaultToFSBackup(pvc.Namespace, pvc.Name, client.CoreV1(), boolptr.IsSetToTrue(backup.Spec.DefaultVolumesToFsBackup))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if isFSUploaderUsed {
		p.Log.Infof("Skipping  PVC %s/%s, PV %s will be backed up using FS uploader", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, nil
	}

	// no storage class: we don't know how to map to a VolumeSnapshotClass
	if pvc.Spec.StorageClassName == nil {
		return item, nil, errors.Errorf("Cannot snapshot PVC %s/%s, PVC has no storage class.", pvc.Namespace, pvc.Name)
	}

	p.Log.Infof("Fetching storage class for PV %s", *pvc.Spec.StorageClassName)
	storageClass, err := client.StorageV1().StorageClasses().Get(context.TODO(), *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {

		message := fmt.Sprintf("Storage Class %s not found", *pvc.Spec.StorageClassName)
		newErr := errors.Wrapf(err, message)
		p.Log.Error(newErr.Error())
		uErr := catalogic.UpdateSnapshotProgress(
			&pvc,
			nil,
			nil,
			"error",
			message,
			backup.Name,
			p.Log,
		)
		if uErr != nil {
			p.Log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
		}
		return nil, nil, errors.Wrap(err, "error getting storage class")
	}

	config, err := catalogic.GetPluginConfig(p.Log)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting plugin config")
	}

	for _, driver := range liveCopyDrivers {
		if config.SnapshotLonghorn && (driver == "driver.longhorn.io") {
			continue
		}
		if storageClass.Provisioner == driver {
			p.Log.Infof("Skipping PVC %s/%s, associated PV %s with provisioner %s is not supported",
				pvc.Namespace, pvc.Name, pv.Name, storageClass.Provisioner)
			return item, nil, nil
		}
	}

	if backupMethod, found := config.StorageClassBackupMethodMap[*pvc.Spec.StorageClassName]; found {
		if strings.HasPrefix(backupMethod, "LIVE") {
			if *pvc.Spec.VolumeMode != corev1api.PersistentVolumeBlock {
				p.Log.Infof("Skipping PVC %s/%s with storage class %s and backup method %s",
					pvc.Namespace, pvc.Name, *pvc.Spec.StorageClassName, backupMethod)
				return item, nil, nil
			} else {
				p.Log.Infof("Ignoring PVC %s/%s backup method %s because it is a block volume",
					pvc.Namespace, pvc.Name, backupMethod)
			}
		}
	}

	p.Log.Debugf("Fetching volumesnapshot class for %s", storageClass.Provisioner)
	snapshotClass, err := util.GetVolumeSnapshotClassForStorageClass(storageClass.Provisioner, snapshotClient.SnapshotV1())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get volumesnapshotclass for storageclass %s", storageClass.Name)
	}
	p.Log.Infof("volumesnapshot class=%s", snapshotClass.Name)
	// If deletetionPolicy is not Retain, then in the event of a disaster, the namespace is lost with the volumesnapshot object in it,
	// the underlying volumesnapshotcontent and the volume snapshot in the storage provider is also deleted.
	// In such a scenario, the backup objects will be useless as the snapshot handle itself will not be valid.
	if snapshotClass.DeletionPolicy != snapshotv1api.VolumeSnapshotContentRetain {
		p.Log.Warnf("DeletionPolicy on VolumeSnapshotClass %s is not %s; Deletion of VolumeSnapshot objects will lead to deletion of snapshot in the storage provider.",
			snapshotClass.Name, snapshotv1api.VolumeSnapshotContentRetain)
	}
	// Craft the snapshot object to be created
	snapshot := snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "cc-" + pvc.Name + "-",
			Namespace:    pvc.Namespace,
			Labels: map[string]string{
				velerov1api.BackupNameLabel: label.GetValidName(backup.Name),
			},
		},
		Spec: snapshotv1api.VolumeSnapshotSpec{
			Source: snapshotv1api.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc.Name,
			},
			VolumeSnapshotClassName: &snapshotClass.Name,
		},
	}

	var upd *snapshotv1api.VolumeSnapshot

	// catalogic variables
	var snapshotState string
	var snapshotStateMessage string
	defer func(err error) {
		uErr := catalogic.UpdateSnapshotProgress(&pvc, upd, nil, snapshotState, snapshotStateMessage, backup.Name, p.Log)
		if uErr != nil {
			p.Log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
		}
	}(err)

	upd, err = snapshotClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Create(context.TODO(), &snapshot, metav1.CreateOptions{})
	if err != nil {
		snapshotStateMessage = "error creating volume snapshot"
		snapshotState = "error"
		newErr := errors.Wrapf(err, snapshotStateMessage)
		p.Log.Error(newErr.Error())
		return nil, nil, errors.Wrapf(err, "error creating volume snapshot")
	}
	p.Log.Infof("Created volumesnapshot %s", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))

	labels := map[string]string{
		util.VolumeSnapshotLabel:        upd.Name,
		velerov1api.BackupNameLabel:     backup.Name,
		"cloudcasa-initial-volume-name": pvc.Spec.VolumeName,
	}

	if storageClass != nil {
		labels["cloudcasa-storage-class-provisioner"] = storageClass.Provisioner
	}

	if storageClass.Provisioner == "file.csi.azure.com" {
		catalogic.SetStaticAzureAnotation(&pvc, labels, storageClass, p.Log)
	}

	annotations := labels
	annotations[util.MustIncludeAdditionalItemAnnotation] = "true"

	util.AddAnnotations(&pvc.ObjectMeta, annotations)
	util.AddLabels(&pvc.ObjectMeta, labels)

	additionalItems := []velero.ResourceIdentifier{
		{
			GroupResource: kuberesource.VolumeSnapshots,
			Namespace:     upd.Namespace,
			Name:          upd.Name,
		},
	}

	p.Log.Infof("Returning from PVCBackupItemAction with %d additionalItems to backup", len(additionalItems))
	for _, ai := range additionalItems {
		p.Log.Debugf("%s: %s", ai.GroupResource.String(), ai.Name)
	}
	snapshotStateMessage = fmt.Sprintf("Waiting for CSI driver to reconcile volumesnapshot %s/%s", pvc.Namespace, pvc.Name)
	snapshotState = "pending"

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: pvcMap}, additionalItems, nil
}
