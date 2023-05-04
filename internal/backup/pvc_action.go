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

	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// StaticPvcAnnotation should be set in the user wants to backup
	// a staticlly provisioned Azure File Share using CSI.
	StaticAzureFilePvcAnnotation                = "cloudcasa-azure-file-share-name"
	StaticAzureFilePvcSecretNameAnnotation      = "cloudcasa-azure-file-share-secret-name"
	StaticAzureFilePvcSecretNamespaceAnnotation = "cloudcasa-azure-file-share-secret-namespace"
	// StaticSecretAnnotation should be set for secret that has credentials
	// to Azure Storage Account in which File Share that needs to be backed up
	// is located
	StaticAzureFileSecretAnnotation = "cloudcasa-azure-storage-account-credentials"
)

// PVCBackupItemAction is a backup item action plugin for Velero.
type PVCBackupItemAction struct {
	Log logrus.FieldLogger
}

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

	// Do nothing if restic is used to backup this PV
	isResticUsed, err := util.IsPVCBackedUpByRestic(pvc.Namespace, pvc.Name, client.CoreV1(), boolptr.IsSetToTrue(backup.Spec.DefaultVolumesToRestic))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if isResticUsed {
		p.Log.Infof("Skipping  PVC %s/%s, PV %s will be backed up using restic", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, nil
	}

	// no storage class: we don't know how to map to a VolumeSnapshotClass
	if pvc.Spec.StorageClassName == nil {
		return item, nil, errors.Errorf("Cannot snapshot PVC %s/%s, PVC has no storage class.", pvc.Namespace, pvc.Name)
	}

	p.Log.Infof("Fetching storage class for PV %s", *pvc.Spec.StorageClassName)
	storageClass, err := client.StorageV1().StorageClasses().Get(context.TODO(), *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting storage class")
	}

	if storageClass.Provisioner == "nfs.csi.k8s.io" || storageClass.Provisioner == "efs.csi.aws.com" {
		p.Log.Infof("Skipping PVC %s/%s, associated PV %s with provisioner %s is not supported", pvc.Namespace, pvc.Name, pv.Name, storageClass.Provisioner)
		return item, nil, nil
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

	upd, err := snapshotClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Create(context.TODO(), &snapshot, metav1.CreateOptions{})
	if err != nil {
		message := "error creating volume snapshot"
		newErr := errors.Wrapf(err, message)
		p.Log.Error(newErr.Error())
		uErr := util.UpdateSnapshotProgress(
			&pvc,
			upd,
			nil,
			"error",
			message,
			backup.Name,
			p.Log,
		)
		if uErr != nil {
			log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
		}

		return nil, nil, errors.Wrapf(err, "error creating volume snapshot")
	}
	p.Log.Infof("Created volumesnapshot %s", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))
	uErr := util.UpdateSnapshotProgress(
		&pvc,
		upd,
		nil,
		"pending",
		fmt.Sprintf("Waiting for CSI driver to reconcile volumesnapshot %s/%s", pvc.Namespace, pvc.Name),
		backup.Name,
		p.Log,
	)
	if uErr != nil {
		log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
	}
	vals := map[string]string{
		util.VolumeSnapshotLabel:        upd.Name,
		velerov1api.BackupNameLabel:     backup.Name,
		"cloudcasa-initial-volume-name": pvc.Spec.VolumeName,
	}
	if storageClass != nil {
		vals["cloudcasa-storage-class-provisioner"] = storageClass.Provisioner
	}

	// For Azure File PVs, we need to check if the volume has been provisioned statically.
	// If so, we need to store file share name, secret name, and secret namespace as annotation.
	// Then, during restore, our Azure Files Mover will pick these values to restore from a
	// statically provisioned volume.
	if storageClass.Provisioner == "file.csi.azure.com" {
		p.Log.Infof("Found %s PVC %s/%s. Getting %s PV details", storageClass.Provisioner, pvc.Namespace, pvc.Name, pvc.Spec.VolumeName)
		config, err := rest.InClusterConfig()
		if err != nil {
			p.Log.Errorf("Failed to get in-cluster Kubernetes config: %w", err)
			return nil, nil, errors.WithStack(err)
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			p.Log.Errorf("Failed to get Kubernetes clientset for in-cluster config: %w", err)
			return nil, nil, errors.WithStack(err)
		}
		pv, err := clientset.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, v1.GetOptions{})
		if err != nil {
			p.Log.Errorf("Failed to get PV %s for PVC %s/%s: %w", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
			return nil, nil, errors.WithStack(err)
		}
		if pv.Spec.CSI != nil {
			// Check if the PV was provisioned dynamically or statically
			if _, exists := pv.Spec.CSI.VolumeAttributes["csi.storage.k8s.io/pv/name"]; exists {
				// The PV is provisioned dynamically. No need to read any attributes
				p.Log.Infof("Dynamically provisioned %s PV %s bounded with PVC %s/%s found", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name)
			} else {
				p.Log.Infof("Statically provisioned %s PV %s bounded with PVC %s/%s found", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name)
				volumeAttributes := pv.Spec.CSI.VolumeAttributes
				nodeStageSecretRef := pv.Spec.CSI.NodeStageSecretRef
				shareName, exists := volumeAttributes["shareName"]
				if !exists {
					err = fmt.Errorf("PV %s bound to PVC %s/%s does not have File Share name set", pv.Name, pvc.Namespace, pvc.Name)
					p.Log.Error(err)
					return nil, nil, errors.WithStack(err)
				}
				if nodeStageSecretRef == nil {
					err = fmt.Errorf("PV %s bound to PVC %s/%s does not have secret details set", pv.Name, pvc.Namespace, pvc.Name)
					p.Log.Error(err)
					return nil, nil, errors.WithStack(err)
				}
				secretName := nodeStageSecretRef.Name
				secretNamespace := nodeStageSecretRef.Namespace
				if secretNamespace == "" {
					p.Log.Infof("Secret for PV %s bounded with PVC %s/%s is empty. Assuming PVC namespace as the secret namespace", pv.Name, pvc.Namespace, pvc.Name)
				}
				ccAzureFilesAnnotations := map[string]string{
					StaticAzureFilePvcAnnotation:                shareName,
					StaticAzureFilePvcSecretNameAnnotation:      secretName,
					StaticAzureFilePvcSecretNamespaceAnnotation: secretNamespace,
				}
				for k, v := range ccAzureFilesAnnotations {
					vals[k] = v
				}
				p.Log.Infof("Setting new annotations %v for PVC %s/%s", vals, pvc.Namespace, pvc.Name)
			}
		}
	}

	util.AddAnnotations(&pvc.ObjectMeta, vals)
	util.AddLabels(&pvc.ObjectMeta, vals)
	//(pvc,upd,nil, p.log)

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

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: pvcMap}, additionalItems, nil
}
