/*
Copyright 2020 the Velero contributors.

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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/restic"
	corev1api "k8s.io/api/core/v1"
	kerror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

type PvcInfo struct {
	Namespace              string `json:"namespace,omitempty"`
	DataTransferSnapshotId string `json:"data_transfer_snapshot_id,omitempty"`
	SnapshotId             string `json:"snapshot_id,omitempty"`
	VolumeId               string `json:"volume_id,omitempty"`
	Name                   string `json:"name,omitempty"`
	PvName                 string `json:"pv_name,omitempty"`
	SnapshotType           string `json:"snapshot_type,omitempty"`
}
type PvcSnapshotProgressData struct {
	JobId            string   `json:"job_id,omitempty"`
	Message          string   `json:"message,omitempty"`
	State            string   `json:"state,omitempty"`
	SnapshotProgress int32    `json:"snapshot_progress,omitempty"`
	Pvc              *PvcInfo `json:"pvc,omitempty"`
}

const (
	//TODO: use annotation from velero https://github.com/vmware-tanzu/velero/pull/2283
	resticPodAnnotation = "backup.velero.io/backup-volumes"

	CloudCasaNamespace = "cloudcasa-io"

	// Name of configmap used to to report progress of snapshot
	SnapshotProgressUpdateConfigMapName = "cloudcasa-io-snapshot-updater"

	TimeFormat = "2006-01-06 15:04:05 UTC: "
)

func GetPVForPVC(pvc *corev1api.PersistentVolumeClaim, corev1 corev1client.PersistentVolumesGetter) (*corev1api.PersistentVolume, error) {
	if pvc.Spec.VolumeName == "" {
		return nil, errors.Errorf("PVC %s/%s has no volume backing this claim", pvc.Namespace, pvc.Name)
	}
	if pvc.Status.Phase != corev1api.ClaimBound {
		// TODO: confirm if this PVC should be snapshotted if it has no PV bound
		return nil, errors.Errorf("PVC %s/%s is in phase %v and is not bound to a volume", pvc.Namespace, pvc.Name, pvc.Status.Phase)
	}
	pvName := pvc.Spec.VolumeName
	pv, err := corev1.PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get PV %s for PVC %s/%s", pvName, pvc.Namespace, pvc.Name)
	}
	return pv, nil
}

func GetPodsUsingPVC(pvcNamespace, pvcName string, corev1 corev1client.PodsGetter) ([]corev1api.Pod, error) {
	podsUsingPVC := []corev1api.Pod{}
	podList, err := corev1.Pods(pvcNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, p := range podList.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				podsUsingPVC = append(podsUsingPVC, p)
			}
		}
	}

	return podsUsingPVC, nil
}

func GetPodVolumeNameForPVC(pod corev1api.Pod, pvcName string) (string, error) {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
			return v.Name, nil
		}
	}
	return "", errors.Errorf("Pod %s/%s does not use PVC %s/%s", pod.Namespace, pod.Name, pod.Namespace, pvcName)
}

func Contains(slice []string, key string) bool {
	for _, i := range slice {
		if i == key {
			return true
		}
	}
	return false
}

func IsPVCBackedUpByRestic(pvcNamespace, pvcName string, podClient corev1client.PodsGetter, defaultVolumesToRestic bool) (bool, error) {
	pods, err := GetPodsUsingPVC(pvcNamespace, pvcName, podClient)
	if err != nil {
		return false, errors.WithStack(err)
	}

	for _, p := range pods {
		resticVols := restic.GetPodVolumesUsingRestic(&p, defaultVolumesToRestic)
		if len(resticVols) > 0 {
			volName, err := GetPodVolumeNameForPVC(p, pvcName)
			if err != nil {
				return false, err
			}
			if Contains(resticVols, volName) {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetVolumeSnapshotClassForStorageClass returns a VolumeSnapshotClass for the supplied volume provisioner/ driver name.
func GetVolumeSnapshotClassForStorageClass(provisioner string, snapshotClient snapshotter.SnapshotV1Interface) (*snapshotv1api.VolumeSnapshotClass, error) {
	snapshotClasses, err := snapshotClient.VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error listing volumesnapshot classes")
	}
	// We pick the volumesnapshotclass that matches the CSI driver name and has a 'velero.io/csi-volumesnapshot-class'
	// label. This allows multiple VolumesnapshotClasses for the same driver with different values for the
	// other fields in the spec.
	// https://github.com/kubernetes-csi/external-snapshotter/blob/release-4.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
	for _, sc := range snapshotClasses.Items {
		_, hasCloudcasaLabelSelector := sc.Labels[CloudcasaVolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasCloudcasaLabelSelector {
			return &sc, nil
		}
	}
	for _, sc := range snapshotClasses.Items {
		_, hasLabelSelector := sc.Labels[VolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasLabelSelector {
			return &sc, nil
		}
	}
	return nil, errors.Errorf("failed to get volumesnapshotclass for provisioner %s, ensure that the desired volumesnapshot class has the %s label", provisioner, VolumeSnapshotClassSelectorLabel)
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the volumesnapshotcontent object associated with the volumesnapshot
func GetVolumeSnapshotContentForVolumeSnapshot(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger, shouldWait bool) (*snapshotv1api.VolumeSnapshotContent, error) {
	if !shouldWait {
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *volSnap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "error getting volume snapshot content from API")
		}
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling every 5s
	// TODO: make this timeout configurable.
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	var snapshotContent *snapshotv1api.VolumeSnapshotContent

	defer DeleteSnapshotProgressConfigMap(log)
	jobID := volSnap.Labels["velero.io/backup-name"]
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name))
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			message := fmt.Sprintf("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volSnap.Namespace, volSnap.Name, interval/time.Second)
			log.Infof(message)
			uErr := UpdateSnapshotProgress(
				nil,
				volSnap,
				nil,
				"pending",
				message,
				jobID,
				log,
			)
			if uErr != nil {
				log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
			}
			return false, nil
		}

		// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			err = nil
			snapshotContent, err = snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
			}
			if snapshotContent.Annotations == nil {
				snapshotContent.Annotations = make(map[string]string)
			}
			// Check here if the annotations contain the PVC name and namespace

			var vscAnnotationsNeedsToBeUpdated bool

			if _, ok := snapshotContent.GetAnnotations()["cc-pvc-name"]; !ok {
				if vs.Spec.Source.PersistentVolumeClaimName != nil {
					snapshotContent.GetAnnotations()["cc-pvc-name"] = *vs.Spec.Source.PersistentVolumeClaimName
					vscAnnotationsNeedsToBeUpdated = true
				}
			}
			if _, ok := snapshotContent.GetAnnotations()["cc-pvc-namespace"]; !ok {
				snapshotContent.GetAnnotations()["cc-pvc-namespace"] = vs.GetNamespace()
				vscAnnotationsNeedsToBeUpdated = true
			}
			if vscAnnotationsNeedsToBeUpdated {
				err = nil
				snapshotContent, err = snapshotClient.VolumeSnapshotContents().Update(context.TODO(), snapshotContent, metav1.UpdateOptions{})
				if err != nil {
					log.Infof("Failed to update VolumeSnapshotContent %s, Error is %v . Will backoff and try again...", *vs.Status.BoundVolumeSnapshotContentName, err)
					return err
				}
				log.Infof("VolumeSnapshotContent %s successfully updated with PVC details", *vs.Status.BoundVolumeSnapshotContentName)
			}
			return nil
		})
		if retryErr != nil {
			log.Errorf("Failed to update VolumeSnapshotContent %s with pvc details in annotations. Error is %v", *vs.Status.BoundVolumeSnapshotContentName, retryErr)
			return false, errors.WithStack(retryErr)
		}

		// If its not present, then, update the volumesnapshotcontent object with this information

		// we need to wait for the VolumeSnaphotContent to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if snapshotContent.Status == nil || snapshotContent.Status.SnapshotHandle == nil {
			message := fmt.Sprintf("Waiting for volumesnapshotcontents %s to have snapshot handle. Retrying in %ds", snapshotContent.Name, interval/time.Second)
			log.Infof(message)
			uErr := UpdateSnapshotProgress(
				nil,
				volSnap,
				snapshotContent,
				"pending",
				message,
				jobID,
				log,
			)
			if uErr != nil {
				log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
			}
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			message := fmt.Sprintf("Timed out awaiting reconciliation of volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
			log.Error(message)
			uErr := UpdateSnapshotProgress(
				nil,
				volSnap,
				nil,
				"error",
				message,
				jobID,
				log,
			)
			if uErr != nil {
				log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
			}
		}
		return nil, err
	}

	var snapshotState string
	var snapshotStateMessage string
	if snapshotContent != nil && snapshotContent.Status != nil && snapshotContent.Status.ReadyToUse != nil && *snapshotContent.Status.ReadyToUse {
		snapshotState = "completed"
		snapshotStateMessage = "CSI Snapshot Complete"
	} else {
		snapshotState = "pending"
		snapshotStateMessage = fmt.Sprintf("Waiting for volume snapshot %s to be ready to use.", *snapshotContent.Status.SnapshotHandle)
	}
	log.Infof(snapshotStateMessage)
	uErr := UpdateSnapshotProgress(
		nil,
		volSnap,
		nil,
		snapshotState,
		snapshotStateMessage,
		jobID,
		log,
	)
	if uErr != nil {
		log.Error(err, "<SNAPSHOT PROGRESS UPDATE> Failed to update snapshot progress. Continuing...")
	}
	return snapshotContent, nil
}

func GetClients() (*kubernetes.Clientset, *snapshotterClientSet.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	snapshotterClient, err := snapshotterClientSet.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return client, snapshotterClient, nil
}

// IsVolumeSnapshotClassHasListerSecret returns whether a volumesnapshotclass has a snapshotlister secret
func IsVolumeSnapshotClassHasListerSecret(vc *snapshotv1api.VolumeSnapshotClass) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L59-L60
	// There is no release w/ these constants exported. Using the strings for now.
	_, nameExists := vc.Annotations[PrefixedSnapshotterListSecretNameKey]
	_, nsExists := vc.Annotations[PrefixedSnapshotterListSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotContentHasDeleteSecret returns whether a volumesnapshotcontent has a deletesnapshot secret
func IsVolumeSnapshotContentHasDeleteSecret(vsc *snapshotv1api.VolumeSnapshotContent) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L56-L57
	// use exported constants in the next release
	_, nameExists := vsc.Annotations[PrefixedSnapshotterSecretNameKey]
	_, nsExists := vsc.Annotations[PrefixedSnapshotterSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotHasVSCDeleteSecret returns whether a volumesnapshot should set the deletesnapshot secret
// for the static volumesnapshotcontent that is created on restore
func IsVolumeSnapshotHasVSCDeleteSecret(vs *snapshotv1api.VolumeSnapshot) bool {
	_, nameExists := vs.Annotations[CSIDeleteSnapshotSecretName]
	_, nsExists := vs.Annotations[CSIDeleteSnapshotSecretNamespace]
	return nameExists && nsExists
}

// AddAnnotations adds the supplied key-values to the annotations on the object
func AddAnnotations(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Annotations == nil {
		o.Annotations = make(map[string]string)
	}
	for k, v := range vals {
		o.Annotations[k] = v
	}
}

// AddLabels adds the supplied key-values to the labels on the object
func AddLabels(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Labels == nil {
		o.Labels = make(map[string]string)
	}
	for k, v := range vals {
		o.Labels[k] = label.GetValidName(v)
	}
}

// IsVolumeSnapshotExists returns whether a specific volumesnapshot object exists.
func IsVolumeSnapshotExists(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface) bool {
	exists := false
	if volSnap != nil {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err == nil && vs != nil {
			exists = true
		}
	}

	return exists
}

func SetVolumeSnapshotContentDeletionPolicy(vscName string, csiClient snapshotter.SnapshotV1Interface) error {
	pb := []byte(`{"spec":{"deletionPolicy":"Delete"}}`)
	_, err := csiClient.VolumeSnapshotContents().Patch(context.TODO(), vscName, types.MergePatchType, pb, metav1.PatchOptions{})

	return err
}

func HasBackupLabel(o *metav1.ObjectMeta, backupName string) bool {
	if o.Labels == nil || len(strings.TrimSpace(backupName)) == 0 {
		return false
	}
	return o.Labels[velerov1api.BackupNameLabel] == label.GetValidName(backupName)
}

// UpdateSnapshotProgress updates the configmap in order to relay the
// snapshot progress to KubeAgent
func UpdateSnapshotProgress(
	pvc *corev1api.PersistentVolumeClaim,
	vs *snapshotv1api.VolumeSnapshot,
	vsc *snapshotv1api.VolumeSnapshotContent,
	state string,
	stateMessage string,
	jobID string,
	log logrus.FieldLogger,
) error {
	log.Info("Update Snapshot Progress - Starting to relay snapshot progress to KubAgent")
	var err error
	// Fill in the PVC realted information
	var pvcProgressObject = PvcInfo{}
	if pvc != nil {
		pvcProgressObject.Name = pvc.GetName()
		pvcProgressObject.Namespace = pvc.GetNamespace()
		pvcProgressObject.PvName = pvc.Spec.VolumeName
	} else if vs != nil {
		if vs.Spec.Source.PersistentVolumeClaimName != nil {
			pvcProgressObject.Name = *vs.Spec.Source.PersistentVolumeClaimName
		}
		pvcProgressObject.Namespace = vs.GetNamespace()
	}
	if vsc != nil {
		if vsc.Spec.Source.VolumeHandle != nil {
			pvcProgressObject.VolumeId = *vsc.Spec.Source.VolumeHandle
		}
		if vsc.Spec.Source.SnapshotHandle != nil {
			pvcProgressObject.SnapshotId = *vsc.Spec.Source.SnapshotHandle
		}
	}
	pvcProgressObject.SnapshotType = "CSI"
	// Fill in the snapshot progress related information
	var progress = PvcSnapshotProgressData{}
	currentTimeString := time.Now().UTC().Format(TimeFormat)
	progress.State = state
	progress.Message = currentTimeString + " " + stateMessage
	progress.JobId = jobID
	progress.Pvc = &pvcProgressObject
	log.Info("Update Snapshot Progress -", "Progress Payload", progress)
	// Prepare the paylod to be embedded into the configmap
	requestData := make(map[string][]byte)
	if requestData["snapshot_progress_payload"], err = json.Marshal(progress); err != nil {
		newErr := errors.Wrap(err, "Failed to marshal progress while creating the snapshot progress configmap")
		log.Error(newErr, "JSON marshalling failed")
		return newErr
	}
	log.Info("Update Snapshot Progress -", "Marsahlled the JSON payload")
	// create the configmap object.
	moverConfigMap := corev1api.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      SnapshotProgressUpdateConfigMapName,
			Namespace: CloudCasaNamespace,
		},
		BinaryData: requestData,
	}
	log.Info("Update Snapshot Progress -", "Created the configmap object")
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create in-cluster config")
		log.Error(newErr, "Failed to create in-cluster config")
		return newErr

	}
	log.Info("Update Snapshot Progress -", "Created in-cluster config")
	// Create clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create clientset")
		log.Error(newErr, "Failed to create clientset")
		return newErr
	}
	log.Info("Update Snapshot Progress -", "Created clientset")
	//Create or update the configmap
	var mcm *corev1api.ConfigMap
	if _, mErr := clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Get(context.TODO(), SnapshotProgressUpdateConfigMapName, metav1.GetOptions{}); kerror.IsNotFound(mErr) {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Create(context.TODO(), &moverConfigMap, metav1.CreateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to create configmap to report snapshotprogress")
			log.Error(newErr, "Failed to create configmap")
			return newErr

		}
		log.Info("Created configmap to report snapshot progress", "Configmap Name", mcm.GetName())
	} else {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Update(context.TODO(), &moverConfigMap, metav1.UpdateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to update configmap to report snapshotprogress")
			log.Error(newErr, "Failed to update configmap")
			return newErr
		}
		log.Info("Updated configmap to report snapshot progress", "Configmap Name", mcm.GetName())
	}
	log.Info("finished relaying snapshot progress to KubeAgent")
	return nil
}

// DeleteSnapshotProgressConfigMap deletes the configmap used to report snapshot progress
func DeleteSnapshotProgressConfigMap(log logrus.FieldLogger) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster config"))
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster clientset"))
	}
	err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Delete(context.TODO(), SnapshotProgressUpdateConfigMapName, metav1.DeleteOptions{})
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to delete configmap used to report snapshot progress"))
	} else {
		log.Info("Deleted configmap used to report snapshot progress", "Configmap Name", SnapshotProgressUpdateConfigMapName)
	}
}
