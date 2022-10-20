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

package backup

import (
	"context"
	"fmt"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// VolumeSnapshotContentBackupItemAction is a backup item action plugin to backup
// CSI VolumeSnapshotcontent objects using Velero
type VolumeSnapshotContentBackupItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the VolumeSnapshotContentBackupItemAction action should be invoked to backup volumesnapshotcontents.
func (p *VolumeSnapshotContentBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("VolumeSnapshotContentBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontent.snapshot.storage.k8s.io"},
	}, nil
}

// Execute returns the unmodified volumesnapshotcontent object along with the snapshot deletion secret, if any, from its annotation
// as additional items to backup.
func (p *VolumeSnapshotContentBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	p.Log.Infof("Executing VolumeSnapshotContentBackupItemAction")

	var snapCont snapshotv1api.VolumeSnapshotContent
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &snapCont); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	_, snapshotClient, err := util.GetClients()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	vs, err := snapshotClient.SnapshotV1().VolumeSnapshots(snapCont.Spec.VolumeSnapshotRef.Namespace).Get(context.TODO(), snapCont.Spec.VolumeSnapshotRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot from volumesnapshotcontent %s", snapCont.GetName()))
	}
	if vs == nil {
		return nil, nil, fmt.Errorf("nil value of VolumeSnapshot received")
	}
	vals := map[string]string{}

	// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vsc, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(context.TODO(), snapCont.Name, metav1.GetOptions{})
		if err != nil {
			return errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s", snapCont.Name))
		}
		if vsc.Annotations == nil {
			vsc.Annotations = make(map[string]string)
		}
		vals["cc-pvc-name"] = *vs.Spec.Source.PersistentVolumeClaimName
		vals["cc-pvc-namespace"] = vs.GetNamespace()
		util.AddAnnotations(&snapCont.ObjectMeta, vals)
		util.AddAnnotations(&vsc.ObjectMeta, vals)
		err = nil
		_, err = snapshotClient.SnapshotV1().VolumeSnapshotContents().Update(context.TODO(), vsc, metav1.UpdateOptions{})
		if err != nil {
			p.Log.Errorf("Failed to update VolumeSnapshotContent %s, Error is %v ", vsc.GetName(), err)
		}
		return err
	})
	if retryErr != nil {
		p.Log.Errorf("Failed to update VolumeSnapshotContent %s with pvc details in annotations. Error is %v", snapCont.Name, retryErr)
		return nil, nil, errors.WithStack(retryErr)
	}

	p.Log.Infof("VolumeSnapshotContent %s successfully updated with PVC details", snapCont.Name)
	additionalItems := []velero.ResourceIdentifier{}

	// we should backup the snapshot deletion secrets that may be referenced in the volumesnapshotcontent's annotation
	if util.IsVolumeSnapshotContentHasDeleteSecret(&snapCont) {
		// TODO: add GroupResource for secret into kuberesource
		additionalItems = append(additionalItems, velero.ResourceIdentifier{
			GroupResource: schema.GroupResource{Group: "", Resource: "secrets"},
			Name:          snapCont.Annotations[util.PrefixedSnapshotterSecretNameKey],
			Namespace:     snapCont.Annotations[util.PrefixedSnapshotterSecretNamespaceKey],
		})
	}

	p.Log.Infof("Returning from VolumeSnapshotContentBackupItemAction with %d additionalItems to backup", len(additionalItems))
	vscMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&snapCont)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return &unstructured.Unstructured{Object: vscMap}, additionalItems, nil
}
