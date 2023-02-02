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

package restore

import (
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// VolumeSnapshotContentRestoreItemAction is a restore item action plugin for Velero
type VolumeSnapshotContentRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating VolumeSnapshotContentRestoreItemAction action should be invoked while restoring
// volumesnapshotcontent.snapshot.storage.k8s.io resources
func (p *VolumeSnapshotContentRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontent.snapshot.storage.k8s.io"},
	}, nil
}

// Execute restores a volumesnapshotcontent object without modification returning the snapshot lister secret, if any, as
// additional items to restore.
func (p *VolumeSnapshotContentRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.Log.Info("Starting VolumeSnapshotContentRestoreItemAction")
	if input.Restore.Spec.RestorePVs != nil && *input.Restore.Spec.RestorePVs == false {
		p.Log.Info("Returning from VolumeSnapshotContentRestoreItemAction as restorePVs flag is set to false")
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}
	var snapCont snapshotv1api.VolumeSnapshotContent

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &snapCont); err != nil {
		return &velero.RestoreItemActionExecuteOutput{}, errors.Wrapf(err, "failed to convert input.Item from unstructured")
	}

	var pvcNamespace string
	var pvcName string

	if snapCont.GetAnnotations() != nil {
		if value, found := snapCont.GetAnnotations()["cc-pvc-namespace"]; found {
			pvcNamespace = value
		} else {
			p.Log.Infof("VolumeSnapshotContent %s does not have cc-pvc-namespace annotation", snapCont.Name)
		}

		if value, found := snapCont.GetAnnotations()["cc-pvc-name"]; found {
			pvcName = value
		} else {
			p.Log.Infof("VolumeSnapshotContent %s does not have cc-pvc-name annotation", snapCont.Name)
		}
	}

	if pvcNamespace != "" && pvcName != "" {
		restoreFromCopy, err := util.IsPVCRestoreFromCopy(p.Log, pvcNamespace, pvcName)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to check if PVC %s/%s is being restored from a copy", pvcNamespace, pvcName)
		}

		if restoreFromCopy {
			p.Log.Infof("PVC %s/%s is being restored from a copy, skip restoring VolumeSnapshotContent %s",
				pvcNamespace, pvcName, snapCont.Name)

			return &velero.RestoreItemActionExecuteOutput{
				SkipRestore: true,
			}, nil
		}
	}

	additionalItems := []velero.ResourceIdentifier{}
	if util.IsVolumeSnapshotContentHasDeleteSecret(&snapCont) {
		additionalItems = append(additionalItems,
			velero.ResourceIdentifier{
				GroupResource: schema.GroupResource{Group: "", Resource: "secrets"},
				Name:          snapCont.Annotations[util.CSIDeleteSnapshotSecretName],
				Namespace:     snapCont.Annotations[util.CSIDeleteSnapshotSecretNamespace],
			},
		)
	}

	p.Log.Infof("Returning from VolumeSnapshotContentRestoreItemAction with %d additionalItems", len(additionalItems))
	return &velero.RestoreItemActionExecuteOutput{
		UpdatedItem:     input.Item,
		AdditionalItems: additionalItems,
	}, nil
}
