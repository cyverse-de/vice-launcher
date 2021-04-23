package internal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"gopkg.in/cyverse-de/model.v5"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	defaultStorageCapacity, _ = resourcev1.ParseQuantity("5Gi")
)

// PathMapping ...
type IRODSFSPathMapping struct {
	IRODSPath      string `yaml:"irods_path" json:"irods_path"`
	MappingPath    string `yaml:"mapping_path" json:"mapping_path"`
	ResourceType   string `yaml:"resource_type" json:"resource_type"` // file or dir
	CreateDir      bool   `yaml:"create_dir" json:"create_dir"`
	IgnoreNotExist bool   `yaml:"ignore_not_exist" json:"ignore_not_exist"`
}

func (i *Internal) getCSIVolumeHandle(job *model.Job) string {
	return fmt.Sprintf("%s-handle-%s", csiDriverVolumeNamePrefix, job.InvocationID)
}

func (i *Internal) getCSIVolumeName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", csiDriverVolumeNamePrefix, job.InvocationID)
}

func (i *Internal) getCSIVolumeClaimName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", csiDriverVolumeClaimNamePrefix, job.InvocationID)
}

func (i *Internal) getInputPathMappings(job *model.Job) ([]IRODSFSPathMapping, error) {
	mappings := []IRODSFSPathMapping{}
	// mark if the mapping path is already occupied
	// key = mount path, val = irods path
	mappingMap := map[string]string{}

	for _, step := range job.Steps {
		for _, stepInput := range step.Input {
			irodsPath := stepInput.IRODSPath()
			if len(irodsPath) > 0 {
				resourceType := "file"
				if strings.ToLower(stepInput.Type) == "fileinput" {
					resourceType = "file"
				} else if strings.ToLower(stepInput.Type) == "multifileselector" {
					resourceType = "file"
				} else if strings.ToLower(stepInput.Type) == "folderinput" {
					resourceType = "dir"
				} else {
					// unknown
					return nil, fmt.Errorf("unknown step input type - %s", stepInput.Type)
				}

				mountPath := fmt.Sprintf("%s/%s", csiDriverInputVolumeMountPath, filepath.Base(irodsPath))
				// check if mountPath is already used by other input
				if existingIRODSPath, ok := mappingMap[mountPath]; ok {
					// exists - error
					return nil, fmt.Errorf("input file %s is trying to mount at %s that is already used by - %s", irodsPath, mountPath, existingIRODSPath)
				}
				mappingMap[mountPath] = irodsPath

				mapping := IRODSFSPathMapping{
					IRODSPath:      irodsPath,
					MappingPath:    mountPath,
					ResourceType:   resourceType,
					CreateDir:      false,
					IgnoreNotExist: true,
				}

				mappings = append(mappings, mapping)
			}
		}
	}
	return mappings, nil

}

func (i *Internal) getOutputPathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for output
	return IRODSFSPathMapping{
		IRODSPath:      job.OutputDirectory(),
		MappingPath:    csiDriverOutputVolumeMountPath,
		ResourceType:   "dir",
		CreateDir:      true,
		IgnoreNotExist: false,
	}
}

func (i *Internal) getCSIVolumeLabels(job *model.Job) (map[string]string, error) {
	labels, err := i.labelsFromJob(job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = i.getCSIVolumeClaimName(job)
	return labels, nil
}

// getPersistentVolume returns the PersistentVolume for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolume(job *model.Job) (*apiv1.PersistentVolume, error) {
	if i.UseCSIDriver {
		pathMappings := []IRODSFSPathMapping{}

		inputPathMappings, err := i.getInputPathMappings(job)
		if err != nil {
			return nil, err
		}
		pathMappings = append(pathMappings, inputPathMappings...)

		outputPathMapping := i.getOutputPathMapping(job)
		pathMappings = append(pathMappings, outputPathMapping)

		// convert pathMappings into json
		pathMappingsJsonBytes, err := json.Marshal(pathMappings)
		if err != nil {
			return nil, err
		}

		volmode := apiv1.PersistentVolumeFilesystem

		volumeLabels, err := i.getCSIVolumeLabels(job)
		if err != nil {
			return nil, err
		}

		volume := &apiv1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   i.getCSIVolumeName(job),
				Labels: volumeLabels,
			},
			Spec: apiv1.PersistentVolumeSpec{
				Capacity: apiv1.ResourceList{
					apiv1.ResourceStorage: defaultStorageCapacity,
				},
				VolumeMode: &volmode,
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimDelete,
				StorageClassName:              csiDriverStorageClassName,
				PersistentVolumeSource: apiv1.PersistentVolumeSource{
					CSI: &apiv1.CSIPersistentVolumeSource{
						Driver:       csiDriverName,
						VolumeHandle: i.getCSIVolumeHandle(job),
						VolumeAttributes: map[string]string{
							"client":            "irodsfuse",
							"path_mapping_json": string(pathMappingsJsonBytes),
							// use proxy access
							"clientUser": job.Submitter,
						},
					},
				},
			},
		}

		return volume, nil
	}

	return nil, nil
}

// getPersistentVolumeClaim returns the PersistentVolume for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeClaim(job *model.Job) (*apiv1.PersistentVolumeClaim, error) {
	if i.UseCSIDriver {
		labels, err := i.labelsFromJob(job)
		if err != nil {
			return nil, err
		}

		storageclassname := csiDriverStorageClassName

		volumeClaim := &apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   i.getCSIVolumeClaimName(job),
				Labels: labels,
			},
			Spec: apiv1.PersistentVolumeClaimSpec{
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				StorageClassName: &storageclassname,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"volume-name": i.getCSIVolumeClaimName(job),
					},
				},
				Resources: apiv1.ResourceRequirements{
					Requests: apiv1.ResourceList{
						apiv1.ResourceStorage: defaultStorageCapacity,
					},
				},
			},
		}

		return volumeClaim, nil
	}

	return nil, nil
}

// getPersistentVolumeSource returns the volume for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeSource(job *model.Job) (*apiv1.Volume, error) {
	if i.UseCSIDriver {
		volume := &apiv1.Volume{
			Name: i.getCSIVolumeClaimName(job),
			VolumeSource: apiv1.VolumeSource{
				PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
					ClaimName: i.getCSIVolumeClaimName(job),
				},
			},
		}
		return volume, nil
	}

	return nil, nil
}

// getPersistentVolumeMount returns the volume mount for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeMount(job *model.Job) (*apiv1.VolumeMount, error) {
	if i.UseCSIDriver {
		volumeMount := &apiv1.VolumeMount{
			Name:      i.getCSIVolumeClaimName(job),
			MountPath: fmt.Sprintf("/%s", csiDriverLocalMountPath),
		}
		return volumeMount, nil
	}

	return nil, nil
}
