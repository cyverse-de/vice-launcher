package internal

import (
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

func (i *Internal) inputCSIVolumeHandle(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-handle-%s-%d", csiDriverInputVolumeNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) outputCSIVolumeHandle(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-handle-%s-%d", csiDriverOutputVolumeNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) inputCSIVolumeName(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-%s-%d", csiDriverInputVolumeNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) outputCSIVolumeName(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-%s-%d", csiDriverOutputVolumeNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) inputCSIVolumeClaimName(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-%s-%d", csiDriverInputVolumeClaimNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) outputCSIVolumeClaimName(job *model.Job, mountID int) string {
	return fmt.Sprintf("%s-%s-%d", csiDriverOutputVolumeClaimNamePrefix, job.InvocationID, mountID)
}

func (i *Internal) getInputIRODSPaths(job *model.Job) []string {
	paths := []string{}
	for _, step := range job.Steps {
		for _, stepInput := range step.Input {
			irodsPath := stepInput.IRODSPath()
			if len(irodsPath) > 0 {
				paths = append(paths, irodsPath)
			}
		}
	}
	return paths
}

func (i *Internal) getOutputIRODSPaths(job *model.Job) []string {
	paths := []string{}
	paths = append(paths, job.OutputDirectory())

	/*
		for _, step := range job.Steps {
			for _, stepOutput := range step.Output {
				irodsPath := stepOutput.Source()
				if len(irodsPath) > 0 {
					paths = append(paths, irodsPath)
				}
			}
		}
	*/
	return paths
}

func (i *Internal) getMountPoints(paths []string) ([]string, error) {
	mountPoints := map[string]int{}

	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("path '%s' is not an absolute path", path)
		}

		pathElems := strings.Split(path[1:], "/")

		if len(pathElems) < 1 {
			return nil, fmt.Errorf("path '%s' does not contain zone", path)
		}

		zone := pathElems[0]

		if len(pathElems) >= 2 {
			if pathElems[1] == "home" {
				// /zone/home/xxx/...
				if len(pathElems) >= 3 {
					mountPath := fmt.Sprintf("/%s/%s/%s", zone, pathElems[1], pathElems[2])
					// add a mountPath
					if _, exist := mountPoints[mountPath]; !exist {
						mountPoints[mountPath] = 1
					}
				} else {
					return nil, fmt.Errorf("path '%s' cannot be mounted", path)
				}
			} else {
				// /zone/xxx/...
				mountPath := fmt.Sprintf("/%s/%s", zone, pathElems[1])
				// add a mountPath
				if _, exist := mountPoints[mountPath]; !exist {
					mountPoints[mountPath] = 1
				}
			}
		} else {
			return nil, fmt.Errorf("path '%s' cannot be mounted", path)
		}
	}

	// convert the map to an array
	mountPointArray := make([]string, len(mountPoints))
	idx := 0
	for k := range mountPoints {
		mountPointArray[idx] = k
		idx++
	}

	return mountPointArray, nil
}

func getIRODSFuseMountZonePath(path string) (string, string, error) {
	pathElems := strings.Split(path[1:], "/")
	if len(pathElems) < 2 {
		return "", "", fmt.Errorf("path '%s' does not contain zone and path", path)
	}

	zone := pathElems[0]
	mountPath := "/" + strings.Join(pathElems[1:], "/")

	return zone, mountPath, nil
}

func (i *Internal) inputCSIVolumeLabels(job *model.Job, mountID int) (map[string]string, error) {
	labels, err := i.labelsFromJob(job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = i.inputCSIVolumeClaimName(job, mountID)
	return labels, nil
}

func (i *Internal) outputCSIVolumeLabels(job *model.Job, mountID int) (map[string]string, error) {
	labels, err := i.labelsFromJob(job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = i.outputCSIVolumeClaimName(job, mountID)
	return labels, nil
}

// getPersistentVolumes returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumes(job *model.Job) ([]*apiv1.PersistentVolume, error) {
	volumes := []*apiv1.PersistentVolume{}

	if i.UseCSIDriver {
		inputPaths := i.getInputIRODSPaths(job)
		outputPaths := i.getOutputIRODSPaths(job)

		inputMountPaths, err := i.getMountPoints(inputPaths)
		if err != nil {
			return nil, err
		}

		outputMountPaths, err := i.getMountPoints(outputPaths)
		if err != nil {
			return nil, err
		}

		volmode := apiv1.PersistentVolumeFilesystem

		// input - can be multiple
		for inputMountID, inputMountPath := range inputMountPaths {
			_, fuseInputMountPath, err := getIRODSFuseMountZonePath(inputMountPath)
			if err != nil {
				return nil, err
			}

			inputVolumeLabels, err := i.inputCSIVolumeLabels(job, inputMountID)
			if err != nil {
				return nil, err
			}

			volumes = append(volumes,
				&apiv1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:   i.inputCSIVolumeName(job, inputMountID),
						Labels: inputVolumeLabels,
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
								VolumeHandle: i.inputCSIVolumeHandle(job, inputMountID),
								VolumeAttributes: map[string]string{
									"client": "irodsfuse",
									"path":   fuseInputMountPath,
									// use proxy access
									"clientUser": job.Submitter,
								},
							},
						},
					},
				},
			)
		}

		// output - can be multiple
		for outputMountID, outputMountPath := range outputMountPaths {
			_, fuseOutputMountPath, err := getIRODSFuseMountZonePath(outputMountPath)
			if err != nil {
				return nil, err
			}

			outputVolumeLabels, err := i.outputCSIVolumeLabels(job, outputMountID)
			if err != nil {
				return nil, err
			}

			volumes = append(volumes,
				&apiv1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:   i.outputCSIVolumeName(job, outputMountID),
						Labels: outputVolumeLabels,
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
								VolumeHandle: i.outputCSIVolumeHandle(job, outputMountID),
								VolumeAttributes: map[string]string{
									"client": "irodsfuse",
									"path":   fuseOutputMountPath,
									// use proxy access
									"clientUser": job.Submitter,
								},
							},
						},
					},
				},
			)
		}
	}

	return volumes, nil
}

// getPersistentVolumeClaims returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeClaims(job *model.Job) ([]*apiv1.PersistentVolumeClaim, error) {
	volumeClaims := []*apiv1.PersistentVolumeClaim{}

	if i.UseCSIDriver {
		inputPaths := i.getInputIRODSPaths(job)
		outputPaths := i.getOutputIRODSPaths(job)

		inputMountPaths, err := i.getMountPoints(inputPaths)
		if err != nil {
			return nil, err
		}

		outputMountPaths, err := i.getMountPoints(outputPaths)
		if err != nil {
			return nil, err
		}

		labels, err := i.labelsFromJob(job)
		if err != nil {
			return nil, err
		}

		storageclassname := csiDriverStorageClassName

		// input - can be multiple
		for inputMountID := range inputMountPaths {
			volumeClaims = append(volumeClaims,
				&apiv1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:   i.inputCSIVolumeClaimName(job, inputMountID),
						Labels: labels,
					},
					Spec: apiv1.PersistentVolumeClaimSpec{
						AccessModes: []apiv1.PersistentVolumeAccessMode{
							apiv1.ReadWriteMany,
						},
						StorageClassName: &storageclassname,
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"volume-name": i.inputCSIVolumeClaimName(job, inputMountID),
							},
						},
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceStorage: defaultStorageCapacity,
							},
						},
					},
				},
			)
		}

		// output - can be multiple
		for outputMountID := range outputMountPaths {
			volumeClaims = append(volumeClaims,
				&apiv1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:   i.outputCSIVolumeClaimName(job, outputMountID),
						Labels: labels,
					},
					Spec: apiv1.PersistentVolumeClaimSpec{
						AccessModes: []apiv1.PersistentVolumeAccessMode{
							apiv1.ReadWriteMany,
						},
						StorageClassName: &storageclassname,
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"volume-name": i.outputCSIVolumeClaimName(job, outputMountID),
							},
						},
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceStorage: defaultStorageCapacity,
							},
						},
					},
				},
			)
		}
	}

	return volumeClaims, nil
}

// getPersistentVolumeSources returns the volumes for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeSources(job *model.Job) ([]apiv1.Volume, error) {
	volumeSources := []apiv1.Volume{}

	if i.UseCSIDriver {
		inputPaths := i.getInputIRODSPaths(job)
		outputPaths := i.getOutputIRODSPaths(job)

		inputMountPaths, err := i.getMountPoints(inputPaths)
		if err != nil {
			return nil, err
		}

		outputMountPaths, err := i.getMountPoints(outputPaths)
		if err != nil {
			return nil, err
		}

		// input - can be multiple
		for inputMountID := range inputMountPaths {
			volumeSources = append(volumeSources,
				apiv1.Volume{
					Name: i.inputCSIVolumeClaimName(job, inputMountID),
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: i.inputCSIVolumeClaimName(job, inputMountID),
						},
					},
				},
			)
		}

		// output - can be multiple
		for outputMountID := range outputMountPaths {
			volumeSources = append(volumeSources,
				apiv1.Volume{
					Name: i.outputCSIVolumeClaimName(job, outputMountID),
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: i.outputCSIVolumeClaimName(job, outputMountID),
						},
					},
				},
			)
		}
	}

	return volumeSources, nil
}

// getPersistentVolumeMounts returns the volume mounts for the VICE analysis. It does
// not call the k8s API.
func (i *Internal) getPersistentVolumeMounts(job *model.Job) ([]apiv1.VolumeMount, error) {
	volumeMounts := []apiv1.VolumeMount{}

	if i.UseCSIDriver {
		inputPaths := i.getInputIRODSPaths(job)
		outputPaths := i.getOutputIRODSPaths(job)

		inputMountPaths, err := i.getMountPoints(inputPaths)
		if err != nil {
			return nil, err
		}

		outputMountPaths, err := i.getMountPoints(outputPaths)
		if err != nil {
			return nil, err
		}

		// input - can be multiple
		for inputMountID, inputMountPath := range inputMountPaths {
			volumeMounts = append(volumeMounts,
				apiv1.VolumeMount{
					Name:      i.inputCSIVolumeClaimName(job, inputMountID),
					MountPath: fmt.Sprintf("/input/%s", inputMountPath),
					ReadOnly:  true,
				},
			)
		}

		// output - can be multiple
		for outputMountID, outputMountPath := range outputMountPaths {
			volumeMounts = append(volumeMounts,
				apiv1.VolumeMount{
					Name:      i.outputCSIVolumeClaimName(job, outputMountID),
					MountPath: fmt.Sprintf("/output/%s", outputMountPath),
					ReadOnly:  false,
				},
			)
		}
	}

	return volumeMounts, nil
}
