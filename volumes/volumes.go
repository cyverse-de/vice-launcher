package volumes

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cyverse-de/model/v6"
	"github.com/cyverse-de/vice-launcher/common"
	"github.com/cyverse-de/vice-launcher/config"
	"github.com/cyverse-de/vice-launcher/constants"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	defaultStorageCapacity, _ = resourcev1.ParseQuantity("5Gi")
)

// IRODSFSPathMapping defines a single path mapping that can be used by the iRODS CSI driver to create a mount point.
type IRODSFSPathMapping struct {
	IRODSPath      string `yaml:"irods_path" json:"irods_path"`
	MappingPath    string `yaml:"mapping_path" json:"mapping_path"`
	ResourceType   string `yaml:"resource_type" json:"resource_type"` // file or dir
	ReadOnly       bool   `yaml:"read_only" json:"read_only"`
	CreateDir      bool   `yaml:"create_dir" json:"create_dir"`
	IgnoreNotExist bool   `yaml:"ignore_not_exist" json:"ignore_not_exist"`
}

type VolumeMaker struct {
	IRODSZone    string
	UseCSIDriver bool
	labeller     common.JobLabeller
}

func New(init *config.Init, labeller common.JobLabeller) *VolumeMaker {
	return &VolumeMaker{
		IRODSZone:    init.IRODSZone,
		UseCSIDriver: init.UseCSIDriver,
		labeller:     labeller,
	}
}

func GetCSIDataVolumeHandle(job *model.Job) string {
	return fmt.Sprintf("%s-handle-%s", constants.CSIDriverDataVolumeNamePrefix, job.InvocationID)
}

func GetCSIDataVolumeName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverDataVolumeNamePrefix, job.InvocationID)
}

func GetCSIDataVolumeClaimName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverDataVolumeClaimNamePrefix, job.InvocationID)
}

func GetInputPathMappings(job *model.Job) ([]IRODSFSPathMapping, error) {
	mappings := []IRODSFSPathMapping{}
	// mark if the mapping path is already occupied
	// key = mount path, val = irods path
	mappingMap := map[string]string{}

	// Mount the input and output files.
	for _, step := range job.Steps {
		for _, stepInput := range step.Config.Inputs {
			irodsPath := stepInput.IRODSPath()
			if len(irodsPath) > 0 {
				var resourceType string
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

				mountPath := fmt.Sprintf("%s/%s", constants.CSIDriverInputVolumeMountPath, filepath.Base(irodsPath))
				// check if mountPath is already used by other input
				if existingIRODSPath, ok := mappingMap[mountPath]; ok {
					// exists - error
					return nil, fmt.Errorf("tried to mount an input file %s at %s already used by - %s", irodsPath, mountPath, existingIRODSPath)
				}
				mappingMap[mountPath] = irodsPath

				mapping := IRODSFSPathMapping{
					IRODSPath:      irodsPath,
					MappingPath:    mountPath,
					ResourceType:   resourceType,
					ReadOnly:       true,
					CreateDir:      false,
					IgnoreNotExist: true,
				}

				mappings = append(mappings, mapping)
			}
		}
	}
	return mappings, nil
}

func getOutputPathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for output
	return IRODSFSPathMapping{
		IRODSPath:      job.OutputDirectory(),
		MappingPath:    constants.CSIDriverOutputVolumeMountPath,
		ResourceType:   "dir",
		ReadOnly:       false,
		CreateDir:      true,
		IgnoreNotExist: false,
	}
}

func (v *VolumeMaker) getHomePathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for home

	return IRODSFSPathMapping{
		IRODSPath:      job.UserHome,
		MappingPath:    job.UserHome,
		ResourceType:   "dir",
		ReadOnly:       false,
		CreateDir:      false,
		IgnoreNotExist: false,
	}
}

func (v *VolumeMaker) getSharedPathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for shared data
	sharedHomeFullPath := fmt.Sprintf("/%s/home/shared", v.GetZoneMountPath())

	return IRODSFSPathMapping{
		IRODSPath:      sharedHomeFullPath,
		MappingPath:    sharedHomeFullPath,
		ResourceType:   "dir",
		ReadOnly:       false,
		CreateDir:      false,
		IgnoreNotExist: true,
	}
}

func (v *VolumeMaker) getCSIDataVolumeLabels(ctx context.Context, job *model.Job) (map[string]string, error) {
	labels, err := v.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = GetCSIDataVolumeClaimName(job)
	return labels, nil
}

// getPersistentVolumes returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumes(ctx context.Context, job *model.Job) ([]*apiv1.PersistentVolume, error) {
	if v.UseCSIDriver {
		dataPathMappings := []IRODSFSPathMapping{}

		// input output path
		inputPathMappings, err := GetInputPathMappings(job)
		if err != nil {
			return nil, err
		}
		dataPathMappings = append(dataPathMappings, inputPathMappings...)

		outputPathMapping := getOutputPathMapping(job)
		dataPathMappings = append(dataPathMappings, outputPathMapping)

		// home path
		if job.UserHome != "" {
			homePathMapping := v.getHomePathMapping(job)
			dataPathMappings = append(dataPathMappings, homePathMapping)
		}

		// shared path
		sharedPathMapping := v.getSharedPathMapping(job)
		dataPathMappings = append(dataPathMappings, sharedPathMapping)

		dataPathMappingsJSONBytes, err := json.Marshal(dataPathMappings)
		if err != nil {
			return nil, err
		}

		volmode := apiv1.PersistentVolumeFilesystem
		persistentVolumes := []*apiv1.PersistentVolume{}

		dataVolumeLabels, err := v.getCSIDataVolumeLabels(ctx, job)
		if err != nil {
			return nil, err
		}

		dataVolume := &apiv1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   GetCSIDataVolumeName(job),
				Labels: dataVolumeLabels,
			},
			Spec: apiv1.PersistentVolumeSpec{
				Capacity: apiv1.ResourceList{
					apiv1.ResourceStorage: defaultStorageCapacity,
				},
				VolumeMode: &volmode,
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimRetain,
				StorageClassName:              constants.CSIDriverStorageClassName,
				PersistentVolumeSource: apiv1.PersistentVolumeSource{
					CSI: &apiv1.CSIPersistentVolumeSource{
						Driver:       constants.CSIDriverName,
						VolumeHandle: GetCSIDataVolumeHandle(job),
						VolumeAttributes: map[string]string{
							"client":            "irodsfuse",
							"path_mapping_json": string(dataPathMappingsJSONBytes),
							// use proxy access
							"clientUser": job.Submitter,
							"uid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
							"gid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
						},
					},
				},
			},
		}

		persistentVolumes = append(persistentVolumes, dataVolume)
		return persistentVolumes, nil
	}

	return nil, nil
}

// getPersistentVolumeClaims returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumeClaims(ctx context.Context, job *model.Job) ([]*apiv1.PersistentVolumeClaim, error) {
	if v.UseCSIDriver {
		labels, err := v.labeller.LabelsFromJob(ctx, job)
		if err != nil {
			return nil, err
		}

		storageclassname := constants.CSIDriverStorageClassName
		volumeClaims := []*apiv1.PersistentVolumeClaim{}

		dataVolumeClaim := &apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   GetCSIDataVolumeClaimName(job),
				Labels: labels,
			},
			Spec: apiv1.PersistentVolumeClaimSpec{
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				StorageClassName: &storageclassname,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"volume-name": GetCSIDataVolumeClaimName(job),
					},
				},
				Resources: apiv1.ResourceRequirements{
					Requests: apiv1.ResourceList{
						apiv1.ResourceStorage: defaultStorageCapacity,
					},
				},
			},
		}

		volumeClaims = append(volumeClaims, dataVolumeClaim)
		return volumeClaims, nil
	}

	return nil, nil
}

// GetPersistentVolumeSources returns the volumes for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumeSources(job *model.Job) ([]*apiv1.Volume, error) {
	if v.UseCSIDriver {
		volumes := []*apiv1.Volume{}

		dataVolume := &apiv1.Volume{
			Name: GetCSIDataVolumeClaimName(job),
			VolumeSource: apiv1.VolumeSource{
				PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
					ClaimName: GetCSIDataVolumeClaimName(job),
				},
			},
		}

		volumes = append(volumes, dataVolume)
		return volumes, nil
	}

	return nil, nil
}

// GetPersistentVolumeMounts returns the volume mount for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumeMounts(job *model.Job) []*apiv1.VolumeMount {
	if v.UseCSIDriver {
		volumeMounts := []*apiv1.VolumeMount{}

		dataVolumeMount := &apiv1.VolumeMount{
			Name:      GetCSIDataVolumeClaimName(job),
			MountPath: constants.CSIDriverLocalMountPath,
		}

		volumeMounts = append(volumeMounts, dataVolumeMount)
		return volumeMounts
	}

	return nil
}

func (v *VolumeMaker) GetZoneMountPath() string {
	return fmt.Sprintf("%s/%s", constants.CSIDriverLocalMountPath, v.IRODSZone)
}
