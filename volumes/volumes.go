package volumes

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cyverse-de/model"
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

func GetCSIInputOutputVolumeHandle(job *model.Job) string {
	return fmt.Sprintf("%s-handle-%s", constants.CSIDriverInputOutputVolumeNamePrefix, job.InvocationID)
}

func GetCSIHomeVolumeHandle(job *model.Job) string {
	return fmt.Sprintf("%s-handle-%s", constants.CSIDriverHomeVolumeNamePrefix, job.InvocationID)
}

func GetCSIInputOutputVolumeName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverInputOutputVolumeNamePrefix, job.InvocationID)
}

func GetCSIHomeVolumeName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverHomeVolumeNamePrefix, job.InvocationID)
}

func GetCSIInputOutputVolumeClaimName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverInputOutputVolumeClaimNamePrefix, job.InvocationID)
}

func GetCSIHomeVolumeClaimName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", constants.CSIDriverHomeVolumeClaimNamePrefix, job.InvocationID)
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

func getHomePathMapping(job *model.Job, irodsZone string) IRODSFSPathMapping {
	// mount a single collection for home
	userHome := strings.TrimPrefix(job.UserHome, fmt.Sprintf("/%s", irodsZone))
	userHome = strings.TrimSuffix(userHome, "/")

	return IRODSFSPathMapping{
		IRODSPath:      job.UserHome,
		MappingPath:    userHome,
		ResourceType:   "dir",
		ReadOnly:       false,
		CreateDir:      false,
		IgnoreNotExist: false,
	}
}

func getSharedPathMapping(job *model.Job, irodsZone string) IRODSFSPathMapping {
	// mount a single collection for shared data
	sharedHomeFullPath := fmt.Sprintf("/%s/home/shared", irodsZone)
	sharedHome := "/home/shared"

	return IRODSFSPathMapping{
		IRODSPath:      sharedHomeFullPath,
		MappingPath:    sharedHome,
		ResourceType:   "dir",
		ReadOnly:       false,
		CreateDir:      false,
		IgnoreNotExist: true,
	}
}

func (v *VolumeMaker) getCSIInputOutputVolumeLabels(ctx context.Context, job *model.Job) (map[string]string, error) {
	labels, err := v.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = GetCSIInputOutputVolumeClaimName(job)
	return labels, nil
}

func (v *VolumeMaker) getCSIHomeVolumeLabels(ctx context.Context, job *model.Job) (map[string]string, error) {
	labels, err := v.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = GetCSIHomeVolumeClaimName(job)
	return labels, nil
}

// getPersistentVolumes returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumes(ctx context.Context, job *model.Job) ([]*apiv1.PersistentVolume, error) {
	if v.UseCSIDriver {
		// input output path
		ioPathMappings := []IRODSFSPathMapping{}

		inputPathMappings, err := GetInputPathMappings(job)
		if err != nil {
			return nil, err
		}
		ioPathMappings = append(ioPathMappings, inputPathMappings...)

		outputPathMapping := getOutputPathMapping(job)
		ioPathMappings = append(ioPathMappings, outputPathMapping)

		// convert pathMappings into json
		ioPathMappingsJSONBytes, err := json.Marshal(ioPathMappings)
		if err != nil {
			return nil, err
		}

		// home path
		homePathMappings := []IRODSFSPathMapping{}
		if job.UserHome != "" {
			homePathMapping := getHomePathMapping(job, v.IRODSZone)
			homePathMappings = append(homePathMappings, homePathMapping)
		}

		// shared path
		sharedPathMapping := getSharedPathMapping(job, v.IRODSZone)
		homePathMappings = append(homePathMappings, sharedPathMapping)

		homePathMappingsJSONBytes, err := json.Marshal(homePathMappings)
		if err != nil {
			return nil, err
		}

		volmode := apiv1.PersistentVolumeFilesystem
		persistentVolumes := []*apiv1.PersistentVolume{}

		ioVolumeLabels, err := v.getCSIInputOutputVolumeLabels(ctx, job)
		if err != nil {
			return nil, err
		}

		ioVolume := &apiv1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   GetCSIInputOutputVolumeName(job),
				Labels: ioVolumeLabels,
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
						VolumeHandle: GetCSIInputOutputVolumeHandle(job),
						VolumeAttributes: map[string]string{
							"client":            "irodsfuse",
							"path_mapping_json": string(ioPathMappingsJSONBytes),
							// use proxy access
							"clientUser": job.Submitter,
							"uid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
							"gid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
						},
					},
				},
			},
		}

		persistentVolumes = append(persistentVolumes, ioVolume)

		if job.UserHome != "" {
			homeVolumeLabels, err := v.getCSIHomeVolumeLabels(ctx, job)
			if err != nil {
				return nil, err
			}

			homeVolume := &apiv1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   GetCSIHomeVolumeName(job),
					Labels: homeVolumeLabels,
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
							VolumeHandle: GetCSIHomeVolumeHandle(job),
							VolumeAttributes: map[string]string{
								"client":            "irodsfuse",
								"path_mapping_json": string(homePathMappingsJSONBytes),
								// use proxy access
								"clientUser": job.Submitter,
								"uid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
								"gid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
							},
						},
					},
				},
			}

			persistentVolumes = append(persistentVolumes, homeVolume)
		}

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

		ioVolumeClaim := &apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   GetCSIInputOutputVolumeClaimName(job),
				Labels: labels,
			},
			Spec: apiv1.PersistentVolumeClaimSpec{
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				StorageClassName: &storageclassname,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"volume-name": GetCSIInputOutputVolumeClaimName(job),
					},
				},
				Resources: apiv1.ResourceRequirements{
					Requests: apiv1.ResourceList{
						apiv1.ResourceStorage: defaultStorageCapacity,
					},
				},
			},
		}

		volumeClaims = append(volumeClaims, ioVolumeClaim)

		if job.UserHome != "" {
			homeVolumeClaim := &apiv1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:   GetCSIHomeVolumeClaimName(job),
					Labels: labels,
				},
				Spec: apiv1.PersistentVolumeClaimSpec{
					AccessModes: []apiv1.PersistentVolumeAccessMode{
						apiv1.ReadWriteMany,
					},
					StorageClassName: &storageclassname,
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"volume-name": GetCSIHomeVolumeClaimName(job),
						},
					},
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceStorage: defaultStorageCapacity,
						},
					},
				},
			}

			volumeClaims = append(volumeClaims, homeVolumeClaim)
		}

		return volumeClaims, nil
	}

	return nil, nil
}

// GetPersistentVolumeSources returns the volumes for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumeSources(job *model.Job) ([]*apiv1.Volume, error) {
	if v.UseCSIDriver {
		volumes := []*apiv1.Volume{}

		ioVolume := &apiv1.Volume{
			Name: GetCSIInputOutputVolumeClaimName(job),
			VolumeSource: apiv1.VolumeSource{
				PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
					ClaimName: GetCSIInputOutputVolumeClaimName(job),
				},
			},
		}

		volumes = append(volumes, ioVolume)

		if job.UserHome != "" {
			homeVolume := &apiv1.Volume{
				Name: GetCSIHomeVolumeClaimName(job),
				VolumeSource: apiv1.VolumeSource{
					PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
						ClaimName: GetCSIHomeVolumeClaimName(job),
					},
				},
			}

			volumes = append(volumes, homeVolume)
		}

		return volumes, nil
	}

	return nil, nil
}

// GetPersistentVolumeMounts returns the volume mount for the VICE analysis. It does
// not call the k8s API.
func (v *VolumeMaker) GetPersistentVolumeMounts(job *model.Job) []*apiv1.VolumeMount {
	if v.UseCSIDriver {
		volumeMounts := []*apiv1.VolumeMount{}

		ioVolumeMount := &apiv1.VolumeMount{
			Name:      GetCSIInputOutputVolumeClaimName(job),
			MountPath: constants.CSIDriverLocalMountPath,
		}

		volumeMounts = append(volumeMounts, ioVolumeMount)

		homeVolumeMount := &apiv1.VolumeMount{
			Name:      GetCSIHomeVolumeClaimName(job),
			MountPath: fmt.Sprintf("/%s", v.IRODSZone),
		}
		volumeMounts = append(volumeMounts, homeVolumeMount)

		return volumeMounts
	}

	return nil
}
