package deployments

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/cyverse-de/model/v6"
	"github.com/cyverse-de/vice-launcher/common"
	"github.com/cyverse-de/vice-launcher/config"
	"github.com/cyverse-de/vice-launcher/configmaps"
	"github.com/cyverse-de/vice-launcher/constants"
	"github.com/cyverse-de/vice-launcher/logging"
	"github.com/cyverse-de/vice-launcher/transfers"
	"github.com/cyverse-de/vice-launcher/volumes"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "deployments"})

// analysisPorts returns a list of container ports needed by the VICE analysis.
func analysisPorts(step *model.Step) []apiv1.ContainerPort {
	ports := []apiv1.ContainerPort{}

	for i, p := range step.Component.Container.Ports {
		ports = append(ports, apiv1.ContainerPort{
			ContainerPort: int32(p.ContainerPort),
			Name:          fmt.Sprintf("tcp-a-%d", i),
			Protocol:      apiv1.ProtocolTCP,
		})
	}

	return ports
}

type DeploymentMaker struct {
	config.Init       // embedded because a TON of the values are used here and this is less annoying.
	VICEProxyImage    string
	VICEProxyPortName string
	VICEProxyPort     int32
	configMapper      *configmaps.ConfigMapMaker
	labeller          common.JobLabeller
	volumeMaker       *volumes.VolumeMaker
	transferMaker     *transfers.FileTransferMaker
}

func New(
	init *config.Init,
	configMapper *configmaps.ConfigMapMaker,
	volumeMaker *volumes.VolumeMaker,
	transferMaker *transfers.FileTransferMaker,
	labeller common.JobLabeller,
) *DeploymentMaker {
	return &DeploymentMaker{
		VICEProxyImage:    init.ViceProxyImage,
		VICEProxyPortName: constants.VICEProxyPortName,
		VICEProxyPort:     constants.VICEProxyPort,
		configMapper:      configMapper,
		labeller:          labeller,
		volumeMaker:       volumeMaker,
		transferMaker:     transferMaker,
	}
}

// deploymentVolumes returns the Volume objects needed for the VICE analyis
// Deployment. This does NOT call the k8s API to actually create the Volumes,
// it returns the objects that can be included in the Deployment object that
// will get passed to the k8s API later. Also not that these are the Volumes,
// not the container-specific VolumeMounts.
func (d *DeploymentMaker) deploymentVolumes(job *model.Job) []apiv1.Volume {
	output := []apiv1.Volume{}

	if len(job.FilterInputsWithoutTickets()) > 0 {
		output = append(output, apiv1.Volume{
			Name: constants.InputPathListVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{
						Name: configmaps.InputPathListConfigMapName(job),
					},
				},
			},
		})
	}

	if d.UseCSIDriver {
		output = append(output,
			apiv1.Volume{
				Name: constants.WorkingDirVolumeName,
				VolumeSource: apiv1.VolumeSource{
					EmptyDir: &apiv1.EmptyDirVolumeSource{},
				},
			},
		)
		volumeSources, err := d.volumeMaker.GetPersistentVolumeSources(job)
		if err != nil {
			log.Warn(err)
		} else {
			for _, volumeSource := range volumeSources {
				output = append(output, *volumeSource)
			}
		}
	} else {
		output = append(output,
			apiv1.Volume{
				Name: constants.FileTransfersVolumeName,
				VolumeSource: apiv1.VolumeSource{
					EmptyDir: &apiv1.EmptyDirVolumeSource{},
				},
			},
			apiv1.Volume{
				Name: constants.PorklockConfigVolumeName,
				VolumeSource: apiv1.VolumeSource{
					Secret: &apiv1.SecretVolumeSource{
						SecretName: constants.PorklockConfigSecretName,
					},
				},
			},
		)
	}

	output = append(output,
		apiv1.Volume{
			Name: constants.ExcludesVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{
						Name: configmaps.ExcludesConfigMapName(job),
					},
				},
			},
		},
	)

	return output
}

func (d *DeploymentMaker) getFrontendURL(job *model.Job) *url.URL {
	// This should be parsed in main(), so we shouldn't worry about it here.
	frontURL, _ := url.Parse(d.FrontendBaseURL)
	frontURL.Host = fmt.Sprintf("%s.%s", common.IngressName(job.UserID, job.InvocationID), frontURL.Host)
	return frontURL
}

func (d *DeploymentMaker) viceProxyCommand(job *model.Job) []string {
	frontURL := d.getFrontendURL(job)
	backendURL := fmt.Sprintf("http://localhost:%s", strconv.Itoa(job.Steps[0].Component.Container.Ports[0].ContainerPort))

	// websocketURL := fmt.Sprintf("ws://localhost:%s", strconv.Itoa(job.Steps[0].Component.Container.Ports[0].ContainerPort))

	output := []string{
		"vice-proxy",
		"--listen-addr", fmt.Sprintf("0.0.0.0:%d", constants.VICEProxyPort),
		"--backend-url", backendURL,
		"--ws-backend-url", backendURL,
		"--cas-base-url", d.CASBaseURL,
		"--cas-validate", "validate",
		"--frontend-url", frontURL.String(),
		"--external-id", job.InvocationID,
		"--get-analysis-id-base", fmt.Sprintf("http://%s.%s", d.GetAnalysisIDService, d.VICEBackendNamespace),
		"--check-resource-access-base", fmt.Sprintf("http://%s.%s", d.CheckResourceAccessService, d.VICEBackendNamespace),
		"--keycloak-base-url", d.KeycloakBaseURL,
		"--keycloak-realm", d.KeycloakRealm,
		"--keycloak-client-id", d.KeycloakClientID,
		"--keycloak-client-secret", d.KeycloakClientSecret,
	}

	return output
}

var (
	defaultCPUResourceRequest, _ = resourcev1.ParseQuantity("1000m")
	defaultMemResourceRequest, _ = resourcev1.ParseQuantity("2Gi")
	defaultStorageRequest, _     = resourcev1.ParseQuantity("16Gi")
	defaultCPUResourceLimit, _   = resourcev1.ParseQuantity("4000m")
	defaultMemResourceLimit, _   = resourcev1.ParseQuantity("8Gi")
)

func cpuResourceRequest(job *model.Job) resourcev1.Quantity {
	var (
		value resourcev1.Quantity
		err   error
	)

	value = defaultCPUResourceRequest

	if job.Steps[0].Component.Container.MinCPUCores != 0 {
		value, err = resourcev1.ParseQuantity(fmt.Sprintf("%fm", job.Steps[0].Component.Container.MinCPUCores*1000))
		if err != nil {
			log.Warn(err)
			value = defaultCPUResourceRequest
		}
	}

	return value
}

func cpuResourceLimit(job *model.Job) resourcev1.Quantity {
	var (
		value resourcev1.Quantity
		err   error
	)

	value = defaultCPUResourceLimit

	if job.Steps[0].Component.Container.MaxCPUCores != 0 {
		value, err = resourcev1.ParseQuantity(fmt.Sprintf("%fm", job.Steps[0].Component.Container.MaxCPUCores*1000))
		if err != nil {
			log.Warn(err)
			value = defaultCPUResourceLimit
		}
	}
	return value
}

func memResourceRequest(job *model.Job) resourcev1.Quantity {
	var (
		value resourcev1.Quantity
		err   error
	)

	value = defaultMemResourceRequest

	if job.Steps[0].Component.Container.MinMemoryLimit != 0 {
		value, err = resourcev1.ParseQuantity(fmt.Sprintf("%d", job.Steps[0].Component.Container.MinMemoryLimit))
		if err != nil {
			log.Warn(err)
			value = defaultMemResourceRequest
		}
	}
	return value
}

func memResourceLimit(job *model.Job) resourcev1.Quantity {
	var (
		value resourcev1.Quantity
		err   error
	)

	value = defaultMemResourceLimit

	if job.Steps[0].Component.Container.MemoryLimit != 0 {
		value, err = resourcev1.ParseQuantity(fmt.Sprintf("%d", job.Steps[0].Component.Container.MemoryLimit))
		if err != nil {
			log.Warn(err)
			value = defaultMemResourceLimit
		}
	}
	return value
}

func storageRequest(job *model.Job) resourcev1.Quantity {
	var (
		value resourcev1.Quantity
		err   error
	)

	value = defaultStorageRequest

	if job.Steps[0].Component.Container.MinDiskSpace != 0 {
		value, err = resourcev1.ParseQuantity(fmt.Sprintf("%d", job.Steps[0].Component.Container.MinDiskSpace))
		if err != nil {
			log.Warn(err)
			value = defaultStorageRequest
		}
	}
	return value
}

// inputStagingContainer returns the init container to be used for staging input files. This init container
// is only used when iRODS CSI driver integration is disabled.
func (d *DeploymentMaker) inputStagingContainer(job *model.Job) apiv1.Container {
	return apiv1.Container{
		Name:            constants.FileTransfersInitContainerName,
		Image:           fmt.Sprintf("%s:%s", d.PorklockImage, d.PorklockTag),
		Command:         append(transfers.FileTransferCommand(job), "--no-service"),
		ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
		WorkingDir:      constants.InputPathListMountPath,
		VolumeMounts:    transfers.FileTransfersVolumeMounts(job),
		Ports: []apiv1.ContainerPort{
			{
				Name:          constants.FileTransfersPortName,
				ContainerPort: constants.FileTransfersPort,
				Protocol:      apiv1.Protocol("TCP"),
			},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{
					"SETPCAP",
					"AUDIT_WRITE",
					"KILL",
					"SETGID",
					"SETUID",
					"NET_BIND_SERVICE",
					"SYS_CHROOT",
					"SETFCAP",
					"FSETID",
					"NET_RAW",
					"MKNOD",
				},
			},
		},
	}
}

// workingDirPrepContainer returns the init container to be used for preparing the working directory volume
// for use within the VICE analysis. This init container is only used when iRODS CSI driver integration is
// enabled.
//
// It may seem odd to use the file transfer image to initialize the working directory when no files are actually
// being transferred, but it works. We use it for a couple of different reasons. First, we need a Unix shell and
// it has one. Second, it's already set up so that we can configure it in a way that avoids image pull rate limits.
func (d *DeploymentMaker) workingDirPrepContainer(job *model.Job) apiv1.Container {

	// Build the command used to initialize the working directory.
	workingDirInitCommand := []string{
		"bash",
		"-c",
		strings.Join([]string{
			fmt.Sprintf("ln -s \"%s\" .", constants.CSIDriverLocalMountPath),
			fmt.Sprintf("ln -s \"/%s/home\" .", d.volumeMaker.GetZoneMountPath()),
		}, " && "),
	}

	// Build the init container spec.
	initContainer := apiv1.Container{
		Name:            constants.WorkingDirInitContainerName,
		Image:           fmt.Sprintf("%s:%s", d.PorklockImage, d.PorklockTag),
		Command:         workingDirInitCommand,
		ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
		WorkingDir:      constants.WorkingDirInitContainerMountPath,
		VolumeMounts: []apiv1.VolumeMount{
			{
				Name:      constants.WorkingDirVolumeName,
				MountPath: constants.WorkingDirInitContainerMountPath,
				ReadOnly:  false,
			},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{
					"SETPCAP",
					"AUDIT_WRITE",
					"KILL",
					"SETGID",
					"SETUID",
					"NET_BIND_SERVICE",
					"SYS_CHROOT",
					"SETFCAP",
					"FSETID",
					"NET_RAW",
					"MKNOD",
				},
			},
		},
	}

	return initContainer
}

// workingDirMountPath returns the path to the directory containing file inputs.
func workingDirMountPath(job *model.Job) string {
	return job.Steps[0].Component.Container.WorkingDirectory()
}

// initContainers returns a []apiv1.Container used for the InitContainers in
// the VICE app Deployment resource.
func (d *DeploymentMaker) initContainers(job *model.Job) []apiv1.Container {
	output := []apiv1.Container{}

	if !d.UseCSIDriver {
		output = append(output, d.inputStagingContainer(job))
	} else {
		output = append(output, d.workingDirPrepContainer(job))
	}

	return output
}

func gpuEnabled(job *model.Job) bool {
	gpuEnabled := false
	for _, device := range job.Steps[0].Component.Container.Devices {
		if strings.HasPrefix(strings.ToLower(device.HostPath), "/dev/nvidia") {
			gpuEnabled = true
		}
	}
	return gpuEnabled
}

func (d *DeploymentMaker) defineAnalysisContainer(job *model.Job) apiv1.Container {
	analysisEnvironment := []apiv1.EnvVar{}
	for envKey, envVal := range job.Steps[0].Environment {
		analysisEnvironment = append(
			analysisEnvironment,
			apiv1.EnvVar{
				Name:  envKey,
				Value: envVal,
			},
		)
	}

	analysisEnvironment = append(
		analysisEnvironment,
		apiv1.EnvVar{
			Name:  "REDIRECT_URL",
			Value: d.getFrontendURL(job).String(),
		},
		apiv1.EnvVar{
			Name:  "IPLANT_USER",
			Value: job.Submitter,
		},
		apiv1.EnvVar{
			Name:  "IPLANT_EXECUTION_ID",
			Value: job.InvocationID,
		},
	)

	cpuRequest := cpuResourceRequest(job)
	memRequest := memResourceRequest(job)
	storageRequest := storageRequest(job)

	requests := apiv1.ResourceList{
		apiv1.ResourceCPU:              cpuRequest,     // job contains # cores
		apiv1.ResourceMemory:           memRequest,     // job contains # bytes mem
		apiv1.ResourceEphemeralStorage: storageRequest, // job contains # bytes storage
	}

	cpuLimit := cpuResourceLimit(job)
	memLimit := memResourceLimit(job)

	limits := apiv1.ResourceList{
		apiv1.ResourceCPU:    cpuLimit, //job contains # cores
		apiv1.ResourceMemory: memLimit, // job contains # bytes mem
	}

	// If a GPU device is configured, then add it to the resource limits.
	if gpuEnabled(job) {
		gpuLimit, err := resourcev1.ParseQuantity("1")
		if err != nil {
			log.Warn(err)
		} else {
			limits[apiv1.ResourceName("nvidia.com/gpu")] = gpuLimit
		}
	}

	volumeMounts := []apiv1.VolumeMount{}
	if d.UseCSIDriver {
		volumeMounts = append(volumeMounts, apiv1.VolumeMount{
			Name:      constants.WorkingDirVolumeName,
			MountPath: workingDirMountPath(job),
			ReadOnly:  false,
		})
		persistentVolumeMounts := d.volumeMaker.GetPersistentVolumeMounts(job)
		for _, persistentVolumeMount := range persistentVolumeMounts {
			volumeMounts = append(volumeMounts, *persistentVolumeMount)
		}
	} else {
		volumeMounts = append(volumeMounts, apiv1.VolumeMount{
			Name:      constants.FileTransfersVolumeName,
			MountPath: workingDirMountPath(job),
			ReadOnly:  false,
		})
	}

	analysisContainer := apiv1.Container{
		Name: constants.AnalysisContainerName,
		Image: fmt.Sprintf(
			"%s:%s",
			job.Steps[0].Component.Container.Image.Name,
			job.Steps[0].Component.Container.Image.Tag,
		),
		ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
		Env:             analysisEnvironment,
		Resources: apiv1.ResourceRequirements{
			Limits:   limits,
			Requests: requests,
		},
		VolumeMounts: volumeMounts,
		Ports:        analysisPorts(&job.Steps[0]),
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			// Capabilities: &apiv1.Capabilities{
			// 	Drop: []apiv1.Capability{
			// 		"SETPCAP",
			// 		"AUDIT_WRITE",
			// 		"KILL",
			// 		//"SETGID",
			// 		//"SETUID",
			// 		"SYS_CHROOT",
			// 		"SETFCAP",
			// 		"FSETID",
			// 		//"MKNOD",
			// 	},
			// },
		},
		ReadinessProbe: &apiv1.Probe{
			InitialDelaySeconds: 0,
			TimeoutSeconds:      30,
			SuccessThreshold:    1,
			FailureThreshold:    10,
			PeriodSeconds:       31,
			ProbeHandler: apiv1.ProbeHandler{
				HTTPGet: &apiv1.HTTPGetAction{
					Port:   intstr.FromInt(job.Steps[0].Component.Container.Ports[0].ContainerPort),
					Scheme: apiv1.URISchemeHTTP,
					Path:   "/",
				},
			},
		},
	}

	if job.Steps[0].Component.Container.EntryPoint != "" {
		analysisContainer.Command = []string{
			job.Steps[0].Component.Container.EntryPoint,
		}
	}

	// Default to the container working directory if it isn't set.
	if job.Steps[0].Component.Container.WorkingDir != "" {
		analysisContainer.WorkingDir = job.Steps[0].Component.Container.WorkingDir
	}

	if len(job.Steps[0].Arguments()) != 0 {
		analysisContainer.Args = append(analysisContainer.Args, job.Steps[0].Arguments()...)
	}

	return analysisContainer

}

// deploymentContainers returns the Containers needed for the VICE analysis
// Deployment. It does not call the k8s API.
func (d *DeploymentMaker) deploymentContainers(job *model.Job) []apiv1.Container {
	output := []apiv1.Container{}

	output = append(output, apiv1.Container{
		Name:            constants.VICEProxyContainerName,
		Image:           d.VICEProxyImage,
		Command:         d.viceProxyCommand(job),
		ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
		Ports: []apiv1.ContainerPort{
			{
				Name:          d.VICEProxyPortName,
				ContainerPort: d.VICEProxyPort,
				Protocol:      apiv1.Protocol("TCP"),
			},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{
					"SETPCAP",
					"AUDIT_WRITE",
					"KILL",
					"SETGID",
					"SETUID",
					"SYS_CHROOT",
					"SETFCAP",
					"FSETID",
					"NET_RAW",
					"MKNOD",
				},
			},
		},
		ReadinessProbe: &apiv1.Probe{
			ProbeHandler: apiv1.ProbeHandler{
				HTTPGet: &apiv1.HTTPGetAction{
					Port:   intstr.FromInt(int(d.VICEProxyPort)),
					Scheme: apiv1.URISchemeHTTP,
					Path:   "/",
				},
			},
		},
	})

	if !d.UseCSIDriver {
		output = append(output, apiv1.Container{
			Name:            constants.FileTransfersContainerName,
			Image:           fmt.Sprintf("%s:%s", d.PorklockImage, d.PorklockTag),
			Command:         transfers.FileTransferCommand(job),
			ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
			WorkingDir:      constants.InputPathListMountPath,
			VolumeMounts:    transfers.FileTransfersVolumeMounts(job),
			Ports: []apiv1.ContainerPort{
				{
					Name:          constants.FileTransfersPortName,
					ContainerPort: constants.FileTransfersPort,
					Protocol:      apiv1.Protocol("TCP"),
				},
			},
			SecurityContext: &apiv1.SecurityContext{
				RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
				RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
				Capabilities: &apiv1.Capabilities{
					Drop: []apiv1.Capability{
						"SETPCAP",
						"AUDIT_WRITE",
						"KILL",
						"SETGID",
						"SETUID",
						"NET_BIND_SERVICE",
						"SYS_CHROOT",
						"SETFCAP",
						"FSETID",
						"NET_RAW",
						"MKNOD",
					},
				},
			},
			ReadinessProbe: &apiv1.Probe{
				ProbeHandler: apiv1.ProbeHandler{
					HTTPGet: &apiv1.HTTPGetAction{
						Port:   intstr.FromInt(int(constants.FileTransfersPort)),
						Scheme: apiv1.URISchemeHTTP,
						Path:   "/",
					},
				},
			},
		})
	}

	output = append(output, d.defineAnalysisContainer(job))
	return output
}

// imagePullSecrets creates an array of LocalObjectReference that refer to any
// configured secrets to use for pulling images This is passed the job because
// it may be advantageous, in the future, to add secrets depending on the
// images actually needed by the job, but at present this uses a static value
func (d *DeploymentMaker) imagePullSecrets(_ *model.Job) []apiv1.LocalObjectReference {
	if d.ImagePullSecretName != "" {
		return []apiv1.LocalObjectReference{
			{Name: d.ImagePullSecretName},
		}
	}
	return []apiv1.LocalObjectReference{}
}

// GetDeployment assembles and returns the Deployment for the VICE analysis. It does
// not call the k8s API.
func (d *DeploymentMaker) GetDeployment(ctx context.Context, job *model.Job) (*appsv1.Deployment, error) {
	labels, err := d.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	autoMount := false

	// Add the tolerations to use by default.
	tolerations := []apiv1.Toleration{
		{
			Key:      constants.VICETolerationKey,
			Operator: apiv1.TolerationOperator(constants.VICETolerationOperator),
			Value:    constants.VICETolerationValue,
			Effect:   apiv1.TaintEffect(constants.VICETolerationEffect),
		},
	}

	// Add the node selector requirements to use by default.
	nodeSelectorRequirements := []apiv1.NodeSelectorRequirement{
		{
			Key:      constants.VICEAffinityKey,
			Operator: apiv1.NodeSelectorOperator(constants.VICEAffinityOperator),
			Values: []string{
				constants.VICEAffinityValue,
			},
		},
	}

	// Add the tolerations and node selector requirements for jobs that require a GPU.
	if gpuEnabled(job) {
		tolerations = append(tolerations, apiv1.Toleration{
			Key:      constants.GPUTolerationKey,
			Operator: apiv1.TolerationOperator(constants.GPUTolerationOperator),
			Value:    constants.GPUTolerationValue,
			Effect:   apiv1.TaintEffect(constants.GPUTolerationEffect),
		})

		nodeSelectorRequirements = append(nodeSelectorRequirements, apiv1.NodeSelectorRequirement{
			Key:      constants.GPUAffinityKey,
			Operator: apiv1.NodeSelectorOperator(constants.GPUAffinityOperator),
			Values: []string{
				constants.GPUAffinityValue,
			},
		})
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   job.InvocationID,
			Labels: labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: constants.Int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"external-id": job.InvocationID,
				},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: apiv1.PodSpec{
					Hostname:                     common.IngressName(job.UserID, job.InvocationID),
					RestartPolicy:                apiv1.RestartPolicy("Always"),
					Volumes:                      d.deploymentVolumes(job),
					InitContainers:               d.initContainers(job),
					Containers:                   d.deploymentContainers(job),
					ImagePullSecrets:             d.imagePullSecrets(job),
					AutomountServiceAccountToken: &autoMount,
					SecurityContext: &apiv1.PodSecurityContext{
						RunAsUser:  constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
						RunAsGroup: constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
						FSGroup:    constants.Int64Ptr(int64(job.Steps[0].Component.Container.UID)),
					},
					Tolerations: tolerations,
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: nodeSelectorRequirements,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	return deployment, nil
}
