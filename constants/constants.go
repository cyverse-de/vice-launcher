package constants

const (
	AnalysisContainerName = "analysis"

	PorklockConfigVolumeName = "porklock-config"
	PorklockConfigSecretName = "porklock-config"
	PorklockConfigMountPath  = "/etc/porklock"

	CSIDriverName                             = "irods.csi.cyverse.org"
	CSIDriverStorageClassName                 = "irods-sc"
	CSIDriverInputOutputVolumeNamePrefix      = "csi-io-volume"
	CSIDriverHomeVolumeNamePrefix             = "csi-home-volume"
	CSIDriverInputOutputVolumeClaimNamePrefix = "csi-io-volume-claim"
	CSIDriverHomeVolumeClaimNamePrefix        = "csi-home-volume-claim"
	CSIDriverInputVolumeMountPath             = "/input"
	CSIDriverOutputVolumeMountPath            = "/output"
	CSIDriverLocalMountPath                   = "/data"

	// The file transfers volume serves as the working directory when IRODS CSI Driver integration is disabled.
	FileTransfersVolumeName        = "input-files"
	FileTransfersContainerName     = "input-files"
	FileTransfersInitContainerName = "input-files-init"
	FileTransfersInputsMountPath   = "/input-files"

	// The working directory volume serves as the working directory when IRODS CSI Driver integration is enabled.
	WorkingDirVolumeName             = "working-dir"
	WorkingDirInitContainerName      = "working-dir-init"
	WorkingDirInitContainerMountPath = "/working-dir"

	VICEProxyContainerName = "vice-proxy"
	VICEProxyPort          = int32(60002)
	VICEProxyPortName      = "tcp-proxy"
	VICEProxyServicePort   = int32(60000)

	ExcludesMountPath  = "/excludes"
	ExcludesFileName   = "excludes-file"
	ExcludesVolumeName = "excludes-file"

	InputPathListMountPath  = "/input-paths"
	InputPathListFileName   = "input-path-list"
	InputPathListVolumeName = "input-path-list"

	IRODSConfigFilePath = "/etc/porklock/irods-config.properties"

	FileTransfersPortName = "tcp-input"
	FileTransfersPort     = int32(60001)

	DownloadBasePath = "/download"
	UploadBasePath   = "/upload"
	DownloadKind     = "download"
	UploadKind       = "upload"

	VICETolerationKey      = "vice"
	VICETolerationOperator = "Equal"
	VICETolerationValue    = "only"
	VICETolerationEffect   = "NoSchedule"

	GPUTolerationKey      = "gpu"
	GPUTolerationOperator = "Equal"
	GPUTolerationValue    = "true"
	GPUTolerationEffect   = "NoSchedule"

	VICEAffinityKey      = "vice"
	VICEAffinityOperator = "In"
	VICEAffinityValue    = "true"

	GPUAffinityKey      = "gpu"
	GPUAffinityOperator = "In"
	GPUAffinityValue    = "true"

	UserSuffix = "@iplantcollaborative.org"
)

func Int32Ptr(i int32) *int32 { return &i }
func Int64Ptr(i int64) *int64 { return &i }
