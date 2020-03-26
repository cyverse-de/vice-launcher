package internal

const (
	analysisContainerName = "analysis"

	porklockConfigVolumeName = "porklock-config"
	porklockConfigSecretName = "porklock-config"
	porklockConfigMountPath  = "/etc/porklock"

	fileTransfersVolumeName        = "input-files"
	fileTransfersContainerName     = "input-files"
	fileTransfersInitContainerName = "input-files-init"
	fileTransfersInputsMountPath   = "/input-files"

	viceProxyContainerName = "vice-proxy"
	viceProxyPort          = int32(60002)
	viceProxyPortName      = "tcp-proxy"
	viceProxyServicePort   = int32(60000)

	excludesMountPath  = "/excludes"
	excludesFileName   = "excludes-file"
	excludesVolumeName = "excludes-file"

	inputPathListMountPath  = "/input-paths"
	inputPathListFileName   = "input-path-list"
	inputPathListVolumeName = "input-path-list"

	irodsConfigFilePath = "/etc/porklock/irods-config.properties"

	fileTransfersPortName = "tcp-input"
	fileTransfersPort     = int32(60001)

	downloadBasePath = "/download"
	uploadBasePath   = "/upload"
	downloadKind     = "download"
	uploadKind       = "upload"

	viceTolerationKey      = "vice"
	viceTolerationOperator = "Equal"
	viceTolerationValue    = "only"
	viceTolerationEffect   = "NoSchedule"

	gpuTolerationKey      = "gpu"
	gpuTolerationOperator = "Equal"
	gpuTolerationValue    = "true"
	gpuTolerationEffect   = "NoSchedule"

	viceAffinityKey      = "vice"
	viceAffinityOperator = "In"
	viceAffinityValue    = "true"

	gpuAffinityKey      = "gpu"
	gpuAffinityOperator = "In"
	gpuAffinityValue    = "true"
)

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }
