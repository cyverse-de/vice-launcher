package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/cyverse-de/model"

	"github.com/pkg/errors"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// RequestedStatus means the the transfer has been requested but hasn't started
	RequestedStatus = "requested"

	// DownloadingStatus means that a downloading request is running
	DownloadingStatus = "downloading"

	// UploadingStatus means that an uploading request is running
	UploadingStatus = "uploading"

	// FailedStatus means that the transfer request failed
	FailedStatus = "failed"

	//CompletedStatus means that the transfer request succeeded
	CompletedStatus = "completed"
)

type transferResponse struct {
	UUID   string `json:"uuid"`
	Status string `json:"status"`
	Kind   string `json:"kind"`
}

// fileTransferMountPath returns the path to the directory containing file inputs.
func fileTransfersMountPath(job *model.Job) string {
	return job.Steps[0].Component.Container.WorkingDirectory()
}

// fileTransferCommand returns a []string containing the command to fire up the vice-file-transfers service.
func fileTransferCommand(job *model.Job) []string {
	retval := []string{
		"/vice-file-transfers",
		"--listen-port", "60001",
		"--user", job.Submitter,
		"--excludes-file", path.Join(excludesMountPath, excludesFileName),
		"--path-list-file", path.Join(inputPathListMountPath, inputPathListFileName),
		"--upload-destination", job.OutputDirectory(),
		"--irods-config", irodsConfigFilePath,
		"--invocation-id", job.InvocationID,
	}
	for _, fm := range job.FileMetadata {
		retval = append(retval, fm.Argument()...)
	}
	return retval
}

// fileTransferVolumeMounts returns the list of VolumeMounts needed by the fileTransfer
// container in the VICE analysis pod. Each VolumeMount should correspond to one of the
// Volumes returned by the deploymentVolumes() function. This does not call the k8s API.
func (i *Internal) fileTransfersVolumeMounts(job *model.Job) []apiv1.VolumeMount {
	retval := []apiv1.VolumeMount{
		{
			Name:      porklockConfigVolumeName,
			MountPath: porklockConfigMountPath,
			ReadOnly:  true,
		},
		{
			Name:      fileTransfersVolumeName,
			MountPath: fileTransfersInputsMountPath,
			ReadOnly:  false,
		},
		{
			Name:      excludesVolumeName,
			MountPath: excludesMountPath,
			ReadOnly:  true,
		},
	}

	if len(job.FilterInputsWithoutTickets()) > 0 {
		retval = append(retval, apiv1.VolumeMount{
			Name:      inputPathListVolumeName,
			MountPath: inputPathListMountPath,
			ReadOnly:  true,
		})
	}

	return retval
}

func requestTransfer(svc apiv1.Service, reqpath string) (*transferResponse, error) {
	var (
		bodybytes []byte
		bodyerr   error
		jsonerr   error
		err       error
	)

	xferresp := &transferResponse{}
	svcurl := url.URL{}

	svcurl.Scheme = "http"
	svcurl.Host = fmt.Sprintf("%s.%s:%d", svc.Name, svc.Namespace, fileTransfersPort)
	svcurl.Path = reqpath

	resp, posterr := http.Post(svcurl.String(), "", nil)
	if posterr != nil {
		return nil, errors.Wrapf(posterr, "error POSTing to %s", svcurl.String())
	}
	if resp == nil {
		return nil, fmt.Errorf("response from %s was nil", svcurl.String())
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return nil, errors.Wrapf(posterr, "download request to %s returned %d", svcurl.String(), resp.StatusCode)
	}

	if bodybytes, bodyerr = ioutil.ReadAll(resp.Body); err != nil {
		return nil, errors.Wrapf(bodyerr, "reading body from %s failed", svcurl.String())
	}

	if jsonerr = json.Unmarshal(bodybytes, xferresp); jsonerr != nil {
		return nil, errors.Wrapf(jsonerr, "error unmarshalling json from %s", svcurl.String())
	}

	return xferresp, nil
}

func getTransferDetails(id string, svc apiv1.Service, reqpath string) (*transferResponse, error) {
	var (
		bodybytes []byte
		bodyerr   error
		jsonerr   error
		err       error
	)

	xferresp := &transferResponse{}
	svcurl := url.URL{}

	svcurl.Scheme = "http"
	svcurl.Host = fmt.Sprintf("%s.%s:%d", svc.Name, svc.Namespace, fileTransfersPort)
	svcurl.Path = reqpath

	resp, posterr := http.Get(svcurl.String())
	if posterr != nil {
		return nil, errors.Wrapf(posterr, "error on GET %s", svcurl.String())
	}
	if resp == nil {
		return nil, fmt.Errorf("response from GET %s was nil", svcurl.String())
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return nil, errors.Wrapf(posterr, "status request to %s returned %d", svcurl.String(), resp.StatusCode)
	}

	if bodybytes, bodyerr = ioutil.ReadAll(resp.Body); err != nil {
		return nil, errors.Wrapf(bodyerr, "reading body from %s failed", svcurl.String())
	}

	if jsonerr = json.Unmarshal(bodybytes, xferresp); jsonerr != nil {
		return nil, errors.Wrapf(jsonerr, "error unmarshalling json from %s", svcurl.String())
	}

	return xferresp, nil
}

func isFinished(status string) bool {
	switch status {
	case FailedStatus:
		return true
	case CompletedStatus:
		return true
	default:
		return false
	}
}

// doFileTransfer handles requests to initial file transfers for a VICE
// analysis. We only need the ID of the job, nothing is required in the
// body of the request.
func (i *Internal) doFileTransfer(externalID, reqpath, kind string, async bool) error {
	if i.UseCSIDriver {
		// if we use CSI Driver, file transfer is not required.
		msg := fmt.Sprintf("%s succeeded for job %s", kind, externalID)

		log.Info(msg)

		if successerr := i.statusPublisher.Running(externalID, msg); successerr != nil {
			log.Error(successerr)
		}

		return nil
	}

	log.Infof("starting %s transfers for job %s", kind, externalID)

	// Make sure that the list of services only comes from the VICE namespace.
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)

	// Filter the list of services so only those tagged with an external-id are
	// returned. external-id is the job ID assigned by the apps service and is
	// not the same as the analysis ID.
	set := labels.Set(map[string]string{
		"external-id": externalID,
	})

	svclist, err := svcclient.List(metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	})
	if err != nil {
		return err
	}

	if len(svclist.Items) < 1 {
		return fmt.Errorf("no services with a label of 'external-id=%s' were found", externalID)
	}

	// It's technically possibly for multiple services to provide file transfer services,
	// so we should block until all of them are complete. We're using a WaitGroup to
	// coordinate the file transfers, since they occur in separate goroutines.
	var wg sync.WaitGroup

	for _, svc := range svclist.Items {
		if !async {
			wg.Add(1)
		}

		go func(svc apiv1.Service) {
			if !async {
				defer wg.Done()
			}

			log.Infof("%s transfer for %s", kind, externalID)

			transferObj, xfererr := requestTransfer(svc, reqpath)
			if xfererr != nil {
				log.Error(xfererr)
				err = xfererr
				return
			}

			currentStatus := transferObj.Status

			var (
				sentUploadStatus   = false
				sentDownloadStatus = false
			)

			for !isFinished(currentStatus) {
				// Set it again here to catch the new values set farther down.
				currentStatus = transferObj.Status

				switch currentStatus {
				case FailedStatus:
					msg := fmt.Sprintf("%s failed for job %s", kind, externalID)

					err = errors.New(msg)

					log.Error(err)

					if failerr := i.statusPublisher.Running(externalID, msg); failerr != nil {
						log.Error(failerr)
					}

					return
				case CompletedStatus:
					msg := fmt.Sprintf("%s succeeded for job %s", kind, externalID)

					log.Info(msg)

					if successerr := i.statusPublisher.Running(externalID, msg); successerr != nil {
						log.Error(successerr)
					}

					return
				case RequestedStatus:
					msg := fmt.Sprintf("%s requested for job %s", kind, externalID)

					if requestederr := i.statusPublisher.Running(externalID, msg); requestederr != nil {
						log.Error(err)
					}

					break
				case UploadingStatus:
					if !sentUploadStatus {
						msg := fmt.Sprintf("%s is in progress for job %s", kind, externalID)

						log.Info(msg)

						if uploadingerr := i.statusPublisher.Running(externalID, msg); uploadingerr != nil {
							log.Error(err)
						}

						sentUploadStatus = true
					}
					break
				case DownloadingStatus:
					if !sentDownloadStatus {
						msg := fmt.Sprintf("%s is in progress for job %s", kind, externalID)

						log.Info(msg)

						if downloadingerr := i.statusPublisher.Running(externalID, msg); downloadingerr != nil {
							log.Error(err)
						}

						sentDownloadStatus = true
					}
					break
				default:
					err = fmt.Errorf("unknown status from %s: %s", svc.Spec.ClusterIP, transferObj.Status)

					log.Error(err)

					return // return and not break because we want to fail out
				}

				fullreqpath := path.Join(reqpath, transferObj.UUID)

				transferObj, xfererr = getTransferDetails(transferObj.UUID, svc, fullreqpath)
				if xfererr != nil {
					log.Error(errors.Wrapf(xfererr, "error getting transfer details for transferObj %s", fullreqpath))
					err = xfererr
					return
				}

				if transferObj == nil {
					log.Error("transferObj is nil")
					return
				}

				time.Sleep(5 * time.Second)
			}
		}(svc)
	}

	// Block until all of the file transfers are complete. There usually will only
	// be a single goroutine to wait for, but we should support more.
	if !async {
		wg.Wait()
	}

	return err
}
