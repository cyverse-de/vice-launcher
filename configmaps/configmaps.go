package configmaps

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cyverse-de/model"
	"github.com/cyverse-de/vice-launcher/common"
	"github.com/cyverse-de/vice-launcher/config"
	"github.com/cyverse-de/vice-launcher/constants"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExcludesConfigMapName returns the name of the ConfigMap containing the list
// of paths that should be excluded from file uploads to iRODS by porklock.
func ExcludesConfigMapName(job *model.Job) string {
	return fmt.Sprintf("excludes-file-%s", job.InvocationID)
}

// excludesFileContents returns a *bytes.Buffer containing the contents of an
// file exclusion list that gets passed to porklock to prevent it from uploading
// content. It's possible that the buffer is empty, but it shouldn't be nil.
func excludesFileContents(job *model.Job) *bytes.Buffer {
	var output bytes.Buffer

	for _, p := range job.ExcludeArguments() {
		output.WriteString(fmt.Sprintf("%s\n", p))
	}
	return &output
}

type ConfigMapMaker struct {
	labeller                      common.JobLabeller
	InputPathListIdentifier       string
	TicketInputPathListIdentifier string
}

func New(init *config.Init, labeller common.JobLabeller) *ConfigMapMaker {
	return &ConfigMapMaker{
		InputPathListIdentifier:       init.InputPathListIdentifier,
		TicketInputPathListIdentifier: init.TicketInputPathListIdentifier,
		labeller:                      labeller,
	}
}

// excludesConfigMap returns the ConfigMap containing the list of paths
// that should be excluded from file uploads to iRODS by porklock. This does NOT
// call the k8s API to actually create the ConfigMap, just returns the object
// that can be passed to the API.
func (c *ConfigMapMaker) ExcludesConfigMap(ctx context.Context, job *model.Job) (*apiv1.ConfigMap, error) {
	labels, err := c.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   ExcludesConfigMapName(job),
			Labels: labels,
		},
		Data: map[string]string{
			constants.ExcludesFileName: excludesFileContents(job).String(),
		},
	}, nil
}

// InputPathListConfigMapName returns the name of the ConfigMap containing
// the list of paths that should be downloaded from iRODS by porklock
// as input files for the VICE analysis.
func InputPathListConfigMapName(job *model.Job) string {
	return fmt.Sprintf("input-path-list-%s", job.InvocationID)
}

// inputPathListContents returns a *bytes.Buffer containing the contents of a
// input path list file. Does not write out the contents to a file. Returns
// (nil, nil) if there aren't any inputs without tickets associated with the
// Job.
func inputPathListContents(job *model.Job, pathListIdentifier, ticketsPathListIdentifier string) (*bytes.Buffer, error) {
	buffer := bytes.NewBufferString("")

	// Add the path list identifier.
	_, err := fmt.Fprintf(buffer, "%s\n", pathListIdentifier)
	if err != nil {
		return nil, err
	}

	// Add the list of paths.
	for _, input := range job.FilterInputsWithoutTickets() {
		_, err = fmt.Fprintf(buffer, "%s\n", input.IRODSPath())
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

// inputPathListConfigMap returns the ConfigMap object containing the the
// list of paths that should be downloaded from iRODS by porklock as input
// files for the VICE analysis. This does NOT call the k8s API to actually
// create the ConfigMap, just returns the object that can be passed to the API.
func (c *ConfigMapMaker) InputPathListConfigMap(ctx context.Context, job *model.Job) (*apiv1.ConfigMap, error) {
	labels, err := c.labeller.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	fileContents, err := inputPathListContents(job, c.InputPathListIdentifier, c.TicketInputPathListIdentifier)
	if err != nil {
		return nil, err
	}

	return &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   InputPathListConfigMapName(job),
			Labels: labels,
		},
		Data: map[string]string{
			constants.InputPathListFileName: fileContents.String(),
		},
	}, nil
}
