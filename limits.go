package internal

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/pkg/errors"
	"gopkg.in/cyverse-de/model.v4"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func shouldCountStatus(status string) bool {
	countIt := true

	skipStatuses := []string{
		"Failed",
		"Completed",
		"Canceled",
	}

	for _, s := range skipStatuses {
		if status == s {
			countIt = false
		}
	}

	return countIt
}

func (i *Internal) countJobsForUser(username string) (int, error) {
	set := labels.Set(map[string]string{
		"username": username,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(listoptions)
	if err != nil {
		return 0, err
	}

	countedDeployments := []v1.Deployment{}
	a := apps.NewApps(i.db)

	for _, deployment := range deplist.Items {
		var (
			externalID, analysisID, analysisStatus string
			ok                                     bool
		)

		labels := deployment.GetLabels()

		// If we don't have the external-id on the deployment, count it.
		if externalID, ok = labels["external-id"]; !ok {
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		if analysisID, err = a.GetAnalysisIDByExternalID(externalID); err != nil {
			// If we failed to get it from the database, count it because it
			// shouldn't be running.
			log.Error(err)
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		analysisStatus, err = a.GetAnalysisStatus(analysisID)
		if err != nil {
			// If we failed to get the status, then something is horribly wrong.
			// Count the analysis.
			log.Error(err)
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		// If the running state is Failed, Completed, or Canceled, don't
		// count it because it's probably in the process of shutting down
		// or the database and the cluster are out of sync which is not
		// the user's fault.
		if shouldCountStatus(analysisStatus) {
			countedDeployments = append(countedDeployments, deployment)
		}
	}

	return len(countedDeployments), nil
}

const getJobLimitForUserSQL = `
	SELECT concurrent_jobs FROM job_limits
	WHERE launcher = $1
`

func (i *Internal) getJobLimitForUser(username string) (*int, error) {
	var jobLimit int
	err := i.db.QueryRow(getJobLimitForUserSQL, username).Scan(&jobLimit)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &jobLimit, nil
}

const getDefaultJobLimitSQL = `
	SELECT concurrent_jobs FROM job_limits
	WHERE launcher IS NULL
`

func (i *Internal) getDefaultJobLimit() (int, error) {
	var defaultJobLimit int
	if err := i.db.QueryRow(getDefaultJobLimitSQL).Scan(&defaultJobLimit); err != nil {
		return 0, err
	}
	return defaultJobLimit, nil
}

func (i *Internal) validateJob(job *model.Job) (int, error) {

	// Verify that the job type is supported by this service
	if strings.ToLower(job.ExecutionTarget) != "interapps" {
		return http.StatusInternalServerError, fmt.Errorf("job type %s is not supported by this service", job.Type)
	}

	// Get the username
	user := slugString(job.Submitter)

	// Verify that the user hasn't exceeded their limit for the number of concurrent jobs.
	jobCount, err := i.countJobsForUser(user)
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the number of jobs the %s is currently running", user)
	}
	jobLimit, err := i.getJobLimitForUser(user)
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the concurrent job limit for %s", user)
	}
	defaultJobLimit, err := i.getDefaultJobLimit()
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the default concurrent job limit")
	}
	var effectiveJobLimit int
	if jobLimit != nil {
		effectiveJobLimit = *jobLimit
	} else {
		effectiveJobLimit = defaultJobLimit
	}

	// Return a detailed error if the user has exceeded thier job limit.
	if jobCount >= effectiveJobLimit {
		return http.StatusBadRequest, common.ErrorResponse{
			Message: fmt.Sprintf("%s is already running %d or more concurrent jobs", user, jobLimit),
			Details: &map[string]interface{}{
				"defaultJobLimit": defaultJobLimit,
				"jobCount":        jobCount,
				"jobLimit":        jobLimit,
			},
		}
	}

	return http.StatusOK, nil
}
