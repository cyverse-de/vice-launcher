package internal

import (
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/permissions"
	"github.com/gosimple/slug"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"gopkg.in/cyverse-de/model.v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/labstack/echo/v4"
)

var log = common.Log

var leadingLabelReplacerRegexp = regexp.MustCompile("^[^0-9A-Za-z]+")
var trailingLabelReplacerRegexp = regexp.MustCompile("[^0-9A-Za-z]+$")

// labelReplacerFn returns a function that can be used to replace invalid leading and trailing characters
// in label values. Hyphens are replaced by the letter "h". Underscores are replaced by the letter "u".
// Other characters in the match are replaced by the empty string. The prefix and suffix are placed before
// and after the replacement, respectively.
func labelReplacerFn(prefix, suffix string) func(string) string {
	replacementFor := map[rune]string{
		'-': "h",
		'_': "u",
	}

	return func(match string) string {
		runes := []rune(match)
		elems := make([]string, len(runes))
		for i, c := range runes {
			elems[i] = replacementFor[c]
		}
		return prefix + strings.Join(elems, "-") + suffix
	}
}

// labelValueString returns a version of the given string that may be used as a value in a Kubernetes
// label. See: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/. Leading and
// trailing underscores and hyphens are replaced by sequences of `u` and `h`, separated by hyphens.
// These sequences are separated from the main part of the label value by `-xxx-`. This is kind of
// hokey, but it makes it at least fairly unlikely that we'll encounter collisions.
func labelValueString(str string) string {
	slug.MaxLength = 63
	str = leadingLabelReplacerRegexp.ReplaceAllStringFunc(str, labelReplacerFn("", "-xxx-"))
	str = trailingLabelReplacerRegexp.ReplaceAllStringFunc(str, labelReplacerFn("-xxx-", ""))
	return slug.Make(str)
}

// Init contains configuration for configuring an *Internal.
type Init struct {
	PorklockImage                 string
	PorklockTag                   string
	UseCSIDriver                  bool
	InputPathListIdentifier       string
	TicketInputPathListIdentifier string
	ViceProxyImage                string
	CASBaseURL                    string
	FrontendBaseURL               string
	ViceDefaultBackendService     string
	ViceDefaultBackendServicePort int
	GetAnalysisIDService          string
	CheckResourceAccessService    string
	VICEBackendNamespace          string
	AppsServiceBaseURL            string
	ViceNamespace                 string
	JobStatusURL                  string
	UserSuffix                    string
	PermissionsURL                string
	KeycloakBaseURL               string
	KeycloakRealm                 string
	KeycloakClientID              string
	KeycloakClientSecret          string
}

// Internal contains information and operations for launching VICE apps inside the
// local k8s cluster.
type Internal struct {
	Init
	clientset       kubernetes.Interface
	db              *sqlx.DB
	statusPublisher AnalysisStatusPublisher
}

// New creates a new *Internal.
func New(init *Init, db *sqlx.DB, clientset kubernetes.Interface) *Internal {
	return &Internal{
		Init:      *init,
		db:        db,
		clientset: clientset,
		statusPublisher: &JSLPublisher{
			statusURL: init.JobStatusURL,
		},
	}
}

// labelsFromJob returns a map[string]string that can be used as labels for K8s resources.
func (i *Internal) labelsFromJob(job *model.Job) (map[string]string, error) {
	name := []rune(job.Name)

	var stringmax int
	if len(name) >= 63 {
		stringmax = 62
	} else {
		stringmax = len(name) - 1
	}

	a := apps.NewApps(i.db, i.UserSuffix)
	ipAddr, err := a.GetUserIP(job.UserID)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"external-id":   job.InvocationID,
		"app-name":      labelValueString(job.AppName),
		"app-id":        job.AppID,
		"username":      labelValueString(job.Submitter),
		"user-id":       job.UserID,
		"analysis-name": labelValueString(string(name[:stringmax])),
		"app-type":      "interactive",
		"subdomain":     IngressName(job.UserID, job.InvocationID),
		"login-ip":      ipAddr,
	}, nil
}

// UpsertExcludesConfigMap uses the Job passed in to assemble the ConfigMap
// containing the files that should not be uploaded to iRODS. It then calls
// the k8s API to create the ConfigMap if it does not already exist or to
// update it if it does.
func (i *Internal) UpsertExcludesConfigMap(job *model.Job) error {
	excludesCM, err := i.excludesConfigMap(job)
	if err != nil {
		return err
	}

	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)

	_, err = cmclient.Get(excludesConfigMapName(job), metav1.GetOptions{})
	if err != nil {
		log.Info(err)
		_, err = cmclient.Create(excludesCM)
		if err != nil {
			return err
		}
	} else {
		_, err = cmclient.Update(excludesCM)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpsertInputPathListConfigMap uses the Job passed in to assemble the ConfigMap
// containing the path list of files to download from iRODS for the VICE analysis.
// It then uses the k8s API to create the ConfigMap if it does not already exist or to
// update it if it does.
func (i *Internal) UpsertInputPathListConfigMap(job *model.Job) error {
	inputCM, err := i.inputPathListConfigMap(job)
	if err != nil {
		return err
	}

	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)

	_, err = cmclient.Get(inputPathListConfigMapName(job), metav1.GetOptions{})
	if err != nil {
		_, err = cmclient.Create(inputCM)
		if err != nil {
			return err
		}
	} else {
		_, err = cmclient.Update(inputCM)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpsertDeployment uses the Job passed in to assemble a Deployment for the
// VICE analysis. If then uses the k8s API to create the Deployment if it does
// not already exist or to update it if it does.
func (i *Internal) UpsertDeployment(job *model.Job) error {
	deployment, err := i.getDeployment(job)
	if err != nil {
		return err
	}

	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	_, err = depclient.Get(job.InvocationID, metav1.GetOptions{})
	if err != nil {
		_, err = depclient.Create(deployment)
		if err != nil {
			return err
		}
	} else {
		_, err = depclient.Update(deployment)
		if err != nil {
			return err
		}
	}

	// Create the persistent volume and persistent volume claim for the job.
	volume, err := i.getPersistentVolume(job)
	if err != nil {
		return err
	}

	volumeclaim, err := i.getPersistentVolumeClaim(job)
	if err != nil {
		return err
	}

	if volume != nil {
		pvclient := i.clientset.CoreV1().PersistentVolumes()
		_, err = pvclient.Get(volume.GetName(), metav1.GetOptions{})
		if err != nil {
			_, err = pvclient.Create(volume)
			if err != nil {
				return err
			}
		} else {
			_, err = pvclient.Update(volume)
			if err != nil {
				return err
			}
		}
	}

	if volumeclaim != nil {
		pvcclient := i.clientset.CoreV1().PersistentVolumeClaims(i.ViceNamespace)
		_, err = pvcclient.Get(volumeclaim.GetName(), metav1.GetOptions{})
		if err != nil {
			_, err = pvcclient.Create(volumeclaim)
			if err != nil {
				return err
			}
		} else {
			_, err = pvcclient.Update(volumeclaim)
			if err != nil {
				return err
			}
		}
	}

	// Create the service for the job.
	svc, err := i.getService(job, deployment)
	if err != nil {
		return err
	}
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	_, err = svcclient.Get(job.InvocationID, metav1.GetOptions{})
	if err != nil {
		_, err = svcclient.Create(svc)
		if err != nil {
			return err
		}
	}

	// Create the ingress for the job
	ingress, err := i.getIngress(job, svc)
	if err != nil {
		return err
	}

	ingressclient := i.clientset.ExtensionsV1beta1().Ingresses(i.ViceNamespace)
	_, err = ingressclient.Get(ingress.Name, metav1.GetOptions{})
	if err != nil {
		_, err = ingressclient.Create(ingress)
		if err != nil {
			return err
		}
	}

	return nil
}

// LaunchAppHandler is the HTTP handler that orchestrates the launching of a VICE analysis inside
// the k8s cluster. This get passed to the router to be associated with a route. The Job
// is passed in as the body of the request.
func (i *Internal) LaunchAppHandler(c echo.Context) error {
	var (
		job *model.Job
		err error
	)

	job = &model.Job{}

	if err = c.Bind(job); err != nil {
		return err
	}

	if status, err := i.validateJob(job); err != nil {
		if validationErr, ok := err.(common.ErrorResponse); ok {
			return validationErr
		}
		return echo.NewHTTPError(status, err.Error())
	}

	// Create the excludes file ConfigMap for the job.
	if err = i.UpsertExcludesConfigMap(job); err != nil {
		return err
	}

	// Create the input path list config map
	if err = i.UpsertInputPathListConfigMap(job); err != nil {
		return err
	}

	// Create the deployment for the job.
	if err = i.UpsertDeployment(job); err != nil {
		return err
	}

	return nil
}

// TriggerDownloadsHandler handles requests to trigger file downloads.
func (i *Internal) TriggerDownloadsHandler(c echo.Context) error {
	return i.doFileTransfer(c.Param("id"), downloadBasePath, downloadKind, true)
}

// AdminTriggerDownloadsHandler handles requests to trigger file downloads
// without requiring user information in the request and also operates from
// the analysis UUID rather than the external ID. For use with tools that
// require the caller to have administrative privileges.
func (i *Internal) AdminTriggerDownloadsHandler(c echo.Context) error {
	var err error

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doFileTransfer(externalID, downloadBasePath, downloadKind, true)
}

// TriggerUploadsHandler handles requests to trigger file uploads.
func (i *Internal) TriggerUploadsHandler(c echo.Context) error {
	return i.doFileTransfer(c.Param("id"), uploadBasePath, uploadKind, true)
}

// AdminTriggerUploadsHandler handles requests to trigger file uploads without
// requiring user information in the request, while also operating from the
// analysis UUID rather than the external UUID. For use with tools that
// require the caller to have administrative privileges.
func (i *Internal) AdminTriggerUploadsHandler(c echo.Context) error {
	var err error

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doFileTransfer(externalID, uploadBasePath, uploadKind, true)
}

func (i *Internal) doExit(externalID string) error {
	set := labels.Set(map[string]string{
		"external-id": externalID,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// Delete the ingress
	ingressclient := i.clientset.ExtensionsV1beta1().Ingresses(i.ViceNamespace)
	ingresslist, err := ingressclient.List(listoptions)
	if err != nil {
		return err
	}

	for _, ingress := range ingresslist.Items {
		if err = ingressclient.Delete(ingress.Name, &metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the service
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(listoptions)
	if err != nil {
		return err
	}

	for _, svc := range svclist.Items {
		if err = svcclient.Delete(svc.Name, &metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(listoptions)
	if err != nil {
		return err
	}

	for _, dep := range deplist.Items {
		if err = depclient.Delete(dep.Name, &metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete volumes used by the deployment
	// Delete persistent volume claims.
	// This will automatically delete persistent volumes associated with them.
	pvcclient := i.clientset.CoreV1().PersistentVolumeClaims(i.ViceNamespace)
	pvclist, err := pvcclient.List(listoptions)
	if err != nil {
		return err
	}

	for _, pvc := range pvclist.Items {
		if err = pvcclient.Delete(pvc.Name, &metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the input files list and the excludes list config maps
	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)
	cmlist, err := cmclient.List(listoptions)
	if err != nil {
		return err
	}

	log.Infof("number of configmaps to be deleted for %s: %d", externalID, len(cmlist.Items))

	for _, cm := range cmlist.Items {
		log.Infof("deleting configmap %s for %s", cm.Name, externalID)
		if err = cmclient.Delete(cm.Name, &metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	return nil
}

// ExitHandler terminates the VICE analysis deployment and cleans up
// resources asscociated with it. Does not save outputs first. Uses
// the external-id label to find all of the objects in the configured
// namespace associated with the job. Deletes the following objects:
// ingresses, services, deployments, and configmaps.
func (i *Internal) ExitHandler(c echo.Context) error {
	return i.doExit(c.Param("id"))
}

// AdminExitHandler terminates the VICE analysis based on the analysisID and
// and should not require any user information to be provided. Otherwise, the
// documentation for VICEExit applies here as well.
func (i *Internal) AdminExitHandler(c echo.Context) error {
	var err error

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doExit(externalID)
}

// getIDFromHost returns the external ID for the running VICE app, which
// is assumed to be the same as the name of the ingress.
func (i *Internal) getIDFromHost(host string) (string, error) {
	ingressclient := i.clientset.ExtensionsV1beta1().Ingresses(i.ViceNamespace)
	ingresslist, err := ingressclient.List(metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, ingress := range ingresslist.Items {
		for _, rule := range ingress.Spec.Rules {
			if rule.Host == host {
				return ingress.Name, nil
			}
		}
	}

	return "", fmt.Errorf("no ingress found for host %s", host)
}

// URLReadyHandler returns whether or not a VICE app is ready
// for users to access it. This version will check the user's permissions
// and return an error if they aren't allowed to access the running app.
func (i *Internal) URLReadyHandler(c echo.Context) error {
	var (
		ingressExists bool
		serviceExists bool
		podReady      bool
	)

	user := c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user query parameter must be set")
	}

	// Since some usernames don't come through the labelling process unscathed, we have to use
	// the user ID.
	fixedUser := i.fixUsername(user)
	a := apps.NewApps(i.db, i.UserSuffix)
	_, err := a.GetUserID(fixedUser)
	if err != nil {
		if err == sql.ErrNoRows {
			return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("user %s not found", fixedUser))
		}
		return err
	}

	host := c.Param("host")

	// Use the name of the ingress to retrieve the externalID
	id, err := i.getIDFromHost(host)
	if err != nil {
		return err
	}

	// If getIDFromHost returns without an error, then the ingress exists
	// since the ingresses are looked at for the host.
	ingressExists = true

	set := labels.Set(map[string]string{
		"external-id": id,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// check the service existence
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(listoptions)
	if err != nil {
		return err
	}
	if len(svclist.Items) > 0 {
		serviceExists = true
	}

	// Check pod status through the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(listoptions)
	if err != nil {
		return err
	}
	for _, dep := range deplist.Items {
		if dep.Status.ReadyReplicas > 0 {
			podReady = true
		}
	}

	data := map[string]bool{
		"ready": ingressExists && serviceExists && podReady,
	}

	analysisID, err := a.GetAnalysisIDByExternalID(id)
	if err != nil {
		return err
	}

	// Make sure the user has permissions to look up info about this analysis.
	p := &permissions.Permissions{
		BaseURL: i.PermissionsURL,
	}

	allowed, err := p.IsAllowed(user, analysisID)
	if err != nil {
		return err
	}

	if !allowed {
		return echo.NewHTTPError(http.StatusForbidden, fmt.Sprintf("user %s cannot access analysis %s", user, analysisID))
	}

	return c.JSON(http.StatusOK, data)
}

// AdminURLReadyHandler handles requests to check the status of a running VICE app in K8s.
// This will return an overall status and status for the individual containers in
// the app's pod. Uses the state of the readiness checks in K8s, along with the
// existence of the various resources created for the app.
func (i *Internal) AdminURLReadyHandler(c echo.Context) error {
	var (
		ingressExists bool
		serviceExists bool
		podReady      bool
	)

	host := c.Param("host")

	// Use the name of the ingress to retrieve the externalID
	id, err := i.getIDFromHost(host)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}

	// If getIDFromHost returns without an error, then the ingress exists
	// since the ingresses are looked at for the host.
	ingressExists = true

	set := labels.Set(map[string]string{
		"external-id": id,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// check the service existence
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(listoptions)
	if err != nil {
		return err
	}
	if len(svclist.Items) > 0 {
		serviceExists = true
	}

	// Check pod status through the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(listoptions)
	if err != nil {
		return err
	}
	for _, dep := range deplist.Items {
		if dep.Status.ReadyReplicas > 0 {
			podReady = true
		}
	}

	data := map[string]bool{
		"ready": ingressExists && serviceExists && podReady,
	}

	return c.JSON(http.StatusOK, data)
}

// SaveAndExitHandler handles requests to save the output files in iRODS and then exit.
// The exit portion will only occur if the save operation succeeds. The operation is
// performed inside of a goroutine so that the caller isn't waiting for hours/days for
// output file transfers to complete.
func (i *Internal) SaveAndExitHandler(c echo.Context) error {
	log.Info("save and exit called")

	// Since file transfers can take a while, we should do this asynchronously by default.
	go func(c echo.Context) {
		var err error

		externalID := c.Param("id")

		log.Infof("calling doFileTransfer for %s", externalID)

		// Trigger a blocking output file transfer request.
		if err = i.doFileTransfer(externalID, uploadBasePath, uploadKind, false); err != nil {
			log.Error(errors.Wrap(err, "error doing file transfer")) // Log but don't exit. Possible to cancel a job that hasn't started yet
		}

		log.Infof("calling VICEExit for %s", externalID)

		if err = i.doExit(externalID); err != nil {
			log.Error(errors.Wrapf(err, "error triggering analysis exit for %s", externalID))
		}

		log.Infof("after VICEExit for %s", externalID)
	}(c)

	log.Info("leaving save and exit")

	return nil
}

// AdminSaveAndExitHandler handles requests to save the output files in iRODS and
// then exit. This version of the call operates based on the analysis ID and does
// not require user information to be required by the caller. Otherwise, the docs
// for the VICESaveAndExit function apply here as well.
func (i *Internal) AdminSaveAndExitHandler(c echo.Context) error {
	log.Info("admin save and exit called")

	// Since file transfers can take a while, we should do this asynchronously by default.
	go func(c echo.Context) {
		var (
			err        error
			externalID string
		)

		log.Debug("calling doFileTransfer")

		analysisID := c.Param("analysis-id")

		if externalID, err = i.getExternalIDByAnalysisID(analysisID); err != nil {
			log.Error(err)
			return
		}

		// Trigger a blocking output file transfer request.
		if err = i.doFileTransfer(externalID, uploadBasePath, uploadKind, false); err != nil {
			log.Error(errors.Wrap(err, "error doing file transfer")) // Log but don't exit. Possible to cancel a job that hasn't started yet
		}

		log.Debug("calling VICEExit")

		if err = i.doExit(externalID); err != nil {
			log.Error(err)
		}

		log.Debug("after VICEExit")
	}(c)

	log.Info("admin leaving save and exit")
	return nil
}

const updateTimeLimitSQL = `
	UPDATE ONLY jobs
	   SET planned_end_date = old_value.planned_end_date + interval '72 hours'
	  FROM (SELECT planned_end_date FROM jobs WHERE id = $2) AS old_value
	 WHERE jobs.id = $2
	   AND jobs.user_id = $1
 RETURNING jobs.planned_end_date
`

const getTimeLimitSQL = `
	SELECT planned_end_date
	  FROM jobs
	 WHERE jobs.id = $2
	   AND jobs.user_id = $1
`

const getUserIDSQL = `
	SELECT users.id
	  FROM users
	 WHERE username = $1
`

// TimeLimitUpdateHandler handles requests to update the time limit on an already running VICE app.
func (i *Internal) TimeLimitUpdateHandler(c echo.Context) error {
	log.Info("update time limit called")

	var (
		err  error
		id   string
		user string
	)

	// user is required
	user = c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusForbidden, "user is not set")
	}

	// id is required
	id = c.Param("analysis-id")
	if id == "" {
		idErr := echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
		log.Error(idErr)
		return idErr
	}

	outputMap, err := i.updateTimeLimit(user, id)
	if err != nil {
		log.Error(err)
		return err
	}

	return c.JSON(http.StatusOK, outputMap)

}

// AdminTimeLimitUpdateHandler is basically the same as VICETimeLimitUpdate
// except that it doesn't require user information in the request.
func (i *Internal) AdminTimeLimitUpdateHandler(c echo.Context) error {
	var (
		err  error
		id   string
		user string
	)
	// id is required
	id = c.Param("analysis-id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	apps := apps.NewApps(i.db, i.UserSuffix)

	user, _, err = apps.GetUserByAnalysisID(id)
	if err != nil {
		return err
	}

	outputMap, err := i.updateTimeLimit(user, id)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, outputMap)
}

// GetTimeLimitHandler implements the handler for getting the current time limit from the database.
func (i *Internal) GetTimeLimitHandler(c echo.Context) error {
	log.Info("get time limit called")

	var (
		err        error
		analysisID string
		user       string
		userID     string
	)

	// user is required
	user = c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusForbidden, "user is not set")
	}

	if !strings.HasSuffix(user, i.UserSuffix) {
		user = fmt.Sprintf("%s%s", user, i.UserSuffix)
	}

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	apps := apps.NewApps(i.db, i.UserSuffix)

	// Could use this to get the username, but we need to not break other services.
	_, userID, err = apps.GetUserByAnalysisID(analysisID)
	if err != nil {
		return err
	}

	outputMap, err := i.getTimeLimit(userID, analysisID)

	return c.JSON(http.StatusOK, outputMap)
}

// AdminGetTimeLimitHandler is the same as VICEGetTimeLimit but doesn't require
// any user information in the request.
func (i *Internal) AdminGetTimeLimitHandler(c echo.Context) error {
	log.Info("get time limit called")

	var (
		err        error
		analysisID string
		userID     string
	)

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	apps := apps.NewApps(i.db, i.UserSuffix)

	// Could use this to get the username, but we need to not break other services.
	_, userID, err = apps.GetUserByAnalysisID(analysisID)
	if err != nil {
		return err
	}

	outputMap, err := i.getTimeLimit(userID, analysisID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, outputMap)
}

func (i *Internal) getTimeLimit(userID, id string) (map[string]string, error) {
	var err error

	var timeLimit pq.NullTime
	if err = i.db.QueryRow(getTimeLimitSQL, userID, id).Scan(&timeLimit); err != nil {
		return nil, errors.Wrapf(err, "error retrieving time limit for user %s on analysis %s", userID, id)
	}

	outputMap := map[string]string{}
	if timeLimit.Valid {
		v, err := timeLimit.Value()
		if err != nil {
			return nil, errors.Wrapf(err, "error getting time limit for user %s on analysis %s", userID, id)
		}
		outputMap["time_limit"] = fmt.Sprintf("%d", v.(time.Time).Unix())
	} else {
		outputMap["time_limit"] = "null"
	}

	return outputMap, nil
}

func (i *Internal) updateTimeLimit(user, id string) (map[string]string, error) {
	var (
		err    error
		userID string
	)

	if !strings.HasSuffix(user, userSuffix) {
		user = fmt.Sprintf("%s%s", user, userSuffix)
	}

	if err = i.db.QueryRow(getUserIDSQL, user).Scan(&userID); err != nil {
		return nil, errors.Wrapf(err, "error looking user ID for %s", user)
	}

	var newTimeLimit pq.NullTime
	if err = i.db.QueryRow(updateTimeLimitSQL, userID, id).Scan(&newTimeLimit); err != nil {
		return nil, errors.Wrapf(err, "error extending time limit for user %s on analysis %s", userID, id)
	}

	outputMap := map[string]string{}
	if newTimeLimit.Valid {
		v, err := newTimeLimit.Value()
		if err != nil {
			return nil, errors.Wrapf(err, "error getting new time limit for user %s on analysis %s", userID, id)
		}
		outputMap["time_limit"] = fmt.Sprintf("%d", v.(time.Time).Unix())
	} else {
		return nil, errors.Wrapf(err, "the time limit for analysis %s was null after extension", id)
	}

	return outputMap, nil
}

// AdminGetExternalIDHandler returns the external ID associated with the analysis ID.
// There is only one external ID for each VICE analysis, unlike non-VICE analyses.
func (i *Internal) AdminGetExternalIDHandler(c echo.Context) error {
	var (
		err        error
		analysisID string
		externalID string
	)

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	externalID, err = i.getExternalIDByAnalysisID(analysisID)
	if err != nil {
		return err
	}

	outputMap := map[string]string{
		"externalID": externalID,
	}

	return c.JSON(http.StatusOK, outputMap)
}
