package internal

import (
	"fmt"
	"net/http"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

// GetAsyncData returns data that is generately asynchronously from the job launch.
func (i *Internal) GetAsyncData(c echo.Context) error {
	externalID := c.QueryParam("external-id")
	if externalID == "" {
		return &echo.HTTPError{
			Code:     http.StatusBadRequest,
			Internal: errors.New("external-id not set"),
		}
	}

	apps := apps.NewApps(i.db)

	analysisID, err := apps.GetAnalysisIDByExternalID(externalID)
	if err != nil {
		log.Error(err)
		return err
	}

	filter := map[string]string{
		"external-id": externalID,
	}

	deployments, err := i.deploymentList(i.ViceNamespace, filter)
	if err != nil {
		return err
	}

	if len(deployments.Items) < 1 {
		msg := "no deployments found."
		return &echo.HTTPError{
			Code:     http.StatusNotFound,
			Internal: errors.New(msg),
			Message:  msg,
		}
	}

	labels := deployments.Items[0].GetLabels()
	userID := labels["user-id"]

	subdomain := IngressName(userID, externalID)
	ipAddr, err := apps.GetUserIP(userID)
	if err != nil {
		log.Error(err)
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{
		"analysisID": analysisID,
		"subdomain":  subdomain,
		"ipAddr":     ipAddr,
	})
}

// getExternalID returns the externalID associated with the analysisID. For now,
// only returns the first result, since VICE analyses only have a single step in
// the database.
func (i *Internal) getExternalIDByAnalysisID(analysisID string) (string, error) {
	apps := apps.NewApps(i.db)
	username, _, err := apps.GetUserByAnalysisID(analysisID)
	if err != nil {
		return "", err
	}

	log.Infof("username %s", username)

	externalIDs, err := i.getExternalIDs(username, analysisID)
	if err != nil {
		return "", err
	}

	if len(externalIDs) == 0 {
		return "", fmt.Errorf("no external-id found for analysis-id %s", analysisID)
	}

	// For now, just use the first external ID
	externalID := externalIDs[0]
	return externalID, nil
}
