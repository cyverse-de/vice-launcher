package internal

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
)

// GetAsyncData returns data that is generately asynchronously from the job launch.
func (i *Internal) GetAsyncData(writer http.ResponseWriter, request *http.Request) {
	externalIDs, found := request.URL.Query()["external-id"]
	if !found || len(externalIDs) < 1 {
		common.Error(writer, "external-id not set", http.StatusBadRequest)
		return
	}

	externalID := externalIDs[0]

	apps := apps.NewApps(i.db)

	analysisID, err := apps.GetAnalysisIDByExternalID(externalID)
	if err != nil {
		log.Error(err)
		common.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	filter := map[string]string{
		"external-id": externalID,
	}

	deployments, err := i.deploymentList(i.ViceNamespace, filter)
	if err != nil {
		common.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(deployments.Items) < 1 {
		common.Error(writer, "no deployments found", http.StatusInternalServerError)
		return
	}

	labels := deployments.Items[0].GetLabels()
	userID := labels["user-id"]

	subdomain := IngressName(userID, externalID)
	ipAddr, err := apps.GetUserIP(userID)
	if err != nil {
		log.Error(err)
		common.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string]string{
		"analysisID": analysisID,
		"subdomain":  subdomain,
		"ipAddr":     ipAddr,
	})
	if err != nil {
		common.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
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
