package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/pkg/errors"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	extv1b1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func getListOptions(customLabels map[string]string) metav1.ListOptions {
	allLabels := map[string]string{
		"app-type": "interactive",
	}

	for k, v := range customLabels {
		allLabels[k] = v
	}

	set := labels.Set(allLabels)

	return metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}
}

func (i *Internal) deploymentList(namespace string, customLabels map[string]string) (*v1.DeploymentList, error) {
	listOptions := getListOptions(customLabels)

	depList, err := i.clientset.AppsV1().Deployments(namespace).List(listOptions)
	if err != nil {
		return nil, err
	}

	return depList, nil
}

func (i *Internal) podList(namespace string, customLabels map[string]string) (*corev1.PodList, error) {
	listOptions := getListOptions(customLabels)

	podList, err := i.clientset.CoreV1().Pods(namespace).List(listOptions)
	if err != nil {
		return nil, err
	}

	return podList, nil
}

func (i *Internal) configmapsList(namespace string, customLabels map[string]string) (*corev1.ConfigMapList, error) {
	listOptions := getListOptions(customLabels)

	cfgList, err := i.clientset.CoreV1().ConfigMaps(namespace).List(listOptions)
	if err != nil {
		return nil, err
	}

	return cfgList, nil
}

func (i *Internal) serviceList(namespace string, customLabels map[string]string) (*corev1.ServiceList, error) {
	listOptions := getListOptions(customLabels)

	svcList, err := i.clientset.CoreV1().Services(namespace).List(listOptions)
	if err != nil {
		return nil, err
	}

	return svcList, nil
}

func (i *Internal) ingressList(namespace string, customLabels map[string]string) (*extv1b1.IngressList, error) {
	listOptions := getListOptions(customLabels)

	ingList, err := i.clientset.ExtensionsV1beta1().Ingresses(namespace).List(listOptions)
	if err != nil {
		return nil, err
	}

	return ingList, nil
}

func filterMap(values url.Values) map[string]string {
	q := map[string]string{}

	for k, v := range values {
		q[k] = v[0]
	}

	return q
}

// MetaInfo contains useful information provided by multiple resource types.
type MetaInfo struct {
	Name              string `json:"name"`
	Namespace         string `json:"namespace"`
	AnalysisName      string `json:"analysisName"`
	AppName           string `json:"appName"`
	AppID             string `json:"appID"`
	ExternalID        string `json:"externalID"`
	UserID            string `json:"userID"`
	Username          string `json:"username"`
	CreationTimestamp string `json:"creationTimestamp"`
}

// DeploymentInfo contains information returned about a Deployment.
type DeploymentInfo struct {
	MetaInfo
	Image   string   `json:"image"`
	Command []string `json:"command"`
	Port    int32    `json:"port"`
	User    int64    `json:"user"`
	Group   int64    `json:"group"`
}

func deploymentInfo(deployment *v1.Deployment) *DeploymentInfo {
	var (
		user    int64
		group   int64
		image   string
		port    int32
		command []string
	)

	labels := deployment.GetObjectMeta().GetLabels()
	containers := deployment.Spec.Template.Spec.Containers

	for _, container := range containers {
		if container.Name == "analysis" {
			image = container.Image
			command = container.Command
			port = container.Ports[0].ContainerPort
			user = *container.SecurityContext.RunAsUser
			group = *container.SecurityContext.RunAsGroup
		}

	}

	return &DeploymentInfo{
		MetaInfo: MetaInfo{
			Name:              deployment.GetName(),
			Namespace:         deployment.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: deployment.GetCreationTimestamp().String(),
		},

		Image:   image,
		Command: command,
		Port:    port,
		User:    user,
		Group:   group,
	}
}

// PodInfo tracks information about the pods for a VICE analysis.
type PodInfo struct {
	MetaInfo
	Phase                 string                   `json:"phase"`
	Message               string                   `json:"message"`
	Reason                string                   `json:"reason"`
	ContainerStatuses     []corev1.ContainerStatus `json:"containerStatuses"`
	InitContainerStatuses []corev1.ContainerStatus `json:"initContainerStatuses"`
}

func podInfo(pod *corev1.Pod) *PodInfo {
	labels := pod.GetObjectMeta().GetLabels()

	return &PodInfo{
		MetaInfo: MetaInfo{
			Name:              pod.GetName(),
			Namespace:         pod.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: pod.GetCreationTimestamp().String(),
		},
		Phase:                 string(pod.Status.Phase),
		Message:               pod.Status.Message,
		Reason:                pod.Status.Reason,
		ContainerStatuses:     pod.Status.ContainerStatuses,
		InitContainerStatuses: pod.Status.InitContainerStatuses,
	}
}

// ConfigMapInfo contains useful info about a config map.
type ConfigMapInfo struct {
	MetaInfo
	Data map[string]string `json:"data"`
}

func configMapInfo(cm *corev1.ConfigMap) *ConfigMapInfo {
	labels := cm.GetObjectMeta().GetLabels()

	return &ConfigMapInfo{
		MetaInfo: MetaInfo{
			Name:              cm.GetName(),
			Namespace:         cm.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: cm.GetCreationTimestamp().String(),
		},
		Data: cm.Data,
	}
}

// ServiceInfoPort contains information about a service's Port.
type ServiceInfoPort struct {
	Name           string `json:"name"`
	NodePort       int32  `json:"nodePort"`
	TargetPort     int32  `json:"targetPort"`
	TargetPortName string `json:"targetPortName"`
	Port           int32  `json:"port"`
	Protocol       string `json:"protocol"`
}

//ServiceInfo contains info about a service
type ServiceInfo struct {
	MetaInfo
	Ports []ServiceInfoPort `json:"ports"`
}

func serviceInfo(svc *corev1.Service) *ServiceInfo {
	labels := svc.GetObjectMeta().GetLabels()

	ports := svc.Spec.Ports
	svcInfoPorts := []ServiceInfoPort{}

	for _, port := range ports {
		svcInfoPorts = append(svcInfoPorts, ServiceInfoPort{
			Name:           port.Name,
			NodePort:       port.NodePort,
			TargetPort:     port.TargetPort.IntVal,
			TargetPortName: port.TargetPort.String(),
			Port:           port.Port,
			Protocol:       string(port.Protocol),
		})
	}

	return &ServiceInfo{
		MetaInfo: MetaInfo{
			Name:              svc.GetName(),
			Namespace:         svc.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: svc.GetCreationTimestamp().String(),
		},

		Ports: svcInfoPorts,
	}
}

// IngressInfo contains useful Ingress VICE info.
type IngressInfo struct {
	MetaInfo
	DefaultBackend string                `json:"defaultBackend"`
	Rules          []extv1b1.IngressRule `json:"rules"`
}

func ingressInfo(ingress *extv1b1.Ingress) *IngressInfo {
	labels := ingress.GetObjectMeta().GetLabels()

	return &IngressInfo{
		MetaInfo: MetaInfo{
			Name:              ingress.GetName(),
			Namespace:         ingress.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: ingress.GetCreationTimestamp().String(),
		},
		Rules: ingress.Spec.Rules,
		DefaultBackend: fmt.Sprintf(
			"%s:%d",
			ingress.Spec.Backend.ServiceName,
			ingress.Spec.Backend.ServicePort.IntValue(),
		),
	}
}

func (i *Internal) getFilteredDeployments(filter map[string]string) ([]DeploymentInfo, error) {
	depList, err := i.deploymentList(i.ViceNamespace, filter)
	if err != nil {
		return nil, err
	}

	deployments := []DeploymentInfo{}

	for _, dep := range depList.Items {
		info := deploymentInfo(&dep)
		deployments = append(deployments, *info)
	}

	return deployments, nil
}

// FilterableDeployments lists all of the deployments.
func (i *Internal) FilterableDeployments(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	deployments, err := i.getFilteredDeployments(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string][]DeploymentInfo{
		"deployments": deployments,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

func (i *Internal) getFilteredPods(filter map[string]string) ([]PodInfo, error) {
	podList, err := i.podList(i.ViceNamespace, filter)
	if err != nil {
		return nil, err
	}

	pods := []PodInfo{}

	for _, pod := range podList.Items {
		info := podInfo(&pod)
		pods = append(pods, *info)
	}

	return pods, nil
}

// FilterablePods returns a listing of the pods in a VICE analysis.
func (i *Internal) FilterablePods(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	pods, err := i.getFilteredPods(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string][]PodInfo{
		"pods": pods,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

func (i *Internal) getFilteredConfigMaps(filter map[string]string) ([]ConfigMapInfo, error) {
	cmList, err := i.configmapsList(i.ViceNamespace, filter)
	if err != nil {
		return nil, err
	}

	cms := []ConfigMapInfo{}

	for _, cm := range cmList.Items {
		info := configMapInfo(&cm)
		cms = append(cms, *info)
	}

	return cms, nil
}

// FilterableConfigMaps lists configmaps in use by VICE apps.
func (i *Internal) FilterableConfigMaps(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	cms, err := i.getFilteredConfigMaps(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string][]ConfigMapInfo{
		"configmaps": cms,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

func (i *Internal) getFilteredServices(filter map[string]string) ([]ServiceInfo, error) {
	svcList, err := i.serviceList(i.ViceNamespace, filter)
	if err != nil {
		return nil, err
	}

	svcs := []ServiceInfo{}

	for _, svc := range svcList.Items {
		info := serviceInfo(&svc)
		svcs = append(svcs, *info)
	}

	return svcs, nil
}

// FilterableServices lists services in use by VICE apps.
func (i *Internal) FilterableServices(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	svcs, err := i.getFilteredServices(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string][]ServiceInfo{
		"services": svcs,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

func (i *Internal) getFilteredIngresses(filter map[string]string) ([]IngressInfo, error) {
	ingList, err := i.ingressList(i.ViceNamespace, filter)
	if err != nil {
		return nil, err
	}

	ingresses := []IngressInfo{}

	for _, ingress := range ingList.Items {
		info := ingressInfo(&ingress)
		ingresses = append(ingresses, *info)
	}

	return ingresses, nil
}

//FilterableIngresses lists ingresses in use by VICE apps.
func (i *Internal) FilterableIngresses(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	ingresses, err := i.getFilteredIngresses(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string][]IngressInfo{
		"ingresses": ingresses,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

// ResourceInfo contains all of the k8s resource information about a running VICE analysis
// that we know of and care about.
type ResourceInfo struct {
	Deployments []DeploymentInfo `json:"deployments"`
	Pods        []PodInfo        `json:"pods"`
	ConfigMaps  []ConfigMapInfo  `json:"configMaps"`
	Services    []ServiceInfo    `json:"services"`
	Ingresses   []IngressInfo    `json:"ingresses"`
}

// FilterableResources returns all of the k8s resources associated with a VICE analysis.
func (i *Internal) FilterableResources(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	filter := filterMap(request.URL.Query())

	deployments, err := i.getFilteredDeployments(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	pods, err := i.getFilteredPods(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	cms, err := i.getFilteredConfigMaps(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	svcs, err := i.getFilteredServices(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	ingresses, err := i.getFilteredIngresses(filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(ResourceInfo{
		Deployments: deployments,
		Pods:        pods,
		ConfigMaps:  cms,
		Services:    svcs,
		Ingresses:   ingresses,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}

func populateAnalysisID(a *apps.Apps, existingLabels map[string]string) (map[string]string, error) {
	if _, ok := existingLabels["analysis-id"]; !ok {
		externalID, ok := existingLabels["external-id"]
		if !ok {
			return existingLabels, fmt.Errorf("missing external-id key")
		}
		analysisID, err := a.GetAnalysisIDByExternalID(externalID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error getting analysis id for external id %s", externalID))
		} else {
			existingLabels["analysis-id"] = analysisID
		}
	}
	return existingLabels, nil
}

func populateSubdomain(existingLabels map[string]string) map[string]string {
	if _, ok := existingLabels["subdomain"]; !ok {
		if externalID, ok := existingLabels["external-id"]; ok {
			if userID, ok := existingLabels["user-id"]; ok {
				existingLabels["subdomain"] = IngressName(userID, externalID)
			}
		}
	}

	return existingLabels
}

func populateLoginIP(a *apps.Apps, existingLabels map[string]string) (map[string]string, error) {
	if _, ok := existingLabels["login-ip"]; !ok {
		if userID, ok := existingLabels["user-id"]; ok {
			ipAddr, err := a.GetUserIP(userID)
			if err != nil {
				return existingLabels, err
			}
			existingLabels["login-ip"] = ipAddr
		}
	}

	return existingLabels, nil
}

func (i *Internal) relabelDeployments() []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	a := apps.NewApps(i.db)

	deployments, err := i.deploymentList(i.ViceNamespace, filter)
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, deployment := range deployments.Items {
		existingLabels := deployment.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		deployment.SetLabels(existingLabels)
		_, err = i.clientset.AppsV1().Deployments(i.ViceNamespace).Update(&deployment)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Internal) relabelConfigMaps() []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	a := apps.NewApps(i.db)

	cms, err := i.configmapsList(i.ViceNamespace, filter)
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, configmap := range cms.Items {
		existingLabels := configmap.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		configmap.SetLabels(existingLabels)
		_, err = i.clientset.CoreV1().ConfigMaps(i.ViceNamespace).Update(&configmap)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Internal) relabelServices() []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	a := apps.NewApps(i.db)

	svcs, err := i.serviceList(i.ViceNamespace, filter)
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, service := range svcs.Items {
		existingLabels := service.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		service.SetLabels(existingLabels)
		_, err = i.clientset.CoreV1().Services(i.ViceNamespace).Update(&service)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Internal) relabelIngresses() []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	a := apps.NewApps(i.db)

	ingresses, err := i.ingressList(i.ViceNamespace, filter)
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, ingress := range ingresses.Items {
		existingLabels := ingress.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(a, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		ingress.SetLabels(existingLabels)
		_, err = i.clientset.ExtensionsV1beta1().Ingresses(i.ViceNamespace).Update(&ingress)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// ApplyAsyncLabels ensures that the required labels are applied to all running VICE analyses.
// This is useful to avoid race conditions between the DE database and the k8s cluster,
// and also for adding new labels to "old" analyses during an update.
func (i *Internal) ApplyAsyncLabels() []error {
	errors := []error{}

	labelDepsErrors := i.relabelDeployments()
	if len(labelDepsErrors) > 0 {
		for _, e := range labelDepsErrors {
			errors = append(errors, e)
		}
	}

	labelCMErrors := i.relabelConfigMaps()
	if len(labelCMErrors) > 0 {
		for _, e := range labelCMErrors {
			errors = append(errors, e)
		}
	}

	labelSVCErrors := i.relabelServices()
	if len(labelSVCErrors) > 0 {
		for _, e := range labelSVCErrors {
			errors = append(errors, e)
		}
	}

	labelIngressesErrors := i.relabelIngresses()
	if len(labelIngressesErrors) > 0 {
		for _, e := range labelIngressesErrors {
			errors = append(errors, e)
		}
	}

	return errors
}

// ApplyAsyncLabelsHandler is the http handler for triggering the application
// of labels on running VICE analyses.
func (i *Internal) ApplyAsyncLabelsHandler(writer http.ResponseWriter, request *http.Request) {
	errs := i.ApplyAsyncLabels()

	if len(errs) > 0 {
		var errMsg strings.Builder
		for _, err := range errs {
			log.Error(err)
			fmt.Fprintf(&errMsg, "%s\n", err.Error())
		}

		http.Error(writer, errMsg.String(), http.StatusInternalServerError)
	}
}

// GetAsyncData returns the data that would be applied as labels as a
// JSON-encoded map instead.
func (i *Internal) GetAsyncData(writer http.ResponseWriter, request *http.Request) {
	externalIDs, externalIDsFound := request.URL.Query()["external-id"]
	if !externalIDsFound || len(externalIDs) < 1 {
		http.Error(writer, "external-id not set", http.StatusBadRequest)
		return
	}

	externalID := externalIDs[0]

	users, usersFound := request.URL.Query()["username"]

	if !usersFound || len(users) < 1 {
		http.Error(writer, "user not set", http.StatusForbidden)
		return
	}

	user := users[0]

	apps := apps.NewApps(i.db)

	analysisID, err := apps.GetAnalysisIDByExternalID(externalID)
	if err != nil {
		log.Error(err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	filter := map[string]string{
		"external-id": externalID,
		"username":    user,
	}

	deployments, err := i.deploymentList(i.ViceNamespace, filter)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(deployments.Items) < 1 {
		http.Error(writer, "no deployments found", http.StatusInternalServerError)
		return
	}

	labels := deployments.Items[0].GetLabels()
	userID := labels["user-id"]

	subdomain := IngressName(userID, externalID)
	ipAddr, err := apps.GetUserIP(userID)
	if err != nil {
		log.Error(err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, err := json.Marshal(map[string]string{
		"analysisID": analysisID,
		"subdomain":  subdomain,
		"ipAddr":     ipAddr,
	})
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	fmt.Fprintf(writer, string(buf))
}
