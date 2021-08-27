package internal

import (
	"crypto/sha256"
	"fmt"

	"github.com/cyverse-de/model"
	apiv1 "k8s.io/api/core/v1"
	extv1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// IngressName returns the name of the ingress created for the running VICE
// analysis. This should match the name created in the apps service.
func IngressName(userID, invocationID string) string {
	return fmt.Sprintf("a%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s", userID, invocationID))))[0:9]
}

// getIngress assembles and returns the Ingress needed for the VICE analysis.
// It does not call the k8s API.
func (i *Internal) getIngress(job *model.Job, svc *apiv1.Service) (*extv1beta1.Ingress, error) {
	var (
		rules       []extv1beta1.IngressRule
		defaultPort int32
	)

	labels, err := i.labelsFromJob(job)
	if err != nil {
		return nil, err
	}
	ingressName := IngressName(job.UserID, job.InvocationID)

	// Find the proxy port, use it as the default
	for _, port := range svc.Spec.Ports {
		if port.Name == viceProxyPortName {
			defaultPort = port.Port
		}
	}

	// Handle if the defaultPort isn't set yet.
	if defaultPort == 0 {
		return nil, fmt.Errorf("port %s was not found in the service", viceProxyPortName)
	}

	// default backend, should point at the VICE default backend, which redirects
	// users to the loading page.
	defaultBackend := &extv1beta1.IngressBackend{
		ServiceName: i.ViceDefaultBackendService,
		ServicePort: intstr.FromInt(i.ViceDefaultBackendServicePort),
	}

	// Backend for the service, not the default backend
	backend := &extv1beta1.IngressBackend{
		ServiceName: svc.Name,
		ServicePort: intstr.FromInt(int(defaultPort)),
	}

	// Add the rule to pass along requests to the Service's proxy port.
	rules = append(rules, extv1beta1.IngressRule{
		Host: ingressName,
		IngressRuleValue: extv1beta1.IngressRuleValue{
			HTTP: &extv1beta1.HTTPIngressRuleValue{
				Paths: []extv1beta1.HTTPIngressPath{
					{
						Backend: *backend, // service backend, not the default backend
					},
				},
			},
		},
	})

	return &extv1beta1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name: job.InvocationID,
			Annotations: map[string]string{
				"kubernetes.io/ingress.class": "nginx",
			},
			Labels: labels,
		},
		Spec: extv1beta1.IngressSpec{
			Backend: defaultBackend, // default backend, not the service backend
			Rules:   rules,
		},
	}, nil
}
