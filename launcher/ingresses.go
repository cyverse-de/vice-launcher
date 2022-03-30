package launcher

import (
	"context"
	"fmt"

	"github.com/cyverse-de/model/v6"
	"github.com/cyverse-de/vice-launcher/common"
	"github.com/cyverse-de/vice-launcher/constants"
	apiv1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getIngress assembles and returns the Ingress needed for the VICE analysis.
// It does not call the k8s API.
func (i *Internal) getIngress(ctx context.Context, job *model.Job, svc *apiv1.Service) (*netv1.Ingress, error) {
	var (
		rules       []netv1.IngressRule
		defaultPort int32
	)

	labels, err := i.LabelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}
	ingressName := common.IngressName(job.UserID, job.InvocationID)

	// Find the proxy port, use it as the default
	for _, port := range svc.Spec.Ports {
		if port.Name == constants.VICEProxyPortName {
			defaultPort = port.Port
		}
	}

	// Handle if the defaultPort isn't set yet.
	if defaultPort == 0 {
		return nil, fmt.Errorf("port %s was not found in the service", constants.VICEProxyPortName)
	}

	// default backend, should point at the VICE default backend, which redirects
	// users to the loading page.
	defaultBackend := &netv1.IngressBackend{
		Service: &netv1.IngressServiceBackend{
			Name: i.ViceDefaultBackendService,
			Port: netv1.ServiceBackendPort{
				Number: int32(i.ViceDefaultBackendServicePort),
			},
		},
	}

	// Backend for the service, not the default backend
	backend := &netv1.IngressBackend{
		Service: &netv1.IngressServiceBackend{
			Name: svc.Name,
			Port: netv1.ServiceBackendPort{
				Number: defaultPort,
			},
		},
	}

	// Add the rule to pass along requests to the Service's proxy port.
	pathTytpe := netv1.PathTypeImplementationSpecific
	rules = append(rules, netv1.IngressRule{
		Host: ingressName,
		IngressRuleValue: netv1.IngressRuleValue{
			HTTP: &netv1.HTTPIngressRuleValue{
				Paths: []netv1.HTTPIngressPath{
					{
						PathType: &pathTytpe,
						Backend:  *backend, // service backend, not the default backend
					},
				},
			},
		},
	})

	return &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name: job.InvocationID,
			Annotations: map[string]string{
				"kubernetes.io/ingress.class": "nginx",
			},
			Labels: labels,
		},
		Spec: netv1.IngressSpec{
			DefaultBackend: defaultBackend, // default backend, not the service backend
			Rules:          rules,
		},
	}, nil
}
