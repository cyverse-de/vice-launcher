package internal

import (
	"fmt"

	"gopkg.in/cyverse-de/model.v4"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// getService assembles and returns the Service needed for the VICE analysis.
// It does not call the k8s API.
func (i *Internal) getService(job *model.Job, deployment *appsv1.Deployment) (*apiv1.Service, error) {
	labels, err := i.labelsFromJob(job)
	if err != nil {
		return nil, err
	}

	svc := apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("vice-%s", job.InvocationID),
			Labels: labels,
		},
		Spec: apiv1.ServiceSpec{
			Selector: map[string]string{
				"external-id": job.InvocationID,
			},
			Ports: []apiv1.ServicePort{
				apiv1.ServicePort{
					Name:       fileTransfersPortName,
					Protocol:   apiv1.ProtocolTCP,
					Port:       fileTransfersPort,
					TargetPort: intstr.FromString(fileTransfersPortName),
				},
				apiv1.ServicePort{
					Name:       viceProxyPortName,
					Protocol:   apiv1.ProtocolTCP,
					Port:       viceProxyServicePort,
					TargetPort: intstr.FromString(viceProxyPortName),
				},
			},
		},
	}

	return &svc, nil
}
