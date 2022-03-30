package launcher

import (
	"context"
	"fmt"

	"github.com/cyverse-de/model/v6"
	"github.com/cyverse-de/vice-launcher/constants"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// getService assembles and returns the Service needed for the VICE analysis.
// It does not call the k8s API.
func (i *Internal) getService(ctx context.Context, job *model.Job, deployment *appsv1.Deployment) (*apiv1.Service, error) {
	labels, err := i.LabelsFromJob(ctx, job)
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
				{
					Name:       constants.FileTransfersPortName,
					Protocol:   apiv1.ProtocolTCP,
					Port:       constants.FileTransfersPort,
					TargetPort: intstr.FromString(constants.FileTransfersPortName),
				},
				{
					Name:       constants.VICEProxyPortName,
					Protocol:   apiv1.ProtocolTCP,
					Port:       constants.VICEProxyServicePort,
					TargetPort: intstr.FromString(constants.VICEProxyPortName),
				},
			},
		},
	}

	return &svc, nil
}
