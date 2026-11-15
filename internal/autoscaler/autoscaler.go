package autoscaler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Autoscaler evaluates cluster queue backlog metrics and orchestrates worker scaling.
type Autoscaler struct {
	mu           sync.RWMutex
	policy       Policy
	metrics      ClusterMetricsProvider
	provisioner  WorkerProvisioner
	lastScaleUp  time.Time
	lastScaleDown time.Time
	history      []Decision
	maxHistory   int
	stopCh       chan struct{}
	stopped      bool
}

// NewAutoscaler creates an Autoscaler with given policy, metrics provider, and provisioner.
func NewAutoscaler(policy Policy, metrics ClusterMetricsProvider, provisioner WorkerProvisioner) (*Autoscaler, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &Autoscaler{
		policy:      policy,
		metrics:     metrics,
		provisioner: provisioner,
		maxHistory:  50,
		stopCh:      make(chan struct{}),
	}, nil
}
