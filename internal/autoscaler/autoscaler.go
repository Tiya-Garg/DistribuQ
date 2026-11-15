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

// History returns a copy of recent autoscaling decisions.
func (a *Autoscaler) History() []Decision {
	a.mu.RLock()
	defer a.mu.RUnlock()
	res := make([]Decision, len(a.history))
	copy(res, a.history)
	return res
}

func (a *Autoscaler) recordDecision(d Decision) {
	a.history = append(a.history, d)
	if len(a.history) > a.maxHistory {
		a.history = a.history[len(a.history)-a.maxHistory:]
	}
}
