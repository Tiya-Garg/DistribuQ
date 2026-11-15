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

// Step evaluates metrics and executes scaling action if cooldowns permit.
func (a *Autoscaler) Step(ctx context.Context) (Decision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	backlog := a.metrics.QueueBacklog()
	current := a.metrics.ActiveWorkers()
	idle := a.metrics.IdleDuration()

	decision := CalculateDesiredWorkers(a.policy, backlog, current, idle)
	now := time.Now()

	// Check cooldowns
	if decision.Direction == ScaleUp {
		if now.Sub(a.lastScaleUp) < a.policy.ScaleUpCooldown {
			decision.Direction = ScaleNone
			decision.Reason = fmt.Sprintf("scale up throttled by cooldown (remaining %v)", a.policy.ScaleUpCooldown-now.Sub(a.lastScaleUp))
			a.recordDecision(decision)
			return decision, nil
		}
	} else if decision.Direction == ScaleDown {
		if now.Sub(a.lastScaleDown) < a.policy.ScaleDownCooldown {
			decision.Direction = ScaleNone
			decision.Reason = fmt.Sprintf("scale down throttled by cooldown (remaining %v)", a.policy.ScaleDownCooldown-now.Sub(a.lastScaleDown))
			a.recordDecision(decision)
			return decision, nil
		}
	}

	if decision.Direction == ScaleUp {
		diff := decision.DesiredWorkers - current
		for i := 0; i < diff; i++ {
			workerID := fmt.Sprintf("autoscale-worker-%d-%d", now.UnixNano(), i)
			if err := a.provisioner.SpawnWorker(ctx, workerID); err != nil {
				return decision, fmt.Errorf("failed to spawn worker %s: %w", workerID, err)
			}
		}
		a.lastScaleUp = now
	} else if decision.Direction == ScaleDown {
		diff := current - decision.DesiredWorkers
		for i := 0; i < diff; i++ {
			if err := a.provisioner.DrainWorker(ctx, ""); err != nil {
				return decision, fmt.Errorf("failed to drain worker: %w", err)
			}
		}
		a.lastScaleDown = now
	}

	a.recordDecision(decision)
	return decision, nil
}

// Start begins a background autoscaling loop with the specified check interval.
func (a *Autoscaler) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			_, _ = a.Step(ctx)
		}
	}
}

// Stop terminates the background autoscaling loop.
func (a *Autoscaler) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.stopped {
		a.stopped = true
		close(a.stopCh)
	}
}
