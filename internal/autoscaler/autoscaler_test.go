package autoscaler

import (
	"context"
	"testing"
	"time"
)

type mockMetrics struct {
	backlog int
	workers int
	idle    time.Duration
}

func (m *mockMetrics) QueueBacklog() int            { return m.backlog }
func (m *mockMetrics) ActiveWorkers() int           { return m.workers }
func (m *mockMetrics) IdleDuration() time.Duration { return m.idle }

type mockProvisioner struct {
	spawned []string
	drained int
}

func (p *mockProvisioner) SpawnWorker(ctx context.Context, id string) error {
	p.spawned = append(p.spawned, id)
	return nil
}

func (p *mockProvisioner) DrainWorker(ctx context.Context, id string) error {
	p.drained++
	return nil
}

func TestAutoscalerStepCooldowns(t *testing.T) {
	metrics := &mockMetrics{backlog: 20, workers: 1}
	prov := &mockProvisioner{}
	policy := Policy{
		MinWorkers:        1,
		MaxWorkers:        10,
		TargetQueueLength: 5,
		ScaleUpCooldown:   1 * time.Second,
		ScaleDownCooldown: 1 * time.Second,
	}

	as, err := NewAutoscaler(policy, metrics, prov)
	if err != nil {
		t.Fatalf("failed to create autoscaler: %v", err)
	}

	// 1. Initial scale up
	d1, err := as.Step(context.Background())
	if err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}
	if d1.Direction != ScaleUp || len(prov.spawned) != 3 {
		t.Fatalf("expected 3 spawned workers, got %d", len(prov.spawned))
	}

	// 2. Immediate second step should be throttled by ScaleUpCooldown
	d2, err := as.Step(context.Background())
	if err != nil {
		t.Fatalf("step 2 failed: %v", err)
	}
	if d2.Direction != ScaleNone {
		t.Fatalf("expected ScaleNone due to cooldown, got %v", d2.Direction)
	}

	if len(as.History()) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(as.History()))
	}
}
