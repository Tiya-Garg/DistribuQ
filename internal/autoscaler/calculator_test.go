package autoscaler

import (
	"testing"
	"time"
)

func TestCalculateDesiredWorkers(t *testing.T) {
	p := Policy{
		MinWorkers:        1,
		MaxWorkers:        10,
		TargetQueueLength: 5,
		ScaleToZeroIdle:   30 * time.Second,
	}

	// 1. Backlog empty, idle less than threshold
	d1 := CalculateDesiredWorkers(p, 0, 3, 10*time.Second)
	if d1.DesiredWorkers != 1 || d1.Direction != ScaleDown {
		t.Fatalf("expected scale down to min 1, got %+v", d1)
	}

	// 2. High backlog, should scale up
	d2 := CalculateDesiredWorkers(p, 25, 2, 0)
	if d2.DesiredWorkers != 5 || d2.Direction != ScaleUp {
		t.Fatalf("expected scale up to 5, got %+v", d2)
	}

	// 3. Huge backlog exceeding max workers
	d3 := CalculateDesiredWorkers(p, 100, 5, 0)
	if d3.DesiredWorkers != 10 || d3.Direction != ScaleUp {
		t.Fatalf("expected cap at max workers 10, got %+v", d3)
	}

	// 4. Scale to zero policy
	pZero := Policy{
		MinWorkers:        0,
		MaxWorkers:        5,
		TargetQueueLength: 2,
		ScaleToZeroIdle:   15 * time.Second,
	}
	d4 := CalculateDesiredWorkers(pZero, 0, 2, 20*time.Second)
	if d4.DesiredWorkers != 0 || d4.Direction != ScaleDown {
		t.Fatalf("expected scale to zero, got %+v", d4)
	}
}
