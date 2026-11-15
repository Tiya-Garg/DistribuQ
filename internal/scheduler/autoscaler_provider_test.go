package scheduler

import (
	"testing"
)

func TestSchedulerClusterMetricsProvider(t *testing.T) {
	s := NewScheduler(nil)
	if s.QueueBacklog() != 0 {
		t.Fatalf("expected backlog 0, got %d", s.QueueBacklog())
	}
	if s.ActiveWorkers() != 0 {
		t.Fatalf("expected active workers 0, got %d", s.ActiveWorkers())
	}

	s.RegisterWorker("w1", "127.0.0.1:9001")
	s.RegisterWorker("w2", "127.0.0.1:9002")
	if s.ActiveWorkers() != 2 {
		t.Fatalf("expected active workers 2, got %d", s.ActiveWorkers())
	}

	_, _ = s.SubmitTask("t1", "echo 1")
	_, _ = s.SubmitTask("t2", "echo 2")
	if s.QueueBacklog() < 1 && s.tasks["t1"].Status != TaskQueued && s.tasks["t1"].Status != TaskAssigned {
		t.Fatalf("expected tasks in queue or assigned")
	}

	// Drain worker with empty ID
	err := s.DrainWorker("")
	if err != nil {
		t.Fatalf("failed to drain worker with empty id: %v", err)
	}
	if s.ActiveWorkers() != 1 {
		t.Fatalf("expected 1 active worker after drain, got %d", s.ActiveWorkers())
	}

	// Idle duration check
	idle := s.IdleDuration()
	if idle < 0 {
		t.Fatalf("idle duration should be non-negative: %v", idle)
	}
}
