package scheduler

import (
	"testing"
	"time"
)

func TestDLQWorkflowAndReplay(t *testing.T) {
	origBackoff := BaseRetryBackoff
	BaseRetryBackoff = 10 * time.Millisecond
	defer func() { BaseRetryBackoff = origBackoff }()

	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	_, err := s.SubmitTask("task-dlq-1", "do-something")
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	// Fail until retries exhausted (max retries = 3)
	for i := 1; i <= 3; i++ {
		time.Sleep(20 * time.Millisecond)
		s.dispatchQueued()

		if err := s.FailTask("task-dlq-1"); err != nil {
			t.Fatalf("FailTask on attempt %d failed: %v", i, err)
		}
	}

	// Now it should be in DLQ
	status, attempts := taskState(s, "task-dlq-1")
	if status != TaskDLQ {
		t.Fatalf("expected task to be in DLQ, got %s (attempts=%d)", status, attempts)
	}

	dlqTasks := s.GetDLQTasks()
	if len(dlqTasks) != 1 {
		t.Fatalf("expected 1 DLQ task, got %d", len(dlqTasks))
	}
	if dlqTasks[0].ID != "task-dlq-1" {
		t.Fatalf("expected DLQ task ID task-dlq-1, got %s", dlqTasks[0].ID)
	}

	// Replay the DLQ task
	if err := s.ReplayDLQTask("task-dlq-1"); err != nil {
		t.Fatalf("ReplayDLQTask failed: %v", err)
	}

	// Should be requeued or immediately dispatched
	status, attempts = taskState(s, "task-dlq-1")
	if status != TaskQueued && status != TaskAssigned {
		t.Fatalf("expected replayed task to be queued or assigned, got %s", status)
	}
	if attempts != 0 && attempts != 1 {
		t.Fatalf("expected attempts to reset, got %d", attempts)
	}

	// Put it back in DLQ and test purge
	s.mu.Lock()
	s.tasks["task-dlq-1"].Status = TaskDLQ
	s.mu.Unlock()

	if err := s.PurgeDLQTask("task-dlq-1"); err != nil {
		t.Fatalf("PurgeDLQTask failed: %v", err)
	}

	s.mu.Lock()
	_, exists := s.tasks["task-dlq-1"]
	s.mu.Unlock()
	if exists {
		t.Fatalf("expected task-dlq-1 to be purged from scheduler")
	}
}
