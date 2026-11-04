package scheduler

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelTaskQueuedAndInFlight(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	var aborted atomic.Bool
	s.SetAbortFn(func(workerID, addr, taskID string) error {
		aborted.Store(true)
		return nil
	})

	// 1. Submit a delayed task and cancel it while still scheduled
	task, err := s.SubmitTaskFull("cancel-1", "echo 1", 1, "", time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("SubmitTaskFull failed: %v", err)
	}
	if task.Status != TaskScheduled {
		t.Fatalf("expected TaskScheduled, got %s", task.Status)
	}

	if err := s.CancelTask("cancel-1"); err != nil {
		t.Fatalf("CancelTask failed: %v", err)
	}
	if task.Status != TaskCanceled {
		t.Fatalf("expected TaskCanceled, got %s", task.Status)
	}

	// 2. Submit an immediate task assigned to worker and cancel it
	task2, err := s.SubmitTask("cancel-2", "echo 2")
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}
	if task2.Status != TaskAssigned {
		t.Fatalf("expected TaskAssigned, got %s", task2.Status)
	}

	if err := s.CancelTask("cancel-2"); err != nil {
		t.Fatalf("CancelTask on assigned task failed: %v", err)
	}
	if task2.Status != TaskCanceled {
		t.Fatalf("expected TaskCanceled, got %s", task2.Status)
	}
	if !aborted.Load() {
		t.Fatalf("expected abortFn to be called for in-flight task")
	}
}
