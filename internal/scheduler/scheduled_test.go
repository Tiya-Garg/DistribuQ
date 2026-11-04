package scheduler

import (
	"testing"
	"time"
)

func TestScheduledTaskExecution(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	runAt := time.Now().Add(150 * time.Millisecond)
	task, err := s.SubmitTaskFull("sched-1", "do-delayed", 1, "", runAt)
	if err != nil {
		t.Fatalf("SubmitTaskFull failed: %v", err)
	}

	if task.Status != TaskScheduled {
		t.Fatalf("expected task status to be %s, got %s", TaskScheduled, task.Status)
	}

	// Immediately sweep: task should not be dispatched yet
	s.dispatchQueued()
	if fd.count("w1") != 0 {
		t.Fatalf("task was dispatched before its scheduled time")
	}

	// Wait past the schedule time
	time.Sleep(200 * time.Millisecond)

	s.dispatchQueued()
	if fd.count("w1") != 1 {
		t.Fatalf("expected task to be dispatched after scheduled time arrived, got count=%d", fd.count("w1"))
	}
}
