package scheduler

import (
	"testing"
	"time"
)

func waitForTaskStatus(t *testing.T, s *Scheduler, taskID string, want TaskStatus) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		task, ok := s.tasks[taskID]
		status := TaskStatus("")
		if ok {
			status = task.Status
		}
		s.mu.Unlock()
		if status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	actual := s.tasks[taskID].Status
	s.mu.Unlock()
	t.Fatalf("task %s status: got %s, want %s", taskID, actual, want)
}

func TestDAGSequentialExecution(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	s.RegisterWorker("w1", "127.0.0.1:9001")

	// Submit task-1
	t1, err := s.SubmitTask("t1", "payload-1")
	if err != nil {
		t.Fatalf("submit t1: %v", err)
	}
	if t1.Status != TaskAssigned {
		t.Fatalf("expected t1 to be assigned, got %s", t1.Status)
	}

	// Submit task-2 depending on task-1
	t2, err := s.SubmitTaskDAG("t2", "payload-2", 0, "", time.Time{}, []string{"t1"})
	if err != nil {
		t.Fatalf("submit t2: %v", err)
	}
	if t2.Status != TaskBlocked {
		t.Fatalf("expected t2 to be blocked, got %s", t2.Status)
	}

	// Complete task-1
	if err := s.CompleteTask("t1"); err != nil {
		t.Fatalf("complete t1: %v", err)
	}

	// Verify t2 unblocks and becomes assigned
	waitForTaskStatus(t, s, "t2", TaskAssigned)
}

func TestDAGDiamondExecution(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	s.RegisterWorker("w1", "127.0.0.1:9001")

	// A -> (B, C) -> D
	if _, err := s.SubmitTask("A", "payload-A"); err != nil {
		t.Fatalf("submit A: %v", err)
	}
	if _, err := s.SubmitTaskDAG("B", "payload-B", 0, "", time.Time{}, []string{"A"}); err != nil {
		t.Fatalf("submit B: %v", err)
	}
	if _, err := s.SubmitTaskDAG("C", "payload-C", 0, "", time.Time{}, []string{"A"}); err != nil {
		t.Fatalf("submit C: %v", err)
	}
	if _, err := s.SubmitTaskDAG("D", "payload-D", 0, "", time.Time{}, []string{"B", "C"}); err != nil {
		t.Fatalf("submit D: %v", err)
	}

	// B, C, D should all be blocked initially
	s.mu.Lock()
	if s.tasks["B"].Status != TaskBlocked || s.tasks["C"].Status != TaskBlocked || s.tasks["D"].Status != TaskBlocked {
		s.mu.Unlock()
		t.Fatalf("expected B, C, D to be blocked initially")
	}
	s.mu.Unlock()

	// Complete A -> B and C should unblock and become assigned
	if err := s.CompleteTask("A"); err != nil {
		t.Fatalf("complete A: %v", err)
	}

	waitForTaskStatus(t, s, "B", TaskAssigned)
	waitForTaskStatus(t, s, "C", TaskAssigned)

	s.mu.Lock()
	if s.tasks["D"].Status != TaskBlocked {
		s.mu.Unlock()
		t.Fatalf("expected D to remain blocked, got %s", s.tasks["D"].Status)
	}
	s.mu.Unlock()

	// Complete B -> D still blocked on C
	if err := s.CompleteTask("B"); err != nil {
		t.Fatalf("complete B: %v", err)
	}

	s.mu.Lock()
	if s.tasks["D"].Status != TaskBlocked {
		s.mu.Unlock()
		t.Fatalf("expected D to still be blocked waiting for C, got %s", s.tasks["D"].Status)
	}
	s.mu.Unlock()

	// Complete C -> D unblocks and gets assigned!
	if err := s.CompleteTask("C"); err != nil {
		t.Fatalf("complete C: %v", err)
	}

	waitForTaskStatus(t, s, "D", TaskAssigned)
}

func TestDAGFailureCascade(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	s.RegisterWorker("w1", "127.0.0.1:9001")

	// A -> B
	if _, err := s.SubmitTask("A", "payload-A"); err != nil {
		t.Fatalf("submit A: %v", err)
	}
	tB, err := s.SubmitTaskDAG("B", "payload-B", 0, "", time.Time{}, []string{"A"})
	if err != nil {
		t.Fatalf("submit B: %v", err)
	}
	if tB.Status != TaskBlocked {
		t.Fatalf("expected B to be blocked, got %s", tB.Status)
	}

	// Fail A until retries are exhausted (max 3)
	for i := 1; i <= 3; i++ {
		time.Sleep(20 * time.Millisecond)
		s.dispatchQueued()
		if err := s.FailTask("A"); err != nil {
			t.Fatalf("FailTask on attempt %d: %v", i, err)
		}
	}

	waitForTaskStatus(t, s, "A", TaskDLQ)
	waitForTaskStatus(t, s, "B", TaskFailed)

	s.mu.Lock()
	lastErrB := s.tasks["B"].LastError
	s.mu.Unlock()

	if lastErrB == "" {
		t.Fatalf("expected B to have last error recorded for dependency failure")
	}
}

func TestDAGCycleDetection(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	// Direct self-cycle: A depends on A
	_, err := s.SubmitTaskDAG("A", "payload-A", 0, "", time.Time{}, []string{"A"})
	if err != ErrCyclicDependency {
		t.Fatalf("expected ErrCyclicDependency on self cycle, got %v", err)
	}

	// Indirect cycle: A -> B -> A
	_, err = s.SubmitTaskDAG("A", "payload-A", 0, "", time.Time{}, nil)
	if err != nil {
		t.Fatalf("submit A: %v", err)
	}
	_, err = s.SubmitTaskDAG("B", "payload-B", 0, "", time.Time{}, []string{"A"})
	if err != nil {
		t.Fatalf("submit B: %v", err)
	}

	s.mu.Lock()
	err = s.checkCyclicDependencyLocked("A", []string{"B"})
	s.mu.Unlock()
	if err != ErrCyclicDependency {
		t.Fatalf("expected ErrCyclicDependency on indirect cycle, got %v", err)
	}
}

func TestDAGSnapshotRestore(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "127.0.0.1:9001")

	_, _ = s.SubmitTask("A", "payload-A")
	_, _ = s.SubmitTaskDAG("B", "payload-B", 0, "", time.Time{}, []string{"A"})

	snapBytes, err := s.EncodeState()
	if err != nil {
		t.Fatalf("EncodeState: %v", err)
	}
	s.Stop()

	// Restore into fresh scheduler
	fd2 := &fakeDispatch{}
	s2 := NewScheduler(fd2.fn)
	defer s2.Stop()
	if err := s2.RestoreFromSnapshot(snapBytes); err != nil {
		t.Fatalf("RestoreFromSnapshot: %v", err)
	}

	s2.mu.Lock()
	if s2.tasks["B"].Status != TaskBlocked {
		s2.mu.Unlock()
		t.Fatalf("expected B to remain blocked after restore, got %s", s2.tasks["B"].Status)
	}
	if len(s2.tasks["B"].DependsOn) != 1 || s2.tasks["B"].DependsOn[0] != "A" {
		s2.mu.Unlock()
		t.Fatalf("expected B DependsOn to be restored properly: %v", s2.tasks["B"].DependsOn)
	}
	s2.mu.Unlock()

	// Register worker and complete A in s2
	s2.RegisterWorker("w1", "127.0.0.1:9001")
	if err := s2.CompleteTask("A"); err != nil {
		t.Fatalf("complete A: %v", err)
	}

	waitForTaskStatus(t, s2, "B", TaskAssigned)
}
