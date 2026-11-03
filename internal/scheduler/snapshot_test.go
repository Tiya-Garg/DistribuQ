package scheduler

import (
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
)

func TestSchedulerEncodeAndRestoreSnapshot(t *testing.T) {
	s := NewScheduler(nil)
	s.ApplyEntry(raft.LogEntry{Type: raft.CmdSubmit, TaskID: "t1", Payload: "echo 1", Priority: 1})
	s.ApplyEntry(raft.LogEntry{Type: raft.CmdSubmit, TaskID: "t2", Payload: "echo 2", Priority: 3})
	s.ApplyEntry(raft.LogEntry{Type: raft.CmdAssign, TaskID: "t1", WorkerID: "w1"})
	s.ApplyEntry(raft.LogEntry{Type: raft.CmdComplete, TaskID: "t1"})

	data, err := s.EncodeState()
	if err != nil {
		t.Fatalf("EncodeState failed: %v", err)
	}

	s2 := NewScheduler(nil)
	if err := s2.RestoreFromSnapshot(data); err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	tasks, _ := s2.Snapshot()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks in restored scheduler, got %d", len(tasks))
	}
	if tasks["t1"] != TaskCompleted {
		t.Errorf("expected t1 completed, got %v", tasks["t1"])
	}
	if tasks["t2"] != TaskQueued {
		t.Errorf("expected t2 queued, got %v", tasks["t2"])
	}

	// Verify task metadata
	t2 := s2.tasks["t2"]
	if t2 == nil || t2.Payload != "echo 2" || t2.MaxRetries != 3 || t2.Priority != 3 {
		t.Errorf("t2 metadata not restored properly: %+v", t2)
	}
}
