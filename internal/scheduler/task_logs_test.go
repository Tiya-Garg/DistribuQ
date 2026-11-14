package scheduler

import (
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
)

type dummyProposer struct {
	proposed []raft.LogEntry
}

func (d *dummyProposer) Propose(entry raft.LogEntry) (int, int, <-chan bool, error) {
	d.proposed = append(d.proposed, entry)
	ch := make(chan bool, 1)
	ch <- true
	return 1, 1, ch, nil
}

func TestSchedulerAppendAndGetTaskLogs(t *testing.T) {
	s := NewScheduler(nil)
	s.AppendTaskLog("task-100", "w1", "starting job...\n", false)
	s.AppendTaskLog("task-100", "w1", "standard output\n", false)
	s.AppendTaskLog("task-100", "w1", "warning: error output\n", true)

	logs, err := s.GetTaskLogs("task-100")
	if err != nil {
		t.Fatalf("GetTaskLogs failed: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(logs))
	}
	if logs[0].Content != "starting job...\n" || logs[0].WorkerID != "w1" {
		t.Fatalf("unexpected chunk 0: %+v", logs[0])
	}
	if logs[2].Content != "warning: error output\n" || !logs[2].IsStderr {
		t.Fatalf("unexpected chunk 2: %+v", logs[2])
	}
}

func TestSchedulerReplicatedTaskLogs(t *testing.T) {
	s := NewScheduler(nil)
	proposer := &dummyProposer{}
	s.SetRaftProposer(proposer)

	s.AppendTaskLog("task-rep", "w2", "replicated log line\n", false)
	if len(proposer.proposed) != 1 {
		t.Fatalf("expected 1 proposed entry, got %d", len(proposer.proposed))
	}
	entry := proposer.proposed[0]
	if entry.Type != raft.CmdTaskLog || entry.TaskID != "task-rep" || entry.LogContent != "replicated log line\n" {
		t.Fatalf("unexpected proposed entry: %+v", entry)
	}

	// Apply replicated entry
	s.ApplyEntry(entry)
	logs, err := s.GetTaskLogs("task-rep")
	if err != nil {
		t.Fatalf("GetTaskLogs after apply: %v", err)
	}
	if len(logs) != 1 || logs[0].Content != "replicated log line\n" {
		t.Fatalf("applied log mismatch: %+v", logs)
	}
}

func TestSchedulerTaskLogsSnapshotRoundTrip(t *testing.T) {
	s1 := NewScheduler(nil)
	s1.AppendTaskLog("t-snap", "w1", "line in snapshot\n", false)

	data, err := s1.EncodeState()
	if err != nil {
		t.Fatalf("EncodeState: %v", err)
	}

	s2 := NewScheduler(nil)
	if err := s2.RestoreFromSnapshot(data); err != nil {
		t.Fatalf("RestoreFromSnapshot: %v", err)
	}

	logs, err := s2.GetTaskLogs("t-snap")
	if err != nil {
		t.Fatalf("GetTaskLogs from restored: %v", err)
	}
	if len(logs) != 1 || logs[0].Content != "line in snapshot\n" {
		t.Fatalf("restored logs mismatch: %+v", logs)
	}
}
