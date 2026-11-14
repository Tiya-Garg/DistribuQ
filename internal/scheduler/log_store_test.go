package scheduler

import (
	"testing"
)

func TestLogStoreAppendAndGet(t *testing.T) {
	ls := NewLogStore(10)

	// Initially empty
	if _, err := ls.Get("task-1"); err != ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound, got %v", err)
	}

	ls.Append("task-1", "w1", "line 1\n", false)
	ls.Append("task-1", "w1", "error line\n", true)

	logs, err := ls.Get("task-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("expected 2 log chunks, got %d", len(logs))
	}
	if logs[0].Content != "line 1\n" || logs[0].IsStderr {
		t.Fatalf("unexpected chunk 0: %+v", logs[0])
	}
	if logs[1].Content != "error line\n" || !logs[1].IsStderr {
		t.Fatalf("unexpected chunk 1: %+v", logs[1])
	}
}

func TestLogStoreBoundedEviction(t *testing.T) {
	ls := NewLogStore(3)

	for i := 1; i <= 5; i++ {
		ls.Append("task-bounded", "w1", string(rune('0'+i)), false)
	}

	logs, err := ls.Get("task-bounded")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("expected 3 chunks due to bounding, got %d", len(logs))
	}
	if logs[0].Content != "3" || logs[1].Content != "4" || logs[2].Content != "5" {
		t.Fatalf("unexpected evicted content: %+v", logs)
	}
}

func TestLogStoreSnapshotAndRestore(t *testing.T) {
	ls := NewLogStore(10)
	ls.Append("task-snap", "w1", "snap output", false)

	snap := ls.Snapshot()
	ls2 := NewLogStore(10)
	ls2.Restore(snap)

	logs, err := ls2.Get("task-snap")
	if err != nil {
		t.Fatalf("Get restored: %v", err)
	}
	if len(logs) != 1 || logs[0].Content != "snap output" {
		t.Fatalf("restored mismatch: %+v", logs)
	}

	ls2.Purge("task-snap")
	if _, err := ls2.Get("task-snap"); err != ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound after purge, got %v", err)
	}
}
