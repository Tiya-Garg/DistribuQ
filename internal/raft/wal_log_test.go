package raft

import (
	"testing"
)

func TestWALPersisterCmdTaskLogPersistence(t *testing.T) {
	dir := t.TempDir()
	p, err := NewWALPersister(dir, "wal-node-log")
	if err != nil {
		t.Fatalf("NewWALPersister: %v", err)
	}
	defer p.Close()

	entry := LogEntry{
		Index:      1,
		Term:       1,
		Type:       CmdTaskLog,
		TaskID:     "task-wal-test",
		WorkerID:   "w1",
		LogContent: "executing command output...\n",
		IsStderr:   false,
	}

	if err := p.Save(1, "wal-node-log", []LogEntry{entry}, 0, 0, nil); err != nil {
		t.Fatalf("Save: %v", err)
	}
	p.Close()

	// Reload persister
	p2, err := NewWALPersister(dir, "wal-node-log")
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	defer p2.Close()

	_, _, entries, _, _, _, err := p2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Type != CmdTaskLog || entries[0].LogContent != "executing command output...\n" {
		t.Fatalf("unexpected entry loaded: %+v", entries[0])
	}
}

