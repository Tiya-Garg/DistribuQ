package raft

import (
	"bytes"
	"testing"
)

func TestWALPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewWALPersister(dir, "node-1")
	if err != nil {
		t.Fatalf("NewWALPersister failed: %v", err)
	}
	defer p.Close()

	term, votedFor, log, _, _, snap, err := p.Load()
	if err != nil {
		t.Fatalf("initial Load failed: %v", err)
	}
	if term != 0 || votedFor != "" || len(log) != 0 || snap != nil {
		t.Fatalf("expected empty initial state, got term=%d votedFor=%s len(log)=%d", term, votedFor, len(log))
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "task-1", Payload: "echo 1"},
		{Index: 2, Term: 1, Type: CmdAssign, TaskID: "task-1", WorkerID: "w-1"},
		{Index: 3, Term: 2, Type: CmdComplete, TaskID: "task-1"},
	}
	snapshotData := []byte("checkpoint-state")

	if err := p.Save(2, "node-2", entries, 1, 1, snapshotData); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Close and reload to verify persistence across restarts
	p.Close()

	p2, err := NewWALPersister(dir, "node-1")
	if err != nil {
		t.Fatalf("reopening NewWALPersister failed: %v", err)
	}
	defer p2.Close()

	rTerm, rVotedFor, rLog, rLastIncIdx, rLastIncTerm, rSnap, err := p2.Load()
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}

	if rTerm != 2 {
		t.Errorf("term mismatch: got %d, want 2", rTerm)
	}
	if rVotedFor != "node-2" {
		t.Errorf("votedFor mismatch: got %s, want node-2", rVotedFor)
	}
	if rLastIncIdx != 1 || rLastIncTerm != 1 {
		t.Errorf("snapshot indices mismatch: got idx=%d term=%d, want 1, 1", rLastIncIdx, rLastIncTerm)
	}
	if !bytes.Equal(rSnap, snapshotData) {
		t.Errorf("snapshot data mismatch: got %s, want %s", string(rSnap), string(snapshotData))
	}
	if len(rLog) != len(entries) {
		t.Fatalf("log length mismatch: got %d, want %d", len(rLog), len(entries))
	}
	for i := range entries {
		if rLog[i].TaskID != entries[i].TaskID || rLog[i].Type != entries[i].Type {
			t.Errorf("entry %d mismatch: got %+v, want %+v", i, rLog[i], entries[i])
		}
	}
}

func TestWALPersisterSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	p, err := NewWALPersister(dir, "node-wal-snap")
	if err != nil {
		t.Fatalf("NewWALPersister: %v", err)
	}
	defer p.Close()

	// Initial log with 5 entries
	entries := []LogEntry{
		{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "t-1"},
		{Index: 2, Term: 1, Type: CmdSubmit, TaskID: "t-2"},
		{Index: 3, Term: 1, Type: CmdSubmit, TaskID: "t-3"},
		{Index: 4, Term: 1, Type: CmdSubmit, TaskID: "t-4"},
		{Index: 5, Term: 1, Type: CmdSubmit, TaskID: "t-5"},
	}
	if err := p.Save(1, "node-wal-snap", entries, 0, 0, nil); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Compact up to index 3
	compactedEntries := entries[3:] // indices 4 and 5
	snapData := []byte("compacted-up-to-3")
	if err := p.Save(1, "node-wal-snap", compactedEntries, 3, 1, snapData); err != nil {
		t.Fatalf("Save snapshot: %v", err)
	}

	p.Close()

	pReloaded, err := NewWALPersister(dir, "node-wal-snap")
	if err != nil {
		t.Fatalf("reloading persister: %v", err)
	}
	defer pReloaded.Close()

	term, votedFor, logEntries, lastIncIdx, lastIncTerm, snapBytes, err := pReloaded.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if term != 1 || votedFor != "node-wal-snap" {
		t.Errorf("state mismatch: term=%d votedFor=%s", term, votedFor)
	}
	if lastIncIdx != 3 || lastIncTerm != 1 {
		t.Errorf("expected last included (3, 1), got (%d, %d)", lastIncIdx, lastIncTerm)
	}
	if !bytes.Equal(snapBytes, snapData) {
		t.Errorf("snapshot data mismatch: got %q, want %q", string(snapBytes), string(snapData))
	}
	if len(logEntries) != 2 {
		t.Fatalf("expected 2 remaining entries after compaction, got %d", len(logEntries))
	}
	if logEntries[0].Index != 4 || logEntries[1].Index != 5 {
		t.Errorf("unexpected remaining log entries: %+v", logEntries)
	}
}
