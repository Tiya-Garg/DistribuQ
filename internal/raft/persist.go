package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// CommandType represents the type of task event recorded in the Raft log.
type CommandType string

const (
	CmdSubmit   CommandType = "submitted"
	CmdAssign   CommandType = "assigned"
	CmdComplete CommandType = "completed"
	CmdFail     CommandType = "failed"
	CmdNoop     CommandType = "noop"
	// CmdSnapshot is a placeholder for future snapshotting logic.
	CmdSnapshot CommandType = "snapshot"
)

// LogEntry is an entry in the replicated Raft log representing a task event.
type LogEntry struct {
	Index    int         `json:"index"`
	Term     int         `json:"term"`
	Type     CommandType `json:"type"`
	TaskID   string      `json:"task_id"`
	Payload  string      `json:"payload,omitempty"`
	WorkerID string      `json:"worker_id,omitempty"`
	Cause    string      `json:"cause,omitempty"`
}

// Persister durably stores the Raft state that must survive a restart:
// currentTerm, votedFor, and the replicated log. If a node forgets its
// vote or log across a crash, safety guarantees can be violated.
//
// Save must not return until the data is on stable storage.
type Persister interface {
	Load() (term int, votedFor string, log []LogEntry, err error)
	Save(term int, votedFor string, log []LogEntry) error
	// SaveSnapshot persists a compacted snapshot of the state machine.
	// index and term refer to the last included log entry.
	SaveSnapshot(index int, term int, data []byte) error
}

// FilePersister is a Persister backed by a single JSON file. Writes go
// to a temp file that is fsynced and atomically renamed over the
// target, so a crash leaves either the old state or the new state,
// never a torn file.
type FilePersister struct {
	path string
}

// NewFilePersister returns a FilePersister that stores state at path.
func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path}
}

type persistedState struct {
	Term          int        `json:"term"`
	VotedFor      string     `json:"voted_for"`
	Log           []LogEntry `json:"log,omitempty"`
	SnapshotIndex int        `json:"snapshot_index,omitempty"`
	SnapshotTerm  int        `json:"snapshot_term,omitempty"`
	SnapshotData  []byte     `json:"snapshot_data,omitempty"`
}

// Load returns the saved state. A missing file is a fresh node, not an
// error: it returns term 0, no vote, and an empty log.
func (p *FilePersister) Load() (int, string, []LogEntry, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, nil
	}
	if err != nil {
		return 0, "", nil, fmt.Errorf("raft: read %s: %w", p.path, err)
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, "", nil, fmt.Errorf("raft: corrupt state file %s: %w", p.path, err)
	}
	return st.Term, st.VotedFor, st.Log, nil
}

// Save atomically and durably replaces the stored state (term, vote, log, and optional snapshot).
func (p *FilePersister) Save(term int, votedFor string, logEntries []LogEntry) error {
	// Load existing snapshot info so we don't lose it on a normal Save.
	existing, _ := p.LoadSnapshot()
	data, err := json.Marshal(persistedState{
		Term:          term,
		VotedFor:      votedFor,
		Log:           logEntries,
		SnapshotIndex: existing.Index,
		SnapshotTerm:  existing.Term,
		SnapshotData:  existing.Data,
	})
	if err != nil {
		return err
	}

	dir := filepath.Dir(p.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(p.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p.path); err != nil {
		os.Remove(tmpName)
		return err
	}

	// fsync the directory so the rename itself survives a crash.
	if runtime.GOOS != "windows" {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}
	return nil
}

// LoadSnapshot returns the snapshot metadata stored in the file, if any.
func (p *FilePersister) LoadSnapshot() (snap struct{ Index int; Term int; Data []byte }, err error) {
	data, err := os.ReadFile(p.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return snap, nil // no snapshot yet
		}
		return snap, err
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return snap, err
	}
	snap.Index = st.SnapshotIndex
	snap.Term = st.SnapshotTerm
	snap.Data = st.SnapshotData
	return snap, nil
}

// SaveSnapshot persists a compacted snapshot.
func (p *FilePersister) SaveSnapshot(index int, term int, data []byte) error {
	// Load current state to preserve term/vote/log.
	termCur, votedFor, logEntries, err := p.Load()
	if err != nil {
		return err
	}
	st := persistedState{
		Term:          termCur,
		VotedFor:      votedFor,
		Log:           logEntries,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		SnapshotData:  data,
	}
	bytes, err := json.Marshal(st)
	if err != nil {
		return err
	}

	dir := filepath.Dir(p.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(p.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	if _, err := tmp.Write(bytes); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p.path); err != nil {
		os.Remove(tmpName)
		return err
	}

	if runtime.GOOS != "windows" {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}
	return nil
}
