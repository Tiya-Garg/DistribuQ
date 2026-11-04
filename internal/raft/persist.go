package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Tiya-Garg/DistribuQ/internal/wal"
)

// CommandType represents the type of task event recorded in the Raft log.
type CommandType string

const (
	CmdSubmit    CommandType = "submitted"
	CmdAssign    CommandType = "assigned"
	CmdComplete  CommandType = "completed"
	CmdFail      CommandType = "failed"
	CmdNoop      CommandType = "noop"
	CmdReplayDLQ CommandType = "replay_dlq"
	CmdPurgeDLQ  CommandType = "purge_dlq"
)

// LogEntry is an entry in the replicated Raft log representing a task event.
type LogEntry struct {
	Index       int         `json:"index"`
	Term        int         `json:"term"`
	Type        CommandType `json:"type"`
	TaskID      string      `json:"task_id"`
	Payload     string      `json:"payload,omitempty"`
	WorkerID    string      `json:"worker_id,omitempty"`
	Cause       string      `json:"cause,omitempty"`
	Priority    int         `json:"priority,omitempty"`
	RequiredTag string      `json:"required_tag,omitempty"`
}

// Persister durably stores the Raft state that must survive a restart:
// currentTerm, votedFor, the replicated log, and snapshot metadata. If a node
// forgets its vote or log across a crash, safety guarantees can be violated.
//
// Save must not return until the data is on stable storage.
type Persister interface {
	Load() (term int, votedFor string, log []LogEntry, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte, err error)
	Save(term int, votedFor string, log []LogEntry, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) error
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
	Term              int        `json:"term"`
	VotedFor          string     `json:"voted_for"`
	LastIncludedIndex int        `json:"last_included_index,omitempty"`
	LastIncludedTerm  int        `json:"last_included_term,omitempty"`
	Snapshot          []byte     `json:"snapshot,omitempty"`
	Log               []LogEntry `json:"log,omitempty"`
}

// Load returns the saved state. A missing file is a fresh node, not an
// error: it returns term 0, no vote, empty log, and no snapshot.
func (p *FilePersister) Load() (int, string, []LogEntry, int, int, []byte, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, 0, 0, nil, nil
	}
	if err != nil {
		return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: read %s: %w", p.path, err)
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: corrupt state file %s: %w", p.path, err)
	}
	return st.Term, st.VotedFor, st.Log, st.LastIncludedIndex, st.LastIncludedTerm, st.Snapshot, nil
}

// Save atomically and durably replaces the stored state.
func (p *FilePersister) Save(term int, votedFor string, logEntries []LogEntry, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) error {
	data, err := json.Marshal(persistedState{
		Term:              term,
		VotedFor:          votedFor,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Snapshot:          snapshot,
		Log:               logEntries,
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
	// On Windows, directory handles do not support FlushFileBuffers and NTFS
	// journals directory metadata automatically.
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

// WALPersister combines atomic snapshot/metadata checkpoints with a high-performance
// append-only write-ahead log for log entries.
type WALPersister struct {
	metaPath string
	walPath  string
	walLog   *wal.Log
}

// NewWALPersister initializes a WAL-backed persister at baseDir for node nodeID.
func NewWALPersister(baseDir, nodeID string) (*WALPersister, error) {
	metaPath := filepath.Join(baseDir, nodeID+".meta.json")
	walPath := filepath.Join(baseDir, nodeID+".entries.wal")

	wlog, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	return &WALPersister{
		metaPath: metaPath,
		walPath:  walPath,
		walLog:   wlog,
	}, nil
}

type walMetaState struct {
	Term              int    `json:"term"`
	VotedFor          string `json:"voted_for"`
	LastIncludedIndex int    `json:"last_included_index,omitempty"`
	LastIncludedTerm  int    `json:"last_included_term,omitempty"`
	Snapshot          []byte `json:"snapshot,omitempty"`
}

// Load recovers the metadata snapshot and replays all entries from the WAL.
func (p *WALPersister) Load() (int, string, []LogEntry, int, int, []byte, error) {
	var meta walMetaState
	data, err := os.ReadFile(p.metaPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: read %s: %w", p.metaPath, err)
	}
	if err == nil {
		if err := json.Unmarshal(data, &meta); err != nil {
			return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: corrupt meta %s: %w", p.metaPath, err)
		}
	}

	rawEntries, err := p.walLog.ReadAll()
	if err != nil {
		return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: read wal %s: %w", p.walPath, err)
	}

	entries := make([]LogEntry, 0, len(rawEntries))
	for _, raw := range rawEntries {
		var ent LogEntry
		if err := json.Unmarshal(raw, &ent); err != nil {
			return 0, "", nil, 0, 0, nil, fmt.Errorf("raft: corrupt wal entry: %w", err)
		}
		entries = append(entries, ent)
	}

	return meta.Term, meta.VotedFor, entries, meta.LastIncludedIndex, meta.LastIncludedTerm, meta.Snapshot, nil
}

// Save atomically writes metadata and updates the WAL.
func (p *WALPersister) Save(term int, votedFor string, logEntries []LogEntry, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) error {
	meta := walMetaState{
		Term:              term,
		VotedFor:          votedFor,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Snapshot:          snapshot,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	dir := filepath.Dir(p.metaPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(p.metaPath)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p.metaPath); err != nil {
		os.Remove(tmpName)
		return err
	}

	// Truncate and rewrite WAL with current logEntries
	if err := p.walLog.Truncate(); err != nil {
		return err
	}
	for _, ent := range logEntries {
		raw, err := json.Marshal(ent)
		if err != nil {
			return err
		}
		if err := p.walLog.Append(raw); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the underlying WAL log file.
func (p *WALPersister) Close() error {
	return p.walLog.Close()
}
