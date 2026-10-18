package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Persister durably stores the two pieces of Raft state that must
// survive a restart: currentTerm and votedFor. If a node forgets its
// vote across a crash, it can vote for a second candidate in the same
// term and two leaders can be elected.
//
// Save must not return until the data is on stable storage.
type Persister interface {
	Load() (term int, votedFor string, err error)
	Save(term int, votedFor string) error
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
	Term     int    `json:"term"`
	VotedFor string `json:"voted_for"`
}

// Load returns the saved state. A missing file is a fresh node, not an
// error: it returns term 0 and no vote.
func (p *FilePersister) Load() (int, string, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("raft: read %s: %w", p.path, err)
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, "", fmt.Errorf("raft: corrupt state file %s: %w", p.path, err)
	}
	return st.Term, st.VotedFor, nil
}

// Save atomically and durably replaces the stored state.
func (p *FilePersister) Save(term int, votedFor string) error {
	data, err := json.Marshal(persistedState{Term: term, VotedFor: votedFor})
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
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
