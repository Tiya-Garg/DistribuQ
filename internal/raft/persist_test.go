package raft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilePersisterMissingFileIsFreshNode(t *testing.T) {
	p := NewFilePersister(filepath.Join(t.TempDir(), "n1.json"))
	term, votedFor, logEntries, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 || votedFor != "" || len(logEntries) != 0 {
		t.Fatalf("want (0, \"\", empty log), got (%d, %q, %d entries)", term, votedFor, len(logEntries))
	}
}

func TestFilePersisterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.json")
	if err := NewFilePersister(path).Save(7, "n2", nil); err != nil {
		t.Fatal(err)
	}
	// A new FilePersister on the same path stands in for a restart.
	term, votedFor, _, err := NewFilePersister(path).Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 7 || votedFor != "n2" {
		t.Fatalf("want (7, n2), got (%d, %q)", term, votedFor)
	}
}

func TestFilePersisterOverwriteLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	p := NewFilePersister(filepath.Join(dir, "n1.json"))
	for term := 1; term <= 5; term++ {
		if err := p.Save(term, "n1", nil); err != nil {
			t.Fatal(err)
		}
	}
	term, _, _, _ := p.Load()
	if term != 5 {
		t.Fatalf("want term 5, got %d", term)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestFilePersisterCorruptFileIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := NewFilePersister(path).Load(); err == nil {
		t.Fatal("expected an error for a corrupt state file")
	}
}
