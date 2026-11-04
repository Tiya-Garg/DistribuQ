package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReadAll(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	log, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer log.Close()

	payloads := [][]byte{
		[]byte("entry 1: submit task A"),
		[]byte("entry 2: assign task A to worker W1"),
		[]byte("entry 3: task A completed"),
	}

	for _, p := range payloads {
		if err := log.Append(p); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	read, err := log.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(read) != len(payloads) {
		t.Fatalf("expected %d entries, got %d", len(payloads), len(read))
	}

	for i := range payloads {
		if !bytes.Equal(read[i], payloads[i]) {
			t.Errorf("entry %d mismatch: got %q, want %q", i, string(read[i]), string(payloads[i]))
		}
	}

	// Re-open and verify persistence
	log.Close()

	log2, err := Open(walPath)
	if err != nil {
		t.Fatalf("re-open failed: %v", err)
	}
	defer log2.Close()

	read2, err := log2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reopen failed: %v", err)
	}
	if len(read2) != len(payloads) {
		t.Fatalf("expected %d entries after reopen, got %d", len(payloads), len(read2))
	}
}

func TestWALDetectsCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "corrupt.wal")

	log, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := log.Append([]byte("valid payload")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	log.Close()

	// Corrupt a byte in the payload
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	log2, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer log2.Close()

	_, err = log2.ReadAll()
	if err != ErrCorruptRecord {
		t.Fatalf("expected ErrCorruptRecord, got %v", err)
	}
}

func TestWALTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "truncate.wal")

	log, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer log.Close()

	if err := log.Append([]byte("some data")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if err := log.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	entries, err := log.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after truncate, got %d", len(entries))
	}
}
