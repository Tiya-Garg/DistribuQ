package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkWALAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-append-")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "bench.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	payload := []byte(`{"id":"task-bench","payload":"echo benchmark","priority":1}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(payload); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

func BenchmarkWALReadAll(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-readall-")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "bench-read.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	payload := []byte(`{"id":"task-bench","payload":"echo benchmark","priority":1}`)
	for i := 0; i < 500; i++ {
		if err := w.Append(payload); err != nil {
			b.Fatalf("pre-fill Append failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := w.ReadAll()
		if err != nil {
			b.Fatalf("ReadAll failed: %v", err)
		}
		if len(records) != 500 {
			b.Fatalf("expected 500 records, got %d", len(records))
		}
	}
}
