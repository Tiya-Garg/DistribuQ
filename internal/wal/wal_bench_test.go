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
