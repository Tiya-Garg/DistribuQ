package wal

import (
	"fmt"
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

func BenchmarkWALAppendParallel(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-parallel-")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "bench-parallel.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	payload := []byte(`{"id":"task-bench","payload":"echo benchmark","priority":1}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(payload); err != nil {
				b.Fatalf("Append failed: %v", err)
			}
		}
	})
}

func BenchmarkWALAppendPayloadSizes(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "bench-wal-size-")
			if err != nil {
				b.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			path := filepath.Join(dir, "bench-size.wal")
			w, err := Open(path)
			if err != nil {
				b.Fatalf("failed to open WAL: %v", err)
			}
			defer w.Close()

			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Append(payload); err != nil {
					b.Fatalf("Append failed: %v", err)
				}
			}
		})
	}
}
