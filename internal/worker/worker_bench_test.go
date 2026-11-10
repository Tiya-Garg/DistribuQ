package worker

import (
	"testing"
)

func BenchmarkWorkerExecutionNoop(b *testing.B) {
	w := &Worker{
		ID:   "bench-worker",
		Addr: "localhost:9999",
		Execute: func(taskID, payload string) error {
			return nil
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Execute("task-id", "echo noop"); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}
