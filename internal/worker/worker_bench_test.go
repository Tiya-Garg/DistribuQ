package worker

import (
	"testing"
	"time"
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

func BenchmarkWorkerExecutionShell(b *testing.B) {
	exec := NewShellExecutor("bench-shell", 5*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := exec("bench-task", "exit 0"); err != nil {
			b.Fatalf("shell execution failed: %v", err)
		}
	}
}
