package scheduler

import (
	"fmt"
	"testing"
)

func BenchmarkSchedulerSubmitTask(b *testing.B) {
	s := NewScheduler(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		if _, err := s.SubmitTask(taskID, "echo test"); err != nil {
			b.Fatalf("SubmitTask failed: %v", err)
		}
	}
}
