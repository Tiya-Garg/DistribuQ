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

func BenchmarkSchedulerPriorityQueueSorting(b *testing.B) {
	s := NewScheduler(nil)
	priorities := []int{0, 1, 2, 3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		taskID := fmt.Sprintf("prio-task-%d", i)
		prio := priorities[i%len(priorities)]
		if _, err := s.SubmitTaskWithPriority(taskID, "echo prio", prio); err != nil {
			b.Fatalf("SubmitTaskWithPriority failed: %v", err)
		}
	}
}
