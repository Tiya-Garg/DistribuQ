package scheduler

import (
	"fmt"
	"testing"
	"time"
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

func BenchmarkSchedulerIdempotentDeduplication(b *testing.B) {
	s := NewScheduler(nil)
	key := "bench-shared-idempotency-key"
	taskID := "task-idempotent-init"
	_, _ = s.SubmitTaskIdempotent(taskID, "echo initial", 1, "", time.Time{}, nil, key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.SubmitTaskIdempotent(fmt.Sprintf("retry-%d", i), "echo initial", 1, "", time.Time{}, nil, key)
		if err != nil {
			b.Fatalf("SubmitTaskIdempotent retry error: %v", err)
		}
	}
}
