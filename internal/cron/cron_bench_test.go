package cron

import (
	"testing"
	"time"
)

func BenchmarkCronParse(b *testing.B) {
	expr := "*/15 2 * * 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(expr); err != nil {
			b.Fatalf("Parse failed: %v", err)
		}
	}
}

func BenchmarkCronNextOccurrence(b *testing.B) {
	sched, err := Parse("*/15 2 * * 1")
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sched.Next(now)
	}
}
