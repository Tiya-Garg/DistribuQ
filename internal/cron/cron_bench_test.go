package cron

import (
	"testing"
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
