package metrics

import (
	"io"
	"testing"
)

func BenchmarkMetricsCounterIncrements(b *testing.B) {
	r := &Registry{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.IncSubmitted()
		r.IncCompleted()
	}
}

func BenchmarkMetricsWritePrometheus(b *testing.B) {
	r := &Registry{}
	r.IncSubmitted()
	r.IncCompleted()
	gauges := map[string]float64{
		"workers_active": 5,
		"raft_term":      2,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.WritePrometheus(io.Discard, gauges)
	}
}
