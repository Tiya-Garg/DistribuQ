package main

import (
	"sort"
	"sync"
	"time"
)

// MetricsCollector tracks submission latencies and calculates summary statistics.
type MetricsCollector struct {
	mu        sync.Mutex
	latencies []time.Duration
	successes int
	failures  int
	startTime time.Time
	endTime   time.Time
}

// NewMetricsCollector creates a new initialized MetricsCollector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		latencies: make([]time.Duration, 0, 1000),
		startTime: time.Now(),
	}
}

// RecordSuccess records a successful RPC invocation with its latency.
func (m *MetricsCollector) RecordSuccess(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, d)
	m.successes++
}

// RecordFailure increments the failure counter.
func (m *MetricsCollector) RecordFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures++
}

// Finish marks the completion time of the benchmark run.
func (m *MetricsCollector) Finish() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.endTime = time.Now()
}

// Percentile calculates the given percentile (e.g. 50, 95, 99) from collected latencies.
func (m *MetricsCollector) Percentile(p float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.latencies) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int(float64(len(sorted)-1) * (p / 100.0))
	return sorted[idx]
}

// Stats returns a summary snapshot of results.
type Stats struct {
	TotalRequests int
	Successes     int
	Failures      int
	Duration      time.Duration
	Throughput    float64
	P50           time.Duration
	P90           time.Duration
	P95           time.Duration
	P99           time.Duration
}

// ComputeStats calculates the aggregate throughput and latency percentiles.
func (m *MetricsCollector) ComputeStats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()

	dur := m.endTime.Sub(m.startTime)
	if dur <= 0 {
		dur = time.Millisecond
	}

	throughput := float64(m.successes) / dur.Seconds()

	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var p50, p90, p95, p99 time.Duration
	if n := len(sorted); n > 0 {
		calcP := func(pct float64) time.Duration {
			idx := int(float64(n) * (pct / 100.0))
			if idx >= n {
				idx = n - 1
			}
			return sorted[idx]
		}
		p50 = calcP(50)
		p90 = calcP(90)
		p95 = calcP(95)
		p99 = calcP(99)
	}

	return Stats{
		TotalRequests: m.successes + m.failures,
		Successes:     m.successes,
		Failures:      m.failures,
		Duration:      dur,
		Throughput:    throughput,
		P50:           p50,
		P90:           p90,
		P95:           p95,
		P99:           p99,
	}
}
