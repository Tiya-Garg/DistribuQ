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
