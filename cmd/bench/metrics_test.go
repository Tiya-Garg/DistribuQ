package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMetricsCollectorPercentiles(t *testing.T) {
	collector := NewMetricsCollector()
	collector.RecordSuccess(10 * time.Millisecond)
	collector.RecordSuccess(20 * time.Millisecond)
	collector.RecordSuccess(30 * time.Millisecond)
	collector.RecordSuccess(40 * time.Millisecond)
	collector.RecordSuccess(50 * time.Millisecond)
	collector.RecordFailure()
	collector.Finish()

	stats := collector.ComputeStats()
	if stats.Successes != 5 {
		t.Fatalf("expected 5 successes, got %d", stats.Successes)
	}
	if stats.Failures != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.Failures)
	}
	if stats.P50 != 30*time.Millisecond {
		t.Fatalf("expected P50=30ms, got %v", stats.P50)
	}
	if stats.P99 != 50*time.Millisecond {
		t.Fatalf("expected P99=50ms, got %v", stats.P99)
	}

	var buf bytes.Buffer
	PrintReport(&buf, stats)
	out := buf.String()
	if !strings.Contains(out, "DistribuQ Benchmark Results") {
		t.Fatalf("report missing header title: %s", out)
	}

	var jsonBuf bytes.Buffer
	if err := PrintJSONReport(&jsonBuf, stats); err != nil {
		t.Fatalf("PrintJSONReport error: %v", err)
	}
	if !strings.Contains(jsonBuf.String(), `"TotalRequests": 6`) {
		t.Fatalf("unexpected json report output: %s", jsonBuf.String())
	}
}
