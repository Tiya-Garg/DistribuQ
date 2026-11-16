package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRunnerMeasureLogsFlag(t *testing.T) {
	cfg := Config{
		Tasks:       10,
		Concurrency: 2,
		MeasureLogs: true,
		Autoscale:   true,
		Webhooks:    true,
		Timeout:     5 * time.Second,
	}
	runner := &Runner{
		cfg: cfg,
	}
	var ids sync.Map
	ids.Store("task-test-1", true)
	// Calling with empty pool shouldn't panic
	runner.measureTaskLogs(context.Background(), &ids)
}
