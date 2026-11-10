// Command bench runs end-to-end performance and latency benchmarks against a
// running DistribuQ cluster, measuring throughput (tasks/sec) and latency percentiles.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

// Config defines the benchmark parameters for the run.
type Config struct {
	Schedulers string
	Tasks      int
	Workers    int
	Concurrency int
	RateLimit  int
	PayloadSize int
	Timeout    time.Duration
}

func main() {
	schedulers := flag.String("schedulers", "localhost:8000", "comma-separated scheduler replica addresses")
	tasks := flag.Int("tasks", 1000, "total number of tasks to submit")
	concurrency := flag.Int("concurrency", 16, "number of concurrent submission worker goroutines")
	rateLimit := flag.Int("rate", 0, "max submissions per second (0 for uncapped)")
	payloadSize := flag.Int("payload-size", 64, "byte size of synthetic task payload")
	timeout := flag.Duration("timeout", 30*time.Second, "overall benchmark timeout")
	flag.Parse()

	cfg := Config{
		Schedulers:  *schedulers,
		Tasks:       *tasks,
		Concurrency: *concurrency,
		RateLimit:   *rateLimit,
		PayloadSize: *payloadSize,
		Timeout:     *timeout,
	}

	fmt.Printf("DistribuQ Benchmark Harness initialized with %d tasks, concurrency %d\n", cfg.Tasks, cfg.Concurrency)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	addrs := ParseSchedulers(cfg.Schedulers)
	pool, err := NewSchedulerPool(addrs)
	if err != nil {
		log.Fatalf("failed to connect to schedulers: %v", err)
	}
	defer pool.Close()

	collector := NewMetricsCollector()
	runner := NewRunner(cfg, pool, collector)

	if err := runner.Run(ctx); err != nil {
		log.Fatalf("benchmark run error: %v", err)
	}

	stats := collector.ComputeStats()
	PrintReport(os.Stdout, stats)
}
