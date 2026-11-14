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

	"github.com/Tiya-Garg/DistribuQ/internal/security"
)

// Config defines the benchmark parameters for the run.
type Config struct {
	Schedulers  string
	Tasks       int
	Workers     int
	Concurrency int
	RateLimit   int
	PayloadSize int
	Timeout     time.Duration
	CACert      string
	TLSCert     string
	TLSKey      string
	ServerName  string
	Verify      bool
	MeasureLogs bool
}

func main() {
	schedulers := flag.String("schedulers", "localhost:8000", "comma-separated scheduler replica addresses")
	tasks := flag.Int("tasks", 1000, "total number of tasks to submit")
	concurrency := flag.Int("concurrency", 16, "number of concurrent submission worker goroutines")
	rateLimit := flag.Int("rate", 0, "max submissions per second (0 for uncapped)")
	payloadSize := flag.Int("payload-size", 64, "byte size of synthetic task payload")
	timeout := flag.Duration("timeout", 30*time.Second, "overall benchmark timeout")
	caCert := flag.String("ca-cert", "", "path to CA root certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "path to client TLS certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "path to client TLS private key for mTLS")
	serverName := flag.String("server-name", "localhost", "expected scheduler TLS server name (SAN)")
	verify := flag.Bool("verify", false, "wait and verify all submitted tasks transition to completed state")
	measureLogs := flag.Bool("measure-logs", false, "measure task log streaming overhead and retrieval")
	jsonOutput := flag.Bool("json", false, "output results in JSON format")
	flag.Parse()

	cfg := Config{
		Schedulers:  *schedulers,
		Tasks:       *tasks,
		Concurrency: *concurrency,
		RateLimit:   *rateLimit,
		PayloadSize: *payloadSize,
		Timeout:     *timeout,
		CACert:      *caCert,
		TLSCert:     *tlsCert,
		TLSKey:      *tlsKey,
		ServerName:  *serverName,
		Verify:      *verify,
		MeasureLogs: *measureLogs,
	}

	fmt.Printf("DistribuQ Benchmark Harness initialized with %d tasks, concurrency %d\n", cfg.Tasks, cfg.Concurrency)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	addrs := ParseSchedulers(cfg.Schedulers)

	tlsCfg := &security.TLSConfig{
		CACertPath: cfg.CACert,
		CertPath:   cfg.TLSCert,
		KeyPath:    cfg.TLSKey,
		ServerName: cfg.ServerName,
	}
	dialOpt, err := tlsCfg.DialOption()
	if err != nil {
		log.Fatalf("failed to configure TLS options: %v", err)
	}

	pool, err := NewSchedulerPoolWithTLS(addrs, dialOpt)
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
	if *jsonOutput {
		if err := PrintJSONReport(os.Stdout, stats); err != nil {
			log.Fatalf("failed to print JSON report: %v", err)
		}
	} else {
		PrintReport(os.Stdout, stats)
	}
}
