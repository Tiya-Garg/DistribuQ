package main

import (
	"fmt"
	"io"
)

// PrintReport formats and outputs a clean human-readable benchmark report.
func PrintReport(w io.Writer, s Stats) {
	fmt.Fprintf(w, "\n===================================================\n")
	fmt.Fprintf(w, "            DistribuQ Benchmark Results            \n")
	fmt.Fprintf(w, "===================================================\n")
	fmt.Fprintf(w, "Total Requests:      %d\n", s.TotalRequests)
	fmt.Fprintf(w, "Successful:          %d\n", s.Successes)
	fmt.Fprintf(w, "Failed:              %d\n", s.Failures)
	fmt.Fprintf(w, "Total Duration:      %v\n", s.Duration)
	fmt.Fprintf(w, "Throughput:          %.2f req/sec\n", s.Throughput)
	fmt.Fprintf(w, "---------------------------------------------------\n")
	fmt.Fprintf(w, "Latency P50 (median): %v\n", s.P50)
	fmt.Fprintf(w, "Latency P90:          %v\n", s.P90)
	fmt.Fprintf(w, "Latency P95:          %v\n", s.P95)
	fmt.Fprintf(w, "Latency P99:          %v\n", s.P99)
	fmt.Fprintf(w, "===================================================\n")
}
