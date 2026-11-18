package main

import (
	"flag"
	"testing"
)

func TestChaosAutoscaleFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	autoscaleWorkers := fs.Bool("autoscale-workers", false, "allow worker count to be managed dynamically by autoscaler")
	traces := fs.Bool("traces", false, "propagate distributed tracing context through submitted tasks")
	err := fs.Parse([]string{"-autoscale-workers", "-traces"})
	if err != nil {
		t.Fatalf("failed to parse autoscale flag: %v", err)
	}
	if !*autoscaleWorkers || !*traces {
		t.Fatalf("expected autoscaleWorkers and traces to be true")
	}
}
