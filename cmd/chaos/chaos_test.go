package main

import (
	"flag"
	"testing"
)

func TestChaosAutoscaleFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	autoscaleWorkers := fs.Bool("autoscale-workers", false, "allow worker count to be managed dynamically by autoscaler")
	err := fs.Parse([]string{"-autoscale-workers"})
	if err != nil {
		t.Fatalf("failed to parse autoscale flag: %v", err)
	}
	if !*autoscaleWorkers {
		t.Fatalf("expected autoscaleWorkers to be true")
	}
}
