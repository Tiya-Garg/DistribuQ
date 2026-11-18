package main

import (
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/tracing"
)

func TestSubmitTraceparentFlagParsing(t *testing.T) {
	header := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, err := tracing.ParseTraceParent(header)
	if err != nil {
		t.Fatalf("failed to parse valid traceparent: %v", err)
	}
	if tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected TraceID: %s", tc.TraceID)
	}
}
