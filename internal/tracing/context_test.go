package tracing

import (
	"context"
	"testing"
)

func TestTraceIDAndSpanIDGeneration(t *testing.T) {
	tid := GenerateTraceID()
	if len(tid) != 32 {
		t.Fatalf("expected 32-char hex trace ID, got %d chars: %s", len(tid), tid)
	}

	sid := GenerateSpanID()
	if len(sid) != 16 {
		t.Fatalf("expected 16-char hex span ID, got %d chars: %s", len(sid), sid)
	}
}

func TestW3CTraceParentParsing(t *testing.T) {
	tc := TraceContext{
		TraceID:    "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:     "00f067aa0ba902b7",
		TraceFlags: "01",
	}

	header := tc.FormatTraceParent()
	expected := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if header != expected {
		t.Fatalf("expected %s, got %s", expected, header)
	}

	parsed, err := ParseTraceParent(header)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if parsed.TraceID != tc.TraceID || parsed.SpanID != tc.SpanID || parsed.TraceFlags != tc.TraceFlags {
		t.Fatalf("parsed context does not match original: %+v vs %+v", parsed, tc)
	}

	_, err = ParseTraceParent("invalid-header")
	if err == nil {
		t.Fatalf("expected error parsing invalid traceparent")
	}
	_ = context.Background()
}
