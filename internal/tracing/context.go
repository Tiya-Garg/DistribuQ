package tracing

import (
	"fmt"
	"strings"
)

// TraceContext encapsulates distributed trace propagation state (W3C Trace Context compliant).
type TraceContext struct {
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	TraceFlags string `json:"trace_flags"` // "01" sampled, "00" not sampled
}

// FormatTraceParent returns the W3C traceparent header format: 00-{traceid}-{spanid}-{traceflags}.
func (tc TraceContext) FormatTraceParent() string {
	flags := tc.TraceFlags
	if flags == "" {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, flags)
}

// ParseTraceParent parses a W3C traceparent string into a TraceContext.
func ParseTraceParent(header string) (TraceContext, error) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return TraceContext{}, fmt.Errorf("invalid traceparent format: %q", header)
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 {
		return TraceContext{}, fmt.Errorf("invalid trace/span ID length in traceparent: %q", header)
	}
	return TraceContext{
		TraceID:    parts[1],
		SpanID:     parts[2],
		TraceFlags: parts[3],
	}, nil
}
