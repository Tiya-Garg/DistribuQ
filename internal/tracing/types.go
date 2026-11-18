package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// SpanKind defines the role a span plays in a distributed trace.
type SpanKind string

const (
	SpanKindInternal SpanKind = "INTERNAL"
	SpanKindServer   SpanKind = "SERVER"
	SpanKindClient   SpanKind = "CLIENT"
	SpanKindProducer SpanKind = "PRODUCER"
	SpanKindConsumer SpanKind = "CONSUMER"
)

// StatusCode indicates whether a traced operation succeeded or errored.
type StatusCode string

const (
	StatusUnset StatusCode = "UNSET"
	StatusOK    StatusCode = "OK"
	StatusError StatusCode = "ERROR"
)

// SpanStatus describes the result status and optional message of a span.
type SpanStatus struct {
	Code        StatusCode `json:"code"`
	Description string     `json:"description,omitempty"`
}

// SpanEvent records an instantaneous event occurrence within a span.
type SpanEvent struct {
	Name       string         `json:"name"`
	Timestamp  time.Time      `json:"timestamp"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// GenerateTraceID creates a new cryptographically random 16-byte hex TraceID.
func GenerateTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// GenerateSpanID creates a new cryptographically random 8-byte hex SpanID.
func GenerateSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
