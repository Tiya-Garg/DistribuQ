package tracing

import (
	"context"
	"sync"
	"time"
)

type contextKey struct{}

var activeSpanKey = contextKey{}

// ContextWithSpan injects a Span into a context.
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, activeSpanKey, span)
}

// SpanFromContext retrieves the currently active Span from context, if any.
func SpanFromContext(ctx context.Context) *Span {
	val := ctx.Value(activeSpanKey)
	if span, ok := val.(*Span); ok {
		return span
	}
	return nil
}

// Tracer creates and manages tracing spans.
type Tracer struct {
	mu         sync.RWMutex
	name       string
	spans      []SpanSnapshot
	maxSpans   int
	exporter   SpanExporter
}

// SpanExporter provides an interface for external tracing sinks (e.g. OTLP, in-memory, log).
type SpanExporter interface {
	ExportSpan(snap SpanSnapshot) error
}

// NewTracer constructs a new Tracer instance.
func NewTracer(name string) *Tracer {
	return &Tracer{
		name:     name,
		maxSpans: 500,
	}
}

// SetExporter registers an external SpanExporter for finished spans.
func (t *Tracer) SetExporter(exp SpanExporter) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.exporter = exp
}

// Start initiates a new span, deriving TraceContext from parent context if present.
func (t *Tracer) Start(ctx context.Context, name string, kind SpanKind) (context.Context, *Span) {
	var traceID string
	var parentSpanID string
	flags := "01"

	parent := SpanFromContext(ctx)
	if parent != nil {
		pCtx := parent.TraceContext()
		traceID = pCtx.TraceID
		parentSpanID = pCtx.SpanID
		flags = pCtx.TraceFlags
	}

	if traceID == "" {
		traceID = GenerateTraceID()
	}
	spanID := GenerateSpanID()

	span := &Span{
		name: name,
		kind: kind,
		context: TraceContext{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: flags,
		},
		parentSpan: parentSpanID,
		startTime:  time.Now(),
		attributes: make(map[string]any),
		status:     SpanStatus{Code: StatusUnset},
		tracer:     t,
	}

	return ContextWithSpan(ctx, span), span
}

func (t *Tracer) recordSpan(s *Span) {
	snap := s.Snapshot()
	t.mu.Lock()
	t.spans = append(t.spans, snap)
	if len(t.spans) > t.maxSpans {
		t.spans = t.spans[len(t.spans)-t.maxSpans:]
	}
	exp := t.exporter
	t.mu.Unlock()

	if exp != nil {
		_ = exp.ExportSpan(snap)
	}
}

// CompletedSpans returns a chronological slice of ended span snapshots.
func (t *Tracer) CompletedSpans() []SpanSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]SpanSnapshot, len(t.spans))
	copy(out, t.spans)
	return out
}
