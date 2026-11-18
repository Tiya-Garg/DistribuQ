package tracing

import (
	"sync"
	"time"
)

// Span represents a single timed operation unit in a distributed trace.
type Span struct {
	mu         sync.RWMutex
	name       string
	kind       SpanKind
	context    TraceContext
	parentSpan string
	startTime  time.Time
	endTime    time.Time
	status     SpanStatus
	attributes map[string]any
	events     []SpanEvent
	tracer     *Tracer
}

// SpanSnapshot represents an immutable exportable snapshot of an ended span.
type SpanSnapshot struct {
	Name         string         `json:"name"`
	Kind         SpanKind       `json:"kind"`
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	StartTime    time.Time      `json:"start_time"`
	EndTime      time.Time      `json:"end_time"`
	DurationMs   float64        `json:"duration_ms"`
	Status       SpanStatus     `json:"status"`
	Attributes   map[string]any `json:"attributes"`
	Events       []SpanEvent    `json:"events"`
}

// TraceContext returns the W3C trace context for this span.
func (s *Span) TraceContext() TraceContext {
	return s.context
}

// SetAttribute adds or updates a key-value attribute on the span.
func (s *Span) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attributes[key] = value
}

// AddEvent attaches a timed event with attributes to the span.
func (s *Span) AddEvent(name string, attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// SetStatus records the final execution outcome of the span.
func (s *Span) SetStatus(code StatusCode, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = SpanStatus{Code: code, Description: description}
}

// Snapshot produces an exportable SpanSnapshot.
func (s *Span) Snapshot() SpanSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	attrs := make(map[string]any, len(s.attributes))
	for k, v := range s.attributes {
		attrs[k] = v
	}
	events := make([]SpanEvent, len(s.events))
	copy(events, s.events)

	dur := s.endTime.Sub(s.startTime).Seconds() * 1000
	if s.endTime.IsZero() {
		dur = time.Since(s.startTime).Seconds() * 1000
	}

	return SpanSnapshot{
		Name:         s.name,
		Kind:         s.kind,
		TraceID:      s.context.TraceID,
		SpanID:       s.context.SpanID,
		ParentSpanID: s.parentSpan,
		StartTime:    s.startTime,
		EndTime:      s.endTime,
		DurationMs:   dur,
		Status:       s.status,
		Attributes:   attrs,
		Events:       events,
	}
}

// End completes span timing and records it to the tracer.
func (s *Span) End() {
	s.mu.Lock()
	if s.endTime.IsZero() {
		s.endTime = time.Now()
	}
	s.mu.Unlock()

	if s.tracer != nil {
		s.tracer.recordSpan(s)
	}
}
