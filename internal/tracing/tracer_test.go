package tracing

import (
	"context"
	"testing"
	"time"
)

type mockExporter struct {
	exported []SpanSnapshot
}

func (m *mockExporter) ExportSpan(snap SpanSnapshot) error {
	m.exported = append(m.exported, snap)
	return nil
}

func TestTracerStartAndEndSpan(t *testing.T) {
	exp := &mockExporter{}
	tracer := NewTracer("test-tracer")
	tracer.SetExporter(exp)

	ctx := context.Background()
	ctx, rootSpan := tracer.Start(ctx, "root-operation", SpanKindServer)
	rootSpan.SetAttribute("tenant", "acme")
	rootSpan.AddEvent("cache_miss", map[string]any{"key": "abc"})

	// Nested child span
	_, childSpan := tracer.Start(ctx, "child-operation", SpanKindInternal)
	childSpan.SetAttribute("db.query", "SELECT 1")
	time.Sleep(5 * time.Millisecond)
	childSpan.SetStatus(StatusOK, "success")
	childSpan.End()

	rootSpan.SetStatus(StatusOK, "completed")
	rootSpan.End()

	spans := tracer.CompletedSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 completed spans, got %d", len(spans))
	}

	// Verify parent-child correlation
	childSnap := spans[0]
	rootSnap := spans[1]

	if childSnap.TraceID != rootSnap.TraceID {
		t.Fatalf("expected identical trace IDs, got %s vs %s", childSnap.TraceID, rootSnap.TraceID)
	}
	if childSnap.ParentSpanID != rootSnap.SpanID {
		t.Fatalf("expected child parent span ID %s to equal root span ID %s", childSnap.ParentSpanID, rootSnap.SpanID)
	}

	if len(exp.exported) != 2 {
		t.Fatalf("expected 2 exported spans in mock exporter, got %d", len(exp.exported))
	}
}
