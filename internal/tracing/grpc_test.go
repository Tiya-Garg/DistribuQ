package tracing

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestGRPCMetadataInjectionAndExtraction(t *testing.T) {
	tracer := NewTracer("grpc-test")
	ctx, span := tracer.Start(context.Background(), "client-call", SpanKindClient)
	defer span.End()

	outCtx := InjectGRPC(ctx, span)
	md, ok := metadata.FromOutgoingContext(outCtx)
	if !ok {
		t.Fatalf("expected outgoing metadata to be set")
	}

	vals := md.Get(HeaderTraceParent)
	if len(vals) == 0 {
		t.Fatalf("expected traceparent header in outgoing metadata")
	}

	// Simulate server receiving incoming metadata
	inCtx := metadata.NewIncomingContext(context.Background(), md)
	extracted, ok := ExtractGRPC(inCtx)
	if !ok {
		t.Fatalf("expected ExtractGRPC to succeed")
	}

	if extracted.TraceID != span.TraceContext().TraceID {
		t.Fatalf("extracted TraceID %s does not match span TraceID %s", extracted.TraceID, span.TraceContext().TraceID)
	}
	if extracted.SpanID != span.TraceContext().SpanID {
		t.Fatalf("extracted SpanID %s does not match span SpanID %s", extracted.SpanID, span.TraceContext().SpanID)
	}
}
