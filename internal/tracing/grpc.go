package tracing

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	HeaderTraceParent = "traceparent"
)

// InjectGRPC injects the active span's W3C traceparent header into outgoing gRPC context metadata.
func InjectGRPC(ctx context.Context, span *Span) context.Context {
	if span == nil {
		return ctx
	}
	traceparent := span.TraceContext().FormatTraceParent()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md.Set(HeaderTraceParent, traceparent)
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractGRPC parses incoming gRPC metadata and extracts any existing TraceContext.
func ExtractGRPC(ctx context.Context) (TraceContext, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return TraceContext{}, false
	}
	vals := md.Get(HeaderTraceParent)
	if len(vals) == 0 {
		return TraceContext{}, false
	}
	tc, err := ParseTraceParent(vals[0])
	if err != nil {
		return TraceContext{}, false
	}
	return tc, true
}
