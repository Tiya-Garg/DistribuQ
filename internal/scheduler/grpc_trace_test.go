package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/tracing"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc/metadata"
)

func TestGRPCSubmitTaskWithTracePropagation(t *testing.T) {
	sched := NewScheduler(nil)
	server := &GRPCSchedulerServer{
		S: sched,
	}

	tracer := tracing.NewTracer("test-client")
	ctx, span := tracer.Start(context.Background(), "submit-call", tracing.SpanKindClient)
	defer span.End()

	// Inject trace into outgoing metadata
	outCtx := tracing.InjectGRPC(ctx, span)
	md, _ := metadata.FromOutgoingContext(outCtx)
	inCtx := metadata.NewIncomingContext(context.Background(), md)

	resp, err := server.SubmitTask(inCtx, &pb.SubmitTaskRequest{
		TaskId:   "trace-t1",
		Payload:  "hello trace",
		Priority: 1,
	})
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}
	if !resp.Ok || resp.TaskId != "trace-t1" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	task, exists := sched.Task("trace-t1")
	if !exists {
		t.Fatalf("expected task trace-t1 to exist in scheduler")
	}

	if task.TraceParent != span.TraceContext().FormatTraceParent() {
		t.Fatalf("expected task TraceParent %s to match span traceparent %s", task.TraceParent, span.TraceContext().FormatTraceParent())
	}
	_ = time.Now()
}
