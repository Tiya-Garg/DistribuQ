package scheduler

import (
	"context"
	"testing"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCSchedulerServer(t *testing.T) {
	s := NewScheduler((&fakeDispatch{}).fn)
	server := &GRPCSchedulerServer{
		S:        s,
		IsLeader: func() bool { return true },
	}

	ctx := context.Background()

	// 1. RegisterWorker
	regResp, err := server.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: "w1",
		Addr:     "localhost:9001",
	})
	if err != nil || !regResp.Ok {
		t.Fatalf("RegisterWorker failed: %v", err)
	}

	// 2. SubmitTask
	submitResp, err := server.SubmitTask(ctx, &pb.SubmitTaskRequest{
		TaskId:  "t1",
		Payload: "payload-1",
	})
	if err != nil || !submitResp.Ok {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	// Duplicate submit should return AlreadyExists
	_, err = server.SubmitTask(ctx, &pb.SubmitTaskRequest{
		TaskId:  "t1",
		Payload: "payload-1",
	})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	// Empty ID should return InvalidArgument
	_, err = server.SubmitTask(ctx, &pb.SubmitTaskRequest{
		TaskId:  "",
		Payload: "payload",
	})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}

	// 3. Heartbeat
	hbResp, err := server.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "w1"})
	if err != nil || !hbResp.Ok {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// Unknown worker heartbeat returns NotFound
	_, err = server.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "unknown-worker"})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}

	// 4. CompleteTask
	compResp, err := server.CompleteTask(ctx, &pb.CompleteTaskRequest{
		TaskId:   "t1",
		WorkerId: "w1",
	})
	if err != nil || !compResp.Ok {
		t.Fatalf("CompleteTask failed: %v", err)
	}

	// 5. FailTask
	_, _ = server.SubmitTask(ctx, &pb.SubmitTaskRequest{TaskId: "t2", Payload: "fail-payload"})
	failResp, err := server.FailTask(ctx, &pb.FailTaskRequest{
		TaskId:   "t2",
		WorkerId: "w1",
		Cause:    "exit 1",
	})
	if err != nil || !failResp.Ok {
		t.Fatalf("FailTask failed: %v", err)
	}

	// 6. GetStatus
	statusResp, err := server.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if statusResp.WorkerCount != 1 {
		t.Fatalf("expected 1 worker, got %d", statusResp.WorkerCount)
	}
	if statusResp.Tasks["t1"] != string(TaskCompleted) {
		t.Fatalf("expected t1 completed, got %v", statusResp.Tasks["t1"])
	}
}

func TestGRPCSchedulerNotLeader(t *testing.T) {
	s := NewScheduler((&fakeDispatch{}).fn)
	server := &GRPCSchedulerServer{
		S:        s,
		IsLeader: func() bool { return false },
	}

	_, err := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{TaskId: "t1", Payload: "test"})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for non-leader submit, got %v", err)
	}
}
