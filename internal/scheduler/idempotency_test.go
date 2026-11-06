package scheduler

import (
	"context"
	"testing"
	"time"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
)

func TestIdempotencyKeyDeduplication(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	s.RegisterWorker("w1", "127.0.0.1:9001")

	// First submission with idempotency key
	t1, err := s.SubmitTaskIdempotent("task-1", "do-work", 1, "", time.Time{}, nil, "idem-key-1")
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}
	if t1.ID != "task-1" {
		t.Fatalf("expected task ID task-1, got %s", t1.ID)
	}

	// Second submission with same idempotency key and same payload, but different requested task ID
	t2, err := s.SubmitTaskIdempotent("task-2", "do-work", 1, "", time.Time{}, nil, "idem-key-1")
	if err != nil {
		t.Fatalf("second submission failed: %v", err)
	}
	// Must return existing task-1
	if t2.ID != "task-1" {
		t.Fatalf("expected returned task ID to be task-1, got %s", t2.ID)
	}

	s.mu.Lock()
	_, task2Exists := s.tasks["task-2"]
	numTasks := len(s.tasks)
	s.mu.Unlock()

	if task2Exists {
		t.Fatalf("task-2 should not exist in scheduler tasks map")
	}
	if numTasks != 1 {
		t.Fatalf("expected exactly 1 task in scheduler, got %d", numTasks)
	}
}

func TestIdempotencyKeyConflict(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	defer s.Stop()

	// Initial submission
	_, err := s.SubmitTaskIdempotent("task-initial", "original-payload", 1, "", time.Time{}, nil, "conflict-key")
	if err != nil {
		t.Fatalf("initial submission failed: %v", err)
	}

	// Conflicting submission with same idempotency key but different payload
	_, err = s.SubmitTaskIdempotent("task-conflicting", "different-payload", 1, "", time.Time{}, nil, "conflict-key")
	if err != ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestIdempotencySnapshotRestore(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	_, err := s.SubmitTaskIdempotent("task-snap-1", "snap-payload", 0, "", time.Time{}, nil, "snap-key-99")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	snapBytes, err := s.EncodeState()
	if err != nil {
		t.Fatalf("EncodeState: %v", err)
	}
	s.Stop()

	// Restore into fresh scheduler
	fd2 := &fakeDispatch{}
	s2 := NewScheduler(fd2.fn)
	defer s2.Stop()
	if err := s2.RestoreFromSnapshot(snapBytes); err != nil {
		t.Fatalf("RestoreFromSnapshot: %v", err)
	}

	// Submit with the same idempotency key in restored scheduler
	tDup, err := s2.SubmitTaskIdempotent("task-snap-2", "snap-payload", 0, "", time.Time{}, nil, "snap-key-99")
	if err != nil {
		t.Fatalf("submit in restored scheduler failed: %v", err)
	}
	if tDup.ID != "task-snap-1" {
		t.Fatalf("expected task ID task-snap-1 after restore deduplication, got %s", tDup.ID)
	}

	s2.mu.Lock()
	numTasks := len(s2.tasks)
	s2.mu.Unlock()
	if numTasks != 1 {
		t.Fatalf("expected exactly 1 task in restored scheduler, got %d", numTasks)
	}

	// Verify TaskByIdempotencyKey on restored scheduler
	lookedUp, ok := s2.TaskByIdempotencyKey("snap-key-99")
	if !ok || lookedUp == nil || lookedUp.ID != "task-snap-1" {
		t.Fatalf("expected TaskByIdempotencyKey to return task-snap-1 on restored scheduler")
	}

	// Conflicting payload on restored scheduler must still trigger ErrIdempotencyConflict
	_, err = s2.SubmitTaskIdempotent("task-snap-conflict", "different-payload", 0, "", time.Time{}, nil, "snap-key-99")
	if err != ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict on restored scheduler, got %v", err)
	}
}

func TestIdempotencyGRPCService(t *testing.T) {
	s := NewScheduler(func(workerID, addr string, task *Task) error { return nil })
	defer s.Stop()

	server := &GRPCSchedulerServer{
		S:        s,
		IsLeader: func() bool { return true },
	}

	// First gRPC SubmitTask
	resp1, err := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         "grpc-task-1",
		Payload:        "grpc-payload",
		IdempotencyKey: "grpc-token-1",
	})
	if err != nil {
		t.Fatalf("first gRPC SubmitTask failed: %v", err)
	}
	if !resp1.Ok || resp1.TaskId != "grpc-task-1" {
		t.Fatalf("unexpected resp1: ok=%v, taskId=%s", resp1.Ok, resp1.TaskId)
	}

	// Second gRPC SubmitTask with same idempotency key and payload
	resp2, err := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         "grpc-task-2",
		Payload:        "grpc-payload",
		IdempotencyKey: "grpc-token-1",
	})
	if err != nil {
		t.Fatalf("second gRPC SubmitTask failed: %v", err)
	}
	// Response must report ok=true and TaskId pointing to original task
	if !resp2.Ok || resp2.TaskId != "grpc-task-1" {
		t.Fatalf("expected resp2 to return original TaskId grpc-task-1, got ok=%v taskId=%s", resp2.Ok, resp2.TaskId)
	}
}
