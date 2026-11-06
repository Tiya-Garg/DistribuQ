package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicatedSubmissionAndIdempotency(t *testing.T) {
	node := raft.NewNode("s1", nil, nil)
	go node.Run()
	defer node.Stop()

	// Wait for leader election
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		t.Fatalf("expected node to become leader, got %v", node.State())
	}

	fd := &fakeDispatch{}
	sched := NewScheduler(fd.fn)
	defer sched.Stop()

	sched.SetRaftProposer(node)
	node.OnApply(func(entry raft.LogEntry) {
		sched.ApplyEntry(entry)
	})

	server := &GRPCSchedulerServer{
		S:        sched,
		IsLeader: func() bool { return node.State() == raft.Leader },
	}

	// 1. Submit a task with a tag no worker has (reproduces Bug 1: nil pointer panic)
	resp, err := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:      "task-nonexistent-tag",
		Payload:     "sleep 10",
		RequiredTag: "nonexistent",
	})
	if err != nil {
		t.Fatalf("SubmitTask with nonexistent tag failed: %v", err)
	}
	if !resp.Ok || resp.TaskId != "task-nonexistent-tag" {
		t.Fatalf("unexpected resp: ok=%v taskId=%s", resp.Ok, resp.TaskId)
	}

	sched.mu.Lock()
	tNonExistent, exists := sched.tasks["task-nonexistent-tag"]
	sched.mu.Unlock()
	if !exists || tNonExistent == nil {
		t.Fatalf("expected task-nonexistent-tag to be applied to scheduler state machine")
	}

	// 2. Submit two tasks in rapid succession with identical idempotency key and different payloads (reproduces Bug 2)
	respT2, errT2 := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         "t2",
		Payload:        "payload-alpha",
		IdempotencyKey: "key-shared-1",
	})
	if errT2 != nil {
		t.Fatalf("first submission t2 failed: %v", errT2)
	}
	if !respT2.Ok || respT2.TaskId != "t2" {
		t.Fatalf("unexpected respT2: ok=%v taskId=%s", respT2.Ok, respT2.TaskId)
	}

	// Immediate second submission with conflicting payload
	_, errT3 := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         "t3",
		Payload:        "payload-beta", // different payload
		IdempotencyKey: "key-shared-1", // same key
	})
	if errT3 == nil {
		t.Fatalf("expected t3 to be rejected with conflict, but it succeeded")
	}
	st, ok := status.FromError(errT3)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("expected code AlreadyExists for conflicting idempotency key, got: %v", errT3)
	}

	// 3. Immediate third submission with identical payload (idempotent duplicate retry)
	respT4, errT4 := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		TaskId:         "t4",
		Payload:        "payload-alpha", // identical payload
		IdempotencyKey: "key-shared-1",  // same key
	})
	if errT4 != nil {
		t.Fatalf("retry t4 should succeed idempotently, got error: %v", errT4)
	}
	if !respT4.Ok || respT4.TaskId != "t2" {
		t.Fatalf("expected retry to return original TaskId t2, got: %s", respT4.TaskId)
	}

	// 4. Verify TaskByIdempotencyKey helper
	tLookup, found := sched.TaskByIdempotencyKey("key-shared-1")
	if !found || tLookup == nil {
		t.Fatalf("expected to find task by idempotency key 'key-shared-1'")
	}
	if tLookup.ID != "t2" {
		t.Fatalf("expected lookup to return task 't2', got %q", tLookup.ID)
	}
}

func TestConcurrentIdempotentSubmissions(t *testing.T) {
	node := raft.NewNode("s-concurrent", nil, nil)
	go node.Run()
	defer node.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		t.Fatalf("expected node to become leader, got %v", node.State())
	}

	fd := &fakeDispatch{}
	sched := NewScheduler(fd.fn)
	defer sched.Stop()

	sched.SetRaftProposer(node)
	node.OnApply(func(entry raft.LogEntry) {
		sched.ApplyEntry(entry)
	})

	server := &GRPCSchedulerServer{
		S:        sched,
		IsLeader: func() bool { return node.State() == raft.Leader },
	}

	const nGoroutines = 8
	errCh := make(chan error, nGoroutines)
	respCh := make(chan *pb.SubmitTaskResponse, nGoroutines)

	for i := 0; i < nGoroutines; i++ {
		go func(idx int) {
			resp, err := server.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
				TaskId:         "task-concurrent",
				Payload:        "echo idempotent-work",
				IdempotencyKey: "key-concurrent-100",
			})
			if err != nil {
				errCh <- err
			} else {
				respCh <- resp
			}
		}(i)
	}

	for i := 0; i < nGoroutines; i++ {
		select {
		case err := <-errCh:
			t.Fatalf("concurrent idempotent submission failed: %v", err)
		case resp := <-respCh:
			if !resp.Ok {
				t.Fatalf("concurrent submission returned ok=false")
			}
			if resp.TaskId != "task-concurrent" {
				t.Fatalf("unexpected task ID: got %s, want task-concurrent", resp.TaskId)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for concurrent submissions")
		}
	}
}
