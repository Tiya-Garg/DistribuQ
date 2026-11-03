package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
)

// startScheduler runs a real scheduler behind a real gRPC server on
// a random local port and returns it with its address. dispatched
// receives the address of every worker a task is dispatched to.
func startScheduler(t *testing.T, dispatched *[]string) (*scheduler.Scheduler, string) {
	t.Helper()

	s := scheduler.NewScheduler(func(workerID, addr string, task *scheduler.Task) error {
		*dispatched = append(*dispatched, addr)
		return nil
	})
	srv := grpc.NewServer()
	pb.RegisterSchedulerServiceServer(srv, &scheduler.GRPCSchedulerServer{S: s})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		ln.Close()
	})

	go func() {
		_ = srv.Serve(ln)
	}()
	return s, ln.Addr().String()
}

func workerCount(s *scheduler.Scheduler) int {
	_, n := s.Snapshot()
	return n
}

// TestHeartbeatRegistersUnknownWorker is the "reaped or new leader"
// case: the scheduler has never heard of this worker, rejects its
// heartbeat, and the worker responds by registering with its address.
func TestHeartbeatRegistersUnknownWorker(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}

	if n := workerCount(s); n != 0 {
		t.Fatalf("setup: expected 0 workers, got %d", n)
	}
	w.sendHeartbeat()
	if n := workerCount(s); n != 1 {
		t.Fatalf("expected worker to re-register after rejected heartbeat, got %d workers", n)
	}

	// It registered with the right address: a task is dispatched there.
	if _, err := s.SubmitTask("t1", "payload"); err != nil {
		t.Fatal(err)
	}
	if len(dispatched) != 1 || dispatched[0] != "localhost:9001" {
		t.Fatalf("expected dispatch to localhost:9001, got %v", dispatched)
	}
}

// TestHeartbeatWithoutAddrDoesNotRegister verifies that a worker with
// no Addr configured can't register itself -- the scheduler has no
// address to reach it at, so doing so would be useless.
func TestHeartbeatWithoutAddrDoesNotRegister(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr} // Addr unset
	w.sendHeartbeat()

	if n := workerCount(s); n != 0 {
		t.Errorf("expected no registration without an Addr, got %d workers", n)
	}
}

// TestHeartbeatUnreachableSchedulerDoesNotPanic checks that network
// failures are survivable: nothing registers and nothing blows up.
func TestHeartbeatUnreachableSchedulerDoesNotPanic(t *testing.T) {
	// Grab a free port, then close it so nothing is listening there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}
	w.sendHeartbeat() // must just log and return
}

// TestRegister checks the explicit registration call, including the
// error when the scheduler is unreachable.
func TestRegister(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if n := workerCount(s); n != 1 {
		t.Errorf("expected 1 registered worker, got %d", n)
	}

	// Registering twice is a refresh, not a duplicate.
	if err := w.Register(); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if n := workerCount(s); n != 1 {
		t.Errorf("expected still 1 registered worker, got %d", n)
	}
}

// waitUntil polls condition every millisecond until it returns true or
// 2 seconds pass.
func waitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// allCompleted reports whether the scheduler knows exactly n tasks and
// all of them have completed.
func allCompleted(s *scheduler.Scheduler, n int) bool {
	tasks, _ := s.Snapshot()
	if len(tasks) != n {
		return false
	}
	for _, st := range tasks {
		if st != scheduler.TaskCompleted {
			return false
		}
	}
	return true
}

// submitAndRegister registers w with a real scheduler and submits n
// tasks (which the scheduler assigns to w), returning their IDs. The
// scheduler's own dispatch is a no-op here: tests hand the tasks to the
// worker themselves so they control exactly when and how.
func submitAndRegister(t *testing.T, s *scheduler.Scheduler, w *Worker, n int) []string {
	t.Helper()
	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("task-%d", i)
		if _, err := s.SubmitTask(ids[i], "payload"); err != nil {
			t.Fatalf("SubmitTask: %v", err)
		}
	}
	return ids
}

// TestMaxConcurrentLimitsParallelism checks that with MaxConcurrent=2
// and six tasks, never more than two run at once, that two really do
// run in parallel, and that every task still completes.
func TestMaxConcurrentLimitsParallelism(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	var current, peak int32
	exec := func(taskID, payload string) error {
		n := atomic.AddInt32(&current, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil
	}
	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001", MaxConcurrent: 2, Execute: exec}
	ids := submitAndRegister(t, s, w, 6)

	svc := NewGRPCWorkerServer(w)
	for _, id := range ids {
		resp, err := svc.ExecuteTask(context.Background(), &pb.ExecuteTaskRequest{TaskId: id, Payload: "payload"})
		if err != nil || !resp.Accepted {
			t.Fatalf("ExecuteTask(%s): accepted=%v err=%v", id, resp.GetAccepted(), err)
		}
	}

	waitUntil(t, "all 6 tasks to complete", func() bool { return allCompleted(s, 6) })
	if got := atomic.LoadInt32(&peak); got != 2 {
		t.Errorf("expected peak concurrency of exactly 2, got %d", got)
	}
}

// TestMaxConcurrentAcceptsExcessTasksWithoutBlocking checks the design
// choice that a full worker still *accepts* more work (ExecuteTask
// returns at once with Accepted=true) and just makes it wait, rather
// than refusing it and costing the task a retry attempt.
func TestMaxConcurrentAcceptsExcessTasksWithoutBlocking(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	var started int32
	release := make(chan struct{})
	exec := func(taskID, payload string) error {
		atomic.AddInt32(&started, 1)
		<-release
		return nil
	}
	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001", MaxConcurrent: 1, Execute: exec}
	ids := submitAndRegister(t, s, w, 4)

	svc := NewGRPCWorkerServer(w)
	returned := make(chan error, len(ids))
	go func() {
		for _, id := range ids {
			resp, err := svc.ExecuteTask(context.Background(), &pb.ExecuteTaskRequest{TaskId: id, Payload: "payload"})
			if err == nil && !resp.Accepted {
				err = errors.New("task was not accepted")
			}
			returned <- err
		}
	}()
	for range ids {
		select {
		case err := <-returned:
			if err != nil {
				t.Fatalf("ExecuteTask: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ExecuteTask blocked while the worker was full; it should accept and queue")
		}
	}

	// Exactly one task is running; the other three are waiting.
	waitUntil(t, "the first task to start", func() bool { return atomic.LoadInt32(&started) >= 1 })
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&started); got != 1 {
		t.Errorf("expected 1 running task with the other 3 waiting, got %d started", got)
	}

	close(release)
	waitUntil(t, "all 4 tasks to complete", func() bool { return allCompleted(s, 4) })
}

// TestNoLimitRunsAllTasksConcurrently checks the default (0 = no cap):
// all five tasks must be running at the same time. Each blocks until
// all five have started, so a hidden cap would make them time out.
func TestNoLimitRunsAllTasksConcurrently(t *testing.T) {
	const n = 5
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	var started, timedOut int32
	allStarted := make(chan struct{})
	exec := func(taskID, payload string) error {
		if atomic.AddInt32(&started, 1) == n {
			close(allStarted)
		}
		select {
		case <-allStarted:
			return nil
		case <-time.After(2 * time.Second):
			atomic.AddInt32(&timedOut, 1)
			return nil
		}
	}
	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001", Execute: exec} // MaxConcurrent unset
	ids := submitAndRegister(t, s, w, n)

	svc := NewGRPCWorkerServer(w)
	for _, id := range ids {
		resp, err := svc.ExecuteTask(context.Background(), &pb.ExecuteTaskRequest{TaskId: id, Payload: "payload"})
		if err != nil || !resp.Accepted {
			t.Fatalf("ExecuteTask: %v", err)
		}
	}

	waitUntil(t, "all tasks to complete", func() bool { return allCompleted(s, n) })
	if got := atomic.LoadInt32(&timedOut); got != 0 {
		t.Errorf("%d task(s) never saw all %d tasks running together; the default must be uncapped", got, n)
	}
}

// TestRunWithSlotReleasesSlotAfterError checks that a failing task
// gives its slot back: with a single slot, a second task must not hang
// behind a first one that errored.
func TestRunWithSlotReleasesSlotAfterError(t *testing.T) {
	calls := 0
	w := &Worker{
		ID:            "w1",
		MaxConcurrent: 1,
		Execute: func(taskID, payload string) error {
			calls++
			if calls == 1 {
				return errors.New("boom")
			}
			return nil
		},
	}

	if err := w.runWithSlot("a", ""); err == nil {
		t.Fatal("expected the first task's error to be returned")
	}

	done := make(chan error, 1)
	go func() { done <- w.runWithSlot("b", "") }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("second task: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second task hung: the failed task did not release its slot")
	}
}
