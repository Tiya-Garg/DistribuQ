package worker

import (
	"net"
	"testing"

	"github.com/TiyaGarg06/DistribuQ/internal/scheduler"
)

// deadAddr returns a local address nothing is listening on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func taskStatus(s *scheduler.Scheduler, id string) scheduler.TaskStatus {
	tasks, _ := s.Snapshot()
	return tasks[id]
}

// With several schedulers configured, a worker is known to all of them,
// so whichever replica wins an election can dispatch to it immediately.
func TestRegisterWithAllSchedulers(t *testing.T) {
	var d1, d2 []string
	s1, a1 := startScheduler(t, &d1)
	s2, a2 := startScheduler(t, &d2)

	w := &Worker{ID: "w1", SchedulerAddrs: []string{a1, a2}, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if workerCount(s1) != 1 || workerCount(s2) != 1 {
		t.Fatalf("worker should be on both schedulers, got %d and %d", workerCount(s1), workerCount(s2))
	}
}

// One replica being down must not stop the worker from joining the rest.
func TestRegisterSucceedsWhenOneSchedulerIsDown(t *testing.T) {
	var d []string
	s, a := startScheduler(t, &d)

	w := &Worker{ID: "w1", SchedulerAddrs: []string{deadAddr(t), a}, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatalf("Register should succeed when one scheduler is up: %v", err)
	}
	if workerCount(s) != 1 {
		t.Fatalf("worker should be registered on the live scheduler, got %d", workerCount(s))
	}
}

func TestRegisterFailsWhenAllSchedulersAreDown(t *testing.T) {
	w := &Worker{ID: "w1", SchedulerAddrs: []string{deadAddr(t), deadAddr(t)}, Addr: "localhost:9001"}
	if err := w.Register(); err == nil {
		t.Fatal("expected an error when no scheduler is reachable")
	}
}

// A heartbeat re-registers the worker with every scheduler that has
// forgotten it, which is what happens to a freshly elected leader.
func TestHeartbeatReregistersWithEverySchedulerThatForgot(t *testing.T) {
	var d1, d2 []string
	s1, a1 := startScheduler(t, &d1)
	s2, a2 := startScheduler(t, &d2)

	w := &Worker{ID: "w1", SchedulerAddrs: []string{a1, a2}, Addr: "localhost:9001"}
	w.sendHeartbeat()
	if workerCount(s1) != 1 || workerCount(s2) != 1 {
		t.Fatalf("heartbeat should register with both, got %d and %d", workerCount(s1), workerCount(s2))
	}
}

// The task's owner is the scheduler that dispatched it. The worker doesn't
// know which replica that is, so it tries each until one accepts.
func TestCompletionReachesTheOwningScheduler(t *testing.T) {
	var d1, d2 []string
	s1, a1 := startScheduler(t, &d1) // doesn't know the task
	s2, a2 := startScheduler(t, &d2) // the "leader": owns it

	w := &Worker{ID: "w1", SchedulerAddrs: []string{a1, a2}, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.SubmitTask("t1", "payload"); err != nil {
		t.Fatal(err)
	}

	if err := w.reportCompletion("t1"); err != nil {
		t.Fatalf("reportCompletion: %v", err)
	}
	if got := taskStatus(s2, "t1"); got != scheduler.TaskCompleted {
		t.Fatalf("owning scheduler has task %q, want completed", got)
	}
	if got := taskStatus(s1, "t1"); got != "" {
		t.Fatalf("non-owning scheduler unexpectedly knows the task: %q", got)
	}
}

func TestFailureReachesTheOwningScheduler(t *testing.T) {
	var d1, d2 []string
	_, a1 := startScheduler(t, &d1)
	s2, a2 := startScheduler(t, &d2)

	w := &Worker{ID: "w1", SchedulerAddrs: []string{a1, a2}, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.SubmitTask("t1", "payload"); err != nil {
		t.Fatal(err)
	}

	// The first scheduler rejects this (it doesn't know the task); the
	// second owns it and accepts. A nil error means the report got there.
	if err := w.reportFailure("t1", errTest); err != nil {
		t.Fatalf("reportFailure: %v", err)
	}
}

func TestReportWithNoOwnerReturnsError(t *testing.T) {
	var d []string
	_, a := startScheduler(t, &d)
	w := &Worker{ID: "w1", SchedulerAddrs: []string{deadAddr(t), a}, Addr: "localhost:9001"}
	if err := w.reportCompletion("nobody-knows-this-task"); err == nil {
		t.Fatal("expected an error when no scheduler knows the task")
	}
}

type testError struct{}

func (testError) Error() string { return "boom" }

var errTest = testError{}
