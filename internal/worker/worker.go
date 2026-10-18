// Package worker implements the worker-side RPC service: it accepts
// tasks pushed by the scheduler, executes them, and reports
// completion or failure back so the scheduler can track task state
// and, on failure, retry or reassign.
package worker

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type ExecuteArgs struct {
	TaskID  string
	Payload string
}

type ExecuteReply struct {
	Accepted bool
}

// TaskExecutor runs a task's payload. Swappable so tests can inject a
// no-op executor instead of doing real work; production code uses
// NewShellExecutor (see executor.go).
type TaskExecutor func(taskID, payload string) error

type Worker struct {
	ID            string
	SchedulerAddr string
	Execute       TaskExecutor

	// SchedulerAddrs optionally lists every scheduler replica. When set,
	// the worker registers and heartbeats with all of them, so whichever
	// replica wins an election already knows this worker, and it reports
	// task results to whichever replica owns the task. When empty, the
	// worker talks only to SchedulerAddr.
	SchedulerAddrs []string

	// Addr is the address the scheduler should dial to reach this
	// worker's RPC service. It is sent on every (re-)registration, so
	// it must be set for a worker to be able to rejoin on its own.
	Addr string

	// MaxConcurrent caps how many tasks this worker executes at the
	// same time; 0 (the default) means no cap. Tasks beyond the cap are
	// still accepted and simply wait for a free slot. They are not
	// rejected: a rejected dispatch counts as a failed attempt on the
	// scheduler side, so refusing work under load would burn retries
	// and could permanently fail tasks that did nothing wrong.
	MaxConcurrent int

	slotsOnce sync.Once
	slots     chan struct{}
}

// addrs returns every scheduler address this worker should talk to.
func (w *Worker) addrs() []string {
	if len(w.SchedulerAddrs) > 0 {
		return w.SchedulerAddrs
	}
	return []string{w.SchedulerAddr}
}

// callAny sends one RPC to each scheduler address in turn and stops at
// the first that accepts it. Replicas that aren't the owner of a task
// (followers, or a leader that doesn't know it) answer with an error,
// so the report keeps going until it reaches the one that does. If none
// accepts, the errors from all of them are returned together.
func (w *Worker) callAny(method string, args any) error {
	var errs []error
	for _, addr := range w.addrs() {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var reply struct{ OK bool }
		err = client.Call(method, args, &reply)
		client.Close()
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
	}
	return errors.Join(errs...)
}

// runWithSlot executes a task, first waiting for a free concurrency
// slot if MaxConcurrent is set. The slot is released as soon as
// execution ends, whether it succeeded or failed, and before the
// result is reported over the network, so reporting never holds up
// other tasks.
func (w *Worker) runWithSlot(taskID, payload string) error {
	if w.MaxConcurrent > 0 {
		w.slotsOnce.Do(func() { w.slots = make(chan struct{}, w.MaxConcurrent) })
		w.slots <- struct{}{}
		defer func() { <-w.slots }()
	}
	return w.Execute(taskID, payload)
}

// WorkerService is the RPC-exported wrapper the scheduler calls into.
type WorkerService struct {
	w *Worker
}

// NewWorkerService wraps a Worker so it can be registered on an
// rpc.Server and receive ExecuteTask calls from the scheduler.
func NewWorkerService(w *Worker) *WorkerService {
	return &WorkerService{w: w}
}

func (ws *WorkerService) ExecuteTask(args *ExecuteArgs, reply *ExecuteReply) error {
	reply.Accepted = true
	go func() {
		if err := ws.w.runWithSlot(args.TaskID, args.Payload); err != nil {
			log.Printf("worker %s: task %s failed: %v", ws.w.ID, args.TaskID, err)
			if rerr := ws.w.reportFailure(args.TaskID, err); rerr != nil {
				log.Printf("worker %s: failed to report failure for %s: %v", ws.w.ID, args.TaskID, rerr)
			}
			return
		}
		if err := ws.w.reportCompletion(args.TaskID); err != nil {
			log.Printf("worker %s: failed to report completion for %s: %v", ws.w.ID, args.TaskID, err)
		}
	}()
	return nil
}

// reportCompletion calls back into the scheduler's RPC service to mark
// a task done.
func (w *Worker) reportCompletion(taskID string) error {
	return w.callAny("SchedulerService.CompleteTask", &CompleteArgs{TaskID: taskID, WorkerID: w.ID})
}

type CompleteArgs struct {
	TaskID   string
	WorkerID string
}

// reportFailure calls back into the scheduler's RPC service to report
// that this task's execution failed, so the scheduler can requeue it
// (or mark it permanently failed) instead of leaving it stuck as
// assigned to this worker forever.
func (w *Worker) reportFailure(taskID string, cause error) error {
	return w.callAny("SchedulerService.FailTask", &FailArgs{TaskID: taskID, WorkerID: w.ID, Cause: cause.Error()})
}

type FailArgs struct {
	TaskID   string
	WorkerID string
	Cause    string
}

// RegisterArgs is what the scheduler's RegisterWorker RPC expects.
type RegisterArgs struct {
	WorkerID string
	Addr     string
}

// Register announces this worker (and the address it can be reached
// at) to the scheduler. It's safe to call repeatedly: registering an
// already-known worker just refreshes it.
//
// With several scheduler addresses it registers with all of them and
// succeeds if at least one accepted; it fails only when none did.
func (w *Worker) Register() error {
	var errs []error
	ok := 0
	for _, addr := range w.addrs() {
		if err := w.registerWith(addr); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		ok++
	}
	if ok == 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (w *Worker) registerWith(addr string) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{ OK bool }
	return client.Call("SchedulerService.RegisterWorker", &RegisterArgs{WorkerID: w.ID, Addr: w.Addr}, &reply)
}

// StartHeartbeatLoop periodically pings the scheduler so it knows this
// worker is alive; missing heartbeats trigger task reassignment on the
// scheduler side (see scheduler.MonitorWorkers).
func (w *Worker) StartHeartbeatLoop(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

// sendHeartbeat pings every scheduler once. If a scheduler answers
// with an error, it is telling us it doesn't know this worker (the
// only error Heartbeat can return): we were reaped after a missed
// heartbeat, or that scheduler restarted or just took over as leader.
// In that case the worker registers with that scheduler again instead
// of heartbeating forever as a worker nobody will send work to.
// Network errors (a scheduler is unreachable) are just logged; the
// next tick retries, and the other schedulers are unaffected.
func (w *Worker) sendHeartbeat() {
	for _, addr := range w.addrs() {
		w.heartbeatOne(addr)
	}
}

func (w *Worker) heartbeatOne(addr string) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("worker %s: heartbeat dial to %s failed: %v", w.ID, addr, err)
		return
	}
	defer client.Close()

	var reply struct{ OK bool }
	err = client.Call("SchedulerService.Heartbeat", &HeartbeatArgs{WorkerID: w.ID}, &reply)
	if err == nil {
		return
	}

	var rejected rpc.ServerError
	if !errors.As(err, &rejected) {
		log.Printf("worker %s: heartbeat to %s failed: %v", w.ID, addr, err)
		return
	}

	if w.Addr == "" {
		log.Printf("worker %s: %s rejected heartbeat (%v) but Addr is unset, cannot re-register", w.ID, addr, err)
		return
	}
	log.Printf("worker %s: %s rejected heartbeat (%v), registering again", w.ID, addr, err)
	if rerr := w.registerWith(addr); rerr != nil {
		log.Printf("worker %s: re-registration with %s failed: %v", w.ID, addr, rerr)
	}
}

type HeartbeatArgs struct {
	WorkerID string
}
