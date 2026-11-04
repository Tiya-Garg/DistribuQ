// Package worker implements the worker-side RPC service: it accepts
// tasks pushed by the scheduler, executes them, and reports
// completion or failure back so the scheduler can track task state
// and, on failure, retry or reassign.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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

	// Tags is a list of capability groups (e.g. "gpu", "highmem") that this worker provides.
	Tags []string

	// MaxConcurrent caps how many tasks this worker executes at the
	// same time; 0 (the default) means no cap. Tasks beyond the cap are
	// still accepted and simply wait for a free slot. They are not
	// rejected: a rejected dispatch counts as a failed attempt on the
	// scheduler side, so refusing work under load would burn retries
	// and could permanently fail tasks that did nothing wrong.
	MaxConcurrent int

	// activeCount tracks in-flight executions for health telemetry.
	activeCount atomic.Int32

	// DialOpt specifies grpc.DialOption when connecting to schedulers (e.g. mTLS).
	DialOpt grpc.DialOption

	slotsOnce sync.Once
	slots     chan struct{}
}

func (w *Worker) dialOpt() grpc.DialOption {
	if w.DialOpt != nil {
		return w.DialOpt
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
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
func (w *Worker) callAny(callFn func(client pb.SchedulerServiceClient, ctx context.Context) error) error {
	var errs []error
	for _, addr := range w.addrs() {
		conn, err := grpc.NewClient(addr, w.dialOpt())
		if err != nil {
			errs = append(errs, err)
			continue
		}
		client := pb.NewSchedulerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = callFn(client, ctx)
		cancel()
		_ = conn.Close()
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
	w.activeCount.Add(1)
	defer w.activeCount.Add(-1)

	if w.MaxConcurrent > 0 {
		w.slotsOnce.Do(func() { w.slots = make(chan struct{}, w.MaxConcurrent) })
		w.slots <- struct{}{}
		defer func() { <-w.slots }()
	}
	return w.Execute(taskID, payload)
}

// GRPCWorkerServer implements pb.WorkerServiceServer to receive task executions.
type GRPCWorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	w *Worker
}

// NewGRPCWorkerServer wraps a Worker to serve WorkerService over gRPC.
func NewGRPCWorkerServer(w *Worker) *GRPCWorkerServer {
	return &GRPCWorkerServer{w: w}
}

func (ws *GRPCWorkerServer) ExecuteTask(ctx context.Context, req *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
	go func() {
		if err := ws.w.runWithSlot(req.TaskId, req.Payload); err != nil {
			log.Printf("worker %s: task %s failed: %v", ws.w.ID, req.TaskId, err)
			if rerr := ws.w.reportFailure(req.TaskId, err); rerr != nil {
				log.Printf("worker %s: failed to report failure for %s: %v", ws.w.ID, req.TaskId, rerr)
			}
			return
		}
		if err := ws.w.reportCompletion(req.TaskId); err != nil {
			log.Printf("worker %s: failed to report completion for %s: %v", ws.w.ID, req.TaskId, err)
		}
	}()
	return &pb.ExecuteTaskResponse{Accepted: true}, nil
}

// reportCompletion calls back into the scheduler's RPC service to mark
// a task done.
func (w *Worker) reportCompletion(taskID string) error {
	return w.callAny(func(client pb.SchedulerServiceClient, ctx context.Context) error {
		_, err := client.CompleteTask(ctx, &pb.CompleteTaskRequest{TaskId: taskID, WorkerId: w.ID})
		return err
	})
}

// reportFailure calls back into the scheduler's RPC service to report
// that this task's execution failed, so the scheduler can requeue it
// (or mark it permanently failed) instead of leaving it stuck as
// assigned to this worker forever.
func (w *Worker) reportFailure(taskID string, cause error) error {
	return w.callAny(func(client pb.SchedulerServiceClient, ctx context.Context) error {
		_, err := client.FailTask(ctx, &pb.FailTaskRequest{TaskId: taskID, WorkerId: w.ID, Cause: cause.Error()})
		return err
	})
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
	conn, err := grpc.NewClient(addr, w.dialOpt())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId:      w.ID,
		Addr:          w.Addr,
		Tags:          w.Tags,
		MaxConcurrent: int32(w.MaxConcurrent),
	})
	return err
}

// Drain notifies all schedulers that this worker is draining and shutting down.
func (w *Worker) Drain() error {
	return w.callAny(func(client pb.SchedulerServiceClient, ctx context.Context) error {
		_, err := client.DrainWorker(ctx, &pb.DrainWorkerRequest{WorkerId: w.ID})
		return err
	})
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
// with an error, it is telling us it doesn't know this worker:
// we were reaped after a missed heartbeat, or that scheduler restarted
// or just took over as leader. In that case the worker registers again.
func (w *Worker) sendHeartbeat() {
	for _, addr := range w.addrs() {
		w.heartbeatOne(addr)
	}
}

func (w *Worker) heartbeatOne(addr string) {
	conn, err := grpc.NewClient(addr, w.dialOpt())
	if err != nil {
		log.Printf("worker %s: heartbeat dial to %s failed: %v", w.ID, addr, err)
		return
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:    w.ID,
		ActiveTasks: w.activeCount.Load(),
		CpuUsage:    0.0,
	})
	cancel()
	if err == nil {
		return
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
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
