// Package scheduler implements task queueing, worker registration, and
// least-loaded task dispatch, plus failure detection that reassigns
// in-flight tasks when a worker stops sending heartbeats.
package scheduler

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TiyaGarg06/DistribuQ/internal/raft"
)

// RaftProposer defines the interface needed to propose log entries to a Raft leader.
type RaftProposer interface {
	Propose(entry raft.LogEntry) (int, int, <-chan bool, error)
}

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskAssigned  TaskStatus = "assigned"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

type Task struct {
	ID         string
	Payload    string
	Status     TaskStatus
	WorkerID   string
	Attempts   int
	MaxRetries int

	// seq is the task's submission order, used to dispatch tasks that
	// were left queued in first-come, first-served order.
	seq uint64

	// tried records the workers this task has already come off of
	// without completing (dispatch error, reported failure, or the
	// worker going silent). Dispatch prefers workers not in this set.
	tried map[string]bool

	// dispatching is true while one goroutine is assigning this task to a
	// worker; it stops a second goroutine from assigning it concurrently.
	dispatching bool
}

type WorkerInfo struct {
	ID            string
	Addr          string
	LastHeartbeat time.Time
	ActiveTasks   map[string]bool
}

var (
	ErrNoWorkersAvailable = errors.New("scheduler: no healthy workers available")
	ErrTaskNotFound       = errors.New("scheduler: task not found")

	// ErrEmptyTaskID is returned by SubmitTask when the task ID is "".
	ErrEmptyTaskID = errors.New("scheduler: task id must not be empty")

	// ErrDuplicateTask is returned by SubmitTask when a task with the
	// same ID was already submitted. Task IDs are unique for the life
	// of the scheduler, including after the task completes or fails;
	// accepting a duplicate would silently overwrite a task that may
	// still be running on a worker.
	ErrDuplicateTask = errors.New("scheduler: a task with this id already exists")

	// ErrUnknownWorker is returned by Heartbeat when the scheduler has
	// no record of the worker: it never registered, or it was reaped
	// after missing heartbeats (or this replica just became leader and
	// has never heard of it). The worker is expected to respond by
	// registering again; a silently ignored heartbeat would leave it
	// alive but never assigned work.
	ErrUnknownWorker = errors.New("scheduler: unknown worker, it must register again")

	// ErrStaleReport is returned when a worker reports the outcome of a
	// task that is no longer assigned to it (for example, the worker was
	// declared dead, the task was reassigned, and then the old worker
	// woke up and reported late). The report is ignored so it can't
	// clobber the state of the task's current assignment.
	ErrStaleReport = errors.New("scheduler: stale report from a worker the task is not assigned to")
)

// WorkerTimeout is how long a worker can go without a heartbeat before
// it's considered dead and its in-flight tasks are reassigned.
// It's a var (not const) so tests can shrink it instead of sleeping
// for the full production timeout.
var WorkerTimeout = 3 * time.Second

type Scheduler struct {
	mu sync.Mutex

	tasks   map[string]*Task
	workers map[string]*WorkerInfo
	nextSeq uint64

	// kick wakes MonitorWorkers to dispatch queued tasks right away
	// (buffered so a registration never blocks on it).
	kick chan struct{}

	// dispatchFn sends a task to a worker; injected so it can be swapped
	// (RPC today, gRPC later) without touching scheduling logic.
	dispatchFn func(workerID, addr string, task *Task) error

	// stopCh is closed by Stop to end MonitorWorkers. stopOnce makes
	// Stop idempotent: closing an already-closed channel panics, and
	// shutdown paths (signal handlers, deferred cleanup, tests) can
	// easily end up calling Stop more than once.
	stopCh   chan struct{}
	stopOnce sync.Once

	// sweeping is true while a background dispatch sweep is running;
	// sweepAgain records that more work may have arrived since it
	// last looked. Together they keep at most one sweep in flight
	// without ever losing a request for another pass (see sweepAsync).
	sweeping   atomic.Bool
	sweepAgain atomic.Bool

	// proposer, if set, replicates task events across the Raft cluster before applying.
	proposer RaftProposer
}

func NewScheduler(dispatchFn func(workerID, addr string, task *Task) error) *Scheduler {
	return &Scheduler{
		tasks:      make(map[string]*Task),
		workers:    make(map[string]*WorkerInfo),
		dispatchFn: dispatchFn,
		kick:       make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
}

// SetRaftProposer attaches a Raft node as proposer for replicating task events.
func (s *Scheduler) SetRaftProposer(p RaftProposer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proposer = p
}

// KickDispatch nudges the worker monitor to sweep queued tasks immediately.
func (s *Scheduler) KickDispatch() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// RegisterWorker adds (or refreshes) a worker in the pool.
func (s *Scheduler) RegisterWorker(id, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w, ok := s.workers[id]; ok {
		w.LastHeartbeat = time.Now()
		w.Addr = addr
		return
	}
	s.workers[id] = &WorkerInfo{
		ID:            id,
		Addr:          addr,
		LastHeartbeat: time.Now(),
		ActiveTasks:   make(map[string]bool),
	}

	// A new worker may be exactly what queued tasks were waiting for.
	// Nudge the monitor without blocking the caller (usually an RPC
	// handler); if a nudge is already pending, that one will do.
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Heartbeat refreshes a worker's liveness timestamp. It returns
// ErrUnknownWorker if the worker isn't registered, so the caller can
// tell it to register again.
func (s *Scheduler) Heartbeat(workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.workers[workerID]
	if !ok {
		return ErrUnknownWorker
	}
	w.LastHeartbeat = time.Now()
	return nil
}

// SubmitTask queues a task and attempts immediate dispatch to the
// least-loaded healthy worker. It rejects an empty ID (ErrEmptyTaskID)
// and an ID that has been used before (ErrDuplicateTask); in both
// cases nothing is queued and existing tasks are left untouched.
//
// When a Raft proposer is attached, the task is committed across a
// majority before SubmitTask returns successfully.
func (s *Scheduler) SubmitTask(id, payload string) (*Task, error) {
	if id == "" {
		return nil, ErrEmptyTaskID
	}

	s.mu.Lock()
	if _, exists := s.tasks[id]; exists {
		s.mu.Unlock()
		return nil, ErrDuplicateTask
	}
	proposer := s.proposer
	if proposer == nil {
		s.nextSeq++
		task := &Task{ID: id, Payload: payload, Status: TaskQueued, MaxRetries: 3, seq: s.nextSeq}
		s.tasks[id] = task
		s.mu.Unlock()

		_ = s.dispatch(task)
		return task, nil
	}
	s.mu.Unlock()

	_, _, commitCh, err := proposer.Propose(raft.LogEntry{
		Type:    raft.CmdSubmit,
		TaskID:  id,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}

	select {
	case ok := <-commitCh:
		if !ok {
			return nil, ErrNotLeader
		}
	case <-time.After(10 * time.Second):
		return nil, errors.New("scheduler: submission timed out waiting for majority commit")
	}

	s.mu.Lock()
	task := s.tasks[id]
	s.mu.Unlock()

	if task != nil {
		_ = s.dispatch(task)
	}
	return task, nil
}

// dispatch picks the least-loaded worker and hands the task to it. If
// dispatchFn fails, the tentative assignment is rolled back and the
// task is requeued (or marked failed past MaxRetries); if requeued,
// dispatch immediately retries against a different worker rather than
// waiting for the next sweep. Retries are naturally bounded: each
// attempt increments task.Attempts, and once Attempts reaches
// MaxRetries the task is marked TaskFailed instead of requeued, which
// stops the recursion.
func (s *Scheduler) dispatch(task *Task) error {
	var firstErr error
	for next := task; next != nil; {
		var err error
		next, err = s.dispatchAttempt(next)
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dispatchAttempt makes one attempt to place task on a worker. If the
// attempt failed and the task was requeued, it returns the task so
// dispatch can try again against a different worker; otherwise nil.
func (s *Scheduler) dispatchAttempt(task *Task) (*Task, error) {
	s.mu.Lock()
	if task.Status != TaskQueued {
		s.mu.Unlock()
		return nil, nil
	}
	// Claim the task for the assignment step. With Raft attached, a task
	// stays TaskQueued until its assignment entry commits and is applied,
	// so the Status check alone cannot stop a second caller (the submit
	// path and the leader's sweep both dispatch new tasks) from picking a
	// worker and handing the same task out twice. The claim is released
	// as soon as the assignment is settled, before the network call to the
	// worker, so a hung worker never blocks reassigning the task.
	if task.dispatching {
		s.mu.Unlock()
		return nil, nil
	}
	worker := s.pickWorkerLocked(task.tried)
	if worker == nil {
		s.mu.Unlock()
		return nil, ErrNoWorkersAvailable
	}
	task.dispatching = true
	workerID := worker.ID
	workerAddr := worker.Addr
	taskID := task.ID
	proposer := s.proposer

	if proposer == nil {
		task.Status = TaskAssigned
		task.WorkerID = workerID
		task.Attempts++
		worker.ActiveTasks[task.ID] = true
		task.dispatching = false
		s.mu.Unlock()
	} else {
		s.mu.Unlock()
		err := s.proposeAssign(taskID, workerID)
		s.mu.Lock()
		task.dispatching = false
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}

	if err := s.dispatchFn(workerID, workerAddr, task); err != nil {
		if proposer != nil {
			_, _, commitCh, perr := proposer.Propose(raft.LogEntry{
				Type:     raft.CmdFail,
				TaskID:   taskID,
				WorkerID: workerID,
				Cause:    err.Error(),
			})
			if perr == nil {
				<-commitCh
			}
			s.mu.Lock()
			t := s.tasks[taskID]
			requeued := t != nil && t.Status == TaskQueued
			s.mu.Unlock()

			if requeued {
				return t, err
			}
			return nil, err
		}

		s.mu.Lock()
		if task.Status != TaskAssigned || task.WorkerID != workerID {
			s.mu.Unlock()
			return nil, err
		}
		if w, ok := s.workers[workerID]; ok {
			delete(w.ActiveTasks, task.ID)
		}
		s.requeueOrFailLocked(task)
		requeued := task.Status == TaskQueued
		s.mu.Unlock()

		if requeued {
			return task, err
		}
		return nil, err
	}
	return nil, nil
}

// proposeAssign replicates an assignment entry and waits for it to commit.
func (s *Scheduler) proposeAssign(taskID, workerID string) error {
	_, _, commitCh, err := s.proposer.Propose(raft.LogEntry{
		Type:     raft.CmdAssign,
		TaskID:   taskID,
		WorkerID: workerID,
	})
	if err != nil {
		return err
	}
	select {
	case ok := <-commitCh:
		if !ok {
			return ErrNotLeader
		}
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("scheduler: timed out waiting for assignment commit")
	}
}

// requeueOrFailLocked decides a task's fate after it comes off a
// worker without completing (dispatch failure, execution failure, or
// worker death): retry it if attempts remain, otherwise mark it
// permanently failed. Callers must hold s.mu and must have already
// cleared the task out of its former worker's ActiveTasks.
//
// The worker the task is coming off of is remembered in task.tried
// first, so the retry goes somewhere else when it can. Without that, a
// worker that keeps failing dispatches looks like the least loaded one
// (its failed tasks never stay on it), so least-loaded selection keeps
// sending it the very tasks it just failed, and they run out of
// attempts while healthy workers sit idle.
func (s *Scheduler) requeueOrFailLocked(task *Task) {
	if task.WorkerID != "" {
		if task.tried == nil {
			task.tried = make(map[string]bool)
		}
		task.tried[task.WorkerID] = true
	}
	if task.Attempts < task.MaxRetries {
		task.Status = TaskQueued
		task.WorkerID = ""
	} else {
		task.Status = TaskFailed
		task.WorkerID = ""
	}
}

// pickLeastLoadedWorkerLocked returns the least-loaded healthy worker,
// or nil if there is none.
func (s *Scheduler) pickLeastLoadedWorkerLocked() *WorkerInfo {
	return s.pickWorkerLocked(nil)
}

// pickWorkerLocked returns the least-loaded healthy worker, preferring
// workers not in avoid. If every healthy worker is in avoid (a one- or
// two-worker cluster, say), it falls back to the least-loaded of those
// rather than leave the task with nowhere to run. Returns nil only if
// there are no healthy workers at all.
func (s *Scheduler) pickWorkerLocked(avoid map[string]bool) *WorkerInfo {
	var best, bestAvoided *WorkerInfo
	now := time.Now()
	for _, w := range s.workers {
		if now.Sub(w.LastHeartbeat) > WorkerTimeout {
			continue // treat as dead, skip
		}
		if avoid[w.ID] {
			if bestAvoided == nil || len(w.ActiveTasks) < len(bestAvoided.ActiveTasks) {
				bestAvoided = w
			}
			continue
		}
		if best == nil || len(w.ActiveTasks) < len(best.ActiveTasks) {
			best = w
		}
	}
	if best != nil {
		return best
	}
	return bestAvoided
}

// CompleteTask marks a task finished and frees the worker's slot. It
// accepts the report regardless of which worker sent it; prefer
// CompleteTaskBy, which rejects stale reports.
func (s *Scheduler) CompleteTask(taskID string) error {
	return s.completeTask(taskID, "", false)
}

// CompleteTaskBy marks a task finished, but only if workerID is the
// worker the task is currently assigned to. A report from any other
// worker (e.g. one that was declared dead and whose task has since
// been reassigned) is ignored and ErrStaleReport is returned. A
// duplicate report from the current assignee for an already-completed
// task is treated as a harmless no-op.
func (s *Scheduler) CompleteTaskBy(taskID, workerID string) error {
	return s.completeTask(taskID, workerID, true)
}

func (s *Scheduler) completeTask(taskID, workerID string, checkWorker bool) error {
	s.mu.Lock()
	task, ok := s.tasks[taskID]
	if !ok {
		s.mu.Unlock()
		return ErrTaskNotFound
	}
	if checkWorker && task.WorkerID != workerID {
		s.mu.Unlock()
		return ErrStaleReport
	}
	proposer := s.proposer
	if proposer == nil {
		task.Status = TaskCompleted
		if w, ok := s.workers[task.WorkerID]; ok {
			delete(w.ActiveTasks, taskID)
		}
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	_, _, commitCh, err := proposer.Propose(raft.LogEntry{
		Type:     raft.CmdComplete,
		TaskID:   taskID,
		WorkerID: workerID,
	})
	if err != nil {
		return err
	}
	select {
	case ok := <-commitCh:
		if !ok {
			return ErrNotLeader
		}
	case <-time.After(10 * time.Second):
		return errors.New("scheduler: complete timed out waiting for commit")
	}
	return nil
}

// FailTask is called when a worker reports that a task's execution
// failed (as opposed to a dispatch-time failure, handled in dispatch,
// or a worker going silent, handled in reapDeadWorkers). It frees the
// worker's slot and, if attempts remain, requeues the task and
// immediately attempts redispatch to a different worker rather than
// leaving the task queued until the next sweep; once MaxRetries is
// exhausted it's marked permanently failed instead.
//
// It accepts the report regardless of which worker sent it; prefer
// FailTaskBy, which rejects stale reports.
func (s *Scheduler) FailTask(taskID string) error {
	return s.failTask(taskID, "", false)
}

// FailTaskBy is FailTask restricted to the worker the task is
// currently assigned to. Reports from any other worker return
// ErrStaleReport and change nothing.
func (s *Scheduler) FailTaskBy(taskID, workerID string) error {
	return s.failTask(taskID, workerID, true)
}

func (s *Scheduler) failTask(taskID, workerID string, checkWorker bool) error {
	s.mu.Lock()
	task, ok := s.tasks[taskID]
	if !ok {
		s.mu.Unlock()
		return ErrTaskNotFound
	}
	if checkWorker && task.WorkerID != workerID {
		s.mu.Unlock()
		return ErrStaleReport
	}
	proposer := s.proposer
	if proposer == nil {
		if w, ok := s.workers[task.WorkerID]; ok {
			delete(w.ActiveTasks, taskID)
		}
		s.requeueOrFailLocked(task)
		requeued := task.Status == TaskQueued
		s.mu.Unlock()

		if requeued {
			s.dispatch(task)
		}
		return nil
	}
	s.mu.Unlock()

	_, _, commitCh, err := proposer.Propose(raft.LogEntry{
		Type:     raft.CmdFail,
		TaskID:   taskID,
		WorkerID: workerID,
		Cause:    "worker reported failure",
	})
	if err != nil {
		return err
	}
	select {
	case ok := <-commitCh:
		if !ok {
			return ErrNotLeader
		}
	case <-time.After(10 * time.Second):
		return errors.New("scheduler: fail timed out waiting for commit")
	}

	s.mu.Lock()
	t := s.tasks[taskID]
	requeued := t != nil && t.Status == TaskQueued
	s.mu.Unlock()

	if requeued {
		s.dispatch(t)
	}
	return nil
}

// MonitorWorkers should run in a goroutine. It periodically checks for
// workers that have gone silent past WorkerTimeout, marks them dead,
// and puts their in-flight tasks back in the queue -- this is the core
// fault-recovery behavior of the system. It also dispatches any tasks
// sitting in the queue (orphans from a dead worker, or tasks submitted
// while no worker was available), both on every tick and immediately
// when a new worker registers.
//
// The monitor itself never makes a network call: dispatching happens
// in a background sweep (see sweepAsync). A dispatch can block for a
// long time on an unresponsive worker, and if it ran inline here, one
// frozen worker would stop the monitor and with it all failure
// detection for every other worker.
func (s *Scheduler) MonitorWorkers(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.reapDeadWorkers()
			s.sweepAsync()
		case <-s.kick:
			s.sweepAsync()
		}
	}
}

// sweepAsync dispatches queued tasks in a background goroutine, with at
// most one sweep running at a time. A request that arrives while a
// sweep is running is not dropped: it sets sweepAgain, and the running
// sweep makes another pass before it finishes. If one dispatch inside
// a sweep is stuck, the sweep waits on it, so queued tasks are delayed
// -- but failure detection, which runs in the monitor, is not.
func (s *Scheduler) sweepAsync() {
	s.sweepAgain.Store(true)
	if !s.sweeping.CompareAndSwap(false, true) {
		return // the running sweep will see sweepAgain and go again
	}
	go func() {
		for s.sweepAgain.CompareAndSwap(true, false) {
			s.dispatchQueued()
		}
		s.sweeping.Store(false)
		// A request may have landed between the last check above and
		// clearing sweeping; if so, start another sweep to handle it.
		if s.sweepAgain.Load() {
			s.sweepAsync()
		}
	}()
}

// dispatchQueued hands every queued task to a worker, oldest
// submission first, one after another. It blocks for as long as the
// dispatches do, so the monitor runs it through sweepAsync rather than
// calling it directly. It is what gets queued tasks moving again: ones
// orphaned by a dead worker, or submitted while no healthy worker
// existed. Tasks that still can't be placed are left queued for the
// next sweep.
func (s *Scheduler) dispatchQueued() {
	s.mu.Lock()
	if s.pickLeastLoadedWorkerLocked() == nil {
		s.mu.Unlock()
		return
	}
	var queued []*Task
	for _, t := range s.tasks {
		if t.Status == TaskQueued {
			queued = append(queued, t)
		}
	}
	s.mu.Unlock()

	sort.Slice(queued, func(i, j int) bool { return queued[i].seq < queued[j].seq })
	for _, t := range queued {
		s.dispatch(t)
	}
}

// Stop ends MonitorWorkers. It is safe to call more than once and from
// multiple goroutines; only the first call has any effect.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// reapDeadWorkers removes workers that have gone silent past
// WorkerTimeout and takes back their in-flight tasks: each is requeued,
// or failed for good if it is out of attempts.
//
// It only changes scheduler state and never calls dispatchFn, so it
// cannot block on the network; the requeued tasks are picked up by the
// dispatch sweep that follows (see MonitorWorkers and dispatchQueued).
//
// Each orphaned task's fate is decided inside the same critical
// section that removes its worker, and its WorkerID is cleared there.
// That closes a race where the "dead" worker could report completion
// between the worker's removal and the task being requeued: the task
// no longer belongs to that worker, so its late report is rejected as
// stale (see CompleteTaskBy) instead of being overwritten, which would
// have resurrected a finished task and run it a second time.
func (s *Scheduler) reapDeadWorkers() {
	now := time.Now()

	s.mu.Lock()
	var deadTasks []struct{ TaskID, WorkerID string }
	for id, w := range s.workers {
		if now.Sub(w.LastHeartbeat) <= WorkerTimeout {
			continue
		}
		for taskID := range w.ActiveTasks {
			t, ok := s.tasks[taskID]
			if !ok || t.WorkerID != id {
				continue // no longer this worker's task
			}
			if t.Status == TaskCompleted || t.Status == TaskFailed {
				continue // already finished; nothing to recover
			}
			deadTasks = append(deadTasks, struct{ TaskID, WorkerID string }{taskID, id})
		}
		delete(s.workers, id)
	}
	proposer := s.proposer
	if proposer == nil {
		for _, dt := range deadTasks {
			if t, ok := s.tasks[dt.TaskID]; ok && t.WorkerID == dt.WorkerID {
				s.requeueOrFailLocked(t)
			}
		}
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	for _, dt := range deadTasks {
		s.mu.Lock()
		t, ok := s.tasks[dt.TaskID]
		valid := ok && t.WorkerID == dt.WorkerID && t.Status == TaskAssigned
		s.mu.Unlock()
		if valid {
			_, _, commitCh, err := proposer.Propose(raft.LogEntry{
				Type:     raft.CmdFail,
				TaskID:   dt.TaskID,
				WorkerID: dt.WorkerID,
				Cause:    "worker heartbeat timeout",
			})
			if err == nil {
				<-commitCh
			}
		}
	}
}

// Snapshot returns a copy of current task states, useful for tests and
// for the dashboard/CLI to display cluster state.
func (s *Scheduler) Snapshot() (tasks map[string]TaskStatus, workerCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks = make(map[string]TaskStatus, len(s.tasks))
	for id, t := range s.tasks {
		tasks[id] = t.Status
	}
	return tasks, len(s.workers)
}

// ApplyEntry applies a committed Raft log entry to the scheduler's state.
func (s *Scheduler) ApplyEntry(entry raft.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch entry.Type {
	case raft.CmdSubmit:
		if _, exists := s.tasks[entry.TaskID]; !exists {
			s.nextSeq++
			s.tasks[entry.TaskID] = &Task{
				ID:         entry.TaskID,
				Payload:    entry.Payload,
				Status:     TaskQueued,
				MaxRetries: 3,
				seq:        s.nextSeq,
			}
		}

	case raft.CmdNoop:
		// No-op command; deliberately ignored.
		// Ensures leader can commit an entry without side effects.

	case raft.CmdAssign:
		task, ok := s.tasks[entry.TaskID]
		if !ok {
			s.nextSeq++
			task = &Task{
				ID:         entry.TaskID,
				Payload:    entry.Payload,
				Status:     TaskQueued,
				MaxRetries: 3,
				seq:        s.nextSeq,
			}
			s.tasks[entry.TaskID] = task
		}
		task.Status = TaskAssigned
		task.WorkerID = entry.WorkerID
		task.Attempts++
		if w, ok := s.workers[entry.WorkerID]; ok {
			w.ActiveTasks[entry.TaskID] = true
		}

	case raft.CmdComplete:
		if task, ok := s.tasks[entry.TaskID]; ok {
			task.Status = TaskCompleted
			if w, ok := s.workers[task.WorkerID]; ok {
				delete(w.ActiveTasks, entry.TaskID)
			}
		}

	case raft.CmdFail:
		if task, ok := s.tasks[entry.TaskID]; ok {
			if w, ok := s.workers[task.WorkerID]; ok {
				delete(w.ActiveTasks, entry.TaskID)
			}
			s.requeueOrFailLocked(task)
		}
	}
}

// RebuildQueueOnLeadership requeues all tasks left in TaskAssigned back to
// TaskQueued and triggers a sweep so that surviving workers can be dispatched work.
func (s *Scheduler) RebuildQueueOnLeadership() {
	s.mu.Lock()
	for _, task := range s.tasks {
		if task.Status == TaskAssigned {
			task.Status = TaskQueued
			task.WorkerID = ""
		}
	}
	s.mu.Unlock()

	s.sweepAsync()
}
