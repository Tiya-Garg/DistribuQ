package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
)

// Runner manages the execution of concurrent task submissions.
type Runner struct {
	cfg       Config
	pool      *SchedulerPool
	collector *MetricsCollector
}

// NewRunner creates a new benchmark runner instance.
func NewRunner(cfg Config, pool *SchedulerPool, collector *MetricsCollector) *Runner {
	return &Runner{
		cfg:       cfg,
		pool:      pool,
		collector: collector,
	}
}

// Run executes the task submission workload using a worker pool of goroutines.
func (r *Runner) Run(ctx context.Context) error {
	taskCh := make(chan int, r.cfg.Tasks)
	for i := 0; i < r.cfg.Tasks; i++ {
		taskCh <- i
	}
	close(taskCh)

	var wg sync.WaitGroup
	wg.Add(r.cfg.Concurrency)

	payload := GeneratePayload(r.cfg.PayloadSize)
	var submittedIDs sync.Map

	// Rate limiter ticker if rate is configured
	var rateTicker *time.Ticker
	if r.cfg.RateLimit > 0 {
		rateTicker = time.NewTicker(time.Second / time.Duration(r.cfg.RateLimit))
		defer rateTicker.Stop()
	}

	for w := 0; w < r.cfg.Concurrency; w++ {
		go func(workerID int) {
			defer wg.Done()
			for taskNum := range taskCh {
				if rateTicker != nil {
					select {
					case <-rateTicker.C:
					case <-ctx.Done():
						return
					}
				}

				select {
				case <-ctx.Done():
					return
				default:
				}

				taskID := fmt.Sprintf("bench-%d-%d", workerID, taskNum)
				if r.submitSingle(ctx, taskID, payload) {
					submittedIDs.Store(taskID, true)
				}
			}
		}(w)
	}

	wg.Wait()
	r.collector.Finish()

	if r.cfg.Verify {
		r.verifyCompletion(ctx, &submittedIDs)
	}

	if r.cfg.MeasureLogs {
		r.measureTaskLogs(ctx, &submittedIDs)
	}

	return nil
}

func (r *Runner) verifyCompletion(ctx context.Context, submittedIDs *sync.Map) {
	deadline := time.Now().Add(r.cfg.Timeout)
	for time.Now().Before(deadline) {
		_, client, err := r.pool.FindLeader(ctx)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		resp, err := client.GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		allDone := true
		submittedIDs.Range(func(key, value any) bool {
			id := key.(string)
			status, exists := resp.Tasks[id]
			if !exists || (status != "completed" && status != "failed" && status != "dlq") {
				allDone = false
				return false
			}
			return true
		})

		if allDone {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (r *Runner) measureTaskLogs(ctx context.Context, submittedIDs *sync.Map) {
	var count int
	submittedIDs.Range(func(key, value any) bool {
		count++
		return count < 50
	})
}

func (r *Runner) submitSingle(ctx context.Context, taskID, payload string) bool {
	_, client, err := r.pool.FindLeader(ctx)
	if err != nil {
		r.collector.RecordFailure()
		return false
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err = client.SubmitTask(callCtx, &pb.SubmitTaskRequest{
		TaskId:         taskID,
		Payload:        payload,
		Priority:       1,
		IdempotencyKey: "key-" + taskID,
	})

	if err != nil {
		r.collector.RecordFailure()
		return false
	} else {
		r.collector.RecordSuccess(time.Since(start))
		return true
	}
}
