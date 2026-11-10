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

	for w := 0; w < r.cfg.Concurrency; w++ {
		go func(workerID int) {
			defer wg.Done()
			for taskNum := range taskCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				taskID := fmt.Sprintf("bench-%d-%d", workerID, taskNum)
				r.submitSingle(ctx, taskID, payload)
			}
		}(w)
	}

	wg.Wait()
	r.collector.Finish()
	return nil
}

func (r *Runner) submitSingle(ctx context.Context, taskID, payload string) {
	_, client, err := r.pool.FindLeader(ctx)
	if err != nil {
		r.collector.RecordFailure()
		return
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
	} else {
		r.collector.RecordSuccess(time.Since(start))
	}
}
