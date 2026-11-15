package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Registry collects system counters, gauges, and Prometheus-formatted metrics.
type Registry struct {
	tasksSubmitted atomic.Int64
	tasksCompleted atomic.Int64
	tasksFailed    atomic.Int64
	tasksCanceled  atomic.Int64
	tasksDLQ       atomic.Int64
	tasksRetried   atomic.Int64
	cronTriggers   atomic.Int64
	taskLogsCount  atomic.Int64
	scaleUpCount   atomic.Int64
	scaleDownCount atomic.Int64
}

// DefaultRegistry is the global metrics registry used by DistribuQ.
var DefaultRegistry = &Registry{}

func (r *Registry) IncSubmitted()     { r.tasksSubmitted.Add(1) }
func (r *Registry) IncCompleted()     { r.tasksCompleted.Add(1) }
func (r *Registry) IncFailed()        { r.tasksFailed.Add(1) }
func (r *Registry) IncCanceled()      { r.tasksCanceled.Add(1) }
func (r *Registry) IncDLQ()           { r.tasksDLQ.Add(1) }
func (r *Registry) IncRetried()       { r.tasksRetried.Add(1) }
func (r *Registry) IncCron()          { r.cronTriggers.Add(1) }
func (r *Registry) IncTaskLogs()      { r.taskLogsCount.Add(1) }
func (r *Registry) TaskLogs() int64   { return r.taskLogsCount.Load() }
func (r *Registry) IncScaleUp()       { r.scaleUpCount.Add(1) }
func (r *Registry) IncScaleDown()     { r.scaleDownCount.Add(1) }
func (r *Registry) ScaleUps() int64   { return r.scaleUpCount.Load() }
func (r *Registry) ScaleDowns() int64 { return r.scaleDownCount.Load() }

// WritePrometheus writes current metrics in standard Prometheus exposition text format.
func (r *Registry) WritePrometheus(w io.Writer, extraGauges map[string]float64) {
	fmt.Fprintf(w, "# HELP distribuq_tasks_submitted_total Total number of tasks submitted.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_submitted_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_submitted_total %d\n\n", r.tasksSubmitted.Load())

	fmt.Fprintf(w, "# HELP distribuq_tasks_completed_total Total number of tasks completed successfully.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_completed_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_completed_total %d\n\n", r.tasksCompleted.Load())

	fmt.Fprintf(w, "# HELP distribuq_tasks_failed_total Total number of task execution failures.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_failed_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_failed_total %d\n\n", r.tasksFailed.Load())

	fmt.Fprintf(w, "# HELP distribuq_tasks_canceled_total Total number of tasks canceled.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_canceled_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_canceled_total %d\n\n", r.tasksCanceled.Load())

	fmt.Fprintf(w, "# HELP distribuq_tasks_dlq_total Total number of tasks sent to Dead Letter Queue.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_dlq_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_dlq_total %d\n\n", r.tasksDLQ.Load())

	fmt.Fprintf(w, "# HELP distribuq_tasks_retried_total Total number of task retries.\n")
	fmt.Fprintf(w, "# TYPE distribuq_tasks_retried_total counter\n")
	fmt.Fprintf(w, "distribuq_tasks_retried_total %d\n\n", r.tasksRetried.Load())

	fmt.Fprintf(w, "# HELP distribuq_cron_triggers_total Total number of recurring cron job triggers.\n")
	fmt.Fprintf(w, "# TYPE distribuq_cron_triggers_total counter\n")
	fmt.Fprintf(w, "distribuq_cron_triggers_total %d\n\n", r.cronTriggers.Load())

	fmt.Fprintf(w, "# HELP distribuq_task_logs_total Total number of task output log chunks recorded.\n")
	fmt.Fprintf(w, "# TYPE distribuq_task_logs_total counter\n")
	fmt.Fprintf(w, "distribuq_task_logs_total %d\n\n", r.taskLogsCount.Load())

	fmt.Fprintf(w, "# HELP distribuq_scale_up_total Total number of worker autoscaler scale up operations.\n")
	fmt.Fprintf(w, "# TYPE distribuq_scale_up_total counter\n")
	fmt.Fprintf(w, "distribuq_scale_up_total %d\n\n", r.scaleUpCount.Load())

	fmt.Fprintf(w, "# HELP distribuq_scale_down_total Total number of worker autoscaler scale down operations.\n")
	fmt.Fprintf(w, "# TYPE distribuq_scale_down_total counter\n")
	fmt.Fprintf(w, "distribuq_scale_down_total %d\n\n", r.scaleDownCount.Load())

	for name, val := range extraGauges {
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		fmt.Fprintf(w, "%s %f\n\n", name, val)
	}
}
