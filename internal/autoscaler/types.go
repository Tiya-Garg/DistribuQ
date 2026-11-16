package autoscaler

import (
	"context"
	"time"
)

// ScaleDirection defines whether an action scales out, scales down, or maintains replicas.
type ScaleDirection string

const (
	ScaleNone ScaleDirection = "NONE"
	ScaleUp   ScaleDirection = "UP"
	ScaleDown ScaleDirection = "DOWN"
)

// Decision captures a scaling evaluation outcome.
type Decision struct {
	Timestamp      time.Time      `json:"timestamp"`
	Direction      ScaleDirection `json:"direction"`
	CurrentWorkers int            `json:"current_workers"`
	DesiredWorkers int            `json:"desired_workers"`
	QueueBacklog   int            `json:"queue_backlog"`
	Reason         string         `json:"reason"`
}

// ClusterMetricsProvider supplies queue backlog and active worker state to the autoscaler.
type ClusterMetricsProvider interface {
	QueueBacklog() int
	ActiveWorkers() int
	IdleDuration() time.Duration
}

// WorkerProvisioner abstractly provisions or drains worker instances.
type WorkerProvisioner interface {
	SpawnWorker(ctx context.Context, id string) error
	DrainWorker(ctx context.Context, id string) error
}

