package autoscaler

import (
	"errors"
	"time"
)

var (
	// ErrInvalidConfig is returned when autoscaler configuration boundaries are invalid.
	ErrInvalidConfig = errors.New("autoscaler: min workers cannot exceed max workers")
)

// Policy defines autoscaling thresholds and smoothing parameters.
type Policy struct {
	MinWorkers        int           `json:"min_workers"`
	MaxWorkers        int           `json:"max_workers"`
	TargetQueueLength int           `json:"target_queue_length"`
	ScaleUpCooldown   time.Duration `json:"scale_up_cooldown"`
	ScaleDownCooldown time.Duration `json:"scale_down_cooldown"`
	ScaleToZeroIdle   time.Duration `json:"scale_to_zero_idle"`
}

// DefaultPolicy returns safe production defaults for worker autoscaling.
func DefaultPolicy() Policy {
	return Policy{
		MinWorkers:        0, // Supports scale-to-zero by default
		MaxWorkers:        10,
		TargetQueueLength: 5,
		ScaleUpCooldown:   5 * time.Second,
		ScaleDownCooldown: 15 * time.Second,
		ScaleToZeroIdle:   30 * time.Second,
	}
}
