package autoscaler

import (
	"fmt"
	"math"
	"time"
)

// CalculateDesiredWorkers computes the optimal target worker count according to the policy and metrics.
func CalculateDesiredWorkers(policy Policy, backlog, currentWorkers int, idleDuration time.Duration) Decision {
	now := time.Now()

	// Scale to zero condition
	if backlog == 0 {
		if policy.MinWorkers == 0 && idleDuration >= policy.ScaleToZeroIdle && currentWorkers > 0 {
			return Decision{
				Timestamp:      now,
				Direction:      ScaleDown,
				CurrentWorkers: currentWorkers,
				DesiredWorkers: 0,
				QueueBacklog:   backlog,
				Reason:         fmt.Sprintf("backlog idle for %v (threshold: %v), scaling to zero", idleDuration, policy.ScaleToZeroIdle),
			}
		}
		// If backlog is 0 but idle threshold not reached or MinWorkers > 0
		target := policy.MinWorkers
		dir := ScaleNone
		if currentWorkers > target {
			dir = ScaleDown
		}
		return Decision{
			Timestamp:      now,
			Direction:      dir,
			CurrentWorkers: currentWorkers,
			DesiredWorkers: target,
			QueueBacklog:   backlog,
			Reason:         "backlog empty, maintaining minimum workers",
		}
	}

	// Active backlog: calculate workers needed to satisfy TargetQueueLength
	needed := int(math.Ceil(float64(backlog) / float64(policy.TargetQueueLength)))
	if needed < policy.MinWorkers {
		needed = policy.MinWorkers
	}
	if needed > policy.MaxWorkers {
		needed = policy.MaxWorkers
	}

	dir := ScaleNone
	reason := fmt.Sprintf("backlog of %d tasks with target %d tasks/worker", backlog, policy.TargetQueueLength)
	if needed > currentWorkers {
		dir = ScaleUp
		reason = fmt.Sprintf("scaling up: %s", reason)
	} else if needed < currentWorkers {
		dir = ScaleDown
		reason = fmt.Sprintf("scaling down: %s", reason)
	}

	return Decision{
		Timestamp:      now,
		Direction:      dir,
		CurrentWorkers: currentWorkers,
		DesiredWorkers: needed,
		QueueBacklog:   backlog,
		Reason:         reason,
	}
}

