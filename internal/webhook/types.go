package webhook

import "time"

// EventType denotes the category of notification event triggered in DistribuQ.
type EventType string

const (
	EventTaskCompleted EventType = "task.completed"
	EventTaskFailed    EventType = "task.failed"
	EventTaskDLQ       EventType = "task.dlq"
	EventWorkerOffline EventType = "worker.offline"
	EventAutoscale     EventType = "autoscale.trigger"
)

// Event represents an actionable cluster event broadcast to subscribers.
type Event struct {
	ID        string         `json:"id"`
	Type      EventType      `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// Subscription defines an external webhook endpoint and its subscribed event types.
type Subscription struct {
	ID        string      `json:"id"`
	URL       string      `json:"url"`
	Secret    string      `json:"secret,omitempty"`
	Events    []EventType `json:"events"`
	CreatedAt time.Time   `json:"created_at"`
}
