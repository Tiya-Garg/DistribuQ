package scheduler

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrLogNotFound = errors.New("scheduler: no logs found for task")
)

// LogChunk represents a fragment of execution output recorded for a task.
type LogChunk struct {
	Timestamp time.Time `json:"timestamp"`
	WorkerID  string    `json:"worker_id"`
	Content   string    `json:"content"`
	IsStderr  bool      `json:"is_stderr"`
}

// LogStore stores and retrieves task execution logs.
type LogStore struct {
	mu       sync.RWMutex
	maxLines int
	logs     map[string][]LogChunk
}

// NewLogStore creates an initialized LogStore with the specified capacity per task.
func NewLogStore(maxLines int) *LogStore {
	if maxLines <= 0 {
		maxLines = 1000
	}
	return &LogStore{
		maxLines: maxLines,
		logs:     make(map[string][]LogChunk),
	}
}

