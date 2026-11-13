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

// Append adds a log chunk for the task, enforcing max line limits.
func (ls *LogStore) Append(taskID, workerID, content string, isStderr bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	chunk := LogChunk{
		Timestamp: time.Now(),
		WorkerID:  workerID,
		Content:   content,
		IsStderr:  isStderr,
	}

	chunks := ls.logs[taskID]
	if len(chunks) >= ls.maxLines {
		// Evict oldest line to prevent unbounded memory growth
		chunks = append(chunks[1:], chunk)
	} else {
		chunks = append(chunks, chunk)
	}
	ls.logs[taskID] = chunks
}

// Get returns all stored log chunks for a task.
func (ls *LogStore) Get(taskID string) ([]LogChunk, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	chunks, exists := ls.logs[taskID]
	if !exists || len(chunks) == 0 {
		return nil, ErrLogNotFound
	}

	result := make([]LogChunk, len(chunks))
	copy(result, chunks)
	return result, nil
}

