package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type logChunkView struct {
	Timestamp time.Time `json:"timestamp"`
	WorkerID  string    `json:"worker_id"`
	Content   string    `json:"content"`
	IsStderr  bool      `json:"is_stderr"`
}

type taskLogsResponse struct {
	TaskID string         `json:"task_id"`
	Logs   []logChunkView `json:"logs"`
}

// FetchTaskLogs queries the scheduler cluster HTTP/REST endpoint to retrieve task logs.
func FetchTaskLogs(addr, taskID string, timeout time.Duration) ([]logChunkView, error) {
	// If address has port :8000, map to default HTTP dashboard port :8080 if not specified
	host := addr
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		// Map standard gRPC port 8000 to HTTP port 8080 for dashboard API
		if parts[1] == "8000" {
			host = parts[0] + ":8080"
		}
	}

	url := fmt.Sprintf("http://%s/api/tasks/%s/logs", host, taskID)
	client := &http.Client{Timeout: timeout}

	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("no logs found for task %s", taskID)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(b))
	}

	var data taskLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode logs JSON: %w", err)
	}

	return data.Logs, nil
}

