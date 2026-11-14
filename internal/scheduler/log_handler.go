package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// LogHTTPHandler serves HTTP GET /api/tasks/{id}/logs requests.
func (s *Scheduler) HandleGetTaskLogsHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Path expected format: /api/tasks/<task-id>/logs
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "api" || parts[1] != "tasks" || parts[3] != "logs" {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	taskID := parts[2]
	logs, err := s.GetTaskLogs(taskID)
	if err != nil {
		if err == ErrLogNotFound {
			http.Error(w, "Logs not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"task_id": taskID,
		"logs":    logs,
	})
}
