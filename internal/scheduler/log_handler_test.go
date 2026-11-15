package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleGetTaskLogsHTTP(t *testing.T) {
	s := NewScheduler(nil)
	s.AppendTaskLog("task-xyz", "w1", "log output\n", false)

	// Valid request
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/task-xyz/logs", nil)
	w := httptest.NewRecorder()
	s.HandleGetTaskLogsHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var resp struct {
		TaskID string     `json:"task_id"`
		Logs   []LogChunk `json:"logs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response error: %v", err)
	}
	if resp.TaskID != "task-xyz" || len(resp.Logs) != 1 || resp.Logs[0].Content != "log output\n" {
		t.Fatalf("unexpected response body: %+v", resp)
	}

	// Not found request
	req404 := httptest.NewRequest(http.MethodGet, "/api/tasks/unknown/logs", nil)
	w404 := httptest.NewRecorder()
	s.HandleGetTaskLogsHTTP(w404, req404)
	if w404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w404.Code)
	}
}
