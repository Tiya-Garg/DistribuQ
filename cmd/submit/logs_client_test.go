package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchTaskLogsSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tasks/task-test-logs/logs" {
			t.Fatalf("unexpected URL path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"task_id": "task-test-logs",
			"logs": []logChunkView{
				{Timestamp: time.Now(), WorkerID: "w1", Content: "hello output", IsStderr: false},
			},
		})
	}))
	defer ts.Close()

	addr := ts.Listener.Addr().String()
	logs, err := FetchTaskLogs(addr, "task-test-logs", 2*time.Second)
	if err != nil {
		t.Fatalf("FetchTaskLogs error: %v", err)
	}
	if len(logs) != 1 || logs[0].Content != "hello output" {
		t.Fatalf("unexpected logs returned: %+v", logs)
	}
}

