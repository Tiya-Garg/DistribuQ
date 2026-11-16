package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleGetAutoscalerHTTPDisabled(t *testing.T) {
	s := NewScheduler(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/autoscaler", nil)
	w := httptest.NewRecorder()

	s.HandleGetAutoscalerHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["enabled"] != false {
		t.Fatalf("expected enabled=false, got %v", resp["enabled"])
	}
}

