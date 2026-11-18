package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/tracing"
)

func TestHandleGetTracesHTTP(t *testing.T) {
	s := NewScheduler(nil)
	tracer := tracing.NewTracer("test-tracer")
	s.SetTracer(tracer)

	_, span := tracer.Start(context.Background(), "test-span", tracing.SpanKindServer)
	span.End()

	req := httptest.NewRequest(http.MethodGet, "/api/traces", nil)
	w := httptest.NewRecorder()

	s.HandleGetTracesHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Enabled bool                   `json:"enabled"`
		Count   int                    `json:"count"`
		Spans   []tracing.SpanSnapshot `json:"spans"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Enabled || resp.Count != 1 || len(resp.Spans) != 1 {
		t.Fatalf("unexpected traces response: %+v", resp)
	}
}
