package scheduler

import (
	"encoding/json"
	"net/http"
)

// HandleGetTracesHTTP returns recently recorded distributed tracing spans.
func (s *Scheduler) HandleGetTracesHTTP(w http.ResponseWriter, r *http.Request) {
	tracer := s.Tracer()
	w.Header().Set("Content-Type", "application/json")
	if tracer == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": false,
			"spans":   []any{},
		})
		return
	}

	spans := tracer.CompletedSpans()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled": true,
		"count":   len(spans),
		"spans":   spans,
	})
}
