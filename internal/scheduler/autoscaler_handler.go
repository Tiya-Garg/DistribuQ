package scheduler

import (
	"encoding/json"
	"net/http"
)

// HandleGetAutoscalerHTTP returns recent autoscaling decisions and policy status.
func (s *Scheduler) HandleGetAutoscalerHTTP(w http.ResponseWriter, r *http.Request) {
	as := s.Autoscaler()
	if as == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":        false,
			"active_workers": s.ActiveWorkers(),
			"queue_backlog":  s.QueueBacklog(),
			"history":        []any{},
		})
		return
	}

	history := as.History()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":        true,
		"active_workers": s.ActiveWorkers(),
		"queue_backlog":  s.QueueBacklog(),
		"history":        history,
	})
}
