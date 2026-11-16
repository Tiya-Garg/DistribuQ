package scheduler

import (
	"encoding/json"
	"net/http"

	"github.com/Tiya-Garg/DistribuQ/internal/webhook"
)

// HandleWebhooksHTTP handles listing, creating, and removing webhook subscriptions via REST.
func (s *Scheduler) HandleWebhooksHTTP(w http.ResponseWriter, r *http.Request) {
	d := s.WebhookDispatcher()
	if d == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":       false,
			"subscriptions": []any{},
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		subs := d.Store().All()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":       true,
			"subscriptions": subs,
		})

	case http.MethodPost:
		var req struct {
			ID     string              `json:"id"`
			URL    string              `json:"url"`
			Secret string              `json:"secret"`
			Events []webhook.EventType `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		sub, err := d.Store().Subscribe(webhook.Subscription{
			ID:     req.ID,
			URL:    req.URL,
			Secret: req.Secret,
			Events: req.Events,
		})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":           true,
			"subscription": sub,
		})

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "missing subscription id parameter"})
			return
		}
		if err := d.Store().Unsubscribe(id); err != nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
