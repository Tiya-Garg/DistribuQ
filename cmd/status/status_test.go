package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStatusAutoscalerJSONFormat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled":        true,
			"active_workers": 2,
			"queue_backlog":  10,
			"history": []map[string]any{
				{
					"timestamp":       time.Now(),
					"direction":       "UP",
					"current_workers": 1,
					"desired_workers": 2,
					"queue_backlog":   10,
					"reason":          "test reason",
				},
			},
		})
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("failed to query mock server: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Enabled       bool `json:"enabled"`
		ActiveWorkers int  `json:"active_workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !payload.Enabled || payload.ActiveWorkers != 2 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestStatusWebhooksJSONFormat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": true,
			"subscriptions": []map[string]any{
				{
					"id":     "sub-1",
					"url":    "https://hook.site/abc",
					"events": []string{"task.completed"},
				},
			},
		})
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("failed to query mock server: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Enabled       bool `json:"enabled"`
		Subscriptions []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"subscriptions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !payload.Enabled || len(payload.Subscriptions) != 1 {
		t.Fatalf("unexpected webhooks payload: %+v", payload)
	}
}

func TestStatusTracesJSONFormat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"enabled": true,
			"count":   1,
			"spans": []map[string]any{
				{
					"name":        "submit-span",
					"kind":        "SERVER",
					"trace_id":    "4bf92f3577b34da6a3ce929d0e0e4736",
					"span_id":     "00f067aa0ba902b7",
					"duration_ms": 1.25,
					"status":      map[string]any{"code": "OK"},
				},
			},
		})
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("failed to query mock server: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Enabled bool `json:"enabled"`
		Count   int  `json:"count"`
		Spans   []struct {
			Name string `json:"name"`
		} `json:"spans"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !payload.Enabled || payload.Count != 1 || len(payload.Spans) != 1 {
		t.Fatalf("unexpected traces payload: %+v", payload)
	}
}
