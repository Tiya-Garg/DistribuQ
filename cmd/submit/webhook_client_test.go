package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisterWebhookSubscription(t *testing.T) {
	var called atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["url"] != "https://target.com/hook" {
			t.Errorf("unexpected target URL: %v", body["url"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	registerWebhookSubscription(addr, "https://target.com/hook", "sec", 2*time.Second)

	if !called.Load() {
		t.Fatalf("expected webhook registration call to execute")
	}
}
