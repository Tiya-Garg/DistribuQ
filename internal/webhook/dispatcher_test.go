package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcherDeliverSuccess(t *testing.T) {
	var receivedEvent string
	var receivedSig string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedEvent = r.Header.Get("X-DistribuQ-Event")
		receivedSig = r.Header.Get("X-DistribuQ-Signature")
		body, _ := io.ReadAll(r.Body)
		if !VerifySignature("test-secret", body, strings.TrimPrefix(receivedSig, "sha256=")) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := NewStore()
	sub, _ := store.Subscribe(Subscription{
		ID:     "sub-test",
		URL:    srv.URL,
		Secret: "test-secret",
	})

	d := NewDispatcher(store, nil)
	evt := Event{
		ID:        "evt-001",
		Type:      EventTaskCompleted,
		Timestamp: time.Now(),
		Payload:   map[string]any{"task_id": "t1"},
	}

	err := d.Deliver(context.Background(), sub, evt)
	if err != nil {
		t.Fatalf("expected delivery to succeed, got %v", err)
	}

	if receivedEvent != string(EventTaskCompleted) {
		t.Fatalf("unexpected event header: %s", receivedEvent)
	}
	if receivedSig == "" {
		t.Fatalf("expected signature header")
	}
}

func TestDispatcherDeliverRetryOnFailure(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := NewStore()
	sub, _ := store.Subscribe(Subscription{
		ID:  "sub-fail",
		URL: srv.URL,
	})

	d := NewDispatcher(store, nil)
	evt := Event{ID: "evt-002", Type: EventTaskFailed}

	err := d.Deliver(context.Background(), sub, evt)
	if err == nil {
		t.Fatalf("expected delivery error after max retries")
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 delivery attempts, got %d", attempts.Load())
	}
}
