package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/metrics"
)

// HTTPClient represents the client contract used to deliver webhooks.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Dispatcher coordinates event emission and delivery to registered webhook subscribers.
type Dispatcher struct {
	mu         sync.RWMutex
	store      *Store
	client     HTTPClient
	eventQueue chan Event
	maxRetries int
	stopCh     chan struct{}
	stopped    bool
}

// NewDispatcher creates a new webhook Dispatcher.
func NewDispatcher(store *Store, client HTTPClient) *Dispatcher {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Dispatcher{
		store:      store,
		client:     client,
		eventQueue: make(chan Event, 1000),
		maxRetries: 3,
		stopCh:     make(chan struct{}),
	}
}

// Publish enqueues a cluster event for non-blocking asynchronous delivery.
func (d *Dispatcher) Publish(event Event) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.stopped {
		return
	}
	select {
	case d.eventQueue <- event:
	default:
		// Queue full, drop to avoid blocking scheduler
	}
}
