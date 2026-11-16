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

// Deliver sends a signed HTTP POST request to a subscriber with retries.
func (d *Dispatcher) Deliver(ctx context.Context, sub *Subscription, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to encode event body: %w", err)
	}

	sig := SignPayload(sub.Secret, body)

	var lastErr error
	backoff := 50 * time.Millisecond

	for attempt := 1; attempt <= d.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "DistribuQ-Webhook/1.0")
		req.Header.Set("X-DistribuQ-Event", string(event.Type))
		req.Header.Set("X-DistribuQ-Delivery", event.ID)
		if sig != "" {
			req.Header.Set("X-DistribuQ-Signature", "sha256="+sig)
		}

		resp, err := d.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				metrics.DefaultRegistry.IncWebhookSuccess()
				return nil
			}
			lastErr = fmt.Errorf("webhook responded with status: %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		if attempt < d.maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}

	metrics.DefaultRegistry.IncWebhookFailure()
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", d.maxRetries, lastErr)
}

// Start begins processing published events in a background dispatch worker pool.
func (d *Dispatcher) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-d.stopCh:
				return
			case event := <-d.eventQueue:
				subs := d.store.SubscriptionsForEvent(event.Type)
				for _, sub := range subs {
					go func(s *Subscription, e Event) {
						delCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						_ = d.Deliver(delCtx, s, e)
					}(sub, event)
				}
			}
		}
	}()
}

// Stop cleanly terminates the event dispatcher background worker.
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.stopped {
		d.stopped = true
		close(d.stopCh)
	}
}
