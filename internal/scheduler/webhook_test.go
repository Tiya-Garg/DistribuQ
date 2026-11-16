package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/webhook"
)

type mockWebhookClient struct {
	mu     sync.Mutex
	events []webhook.Event
}

func (m *mockWebhookClient) record(e webhook.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

func TestSchedulerWebhookEvents(t *testing.T) {
	store := webhook.NewStore()
	_, _ = store.Subscribe(webhook.Subscription{
		ID:     "sub-test",
		URL:    "https://webhook.site/test",
		Events: nil,
	})

	dispatcher := webhook.NewDispatcher(store, nil)
	s := NewScheduler(nil)
	s.SetWebhookDispatcher(dispatcher)

	// Verify dispatcher getter
	if s.WebhookDispatcher() != dispatcher {
		t.Fatalf("expected attached webhook dispatcher")
	}

	// 1. Submit and complete task
	s.RegisterWorker("w1", "127.0.0.1:9001")
	task, err := s.SubmitTask("t1", "echo hi")
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}

	err = s.CompleteTaskBy(task.ID, "w1")
	if err != nil {
		t.Fatalf("complete task failed: %v", err)
	}

	// 2. Fail task until DLQ
	t2, _ := s.SubmitTask("t2", "fail-cmd")
	t2.Attempts = 2
	t2.MaxRetries = 3
	t2.WorkerID = "w1"
	_ = s.FailTaskBy(t2.ID, "w1")

	// Allow goroutines a brief moment
	time.Sleep(10 * time.Millisecond)
}
