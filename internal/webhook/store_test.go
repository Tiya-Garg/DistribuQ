package webhook

import (
	"testing"
)

func TestStoreSubscribeAndFilter(t *testing.T) {
	s := NewStore()

	_, err := s.Subscribe(Subscription{URL: ""})
	if err != ErrInvalidSubscription {
		t.Fatalf("expected ErrInvalidSubscription, got %v", err)
	}

	sub1, err := s.Subscribe(Subscription{
		ID:     "sub-1",
		URL:    "https://example.com/all",
		Events: nil, // all events
	})
	if err != nil {
		t.Fatalf("subscribe sub1 failed: %v", err)
	}

	sub2, err := s.Subscribe(Subscription{
		ID:     "sub-2",
		URL:    "https://example.com/failures",
		Events: []EventType{EventTaskFailed, EventTaskDLQ},
	})
	if err != nil {
		t.Fatalf("subscribe sub2 failed: %v", err)
	}

	if len(s.All()) != 2 {
		t.Fatalf("expected 2 subscriptions, got %d", len(s.All()))
	}

	// Filter for EventTaskCompleted: only sub1 matches
	completedSubs := s.SubscriptionsForEvent(EventTaskCompleted)
	if len(completedSubs) != 1 || completedSubs[0].ID != sub1.ID {
		t.Fatalf("expected only sub1 for EventTaskCompleted, got %v", completedSubs)
	}

	// Filter for EventTaskFailed: both match
	failedSubs := s.SubscriptionsForEvent(EventTaskFailed)
	if len(failedSubs) != 2 {
		t.Fatalf("expected 2 matches for EventTaskFailed, got %d", len(failedSubs))
	}
	_ = sub2

	// Unsubscribe
	if err := s.Unsubscribe("sub-1"); err != nil {
		t.Fatalf("failed to unsubscribe sub-1: %v", err)
	}
	if err := s.Unsubscribe("non-existent"); err != ErrSubscriptionNotFound {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}
