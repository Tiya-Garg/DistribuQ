package webhook

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrSubscriptionNotFound = errors.New("webhook: subscription not found")
	ErrInvalidSubscription  = errors.New("webhook: subscription URL cannot be empty")
)

// Store manages webhook subscriptions in memory.
type Store struct {
	mu            sync.RWMutex
	subscriptions map[string]*Subscription
}

// NewStore creates a new subscription Store.
func NewStore() *Store {
	return &Store{
		subscriptions: make(map[string]*Subscription),
	}
}

// Subscribe registers a new webhook subscription.
func (s *Store) Subscribe(sub Subscription) (*Subscription, error) {
	if sub.URL == "" {
		return nil, ErrInvalidSubscription
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if sub.ID == "" {
		sub.ID = "sub-" + sub.URL
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}
	s.subscriptions[sub.ID] = &sub
	return &sub, nil
}

// Unsubscribe removes a webhook subscription by ID.
func (s *Store) Unsubscribe(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[id]; !exists {
		return ErrSubscriptionNotFound
	}
	delete(s.subscriptions, id)
	return nil
}

// SubscriptionsForEvent returns all active subscriptions interested in the given event type.
func (s *Store) SubscriptionsForEvent(eventType EventType) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matching []*Subscription
	for _, sub := range s.subscriptions {
		if len(sub.Events) == 0 {
			matching = append(matching, sub)
			continue
		}
		for _, e := range sub.Events {
			if e == eventType {
				matching = append(matching, sub)
				break
			}
		}
	}
	return matching
}

// All returns a slice of all registered subscriptions.
func (s *Store) All() []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		list = append(list, sub)
	}
	return list
}
