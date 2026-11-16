package webhook

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFormatSlack(t *testing.T) {
	evt := Event{
		ID:        "evt-123",
		Type:      EventTaskFailed,
		Timestamp: time.Now(),
		Payload: map[string]any{
			"task_id": "task-xyz",
			"error":   "exit code 1",
		},
	}

	data, err := FormatSlack(evt)
	if err != nil {
		t.Fatalf("failed to format slack payload: %v", err)
	}

	var msg SlackPayload
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal formatted slack JSON: %v", err)
	}

	if len(msg.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(msg.Attachments))
	}
	if msg.Attachments[0].Color != "#dc3545" {
		t.Fatalf("expected red error color for failure event, got %s", msg.Attachments[0].Color)
	}
}
