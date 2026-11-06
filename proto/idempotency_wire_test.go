package distribuq

import (
	"bytes"
	"testing"

	gp "google.golang.org/protobuf/proto"
)

// TestIdempotencyKeySurvivesMarshal guards against a stale distribuq.pb.go:
// if the generated descriptor is out of date with distribuq.proto,
// idempotency_key is silently dropped on the wire.
func TestIdempotencyKeySurvivesMarshal(t *testing.T) {
	msgs := map[string]gp.Message{
		"SubmitTaskRequest": &SubmitTaskRequest{TaskId: "a", IdempotencyKey: "key-xyz"},
		"LogEntry":          &LogEntry{IdempotencyKey: "key-xyz"},
		"TaskInfo":          &TaskInfo{IdempotencyKey: "key-xyz"},
	}
	for name, m := range msgs {
		b, err := gp.Marshal(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		if !bytes.Contains(b, []byte("key-xyz")) {
			t.Errorf("%s: idempotency_key not serialized; regenerate proto/distribuq.pb.go", name)
		}

		// Verify round-trip unmarshaling retains the exact idempotency key
		switch typed := m.(type) {
		case *SubmitTaskRequest:
			out := &SubmitTaskRequest{}
			if err := gp.Unmarshal(b, out); err != nil {
				t.Fatalf("%s: unmarshal: %v", name, err)
			}
			if out.IdempotencyKey != typed.IdempotencyKey {
				t.Errorf("%s: key mismatch on round-trip: got %q, want %q", name, out.IdempotencyKey, typed.IdempotencyKey)
			}
		case *LogEntry:
			out := &LogEntry{}
			if err := gp.Unmarshal(b, out); err != nil {
				t.Fatalf("%s: unmarshal: %v", name, err)
			}
			if out.IdempotencyKey != typed.IdempotencyKey {
				t.Errorf("%s: key mismatch on round-trip: got %q, want %q", name, out.IdempotencyKey, typed.IdempotencyKey)
			}
		case *TaskInfo:
			out := &TaskInfo{}
			if err := gp.Unmarshal(b, out); err != nil {
				t.Fatalf("%s: unmarshal: %v", name, err)
			}
			if out.IdempotencyKey != typed.IdempotencyKey {
				t.Errorf("%s: key mismatch on round-trip: got %q, want %q", name, out.IdempotencyKey, typed.IdempotencyKey)
			}
		}
	}
}
