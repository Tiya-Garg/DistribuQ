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
	}
}
