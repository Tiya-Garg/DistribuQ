package distribuq

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func BenchmarkProtobufSubmitTaskRequestMarshal(b *testing.B) {
	req := &SubmitTaskRequest{
		TaskId:         "task-bench-pb",
		Payload:        "echo benchmark",
		Priority:       2,
		RequiredTag:    "gpu",
		RunAtUnixMs:    1700000000000,
		DependsOn:      []string{"dep-1", "dep-2"},
		IdempotencyKey: "idemp-key-12345",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(req)
		if err != nil {
			b.Fatalf("Marshal failed: %v", err)
		}
		_ = data
	}
}

func BenchmarkProtobufSubmitTaskRequestUnmarshal(b *testing.B) {
	req := &SubmitTaskRequest{
		TaskId:         "task-bench-pb",
		Payload:        "echo benchmark",
		Priority:       2,
		RequiredTag:    "gpu",
		RunAtUnixMs:    1700000000000,
		DependsOn:      []string{"dep-1", "dep-2"},
		IdempotencyKey: "idemp-key-12345",
	}
	data, err := proto.Marshal(req)
	if err != nil {
		b.Fatalf("setup Marshal failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded SubmitTaskRequest
		if err := proto.Unmarshal(data, &decoded); err != nil {
			b.Fatalf("Unmarshal failed: %v", err)
		}
	}
}

func BenchmarkProtobufLogEntryMarshal(b *testing.B) {
	entry := &LogEntry{
		Index:          100,
		Term:           5,
		Type:           "submitted",
		TaskId:         "log-entry-bench",
		Payload:        "echo benchmark log entry",
		Priority:       3,
		RequiredTag:    "highmem",
		RunAtUnixMs:    1700000000000,
		IdempotencyKey: "log-idemp-12345",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(entry)
		if err != nil {
			b.Fatalf("Marshal log entry failed: %v", err)
		}
		_ = data
	}
}
