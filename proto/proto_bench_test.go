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
