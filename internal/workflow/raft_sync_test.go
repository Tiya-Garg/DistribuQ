package workflow

import (
	"testing"
)

func TestEncodeDecodeRaftEvent(t *testing.T) {
	orig := &PipelineRaftEvent{
		Type:       "start_pipeline",
		PipelineID: "p-100",
		StageID:    "stage-build",
		Status:     StageRunning,
		PipelineDef: &PipelineDef{
			ID:   "p-100",
			Name: "Raft Pipeline",
		},
	}

	data, err := EncodeRaftEvent(orig)
	if err != nil {
		t.Fatalf("EncodeRaftEvent failed: %v", err)
	}

	decoded, err := DecodeRaftEvent(data)
	if err != nil {
		t.Fatalf("DecodeRaftEvent failed: %v", err)
	}

	if decoded.PipelineID != orig.PipelineID || decoded.Type != orig.Type {
		t.Fatalf("decoded mismatch: expected %v, got %v", orig, decoded)
	}
}
