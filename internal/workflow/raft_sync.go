package workflow

import (
	"encoding/json"
	"fmt"
)

// PipelineRaftEvent represents an event payload replicated via Raft.
type PipelineRaftEvent struct {
	Type        string       `json:"type"` // "start_pipeline" or "stage_transition"
	PipelineDef *PipelineDef `json:"pipeline_def,omitempty"`
	PipelineID  string       `json:"pipeline_id,omitempty"`
	StageID     string       `json:"stage_id,omitempty"`
	Status      StageStatus  `json:"status,omitempty"`
}

// EncodeRaftEvent serializes a pipeline event for Raft replication.
func EncodeRaftEvent(event *PipelineRaftEvent) ([]byte, error) {
	if event == nil {
		return nil, fmt.Errorf("event cannot be nil")
	}
	return json.Marshal(event)
}

// DecodeRaftEvent deserializes a pipeline event from a Raft log entry payload.
func DecodeRaftEvent(data []byte) (*PipelineRaftEvent, error) {
	var ev PipelineRaftEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, fmt.Errorf("failed to decode pipeline raft event: %w", err)
	}
	return &ev, nil
}
