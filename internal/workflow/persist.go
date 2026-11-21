package workflow

import (
	"encoding/json"
	"fmt"
)

// PipelineSnapshot represents the serializable snapshot format of all pipeline states.
type PipelineSnapshot struct {
	Pipelines []*Pipeline `json:"pipelines"`
}

// Snapshot serializes all managed pipelines into a JSON byte slice.
func (s *PipelineStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := &PipelineSnapshot{
		Pipelines: make([]*Pipeline, 0, len(s.pipelines)),
	}

	for _, p := range s.pipelines {
		snap.Pipelines = append(snap.Pipelines, p)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize pipeline snapshot: %w", err)
	}
	return data, nil
}

// Restore restores pipelines from a snapshot payload, overwriting matching entries.
func (s *PipelineStore) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snap PipelineSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("failed to unmarshal pipeline snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range snap.Pipelines {
		if p != nil && p.ID != "" {
			s.pipelines[p.ID] = p
		}
	}

	return nil
}
