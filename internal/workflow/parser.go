package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ParsePipelineJSON parses and validates a JSON string or byte slice into a PipelineDef.
func ParsePipelineJSON(data []byte) (*PipelineDef, error) {
	if len(data) == 0 {
		return nil, errors.New("workflow: empty pipeline payload")
	}

	var def PipelineDef
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("workflow: failed to unmarshal json: %w", err)
	}

	if def.ID == "" {
		return nil, ErrEmptyPipelineID
	}
	if len(def.Stages) == 0 {
		return nil, ErrNoStagesDefined
	}

	// Validate stages have non-empty IDs and steps
	for _, stage := range def.Stages {
		if strings.TrimSpace(stage.ID) == "" {
			return nil, errors.New("workflow: stage id must not be empty")
		}
		if len(stage.Steps) == 0 {
			return nil, fmt.Errorf("workflow: stage %s must have at least one step", stage.ID)
		}
		for _, step := range stage.Steps {
			if strings.TrimSpace(step.ID) == "" {
				return nil, fmt.Errorf("workflow: step id in stage %s must not be empty", stage.ID)
			}
			if strings.TrimSpace(step.Payload) == "" {
				return nil, fmt.Errorf("workflow: step payload for %s.%s must not be empty", stage.ID, step.ID)
			}
		}
	}

	// Verify graph structure and absence of cycles
	if _, err := BuildGraph(&def); err != nil {
		return nil, err
	}

	return &def, nil
}
