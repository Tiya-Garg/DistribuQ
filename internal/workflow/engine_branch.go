package workflow

import (
	"time"
)

// pruneBranch marks a stage and its conditional branches as skipped if prerequisites are unmet.
func (e *Engine) pruneBranch(p *Pipeline, stageID string) error {
	stage := p.Stages[stageID]
	stage.Status = StageSkipped
	now := time.Now().UTC()
	stage.StartedAt = now
	stage.CompletedAt = now

	for _, step := range stage.Steps {
		step.Status = StepSkipped
		step.StartedAt = now
		step.CompletedAt = now
	}

	e.checkDownstream(p.ID, stageID)
	return nil
}

// checkDownstream triggers child stages if all their upstream dependencies have resolved.
func (e *Engine) checkDownstream(pipelineID, stageID string) {
	e.mu.RLock()
	graph := e.graphs[pipelineID]
	e.mu.RUnlock()

	p, err := e.store.Get(pipelineID)
	if err != nil {
		return
	}

	for _, childID := range graph.Forward[stageID] {
		childStage := p.Stages[childID]
		if childStage.Status != StagePending {
			continue
		}

		// Check if all upstream parents have completed, failed, or skipped
		parentsDone := true
		for _, parentID := range graph.Backward[childID] {
			parentStatus := p.Stages[parentID].Status
			if parentStatus == StagePending || parentStatus == StageRunning {
				parentsDone = false
				break
			}
		}

		if parentsDone {
			_ = e.triggerStage(pipelineID, childID)
		}
	}

	e.updatePipelineStatus(p)
}

// updatePipelineStatus checks if the entire pipeline has concluded.
func (e *Engine) updatePipelineStatus(p *Pipeline) {
	allTerminal := true
	hasFailed := false

	for _, s := range p.Stages {
		if s.Status == StagePending || s.Status == StageRunning {
			allTerminal = false
			break
		}
		if s.Status == StageFailed {
			hasFailed = true
		}
	}

	if allTerminal {
		p.CompletedAt = time.Now().UTC()
		p.UpdatedAt = p.CompletedAt
		if hasFailed {
			p.Status = PipelineFailed
		} else {
			p.Status = PipelineCompleted
		}
	}
}
