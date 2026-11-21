package workflow

import (
	"fmt"
	"time"
)

// triggerStage begins the execution of an individual stage.
func (e *Engine) triggerStage(pipelineID, stageID string) error {
	p, err := e.store.Get(pipelineID)
	if err != nil {
		return err
	}

	stage, ok := p.Stages[stageID]
	if !ok {
		return fmt.Errorf("stage %s not found in pipeline %s", stageID, pipelineID)
	}

	def := e.defs[pipelineID]
	var stageDef *StageDef
	for _, s := range def.Stages {
		if s.ID == stageID {
			stageDef = s
			break
		}
	}
	if stageDef == nil {
		return fmt.Errorf("stage definition %s not found", stageID)
	}

	// Build context from upstream parent stages
	parentMap := make(map[string]*StageState)
	for _, depID := range stageDef.DependsOn {
		if parentStage, exists := p.Stages[depID]; exists {
			parentMap[depID] = parentStage
		}
	}

	evalCtx := &EvalContext{
		ParentStages: parentMap,
	}

	shouldRun, err := EvaluateCondition(stageDef.Condition, stageDef.Expression, evalCtx)
	if err != nil {
		return fmt.Errorf("evaluating condition for stage %s failed: %w", stageID, err)
	}

	if !shouldRun {
		return e.pruneBranch(p, stageID)
	}

	stage.Status = StageRunning
	stage.StartedAt = time.Now().UTC()

	// Dispatch each step in this stage
	for _, stepDef := range stageDef.Steps {
		taskID := fmt.Sprintf("%s-%s-%s", pipelineID, stageID, stepDef.ID)
		stepState := stage.Steps[stepDef.ID]
		stepState.TaskID = taskID
		stepState.Status = StepRunning
		stepState.StartedAt = time.Now().UTC()

		e.mu.Lock()
		e.taskMap[taskID] = stepRef{
			PipelineID: pipelineID,
			StageID:    stageID,
			StepID:     stepDef.ID,
		}
		e.mu.Unlock()

		if e.submitter != nil {
			if err := e.submitter.SubmitTask(taskID, stepDef.Payload, stepDef.Priority, stepDef.RequiredTag); err != nil {
				stepState.Status = StepFailed
				stepState.Error = err.Error()
				stepState.CompletedAt = time.Now().UTC()
			}
		}
	}

	return nil
}

// HandleTaskCompletion updates step status when a scheduler task finishes or fails.
func (e *Engine) HandleTaskCompletion(taskID string, success bool, output, errMsg string) error {
	e.mu.RLock()
	ref, exists := e.taskMap[taskID]
	e.mu.RUnlock()

	if !exists {
		// Not a workflow-managed task; ignore
		return nil
	}

	p, err := e.store.Get(ref.PipelineID)
	if err != nil {
		return err
	}

	stage := p.Stages[ref.StageID]
	step := stage.Steps[ref.StepID]

	step.CompletedAt = time.Now().UTC()
	step.Output = output
	step.Error = errMsg
	if success {
		step.Status = StepCompleted
	} else {
		step.Status = StepFailed
	}

	// Check if all steps in this stage are done
	allDone := true
	stageFailed := false
	for _, s := range stage.Steps {
		if s.Status == StepRunning || s.Status == StepPending {
			allDone = false
			break
		}
		if s.Status == StepFailed {
			stageFailed = true
		}
	}

	if allDone {
		stage.CompletedAt = time.Now().UTC()
		if stageFailed {
			stage.Status = StageFailed
		} else {
			stage.Status = StageCompleted
		}

		// Trigger downstream stages
		e.checkDownstream(ref.PipelineID, ref.StageID)
	}

	return nil
}
