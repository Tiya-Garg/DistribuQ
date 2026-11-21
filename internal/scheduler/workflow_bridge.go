package scheduler

import (
	"context"
	"fmt"

	"github.com/Tiya-Garg/DistribuQ/internal/workflow"
)

// WorkflowBridge adapts Scheduler to serve as a workflow.TaskSubmitter and embeds the workflow.Engine.
type WorkflowBridge struct {
	scheduler *Scheduler
	engine    *workflow.Engine
	store     *workflow.PipelineStore
}

// NewWorkflowBridge initializes a workflow bridge for the scheduler.
func NewWorkflowBridge(s *Scheduler) *WorkflowBridge {
	store := workflow.NewPipelineStore()
	wb := &WorkflowBridge{
		scheduler: s,
		store:     store,
	}
	wb.engine = workflow.NewEngine(store, wb)
	return wb
}

// SubmitTask implements workflow.TaskSubmitter by submitting a task into the DistribuQ scheduler queue.
func (wb *WorkflowBridge) SubmitTask(id, payload string, priority int, tag string) error {
	_, err := wb.scheduler.SubmitTaskWithOpts(id, payload, priority, tag)
	return err
}

// Engine returns the underlying workflow Engine.
func (wb *WorkflowBridge) Engine() *workflow.Engine {
	return wb.engine
}

// Store returns the underlying workflow PipelineStore.
func (wb *WorkflowBridge) Store() *workflow.PipelineStore {
	return wb.store
}

// StartPipeline initiates the execution of a workflow pipeline.
func (wb *WorkflowBridge) StartPipeline(ctx context.Context, def *workflow.PipelineDef) (*workflow.Pipeline, error) {
	if def == nil {
		return nil, fmt.Errorf("pipeline definition cannot be nil")
	}
	return wb.engine.StartPipeline(ctx, def)
}
