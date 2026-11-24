package scheduler

import (
	"context"
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/workflow"
)

func TestWorkflowBridgeSubmissionAndCallback(t *testing.T) {
	sched := NewScheduler(func(workerID, addr string, task *Task) error {
		return nil
	})
	wb := NewWorkflowBridge(sched)

	def := &workflow.PipelineDef{
		ID:   "bridge-test-pipe",
		Name: "Bridge Test",
		Stages: []*workflow.StageDef{
			{
				ID: "stage-1",
				Steps: []*workflow.StepDef{
					{ID: "step-1", Payload: "echo 1"},
				},
			},
		},
	}

	pipeline, err := wb.StartPipeline(context.Background(), def)
	if err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	if pipeline.Status != workflow.PipelineRunning {
		t.Fatalf("expected PipelineRunning, got %s", pipeline.Status)
	}

	taskID := "bridge-test-pipe-stage-1-step-1"
	wb.OnTaskCompleted(taskID, "done")

	p, err := wb.Store().Get("bridge-test-pipe")
	if err != nil {
		t.Fatalf("failed to get pipeline: %v", err)
	}

	if p.Status != workflow.PipelineCompleted {
		t.Fatalf("expected pipeline completed after step completion, got %s", p.Status)
	}
}
