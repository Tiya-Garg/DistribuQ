package workflow

import (
	"context"
	"testing"
)

type mockSubmitter struct {
	submitted []string
}

func (m *mockSubmitter) SubmitTask(id, payload string, priority int, tag string) error {
	m.submitted = append(m.submitted, id)
	return nil
}

func TestEngineFullExecutionLifecycle(t *testing.T) {
	sub := &mockSubmitter{}
	store := NewPipelineStore()
	eng := NewEngine(store, sub)

	def := &PipelineDef{
		ID:   "full-flow",
		Name: "Build and Deploy",
		Stages: []*StageDef{
			{
				ID: "build",
				Steps: []*StepDef{
					{ID: "compile", Payload: "go build"},
				},
			},
			{
				ID:        "deploy",
				DependsOn: []string{"build"},
				Condition: ConditionOnSuccess,
				Steps: []*StepDef{
					{ID: "publish", Payload: "docker push"},
				},
			},
			{
				ID:        "notify-failure",
				DependsOn: []string{"build"},
				Condition: ConditionOnFailure,
				Steps: []*StepDef{
					{ID: "alert", Payload: "send alert"},
				},
			},
		},
	}

	p, err := eng.StartPipeline(context.Background(), def)
	if err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	if p.Status != PipelineRunning {
		t.Fatalf("expected PipelineRunning, got %s", p.Status)
	}

	// build stage should be running
	buildStage := p.Stages["build"]
	if buildStage.Status != StageRunning {
		t.Fatalf("expected build stage running, got %s", buildStage.Status)
	}

	// Mark build compile step completed successfully
	taskID := "full-flow-build-compile"
	err = eng.HandleTaskCompletion(taskID, true, "build ok", "")
	if err != nil {
		t.Fatalf("HandleTaskCompletion failed: %v", err)
	}

	// build stage should now be completed
	if buildStage.Status != StageCompleted {
		t.Fatalf("expected build stage completed, got %s", buildStage.Status)
	}

	// deploy stage should now be triggered and running
	deployStage := p.Stages["deploy"]
	if deployStage.Status != StageRunning {
		t.Fatalf("expected deploy stage running, got %s", deployStage.Status)
	}

	// notify-failure stage should be pruned/skipped because build succeeded
	notifyStage := p.Stages["notify-failure"]
	if notifyStage.Status != StageSkipped {
		t.Fatalf("expected notify-failure stage skipped, got %s", notifyStage.Status)
	}

	// Mark deploy publish step completed
	deployTaskID := "full-flow-deploy-publish"
	_ = eng.HandleTaskCompletion(deployTaskID, true, "deploy ok", "")

	// Entire pipeline should now be completed
	if p.Status != PipelineCompleted {
		t.Fatalf("expected pipeline completed, got %s", p.Status)
	}
}
