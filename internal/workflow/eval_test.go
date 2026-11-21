package workflow

import (
	"testing"
)

func TestEvaluateConditionDefaultOnSuccess(t *testing.T) {
	ctx := &EvalContext{
		ParentStages: map[string]*StageState{
			"build": {Status: StageCompleted},
			"test":  {Status: StageCompleted},
		},
	}

	ok, err := EvaluateCondition(ConditionOnSuccess, "", ctx)
	if err != nil || !ok {
		t.Fatalf("expected true, got %v (err: %v)", ok, err)
	}

	// With a failed parent
	ctx.ParentStages["test"].Status = StageFailed
	ok, err = EvaluateCondition(ConditionOnSuccess, "", ctx)
	if err != nil || ok {
		t.Fatalf("expected false, got %v (err: %v)", ok, err)
	}
}

func TestEvaluateConditionOnFailure(t *testing.T) {
	ctx := &EvalContext{
		ParentStages: map[string]*StageState{
			"build": {Status: StageCompleted},
		},
	}

	ok, err := EvaluateCondition(ConditionOnFailure, "", ctx)
	if err != nil || ok {
		t.Fatalf("expected false when all succeeded, got %v", ok)
	}

	ctx.ParentStages["build"].Status = StageFailed
	ok, err = EvaluateCondition(ConditionOnFailure, "", ctx)
	if err != nil || !ok {
		t.Fatalf("expected true when a parent failed, got %v", ok)
	}
}

func TestEvaluateConditionAlways(t *testing.T) {
	ctx := &EvalContext{
		ParentStages: map[string]*StageState{
			"build": {Status: StageFailed},
		},
	}

	ok, err := EvaluateCondition(ConditionAlways, "", ctx)
	if err != nil || !ok {
		t.Fatalf("expected true for always condition, got %v", ok)
	}
}

func TestEvaluateConditionExpression(t *testing.T) {
	ctx := &EvalContext{
		ParentStages: map[string]*StageState{
			"deploy": {Status: StageCompleted},
		},
		Variables: map[string]string{
			"env": "production",
		},
	}

	// Match stage status
	ok, err := EvaluateCondition(ConditionExpression, "stages.deploy.status == completed", ctx)
	if err != nil || !ok {
		t.Fatalf("expected true for stages.deploy.status == completed, got %v, err: %v", ok, err)
	}

	// Match variable
	ok, err = EvaluateCondition(ConditionExpression, "vars.env == production", ctx)
	if err != nil || !ok {
		t.Fatalf("expected true for vars.env == production, got %v, err: %v", ok, err)
	}

	// Mismatch variable
	ok, err = EvaluateCondition(ConditionExpression, "vars.env != production", ctx)
	if err != nil || ok {
		t.Fatalf("expected false for vars.env != production, got %v, err: %v", ok, err)
	}
}
