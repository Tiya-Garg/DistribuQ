package workflow

import (
	"testing"
)

func TestPipelineStoreCreateAndGet(t *testing.T) {
	store := NewPipelineStore()
	def := &PipelineDef{
		ID:   "p1",
		Name: "Test Pipeline",
		Stages: []*StageDef{
			{
				ID: "s1",
				Steps: []*StepDef{
					{ID: "step1", Payload: "echo 1"},
				},
			},
		},
	}

	p, err := store.Create(def)
	if err != nil {
		t.Fatalf("unexpected error creating pipeline: %v", err)
	}

	if p.ID != "p1" || p.Status != PipelinePending {
		t.Fatalf("unexpected pipeline state: %v", p)
	}

	// Retrieve
	retrieved, err := store.Get("p1")
	if err != nil {
		t.Fatalf("unexpected error getting pipeline: %v", err)
	}
	if retrieved.ID != "p1" {
		t.Errorf("expected ID p1, got %s", retrieved.ID)
	}

	// Duplicate create should fail
	if _, err := store.Create(def); err != ErrPipelineExists {
		t.Errorf("expected ErrPipelineExists, got %v", err)
	}

	// Non-existent get
	if _, err := store.Get("nonexistent"); err != ErrPipelineNotFound {
		t.Errorf("expected ErrPipelineNotFound, got %v", err)
	}
}

func TestPipelineStoreList(t *testing.T) {
	store := NewPipelineStore()
	def1 := &PipelineDef{ID: "p1", Stages: []*StageDef{{ID: "s1", Steps: []*StepDef{{ID: "st1"}}}}}
	def2 := &PipelineDef{ID: "p2", Stages: []*StageDef{{ID: "s2", Steps: []*StepDef{{ID: "st2"}}}}}

	_, _ = store.Create(def1)
	_, _ = store.Create(def2)

	list := store.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 items, got %d", len(list))
	}
}
