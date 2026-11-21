package workflow

import (
	"testing"
	"time"
)

func TestPipelineStoreSnapshotAndRestore(t *testing.T) {
	store := NewPipelineStore()
	def := &PipelineDef{
		ID:   "snap-pipe",
		Name: "Snap Pipe",
		Stages: []*StageDef{
			{
				ID: "s1",
				Steps: []*StepDef{
					{ID: "step1", Payload: "test"},
				},
			},
		},
	}

	p, err := store.Create(def)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	p.Status = PipelineCompleted
	p.CompletedAt = time.Now().UTC()

	data, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	newStore := NewPipelineStore()
	err = newStore.Restore(data)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	restored, err := newStore.Get("snap-pipe")
	if err != nil {
		t.Fatalf("Get on restored store failed: %v", err)
	}

	if restored.ID != "snap-pipe" || restored.Status != PipelineCompleted {
		t.Errorf("restored pipeline mismatch: %v", restored)
	}
}
