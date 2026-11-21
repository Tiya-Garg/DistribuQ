package workflow

import (
	"testing"
)

func TestBuildGraphValid(t *testing.T) {
	def := &PipelineDef{
		ID:   "pipeline-1",
		Name: "CI/CD Pipeline",
		Stages: []*StageDef{
			{ID: "lint"},
			{ID: "build", DependsOn: []string{"lint"}},
			{ID: "test", DependsOn: []string{"build"}},
			{ID: "deploy", DependsOn: []string{"test"}},
		},
	}

	g, err := BuildGraph(def)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	roots := g.RootStages()
	if len(roots) != 1 || roots[0].ID != "lint" {
		t.Fatalf("expected root stage 'lint', got %v", roots)
	}

	if len(g.Forward["lint"]) != 1 || g.Forward["lint"][0] != "build" {
		t.Fatalf("expected 'lint' forward to 'build', got %v", g.Forward["lint"])
	}
}

func TestBuildGraphCyclic(t *testing.T) {
	def := &PipelineDef{
		ID: "cyclic-pipeline",
		Stages: []*StageDef{
			{ID: "a", DependsOn: []string{"c"}},
			{ID: "b", DependsOn: []string{"a"}},
			{ID: "c", DependsOn: []string{"b"}},
		},
	}

	_, err := BuildGraph(def)
	if err == nil {
		t.Fatal("expected cyclic dependency error, got nil")
	}
}

func TestBuildGraphMissingStage(t *testing.T) {
	def := &PipelineDef{
		ID: "missing-stage",
		Stages: []*StageDef{
			{ID: "a", DependsOn: []string{"nonexistent"}},
		},
	}

	_, err := BuildGraph(def)
	if err == nil {
		t.Fatal("expected stage not found error, got nil")
	}
}
