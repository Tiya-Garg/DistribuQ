package workflow

import (
	"testing"
)

func TestParsePipelineJSONSuccess(t *testing.T) {
	raw := `{
		"id": "pipe-deploy-1",
		"name": "Deployment Workflow",
		"stages": [
			{
				"id": "build",
				"steps": [
					{"id": "compile", "payload": "go build ./..."},
					{"id": "test", "payload": "go test ./..."}
				]
			},
			{
				"id": "notify",
				"depends_on": ["build"],
				"condition": "always",
				"steps": [
					{"id": "slack", "payload": "curl slack.com"}
				]
			}
		]
	}`

	def, err := ParsePipelineJSON([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if def.ID != "pipe-deploy-1" {
		t.Errorf("expected ID pipe-deploy-1, got %s", def.ID)
	}
	if len(def.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(def.Stages))
	}
}

func TestParsePipelineJSONInvalid(t *testing.T) {
	// Empty payload
	if _, err := ParsePipelineJSON([]byte{}); err == nil {
		t.Error("expected error for empty payload")
	}

	// Missing stages
	if _, err := ParsePipelineJSON([]byte(`{"id":"p1","stages":[]}`)); err == nil {
		t.Error("expected error for empty stages")
	}

	// Empty step payload
	badStep := `{"id":"p1","stages":[{"id":"s1","steps":[{"id":"st1","payload":""}]}]}`
	if _, err := ParsePipelineJSON([]byte(badStep)); err == nil {
		t.Error("expected error for empty step payload")
	}
}
