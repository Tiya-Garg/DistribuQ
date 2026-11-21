package workflow

import "time"

// StepDef defines a single task unit within a stage.
type StepDef struct {
	ID          string   `json:"id" yaml:"id"`
	Payload     string   `json:"payload" yaml:"payload"`
	Priority    int      `json:"priority" yaml:"priority"`
	RequiredTag string   `json:"required_tag,omitempty" yaml:"required_tag,omitempty"`
	MaxRetries  int      `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	Timeout     string   `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

// StageDef defines an execution stage comprised of multiple parallel or sequential steps.
type StageDef struct {
	ID         string     `json:"id" yaml:"id"`
	Name       string     `json:"name" yaml:"name"`
	DependsOn  []string   `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
	Condition  Condition  `json:"condition,omitempty" yaml:"condition,omitempty"`
	Expression string     `json:"expression,omitempty" yaml:"expression,omitempty"`
	Steps      []*StepDef `json:"steps" yaml:"steps"`
}

// PipelineDef represents the static blueprint of a workflow pipeline.
type PipelineDef struct {
	ID          string      `json:"id" yaml:"id"`
	Name        string      `json:"name" yaml:"name"`
	Description string      `json:"description,omitempty" yaml:"description,omitempty"`
	Stages      []*StageDef `json:"stages" yaml:"stages"`
}

// StepState tracks execution details for an instantiated step.
type StepState struct {
	ID          string     `json:"id"`
	TaskID      string     `json:"task_id"`
	Status      StepStatus `json:"status"`
	Output      string     `json:"output,omitempty"`
	Error       string     `json:"error,omitempty"`
	WorkerID    string     `json:"worker_id,omitempty"`
	StartedAt   time.Time  `json:"started_at,omitempty"`
	CompletedAt time.Time  `json:"completed_at,omitempty"`
}

// StageState tracks runtime state for an instantiated stage.
type StageState struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Status      StageStatus           `json:"status"`
	Condition   Condition             `json:"condition"`
	Expression  string                `json:"expression,omitempty"`
	DependsOn   []string              `json:"depends_on,omitempty"`
	Steps       map[string]*StepState `json:"steps"`
	StartedAt   time.Time             `json:"started_at,omitempty"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
}

// Pipeline tracks the live execution state of a workflow pipeline.
type Pipeline struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Status      PipelineStatus         `json:"status"`
	Stages      map[string]*StageState `json:"stages"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	CompletedAt time.Time              `json:"completed_at,omitempty"`
}
