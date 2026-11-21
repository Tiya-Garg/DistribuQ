package workflow

import "time"

// Condition defines when a stage or step should execute relative to its upstream dependencies.
type Condition string

const (
	// ConditionOnSuccess executes if all upstream dependencies completed successfully.
	ConditionOnSuccess Condition = "on_success"
	// ConditionOnFailure executes only if at least one upstream dependency failed.
	ConditionOnFailure Condition = "on_failure"
	// ConditionAlways executes regardless of whether upstream dependencies succeeded or failed.
	ConditionAlways Condition = "always"
	// ConditionExpression evaluates a custom boolean expression against upstream output/status.
	ConditionExpression Condition = "expression"
)

// PipelineStatus represents the lifecycle state of a workflow pipeline.
type PipelineStatus string

const (
	PipelinePending   PipelineStatus = "pending"
	PipelineRunning   PipelineStatus = "running"
	PipelineCompleted PipelineStatus = "completed"
	PipelineFailed    PipelineStatus = "failed"
	PipelineCanceled  PipelineStatus = "canceled"
)

// StageStatus represents the execution state of an individual pipeline stage.
type StageStatus string

const (
	StagePending   StageStatus = "pending"
	StageRunning   StageStatus = "running"
	StageCompleted StageStatus = "completed"
	StageFailed    StageStatus = "failed"
	StageSkipped   StageStatus = "skipped"
	StageCanceled  StageStatus = "canceled"
)

// StepStatus represents the status of an atomic task step inside a stage.
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

// PipelineSummary provides a high-level overview of a pipeline instance.
type PipelineSummary struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Status      PipelineStatus `json:"status"`
	TotalStages int            `json:"total_stages"`
	DoneStages  int            `json:"done_stages"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}
