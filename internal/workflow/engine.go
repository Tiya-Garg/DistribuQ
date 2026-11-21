package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TaskSubmitter is an interface implemented by the scheduler to accept atomic tasks from workflow steps.
type TaskSubmitter interface {
	SubmitTask(id, payload string, priority int, tag string) error
}

// Engine coordinates the execution of workflow pipelines.
type Engine struct {
	mu        sync.RWMutex
	store     *PipelineStore
	graphs    map[string]*Graph
	defs      map[string]*PipelineDef
	submitter TaskSubmitter
	taskMap   map[string]stepRef // taskID -> {pipelineID, stageID, stepID}
}

type stepRef struct {
	PipelineID string
	StageID    string
	StepID     string
}

// NewEngine creates an initialized workflow execution Engine.
func NewEngine(store *PipelineStore, submitter TaskSubmitter) *Engine {
	if store == nil {
		store = NewPipelineStore()
	}
	return &Engine{
		store:     store,
		graphs:    make(map[string]*Graph),
		defs:      make(map[string]*PipelineDef),
		submitter: submitter,
		taskMap:   make(map[string]stepRef),
	}
}

// StartPipeline initiates the execution of a registered pipeline blueprint.
func (e *Engine) StartPipeline(ctx context.Context, def *PipelineDef) (*Pipeline, error) {
	graph, err := BuildGraph(def)
	if err != nil {
		return nil, fmt.Errorf("invalid pipeline graph: %w", err)
	}

	p, err := e.store.Create(def)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	e.graphs[def.ID] = graph
	e.defs[def.ID] = def
	p.Status = PipelineRunning
	p.UpdatedAt = time.Now().UTC()
	e.mu.Unlock()

	// Launch root stages that have no dependencies
	for _, root := range graph.RootStages() {
		if err := e.triggerStage(p.ID, root.ID); err != nil {
			return nil, err
		}
	}

	return p, nil
}
