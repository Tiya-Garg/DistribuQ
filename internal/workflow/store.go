package workflow

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrPipelineNotFound = errors.New("workflow: pipeline not found")
	ErrPipelineExists   = errors.New("workflow: pipeline already exists")
)

// PipelineStore stores and retrieves pipelines in memory with thread safety.
type PipelineStore struct {
	mu        sync.RWMutex
	pipelines map[string]*Pipeline
}

// NewPipelineStore creates an initialized PipelineStore.
func NewPipelineStore() *PipelineStore {
	return &PipelineStore{
		pipelines: make(map[string]*Pipeline),
	}
}

// Create instantiates a new pipeline from a validated definition.
func (s *PipelineStore) Create(def *PipelineDef) (*Pipeline, error) {
	if def == nil {
		return nil, errors.New("pipeline definition cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.pipelines[def.ID]; exists {
		return nil, ErrPipelineExists
	}

	now := time.Now().UTC()
	p := &Pipeline{
		ID:          def.ID,
		Name:        def.Name,
		Description: def.Description,
		Status:      PipelinePending,
		Stages:      make(map[string]*StageState),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	for _, stageDef := range def.Stages {
		ss := &StageState{
			ID:         stageDef.ID,
			Name:       stageDef.Name,
			Status:     StagePending,
			Condition:  stageDef.Condition,
			Expression: stageDef.Expression,
			DependsOn:  stageDef.DependsOn,
			Steps:      make(map[string]*StepState),
		}
		for _, stepDef := range stageDef.Steps {
			ss.Steps[stepDef.ID] = &StepState{
				ID:     stepDef.ID,
				Status: StepPending,
			}
		}
		p.Stages[stageDef.ID] = ss
	}

	s.pipelines[def.ID] = p
	return p, nil
}

// Get returns the pipeline matching the given ID.
func (s *PipelineStore) Get(id string) (*Pipeline, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.pipelines[id]
	if !ok {
		return nil, ErrPipelineNotFound
	}
	return p, nil
}

// List returns summaries of all active pipelines.
func (s *PipelineStore) List() []*PipelineSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	summaries := make([]*PipelineSummary, 0, len(s.pipelines))
	for _, p := range s.pipelines {
		done := 0
		for _, stage := range p.Stages {
			if stage.Status == StageCompleted || stage.Status == StageFailed || stage.Status == StageSkipped {
				done++
			}
		}
		summaries = append(summaries, &PipelineSummary{
			ID:          p.ID,
			Name:        p.Name,
			Status:      p.Status,
			TotalStages: len(p.Stages),
			DoneStages:  done,
			CreatedAt:   p.CreatedAt,
			UpdatedAt:   p.UpdatedAt,
		})
	}
	return summaries
}
