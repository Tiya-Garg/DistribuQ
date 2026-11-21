package workflow

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyPipelineID   = errors.New("workflow: pipeline id must not be empty")
	ErrNoStagesDefined   = errors.New("workflow: pipeline must contain at least one stage")
	ErrDuplicateStageID  = errors.New("workflow: duplicate stage id detected")
	ErrStageNotFound     = errors.New("workflow: referenced stage does not exist")
	ErrCyclicDependency  = errors.New("workflow: cyclic dependency detected in stages DAG")
)

// Graph represents the validated dependency graph of stages within a pipeline.
type Graph struct {
	Stages    map[string]*StageDef
	Forward   map[string][]string // parent -> children
	Backward  map[string][]string // child -> parents
}

// BuildGraph constructs and validates a dependency graph from a PipelineDef.
func BuildGraph(def *PipelineDef) (*Graph, error) {
	if def == nil || def.ID == "" {
		return nil, ErrEmptyPipelineID
	}
	if len(def.Stages) == 0 {
		return nil, ErrNoStagesDefined
	}

	g := &Graph{
		Stages:   make(map[string]*StageDef),
		Forward:  make(map[string][]string),
		Backward: make(map[string][]string),
	}

	for _, s := range def.Stages {
		if _, exists := g.Stages[s.ID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateStageID, s.ID)
		}
		g.Stages[s.ID] = s
		g.Forward[s.ID] = make([]string, 0)
		g.Backward[s.ID] = make([]string, 0)
	}

	for _, s := range def.Stages {
		for _, dep := range s.DependsOn {
			if _, exists := g.Stages[dep]; !exists {
				return nil, fmt.Errorf("%w: stage %s depends on non-existent stage %s", ErrStageNotFound, s.ID, dep)
			}
			g.Forward[dep] = append(g.Forward[dep], s.ID)
			g.Backward[s.ID] = append(g.Backward[s.ID], dep)
		}
	}

	if err := g.detectCycles(); err != nil {
		return nil, err
	}

	return g, nil
}

// detectCycles runs a DFS cycle detection across all stages.
func (g *Graph) detectCycles() error {
	visited := make(map[string]int) // 0: unvisited, 1: visiting, 2: visited

	var dfs func(u string) error
	dfs = func(u string) error {
		visited[u] = 1
		for _, v := range g.Forward[u] {
			if visited[v] == 1 {
				return fmt.Errorf("%w: edge %s -> %s forms a cycle", ErrCyclicDependency, u, v)
			}
			if visited[v] == 0 {
				if err := dfs(v); err != nil {
					return err
				}
			}
		}
		visited[u] = 2
		return nil
	}

	for id := range g.Stages {
		if visited[id] == 0 {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}
	return nil
}

// RootStages returns all stages that have no upstream dependencies.
func (g *Graph) RootStages() []*StageDef {
	roots := make([]*StageDef, 0)
	for id, parents := range g.Backward {
		if len(parents) == 0 {
			roots = append(roots, g.Stages[id])
		}
	}
	return roots
}
