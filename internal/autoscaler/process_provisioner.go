package autoscaler

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
)

// ProcessProvisioner manages local worker processes via command execution.
type ProcessProvisioner struct {
	mu            sync.Mutex
	workerBin     string
	schedulerAddr string
	procs         map[string]*exec.Cmd
}

// NewProcessProvisioner constructs a ProcessProvisioner.
func NewProcessProvisioner(workerBin, schedulerAddr string) *ProcessProvisioner {
	return &ProcessProvisioner{
		workerBin:     workerBin,
		schedulerAddr: schedulerAddr,
		procs:         make(map[string]*exec.Cmd),
	}
}

// SpawnWorker launches a new worker process pointed at the scheduler.
func (p *ProcessProvisioner) SpawnWorker(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cmd := exec.Command(p.workerBin, "-id", id, "-scheduler", p.schedulerAddr)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start worker process %s: %w", id, err)
	}
	p.procs[id] = cmd
	return nil
}

// DrainWorker gracefully stops one of the managed worker processes.
func (p *ProcessProvisioner) DrainWorker(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	targetID := id
	if targetID == "" {
		for k := range p.procs {
			targetID = k
			break
		}
	}
	if targetID == "" {
		return nil
	}

	cmd, exists := p.procs[targetID]
	if !exists {
		return nil
	}
	delete(p.procs, targetID)
	if cmd.Process != nil {
		return cmd.Process.Kill()
	}
	return nil
}

