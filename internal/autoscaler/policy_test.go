package autoscaler

import (
	"testing"
)

func TestPolicyValidate(t *testing.T) {
	p := DefaultPolicy()
	if err := p.Validate(); err != nil {
		t.Fatalf("expected DefaultPolicy to be valid, got: %v", err)
	}

	badMin := Policy{MinWorkers: -1, MaxWorkers: 5, TargetQueueLength: 2}
	if err := badMin.Validate(); err == nil {
		t.Fatalf("expected error for negative min workers")
	}

	badMax := Policy{MinWorkers: 5, MaxWorkers: 2, TargetQueueLength: 2}
	if err := badMax.Validate(); err != ErrInvalidConfig {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}

	badTarget := Policy{MinWorkers: 1, MaxWorkers: 5, TargetQueueLength: 0}
	if err := badTarget.Validate(); err == nil {
		t.Fatalf("expected error for non-positive target queue length")
	}
}
