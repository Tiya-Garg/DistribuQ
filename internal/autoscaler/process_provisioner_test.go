package autoscaler

import (
	"context"
	"testing"
)

func TestProcessProvisionerInit(t *testing.T) {
	prov := NewProcessProvisioner("worker.exe", "localhost:8000")
	if prov.workerBin != "worker.exe" || prov.schedulerAddr != "localhost:8000" {
		t.Fatalf("unexpected provisioner state: %+v", prov)
	}

	// Drain non-existent worker shouldn't error
	if err := prov.DrainWorker(context.Background(), "non-existent"); err != nil {
		t.Fatalf("unexpected error draining non-existent worker: %v", err)
	}
}

