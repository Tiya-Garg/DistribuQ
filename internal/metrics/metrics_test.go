package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestMetricsWritePrometheus(t *testing.T) {
	reg := &Registry{}
	reg.IncSubmitted()
	reg.IncSubmitted()
	reg.IncCompleted()
	reg.IncFailed()
	reg.IncCanceled()
	reg.IncDLQ()
	reg.IncRetried()
	reg.IncCron()

	var buf bytes.Buffer
	reg.WritePrometheus(&buf, map[string]float64{
		"distribuq_workers_count": 3,
		"distribuq_tasks_queued":  5,
	})

	output := buf.String()
	expectedSubstrings := []string{
		"distribuq_tasks_submitted_total 2",
		"distribuq_tasks_completed_total 1",
		"distribuq_tasks_failed_total 1",
		"distribuq_tasks_canceled_total 1",
		"distribuq_tasks_dlq_total 1",
		"distribuq_tasks_retried_total 1",
		"distribuq_cron_triggers_total 1",
		"distribuq_workers_count 3.000000",
		"distribuq_tasks_queued 5.000000",
	}

	for _, substr := range expectedSubstrings {
		if !strings.Contains(output, substr) {
			t.Errorf("expected output to contain %q, but got:\n%s", substr, output)
		}
	}
}
