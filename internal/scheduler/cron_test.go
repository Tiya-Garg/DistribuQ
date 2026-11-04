package scheduler

import (
	"testing"
	"time"
)

func TestCronJobTriggerAndPersistence(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	// Add cron job running every minute
	err := s.AddCronJob("cron-job-1", "* * * * *", "echo cron", 2, "")
	if err != nil {
		t.Fatalf("AddCronJob failed: %v", err)
	}

	jobs := s.CronJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 cron job, got %d", len(jobs))
	}
	if jobs[0].ID != "cron-job-1" {
		t.Fatalf("expected job ID cron-job-1, got %s", jobs[0].ID)
	}

	// Fast-forward its NextRun to the past to simulate time passing
	s.mu.Lock()
	s.cronJobs["cron-job-1"].NextRun = time.Now().Add(-time.Second)
	s.mu.Unlock()

	// Trigger dispatchQueued, which should instantiate the scheduled task
	s.dispatchQueued()

	// Verify task was spawned and dispatched
	if fd.count("w1") != 1 {
		t.Fatalf("expected cron task to be dispatched to w1, got count=%d", fd.count("w1"))
	}

	// Verify snapshot persistence of cron jobs
	snap, err := s.EncodeState()
	if err != nil {
		t.Fatalf("EncodeState failed: %v", err)
	}

	s2 := NewScheduler(fd.fn)
	if err := s2.RestoreFromSnapshot(snap); err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	jobs2 := s2.CronJobs()
	if len(jobs2) != 1 {
		t.Fatalf("expected restored scheduler to have 1 cron job, got %d", len(jobs2))
	}
	if jobs2[0].Expression != "* * * * *" {
		t.Fatalf("expected expression * * * * *, got %s", jobs2[0].Expression)
	}

	// Remove cron job
	if err := s.RemoveCronJob("cron-job-1"); err != nil {
		t.Fatalf("RemoveCronJob failed: %v", err)
	}
	if len(s.CronJobs()) != 0 {
		t.Fatalf("expected 0 cron jobs after removal")
	}
}
