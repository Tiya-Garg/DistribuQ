package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
)

type dummyTransport struct{}

func (d *dummyTransport) RequestVote(peerID string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, nil
}
func (d *dummyTransport) Heartbeat(peerID string, args *raft.HeartbeatArgs) (*raft.HeartbeatReply, error) {
	return nil, nil
}
func (d *dummyTransport) InstallSnapshot(peerID string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, nil
}

func BenchmarkDashboardStatusJSONSerialization(b *testing.B) {
	node := raft.NewNode("dash-node", []string{"dash-peer"}, &dummyTransport{})
	sched := scheduler.NewScheduler(nil)
	for i := 0; i < 20; i++ {
		_, _ = sched.SubmitTask("t", "echo 1")
	}

	s := NewServer(":0", node, sched)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		s.handleStatus(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected status 200, got %d", w.Code)
		}
	}
}

func BenchmarkDashboardTaskLogsJSON(b *testing.B) {
	node := raft.NewNode("dash-node", []string{"dash-peer"}, &dummyTransport{})
	sched := scheduler.NewScheduler(nil)
	for i := 0; i < 50; i++ {
		sched.AppendTaskLog("bench-log-task", "w1", "standard output log line content\n", false)
	}

	s := NewServer(":0", node, sched)
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/bench-log-task/logs", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		s.handleTaskLogs(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected status 200, got %d", w.Code)
		}
	}
}
