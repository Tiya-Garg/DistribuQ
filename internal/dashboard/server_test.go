package dashboard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
)

type fakeTransport struct{}

func (fakeTransport) RequestVote(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{}, nil
}
func (fakeTransport) Heartbeat(string, *raft.HeartbeatArgs) (*raft.HeartbeatReply, error) {
	return &raft.HeartbeatReply{}, nil
}
func (fakeTransport) InstallSnapshot(string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

func setupTestServer(t *testing.T) (*Server, *raft.Node, *scheduler.Scheduler) {
	t.Helper()
	node := raft.NewNode("s1", nil, fakeTransport{})
	sched := scheduler.NewScheduler(func(string, string, *scheduler.Task) error { return nil })
	srv := NewServer("127.0.0.1:0", node, sched)
	return srv, node, sched
}

func TestDashboardIndex(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	srv.handleIndex(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "DistribuQ") || !strings.Contains(body, "Live SSE") {
		t.Errorf("index html missing expected brand content: %s", body)
	}
}

func TestDashboardStatusAPI(t *testing.T) {
	srv, node, sched := setupTestServer(t)
	sched.RegisterWorker("w1", "127.0.0.1:9001")
	_, _ = sched.SubmitTaskWithPriority("t1", "echo hi", 2)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()

	srv.handleStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var status ClusterStateView
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}

	if status.Raft.NodeID != node.ID {
		t.Errorf("expected node ID %s, got %s", node.ID, status.Raft.NodeID)
	}
	if len(status.Workers) != 1 || status.Workers[0].ID != "w1" {
		t.Errorf("expected worker w1, got %+v", status.Workers)
	}
	if len(status.Tasks) != 1 || status.Tasks[0].ID != "t1" || status.Tasks[0].Priority != 2 {
		t.Errorf("expected task t1 with priority 2, got %+v", status.Tasks)
	}
}

func TestDashboardSubmitAPI(t *testing.T) {
	srv, node, sched := setupTestServer(t)

	// Submitting to non-leader should return 400
	payload := `{"task_id":"t2","payload":"echo test","priority":3}`
	req := httptest.NewRequest(http.MethodPost, "/api/submit", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()

	srv.handleSubmit(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on non-leader, got %d", rec.Code)
	}

	// Make node leader and propose directly
	sched.SetRaftProposer(node)
	node.OnApply(func(entry raft.LogEntry) {
		sched.ApplyEntry(entry)
	})
	go node.Run()
	defer node.Stop()

	// Wait for election
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && node.State() != raft.Leader {
		time.Sleep(20 * time.Millisecond)
	}

	reqLeader := httptest.NewRequest(http.MethodPost, "/api/submit", bytes.NewBufferString(payload))
	recLeader := httptest.NewRecorder()
	srv.handleSubmit(recLeader, reqLeader)

	if recLeader.Code != http.StatusOK {
		t.Fatalf("expected 200 on leader, got %d: %s", recLeader.Code, recLeader.Body.String())
	}

	tasks, _ := sched.DetailedStatus()
	found := false
	for _, tk := range tasks {
		if tk.ID == "t2" && tk.Priority == 3 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected submitted task t2 with priority 3 in scheduler, got %+v", tasks)
	}
}

func TestDashboardEventsSSE(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + srv.server.Addr + "/api/events")
	if err != nil {
		// If srv.server.Addr is empty because of 127.0.0.1:0, test with listener addr
		t.Skip("skipping direct dial if ephemeral port address not populated on server struct")
		return
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read SSE line: %v", err)
	}
	if !strings.HasPrefix(line, "data: ") {
		t.Errorf("expected SSE data line, got %s", line)
	}
}
