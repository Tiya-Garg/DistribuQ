package raft

import (
	"bytes"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestSnapshotTruncatesLog(t *testing.T) {
	n := NewNode("n1", []string{"n2"}, newFakeTransport())
	n.currentTerm = 2
	n.state = Leader
	for i := 1; i <= 5; i++ {
		n.log = append(n.log, LogEntry{Index: i, Term: 2, TaskID: "t"})
	}
	n.commitIndex = 5

	snapData := []byte("state-machine-snapshot")
	if err := n.Snapshot(3, snapData); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if n.LastIncludedIndex() != 3 {
		t.Errorf("expected LastIncludedIndex=3, got %d", n.LastIncludedIndex())
	}
	if n.LastIncludedTerm() != 2 {
		t.Errorf("expected LastIncludedTerm=2, got %d", n.LastIncludedTerm())
	}
	if len(n.Log()) != 2 {
		t.Fatalf("expected 2 entries left in log, got %d", len(n.Log()))
	}
	if n.Log()[0].Index != 4 || n.Log()[1].Index != 5 {
		t.Errorf("unexpected remaining log entries: %+v", n.Log())
	}
}

func TestHandleInstallSnapshot(t *testing.T) {
	n := NewNode("n2", []string{"n1"}, newFakeTransport())
	n.currentTerm = 1
	var applied atomic.Value

	n.OnApplySnapshot(func(data []byte) {
		applied.Store(data)
	})

	snapData := []byte("checkpoint-123")
	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "n1",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              snapData,
	})

	if reply.Term != 2 {
		t.Errorf("expected reply term 2, got %d", reply.Term)
	}
	if n.Term() != 2 {
		t.Errorf("expected node term 2, got %d", n.Term())
	}
	if n.LastIncludedIndex() != 10 || n.LastIncludedTerm() != 2 {
		t.Errorf("expected last included (10, 2), got (%d, %d)", n.LastIncludedIndex(), n.LastIncludedTerm())
	}
	if n.CommitIndex() != 10 {
		t.Errorf("expected commitIndex 10, got %d", n.CommitIndex())
	}

	// Verify asynchronous application to state machine
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if val := applied.Load(); val != nil {
			if !bytes.Equal(val.([]byte), snapData) {
				t.Fatalf("applied snapshot data mismatch: got %s, want %s", string(val.([]byte)), string(snapData))
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for snapshot to be applied to state machine")
}

func TestInstallSnapshotCatchesUpLaggingFollower(t *testing.T) {
	ft, nodes := newCluster(t, []string{"n1", "n2", "n3"})
	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	var leader *Node
	waitForCondition(t, 3*time.Second, func() bool {
		for _, n := range nodes {
			if n.State() == Leader {
				leader = n
				return true
			}
		}
		return false
	})

	// Disconnect follower from all nodes so it lags behind
	var follower *Node
	for _, n := range nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}
	for _, n := range nodes {
		if n.ID != follower.ID {
			ft.Disconnect(n.ID, follower.ID)
			ft.Disconnect(follower.ID, n.ID)
		}
	}

	// Propose 5 entries while follower is disconnected
	for i := 1; i <= 5; i++ {
		_, _, ch, err := leader.Propose(LogEntry{Type: CmdSubmit, TaskID: "t"})
		if err != nil {
			t.Fatalf("propose: %v", err)
		}
		<-ch
	}

	// Leader takes a snapshot up to entry 4 and discards them from its log
	snapData := []byte("snapshot-up-to-4")
	if err := leader.Snapshot(4, snapData); err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}

	var receivedSnapshot atomic.Value
	follower.OnApplySnapshot(func(data []byte) {
		receivedSnapshot.Store(data)
	})

	// Reconnect follower to all nodes
	for _, n := range nodes {
		if n.ID != follower.ID {
			ft.Connect(n.ID, follower.ID)
			ft.Connect(follower.ID, n.ID)
		}
	}

	// The leader must detect nextIndex[follower] <= lastIncludedIndex and send InstallSnapshot
	waitForCondition(t, 5*time.Second, func() bool {
		return follower.LastIncludedIndex() >= 4 && follower.CommitIndex() >= 5
	})

	if val := receivedSnapshot.Load(); val == nil || !bytes.Equal(val.([]byte), snapData) {
		t.Errorf("follower did not receive snapshot data properly: %v", val)
	}
}

func TestSnapshotPersistedRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.raft.json")
	p := NewFilePersister(path)
	n := NewNode("n1", []string{"n2"}, newFakeTransport())
	if err := n.SetPersister(p); err != nil {
		t.Fatal(err)
	}

	n.currentTerm = 3
	n.votedFor = "n1"
	n.state = Leader
	n.log = []LogEntry{{Index: 1, Term: 3, TaskID: "t1"}, {Index: 2, Term: 3, TaskID: "t2"}}
	n.commitIndex = 2

	if err := n.Snapshot(2, []byte("durable-snap")); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Restarted node
	nRestarted := NewNode("n1", []string{"n2"}, newFakeTransport())
	if err := nRestarted.SetPersister(NewFilePersister(path)); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if nRestarted.Term() != 3 {
		t.Errorf("want term 3, got %d", nRestarted.Term())
	}
	if nRestarted.LastIncludedIndex() != 2 || nRestarted.LastIncludedTerm() != 3 {
		t.Errorf("want last included (2, 3), got (%d, %d)", nRestarted.LastIncludedIndex(), nRestarted.LastIncludedTerm())
	}
	if string(nRestarted.SnapshotData()) != "durable-snap" {
		t.Errorf("want durable-snap, got %s", string(nRestarted.SnapshotData()))
	}
}

func TestMultiRoundSnapshotCompaction(t *testing.T) {
	n := NewNode("n-compact", []string{"peer-1"}, newFakeTransport())
	n.currentTerm = 1
	n.state = Leader

	// Round 1
	for i := 1; i <= 10; i++ {
		n.log = append(n.log, LogEntry{Index: i, Term: 1, TaskID: "t"})
	}
	n.commitIndex = 10
	if err := n.Snapshot(6, []byte("snap-round-1")); err != nil {
		t.Fatalf("Snapshot round 1: %v", err)
	}
	if n.LastIncludedIndex() != 6 || len(n.Log()) != 4 {
		t.Fatalf("round 1: expected lastIncludedIndex 6 and 4 remaining entries, got %d, %d", n.LastIncludedIndex(), len(n.Log()))
	}

	// Round 2
	for i := 11; i <= 20; i++ {
		n.log = append(n.log, LogEntry{Index: i, Term: 1, TaskID: "t"})
	}
	n.commitIndex = 20
	if err := n.Snapshot(15, []byte("snap-round-2")); err != nil {
		t.Fatalf("Snapshot round 2: %v", err)
	}
	if n.LastIncludedIndex() != 15 || len(n.Log()) != 5 {
		t.Fatalf("round 2: expected lastIncludedIndex 15 and 5 remaining entries, got %d, %d", n.LastIncludedIndex(), len(n.Log()))
	}
	if string(n.SnapshotData()) != "snap-round-2" {
		t.Fatalf("round 2: expected data 'snap-round-2', got %q", string(n.SnapshotData()))
	}
}
