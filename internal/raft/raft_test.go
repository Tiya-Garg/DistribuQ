package raft

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeTransport routes RequestVote/Heartbeat calls directly to in-memory
// Node instances with controllable network partitions for deterministic testing.
type fakeTransport struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]map[string]bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		nodes:   make(map[string]*Node),
		blocked: make(map[string]map[string]bool),
	}
}

func (f *fakeTransport) register(n *Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[n.ID] = n
}

func (f *fakeTransport) isBlocked(from, to string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blocked[from] != nil && f.blocked[from][to]
}

func (f *fakeTransport) Disconnect(from, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked[from] == nil {
		f.blocked[from] = make(map[string]bool)
	}
	f.blocked[from][to] = true
}

func (f *fakeTransport) Connect(from, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked[from] != nil {
		delete(f.blocked[from], to)
	}
}

func (f *fakeTransport) Partition(group1, group2 []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range group1 {
		if f.blocked[a] == nil {
			f.blocked[a] = make(map[string]bool)
		}
		for _, b := range group2 {
			f.blocked[a][b] = true
			if f.blocked[b] == nil {
				f.blocked[b] = make(map[string]bool)
			}
			f.blocked[b][a] = true
		}
	}
}

func (f *fakeTransport) RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if f.isBlocked(args.CandidateID, peerID) {
		return nil, errNoPeer
	}
	f.mu.Lock()
	peer, ok := f.nodes[peerID]
	f.mu.Unlock()
	if !ok {
		return nil, errNoPeer
	}
	return peer.HandleRequestVote(args), nil
}

func (f *fakeTransport) Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error) {
	if f.isBlocked(args.LeaderID, peerID) {
		return nil, errNoPeer
	}
	f.mu.Lock()
	peer, ok := f.nodes[peerID]
	f.mu.Unlock()
	if !ok {
		return nil, errNoPeer
	}
	return peer.HandleHeartbeat(args), nil
}

var errNoPeer = &peerError{"no such peer"}

type peerError struct{ msg string }

func (e *peerError) Error() string { return e.msg }

func newCluster(t *testing.T, ids []string) (*fakeTransport, []*Node) {
	t.Helper()
	ft := newFakeTransport()
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := NewNode(id, peers, ft)
		nodes[i] = n
		ft.register(n)
	}
	return ft, nodes
}

func TestSingleLeaderElected(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	deadline := time.After(2 * time.Second)
	for {
		leaders := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leaders++
			}
		}
		if leaders == 1 {
			break // exactly one leader elected -- test passes
		}
		if leaders > 1 {
			t.Fatalf("split brain: %d nodes claim leadership simultaneously", leaders)
		}
		select {
		case <-deadline:
			t.Fatal("no leader elected within timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestFollowersRecognizeLeader(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	time.Sleep(1 * time.Second)

	var leaderID string
	for _, n := range nodes {
		if n.State() == Leader {
			leaderID = n.ID
		}
	}
	if leaderID == "" {
		t.Fatal("no leader elected")
	}

	for _, n := range nodes {
		if n.State() == Follower && n.LeaderID() != leaderID {
			t.Errorf("follower %s does not recognize leader %s (sees %q)", n.ID, leaderID, n.LeaderID())
		}
	}
}

func TestNewLeaderElectedAfterLeaderStops(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
	}

	// Wait for initial leader.
	time.Sleep(1 * time.Second)

	var firstLeader *Node
	for _, n := range nodes {
		if n.State() == Leader {
			firstLeader = n
		}
	}
	if firstLeader == nil {
		t.Fatal("no initial leader elected")
	}
	firstLeader.Stop()

	// A surviving node should take over within a couple of election
	// timeout windows.
	deadline := time.After(2 * time.Second)
	for {
		newLeader := ""
		for _, n := range nodes {
			if n == firstLeader {
				continue
			}
			if n.State() == Leader {
				newLeader = n.ID
			}
		}
		if newLeader != "" && newLeader != firstLeader.ID {
			for _, n := range nodes {
				if n != firstLeader {
					n.Stop()
				}
			}
			return // success
		}
		select {
		case <-deadline:
			t.Fatal("no new leader elected after original leader stopped")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestFollowerLogRepair(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2"})
	n1, n2 := nodes[0], nodes[1]

	// Setup n1 as leader with 4 log entries
	n1.mu.Lock()
	n1.state = Leader
	n1.currentTerm = 2
	n1.leaderID = "n1"
	n1.log = []LogEntry{
		{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "t1"},
		{Index: 2, Term: 1, Type: CmdSubmit, TaskID: "t2"},
		{Index: 3, Term: 2, Type: CmdSubmit, TaskID: "t3"},
		{Index: 4, Term: 2, Type: CmdSubmit, TaskID: "t4"},
	}
	n1.nextIndex = map[string]int{"n2": 5}
	n1.matchIndex = map[string]int{"n2": 0}
	n1.mu.Unlock()

	// n2 has conflicting log entry at index 3 from an older term
	n2.mu.Lock()
	n2.currentTerm = 1
	n2.state = Follower
	n2.log = []LogEntry{
		{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "t1"},
		{Index: 2, Term: 1, Type: CmdSubmit, TaskID: "t2"},
		{Index: 3, Term: 1, Type: CmdSubmit, TaskID: "conflict-t3"},
	}
	n2.mu.Unlock()

	// Leader heartbeats should repair follower's log
	for i := 0; i < 5; i++ {
		n1.sendHeartbeats()
		time.Sleep(30 * time.Millisecond)
	}

	n2Log := n2.Log()
	if len(n2Log) != 4 {
		t.Fatalf("expected n2 log len 4 after repair, got %d", len(n2Log))
	}
	if n2Log[2].TaskID != "t3" || n2Log[2].Term != 2 {
		t.Errorf("expected entry 3 repaired to (t3, term 2), got (%s, term %d)", n2Log[2].TaskID, n2Log[2].Term)
	}
	if n2Log[3].TaskID != "t4" || n2Log[3].Term != 2 {
		t.Errorf("expected entry 4 repaired to (t4, term 2), got (%s, term %d)", n2Log[3].TaskID, n2Log[3].Term)
	}
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	ft, nodes := newCluster(t, []string{"n1", "n2", "n3"})
	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	// Wait for a leader
	var leader *Node
	deadline := time.After(3 * time.Second)
	for leader == nil {
		select {
		case <-deadline:
			t.Fatal("no leader elected")
		case <-time.After(20 * time.Millisecond):
			for _, n := range nodes {
				if n.State() == Leader {
					leader = n
					break
				}
			}
		}
	}

	// Partition leader from the other two nodes
	var peers []string
	for _, n := range nodes {
		if n.ID != leader.ID {
			peers = append(peers, n.ID)
		}
	}
	ft.Partition([]string{leader.ID}, peers)

	// Attempt to propose an entry on the isolated leader
	_, _, commitCh, err := leader.Propose(LogEntry{
		Type:   CmdSubmit,
		TaskID: "partition-task",
	})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Must NOT commit in the minority partition
	select {
	case ok := <-commitCh:
		if ok {
			t.Fatal("entry was committed in a minority partition!")
		}
	case <-time.After(300 * time.Millisecond):
		// Success: timed out without commit
	}

	if leader.CommitIndex() >= leader.LastLogIndex() {
		t.Fatalf("commitIndex %d should be less than lastLogIndex %d in minority", leader.CommitIndex(), leader.LastLogIndex())
	}
}

func TestStaleNodeCannotWinElection(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	// n1 and n2 have up-to-date logs in term 2
	for _, n := range []*Node{n1, n2} {
		n.mu.Lock()
		n.currentTerm = 2
		n.log = []LogEntry{
			{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "t1"},
			{Index: 2, Term: 2, Type: CmdSubmit, TaskID: "t2"},
		}
		n.mu.Unlock()
	}

	// n3 is stale (was partitioned, term 1, only 1 entry)
	n3.mu.Lock()
	n3.currentTerm = 1
	n3.log = []LogEntry{
		{Index: 1, Term: 1, Type: CmdSubmit, TaskID: "t1"},
	}
	n3.mu.Unlock()

	// n3 attempts to win an election in term 3
	n3.startElection()

	if n3.State() == Leader {
		t.Fatal("stale node n3 won an election against more up-to-date nodes!")
	}
}

func TestCommittedTaskSurvivesLeaderKill(t *testing.T) {
	ft, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	// Elect n1 deterministically
	n1 := nodes[0]
	n1.startElection()
	if n1.State() != Leader {
		t.Fatalf("expected n1 to become leader, got %v", n1.State())
	}
	leader := n1

	// Propose a task on leader n1
	_, _, commitCh, err := leader.Propose(LogEntry{
		Type:    CmdSubmit,
		TaskID:  "survivor-1",
		Payload: "important-job",
	})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Wait for commit across cluster
	select {
	case ok := <-commitCh:
		if !ok {
			t.Fatal("failed to commit entry")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for commit")
	}

	// Kill leader n1 and disconnect it
	leader.Stop()
	ft.Disconnect("n1", "n2")
	ft.Disconnect("n1", "n3")
	ft.Disconnect("n2", "n1")
	ft.Disconnect("n3", "n1")

	// Elect new leader between n2 and n3
	nodes[1].startElection()
	if nodes[1].State() != Leader {
		nodes[2].startElection()
	}

	var newLeader *Node
	if nodes[1].State() == Leader {
		newLeader = nodes[1]
	} else if nodes[2].State() == Leader {
		newLeader = nodes[2]
	} else {
		t.Fatal("neither n2 nor n3 could be elected leader")
	}

	// Assert the new leader has the committed task in its log
	newLog := newLeader.Log()
	found := false
	for _, entry := range newLog {
		if entry.TaskID == "survivor-1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("committed task was lost after leader was killed!")
	}
}

func TestRestartedNodeRecoversLogFromDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-disk.json")
	persister := NewFilePersister(path)

	ft := newFakeTransport()
	n := NewNode("n1", []string{}, ft)
	if err := n.SetPersister(persister); err != nil {
		t.Fatal(err)
	}

	// Force election as single node
	n.startElection()
	if n.State() != Leader {
		t.Fatal("single node should become leader")
	}

	// Propose multiple task events
	events := []LogEntry{
		{Type: CmdSubmit, TaskID: "t-disk-1", Payload: "echo 1"},
		{Type: CmdAssign, TaskID: "t-disk-1", WorkerID: "w-1"},
		{Type: CmdComplete, TaskID: "t-disk-1", WorkerID: "w-1"},
	}
	for _, ev := range events {
		_, _, commitCh, err := n.Propose(ev)
		if err != nil {
			t.Fatalf("propose failed: %v", err)
		}
		if !<-commitCh {
			t.Fatal("commit failed")
		}
	}
	n.Stop()

	// Restart node from disk
	nRestarted := NewNode("n1", []string{}, ft)
	if err := nRestarted.SetPersister(NewFilePersister(path)); err != nil {
		t.Fatalf("failed to restore persister: %v", err)
	}

	if nRestarted.LastLogIndex() != 3 {
		t.Fatalf("expected 3 recovered log entries, got %d", nRestarted.LastLogIndex())
	}
	recoveredLog := nRestarted.Log()
	for i, want := range events {
		got := recoveredLog[i]
		if got.Type != want.Type || got.TaskID != want.TaskID {
			t.Errorf("entry %d mismatch: got %+v, want %+v", i, got, want)
		}
	}
}

func TestLeaderProposesNoopOnElection(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})
	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	leaderElectedCh := make(chan string, 1)
	for _, n := range nodes {
		node := n
		node.OnBecomeLeader(func() {
			select {
			case leaderElectedCh <- node.ID:
			default:
			}
		})
	}

	select {
	case leaderID := <-leaderElectedCh:
		var leaderNode *Node
		for _, n := range nodes {
			if n.ID == leaderID {
				leaderNode = n
				break
			}
		}
		if leaderNode == nil {
			t.Fatalf("could not find leader node %s", leaderID)
		}

		// Ensure the leader has a CmdNoop entry committed
		logEntries := leaderNode.Log()
		if len(logEntries) == 0 {
			t.Fatalf("expected at least 1 log entry (CmdNoop), got 0")
		}
		if logEntries[0].Type != CmdNoop {
			t.Fatalf("expected first entry to be CmdNoop, got %v", logEntries[0].Type)
		}
		if leaderNode.CommitIndex() < 1 {
			t.Fatalf("expected CmdNoop to be committed before OnBecomeLeader callback, commitIndex=%d", leaderNode.CommitIndex())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for leader election and noop commit")
	}
}

func TestApplyOrderingStrictlySequential(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1"})
	n := nodes[0]

	var appliedMu sync.Mutex
	var applied []int

	n.OnApply(func(e LogEntry) {
		appliedMu.Lock()
		defer appliedMu.Unlock()
		applied = append(applied, e.Index)
	})

	leaderReady := make(chan struct{}, 1)
	n.OnBecomeLeader(func() {
		leaderReady <- struct{}{}
	})

	n.startElection()
	if n.State() != Leader {
		t.Fatal("single node should become leader")
	}

	<-leaderReady

	numEntries := 50
	for i := 0; i < numEntries; i++ {
		_, _, commitCh, err := n.Propose(LogEntry{
			Type:   CmdSubmit,
			TaskID: fmt.Sprintf("task-%d", i),
		})
		if err != nil {
			t.Fatalf("propose failed: %v", err)
		}
		if !<-commitCh {
			t.Fatalf("commit failed for task %d", i)
		}
	}

	// Give a small grace period for all applications to finish
	time.Sleep(100 * time.Millisecond)

	appliedMu.Lock()
	defer appliedMu.Unlock()

	// Including the election no-op entry, there should be numEntries + 1 entries
	if len(applied) != numEntries+1 {
		t.Fatalf("expected %d entries applied, got %d", numEntries+1, len(applied))
	}
	for i := 0; i < len(applied); i++ {
		expectedIndex := i + 1
		if applied[i] != expectedIndex {
			t.Fatalf("applied entries out of order: at pos %d expected index %d, got %d", i, expectedIndex, applied[i])
		}
	}
}

func TestLeadershipLossSignalsFalseToCommitWaiters(t *testing.T) {
	ft, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	// Start n1 as leader
	n1 := nodes[0]
	n1.startElection()
	if n1.State() != Leader {
		t.Fatalf("expected n1 to be leader")
	}

	// Isolate n1 so it cannot replicate
	ft.Partition([]string{"n1"}, []string{"n2", "n3"})

	// Propose entry on isolated leader n1
	_, _, commitCh, err := n1.Propose(LogEntry{
		Type:   CmdSubmit,
		TaskID: "uncommitted-task",
	})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// n2 and n3 start an election in a higher term
	n2 := nodes[1]
	n2.startElection()

	// Reconnect and have n2 send heartbeat to n1 with higher term
	ft.Connect("n2", "n1")
	ft.Connect("n1", "n2")

	// Trigger heartbeat from new leader n2 to n1
	n2.sendHeartbeats()

	// The isolated former leader n1 must step down and signal false on commitCh
	select {
	case ok := <-commitCh:
		if ok {
			t.Fatal("expected commitCh to signal false upon leadership loss, got true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leadership loss notification on commitCh")
	}
}

func TestFastLogRollbackOnConflict(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2"})
	n1, n2 := nodes[0], nodes[1]

	// Leader n1 has 50 entries in term 5
	n1.mu.Lock()
	n1.state = Leader
	n1.currentTerm = 5
	n1.leaderID = "n1"
	n1.log = make([]LogEntry, 50)
	for i := 0; i < 50; i++ {
		n1.log[i] = LogEntry{Index: i + 1, Term: 5, Type: CmdSubmit, TaskID: fmt.Sprintf("leader-%d", i+1)}
	}
	n1.nextIndex = map[string]int{"n2": 51}
	n1.matchIndex = map[string]int{"n2": 0}
	n1.mu.Unlock()

	// Follower n2 has 50 conflicting entries in term 3
	n2.mu.Lock()
	n2.currentTerm = 3
	n2.state = Follower
	n2.log = make([]LogEntry, 50)
	for i := 0; i < 50; i++ {
		n2.log[i] = LogEntry{Index: i + 1, Term: 3, Type: CmdSubmit, TaskID: fmt.Sprintf("conflicting-%d", i+1)}
	}
	n2.mu.Unlock()

	// With fast rollback, the leader should jump back across the entire conflicting term
	// within 2 heartbeat rounds, instead of decrementing nextIndex by 1 for 50 rounds!
	n1.sendHeartbeats()
	time.Sleep(30 * time.Millisecond)
	n1.sendHeartbeats()
	time.Sleep(30 * time.Millisecond)

	n1.mu.Lock()
	nextIdx := n1.nextIndex["n2"]
	n1.mu.Unlock()

	// n2 had no entries in term 5 or term 4, so fast rollback should have set nextIndex to 1 (or match the leader's start)
	// without 50 roundtrips.
	if nextIdx > 2 {
		t.Fatalf("expected nextIndex to fast rollback close to 1 in 2 rounds, but nextIndex is %d", nextIdx)
	}
}
