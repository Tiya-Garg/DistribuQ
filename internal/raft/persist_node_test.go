package raft

import (
	"errors"
	"path/filepath"
	"testing"
)

func newPersistedNode(t *testing.T, path string) *Node {
	t.Helper()
	n := NewNode("n1", []string{"n2", "n3"}, newFakeTransport())
	if err := n.SetPersister(NewFilePersister(path)); err != nil {
		t.Fatal(err)
	}
	return n
}

// The bug this guards against: a node votes for A in term 5, crashes,
// restarts with no memory of the vote, and votes for B in term 5 too.
func TestRestartDoesNotDoubleVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.json")

	n := newPersistedNode(t, path)
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n2"}); !r.VoteGranted {
		t.Fatal("first vote in term 5 should be granted")
	}

	// "Restart": a brand new Node reading the same state file.
	restarted := newPersistedNode(t, path)
	if got := restarted.Term(); got != 5 {
		t.Fatalf("restarted node term = %d, want 5", got)
	}
	if r := restarted.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n3"}); r.VoteGranted {
		t.Fatal("restarted node voted twice in term 5")
	}
	// The original candidate may still re-ask and get the same answer.
	if r := restarted.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n2"}); !r.VoteGranted {
		t.Fatal("restarted node should repeat its vote for n2")
	}
}

func TestWithoutPersisterVoteIsForgottenOnRestart(t *testing.T) {
	// Documents the failure mode the persister fixes.
	n := NewNode("n1", []string{"n2", "n3"}, newFakeTransport())
	n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n2"})

	restarted := NewNode("n1", []string{"n2", "n3"}, newFakeTransport())
	if r := restarted.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n3"}); !r.VoteGranted {
		t.Fatal("expected the unpersisted node to have forgotten its vote")
	}
}

func TestElectionPersistsTermAndSelfVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.json")
	n := newPersistedNode(t, path)
	n.startElection() // peers are unknown to the fake transport; election just can't win

	term, votedFor, _, err := NewFilePersister(path).Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 1 || votedFor != "n1" {
		t.Fatalf("persisted (%d, %q), want (1, n1)", term, votedFor)
	}
}

type failingPersister struct{}

func (failingPersister) Load() (int, string, []LogEntry, error) { return 0, "", nil, nil }
func (failingPersister) Save(int, string, []LogEntry) error     { return errors.New("disk full") }

func TestVoteRefusedWhenItCannotBePersisted(t *testing.T) {
	n := NewNode("n1", []string{"n2", "n3"}, newFakeTransport())
	if err := n.SetPersister(failingPersister{}); err != nil {
		t.Fatal(err)
	}
	if r := n.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "n2"}); r.VoteGranted {
		t.Fatal("granted a vote that could not be made durable")
	}
}

func TestElectionAbandonedWhenStateCannotBePersisted(t *testing.T) {
	n := NewNode("n1", []string{"n2", "n3"}, newFakeTransport())
	if err := n.SetPersister(failingPersister{}); err != nil {
		t.Fatal(err)
	}
	n.startElection()
	if n.State() != Follower || n.Term() != 0 {
		t.Fatalf("state=%v term=%d, want follower at term 0", n.State(), n.Term())
	}
}
