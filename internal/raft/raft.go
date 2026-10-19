// Package raft implements a simplified Raft-inspired leader election
// protocol used to elect a single active scheduler among a cluster of
// scheduler replicas, so the system tolerates a scheduler crash without
// losing the ability to dispatch tasks.
package raft

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ErrNotLeader is returned when a non-leader node is asked to propose a command.
var ErrNotLeader = errors.New("raft: this node is not the leader")

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Transport abstracts sending RequestVote / Heartbeat RPCs to a peer.
// A concrete implementation (net/rpc today, gRPC later) satisfies this
// interface so the election algorithm itself stays transport-agnostic.
type Transport interface {
	RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error)
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type HeartbeatArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type HeartbeatReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

// Node is a single participant in the leader election cluster.
type Node struct {
	mu sync.Mutex

	ID    string
	Peers []string

	currentTerm int
	votedFor    string
	state       State
	leaderID    string

	log         []LogEntry
	commitIndex int
	lastApplied int

	// Leader-specific tracking
	nextIndex     map[string]int
	matchIndex    map[string]int
	commitWaiters map[int][]chan bool

	transport Transport

	// persister, if set, durably stores currentTerm, votedFor, and log. Every
	// change to either is saved before the node acts on it or replies
	// to a peer (see persistLocked).
	persister Persister

	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}

	// onBecomeLeader / onBecomeFollower let the scheduler react to
	// role changes (e.g. start/stop accepting task submissions).
	onBecomeLeader   func()
	onBecomeFollower func()
	onApply          func(LogEntry)
}

func NewNode(id string, peers []string, transport Transport) *Node {
	return &Node{
		ID:              id,
		Peers:           peers,
		state:           Follower,
		transport:       transport,
		electionTimeout: randomElectionTimeout(),
		stopCh:          make(chan struct{}),
	}
}

// SetPersister attaches durable storage and restores currentTerm,
// votedFor, and log from it. It must be called before Run.
func (n *Node) SetPersister(p Persister) error {
	term, votedFor, logEntries, err := p.Load()
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.persister = p
	n.currentTerm = term
	n.votedFor = votedFor
	n.log = append([]LogEntry{}, logEntries...)
	return nil
}

// persistLocked saves currentTerm, votedFor, and log. Callers must hold n.mu.
// A node with no persister always succeeds.
func (n *Node) persistLocked() error {
	if n.persister == nil {
		return nil
	}
	return n.persister.Save(n.currentTerm, n.votedFor, n.log)
}

func (n *Node) OnBecomeLeader(fn func())   { n.onBecomeLeader = fn }
func (n *Node) OnBecomeFollower(fn func()) { n.onBecomeFollower = fn }
func (n *Node) OnApply(fn func(LogEntry))  { n.onApply = fn }

func randomElectionTimeout() time.Duration {
	// 150-300ms range, randomized per node so simultaneous elections
	// (split votes) are rare, mirroring the standard Raft approach.
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Run starts the election timeout loop and heartbeat loop (if leader).
// It blocks until Stop() is called, so callers should run it in a
// goroutine.
func (n *Node) Run() {
	n.resetElectionTimer()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) Stop() {
	n.mu.Lock()
	close(n.stopCh)
	for _, chs := range n.commitWaiters {
		for _, ch := range chs {
			select {
			case ch <- false:
			default:
			}
		}
	}
	n.commitWaiters = make(map[int][]chan bool)
	n.mu.Unlock()
}

func (n *Node) tick() {
	n.mu.Lock()
	state := n.state
	timedOut := time.Since(n.electionResetAt) > n.electionTimeout
	n.mu.Unlock()

	switch state {
	case Leader:
		n.sendHeartbeats()
	default:
		if timedOut {
			n.startElection()
		}
	}
}

func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	n.electionResetAt = time.Now()
	n.electionTimeout = randomElectionTimeout()
	n.mu.Unlock()
}

func (n *Node) stepDownLocked(term int) {
	wasLeaderOrCandidate := n.state != Follower
	n.currentTerm = term
	n.state = Follower
	n.leaderID = ""
	n.votedFor = ""
	n.electionResetAt = time.Now()

	for idx, chs := range n.commitWaiters {
		if idx > n.commitIndex {
			for _, ch := range chs {
				select {
				case ch <- false:
				default:
				}
			}
		}
	}
	n.commitWaiters = make(map[int][]chan bool)

	if err := n.persistLocked(); err != nil {
		log.Printf("raft[%s]: persist failed: %v", n.ID, err)
	}
	if wasLeaderOrCandidate && n.onBecomeFollower != nil {
		go n.onBecomeFollower()
	}
}

// startElection transitions this node to Candidate and requests votes
// from all peers in parallel. If it wins a majority, it becomes Leader.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID
	if err := n.persistLocked(); err != nil {
		// Without a durable vote we must not ask anyone for theirs.
		log.Printf("raft[%s]: cannot persist election state, abandoning election: %v", n.ID, err)
		n.currentTerm--
		n.votedFor = ""
		n.state = Follower
		n.mu.Unlock()
		n.resetElectionTimer()
		return
	}
	term := n.currentTerm
	peers := append([]string{}, n.Peers...)
	lastIndex := len(n.log)
	lastTerm := 0
	if lastIndex > 0 {
		lastTerm = n.log[lastIndex-1].Term
	}
	n.mu.Unlock()

	n.resetElectionTimer()

	votes := 1 // vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			reply, err := n.transport.RequestVote(peerID, &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.ID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil || reply == nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()

	majority := (len(peers)+1)/2 + 1
	if n.state == Candidate && n.currentTerm == term && votes >= majority {
		n.state = Leader
		n.leaderID = n.ID
		n.nextIndex = make(map[string]int)
		n.matchIndex = make(map[string]int)
		lastIdx := len(n.log)
		for _, p := range n.Peers {
			n.nextIndex[p] = lastIdx + 1
			n.matchIndex[p] = 0
		}
		if n.onBecomeLeader != nil {
			go n.onBecomeLeader()
		}
		go n.sendHeartbeats()
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderID := n.ID
	commitIndex := n.commitIndex
	peers := append([]string{}, n.Peers...)

	for _, peer := range peers {
		prevIndex := n.nextIndex[peer] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		prevTerm := 0
		if prevIndex > 0 && prevIndex <= len(n.log) {
			prevTerm = n.log[prevIndex-1].Term
		}
		var entries []LogEntry
		if prevIndex < len(n.log) {
			entries = append([]LogEntry{}, n.log[prevIndex:]...)
		}

		go func(peerID string, prevIdx, prevTrm int, ents []LogEntry) {
			reply, err := n.transport.Heartbeat(peerID, &HeartbeatArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTrm,
				Entries:      ents,
				LeaderCommit: commitIndex,
			})
			if err != nil || reply == nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
				return
			}
			if n.state != Leader || n.currentTerm != term {
				return
			}

			if reply.Success {
				if reply.MatchIndex > n.matchIndex[peerID] {
					n.matchIndex[peerID] = reply.MatchIndex
					n.nextIndex[peerID] = reply.MatchIndex + 1
				}
				n.checkCommitLocked()
			} else {
				// Follower log repair on mismatch
				if n.nextIndex[peerID] > 1 {
					n.nextIndex[peerID]--
				}
			}
		}(peer, prevIndex, prevTerm, entries)
	}
	n.mu.Unlock()
}

func (n *Node) checkCommitLocked() {
	for idx := len(n.log); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}
		matches := 1 // leader itself
		for _, peer := range n.Peers {
			if n.matchIndex[peer] >= idx {
				matches++
			}
		}
		majority := (len(n.Peers)+1)/2 + 1
		if matches >= majority {
			n.commitIndex = idx
			for cIdx, chs := range n.commitWaiters {
				if cIdx <= n.commitIndex {
					for _, ch := range chs {
						select {
						case ch <- true:
						default:
						}
					}
					delete(n.commitWaiters, cIdx)
				}
			}
			toApply := n.applyCommittedLocked()
			if len(toApply) > 0 && n.onApply != nil {
				go func(ents []LogEntry) {
					for _, e := range ents {
						n.onApply(e)
					}
				}(toApply)
			}
			break
		}
	}
}

func (n *Node) applyCommittedLocked() []LogEntry {
	var toApply []LogEntry
	for n.lastApplied < n.commitIndex && n.lastApplied < len(n.log) {
		n.lastApplied++
		toApply = append(toApply, n.log[n.lastApplied-1])
	}
	return toApply
}

// HandleRequestVote is invoked (via the transport's RPC server side)
// when a peer asks this node for its vote.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID

	// Election restriction: candidate's log must be at least as up to date as voter's log.
	myLastTerm := 0
	myLastIndex := len(n.log)
	if myLastIndex > 0 {
		myLastTerm = n.log[myLastIndex-1].Term
	}

	upToDate := false
	if args.LastLogTerm > myLastTerm {
		upToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex {
		upToDate = true
	}

	granted := canVote && upToDate
	if granted {
		n.votedFor = args.CandidateID
		n.electionResetAt = time.Now()
	}

	if err := n.persistLocked(); err != nil {
		log.Printf("raft[%s]: persist failed, refusing vote for %s: %v", n.ID, args.CandidateID, err)
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	return &RequestVoteReply{Term: n.currentTerm, VoteGranted: granted}
}

// HandleHeartbeat is invoked when this node receives a leader heartbeat.
func (n *Node) HandleHeartbeat(args *HeartbeatArgs) *HeartbeatReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &HeartbeatReply{Term: n.currentTerm, Success: false, MatchIndex: len(n.log)}
	}

	wasLeaderOrCandidate := n.state != Follower
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()

	if wasLeaderOrCandidate && n.onBecomeFollower != nil {
		go n.onBecomeFollower()
	}

	// PrevLog consistency check
	if args.PrevLogIndex > 0 {
		if len(n.log) < args.PrevLogIndex {
			_ = n.persistLocked()
			return &HeartbeatReply{Term: n.currentTerm, Success: false, MatchIndex: len(n.log)}
		}
		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			_ = n.persistLocked()
			return &HeartbeatReply{Term: n.currentTerm, Success: false, MatchIndex: args.PrevLogIndex - 1}
		}
	}

	// Insert entries, truncating on conflict
	logChanged := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= len(n.log) {
			if n.log[index-1].Term != entry.Term {
				n.log = n.log[:index-1]
				n.log = append(n.log, entry)
				logChanged = true
			}
		} else {
			n.log = append(n.log, entry)
			logChanged = true
		}
	}

	if logChanged {
		if err := n.persistLocked(); err != nil {
			log.Printf("raft[%s]: persist failed: %v", n.ID, err)
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		if n.commitIndex > len(n.log) {
			n.commitIndex = len(n.log)
		}
		toApply := n.applyCommittedLocked()
		if len(toApply) > 0 && n.onApply != nil {
			go func(ents []LogEntry) {
				for _, e := range ents {
					n.onApply(e)
				}
			}(toApply)
		}
	}

	return &HeartbeatReply{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: len(n.log),
	}
}

// Propose appends a new LogEntry to the leader's log and begins replicating it.
// It returns the entry's index, term, and a channel that signals when the entry
// has been committed by a majority (true) or leadership was lost before commit (false).
func (n *Node) Propose(entry LogEntry) (int, int, <-chan bool, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, 0, nil, ErrNotLeader
	}

	entry.Index = len(n.log) + 1
	entry.Term = n.currentTerm
	n.log = append(n.log, entry)
	index := entry.Index
	term := entry.Term

	if err := n.persistLocked(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.mu.Unlock()
		return 0, 0, nil, err
	}

	commitCh := make(chan bool, 1)
	if n.commitWaiters == nil {
		n.commitWaiters = make(map[int][]chan bool)
	}
	n.commitWaiters[index] = append(n.commitWaiters[index], commitCh)

	// If single node cluster, commit immediately
	if len(n.Peers) == 0 {
		n.commitIndex = index
		commitCh <- true
		toApply := n.applyCommittedLocked()
		if len(toApply) > 0 && n.onApply != nil {
			go func(ents []LogEntry) {
				for _, e := range ents {
					n.onApply(e)
				}
			}(toApply)
		}
	}

	n.mu.Unlock()

	go n.sendHeartbeats()
	return index, term, commitCh, nil
}

func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) LastLogIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

func (n *Node) LastLogTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) Log() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry{}, n.log...)
}
