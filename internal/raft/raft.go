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

// Transport abstracts sending RequestVote / Heartbeat / InstallSnapshot RPCs to a peer.
// Concrete implementations (gRPC, net/rpc, in-memory) satisfy this interface
// so the consensus algorithm itself stays transport-agnostic.
type Transport interface {
	RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error)
	InstallSnapshot(peerID string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
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
	Term          int
	Success       bool
	MatchIndex    int
	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshotArgs carries a state-machine snapshot from the leader to a lagging follower.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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

	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotData      []byte
	commitIndex       int
	lastApplied       int

	// Leader-specific tracking
	nextIndex     map[string]int
	matchIndex    map[string]int
	commitWaiters map[int][]chan bool

	transport Transport

	// persister, if set, durably stores currentTerm, votedFor, log, and snapshot.
	// Every change is saved before the node acts on it or replies to a peer (see persistLocked).
	persister Persister

	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh          chan struct{}
	applyCh         chan LogEntry
	applySnapshotCh chan []byte

	// onBecomeLeader / onBecomeFollower let the scheduler react to
	// role changes (e.g. start/stop accepting task submissions).
	onBecomeLeader   func()
	onBecomeFollower func()
	onApply          func(LogEntry)
	onApplySnapshot  func([]byte)
}

func NewNode(id string, peers []string, transport Transport) *Node {
	n := &Node{
		ID:              id,
		Peers:           peers,
		state:           Follower,
		transport:       transport,
		electionTimeout: randomElectionTimeout(),
		stopCh:          make(chan struct{}),
		applyCh:         make(chan LogEntry, 1024),
		applySnapshotCh: make(chan []byte, 16),
	}
	go n.applyLoop()
	return n
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case entry := <-n.applyCh:
			n.mu.Lock()
			fn := n.onApply
			n.mu.Unlock()
			if fn != nil {
				fn(entry)
			}
		case snap := <-n.applySnapshotCh:
			n.mu.Lock()
			fn := n.onApplySnapshot
			n.mu.Unlock()
			if fn != nil {
				fn(snap)
			}
		}
	}
}

// SetPersister attaches durable storage and restores currentTerm,
// votedFor, log, and snapshot from it. It must be called before Run.
func (n *Node) SetPersister(p Persister) error {
	term, votedFor, logEntries, lastIdx, lastTerm, snap, err := p.Load()
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.persister = p
	n.currentTerm = term
	n.votedFor = votedFor
	n.log = append([]LogEntry{}, logEntries...)
	n.lastIncludedIndex = lastIdx
	n.lastIncludedTerm = lastTerm
	n.snapshotData = snap
	if lastIdx > n.commitIndex {
		n.commitIndex = lastIdx
	}
	if lastIdx > n.lastApplied {
		n.lastApplied = lastIdx
	}
	return nil
}

// persistLocked saves currentTerm, votedFor, log, and snapshot. Callers must hold n.mu.
// A node with no persister always succeeds.
func (n *Node) persistLocked() error {
	if n.persister == nil {
		return nil
	}
	return n.persister.Save(n.currentTerm, n.votedFor, n.log, n.lastIncludedIndex, n.lastIncludedTerm, n.snapshotData)
}

func (n *Node) OnApplySnapshot(fn func([]byte)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onApplySnapshot = fn
}

func (n *Node) OnBecomeLeader(fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onBecomeLeader = fn
}

func (n *Node) OnBecomeFollower(fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onBecomeFollower = fn
}

func (n *Node) OnApply(fn func(LogEntry)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onApply = fn
}

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
	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()
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
		lastIdx := n.lastLogIndexLocked()
		for _, p := range n.Peers {
			n.nextIndex[p] = lastIdx + 1
			n.matchIndex[p] = 0
		}
		if n.onBecomeLeader != nil {
			cb := n.onBecomeLeader
			// Raft paper Section 5.4.2: A leader cannot commit log entries from previous terms
			// by counting replicas; it must commit an entry from its current term.
			// Propose a no-op entry and invoke onBecomeLeader once committed.
			go func() {
				_, _, commitCh, err := n.Propose(LogEntry{Type: CmdNoop})
				if err != nil {
					return
				}
				if ok := <-commitCh; ok {
					cb()
				}
			}()
		}
		go n.sendHeartbeats()
	}
}

func (n *Node) lastLogIndexLocked() int {
	if len(n.log) == 0 {
		return n.lastIncludedIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() int {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termForIndexLocked(index int) int {
	if index == 0 {
		return 0
	}
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	offset := index - n.lastIncludedIndex - 1
	if offset >= 0 && offset < len(n.log) {
		return n.log[offset].Term
	}
	return 0
}

func (n *Node) entriesFromLocked(startIndex int) []LogEntry {
	if startIndex <= n.lastIncludedIndex {
		return nil
	}
	offset := startIndex - n.lastIncludedIndex - 1
	if offset < 0 || offset > len(n.log) {
		return nil
	}
	return append([]LogEntry{}, n.log[offset:]...)
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

		// If follower has fallen behind our compacted log, stream the snapshot
		if prevIndex < n.lastIncludedIndex {
			snapData := append([]byte{}, n.snapshotData...)
			lastIdx := n.lastIncludedIndex
			lastTrm := n.lastIncludedTerm
			go func(peerID string, sData []byte, lIdx, lTrm int) {
				reply, err := n.transport.InstallSnapshot(peerID, &InstallSnapshotArgs{
					Term:              term,
					LeaderID:          leaderID,
					LastIncludedIndex: lIdx,
					LastIncludedTerm:  lTrm,
					Data:              sData,
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
				if lIdx > n.matchIndex[peerID] {
					n.matchIndex[peerID] = lIdx
					n.nextIndex[peerID] = lIdx + 1
				}
			}(peer, snapData, lastIdx, lastTrm)
			continue
		}

		prevTerm := n.termForIndexLocked(prevIndex)
		entries := n.entriesFromLocked(prevIndex + 1)

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
				// Follower fast log rollback on mismatch
				if reply.ConflictTerm > 0 {
					// Leader searches its own log for the last entry with ConflictTerm
					lastIdxForTerm := -1
					for i := len(n.log) - 1; i >= 0; i-- {
						if n.log[i].Term == reply.ConflictTerm {
							lastIdxForTerm = n.log[i].Index
							break
						}
					}
					if lastIdxForTerm > 0 {
						n.nextIndex[peerID] = lastIdxForTerm + 1
					} else {
						n.nextIndex[peerID] = reply.ConflictIndex
					}
				} else if reply.ConflictIndex > 0 {
					n.nextIndex[peerID] = reply.ConflictIndex
				} else if n.nextIndex[peerID] > 1 {
					n.nextIndex[peerID]--
				}
				if n.nextIndex[peerID] < 1 {
					n.nextIndex[peerID] = 1
				}
			}
		}(peer, prevIndex, prevTerm, entries)
	}
	n.mu.Unlock()
}

func (n *Node) checkCommitLocked() {
	for idx := n.lastLogIndexLocked(); idx > n.commitIndex; idx-- {
		if n.termForIndexLocked(idx) != n.currentTerm {
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
			for _, e := range toApply {
				select {
				case n.applyCh <- e:
				default:
					go func(entry LogEntry) { n.applyCh <- entry }(e)
				}
			}
			break
		}
	}
}

func (n *Node) applyCommittedLocked() []LogEntry {
	var toApply []LogEntry
	for n.lastApplied < n.commitIndex && n.lastApplied < n.lastLogIndexLocked() {
		n.lastApplied++
		offset := n.lastApplied - n.lastIncludedIndex - 1
		if offset >= 0 && offset < len(n.log) {
			toApply = append(toApply, n.log[offset])
		}
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
		n.stepDownLocked(args.Term)
	}

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID

	// Election restriction: candidate's log must be at least as up to date as voter's log.
	myLastTerm := n.lastLogTermLocked()
	myLastIndex := n.lastLogIndexLocked()

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
		return &HeartbeatReply{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndexLocked()}
	}

	wasLeaderOrCandidate := n.state != Follower
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	} else if wasLeaderOrCandidate {
		n.stepDownLocked(n.currentTerm)
	}
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()

	// If PrevLogIndex is behind our compacted snapshot, reject and inform leader of our snapshot point
	if args.PrevLogIndex < n.lastIncludedIndex {
		return &HeartbeatReply{
			Term:          n.currentTerm,
			Success:       false,
			MatchIndex:    n.lastIncludedIndex,
			ConflictIndex: n.lastIncludedIndex + 1,
		}
	}

	// PrevLog consistency check
	if args.PrevLogIndex > 0 {
		if n.lastLogIndexLocked() < args.PrevLogIndex {
			_ = n.persistLocked()
			return &HeartbeatReply{
				Term:          n.currentTerm,
				Success:       false,
				MatchIndex:    n.lastLogIndexLocked(),
				ConflictTerm:  0,
				ConflictIndex: n.lastLogIndexLocked() + 1,
			}
		}
		if n.termForIndexLocked(args.PrevLogIndex) != args.PrevLogTerm {
			conflictTerm := n.termForIndexLocked(args.PrevLogIndex)
			conflictIndex := args.PrevLogIndex
			for idx := n.lastIncludedIndex + 1; idx <= n.lastLogIndexLocked(); idx++ {
				if n.termForIndexLocked(idx) == conflictTerm {
					conflictIndex = idx
					break
				}
			}
			_ = n.persistLocked()
			return &HeartbeatReply{
				Term:          n.currentTerm,
				Success:       false,
				MatchIndex:    args.PrevLogIndex - 1,
				ConflictTerm:  conflictTerm,
				ConflictIndex: conflictIndex,
			}
		}
	}

	// Insert entries, truncating on conflict
	logChanged := false
	for _, entry := range args.Entries {
		if entry.Index <= n.lastIncludedIndex {
			continue
		}
		offset := entry.Index - n.lastIncludedIndex - 1
		if offset < len(n.log) {
			if n.log[offset].Term != entry.Term {
				n.log = n.log[:offset]
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
		if n.commitIndex > n.lastLogIndexLocked() {
			n.commitIndex = n.lastLogIndexLocked()
		}
		toApply := n.applyCommittedLocked()
		for _, e := range toApply {
			select {
			case n.applyCh <- e:
			default:
				go func(entry LogEntry) { n.applyCh <- entry }(e)
			}
		}
	}

	return &HeartbeatReply{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: n.lastLogIndexLocked(),
	}
}

// Snapshot discards all log entries up to lastIncludedIndex and saves snapshotData.
func (n *Node) Snapshot(lastIncludedIndex int, snapshotData []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if lastIncludedIndex <= n.lastIncludedIndex || lastIncludedIndex > n.commitIndex {
		return nil
	}

	term := n.termForIndexLocked(lastIncludedIndex)
	offset := lastIncludedIndex - n.lastIncludedIndex - 1
	if offset >= 0 && offset < len(n.log) {
		n.log = append([]LogEntry{}, n.log[offset+1:]...)
	} else {
		n.log = nil
	}

	n.lastIncludedIndex = lastIncludedIndex
	n.lastIncludedTerm = term
	n.snapshotData = snapshotData

	return n.persistLocked()
}

// HandleInstallSnapshot installs a snapshot sent by the leader.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}

	wasLeaderOrCandidate := n.state != Follower
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	} else if wasLeaderOrCandidate {
		n.stepDownLocked(n.currentTerm)
	}
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()

	if args.LastIncludedIndex <= n.lastIncludedIndex {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}

	// Truncate existing log
	if args.LastIncludedIndex <= n.lastLogIndexLocked() && n.termForIndexLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		offset := args.LastIncludedIndex - n.lastIncludedIndex - 1
		n.log = append([]LogEntry{}, n.log[offset+1:]...)
	} else {
		n.log = nil
	}

	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm
	n.snapshotData = args.Data

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}

	_ = n.persistLocked()

	// Notify application to restore state machine from snapshot
	snapCopy := append([]byte{}, args.Data...)
	select {
	case n.applySnapshotCh <- snapCopy:
	default:
		go func(data []byte) { n.applySnapshotCh <- data }(snapCopy)
	}

	return &InstallSnapshotReply{Term: n.currentTerm}
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

	entry.Index = n.lastLogIndexLocked() + 1
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
		for _, e := range toApply {
			select {
			case n.applyCh <- e:
			default:
				go func(entry LogEntry) { n.applyCh <- entry }(e)
			}
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
	return n.lastLogIndexLocked()
}

func (n *Node) LastLogTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogTermLocked()
}

func (n *Node) LastIncludedIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIncludedIndex
}

func (n *Node) LastIncludedTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIncludedTerm
}

func (n *Node) SnapshotData() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]byte{}, n.snapshotData...)
}

func (n *Node) Log() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry{}, n.log...)
}
