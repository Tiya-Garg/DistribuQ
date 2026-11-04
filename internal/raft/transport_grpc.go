package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements Transport using gRPC.
type GRPCTransport struct {
	mu        sync.Mutex
	peerAddrs map[string]string
	conns     map[string]*grpc.ClientConn
	clients   map[string]pb.RaftServiceClient
	dialOpt   grpc.DialOption
}

// NewGRPCTransport returns a Transport backed by gRPC.
func NewGRPCTransport(peerAddrs map[string]string) *GRPCTransport {
	return NewGRPCTransportWithDialOpt(peerAddrs, nil)
}

// NewGRPCTransportWithDialOpt returns a Transport backed by gRPC using a custom dial option (e.g. mTLS).
func NewGRPCTransportWithDialOpt(peerAddrs map[string]string, dialOpt grpc.DialOption) *GRPCTransport {
	if dialOpt == nil {
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	return &GRPCTransport{
		peerAddrs: peerAddrs,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]pb.RaftServiceClient),
		dialOpt:   dialOpt,
	}
}

func (t *GRPCTransport) client(peerID string) (pb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[peerID]; ok {
		return c, nil
	}

	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("raft: unknown peer %q", peerID)
	}

	conn, err := grpc.NewClient(addr, t.dialOpt)
	if err != nil {
		return nil, err
	}

	c := pb.NewRaftServiceClient(conn)
	t.conns[peerID] = conn
	t.clients[peerID] = c
	return c, nil
}

func (t *GRPCTransport) invalidate(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peerID]; ok {
		_ = conn.Close()
		delete(t.conns, peerID)
		delete(t.clients, peerID)
	}
}

// SetPeerAddr registers or updates the dial address for a peer (supporting dynamic membership).
func (t *GRPCTransport) SetPeerAddr(peerID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if oldAddr, ok := t.peerAddrs[peerID]; ok && oldAddr == addr {
		return
	}
	t.peerAddrs[peerID] = addr
	if conn, ok := t.conns[peerID]; ok {
		_ = conn.Close()
		delete(t.conns, peerID)
		delete(t.clients, peerID)
	}
}

// RemovePeer removes a peer from the transport configuration and closes its connection.
func (t *GRPCTransport) RemovePeer(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peerAddrs, peerID)
	if conn, ok := t.conns[peerID]; ok {
		_ = conn.Close()
		delete(t.conns, peerID)
		delete(t.clients, peerID)
	}
}

// Close closes all open peer connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]pb.RaftServiceClient)
}

func toPBLogEntries(entries []LogEntry) []*pb.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	res := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		res[i] = &pb.LogEntry{
			Index:       int64(e.Index),
			Term:        int64(e.Term),
			Type:        string(e.Type),
			TaskId:      e.TaskID,
			Payload:     e.Payload,
			WorkerId:    e.WorkerID,
			Cause:       e.Cause,
			Priority:    int32(e.Priority),
			RequiredTag: e.RequiredTag,
		}
	}
	return res
}

func fromPBLogEntries(entries []*pb.LogEntry) []LogEntry {
	if len(entries) == 0 {
		return nil
	}
	res := make([]LogEntry, len(entries))
	for i, e := range entries {
		res[i] = LogEntry{
			Index:       int(e.Index),
			Term:        int(e.Term),
			Type:        CommandType(e.Type),
			TaskID:      e.TaskId,
			Payload:     e.Payload,
			WorkerID:    e.WorkerId,
			Cause:       e.Cause,
			Priority:    int(e.Priority),
			RequiredTag: e.RequiredTag,
		}
	}
	return res
}

func (t *GRPCTransport) RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  args.CandidateID,
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	})
	if err != nil {
		t.invalidate(peerID)
		return nil, err
	}
	return &RequestVoteReply{
		Term:        int(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (t *GRPCTransport) Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	resp, err := c.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         int64(args.Term),
		LeaderId:     args.LeaderID,
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      toPBLogEntries(args.Entries),
		LeaderCommit: int64(args.LeaderCommit),
	})
	if err != nil {
		t.invalidate(peerID)
		return nil, err
	}
	return &HeartbeatReply{
		Term:          int(resp.Term),
		Success:       resp.Success,
		MatchIndex:    int(resp.MatchIndex),
		ConflictTerm:  int(resp.ConflictTerm),
		ConflictIndex: int(resp.ConflictIndex),
	}, nil
}

func (t *GRPCTransport) InstallSnapshot(peerID string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := c.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term:              int64(args.Term),
		LeaderId:          args.LeaderID,
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Data:              args.Data,
	})
	if err != nil {
		t.invalidate(peerID)
		return nil, err
	}
	return &InstallSnapshotReply{
		Term: int(resp.Term),
	}, nil
}

// GRPCRaftServer implements pb.RaftServiceServer and forwards calls to the underlying Node.
type GRPCRaftServer struct {
	pb.UnimplementedRaftServiceServer
	Node *Node
}

func (s *GRPCRaftServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	args := &RequestVoteArgs{
		Term:         int(req.Term),
		CandidateID:  req.CandidateId,
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}
	reply := s.Node.HandleRequestVote(args)
	return &pb.RequestVoteResponse{
		Term:        int64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *GRPCRaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	args := &HeartbeatArgs{
		Term:         int(req.Term),
		LeaderID:     req.LeaderId,
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      fromPBLogEntries(req.Entries),
		LeaderCommit: int(req.LeaderCommit),
	}
	reply := s.Node.HandleHeartbeat(args)
	return &pb.AppendEntriesResponse{
		Term:          int64(reply.Term),
		Success:       reply.Success,
		MatchIndex:    int64(reply.MatchIndex),
		ConflictTerm:  int64(reply.ConflictTerm),
		ConflictIndex: int64(reply.ConflictIndex),
	}, nil
}

func (s *GRPCRaftServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	args := &InstallSnapshotArgs{
		Term:              int(req.Term),
		LeaderID:          req.LeaderId,
		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),
		Data:              req.Data,
	}
	reply := s.Node.HandleInstallSnapshot(args)
	return &pb.InstallSnapshotResponse{
		Term: int64(reply.Term),
	}, nil
}
