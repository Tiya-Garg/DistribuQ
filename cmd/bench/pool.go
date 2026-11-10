package main

import (
	"context"
	"fmt"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SchedulerPool manages gRPC connections across multiple scheduler replicas.
type SchedulerPool struct {
	addrs   []string
	clients map[string]pb.SchedulerServiceClient
	conns   map[string]*grpc.ClientConn
}

// NewSchedulerPool connects to all provided scheduler replicas.
func NewSchedulerPool(addrs []string) (*SchedulerPool, error) {
	return NewSchedulerPoolWithTLS(addrs, nil)
}

// NewSchedulerPoolWithTLS connects to all provided scheduler replicas with optional TLS dial options.
func NewSchedulerPoolWithTLS(addrs []string, dialOpt grpc.DialOption) (*SchedulerPool, error) {
	pool := &SchedulerPool{
		addrs:   addrs,
		clients: make(map[string]pb.SchedulerServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}

	if dialOpt == nil {
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, dialOpt)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("dial %s failed: %w", addr, err)
		}
		pool.conns[addr] = conn
		pool.clients[addr] = pb.NewSchedulerServiceClient(conn)
	}

	return pool, nil
}

// Close terminates all client connections in the pool.
func (p *SchedulerPool) Close() {
	for _, conn := range p.conns {
		_ = conn.Close()
	}
}

// FindLeader queries replicas to determine which scheduler is currently the Raft leader.
func (p *SchedulerPool) FindLeader(ctx context.Context) (string, pb.SchedulerServiceClient, error) {
	for addr, client := range p.clients {
		resp, err := client.GetClusterMembership(ctx, &pb.ClusterMembershipRequest{})
		if err == nil && resp.LeaderId != "" {
			return addr, client, nil
		}
	}
	// Fall back to first client
	for addr, client := range p.clients {
		return addr, client, nil
	}
	return "", nil, fmt.Errorf("no scheduler replicas available")
}
