// Command scheduler runs one replica of the DistribuQ scheduler. Multiple
// replicas form a cluster; exactly one becomes leader (via the raft
// package) and accepts task submissions, while followers stand by to
// take over if the leader crashes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "", "unique ID for this scheduler replica")
	addr := flag.String("addr", ":8000", "address to listen on for RPC")
	peersFlag := flag.String("peers", "", "comma-separated id=addr pairs for other scheduler replicas")
	dataDir := flag.String("data-dir", "", "directory for durable Raft state (term and vote); empty keeps state in memory only")
	flag.Parse()

	if *id == "" {
		log.Fatal("scheduler: -id is required")
	}

	peerAddrs := parsePeers(*peersFlag)
	var peerIDs []string
	for pid := range peerAddrs {
		peerIDs = append(peerIDs, pid)
	}

	dispatch := func(workerID, workerAddr string, task *scheduler.Task) error {
		conn, err := grpc.NewClient(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		client := pb.NewWorkerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.ExecuteTask(ctx, &pb.ExecuteTaskRequest{
			TaskId:  task.ID,
			Payload: task.Payload,
		})
		if err != nil {
			return err
		}
		if !resp.Accepted {
			return fmt.Errorf("worker rejected task")
		}
		return nil
	}

	sched := scheduler.NewScheduler(dispatch)
	go sched.MonitorWorkers(500 * time.Millisecond)

	transport := raft.NewGRPCTransport(peerAddrs)
	node := raft.NewNode(*id, peerIDs, transport)
	sched.SetRaftProposer(node)
	node.OnApply(func(entry raft.LogEntry) {
		sched.ApplyEntry(entry)
		if node.State() == raft.Leader && entry.Type == raft.CmdSubmit {
			sched.KickDispatch()
		}
	})
	if *dataDir != "" {
		statePath := filepath.Join(*dataDir, *id+".raft.json")
		if err := node.SetPersister(raft.NewFilePersister(statePath)); err != nil {
			log.Fatalf("failed to restore raft state from %s: %v", statePath, err)
		}
		for _, entry := range node.Log() {
			sched.ApplyEntry(entry)
		}
		log.Printf("[%s] raft state at %s (restored term %d, log len %d)", *id, statePath, node.Term(), node.LastLogIndex())
	}
	node.OnBecomeLeader(func() {
		log.Printf("[%s] elected LEADER (term %d) -- now accepting task submissions", *id, node.Term())
		sched.RebuildQueueOnLeadership()
	})
	node.OnBecomeFollower(func() {
		log.Printf("[%s] stepped down to FOLLOWER (leader is %q)", *id, node.LeaderID())
	})

	server := grpc.NewServer()
	pb.RegisterRaftServiceServer(server, &raft.GRPCRaftServer{Node: node})
	pb.RegisterSchedulerServiceServer(server, &scheduler.GRPCSchedulerServer{
		S:        sched,
		IsLeader: func() bool { return node.State() == raft.Leader },
	})

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] scheduler replica listening on %s", *id, *addr)

	go node.Run()
	if err := server.Serve(listener); err != nil {
		log.Fatalf("scheduler server exited: %v", err)
	}
}

func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	if s == "" {
		return peers
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		peers[parts[0]] = parts[1]
	}
	return peers
}
