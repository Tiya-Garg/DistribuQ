// Command worker runs a single worker node: it registers itself with
// the scheduler, sends periodic heartbeats, and executes tasks pushed
// to it over RPC. Each task's payload is run as a shell command (see
// internal/worker.NewShellExecutor).
package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/worker"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "unique ID for this worker")
	addr := flag.String("addr", ":9000", "address to listen on for RPC")
	schedulerFlag := flag.String("scheduler", "localhost:8000", "scheduler address, or a comma-separated list of every scheduler replica so the worker survives a leader change")
	timeout := flag.Duration("timeout", worker.DefaultExecTimeout, "max duration a single task's command may run before being killed")
	flag.Parse()

	if *id == "" {
		log.Fatal("worker: -id is required")
	}

	var schedulerAddrs []string
	for _, a := range strings.Split(*schedulerFlag, ",") {
		if a = strings.TrimSpace(a); a != "" {
			schedulerAddrs = append(schedulerAddrs, a)
		}
	}
	if len(schedulerAddrs) == 0 {
		log.Fatal("worker: -scheduler needs at least one address")
	}

	w := &worker.Worker{
		ID:             *id,
		SchedulerAddr:  schedulerAddrs[0],
		SchedulerAddrs: schedulerAddrs,
		Addr:           *addr,
		Execute:        worker.NewShellExecutor(*id, *timeout),
	}

	server := grpc.NewServer()
	pb.RegisterWorkerServiceServer(server, worker.NewGRPCWorkerServer(w))

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] worker listening on %s, reporting to scheduler(s) %v", *id, *addr, schedulerAddrs)

	stopCh := make(chan struct{})
	go w.StartHeartbeatLoop(1*time.Second, stopCh)

	// If the scheduler isn't reachable yet, that's fine: the heartbeat
	// loop registers this worker as soon as the scheduler answers.
	if err := w.Register(); err != nil {
		log.Printf("worker %s: initial registration failed, will retry via heartbeats: %v", *id, err)
	}

	if err := server.Serve(listener); err != nil {
		log.Fatalf("worker server exited: %v", err)
	}
}
