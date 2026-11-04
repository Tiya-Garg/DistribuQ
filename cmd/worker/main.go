// Command worker runs a single worker node: it registers itself with
// the scheduler, sends periodic heartbeats, and executes tasks pushed
// to it over RPC. Each task's payload is run as a shell command (see
// internal/worker.NewShellExecutor).
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	tagsFlag := flag.String("tags", "", "comma-separated capability tags (e.g. gpu,highmem)")
	maxConcurrent := flag.Int("max-concurrent", 0, "max concurrent task executions (0 = uncapped)")
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

	var tags []string
	if *tagsFlag != "" {
		for _, t := range strings.Split(*tagsFlag, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tags = append(tags, t)
			}
		}
	}

	w := &worker.Worker{
		ID:             *id,
		SchedulerAddr:  schedulerAddrs[0],
		SchedulerAddrs: schedulerAddrs,
		Addr:           *addr,
		Tags:           tags,
		MaxConcurrent:  *maxConcurrent,
		Execute:        worker.NewShellExecutor(*id, *timeout),
	}

	server := grpc.NewServer()
	pb.RegisterWorkerServiceServer(server, worker.NewGRPCWorkerServer(w))

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] worker listening on %s (tags=%v, max-concurrent=%d), reporting to scheduler(s) %v", *id, *addr, tags, *maxConcurrent, schedulerAddrs)

	stopCh := make(chan struct{})
	go w.StartHeartbeatLoop(1*time.Second, stopCh)

	// Intercept SIGINT/SIGTERM for graceful drain
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[%s] received shutdown signal, draining worker...", *id)
		if err := w.Drain(); err != nil {
			log.Printf("[%s] drain notification error: %v", *id, err)
		}
		close(stopCh)
		server.GracefulStop()
		os.Exit(0)
	}()

	// If the scheduler isn't reachable yet, that's fine: the heartbeat
	// loop registers this worker as soon as the scheduler answers.
	if err := w.Register(); err != nil {
		log.Printf("worker %s: initial registration failed, will retry via heartbeats: %v", *id, err)
	}

	if err := server.Serve(listener); err != nil {
		log.Fatalf("worker server exited: %v", err)
	}
}
