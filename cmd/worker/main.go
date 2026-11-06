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

	"github.com/Tiya-Garg/DistribuQ/internal/security"
	"github.com/Tiya-Garg/DistribuQ/internal/worker"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "unique ID for this worker")
	addr := flag.String("addr", ":9000", "address to listen on for RPC")
	schedulerFlag := flag.String("scheduler", "localhost:8000", "scheduler address, or a comma-separated list of every scheduler replica so the worker survives a leader change")
	heartbeatInterval := flag.Duration("heartbeat-interval", 1*time.Second, "interval between heartbeat reports to schedulers")
	timeout := flag.Duration("timeout", worker.DefaultExecTimeout, "max duration a single task's command may run before being killed")
	tagsFlag := flag.String("tags", "", "comma-separated capability tags (e.g. gpu,highmem)")
	maxConcurrent := flag.Int("max-concurrent", 0, "max concurrent task executions (0 = uncapped)")
	caCert := flag.String("ca-cert", "", "path to CA root certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "path to worker TLS certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "path to worker TLS private key for mTLS")
	serverName := flag.String("server-name", "localhost", "expected scheduler TLS server name (SAN) for dial verification")
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

	tlsCfg := &security.TLSConfig{
		CACertPath: *caCert,
		CertPath:   *tlsCert,
		KeyPath:    *tlsKey,
		ServerName: *serverName,
	}

	dialOpt, err := tlsCfg.DialOption()
	if err != nil {
		log.Fatalf("failed to configure TLS dial options: %v", err)
	}

	w := &worker.Worker{
		ID:             *id,
		SchedulerAddr:  schedulerAddrs[0],
		SchedulerAddrs: schedulerAddrs,
		Addr:           *addr,
		Tags:           tags,
		MaxConcurrent:  *maxConcurrent,
		DialOpt:        dialOpt,
		Execute:        worker.NewShellExecutor(*id, *timeout),
	}

	var srvOpts []grpc.ServerOption
	if tlsCfg.IsEnabled() {
		srvOpt, err := tlsCfg.ServerOption()
		if err != nil {
			log.Fatalf("failed to configure TLS server option: %v", err)
		}
		srvOpts = append(srvOpts, srvOpt)
		log.Printf("[%s] mTLS enabled for worker service (CA: %s)", *id, *caCert)
	}

	server := grpc.NewServer(srvOpts...)
	pb.RegisterWorkerServiceServer(server, worker.NewGRPCWorkerServer(w))

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] worker listening on %s (tags=%v, max-concurrent=%d), reporting to scheduler(s) %v", *id, *addr, tags, *maxConcurrent, schedulerAddrs)

	stopCh := make(chan struct{})
	go w.StartHeartbeatLoop(*heartbeatInterval, stopCh)

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
