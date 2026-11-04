// Command submit is a small CLI client for submitting a task to a
// running scheduler cluster, used for manual testing/demo purposes.
// Since only the current Raft leader accepts submissions (see
// SchedulerService.SubmitTask), this tries each given address in turn
// until one succeeds, rather than requiring the caller to already
// know which replica is leader.
package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
	"github.com/Tiya-Garg/DistribuQ/internal/security"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parsePriority(s string) int32 {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "critical", "3":
		return 3
	case "high", "2":
		return 2
	case "normal", "medium", "1", "":
		return 1
	case "low", "0":
		return 0
	default:
		log.Printf("unknown priority %q, defaulting to normal (1)", s)
		return 1
	}
}

func main() {
	addrsFlag := flag.String("schedulers", "localhost:8000", "comma-separated scheduler replica addresses to try")
	id := flag.String("id", "", "task id (required)")
	payload := flag.String("payload", "hello", "task payload")
	priorityFlag := flag.String("priority", "normal", "task priority: low, normal, high, critical (or 0-3)")
	tagFlag := flag.String("tag", "", "optional required worker capability tag (e.g. gpu, highmem)")
	caCert := flag.String("ca-cert", "", "path to CA root certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "path to client TLS certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "path to client TLS private key for mTLS")
	serverName := flag.String("server-name", "localhost", "expected scheduler TLS server name (SAN) for dial verification")
	flag.Parse()

	if *id == "" {
		log.Fatal("submit: -id is required")
	}

	priority := parsePriority(*priorityFlag)
	addrs := strings.Split(*addrsFlag, ",")

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

	var lastErr error
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		conn, err := grpc.NewClient(addr, dialOpt)
		if err != nil {
			lastErr = err
			log.Printf("dial %s failed: %v, trying next", addr, err)
			continue
		}

		client := pb.NewSchedulerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, callErr := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
			TaskId:      *id,
			Payload:     *payload,
			Priority:    priority,
			RequiredTag: *tagFlag,
		})
		cancel()
		conn.Close()

		if callErr == nil {
			log.Printf("submitted task %s to %s: ok=%v", *id, addr, resp.Ok)
			return
		}

		lastErr = callErr
		if st, ok := status.FromError(callErr); ok && st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), scheduler.ErrNotLeader.Error()) {
			log.Printf("%s is not the leader, trying next", addr)
			continue
		}
		log.Printf("submit to %s failed: %v, trying next", addr, callErr)
	}
	log.Fatalf("submit failed against all %d address(es): %v", len(addrs), lastErr)
}
