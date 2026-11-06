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
	delay := flag.Duration("delay", 0, "optional execution delay before task runs (e.g. 5s, 1m)")
	runAtFlag := flag.String("run-at", "", "optional execution timestamp (RFC3339 format, e.g. 2026-11-04T18:00:00Z)")
	dependsOnFlag := flag.String("depends-on", "", "optional comma-separated list of parent task IDs that must complete first")
	idempotencyKey := flag.String("idempotency-key", "", "optional deduplication key to prevent duplicate executions across retries")
	cancelFlag := flag.Bool("cancel", false, "cancel/abort a task by ID")
	replayDLQ := flag.Bool("replay-dlq", false, "replay task from DLQ back into active queue")
	purgeDLQ := flag.Bool("purge-dlq", false, "purge task from DLQ")
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

	var runAtUnixMs int64
	if *delay > 0 {
		runAtUnixMs = time.Now().Add(*delay).UnixMilli()
	} else if *runAtFlag != "" {
		t, err := time.Parse(time.RFC3339, *runAtFlag)
		if err != nil {
			log.Fatalf("invalid -run-at format: %v (expected RFC3339)", err)
		}
		runAtUnixMs = t.UnixMilli()
	}

	var dependsOn []string
	if *dependsOnFlag != "" {
		for _, dep := range strings.Split(*dependsOnFlag, ",") {
			dep = strings.TrimSpace(dep)
			if dep != "" {
				dependsOn = append(dependsOn, dep)
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

		var callErr error
		if *cancelFlag {
			resp, cErr := client.CancelTask(ctx, &pb.CancelTaskRequest{TaskId: *id})
			callErr = cErr
			if callErr == nil {
				log.Printf("canceled task %s via %s: ok=%v", *id, addr, resp.Ok)
			}
		} else if *replayDLQ {
			resp, rErr := client.ReplayDLQ(ctx, &pb.ReplayDLQRequest{TaskId: *id})
			callErr = rErr
			if callErr == nil {
				log.Printf("replayed DLQ task %s via %s: ok=%v", *id, addr, resp.Ok)
			}
		} else if *purgeDLQ {
			resp, pErr := client.PurgeDLQ(ctx, &pb.PurgeDLQRequest{TaskId: *id})
			callErr = pErr
			if callErr == nil {
				log.Printf("purged DLQ task %s via %s: ok=%v", *id, addr, resp.Ok)
			}
		} else {
			resp, sErr := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
				TaskId:         *id,
				Payload:        *payload,
				Priority:       priority,
				RequiredTag:    *tagFlag,
				RunAtUnixMs:    runAtUnixMs,
				DependsOn:      dependsOn,
				IdempotencyKey: *idempotencyKey,
			})
			callErr = sErr
			if callErr == nil {
				if resp.TaskId != "" && resp.TaskId != *id {
					log.Printf("task deduplicated (idempotency key %q matched original task %s) on %s", *idempotencyKey, resp.TaskId, addr)
				} else {
					log.Printf("submitted task %s to %s: ok=%v", *id, addr, resp.Ok)
				}
			}
		}
		cancel()
		conn.Close()

		if callErr == nil {
			return
		}

		lastErr = callErr
		if st, ok := status.FromError(callErr); ok {
			if st.Code() == codes.AlreadyExists {
				log.Fatalf("idempotency conflict: %s (task submission rejected)", st.Message())
			}
			if st.Code() == codes.InvalidArgument {
				log.Fatalf("invalid task argument: %s", st.Message())
			}
			if st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), scheduler.ErrNotLeader.Error()) {
				log.Printf("%s is not the leader, trying next", addr)
				continue
			}
		}
		log.Printf("operation on %s failed: %v, trying next", addr, callErr)
	}
	log.Fatalf("operation failed against all %d address(es): %v", len(addrs), lastErr)
}
