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
	"github.com/Tiya-Garg/DistribuQ/internal/tracing"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
	timeout := flag.Duration("timeout", 2*time.Second, "per-replica RPC call timeout")
	id := flag.String("id", "", "task id (required)")
	payload := flag.String("payload", "hello", "task payload")
	priorityFlag := flag.String("priority", "normal", "task priority: low, normal, high, critical (or 0-3)")
	tagFlag := flag.String("tag", "", "optional required worker capability tag (e.g. gpu, highmem)")
	delay := flag.Duration("delay", 0, "optional execution delay before task runs (e.g. 5s, 1m)")
	runAtFlag := flag.String("run-at", "", "optional execution timestamp (RFC3339 format, e.g. 2026-11-04T18:00:00Z)")
	dependsOnFlag := flag.String("depends-on", "", "optional comma-separated list of parent task IDs that must complete first")
	idempotencyKey := flag.String("idempotency-key", "", "optional deduplication key to prevent duplicate executions across retries")
	subscribeWebhook := flag.String("subscribe-webhook", "", "optional webhook URL to subscribe for task completion notifications")
	webhookSecret := flag.String("webhook-secret", "", "optional shared secret for HMAC-SHA256 signature verification")
	traceparentFlag := flag.String("traceparent", "", "optional W3C traceparent header to propagate distributed tracing context (e.g. 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01)")
	showLogs := flag.Bool("logs", false, "fetch and display recorded stdout/stderr execution output for this task ID")
	cancelFlag := flag.Bool("cancel", false, "cancel/abort a task by ID")
	pipelineFile := flag.String("pipeline", "", "path to workflow pipeline JSON definition file to submit")
	replayDLQ := flag.Bool("replay-dlq", false, "replay task from DLQ back into active queue")
	purgeDLQ := flag.Bool("purge-dlq", false, "purge task from DLQ")
	tokenFlag := flag.String("token", "", "bearer API token for authentication and RBAC authorization")
	caCert := flag.String("ca-cert", "", "path to CA root certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "path to client TLS certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "path to client TLS private key for mTLS")
	serverName := flag.String("server-name", "localhost", "expected scheduler TLS server name (SAN) for dial verification")
	flag.Parse()

	if *id == "" && *pipelineFile == "" {
		log.Fatal("submit: -id (or -pipeline) is required")
	}

	if *pipelineFile != "" {
		addrs := strings.Split(*addrsFlag, ",")
		var lastErr error
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			if err := submitPipelineFile(addr, *pipelineFile, *tokenFlag, *timeout); err == nil {
				return
			} else {
				lastErr = err
				log.Printf("submitting pipeline to %s failed: %v, trying next", addr, err)
			}
		}
		log.Fatalf("failed to submit pipeline across all schedulers: %v", lastErr)
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
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		if *tokenFlag != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*tokenFlag)
		}

		var callErr error
		if *showLogs {
			logs, lErr := FetchTaskLogs(addr, *id, *timeout)
			if lErr == nil {
				log.Printf("retrieved %d log lines for task %s:", len(logs), *id)
				for _, l := range logs {
					stream := "stdout"
					if l.IsStderr {
						stream = "stderr"
					}
					log.Printf("[%s][%s] %s", l.WorkerID, stream, l.Content)
				}
				return
			}
			callErr = lErr
		} else if *cancelFlag {
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
			callCtx := ctx
			if *traceparentFlag != "" {
				if tc, err := tracing.ParseTraceParent(*traceparentFlag); err == nil {
					tracer := tracing.NewTracer("submit-cli")
					_, span := tracer.Start(ctx, "submit-task", tracing.SpanKindClient)
					// Override trace ID & span ID with parsed W3C parent
					spanTrace := span.TraceContext()
					spanTrace.TraceID = tc.TraceID
					spanTrace.SpanID = tc.SpanID
					callCtx = tracing.InjectGRPC(ctx, span)
					defer span.End()
				}
			}
			resp, sErr := client.SubmitTask(callCtx, &pb.SubmitTaskRequest{
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
				if *subscribeWebhook != "" {
					registerWebhookSubscription(addr, *subscribeWebhook, *webhookSecret, *timeout)
				}
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
