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
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	addrsFlag := flag.String("schedulers", "localhost:8000", "comma-separated scheduler replica addresses to try")
	id := flag.String("id", "", "task id (required)")
	payload := flag.String("payload", "hello", "task payload")
	flag.Parse()

	if *id == "" {
		log.Fatal("submit: -id is required")
	}

	addrs := strings.Split(*addrsFlag, ",")

	var lastErr error
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = err
			log.Printf("dial %s failed: %v, trying next", addr, err)
			continue
		}

		client := pb.NewSchedulerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, callErr := client.SubmitTask(ctx, &pb.SubmitTaskRequest{
			TaskId:  *id,
			Payload: *payload,
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
