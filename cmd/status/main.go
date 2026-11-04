// Command status queries a scheduler replica for its current view of
// task states and worker count.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/security"
	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("scheduler", "localhost:8000", "scheduler replica address to query")
	showDLQ := flag.Bool("dlq", false, "display only Dead Letter Queue (DLQ) tasks with error reasons")
	caCert := flag.String("ca-cert", "", "path to CA root certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "path to client TLS certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "path to client TLS private key for mTLS")
	serverName := flag.String("server-name", "localhost", "expected scheduler TLS server name (SAN) for dial verification")
	flag.Parse()

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

	conn, err := grpc.NewClient(*addr, dialOpt)
	if err != nil {
		log.Fatalf("dial %s failed: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if *showDLQ {
		dlqReply, err := client.GetDLQ(ctx, &pb.GetDLQRequest{})
		if err != nil {
			log.Fatalf("dlq query failed: %v", err)
		}
		if len(dlqReply.Tasks) == 0 {
			fmt.Println("dlq: (empty)")
			return
		}
		fmt.Printf("dlq tasks (%d):\n", len(dlqReply.Tasks))
		for _, t := range dlqReply.Tasks {
			fmt.Printf("  %-20s attempts=%d/%d priority=%d error=%q\n", t.Id, t.Attempts, t.MaxRetries, t.Priority, t.LastError)
		}
		return
	}

	reply, err := client.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		log.Fatalf("status query failed: %v", err)
	}

	fmt.Printf("workers: %d\n", reply.WorkerCount)
	if len(reply.Tasks) == 0 {
		fmt.Println("tasks: (none)")
		return
	}

	ids := make([]string, 0, len(reply.Tasks))
	for id := range reply.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Println("tasks:")
	for _, id := range ids {
		fmt.Printf("  %-20s %s\n", id, reply.Tasks[id])
	}
}
