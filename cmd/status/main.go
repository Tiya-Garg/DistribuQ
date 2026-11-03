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

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("scheduler", "localhost:8000", "scheduler replica address to query")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s failed: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

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
