package raft

import (
	"testing"
	"time"
)

func BenchmarkRaftProposeSingleNode(b *testing.B) {
	ft := newFakeTransport()
	node := NewNode("node-1", nil, ft)
	node.state = Leader
	node.currentTerm = 1
	ft.register(node)

	go node.Run()
	defer node.Stop()

	entry := LogEntry{
		Type:     CmdSubmit,
		TaskID:   "bench-task-1",
		Payload:  "echo hello",
		Priority: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Index = 0
		_, _, ch, err := node.Propose(entry)
		if err != nil {
			b.Fatalf("Propose failed: %v", err)
		}
		if ok := <-ch; !ok {
			b.Fatalf("entry was not committed")
		}
	}
}

func BenchmarkRaftReplicatedCluster(b *testing.B) {
	_, nodes := newCluster(nil, []string{"b1", "b2", "b3"})
	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.State() == Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader == nil {
		b.Fatalf("no leader elected in benchmark cluster")
	}

	entry := LogEntry{
		Type:     CmdSubmit,
		TaskID:   "replicated-bench",
		Payload:  "echo replicated",
		Priority: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, ch, err := leader.Propose(entry)
		if err != nil {
			b.Fatalf("Propose failed: %v", err)
		}
		if ok := <-ch; !ok {
			b.Fatalf("replicated entry was not committed")
		}
	}
}

func BenchmarkRaftSnapshotCompaction(b *testing.B) {
	snapData := []byte("benchmark-snapshot-payload-12345")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		n := NewNode("bench-snap", nil, newFakeTransport())
		n.state = Leader
		n.currentTerm = 1
		for j := 1; j <= 100; j++ {
			n.log = append(n.log, LogEntry{Index: j, Term: 1, TaskID: "t"})
		}
		n.commitIndex = 100
		b.StartTimer()

		if err := n.Snapshot(75, snapData); err != nil {
			b.Fatalf("Snapshot failed: %v", err)
		}
	}
}

func BenchmarkRaftHandleHeartbeat(b *testing.B) {
	n := NewNode("follower-bench", nil, newFakeTransport())
	n.state = Follower
	n.currentTerm = 2
	n.lastIncludedIndex = 0

	args := &HeartbeatArgs{
		Term:         2,
		LeaderID:     "leader-bench",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 2, Type: CmdSubmit, TaskID: "hb-t1"},
		},
		LeaderCommit: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reply := n.HandleHeartbeat(args)
		if !reply.Success {
			b.Fatalf("HandleHeartbeat failed: %+v", reply)
		}
	}
}
