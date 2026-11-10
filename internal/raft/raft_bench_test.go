package raft

import (
	"testing"
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
