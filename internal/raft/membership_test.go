package raft

import (
	"testing"
	"time"
)

func TestDynamicMembershipAddAndRemovePeer(t *testing.T) {
	ft, nodes := newCluster(t, []string{"n1", "n2"})

	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	var leader *Node
	waitForCondition(t, 3*time.Second, func() bool {
		for _, n := range nodes {
			if n.State() == Leader {
				leader = n
				return true
			}
		}
		return false
	})

	// Add node n3 dynamically to the cluster
	n3 := NewNode("n3", []string{leader.ID}, ft)
	ft.register(n3)
	go n3.Run()
	defer n3.Stop()

	commitCh, err := leader.AddPeer("n3", "localhost:9003")
	if err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}

	select {
	case ok := <-commitCh:
		if !ok {
			t.Fatalf("expected AddPeer to commit")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("AddPeer timed out")
	}

	// Verify n3 is now in leader's active peers
	_, peers := leader.ClusterMembership()
	found := false
	for _, p := range peers {
		if p == "n3" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected n3 to be present in peers: %v", peers)
	}

	// Remove node n2 from the cluster dynamically
	removeCh, err := leader.RemovePeer("n2")
	if err != nil {
		t.Fatalf("RemovePeer failed: %v", err)
	}

	select {
	case ok := <-removeCh:
		if !ok {
			t.Fatalf("expected RemovePeer to commit")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("RemovePeer timed out")
	}

	_, peersAfter := leader.ClusterMembership()
	for _, p := range peersAfter {
		if p == "n2" {
			t.Fatalf("expected n2 to be removed from peers: %v", peersAfter)
		}
	}
}
