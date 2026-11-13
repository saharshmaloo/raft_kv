package integration

import (
	"github.com/saharshmaloo/raft_kv"
	"github.com/saharshmaloo/raft_kv/raft"
	"net"
	"testing"
	"time"
)

// A healthy TCP cluster keeps its leader, then elects a replacement after leader shutdown.
func TestTCPHeartbeatsKeepLeaderStableAndAllowFailover(t *testing.T) {
	servers := make([]*raftkv.Node, 3)
	addresses := make([]string, 3)
	for i := range servers {
		var peers []int
		for j := range servers {
			if i != j {
				peers = append(peers, j+1)
			}
		}
		s := raftkv.NewNode(i+1, peers)
		servers[i] = s
		t.Cleanup(s.Shutdown)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[i] = listener.Addr().String()
		if err := s.Serve(listener); err != nil {
			listener.Close()
			t.Fatal(err)
		}
	}
	for i, s := range servers {
		for j, address := range addresses {
			if i != j {
				if err := s.ConnectToPeer(j+1, address); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	state := func(s *raftkv.Node) (raft.CMState, int) {
		status := s.Status()
		return status.State, status.Term
	}
	waitForLeader := func(stopped *raftkv.Node, minTerm int) (*raftkv.Node, int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, s := range servers {
				if s == stopped {
					continue
				}
				role, term := state(s)
				if role != raft.Leader || term < minTerm {
					continue
				}
				converged := true
				for _, peer := range servers {
					if peer != s && peer != stopped {
						peerRole, peerTerm := state(peer)
						converged = converged && peerRole == raft.Follower && peerTerm == term
					}
				}
				if converged {
					return s, term
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("cluster did not converge on a leader")
		return nil, 0
	}
	leader, term := waitForLeader(nil, 1)
	// Observe more than two maximum election timeouts, so voting alone cannot
	// keep this assertion passing without periodic leader contact.
	until := time.Now().Add(650 * time.Millisecond)
	for time.Now().Before(until) {
		for _, s := range servers {
			role, gotTerm := state(s)
			wantRole := raft.Follower
			if s == leader {
				wantRole = raft.Leader
			}
			if role != wantRole || gotTerm != term {
				t.Fatal("healthy cluster changed leadership despite heartbeats")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	leader.Shutdown()
	waitForLeader(leader, term+1)
}
