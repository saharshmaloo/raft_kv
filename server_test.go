package raft

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

func TestServerLifecycle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(1, []int{2})
	if !s.cm.electionDeadline.IsZero() {
		t.Fatal("election timer started before Serve")
	}
	if err := s.Serve(listener); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	defer s.Shutdown()
	s.cm.mu.Lock()
	timerStarted := !s.cm.electionDeadline.IsZero()
	s.cm.mu.Unlock()
	if !timerStarted {
		t.Fatal("Serve did not initialize the election timer")
	}
	peer := NewServer(2, []int{1})
	defer peer.Shutdown()
	if err := peer.ConnectToPeer(1, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	s.Shutdown()
	s.Shutdown()
	select {
	case <-s.cm.done:
	default:
		t.Fatal("Shutdown did not signal the election timer to stop")
	}
	if err := s.Call(2, "ConsensusModule.RequestVote", nil, nil); err == nil {
		t.Fatal("closed server accepted a call")
	}
}

func TestElectionRequestsVotesOverRPC(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	t.Cleanup(s.Shutdown)
	peers := []*Server{NewServer(2, nil), NewServer(3, nil)}
	for _, peer := range peers {
		peer := peer // This module targets Go 1.21's shared range variables.
		// In-memory connections exercise the real RPC service without timers
		// or sleeps. Each peer starts with an empty log and no prior vote.
		local, remote := net.Pipe()
		s.peers[peer.cm.id] = rpc.NewClient(local)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			peer.rpcServer.ServeConn(remote)
		}()
		t.Cleanup(func() {
			local.Close()
			remote.Close()
			<-finished
		})
	}
	s.cm.tickElection(time.Unix(0, 0))
	s.cm.wg.Wait() // Both outgoing calls and their reply handlers have completed.
	if s.cm.state != Leader || s.cm.currentTerm != 1 {
		t.Fatal("RPC votes did not elect the candidate")
	}
	for _, peer := range peers {
		peer.cm.mu.Lock()
		votedFor, term := peer.cm.votedFor, peer.cm.currentTerm
		peer.cm.mu.Unlock()
		if votedFor != 1 || term != 1 {
			t.Fatal("candidate did not request a vote from every peer")
		}
	}
}

func TestFailedVoteRPCLeavesCandidate(t *testing.T) {
	s := NewServer(1, []int{2, 3})
	defer s.Shutdown()
	s.cm.tickElection(time.Unix(0, 0))
	s.cm.wg.Wait()
	if s.cm.state != Candidate || len(s.cm.votesReceived) != 1 {
		t.Fatal("disconnected peers must not count as votes")
	}
}

func TestConsensusModuleCopiesPeerIDs(t *testing.T) {
	ids := []int{2, 3}
	s := NewServer(1, ids)
	ids[0] = 99
	if s.cm.id != 1 || s.cm.peerIds[0] != 2 || s.cm.server != s {
		t.Fatal("consensus module identity or containing server is incorrect")
	}
	if err := s.Call(2, "ConsensusModule.RequestVote", nil, nil); err == nil {
		t.Fatal("call to disconnected peer succeeded")
	}
}

func TestWinningVoteSendsImmediateHeartbeat(t *testing.T) {
	s := NewServer(1, []int{2})
	t.Cleanup(s.Shutdown)
	peer := NewServer(2, []int{1})
	t.Cleanup(peer.Shutdown)
	local, remote := net.Pipe()
	s.peers[2] = rpc.NewClient(local)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		peer.rpcServer.ServeConn(remote)
	}()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
		<-finished
	})
	s.cm.state, s.cm.currentTerm = Candidate, 1
	s.cm.votesReceived = map[int]bool{1: true}
	peer.cm.state, peer.cm.currentTerm, peer.cm.votedFor = Candidate, 1, 2
	s.cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	s.cm.wg.Wait()
	peer.cm.mu.Lock()
	defer peer.cm.mu.Unlock()
	if peer.cm.state != Follower || peer.cm.votedFor != 2 || peer.cm.electionDeadline.IsZero() {
		t.Fatal("winning vote did not immediately deliver a heartbeat preserving the peer's vote")
	}
}

func TestShutdownUnblocksStalledHeartbeat(t *testing.T) {
	s := NewServer(1, []int{2})
	t.Cleanup(s.Shutdown)
	local, remote := net.Pipe()
	defer remote.Close()
	s.peers[2] = rpc.NewClient(local)
	// With no reader on the other end, sending the request blocks until shutdown.
	s.cm.sendHeartbeat(2, AppendEntriesArgs{Term: 1, LeaderID: 1})
	finished := make(chan struct{})
	go func() {
		s.Shutdown()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		remote.Close()
		t.Fatal("shutdown did not unblock the heartbeat RPC")
	}
}

func TestTCPHeartbeatsKeepLeaderStableAndAllowFailover(t *testing.T) {
	servers := make([]*Server, 3)
	addresses := make([]string, 3)
	for i := range servers {
		var peers []int
		for j := range servers {
			if i != j {
				peers = append(peers, j+1)
			}
		}
		s := NewServer(i+1, peers)
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
	state := func(s *Server) (CMState, int) {
		s.cm.mu.Lock()
		defer s.cm.mu.Unlock()
		return s.cm.state, s.cm.currentTerm
	}
	waitForLeader := func(stopped *Server, minTerm int) (*Server, int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, s := range servers {
				if s == stopped {
					continue
				}
				role, term := state(s)
				if role != Leader || term < minTerm {
					continue
				}
				converged := true
				for _, peer := range servers {
					if peer != s && peer != stopped {
						peerRole, peerTerm := state(peer)
						converged = converged && peerRole == Follower && peerTerm == term
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
			wantRole := Follower
			if s == leader {
				wantRole = Leader
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
