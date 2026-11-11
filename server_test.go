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
	case <-s.done:
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
	s.wg.Wait() // Both outgoing calls and their reply handlers have completed.
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
	s.wg.Wait()
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
