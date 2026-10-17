package raft

import (
	"net"
	"testing"
)

func TestServerLifecycle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(1, []int{2})
	if err := s.Serve(listener); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	defer s.Shutdown()
	peer := NewServer(2, []int{1})
	defer peer.Shutdown()
	if err := peer.ConnectToPeer(1, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	s.Shutdown()
	s.Shutdown()
	if err := s.Call(2, "ConsensusModule.RequestVote", nil, nil); err == nil {
		t.Fatal("closed server accepted a call")
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
