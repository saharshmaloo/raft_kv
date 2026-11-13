package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"testing"
)

// Test clients are installed before protocol work begins and closed after it.
type rpcTestTransport struct {
	clients map[int]*rpc.Client
}

// Missing clients model unreachable peers; connected clients use real RPC encoding.
func (transport *rpcTestTransport) Call(peerID int, method string, args, reply any) error {
	client := transport.clients[peerID]
	if client == nil {
		return fmt.Errorf("peer %d disconnected", peerID)
	}
	return client.Call(method, args, reply)
}

// Wire a peer through an in-memory RPC connection and register cleanup for both ends.
func connectTestPeer(t *testing.T, transport *rpcTestTransport, peer *ConsensusModule) {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("ConsensusModule", peer); err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	transport.clients[peer.id] = rpc.NewClient(local)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		server.ServeConn(remote)
	}()
	t.Cleanup(func() {
		transport.clients[peer.id].Close()
		remote.Close()
		<-finished
	})
}
