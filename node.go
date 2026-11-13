package raftkv

import (
	"net"
	"sync"

	"github.com/saharshmaloo/raft_kv/raft"
	"github.com/saharshmaloo/raft_kv/transport"
)

// Node wires consensus to its RPC transport and coordinates their lifecycle.
type Node struct {
	mu        sync.Mutex
	consensus *raft.ConsensusModule
	server    *transport.Server
}

// NewNode constructs a node without opening a listener or starting elections.
func NewNode(id int, peerIDs []int) *Node {
	server := transport.NewServer()
	consensus := raft.NewConsensusModule(id, peerIDs, server)
	if err := server.Register("ConsensusModule", consensus); err != nil {
		panic(err) // The fixed receiver and service name must be valid together.
	}
	return &Node{consensus: consensus, server: server}
}

// Serve takes ownership of listener and starts RPC serving and consensus.
func (n *Node) Serve(listener net.Listener) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.server.Serve(listener); err != nil {
		return err
	}
	n.consensus.Start()
	return nil
}

// ConnectToPeer establishes or replaces an outgoing peer connection.
func (n *Node) ConnectToPeer(peerID int, address string) error {
	return n.server.ConnectToPeer(peerID, address)
}

// DisconnectPeer closes an outgoing connection without changing membership.
func (n *Node) DisconnectPeer(peerID int) {
	n.server.DisconnectPeer(peerID)
}

// Status returns a snapshot of this node's consensus state.
func (n *Node) Status() raft.Status {
	return n.consensus.Status()
}

// Shutdown closes networking before stopping consensus so pending RPCs unblock.
// It waits for both components and is safe to call more than once.
func (n *Node) Shutdown() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.server.Shutdown()
	n.consensus.Stop()
}
