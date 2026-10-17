package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// Server owns transport resources. ConsensusModule uses Call without knowing
// peer addresses or managing network connections.
type Server struct {
	mu          sync.Mutex
	cm          *ConsensusModule
	rpcServer   *rpc.Server
	listener    net.Listener
	peers       map[int]*rpc.Client
	connections map[net.Conn]struct{}
	closed      bool
	wg          sync.WaitGroup
}

func NewServer(id int, peerIds []int) *Server {
	s := &Server{
		rpcServer:   rpc.NewServer(),
		peers:       make(map[int]*rpc.Client),
		connections: make(map[net.Conn]struct{}),
	}
	s.cm = newConsensusModule(id, peerIds, s)
	// Register the CM with rpcServer once it has Raft RPC handlers.
	return s
}

// Serve takes ownership of listener and starts accepting RPC connections.
// No Raft RPC methods are registered in this initial scaffold.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.listener != nil {
		return fmt.Errorf("server is closed or already serving")
	}
	s.listener = listener
	s.wg.Add(1)
	go s.accept()
	return nil
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.connections[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	s.rpcServer.ServeConn(conn)
	conn.Close()
	s.mu.Lock()
	delete(s.connections, conn)
	s.mu.Unlock()
}

// ConnectToPeer establishes an outgoing connection. Addresses belong here,
// outside the consensus module. Callers may retry after a connection failure.
func (s *Server) ConnectToPeer(peerID int, address string) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		client.Close()
		return fmt.Errorf("server is closed")
	}
	if previous := s.peers[peerID]; previous != nil {
		previous.Close()
	}
	s.peers[peerID] = client
	return nil
}

// Call sends an RPC using a method name such as "ConsensusModule.RequestVote".
// The mutex protects the connection map, not the duration of a network call.
func (s *Server) Call(peerID int, method string, args, reply any) error {
	s.mu.Lock()
	client := s.peers[peerID]
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return fmt.Errorf("server is closed")
	}
	if client == nil {
		return fmt.Errorf("peer %d is not connected", peerID)
	}
	return client.Call(method, args, reply)
}

// Shutdown closes transport resources and waits for incoming connections to
// finish. It is safe to call more than once.
func (s *Server) Shutdown() {
	s.mu.Lock()
	s.closed = true
	if s.listener != nil {
		s.listener.Close()
	}
	for _, client := range s.peers {
		client.Close()
	}
	for conn := range s.connections {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
