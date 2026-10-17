package raft

// ConsensusModule holds the Raft protocol logic for one replica.
type ConsensusModule struct {
	id      int
	peerIds []int
	server  *Server
}

func newConsensusModule(id int, peerIds []int, server *Server) *ConsensusModule {
	return &ConsensusModule{
		id:      id,
		peerIds: append([]int(nil), peerIds...),
		server:  server,
	}
}
