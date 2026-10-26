package raft

import (
	"math/rand"
	"sync"
	"time"
)

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
)

// ConsensusModule holds the Raft protocol logic for one replica.
type ConsensusModule struct {
	mu      sync.Mutex
	id      int
	peerIds []int
	server  *Server

	// Raft requires term and vote changes to be persisted before RPC responses.
	currentTerm int
	votedFor    int // -1 means no vote in the current term.

	// Volatile state. mu protects this state and the term/vote above.
	state            CMState
	electionDeadline time.Time
	electionTimeout  func() time.Duration
}

func newConsensusModule(id int, peerIds []int, server *Server) *ConsensusModule {
	return &ConsensusModule{
		id:       id,
		peerIds:  append([]int(nil), peerIds...),
		server:   server,
		votedFor: -1,
		state:    Follower,
		electionTimeout: func() time.Duration {
			return 150*time.Millisecond + time.Duration(rand.Int63n(int64(150*time.Millisecond)))
		},
	}
}

// resetElectionTimerLocked chooses a fresh timeout on every reset, including
// election retries, so candidates do not repeatedly time out together.
// The caller must hold cm.mu. RPC handlers should reset only for a
// current-term leader's AppendEntries (after adopting a higher term if needed)
// or a granted vote, not for stale RPCs or rejected vote requests (Figure 2).
func (cm *ConsensusModule) resetElectionTimerLocked(now time.Time) {
	cm.electionDeadline = now.Add(cm.electionTimeout())
}

// tickElection accepts time explicitly so protocol tests need no real sleeps.
func (cm *ConsensusModule) tickElection(now time.Time) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Leader || now.Before(cm.electionDeadline) {
		return
	}

	cm.state = Candidate
	cm.currentTerm++
	cm.votedFor = cm.id
	cm.resetElectionTimerLocked(now)
	// A timeout starts candidacy; leadership requires a majority of votes.
}

// runElectionTimer adapts clock ticks to protocol logic. Server owns the clock
// and stop signal; tests can supply ticks directly.
func (cm *ConsensusModule) runElectionTimer(ticks <-chan time.Time, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			cm.tickElection(now)
		}
	}
}
