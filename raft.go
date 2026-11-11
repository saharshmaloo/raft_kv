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

// RequestVoteArgs identifies the candidate and election term.
type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

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
	votesReceived    map[int]bool // Grants in currentTerm, including our own vote.
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
	if cm.state == Leader || now.Before(cm.electionDeadline) {
		cm.mu.Unlock()
		return
	}

	cm.state = Candidate
	cm.currentTerm++
	cm.votedFor = cm.id
	cm.votesReceived = map[int]bool{cm.id: true}
	cm.resetElectionTimerLocked(now)
	args := RequestVoteArgs{Term: cm.currentTerm, CandidateID: cm.id}
	cm.becomeLeaderIfMajorityLocked()
	cm.mu.Unlock()

	// Network calls run independently: an unreachable peer must not block the
	// timer, incoming RPCs, or a majority of other peers from electing us.
	if cm.server != nil {
		for _, peerID := range cm.peerIds {
			cm.server.requestVote(peerID, args)
		}
	}
}

// becomeFollowerLocked adopts a higher term. It does not reset the timer:
// learning a term alone is not evidence of a leader or a granted vote.
func (cm *ConsensusModule) becomeFollowerLocked(term int) {
	cm.currentTerm = term
	cm.state = Follower
	cm.votedFor = -1
	cm.votesReceived = nil
}

func (cm *ConsensusModule) becomeLeaderIfMajorityLocked() {
	if len(cm.votesReceived) > (len(cm.peerIds)+1)/2 {
		cm.state = Leader
	}
}

// handleRequestVoteReply accepts the election term separately so a delayed
// grant cannot be counted in a later election.
func (cm *ConsensusModule) handleRequestVoteReply(peerID, electionTerm int, reply RequestVoteReply) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// Even a reply from an old election can teach us about a newer term.
	if reply.Term > cm.currentTerm {
		cm.becomeFollowerLocked(reply.Term)
		return
	}
	if cm.state != Candidate || cm.currentTerm != electionTerm || reply.Term != electionTerm {
		return
	}
	if reply.VoteGranted {
		cm.votesReceived[peerID] = true
		cm.becomeLeaderIfMajorityLocked()
	}
}

// RequestVote grants votes to at most one candidate per term, allowing retries
// from that candidate. Term and vote are held in memory.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if args.Term > cm.currentTerm {
		cm.becomeFollowerLocked(args.Term)
	}
	reply.Term = cm.currentTerm
	reply.VoteGranted = false
	if args.Term == cm.currentTerm && (cm.votedFor == -1 || cm.votedFor == args.CandidateID) {
		cm.votedFor = args.CandidateID
		reply.VoteGranted = true
		cm.resetElectionTimerLocked(time.Now())
	}
	return nil
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
