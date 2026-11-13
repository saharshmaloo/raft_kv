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

// Transport sends peer RPCs without exposing connection management to Raft.
type Transport interface {
	Call(peerID int, method string, args, reply any) error
}

// Status is a consistent snapshot of one node's protocol and lifecycle state.
type Status struct {
	ID      int
	State   CMState
	Term    int
	Started bool
	Stopped bool
}

// ConsensusModule holds the Raft protocol logic for one replica.
type ConsensusModule struct {
	mu        sync.Mutex
	id        int
	peerIds   []int
	transport Transport

	// Raft requires term and vote changes to be persisted before RPC responses.
	currentTerm int
	votedFor    int // -1 means no vote in the current term.

	// Volatile state. mu protects this state and the term/vote above.
	state             CMState
	electionDeadline  time.Time
	electionTimeout   func() time.Duration
	votesReceived     map[int]bool // Grants in currentTerm, including our own vote.
	heartbeatDeadline time.Time

	heartbeats map[int]bool
	started    bool
	stopped    bool
	done       chan struct{}
	wg         sync.WaitGroup
}

// NewConsensusModule constructs a follower without starting its clock.
func NewConsensusModule(id int, peerIds []int, transport Transport) *ConsensusModule {
	return &ConsensusModule{
		id:         id,
		peerIds:    append([]int(nil), peerIds...),
		transport:  transport,
		votedFor:   -1,
		state:      Follower,
		heartbeats: make(map[int]bool),
		done:       make(chan struct{}),
		electionTimeout: func() time.Duration {
			return 150*time.Millisecond + time.Duration(rand.Int63n(int64(150*time.Millisecond)))
		},
	}
}

// Start initializes the election deadline before launching the ticker that drives
// elections and heartbeats, so a follower waits a full timeout before campaigning.
func (cm *ConsensusModule) Start() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.started || cm.stopped {
		return
	}
	cm.started = true
	cm.resetElectionTimerLocked(time.Now())
	ticker := time.NewTicker(10 * time.Millisecond)
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		defer ticker.Stop()
		cm.runTicker(ticker.C, cm.done)
	}()
}

// Stop prevents new protocol goroutines before waiting for existing work.
// The transport must close connections first to unblock pending RPCs.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	if !cm.stopped {
		cm.stopped = true
		close(cm.done)
	}
	cm.mu.Unlock()
	cm.wg.Wait()
}

// Status returns a snapshot without exposing mutable protocol state.
func (cm *ConsensusModule) Status() Status {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return Status{ID: cm.id, State: cm.state, Term: cm.currentTerm, Started: cm.started, Stopped: cm.stopped}
}

// runTicker delivers clock ticks to Raft protocol logic until stopped or the
// tick channel closes. Tests can supply ticks directly to control time.
func (cm *ConsensusModule) runTicker(ticks <-chan time.Time, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			cm.tick(now)
		}
	}
}

// tick drives time-based Raft work: leaders send heartbeats when due, while
// followers and candidates start a new election when their deadline expires.
// Passing time explicitly lets tests exercise deadlines without real sleeps.
func (cm *ConsensusModule) tick(now time.Time) {
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return
	}
	if cm.state == Leader {
		cm.mu.Unlock()
		cm.sendHeartbeats(now)
		return
	}
	if now.Before(cm.electionDeadline) {
		cm.mu.Unlock()
		return
	}

	cm.state = Candidate
	cm.currentTerm++
	cm.votedFor = cm.id
	cm.votesReceived = map[int]bool{cm.id: true}
	cm.resetElectionTimerLocked(now)
	args := RequestVoteArgs{Term: cm.currentTerm, CandidateID: cm.id}
	elected := cm.becomeLeaderIfMajorityLocked()
	cm.mu.Unlock()
	if elected {
		cm.sendHeartbeats(now)
		return
	}

	// Network calls run independently: an unreachable peer must not block the
	// timer, incoming RPCs, or a majority of other peers from electing us.
	if cm.transport != nil {
		for _, peerID := range cm.peerIds {
			cm.requestVote(peerID, args)
		}
	}
}
