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

// RequestVoteArgs includes the candidate's last log position so a voter can
// compare log freshness by term, then by index.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// LogEntry associates an opaque state-machine command with its election term.
type LogEntry struct {
	Term    int
	Command []byte
}

// AppendEntriesArgs identifies the log prefix preceding Entries. An empty
// Entries slice represents a heartbeat. Log indexes start at 1; 0 denotes no entry.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Heartbeats leave room for multiple deliveries before the shortest election timeout.
const heartbeatInterval = 50 * time.Millisecond

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

func newConsensusModule(id int, peerIds []int, server *Server) *ConsensusModule {
	return &ConsensusModule{
		id:         id,
		peerIds:    append([]int(nil), peerIds...),
		server:     server,
		votedFor:   -1,
		state:      Follower,
		heartbeats: make(map[int]bool),
		done:       make(chan struct{}),
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
	if cm.server != nil {
		for _, peerID := range cm.peerIds {
			cm.requestVote(peerID, args)
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

func (cm *ConsensusModule) becomeLeaderIfMajorityLocked() bool {
	if len(cm.votesReceived) > (len(cm.peerIds)+1)/2 {
		cm.state = Leader
		cm.heartbeatDeadline = time.Time{}
		return true
	}
	return false
}

// handleRequestVoteReply accepts the election term separately so a delayed
// grant cannot be counted in a later election.
func (cm *ConsensusModule) handleRequestVoteReply(peerID, electionTerm int, reply RequestVoteReply) {
	cm.mu.Lock()
	// Even a reply from an old election can teach us about a newer term.
	if reply.Term > cm.currentTerm {
		cm.becomeFollowerLocked(reply.Term)
		cm.mu.Unlock()
		return
	}
	if cm.state != Candidate || cm.currentTerm != electionTerm || reply.Term != electionTerm {
		cm.mu.Unlock()
		return
	}
	elected := false
	if reply.VoteGranted {
		cm.votesReceived[peerID] = true
		elected = cm.becomeLeaderIfMajorityLocked()
	}
	cm.mu.Unlock()
	if elected {
		// Announce leadership immediately, before followers' election timers expire.
		cm.sendHeartbeats(time.Now())
	}
}

func (cm *ConsensusModule) sendHeartbeats(now time.Time) {
	cm.mu.Lock()
	if cm.state != Leader || now.Before(cm.heartbeatDeadline) {
		cm.mu.Unlock()
		return
	}
	cm.heartbeatDeadline = now.Add(heartbeatInterval)
	args := AppendEntriesArgs{Term: cm.currentTerm, LeaderID: cm.id}
	cm.mu.Unlock()
	if cm.server != nil {
		for _, peerID := range cm.peerIds {
			cm.sendHeartbeat(peerID, args)
		}
	}
}

// AppendEntries recognizes leader contact and postpones the receiver's election.
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	return cm.appendEntries(args, reply, time.Now())
}

func (cm *ConsensusModule) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply, now time.Time) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if args.Term > cm.currentTerm {
		cm.becomeFollowerLocked(args.Term)
	}
	reply.Term = cm.currentTerm
	reply.Success = false
	if args.Term < cm.currentTerm {
		return nil
	}
	// Same-term leader contact ends candidacy without clearing the vote:
	// clearing it would allow a second candidate to receive a vote in this term.
	cm.state = Follower
	cm.votesReceived = nil
	cm.resetElectionTimerLocked(now)
	reply.Success = true
	return nil
}

func (cm *ConsensusModule) handleAppendEntriesReply(reply AppendEntriesReply) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if reply.Term > cm.currentTerm {
		cm.becomeFollowerLocked(reply.Term)
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

// requestVote tracks outgoing work so stopping consensus waits for reply handling.
func (cm *ConsensusModule) requestVote(peerID int, args RequestVoteArgs) {
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return
	}
	cm.wg.Add(1)
	cm.mu.Unlock()
	go func() {
		defer cm.wg.Done()
		var reply RequestVoteReply
		if err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err != nil {
			return // A failed RPC is not a vote; the next timeout retries the election.
		}
		cm.handleRequestVoteReply(peerID, args.Term, reply)
	}()
}

func (cm *ConsensusModule) sendHeartbeat(peerID int, args AppendEntriesArgs) {
	cm.mu.Lock()
	// A stalled peer must not accumulate a goroutine on every heartbeat interval.
	if cm.stopped || cm.heartbeats[peerID] {
		cm.mu.Unlock()
		return
	}
	cm.heartbeats[peerID] = true
	cm.wg.Add(1)
	cm.mu.Unlock()
	go func() {
		defer cm.wg.Done()
		defer func() {
			cm.mu.Lock()
			delete(cm.heartbeats, peerID)
			cm.mu.Unlock()
		}()
		var reply AppendEntriesReply
		if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err != nil {
			return
		}
		cm.handleAppendEntriesReply(reply)
	}()
}

// start initializes the deadline before launching the clock, so the first tick
// waits for a full election timeout. Construction alone does not start elections.
func (cm *ConsensusModule) start() {
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
		cm.runElectionTimer(ticker.C, cm.done)
	}()
}

// stop prevents new protocol goroutines before waiting for existing work.
// The transport must close connections first to unblock pending RPCs.
func (cm *ConsensusModule) stop() {
	cm.mu.Lock()
	if !cm.stopped {
		cm.stopped = true
		close(cm.done)
	}
	cm.mu.Unlock()
	cm.wg.Wait()
}

// runElectionTimer adapts clock ticks to protocol logic; tests can supply ticks directly.
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
