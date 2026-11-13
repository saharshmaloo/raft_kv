package raft

import "time"

// resetElectionTimerLocked chooses a fresh timeout on every reset, including
// election retries, so candidates do not repeatedly time out together.
// The caller must hold cm.mu. RPC handlers should reset only for a
// current-term leader's AppendEntries (after adopting a higher term if needed)
// or a granted vote, not for stale RPCs or rejected vote requests (Figure 2).
func (cm *ConsensusModule) resetElectionTimerLocked(now time.Time) {
	cm.electionDeadline = now.Add(cm.electionTimeout())
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
		if err := cm.transport.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err != nil {
			return // A failed RPC is not a vote; the next timeout retries the election.
		}
		cm.handleRequestVoteReply(peerID, args.Term, reply)
	}()
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
