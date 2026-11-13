package raft

import "time"

// Heartbeats leave room for multiple deliveries before the shortest election timeout.
const heartbeatInterval = 50 * time.Millisecond

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

func (cm *ConsensusModule) sendHeartbeats(now time.Time) {
	cm.mu.Lock()
	if cm.state != Leader || now.Before(cm.heartbeatDeadline) {
		cm.mu.Unlock()
		return
	}
	cm.heartbeatDeadline = now.Add(heartbeatInterval)
	args := AppendEntriesArgs{Term: cm.currentTerm, LeaderID: cm.id}
	cm.mu.Unlock()
	if cm.transport != nil {
		for _, peerID := range cm.peerIds {
			cm.sendHeartbeat(peerID, args)
		}
	}
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
		if err := cm.transport.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err != nil {
			return
		}
		cm.handleAppendEntriesReply(reply)
	}()
}

func (cm *ConsensusModule) handleAppendEntriesReply(reply AppendEntriesReply) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if reply.Term > cm.currentTerm {
		cm.becomeFollowerLocked(reply.Term)
	}
}
