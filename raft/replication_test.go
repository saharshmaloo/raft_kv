package raft

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

// Valid leader contact resets the deadline and preserves same-term votes; stale contact is rejected.
func TestAppendEntries(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     CMState
		term      int
		wantTerm  int
		wantVote  int
		wantState CMState
		accepted  bool
	}{
		{"stale heartbeat", Candidate, 1, 2, 1, Candidate, false},
		{"same term follower", Follower, 2, 2, 1, Follower, true},
		{"same term candidate", Candidate, 2, 2, 1, Follower, true},
		{"higher term candidate", Candidate, 3, 3, -1, Follower, true},
		{"higher term leader", Leader, 3, 3, -1, Follower, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cm, now := newTimerTestModule()
			cm.state, cm.currentTerm, cm.votedFor = test.state, 2, 1
			cm.votesReceived = map[int]bool{1: true}
			deadline := cm.electionDeadline
			var reply AppendEntriesReply
			if err := cm.appendEntries(AppendEntriesArgs{Term: test.term, LeaderID: 2}, &reply, now.Add(100*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if reply.Term != test.wantTerm || reply.Success != test.accepted || cm.currentTerm != test.wantTerm || cm.state != test.wantState || cm.votedFor != test.wantVote {
				t.Fatalf("unexpected heartbeat result: reply=%+v state=%v term=%d vote=%d", reply, cm.state, cm.currentTerm, cm.votedFor)
			}
			if test.accepted {
				deadline = now.Add(250 * time.Millisecond)
				if cm.votesReceived != nil {
					t.Fatal("leader contact did not discard election grants")
				}
			}
			if !cm.electionDeadline.Equal(deadline) {
				t.Fatal("heartbeat changed the election deadline incorrectly")
			}
		})
	}
}

// Regular leader contact suppresses elections until a full timeout after the last heartbeat.
func TestHeartbeatsPreventElectionUntilContactStops(t *testing.T) {
	cm, now := newTimerTestModule()
	for elapsed := heartbeatInterval; elapsed <= time.Second; elapsed += heartbeatInterval {
		at := now.Add(elapsed)
		var reply AppendEntriesReply
		cm.appendEntries(AppendEntriesArgs{Term: 1, LeaderID: 2}, &reply, at)
		cm.tick(at)
		if cm.state != Follower || cm.currentTerm != 1 {
			t.Fatal("follower started an election despite regular heartbeats")
		}
	}
	cm.tick(now.Add(time.Second + 150*time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 2 {
		t.Fatal("follower did not start an election after heartbeats stopped")
	}
}

// Leaders schedule heartbeats at the configured interval rather than on every tick.
func TestHeartbeatSchedule(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.state, cm.currentTerm = Leader, 1
	cm.tick(now)
	want := now.Add(heartbeatInterval)
	if !cm.heartbeatDeadline.Equal(want) {
		t.Fatal("leader did not schedule its next heartbeat")
	}
	cm.tick(want.Add(-time.Millisecond))
	if !cm.heartbeatDeadline.Equal(want) {
		t.Fatal("leader sent a heartbeat before the interval elapsed")
	}
	cm.tick(want)
	if !cm.heartbeatDeadline.Equal(want.Add(heartbeatInterval)) {
		t.Fatal("leader did not schedule a periodic heartbeat")
	}
}

// Stale heartbeat replies are ignored; a higher term ends leadership without extending the deadline.
func TestHeartbeatReplyHigherTermStepsDown(t *testing.T) {
	cm, _ := newTimerTestModule()
	cm.state, cm.currentTerm, cm.votedFor = Leader, 2, 1
	deadline := cm.electionDeadline
	cm.handleAppendEntriesReply(AppendEntriesReply{Term: 1, Success: true})
	if cm.state != Leader || cm.currentTerm != 2 {
		t.Fatal("stale heartbeat reply changed leadership")
	}
	cm.handleAppendEntriesReply(AppendEntriesReply{Term: 3})
	if cm.state != Follower || cm.currentTerm != 3 || cm.votedFor != -1 || !cm.electionDeadline.Equal(deadline) {
		t.Fatal("higher-term heartbeat reply must step down without resetting the timer")
	}
}

// Winning a majority immediately ends peer candidacy without clearing its existing vote.
func TestWinningVoteSendsImmediateHeartbeat(t *testing.T) {
	transport := &rpcTestTransport{clients: make(map[int]*rpc.Client)}
	cm := NewConsensusModule(1, []int{2}, transport)
	t.Cleanup(cm.Stop)
	peer := NewConsensusModule(2, []int{1}, nil)
	connectTestPeer(t, transport, peer)
	cm.state, cm.currentTerm = Candidate, 1
	cm.votesReceived = map[int]bool{1: true}
	peer.state, peer.currentTerm, peer.votedFor = Candidate, 1, 2
	cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	cm.wg.Wait()
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.state != Follower || peer.votedFor != 2 || peer.electionDeadline.IsZero() {
		t.Fatal("winning vote did not immediately deliver a heartbeat preserving the peer's vote")
	}
}

// Closing the transport releases blocked heartbeat work so consensus shutdown can finish.
func TestStopUnblocksAfterTransportCloses(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	client := rpc.NewClient(local)
	defer client.Close()
	transport := &rpcTestTransport{clients: map[int]*rpc.Client{2: client}}
	cm := NewConsensusModule(1, []int{2}, transport)
	// With no reader, the heartbeat cannot finish until the transport closes.
	cm.sendHeartbeat(2, AppendEntriesArgs{Term: 1, LeaderID: 1})
	finished := make(chan struct{})
	go func() {
		client.Close()
		cm.Stop()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("closing transport did not unblock consensus shutdown")
	}
}
