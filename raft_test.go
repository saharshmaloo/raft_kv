package raft

import (
	"testing"
	"time"
)

func newTimerTestModule() (*ConsensusModule, time.Time) {
	cm := newConsensusModule(1, []int{2, 3}, nil)
	cm.electionTimeout = func() time.Duration { return 150 * time.Millisecond }
	now := time.Unix(0, 0)
	cm.resetElectionTimerLocked(now)
	return cm, now
}

func TestElectionTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.tick(now.Add(149 * time.Millisecond))
	if cm.state != Follower || cm.currentTerm != 0 || cm.votedFor != -1 {
		t.Fatal("election started before the deadline")
	}
	cm.tick(now.Add(150 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 1 || cm.votedFor != cm.id {
		t.Fatal("timeout must become candidate, increment term, and vote for self")
	}
}

func TestCandidateRetriesWithFreshTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.electionTimeout = func() time.Duration { return 250 * time.Millisecond }
	cm.tick(now.Add(150 * time.Millisecond))
	cm.tick(now.Add(399 * time.Millisecond))
	if cm.currentTerm != 1 {
		t.Fatal("candidate retried before the newly sampled deadline")
	}
	cm.tick(now.Add(400 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 2 || cm.votedFor != cm.id {
		t.Fatal("candidate must retry in a new term and vote for self")
	}
}

func TestElectionTimerReset(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.mu.Lock()
	cm.resetElectionTimerLocked(now.Add(100 * time.Millisecond))
	cm.mu.Unlock()
	cm.tick(now.Add(150 * time.Millisecond))
	if cm.state != Follower {
		t.Fatal("old deadline triggered an election after reset")
	}
	cm.tick(now.Add(250 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 1 {
		t.Fatal("reset deadline did not trigger an election")
	}
}

func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.state = Leader
	cm.currentTerm = 3
	cm.tick(now.Add(time.Hour))
	if cm.state != Leader || cm.currentTerm != 3 || cm.votedFor != -1 {
		t.Fatal("leader started an election")
	}
}

func TestDelayedTickStartsOnlyOneElection(t *testing.T) {
	cm, now := newTimerTestModule()
	late := now.Add(time.Hour)
	cm.tick(late)
	cm.tick(late)
	if cm.currentTerm != 1 || !cm.electionDeadline.Equal(late.Add(150*time.Millisecond)) {
		t.Fatal("delayed tick must begin one election with a fresh full timeout")
	}
}

func TestRandomElectionTimeoutRange(t *testing.T) {
	cm := newConsensusModule(1, nil, nil)
	for i := 0; i < 100; i++ {
		timeout := cm.electionTimeout()
		if timeout < 150*time.Millisecond || timeout >= 300*time.Millisecond {
			t.Fatalf("timeout outside [150ms, 300ms): %v", timeout)
		}
	}
}

func TestTickerLoop(t *testing.T) {
	cm, now := newTimerTestModule()
	ticks := make(chan time.Time)
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		cm.runTicker(ticks, stop)
	}()
	// The second send ensures the loop has processed the first tick.
	ticks <- now.Add(150 * time.Millisecond)
	ticks <- now.Add(151 * time.Millisecond)
	close(stop)
	<-finished
	if cm.state != Candidate || cm.currentTerm != 1 {
		t.Fatal("ticker loop did not deliver the timeout")
	}
}

func TestTickerLoopAlreadyStopped(t *testing.T) {
	cm, _ := newTimerTestModule()
	stop := make(chan struct{})
	close(stop)
	cm.runTicker(make(chan time.Time), stop)
	if cm.state != Follower || cm.currentTerm != 0 {
		t.Fatal("stopped ticker changed consensus state")
	}
}

func TestConsensusTickerLifecycle(t *testing.T) {
	cm := newConsensusModule(1, nil, nil)
	t.Cleanup(cm.stop)
	if !cm.electionDeadline.IsZero() {
		t.Fatal("construction initialized the election deadline")
	}
	cm.start()
	cm.mu.Lock()
	deadline := cm.electionDeadline
	cm.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("starting consensus did not initialize the election deadline")
	}
	cm.start()
	cm.mu.Lock()
	unchanged := cm.electionDeadline.Equal(deadline)
	cm.mu.Unlock()
	if !unchanged {
		t.Fatal("starting consensus twice reset the election deadline")
	}
	cm.stop()
	cm.stop()
	select {
	case <-cm.done:
	default:
		t.Fatal("stopping consensus did not signal the ticker")
	}
	term := cm.currentTerm
	cm.start()
	cm.tick(deadline.Add(time.Hour))
	if cm.currentTerm != term {
		t.Fatal("stopped consensus restarted an election")
	}
}

func TestConsensusStopBeforeStart(t *testing.T) {
	cm := newConsensusModule(1, nil, nil)
	cm.stop()
	cm.start()
	if !cm.electionDeadline.IsZero() {
		t.Fatal("stopped consensus initialized its election deadline")
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	cm := newConsensusModule(1, nil, nil)
	cm.tick(time.Unix(0, 0))
	if cm.state != Leader || cm.currentTerm != 1 || cm.votedFor != 1 {
		t.Fatal("a single node's self-vote is a majority")
	}
}

func TestCandidateNeedsDistinctMajority(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.peerIds = []int{2, 3, 4, 5}
	cm.tick(now.Add(150 * time.Millisecond))
	cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	cm.handleRequestVoteReply(3, 1, RequestVoteReply{Term: 1})
	if cm.state != Candidate || len(cm.votesReceived) != 2 {
		t.Fatal("duplicate grants and rejections must not create a majority")
	}
	cm.handleRequestVoteReply(4, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	if cm.state != Leader {
		t.Fatal("three votes out of five must elect a leader")
	}
}

func TestEvenClusterNeedsMoreThanHalf(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.peerIds = []int{2, 3, 4}
	cm.tick(now.Add(150 * time.Millisecond))
	cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	if cm.state != Candidate {
		t.Fatal("two votes out of four is not a majority")
	}
	cm.handleRequestVoteReply(3, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	if cm.state != Leader {
		t.Fatal("three votes out of four is a majority")
	}
}

func TestElectionRetryDiscardsOldVotes(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.tick(now.Add(150 * time.Millisecond))
	cm.tick(now.Add(300 * time.Millisecond))
	cm.handleRequestVoteReply(2, 1, RequestVoteReply{Term: 1, VoteGranted: true})
	cm.handleRequestVoteReply(3, 2, RequestVoteReply{Term: 1, VoteGranted: true})
	if cm.state != Candidate || len(cm.votesReceived) != 1 {
		t.Fatal("old-election and stale-term grants must be ignored")
	}
	cm.handleRequestVoteReply(2, 2, RequestVoteReply{Term: 2, VoteGranted: true})
	if cm.state != Leader {
		t.Fatal("current-election grant was not counted")
	}
}

func TestHigherTermReplyStepsDown(t *testing.T) {
	for _, state := range []CMState{Candidate, Leader} {
		cm, now := newTimerTestModule()
		cm.tick(now.Add(150 * time.Millisecond))
		cm.state = state
		deadline := cm.electionDeadline
		cm.handleRequestVoteReply(2, 0, RequestVoteReply{Term: 4})
		if cm.state != Follower || cm.currentTerm != 4 || cm.votedFor != -1 {
			t.Fatal("higher term must override even a stale election reply")
		}
		if !cm.electionDeadline.Equal(deadline) {
			t.Fatal("learning a higher term alone must not reset the timer")
		}
		cm.handleRequestVoteReply(3, 1, RequestVoteReply{Term: 1, VoteGranted: true})
		if cm.state != Follower {
			t.Fatal("a late grant revived an abandoned election")
		}
	}
}

func TestRequestVote(t *testing.T) {
	for _, test := range []struct {
		name      string
		term      int
		votedFor  int
		args      RequestVoteArgs
		wantTerm  int
		wantVote  int
		wantGrant bool
	}{
		{"stale term", 2, -1, RequestVoteArgs{Term: 1, CandidateID: 2}, 2, -1, false},
		{"first vote", 2, -1, RequestVoteArgs{Term: 2, CandidateID: 2}, 2, 2, true},
		{"repeat vote", 2, 2, RequestVoteArgs{Term: 2, CandidateID: 2}, 2, 2, true},
		{"already voted", 2, 3, RequestVoteArgs{Term: 2, CandidateID: 2}, 2, 3, false},
		{"higher term clears vote", 2, 3, RequestVoteArgs{Term: 3, CandidateID: 2}, 3, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cm, _ := newTimerTestModule()
			cm.currentTerm, cm.votedFor = test.term, test.votedFor
			deadline := cm.electionDeadline
			var reply RequestVoteReply
			if err := cm.RequestVote(test.args, &reply); err != nil {
				t.Fatal(err)
			}
			if reply.Term != test.wantTerm || reply.VoteGranted != test.wantGrant || cm.votedFor != test.wantVote {
				t.Fatalf("unexpected vote: reply=%+v votedFor=%d", reply, cm.votedFor)
			}
			if test.wantGrant == cm.electionDeadline.Equal(deadline) {
				t.Fatal("only granted votes should reset the election deadline")
			}
		})
	}
}

func TestHigherTermRequestStepsDown(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.tick(now.Add(150 * time.Millisecond))
	var reply RequestVoteReply
	cm.RequestVote(RequestVoteArgs{Term: 2, CandidateID: 2}, &reply)
	if cm.state != Follower || cm.currentTerm != 2 || !reply.VoteGranted {
		t.Fatal("a higher-term request must make a candidate step down")
	}
}

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
