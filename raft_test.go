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
	cm.tickElection(now.Add(149 * time.Millisecond))
	if cm.state != Follower || cm.currentTerm != 0 || cm.votedFor != -1 {
		t.Fatal("election started before the deadline")
	}
	cm.tickElection(now.Add(150 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 1 || cm.votedFor != cm.id {
		t.Fatal("timeout must become candidate, increment term, and vote for self")
	}
}

func TestCandidateRetriesWithFreshTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.electionTimeout = func() time.Duration { return 250 * time.Millisecond }
	cm.tickElection(now.Add(150 * time.Millisecond))
	cm.tickElection(now.Add(399 * time.Millisecond))
	if cm.currentTerm != 1 {
		t.Fatal("candidate retried before the newly sampled deadline")
	}
	cm.tickElection(now.Add(400 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 2 || cm.votedFor != cm.id {
		t.Fatal("candidate must retry in a new term and vote for self")
	}
}

func TestElectionTimerReset(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.mu.Lock()
	cm.resetElectionTimerLocked(now.Add(100 * time.Millisecond))
	cm.mu.Unlock()
	cm.tickElection(now.Add(150 * time.Millisecond))
	if cm.state != Follower {
		t.Fatal("old deadline triggered an election after reset")
	}
	cm.tickElection(now.Add(250 * time.Millisecond))
	if cm.state != Candidate || cm.currentTerm != 1 {
		t.Fatal("reset deadline did not trigger an election")
	}
}

func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.state = Leader
	cm.currentTerm = 3
	cm.tickElection(now.Add(time.Hour))
	if cm.state != Leader || cm.currentTerm != 3 || cm.votedFor != -1 {
		t.Fatal("leader started an election")
	}
}

func TestDelayedTickStartsOnlyOneElection(t *testing.T) {
	cm, now := newTimerTestModule()
	late := now.Add(time.Hour)
	cm.tickElection(late)
	cm.tickElection(late)
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

func TestElectionTimerLoop(t *testing.T) {
	cm, now := newTimerTestModule()
	ticks := make(chan time.Time)
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		cm.runElectionTimer(ticks, stop)
	}()
	// The second send ensures the loop has processed the first tick.
	ticks <- now.Add(150 * time.Millisecond)
	ticks <- now.Add(151 * time.Millisecond)
	close(stop)
	<-finished
	if cm.state != Candidate || cm.currentTerm != 1 {
		t.Fatal("timer loop did not deliver the timeout")
	}
}

func TestElectionTimerLoopAlreadyStopped(t *testing.T) {
	cm, _ := newTimerTestModule()
	stop := make(chan struct{})
	close(stop)
	cm.runElectionTimer(make(chan time.Time), stop)
	if cm.state != Follower || cm.currentTerm != 0 {
		t.Fatal("stopped timer changed consensus state")
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	cm := newConsensusModule(1, nil, nil)
	cm.tickElection(time.Unix(0, 0))
	if cm.state != Leader || cm.currentTerm != 1 || cm.votedFor != 1 {
		t.Fatal("a single node's self-vote is a majority")
	}
}

func TestCandidateNeedsDistinctMajority(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.peerIds = []int{2, 3, 4, 5}
	cm.tickElection(now.Add(150 * time.Millisecond))
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
	cm.tickElection(now.Add(150 * time.Millisecond))
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
	cm.tickElection(now.Add(150 * time.Millisecond))
	cm.tickElection(now.Add(300 * time.Millisecond))
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
		cm.tickElection(now.Add(150 * time.Millisecond))
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
		{"stale term", 2, -1, RequestVoteArgs{1, 2}, 2, -1, false},
		{"first vote", 2, -1, RequestVoteArgs{2, 2}, 2, 2, true},
		{"repeat vote", 2, 2, RequestVoteArgs{2, 2}, 2, 2, true},
		{"already voted", 2, 3, RequestVoteArgs{2, 2}, 2, 3, false},
		{"higher term clears vote", 2, 3, RequestVoteArgs{3, 2}, 3, 2, true},
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
	cm.tickElection(now.Add(150 * time.Millisecond))
	var reply RequestVoteReply
	cm.RequestVote(RequestVoteArgs{Term: 2, CandidateID: 2}, &reply)
	if cm.state != Follower || cm.currentTerm != 2 || !reply.VoteGranted {
		t.Fatal("a higher-term request must make a candidate step down")
	}
}
