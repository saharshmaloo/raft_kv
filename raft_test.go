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
