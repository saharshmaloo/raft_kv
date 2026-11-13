package raft

import (
	"testing"
	"time"
)

// Use a fixed timeout and synthetic clock origin so deadline tests need no sleeps.
func newTimerTestModule() (*ConsensusModule, time.Time) {
	cm := NewConsensusModule(1, []int{2, 3}, nil)
	cm.electionTimeout = func() time.Duration { return 150 * time.Millisecond }
	now := time.Unix(0, 0)
	cm.resetElectionTimerLocked(now)
	return cm, now
}

// Injected ticks reach the protocol loop and trigger an election.
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

// A closed stop channel exits the loop without protocol work.
func TestTickerLoopAlreadyStopped(t *testing.T) {
	cm, _ := newTimerTestModule()
	stop := make(chan struct{})
	close(stop)
	cm.runTicker(make(chan time.Time), stop)
	if cm.state != Follower || cm.currentTerm != 0 {
		t.Fatal("stopped ticker changed consensus state")
	}
}

// Construction is passive, repeated starts preserve the deadline, and stopping prevents restart.
func TestConsensusTickerLifecycle(t *testing.T) {
	cm := NewConsensusModule(1, nil, nil)
	t.Cleanup(cm.Stop)
	if !cm.electionDeadline.IsZero() {
		t.Fatal("construction initialized the election deadline")
	}
	cm.Start()
	cm.mu.Lock()
	deadline := cm.electionDeadline
	cm.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("starting consensus did not initialize the election deadline")
	}
	cm.Start()
	cm.mu.Lock()
	unchanged := cm.electionDeadline.Equal(deadline)
	cm.mu.Unlock()
	if !unchanged {
		t.Fatal("starting consensus twice reset the election deadline")
	}
	cm.Stop()
	cm.Stop()
	select {
	case <-cm.done:
	default:
		t.Fatal("stopping consensus did not signal the ticker")
	}
	term := cm.currentTerm
	cm.Start()
	cm.tick(deadline.Add(time.Hour))
	if cm.currentTerm != term {
		t.Fatal("stopped consensus restarted an election")
	}
}

// Stopping before startup permanently prevents the election clock from starting.
func TestConsensusStopBeforeStart(t *testing.T) {
	cm := NewConsensusModule(1, nil, nil)
	cm.Stop()
	cm.Start()
	if !cm.electionDeadline.IsZero() {
		t.Fatal("stopped consensus initialized its election deadline")
	}
}

// Caller mutations to the peer slice must not change cluster membership.
func TestConsensusModuleCopiesPeerIDs(t *testing.T) {
	ids := []int{2, 3}
	transport := &rpcTestTransport{}
	cm := NewConsensusModule(1, ids, transport)
	ids[0] = 99
	if cm.id != 1 || cm.peerIds[0] != 2 || cm.transport != transport {
		t.Fatal("consensus module identity, peers, or transport is incorrect")
	}
}
