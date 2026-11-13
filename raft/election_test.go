package raft

import (
	"net/rpc"
	"testing"
	"time"
)

// Reaching the deadline begins a new term with a self-vote; earlier ticks do nothing.
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

// A failed election gets a full newly sampled timeout before another term begins.
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

// Resetting the deadline prevents an election at the superseded deadline.
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

// Passing an election deadline must not make a leader campaign again.
func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.state = Leader
	cm.currentTerm = 3
	cm.tick(now.Add(time.Hour))
	if cm.state != Leader || cm.currentTerm != 3 || cm.votedFor != -1 {
		t.Fatal("leader started an election")
	}
}

// A late tick starts one election rather than replaying every missed timeout.
func TestDelayedTickStartsOnlyOneElection(t *testing.T) {
	cm, now := newTimerTestModule()
	late := now.Add(time.Hour)
	cm.tick(late)
	cm.tick(late)
	if cm.currentTerm != 1 || !cm.electionDeadline.Equal(late.Add(150*time.Millisecond)) {
		t.Fatal("delayed tick must begin one election with a fresh full timeout")
	}
}

// Sampled timeouts stay within the configured [150 ms, 300 ms) interval.
func TestRandomElectionTimeoutRange(t *testing.T) {
	cm := NewConsensusModule(1, nil, nil)
	for i := 0; i < 100; i++ {
		timeout := cm.electionTimeout()
		if timeout < 150*time.Millisecond || timeout >= 300*time.Millisecond {
			t.Fatalf("timeout outside [150ms, 300ms): %v", timeout)
		}
	}
}

// A single-node cluster wins immediately because its self-vote is a majority.
func TestSingleNodeElectsItself(t *testing.T) {
	cm := NewConsensusModule(1, nil, nil)
	cm.tick(time.Unix(0, 0))
	if cm.state != Leader || cm.currentTerm != 1 || cm.votedFor != 1 {
		t.Fatal("a single node's self-vote is a majority")
	}
}

// Duplicate grants and rejections cannot inflate the majority count.
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

// Exactly half the votes cannot elect a leader in an even-sized cluster.
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

// Delayed grants count only when both the election and reply terms match.
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

// A higher term overrides leadership even in an old reply, without resetting the deadline.
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

// Votes honor term and prior-vote rules; only a granted vote postpones an election.
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

// A candidate adopts a higher term before considering the incoming vote request.
func TestHigherTermRequestStepsDown(t *testing.T) {
	cm, now := newTimerTestModule()
	cm.tick(now.Add(150 * time.Millisecond))
	var reply RequestVoteReply
	cm.RequestVote(RequestVoteArgs{Term: 2, CandidateID: 2}, &reply)
	if cm.state != Follower || cm.currentTerm != 2 || !reply.VoteGranted {
		t.Fatal("a higher-term request must make a candidate step down")
	}
}

// Real RPC exchanges over in-memory connections must produce a majority and leadership.
func TestElectionRequestsVotesOverRPC(t *testing.T) {
	transport := &rpcTestTransport{clients: make(map[int]*rpc.Client)}
	cm := NewConsensusModule(1, []int{2, 3}, transport)
	t.Cleanup(cm.Stop)
	peers := []*ConsensusModule{NewConsensusModule(2, nil, nil), NewConsensusModule(3, nil, nil)}
	for _, peer := range peers {
		connectTestPeer(t, transport, peer)
	}
	cm.tick(time.Unix(0, 0))
	cm.wg.Wait() // Outgoing calls and reply handling must complete before observation.
	if cm.state != Leader || cm.currentTerm != 1 {
		t.Fatal("RPC votes did not elect the candidate")
	}
	for _, peer := range peers {
		peer.mu.Lock()
		votedFor, term := peer.votedFor, peer.currentTerm
		peer.mu.Unlock()
		if votedFor != 1 || term != 1 {
			t.Fatal("candidate did not request a vote from every peer")
		}
	}
}

// Unreachable peers supply no votes, leaving the candidate short of a majority.
func TestFailedVoteRPCLeavesCandidate(t *testing.T) {
	transport := &rpcTestTransport{}
	cm := NewConsensusModule(1, []int{2, 3}, transport)
	defer cm.Stop()
	cm.tick(time.Unix(0, 0))
	cm.wg.Wait()
	if cm.state != Candidate || len(cm.votesReceived) != 1 {
		t.Fatal("disconnected peers must not count as votes")
	}
}
