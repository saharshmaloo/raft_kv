package raft

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
