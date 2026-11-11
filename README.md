# Raft KV

A Go implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) for a distributed key-value store.

The project separates consensus logic from RPC networking so protocol behavior can be tested independently.

## How Raft works

Raft helps a group of nodes agree on an ordered sequence of operations, even when some nodes fail or lose connectivity. Each node keeps a log of operations and applies committed entries in the same order to maintain a consistent state.

- **Leader election:** Nodes act as followers, candidates, or leaders. Followers expect regular heartbeats from a leader. If those stop arriving, a follower becomes a candidate and requests votes. A majority elects a leader. Elections take place in numbered terms, allowing nodes to recognize outdated leadership.
- **Log replication:** The leader accepts client operations and sends log entries to followers. Once an entry from the leader's term is stored on a majority, the leader can commit it along with preceding entries. Nodes apply committed operations to their state machines—for a key-value store, these operations update stored data.
- **Safety and recovery:** Voting rules require candidates to have sufficiently up-to-date logs, and replication repairs conflicting entries. These rules preserve committed operations across leader changes. A partition without a majority cannot commit new operations; progress resumes when a majority can communicate and elect a leader.

## Project layout

- `raft.go` — node state, election timing, and voting.
- `server.go` — peer connections, RPC transport, and server lifecycle.
- `*_test.go` — protocol and transport tests.

## Getting started

Requires Go 1.21 or later.

```sh
git clone https://github.com/saharshmaloo/raft_kv.git
cd raft_kv
go test -race ./...
```
