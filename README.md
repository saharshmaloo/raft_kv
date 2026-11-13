# Raft KV

A Go implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) for a distributed key-value store.

## How Raft works

Raft helps a group of nodes agree on an ordered sequence of operations, even when some nodes fail or lose connectivity. Each node keeps a log of operations and applies committed entries in the same order to maintain a consistent state.

- **Leader election:** Nodes act as followers, candidates, or leaders. Followers expect regular heartbeats from a leader. If those stop arriving, a follower becomes a candidate and requests votes. A majority elects a leader. Elections take place in numbered terms, allowing nodes to recognize outdated leadership.
- **Log replication:** The leader accepts client operations and sends log entries to followers. Once an entry from the leader's term is stored on a majority, the leader can commit it along with preceding entries. Nodes apply committed operations to their state machines—for a key-value store, these operations update stored data.
- **Safety and recovery:** Voting rules require candidates to have sufficiently up-to-date logs, and replication repairs conflicting entries. These rules preserve committed operations across leader changes. A partition without a majority cannot commit new operations; progress resumes when a majority can communicate and elect a leader.

## Project layout

- `node.go` — consensus and transport wiring, node startup, and shutdown.
- `raft/` — protocol state, ticking, elections, heartbeats, and RPC types.
- `transport/` — TCP listeners, peer connections, and RPC serving.
- `integration/` — cluster scenarios and fault injection helpers.
- `*_test.go` — unit tests alongside each component.

## Getting started

Requires Go 1.21 or later.

```sh
git clone https://github.com/saharshmaloo/raft_kv.git
cd raft_kv
```

## Testing

Unit tests check election timing, voting, and RPC behavior. Integration and
end-to-end tests run five-node TCP clusters with partitions, request and reply
loss, delays, disconnections, and node shutdowns to check election safety,
leader stability, and recovery.

Run all tests with the race detector:

```sh
go test -race ./...
```

Set `RAFT_TEST_SEED` to choose a different randomized fault sequence; concurrent
RPC scheduling can still vary between runs.
