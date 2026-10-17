# Go-Raft KV 🚀

A learning project implementing the **Raft Consensus Algorithm** in Go, with a distributed key-value store as the eventual application.

Currently implemented: a `ConsensusModule` identity scaffold in `raft.go` and a TCP `net/rpc` transport wrapper in `server.go`. No Raft RPC handlers, elections, replication, persistence, or key-value operations exist yet.

Run scaffold checks with `go test -race ./...`. This is currently a library package, with no executable entry point.

## Planned features (not implemented)

* **Strong Consistency:** Linearizable reads and writes via the Raft protocol.
* **Fault Tolerance:** The cluster remains operational as long as a quorum (majority) of nodes are healthy.
* **Dynamic Membership:** Support for adding or removing nodes from the cluster.
* **Log Compaction:** Implements snapshotting to prevent log explosion and speed up restarts.
* **Simple API:** Easy-to-use HTTP/gRPC interface for `GET`, `SET`, and `DELETE` operations.

---

## Intended architecture

The system consists of multiple nodes, each running an instance of the Raft FSM (Finite State Machine).

* **Leader:** Handles all client requests and replicates log entries to followers.
* **Followers:** Replicate the leader's log and step up for election if the leader fails.
* **Storage:** Uses an LSM-tree or BoltDB backend (standardize based on your implementation) for persistent storage.

---

## Getting Started

### Prerequisites

* Go 1.21+
* `protoc` (if using gRPC)

### Installation

```bash
git clone https://github.com/saharshmaloo/raft_kv.git
cd raft_kv
go mod tidy
go test -race ./...
```
