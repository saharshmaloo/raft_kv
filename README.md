# Go-Raft KV 🚀

A high-performance, distributed key-value store built in Go, powered by the **Raft Consensus Algorithm**. This project ensures strong consistency and high availability across a cluster of nodes.

## Features

* **Strong Consistency:** Linearizable reads and writes via the Raft protocol.
* **Fault Tolerance:** The cluster remains operational as long as a quorum (majority) of nodes are healthy.
* **Dynamic Membership:** Support for adding or removing nodes from the cluster.
* **Log Compaction:** Implements snapshotting to prevent log explosion and speed up restarts.
* **Simple API:** Easy-to-use HTTP/gRPC interface for `GET`, `SET`, and `DELETE` operations.

---

## Architecture

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
go build -o raft_kv