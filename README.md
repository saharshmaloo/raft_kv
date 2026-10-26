# Go-Raft KV 🚀

A learning project implementing the **Raft Consensus Algorithm** in Go for a distributed key-value store.

`ConsensusModule` in `raft.go` holds the protocol logic; `Server` in `server.go` manages TCP `net/rpc` connections and drives the consensus clock.

Serving a node starts its election timer with a timeout in [150ms, 300ms), checked every 10ms. On timeout it becomes a candidate, increments its term, votes for itself, and chooses a fresh timeout for another attempt. Shutdown stops the timer and waits for its goroutine. The timeout range follows the example in the [Raft paper, §5.2](https://raft.github.io/raft.pdf).

Timer tests advance time explicitly, without sleeps.

Run tests with `go test -race ./...`.

## Getting Started

### Prerequisites

* Go 1.21+

### Installation

```bash
git clone https://github.com/saharshmaloo/raft_kv.git
cd raft_kv
go mod tidy
go test -race ./...
```
