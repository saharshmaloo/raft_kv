# Project instructions

## Purpose

This is a **learning project**: the goal is to learn the Raft consensus algorithm by implementing it in Go, with a distributed key-value store as the application. Understanding the protocol and its correctness matters more than feature count, performance, or production readiness.

The README describes intended capabilities, not necessarily implemented behavior. Inspect the code before claiming a feature exists. Storage, networking, and API choices remain open until implemented or explicitly decided.

## How to collaborate

- Act as a learning partner. Explain the Raft concepts, invariants, and tradeoffs behind meaningful changes in plain language.
- Make small, focused changes that are easy to read, run, and reason about. Avoid implementing several future milestones at once.
- Follow the user's requested scope. When asked for guidance or review, help them reason through the problem without automatically writing the solution. When asked to implement something, do the work and explain it.
- Favor straightforward code and explicit state transitions over clever abstractions or premature optimization.
- Implement Raft itself rather than replacing it with an existing consensus library. Supporting libraries are fine when they do not hide the protocol being learned.
- Document why subtle protocol rules exist, especially where a seemingly simpler implementation would violate safety or liveness.

## Implementation approach

Build incrementally: establish node state and RPCs, leader election, log replication and commitment, persistence and recovery, then the key-value state machine and client behavior. Add snapshotting, membership changes, and performance work when the foundations are understood and tested. This is guidance, not a requirement to implement everything up front.

- Use the Raft paper as the protocol reference, especially Figure 2. Call out intentional simplifications and their limitations.
- Keep protocol logic separate from networking, storage, timers, and the key-value state machine where that makes behavior easier to test and understand.
- Be explicit about persistent versus volatile state and when persistence must happen before responding to an RPC.
- Preserve Raft's safety rules around terms, voting, log matching, election restrictions, and committing entries. Explain correctness-sensitive decisions before changing them.
- Do not assume that a leader can serve linearizable reads merely because it believes it is leader; implement and explain the required authority checks when reads are introduced.

## Verification and documentation

- Add focused tests as behavior is introduced, including failure cases such as split votes, stale RPCs, conflicting logs, node restarts, and network partitions when relevant.
- Prefer controllable clocks and deterministic event delivery for protocol tests. Avoid tests that depend on arbitrary sleeps.
- Run checks appropriate to the change; use Go's race detector when concurrent code exists. Report what was checked and any remaining limitations.
- Keep documentation aligned with actual behavior. Distinguish implemented features from planned ones, and avoid unsupported claims of production readiness, high availability, or linearizability.
- Leave a brief explanation of what changed, why it matters to Raft, and what the user can run or inspect to understand it.
