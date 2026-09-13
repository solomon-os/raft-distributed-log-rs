# Raft Core and Persistent-Log Prototype

This repository is a learning prototype for implementing the core ideas behind Raft in Rust. It is intentionally preserved as a protocol-core and storage project, not presented as a runnable or production-ready distributed system.

## What is implemented

- A deterministic Raft core expressed as `Event -> state transition -> Effect`.
- Follower, candidate, and leader state transitions.
- RequestVote handling with log freshness checks and higher-term adoption.
- Current-term vote-response validation and duplicate-vote suppression.
- AppendEntries validation and follower progress tracking.
- Majority-based commit advancement for a static voter set.
- A segmented append-only log with memory-mapped indexes.
- Durable hard state for `current_term` and `voted_for`.
- Runtime-local operation tracking with one-shot client responses.
- An `FSM` trait that receives commands only after their log entries are committed.
- A minimal single-node write path covering persistence, commit, FSM application, and client completion.
- Serf-based address-discovery experiments that remain separate from Raft membership decisions.

The current suite contains 112 passing tests:

```sh
cargo test --all-targets
```

## Architecture

The Raft core does not perform I/O directly:

```text
timers / transport / client
            |
            v
          Event
            |
            v
       Raft::handle
            |
            v
          Effect
            |
            v
 persistence / transport / FSM
```

Only the runtime owns and mutates the Raft state. Commands are first persisted in the segmented log. Once the core decides that an index is committed, the runtime reads and decodes each unapplied entry and calls:

```rust
pub trait FSM {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&mut self, command: &[u8]) -> Result<(), Self::Error>;
}
```

Hard-state data is carried alongside vote-send effects so the runtime can persist the term and vote before a corresponding vote message becomes externally visible.

## Intentionally unfinished

The following pieces are scaffolding or placeholders:

- The multi-node runtime effect executor.
- Real Raft gRPC transport and non-empty Raft protobuf messages.
- Election and heartbeat timer wiring.
- Application startup and shutdown wiring in `main`.
- Serf-to-runtime address updates.
- Reconstruction of `last_log_index` and `last_log_term` when reopening the log.
- Segment-local conflict truncation.
- Full crash recovery and replay.
- Dynamic membership and learner promotion.
- Snapshots, InstallSnapshot, pre-vote, ReadIndex, batching, and production hardening.

The voter set is treated as static. Serf discovery must not add, remove, promote, or demote voters: changing a Raft quorum requires a committed configuration-change protocol, which this prototype does not implement.

## Known limitations

- Several network-facing runtime effect arms still contain `todo!()` and will panic if exercised.
- `raft.proto` contains service placeholders rather than complete wire messages.
- The executable entry point is empty, so this repository does not currently start a cluster.
- Persistence ordering is represented for voting paths, but the complete transport/persistence pipeline is not implemented.
- Log replication and conflict recovery have unit-tested state transitions but no end-to-end multi-node execution.
- The current operation identifier is runtime-local and is not a durable client-idempotency mechanism.

## Project boundary

This project demonstrates protocol-state modeling, side-effect isolation, durable metadata, segmented storage, quorum reasoning, and asynchronous-operation correlation. Completing it as a production Raft implementation would require a separate engineering phase with failure injection, deterministic cluster simulation, crash testing, and a fully specified transport and persistence contract.
