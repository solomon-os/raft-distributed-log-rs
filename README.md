# Raft Core and Persistent-Log Prototype

> **Status:** learning prototype. The deterministic Raft core, segmented log, hard-state storage, and a minimal single-node write path are implemented and tested. The multi-node runtime, timers, and Raft gRPC transport are intentionally unfinished, so this repository does not currently start a working cluster.

This project explores how a consensus protocol can be modeled as a deterministic state machine while remaining separate from networking, storage, timers, and application code. It was built from first principles in Rust to study both Raft's protocol rules and the systems concerns surrounding them.

The objective was not to depend on an existing Raft implementation. The objective was to understand and implement the boundaries that make consensus code testable:

~~~text
(current Raft state, Event)
            |
            v
       Raft::handle
            |
            v
  (new Raft state, Effect)
~~~

## What this project demonstrates

- Modeling a consensus protocol as deterministic state transitions.
- Separating protocol decisions from asynchronous side effects.
- Serializing mutation through a single runtime owner.
- Tracking elections, terms, votes, logs, and follower progress.
- Making persistence-before-message ordering explicit.
- Building a segmented append-only log with memory-mapped indexes.
- Correlating a client request with later disk, replication, commit, and apply stages.
- Applying commands to an application FSM only after commit.
- Testing failure-sensitive behavior without requiring a network.
- Keeping service discovery separate from consensus membership.

## Current capabilities

### Raft core

The core implements and tests:

- Follower, candidate, and leader roles.
- Election timeout transitions and self-voting.
- RequestVote log-freshness checks.
- Adoption of a higher term before evaluating a vote request.
- Current-term vote-response validation.
- Duplicate-vote suppression by voter identity.
- Majority election calculation.
- Leader heartbeat generation and follower heartbeat handling.
- AppendEntries prefix validation.
- Follower progress using **next_index** and **match_index**.
- Retry effects for followers whose logs are behind.
- Majority-based commit advancement for a static voter set.
- Application of committed entries through an **FSM** boundary.

The core contains no socket, file, timer, Tokio, or gRPC calls. It only mutates Raft state and returns a description of work for the runtime.

### Persistent log

The log implementation includes:

- Append-only, length-prefixed records.
- Multiple size-bounded segments.
- A store file for record data.
- A memory-mapped index from logical offsets to store positions.
- Segment rotation.
- Reads across segments.
- Recovery of existing segment files.
- Removal of trailing segments during truncation.
- Optional synchronous writes.

### Hard-state storage

Raft's durable election state has dedicated storage:

~~~text
current_term
voted_for
~~~

The storage format supports overwriting and clearing votes, Unicode node identifiers, reopening storage, and maximum **u64** terms.

Vote-send effects carry the hard state that must be saved first. The runtime persists it before reaching the unfinished transport boundary:

~~~text
change term or vote
        |
        v
persist hard state
        |
        v
send vote request or response
~~~

### Runtime and FSM boundary

The runtime owns the mutable Raft core, persistent log, hard-state storage, pending-operation table, reusable encoding buffer, and application FSM.

Application state is represented by:

~~~rust
pub trait FSM {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&mut self, command: &[u8]) -> Result<(), Self::Error>;
}
~~~

The runtime reads and decodes committed log entries, then calls **FSM::apply** in index order. The core advances **last_applied** only after the runtime reports that application completed.

A tested single-node write follows the complete local path:

~~~text
client write
    |
    v
Event::Write
    |
    v
Effect::AppendLocal
    |
    v
persist encoded LogEntry
    |
    v
Event::LocalEntriesAppended
    |
    v
single-node commit
    |
    v
Effect::ApplyCommitted
    |
    v
FSM::apply
    |
    v
Effect::CompleteOperation
    |
    v
client one-shot response
~~~

Multi-node execution stops at the transport placeholders and is not claimed as complete.

## Architecture

~~~text
                         External world
                               |
             +-----------------+-----------------+
             |                 |                 |
             v                 v                 v
           Timers          Raft RPCs        Client RPCs
             |                 |                 |
             +-----------------+-----------------+
                               |
                               v
                         Runtime messages
                               |
                               v
                       +---------------+
                       | Single runtime|
                       | task / owner  |
                       +---------------+
                               |
                               | Event
                               v
                       +---------------+
                       |   Raft core   |
                       | deterministic |
                       +---------------+
                               |
                               | Effect
                               v
             +-----------------+-----------------+
             |                 |                 |
             v                 v                 v
         Persistence       Transport           FSM
~~~

This design avoids sharing Raft through **Arc&lt;Mutex&lt;Raft&gt;&gt;**. Other tasks communicate with the runtime through a bounded Tokio channel. Only one task owns and mutates protocol state, making ordering and invariants easier to reason about.

## Events and effects

An **Event** describes something that has already happened:

- An election or heartbeat timer fired.
- A vote request or response arrived.
- AppendEntries arrived or completed.
- Entries were persisted or applied.
- A client requested a write.

An **Effect** describes work the core has decided must happen elsewhere:

- Persist an entry or hard state.
- Send a vote request or response.
- Send AppendEntries or a heartbeat.
- Apply committed entries.
- Complete a pending client operation.

This separation lets unit tests inspect protocol decisions directly without mocking sockets, files, or clocks.

## Operation tracking

A write may pass through local persistence, replication, majority acknowledgement, commitment, and FSM application before its caller receives a response.

The runtime assigns a local **OperationId** and stores the corresponding one-shot sender in an operation table. Events and effects carry that identifier through the lifecycle. Intermediate stages leave the sender untouched. Terminal completion or failure removes the operation and sends exactly one response.

The identifier is runtime-local. It is not persisted and does not provide durable client deduplication or exactly-once semantics.

## Static membership boundary

The runtime accepts a fixed voter set when constructed. Replication progress may change, but being slow or caught up never changes whether a node is a voter.

Learner structures remain as protocol experiments, but automatic promotion and demotion are disabled. Correct voter-set changes require committed configuration entries and joint consensus, neither of which is implemented here.

Serf is used only as an address-discovery experiment. A Serf join, failure, or reap event must not directly modify the Raft voter set. Failure detection and consensus membership are separate concerns.

## Repository map

| Path | Purpose |
| --- | --- |
| [src/raft/core.rs](src/raft/core.rs) | Deterministic event handling and protocol transitions. |
| [src/raft/types.rs](src/raft/types.rs) | Raft state, events, effects, messages, operations, errors, and the FSM trait. |
| [src/raft/runtime.rs](src/raft/runtime.rs) | Effect chaining, local persistence, FSM application, and operation completion. |
| [src/raft/storage.rs](src/raft/storage.rs) | Durable term and vote storage. |
| [src/log.rs](src/log.rs) | Segmented log coordination, append, read, rotation, and truncation. |
| [src/log/segment.rs](src/log/segment.rs) | One store/index segment pair. |
| [src/log/store.rs](src/log/store.rs) | Length-prefixed record storage. |
| [src/log/index.rs](src/log/index.rs) | Memory-mapped offset-to-position index. |
| [src/membership.rs](src/membership.rs) | Serf discovery experiment, disconnected from Raft membership. |
| [src/grpc/client_transport.rs](src/grpc/client_transport.rs) | Early client-facing gRPC adapter. |
| [proto/client.proto](proto/client.proto) | Client write service schema. |
| [proto/raft.proto](proto/raft.proto) | Placeholder Raft service schema. |

## Testing

Run the complete test suite with:

~~~sh
cargo test --all-targets
~~~

The current suite contains **112 passing tests** covering the log, storage, membership experiment, Raft transitions, and the minimal runtime/FSM path.

Notable cases include:

- A higher-term vote request updates local term even when its log is stale.
- A stale vote response cannot elect a leader.
- Duplicate responses from one voter count only once.
- A leader plus one follower forms a majority in a three-voter cluster.
- A new multi-node entry is not committed before a majority responds.
- A lagging voter remains a voter.
- A caught-up learner is not promoted without a configuration change.
- A committed single-node write is persisted, applied, and acknowledged.

These are primarily deterministic unit tests. They do not simulate a complete multi-node runtime, partitions, message reordering, or crash/restart execution.

## Building

The library and test targets compile with:

~~~sh
cargo check --all-targets
~~~

There is deliberately no cluster startup command. **src/main.rs** is empty, and network-facing runtime effects are not implemented. Running **cargo run** does not launch a Raft node.

## Intentionally unfinished

- Complete multi-node runtime effect execution.
- Real Raft gRPC transport and non-empty Raft protobuf messages.
- Election and heartbeat timer wiring.
- Application startup, shutdown, and configuration parsing.
- Serf-to-runtime address updates.
- Reconstruction of **last_log_index** and **last_log_term** during restart.
- Segment-local conflict truncation.
- Full crash recovery and committed-entry replay.
- Dynamic membership and joint consensus.
- Production learner support.
- Snapshots and InstallSnapshot.
- Pre-vote, leadership transfer, ReadIndex, and lease reads.
- Replication batching and pipelining.
- Deterministic multi-node simulation and failure injection.
- Production observability and hardening.

## Known correctness limitations

- Several network-facing runtime effect arms contain **todo!()** and panic if exercised.
- **raft.proto** contains empty placeholder messages.
- The runtime restores hard state but does not reconstruct log metadata or replay committed entries.
- Persistence ordering is represented for voting paths, but the complete persistence/transport contract is not implemented for every higher-term transition.
- Log conflict recovery is incomplete within an existing segment.
- Replication transitions have unit coverage but no end-to-end multi-node execution.
- The application FSM has no snapshot or restore mechanism.
- Client requests do not have durable idempotency keys.

These limitations mean the project must not be used as a correctness-critical consensus implementation.

## Design lessons

### Protocol state and runtime state are different

Terms, votes, log positions, roles, and replication progress belong to Raft. Channels, buffers, sockets, tasks, and pending callers belong to the runtime. Combining them makes protocol tests harder and failure ordering less visible.

### Persistence is part of the protocol

Persisting a term or vote is not optional bookkeeping. A node must not send a vote-related message based on state it could forget after a crash.

### Replication progress is not membership

A follower being ten entries behind is a replication concern, not permission to remove it from the quorum. Membership changes require consensus of their own.

### Committed and applied are different

Commitment means a log position is safe according to Raft's quorum rules. Application means the corresponding command has been executed against application state. The runtime preserves that boundary through **ApplyCommitted** and **EntriesApplied**.

### Failure detectors are advisory

Serf can help locate nodes and report suspected failures, but it cannot authoritatively change the consensus configuration.

## Project boundary

This repository demonstrates protocol-state modeling, side-effect isolation, durable metadata, segmented storage, quorum reasoning, and asynchronous-operation correlation.

Completing it as a production Raft implementation would be a separate engineering phase requiring a fully specified transport and persistence contract, deterministic cluster simulation, failure injection, crash testing, snapshots, configuration changes, and operational hardening.
