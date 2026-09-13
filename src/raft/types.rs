use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use object_pool::ReusableOwned;
use tokio::sync::oneshot::Sender;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(pub(super) u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub(super) String);

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Role {
    Candidate,
    Follower,
    Leader,
}

pub enum Event {
    ElectionTimeout,
    HeartbeatTimeout,
    Heartbeat(HeartbeatRequest),
    RequestVote(VoteRequest),
    AppendEntries(ReceivedAppendEntries),
    AppendEntriesResponse(ReceivedAppendEntriesResponse),
    EntriesPersisted(PersistedEntries),
    EntriesApplied(AppliedEntries),
    VoteResponse(ReceivedVoteResponse),
    HeartbeatResponse(ReceivedHeartbeatResponse),
    LocalEntriesAppended(LocalEntry),
    Write(OperationId),
}

pub enum Effect {
    SendRequestVotes {
        hard_state: State,
        peers: Vec<NodeId>,
        request: VoteRequest,
    },
    SendAppendEntries {
        operation_id: OperationId,
        targets: Vec<ReplicationTarget>,
    },
    SendRequestVoteResponse {
        hard_state: Option<State>,
        peer: NodeId,
        response: VoteResponse,
    },
    SendHeartbeat {
        peers: Vec<NodeId>,
        request: HeartbeatRequest,
    },
    SendHeartbeatResponse {
        peer: NodeId,
        response: AppendEntriesResponse,
        reset_election_timer: bool,
        apply_through: Option<u64>,
    },
    PersistEntries {
        operation_id: OperationId,
        peer: NodeId,
        response: AppendEntriesResponse,
        truncate_after: Option<u64>,
        leader_commit_index: u64,
    },
    ApplyCommitted {
        operation_id: OperationId,
        through: u64,
    },
    RejectAppendEntries {
        operation_id: OperationId,
        peer: NodeId,
        response: AppendEntriesResponse,
    },
    AppendLocal {
        operation_id: OperationId,
        term: u64,
        last_log_index: u64,
    },
    CompleteOperation {
        operation_id: OperationId,
    },
}

pub enum RuntimeMessage {
    Write {
        data: ReusableOwned<Vec<u8>>,
        reply: Sender<Result<()>>,
    },
}

#[derive(Debug)]
pub struct Raft {
    pub(super) id: NodeId,
    pub(super) voters: HashMap<NodeId, Progress>,
    pub(super) learners: HashMap<NodeId, Progress>,
    pub(super) role: Role,
    pub(super) current_term: u64,
    pub(super) leader_id: Option<NodeId>,
    pub(super) voted_for: Option<NodeId>,
    pub(super) last_applied: u64,
    pub(super) commit_index: u64,
    pub(super) last_log_index: u64,
    pub(super) last_log_term: u64,
    pub(super) granted_votes: HashSet<NodeId>,
}

#[derive(Debug)]
pub(super) struct Progress {
    pub(super) next_index: u64,
    pub(super) match_index: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node is not the leader")]
    NotLeader(Option<NodeId>),
    #[error("node is not a candidate")]
    NotCandidate,
    #[error("appending to local storage failed")]
    AppendFailed,
    #[error("log entry serialisation failed")]
    EntrySerialisationFailed(String),
    #[error("channel is not able to recieve response for entry rejection")]
    RejectingEntryFailed(String),
    #[error("persisting Raft hard state failed: {0}")]
    PersistStateFailed(String),
    #[error("reading a committed log entry failed: {0}")]
    ReadCommittedEntryFailed(String),
    #[error("decoding a committed log entry failed: {0}")]
    EntryDeserialisationFailed(String),
    #[error("applying a committed log entry failed: {0}")]
    ApplyCommittedEntryFailed(String),
}

pub struct VoteRequest {
    pub(super) term: u64,
    pub(super) candidate_id: NodeId,
    pub(super) last_log_index: u64,
    pub(super) last_log_term: u64,
}

pub struct HeartbeatRequest {
    pub(super) term: u64,
    pub(super) leader_id: NodeId,
    pub(super) log_index: u64,
    pub(super) log_term: u64,
    pub(super) leader_commit_index: u64,
}

#[derive(Debug)]
pub struct VoteResponse {
    pub(super) term: u64,
    pub(super) vote_granted: bool,
}

pub struct AppendEntriesRequest {
    pub(super) term: u64,
    pub(super) leader_id: NodeId,
    pub(super) prev_log_index: u64,
    pub(super) prev_log_term: u64,
    pub(super) commit_index: u64,
}

pub struct AppendEntriesResponse {
    pub(super) term: u64,
    pub(super) success: bool,
    pub(super) last_log_index: u64,
    pub(super) last_log_term: u64,
    pub(super) leader_id: Option<NodeId>,
}

pub struct PersistedEntries {
    pub(super) operation_id: OperationId,
    pub(super) last_log_index: u64,
    pub(super) last_log_term: u64,
    pub(super) leader_commit_index: u64,
}

pub struct AppliedEntries {
    pub(super) operation_id: OperationId,
    pub(super) through: u64,
}

#[derive(Debug)]
pub struct ReceivedVoteResponse {
    pub(super) from: NodeId,
    pub(super) response: VoteResponse,
}

pub struct ReceivedHeartbeatResponse {
    pub(super) from: NodeId,
    pub(super) response: AppendEntriesResponse,
}

pub struct LocalEntry {
    pub(super) operation_id: OperationId,
    pub(super) index: u64,
    pub(super) term: u64,
}

#[derive(Debug)]
pub struct ReplicationTarget {
    pub(super) peer: NodeId,
    pub(super) last_log_index: u64,
    pub(super) last_log_term: u64,
    pub(super) leader_id: NodeId,
    pub(super) leader_commit_index: u64,
}

pub struct ReceivedAppendEntriesResponse {
    pub(super) operation_id: OperationId,
    pub(super) from: NodeId,
    pub(super) response: AppendEntriesResponse,
}

pub struct ReceivedAppendEntries {
    pub(super) operation_id: OperationId,
    pub(super) request: AppendEntriesRequest,
    pub(super) local_prev_log_term: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub command: Vec<u8>,
}

pub enum Operation {
    Write {
        data: Option<ReusableOwned<Vec<u8>>>,
        reply: Sender<Result<()>>,
    },
    Vote {
        reply: Sender<Result<VoteResponse>>,
    },
    AppendEntries {
        data: ReusableOwned<Vec<u8>>,
        reply: Sender<Result<AppendEntriesResponse>>,
    },
}

pub trait FSM {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&mut self, command: &[u8]) -> std::result::Result<(), Self::Error>;
}
