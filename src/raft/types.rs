use std::collections::HashMap;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(pub(super) u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub(super) String);

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
        peers: Vec<NodeId>,
        request: VoteRequest,
    },
    SendAppendEntries {
        operation_id: OperationId,
        targets: Vec<ReplicationTarget>,
    },
    SendRequestVoteResponse {
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
    pub(super) current_votes: u64,
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
pub struct State {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}
