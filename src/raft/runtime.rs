use std::collections::HashMap;

use crate::{
    log::Log,
    raft::types::{NodeId, Progress, Raft, Role},
};

pub struct RaftRuntime {
    raft: Raft,
}

impl RaftRuntime {
    pub fn new(id: NodeId, log: Log) -> Self {
        Self {
            raft: Raft {
                id: id.clone(),
                voters: HashMap::from([(
                    id.clone(),
                    Progress {
                        next_index: log.next_offset(),
                        match_index: log.next_offset().saturating_sub(1),
                    },
                )]),
                learners: HashMap::new(),
                role: Role::Follower,
                current_term: 1,
                leader_id: None,
                voted_for: None,
                last_applied: 0,
                commit_index: 0,
                last_log_index: 0,
                last_log_term: 0,
                current_votes: 0,
            },
        }
    }
}
