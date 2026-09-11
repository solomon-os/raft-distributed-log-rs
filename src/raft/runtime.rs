use std::{collections::HashMap, io::Result};

use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{
    log::Log,
    raft::{
        handle::Handle,
        storage::Storage,
        types::{NodeId, Progress, Raft, Role, RuntimeMessage},
    },
};

pub struct Runtime {
    raft: Raft,
    log: Log,
    storage: Storage,
}

impl Runtime {
    pub fn new(id: NodeId, log: Log) -> Self {
        let mut storage = Storage::new(log.dir());
        let state = storage.load_state();
        let raft = Raft {
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
            current_term: state.current_term,
            leader_id: None,
            voted_for: state.voted_for,
            last_applied: 0,
            commit_index: 0,
            last_log_index: 0,
            last_log_term: 0,
            current_votes: 0,
        };

        Self { storage, raft, log }
    }

    async fn handle_message(&self, message: RuntimeMessage) {
        match message {}
    }

    async fn run(mut self, shutdown: CancellationToken, mut rx: Receiver<RuntimeMessage>) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                },

                Some(message) = rx.recv() => {
                    self.handle_message(message).await;
                }
            }
        }
    }

    fn shutdown() {}
}
