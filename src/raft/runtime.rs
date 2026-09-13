use std::collections::HashMap;

use object_pool::ReusableOwned;
use prost::Message;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};
use tokio_util::sync::CancellationToken;

use crate::{
    log::Log,
    raft::{
        Error,
        storage::Storage,
        types::{
            AppendEntriesResponse, Effect, Event, LocalEntry, LogEntry, NodeId, Operation,
            OperationId, Progress, Raft, Result as RaftResult, Role, RuntimeMessage,
        },
    },
};

pub struct Runtime {
    raft: Raft,
    log: Log,
    storage: Storage,
    next_operation_id: u64,
    operations: HashMap<OperationId, Operation>,
    buf: Vec<u8>,
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

        Self {
            storage,
            raft,
            log,
            next_operation_id: 1,
            operations: HashMap::new(),
            buf: Vec::with_capacity(1024),
        }
    }

    fn next_operation_id(&mut self) -> OperationId {
        let operation_id = OperationId(self.next_operation_id);
        self.next_operation_id = self
            .next_operation_id
            .checked_add(1)
            .expect("operation ID space exhausted");
        operation_id
    }

    async fn handle_message(&mut self, message: RuntimeMessage) {
        match message {
            RuntimeMessage::Write { data, reply } => {
                let operation_id = self.next_operation_id();
                let replaced = self.operations.insert(
                    operation_id,
                    Operation::Write {
                        data: Some(data),
                        reply,
                    },
                );
                debug_assert!(replaced.is_none());

                match self.raft.handle(Event::Write(operation_id)) {
                    Ok(_effect) => {}
                    Err(err) => {
                        let operation = self
                            .operations
                            .remove(&operation_id)
                            .expect("newly inserted operation must exist");
                        let Operation::Write { reply, .. } = operation else {
                            panic!("write event must reference a write operation");
                        };
                        let _ = reply.send(Err(err));
                    }
                }
            }
        }
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

    async fn execute(&mut self, effect: Effect) -> RaftResult<Option<Event>> {
        match effect {
            Effect::SendRequestVotes { peers, request } => todo!(),

            Effect::SendAppendEntries {
                operation_id,
                targets,
            } => {
                for target in targets {}

                Ok(None)
            }

            Effect::SendRequestVoteResponse { peer, response } => todo!(),

            Effect::SendHeartbeat { peers, request } => todo!(),

            Effect::SendHeartbeatResponse {
                peer,
                response,
                reset_election_timer,
                apply_through,
            } => todo!(),

            Effect::PersistEntries {
                operation_id,
                peer,
                response,
                truncate_after,
                leader_commit_index,
            } => todo!(),
            Effect::ApplyCommitted {
                operation_id,
                through,
            } => todo!(),

            Effect::RejectAppendEntries {
                operation_id,
                peer,
                response,
            } => {
                let operation = self.get_operation(&operation_id);
                let Operation::AppendEntries { reply, .. } = operation else {
                    panic!("operation must exist for append entries")
                };

                reply.send(Ok(response)).map_err(|_| {
                    Error::RejectingEntryFailed("response receiver was dropped".into())
                })?;

                Ok(None)
            }

            Effect::AppendLocal {
                operation_id,
                term,
                last_log_index,
            } => {
                // write to log
                let operation = self
                    .operations
                    .remove(&operation_id)
                    .expect("newly inserted operation must exist");

                let Operation::Write { data, reply } = operation else {
                    panic!("AppendLocal effect must reference a write operation");
                };

                let data = data.expect("write data must exist before it is appended locally");

                self.buf.clear();

                // Temporarily detach the command buffer so it can be owned by the
                // log entry, then return it to the pool after encoding.
                let (pool, command) = data.detach();
                let entry = LogEntry { term, command };

                let encode_result = entry.encode(&mut self.buf);
                pool.attach(entry.command);
                encode_result.map_err(|err| Error::EntrySerialisationFailed(err.to_string()))?;

                let index = self
                    .log
                    .append(&self.buf)
                    .map_err(|_| Error::AppendFailed)?;

                self.operations
                    .insert(operation_id, Operation::Write { data: None, reply });

                Ok(Some(Event::LocalEntriesAppended(LocalEntry {
                    term,
                    operation_id,
                    index,
                })))
            }
            Effect::CompleteOperation { operation_id } => todo!(),
        }
    }

    fn get_operation(&mut self, operation_id: &OperationId) -> Operation {
        self.operations
            .remove(&operation_id)
            .expect("newly inserted operation must exist")
    }

    fn shutdown() {}
}
