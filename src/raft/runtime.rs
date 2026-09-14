use std::collections::{HashMap, HashSet};

use prost::Message;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{
    log::Log,
    raft::{
        Error,
        storage::Storage,
        types::{
            AppliedEntries, Effect, Event, FSM, LocalEntry, LogEntry, NodeId, Operation,
            OperationId, Progress, Raft, Result as RaftResult, Role, RuntimeMessage, State,
        },
    },
};

pub struct Runtime<F: FSM> {
    raft: Raft,
    log: Log,
    storage: Storage,
    fsm: F,
    next_operation_id: u64,
    operations: HashMap<OperationId, Operation>,
    buf: Vec<u8>,
}

impl<F: FSM> Runtime<F> {
    pub fn new(id: NodeId, voters: impl IntoIterator<Item = NodeId>, log: Log, fsm: F) -> Self {
        let mut storage = Storage::new(log.dir());
        let state = storage.load_state();
        let next_index = log.next_offset();
        let last_log_index = next_index.saturating_sub(1);
        let voters: HashMap<_, _> = voters
            .into_iter()
            .map(|voter| {
                let match_index = if voter == id { last_log_index } else { 0 };
                (
                    voter,
                    Progress {
                        next_index,
                        match_index,
                    },
                )
            })
            .collect();
        assert!(voters.contains_key(&id), "the local node must be a voter");

        let raft = Raft {
            id,
            voters,
            learners: HashMap::new(),
            role: Role::Follower,
            current_term: state.current_term,
            leader_id: None,
            voted_for: state.voted_for,
            last_applied: 0,
            commit_index: 0,
            last_log_index: 0,
            last_log_term: 0,
            granted_votes: HashSet::new(),
        };

        Self {
            storage,
            raft,
            log,
            fsm,
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
        let (event, operation_id) = match message {
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

                (Event::Write(operation_id), operation_id)
            }
        };

        if let Err(err) = self.drive(event).await {
            self.fail_operation(operation_id, err);
        }
    }

    async fn drive(&mut self, mut event: Event) -> RaftResult<()> {
        loop {
            let Some(effect) = self.raft.handle(event)? else {
                return Ok(());
            };

            let Some(next_event) = self.execute(effect).await? else {
                return Ok(());
            };

            event = next_event;
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
            Effect::SendRequestVotes {
                hard_state,
                peers,
                request,
            } => {
                self.persist_hard_state(&hard_state)?;
                todo!("send RequestVote through the Raft transport")
            }

            Effect::SendAppendEntries {
                operation_id,
                targets,
            } => {
                for target in targets {}

                Ok(None)
            }

            Effect::SendRequestVoteResponse {
                hard_state,
                peer,
                response,
            } => {
                if let Some(hard_state) = hard_state {
                    self.persist_hard_state(&hard_state)?;
                }
                todo!("send RequestVoteResponse through the Raft transport")
            }

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
            } => {
                let first_unapplied = self.raft.last_applied.saturating_add(1);
                for index in first_unapplied..=through {
                    self.buf.clear();
                    self.log
                        .read(index, &mut self.buf)
                        .map_err(|err| Error::ReadCommittedEntryFailed(err.to_string()))?;

                    let entry = LogEntry::decode(self.buf.as_slice())
                        .map_err(|err| Error::EntryDeserialisationFailed(err.to_string()))?;

                    self.fsm
                        .apply(&entry.command)
                        .map_err(|err| Error::ApplyCommittedEntryFailed(err.to_string()))?;
                }

                Ok(Some(Event::EntriesApplied(AppliedEntries {
                    operation_id,
                    through,
                })))
            }

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

                self.operations
                    .insert(operation_id, Operation::Write { data: None, reply });

                encode_result.map_err(|err| Error::EntrySerialisationFailed(err.to_string()))?;

                let index = self
                    .log
                    .append(&self.buf)
                    .map_err(|_| Error::AppendFailed)?;

                Ok(Some(Event::LocalEntriesAppended(LocalEntry {
                    term,
                    operation_id,
                    index,
                })))
            }
            Effect::CompleteOperation { operation_id } => {
                let operation = self.get_operation(&operation_id);
                let Operation::Write { reply, .. } = operation else {
                    panic!("CompleteOperation effect must reference a write operation");
                };
                let _ = reply.send(Ok(()));
                Ok(None)
            }
        }
    }

    fn persist_hard_state(&mut self, state: &State) -> RaftResult<()> {
        self.storage
            .save_state(state)
            .map_err(|err| Error::PersistStateFailed(err.to_string()))
    }

    fn fail_operation(&mut self, operation_id: OperationId, err: Error) {
        let Some(operation) = self.operations.remove(&operation_id) else {
            return;
        };

        match operation {
            Operation::Write { reply, .. } => {
                let _ = reply.send(Err(err));
            }
            Operation::Vote { reply } => {
                let _ = reply.send(Err(err));
            }
            Operation::AppendEntries { reply, .. } => {
                let _ = reply.send(Err(err));
            }
        }
    }

    fn get_operation(&mut self, operation_id: &OperationId) -> Operation {
        self.operations
            .remove(&operation_id)
            .expect("newly inserted operation must exist")
    }

    fn shutdown() {}
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        fs,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use object_pool::Pool;
    use tokio::sync::oneshot;

    use super::*;
    use crate::log::Config;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "raft-runtime-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct RecordingFsm {
        applied: Vec<Vec<u8>>,
    }

    impl FSM for RecordingFsm {
        type Error = Infallible;

        fn apply(&mut self, command: &[u8]) -> std::result::Result<(), Self::Error> {
            self.applied.push(command.to_vec());
            Ok(())
        }
    }

    fn log(directory: &TestDirectory) -> Log {
        Log::new(
            Config {
                inital_offset: 1,
                sync_writes: false,
                max_size_bytes: 1024,
                max_store_bytes: 1024 * 1024,
            },
            directory.0.clone(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn single_node_write_is_persisted_applied_and_replied_to() {
        let directory = TestDirectory::new();
        let id = NodeId("node-1".into());
        let mut runtime = Runtime::new(id.clone(), [id], log(&directory), RecordingFsm::default());
        runtime.raft.role = Role::Leader;
        runtime.raft.current_term = 1;

        let pool = Arc::new(Pool::new(1, Vec::new));
        let mut data = pool.pull_owned(Vec::new);
        data.extend_from_slice(b"set x=1");
        let (reply, response) = oneshot::channel();

        runtime
            .handle_message(RuntimeMessage::Write { data, reply })
            .await;

        assert!(response.await.unwrap().is_ok());
        assert_eq!(runtime.raft.commit_index, 1);
        assert_eq!(runtime.raft.last_applied, 1);
        assert_eq!(runtime.fsm.applied, vec![b"set x=1".to_vec()]);
        assert!(runtime.operations.is_empty());
    }
}
