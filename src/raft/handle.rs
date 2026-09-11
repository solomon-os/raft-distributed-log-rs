use crate::raft::types::RuntimeMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct Handle {
    pub tx: mpsc::Sender<RuntimeMessage>,
    pub shutdown: CancellationToken,
}

impl Handle {}
