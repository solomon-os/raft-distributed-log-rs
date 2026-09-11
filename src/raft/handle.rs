use crate::raft::types::RuntimeMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct Handle {
    pub(super) rx: mpsc::Receiver<RuntimeMessage>,
    pub tx: mpsc::Sender<RuntimeMessage>,
    pub shutdown: CancellationToken,
}

impl Handle {}
