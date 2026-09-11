use object_pool::Pool;
use prost::Message;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    grpc::client_transport::pb_client::{
        WriteRequest, WriteResponse, client_service_server::ClientService,
    },
    raft,
};

pub mod pb_client {
    tonic::include_proto!("client");
}

pub struct ClientTransport {
    raft_handle: raft::Handle,
    pool: Arc<Pool<Vec<u8>>>,
}

impl ClientTransport {
    pub fn new(handle: raft::Handle) -> Self {
        Self {
            raft_handle: handle,
            pool: Arc::new(Pool::new(5, || Vec::with_capacity(1024))),
        }
    }
}

#[tonic::async_trait]
impl ClientService for ClientTransport {
    async fn write(
        &self,
        req: tonic::Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // match request into buf;
        let message = req.into_inner();
        let mut buf = self.pool.pull_owned(|| Vec::with_capacity(1024));
        if message.encoded_len() > buf.capacity() {
            buf.resize(message.encoded_len(), 0u8);
        }
        message
            .encode(&mut *buf)
            .map_err(|err| Status::internal(err.to_string()))?;

        let (tx, rx) = oneshot::channel();

        let raft_msg = raft::RuntimeMessage::Write {
            data: buf,
            reply: tx,
        };

        // send to the raft handle
        self.raft_handle
            .tx
            .send(raft_msg)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        match rx.await.map_err(|err| Status::internal(err.to_string()))? {
            Ok(_) => Ok(Response::new(WriteResponse { ok: true })),
            Err(raft::Error::NotLeader(Some(leader_id))) => Err(Status::failed_precondition(
                format!("node is not the leader; known leader: {leader_id}"),
            )),
            Err(raft::Error::NotLeader(None)) => Err(Status::failed_precondition(
                "node is not the leader; leader is unknown",
            )),
            Err(err) => Err(Status::internal(err.to_string())),
        }

        // extract error and if it's not leader handle it
    }
}
