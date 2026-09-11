use std::net::SocketAddr;

use serf::{
    MemberlistOptions, Options,
    delegate::CompositeDelegate,
    event::{Event, EventProducer, EventSubscriber, MemberEventType},
    net::{NetTransportOptions, NodeId, TokioNetTransport},
    tokio::{TokioHostAddrResolver, TokioTcp, TokioTcpSerf},
    types::{HostAddr, Member},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type ConsulDelegate = CompositeDelegate<NodeId, SocketAddr>;
type SerfHandle = TokioTcpSerf<NodeId, TokioHostAddrResolver, ConsulDelegate>;

#[derive(Debug)]
struct Config {
    pub peer_addresses: Vec<String>,
    pub addr: String,
    pub advertise_addr: String,
    pub node_id: String,
    pub rpc_addr: String,
}

pub struct Membership {
    task: JoinHandle<()>,
    shutdown: CancellationToken,
    serf: SerfHandle,
}

impl Membership {
    /// Initialiases the serf binding to addr in config and joins using `peer_addresses`.
    /// this function panics because raft instance cannot run without discovery
    pub async fn new(cfg: Config) -> Self {
        // convert to SocketAddr;

        // A single [::]:PORT bind is used instead of separate v4/v6 binds:
        // this stream layer has no hook to set IPV6_V6ONLY, so binding both
        // 0.0.0.0 and [::] on the same port conflicts on platforms (Linux)
        // where the OS default is dual-stack.
        // Numeric, so the resolver parses it as an IP literal and never
        // touches DNS for the local bind address.
        let bind_addr: HostAddr = format!("[::]:{}", cfg.addr).parse().unwrap();
        let advertise_addr: SocketAddr = cfg.advertise_addr.parse().unwrap();
        let node_id: NodeId = cfg.node_id.parse().unwrap();
        let transport = NetTransportOptions::<NodeId, TokioHostAddrResolver, _>::new(node_id)
            .with_bind_addresses([bind_addr].into_iter().collect())
            .with_advertise_address(advertise_addr);

        let opts = Options::new()
            .with_memberlist_options(MemberlistOptions::local())
            .with_event_buffer_size(256)
            .with_tags([("rpc_addr", cfg.rpc_addr)].into_iter());

        let (producer, subscriber) = EventProducer::unbounded();
        let serf_server = TokioTcpSerf::with_event_producer(transport, opts, producer)
            .await
            .expect("");

        for peer in cfg.peer_addresses {
            // Kept as a domain name: resolved lazily (and re-resolved on the
            // resolver's TTL) whenever the transport actually dials it.
            let peer: HostAddr = peer.parse().unwrap();
            serf_server
                .join(serf::types::MaybeResolvedAddress::Unresolved(peer), false)
                .await
                .unwrap();
        }

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(Self::handle_serf_events(
            subscriber,
            serf_server.clone(),
            shutdown.clone(),
        ));

        Self {
            task,
            shutdown,
            serf: serf_server,
        }
    }

    async fn handle_serf_events(
        subscriber: EventSubscriber<
            TokioNetTransport<NodeId, TokioHostAddrResolver, TokioTcp>,
            ConsulDelegate,
        >,
        serf: SerfHandle,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                ev = subscriber.recv() => {
                    let Ok(ev) = ev else { break };

                    if let Event::Member(ev) = ev {
                        match ev.ty() {
                            MemberEventType::Join => Self::handle_member_join(&serf, ev.members()),
                            MemberEventType::Leave | MemberEventType::Failed => {
                                Self::handle_member_leave_or_failed(&serf, ev.members())
                            }
                            MemberEventType::Update => Self::handle_member_update(&serf, ev.members()),
                            MemberEventType::Reap => Self::handle_member_reap(&serf, ev.members()),
                        }
                    }
                }
            }
        }
    }

    fn handle_member_join(serf: &SerfHandle, members: &[Member<NodeId, SocketAddr>]) {
        for member in members {
            if member.node().id() == serf.local_id() {
                continue;
            }
            // addd to raft
        }
    }

    fn handle_member_leave_or_failed(serf: &SerfHandle, members: &[Member<NodeId, SocketAddr>]) {
        for member in members {
            if member.node().id() == serf.local_id() {
                continue;
            }
            // remove from raft
        }
    }

    fn handle_member_update(serf: &SerfHandle, members: &[Member<NodeId, SocketAddr>]) {
        for member in members {
            if member.node().id() == serf.local_id() {
                continue;
            }
            // update raft
        }
    }

    fn handle_member_reap(serf: &SerfHandle, members: &[Member<NodeId, SocketAddr>]) {
        for member in members {
            // what is this?
            // remove from raft
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    // IPv6 loopback throughout: the bind address is always `[::]:PORT`
    // (see the comment in `new`), which on macOS/BSD's default v6-only
    // socket behavior will not accept IPv4 connections, so peer addresses
    // must stay in the same address family to actually connect.
    fn config(port: u16, node_id: &str, peers: Vec<String>) -> Config {
        Config {
            peer_addresses: peers,
            addr: port.to_string(),
            advertise_addr: format!("[::1]:{port}"),
            node_id: node_id.to_string(),
            rpc_addr: format!("[::1]:{port}"),
        }
    }

    fn loopback_peer(port: u16) -> String {
        format!("[::1]:{port}")
    }

    async fn wait_for_member_count(membership: &Membership, expected: usize, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count = membership.serf.num_members().await;
            if count >= expected {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {expected} members, saw {count}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn membership_creates_server() {
        let solo = Membership::new(config(28_301, "solo", vec![])).await;

        assert_eq!(solo.serf.num_members().await, 1);
    }

    #[tokio::test]
    async fn two_instances_join_and_see_each_other() {
        let node1 = Membership::new(config(28_101, "node-1", vec![])).await;
        let node2 = Membership::new(config(28_102, "node-2", vec![loopback_peer(28_101)])).await;

        wait_for_member_count(&node1, 2, Duration::from_secs(5)).await;
        wait_for_member_count(&node2, 2, Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn three_instances_converge_via_single_seed() {
        let seed = Membership::new(config(28_201, "seed", vec![])).await;
        let follower1 =
            Membership::new(config(28_202, "follower-1", vec![loopback_peer(28_201)])).await;
        let follower2 =
            Membership::new(config(28_203, "follower-2", vec![loopback_peer(28_201)])).await;

        // Neither follower joins the other directly; convergence to a full
        // 3-member view on all three has to come from SWIM gossip fanning
        // out through the seed, not just from the explicit joins above.
        wait_for_member_count(&seed, 3, Duration::from_secs(5)).await;
        wait_for_member_count(&follower1, 3, Duration::from_secs(5)).await;
        wait_for_member_count(&follower2, 3, Duration::from_secs(5)).await;
    }
}
