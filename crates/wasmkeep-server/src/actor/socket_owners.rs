//! Closing the sockets of actors this node does not own any more. A socket
//! outlives its actor's stay in memory (it hibernates with sockets open,
//! releasing its ownership record), so another node may take the actor
//! while its sockets stay here. The owner sweep closes such sockets with
//! 1012, so their clients reconnect to the owner. An event on such a
//! socket closes all of them too ([`ActorState::ws_prepare`]).

use super::*;

/// Ownership records the sweep reads at once.
const SWEEP_PARALLEL: usize = 16;
/// How long the sweep waits for one record; past it, it closes nothing.
const SWEEP_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Every [`Cluster::socket_sweep`]: for each actor with sockets on this
/// node and no hold here (it hibernated with them open), reads its
/// ownership record once, and closes its sockets with 1012 if another live
/// node holds it. Events check ownership, but pings do not: without this, a
/// client that only listens would stay connected to a room another node
/// now runs, and miss every broadcast without a close. Skipped while this
/// node's lease is not valid (the close at the lapse handles that); a read
/// that fails closes nothing.
pub(super) async fn sweep_sockets(service: std::sync::Weak<ServiceInner>, cluster: Arc<Cluster>) {
    let every = cluster.socket_sweep;
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return; // shutting down: every socket is closed already
        }
        if !cluster.node_lease_valid() {
            continue;
        }
        let unheld: Vec<ActorAddress> = inner
            .sockets
            .owners()
            .into_iter()
            .filter(|address| !cluster.holds_here(address))
            .collect();
        futures_util::StreamExt::for_each_concurrent(
            futures_util::stream::iter(unheld),
            SWEEP_PARALLEL,
            |address| {
                let (inner, cluster) = (&inner, &cluster);
                async move {
                    let Ok(Ok(Some(owner))) =
                        tokio::time::timeout(SWEEP_READ_TIMEOUT, cluster.other_owner(&address))
                            .await
                    else {
                        return;
                    };
                    // Taken here meanwhile: the sockets are where they belong.
                    if cluster.holds_here(&address) {
                        return;
                    }
                    let closed = inner
                        .sockets
                        .close_owner(&address, 1012, "actor moved; reconnect");
                    if closed > 0 {
                        tracing::info!(actor = %address, %owner, sockets = closed, "closed sockets of an actor another node holds");
                        crate::metrics::owner_closes("sweep", closed);
                    }
                }
            },
        )
        .await;
        drop(inner);
    }
}
