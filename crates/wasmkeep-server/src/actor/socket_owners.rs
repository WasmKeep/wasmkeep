//! Closing the sockets of actors this node does not own any more. A socket
//! outlives its actor's stay in memory (it hibernates with sockets open,
//! releasing its ownership record), so another node may take the actor
//! while its sockets stay here, or this node's lease may run out under
//! them. Two background tasks close such sockets with 1012, so their
//! clients reconnect to the owner: the owner sweep, and the close at the
//! end of the node lease's validity. An event on such a socket closes all
//! of them too ([`ActorState::ws_prepare`]).

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

/// Closes every socket on this node with 1012 when its lease's validity
/// ends (ADR 0020: no acknowledged write for ttl minus safety), at that
/// moment rather than at the next write that lands: from then on no event
/// may run for them, and a peer may claim their actors once ttl plus grace
/// passed since the last write, later than this. Also when the epoch moved
/// since (a lapse this task saw late, or the lease made invalid), since
/// every hold of the earlier epoch has ended. Once per lapse; nothing
/// before the first valid lease. Ends when the cluster goes.
pub(super) async fn close_sockets_at_lapse(
    service: std::sync::Weak<ServiceInner>,
    mut changes: tokio::sync::watch::Receiver<wasmkeep_cluster::holds::Validity>,
) {
    // The epoch of the validity sockets were opened under, while valid.
    let mut armed: Option<u64> = None;
    loop {
        // Marked seen before the check: a change landing after it wakes the
        // wait below at once.
        let validity = *changes.borrow_and_update();
        let now = now_ms();
        let valid = validity.valid(now);
        if let Some(epoch) = armed
            && (!valid || validity.epoch != epoch)
        {
            armed = None;
            let Some(inner) = service.upgrade() else {
                return;
            };
            let closed = inner.sockets.close_all(1012, "node lost its lease");
            if closed > 0 {
                tracing::warn!(
                    sockets = closed,
                    "this node's lease ran out: its WebSockets are closed"
                );
                crate::metrics::owner_closes("lease_lapse", closed);
            }
        }
        if valid {
            armed.get_or_insert(validity.epoch);
            let left = validity.until_ms.unwrap_or(now).saturating_sub(now);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(left)) => {}
                changed = changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        } else if changes.changed().await.is_err() {
            return;
        }
    }
}
