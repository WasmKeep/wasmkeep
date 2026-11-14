//! Closing the sockets of actors this node does not own any more, and
//! releasing the records of parked ones (ADR 0024). A socket outlives its
//! actor's stay in memory: an actor that hibernates with sockets open
//! parks its ownership record, so it wakes here, where they are. Another
//! node takes it only if the record stopped naming this node (a fault, or
//! a node that does not park), or this node's lease ran out under them. Two background tasks close such sockets with 1012, so their
//! clients reconnect to the owner: the owner sweep, and the close at the
//! end of the node lease's validity. An event on such a socket closes all
//! of them too ([`ActorState::ws_prepare`]).
//!
//! The sweep's read and its close are not atomic with a wake on another
//! thread: a room this node takes back between the two has its sockets
//! closed once, needlessly. Closing is the safe direction, and its clients
//! reconnect.

use super::*;

/// Ownership records the sweep reads at once.
const SWEEP_PARALLEL: usize = 16;
/// How long the sweep waits for one record; past it, it closes nothing.
const SWEEP_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Most records one sweep pass reads; the next pass goes on where it
/// stopped, so every room is read in turn, at a bounded bucket cost.
const SWEEP_MAX_READS: usize = 5_000;
/// One sweep pass in this many also reads every parked record, as a
/// safeguard (ADR 0024, section 5); the others read none of them.
const PARKED_RECHECK_EVERY: u64 = 10;

/// Releases the parked record of each actor whose last socket slot was
/// freed (ADR 0024, section 3), unless it has a socket open again. The
/// sweep does the same for any it missed.
pub(super) async fn release_vacated(
    service: std::sync::Weak<ServiceInner>,
    mut vacated: tokio::sync::mpsc::UnboundedReceiver<ActorAddress>,
) {
    while let Some(address) = vacated.recv().await {
        let Some(inner) = service.upgrade() else {
            return;
        };
        let Some(cluster) = inner.cluster.clone() else {
            return;
        };
        if cluster.is_parked(&address) && !inner.sockets.has_open(&address) {
            let sockets = Arc::clone(&inner.sockets);
            drop(inner);
            tokio::spawn(async move {
                // Asked again under the lock's wait: a socket opened
                // meanwhile woke the room, which dropped the parked entry.
                if !sockets.has_open(&address) {
                    cluster.release_parked(&address, "last_socket").await;
                }
            });
        }
    }
}

/// Every sweep pass, before the reads: releases the parked records of
/// actors with no socket open here any more (the nudge of
/// [`release_vacated`] missed, or a socket closed without its slot being
/// freed yet counts as gone).
async fn release_unsocketed(inner: &ServiceInner, cluster: &Cluster) {
    for (address, _) in cluster.parked() {
        if !inner.sockets.has_open(&address) {
            cluster.release_parked(&address, "last_socket").await;
        }
    }
}

/// The safeguard re-read (ADR 0024, section 5): each parked record read
/// once; one that no longer names this node at the parked generation is
/// forgotten, and its room's sockets are swept as any other's. A failed
/// read stops the re-read until next time.
async fn recheck_parked(inner: &ServiceInner, cluster: &Cluster) {
    let stop = std::sync::atomic::AtomicBool::new(false);
    let parked: Vec<_> = cluster.parked().into_iter().take(SWEEP_MAX_READS).collect();
    futures_util::StreamExt::for_each_concurrent(
        futures_util::stream::iter(parked),
        SWEEP_PARALLEL,
        |(address, generation)| {
            let stop = &stop;
            async move {
                use std::sync::atomic::Ordering::Relaxed;
                if stop.load(Relaxed) || inner.mailboxes.is_closed() || !cluster.node_lease_valid()
                {
                    return;
                }
                crate::metrics::inc(&crate::metrics::METRICS.websocket_sweep_reads);
                if tokio::time::timeout(
                    SWEEP_READ_TIMEOUT,
                    cluster.drop_parked_if_stale(&address, generation),
                )
                .await
                .is_err()
                {
                    stop.store(true, Relaxed);
                }
            }
        },
    )
    .await;
}

/// Every [`Cluster::socket_sweep`]: releases parked records whose sockets
/// are gone, re-reads every parked record on one pass in
/// [`PARKED_RECHECK_EVERY`], then, for each actor with sockets on this
/// node (not closing), no hold here and no parked record (its record named
/// nobody when it hibernated: it does not park then), reads its ownership
/// record once, and closes its sockets with 1012 if
/// another live node holds it. Events check ownership, but pings do not:
/// without this, a client that only listens would stay connected to a
/// room another node now runs, and miss every broadcast without a close.
/// Skipped while this node's lease is not valid (the close at the lapse
/// handles that); a read that fails closes nothing, and ends the pass (the
/// bucket is slow or out of reach: the next pass tries again). At most
/// [`SWEEP_MAX_READS`] reads a pass. Holds neither the service nor the
/// cluster while it waits, so a node dropped meanwhile stops at once (its
/// lease is not kept renewing).
pub(super) async fn sweep_sockets(service: std::sync::Weak<ServiceInner>, every: Duration) {
    // Where the next pass starts, in the sorted list of rooms to read.
    let mut cursor = 0_usize;
    let mut pass = 0_u64;
    loop {
        tokio::time::sleep(every).await;
        pass = pass.wrapping_add(1);
        let Some(inner) = service.upgrade() else {
            return;
        };
        let Some(cluster) = inner.cluster.clone() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return; // shutting down: every socket is closed already
        }
        if !cluster.node_lease_valid() {
            continue;
        }
        release_unsocketed(&inner, &cluster).await;
        if pass.is_multiple_of(PARKED_RECHECK_EVERY) {
            recheck_parked(&inner, &cluster).await;
        }
        // A parked room is this node's own: no read (ADR 0024).
        let mut unheld: Vec<ActorAddress> = inner
            .sockets
            .open_owners()
            .into_iter()
            .filter(|address| !cluster.holds_here(address) && !cluster.is_parked(address))
            .collect();
        unheld.sort_unstable();
        let batch = window(&unheld, &mut cursor, SWEEP_MAX_READS);
        let stop = std::sync::atomic::AtomicBool::new(false);
        futures_util::StreamExt::for_each_concurrent(
            futures_util::stream::iter(batch),
            SWEEP_PARALLEL,
            |address| {
                let (inner, cluster, stop) = (&inner, &cluster, &stop);
                async move {
                    use std::sync::atomic::Ordering::Relaxed;
                    if stop.load(Relaxed)
                        || inner.mailboxes.is_closed()
                        || !cluster.node_lease_valid()
                    {
                        return;
                    }
                    crate::metrics::inc(&crate::metrics::METRICS.websocket_sweep_reads);
                    let read =
                        tokio::time::timeout(SWEEP_READ_TIMEOUT, cluster.other_owner(&address))
                            .await;
                    let owner = match read {
                        Ok(Ok(Some(owner))) => owner,
                        Ok(Ok(None)) => return,
                        Ok(Err(_)) | Err(_) => {
                            stop.store(true, Relaxed);
                            return;
                        }
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
        drop((inner, cluster));
    }
}

/// Up to `max` of `all` from `*cursor` on, wrapping around; moves the
/// cursor past them.
fn window<T: Clone>(all: &[T], cursor: &mut usize, max: usize) -> Vec<T> {
    if all.is_empty() {
        *cursor = 0;
        return Vec::new();
    }
    let start = *cursor % all.len();
    let taken: Vec<T> = all
        .iter()
        .cycle()
        .skip(start)
        .take(max.min(all.len()))
        .cloned()
        .collect();
    *cursor = start + taken.len();
    taken
}

/// Closes every socket on this node with 1012: its lease is no longer
/// valid.
pub(super) fn close_for_lapse(sockets: &Sockets) {
    let closed = sockets.close_all(1012, "node lost its lease");
    if closed > 0 {
        tracing::warn!(
            sockets = closed,
            "this node's lease is no longer valid: its WebSockets are closed"
        );
        crate::metrics::owner_closes("lease_lapse", closed);
    }
}

/// Closes every socket on this node with 1012 when its lease's validity
/// ends (ADR 0020: no acknowledged write for ttl minus safety), at that
/// moment rather than at the next write that lands: from then on no event
/// may run for them. Also when the epoch moved since (a lapse this task
/// saw late, or the lease made invalid), since every hold of the earlier
/// epoch has ended. A write that lands after a lapse also closes them,
/// before ending the holds ([`Cluster::on_lease_lapse`]), in case this
/// task has not run yet. Once per lapse; nothing before the first valid
/// lease. Ends when the cluster goes.
pub(super) async fn close_sockets_at_lapse(
    sockets: std::sync::Weak<Sockets>,
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
            let Some(sockets) = sockets.upgrade() else {
                return;
            };
            close_for_lapse(&sockets);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::Outgoing;
    use wasmkeep_cluster::holds::Validity;

    fn opened(sockets: &Sockets, name: &str) -> crate::websocket::Opened {
        let owner = ActorAddress::new("default", "chat", name).unwrap();
        let opened = sockets.open(owner).unwrap();
        sockets.accept(opened.id);
        opened
    }

    async fn closed(opened: &mut crate::websocket::Opened) -> (u16, String) {
        match tokio::time::timeout(Duration::from_secs(5), opened.outgoing.recv()).await {
            Ok(Some(Outgoing::Close(code, reason))) => (code, reason),
            Ok(Some(Outgoing::Message(_))) => panic!("a message, not a close"),
            Ok(None) => panic!("dropped without a close"),
            Err(_) => panic!("not closed"),
        }
    }

    #[tokio::test]
    async fn a_moved_epoch_closes_every_socket() {
        let sockets = Arc::new(Sockets::default());
        let mut one = opened(&sockets, "one");
        let far = now_ms() + 600_000;
        let (validity, changes) = tokio::sync::watch::channel(Validity {
            until_ms: Some(far),
            epoch: 1,
        });
        tokio::spawn(close_sockets_at_lapse(Arc::downgrade(&sockets), changes));
        tokio::task::yield_now().await;
        // Seen late: valid again already, in a later epoch.
        validity.send_replace(Validity {
            until_ms: Some(far),
            epoch: 2,
        });
        assert_eq!(closed(&mut one).await, (1012, "node lost its lease".into()));
        // Armed again in the new epoch: a socket opened now is closed at
        // the next lapse.
        let mut two = opened(&sockets, "two");
        validity.send_replace(Validity {
            until_ms: None,
            epoch: 3,
        });
        assert_eq!(closed(&mut two).await.0, 1012);
    }

    #[tokio::test]
    async fn the_end_of_validity_closes_every_socket_without_a_write() {
        let sockets = Arc::new(Sockets::default());
        let mut one = opened(&sockets, "one");
        let (_validity, changes) = tokio::sync::watch::channel(Validity {
            until_ms: Some(now_ms() + 200),
            epoch: 1,
        });
        tokio::spawn(close_sockets_at_lapse(Arc::downgrade(&sockets), changes));
        assert_eq!(closed(&mut one).await.0, 1012);
    }

    #[test]
    fn the_sweep_window_wraps_and_moves_on() {
        let all = [1, 2, 3, 4, 5];
        let mut cursor = 0;
        assert_eq!(window(&all, &mut cursor, 2), [1, 2]);
        assert_eq!(window(&all, &mut cursor, 2), [3, 4]);
        assert_eq!(window(&all, &mut cursor, 2), [5, 1]);
        assert_eq!(window(&all, &mut cursor, 9), [2, 3, 4, 5, 1]);
        assert!(window::<u8>(&[], &mut cursor, 2).is_empty());
    }
}
