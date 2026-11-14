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
use wasmkeep_cluster::node_lease::AcquireError;

/// Ownership records the sweep reads at once.
const SWEEP_PARALLEL: usize = 16;
/// How long the sweep waits for one record; past it, it closes nothing.
const SWEEP_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Most records one sweep pass reads; the next pass goes on where it
/// stopped, so every room is read in turn, at a bounded bucket cost.
const SWEEP_MAX_READS: usize = 5_000;
/// One sweep pass in this many also re-reads parked records, as a
/// safeguard (ADR 0024, section 5): up to [`SWEEP_MAX_READS`] of them,
/// going on where the last re-read stopped; the others read none of them.
const PARKED_RECHECK_EVERY: u64 = 10;
/// Releases of parked records whose last socket went, sent at once: a
/// burst of closed sockets does not become a burst of writes.
const VACATED_PARALLEL: usize = 32;
/// Rooms whose unreadable record the sweep remembers having logged: past
/// it, further ones are counted, not logged.
const UNREADABLE_LOGGED: usize = 1_000;

/// What one read of the sweep found, for [`read_in_turn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Read (or nothing to read): go on.
    Next,
    /// This record cannot be decoded: counted, logged once, and passed
    /// over, so it does not hold back the rooms after it.
    Unreadable,
    /// The bucket did not answer in time, or the node is shutting down or
    /// its lease is not valid: end the pass, and start the next one here.
    Stop,
}

/// Rooms whose unreadable record was logged already ([`UNREADABLE_LOGGED`]
/// at most).
type Logged = std::sync::Mutex<std::collections::HashSet<ActorAddress>>;

/// Counts an unreadable record of `address` for `read` (`owner`, `parked`)
/// and logs it the first time.
fn unreadable(logged: &Logged, read: &str, address: &ActorAddress) -> Step {
    crate::metrics::sweep_unreadable(read);
    let mut logged = logged.lock().unwrap_or_else(|p| p.into_inner());
    if logged.len() < UNREADABLE_LOGGED && logged.insert(address.clone()) {
        tracing::warn!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), read, "the WebSocket sweep cannot decode this actor's ownership record; passed over");
    }
    Step::Unreadable
}

/// Maps one bounded read (`None`: timed out) to its value, or to the step
/// that passes over it: an undecodable record is that record's fault, any
/// other failure the bucket's.
fn sweep_read<T>(read: Option<Result<T, AcquireError>>) -> Result<T, Step> {
    match read {
        Some(Ok(value)) => Ok(value),
        Some(Err(AcquireError::Corrupt)) => Err(Step::Unreadable),
        Some(Err(_)) | None => Err(Step::Stop),
    }
}

/// Releases the parked record of each actor whose last socket slot was
/// freed (ADR 0024, section 3), unless it has a socket open again; at most
/// [`VACATED_PARALLEL`] at once. The sweep does the same for any it missed.
/// Nothing while this node's lease is not valid or it is shutting down:
/// the sockets were closed by the lapse or the stop, which release the
/// parked records themselves (a lapse into the pending releases, a stop
/// with every other), so a nudge then would only be a second write each.
pub(super) async fn release_vacated(
    service: std::sync::Weak<ServiceInner>,
    mut vacated: tokio::sync::mpsc::UnboundedReceiver<ActorAddress>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(VACATED_PARALLEL));
    while let Some(address) = vacated.recv().await {
        let Some(inner) = service.upgrade() else {
            return;
        };
        let Some(cluster) = inner.cluster.clone() else {
            return;
        };
        if inner.mailboxes.is_closed() || !cluster.node_lease_valid() {
            continue;
        }
        if cluster.is_parked(&address) && !inner.sockets.has_open(&address) {
            let sockets = Arc::clone(&inner.sockets);
            drop(inner);
            let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
                return;
            };
            tokio::spawn(async move {
                let _slot = slot;
                // Asked again under the lock's wait: a socket opened
                // meanwhile woke the room, which dropped the parked entry.
                if !sockets.has_open(&address) && cluster.node_lease_valid() {
                    let _ = cluster.release_parked(&address, "last_socket").await;
                }
            });
        }
    }
}

/// Every sweep pass, before the reads: releases the parked records of
/// actors with no socket open here any more (the nudge of
/// [`release_vacated`] missed, or a socket closed without its slot being
/// freed yet counts as gone), [`VACATED_PARALLEL`] at once.
async fn release_unsocketed(inner: &ServiceInner, cluster: &Cluster) {
    let gone: Vec<ActorAddress> = cluster
        .parked()
        .into_iter()
        .map(|(address, _)| address)
        .filter(|address| !inner.sockets.has_open(address))
        .collect();
    futures_util::StreamExt::for_each_concurrent(
        futures_util::stream::iter(gone),
        VACATED_PARALLEL,
        |address| async move {
            if !inner.sockets.has_open(&address) {
                let _ = cluster.release_parked(&address, "last_socket").await;
            }
        },
    )
    .await;
}

/// The safeguard re-read (ADR 0024, section 5): up to [`SWEEP_MAX_READS`]
/// parked records read once each, in a stable order from where the last
/// re-read stopped; one that no longer names this node at the parked
/// generation is forgotten, and its room's sockets are swept as any
/// other's. A read that times out or that the bucket does not answer stops
/// the re-read until next time, which starts at the first record not read;
/// a record that cannot be decoded is kept, counted and passed over.
async fn recheck_parked(
    inner: &ServiceInner,
    cluster: &Cluster,
    cursor: &mut usize,
    logged: &Logged,
) {
    let mut all = cluster.parked();
    all.sort_unstable();
    read_in_turn(
        &all,
        cursor,
        SWEEP_MAX_READS,
        |(address, generation)| async move {
            if inner.mailboxes.is_closed() || !cluster.node_lease_valid() {
                return Step::Stop;
            }
            crate::metrics::inc(&crate::metrics::METRICS.websocket_sweep_reads);
            let read = tokio::time::timeout(
                SWEEP_READ_TIMEOUT,
                cluster.drop_parked_if_stale(&address, generation),
            )
            .await;
            match read {
                Ok(crate::cluster::Recheck::Read) => Step::Next,
                Ok(crate::cluster::Recheck::Unreadable) => unreadable(logged, "parked", &address),
                Ok(crate::cluster::Recheck::Unanswered) | Err(_) => Step::Stop,
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
/// bucket is slow or out of reach: the next pass tries again), except a
/// record that cannot be decoded: counted in
/// `wasmkeep_websocket_sweep_unreadable_total`, logged once, and passed
/// over, so one bad record never holds back the rooms after it. At most
/// [`SWEEP_MAX_READS`] reads a pass. Holds neither the service nor the
/// cluster while it waits, so a node dropped meanwhile stops at once (its
/// lease is not kept renewing).
pub(super) async fn sweep_sockets(service: std::sync::Weak<ServiceInner>, every: Duration) {
    // Where the next pass starts, in the sorted list of rooms to read.
    let mut cursor = 0_usize;
    let mut pass = 0_u64;
    let mut recheck_cursor = 0_usize;
    let logged = Logged::default();
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
            recheck_parked(&inner, &cluster, &mut recheck_cursor, &logged).await;
        }
        // A parked room is this node's own: no read (ADR 0024).
        let mut unheld: Vec<ActorAddress> = inner
            .sockets
            .open_owners()
            .into_iter()
            .filter(|address| !cluster.holds_here(address) && !cluster.is_parked(address))
            .collect();
        unheld.sort_unstable();
        let (inner_ref, cluster_ref, logged) = (&inner, &cluster, &logged);
        read_in_turn(&unheld, &mut cursor, SWEEP_MAX_READS, |address| async move {
            let (inner, cluster) = (inner_ref, cluster_ref);
            if inner.mailboxes.is_closed() || !cluster.node_lease_valid() {
                return Step::Stop;
            }
            crate::metrics::inc(&crate::metrics::METRICS.websocket_sweep_reads);
            let read = tokio::time::timeout(SWEEP_READ_TIMEOUT, cluster.other_owner(&address))
                .await
                .ok();
            let owner = match sweep_read(read) {
                Ok(Some(owner)) => owner,
                Ok(None) => return Step::Next,
                Err(Step::Unreadable) => return unreadable(logged, "owner", &address),
                Err(step) => return step,
            };
            // Taken here meanwhile: the sockets are where they belong.
            if cluster.holds_here(&address) {
                return Step::Next;
            }
            let closed = inner
                .sockets
                .close_owner(&address, 1012, "actor moved; reconnect");
            if closed > 0 {
                tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %owner, sockets = closed, "closed sockets of an actor another node holds");
                crate::metrics::owner_closes("sweep", closed);
            }
            Step::Next
        })
        .await;
        drop((inner, cluster));
    }
}

/// Up to `max` of `all` from `*cursor` on, wrapping around, with where
/// they start; the cursor is left alone.
fn window<T: Clone>(all: &[T], cursor: usize, max: usize) -> (usize, Vec<T>) {
    if all.is_empty() {
        return (0, Vec::new());
    }
    let start = cursor % all.len();
    let taken: Vec<T> = all
        .iter()
        .cycle()
        .skip(start)
        .take(max.min(all.len()))
        .cloned()
        .collect();
    (start, taken)
}

/// Reads up to `max` of `all` from `*cursor` on ([`window`]), in groups of
/// [`SWEEP_PARALLEL`] at once, and moves the cursor past the groups read
/// in full. `read` says whether to go on: [`Step::Stop`] (the bucket did
/// not answer, or the node is shutting down or its lease not valid) ends
/// the pass after its group, and the next pass starts at that group
/// again, so no record is skipped for a pass that stopped early;
/// [`Step::Unreadable`] goes on, as that record would stop every pass.
async fn read_in_turn<T: Clone, F: std::future::Future<Output = Step>>(
    all: &[T],
    cursor: &mut usize,
    max: usize,
    read: impl Fn(T) -> F,
) {
    let (start, taken) = window(all, *cursor, max);
    let mut done = 0;
    for group in taken.chunks(SWEEP_PARALLEL) {
        let ok = futures_util::future::join_all(group.iter().map(|x| read(x.clone()))).await;
        if ok.contains(&Step::Stop) {
            break;
        }
        done += group.len();
    }
    *cursor = start + done;
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
    fn the_sweep_window_wraps() {
        let all = [1, 2, 3, 4, 5];
        assert_eq!(window(&all, 0, 2), (0, vec![1, 2]));
        assert_eq!(window(&all, 4, 2), (4, vec![5, 1]));
        assert_eq!(window(&all, 6, 9), (1, vec![2, 3, 4, 5, 1]));
        assert!(window::<u8>(&[], 3, 2).1.is_empty());
    }

    /// The cursor moves past what was read, and only that: a pass that
    /// stops early starts the next at the first group not read in full.
    #[tokio::test]
    async fn the_sweep_moves_on_only_past_what_it_read() {
        let all: Vec<usize> = (0..SWEEP_PARALLEL * 3).collect();
        let seen = std::sync::Mutex::new(Vec::new());
        let read_all = |x: usize| {
            seen.lock().unwrap().push(x);
            async { Step::Next }
        };
        let mut cursor = 0;
        read_in_turn(&all, &mut cursor, SWEEP_PARALLEL * 2, read_all).await;
        assert_eq!(cursor, SWEEP_PARALLEL * 2);
        // A read in the second group fails: the cursor moves one group.
        let failing = SWEEP_PARALLEL * 3 + 1;
        let mut cursor = SWEEP_PARALLEL;
        let all: Vec<usize> = (0..SWEEP_PARALLEL * 4).collect();
        read_in_turn(&all, &mut cursor, all.len(), |x: usize| async move {
            if x == failing { Step::Stop } else { Step::Next }
        })
        .await;
        assert_eq!(
            cursor,
            SWEEP_PARALLEL * 3,
            "moved past a group not read in full"
        );
        // Then it reads from there, wrapping around.
        read_in_turn(&all, &mut cursor, SWEEP_PARALLEL, read_all).await;
        assert_eq!(cursor, SWEEP_PARALLEL * 4);
        assert_eq!(
            seen.lock().unwrap()[SWEEP_PARALLEL * 2..],
            (SWEEP_PARALLEL * 3..SWEEP_PARALLEL * 4).collect::<Vec<_>>()[..]
        );
    }

    /// A record that cannot be decoded is passed over, every pass: the
    /// rooms after it (here two groups on) are still read, and the cursor
    /// moves past it. Only the bucket not answering stops a pass.
    #[tokio::test]
    async fn an_unreadable_record_does_not_stop_the_sweep() {
        assert_eq!(
            sweep_read::<Option<String>>(Some(Err(AcquireError::Corrupt))),
            Err(Step::Unreadable)
        );
        assert_eq!(
            sweep_read::<Option<String>>(Some(Err(AcquireError::Unavailable))),
            Err(Step::Stop)
        );
        assert_eq!(sweep_read::<Option<String>>(None), Err(Step::Stop));
        assert_eq!(sweep_read(Some(Ok(None::<String>))), Ok(None));
        let all: Vec<usize> = (0..SWEEP_PARALLEL * 4).collect();
        let (corrupt, after) = (1, SWEEP_PARALLEL * 2 + 1);
        let seen = std::sync::Mutex::new(Vec::new());
        let read = |x: usize| {
            seen.lock().unwrap().push(x);
            let record: Result<Option<String>, AcquireError> = if x == corrupt {
                Err(AcquireError::Corrupt)
            } else {
                Ok(None)
            };
            async move {
                match sweep_read(Some(record)) {
                    Ok(_) => Step::Next,
                    Err(step) => step,
                }
            }
        };
        for _ in 0..2 {
            let mut cursor = 0;
            read_in_turn(&all, &mut cursor, all.len(), read).await;
            assert_eq!(cursor, all.len(), "the pass stopped at the bad record");
        }
        assert_eq!(
            seen.lock().unwrap().iter().filter(|x| **x == after).count(),
            2
        );
        // The counter and the log: one warning, both counted.
        let logged = Logged::default();
        let address = ActorAddress::new("default", "chat", "bad").unwrap();
        let before = crate::metrics::METRICS
            .websocket_sweep_unreadable
            .get(&[("read", "owner")])
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..2 {
            assert_eq!(unreadable(&logged, "owner", &address), Step::Unreadable);
        }
        assert_eq!(logged.lock().unwrap().len(), 1);
        assert!(
            crate::metrics::METRICS
                .websocket_sweep_unreadable
                .get(&[("read", "owner")])
                .load(std::sync::atomic::Ordering::Relaxed)
                >= before + 2
        );
    }
}
