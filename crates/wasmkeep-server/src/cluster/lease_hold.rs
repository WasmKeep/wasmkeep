//! This node's hold on an actor (ADR 0020): its ownership record
//! `owners/<tenant>/<class>/<name>`, written when the actor changes hands.
//! A hold is valid while it exists here and this node's lease is valid,
//! without renewals; it ends with a release (hibernation, a drain, a move,
//! a clean stop) that names nobody in the record.
//!
//! Releases that fail or whose outcome is unknown, and acquisitions whose
//! outcome is unknown, wait in this node's set of pending releases
//! ([`Pending`]), retried in the background while the node lease is valid.
//! One lock per actor orders an acquisition and the resolution of an
//! unresolved acquire of the same actor.
//!
//! A hold whose actor hibernates with sockets open on this node is parked
//! instead (ADR 0024): it stops acting, and its record keeps naming this
//! node until a wake acquires it again (at a new generation) or the last
//! socket, a drain, a move, a lapse or a clean stop releases it.

use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};

use wasmkeep_cluster::app_format::{self, AppFormat};
use wasmkeep_cluster::hold_cell::{self, Late};
pub(super) use wasmkeep_cluster::holds::PendingEntry;
use wasmkeep_cluster::holds::{Parked, Retried, Validity};
use wasmkeep_cluster::local_heads::{LocalHeads, Requirement};
use wasmkeep_cluster::node_lease::OwnerRecord;
use wasmkeep_cluster::node_lease::{AcquireError, Hold, Released};
use wasmkeep_cluster::recorded_head::Position;

use super::*;

/// One hold's state: the record hold while this node keeps it.
type Cell = Arc<Mutex<Option<Hold>>>;

/// This node's lease validity, shared with every hold (see
/// [`LeaseView::is_valid`]): `wasmkeep_cluster::holds::Validity` (until when
/// the lease is valid, and the epoch a lapse moves), in a watch channel so
/// waiters learn of every change. One value: a read never sees a new
/// validity with an old epoch.
#[derive(Debug)]
pub(crate) struct NodeValidity(tokio::sync::watch::Sender<Validity>);

impl NodeValidity {
    pub(crate) fn new() -> Self {
        Self(tokio::sync::watch::Sender::new(Validity::default()))
    }

    /// The validity as of now.
    pub(crate) fn get(&self) -> Validity {
        *self.0.borrow()
    }

    /// Until when the lease is valid (ms, this node's clock).
    pub(crate) fn until(&self) -> Option<u64> {
        self.get().until_ms
    }

    /// The current epoch: a hold acquired now records it.
    pub(crate) fn epoch(&self) -> u64 {
        self.get().epoch
    }

    /// Whether the lease is valid now.
    pub(crate) fn valid(&self) -> bool {
        self.get().valid(now_ms())
    }

    /// Whether a hold acquired in `epoch` may act now.
    pub(crate) fn valid_in(&self, epoch: u64) -> bool {
        self.get().valid_in(epoch, now_ms())
    }

    /// Records a lease write's outcome ([`Validity::publish`]); `true` if
    /// the lease had run out since, and every hold must end.
    pub(crate) fn publish(&self, until_ms: Option<u64>) -> bool {
        let now = now_ms();
        let mut lapsed = false;
        self.0.send_modify(|v| lapsed = v.publish(until_ms, now));
        lapsed
    }

    /// Makes the lease invalid at once, for every hold so far.
    pub(crate) fn invalidate(&self) {
        self.0.send_modify(Validity::invalidate);
    }

    /// Wakes on every change.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<Validity> {
        self.0.subscribe()
    }

    /// Sets the validity directly (tests).
    #[cfg(test)]
    pub(crate) fn set_until(&self, until_ms: Option<u64>) {
        self.0.send_modify(|v| v.until_ms = until_ms);
    }

    /// Moves the epoch alone (tests).
    #[cfg(test)]
    pub(crate) fn bump_epoch(&self) {
        self.0.send_modify(|v| v.epoch = v.epoch.wrapping_add(1));
    }
}

/// This node's pending releases and unresolved acquires, its live holds,
/// and its lock per actor.
#[derive(Debug, Default)]
pub(crate) struct Pending {
    entries: Mutex<Vec<PendingEntry>>,
    /// Live holds by record key.
    holds: Mutex<HashMap<String, Cell>>,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Releases sent and not answered yet.
    in_flight: Arc<AtomicUsize>,
    /// Holds parked by hibernations with sockets open (ADR 0024), each
    /// with its actor's address. Locked after `holds`, never before.
    parked: Mutex<Parked<ActorAddress>>,
    /// The heads ended holds froze, by key (ADR 0035, section 2).
    /// Locked after `holds`, never before, and never with `entries` or
    /// `parked` held.
    local: Mutex<LocalHeads>,
    /// The app's ownership format as last read, and the lease epoch it
    /// was read in (ADR 0035, section 5): read again in each new epoch.
    format: Mutex<Option<(u64, AppFormat)>>,
}

/// Records the parked set's size moving from `before` to `after` in the
/// `wasmkeep_parked_records` gauge (shared by every node of a process).
fn parked_moved(before: usize, after: usize) {
    let m = &crate::metrics::METRICS.parked_records;
    if after > before {
        m.fetch_add(
            u64::try_from(after - before).unwrap_or(0),
            Ordering::Relaxed,
        );
    } else if before > after {
        m.fetch_sub(
            u64::try_from(before - after).unwrap_or(0),
            Ordering::Relaxed,
        );
    }
}

impl Pending {
    /// Adds an entry; an unresolved acquire of the same write already
    /// there is kept once ([`wasmkeep_cluster::holds::add`]).
    pub(super) fn push(&self, entry: PendingEntry) {
        wasmkeep_cluster::holds::add(
            &mut self.entries.lock().unwrap_or_else(|p| p.into_inner()),
            entry,
        );
    }

    /// Whether no release or acquire waits for its outcome, none is in
    /// flight, and no record is parked (ADR 0024: a drain is drained, and a
    /// clean stop's releases done, only with none left).
    pub(crate) fn is_empty(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0
            && self
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
            && self.parked().is_empty()
    }

    /// Whether no release waits for its outcome or is in flight, and no
    /// record is parked: what a clean stop waits for before it writes the
    /// node lease `released`. Unresolved acquires are not waited for: that
    /// write frees a record one of them left naming this node, landed or
    /// still to land, as it frees one whose release did not land.
    fn releases_empty(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0
            && self
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .all(|e| matches!(e, PendingEntry::Unresolved { .. }))
            && self.parked().is_empty()
    }

    /// Whether a release or an unresolved acquire waits in the set.
    fn has_entries(&self) -> bool {
        !self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    fn local(&self) -> std::sync::MutexGuard<'_, LocalHeads> {
        self.local.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The preparation of an acquisition of `key` at `attempted` (ADR
    /// 0035, section 2), after its authoritative read found `record`:
    /// refused while a live hold of the actor remains here (it is ending:
    /// its frozen head is not registered yet); otherwise the local
    /// evidence, retired if the record proves a remote successor, and
    /// reserved until the acquisition finishes.
    fn prepare(
        &self,
        key: &str,
        record: Option<&OwnerRecord>,
        attempted: u64,
        node: &str,
        instance: &str,
    ) -> Result<Option<Requirement>, AcquireError> {
        let holds = self.holds.lock().unwrap_or_else(|p| p.into_inner());
        if holds
            .get(key)
            .is_some_and(|c| c.lock().unwrap_or_else(|p| p.into_inner()).is_some())
        {
            return Err(AcquireError::LocalHold);
        }
        Ok(self.local().prepare(key, record, attempted, node, instance))
    }

    /// A release of `hold` landed: its frozen head's entry ends, once no
    /// acquisition that read it is unfinished.
    fn release_landed(&self, hold: &Hold) {
        self.local().released(&hold.key, hold.generation);
    }

    /// An acquisition of `key` that prepared finished without a hold.
    fn acquisition_finished(&self, key: &str) {
        self.local().finished(key);
    }

    /// The local evidence of `key` (tests).
    #[cfg(test)]
    pub(crate) fn local_head(&self, key: &str) -> Option<Requirement> {
        self.local().get(key).map(|e| e.requirement)
    }

    fn parked(&self) -> std::sync::MutexGuard<'_, Parked<ActorAddress>> {
        self.parked.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Moves the hold in `cell` from the live set to the parked set in one
    /// step, under the live set's lock, if `valid` says it may still act
    /// (ADR 0024, section 1): the key is never in neither set, so a lapse
    /// ending every hold, or the resolver, cannot miss it. Returns what to
    /// release instead: the hold, if it may not act any more, or a hold
    /// parked earlier for the same key.
    fn park(
        &self,
        key: &str,
        cell: &Cell,
        address: ActorAddress,
        valid: impl Fn() -> bool,
    ) -> Option<Hold> {
        let mut holds = self.holds.lock().unwrap_or_else(|p| p.into_inner());
        let hold = hold_cell::end(cell);
        if let Some(hold) = &hold {
            self.local().register(hold);
        }
        if holds.get(key).is_some_and(|c| Arc::ptr_eq(c, cell)) {
            holds.remove(key);
        }
        let hold = hold?;
        if !valid() {
            return Some(hold);
        }
        let mut parked = self.parked();
        let before = parked.len();
        let earlier = parked.park(hold, address);
        parked_moved(before, parked.len());
        earlier
    }

    /// A wake's outcome for `key` (ADR 0024, section 2): drops the parked
    /// hold if the acquisition may have moved the record. Whether one was
    /// dropped.
    fn after_wake(&self, key: &str, acquired: &Result<Hold, AcquireError>) -> bool {
        let mut parked = self.parked();
        let before = parked.len();
        let dropped = parked.after_wake(key, acquired).is_some();
        parked_moved(before, parked.len());
        dropped
    }

    /// Takes the parked hold of `key` out (at `generation`, if given).
    fn take_parked(&self, key: &str, generation: Option<u64>) -> Option<Hold> {
        let mut parked = self.parked();
        let before = parked.len();
        let taken = match generation {
            Some(g) => parked.take_at(key, g),
            None => parked.take(key),
        };
        parked_moved(before, parked.len());
        taken.map(|(hold, _)| hold)
    }

    /// Whether a hold of `key` is held here at `generation`, live or
    /// parked: what the resolver of an unresolved acquire asks. A live hold
    /// counts at any generation (ADR 0020); a parked one only at its own
    /// (ADR 0024, section 3).
    fn held_at(&self, key: &str, generation: u64) -> bool {
        self.holds(key) || self.parked().holds_at(key, generation)
    }

    /// Actors whose lock has a handle somewhere (tests).
    #[cfg(test)]
    pub(crate) fn locks_in_use(&self) -> usize {
        self.locks.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// The lock of the actor whose record is `key` (ADR 0020, section 3):
    /// an acquisition and the resolver of an unresolved acquire take it
    /// for their whole read and write. Its map entry goes when its last
    /// handle does, so the map holds only locks in use.
    pub(super) fn lock_of(&self, key: &str) -> ActorLock<'_> {
        let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        let lock = Arc::clone(locks.entry(key.to_owned()).or_default());
        ActorLock {
            pending: self,
            key: key.to_owned(),
            lock,
        }
    }

    fn register(&self, key: &str, cell: &Cell) {
        self.holds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.to_owned(), Arc::clone(cell));
    }

    /// Freezes `cell` and removes it from the live set in one step under
    /// the live set's lock (the order [`Self::end_all`] and [`Self::park`]
    /// use): the hold, its head frozen, if it was still there.
    fn take_out(&self, key: &str, cell: &Cell) -> Option<Hold> {
        let mut holds = self.holds.lock().unwrap_or_else(|p| p.into_inner());
        let hold = hold_cell::end(cell);
        if let Some(hold) = &hold {
            self.local().register(hold);
        }
        if holds.get(key).is_some_and(|c| Arc::ptr_eq(c, cell)) {
            holds.remove(key);
        }
        hold
    }

    /// The heads of `key`'s pending releases and parked hold, as frozen
    /// when each ended (tests of ADR 0035, section 2).
    #[cfg(test)]
    pub(crate) fn frozen_heads(&self, key: &str) -> Vec<Option<Position>> {
        let mut heads: Vec<Option<Position>> = self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(|e| match e {
                PendingEntry::Release(hold) if hold.key == key => Some(hold.head.acknowledged),
                _ => None,
            })
            .collect();
        heads.extend(
            self.parked()
                .iter()
                .filter(|(hold, _)| hold.key == key)
                .map(|(hold, _)| hold.head.acknowledged),
        );
        heads
    }

    /// Whether this node holds the actor whose record is `key`.
    fn holds(&self, key: &str) -> bool {
        self.holds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .is_some_and(|c| c.lock().unwrap_or_else(|p| p.into_inner()).is_some())
    }

    /// Ends every hold at once: each becomes a pending release if
    /// `release`, and is dropped otherwise (a lost lease: the fence or the
    /// other process frees the records; a clean stop: `released` does).
    ///
    /// Each cell is frozen (its hold, acknowledged head included, taken out
    /// under the cell's lock, ADR 0035 section 2) and its release queued
    /// while the live set's lock is still held: a hold is never out of the
    /// live set before it is frozen and in the pending set. Lock order: the
    /// live set, then a cell, then the pending set; an acknowledgement
    /// takes only its cell.
    pub(super) fn end_all(&self, release: bool) {
        let mut holds = self.holds.lock().unwrap_or_else(|p| p.into_inner());
        for (_, cell) in holds.drain() {
            if let Some(hold) = hold_cell::end(&cell) {
                self.local().register(&hold);
                if release {
                    self.push(PendingEntry::Release(hold));
                }
            }
        }
        // Parked records too (ADR 0024, section 3), under the live set's
        // lock as parking takes it: a park cannot land after this.
        let parked = {
            let mut parked = self.parked();
            let before = parked.len();
            let all = parked.take_all();
            parked_moved(before, 0);
            all
        };
        drop(holds);
        if release && !parked.is_empty() {
            crate::metrics::parked_released("lapse", parked.len());
        }
        for (hold, _) in parked {
            if release {
                self.push(PendingEntry::Release(hold));
            }
        }
    }
}

/// Releases that landed while this node's lease was not valid (ADR 0020,
/// section 1, amendment of 2026-11-14), on this node: another node may
/// have taken those actors at once, without a fence. Every one is counted
/// (here and in `wasmkeep_releases_after_lapse_total`); the first after
/// the lease was last valid is logged, and how many followed it is logged
/// once the lease is valid again, so a lapse that closes many sockets
/// logs two lines, not one per actor.
#[derive(Debug, Default)]
pub(crate) struct InvalidReleases {
    /// Since this node started.
    total: AtomicUsize,
    /// Since the lease was last found valid again.
    since_valid: AtomicUsize,
}

impl InvalidReleases {
    /// Counts one; whether it is the first since the lease was last valid
    /// (the one to log).
    pub(crate) fn count(&self) -> bool {
        self.total.fetch_add(1, Ordering::SeqCst);
        self.since_valid.fetch_add(1, Ordering::SeqCst) == 0
    }

    /// The lease is valid again: how many were counted since it was last
    /// (the count starts again).
    pub(crate) fn valid_again(&self) -> usize {
        self.since_valid.swap(0, Ordering::SeqCst)
    }

    /// How many since this node started (tests: the metric is shared by
    /// every node of a process).
    #[cfg(test)]
    pub(crate) fn total(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }
}

/// A handle on one actor's lock ([`Pending::lock_of`]).
pub(super) struct ActorLock<'a> {
    pending: &'a Pending,
    key: String,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl ActorLock<'_> {
    /// Waits for the lock and takes it.
    pub(super) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
    }
}

impl Drop for ActorLock<'_> {
    fn drop(&mut self) {
        // Under the map's lock, as every new handle is made: the count read
        // here cannot change before the entry goes.
        let mut locks = self.pending.locks.lock().unwrap_or_else(|p| p.into_inner());
        // This handle's reference is let go here too, under the same lock
        // (a placeholder takes its place), so two handles dropped at once
        // cannot both see the other still there.
        let lock = std::mem::take(&mut self.lock);
        // The map's and this one: no other handle.
        if Arc::strong_count(&lock) == 2 {
            locks.remove(&self.key);
        }
        drop(lock);
        drop(locks);
    }
}

/// Counts one release in flight until dropped (also if its task is
/// cancelled).
struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn start(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(count))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Why an idle expiry's acquisition was refused (ADR 0028): the record is
/// not the one the listing showed (a wake rewrote it), or names a holder.
/// Nothing was written; the actor is not expired.
pub(crate) const RECORD_CHANGED: &str = "the ownership record changed since the listing";

/// This node's hold on one actor, released when dropped (hibernation).
pub(crate) struct LeaseHold {
    cell: Cell,
    epoch: u64,
    key: String,
    cluster: Arc<Cluster>,
}

impl LeaseHold {
    /// [`Self::acquire_with`], as an event acquires (tests).
    #[cfg(test)]
    pub(crate) async fn acquire(
        cluster: &Arc<Cluster>,
        address: &ActorAddress,
    ) -> Result<Self, OwnershipError> {
        Self::acquire_with(cluster, address, None).await
    }

    /// Acquires the actor's ownership record (ADR 0020, section 3).
    /// `NotOwner` carries the node that holds it. Waits for the node id
    /// check and the node lease's first write.
    ///
    /// With `listed`, for an idle expiry (ADR 0028, section 4) when
    /// `listed` is the record's `ETag` as the listing of `owners/` showed
    /// it: the record is taken only if it names nobody and is still that
    /// version (`Owners::acquire_unchanged`); otherwise
    /// [`OwnershipError::Changed`].
    pub(crate) async fn acquire_with(
        cluster: &Arc<Cluster>,
        address: &ActorAddress,
        listed: Option<&str>,
    ) -> Result<Self, OwnershipError> {
        cluster.identity().await.map_err(OwnershipError::Refused)?;
        let key = cluster.owner_key(address);
        let lock = cluster.pending.lock_of(&key);
        // From the read until the hold is registered (or the failure
        // recorded): the resolver takes the same lock.
        let _locked = lock.lock().await;
        // Read before the lease is asked: a lapse after this makes the
        // hold invalid from the start, and the write is not sent (the
        // question right before it is asked in this epoch).
        let epoch = cluster.validity.epoch();
        // An app deleted (its tombstone seen), or not checked for one since
        // this node's lease was last valid: no actor is acquired (ADR 0028,
        // section 5). Asked after the epoch is read: a lapse re-arms the
        // check before it publishes the new epoch, so an acquisition either
        // sees the check due or holds an epoch that is no longer valid.
        if let Some(why) = cluster.app_refusal() {
            return Err(OwnershipError::Refused(why.into()));
        }
        cluster.format_gate(epoch).await?;
        // ADR 0035, section 2: after the authoritative read, before the
        // write, this process's local evidence goes into the attempt.
        let mut prepared = false;
        let prepare = |record: Option<&OwnerRecord>, attempted: u64| {
            prepared = true;
            cluster.pending.prepare(
                &key,
                record,
                attempted,
                cluster.owners.node(),
                cluster.owners.instance(),
            )
        };
        let acquired = match listed {
            None => {
                cluster
                    .owners
                    .acquire_prepared(
                        &key,
                        || cluster.validity.valid_in(epoch),
                        async || cluster.seed_generation(address).await,
                        prepare,
                    )
                    .await
            }
            Some(listed) => {
                cluster
                    .owners
                    .acquire_unchanged_prepared(
                        &key,
                        |v| {
                            let read = v.0.split('\n').next().unwrap_or_default();
                            wasmkeep_cluster::expiry::etag(read)
                                == wasmkeep_cluster::expiry::etag(listed)
                        },
                        || cluster.validity.valid_in(epoch),
                        prepare,
                    )
                    .await
            }
        };
        // A prepared acquisition's reservation ends now unless a hold
        // inherits it (below) or its write is unresolved (the resolver
        // ends it).
        if prepared && !matches!(acquired, Ok(_) | Err(AcquireError::Unresolved { .. })) {
            cluster.pending.acquisition_finished(&key);
        }
        // A parked hold of this actor (ADR 0024): dropped if this write may
        // have moved the record.
        if cluster.pending.after_wake(&key, &acquired) && acquired.is_ok() {
            crate::metrics::inc(&crate::metrics::METRICS.parked_wakes);
        }
        match acquired {
            Ok(hold) => {
                if let Some(previous) = &hold.previous {
                    crate::metrics::inc(&crate::metrics::METRICS.failovers);
                    tracing::info!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), from = %previous, generation = hold.generation, "took over an actor whose node stopped");
                }
                let generation = hold.generation;
                let cell = Arc::new(Mutex::new(Some(hold)));
                cluster.pending.register(&key, &cell);
                // The new hold carries the local evidence now.
                cluster.pending.local().transferred(&key, generation);
                Ok(Self {
                    cell,
                    epoch,
                    key,
                    cluster: Arc::clone(cluster),
                })
            }
            Err(AcquireError::Held { node, .. }) => Err(OwnershipError::NotOwner {
                owner: node,
                until_ms: now_ms().saturating_add(cluster.lease_config.ttl_ms()),
            }),
            Err(e @ AcquireError::Unresolved { .. }) => {
                if let Some(entry) = PendingEntry::unresolved(&cluster.owners, &key, &e) {
                    cluster.pending.push(entry);
                }
                Err(OwnershipError::Unavailable(
                    "the ownership record's write has no answer; retry".into(),
                ))
            }
            Err(AcquireError::LeaseInvalid) => Err(OwnershipError::Unavailable(
                "this node's lease is not valid".into(),
            )),
            Err(AcquireError::Corrupt) => Err(OwnershipError::Broken(
                "the ownership record or a node lease is unreadable".into(),
            )),
            // ADR 0035, section 5: a record in the recorded-head format on
            // an app this node does not write it for. Never overwritten.
            Err(AcquireError::LocalHold) => Err(OwnershipError::Unavailable(
                "this node is still ending its previous hold of the actor; retry".into(),
            )),
            Err(AcquireError::FormatMismatch) => Err(OwnershipError::Broken(
                "the ownership record is in the recorded-head format (wasmkeep-owner 2), \
                 which this app has not enabled on this node: a setting mismatch"
                    .into(),
            )),
            Err(AcquireError::Unavailable) => Err(OwnershipError::Unavailable(
                "the bucket did not answer".into(),
            )),
            // Only an expiry's acquisition gives it (ADR 0028).
            Err(AcquireError::Changed) => Err(OwnershipError::Refused(RECORD_CHANGED.into())),
        }
    }

    /// A cheap handle that answers "is the hold still valid" for code
    /// that does not own the hold (the replicator).
    pub(crate) fn view(&self) -> LeaseView {
        LeaseView {
            cell: Arc::clone(&self.cell),
            epoch: self.epoch,
            validity: Arc::clone(&self.cluster.validity),
        }
    }

    /// The generation this node holds, while it holds it.
    pub(crate) fn generation(&self) -> Option<u64> {
        self.cell
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|h| h.generation)
    }

    /// Whether this node may act as the actor's owner right now.
    pub(crate) fn is_valid(&self) -> bool {
        self.view().is_valid()
    }

    /// Resolves once this node may no longer act as the owner: its lease
    /// ran out, was lost or made invalid, or the hold ended.
    /// The node's clean stop reaching its deadline (ADR 0034, section 10).
    pub(crate) fn stop_reached(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.cluster.stop_reached()
    }

    /// The node's clean stop deadline, if one began.
    pub(crate) fn stop_deadline(&self) -> Option<tokio::time::Instant> {
        self.cluster.stop_deadline()
    }

    pub(crate) fn expiry(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let view = self.view();
        let cluster = Arc::clone(&self.cluster);
        let expired = async move {
            let mut changes = view.validity.subscribe();
            loop {
                // Marked seen before the check: a change landing after it
                // wakes the wait below at once, never a renewal later.
                let until = changes.borrow_and_update().until_ms.unwrap_or(0);
                if !view.is_valid() {
                    return;
                }
                let left = until.saturating_sub(now_ms());
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(left)) => {}
                    changed = changes.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        };
        async move {
            // Another process found running with this node id ends it too.
            tokio::select! {
                () = expired => {}
                _ = cluster.identity_lost() => {}
            }
        }
    }

    /// Parks the hold (ADR 0024): its actor hibernates with sockets open
    /// on this node. It stops acting at once, and its record keeps naming
    /// this node, unwritten. A hold that may no longer act is released as
    /// a drop would.
    pub(crate) fn park(self, address: &ActorAddress) {
        let validity = Arc::clone(&self.cluster.validity);
        let epoch = self.epoch;
        // In flight from before the hold leaves the live set: the node never
        // reads as having nothing pending while a release is still due.
        let in_flight = InFlight::start(&self.cluster.pending.in_flight);
        let release = self
            .cluster
            .pending
            .park(&self.key, &self.cell, address.clone(), || {
                validity.valid_in(epoch)
            });
        if let Some(hold) = release {
            let cluster = Arc::clone(&self.cluster);
            tokio::spawn(async move {
                let _in_flight = in_flight;
                cluster.release_or_park(hold).await;
            });
        }
        // Dropped with an empty cell: sends nothing.
    }

    /// Takes the hold out: this node stops acting as owner before any
    /// release is sent.
    /// The hold comes out with its acknowledged head frozen (ADR 0035,
    /// section 2), under the live set's lock, then the cell's.
    fn take(&self) -> Option<Hold> {
        self.cluster.pending.take_out(&self.key, &self.cell)
    }
}

impl Drop for LeaseHold {
    fn drop(&mut self) {
        let Some(hold) = self.take() else {
            return;
        };
        let cluster = Arc::clone(&self.cluster);
        let in_flight = InFlight::start(&cluster.pending.in_flight);
        tokio::spawn(async move {
            let _in_flight = in_flight;
            cluster.release_or_park(hold).await;
        });
    }
}

/// What the safeguard re-read of a parked record found
/// ([`Cluster::drop_parked_if_stale`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recheck {
    /// Read: kept, or dropped if no longer this hold's.
    Read,
    /// The record cannot be decoded: kept.
    Unreadable,
    /// The bucket did not answer: kept.
    Unanswered,
}

/// What handing an actor back did (ADR 0014, 0020).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseReturn {
    /// Released: another node may take it at once.
    Released,
    /// Not ours any more (lost, taken over or already released).
    Superseded,
    /// The release did not land: it is pending, retried in the background;
    /// the actor is free once it lands, or when this node stops or is
    /// fenced.
    Pending,
}

/// How long a release is tried before it is left to the background.
const RELEASE_TRIES: Duration = Duration::from_secs(1);

/// Pending entries a pass tries at once.
const PENDING_CONCURRENCY: usize = 32;

/// What one try of a pending entry in a pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tried {
    Ended,
    /// Refused, unanswered, or not tried (the lease invalid, the pass
    /// throttled).
    Refused,
    /// An unresolved acquire still waiting for its unchanged record.
    Waiting,
}

/// What a pass over the pending set did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pass {
    /// Some entry ended.
    pub(super) ended: bool,
    /// Some entry was refused, unanswered, or not tried.
    pub(super) refused: bool,
}

impl LeaseHold {
    /// Releases the hold now, trying for up to a second, then leaving it
    /// pending. Unlike dropping the hold, the caller knows when another
    /// node may take the actor.
    pub(crate) async fn release(self) -> LeaseReturn {
        let Some(hold) = self.take() else {
            return LeaseReturn::Superseded;
        };
        let cluster = Arc::clone(&self.cluster);
        drop(self); // finds no hold: sends nothing
        let _in_flight = InFlight::start(&cluster.pending.in_flight);
        let deadline = tokio::time::Instant::now() + RELEASE_TRIES;
        loop {
            // The last try ends by the deadline too.
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let bound = cluster
                .release_attempt()
                .min(left.max(Duration::from_millis(50)));
            let invalid = !cluster.node_lease_valid();
            let sent = tokio::time::timeout(bound, cluster.owners.release(&hold))
                .await
                .unwrap_or(Released::Pending);
            if invalid && sent == Released::Released {
                cluster.released_after_lapse(&hold.key);
            }
            match sent {
                Released::Released => {
                    cluster.pending.release_landed(&hold);
                    return LeaseReturn::Released;
                }
                Released::Superseded => return LeaseReturn::Superseded,
                Released::Pending | Released::Unchanged
                    if tokio::time::Instant::now() >= deadline =>
                {
                    tracing::warn!(key = %hold.key, "releasing an actor failed; retried in the background");
                    cluster.pending.push(PendingEntry::Release(hold));
                    return LeaseReturn::Pending;
                }
                Released::Pending | Released::Unchanged => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
}

impl Cluster {
    /// The app's ownership-format setting (ADR 0035, section 5), read
    /// once per lease epoch (so again after a lapse), before any
    /// acquisition. Absent: version 1, as before. Enabled: this version
    /// reads the recorded-head format but does not enforce it yet, so it
    /// serves the app nowhere rather than write version 1 records there.
    /// Unreadable: refused. Unanswered: retry.
    async fn format_gate(&self, epoch: u64) -> Result<(), OwnershipError> {
        let cached = *self
            .pending
            .format
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let format = match cached {
            Some((e, format)) if e == epoch => format,
            _ => {
                let key = StorePath::from(app_format::key_of(&self.root));
                let read = tokio::time::timeout(self.lease_attempt, async {
                    match self.store.get(&key).await {
                        Ok(got) => got.bytes().await.map(Some),
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(e) => Err(e),
                    }
                })
                .await;
                let body = match read {
                    Ok(Ok(body)) => body,
                    Ok(Err(e)) => {
                        return Err(OwnershipError::Unavailable(format!(
                            "reading the app's ownership-format setting: {e}"
                        )));
                    }
                    Err(_) => {
                        return Err(OwnershipError::Unavailable(
                            "reading the app's ownership-format setting: no answer".into(),
                        ));
                    }
                };
                let format = app_format::decode(body.as_deref())
                    .map_err(|e| OwnershipError::Broken(e.to_string()))?;
                *self
                    .pending
                    .format
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()) = Some((epoch, format));
                format
            }
        };
        match format {
            AppFormat::Version1 => Ok(()),
            AppFormat::RecordedHead => Err(OwnershipError::Refused(
                "the app has enabled the recorded-head ownership format (ADR 0035), which this \
                 server version reads but does not enforce: serve it with a version that does"
                    .into(),
            )),
        }
    }

    /// The ownership record of an actor (ADR 0020). The name is encoded
    /// like the database file name, so any actor name is a safe key.
    pub(crate) fn owner_key(&self, address: &ActorAddress) -> String {
        self.assert_mine(address);
        self.root.key(
            Family::Owners,
            &format!(
                "{}/{}",
                address.class(),
                wasmkeep_storage::encode_segment(address.name())
            ),
        )
    }

    /// The first generation of an actor's first ownership record: above
    /// the generation of its per-actor lease from before ADR 0020 and above
    /// every generation its keys carry, so no hold reuses one. The old
    /// lease is replaced by a marker an old binary never takes over, and
    /// one still live refuses the acquisition (section 6).
    async fn seed_generation(&self, address: &ActorAddress) -> Result<u64, AcquireError> {
        let keys = crate::replica::highest_generation_in_bucket(self, address)
            .await
            .map_err(|_| AcquireError::Unavailable)?;
        let seed = wasmkeep_cluster::node_lease::mark_old_lease(
            &self.control_keys,
            &SystemClock,
            &self.lease_config,
            &self.lease_key(address),
            keys,
        )
        .await?;
        seed.checked_add(1).ok_or(AcquireError::Corrupt)
    }

    /// [`wasmkeep_cluster::holds::retry`], bounded by an attempt timeout (no
    /// answer: still pending). Background passes keep the full attempt: a
    /// release sent again after one that landed unanswered needs a write
    /// and a read, and a slow entry holds up one slot, not the pass.
    async fn retry_bounded(
        &self,
        entry: PendingEntry,
        held_here: impl Fn(&str, u64) -> bool,
    ) -> Retried {
        let again = entry.clone();
        tokio::time::timeout(
            self.lease_attempt,
            wasmkeep_cluster::holds::retry(&self.owners, entry, held_here),
        )
        .await
        .unwrap_or(Retried::Pending(again))
    }

    /// How long a release made in the foreground (hibernation, a
    /// hand-over, a hold dropped) waits: a lease attempt, at most a
    /// second. One that does not land is parked and retried in the
    /// background, with the full attempt.
    pub(super) fn release_attempt(&self) -> Duration {
        self.lease_attempt.min(Duration::from_secs(1))
    }

    /// One release, bounded by [`Self::release_attempt`]. One sent while
    /// this node's lease is not valid, with a hold in hand, is counted if
    /// it lands (ADR 0020, section 1, amendment of 2026-11-14): sent
    /// before the lease task saw a lapse (it ends every hold into the
    /// pending releases, which wait for a valid lease, only at the end of
    /// its next attempt), between its publishing the lapse and its ending
    /// the holds (the lapse hook closing sockets runs in between), or for
    /// a hold registered after that (an acquisition asked before the
    /// lapse, its write landing after).
    async fn send_release(&self, hold: &Hold) -> Released {
        let invalid = !self.node_lease_valid();
        let sent = tokio::time::timeout(self.release_attempt(), self.owners.release(hold))
            .await
            .unwrap_or(Released::Pending);
        if invalid && sent == Released::Released {
            self.released_after_lapse(&hold.key);
        }
        if sent == Released::Released {
            self.pending.release_landed(hold);
        }
        sent
    }

    /// A release that landed while this node's lease was not valid:
    /// another node may take the actor at once, without the fence.
    /// Counted here, since the record it leaves names nobody, and the
    /// node that takes it sees an ordinary released actor; logged for the
    /// first since the lease was last valid ([`InvalidReleases`]).
    fn released_after_lapse(&self, key: &str) {
        crate::metrics::inc(&crate::metrics::METRICS.releases_after_lapse);
        if self.invalid_releases.count() {
            tracing::warn!(
                key,
                "released an actor after this node's lease ran out: another node may take it \
                 now; any more until the lease is valid again are counted, and logged then"
            );
        }
    }

    /// This node's lease is valid again: logs how many actors were
    /// released while it was not, if more than the one already logged.
    pub(super) fn lease_valid_again(&self) {
        let n = self.invalid_releases.valid_again();
        if n > 1 {
            tracing::warn!(
                released = n,
                "released actors after this node's lease ran out, the first logged above; \
                 its lease is valid again"
            );
        }
    }

    /// Sends a release once; if it did not land, it waits in the pending
    /// set.
    async fn release_or_park(&self, hold: Hold) -> LeaseReturn {
        match self.send_release(&hold).await {
            Released::Released => LeaseReturn::Released,
            Released::Superseded => LeaseReturn::Superseded,
            Released::Pending | Released::Unchanged => {
                self.pending.push(PendingEntry::Release(hold));
                LeaseReturn::Pending
            }
        }
    }

    /// Where to send a request for `address`: the node its record names,
    /// if that is another node whose named process this node's view shows
    /// live; `None` to try here (ADR 0020, section 3). A hint only.
    pub(crate) async fn other_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<String>, AcquireError> {
        let key = self.owner_key(address);
        self.owners
            .other_owner(&key, |node, instance| {
                self.peers
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(node)
                    .is_some_and(|p| p.instance.as_deref() == Some(instance))
            })
            .await
    }

    /// Whether `address`'s record names this process, at any generation
    /// (one read; ADR 0024, section 3).
    pub(crate) async fn names_this_instance(
        &self,
        address: &ActorAddress,
    ) -> Result<bool, AcquireError> {
        self.owners
            .names_this_instance(&self.owner_key(address))
            .await
    }

    /// Whether this node holds `address` now, with its lease valid: a
    /// resident actor that acquired it. One that hibernated released it.
    pub(crate) fn holds_here(&self, address: &ActorAddress) -> bool {
        self.node_lease_valid() && self.pending.holds(&self.owner_key(address))
    }

    /// Whether this node keeps `address`'s record parked (ADR 0024).
    pub(crate) fn is_parked(&self, address: &ActorAddress) -> bool {
        self.pending.parked().contains(&self.owner_key(address))
    }

    /// The actors whose record this node keeps parked, each with the
    /// parked generation.
    pub(crate) fn parked(&self) -> Vec<(ActorAddress, u64)> {
        self.pending
            .parked()
            .iter()
            .map(|(hold, address)| (address.clone(), hold.generation))
            .collect()
    }

    /// Releases `address`'s parked record, if it has one, under its
    /// actor's lock (a wake takes it too), and counts it for `reason`; a
    /// release that does not land waits in the pending set. Counted as in
    /// flight from before the parked set is touched: the node never reads
    /// as having nothing pending in between. What the release did, or
    /// `None` if nothing was parked (never parked, or a wake took it).
    pub(crate) async fn release_parked(
        &self,
        address: &ActorAddress,
        reason: &str,
    ) -> Option<LeaseReturn> {
        let key = self.owner_key(address);
        let _in_flight = InFlight::start(&self.pending.in_flight);
        let lock = self.pending.lock_of(&key);
        let _locked = lock.lock().await;
        let hold = self.pending.take_parked(&key, None)?;
        crate::metrics::parked_released(reason, 1);
        tracing::debug!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), reason, "released a parked record");
        Some(self.release_or_park(hold).await)
    }

    /// Releases every parked record (a drain, a clean stop).
    pub(crate) async fn release_all_parked(&self, reason: &str) {
        let all = self.parked();
        futures_util::StreamExt::for_each_concurrent(
            futures_util::stream::iter(all),
            PENDING_CONCURRENCY,
            |(address, _)| async move {
                let _ = self.release_parked(&address, reason).await;
            },
        )
        .await;
    }

    /// The safeguard re-read of a parked record (ADR 0024, section 5): if
    /// the record no longer names this instance at the parked generation,
    /// the entry is dropped (nothing to release: the record is not this
    /// hold's any more). A failed read drops nothing; a record that cannot
    /// be decoded neither (it proves nothing).
    pub(crate) async fn drop_parked_if_stale(
        &self,
        address: &ActorAddress,
        generation: u64,
    ) -> Recheck {
        let key = self.owner_key(address);
        let lock = self.pending.lock_of(&key);
        let _locked = lock.lock().await;
        if !self.pending.parked().holds_at(&key, generation) {
            return Recheck::Read;
        }
        match self.owners.still_names_this(&key, generation).await {
            Ok(false) => {
                if self.pending.take_parked(&key, Some(generation)).is_some() {
                    tracing::warn!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), generation, "a parked record no longer names this node; forgotten");
                }
                Recheck::Read
            }
            Ok(true) => Recheck::Read,
            Err(AcquireError::Corrupt) => Recheck::Unreadable,
            Err(_) => Recheck::Unanswered,
        }
    }

    /// After an unresolved acquire of `key` at `tried` ended (the caller
    /// holds the actor's lock): a hold parked at an earlier generation is
    /// forgotten if the record no longer names it (the unresolved wake
    /// landed, and the resolver released it, ADR 0024 section 3). Kept if
    /// it still does (the write never landed) or the read fails: the
    /// safeguard re-read and the last socket's release handle the rest.
    async fn forget_parked_behind(&self, key: &str, tried: u64) {
        let Some(parked) = self.pending.parked().generation(key) else {
            return;
        };
        if parked >= tried {
            return;
        }
        let read = tokio::time::timeout(
            self.lease_attempt,
            self.owners.still_names_this(key, parked),
        )
        .await;
        if matches!(read, Ok(Ok(false))) {
            let _ = self.pending.take_parked(key, Some(parked));
            tracing::info!(
                key,
                parked,
                "a parked record was superseded by a wake that landed unanswered; forgotten"
            );
        }
    }

    /// Whether no release or acquire of this node waits for its outcome.
    pub(crate) fn nothing_pending(&self) -> bool {
        self.pending.is_empty()
    }

    /// Waits until no release is pending or in flight, or `limit` passes.
    /// A node shutting down calls it after its actors stopped, so the
    /// releases reach the bucket before it writes `released` (which frees
    /// what an unresolved acquire may have left: those are not waited for,
    /// [`Pending::releases_empty`]).
    pub(crate) async fn releases_done(&self, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while !self.pending.releases_empty() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    /// One pass over the pending set: each release sent again, each
    /// unresolved acquire read (under its actor's lock) and released if it
    /// landed and no hold of it exists here, kept while its record is
    /// unchanged and its wait lasts. Nothing while this node's lease is
    /// not valid.
    /// Whether some entry ended (landed, or found superseded), and whether
    /// some was refused or not tried.
    pub(super) async fn retry_pending(&self) -> Pass {
        if !self.node_lease_valid()
            || self
                .pending
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        {
            return Pass {
                ended: false,
                refused: true,
            };
        }
        // For the whole pass: the entries are out of the set while they are
        // tried, and the set must not read as empty meanwhile (a drain
        // would report drained, a clean stop go on).
        let _pass = InFlight::start(&self.pending.in_flight);
        let entries = std::mem::take(
            &mut *self
                .pending
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        // Up to PENDING_CONCURRENCY entries at once: after a lapse a node
        // may hold thousands, and one at a time over a slow bucket a pass
        // would take minutes (a drain could not report drained meanwhile,
        // nor a shutdown's releases finish).
        //
        // A bucket that refuses or throttles leaves every entry pending:
        // after PENDING_CONCURRENCY such outcomes in a row with none ended,
        // the pass starts no more (the rest go back), so it does not send
        // thousands of writes to a refusing bucket, beside the node lease's
        // own renewals. The loop's back-off then spaces the passes.
        use futures_util::StreamExt as _;
        let refused_in_a_row = AtomicUsize::new(0);
        let refused_in_a_row = &refused_in_a_row;
        let outcomes: Vec<Tried> = futures_util::stream::iter(entries)
            .map(|entry| async move {
                if !self.node_lease_valid()
                    || refused_in_a_row.load(Ordering::SeqCst) >= PENDING_CONCURRENCY
                {
                    self.pending.push(entry);
                    return Tried::Refused;
                }
                let retried = match entry {
                    // As `holds::retry` does, keeping apart a release that
                    // landed (its frozen head's entry ends) from one
                    // superseded (it stays, ADR 0035 section 2).
                    PendingEntry::Release(hold) => {
                        let sent =
                            tokio::time::timeout(self.lease_attempt, self.owners.release(&hold))
                                .await
                                .unwrap_or(Released::Pending);
                        match sent {
                            Released::Released => {
                                self.pending.release_landed(&hold);
                                Retried::Ended
                            }
                            Released::Superseded => Retried::Ended,
                            Released::Pending | Released::Unchanged => {
                                Retried::Pending(PendingEntry::Release(hold))
                            }
                        }
                    }
                    entry @ PendingEntry::Unresolved { .. } => {
                        let lock = self.pending.lock_of(entry.key());
                        let _locked = lock.lock().await;
                        let (key, tried) = match &entry {
                            PendingEntry::Unresolved {
                                key, generation, ..
                            } => (key.clone(), *generation),
                            PendingEntry::Release(_) => unreachable!("matched above"),
                        };
                        let retried = self
                            .retry_bounded(entry, |key, generation| {
                                self.pending.held_at(key, generation)
                            })
                            .await;
                        if retried == Retried::Ended {
                            self.pending.acquisition_finished(&key);
                            self.forget_parked_behind(&key, tried).await;
                        }
                        retried
                    }
                };
                match retried {
                    Retried::Pending(entry) => {
                        self.pending.push(entry);
                        refused_in_a_row.fetch_add(1, Ordering::SeqCst);
                        Tried::Refused
                    }
                    // The bucket answered a read: not a refusal, though the
                    // entry stays (its record unchanged, its wait not
                    // over). Nor proof that writes go through: the count of
                    // refusals in a row is left as it is.
                    Retried::Waiting(entry) => {
                        self.pending.push(entry);
                        Tried::Waiting
                    }
                    Retried::Ended => {
                        refused_in_a_row.store(0, Ordering::SeqCst);
                        Tried::Ended
                    }
                }
            })
            .buffer_unordered(PENDING_CONCURRENCY)
            .collect()
            .await;
        Pass {
            ended: outcomes.contains(&Tried::Ended),
            refused: outcomes.contains(&Tried::Refused),
        }
    }
}

/// Retries the pending set every quarter second, backing off to every 4 s
/// while passes end nothing and some entry is refused (the bucket
/// refusing, ADR 0020 section 3), until the cluster is dropped. A pass
/// whose entries only wait (unresolved acquires whose record has not
/// changed) runs again a second later: they are read again often enough,
/// and a release added meanwhile waits a second at most.
pub(super) async fn pending_loop(cluster: Weak<Cluster>) {
    const FIRST: Duration = Duration::from_millis(250);
    const WAITING: Duration = Duration::from_secs(1);
    const MOST: Duration = Duration::from_secs(4);
    let mut wait = FIRST;
    loop {
        tokio::time::sleep(wait).await;
        let Some(c) = cluster.upgrade() else {
            return;
        };
        let pass = c.retry_pending().await;
        // Parked records are not entries to retry: a node with one parked
        // for days must still try a new entry within a quarter second.
        wait = if pass.ended || !c.pending.has_entries() {
            FIRST
        } else if !pass.refused {
            WAITING
        } else {
            (wait * 2).min(MOST)
        };
    }
}

/// Whether a hold is still valid, without owning it.
#[derive(Debug, Clone)]
pub(crate) struct LeaseView {
    cell: Cell,
    epoch: u64,
    validity: Arc<NodeValidity>,
}

impl LeaseView {
    /// A view that is always valid (tests of code that takes one).
    #[cfg(test)]
    pub(crate) fn always_valid(generation: u64) -> Self {
        let validity = NodeValidity::new();
        let _ = validity.publish(Some(u64::MAX));
        Self {
            cell: Arc::new(Mutex::new(Some(Hold {
                key: "owners/test".into(),
                generation,
                version: wasmkeep_cluster::lease::Version(String::new()),
                previous: None,
                // Verified at the explicit pre-history position.
                head: wasmkeep_cluster::hold_cell::HoldHead {
                    acknowledged: Some(wasmkeep_cluster::recorded_head::Position::Empty),
                    inherited: None,
                },
            }))),
            epoch: 0,
            validity: Arc::new(validity),
        }
    }

    /// A view valid until the returned validity is invalidated (tests of
    /// what a lapse during an operation does).
    #[cfg(test)]
    pub(crate) fn lapsable(generation: u64) -> (Self, Arc<NodeValidity>) {
        let view = Self::always_valid(generation);
        let validity = Arc::clone(&view.validity);
        (view, validity)
    }

    /// The acknowledgement critical section (ADR 0035, section 2): under
    /// the hold cell's lock, the hold must still be here and valid in its
    /// epoch; then `advance`, if given, becomes its acknowledged head.
    /// `None` only checks (a periodic snapshot, the snapshot after a
    /// reset). Refused: [`ReplicaError::Late`](crate::replica::ReplicaError).
    pub(crate) fn acknowledge(&self, advance: Option<Position>) -> Result<(), Late> {
        let (validity, epoch) = (&self.validity, self.epoch);
        hold_cell::acknowledge(&self.cell, || validity.valid_in(epoch), advance)
    }

    /// After a successful restore: the verified head, set under the cell's
    /// lock and validity check before activation.
    pub(crate) fn verify(&self, position: Position) -> Result<(), Late> {
        let (validity, epoch) = (&self.validity, self.epoch);
        hold_cell::verify(&self.cell, || validity.valid_in(epoch), position)
    }

    /// The hold's acknowledged head while it lives (`None`: ended, or not
    /// verified yet). Tests.
    #[cfg(test)]
    pub(crate) fn acknowledged_head(&self) -> Option<Position> {
        self.cell
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|h| h.head.acknowledged)
    }

    /// The requirement the hold inherited at acquisition (tests).
    #[cfg(test)]
    pub(crate) fn inherited(&self) -> Option<wasmkeep_cluster::local_heads::Requirement> {
        self.cell
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|h| h.head.inherited)
    }

    /// Whether this node may still act as the owner: the hold exists here
    /// and this node's lease is valid, without a lapse since the hold was
    /// acquired.
    pub(crate) fn is_valid(&self) -> bool {
        self.validity.valid_in(self.epoch)
            && self
                .cell
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_some()
    }
}

/// Why this node may not run an event for an actor.
#[derive(Debug)]
pub(crate) enum OwnershipError {
    /// Another node holds the actor; retry it there until about `until_ms`.
    NotOwner {
        /// The holder's node id.
        owner: String,
        /// Until when to send to it without asking the bucket again.
        until_ms: u64,
    },
    /// Not now: the bucket did not answer, this node's lease is not valid,
    /// a write's outcome is unknown, the actor's previous connection is
    /// still closing. Answered 503 with `Retry-After`.
    Unavailable(String),
    /// Something a retry will not fix: an unreadable ownership record or
    /// node lease, a history that cannot be restored, local file trouble.
    /// Answered 500.
    Broken(String),
    /// This node may not run the actor now and no peer can take it: it
    /// cannot open the app's secrets (ADR 0016). Answered 503.
    Refused(String),
    /// This node is draining and does not claim: send the event to `peer`,
    /// which claims for itself (ADR 0014).
    Elsewhere {
        /// The chosen peer's node id.
        peer: String,
    },
}
