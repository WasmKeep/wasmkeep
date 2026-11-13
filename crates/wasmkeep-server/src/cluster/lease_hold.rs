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

use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) use wasmkeep_cluster::holds::PendingEntry;
use wasmkeep_cluster::holds::Validity;
use wasmkeep_cluster::node_lease::{AcquireError, Hold, Released};

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
}

impl Pending {
    pub(super) fn push(&self, entry: PendingEntry) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(entry);
    }

    /// Whether no release or acquire waits for its outcome, and none is in
    /// flight.
    pub(crate) fn is_empty(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0
            && self
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
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

    fn unregister(&self, key: &str, cell: &Cell) {
        let mut holds = self.holds.lock().unwrap_or_else(|p| p.into_inner());
        if holds.get(key).is_some_and(|c| Arc::ptr_eq(c, cell)) {
            holds.remove(key);
        }
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
    pub(super) fn end_all(&self, release: bool) {
        let cells: Vec<Cell> = self
            .holds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .map(|(_, c)| c)
            .collect();
        for cell in cells {
            let hold = cell.lock().unwrap_or_else(|p| p.into_inner()).take();
            if let Some(hold) = hold
                && release
            {
                self.push(PendingEntry::Release(hold));
            }
        }
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

/// This node's hold on one actor, released when dropped (hibernation).
pub(crate) struct LeaseHold {
    cell: Cell,
    epoch: u64,
    key: String,
    cluster: Arc<Cluster>,
}

impl LeaseHold {
    /// Acquires the actor's ownership record (ADR 0020, section 3).
    /// `NotOwner` carries the node that holds it. Waits for the node id
    /// check and the node lease's first write.
    pub(crate) async fn acquire(
        cluster: &Arc<Cluster>,
        address: &ActorAddress,
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
        let acquired = cluster
            .owners
            .acquire(
                &key,
                || cluster.validity.valid_in(epoch),
                async || cluster.seed_generation(address).await,
            )
            .await;
        match acquired {
            Ok(hold) => {
                if let Some(previous) = &hold.previous {
                    crate::metrics::inc(&crate::metrics::METRICS.failovers);
                    tracing::info!(actor = %address, from = %previous, generation = hold.generation, "took over an actor whose node stopped");
                }
                let cell = Arc::new(Mutex::new(Some(hold)));
                cluster.pending.register(&key, &cell);
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
            Err(AcquireError::Unresolved { generation }) => {
                cluster
                    .pending
                    .push(PendingEntry::Unresolved { key, generation });
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
            Err(AcquireError::Unavailable) => Err(OwnershipError::Unavailable(
                "the bucket did not answer".into(),
            )),
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

    /// Takes the hold out: this node stops acting as owner before any
    /// release is sent.
    fn take(&self) -> Option<Hold> {
        let hold = self.cell.lock().unwrap_or_else(|p| p.into_inner()).take();
        self.cluster.pending.unregister(&self.key, &self.cell);
        hold
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
            let sent = tokio::time::timeout(bound, cluster.owners.release(&hold))
                .await
                .unwrap_or(Released::Pending);
            match sent {
                Released::Released => return LeaseReturn::Released,
                Released::Superseded => return LeaseReturn::Superseded,
                Released::Pending if tokio::time::Instant::now() >= deadline => {
                    tracing::warn!(key = %hold.key, "releasing an actor failed; retried in the background");
                    cluster.pending.push(PendingEntry::Release(hold));
                    return LeaseReturn::Pending;
                }
                Released::Pending => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    }
}

impl Cluster {
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
        held_here: impl Fn(&str) -> bool,
    ) -> Option<PendingEntry> {
        let again = entry.clone();
        tokio::time::timeout(
            self.lease_attempt,
            wasmkeep_cluster::holds::retry(&self.owners, entry, held_here),
        )
        .await
        .unwrap_or(Some(again))
    }

    /// How long a release made in the foreground (hibernation, a
    /// hand-over, a hold dropped) waits: a lease attempt, at most a
    /// second. One that does not land is parked and retried in the
    /// background, with the full attempt.
    pub(super) fn release_attempt(&self) -> Duration {
        self.lease_attempt.min(Duration::from_secs(1))
    }

    /// One release, bounded by [`Self::release_attempt`].
    async fn send_release(&self, hold: &Hold) -> Released {
        tokio::time::timeout(self.release_attempt(), self.owners.release(hold))
            .await
            .unwrap_or(Released::Pending)
    }

    /// Sends a release once; if it did not land, it waits in the pending
    /// set.
    async fn release_or_park(&self, hold: Hold) {
        if self.send_release(&hold).await == Released::Pending {
            self.pending.push(PendingEntry::Release(hold));
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

    /// Whether this node holds `address` now, with its lease valid: a
    /// resident actor that acquired it. One that hibernated released it.
    pub(crate) fn holds_here(&self, address: &ActorAddress) -> bool {
        self.node_lease_valid() && self.pending.holds(&self.owner_key(address))
    }

    /// Whether no release or acquire of this node waits for its outcome.
    pub(crate) fn nothing_pending(&self) -> bool {
        self.pending.is_empty()
    }

    /// Waits until no release is pending or in flight, or `limit` passes.
    /// A node shutting down calls it after its actors stopped, so the
    /// releases reach the bucket before it writes `released`.
    pub(crate) async fn releases_done(&self, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while !self.pending.is_empty() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    /// One pass over the pending set: each release sent again, each
    /// unresolved acquire read (under its actor's lock) and released if it
    /// landed and no hold of it exists here. Nothing while this node's
    /// lease is not valid.
    /// `true` if some entry ended (landed, or found superseded).
    pub(super) async fn retry_pending(&self) -> bool {
        if !self.node_lease_valid()
            || self
                .pending
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        {
            return false;
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
        let outcomes: Vec<bool> = futures_util::stream::iter(entries)
            .map(|entry| async move {
                if !self.node_lease_valid()
                    || refused_in_a_row.load(Ordering::SeqCst) >= PENDING_CONCURRENCY
                {
                    self.pending.push(entry);
                    return false;
                }
                let still = match entry {
                    entry @ PendingEntry::Release(_) => self.retry_bounded(entry, |_| false).await,
                    entry @ PendingEntry::Unresolved { .. } => {
                        let lock = self.pending.lock_of(entry.key());
                        let _locked = lock.lock().await;
                        self.retry_bounded(entry, |key| self.pending.holds(key))
                            .await
                    }
                };
                match still {
                    Some(entry) => {
                        self.pending.push(entry);
                        refused_in_a_row.fetch_add(1, Ordering::SeqCst);
                        false
                    }
                    None => {
                        refused_in_a_row.store(0, Ordering::SeqCst);
                        true
                    }
                }
            })
            .buffer_unordered(PENDING_CONCURRENCY)
            .collect()
            .await;
        outcomes.into_iter().any(|ended| ended)
    }
}

/// Retries the pending set every quarter second, backing off to every 4 s
/// while passes end nothing (the bucket refusing, ADR 0020 section 3),
/// until the cluster is dropped.
pub(super) async fn pending_loop(cluster: Weak<Cluster>) {
    const FIRST: Duration = Duration::from_millis(250);
    const MOST: Duration = Duration::from_secs(4);
    let mut wait = FIRST;
    loop {
        tokio::time::sleep(wait).await;
        let Some(c) = cluster.upgrade() else {
            return;
        };
        let ended = c.retry_pending().await;
        wait = if ended || c.pending.is_empty() {
            FIRST
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
