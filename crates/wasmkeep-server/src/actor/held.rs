//! This node's hold on an actor: its lease and the actor's replication,
//! kept together, and the only ways to end them (ADR 0034, section 6).
//!
//! The fields are private: no code outside this module can take, replace
//! or drop the lease or the replica apart. A hold ends through
//! [`Held::release`], [`Held::park`] or [`Held::drop_now`], and the first
//! two take the [`Barrier`] it reached. Parking needs [`Barrier::Flushed`].
//! Rust cannot forbid dropping a whole `Held`: that releases the lease in
//! the background, as dropping the lease did.
//!
//! A strict class (ADR 0034, section 1) uploads every event's writes
//! before the next event starts, so nothing is ever pending when a hold
//! ends: its barrier is always `Flushed`, and ending it does what dropping
//! the lease did before (a release in the background, or a park). Only a
//! batched hold can end with writes committed but not acknowledged; those
//! are what `wasmkeep_plain_events_dropped_total` counts (section 10). A
//! batched hold also keeps its database's capture token
//! ([`CaptureHold`]): while it lives, the database may not close or
//! restart its capture (section 2). Ending the hold drops it first.

use super::{ActorAddress, LeaseHold, SharedReplica};
use crate::apps::Writes;
use crate::cluster::LeaseReturn;
use wasmkeep_storage::CaptureHold;

/// How far a hold got before it ends (ADR 0034, section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Barrier {
    /// Nothing in flight, nothing uncaptured: the acknowledged head is the
    /// local database.
    Flushed,
    /// The hold could not acknowledge what it holds, for this reason.
    GaveUp(Dropped),
}

/// Why a hold ended without a flush: the `reason` of
/// `wasmkeep_plain_events_dropped_total` (ADR 0034, section 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Dropped {
    /// A clean stop's deadline passed.
    Stop,
    /// An upload, a capture or the database worker failed.
    Failure,
    /// The hold lapsed.
    Lapse,
    /// The node lease was lost or fenced.
    #[allow(dead_code, reason = "a batched hold's end (ADR 0034, section 6)")]
    LostLease,
    /// An event panicked.
    Panic,
    /// The app was deleted.
    AppDeleted,
    /// The hold was dropped without a barrier.
    Unbarriered,
}

impl Dropped {
    /// The metric's label (one of [`crate::metrics::PLAIN_DROP_REASONS`]).
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Failure => "failure",
            Self::Lapse => "lapse",
            Self::LostLease => "lost_lease",
            Self::Panic => "panic",
            Self::AppDeleted => "app_deleted",
            Self::Unbarriered => "unbarriered",
        }
    }
}

/// This node's hold on an actor: the lease, and the replica once the actor
/// is restored.
pub(super) struct Held {
    lease: LeaseHold,
    replica: Option<SharedReplica>,
    /// How this hold's plain writes reach the bucket (ADR 0034, section
    /// 1): the class's mode when the hold was acquired, changed only at a
    /// `Flushed` barrier, never read from the class at each use.
    writes: Writes,
    /// A batched hold's capture token, while its database captures in
    /// batched mode. Dropped before the lease and the replica.
    capture: Option<CaptureHold>,
}

impl Held {
    /// A hold just acquired, under the class's mode of writes: nothing
    /// restored yet.
    pub(super) fn new(lease: LeaseHold, writes: Writes) -> Self {
        Self {
            lease,
            replica: None,
            writes,
            capture: None,
        }
    }

    /// The capture token the database's batched capture is enabled under,
    /// made the first time it is asked for.
    pub(super) fn capture_token(&mut self) -> &CaptureHold {
        self.capture.get_or_insert_with(CaptureHold::default)
    }

    /// Whether the hold keeps a capture token.
    pub(super) fn captures(&self) -> bool {
        self.capture.is_some()
    }

    /// Drops the capture token: at a `Flushed` barrier, once the database
    /// left batched capture, or before a database that is being deleted
    /// closes.
    pub(super) fn end_capture(&mut self) {
        self.capture = None;
    }

    /// The hold's mode of writes.
    pub(super) fn writes(&self) -> Writes {
        self.writes
    }

    /// Switches the hold to `writes` (a deploy changed the class's mode)
    /// if `barrier` is [`Barrier::Flushed`]: nothing is then in flight or
    /// uncaptured, so the next event may run under either mode. Returns
    /// whether it switched; a hold that gave up keeps its mode until it
    /// ends.
    pub(super) fn switch_writes(&mut self, writes: Writes, barrier: Barrier) -> bool {
        if barrier != Barrier::Flushed {
            return false;
        }
        self.writes = writes;
        true
    }

    /// The actor restored under this hold: its replication from now on.
    pub(super) fn restored(&mut self, replica: SharedReplica) {
        debug_assert!(self.replica.is_none(), "a hold restored twice");
        self.replica = Some(replica);
    }

    /// The lease, to read (its validity, its expiry, its generation).
    pub(super) fn lease(&self) -> &LeaseHold {
        &self.lease
    }

    /// Whether this node may still act as the owner.
    pub(super) fn is_valid(&self) -> bool {
        self.lease.is_valid()
    }

    /// The replication, once restored.
    pub(super) fn replica(&self) -> Option<&SharedReplica> {
        self.replica.as_ref()
    }

    /// The barrier this hold has reached now, without waiting: `Flushed`
    /// with nothing pending, otherwise given up for `reason`. A strict
    /// hold's uploads end with their events, so it is always
    /// [`Barrier::Flushed`]. A batched hold is flushed when its replica has
    /// nothing marked, in flight or to apply (a replica in use is not); the
    /// wait for it is `ActorState::flush_to_barrier`.
    pub(super) fn barrier_or(&self, reason: Dropped) -> Barrier {
        if self.writes == Writes::Strict || self.pending() == Some(false) {
            Barrier::Flushed
        } else {
            Barrier::GaveUp(reason)
        }
    }

    /// The barrier a state's drop finds (ADR 0034, section 6). A batched
    /// hold is flushed there only if the step before the drop found it so
    /// (`stepped`), its replica has nothing pending, and its database has
    /// no commit after the last boundary (`unmarked`, `None`: cannot tell,
    /// the connection being with work cut short). Otherwise it gives up:
    /// `panic` without a step (the mailbox drops a panicked state outside
    /// it), `unbarriered` with one. A strict hold is always flushed.
    pub(super) fn barrier_at_drop(&self, stepped: bool, unmarked: Option<bool>) -> Barrier {
        if self.writes == Writes::Strict {
            return Barrier::Flushed;
        }
        if stepped && unmarked == Some(false) && self.pending() == Some(false) {
            return Barrier::Flushed;
        }
        Barrier::GaveUp(if stepped {
            Dropped::Unbarriered
        } else {
            Dropped::Panic
        })
    }

    /// [`Self::barrier_or`], a hold not flushed counted `unbarriered`.
    pub(super) fn barrier(&self) -> Barrier {
        self.barrier_or(Dropped::Unbarriered)
    }

    /// Whether a batched hold has anything not acknowledged: `None` when
    /// its replica is in use and cannot tell now. A hold not restored has
    /// nothing.
    fn pending(&self) -> Option<bool> {
        match &self.replica {
            None => Some(false),
            Some(r) => r.try_lock().ok().map(|r| !r.nothing_pending()),
        }
    }

    /// Ends the hold and releases the lease: [`Releasing`] dropped releases
    /// it in the background, as dropping the lease does;
    /// [`Releasing::returned`] waits for the release. Given up, the drop is
    /// counted first.
    pub(super) fn release(self, barrier: Barrier) -> Releasing {
        if let Barrier::GaveUp(reason) = barrier {
            self.count(reason);
        }
        let Self {
            lease,
            replica,
            capture,
            ..
        } = self;
        drop(capture);
        drop(replica);
        Releasing(lease)
    }

    /// Ends the hold by parking it (ADR 0024): the record keeps naming this
    /// node. Only a flushed hold parks; one that gave up is dropped now
    /// instead. A hold that may no longer act is released, as
    /// [`LeaseHold::park`] does.
    pub(super) fn park(self, barrier: Barrier, address: &ActorAddress) {
        match barrier {
            Barrier::Flushed => {
                let Self {
                    lease,
                    replica,
                    capture,
                    ..
                } = self;
                drop(capture);
                lease.park(address);
                drop(replica);
            }
            Barrier::GaveUp(reason) => self.drop_now(reason),
        }
    }

    /// Ends the hold now, without waiting (ADR 0034, section 6): the
    /// lease's cell is emptied first (a later upload finds it gone) and its
    /// release sent in the background, then what the hold had not
    /// acknowledged is counted as dropped.
    pub(super) fn drop_now(self, reason: Dropped) {
        self.drop_now_with(reason, 0);
    }

    /// [`Self::drop_now`], counting `unmarked` more events: committed after
    /// the last boundary, so known to no mark.
    pub(super) fn drop_now_with(self, reason: Dropped, unmarked: u64) {
        let dropped = self.unacknowledged().saturating_add(unmarked);
        let Self {
            lease,
            replica,
            capture,
            ..
        } = self;
        drop(capture);
        drop(lease);
        drop(replica);
        crate::metrics::plain_events_dropped(reason.label(), dropped);
    }

    /// Counts the events this hold committed and had not acknowledged.
    fn count(&self, reason: Dropped) {
        crate::metrics::plain_events_dropped(reason.label(), self.unacknowledged());
    }

    /// The events whose writes are committed and not acknowledged. A
    /// strict hold acknowledges each event before the next starts: none.
    /// A batched one's replica counts them (marked and uncaptured, and in
    /// flight); one in use at the moment counts as one.
    fn unacknowledged(&self) -> u64 {
        if self.writes == Writes::Strict {
            return 0;
        }
        match &self.replica {
            None => 0,
            Some(r) => r.try_lock().map_or(1, |r| r.unacknowledged_events()),
        }
    }
}

/// A hold that has ended, its lease on the way back. Dropped, the lease is
/// released in the background.
pub(super) struct Releasing(LeaseHold);

impl Releasing {
    /// Releases the lease now, telling when another node may take the
    /// actor ([`LeaseHold::release`]).
    pub(super) async fn returned(self) -> LeaseReturn {
        self.0.release().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::ObjectStore;
    use wasmkeep_cluster::lease::LeaseConfig;
    use wasmkeep_cluster::state_root::StateRoot;

    use super::*;
    use crate::cluster::{Cluster, ClusterConfig};

    const ALL: [Dropped; 7] = [
        Dropped::Stop,
        Dropped::Failure,
        Dropped::Lapse,
        Dropped::LostLease,
        Dropped::Panic,
        Dropped::AppDeleted,
        Dropped::Unbarriered,
    ];

    async fn node(store: &Arc<dyn ObjectStore>) -> Arc<Cluster> {
        let mut config = ClusterConfig::new("a", "http://a-held", Arc::clone(store));
        config.heartbeat = Duration::from_millis(100);
        config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        let node = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        node.identity().await.unwrap();
        let started = std::time::Instant::now();
        while !node.node_lease_valid() {
            assert!(started.elapsed() < Duration::from_secs(3), "never valid");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        node
    }

    async fn held(node: &Arc<Cluster>, x: &ActorAddress) -> Held {
        Held::new(LeaseHold::acquire(node, x).await.unwrap(), Writes::Strict)
    }

    /// The mode is the hold's, set at acquire, and changes only at a
    /// `Flushed` barrier (ADR 0034, section 1).
    #[tokio::test]
    async fn a_hold_keeps_its_mode_until_a_flushed_barrier() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        let mut hold = held(&a, &x).await;
        assert_eq!(hold.writes(), Writes::Strict);
        assert!(!hold.switch_writes(Writes::Batched, Barrier::GaveUp(Dropped::Failure)));
        assert_eq!(
            hold.writes(),
            Writes::Strict,
            "not past a hold that gave up"
        );
        assert!(hold.switch_writes(Writes::Batched, Barrier::Flushed));
        assert_eq!(hold.writes(), Writes::Batched);
        let barrier = hold.barrier();
        assert!(hold.switch_writes(Writes::Strict, barrier));
        assert_eq!(hold.writes(), Writes::Strict);
        drop(hold.release(Barrier::Flushed));
        released(&a, &x).await;
    }

    /// A state's drop (ADR 0034, section 6): a batched hold parks only when
    /// its step found it flushed and its database has no commit the marks
    /// miss; otherwise it gives up, as a panic's unless a step had found it
    /// flushed. A strict hold is always flushed.
    #[tokio::test]
    async fn a_dropped_state_parks_a_batched_hold_only_when_nothing_is_unmarked() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        let strict = held(&a, &x).await;
        for unmarked in [None, Some(true), Some(false)] {
            assert_eq!(strict.barrier_at_drop(false, unmarked), Barrier::Flushed);
        }
        drop(strict.release(Barrier::Flushed));
        released(&a, &x).await;
        let batched = Held::new(LeaseHold::acquire(&a, &x).await.unwrap(), Writes::Batched);
        let gave_up = |reason| Barrier::GaveUp(reason);
        assert_eq!(
            batched.barrier_at_drop(false, Some(false)),
            gave_up(Dropped::Panic)
        );
        assert_eq!(
            batched.barrier_at_drop(false, None),
            gave_up(Dropped::Panic)
        );
        assert_eq!(
            batched.barrier_at_drop(true, None),
            gave_up(Dropped::Unbarriered)
        );
        assert_eq!(
            batched.barrier_at_drop(true, Some(true)),
            gave_up(Dropped::Unbarriered)
        );
        assert_eq!(batched.barrier_at_drop(true, Some(false)), Barrier::Flushed);
        drop(batched.release(Barrier::Flushed));
        released(&a, &x).await;
    }

    /// A count lands on its reason's series, and only a positive one.
    #[test]
    fn a_drop_is_counted_under_its_reason() {
        let series = |r: &str| {
            crate::metrics::METRICS
                .plain_events_dropped
                .get(&[("reason", r)])
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let before = (series("failure"), series("lapse"));
        crate::metrics::plain_events_dropped(Dropped::Failure.label(), 2);
        crate::metrics::plain_events_dropped(Dropped::Lapse.label(), 0);
        assert_eq!(
            (series("failure"), series("lapse")),
            (before.0 + 2, before.1)
        );
    }

    fn dropped_total() -> u64 {
        crate::metrics::METRICS.plain_events_dropped.total()
    }

    async fn released(node: &Cluster, x: &ActorAddress) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while node.holds_here(x) || !node.nothing_pending() {
            assert!(tokio::time::Instant::now() < deadline, "never released");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[test]
    fn every_reason_is_a_seeded_label() {
        let labels = ALL.map(Dropped::label);
        assert_eq!(labels, crate::metrics::PLAIN_DROP_REASONS);
    }

    /// A strict hold is flushed at once, and parks as the lease does.
    #[tokio::test]
    async fn a_strict_hold_is_flushed_and_parks() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        let hold = held(&a, &x).await;
        assert_eq!(hold.barrier(), Barrier::Flushed);
        hold.park(Barrier::Flushed, &x);
        assert!(a.is_parked(&x));
        a.release_all_parked("stop").await;
        assert_eq!(dropped_total(), 0);
    }

    /// The step before a drop finds a strict hold at its barrier at once,
    /// whatever the reason it would give up for (ADR 0034, section 10).
    #[tokio::test]
    async fn a_strict_hold_is_at_its_barrier_at_once() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        let hold = held(&a, &x).await;
        for reason in ALL {
            assert_eq!(hold.barrier_or(reason), Barrier::Flushed);
        }
        drop(hold.release(Barrier::Flushed));
        released(&a, &x).await;
        assert_eq!(dropped_total(), 0);
    }

    /// Parking needs `Flushed`: a hold that gave up is released instead.
    #[tokio::test]
    async fn a_hold_that_gave_up_is_released_not_parked() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        held(&a, &x)
            .await
            .park(Barrier::GaveUp(Dropped::Failure), &x);
        assert!(!a.is_parked(&x));
        released(&a, &x).await;
    }

    /// Releasing waits for the record when asked; dropping a strict hold
    /// releases it in the background; neither counts a drop.
    #[tokio::test]
    async fn a_strict_hold_ends_without_counting_a_drop() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = node(&store).await;
        let x = ActorAddress::new("t", "c", "x").unwrap();
        let hold = held(&a, &x).await;
        let barrier = hold.barrier();
        assert_eq!(
            hold.release(barrier).returned().await,
            LeaseReturn::Released
        );
        for reason in ALL {
            held(&a, &x).await.drop_now(reason);
            released(&a, &x).await;
        }
        let hold = held(&a, &x).await;
        let barrier = hold.barrier();
        drop(hold.release(barrier));
        released(&a, &x).await;
        assert_eq!(dropped_total(), 0);
    }
}
