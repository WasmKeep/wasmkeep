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
//! are what `wasmkeep_plain_events_dropped_total` counts (section 10).

use super::{ActorAddress, LeaseHold, SharedReplica};
use crate::cluster::LeaseReturn;

/// How far a hold got before it ends (ADR 0034, section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Barrier {
    /// Nothing in flight, nothing uncaptured: the acknowledged head is the
    /// local database.
    Flushed,
    /// The hold could not acknowledge what it holds, for this reason.
    #[allow(
        dead_code,
        reason = "only a batched hold gives up (ADR 0034, section 6); a strict one is always flushed"
    )]
    GaveUp(Dropped),
}

/// Why a hold ended without a flush: the `reason` of
/// `wasmkeep_plain_events_dropped_total` (ADR 0034, section 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Dropped {
    /// A clean stop's deadline passed.
    #[allow(dead_code, reason = "the clean stop's step (ADR 0034, section 10)")]
    Stop,
    /// An upload, a capture or the database worker failed.
    Failure,
    /// The hold lapsed.
    Lapse,
    /// The node lease was lost or fenced.
    #[allow(dead_code, reason = "a batched hold's end (ADR 0034, section 6)")]
    LostLease,
    /// An event panicked.
    #[allow(dead_code, reason = "a batched hold's end (ADR 0034, section 6)")]
    Panic,
    /// The app was deleted.
    #[allow(dead_code, reason = "a batched hold's end (ADR 0034, section 6)")]
    AppDeleted,
    /// The hold was dropped without a barrier.
    #[allow(dead_code, reason = "a batched hold's end (ADR 0034, section 6)")]
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
}

impl Held {
    /// A hold just acquired: nothing restored yet.
    pub(super) fn new(lease: LeaseHold) -> Self {
        Self {
            lease,
            replica: None,
        }
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

    /// The barrier this hold has reached now. A strict hold's uploads end
    /// with their events, so it is always [`Barrier::Flushed`].
    #[allow(
        clippy::unused_self,
        reason = "a batched hold's barrier depends on its uploads (ADR 0034, section 6)"
    )]
    pub(super) fn barrier(&self) -> Barrier {
        Barrier::Flushed
    }

    /// Ends the hold and releases the lease: [`Releasing`] dropped releases
    /// it in the background, as dropping the lease does;
    /// [`Releasing::returned`] waits for the release. Given up, the drop is
    /// counted first.
    pub(super) fn release(self, barrier: Barrier) -> Releasing {
        if let Barrier::GaveUp(reason) = barrier {
            self.count(reason);
        }
        let Self { lease, replica } = self;
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
                let Self { lease, replica } = self;
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
        let Self { lease, replica } = self;
        drop(lease);
        count(reason, replica.as_ref());
    }

    /// Counts the events this hold committed and had not acknowledged.
    fn count(&self, reason: Dropped) {
        count(reason, self.replica.as_ref());
    }
}

/// Counts the events committed under a hold and not acknowledged by its
/// replica, as dropped for `reason`.
fn count(reason: Dropped, replica: Option<&SharedReplica>) {
    crate::metrics::plain_events_dropped(reason.label(), unacknowledged(replica));
}

/// The events whose writes are committed and not acknowledged. A strict
/// hold acknowledges each event before the next starts: none.
fn unacknowledged(_replica: Option<&SharedReplica>) -> u64 {
    0
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
        Held::new(LeaseHold::acquire(node, x).await.unwrap())
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
