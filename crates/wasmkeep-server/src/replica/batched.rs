//! Batched plain writes: the uploader inside [`Replica`] (ADR 0034,
//! sections 2, 5 and 8).
//!
//! An actor of a batched class has at most one segment in flight. Its
//! replica keeps two positions apart: the **acknowledged** head
//! ([`Replica::head`] and its checksum, what the sidecar names, what
//! snapshots, resets and `local_is_at` are judged on) and the **captured**
//! position ([`Replica::captured`], the end of the segment in flight, or
//! the acknowledged head when nothing is). A segment goes through four
//! steps:
//!
//! 1. **Reserve.** The caller plans the capture
//!    (`ActorDb::next_bounded_capture`) and reserves its bytes in the
//!    node's [`UploadBudget`] ([`UploadBudget::reserve`] on the plan's
//!    estimate), in FIFO order, before anything is captured:
//!    a reservation cancelled while it waits consumes nothing (and leaves
//!    its place in the queue).
//! 2. **Prepare.** [`Replica::prepare`] checks that nothing is in flight
//!    and nothing failed, picks the next txid on the acknowledged head and
//!    puts the segment in flight. It returns a [`Ticket`].
//! 3. **Hand off.** [`Ticket::hand_off`] is one synchronous step, run where
//!    the database is (the database worker): capture the reserved plan,
//!    check both tripwires (the capture chains from the captured position;
//!    its pre-apply checksum is the acknowledged one), encode, and spawn the
//!    upload task. Nothing in it awaits, so a cut of the guest's wait (the
//!    wall, a lease cut, `Cut::Replica`) never interrupts it half-way: once
//!    the WAL is consumed, the segment reaches the task, or the replica
//!    fails.
//! 4. **Upload, then apply.** The task runs the same bounded PUT as a
//!    strict segment ([`UploadTarget::put_bounded`]), frees its budget
//!    permit as soon as that PUT ends, checks the acknowledgement rule and
//!    records the outcome in this replica's own cell, publishing a failure
//!    at once on the failure flag. [`Replica::apply`] then applies it on
//!    the actor's side: the acknowledged head moves, the sidecar is written
//!    (only then), and the waiters it satisfies wake.
//!
//! The cell belongs to one `Replica`, and a `Replica` to one hold (a new
//! hold builds a new one), so a completion can never apply to another hold
//! than the one that uploaded (ADR 0034, section 2; the model's
//! `CompletionOtherHold`).
//!
//! Strict classes never come here: their writes keep [`Replica::ship`].
//! Nothing outside the tests calls this module before S9 wires it.

#![allow(
    dead_code,
    reason = "wired to actors with batched writes in S9 (ADR 0034, plan)"
)]

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use wasmkeep_cluster::replication::{Segment, SegmentId, segment_key};
use wasmkeep_storage::ActorDb;
use wasmkeep_storage::ltx::DbChecksum;
use wasmkeep_storage::wal::{BoundaryPlan, DEFAULT_SEGMENT_BYTES};

use super::{Replica, ReplicaError, UploadTarget};

/// The node-wide budget of batched segments in flight, in encoded bytes
/// (ADR 0034, section 2), as one node's restores are bounded.
pub(crate) const NODE_UPLOAD_BYTES: u64 = 256 << 20;

/// The encoded bytes of every batched actor's segment in flight on this
/// node (ADR 0034, section 2): a FIFO semaphore, so captures get their
/// room in the order they asked. A segment charges its estimate, at most
/// the cap, whatever its size: an oversize event (uploaded alone, its
/// actor waiting for it) cannot hold the whole budget.
#[derive(Debug)]
pub(crate) struct UploadBudget {
    permits: Arc<Semaphore>,
    total: u64,
    cap: u64,
}

impl UploadBudget {
    /// A budget of `total` bytes, charging a segment at most `cap`. Both
    /// are at least one byte; `total` is at most `u32::MAX` (a permit's
    /// unit is a byte) and `cap` at most `total`.
    pub(crate) fn new(total: u64, cap: u64) -> Self {
        let total = total.clamp(1, u64::from(u32::MAX));
        let cap = cap.clamp(1, total);
        Self {
            permits: Arc::new(Semaphore::new(usize::try_from(total).unwrap_or(usize::MAX))),
            total,
            cap,
        }
    }

    /// The node's: [`NODE_UPLOAD_BYTES`], charging at most the default
    /// segment cap ([`DEFAULT_SEGMENT_BYTES`], 1 MiB).
    pub(crate) fn node() -> Self {
        Self::new(NODE_UPLOAD_BYTES, DEFAULT_SEGMENT_BYTES)
    }

    /// What a segment estimated at `estimated` bytes charges.
    pub(crate) fn charge(&self, estimated: u64) -> u64 {
        estimated.clamp(1, self.cap)
    }

    /// Waits for room for a segment estimated at `estimated` bytes (a
    /// plan's `boundary.estimated_bytes`), behind every reservation that
    /// asked before it. Cancel-safe: a reservation dropped while it waits
    /// takes nothing, and gives up its place.
    pub(crate) async fn reserve(&self, estimated: u64) -> Result<BudgetPermit, ReplicaError> {
        let charge = self.charge(estimated);
        let units = u32::try_from(charge).unwrap_or(u32::MAX);
        let permit = Arc::clone(&self.permits)
            .acquire_many_owned(units)
            .await
            .map_err(|_| ReplicaError::Local("the node's upload budget is closed".into()))?;
        Ok(BudgetPermit {
            _permit: permit,
            charged: charge,
            cap: self.cap,
        })
    }

    /// The bytes not reserved now.
    pub(crate) fn available(&self) -> u64 {
        u64::try_from(self.permits.available_permits()).unwrap_or(u64::MAX)
    }

    /// The whole budget.
    pub(crate) fn total(&self) -> u64 {
        self.total
    }
}

/// One segment's room in the [`UploadBudget`]. It moves into the segment's
/// upload task and is freed when that task's PUT ends, not at a barrier: a
/// hold given up can leave a PUT running for up to an upload's budget, and
/// its bytes are still held.
#[derive(Debug)]
pub(crate) struct BudgetPermit {
    _permit: OwnedSemaphorePermit,
    charged: u64,
    cap: u64,
}

impl BudgetPermit {
    /// The bytes it holds.
    pub(crate) fn charged(&self) -> u64 {
        self.charged
    }
}

/// Why a batched segment was not acknowledged, as its task records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UploadFailure {
    /// It completed after the hold could no longer act.
    Late,
    /// Another object sits at its key.
    Exists,
    /// The bucket refused it or did not answer within the upload's budget.
    Bucket(String),
    /// Its capture or encoding failed, or did not chain (local).
    Local(String),
}

impl UploadFailure {
    fn from_error(e: &ReplicaError) -> Self {
        match e {
            ReplicaError::Late => Self::Late,
            ReplicaError::Exists => Self::Exists,
            ReplicaError::Bucket(m) => Self::Bucket(m.clone()),
            other => Self::Local(other.to_string()),
        }
    }

    fn error(&self) -> ReplicaError {
        match self {
            Self::Late => ReplicaError::Late,
            Self::Exists => ReplicaError::Exists,
            Self::Bucket(m) => ReplicaError::Bucket(m.clone()),
            Self::Local(m) => ReplicaError::Local(m.clone()),
        }
    }
}

impl std::fmt::Display for UploadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error().fmt(f)
    }
}

/// Where the segment in flight is, as the cell records it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    /// Nothing in flight.
    Idle,
    /// Prepared, not handed off yet: nothing captured.
    Reserved(SegmentId),
    /// Captured, encoded and with its task: the captured position is its
    /// end.
    Uploading {
        id: SegmentId,
        post_apply: DbChecksum,
    },
    /// The task (or the hand-off) ended; waiting to be applied.
    Landed {
        id: SegmentId,
        post_apply: DbChecksum,
        result: Result<(), UploadFailure>,
    },
    /// A ticket dropped before its capture: nothing consumed.
    Abandoned(SegmentId),
}

/// What a replica shares with its tickets and upload tasks: the outcome
/// cell and the failure flag. Owned by one `Replica`, so by one hold.
#[derive(Debug)]
struct Shared {
    slot: watch::Sender<Slot>,
    /// Set by the upload task (or the hand-off) the moment a segment fails,
    /// before anyone applies it, for `Cut::Replica` (ADR 0034, section 8)
    /// and the waiters. Strict's inline failures never touch it.
    failure: watch::Sender<Option<UploadFailure>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            slot: watch::Sender::new(Slot::Idle),
            failure: watch::Sender::new(None),
        }
    }

    /// Records `failure` on the flag (the first one stays) and, for
    /// segment `id` ending at `post_apply`, in the cell.
    fn failed(&self, id: SegmentId, post_apply: DbChecksum, failure: UploadFailure) {
        self.failure.send_if_modified(|f| {
            if f.is_some() {
                return false;
            }
            *f = Some(failure.clone());
            true
        });
        self.slot.send_replace(Slot::Landed {
            id,
            post_apply,
            result: Err(failure),
        });
    }
}

/// The segment in flight, as its replica knows it.
#[derive(Debug, Clone, Copy)]
struct Flight {
    id: SegmentId,
    /// The last boundary it carries (`WalBoundary::sequence`).
    sequence: u64,
    /// Whole events (boundaries) it carries.
    events: usize,
    /// One event alone over the byte cap: uploaded strict-style, its
    /// actor to wait for it at the event's end.
    oversize: bool,
}

/// A replica's batched state. Idle, and never used, in a strict class.
#[derive(Debug)]
pub(super) struct Uploads {
    shared: Arc<Shared>,
    flight: Option<Flight>,
    /// The last boundary sequence the acknowledged head covers, published
    /// once its sidecar is written. Boundary sequences start at 1, so 0
    /// covers nothing.
    acked: watch::Sender<u64>,
    /// An acknowledged boundary whose sidecar write has not completed (an
    /// apply cancelled while writing it): the next apply writes it again,
    /// then publishes it.
    sidecar_due: Option<u64>,
}

impl Default for Uploads {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared::new()),
            flight: None,
            acked: watch::Sender::new(0),
            sidecar_due: None,
        }
    }
}

/// What [`Replica::apply`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Applied {
    /// No outcome to apply: nothing in flight, or still uploading.
    Nothing,
    /// The segment was acknowledged: the acknowledged head is now `0`.
    Acknowledged(SegmentId),
    /// A ticket was dropped before its capture: nothing is in flight.
    Abandoned,
}

impl Replica {
    /// The captured position: the end of the segment in flight once it is
    /// captured, otherwise the acknowledged head (ADR 0034, section 5).
    /// The next capture chains from it.
    pub(crate) fn captured(&self) -> (SegmentId, DbChecksum) {
        match &*self.batched.shared.slot.borrow() {
            Slot::Uploading { id, post_apply } | Slot::Landed { id, post_apply, .. } => {
                (*id, *post_apply)
            }
            Slot::Idle | Slot::Reserved(_) | Slot::Abandoned(_) => (self.head, self.checksum),
        }
    }

    /// The acknowledged head and its checksum: what the sidecar names, and
    /// what every rule asking "the head" asks (ADR 0034, section 5).
    pub(crate) fn acknowledged_position(&self) -> (SegmentId, DbChecksum) {
        (self.head, self.checksum)
    }

    /// Whether a capture may start now without refusal: nothing in flight,
    /// nothing to apply, nothing failed. Snapshots and resets want the
    /// same (the guards of ADR 0034, section 7, fail the replica when they
    /// refuse; this asks without failing).
    pub(crate) fn uploader_idle(&self) -> bool {
        self.in_flight.is_none()
            && self.batched.flight.is_none()
            && self.failed.is_none()
            && self.batched.shared.failure.borrow().is_none()
            && matches!(*self.batched.shared.slot.borrow(), Slot::Idle)
    }

    /// Whether the segment in flight is one event over the byte cap, which
    /// its actor waits for at the event's end (ADR 0034, section 2).
    pub(crate) fn in_flight_oversize(&self) -> bool {
        self.batched.flight.is_some_and(|f| f.oversize)
    }

    /// The events (boundaries) the segment in flight carries.
    pub(crate) fn in_flight_events(&self) -> usize {
        self.batched.flight.map_or(0, |f| f.events)
    }

    /// The failure flag (ADR 0034, section 8): set the moment a batched
    /// upload or capture fails, before the outcome is applied. Only this
    /// module sets it, never strict's inline failures.
    pub(crate) fn failure_flag(&self) -> watch::Receiver<Option<UploadFailure>> {
        self.batched.shared.failure.subscribe()
    }

    /// Puts the next segment in flight, for `plan` (from
    /// `ActorDb::next_bounded_capture`) and the room reserved for it.
    /// The caller then runs [`Ticket::hand_off`] on the database, with the
    /// mailbox still serialized since the plan was made.
    ///
    /// Refused, with nothing changed, after a failure (applied or only
    /// flagged: a batched hold never captures again after one), before the
    /// first snapshot, for a permit smaller than the plan's charge, or
    /// outside a runtime. Refused, and replication stopped, with a segment
    /// already in flight: one in flight keeps the chain a chain, and a
    /// caller that starts a second is a bug (ask [`Self::uploader_idle`]).
    pub(crate) fn prepare(
        &mut self,
        plan: BoundaryPlan,
        permit: BudgetPermit,
    ) -> Result<Ticket, ReplicaError> {
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        if let Some(failure) = &*self.batched.shared.failure.borrow() {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {failure}"
            )));
        }
        if self.needs_snapshot {
            return Err(ReplicaError::Local(
                "the first snapshot is not uploaded yet".into(),
            ));
        }
        if let Some(segment) = self.in_flight.or(self.batched.flight.map(|f| f.id)) {
            return Err(self.refuse_batched(format!("a segment while {segment:?} is in flight")));
        }
        if !matches!(*self.batched.shared.slot.borrow(), Slot::Idle) {
            return Err(
                self.refuse_batched("a segment while an outcome waits to be applied".into())
            );
        }
        if permit.charged < plan.boundary.estimated_bytes.clamp(1, permit.cap) {
            return Err(ReplicaError::Local(format!(
                "a permit of {} bytes for a segment charging {}",
                permit.charged,
                plan.boundary.estimated_bytes.clamp(1, permit.cap)
            )));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| ReplicaError::Local(format!("no runtime for the upload: {e}")))?;
        let segment = Segment {
            id: SegmentId {
                generation: self.generation,
                txid: self.next_txid()?,
            },
            parent_generation: self.head.generation,
        };
        let (captured_id, captured) = self.captured();
        debug_assert_eq!(captured_id, self.head, "nothing in flight");
        self.in_flight = Some(segment.id);
        self.batched.flight = Some(Flight {
            id: segment.id,
            sequence: plan.boundary.sequence,
            events: plan.boundaries,
            oversize: plan.oversize,
        });
        self.batched
            .shared
            .slot
            .send_replace(Slot::Reserved(segment.id));
        Ok(Ticket {
            plan,
            permit: Some(permit),
            segment,
            captured,
            acknowledged: self.checksum,
            target: self.target(),
            shared: Arc::clone(&self.batched.shared),
            runtime,
            consumed: false,
            reported: false,
        })
    }

    /// Resolves once the segment in flight has an outcome to apply (or at
    /// once when none is in flight). Borrows nothing: the caller awaits it
    /// without holding the replica, then calls [`Self::apply`].
    pub(crate) fn landed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut slot = self.batched.shared.slot.subscribe();
        async move {
            let _ = slot
                .wait_for(|s| !matches!(s, Slot::Reserved(_) | Slot::Uploading { .. }))
                .await;
        }
    }

    /// Applies the outcome in the cell, if there is one (ADR 0034, section
    /// 2). Acknowledged: the acknowledged head and its checksum move, the
    /// segment is no longer in flight, the sidecar is written, and only
    /// then the waiters it covers wake. Failed: replication stops, and
    /// every waiter fails. A ticket dropped before its capture: nothing is
    /// in flight any more.
    ///
    /// Everything but the sidecar write happens before the first await,
    /// so an apply cancelled half-way leaves the sidecar behind the
    /// acknowledged head, never ahead of it; the next apply writes it.
    pub(crate) async fn apply(&mut self) -> Result<Applied, ReplicaError> {
        let slot = self.batched.shared.slot.borrow().clone();
        let applied = match slot {
            Slot::Idle | Slot::Reserved(_) | Slot::Uploading { .. } => Applied::Nothing,
            Slot::Abandoned(id) => {
                self.end_flight(id)?;
                Applied::Abandoned
            }
            Slot::Landed { id, .. } if self.failed.is_some() => {
                // Replication stopped meanwhile (a refused snapshot or
                // reset): nothing more is recorded under this hold, and its
                // waiters fail.
                let _ = self.end_flight(id);
                let reason = self.failed.clone().unwrap_or_default();
                self.stop_batched(UploadFailure::Local(reason.clone()));
                return Err(ReplicaError::Local(format!(
                    "replication stopped: {reason}"
                )));
            }
            Slot::Landed {
                id,
                result: Err(failure),
                ..
            } => {
                let _ = self.end_flight(id);
                self.stop_batched(failure.clone());
                return Err(failure.error());
            }
            Slot::Landed {
                id,
                post_apply,
                result: Ok(()),
            } => {
                let flight = self.end_flight(id)?;
                self.head = id;
                self.checksum = post_apply;
                self.since_snapshot += 1;
                self.batched.sidecar_due = Some(flight.sequence);
                Applied::Acknowledged(id)
            }
        };
        if let Some(sequence) = self.batched.sidecar_due {
            if let Err(e) = self.record(self.checksum).await {
                self.stop_batched(UploadFailure::Local(e.to_string()));
                return Err(e);
            }
            self.batched.sidecar_due = None;
            self.batched.acked.send_if_modified(|acked| {
                let moved = sequence > *acked;
                *acked = (*acked).max(sequence);
                moved
            });
        }
        Ok(applied)
    }

    /// Ends the flight `id` the cell names: checked against this replica's
    /// own (a completion for any other segment is a bug, and stops
    /// replication rather than moving the head).
    fn end_flight(&mut self, id: SegmentId) -> Result<Flight, ReplicaError> {
        let flight = self.batched.flight.take();
        self.in_flight = None;
        self.batched.shared.slot.send_replace(Slot::Idle);
        match flight {
            Some(f) if f.id == id => Ok(f),
            other => {
                let reason = format!(
                    "a completion for segment {id:?}, while {:?} was in flight here",
                    other.map(|f| f.id)
                );
                self.stop_batched(UploadFailure::Local(reason.clone()));
                Err(ReplicaError::Local(reason))
            }
        }
    }

    /// [`Replica::refuse`], for a batched caller's bug: replication stops,
    /// and the failure flag is raised so the waiters fail too.
    fn refuse_batched(&mut self, reason: String) -> ReplicaError {
        let refused = self.refuse(reason);
        self.stop_batched(UploadFailure::Local(refused.to_string()));
        refused
    }

    /// Stops a batched replica: `failed` set, the failure flag raised, and
    /// with it every waiter failed. Nothing is captured again.
    fn stop_batched(&mut self, failure: UploadFailure) {
        self.fail(failure.to_string());
        self.batched.shared.failure.send_if_modified(|f| {
            if f.is_some() {
                return false;
            }
            *f = Some(failure);
            true
        });
    }

    /// Whether the acknowledged head covers the boundary `sequence`
    /// (`WalBoundary::sequence`): every commit up to it is in the bucket,
    /// uploaded while this hold was valid, and the sidecar says so.
    pub(crate) fn acknowledged_covers(&self, sequence: u64) -> bool {
        *self.batched.acked.borrow() >= sequence
    }

    /// Waits until the acknowledged head covers the boundary `sequence`:
    /// what `flush` and a batched `exec-durable` wait for (S9). Never on
    /// the captured position. Fails at once when a batched upload fails
    /// (the failure flag), when replication had already stopped, and when
    /// this replica (the hold) is dropped. Borrows nothing; something else
    /// must apply the outcomes ([`Self::apply`]) for it to resolve.
    pub(crate) fn wait_acknowledged(
        &self,
        sequence: u64,
    ) -> impl Future<Output = Result<(), ReplicaError>> + Send + 'static {
        let mut acked = self.batched.acked.subscribe();
        let mut failure = self.batched.shared.failure.subscribe();
        let stopped = self.failed.clone();
        async move {
            loop {
                if *acked.borrow_and_update() >= sequence {
                    return Ok(());
                }
                if let Some(reason) = &stopped {
                    return Err(ReplicaError::Local(format!(
                        "replication stopped: {reason}"
                    )));
                }
                if let Some(f) = &*failure.borrow_and_update() {
                    return Err(ReplicaError::Local(format!("replication stopped: {f}")));
                }
                let ended = tokio::select! {
                    r = acked.changed() => r.is_err(),
                    r = failure.changed() => r.is_err(),
                };
                if ended && *acked.borrow() < sequence {
                    return Err(ReplicaError::Local(
                        "replication stopped: the hold ended".into(),
                    ));
                }
            }
        }
    }
}

/// A segment put in flight by [`Replica::prepare`], to be captured and
/// handed to its upload task by [`Self::hand_off`]. Dropped unused, it
/// takes the segment out of flight again (nothing was captured).
#[derive(Debug)]
pub(crate) struct Ticket {
    plan: BoundaryPlan,
    permit: Option<BudgetPermit>,
    segment: Segment,
    /// The captured position's checksum: the capture must chain from it.
    captured: DbChecksum,
    /// The acknowledged head's checksum: the segment's pre-apply checksum
    /// must be it when its upload starts (the fork detector).
    acknowledged: DbChecksum,
    target: UploadTarget,
    shared: Arc<Shared>,
    runtime: tokio::runtime::Handle,
    /// The WAL was consumed for this segment.
    consumed: bool,
    /// An outcome (the upload task, or a failure) is in the cell.
    reported: bool,
}

impl Ticket {
    /// The segment this ticket uploads.
    pub(crate) fn segment(&self) -> SegmentId {
        self.segment.id
    }

    /// Captures the reserved plan from `db`, checks it, encodes it and
    /// hands it to its upload task, in one step with no await: run it on
    /// the database's worker, which completes it whatever happens to the
    /// guest call waiting for it (ADR 0034, section 2). Both tripwires of
    /// section 5 are checked here: the capture's pre-apply checksum must be
    /// the captured position's (the chain) and the acknowledged head's (the
    /// fork detector); with one segment in flight they are the same check,
    /// and a capture started early fails it.
    ///
    /// A failure of any kind fails the replica: the failure flag is raised
    /// now, and the outcome waits in the cell for [`Replica::apply`].
    pub(crate) fn hand_off(mut self, db: &mut ActorDb) -> Result<SegmentId, ReplicaError> {
        let id = self.segment.id;
        let got = match db.take_bounded_changes(self.plan) {
            Ok(got) => got,
            Err(e) => return Err(self.fail(format!("capture: {e}"))),
        };
        self.consumed = true;
        let changes = got.changes;
        if changes.pre_apply != self.captured {
            return Err(self.fail(format!(
                "a capture from {:x} does not chain from the captured position {:x}",
                changes.pre_apply.value(),
                self.captured.value()
            )));
        }
        if changes.pre_apply != self.acknowledged {
            return Err(self.fail("local changes do not follow the acknowledged head".into()));
        }
        let bytes = match changes.to_ltx(id.txid, id.txid, crate::alarm::now_ms_i64()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(self.fail(format!("encode: {e}"))),
        };
        let post_apply = changes.post_apply;
        let Some(permit) = self.permit.take() else {
            return Err(self.fail("a ticket without its budget permit".into()));
        };
        self.shared
            .slot
            .send_replace(Slot::Uploading { id, post_apply });
        self.reported = true;
        let key = segment_key(&self.segment);
        let target = self.target.clone();
        let shared = Arc::clone(&self.shared);
        self.runtime.spawn(async move {
            let sent = target.put_bounded(&key, bytes, "segment").await;
            // The PUT future has ended: its bytes leave the node's budget
            // now, whoever applies the outcome, and whenever.
            drop(permit);
            match sent {
                Ok(()) => {
                    shared.slot.send_replace(Slot::Landed {
                        id,
                        post_apply,
                        result: Ok(()),
                    });
                }
                Err(e) => shared.failed(id, post_apply, UploadFailure::from_error(&e)),
            }
        });
        Ok(id)
    }

    /// Records a failed hand-off: the flag now, the outcome in the cell.
    fn fail(&mut self, reason: String) -> ReplicaError {
        tracing::error!(prefix = %self.target.prefix, segment = ?self.segment.id, %reason, "a batched capture failed: replication stops");
        self.reported = true;
        self.shared.failed(
            self.segment.id,
            self.captured,
            UploadFailure::Local(reason.clone()),
        );
        ReplicaError::Local(reason)
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if self.reported {
            return;
        }
        if self.consumed {
            // Captured and not handed off (a panic between the two): the
            // WAL is consumed, so the chain is broken. Fail.
            self.shared.failed(
                self.segment.id,
                self.captured,
                UploadFailure::Local("a capture was interrupted".into()),
            );
        } else {
            self.shared
                .slot
                .send_replace(Slot::Abandoned(self.segment.id));
        }
    }
}
