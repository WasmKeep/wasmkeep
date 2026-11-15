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
use wasmkeep_storage::wal::{BoundaryPlan, DEFAULT_SEGMENT_BYTES, WalBoundary};

use super::{Replica, ReplicaError, UploadTarget};
use wasmkeep_cluster::alarm_index::{self, Entry};

/// One request to the bucket's alarm index, for an entry.
type IndexCall = Arc<
    dyn Fn(Entry) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Where a batched hold's segments list and retire alarm entries (ADR
/// 0034, section 9): the PUT and the DELETE of one exact entry of this
/// actor's in the bucket's index, run on the segment's upload task.
#[derive(Clone)]
pub(crate) struct AlarmIndex {
    put: IndexCall,
    delete: IndexCall,
}

impl std::fmt::Debug for AlarmIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AlarmIndex")
    }
}

impl AlarmIndex {
    /// From the two requests.
    pub(crate) fn new<P, D, PF, DF>(put: P, delete: D) -> Self
    where
        P: Fn(Entry) -> PF + Send + Sync + 'static,
        PF: Future<Output = Result<(), String>> + Send + 'static,
        D: Fn(Entry) -> DF + Send + Sync + 'static,
        DF: Future<Output = Result<(), String>> + Send + 'static,
    {
        Self {
            put: Arc::new(move |e| Box::pin(put(e))),
            delete: Arc::new(move |e| Box::pin(delete(e))),
        }
    }

    /// `address`'s entries in `cluster`'s index.
    pub(crate) fn of(
        cluster: Arc<crate::cluster::Cluster>,
        address: wasmkeep_runtime::ActorAddress,
    ) -> Self {
        let (c, a) = (Arc::clone(&cluster), address.clone());
        Self::new(
            move |entry| {
                let (c, a) = (Arc::clone(&c), a.clone());
                async move {
                    c.put_alarm_entry(&a, &entry)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            move |entry| {
                let (c, a) = (Arc::clone(&cluster), address.clone());
                async move {
                    c.delete_alarm_entry(&a, &entry)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
    }
}

/// What a segment's upload task does in the alarm index: the entry its
/// end names, listed before the segment, and the entry the acknowledged
/// head named, retired once the segment is acknowledged (ADR 0034,
/// section 9).
#[derive(Debug, Clone)]
struct SegmentAlarm {
    index: AlarmIndex,
    publish: Option<Entry>,
    retire: Option<Entry>,
    generation: u64,
}

/// A batched hold's periodic snapshot is due once its acknowledged segments
/// since the last one hold this many encoded bytes, if 64 segments did not
/// make it due first (ADR 0034, section 7).
pub(crate) const SNAPSHOT_BYTES: u64 = 64 << 20;

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

    /// Takes every byte not reserved now, until the guard is dropped: a
    /// node budget a test fills.
    pub(crate) fn hold_all(&self) -> OwnedSemaphorePermit {
        let free = u32::try_from(self.permits.available_permits()).unwrap_or(u32::MAX);
        Arc::clone(&self.permits)
            .try_acquire_many_owned(free)
            .unwrap_or_else(|_| unreachable!("a semaphore of this node is never closed"))
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
///
/// The flag is raised by every batched failure: the upload task's, a
/// failed hand-off, and the uploader's own refusals and failed applies
/// (a completion that is not this hold's, a sidecar that cannot be
/// written). Strict inline stops use the same watch with `batched: false`.
#[derive(Debug)]
pub(super) struct Shared {
    active: std::sync::atomic::AtomicBool,
    slot: watch::Sender<Slot>,
    /// Set by the upload task (or the hand-off) the moment a segment fails,
    /// before anyone applies it, for `Cut::Replica` (ADR 0034, section 8)
    /// and the waiters. Strict inline stops are distinguished by the tag.
    pub(super) failure: watch::Sender<Option<Stop>>,
    /// The encoded bytes of the segment in flight, set at its hand-off:
    /// what its acknowledgement adds toward a snapshot by bytes (ADR 0034,
    /// section 7). One segment in flight, so one value.
    in_flight_bytes: std::sync::atomic::AtomicU64,
}

/// One stop notification for waiters and the batched cut signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stop {
    pub(crate) reason: UploadFailure,
    pub(crate) batched: bool,
}

/// A cheap, synchronous gate for effects after an asynchronous upload failed.
/// Strict inline stops deliberately do not close this gate.
pub(crate) fn stopped(flag: Option<&watch::Receiver<Option<Stop>>>) -> bool {
    flag.is_some_and(|flag| flag.borrow().as_ref().is_some_and(|stop| stop.batched))
}

impl Shared {
    fn new() -> Self {
        Self {
            active: std::sync::atomic::AtomicBool::new(false),
            slot: watch::Sender::new(Slot::Idle),
            failure: watch::Sender::new(None),
            in_flight_bytes: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Records `failure` on the flag (the first one stays) and, for
    /// segment `id` ending at `post_apply`, in the cell.
    fn failed(&self, id: SegmentId, post_apply: DbChecksum, failure: UploadFailure) {
        self.failure.send_if_modified(|f| {
            if f.is_some() {
                return false;
            }
            *f = Some(Stop {
                reason: failure.clone(),
                batched: true,
            });
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
    sequence: WalBoundary,
    /// Whole events (boundaries) it carries.
    events: usize,
    /// One event alone over the byte cap: uploaded strict-style, its
    /// actor to wait for it at the event's end.
    oversize: bool,
}

/// Acknowledged prefixes for every capture session seen by this hold.
#[derive(Debug, Clone, Default)]
struct Acked {
    prefixes: std::collections::BTreeMap<u64, u64>,
    current: Option<u64>,
    closed: std::collections::BTreeSet<u64>,
}
impl Acked {
    fn covers(&self, boundary: WalBoundary) -> bool {
        self.prefixes
            .get(&boundary.session)
            .is_some_and(|s| *s >= boundary.sequence)
    }
}

/// A sidecar write started by [`Replica::apply`], for the head it names.
type SidecarWrite = (SegmentId, tokio::task::JoinHandle<Result<(), ReplicaError>>);

/// A replica's batched state. Idle, and never used, in a strict class.
#[derive(Debug)]
pub(super) struct Uploads {
    pub(super) shared: Arc<Shared>,
    flight: Option<Flight>,
    /// What the acknowledged head covers, published once its sidecar is
    /// written.
    acked: watch::Sender<Acked>,
    /// An acknowledged boundary whose sidecar is not written yet (an apply
    /// cancelled while writing it): the next apply finishes the write,
    /// then publishes it.
    sidecar_due: Option<WalBoundary>,
    /// The sidecar write under way, kept across a cancelled apply so that
    /// two writes of the sidecar never run at once.
    sidecar_write: Option<SidecarWrite>,
    /// Last captured boundary, retained while a cut apply finishes its sidecar.
    captured: Option<WalBoundary>,
    /// Boundaries marked and not captured yet, oldest first, with when they
    /// were marked: what the event-start rule (ADR 0034, section 3) and the
    /// drop counter (section 10) read. Pruned when a segment is prepared.
    marked: std::collections::VecDeque<(WalBoundary, std::time::Instant)>,
    /// The alarm entry each marked boundary's state names, where known, by
    /// (session, sequence): what a segment ending there lists (section 9).
    names: std::collections::BTreeMap<(u64, u64), Option<Entry>>,
    /// Where segments list and retire entries; `None`: not at all (a hold
    /// without an index, or the uploader's own tests).
    index: Option<AlarmIndex>,
    /// The entry the acknowledged head names; `None`: not known yet.
    acked_entry: Option<Option<Entry>>,
    /// The entry the segment in flight's end names, if known.
    flight_named: Option<Option<Entry>>,
    /// The encoded bytes of the segments acknowledged since the last
    /// snapshot, and the bound past which one is due (ADR 0034, section 7).
    pub(super) bytes_since_snapshot: u64,
    snapshot_bytes: u64,
    /// A failed snapshot's wait a restore set aside (fewer than 64
    /// segments restored), for a batched hold due by bytes.
    pub(super) set_aside_wait: Option<super::SnapshotWait>,
}

impl Default for Uploads {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared::new()),
            flight: None,
            acked: watch::Sender::new(Acked::default()),
            sidecar_due: None,
            sidecar_write: None,
            captured: None,
            marked: std::collections::VecDeque::new(),
            names: std::collections::BTreeMap::new(),
            index: None,
            acked_entry: None,
            flight_named: None,
            bytes_since_snapshot: 0,
            snapshot_bytes: SNAPSHOT_BYTES,
            set_aside_wait: None,
        }
    }
}

impl Uploads {
    /// The bytes bound of a snapshot due by bytes.
    pub(super) fn snapshot_bytes(&self) -> u64 {
        self.snapshot_bytes
    }

    /// Publish the reason to all waiters, identifying batched holds for Cut::Replica.
    pub(super) fn stopped(&self, reason: &str) {
        self.shared.failure.send_if_modified(|r| {
            if r.as_ref().is_some_and(|s| s.batched) {
                return false;
            }
            *r = Some(Stop {
                reason: UploadFailure::Local(reason.to_owned()),
                batched: self
                    .shared
                    .active
                    .load(std::sync::atomic::Ordering::Relaxed),
            });
            true
        });
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
            && self.batched.sidecar_due.is_none()
            && self.batched.sidecar_write.is_none()
            && self.failed.is_none()
            && self.batched.shared.failure.borrow().is_none()
            && matches!(*self.batched.shared.slot.borrow(), Slot::Idle)
    }

    /// Whether the segment in flight is one event over the byte cap, which
    /// its actor waits for at the event's end (ADR 0034, section 2).
    pub(crate) fn in_flight_oversize(&self) -> bool {
        self.batched.flight.is_some_and(|f| f.oversize)
    }

    /// The last boundary the captured position covers: the segment in
    /// flight's, or the acknowledged head's. A flush with nothing left
    /// uncaptured waits for this one ([`Self::wait_acknowledged`]).
    pub(crate) fn captured_sequence(&self) -> Option<WalBoundary> {
        match *self.batched.shared.slot.borrow() {
            Slot::Uploading { .. } | Slot::Landed { .. } => self.batched.flight.map(|f| f.sequence),
            _ => self.batched.captured,
        }
    }

    /// Optional barrier notification; storage owns capture identities.
    pub(crate) async fn capture_restarted(&mut self) -> Result<(), ReplicaError> {
        self.finish_sidecar().await?;
        self.settle_abandoned();
        if !self.uploader_idle() {
            return Err(self
                .refuse_batched("a capture restarted with a segment in flight or failed".into()));
        }
        self.batched.acked.send_modify(|a| {
            if let Some(session) = a.current.take() {
                a.closed.insert(session);
            }
        });
        self.batched.captured = None;
        self.batched.marked.clear();
        self.batched.names.clear();
        Ok(())
    }

    /// Takes a segment whose ticket was dropped before its capture out of
    /// flight, so a guard does not refuse it. Nothing for a strict class.
    pub(super) fn settle_abandoned(&mut self) {
        let abandoned = matches!(
            *self.batched.shared.slot.borrow(),
            Slot::Abandoned(id) if self.batched.flight.is_some_and(|f| f.id == id)
        );
        if abandoned {
            self.batched.flight = None;
            self.in_flight = None;
            self.batched.shared.slot.send_replace(Slot::Idle);
        }
    }

    /// The events (boundaries) the segment in flight carries.
    pub(crate) fn in_flight_events(&self) -> usize {
        self.batched.flight.map_or(0, |f| f.events)
    }

    /// Records a boundary the database marked (`ActorDb::mark_boundary`),
    /// at `at`. The same boundary again (nothing committed since) is not
    /// counted twice.
    pub(crate) fn note_boundary(&mut self, boundary: WalBoundary, at: std::time::Instant) {
        let known =
            self.batched.marked.back().is_some_and(|(b, _)| {
                b.session == boundary.session && b.sequence >= boundary.sequence
            });
        if !known && !self.acknowledged_covers(boundary) {
            self.batched.marked.push_back((boundary, at));
        }
    }

    /// [`Self::note_boundary`], with the alarm entry the state at the
    /// boundary names (`ALARM_INDEXED_KEY` read with the mark): what a
    /// segment ending there lists (ADR 0034, section 9).
    pub(crate) fn note_named_boundary(
        &mut self,
        boundary: WalBoundary,
        at: std::time::Instant,
        named: Option<Entry>,
    ) {
        self.note_boundary(boundary, at);
        if !self.acknowledged_covers(boundary) {
            self.batched
                .names
                .insert((boundary.session, boundary.sequence), named);
        }
    }

    /// Where this hold's segments list and retire alarm entries, and the
    /// entry its acknowledged head names: set where the local database is
    /// the acknowledged head (batched capture enabled at a barrier).
    pub(crate) fn set_alarm_index(&mut self, index: AlarmIndex, acknowledged: Option<Entry>) {
        self.batched.index = Some(index);
        self.batched.acked_entry = Some(acknowledged);
    }

    /// The alarm entry the acknowledged head names (`Some(None)`: none),
    /// if known: what every R2 decision of a batched hold uses (ADR 0034,
    /// section 9).
    pub(crate) fn acknowledged_entry(&self) -> Option<Option<Entry>> {
        self.batched.acked_entry.clone()
    }

    /// Whether a batched hold's periodic snapshot is due: by segments, as
    /// [`Replica::snapshot_due`] says, or by the acknowledged segments'
    /// bytes ([`SNAPSHOT_BYTES`]), under the same backoff after a failed
    /// one (ADR 0034, section 7). Asked only at a mailbox gap.
    pub(crate) fn batched_snapshot_due(&self) -> bool {
        let reached = self.since_snapshot >= super::SNAPSHOT_EVERY
            || self.batched.bytes_since_snapshot >= self.batched.snapshot_bytes;
        // Both gates of a failed snapshot's backoff, whichever bound came.
        reached
            && self.since_snapshot >= self.snapshot_retry_at
            && (self.snapshot_failures == 0
                || tokio::time::Instant::now() >= self.snapshot_retry_after)
    }

    /// The bytes bound of [`Self::batched_snapshot_due`] (tests shorten it).
    pub(crate) fn set_snapshot_bytes(&mut self, bytes: u64) {
        self.batched.snapshot_bytes = bytes.max(1);
    }

    /// Forgets every boundary marked and not captured: the database has
    /// nothing left to capture (its own count is the one that holds).
    pub(crate) fn forget_marked(&mut self) {
        self.batched.marked.clear();
        self.batched.names.clear();
    }

    /// When the oldest boundary not captured yet was marked: the age the
    /// event-start rule bounds (ADR 0034, section 3). Boundaries the
    /// segment in flight carries are captured; a ticket dropped before its
    /// capture captured nothing.
    pub(crate) fn oldest_unsent(&self) -> Option<std::time::Instant> {
        let captured = match *self.batched.shared.slot.borrow() {
            Slot::Uploading { .. } | Slot::Landed { .. } => self.batched.flight.map(|f| f.sequence),
            _ => None,
        };
        self.batched
            .marked
            .iter()
            .find(|(b, _)| {
                captured.is_none_or(|c| b.session != c.session || b.sequence > c.sequence)
            })
            .map(|(_, at)| *at)
    }

    /// What a flush waits to see acknowledged when the database marked
    /// nothing new (ADR 0034, section 4): the boundary the database
    /// returned, else the segment in flight's, else nothing.
    pub(crate) fn flush_target(&self, marked: Option<WalBoundary>) -> Option<WalBoundary> {
        marked.or_else(|| self.batched.flight.map(|f| f.sequence))
    }

    /// The last boundary marked under this hold: the one a flush waits to
    /// see acknowledged. `None`: nothing was ever committed in this
    /// capture session.
    pub(crate) fn last_marked(&self) -> Option<WalBoundary> {
        self.batched
            .marked
            .back()
            .map(|(b, _)| *b)
            .or_else(|| self.captured_sequence())
    }

    /// The events committed and not acknowledged: every boundary marked
    /// and not acknowledged yet, in flight or not. What a hold that ends
    /// without a flush drops (ADR 0034, section 10).
    pub(crate) fn unacknowledged_events(&self) -> u64 {
        u64::try_from(self.batched.marked.len()).unwrap_or(u64::MAX)
    }

    /// Whether this hold is at its `Flushed` barrier as far as the replica
    /// knows: nothing marked and uncaptured, nothing in flight, nothing to
    /// apply, nothing failed (ADR 0034, section 6). The database's own
    /// uncaptured commits are the caller's to check.
    pub(crate) fn nothing_pending(&self) -> bool {
        self.batched.marked.is_empty() && self.uploader_idle()
    }

    /// One upload's budget at the byte cap: the longest a segment's upload
    /// task runs (ADR 0034, section 2), 21 s at the defaults. The deadline
    /// of a hibernation's flush and of a mode switch's (sections 1 and 6).
    pub(crate) fn upload_budget(&self) -> std::time::Duration {
        let megabytes = u32::try_from(DEFAULT_SEGMENT_BYTES.div_ceil(1 << 20)).unwrap_or(1);
        self.upload_idle
            .saturating_mul(super::UPLOAD_BUDGET_IDLES)
            .saturating_add(std::time::Duration::from_secs(1).saturating_mul(megabytes))
    }

    /// The failure flag (ADR 0034, section 8): set the moment a batched
    /// upload or capture fails, before the outcome is applied. Only this
    /// module marks batched stops; strict inline stops carry `batched: false`.
    pub(crate) fn failure_flag(&self) -> watch::Receiver<Option<Stop>> {
        self.batched
            .shared
            .active
            .store(true, std::sync::atomic::Ordering::Relaxed);
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
    pub(crate) async fn prepare(
        &mut self,
        plan: BoundaryPlan,
        permit: BudgetPermit,
    ) -> Result<Ticket, ReplicaError> {
        self.batched
            .shared
            .active
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.finish_sidecar().await?;
        self.settle_abandoned();
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        if let Some(failure) = &*self.batched.shared.failure.borrow() {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {:?}",
                failure.reason
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
        if self.batched.acked.borrow().covers(plan.boundary) {
            return Err(self.refuse_batched("boundary is already acknowledged".into()));
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
        // Nothing is in flight, so the captured position is the
        // acknowledged head: the ticket's two tripwires compare with the
        // same checksum, kept apart as the two rules of section 5.
        let (captured_id, captured) = self.captured();
        debug_assert_eq!(captured_id, self.head, "nothing in flight");
        self.batched.acked.send_modify(|a| {
            if a.current != Some(plan.boundary.session)
                && let Some(session) = a.current.replace(plan.boundary.session)
            {
                a.closed.insert(session);
            }
        });
        // The entry the segment's end names goes up before it unless the
        // acknowledged head names it already (the captured position's,
        // with nothing in flight); the acknowledged head's, if another, is
        // retired once the segment is acknowledged. Entries named only
        // inside the segment are never listed (ADR 0034, section 9).
        let named = self
            .batched
            .names
            .get(&(plan.boundary.session, plan.boundary.sequence))
            .cloned();
        let alarm = match (&self.batched.index, &named) {
            (None, _) => None,
            (Some(_), None) => {
                return Err(
                    self.refuse_batched("a segment whose end names no known alarm entry".into())
                );
            }
            (Some(index), Some(named)) => {
                let acked = self.batched.acked_entry.clone();
                let publish = named
                    .clone()
                    .filter(|n| acked.as_ref().is_none_or(|a| a.as_ref() != Some(n)));
                let retire = match acked {
                    Some(Some(a)) if named.as_ref() != Some(&a) => Some(a),
                    _ => None,
                };
                Some(SegmentAlarm {
                    index: index.clone(),
                    publish,
                    retire,
                    generation: self.generation,
                })
            }
        };
        // Kept until acknowledged: a ticket dropped before its capture
        // leaves them uncaptured. Only another capture session's go.
        let session = plan.boundary.session;
        self.batched.marked.retain(|(b, _)| b.session == session);
        self.batched.names.retain(|(s, _), _| *s == session);
        self.batched.flight_named = named;
        self.in_flight = Some(segment.id);
        self.batched.flight = Some(Flight {
            id: segment.id,
            sequence: plan.boundary,
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
            alarm,
        })
    }

    /// Resolves once the segment in flight has an outcome to apply (or at
    /// once when none is in flight). Borrows nothing: the caller awaits it
    /// without holding the replica, then calls [`Self::apply`].
    pub(crate) fn landed(&self) -> impl Future<Output = ()> + Send + use<> {
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
                return Ok(Applied::Abandoned);
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
                self.batched.marked.retain(|(b, _)| {
                    b.session != flight.sequence.session || b.sequence > flight.sequence.sequence
                });
                let end = (flight.sequence.session, flight.sequence.sequence);
                self.batched
                    .names
                    .retain(|(s, q), _| *s != end.0 || *q > end.1);
                self.batched.bytes_since_snapshot =
                    self.batched.bytes_since_snapshot.saturating_add(
                        self.batched
                            .shared
                            .in_flight_bytes
                            .swap(0, std::sync::atomic::Ordering::Relaxed),
                    );
                if let Some(named) = self.batched.flight_named.take() {
                    self.batched.acked_entry = Some(named);
                }
                self.head = id;
                self.checksum = post_apply;
                self.since_snapshot += 1;
                self.batched.captured = Some(flight.sequence);
                self.batched.sidecar_due = Some(flight.sequence);
                Applied::Acknowledged(id)
            }
        };
        self.finish_sidecar().await?;
        Ok(applied)
    }

    pub(super) async fn finish_sidecar(&mut self) -> Result<(), ReplicaError> {
        while let Some(sequence) = self.batched.sidecar_due {
            if self.batched.sidecar_write.is_none() {
                let (path, head, sum) = (self.db_path.clone(), self.head, self.checksum);
                let write =
                    tokio::task::spawn_blocking(move || super::write_sidecar(&path, head, sum));
                self.batched.sidecar_write = Some((head, write));
            }
            let Some((written, write)) = self.batched.sidecar_write.as_mut() else {
                break;
            };
            let written = *written;
            // Borrowed: cancelling this await leaves the write here, for
            // the next apply to wait for before it writes again.
            let done = write.await;
            self.batched.sidecar_write = None;
            if let Err(e) = done
                .map_err(|e| ReplicaError::Local(format!("sidecar: {e}")))
                .and_then(|r| r)
            {
                self.stop_batched(UploadFailure::Local(e.to_string()));
                return Err(e);
            }
            if written == self.head {
                self.batched.sidecar_due = None;
                self.batched.acked.send_if_modified(|acked| {
                    let previous = acked.prefixes.insert(sequence.session, sequence.sequence);
                    previous != Some(sequence.sequence)
                });
            }
        }
        Ok(())
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
        self.batched.shared.failure.send_if_modified(|f| {
            if f.is_some() {
                return false;
            }
            *f = Some(Stop {
                reason: failure.clone(),
                batched: true,
            });
            true
        });
        self.fail(failure.to_string());
    }

    /// Whether the acknowledged head covers the boundary `sequence`
    /// (`WalBoundary::sequence`): every commit up to it is in the bucket,
    /// uploaded while this hold was valid, and the sidecar says so.
    pub(crate) fn acknowledged_covers(&self, boundary: WalBoundary) -> bool {
        self.batched.acked.borrow().covers(boundary)
    }

    pub(crate) fn wait_acknowledged(
        &self,
        boundary: WalBoundary,
    ) -> impl Future<Output = Result<(), ReplicaError>> + Send + 'static {
        self.batched
            .shared
            .active
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut acked = self.batched.acked.subscribe();
        let mut stopped = self.batched.shared.failure.subscribe();
        async move {
            loop {
                if acked.borrow_and_update().covers(boundary) {
                    return Ok(());
                }
                if acked.borrow().closed.contains(&boundary.session) {
                    return Err(ReplicaError::Local("replication stopped: the capture restarted before this boundary was acknowledged".into()));
                }
                if let Some(stop) = &*stopped.borrow_and_update() {
                    return Err(ReplicaError::Local(format!(
                        "replication stopped: {}",
                        stop.reason
                    )));
                }
                let ended = tokio::select! {
                    r = acked.changed() => r.is_err(),
                    r = stopped.changed() => r.is_err(),
                };
                if ended && !acked.borrow().covers(boundary) {
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
    pub(super) acknowledged: DbChecksum,
    target: UploadTarget,
    shared: Arc<Shared>,
    runtime: tokio::runtime::Handle,
    /// The WAL was consumed for this segment.
    consumed: bool,
    /// An outcome (the upload task, or a failure) is in the cell.
    reported: bool,
    /// The alarm entries its task lists and retires.
    alarm: Option<SegmentAlarm>,
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
    pub(crate) fn hand_off(self, db: &mut ActorDb) -> Result<SegmentId, ReplicaError> {
        {
            let plan = self.plan;
            self.hand_off_with(|| db.take_bounded_changes(plan))
        }
    }

    pub(super) fn hand_off_with(
        mut self,
        capture: impl FnOnce() -> Result<
            wasmkeep_storage::wal::BoundaryChanges,
            wasmkeep_storage::wal::CaptureError,
        >,
    ) -> Result<SegmentId, ReplicaError> {
        let id = self.segment.id;
        if let Some(stop) = &*self.shared.failure.borrow() {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {}",
                stop.reason
            )));
        }
        if !self.target.lease.is_valid() {
            return Err(ReplicaError::Late);
        }
        // Before the call: a panic inside it may come after the WAL was
        // consumed, and must fail the replica, not abandon the segment.
        self.consumed = true;
        let got = match capture() {
            Ok(got) => got,
            Err(e) => return Err(self.fail(format!("capture: {e}"))),
        };
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
        self.shared.in_flight_bytes.store(
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        let post_apply = changes.post_apply;
        let Some(permit) = self.permit.take() else {
            return Err(self.fail("a ticket without its budget permit".into()));
        };
        let key = segment_key(&self.segment);
        let target = self.target.clone();
        // Moved into the task: dropped without an outcome (the task
        // panicked, or the runtime dropped it unrun), it fails the replica.
        let mut outcome = Outcome {
            shared: Arc::clone(&self.shared),
            id,
            post_apply,
            recorded: false,
        };
        self.shared
            .slot
            .send_replace(Slot::Uploading { id, post_apply });
        self.reported = true;
        let alarm = self.alarm.take();
        self.runtime.spawn(async move {
            // One budget for the entry and the segment (ADR 0034, section 9).
            let until = tokio::time::Instant::now() + target.budget_for(bytes.len());
            if let Some(entry) = alarm.as_ref().and_then(|a| a.publish.clone().map(|e| (a, e))) {
                let (a, entry) = entry;
                let listed = tokio::time::timeout_at(until, (a.index.put)(entry)).await;
                let failed = match listed {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(format!("alarm index: {e}")),
                    Err(_) => Some("alarm index: the entry's upload ran out of time".to_owned()),
                };
                if let Some(e) = failed {
                    // Never a segment before its entry: the replica fails.
                    drop(permit);
                    outcome.record(Err(ReplicaError::Bucket(e)));
                    return;
                }
            }
            let sent = target
                .put_bounded_by(&key, bytes, "segment", Some(until))
                .await;
            // The PUT future has ended: its bytes leave the node's budget
            // now, whoever applies the outcome, and whenever.
            drop(permit);
            let acknowledged = sent.is_ok();
            outcome.record(sent);
            // Only an acknowledged segment retires the entry the head before
            // it named, and never a newer owner's (R2, R3). Off the actor's
            // path; a failed DELETE leaves an extra entry, removed as
            // unnamed when it comes due.
            if acknowledged
                && let Some(a) = alarm
                && let Some(stale) = a.retire
                && alarm_index::may_delete(&stale, a.generation)
                && let Err(e) = (a.index.delete)(stale).await
            {
                tracing::debug!(prefix = %target.prefix, error = %e, "retiring an alarm entry failed");
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

/// Where an upload task records its outcome, failing the replica if the
/// task ends without recording one.
struct Outcome {
    shared: Arc<Shared>,
    id: SegmentId,
    post_apply: DbChecksum,
    recorded: bool,
}

impl Outcome {
    fn record(&mut self, sent: Result<(), ReplicaError>) {
        self.recorded = true;
        match sent {
            Ok(()) => {
                self.shared.slot.send_replace(Slot::Landed {
                    id: self.id,
                    post_apply: self.post_apply,
                    result: Ok(()),
                });
            }
            Err(e) => self
                .shared
                .failed(self.id, self.post_apply, UploadFailure::from_error(&e)),
        }
    }
}

impl Drop for Outcome {
    fn drop(&mut self) {
        if !self.recorded {
            self.shared.failed(
                self.id,
                self.post_apply,
                UploadFailure::Local("the upload task ended without an outcome".into()),
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropped_unrun_upload_records_failure() {
        let shared = Arc::new(Shared::new());
        let id = SegmentId {
            generation: 1,
            txid: 2,
        };
        let outcome = Outcome {
            shared: Arc::clone(&shared),
            id,
            post_apply: DbChecksum::default(),
            recorded: false,
        };
        shared.slot.send_replace(Slot::Uploading {
            id,
            post_apply: DbChecksum::default(),
        });
        // The runtime may discard a spawned future without polling it.
        let task = async move {
            let _outcome = outcome;
            std::future::pending::<()>().await;
        };
        drop(task);
        assert!(shared.failure.borrow().as_ref().unwrap().batched);
        assert!(matches!(
            *shared.slot.borrow(),
            Slot::Landed { result: Err(_), .. }
        ));
    }
}
