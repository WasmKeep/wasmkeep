//! Taking snapshots (the first, periodic and idle ones) and waiting out the
//! failed ones.

use super::*;

/// Upload a snapshot after this many segments, so restores stay short.
pub(super) const SNAPSHOT_EVERY: u64 = 64;

/// An actor that hibernates gets an idle snapshot at its head after this
/// many segments since its last snapshot (ADR 0042, section 2), counted
/// across holds as [`SNAPSHOT_EVERY`]'s are. A proposal, to measure: it
/// bounds the extra PUTs of an actor that writes once per wake, and an
/// actor at rest then holds at most 2 + 7 objects once compacted.
pub(crate) const IDLE_SNAPSHOT_EVERY: u64 = 8;

/// A database at most this large gets an idle snapshot after a single
/// segment: one cheap PUT (ADR 0042, section 2).
pub(crate) const IDLE_SNAPSHOT_SMALL_BYTES: u64 = 256 << 10;

/// Idle snapshots a node copies and uploads at once: a hibernation that
/// finds them all taken takes none (counted `busy`), so a burst of
/// evictions is not held behind a queue of uploads.
pub(crate) const IDLE_SNAPSHOTS_AT_ONCE: usize = 8;

/// No idle snapshot of a database larger than this, whatever was written:
/// a hibernation does not copy and upload a large database (it holds an
/// eviction for its upload's one attempt). Such an actor keeps its
/// periodic snapshots and their collection.
pub(crate) const IDLE_SNAPSHOT_MAX_BYTES: u64 = 64 << 20;

/// Why a hibernation took no idle snapshot (ADR 0042, section 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdleSkip {
    /// This hold did not write the head: a snapshot keyed with an earlier
    /// generation must come from that generation only.
    NotWriter,
    /// A segment is in flight: not at a `Flushed` barrier.
    InFlight,
    /// A snapshot is at the head already.
    AtHead,
    /// Too few segments since the last snapshot for the database's size.
    BelowThreshold,
    /// The database is above [`IDLE_SNAPSHOT_MAX_BYTES`].
    TooLarge,
    /// A periodic snapshot failed and its wait before the next try is not
    /// over: the bucket refuses snapshots, so none is tried here either.
    BackingOff,
    /// A periodic or idle snapshot of this actor is uploading.
    Uploading,
    /// Replication stopped, or the sidecar could not be finished.
    Stopped(String),
    /// Refused as `ship_snapshot` refuses a snapshot that does not hold
    /// the head's database: a bug, and replication stops.
    Refused(String),
}

/// A periodic snapshot is an optimisation (the segments are the history),
/// taken after an event while the actor is held (with strict writes; a
/// batched hold's uploads off the mailbox, ADR 0034, section 7, and holds
/// only a barrier, a reset or another snapshot): it must cost the events
/// behind it little even when the bucket leaves every snapshot upload
/// hanging. A caller queued behind the actor waits 30 s at most where its
/// wait is bounded at all (`wall_per_event` for `actors.call`,
/// `FORWARD_TIMEOUT` for a request another node forwards; a request the
/// owner received itself has no limit of its own). So a periodic snapshot
/// gets one attempt (one call to the client, see [`UPLOAD_IDLE`]), which
/// this code does not send again, given up after [`UPLOAD_IDLE`] plus a
/// second per started megabyte and at most this many [`UPLOAD_IDLE`]s: 6 s
/// under a megabyte, 10 s from 5 MB on, a third of that wait and under
/// the bucket client's own 30 s per request. Whatever the client sends
/// again after an error answer, it sends within that bound.
///
/// After a failure the next try waits for one more segment, then
/// [`SNAPSHOT_RETRY_GROWTH`] times as many each time up to
/// [`SNAPSHOT_EVERY`] (1, 8, 64, 64...), and in any case for
/// [`SNAPSHOT_RETRY_MIN`] from the start of the try that failed.
///
/// Worst case, the bucket hanging every snapshot upload: a try holds the
/// actor for 10 s (6 s under a megabyte); the tries come at the segment
/// the snapshot was due at, 1 later, 9 later, 73 later and then every 64,
/// so at most three in any 64 segments, 30 s of hold (18 s), then one per
/// 64 segments, 10 s (6 s); never two starting within 30 s. Before
/// uploads had bounds, one hung snapshot per 64 segments held the actor
/// for the client's 30 s.
///
/// That holds on one node for as long as its process lives: the wait is
/// the node's ([`SnapshotWaits`]), so an actor dropped from memory when
/// idle and restored at its next event keeps it. A restore counts every
/// segment since the last snapshot, so without a wait on record (another
/// node took the actor over, this one restarted, or the record was full)
/// a chain of [`SNAPSHOT_EVERY`] segments or more makes a snapshot due
/// at the first event, as it always did: one more try, and the waits
/// start from there.
///
/// The price: a database the bucket cannot take within 10 s (slower than
/// a megabyte a second up to 5 MB, than its size in 10 s above) gets no
/// periodic snapshot. Each try is one counted, bounded attempt, and a
/// restore replays every segment since the last snapshot there is.
const PERIODIC_SNAPSHOT_IDLES: u32 = 2;
/// See [`PERIODIC_SNAPSHOT_IDLES`]: each failure in a row multiplies the
/// segments the next try waits for by this.
const SNAPSHOT_RETRY_GROWTH: u64 = 8;
/// See [`PERIODIC_SNAPSHOT_IDLES`]: the least time between a periodic
/// snapshot that failed and the next try, however fast segments follow:
/// as long as the longest bounded wait of a queued caller.
pub(crate) const SNAPSHOT_RETRY_MIN: std::time::Duration = std::time::Duration::from_secs(30);

/// Each actor's wait after a periodic snapshot that failed, kept by the
/// node ([`Cluster::snapshot_waits`]) so that it outlives the actor's
/// [`Replica`]: an idle actor is dropped from memory and restored at its
/// next event with every segment since the last snapshot counted again,
/// and without this a snapshot would be due at that first event, wait or
/// no wait. An entry is written when a try starts, removed when a
/// snapshot is there, when the actor is deleted and when this node sees
/// the actor go to another; a restore that finds a later snapshot drops
/// it. Only this process knows it: another node that takes the actor
/// over, or this one after a restart, starts with none.
///
/// At most [`SNAPSHOT_WAITS_MAX`] entries: one more pushes out the one
/// whose time comes first, whose actor may then try again at its next
/// restore, as it did before the wait was kept.
#[derive(Debug, Default)]
pub(crate) struct SnapshotWaits(
    std::sync::Mutex<std::collections::HashMap<ActorAddress, SnapshotWait>>,
);

/// See [`SnapshotWaits`].
pub(super) const SNAPSHOT_WAITS_MAX: usize = 10_000;

/// One actor's wait: no periodic snapshot is tried before `after`, nor
/// before the actor's head has reached transaction `txid` (each segment
/// is one transaction, whoever uploads it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SnapshotWait {
    /// Periodic snapshots that failed in a row.
    pub(super) failures: u32,
    pub(super) txid: u64,
    pub(super) after: tokio::time::Instant,
}

impl SnapshotWaits {
    pub(super) fn entries(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<ActorAddress, SnapshotWait>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn get(&self, address: &ActorAddress) -> Option<SnapshotWait> {
        self.entries().get(address).copied()
    }

    pub(super) fn set(&self, address: &ActorAddress, wait: SnapshotWait) {
        let mut entries = self.entries();
        if entries.len() >= SNAPSHOT_WAITS_MAX && !entries.contains_key(address) {
            let first = entries
                .iter()
                .min_by_key(|(_, w)| w.after)
                .map(|(a, _)| a.clone());
            if let Some(first) = first {
                entries.remove(&first);
            }
        }
        entries.insert(address.clone(), wait);
    }

    /// The actor has a snapshot, was deleted, or is another node's now.
    pub(crate) fn forget(&self, address: &ActorAddress) {
        self.entries().remove(address);
    }
}

/// How long a periodic snapshot's one attempt may take, for an idle bound
/// of `idle` and a snapshot of `bytes`: see [`PERIODIC_SNAPSHOT_IDLES`].
pub(super) fn periodic_snapshot_bound(
    idle: std::time::Duration,
    bytes: usize,
) -> std::time::Duration {
    let megabytes = started_mib(bytes);
    idle.saturating_add(std::time::Duration::from_secs(1).saturating_mul(megabytes))
        .min(idle.saturating_mul(PERIODIC_SNAPSHOT_IDLES))
}

/// A node's periodic snapshots of batched holds uploading off the mailbox,
/// by actor ([`Cluster::periodic_flights`]). An upload outlives the hold
/// that started it when that hold ends without its barrier (given up,
/// failed, a stop past its deadline): the actor's next hold on this node
/// finds it here, and its resets and barriers wait for it as for its own
/// (ADR 0034, section 7). A hold that ends without its barrier cancels it:
/// an upload whose request has not started then never sends it; one whose
/// request is under way cannot be recalled, and is waited for until its
/// one attempt ends.
#[derive(Debug, Default)]
pub(crate) struct PeriodicFlights(std::sync::Mutex<PeriodicFlightsInner>);

#[derive(Debug, Default)]
struct PeriodicFlightsInner {
    next: u64,
    flights: std::collections::HashMap<ActorAddress, PeriodicFlight>,
}

#[derive(Debug)]
struct PeriodicFlight {
    id: u64,
    settled: watch::Receiver<Option<PeriodicOutcome>>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl PeriodicFlights {
    fn inner(&self) -> std::sync::MutexGuard<'_, PeriodicFlightsInner> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records `address`'s upload; `None` if one is recorded already.
    fn start(
        &self,
        address: &ActorAddress,
        settled: watch::Receiver<Option<PeriodicOutcome>>,
    ) -> Option<(u64, Arc<std::sync::atomic::AtomicBool>)> {
        let mut inner = self.inner();
        if inner.flights.contains_key(address) {
            return None;
        }
        inner.next = inner.next.wrapping_add(1);
        let (id, cancelled) = (inner.next, Arc::default());
        inner.flights.insert(
            address.clone(),
            PeriodicFlight {
                id,
                settled,
                cancelled: Arc::clone(&cancelled),
            },
        );
        Some((id, cancelled))
    }

    /// The upload `id` of `address` ended.
    fn end(&self, address: &ActorAddress, id: u64) {
        let mut inner = self.inner();
        if inner.flights.get(address).is_some_and(|f| f.id == id) {
            inner.flights.remove(address);
        }
    }

    /// Whether `address` has an upload recorded.
    pub(crate) fn uploading(&self, address: &ActorAddress) -> bool {
        self.inner().flights.contains_key(address)
    }

    /// A wait for `address`'s upload to end; `None` if none is recorded.
    pub(crate) fn settled(
        &self,
        address: &ActorAddress,
    ) -> Option<impl std::future::Future<Output = ()> + Send + use<>> {
        let mut settled = self.inner().flights.get(address)?.settled.clone();
        Some(async move {
            let _ = settled.wait_for(Option::is_some).await;
        })
    }

    /// `address`'s hold ended without its barrier: its upload sends no
    /// request it has not started. It stays recorded until it ends.
    pub(crate) fn cancel(&self, address: &ActorAddress) {
        if let Some(f) = self.inner().flights.get(address) {
            f.cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// Ends an upload's record in [`PeriodicFlights`] however its task ends.
struct FlightRecord {
    flights: Arc<PeriodicFlights>,
    address: ActorAddress,
    id: u64,
}

impl Drop for FlightRecord {
    fn drop(&mut self) {
        self.flights.end(&self.address, self.id);
    }
}

/// How a batched hold's periodic snapshot's upload ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeriodicOutcome {
    /// Created by this upload and acknowledged: collection may follow.
    Created,
    /// The same position already held it.
    Existed,
    /// Not acknowledged (given up, refused, landed after the lease ran
    /// out): counted in `wasmkeep_snapshot_upload_failures_total`.
    Failed,
}

/// The replica's side of a periodic snapshot uploading off the mailbox.
#[derive(Debug)]
pub(crate) struct PeriodicUpload {
    /// Segments and bytes counted toward a snapshot when it was taken: what
    /// its landing takes off the counts.
    pub(super) segments: u64,
    pub(super) bytes: u64,
    pub(super) settled: watch::Receiver<Option<PeriodicOutcome>>,
}

/// A periodic or idle snapshot's upload, run off the actor's mailbox
/// ([`Replica::start_periodic_snapshot`], [`Replica::start_idle_snapshot`]):
/// it holds nothing of the replica.
pub(crate) struct PeriodicSnapshot {
    /// [`SnapshotKind::Periodic`] or [`SnapshotKind::Idle`].
    kind: SnapshotKind,
    target: UploadTarget,
    position: SegmentId,
    all: Changes,
    collector: Collector,
    outcome: watch::Sender<Option<PeriodicOutcome>>,
    record: FlightRecord,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl PeriodicSnapshot {
    /// The acknowledged head it is labelled at: the snapshot a collection
    /// after it names, whatever the head is once it lands.
    pub(crate) fn position(&self) -> SegmentId {
        self.position
    }

    /// What a collection after it runs with.
    pub(crate) fn collector(&self) -> Collector {
        self.collector.clone()
    }

    /// Encodes and uploads it under a periodic snapshot's bound (one
    /// attempt, [`periodic_snapshot_bound`]), and publishes the outcome
    /// to the replica. The object's conditional create is the only
    /// publication: a restore lists it once it is whole, and until then
    /// finds the previous snapshot and every segment after it.
    pub(crate) async fn run(self) -> PeriodicOutcome {
        let Self {
            kind,
            target,
            position,
            all,
            outcome,
            record,
            cancelled,
            ..
        } = self;
        let what = if kind == SnapshotKind::Idle {
            "an idle snapshot"
        } else {
            "a periodic snapshot"
        };
        let encoded = tokio::task::spawn_blocking(move || {
            all.to_ltx(1, position.txid.max(1), crate::alarm::now_ms_i64())
        })
        .await;
        let result = match encoded {
            // Its hold ended without its barrier before the request: none.
            Ok(Ok(_)) if cancelled.load(std::sync::atomic::Ordering::SeqCst) => {
                PeriodicOutcome::Failed
            }
            Ok(Ok(bytes)) => {
                let bound = periodic_snapshot_bound(target.upload_idle, bytes.len());
                match target.put_once(&snapshot_key(position), bytes, bound).await {
                    Ok(()) => PeriodicOutcome::Created,
                    Err(ReplicaError::Exists) => PeriodicOutcome::Existed,
                    Err(e) => {
                        tracing::warn!(prefix = %crate::logging::text(&target.prefix), ?position, error = %e, "{what} failed");
                        PeriodicOutcome::Failed
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(prefix = %crate::logging::text(&target.prefix), ?position, error = %e, "{what} could not be encoded");
                PeriodicOutcome::Failed
            }
            Err(e) => {
                tracing::warn!(prefix = %crate::logging::text(&target.prefix), ?position, error = %e, "{what}'s encoding stopped");
                PeriodicOutcome::Failed
            }
        };
        if result == PeriodicOutcome::Failed {
            crate::metrics::inc(&crate::metrics::METRICS.snapshot_upload_failures);
        }
        if kind == SnapshotKind::Idle {
            crate::metrics::idle_snapshot(match result {
                PeriodicOutcome::Created => "created",
                PeriodicOutcome::Existed => "existed",
                PeriodicOutcome::Failed => "failed",
            });
        }
        outcome.send_replace(Some(result));
        drop(record);
        result
    }
}

/// Which snapshot an upload is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotKind {
    /// An actor's first, at a new position: its history starts there, so
    /// a failure stops replication. Sent again as a segment is.
    First,
    /// The periodic one, at the head: one short attempt, tried again
    /// later ([`Replica::snapshot_due`]).
    Periodic,
    /// The one after a delete, at the head, so the history before it can
    /// be erased: sent again as a segment is; a failure is not tried
    /// again, and leaves the periodic snapshot's schedule as it was.
    AfterDelete,
    /// The one at hibernation, at the head this hold wrote (ADR 0042,
    /// section 2): one short attempt off the mailbox, as a periodic one
    /// of a batched hold; a failure is not tried again, and leaves the
    /// periodic snapshot's schedule as it was.
    Idle,
    /// A replacement at the head; retains history using the ordinary window.
    AfterRestore,
}

impl Replica {
    /// Whether the first upload must be a snapshot (the bucket was empty).
    pub(crate) fn needs_snapshot(&self) -> bool {
        self.needs_snapshot
    }

    /// Uploads a snapshot of the whole database: an actor's first at a new
    /// position, the others at the current one. The caller captures it
    /// right after `start_capture`, or with nothing pending.
    ///
    /// `Ok(true)`: this call created the snapshot, and it was acknowledged
    /// (completed while the lease was valid), as garbage collection
    /// requires (ADR 0012); `Ok(false)`: one at the head that was already
    /// there.
    ///
    /// The first one and the one after a delete are bounded and sent again
    /// as a segment is ([`Self::put_bounded`]); a periodic one gets a
    /// single short attempt ([`periodic_snapshot_bound`]). A snapshot at
    /// the head that fails stops nothing: the segments already uploaded
    /// are the history. For a periodic one the count of segments since the
    /// last snapshot stays, and [`Self::snapshot_due`] says when it is
    /// tried again; the one after a delete changes nothing of that. Every
    /// upload not acknowledged is counted
    /// (`wasmkeep_snapshot_upload_failures_total`), whether or not the
    /// bucket ends up holding it: an attempt given up may still land.
    ///
    /// Refused before any of that, and replication stopped, while a
    /// segment is in flight, or for a snapshot at the head that does not
    /// hold the head's database ([`Self::refuse_unless_at_head`]).
    pub(crate) async fn ship_snapshot(
        &mut self,
        all: &Changes,
        kind: SnapshotKind,
    ) -> Result<bool, ReplicaError> {
        // A batched hold's periodic snapshot still uploading ends first
        // (bounded by its one attempt): nothing labelled at an older head
        // lands after this one, or after the reset this one may follow.
        self.settle_periodic_upload().await;
        self.finish_sidecar().await?;
        let position = match kind {
            SnapshotKind::First => SegmentId {
                generation: self.generation,
                txid: self.next_txid()?,
            },
            SnapshotKind::Periodic
            | SnapshotKind::AfterDelete
            | SnapshotKind::AfterRestore
            | SnapshotKind::Idle => self.head,
        };
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        // Before anything is counted or uploaded, so a refusal leaves the
        // periodic snapshot's schedule as it was.
        self.refuse_unless_at_head(all, kind)?;
        let bytes = all
            .to_ltx(1, position.txid.max(1), crate::alarm::now_ms_i64())
            .map_err(|e| ReplicaError::Local(e.to_string()))?;
        let key = snapshot_key(position);
        if kind != SnapshotKind::First {
            let sent = if matches!(
                kind,
                SnapshotKind::Periodic | SnapshotKind::AfterRestore | SnapshotKind::Idle
            ) {
                // Counted as failed until it is known to have succeeded,
                // so an upload cancelled half-way waits like one that
                // failed.
                if kind == SnapshotKind::Periodic {
                    self.snapshot_failed();
                }
                let bound = periodic_snapshot_bound(self.upload_idle, bytes.len());
                self.put_once(&key, bytes, bound).await
            } else {
                // After a delete: it confirms the reset position, which
                // the reset segment's acknowledgement recorded.
                self.put_bounded(&key, bytes, "snapshot", None).await
            };
            return match sent {
                Ok(()) => {
                    self.snapshot_taken();
                    Ok(true)
                }
                // The same position holds the same database: already there.
                Err(ReplicaError::Exists) => {
                    self.snapshot_taken();
                    Ok(false)
                }
                Err(e) => {
                    crate::metrics::inc(&crate::metrics::METRICS.snapshot_upload_failures);
                    Err(e)
                }
            };
        }
        self.fail(INTERRUPTED);
        let advance = Some(position_of(position, all.post_apply));
        if let Err(e) = self.put_bounded(&key, bytes, "snapshot", advance).await {
            if !matches!(e, ReplicaError::Exists) {
                crate::metrics::inc(&crate::metrics::METRICS.snapshot_upload_failures);
            }
            self.fail(e.to_string());
            return Err(e);
        }
        self.head = position;
        self.needs_snapshot = false;
        self.snapshot_taken();
        if let Err(e) = self.record(all.post_apply).await {
            self.fail(e.to_string());
            return Err(e);
        }
        self.failed = None;
        self.batched.shared.failure.send_replace(None);
        Ok(true)
    }

    /// Starts a batched hold's periodic snapshot from `all`, the database
    /// at the captured position (`ActorDb::snapshot_at_captured`), taken
    /// in a mailbox gap with nothing in flight: so at the acknowledged
    /// head, which is where it is labelled (ADR 0034, section 7). Checked
    /// here as `ship_snapshot` checks it, before anything is counted.
    ///
    /// Only the label and the bookkeeping happen here; the encoding and the
    /// upload run in the returned job, off the actor's mailbox, while the
    /// actor captures and uploads the segments after the head. Until the
    /// job ends no other periodic snapshot starts, and resets, other
    /// snapshots and barriers wait for it (see
    /// [`Self::periodic_upload_settled`]). The try counts as failed until
    /// it is known to have landed, as in `ship_snapshot`.
    pub(crate) async fn start_periodic_snapshot(
        &mut self,
        all: Changes,
    ) -> Result<PeriodicSnapshot, ReplicaError> {
        self.finish_sidecar().await?;
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        if self.batched.periodic.is_some() || self.periodic_flights.uploading(&self.address) {
            return Err(ReplicaError::Local(
                "a periodic snapshot is uploading".into(),
            ));
        }
        self.refuse_unless_at_head(&all, SnapshotKind::Periodic)?;
        let (outcome, settled) = watch::channel(None);
        let Some((id, cancelled)) = self.periodic_flights.start(&self.address, settled.clone())
        else {
            return Err(ReplicaError::Local(
                "a periodic snapshot is uploading".into(),
            ));
        };
        self.snapshot_failed();
        self.batched.periodic = Some(PeriodicUpload {
            segments: self.since_snapshot,
            bytes: self.batched.bytes_since_snapshot,
            settled,
        });
        Ok(PeriodicSnapshot {
            kind: SnapshotKind::Periodic,
            target: self.target(),
            position: self.head,
            all,
            collector: self.collector(),
            outcome,
            record: FlightRecord {
                flights: Arc::clone(&self.periodic_flights),
                address: self.address.clone(),
                id,
            },
            cancelled,
        })
    }

    /// Starts the idle snapshot of a hold that hibernates (ADR 0042,
    /// section 2) from `all`, the database at the acknowledged head with
    /// nothing in flight: for a strict hold `ActorDb::snapshot`, for a
    /// batched one at its `Flushed` barrier `ActorDb::snapshot_at_captured`.
    /// Taken only if this hold wrote the head, no snapshot is at the head
    /// yet, and it is worth it: [`IDLE_SNAPSHOT_EVERY`] segments since the
    /// last snapshot, or one and a database of at most
    /// [`IDLE_SNAPSHOT_SMALL_BYTES`].
    ///
    /// Recorded as a flight as a batched hold's periodic snapshot is
    /// ([`Self::start_periodic_snapshot`]), in both modes: the step before
    /// a drop that starts it is cancelled when a job arrives, and the
    /// upload goes on, waited for by this node's resets, other snapshots
    /// and barriers, and taken in at the next apply
    /// ([`Self::settle_periodic`]). Unlike a periodic snapshot it leaves
    /// the periodic schedule as it was: its failure is not a failed try.
    pub(crate) async fn start_idle_snapshot(
        &mut self,
        all: Changes,
    ) -> Result<PeriodicSnapshot, IdleSkip> {
        if let Err(e) = self.finish_sidecar().await {
            return Err(IdleSkip::Stopped(e.to_string()));
        }
        let bytes = u64::from(all.commit).saturating_mul(u64::from(all.page_size));
        self.idle_snapshot_wanted(bytes)?;
        if let Err(e) = self.refuse_unless_at_head(&all, SnapshotKind::Idle) {
            return Err(IdleSkip::Refused(e.to_string()));
        }
        let (outcome, settled) = watch::channel(None);
        let Some((id, cancelled)) = self.periodic_flights.start(&self.address, settled.clone())
        else {
            return Err(IdleSkip::Uploading);
        };
        self.batched.periodic = Some(PeriodicUpload {
            segments: self.since_snapshot,
            bytes: self.batched.bytes_since_snapshot,
            settled,
        });
        Ok(PeriodicSnapshot {
            kind: SnapshotKind::Idle,
            target: self.target(),
            position: self.head,
            all,
            collector: self.collector(),
            outcome,
            record: FlightRecord {
                flights: Arc::clone(&self.periodic_flights),
                address: self.address.clone(),
                id,
            },
            cancelled,
        })
    }

    /// Whether a hibernation should take an idle snapshot of a database of
    /// `bytes` ([`Self::start_idle_snapshot`]'s conditions), asked before
    /// the database is copied, and again by that call.
    pub(crate) fn idle_snapshot_wanted(&mut self, bytes: u64) -> Result<(), IdleSkip> {
        self.idle_snapshot_possible()?;
        if self.since_snapshot < IDLE_SNAPSHOT_EVERY && bytes > IDLE_SNAPSHOT_SMALL_BYTES {
            return Err(IdleSkip::BelowThreshold);
        }
        if bytes > IDLE_SNAPSHOT_MAX_BYTES {
            return Err(IdleSkip::TooLarge);
        }
        Ok(())
    }

    /// [`Self::idle_snapshot_wanted`]'s checks that need no size, asked
    /// before the size is read. An upload that ended is taken in first, so
    /// a step cut short does not hold back every later one.
    pub(crate) fn idle_snapshot_possible(&mut self) -> Result<(), IdleSkip> {
        self.settle_periodic();
        if let Some(reason) = &self.failed {
            return Err(IdleSkip::Stopped(reason.clone()));
        }
        if self.head == EMPTY || self.head.generation != self.generation {
            return Err(IdleSkip::NotWriter);
        }
        self.settle_abandoned();
        if self.in_flight.is_some() {
            return Err(IdleSkip::InFlight);
        }
        if self.since_snapshot == 0 {
            return Err(IdleSkip::AtHead);
        }
        if self.batched.periodic.is_some() || self.periodic_flights.uploading(&self.address) {
            return Err(IdleSkip::Uploading);
        }
        if self.snapshot_failures > 0
            && (self.since_snapshot < self.snapshot_retry_at
                || tokio::time::Instant::now() < self.snapshot_retry_after)
        {
            return Err(IdleSkip::BackingOff);
        }
        Ok(())
    }

    /// Makes the replica's database checksum `value`, so that a copy of
    /// the database no longer matches its head (tests of a refusal).
    #[cfg(test)]
    pub(crate) fn set_checksum_for_tests(&mut self, value: u32) {
        self.checksum = DbChecksum::from_value(value.into());
    }

    /// A periodic snapshot's copy failed before anything was labelled: the
    /// try waits as after a failed upload.
    pub(crate) fn periodic_try_failed(&mut self) {
        self.snapshot_failed();
    }

    /// Takes in the outcome of a periodic snapshot's upload if it ended:
    /// landed, the counts start again from its position (the segments
    /// acknowledged since stay counted); not, the try stays counted as
    /// failed. Called at every apply.
    pub(crate) fn settle_periodic(&mut self) {
        let Some(upload) = &self.batched.periodic else {
            return;
        };
        let outcome = *upload.settled.borrow();
        // A job dropped before its outcome (its task cancelled) failed.
        let ended = outcome.is_some() || upload.settled.has_changed().is_err();
        if !ended {
            return;
        }
        let Some(upload) = self.batched.periodic.take() else {
            return;
        };
        if outcome.is_some_and(|o| o != PeriodicOutcome::Failed) {
            self.since_snapshot = self.since_snapshot.saturating_sub(upload.segments);
            self.batched.bytes_since_snapshot = self
                .batched
                .bytes_since_snapshot
                .saturating_sub(upload.bytes);
            self.snapshot_failures = 0;
            self.snapshot_retry_at = 0;
            self.snapshot_waits.forget(&self.address);
        }
    }

    /// A wait for the actor's periodic snapshot uploading now to end, this
    /// hold's or one an earlier hold on this node left ([`PeriodicFlights`]),
    /// held without the replica; `None` if none is. Cancel-safe.
    pub(crate) fn periodic_upload_settled(
        &self,
    ) -> Option<impl std::future::Future<Output = ()> + Send + use<>> {
        self.periodic_flights.settled(&self.address)
    }

    /// Waits, holding the replica, for a periodic snapshot's upload to end
    /// (its one attempt is bounded), then takes in its outcome.
    pub(super) async fn settle_periodic_upload(&mut self) {
        if let Some(settled) = self.periodic_upload_settled() {
            settled.await;
        }
        self.settle_periodic();
    }

    /// Whether a periodic snapshot is due: [`SNAPSHOT_EVERY`] segments
    /// since the last one. One that failed is tried again once the actor
    /// has shipped one more segment and [`SNAPSHOT_RETRY_MIN`] has passed
    /// since that try started; if that fails too, after
    /// [`SNAPSHOT_RETRY_GROWTH`] times as many segments, up to
    /// [`SNAPSHOT_EVERY`]. An actor that only reads, or is idle, does not
    /// try again, and neither does one restored on this node after a
    /// hibernation: its wait is taken up again
    /// ([`Self::resume_snapshot_wait`]). Each try captures the whole
    /// database and, with strict writes, holds the actor for its upload:
    /// [`PERIODIC_SNAPSHOT_IDLES`] has the sums. A batched hold's upload
    /// runs off the mailbox ([`Self::start_periodic_snapshot`]).
    pub(crate) fn snapshot_due(&self) -> bool {
        self.since_snapshot >= SNAPSHOT_EVERY.max(self.snapshot_retry_at)
            && (self.snapshot_failures == 0
                || tokio::time::Instant::now() >= self.snapshot_retry_after)
    }

    /// A snapshot holds the database at the head: the count starts again.
    fn snapshot_taken(&mut self) {
        self.since_snapshot = 0;
        self.batched.bytes_since_snapshot = 0;
        self.snapshot_failures = 0;
        self.snapshot_retry_at = 0;
        self.snapshot_waits.forget(&self.address);
    }

    /// After a restore: takes up the wait an earlier `Replica` of this
    /// actor left on this node ([`SnapshotWaits`]), so a snapshot that
    /// failed is not tried again by the first event after a hibernation.
    /// Fewer than [`SNAPSHOT_EVERY`] segments restored means a snapshot
    /// landed since (an upload given up, or another node's): nothing to
    /// wait for.
    pub(super) fn resume_snapshot_wait(&mut self) {
        let Some(wait) = self.snapshot_waits.get(&self.address) else {
            return;
        };
        if self.since_snapshot < SNAPSHOT_EVERY {
            self.snapshot_waits.forget(&self.address);
            // Kept aside for a batched hold, due by bytes all the same.
            if self.batched.bytes_since_snapshot >= self.batched.snapshot_bytes() {
                self.batched.set_aside_wait = Some(wait);
            }
            return;
        }
        self.take_up_snapshot_wait(&wait);
    }

    /// Makes `wait` this replica's own: its failures, the segment count and
    /// the time before the next try.
    fn take_up_snapshot_wait(&mut self, wait: &SnapshotWait) {
        self.snapshot_failures = wait.failures;
        self.snapshot_retry_at = self
            .since_snapshot
            .saturating_add(wait.txid.saturating_sub(self.head.txid));
        self.snapshot_retry_after = wait.after;
    }

    /// For a batched hold, once its mode is known: takes up a wait that
    /// [`Self::resume_snapshot_wait`] set aside because fewer than
    /// [`SNAPSHOT_EVERY`] segments were restored, when the restored
    /// segments' bytes make a snapshot due by bytes all the same (ADR 0034,
    /// section 7). A strict hold never asks, and keeps its rule.
    pub(crate) fn resume_batched_snapshot_wait(&mut self) {
        let Some(wait) = self.batched.set_aside_wait.take() else {
            return;
        };
        self.take_up_snapshot_wait(&wait);
        self.snapshot_waits.set(&self.address, wait);
    }

    /// A periodic snapshot failed (or is starting): when the next try is
    /// allowed.
    pub(super) fn snapshot_failed(&mut self) {
        // 1 segment after the first failure in a row, then 8, then 64.
        let wait = SNAPSHOT_RETRY_GROWTH
            .checked_pow(self.snapshot_failures)
            .map_or(SNAPSHOT_EVERY, |w| w.min(SNAPSHOT_EVERY));
        self.snapshot_failures = self.snapshot_failures.saturating_add(1);
        self.snapshot_retry_at = self.since_snapshot.saturating_add(wait);
        self.snapshot_retry_after = tokio::time::Instant::now() + self.snapshot_retry_min;
        self.snapshot_waits.set(
            &self.address,
            SnapshotWait {
                failures: self.snapshot_failures,
                txid: self.head.txid.saturating_add(wait),
                after: self.snapshot_retry_after,
            },
        );
    }
}
