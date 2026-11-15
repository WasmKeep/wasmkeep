//! Batched plain writes on the actor's side (ADR 0034, sections 2 to 4
//! and 6): boundaries, the capture step, the completion job, the wait at
//! an event's end, `flush` and the barriers.
//!
//! The replica's uploader ([`crate::replica::Ticket`]) does the upload;
//! this module decides when. At each mailbox gap the actor marks a
//! boundary, applies an outcome that landed, and, when nothing is in
//! flight, captures up to the last boundary within the cap and hands the
//! segment to its upload task, without waiting for it. A small driver task
//! waits for the segment to land and posts the completion job, which
//! applies the outcome and captures again. The driver owns nothing of the
//! actor's state: the outcome is in the replica's cell, and the job finds
//! the state through the mailbox, or not at all (then the next boundary,
//! flush or step before a drop applies it).
//!
//! Strict classes never come here.

use std::sync::LazyLock;

use super::*;
use crate::replica::UploadBudget;
use wasmkeep_storage::CaptureHold;
use wasmkeep_storage::wal::WalBoundary;

/// The node's budget of batched segments in flight (ADR 0034, section 2).
static NODE_BUDGET: LazyLock<UploadBudget> = LazyLock::new(UploadBudget::node);

/// Takes the whole node budget of batched uploads until the guard is
/// dropped, as a node whose budget is full: for tests, in a binary built
/// with `batched-writes`.
#[cfg(feature = "batched-writes")]
#[doc(hidden)]
pub fn hold_node_upload_budget() -> impl Drop + Send {
    NODE_BUDGET.hold_all()
}

/// How far a batched flush got by its deadline (ADR 0034, section 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FlushError {
    /// The wait ran out while the upload went on: `not durable: still
    /// uploading`. The replica stays healthy.
    StillUploading,
    /// Replication stopped: the actor is to be given up.
    Stopped(String),
}

impl FlushError {
    /// What the guest's `flush` (or `exec-durable`) answers.
    pub(super) fn to_sql(&self) -> SqlError {
        let message = match self {
            Self::StillUploading => STILL_UPLOADING.to_owned(),
            Self::Stopped(e) if e.starts_with(REPLICATION_STOPPED) => e.clone(),
            Self::Stopped(e) => format!("{REPLICATION_STOPPED}: {e}"),
        };
        SqlError {
            kind: SqlErrorKind::Internal,
            message,
        }
    }
}

/// Where a batched step reaches the actor's database.
pub(super) enum DbAt<'a> {
    /// The instance's: run on the blocking pool, and kept across a
    /// cancelled wait ([`ActorHost::with_db`]), so a capture under way
    /// completes and hands its segment over whatever happens to the wait.
    Host(&'a mut ActorHost),
    /// The database while no instance holds it: run synchronously, so no
    /// cancellation can part it from the state, and on a multi-threaded
    /// runtime through `block_in_place`, so its I/O (a capture of at most
    /// the cap, a checkpoint, an fsync) does not stall the worker's other
    /// tasks.
    Resident(&'a mut ActorDb),
    /// No database open: nothing local to capture.
    Closed,
}

impl DbAt<'_> {
    async fn run<T: Send + 'static>(
        &mut self,
        f: impl FnOnce(&mut ActorDb) -> T + Send + 'static,
    ) -> Result<Option<T>, String> {
        match self {
            Self::Host(host) => host
                .with_db(f)
                .await
                .map(Some)
                .map_err(|e| format!("{e:#}")),
            Self::Resident(db) => {
                let multi = tokio::runtime::Handle::try_current().is_ok_and(|h| {
                    h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
                });
                Ok(Some(if multi {
                    tokio::task::block_in_place(|| f(db))
                } else {
                    f(db)
                }))
            }
            Self::Closed => Ok(None),
        }
    }
}

/// Who to tell when a segment lands: the actor's mailbox, through the
/// service. Cheap to clone; holds no state of the actor.
#[derive(Clone)]
pub(super) struct Completion {
    pub(super) service: Option<std::sync::Weak<ServiceInner>>,
    pub(super) address: ActorAddress,
}

impl Completion {
    /// Spawns the driver of one segment: it waits for `landed`, then posts
    /// the completion job if the actor is resident. A `Full` or `Gone`
    /// answer is dropped: the outcome waits in the replica's cell.
    fn spawn(&self, landed: impl std::future::Future<Output = ()> + Send + 'static) {
        let (service, address) = (self.service.clone(), self.address.clone());
        tokio::spawn(async move {
            landed.await;
            let Some(inner) = service.and_then(|w| w.upgrade()) else {
                return;
            };
            let job = inner.mailboxes.submit_if_resident(&address, completion_job);
            drop(inner);
            let _ = job.await;
        });
    }
}

/// The completion job (ADR 0034, section 2). A plain function returning a
/// boxed future: the driver it may spawn again names this function, not
/// an async body, so the future's type does not refer to itself.
fn completion_job(state: &mut ActorState) -> BoxFuture<'_, ()> {
    state.complete_uploads()
}

/// Turns batched capture on for `db` under `token`, starting capture first
/// if it has not (a database opened without an event, for an alarm's check
/// for state: nothing uncaptured under this hold yet).
fn enable_batched(db: &mut ActorDb, token: &CaptureHold) -> Result<(), String> {
    if db.batched_capture_enabled() {
        return Ok(());
    }
    if db.captured_checksum().is_none() {
        db.start_capture()
            .map_err(|e| format!("start capture: {e}"))?;
    }
    db.enable_batched_capture(token)
        .map_err(|e| format!("batched capture: {e}"))
}

impl ActorHost {
    /// [`enable_batched`] on the instance's database, recovered first from
    /// database work a cancelled step left running (`pending_db`).
    pub(super) async fn ensure_batched_capture(
        &mut self,
        token: &CaptureHold,
    ) -> Result<(), String> {
        self.drain_db().await.map_err(|e| format!("{e:#}"))?;
        let db = self
            .db
            .as_mut()
            .ok_or_else(|| "actor database unavailable".to_owned())?;
        enable_batched(db, token)
    }
}

/// Marks a boundary at the last commit and records it with the replica.
/// A database not capturing in batched mode has nothing to mark.
async fn mark(slot: &SharedReplica, db: &mut DbAt<'_>) -> Result<Option<WalBoundary>, String> {
    let marked = db
        .run(|db| {
            if db.batched_capture_enabled() {
                db.mark_boundary()
            } else {
                Ok(None)
            }
        })
        .await?
        .transpose()
        .map_err(|e| format!("boundary: {e}"))?
        .flatten();
    if let Some(boundary) = marked {
        slot.lock()
            .await
            .note_boundary(boundary, std::time::Instant::now());
    }
    Ok(marked)
}

/// Applies an outcome that landed, then, if nothing is in flight and
/// something is uncaptured, starts the next segment: reserve its room in
/// the node budget, prepare it, and capture, encode and hand it off in one
/// step on the database. Never waits for an upload. Waits for room in the
/// budget without holding the replica, and only until `deadline`:
/// `Ok(false)` if it passed first (nothing captured). `Err`: replication
/// stopped, and the caller gives the actor up.
async fn pump(
    slot: &SharedReplica,
    db: &mut DbAt<'_>,
    completion: Option<&Completion>,
    deadline: Option<tokio::time::Instant>,
) -> Result<bool, String> {
    let mut replica = slot.lock().await;
    replica.apply().await.map_err(|e| e.to_string())?;
    if let Some(reason) = replica.failed() {
        return Err(format!("{REPLICATION_STOPPED}: {reason}"));
    }
    if !replica.uploader_idle() {
        return Ok(true);
    }
    let plan = db
        .run(|db| {
            db.batched_capture_enabled()
                .then(|| db.next_bounded_capture())
                .flatten()
        })
        .await?
        .flatten();
    let Some(plan) = plan else {
        // The database has nothing left to capture: its count holds.
        replica.forget_marked();
        return Ok(true);
    };
    // Waits for room in FIFO order, the actor held meanwhile (ADR 0034,
    // section 3, rule 3, applied at once rather than past the age bound),
    // the replica free for the completions and the deadline kept.
    drop(replica);
    let reserve = NODE_BUDGET.reserve(plan.boundary.estimated_bytes);
    let permit = match deadline {
        Some(at) => tokio::select! {
            biased;
            permit = reserve => permit,
            () = tokio::time::sleep_until(at) => return Ok(false),
        },
        None => reserve.await,
    }
    .map_err(|e| e.to_string())?;
    let mut replica = slot.lock().await;
    if !replica.uploader_idle() {
        return Ok(true);
    }
    let ticket = replica
        .prepare(plan, permit)
        .await
        .map_err(|e| e.to_string())?;
    let landed = replica.landed();
    let handed = db.run(move |db| ticket.hand_off(db)).await?;
    match handed {
        Some(Ok(_)) => {}
        Some(Err(e)) => return Err(e.to_string()),
        // Unreachable: a closed database has no plan.
        None => return Err("no database to capture from".into()),
    }
    drop(replica);
    if let Some(completion) = completion {
        completion.spawn(landed);
    }
    Ok(true)
}

/// The wait at an event's end (ADR 0034, sections 2 and 3): an event over
/// the cap waits for its own segment, and while the event-start rule does
/// not hold (the oldest uncaptured commit is older than `max_unsent`, the
/// next segment is already full, or the WAL wants a checkpoint), the actor
/// waits for the segment in flight to land and the next capture to start.
/// Bounded: each turn waits for one upload, whose task has its budget.
async fn hold_at_event_end(
    slot: &SharedReplica,
    db: &mut DbAt<'_>,
    completion: &Completion,
    max_unsent: Duration,
) -> Result<(), String> {
    loop {
        let (oversize, old, idle, landed) = {
            let r = slot.lock().await;
            (
                r.in_flight_oversize(),
                r.oldest_unsent().is_some_and(|t| t.elapsed() >= max_unsent),
                r.uploader_idle(),
                r.landed(),
            )
        };
        let (full, pressure) = db
            .run(|db| (db.next_segment_full(), db.wal_wants_checkpoint()))
            .await?
            .unwrap_or((false, false));
        if !(oversize || old || full || pressure) {
            return Ok(());
        }
        if idle {
            pump(slot, db, Some(completion), None).await?;
            if slot.lock().await.uploader_idle() {
                // Nothing left to capture: relieve the WAL if it asks.
                if pressure {
                    let _ = db.run(ActorDb::checkpoint_batched_capture).await?;
                }
                return Ok(());
            }
            continue;
        }
        landed.await;
        pump(slot, db, Some(completion), None).await?;
    }
}

/// Flushes every commit made before the call (ADR 0034, section 4): marks
/// a boundary there, then applies, captures and waits, the flushing event
/// doing the completions itself, until the acknowledged head covers it or
/// `deadline` passes. With less than a second of the wall left the caller
/// passes a deadline already past: the boundary is recorded, its capture
/// started, and the answer given at once.
pub(super) async fn flush_through(
    slot: &SharedReplica,
    db: &mut DbAt<'_>,
    completion: Option<&Completion>,
    deadline: tokio::time::Instant,
) -> Result<(), FlushError> {
    let marked = mark(slot, db).await.map_err(FlushError::Stopped)?;
    let Some(target) = slot.lock().await.flush_target(marked) else {
        return Ok(());
    };
    loop {
        let started = pump(slot, db, completion, Some(deadline))
            .await
            .map_err(FlushError::Stopped)?;
        if !started {
            return Err(FlushError::StillUploading);
        }
        let landed = {
            let r = slot.lock().await;
            if r.acknowledged_covers(target) {
                return Ok(());
            }
            r.landed()
        };
        tokio::select! {
            () = landed => {}
            () = tokio::time::sleep_until(deadline) => return Err(FlushError::StillUploading),
        }
    }
}

/// Flushes the hold to its barrier (ADR 0034, section 6): nothing
/// uncaptured, nothing in flight. `Some(Flushed)` there; `Some(GaveUp)` if
/// replication stopped; `None` if `deadline` passed first. Cancel-safe.
async fn to_barrier(
    slot: &SharedReplica,
    db: &mut DbAt<'_>,
    completion: Option<&Completion>,
    deadline: Option<tokio::time::Instant>,
) -> Option<Barrier> {
    if mark(slot, db).await.is_err() {
        return Some(Barrier::GaveUp(Dropped::Failure));
    }
    loop {
        match pump(slot, db, completion, deadline).await {
            Ok(true) => {}
            Ok(false) => return None,
            Err(_) => return Some(Barrier::GaveUp(Dropped::Failure)),
        }
        let (done, landed) = {
            let r = slot.lock().await;
            (r.nothing_pending(), r.landed())
        };
        if done {
            match db.run(|db| db.has_pending_boundaries()).await {
                Ok(Some(true)) => continue,
                Ok(_) => return Some(Barrier::Flushed),
                Err(_) => return Some(Barrier::GaveUp(Dropped::Failure)),
            }
        }
        match deadline {
            Some(at) => tokio::select! {
                () = landed => {}
                () = tokio::time::sleep_until(at) => return None,
            },
            None => landed.await,
        }
    }
}

impl ActorState {
    /// The replica of a batched hold, if this node holds the actor
    /// restored under one.
    pub(super) fn batched_replica(&self) -> Option<SharedReplica> {
        let held = self.held.as_ref()?;
        (held.writes() == crate::apps::Writes::Batched)
            .then(|| held.replica().cloned())
            .flatten()
    }

    pub(super) fn completion(&self) -> Completion {
        Completion {
            service: self.service.get().cloned(),
            address: self.address.clone(),
        }
    }

    /// The actor's database, wherever it is now.
    fn db_at(&mut self) -> DbAt<'_> {
        if let Some(live) = self.live.as_mut() {
            DbAt::Host(live.store.data_mut())
        } else if let Some(db) = self.db.as_mut() {
            DbAt::Resident(db)
        } else {
            DbAt::Closed
        }
    }

    /// The completion job: applies what landed and captures again. Acts
    /// only on the hold it finds: never acquires, restores or builds
    /// anything, and is no event (it runs no guest code and writes nothing
    /// to the owner record, which is what idle expiry reads, ADR 0028).
    pub(super) fn complete_uploads(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.stepped = false;
            let Some(slot) = self.batched_replica() else {
                return;
            };
            let completion = self.completion();
            let pumped = pump(&slot, &mut self.db_at(), Some(&completion), None).await;
            if let Err(e) = pumped {
                self.abandon(&e);
            }
        })
    }

    /// After an event of a batched hold (in place of strict's inline
    /// upload): a boundary, the outcome that landed, the next capture,
    /// then the wait the event-start rule asks for. A failure gives the
    /// actor up (section 8: no queued event starts on that state).
    pub(super) async fn replicate_batched(&mut self, slot: SharedReplica) {
        let completion = self.completion();
        let max_unsent = self.limits.max_unsent_age;
        let mut db = self.db_at();
        let done = async {
            mark(&slot, &mut db).await?;
            pump(&slot, &mut db, Some(&completion), None).await?;
            hold_at_event_end(&slot, &mut db, &completion, max_unsent).await
        }
        .await;
        if let Err(e) = done {
            self.abandon(&e);
        }
    }

    /// Flushes the hold to its barrier by `deadline` (ADR 0034, section 6).
    /// A strict hold is there at once; so is one with nothing restored.
    pub(super) async fn flush_to_barrier(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> Option<Barrier> {
        let hold = self.held.as_ref()?;
        let Some(slot) = self.batched_replica() else {
            return Some(hold.barrier());
        };
        // A job that skips `prepare` (a hand-over, a delete, an alarm) may
        // find capture ended by a step before a drop that an event cut
        // short: enabled again first, as before an event.
        if let Err(e) = self.ensure_batched_capture().await {
            tracing::warn!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), error = %e, "batched capture could not start");
            return Some(Barrier::GaveUp(Dropped::Failure));
        }
        // No completion jobs: the step before a drop applies them itself,
        // and a job it posted would cut it short (the mailbox serves a job
        // that arrives during the step first).
        to_barrier(&slot, &mut self.db_at(), None, deadline).await
    }

    /// One upload's budget from now: the deadline of a hibernation's flush
    /// and of a mode switch's (ADR 0034, sections 1 and 6).
    pub(super) fn upload_deadline(
        &self,
    ) -> impl std::future::Future<Output = tokio::time::Instant> + Send + 'static {
        let replica = self.replica().cloned();
        async move {
            let budget = match replica {
                Some(r) => r.lock().await.upload_budget(),
                None => Duration::from_secs(21),
            };
            tokio::time::Instant::now() + budget
        }
    }

    /// At a `Flushed` barrier: the database leaves batched capture (its WAL
    /// checkpointed, its close behaviour restored), and the hold drops its
    /// capture token. The next event of a batched hold enables it again
    /// ([`Self::ensure_batched_capture`]).
    pub(super) async fn end_capture_at_barrier(&mut self) {
        if !self.held.as_ref().is_some_and(Held::captures) {
            return;
        }
        let finished = self
            .db_at()
            .run(|db| {
                if db.batched_capture_enabled() {
                    db.finish_batched_capture()
                } else {
                    Ok(())
                }
            })
            .await;
        if !matches!(finished, Ok(None | Some(Ok(())))) {
            tracing::warn!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), ?finished, "leaving batched capture failed; its WAL stays");
        }
        if let Some(held) = self.held.as_mut() {
            held.end_capture();
        }
    }

    /// Before an event of a batched hold, and before any flush to its
    /// barrier: its database captures in batched mode, under the hold's
    /// token. A strict hold, or one not restored, has nothing to do.
    pub(super) async fn ensure_batched_capture(&mut self) -> Result<(), String> {
        if self.batched_replica().is_none() {
            return Ok(());
        }
        let Some(held) = self.held.as_mut() else {
            return Ok(());
        };
        let token = held.capture_token();
        if let Some(live) = self.live.as_mut() {
            return live.store.data_mut().ensure_batched_capture(token).await;
        }
        match self.db.as_mut() {
            Some(db) => enable_batched(db, token),
            None => Ok(()),
        }
    }

    /// A deploy changed the class's mode of writes: the hold switches at a
    /// `Flushed` barrier (ADR 0034, section 1). Strict to batched needs no
    /// flush. Batched to strict flushes with one upload budget as its
    /// deadline; past it the hold stays batched and tries again at the
    /// next event; on a failure it is given up.
    pub(super) async fn switch_writes(&mut self) -> Result<(), String> {
        let want = self.class.writes;
        let Some(held) = self.held.as_mut() else {
            return Ok(());
        };
        if held.writes() == want {
            return Ok(());
        }
        if want == crate::apps::Writes::Batched {
            held.switch_writes(want, Barrier::Flushed);
            tracing::info!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), writes = want.as_str(), "the hold now runs the class's new mode of writes");
            return Ok(());
        }
        let deadline = self.upload_deadline().await;
        match self.flush_to_barrier(Some(deadline)).await {
            Some(Barrier::Flushed) => {
                self.end_capture_at_barrier().await;
                if let Some(held) = self.held.as_mut()
                    && held.switch_writes(want, Barrier::Flushed)
                {
                    tracing::info!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), writes = want.as_str(), "the hold now runs the class's new mode of writes");
                }
                Ok(())
            }
            Some(Barrier::GaveUp(_)) => {
                self.abandon("replication stopped before switching to strict writes");
                Err(format!("{REPLICATION_STOPPED} before switching modes"))
            }
            None => Ok(()),
        }
    }
}
