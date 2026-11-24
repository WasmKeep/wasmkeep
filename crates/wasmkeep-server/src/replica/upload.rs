//! Uploading segments: the bounded, retried put and its read-back.

use super::*;

/// A segment upload (one transaction, usually a few pages) is bounded too:
/// an attempt the bucket has not answered within this, plus a second per
/// started megabyte, is given up and sent again, the next attempt allowed
/// twice as long, up to [`UPLOAD_ATTEMPTS`] times. Without it one hung PUT
/// waited for the client's own 30 s timeout, the event was cut and the
/// actor given up (seen in a smoke run against a real bucket).
///
/// An attempt here is one call to the bucket's client, which may send
/// more than one request within it: after an error answer (a 5xx, 429 or
/// 408) or a request it could not send, it pauses and sends again. It
/// has a timeout of its own on each request, 30 s from connecting to the
/// last byte of the answer (its default, which `wasmkeep serve` leaves
/// as the environment has it), and a conditional create that times out,
/// or whose connection breaks, it does not send again: the call fails.
/// So the bound above is the only limit on an attempt (one answered with
/// errors can hold several requests and last past 30 s where the bound
/// allows), and 30 s is the limit on a request: the second per megabyte
/// gives a first attempt's request more time up to 25 MB and no further,
/// and an object the bucket cannot take within 30 s in one request is
/// never uploaded, whatever the deadline allows.
///
/// An actor's first snapshot and the one after a delete (the whole
/// database) are uploaded under these bounds; a periodic snapshot under
/// shorter ones, see [`periodic_snapshot_bound`]. An upload holds the
/// actor's replica, and with it the actor, until it ends.
pub(super) const UPLOAD_IDLE: std::time::Duration = std::time::Duration::from_secs(5);
/// Attempts per upload before it fails (a segment's stops replication;
/// a periodic snapshot has one, and is tried again later).
pub(super) const UPLOAD_ATTEMPTS: u32 = 3;
/// The whole upload (every attempt and any read-back) ends within this
/// many [`UPLOAD_IDLE`]s, plus the same second per started megabyte: 21 s
/// for a segment under a megabyte, so one stalled upload leaves an event
/// (30 s) some room; the bound is per upload, not per event. The last
/// attempt has whatever is left. Attempts before it are cut at their
/// limit even while moving, so a multi-megabyte upload slower than about
/// a megabyte a second still fails, as it did under the client's 30 s.
pub(super) const UPLOAD_BUDGET_IDLES: u32 = 4;

/// What [`Replica::put_bounded`] uploads with: the bucket, the actor's
/// history prefix, the hold's validity and the attempt's base bound.
#[derive(Debug, Clone)]
pub(super) struct UploadTarget {
    store: Arc<dyn ObjectStore>,
    pub(super) prefix: String,
    pub(super) lease: LeaseView,
    pub(super) upload_idle: std::time::Duration,
}

impl UploadTarget {
    /// A periodic snapshot's upload: one conditional create (one call
    /// to the client, which may send it again after an error answer),
    /// given up after `bound` and not called again, then the
    /// acknowledgement rule as
    /// in [`Self::put_bounded`]. What is given up may still land; nothing
    /// depends on it. A create refused as existing counts as "already
    /// there" only if the object is seen within the same bound.
    pub(super) async fn put_once(
        &self,
        key: &str,
        bytes: Vec<u8>,
        bound: std::time::Duration,
    ) -> Result<(), ReplicaError> {
        use tokio::time::timeout_at;
        let location = StorePath::from(format!("{}{key}", self.prefix));
        let until = tokio::time::Instant::now() + bound;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let sent = self
            .store
            .put_opts(&location, PutPayload::from(bytes), opts);
        let reason = match timeout_at(until, sent).await {
            // A snapshot at the head: a check, never an advance.
            Ok(Ok(_)) => return self.acknowledged(None),
            Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                match timeout_at(until, self.store.head(&location)).await {
                    Ok(Ok(_)) => return Err(ReplicaError::Exists),
                    Ok(Err(e)) => format!("refused as existing, then: {e}"),
                    Err(_) => "refused as existing, then no answer in time".into(),
                }
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => format!("no answer within {bound:?}"),
        };
        tracing::warn!(object = %location, what = "snapshot", %reason, "a periodic snapshot's upload was given up");
        Err(ReplicaError::Bucket(format!("{location}: {reason}")))
    }

    /// See [`Replica::put_bounded`].
    pub(super) async fn put_bounded(
        &self,
        key: &str,
        bytes: Vec<u8>,
        what: &'static str,
        advance: Option<Position>,
    ) -> Result<(), ReplicaError> {
        self.put_bounded_by(key, bytes, what, advance, None).await
    }

    /// The upload's budget for `len` bytes: its base idle bounds plus a
    /// second per started megabyte (what [`Self::put_bounded`] allows).
    pub(super) fn budget_for(&self, len: usize) -> std::time::Duration {
        let megabytes = started_mib(len);
        self.upload_idle
            .saturating_mul(UPLOAD_BUDGET_IDLES)
            .saturating_add(std::time::Duration::from_secs(1).saturating_mul(megabytes))
    }

    /// [`Self::put_bounded`], ending by `cap` at the latest: a batched
    /// segment whose alarm entry went first, within the same budget (ADR
    /// 0034, section 9).
    pub(super) async fn put_bounded_by(
        &self,
        key: &str,
        bytes: Vec<u8>,
        what: &'static str,
        advance: Option<Position>,
        cap: Option<tokio::time::Instant>,
    ) -> Result<(), ReplicaError> {
        let started = std::time::Instant::now();
        let mut resends = 0u64;
        let result = self
            .put_bounded_attempts(key, bytes, what, advance, cap, &mut resends)
            .await;
        if what == "segment" {
            crate::metrics::segment_upload(started.elapsed(), resends, result.is_ok());
        }
        result
    }

    /// [`Self::put_bounded_by`]'s attempts; `resends` counts the PUTs sent
    /// again after the first.
    async fn put_bounded_attempts(
        &self,
        key: &str,
        bytes: Vec<u8>,
        what: &'static str,
        advance: Option<Position>,
        cap: Option<tokio::time::Instant>,
        resends: &mut u64,
    ) -> Result<(), ReplicaError> {
        use tokio::time::{Instant, timeout_at};
        let location = StorePath::from(format!("{}{key}", self.prefix));
        // Shared, not copied, by each attempt.
        let bytes = bytes::Bytes::from(bytes);
        let megabytes = started_mib(bytes.len());
        let per_size = std::time::Duration::from_secs(1).saturating_mul(megabytes);
        let first = self.upload_idle.saturating_add(per_size);
        let deadline = Instant::now()
            + self
                .upload_idle
                .saturating_mul(UPLOAD_BUDGET_IDLES)
                .saturating_add(per_size);
        let deadline = cap.map_or(deadline, |cap| deadline.min(cap));
        let mut last: String;
        // Attempts given up unanswered; a refusal with nothing there does
        // not count (the attempt it raced may be about to land), it is
        // sent again after a pause that grows, until the deadline.
        let mut stalled = 0;
        let mut pause = self.upload_idle / 10;
        loop {
            let until = if stalled + 1 >= UPLOAD_ATTEMPTS {
                deadline
            } else {
                deadline.min(Instant::now() + first.saturating_mul(1 << stalled))
            };
            let opts = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            let sent = self
                .store
                .put_opts(&location, PutPayload::from(bytes.clone()), opts);
            match timeout_at(until, sent).await {
                Ok(Ok(_)) => return self.acknowledged(advance),
                Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                    match self.read_back(&location, &bytes, deadline).await {
                        ReadBack::Ours => {
                            tracing::info!(object = %location, what, "an upload had landed unanswered; acknowledged");
                            return self.acknowledged(advance);
                        }
                        ReadBack::Other => return Err(ReplicaError::Exists),
                        ReadBack::Refused(e) => return Err(e),
                        ReadBack::Missing => {
                            last = "refused as existing, yet not there".into();
                            if !self.lease.is_valid() {
                                last = format!("{last}; the lease ran out meanwhile");
                                break;
                            }
                            *resends += 1;
                            tracing::info!(object = %location, what, reason = %last, "an upload raced an earlier attempt; sending it again");
                            let left = deadline.saturating_duration_since(Instant::now());
                            tokio::time::sleep(pause.min(left)).await;
                            pause = pause.saturating_mul(2).min(self.upload_idle);
                        }
                        // Maybe this upload's own: asked again until the
                        // deadline, as a refusal with nothing there is.
                        ReadBack::Failed(e) => {
                            last = format!("reading back an existing object: {e}");
                            if !self.lease.is_valid() {
                                last = format!("{last}; the lease ran out meanwhile");
                                break;
                            }
                            *resends += 1;
                            tracing::info!(object = %location, what, reason = %last, "an upload's read-back failed; trying again");
                            let left = deadline.saturating_duration_since(Instant::now());
                            tokio::time::sleep(pause.min(left)).await;
                            pause = pause.saturating_mul(2).min(self.upload_idle);
                        }
                        ReadBack::OutOfTime => {
                            last = "reading back an existing object ran out of time".into();
                            break;
                        }
                    }
                }
                Ok(Err(e)) if definite(&e) => return Err(ReplicaError::Bucket(e.to_string())),
                // A failure a resend may get past (a reset connection, a
                // 5xx once the client's own retries are spent): an attempt
                // used, sent again after a pause; a resend that meets an
                // object it had landed reads it back.
                Ok(Err(e)) => {
                    stalled += 1;
                    last = e.to_string();
                    if stalled >= UPLOAD_ATTEMPTS {
                        break;
                    }
                    if !self.lease.is_valid() {
                        last = format!("{last}; the lease ran out meanwhile");
                        break;
                    }
                    *resends += 1;
                    tracing::info!(object = %location, what, reason = %last, "an upload failed; sending it again");
                    let left = deadline.saturating_duration_since(Instant::now());
                    tokio::time::sleep(pause.min(left)).await;
                    pause = pause.saturating_mul(2).min(self.upload_idle);
                }
                Err(_) => {
                    stalled += 1;
                    last = "no answer in time".into();
                    if stalled >= UPLOAD_ATTEMPTS {
                        break;
                    }
                    *resends += 1;
                    tracing::info!(object = %location, what, reason = %last, "an upload stalled; sending it again");
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            // No resend once the hold may not act: what lands then would
            // not be acknowledged anyway (`Late`), and the event ends.
            if !self.lease.is_valid() {
                last = format!("{last}; the lease ran out meanwhile");
                break;
            }
        }
        tracing::warn!(object = %location, what, reason = %last, "an upload kept failing");
        Err(ReplicaError::Bucket(format!("{location}: {last}")))
    }
}

/// A history position and the database checksum after it, as the hold
/// cell records an acknowledged head (ADR 0035, section 2).
pub(crate) fn position_of(head: SegmentId, checksum: DbChecksum) -> Position {
    if head == EMPTY {
        Position::Empty
    } else {
        Position::Known {
            generation: head.generation,
            txid: head.txid,
            checksum: checksum.value(),
        }
    }
}

/// A failure no resend changes: the bucket refused the request itself (on
/// an upload, `NotFound` is the bucket missing, and a precondition cannot
/// fail a create). An attempt in flight, or a pause, still runs out
/// before a lapse is seen: the bound is one attempt or one pause.
pub(super) fn definite(e: &object_store::Error) -> bool {
    matches!(
        e,
        object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::NotFound { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
    )
}

impl UploadTarget {
    /// What sits at `location` after a create was refused, read before
    /// `deadline`.
    async fn read_back(
        &self,
        location: &StorePath,
        bytes: &[u8],
        deadline: tokio::time::Instant,
    ) -> ReadBack {
        use tokio::time::timeout_at;
        match timeout_at(deadline, self.store.head(location)).await {
            Err(_) => return ReadBack::OutOfTime,
            Ok(Err(object_store::Error::NotFound { .. })) => return ReadBack::Missing,
            Ok(Err(e)) if definite(&e) => {
                return ReadBack::Refused(ReplicaError::Bucket(e.to_string()));
            }
            Ok(Err(e)) => return ReadBack::Failed(ReplicaError::Bucket(e.to_string())),
            Ok(Ok(meta)) if meta.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) => {
                return ReadBack::Other;
            }
            Ok(Ok(_)) => {}
        }
        let max = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        match timeout_at(
            deadline,
            bounded_get(
                &self.store,
                location,
                max,
                max,
                Some(max),
                &ReadTime::new(max, None),
                None,
            ),
        )
        .await
        {
            Err(_) => ReadBack::OutOfTime,
            Ok(Ok(found)) if found.as_ref() == bytes => ReadBack::Ours,
            // Other bytes, or more of them than this upload sent.
            Ok(Ok(_) | Err(ReplicaError::Restore(_))) => ReadBack::Other,
            Ok(Err(e)) => ReadBack::Failed(e),
        }
    }

    /// The acknowledgement rule: an upload counts only if it completed
    /// while the hold could still act. Checked and, with `advance`, the
    /// hold's acknowledged head moved, in one critical section of the hold
    /// cell (ADR 0035, section 2), so an ending hold freezes it or this is
    /// late.
    fn acknowledged(&self, advance: Option<Position>) -> Result<(), ReplicaError> {
        self.lease
            .acknowledge(advance)
            .map_err(|_| ReplicaError::Late)
    }
}

/// What a refused create found at its key.
enum ReadBack {
    /// This upload's own bytes: an earlier attempt landed.
    Ours,
    /// Anything else: a conflict.
    Other,
    /// Nothing yet (the refusal raced an attempt still in flight).
    Missing,
    /// The bucket failed the read; a resend may get past it.
    Failed(ReplicaError),
    /// The bucket refused the read itself (permission, authentication):
    /// no resend changes it.
    Refused(ReplicaError),
    /// The upload's deadline passed first.
    OutOfTime,
}

impl Replica {
    /// Uploads one transaction. `Ok` means durable. Refuses changes that do
    /// not follow the current head, and anything after a failure.
    pub(crate) async fn ship(&mut self, changes: &Changes) -> Result<(), ReplicaError> {
        self.finish_sidecar().await?;
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        self.settle_abandoned();
        // Only a batched segment can be in flight here (a strict `ship`
        // cut half-way left `failed` set, refused above): an inline upload
        // beside it would be a second segment in flight (ADR 0034,
        // section 2), and a mode switch waits for a `Flushed` barrier.
        if let Some(segment) = self.in_flight {
            return Err(self.refuse(format!("a segment while {segment:?} is in flight")));
        }
        if changes.pre_apply != self.checksum {
            // Something committed locally was never shipped: building on
            // the bucket's head now would fork the history.
            self.fail("local changes do not follow the replicated head");
            return Err(ReplicaError::Local(
                "local database is ahead of the bucket".into(),
            ));
        }
        let segment = Segment {
            id: SegmentId {
                generation: self.generation,
                txid: self.next_txid()?,
            },
            parent_generation: self.head.generation,
        };
        let bytes = changes
            .to_ltx(segment.id.txid, segment.id.txid, crate::alarm::now_ms_i64())
            .map_err(|e| ReplicaError::Local(e.to_string()))?;
        // Marked failed until the upload is known to have succeeded, so a
        // cancelled upload (the event was cut mid-way) stops replication;
        // and in flight until it ends, which a cancelled one never does.
        self.fail(INTERRUPTED);
        self.in_flight = Some(segment.id);
        let sent = self
            .put_bounded(
                &segment_key(&segment),
                bytes,
                "segment",
                Some(position_of(segment.id, changes.post_apply)),
            )
            .await;
        self.in_flight = None;
        if let Err(e) = sent {
            self.fail(e.to_string());
            return Err(e);
        }
        self.head = segment.id;
        self.since_snapshot += 1;
        if let Err(e) = self.record(changes.post_apply).await {
            self.fail(e.to_string());
            return Err(e);
        }
        self.failed = None;
        self.batched.shared.failure.send_replace(None);
        Ok(())
    }

    /// Replaces the whole database in the history (after `delete-self`):
    /// one segment holding every page of the new database, applied on top
    /// of the current head, so the current chain ends in the new state.
    ///
    /// Refused, and replication stopped, while a segment is in flight
    /// (ADR 0034, section 7): the reset's key would be that segment's.
    /// It needs no checksum guard: it chains on the acknowledged head's
    /// checksum by construction, whatever the database it replaces.
    pub(crate) async fn ship_reset(&mut self, all: Changes) -> Result<(), ReplicaError> {
        // A periodic snapshot still uploading ends first: one that landed
        // after the reset (and the erasure after it) would bring back what
        // the reset erases.
        self.settle_periodic_upload().await;
        self.finish_sidecar().await?;
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        self.settle_abandoned();
        if let Some(segment) = self.in_flight {
            return Err(self.refuse(format!("a reset while segment {segment:?} is in flight")));
        }
        let changes = Changes {
            pre_apply: self.checksum,
            ..all
        };
        self.ship(&changes).await
    }

    /// The guard of [`Self::ship_snapshot`] (ADR 0034, section 7): a
    /// snapshot is refused while a segment is in flight, and one labelled
    /// at the head (periodic, after a delete) unless it holds the database
    /// at the head: its post-apply checksum must be the acknowledged
    /// head's. Without it, a snapshot of a database that moved past the
    /// head would be labelled at the head, collection would delete what
    /// lies below it, and the next segment's pre-apply checksum would not
    /// match it: an actor that never restores again. The first snapshot
    /// is at a new position on an empty bucket, and keeps today's rule.
    ///
    /// Neither case can happen with strict writes (a periodic snapshot is
    /// taken right after the event's segment, the one after a delete
    /// right after the reset, and nothing is in flight then): a refusal
    /// is a bug, so it stops replication ([`Self::failed`]) and the actor
    /// is given up as after a failed upload (its next event restores from
    /// the bucket), rather than a history built on a wrong label. Logged
    /// as an error.
    pub(super) fn refuse_unless_at_head(
        &mut self,
        all: &Changes,
        kind: SnapshotKind,
    ) -> Result<(), ReplicaError> {
        self.settle_abandoned();
        if let Some(segment) = self.in_flight {
            return Err(self.refuse(format!("a snapshot while segment {segment:?} is in flight")));
        }
        if kind != SnapshotKind::First && all.post_apply != self.checksum {
            return Err(self.refuse(format!(
                "a snapshot of database {:x} labelled at head {:?}, whose database is {:x}",
                all.post_apply.value(),
                self.head,
                self.checksum.value()
            )));
        }
        Ok(())
    }

    /// Stops replication for `reason`, a refused snapshot or reset.
    pub(super) fn refuse(&mut self, reason: String) -> ReplicaError {
        tracing::error!(prefix = %crate::logging::text(&self.prefix), head = ?self.head, %reason, "refused: replication stops");
        self.fail(format!("refused: {reason}"));
        ReplicaError::Refused(reason)
    }

    /// See [`UploadTarget::put_once`].
    pub(super) async fn put_once(
        &self,
        key: &str,
        bytes: Vec<u8>,
        bound: std::time::Duration,
    ) -> Result<(), ReplicaError> {
        self.target().put_once(key, bytes, bound).await
    }

    pub(super) async fn record(&mut self, sum: DbChecksum) -> Result<(), ReplicaError> {
        self.checksum = sum;
        let (path, head) = (self.db_path.clone(), self.head);
        tokio::task::spawn_blocking(move || write_sidecar(&path, head, sum))
            .await
            .map_err(|e| ReplicaError::Local(e.to_string()))?
    }

    /// A segment's upload, or a first or after-delete snapshot's (`what`,
    /// for the log): a
    /// conditional create, each attempt bounded and sent again when the
    /// bucket does not answer, the whole upload within one deadline, then
    /// the acknowledgement rule: the upload counts only if it completed
    /// while the lease was still valid. A resend that finds the object (an
    /// earlier attempt landed with its reply lost, or the client's own
    /// retry did) reads it back: this upload's own bytes are acknowledged,
    /// as any landed upload is; anything else is a conflict. Nobody else
    /// writes these bytes at this key, which names this hold's generation
    /// and the transaction, and every attempt sends the same bytes (a
    /// snapshot's carry the time it was taken, so one taken again at the
    /// same position is a conflict, which its caller reads as "already
    /// there"). A refusal with nothing there yet (a bucket answering 409
    /// to a create racing an earlier attempt still in flight) is sent
    /// again.
    ///
    /// `advance`: the position this upload makes durable, which its
    /// acknowledgement moves the hold's head to; `None` for an upload at
    /// the head (ADR 0035, section 2).
    pub(super) async fn put_bounded(
        &self,
        key: &str,
        bytes: Vec<u8>,
        what: &'static str,
        advance: Option<Position>,
    ) -> Result<(), ReplicaError> {
        self.target().put_bounded(key, bytes, what, advance).await
    }

    /// What an upload needs of this replica, owned: a batched segment's
    /// upload runs on a task of its own, after the event that captured it
    /// (ADR 0034, section 2).
    pub(super) fn target(&self) -> UploadTarget {
        UploadTarget {
            store: Arc::clone(&self.store),
            prefix: self.prefix.clone(),
            lease: self.lease.clone(),
            upload_idle: self.upload_idle,
        }
    }
}
