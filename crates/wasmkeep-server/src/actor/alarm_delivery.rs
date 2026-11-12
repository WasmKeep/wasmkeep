//! Alarm events and the bucket's alarm index (ADR 0009): delivering an
//! alarm, keeping the index in line with the committed alarm (R1 to R3),
//! clearing stale entries behind a fence, and the sweep.

use super::*;

/// After a failed alarm hand-off, how soon this node tries again (ms).
const HANDOFF_RETRY_MS: u64 = 500;

impl ActorState {
    /// An alarm came due: run `on-alarm` if the committed alarm is still due.
    /// Cleared only on success (unless the handler set a new one); failures
    /// retry with exponential backoff, so delivery is at least once.
    ///
    /// `hint` is the time the alarm was registered for on this node (from an
    /// event, the startup scan, a retry or the bucket's index); the
    /// committed alarm in the database decides what runs.
    pub(super) async fn alarm(&mut self, hint: u64) {
        self.chain = Arc::from(Vec::new());
        // The event runs on the class it takes now (ADR 0013).
        if let Err(e) = self.refresh_class() {
            // Nothing to run it; it stays in the database (and the
            // bucket's index), for a deploy that brings the class back.
            tracing::info!(actor = %self.address, error = %e, "alarm: class no longer served");
            self.alarms.clear(self.address.clone());
            return;
        }
        match self.ensure_owner().await {
            Ok(()) => {}
            // Another node owns the actor: it fires the alarm (it is in the
            // owner's committed state and the bucket's index).
            Err(OwnershipError::NotOwner { owner, .. }) => {
                tracing::debug!(actor = %self.address, %owner, "alarm: not the owner");
                return;
            }
            // This node is draining (ADR 0014): the chosen peer arms the
            // alarm and fires it, from its own timer. If it does not take
            // it, this node's timer tries again soon: then another peer, or
            // with none left, this node claims.
            Err(OwnershipError::Elsewhere { peer }) => {
                let Some(cluster) = self.cluster.clone() else {
                    return;
                };
                match cluster.hand_off_alarm(&peer, &self.address, hint).await {
                    Ok(()) => {
                        tracing::debug!(actor = %self.address, %peer, hint, "alarm handed off");
                    }
                    Err(e) => {
                        // Refused or failed alike: skip this peer for a
                        // while (the rendezvous choice would pick it again).
                        tracing::info!(actor = %self.address, %peer, error = %e, "alarm hand-off failed; retrying");
                        cluster.suspect(&peer);
                        self.alarms.set_if_earlier(
                            self.address.clone(),
                            now_ms().saturating_add(HANDOFF_RETRY_MS),
                        );
                    }
                }
                return;
            }
            // The bucket is unreachable, or this node may not run the actor
            // now and no peer can (ADR 0016): try again after a lease period.
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => {
                tracing::debug!(actor = %self.address, error = %e, "alarm: bucket unavailable");
                let wait = self
                    .cluster
                    .as_ref()
                    .map_or(Duration::from_secs(10), |c| c.ttl);
                let at =
                    now_ms().saturating_add(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX));
                self.alarms.set_if_earlier(self.address.clone(), at);
                return;
            }
        }
        // This node owns it, and a required value is missing (ADR 0016):
        // not run now, kept for when
        // it is set (tried again at the longest backoff).
        if self.class.missing_required(self.address.class()).is_some() {
            let wait = u64::try_from(self.limits.retry_max.as_millis()).unwrap_or(u64::MAX);
            self.alarms
                .set_if_earlier(self.address.clone(), now_ms().saturating_add(wait));
            return;
        }
        // An entry left for an actor that was deleted since (or never
        // committed anything) has nothing to deliver: running the actor for
        // it would only create it again. Its entries at the hint go instead.
        if let Some(indexed) = self.without_state().await {
            tracing::debug!(actor = %self.address, hint, "alarm: the actor has no state; removing its entries");
            self.alarms.clear(self.address.clone());
            if !self.clear_stale_hint(Some(hint), Some(indexed)).await {
                // Not removable yet (the bucket has nothing for the actor:
                // an earlier owner's first upload may still land and name
                // them). Come back after the longest backoff instead of on
                // every sweep: a registered time within it keeps priority
                // over the sweep's delivery of the same entries. Fixed
                // rather than doubling, since the actor's mailbox (and its
                // failure count) does not outlive its idle timeout.
                let wait = u64::try_from(self.limits.retry_max.as_millis()).unwrap_or(u64::MAX);
                self.alarms
                    .set(self.address.clone(), now_ms().saturating_add(wait));
                // And let the actor go at once: holding its lease until the
                // idle timeout would route client requests here, and the
                // first would create the actor empty, while the node that
                // holds its data may be about to upload it.
                self.replica = None;
                self.db = None;
                self.lease = None; // releases
            }
            return;
        }
        if let Err(e) = self.ensure_ready().await {
            tracing::warn!(actor = %self.address, error = %e, "alarm: actor not ready");
            self.retry_alarm().await;
            return;
        }
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let at = match with_db(&mut live.store, |db| db.alarm()).await {
            Ok(Ok(at)) => at,
            _ => {
                // `ensure_ready` may have run `on-init`: finish the event so
                // its writes are replicated like any event's.
                self.finish(false, false).await;
                self.retry_alarm().await;
                return;
            }
        };
        // An index entry that no longer matches the committed alarm (it was
        // cleared or moved since) is stale: removed below, once the
        // committed alarm is indexed.
        let stale_hint = (at != Some(hint)).then_some(hint);
        match at {
            // Nothing to run, but `ensure_ready` may have run `on-init`,
            // whose writes must be replicated like any event's.
            None => {
                self.alarms.clear(self.address.clone());
                let named = self.index_and_replicate(None).await;
                self.clear_stale_hint(stale_hint, named).await;
                return;
            }
            Some(at) if at > now_ms() => {
                self.alarms.set(self.address.clone(), at);
                let named = self.index_and_replicate(Some(at)).await;
                self.clear_stale_hint(stale_hint, named).await;
                return;
            }
            Some(_) => {}
        }
        let at = at.unwrap_or_default();

        self.install_call_context();
        let Some(live) = self.live.as_mut() else {
            return;
        };
        // Read before the handler runs: a handler that sets an alarm resets
        // the count, and that must not cancel the backoff of its own failure.
        let failures = with_db(&mut live.store, |db| db.meta(ALARM_FAILURES_KEY))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        live.start_event("alarm");
        let run = limit(
            self.limits.wall,
            self.lease.as_ref(),
            live.core.call_on_alarm(&mut live.store),
        )
        .await;
        let trapped = !matches!(run, Ok(Ok(_)));
        // A handler that returned an error failed too (it is retried).
        live.record_event(self.address.class(), !matches!(run, Ok(Ok(Ok(())))));
        match run {
            Ok(Ok(Ok(()))) => {
                let cleared = with_db(&mut live.store, move |db| {
                    db.end_event()?;
                    // A handler that scheduled a new alarm keeps it.
                    if db.alarm()? == Some(at) {
                        db.clear_alarm()?;
                    }
                    db.set_meta(ALARM_FAILURES_KEY, "0")
                })
                .await;
                let named = if matches!(cleared, Ok(Ok(()))) {
                    self.alarm_failures = 0;
                    self.finish(false, true).await
                } else {
                    let named = self.finish(false, false).await;
                    self.retry_alarm().await;
                    named
                };
                self.clear_stale_hint(stale_hint, named).await;
            }
            other => {
                let reason = match other {
                    Ok(Ok(Err(msg))) => format!("on-alarm returned an error: {msg}"),
                    Ok(Err(trap)) => format!("on-alarm trapped: {trap:#}"),
                    Err(cut) => format!("on-alarm {cut}"),
                    Ok(Ok(Ok(()))) => unreachable!("handled above"),
                };
                tracing::warn!(actor = %self.address, %reason, "alarm will be retried");
                // The retry is the committed alarm, moved later with backoff:
                // it survives hibernation and moves, and the index (which
                // mirrors the committed alarm) lists the retry time, so no
                // sweep delivers it before then.
                let retry = retry_at(&self.limits, failures);
                let moved = with_db(&mut live.store, move |db| {
                    db.end_event()?;
                    // Whatever the handler left due before the retry time
                    // (the same alarm, or one it set for now) waits until
                    // then; one it set later, or a clear, stands. The count
                    // is the one from before the handler ran.
                    match db.alarm()? {
                        Some(next) if next <= retry => {
                            db.set_alarm(retry)?;
                            db.set_meta(ALARM_FAILURES_KEY, &(failures + 1).to_string())
                        }
                        _ => Ok(()),
                    }
                })
                .await;
                let named = if matches!(moved, Ok(Ok(()))) {
                    self.finish(trapped, true).await
                } else {
                    let named = self.finish(trapped, false).await;
                    self.retry_alarm().await;
                    named
                };
                self.clear_stale_hint(stale_hint, named).await;
            }
        }
    }

    /// Whether this node may write the actor's alarm index entries: it
    /// holds a valid lease and replication is healthy (so everything the
    /// actor's database records is in the bucket). Returns the lease
    /// generation entries are written and checked under, read from the
    /// replica of this hold.
    pub(super) fn may_index(
        &self,
    ) -> impl std::future::Future<Output = Option<(Arc<Cluster>, u64)>> + Send + 'static {
        let parts = (|| {
            let (cluster, lease) = (self.cluster.as_ref()?, self.lease.as_ref()?);
            let held = lease.generation()?;
            if !lease.is_valid() {
                return None;
            }
            Some((
                Arc::clone(cluster),
                Arc::clone(self.replica.as_ref()?),
                held,
            ))
        })();
        async move {
            let (cluster, replica, held) = parts?;
            let replica = replica.lock().await;
            if replica.failed().is_some() {
                return None;
            }
            debug_assert_eq!(replica.generation(), held, "a replica of another hold");
            Some((cluster, replica.generation()))
        }
    }

    /// In a cluster, before an alarm event when no instance is loaded:
    /// whether the actor has no state an alarm could belong to, read from
    /// its (restored) database without instantiating it. That is a
    /// database with no committed alarm and no `on-init` recorded: deleted
    /// since (the bucket holds the empty database), or never initialised.
    /// An `on-init` that failed after setting an alarm leaves that alarm
    /// committed, so it still runs (and `on-init` is retried). Returns the
    /// index entry the database names (`Some(None)`: none, or no database
    /// at all), or `None` if the actor has state or it could not be read.
    /// The database stays open for the event that follows.
    pub(super) async fn without_state(&mut self) -> Option<Option<Entry>> {
        if self.cluster.is_none() || self.live.is_some() {
            return None;
        }
        let db = match self.db.take() {
            Some(db) => db,
            None => {
                let storage = self.storage.clone();
                let a = self.address.clone();
                let residency = self.residency.clone();
                let opened = tokio::task::spawn_blocking(move || {
                    // Restored from the bucket by `ensure_owner`: no file,
                    // nothing in the bucket either.
                    if !storage.db_path(a.class(), a.name()).exists() {
                        return Ok(None);
                    }
                    storage
                        .open(a.class(), a.name())
                        .map(|db| Some(residency.retain(db)))
                })
                .await;
                match opened {
                    Ok(Ok(Some(db))) => db,
                    Ok(Ok(None)) => return Some(None),
                    _ => return None,
                }
            }
        };
        let (db, read) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            let read = (|| {
                Ok::<_, wasmkeep_storage::SqlError>((
                    db.meta(INIT_VERSION_KEY)?,
                    db.alarm()?,
                    db.meta(ALARM_INDEXED_KEY)?,
                ))
            })();
            (db, read)
        })
        .await
        .ok()?;
        self.db = Some(db);
        match read {
            Ok((None, None, indexed)) => Some(indexed.as_deref().and_then(alarm_index::parse_meta)),
            _ => None,
        }
    }

    /// The index entry the actor's database names (`ALARM_INDEXED_KEY`):
    /// `Some(None)` for none, `None` if it could not be read.
    pub(super) async fn indexed_entry(&mut self) -> Option<Option<Entry>> {
        let live = self.live.as_mut()?;
        let meta = with_db(&mut live.store, |db| db.meta(ALARM_INDEXED_KEY))
            .await
            .ok()?
            .ok()?;
        Some(meta.as_deref().and_then(alarm_index::parse_meta))
    }

    /// Lists `at` in the bucket's alarm index under a fresh key, so other
    /// nodes can fire it if this one dies, and records that entry in the
    /// database (`ALARM_INDEXED_KEY`, replicated with the event). Called
    /// *before* the event is replicated: a crash in between leaves an entry
    /// for an alarm the bucket does not have, which costs one wake-up and
    /// is then removed. Nothing is written while the time is unchanged
    /// (unless the entry is a legacy one, replaced to migrate it).
    ///
    /// `None` without a cluster (or replica), or when what the database
    /// names is unknown. The replica is failed only when the event would
    /// otherwise replicate an alarm no entry lists (the recorded entry is
    /// unreadable, the lease lapsed or the PUT failed, for a new time): the
    /// event is then not replicated (that alarm would be lost with this
    /// node) and the actor is restored from the bucket on its next event.
    /// When the event clears the alarm (or deletes the actor), or only
    /// re-tags a legacy entry already listing the time, nothing is at risk:
    /// a failure there leaves an extra entry, which costs one wake-up.
    pub(super) async fn publish_alarm(&mut self, at: Option<u64>) -> Option<Published> {
        if self.cluster.is_none() || self.replica.is_none() {
            return None;
        }
        let Some(previous) = self.indexed_entry().await else {
            return match at {
                Some(_) => self.index_missed("the recorded entry is unreadable").await,
                None => None,
            };
        };
        if !alarm_index::needs_publish(previous.as_ref(), at) {
            return Some(Published {
                named: previous,
                replaced: None,
            });
        }
        // Whether `previous` already lists the alarm (a legacy entry being
        // re-tagged): failing to replace it loses nothing.
        let listed = at.is_none() || previous.as_ref().is_some_and(|p| Some(p.at) == at);
        let kept = Some(Published {
            named: previous.clone(),
            replaced: None,
        });
        let Some((cluster, generation)) = self.may_index().await else {
            if listed {
                return kept;
            }
            return self
                .index_missed("the lease is no longer valid, or replication stopped")
                .await;
        };
        let entry = match at {
            Some(at) => match cluster
                .put_new_alarm_entry(&self.address, at, generation)
                .await
            {
                Ok(entry) => Some(entry),
                Err(_) if listed => return kept,
                Err(e) => return self.index_missed(&e.to_string()).await,
            },
            None => None,
        };
        let value = alarm_index::to_meta(entry.as_ref());
        let recorded = match self.live.as_mut() {
            Some(live) => {
                with_db(&mut live.store, move |db| {
                    db.set_meta(ALARM_INDEXED_KEY, &value)
                })
                .await
            }
            None => return kept,
        };
        if !matches!(recorded, Ok(Ok(()))) {
            // The database still names `previous`; the new entry (if any)
            // lists the alarm but is named by nothing, so it is removed
            // when it comes due.
            return kept;
        }
        Some(Published {
            named: entry,
            replaced: previous,
        })
    }

    /// Before the actor's first upload (the bucket had nothing for it):
    /// lists the alarm its database already holds (set before this node
    /// joined a cluster) under a fresh key, and records that entry in the
    /// database, so the snapshot that carries the alarm into the bucket
    /// names an entry that exists (ADR 0009). Without this, a node cut off
    /// before the first event ends would leave an alarm in the bucket that
    /// no index entry lists, and no other node would ever fire it.
    ///
    /// An `Err` means nothing may be uploaded: the alarm then stays on
    /// this node only, as before it joined. A crash after the PUT and
    /// before the upload leaves an entry the bucket's state does not name;
    /// it is never removed while the bucket has nothing for the actor (see
    /// `fence`), and is resolved once that node uploads.
    pub(super) async fn index_first_alarm(&mut self, db: ResidentDb) -> Result<ResidentDb, String> {
        if self.cluster.is_none() {
            return Ok(db);
        }
        let (db, read) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            let read = db.alarm();
            (db, read)
        })
        .await
        .map_err(|e| e.to_string())?;
        let Some(at) = read.map_err(|e| format!("reading the alarm: {e}"))? else {
            return Ok(db);
        };
        let Some((cluster, generation)) = self.may_index().await else {
            return Err("alarm index: the lease is no longer valid, or replication stopped".into());
        };
        let entry = cluster
            .put_new_alarm_entry(&self.address, at, generation)
            .await
            .map_err(|e| format!("alarm index: {e}"))?;
        let value = alarm_index::to_meta(Some(&entry));
        let (db, recorded) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            let recorded = db.set_meta(ALARM_INDEXED_KEY, &value);
            (db, recorded)
        })
        .await
        .map_err(|e| e.to_string())?;
        recorded.map_err(|e| format!("alarm index: recording the entry: {e}"))?;
        Ok(db)
    }

    /// The alarm index could not follow the event: fails the replica, so
    /// the event is not replicated (`replicate` gives the actor up).
    pub(super) fn index_missed(
        &self,
        reason: &str,
    ) -> impl std::future::Future<Output = Option<Published>> + Send + 'static {
        let (replica, reason) = (self.replica.clone(), reason.to_owned());
        async move {
            if let Some(replica) = replica {
                replica
                    .lock()
                    .await
                    .fail(format!("alarm index not updated: {reason}"));
            }
            None
        }
    }

    /// `publish_alarm`, then the event replicated, then the entry it
    /// replaced removed. Returns the entry the replicated state names
    /// (`Some(None)`: none), if known.
    pub(super) async fn index_and_replicate(&mut self, at: Option<u64>) -> Option<Option<Entry>> {
        let published = self.publish_alarm(at).await;
        self.replicate().await;
        let (named, replaced) = match published {
            Some(Published { named, replaced }) => (Some(named), replaced),
            None => (None, None),
        };
        self.retire_alarm_entry(replaced).await;
        named
    }

    /// Removes `stale`, the entry the event replaced, once the event is
    /// replicated: the database no longer names it (R2). Never one a newer
    /// owner wrote (R3; the entry came from this actor's own state, so that
    /// would be a bug).
    pub(super) async fn retire_alarm_entry(&mut self, stale: Option<Entry>) {
        let Some(stale) = stale else {
            return;
        };
        let Some((cluster, generation)) = self.may_index().await else {
            return;
        };
        if !alarm_index::may_delete(&stale, generation) {
            tracing::warn!(actor = %self.address, ?stale, generation, "not removing an alarm entry from a newer owner");
            return;
        }
        if let Err(e) = cluster.delete_alarm_entry(&self.address, &stale).await {
            tracing::debug!(actor = %self.address, error = %e, "removing a stale alarm entry failed");
        }
    }

    /// After an alarm event delivered for `hint` that no longer matches the
    /// committed alarm (cleared or moved since), with that event
    /// replicated: removes the actor's entries due at `hint`, except the
    /// one its database names (R2) and any a newer owner wrote (R3). Older
    /// owners' entries there are named by no state: this owner restored
    /// the latest, and keys are never reused.
    ///
    /// `indexed` is the entry the replicated state names (`Some(None)`:
    /// none); unknown (`None`), nothing is known to be unnamed, so nothing
    /// goes.
    ///
    /// Returns `false` when the fence did not pass (nothing was removed):
    /// the bucket has nothing for the actor and this owner uploaded
    /// nothing (an upload still to land may name the entries), or the
    /// fence's own upload failed. `true` otherwise, including when there
    /// was nothing to clear or a removal failed (an entry left over costs
    /// one wake-up).
    pub(super) async fn clear_stale_hint(
        &mut self,
        hint: Option<u64>,
        indexed: Option<Option<Entry>>,
    ) -> bool {
        let (Some(hint), Some(indexed)) = (hint, indexed) else {
            return true;
        };
        // R2 relies on the replicated state being this owner's; until its
        // own upload ends the history, an earlier owner's late upload can
        // still become it (a late commit) and name an entry removed here.
        if !self.fence().await {
            tracing::debug!(actor = %self.address, hint, "not clearing stale alarm entries: no upload of this owner yet");
            return false;
        }
        let Some((cluster, generation)) = self.may_index().await else {
            return true;
        };
        let listed = match cluster.alarm_entries_at(&self.address, hint).await {
            Ok(listed) => listed,
            Err(e) => {
                tracing::debug!(actor = %self.address, error = %e, "listing stale alarm entries failed");
                return true;
            }
        };
        for stale in alarm_index::entries_to_clear(hint, &listed, indexed.as_ref(), generation) {
            if let Err(e) = cluster.delete_alarm_entry(&self.address, &stale).await {
                tracing::debug!(actor = %self.address, error = %e, "removing a stale alarm entry failed");
            }
        }
        true
    }

    /// Makes sure the bucket's history ends in an upload of this owner's
    /// generation, uploading a one-line change (`OWNER_FENCE_KEY`) if this
    /// owner has uploaded nothing yet. Until then, an earlier owner's
    /// upload that lands late, after this owner restored, is on no chain
    /// anyone built on, so it becomes the actor's state (a late commit,
    /// ADR 0007): what this owner restored is not yet the state. Returns
    /// whether the history now ends in this generation (`false`:
    /// replication stopped, or the fence could not be written).
    pub(super) async fn fence(&mut self) -> bool {
        let Some(slot) = self.replica.clone() else {
            return false;
        };
        let generation = {
            let replica = slot.lock().await;
            if replica.failed().is_some() {
                return false;
            }
            if replica.head_generation() == replica.generation() {
                return true;
            }
            // The bucket had nothing for this actor when it was restored,
            // and this owner has uploaded nothing. An earlier owner lists
            // an alarm its data already held before its first snapshot
            // (`index_first_alarm`), so that snapshot, landing late, can
            // name an entry: nothing may be removed as stale. Uploading
            // here instead is not an option either: it would create the
            // actor in the bucket, empty, and the earlier owner's data
            // (single-node data it never managed to upload) would be
            // replaced by it when that node restores the actor.
            if replica.needs_snapshot() {
                return false;
            }
            replica.generation()
        };
        let value = generation.to_string();
        if let Some(live) = self.live.as_mut() {
            let wrote = with_db(&mut live.store, move |db| {
                db.set_meta(OWNER_FENCE_KEY, &value)
            })
            .await;
            if !matches!(wrote, Ok(Ok(()))) {
                return false;
            }
            self.replicate().await;
        } else if let Some(db) = self.db.take() {
            // No instance (the deleted-actor guard): capture, write and
            // ship from the database itself, as `prepare` and an event do.
            let done = tokio::task::spawn_blocking(move || {
                let mut db = db;
                let changes = (|| {
                    if db.captured_checksum().is_none() {
                        db.start_capture().map_err(|e| e.to_string())?;
                    }
                    db.set_meta(OWNER_FENCE_KEY, &value)
                        .map_err(|e| e.to_string())?;
                    db.take_changes().map_err(|e| e.to_string())
                })();
                (db, changes)
            })
            .await;
            let Ok((db, changes)) = done else {
                return false;
            };
            self.db = Some(db);
            let shipped = match changes {
                Ok(Some(changes)) => slot
                    .lock()
                    .await
                    .ship(&changes)
                    .await
                    .map_err(|e| e.to_string()),
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = shipped {
                self.abandon(&format!("fencing before clearing alarm entries: {e}"));
                return false;
            }
        } else {
            return false;
        }
        let Some(slot) = self.replica.clone() else {
            return false;
        };
        let replica = slot.lock().await;
        replica.failed().is_none() && replica.head_generation() == replica.generation()
    }

    /// Schedules the alarm again on this node with exponential backoff,
    /// when the database could not be used (the actor is not ready). A
    /// failing handler's retries are in the database instead (`alarm`).
    pub(super) async fn retry_alarm(&mut self) {
        let at = retry_at(&self.limits, self.alarm_failures);
        self.alarm_failures = self.alarm_failures.saturating_add(1);
        self.alarms.set(self.address.clone(), at);
    }
}

/// When to retry an alarm that failed `failures` times before (0: the
/// first failure): `retry_initial`, doubling up to `retry_max`.
pub(super) fn retry_at(limits: &EventLimits, failures: u32) -> u64 {
    let factor = 2u32.saturating_pow(failures.min(20));
    let delay = limits
        .retry_initial
        .saturating_mul(factor)
        .min(limits.retry_max);
    now_ms().saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
}

/// Most alarms one sweep delivers on a node, and most index entries it
/// reads; the next sweep resumes where this one stopped, so entries no node
/// can act on (a class nobody serves, an actor that cannot start) cannot
/// hold the rest back.
pub(super) const SWEEP_DELIVER: usize = 256;

pub(super) const SWEEP_SCAN: usize = 2_048;

/// Every `alarm_sweep`, reads the bucket's alarm index up to "now minus a
/// lease period plus grace" and delivers what it finds: by then the owner
/// had fired those alarms (or moved them, retries included), or it is gone
/// and the actor's lease is free. The live nodes share the work by a hash
/// of the address; an entry overdue three times longer is anyone's (the
/// nodes may disagree about who is alive). A node delivers its own share
/// first and anyone's entries with what the cap leaves
/// ([`alarm_index::plan_sweep`]), so in a long backlog each live node
/// delivers a different share instead of all the same entries.
/// Anyone's entries the walk meets keep an eighth of the cap, and entries
/// this node already has in flight or retrying do not count
/// ([`AlarmHandle::pending`]). Delivering one runs the usual
/// alarm event, which takes the actor (restoring it from the bucket) and
/// checks its committed alarm. A time this node already has registered
/// earlier, or within the longest backoff, keeps priority.
pub(super) async fn sweep_alarms(
    service: std::sync::Weak<ServiceInner>,
    cluster: Arc<Cluster>,
    alarms: AlarmHandle,
    retry_max: Duration,
) {
    let overdue = u64::try_from(cluster.alarm_overdue.as_millis()).unwrap_or(u64::MAX);
    let mut cursor: Option<object_store::path::Path> = None;
    loop {
        tokio::time::sleep(cluster.alarm_sweep).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return; // shutting down: other nodes take over
        }
        let now = now_ms();
        let (before, anyones) = (
            now.saturating_sub(overdue),
            now.saturating_sub(overdue.saturating_mul(3)),
        );
        match cluster
            .alarms_due_before(before, cursor.as_ref(), SWEEP_SCAN)
            .await
        {
            Ok((due, next)) => {
                // Entries this node would deliver for nothing (in flight,
                // backing off, registered soon) do not count against the
                // cap: actors that cannot start stay in the index.
                let pending = alarms
                    .pending(
                        due.iter()
                            .map(|(address, at, _)| (address.clone(), *at))
                            .collect(),
                        retry_max,
                    )
                    .await;
                // One view of the live nodes and classes for the whole page.
                let shards = cluster.alarm_shards();
                let claims: Vec<SweepClaim> = {
                    let classes = read(&inner.classes);
                    due.iter()
                        .zip(&pending)
                        .map(|((address, at, _), pending)| {
                            if *pending || !classes.contains_key(address.class()) {
                                SweepClaim::Skip
                            } else if shards.is_mine(address) {
                                SweepClaim::Mine
                            } else if *at < anyones {
                                SweepClaim::Anyones
                            } else {
                                SweepClaim::Skip
                            }
                        })
                        .collect()
                };
                // A fresh rotation every sweep: which anyone's entries get
                // their share changes from pass to pass and node to node.
                let rotation =
                    std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), now);
                let plan = alarm_index::plan_sweep(&claims, SWEEP_DELIVER, rotation);
                // Listings resume strictly after the cursor: if the walk
                // stopped short of the page, resume after its last entry.
                if plan.walked < due.len() {
                    if let Some(last) = plan.walked.checked_sub(1) {
                        cursor = due.get(last).map(|(_, _, key)| key.clone());
                    }
                } else {
                    cursor = next;
                }
                for i in plan.deliver {
                    let Some((address, at, _)) = due.get(i) else {
                        continue;
                    };
                    tracing::debug!(actor = %address, at, "alarm sweep: delivering an overdue alarm");
                    alarms.sweep(address.clone(), *at, retry_max);
                }
            }
            Err(e) => {
                cursor = None;
                tracing::warn!(error = %e, "alarm sweep: listing the index failed");
            }
        }
    }
}
