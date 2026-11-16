//! The host side of the `wasmkeep:actor` world: what a guest can call
//! (storage, alarms, calls, env, WebSockets), and running database work
//! within an event's limits.

use super::*;

/// Why a guest call was cut short.
#[derive(Debug, Clone, Copy)]
pub(super) enum Cut {
    /// It ran past the per-event wall-clock limit.
    Wall(Duration),
    /// This node's lease on the actor ran out while it ran: another node
    /// may own the actor now, so this one must stop.
    Lease,
    /// An asynchronous batched upload failed during this event.
    Replica,
    /// A clean stop reached its deadline during a batched hold's event
    /// (ADR 0034, section 10).
    Stop,
}

impl std::fmt::Display for Cut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wall(d) => write!(f, "exceeded the {d:?} wall-clock limit"),
            Self::Replica => f.write_str("stopped: replication stopped"),
            Self::Stop => f.write_str("stopped: the node's clean stop reached its deadline"),
            Self::Lease => f.write_str("stopped: this node's lease on the actor ran out"),
        }
    }
}

/// Runs a guest call under the wall-clock limit and, in a cluster, only
/// while this node holds the actor's lease. Dropping the call stops the
/// guest; callers treat a cut like a trap and discard the instance.
pub(super) async fn limit<F: std::future::Future>(
    wall: Duration,
    held: Option<&Held>,
    f: F,
) -> Result<F::Output, Cut> {
    let expiry = held.map(|h| h.lease().expiry());
    let batched = held.filter(|h| h.writes() == crate::apps::Writes::Batched);
    let flag = match batched {
        Some(h) => match h.replica() {
            Some(r) => Some(r.lock().await.failure_flag()),
            None => None,
        },
        None => None,
    };
    // A batched hold's event ends at a clean stop's deadline, so the stop
    // counts what it drops before the node lets its actors go; a strict
    // one runs on as before (ADR 0034, section 10).
    let stop = batched.map(|h| h.lease().stop_reached());
    let stopping = async move {
        match stop {
            Some(s) => s.await,
            None => std::future::pending().await,
        }
    };
    let cut = |reason: &str| {
        crate::metrics::inc(&crate::metrics::METRICS.cuts.get(&[("reason", reason)]));
    };
    let f = async move {
        tokio::select! {
            r = f => Ok(r),
            () = stopping => {
                cut("stop");
                Err(Cut::Stop)
            }
        }
    };
    let lease_gone = async move {
        match expiry {
            Some(e) => e.await,
            None => std::future::pending().await,
        }
    };
    limit_with_failure(wall, lease_gone, flag, f).await?
}

async fn limit_with_failure<F: std::future::Future>(
    wall: Duration,
    lease_gone: impl std::future::Future<Output = ()>,
    flag: Option<tokio::sync::watch::Receiver<Option<crate::replica::Stop>>>,
    f: F,
) -> Result<F::Output, Cut> {
    let replica_stopped = async move {
        if let Some(mut flag) = flag
            && flag
                .wait_for(|s| s.as_ref().is_some_and(|s| s.batched))
                .await
                .is_ok()
        {
            return;
        }
        std::future::pending::<()>().await;
    };
    let cut = |reason: &str| {
        crate::metrics::inc(&crate::metrics::METRICS.cuts.get(&[("reason", reason)]));
    };
    tokio::select! {
        () = replica_stopped => {
            cut("replica");
            Err(Cut::Replica)
        }
        r = tokio::time::timeout(wall, f) => r.map_err(|_| {
            cut("wall");
            Cut::Wall(wall)
        }),
        () = lease_gone => {
            cut("lease");
            Err(Cut::Lease)
        }
    }
}

/// Runs `f` on the store's database on a blocking thread.
/// Runs `f` on the actor's database on this thread: only for host
/// bookkeeping known to be quick (a meta row, the alarm row, ending a
/// transaction). Guest SQL, captures and snapshots go through [`with_db`].
pub(super) fn with_db_inline<T>(
    store: &mut Store<ActorHost>,
    f: impl FnOnce(&mut ActorDb) -> T,
) -> wasmtime::Result<T> {
    let db = store
        .data_mut()
        .db
        .as_mut()
        .ok_or_else(|| wasmtime::format_err!("actor database unavailable"))?;
    Ok(f(db))
}

pub(super) async fn with_db<T: Send + 'static>(
    store: &mut Store<ActorHost>,
    f: impl FnOnce(&mut ActorDb) -> T + Send + 'static,
) -> wasmtime::Result<T> {
    store.data_mut().with_db(f).await
}

type ResponseResult =
    Result<Response<HyperOutgoingBody>, wasmtime_wasi_http::p2::bindings::http::types::ErrorCode>;

impl ActorHost {
    pub(super) fn response_outparam(
        &mut self,
        sender: oneshot::Sender<ResponseResult>,
    ) -> wasmtime::Result<
        wasmtime::component::Resource<wasmtime_wasi_http::p2::types::HostResponseOutparam>,
    > {
        let stop = self.replica_stop.clone();
        self.http()
            .new_response_outparam_from_callback(move |response| {
                let response = if crate::replica::stopped(stop.as_ref()) {
                    Err(
                        wasmtime_wasi_http::p2::bindings::http::types::ErrorCode::InternalError(
                            Some("replication stopped".into()),
                        ),
                    )
                } else {
                    response
                };
                let _ = sender.send(response);
            })
    }

    fn replication_stopped(&self) -> bool {
        crate::replica::stopped(self.replica_stop.as_ref())
    }

    fn check_replication(&self) -> wasmtime::Result<()> {
        if self.replication_stopped() {
            wasmtime::bail!("replication stopped");
        }
        Ok(())
    }

    fn stopped_sql(&self) -> Option<SqlError> {
        self.replication_stopped().then(|| SqlError {
            kind: SqlErrorKind::Internal,
            message: REPLICATION_STOPPED.into(),
        })
    }

    /// `exec-durable` without the timing (see the `storage::Host` impl).
    pub(super) async fn exec_durable_untimed(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        let params = params.into_iter().map(to_db).collect::<Vec<_>>();
        let Some(slot) = self.replica.clone() else {
            let r = self
                .with_db(move |db| db.exec_durable(&sql, &params))
                .await?;
            return Ok(r.map_err(from_db_error));
        };
        // A batched hold: `exec` and then `flush` (ADR 0034, section 4).
        if let Some(slot) = self.batched_slot() {
            let changed = self
                .with_db(move |db| db.exec_commit(&sql, &params))
                .await?;
            let changed = match changed {
                Ok(n) => n,
                Err(e) => return Ok(Err(from_db_error(e))),
            };
            return Ok(self.flush_batched(slot).await.map(|()| changed));
        }
        // In a cluster, durable means uploaded while the lease was valid:
        // the commit is not fsynced locally as well. A node trusts a local
        // file again only if it is exactly the bucket's head
        // (`Replica::restore`), so nothing rests on what the disk kept.
        let mut replica = slot.lock().await;
        if let Some(reason) = replica.failed() {
            // The local database may be ahead of the bucket: run nothing
            // more until the actor is restored.
            return Ok(Err(SqlError {
                kind: SqlErrorKind::Internal,
                message: format!("replication stopped ({reason}); retry"),
            }));
        }
        let outcome = self
            .with_db(move |db| {
                // Inside a transaction this runs only a COMMIT (refusing
                // anything else before it runs): nothing in a transaction is
                // durable before then. Either way no transaction is open
                // past this point, so what follows uploads committed state.
                let changed = db.exec_commit(&sql, &params).map_err(from_db_error)?;
                debug_assert!(!db.in_transaction());
                let alarm = db.alarm().map_err(from_db_error)?;
                let indexed = db
                    .meta(ALARM_INDEXED_KEY)
                    .map_err(from_db_error)?
                    .as_deref()
                    .and_then(alarm_index::parse_meta);
                Ok((changed, alarm, indexed))
            })
            .await?;
        let (changed, alarm, indexed) = match outcome {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        // The upload carries every change of the event so far, a pending
        // alarm included: list it in the bucket's index first (as `finish`
        // does), under a fresh key and this replica's generation (held
        // under its lock), and record that with the same upload.
        let mut retired = None;
        if let Some((cluster, address)) = self.alarm_index.clone()
            && alarm_index::needs_publish(indexed.as_ref(), alarm)
        {
            let generation = replica.generation();
            let entry = match alarm {
                Some(at) => match cluster.put_new_alarm_entry(&address, at, generation).await {
                    Ok(entry) => Some(entry),
                    Err(e) => {
                        replica.fail(format!("alarm index: {e}"));
                        return Ok(Err(SqlError {
                            kind: SqlErrorKind::Internal,
                            message: format!("committed locally but not durable: alarm index: {e}"),
                        }));
                    }
                },
                None => None,
            };
            let value = alarm_index::to_meta(entry.as_ref());
            self.with_db(move |db| db.set_meta(ALARM_INDEXED_KEY, &value))
                .await?
                .map_err(|e| wasmtime::format_err!("alarm index: {e}"))?;
            retired = indexed.map(|p| (cluster, address, p, generation));
        }
        let captured = self.with_db(|db| db.take_changes()).await?;
        let shipped = match captured {
            Ok(None) => Ok(()),
            Ok(Some(changes)) => replica.ship(&changes).await,
            Err(e) => {
                replica.fail(format!("capture: {e}"));
                Err(ReplicaError::Local(e.to_string()))
            }
        };
        if let Err(e) = shipped {
            return Ok(Err(SqlError {
                kind: SqlErrorKind::Internal,
                message: format!("committed locally but not durable: {e}"),
            }));
        }
        // The entry the upload replaced, now that the bucket has the change
        // (R2), unless a newer owner wrote it (R3).
        if let Some((cluster, address, previous, generation)) = retired
            && alarm_index::may_delete(&previous, generation)
            && let Err(e) = cluster.delete_alarm_entry(&address, &previous).await
        {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = %e, "removing a stale alarm entry failed");
        }
        Ok(Ok(changed))
    }

    /// `storage.flush` (ADR 0034, section 4) for a strict class and a
    /// single node. A strict class uploads every event's writes before the
    /// next event starts, so what is not yet durable is this event's writes
    /// so far: shipped here in one upload, as `exec-durable` ships them
    /// after its commit (a pending alarm listed in the index first), or
    /// nothing at all when there are none. On a single node the WAL is
    /// fsynced, plain commits included. Refused inside an open transaction.
    pub(super) async fn flush_untimed(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        let Some(slot) = self.replica.clone() else {
            let r = self.with_db(ActorDb::sync_committed).await?;
            return Ok(r.map_err(from_db_error));
        };
        if let Some(slot) = self.batched_slot() {
            return Ok(self.flush_batched(slot).await);
        }
        let mut replica = slot.lock().await;
        let outcome = self
            .with_db(|db| {
                if let Err(e) = db.flushable() {
                    return Ok(Err(from_db_error(e)));
                }
                let alarm = db.alarm().map_err(from_db_error)?;
                let indexed = db
                    .meta(ALARM_INDEXED_KEY)
                    .map_err(from_db_error)?
                    .as_deref()
                    .and_then(alarm_index::parse_meta);
                Ok(Ok((alarm, indexed)))
            })
            .await?;
        let (alarm, indexed) = match outcome {
            Ok(Ok(v)) => v,
            Ok(Err(refused)) | Err(refused) => return Ok(Err(refused)),
        };
        if let Some(reason) = replica.failed() {
            // The local database may be ahead of the bucket: nothing is
            // shipped until the actor is restored.
            return Ok(Err(SqlError {
                kind: SqlErrorKind::Internal,
                message: format!("{REPLICATION_STOPPED} ({reason})"),
            }));
        }
        let stopped = |e: &dyn std::fmt::Display| SqlError {
            kind: SqlErrorKind::Internal,
            message: format!("{REPLICATION_STOPPED}: {e}"),
        };
        // As `exec-durable`: a pending alarm is listed in the bucket's
        // index before the upload that carries it, under a fresh key and
        // this replica's generation, and the upload records that.
        let mut retired = None;
        if let Some((cluster, address)) = self.alarm_index.clone()
            && alarm_index::needs_publish(indexed.as_ref(), alarm)
        {
            let generation = replica.generation();
            let entry = match alarm {
                Some(at) => match cluster.put_new_alarm_entry(&address, at, generation).await {
                    Ok(entry) => Some(entry),
                    Err(e) => {
                        replica.fail(format!("alarm index: {e}"));
                        return Ok(Err(stopped(&format_args!("alarm index: {e}"))));
                    }
                },
                None => None,
            };
            let value = alarm_index::to_meta(entry.as_ref());
            self.with_db(move |db| db.set_meta(ALARM_INDEXED_KEY, &value))
                .await?
                .map_err(|e| wasmtime::format_err!("alarm index: {e}"))?;
            retired = indexed.map(|p| (cluster, address, p, generation));
        }
        let captured = self.with_db(|db| db.take_changes()).await?;
        let shipped = match captured {
            Ok(None) => Ok(()),
            Ok(Some(changes)) => replica.ship(&changes).await,
            Err(e) => {
                replica.fail(format!("capture: {e}"));
                Err(ReplicaError::Local(format!("capture: {e}")))
            }
        };
        if let Err(e) = shipped {
            return Ok(Err(stopped(&e)));
        }
        if let Some((cluster, address, previous, generation)) = retired
            && alarm_index::may_delete(&previous, generation)
            && let Err(e) = cluster.delete_alarm_entry(&address, &previous).await
        {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), error = %e, "removing a stale alarm entry failed");
        }
        Ok(Ok(()))
    }

    /// A batched hold's `flush` (ADR 0034, section 4), and the flush of its
    /// `exec-durable` once the statement committed: a pending alarm's entry
    /// named first (recorded in `ALARM_INDEXED_KEY` before the boundary, and
    /// listed by the segment's upload task before the segment, section 9),
    /// then every commit so far flushed through the uploader, until the
    /// event's wall less one second. Refused inside a transaction.
    async fn flush_batched(&mut self, slot: SharedReplica) -> Result<(), SqlError> {
        let read = self
            .with_db(|db| {
                db.flushable()?;
                let alarm = db.alarm()?;
                let indexed = db
                    .meta(ALARM_INDEXED_KEY)?
                    .as_deref()
                    .and_then(alarm_index::parse_meta);
                Ok((alarm, indexed))
            })
            .await
            .map_err(|e| uploads::FlushError::Stopped(format!("{e:#}")).to_sql())?;
        let (alarm, indexed) = read.map_err(from_db_error)?;
        if let Some((cluster, _)) = self.alarm_index.clone()
            && alarm_index::needs_publish(indexed.as_ref(), alarm)
        {
            let generation = slot.lock().await.generation();
            let entry = match alarm {
                Some(at) => match cluster.name_alarm_entry(at, generation) {
                    Ok(entry) => Some(entry),
                    Err(e) => {
                        slot.lock().await.fail(format!("alarm index: {e}"));
                        return Err(
                            uploads::FlushError::Stopped(format!("alarm index: {e}")).to_sql()
                        );
                    }
                },
                None => None,
            };
            let value = alarm_index::to_meta(entry.as_ref());
            let wrote = self
                .with_db(move |db| db.set_meta(ALARM_INDEXED_KEY, &value))
                .await;
            if !matches!(wrote, Ok(Ok(()))) {
                slot.lock()
                    .await
                    .fail("alarm index: recording the entry failed");
                return Err(uploads::FlushError::Stopped(
                    "alarm index: recording the entry failed".into(),
                )
                .to_sql());
            }
        }
        let (until, completion) = match &self.batched {
            Some(b) => (b.flush_until, Some(b.completion.clone())),
            None => (tokio::time::Instant::now(), None),
        };
        let started = std::time::Instant::now();
        let flushed = uploads::flush_through(
            &slot,
            &mut uploads::DbAt::Host(self),
            completion.as_ref(),
            until,
        )
        .await;
        crate::metrics::METRICS
            .flush_seconds
            .observe(started.elapsed());
        flushed.map_err(|e| e.to_sql())
    }

    /// The batched hold's replica, if the running event's hold is batched.
    fn batched_slot(&self) -> Option<SharedReplica> {
        self.batched.as_ref()?;
        self.replica.clone()
    }

    pub(super) fn new(db: ResidentDb, sockets: Arc<Sockets>) -> Self {
        Self {
            checked_seq: None,
            init_version: None,
            opening: None,
            wasi: crate::egress::guest_wasi(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            db: Some(db),
            pending_db: None,
            delete_requested: false,
            deleted: None,
            delete_retry: false,
            deleter: None,
            call: None,
            sockets,
            replica: None,
            replica_stop: None,
            alarm_index: None,
            batched: None,
            egress: crate::egress::EgressHooks::default(),
            env: Arc::default(),
        }
    }

    pub(super) async fn with_db<T: Send + 'static>(
        &mut self,
        f: impl FnOnce(&mut ActorDb) -> T + Send + 'static,
    ) -> wasmtime::Result<T> {
        self.drain_db().await?;
        let mut db = self
            .db
            .take()
            .ok_or_else(|| wasmtime::format_err!("actor database unavailable"))?;
        let (result, received) = oneshot::channel();
        self.pending_db = Some(tokio::task::spawn_blocking(move || {
            let out = f(&mut db);
            let _ = result.send(out);
            db
        }));
        self.drain_db().await?;
        Ok(received.await?)
    }

    pub(super) async fn drain_db(&mut self) -> wasmtime::Result<()> {
        if let Some(pending) = self.pending_db.as_mut() {
            // Borrow the handle: cancelling this await must leave it here.
            let completed = pending.await;
            self.pending_db = None;
            self.db = Some(completed?);
        }
        Ok(())
    }
}

impl WasiView for ActorHost {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for ActorHost {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.egress,
        }
    }
}

impl types::Host for ActorHost {}

/// What a storage call gets once the actor deleted itself durably in this
/// event (ADR 0028).
fn deleted_in_this_event() -> SqlError {
    SqlError {
        kind: SqlErrorKind::NotAllowed,
        message: "the actor was deleted in this event".into(),
    }
}

/// How every `internal` from `storage.flush` starts once replication has
/// stopped (ADR 0034, section 4), as `exec-durable`'s refusal after a
/// failure starts.
pub(crate) const REPLICATION_STOPPED: &str = "replication stopped";

/// How a batched class's `storage.flush` starts its `internal` when the
/// event's wall ran out while the upload went on (ADR 0034, section 4).
/// A strict class never answers it, its upload being inline.
pub(crate) const STILL_UPLOADING: &str = "not durable: still uploading";

/// A durable delete not known to be durable.
fn not_durable(e: impl std::fmt::Display) -> SqlError {
    SqlError {
        kind: SqlErrorKind::Internal,
        message: format!("the delete is not known to be durable: {e}; retry"),
    }
}

impl ActorHost {
    /// `delete-self-durable` (ADR 0028): deletes the actor inside the call.
    /// In a cluster the reset (an empty database, carrying the operator
    /// delete marker) is uploaded on the replica's head, as `exec-durable`
    /// uploads; on a single node the files are removed and the directory
    /// fsynced. The rest of a delete happens when the event ends.
    async fn delete_durably(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        if self.deleted.is_some() {
            return Ok(Ok(()));
        }
        if self.delete_retry {
            return Ok(self.remove_files_durably().await);
        }
        let (in_tx, carried, restored) = self
            .with_db(|db| {
                (
                    db.in_transaction(),
                    db.meta(OPERATOR_DELETE_KEY),
                    db.meta(OPERATOR_RESTORE_KEY),
                )
            })
            .await?;
        if in_tx {
            return Ok(Err(SqlError {
                kind: SqlErrorKind::NotAllowed,
                message: "delete-self-durable inside a transaction: commit or roll back first"
                    .into(),
            }));
        }
        let Some(slot) = self.replica.clone() else {
            // One node: the files go now. The database is closed first; if
            // removing them fails it stays closed (storage refused), a
            // retry removes them, and the end of the event deletes the
            // actor as `delete-self` would.
            self.drain_db().await?;
            self.db = None;
            self.delete_requested = true;
            self.delete_retry = true;
            return Ok(self.remove_files_durably().await);
        };
        // A batched hold resets only at its acknowledged head (ADR 0034,
        // section 7): everything committed so far is flushed first, a
        // pending alarm listed in the index before it (as `flush` does); a
        // flush that fails or runs out answers `internal`, and calling this
        // again is safe.
        if let Some(batched) = self.batched_slot()
            && let Err(e) = self.flush_batched(batched).await
        {
            return Ok(Err(not_durable(e.message)));
        }
        let mut replica = slot.lock().await;
        if let Some(reason) = replica.failed() {
            return Ok(Err(not_durable(format!("replication stopped ({reason})"))));
        }
        // Not known durable from here on: replication stops, as after a
        // failed `exec-durable`, and the actor is restored by its next
        // event.
        let carried = match carried {
            Ok(v) => v,
            Err(e) => {
                replica.fail(format!("delete-self-durable: {}", e.message));
                return Ok(Err(not_durable(e.message)));
            }
        };
        let Some((storage, address)) = self.deleter.clone() else {
            replica.fail("delete-self-durable: no database to delete");
            return Ok(Err(not_durable("no database to delete")));
        };
        // A new delete carries its immutable reset position. Restore replay
        // checks that object's bucket time even if this marker could not be read.
        let restored = restored.unwrap_or_default();
        let replacement = format!(
            "delete {} {} {}",
            replica.head().txid.saturating_add(1),
            crate::alarm::now_ms_i64(),
            replica.generation()
        );
        let built = tokio::task::spawn_blocking(move || {
            empty_database_snapshot(
                &storage,
                &address,
                carried.as_deref(),
                restored.as_deref(),
                &replacement,
            )
        })
        .await;
        let all = match built {
            Ok(Ok(all)) => all,
            Ok(Err(e)) => {
                replica.fail(format!("delete-self-durable: {e}"));
                return Ok(Err(not_durable(e)));
            }
            Err(e) => {
                replica.fail(format!("delete-self-durable: {e}"));
                return Ok(Err(not_durable(e)));
            }
        };
        // On failure the replica is marked failed: the event's end gives
        // the actor up, and its next event restores what the bucket has.
        if let Err(e) = replica.ship_reset(all.clone()).await {
            crate::metrics::delete_failed("self_durable");
            return Ok(Err(not_durable(e)));
        }
        crate::metrics::deleted("self_durable");
        self.delete_requested = true;
        self.deleted = Some(Some(all));
        Ok(Ok(()))
    }

    /// One node: removes the closed database's files and fsyncs their
    /// directory. Only once that worked is the actor deleted.
    async fn remove_files_durably(&mut self) -> Result<(), SqlError> {
        let Some((storage, address)) = self.deleter.clone() else {
            return Err(not_durable("no database to delete"));
        };
        let removed = tokio::task::spawn_blocking(move || {
            storage.delete_durably(address.class(), address.name())
        })
        .await;
        let failed = match removed {
            Ok(Ok(())) => {
                crate::metrics::deleted("self_durable");
                self.delete_retry = false;
                self.deleted = Some(None);
                return Ok(());
            }
            Ok(Err(e)) => format!("{e:#}"),
            Err(e) => e.to_string(),
        };
        crate::metrics::delete_failed("self_durable");
        Err(not_durable(failed))
    }
}

impl storage::Host for ActorHost {
    async fn exec(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        if let Some(error) = self.stopped_sql() {
            return Ok(Err(error));
        }
        if self.deleted.is_some() || self.delete_retry {
            return Ok(Err(deleted_in_this_event()));
        }
        let params = params.into_iter().map(to_db).collect::<Vec<_>>();
        let r = self.with_db(move |db| db.exec(&sql, &params)).await?;
        Ok(r.map_err(from_db_error))
    }

    async fn exec_durable(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        if let Some(error) = self.stopped_sql() {
            return Ok(Err(error));
        }
        if self.deleted.is_some() || self.delete_retry {
            return Ok(Err(deleted_in_this_event()));
        }
        let started = std::time::Instant::now();
        let result = self.exec_durable_untimed(sql, params).await;
        if matches!(result, Ok(Ok(_))) {
            crate::metrics::METRICS
                .durable_write_seconds
                .observe(started.elapsed());
        }
        result
    }

    async fn query(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<Rows, SqlError>> {
        if let Some(error) = self.stopped_sql() {
            return Ok(Err(error));
        }
        if self.deleted.is_some() || self.delete_retry {
            return Ok(Err(deleted_in_this_event()));
        }
        let params = params.into_iter().map(to_db).collect::<Vec<_>>();
        let r = self.with_db(move |db| db.query(&sql, &params)).await?;
        Ok(r.map(|rows| Rows {
            columns: rows.columns,
            values: rows
                .values
                .into_iter()
                .map(|row| row.into_iter().map(from_db).collect())
                .collect(),
        })
        .map_err(from_db_error))
    }

    async fn delete_self(&mut self) -> wasmtime::Result<()> {
        self.check_replication()?;
        self.delete_requested = true;
        Ok(())
    }

    async fn delete_self_durable(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        if let Some(error) = self.stopped_sql() {
            return Ok(Err(error));
        }
        self.delete_durably().await
    }

    async fn flush(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        if let Some(error) = self.stopped_sql() {
            return Ok(Err(error));
        }
        if self.deleted.is_some() || self.delete_retry {
            return Ok(Err(deleted_in_this_event()));
        }
        self.flush_untimed().await
    }
}

impl alarms::Host for ActorHost {
    async fn set_alarm(&mut self, at: u64) -> wasmtime::Result<()> {
        self.check_replication()?;
        if self.deleted.is_some() || self.delete_retry {
            return Ok(()); // the actor was deleted in this event
        }
        self.with_db(move |db| {
            db.set_alarm(at)?;
            db.set_meta(ALARM_FAILURES_KEY, "0")
        })
        .await?
        .map_err(|e| wasmtime::format_err!("set-alarm: {e}"))
    }

    async fn get_alarm(&mut self) -> wasmtime::Result<Option<u64>> {
        self.check_replication()?;
        if self.deleted.is_some() || self.delete_retry {
            return Ok(None);
        }
        self.with_db(|db| db.alarm())
            .await?
            .map_err(|e| wasmtime::format_err!("get-alarm: {e}"))
    }

    async fn clear_alarm(&mut self) -> wasmtime::Result<()> {
        self.check_replication()?;
        if self.deleted.is_some() || self.delete_retry {
            return Ok(());
        }
        self.with_db(|db| {
            db.clear_alarm()?;
            db.set_meta(ALARM_FAILURES_KEY, "0")
        })
        .await?
        .map_err(|e| wasmtime::format_err!("clear-alarm: {e}"))
    }
}

impl actors::Host for ActorHost {
    async fn call(
        &mut self,
        address: String,
        request: CallRequest,
    ) -> wasmtime::Result<Result<CallResponse, CallError>> {
        self.check_replication()?;
        let Some(ctx) = self.call.clone() else {
            return Ok(Err(CallError::Failed("no call context".into())));
        };
        let Some(service) = ctx.service.as_ref().and_then(std::sync::Weak::upgrade) else {
            return Ok(Err(CallError::Failed("node is shutting down".into())));
        };
        let target = match service.call_target(ctx.me.tenant(), &address) {
            Ok(target) => target,
            Err(e) => return Ok(Err(e)),
        };
        // Its deadline passed: the event keeps the rest of its wall to
        // answer (ADR 0025).
        let limit = ctx
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if limit.is_zero() {
            return Ok(Err(CallError::Timeout));
        }
        Ok(service
            .call(
                &Caller::Actor(ctx.me.clone()),
                &ctx.chain,
                target,
                request,
                limit,
                &BodyBudget::one(),
            )
            .await)
    }

    async fn self_address(&mut self) -> wasmtime::Result<actors::ActorAddress> {
        // Installed before every guest call, `on-init` included.
        let me = self
            .call
            .as_ref()
            .map(|c| c.me.clone())
            .ok_or_else(|| wasmtime::format_err!("self-address: no event in progress"))?;
        Ok(actors::ActorAddress {
            tenant: me.tenant().to_owned(),
            class: me.class().to_owned(),
            name: me.name().to_owned(),
        })
    }
}

impl calls::Host for ActorHost {
    /// ADR 0025: every entry is an `actors.call` from this actor, with the
    /// chain one call would carry, all started at once, under the event's
    /// call deadline and one response budget.
    async fn call_many(
        &mut self,
        calls: Vec<(String, CallRequest)>,
    ) -> wasmtime::Result<Vec<Result<CallResponse, CallError>>> {
        self.check_replication()?;
        if let Some(refused) = refuse_batch(&calls) {
            return Ok(refused);
        }
        let failed = |m: &str| {
            Ok(calls
                .iter()
                .map(|_| Err(CallError::Failed(m.to_owned())))
                .collect())
        };
        let Some(ctx) = self.call.clone() else {
            return failed("no call context");
        };
        let Some(service) = ctx.service.as_ref().and_then(std::sync::Weak::upgrade) else {
            return failed("node is shutting down");
        };
        let deadline = ctx.deadline;
        let caller = Caller::Actor(ctx.me.clone());
        let budget = BodyBudget::one();
        let entries = calls.into_iter().map(|(address, request)| {
            let service = Arc::clone(&service);
            let (caller, ctx, budget) = (&caller, &ctx, &budget);
            async move {
                let target = service.call_target(ctx.me.tenant(), &address)?;
                let limit = deadline.saturating_duration_since(tokio::time::Instant::now());
                if limit.is_zero() {
                    return Err(CallError::Timeout);
                }
                service
                    .call(caller, &ctx.chain, target, request, limit, budget)
                    .await
            }
        });
        Ok(futures_util::future::join_all(entries).await)
    }
}

impl env::Host for ActorHost {
    async fn get(&mut self, name: String) -> wasmtime::Result<Option<String>> {
        Ok(self.env.get(&name).cloned())
    }
}

impl websockets::Host for ActorHost {
    async fn send(
        &mut self,
        conn: u64,
        msg: WsMessageWit,
    ) -> wasmtime::Result<Result<(), WsSendError>> {
        self.check_replication()?;
        let Some(me) = self.call.as_ref().map(|c| c.me.clone()) else {
            return Ok(Err(WsSendError::UnknownConnection));
        };
        let msg = match msg {
            WsMessageWit::Text(t) => WsMessage::text(t),
            WsMessageWit::Binary(b) => WsMessage::binary(b),
        };
        Ok(self
            .sockets
            .send(&me, conn, msg, self.opening)
            .map_err(|e| match e {
                SendError::UnknownConnection => WsSendError::UnknownConnection,
                SendError::Backpressure => WsSendError::Backpressure,
            }))
    }

    async fn close(&mut self, conn: u64, code: u16, reason: String) -> wasmtime::Result<()> {
        self.check_replication()?;
        if let Some(me) = self.call.as_ref().map(|c| c.me.clone()) {
            self.sockets.close(&me, conn, code, truncate_reason(reason));
        }
        Ok(())
    }

    async fn connections(&mut self) -> wasmtime::Result<Vec<u64>> {
        Ok(self
            .call
            .as_ref()
            .map(|c| self.sockets.of(&c.me, self.opening))
            .unwrap_or_default())
    }
}

pub(super) fn to_db(v: Value) -> wasmkeep_storage::SqlValue {
    use wasmkeep_storage::SqlValue as D;
    match v {
        Value::Null => D::Null,
        Value::Integer(i) => D::Integer(i),
        Value::Real(f) => D::Real(f),
        Value::Text(s) => D::Text(s),
        Value::Blob(b) => D::Blob(b),
    }
}

pub(super) fn from_db(v: wasmkeep_storage::SqlValue) -> Value {
    use wasmkeep_storage::SqlValue as D;
    match v {
        D::Null => Value::Null,
        D::Integer(i) => Value::Integer(i),
        D::Real(f) => Value::Real(f),
        D::Text(s) => Value::Text(s),
        D::Blob(b) => Value::Blob(b),
    }
}

pub(super) fn from_db_error(e: wasmkeep_storage::SqlError) -> SqlError {
    use wasmkeep_storage::SqlErrorKind as K;
    let kind = match e.kind {
        K::Syntax => SqlErrorKind::Syntax,
        K::Constraint => SqlErrorKind::Constraint,
        K::Bind => SqlErrorKind::Bind,
        K::MultipleStatements => SqlErrorKind::MultipleStatements,
        K::QuotaExceeded => SqlErrorKind::QuotaExceeded,
        K::NotAllowed => SqlErrorKind::NotAllowed,
        K::Internal => SqlErrorKind::Internal,
    };
    SqlError {
        kind,
        message: e.message,
    }
}

#[cfg(test)]
mod batched_failure_tests {
    use super::*;
    use crate::replica::{Stop, UploadFailure};

    #[tokio::test]
    async fn a_batched_failure_cuts_a_running_event_without_applying_it() {
        let (sender, flag) = tokio::sync::watch::channel(None);
        let event = async move {
            sender.send_replace(Some(Stop {
                reason: UploadFailure::Late,
                batched: true,
            }));
            std::future::pending::<()>().await;
        };
        assert!(matches!(
            limit_with_failure(
                Duration::from_secs(1),
                std::future::pending(),
                Some(flag),
                event
            )
            .await,
            Err(Cut::Replica)
        ));
    }

    #[tokio::test]
    async fn strict_failure_and_closed_watch_do_not_cut_an_event() {
        for close in [false, true] {
            let (sender, flag) = tokio::sync::watch::channel(Some(Stop {
                reason: UploadFailure::Late,
                batched: false,
            }));
            if close {
                drop(sender);
            }
            let event = async {
                tokio::task::yield_now().await;
                42
            };
            assert_eq!(
                limit_with_failure(
                    Duration::from_secs(1),
                    std::future::pending(),
                    Some(flag),
                    event
                )
                .await
                .unwrap(),
                42
            );
        }
    }
}
