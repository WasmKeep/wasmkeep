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
}

impl std::fmt::Display for Cut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wall(d) => write!(f, "exceeded the {d:?} wall-clock limit"),
            Self::Lease => f.write_str("stopped: this node's lease on the actor ran out"),
        }
    }
}

/// Runs a guest call under the wall-clock limit and, in a cluster, only
/// while this node holds the actor's lease. Dropping the call stops the
/// guest; callers treat a cut like a trap and discard the instance.
pub(super) async fn limit<F: std::future::Future>(
    wall: Duration,
    lease: Option<&LeaseHold>,
    f: F,
) -> Result<F::Output, Cut> {
    let expiry = lease.map(LeaseHold::expiry);
    let lease_gone = async move {
        match expiry {
            Some(e) => e.await,
            None => std::future::pending().await,
        }
    };
    let cut = |reason: &str| {
        crate::metrics::inc(&crate::metrics::METRICS.cuts.get(&[("reason", reason)]));
    };
    tokio::select! {
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

impl ActorHost {
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
            call: None,
            sockets,
            replica: None,
            alarm_index: None,
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

impl storage::Host for ActorHost {
    async fn exec(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        let params = params.into_iter().map(to_db).collect::<Vec<_>>();
        let r = self.with_db(move |db| db.exec(&sql, &params)).await?;
        Ok(r.map_err(from_db_error))
    }

    async fn exec_durable(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
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
        self.delete_requested = true;
        Ok(())
    }
}

impl alarms::Host for ActorHost {
    async fn set_alarm(&mut self, at: u64) -> wasmtime::Result<()> {
        self.with_db(move |db| {
            db.set_alarm(at)?;
            db.set_meta(ALARM_FAILURES_KEY, "0")
        })
        .await?
        .map_err(|e| wasmtime::format_err!("set-alarm: {e}"))
    }

    async fn get_alarm(&mut self) -> wasmtime::Result<Option<u64>> {
        self.with_db(|db| db.alarm())
            .await?
            .map_err(|e| wasmtime::format_err!("get-alarm: {e}"))
    }

    async fn clear_alarm(&mut self) -> wasmtime::Result<()> {
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
        let wall = service.wall;
        Ok(service
            .call(
                &Caller::Actor(ctx.me.clone()),
                &ctx.chain,
                target,
                request,
                wall,
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
    /// chain one call would carry, all started at once, under one deadline
    /// and one response budget.
    async fn call_many(
        &mut self,
        calls: Vec<(String, CallRequest)>,
    ) -> wasmtime::Result<Vec<Result<CallResponse, CallError>>> {
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
        let deadline = tokio::time::Instant::now() + service.wall;
        let caller = Caller::Actor(ctx.me.clone());
        let budget = BodyBudget::one();
        let entries = calls.into_iter().map(|(address, request)| {
            let service = Arc::clone(&service);
            let (caller, ctx, budget) = (&caller, &ctx, &budget);
            async move {
                let target = service.call_target(ctx.me.tenant(), &address)?;
                let limit = deadline.saturating_duration_since(tokio::time::Instant::now());
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
