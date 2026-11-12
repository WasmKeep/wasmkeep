//! One actor's state, owned by its mailbox task: ownership, the instance,
//! replication, and preparing and finishing each event.

use super::*;

/// How an actor event answered a request.
pub(super) enum Outcome<B> {
    /// The guest's response.
    Served(Response<HyperOutgoingBody>),
    /// The event failed (trap, timeout, bad response, bucket down...).
    Failed(String),
    /// Another node owns the actor; the request is handed back untouched so
    /// the caller can forward it there.
    NotOwner(String, Request<B>),
    /// This node may not run the actor now (it cannot open the app's
    /// secrets, ADR 0016): 503 with `Retry-After`.
    Refused(String),
    /// This node is draining: the request is handed back for the chosen
    /// peer, which does not own the actor yet, so it is no owner hint
    /// (ADR 0014).
    Elsewhere(String, Request<B>),
    /// The actor does not exist and the request may not create it
    /// ([`crate::access::Access::Existing`]).
    Missing,
    /// An outside request to an internal class
    /// ([`crate::access::Access::Internal`]).
    Forbidden,
}

/// A request `dispatch` hands back to be sent to another node: its owner,
/// or (`elsewhere`) the peer a draining node chose (ADR 0014).
pub(super) struct Handback<B> {
    pub(super) node: String,
    pub(super) req: Request<B>,
    pub(super) elsewhere: bool,
}

/// Whether an event may reach the actor, decided with its class's rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Admitted {
    Yes,
    /// The class's values lack a name it requires (ADR 0016).
    Unconfigured(String),
    /// Outside, `existing`, and the actor does not exist.
    Missing,
    /// Outside, `internal`.
    Forbidden,
    /// Outside, `existing`, and whether the actor exists could not be told
    /// now (the bucket did not answer): retry.
    Unavailable(String),
}

#[derive(Clone, Copy)]
pub(super) struct EventLimits {
    pub(super) wall: Duration,
    pub(super) retry_initial: Duration,
    pub(super) retry_max: Duration,
}

/// Per-actor state owned by its mailbox task.
pub(super) struct ActorState {
    /// Moves under way on this node (ADR 0017), shared with the service:
    /// while one is set for this actor, events go to its target instead of
    /// being run or claimed here, whatever state they find (a stub
    /// hibernated, evicted or rebuilt still sees it).
    pub(super) moves: drain::Moves,
    /// This node cannot open the app's secrets (ADR 0016): shared with the
    /// service.
    pub(super) secrets_blocked: Arc<std::sync::atomic::AtomicBool>,
    pub(super) address: ActorAddress,
    pub(super) class: Arc<ActorClass>,
    pub(super) runtime: Runtime,
    pub(super) storage: Storage,
    pub(super) limits: EventLimits,
    pub(super) alarms: AlarmHandle,
    /// Consecutive failed `on-alarm` runs, for backoff.
    pub(super) alarm_failures: u32,
    /// The service, for `actors.call` (set right after construction).
    pub(super) service: ServiceSlot,
    /// The node's WebSocket connections.
    pub(super) sockets: Arc<Sockets>,
    /// Actors waiting on the current event, outermost first.
    pub(super) chain: Chain,
    /// The database while no instance holds it (between rebuilds).
    pub(super) db: Option<ResidentDb>,
    pub(super) live: Option<Live>,
    /// The node's cluster, if it is in one.
    pub(super) cluster: Option<Arc<Cluster>>,
    /// This node's lease on the actor; released when the state is dropped
    /// (hibernation).
    pub(super) lease: Option<LeaseHold>,
    /// Another node's lease seen on the bucket: (owner, expiry in ms).
    pub(super) owner_hint: Option<(String, u64)>,
    /// The last `ensure_ready` failed because the actor already ran a newer
    /// deploy than this node has: a temporary refusal (503), not an error.
    pub(super) behind_deploy: bool,
    /// Replication to the bucket, while this node holds the lease.
    pub(super) replica: Option<SharedReplica>,
    /// What the actor's outbound HTTP may reach.
    pub(super) egress: Arc<crate::egress::EgressPolicy>,
    /// Where the instance's stdout and stderr go.
    pub(super) guest_log: crate::guest_log::GuestLog,
    /// Shared with open databases and blocking work until their files close.
    pub(super) residency: Residency,
    /// The actor's garbage collection (ADR 0012): one pass at a time, and
    /// one pending request while it runs.
    pub(super) collecting: Arc<Collection>,
}

/// The database drops before its reservation, including on a blocking thread
/// whose awaiting event was cancelled. Cloning reserves no additional slot.
pub(super) type ResidentDb = wasmkeep_runtime::Retained<ActorDb>;

/// The replica, shared with the instance for `exec-durable`.
pub(super) type SharedReplica = Arc<tokio::sync::Mutex<Replica>>;

pub(super) struct Live {
    pub(super) store: Store<ActorHost>,
    pub(super) meter: CpuMeter,
    pub(super) core: ActorCore,
    pub(super) proxy: Proxy,
    /// The running event: when it started and its kind, for metrics.
    pub(super) event: Option<(std::time::Instant, &'static str)>,
}

impl Live {
    /// A new event: the full CPU budget, and its clock started. An event
    /// still open (one that ended without being recorded) is dropped, never
    /// recorded later with a stale start.
    pub(super) fn start_event(&mut self, kind: &'static str) {
        self.meter.reset();
        self.event = Some((std::time::Instant::now(), kind));
    }

    /// Records the running event once: its outcome (`failed` for a trap, a
    /// cut or a guest error) and how long it ran.
    pub(super) fn record_event(&mut self, class: &str, failed: bool) {
        let Some((started, kind)) = self.event.take() else {
            return;
        };
        let m = &crate::metrics::METRICS;
        let outcome = if failed { "failed" } else { "ok" };
        crate::metrics::inc(&m.events.get(&[
            ("class", class),
            ("kind", kind),
            ("outcome", outcome),
        ]));
        m.event_seconds
            .get(&[("class", class)])
            .observe(started.elapsed());
    }
}

/// An instance leaving memory: hibernated, evicted, discarded after a trap,
/// replaced by a new version, or shut down.
impl Drop for Live {
    fn drop(&mut self) {
        crate::metrics::inc(&crate::metrics::METRICS.hibernations);
    }
}

impl ActorState {
    /// A request event: runs it if this node owns the actor, otherwise hands
    /// the request back with the owner's id.
    pub(super) async fn owned_request<B>(
        &mut self,
        req: Request<B>,
        response_tx: oneshot::Sender<Outcome<B>>,
        chain: Chain,
        outside: bool,
    ) where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        if self.abandoned(&response_tx) {
            return;
        }
        match self.admit(outside).await {
            Ok(Admitted::Yes) => {}
            Ok(Admitted::Missing) => {
                let _ = response_tx.send(Outcome::Missing);
                return;
            }
            Ok(Admitted::Unconfigured(name)) => {
                let _ = response_tx.send(Outcome::Refused(format!("{name} is not set")));
                return;
            }
            Ok(Admitted::Forbidden) => {
                let _ = response_tx.send(Outcome::Forbidden);
                return;
            }
            Ok(Admitted::Unavailable(why)) => {
                tracing::info!(actor = %self.address, %why, "event refused for now; retry");
                let _ = response_tx.send(Outcome::Refused(why));
                return;
            }
            Err(e) => {
                let _ = response_tx.send(Outcome::Failed(e));
                return;
            }
        }
        match self.ensure_owner().await {
            // Asked again after the wait for ownership (a takeover's restore
            // can be long): whoever sent it may have gone meanwhile.
            Ok(()) if self.abandoned(&response_tx) => {}
            Ok(()) => self.request(req, response_tx, chain).await,
            Err(OwnershipError::NotOwner { owner, .. }) => {
                let _ = response_tx.send(Outcome::NotOwner(owner, req));
            }
            // Sent to the peer like to an owner: it claims (or, if the
            // request was already forwarded, answers 503).
            Err(OwnershipError::Elsewhere { peer }) => {
                let _ = response_tx.send(Outcome::Elsewhere(peer, req));
            }
            // Not now (the bucket did not answer, say): temporary, like a
            // refusal (503 with Retry-After), not the actor's error (review
            // finding D4). Logged above debug: an outage shows in the log.
            Err(OwnershipError::Unavailable(e)) => {
                tracing::info!(actor = %self.address, why = %e, "event refused for now; retry");
                let _ = response_tx.send(Outcome::Refused(e));
            }
            // A fault a retry will not fix: the actor's error (500).
            Err(OwnershipError::Broken(e)) => {
                let _ = response_tx.send(Outcome::Failed(e));
            }
            Err(OwnershipError::Refused(why)) => {
                let _ = response_tx.send(Outcome::Refused(why));
            }
        }
    }

    /// Starts an event: takes the class (with its sequence, rules and
    /// values) as the service has it now, then decides with *that* class's
    /// rules whether an `outside` event may reach the actor (ADR 0010, ADR
    /// 0013). The event runs on that class: nothing later takes another.
    ///
    /// `Err` is the class no longer being served, or a failed existence
    /// check.
    pub(super) async fn admit(&mut self, outside: bool) -> Result<Admitted, String> {
        self.refresh_class()?;
        // A required value missing (ADR 0016): refused, inside or out.
        if let Some(name) = self.class.missing_required(self.address.class()) {
            return Ok(Admitted::Unconfigured(name.to_owned()));
        }
        if !outside {
            return Ok(Admitted::Yes);
        }
        match self.rule() {
            crate::access::Access::Open => Ok(Admitted::Yes),
            crate::access::Access::Internal => Ok(Admitted::Forbidden),
            crate::access::Access::Existing => match self.exists().await {
                Ok(true) => Ok(Admitted::Yes),
                Ok(false) => Ok(Admitted::Missing),
                Err(e) => Ok(Admitted::Unavailable(format!("existence check: {e}"))),
            },
        }
    }

    /// This actor's mode under the rules of the class the event took.
    pub(super) fn rule(&self) -> crate::access::Access {
        self.class
            .rules
            .for_actor(self.address.class(), self.address.name())
    }

    /// Whether the actor exists, checked without creating anything: no
    /// database opened, no lease taken. It does if it is in memory, has a
    /// database on this node, or (in a cluster) anything in the bucket.
    pub(super) async fn exists(&mut self) -> Result<bool, String> {
        if self.live.is_some()
            || self.db.is_some()
            || self.lease.as_ref().is_some_and(LeaseHold::is_valid)
        {
            return Ok(true);
        }
        let path = self.storage.db_path(
            self.address.tenant(),
            self.address.class(),
            self.address.name(),
        );
        let local = tokio::task::spawn_blocking(move || path.exists())
            .await
            .map_err(|e| e.to_string())?;
        if local {
            return Ok(true);
        }
        match &self.cluster {
            Some(cluster) => crate::replica::exists_in_bucket(cluster, &self.address)
                .await
                .map_err(|e| e.to_string()),
            None => Ok(false),
        }
    }

    /// Makes sure this node holds the actor's lease before an event runs.
    /// Without a cluster every actor is local. A lost or expired lease drops
    /// the instance (it may be stale: another node may have run events since)
    /// and is acquired again.
    pub(super) async fn ensure_owner(&mut self) -> Result<(), OwnershipError> {
        let blocked = self
            .secrets_blocked
            .load(std::sync::atomic::Ordering::Acquire);
        let Some(cluster) = self.cluster.clone() else {
            // One node: nobody to hand to; refuse rather than run with an
            // older set (ADR 0016).
            if blocked {
                return Err(OwnershipError::Refused(
                    "this node cannot open the app's secrets".into(),
                ));
            }
            return Ok(());
        };
        if self.lease.as_ref().is_some_and(LeaseHold::is_valid) {
            // Held, but this node cannot open the app's current secrets:
            // never run the actor with an older set. The drain hands it
            // over; until then its events are refused (ADR 0016).
            if blocked {
                return Err(OwnershipError::Refused(
                    "this node cannot open the app's secrets".into(),
                ));
            }
            return Ok(());
        }
        if let Some((owner, until)) = &self.owner_hint
            && now_ms() < *until
        {
            return Err(OwnershipError::NotOwner {
                owner: owner.clone(),
                until_ms: *until,
            });
        }
        // Whatever ran before may be stale now: close it all. The database
        // is rebuilt from the bucket unless it is already current.
        self.lease = None;
        self.replica = None;
        self.live = None;
        self.db = None;
        // A move under way (ADR 0017): as a drain of this one actor, with the
        // move's target as the peer, until it ends or its deadline passes.
        if let Some(target) = self.moving_target() {
            return Err(OwnershipError::Elsewhere { peer: target });
        }
        // A draining node takes no lease it does not hold in memory: not a
        // free one, and not its own one either (ADR 0014). Checked after
        // the close above, so a lapsed hold is released, not kept renewing.
        // With no eligible peer it claims as usual: draining never makes an
        // actor unreachable.
        if cluster.draining()
            && let Some(peer) = cluster.chosen_peer(&self.address)
        {
            return Err(OwnershipError::Elsewhere { peer });
        }
        // A drain claims when no peer can take the actor; a node that
        // cannot open the secrets refuses instead (ADR 0016).
        if blocked {
            return Err(OwnershipError::Refused(
                "this node cannot open the app's secrets".into(),
            ));
        }
        match LeaseHold::acquire(&cluster, &self.address).await {
            Ok(hold) => {
                self.owner_hint = None;
                let (view, generation) = (hold.view(), hold.generation().unwrap_or(0));
                self.lease = Some(hold);
                let path = self.storage.db_path(
                    self.address.tenant(),
                    self.address.class(),
                    self.address.name(),
                );
                // A call cut short may still be closing the old connection
                // on a blocking thread; never replace the file under it.
                let deadline = std::time::Instant::now() + self.limits.wall;
                while self.storage.is_open(&path) {
                    if std::time::Instant::now() > deadline {
                        self.lease = None;
                        return Err(OwnershipError::Unavailable(
                            "the actor's previous database connection is still open".into(),
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                match Replica::restore(
                    &cluster,
                    &self.address,
                    view,
                    generation,
                    path,
                    self.storage.config().max_db_bytes,
                )
                .await
                {
                    Ok(replica) => {
                        self.replica = Some(Arc::new(tokio::sync::Mutex::new(replica)));
                        Ok(())
                    }
                    Err(e) => {
                        tracing::warn!(actor = %self.address, error = %e, "restore failed");
                        self.lease = None; // release: another node may try
                        // The bucket not answering is temporary; a history
                        // that cannot be restored, or local file trouble,
                        // is not.
                        Err(match e {
                            crate::replica::ReplicaError::Bucket(_) => {
                                OwnershipError::Unavailable(e.to_string())
                            }
                            _ => OwnershipError::Broken(e.to_string()),
                        })
                    }
                }
            }
            Err(e) => {
                // The holder changes only when its lease runs out or is
                // released, so skip the bucket for a while. Forwarding to a stale owner is still
                // safe: that node acquires (or refuses) for itself.
                // At most a second, though: the holder may release early
                // (hibernation) and another node take over.
                if let OwnershipError::NotOwner { owner, until_ms } = &e {
                    let until = (*until_ms).min(now_ms().saturating_add(HINT_MS));
                    self.owner_hint = Some((owner.clone(), until));
                    // Another node's actor now: its periodic snapshots
                    // are that node's to schedule.
                    cluster.snapshot_waits.forget(&self.address);
                }
                Err(e)
            }
        }
    }

    /// Whether a request's sender has gone while it waited in this actor's
    /// queue (a client's connection closed, or an actor's call timed out):
    /// it is not run, since nobody would get its answer and nothing it
    /// would write was acknowledged. One that has started runs to its end.
    /// A client's departure shows only once its request was received
    /// whole (no body, or one that arrived in the first read): hyper does
    /// not watch the connection while a body streams.
    fn abandoned<T>(&self, response_tx: &oneshot::Sender<T>) -> bool {
        if !response_tx.is_closed() {
            return false;
        }
        crate::metrics::inc(&crate::metrics::METRICS.requests_abandoned);
        tracing::debug!(actor = %self.address, "a request's sender left before it ran; not run");
        true
    }

    pub(super) async fn request<B>(
        &mut self,
        req: Request<B>,
        response_tx: oneshot::Sender<Outcome<B>>,
        chain: Chain,
    ) where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        self.chain = chain;
        if let Err(e) = self.ensure_ready().await {
            // This node is behind a deploy the actor already ran: it is
            // updating, so retry (503), not an actor error.
            let outcome = if std::mem::take(&mut self.behind_deploy) {
                Outcome::Refused(e)
            } else {
                Outcome::Failed(e)
            };
            let _ = response_tx.send(outcome);
            return;
        }
        self.install_call_context();
        let Some(live) = self.live.as_mut() else {
            let _ = response_tx.send(Outcome::Failed("actor not ready".into()));
            return;
        };
        live.start_event("request");

        let outcome = async {
            let req = live
                .store
                .data_mut()
                .http()
                .new_incoming_request(Scheme::Http, crate::http::CappedBody::of(req))?;
            let (out_tx, out_rx) = oneshot::channel();
            let out = live.store.data_mut().http().new_response_outparam(out_tx)?;
            let forward = async move {
                let result = match out_rx.await {
                    Ok(Ok(response)) => Outcome::Served(response),
                    Ok(Err(code)) => Outcome::Failed(format!("guest error code: {code:?}")),
                    Err(_) => Outcome::Failed("guest did not set a response".to_owned()),
                };
                let _ = response_tx.send(result);
            };
            let handle =
                live.proxy
                    .wasi_http_incoming_handler()
                    .call_handle(&mut live.store, req, out);
            let mut handle = std::pin::pin!(handle);
            let mut forward = std::pin::pin!(forward);
            tokio::select! {
                // Response set: hand it over, then let the guest finish
                // (it may still be streaming the body).
                () = &mut forward => handle.await,
                // The guest returned or trapped first. A response it set is
                // already queued and is delivered by one more poll; if it set
                // none, the outparam lives in the store and would never
                // resolve, so stop waiting (the caller gets a 500 now).
                handled = &mut handle => {
                    let _ = tokio::time::timeout(Duration::ZERO, forward).await;
                    handled
                }
            }
        };
        let result = match limit(self.limits.wall, self.lease.as_ref(), outcome).await {
            Ok(r) => r.map_err(|e| format!("{e:#}")),
            Err(cut) => Err(format!("event {cut}")),
        };
        if let Err(e) = &result {
            tracing::warn!(actor = %self.address, error = %e, "guest trapped; instance discarded");
        }
        self.finish_event(result.is_err()).await;
    }

    /// Switches to the current version of this actor's class if it was
    /// replaced: the instance is dropped (the database is kept) and the next
    /// lines of `ensure_ready` instantiate the new component.
    /// Takes the class (and its deploy sequence) as the service has it now.
    /// New code drops the instance, so the next one runs it; the same code
    /// under a newer sequence keeps the instance (its sequence is recorded
    /// before the event runs). A class no longer served refuses the event.
    pub(super) fn refresh_class(&mut self) -> Result<(), String> {
        let Some(inner) = self.service.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        let current = read(&inner.classes).get(self.address.class()).cloned();
        let Some(current) = current else {
            if let Some(mut live) = self.live.take() {
                self.db = live.store.data_mut().db.take();
            }
            return Err(format!("{CLASS_GONE}: {}", self.address.class()));
        };
        if !Arc::ptr_eq(&current, &self.class) {
            // New code, or new values (a guest may have cached one, or
            // derived state from it): a new instance. The database is kept,
            // and `on-init` reruns only for new code (it is keyed on it).
            let same = current.version == self.class.version && current.env == self.class.env;
            if !same && let Some(mut live) = self.live.take() {
                self.db = live.store.data_mut().db.take();
            }
            self.class = current;
        }
        Ok(())
    }

    /// Gives the instance what `actors.call` needs for the current event.
    pub(super) fn install_call_context(&mut self) {
        let ctx = CallContext {
            service: self.service.get().cloned(),
            me: self.address.clone(),
            chain: Arc::clone(&self.chain),
        };
        if let Some(live) = self.live.as_mut() {
            live.store.data_mut().call = Some(ctx);
        }
    }

    /// In a cluster, makes sure the database is being captured, and uploads
    /// the first snapshot if the bucket had nothing for this actor.
    pub(super) async fn start_replication(&mut self, db: ResidentDb) -> Result<ResidentDb, String> {
        let Some(slot) = self.replica.clone() else {
            return Ok(db);
        };
        let needs_snapshot = slot.lock().await.needs_snapshot();
        // The first snapshot may carry an alarm set before this node joined
        // a cluster: the index must list it before the bucket holds it.
        let db = if needs_snapshot {
            match self.index_first_alarm(db).await {
                Ok(db) => db,
                Err(e) => {
                    self.abandon(&format!("before the first snapshot: {e}"));
                    return Err(format!("replication: {e}"));
                }
            }
        } else {
            db
        };
        let (db, snapshot) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            if db.captured_checksum().is_none()
                && let Err(e) = db.start_capture()
            {
                return Err(format!("start capture: {e}"));
            }
            let snapshot = if needs_snapshot {
                Some(db.snapshot().map_err(|e| format!("snapshot: {e}"))?)
            } else {
                None
            };
            Ok((db, snapshot))
        })
        .await
        .map_err(|e| e.to_string())??;
        if let Some(all) = snapshot {
            let shipped = slot
                .lock()
                .await
                .ship_snapshot(&all, crate::replica::SnapshotKind::First)
                .await;
            if let Err(e) = shipped {
                self.abandon(&format!("first snapshot: {e}"));
                return Err(format!("replication: {e}"));
            }
        }
        Ok(db)
    }

    /// After an event: uploads what it committed (and a periodic snapshot).
    /// If replication failed, here or during the event, the actor stops: the
    /// lease is released and the next event restores from the bucket.
    pub(super) async fn replicate(&mut self) {
        let Some(slot) = self.replica.clone() else {
            return;
        };
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let mut replica = slot.lock().await;
        if replica.failed().is_none() {
            match with_db(&mut live.store, |db| db.take_changes()).await {
                Ok(Ok(Some(changes))) => {
                    // On failure `ship` records why; handled below.
                    let _ = replica.ship(&changes).await;
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => replica.fail(format!("capture: {e}")),
                Err(e) => replica.fail(format!("capture: {e:#}")),
            }
        }
        if replica.failed().is_none() && replica.snapshot_due() {
            match with_db(&mut live.store, |db| db.snapshot()).await {
                Ok(Ok(all)) => match replica
                    .ship_snapshot(&all, crate::replica::SnapshotKind::Periodic)
                    .await
                {
                    // Created and acknowledged: collect what it makes
                    // unnecessary (ADR 0012), in the background.
                    Ok(true) => {
                        if let Some(cluster) = &self.cluster {
                            spawn_collection(
                                self.address.clone(),
                                Arc::clone(&self.collecting),
                                CollectRequest {
                                    collector: replica.collector(),
                                    snapshot: replica.head(),
                                    retention: cluster.history_retention,
                                    erase: false,
                                },
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(actor = %self.address, error = %e, "periodic snapshot failed");
                    }
                },
                Ok(Err(wasmkeep_storage::wal::CaptureError::Pending(_))) => {
                    replica.fail("changes appeared during a snapshot");
                }
                other => tracing::warn!(actor = %self.address, ?other, "periodic snapshot failed"),
            }
        }
        if let Some(reason) = replica.failed().map(str::to_owned) {
            drop(replica);
            self.abandon(&reason);
        }
    }

    /// After `delete-self` in a cluster: records the empty database in the
    /// bucket's history, so no node restores the deleted data.
    ///
    /// `files_removed`: whether the local database files were removed. If
    /// not, reopening them would give the old data back, so the empty
    /// database is made in a scratch directory instead; the stale local
    /// file no longer matches what the bucket and its sidecar name, so the
    /// next restore sets it aside rather than using it.
    pub(super) async fn replicate_delete(&mut self, files_removed: bool) {
        let Some(slot) = self.replica.clone() else {
            return;
        };
        let storage = self.storage.clone();
        let a = self.address.clone();
        let residency = self.residency.clone();
        let opened = tokio::task::spawn_blocking(move || {
            if !files_removed {
                return empty_database_snapshot(&storage, &a).map(|all| (None, all));
            }
            let mut db = residency.retain(
                storage
                    .open(a.tenant(), a.class(), a.name())
                    .map_err(|e| format!("{e:#}"))?,
            );
            db.start_capture().map_err(|e| e.to_string())?;
            let all = db.snapshot().map_err(|e| e.to_string())?;
            Ok::<_, String>((Some(db), all))
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r);
        let result = match opened {
            Ok((db, all)) => {
                self.db = db;
                let mut replica = slot.lock().await;
                let empty = all.clone();
                match replica.ship_reset(all).await {
                    Ok(()) => {
                        // A snapshot of the empty database at the reset,
                        // then everything before it goes, whatever the
                        // retention window: erasing the data is the point
                        // of the delete (ADR 0012).
                        match replica
                            .ship_snapshot(&empty, crate::replica::SnapshotKind::AfterDelete)
                            .await
                        {
                            Ok(true) => {
                                let request = CollectRequest {
                                    collector: replica.collector(),
                                    snapshot: replica.head(),
                                    retention: Duration::ZERO,
                                    erase: true,
                                };
                                if files_removed {
                                    spawn_collection(
                                        self.address.clone(),
                                        Arc::clone(&self.collecting),
                                        request,
                                    );
                                } else {
                                    // The actor is given up right after
                                    // (below), which would stop a pass in
                                    // the background: erase now.
                                    run_collection(&self.address, &request).await;
                                }
                            }
                            Ok(false) => {}
                            // The delete itself is recorded (the reset);
                            // only its history stays until the actor
                            // writes again.
                            Err(e) => {
                                crate::metrics::inc(
                                    &crate::metrics::METRICS.history_collect_failures,
                                );
                                tracing::warn!(actor = %self.address, error = %e, "snapshot after a delete failed; its history is not erased");
                            }
                        }
                        Ok(())
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e),
        };
        // A deleted actor leaves no wait behind on this node; the replica
        // in memory keeps its own for as long as it lives.
        if let Some(cluster) = &self.cluster {
            cluster.snapshot_waits.forget(&self.address);
        }
        if let Err(e) = result {
            self.abandon(&format!("recording the delete: {e}"));
        }
    }

    /// Gives the actor up after a replication failure: everything local may
    /// be ahead of the bucket, so it is closed and rebuilt on the next event.
    pub(super) fn abandon(&mut self, reason: &str) {
        tracing::warn!(actor = %self.address, %reason, "replication failed; giving the actor up");
        self.replica = None;
        self.live = None;
        self.db = None;
        self.lease = None; // releases
    }

    /// No old code on newer data (ADR 0008): an actor whose database a newer
    /// deploy already ran on is refused here until this node catches up;
    /// otherwise the actor records the deploy it now runs.
    pub(super) async fn check_deploy_seq(&mut self) -> Result<(), String> {
        let Some(inner) = self.service.get().and_then(std::sync::Weak::upgrade) else {
            return Ok(());
        };
        // The sequence the code was deployed under, not the node's: the two
        // may differ for an event that took its class just before a deploy
        // landed, and recording the node's would mark old code as new.
        let node = self.class.seq;
        let Some(live) = self.live.as_mut() else {
            return Ok(());
        };
        if node == 0 {
            live.store.data_mut().checked_seq = Some(0);
            return Ok(());
        }
        let recorded = with_db(&mut live.store, |db| db.meta(DEPLOY_SEQ_KEY))
            .await
            .map_err(|e| format!("{e:#}"))?
            .map_err(|e| e.to_string())?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if recorded > node {
            inner.deploy_wakeup.notify_one();
            self.behind_deploy = true;
            if let Some(mut live) = self.live.take() {
                self.db = live.store.data_mut().db.take();
            }
            return Err(format!(
                "the actor already ran deploy {recorded}; this node runs {node} and is updating"
            ));
        }
        if recorded < node {
            let value = node.to_string();
            with_db(&mut live.store, move |db| {
                db.set_meta(DEPLOY_SEQ_KEY, &value)
            })
            .await
            .map_err(|e| format!("{e:#}"))?
            .map_err(|e| e.to_string())?;
        }
        live.store.data_mut().checked_seq = Some(node);
        Ok(())
    }

    /// Opens the database, instantiates the component and runs `on-init`
    /// when this component version has not been initialised yet.
    ///
    /// A host failure part way (an I/O error recording the deploy sequence
    /// or the `on-init` version, say) may come after commits: `on-init`'s
    /// writes, an alarm it set. The event is then finished like any other,
    /// so those are synced with the timer, indexed and replicated, not left
    /// only on this disk where a failover loses them; and the instance is
    /// dropped, so the next event redoes every step it may have missed.
    pub(super) async fn ensure_ready(&mut self) -> Result<(), String> {
        self.behind_deploy = false;
        match self.prepare().await {
            // `on-init`'s own outcome: its event is already finished.
            Ok(outcome) => outcome,
            Err(e) => {
                self.finish(false, true).await;
                if let Some(mut live) = self.live.take() {
                    self.db = live.store.data_mut().db.take();
                }
                Err(e)
            }
        }
    }

    /// `ensure_ready`'s steps. `Err` is a host failure before any event was
    /// finished; `Ok` carries `on-init`'s outcome, or `Ok(())` when it
    /// needed no run. Runs on the class the event took when it started
    /// (`admit`, or `refresh_class` for alarms): never a newer one, whose
    /// rules the event did not check.
    pub(super) async fn prepare(&mut self) -> Result<Result<(), String>, String> {
        if self.live.is_none() {
            let db = match self.db.take() {
                Some(db) => db,
                None => {
                    // A host panic can retire the old mailbox while its
                    // blocking worker still owns this actor's connection.
                    // A fresh reservation must not overlap that SQL work.
                    let path = self.storage.db_path(
                        self.address.tenant(),
                        self.address.class(),
                        self.address.name(),
                    );
                    let deadline = tokio::time::Instant::now() + self.limits.wall;
                    while self.storage.is_open(&path) {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(
                                "the actor's previous database connection is still open".into()
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let storage = self.storage.clone();
                    let a = self.address.clone();
                    let residency = self.residency.clone();
                    tokio::task::spawn_blocking(move || {
                        storage
                            .open(a.tenant(), a.class(), a.name())
                            .map(|db| residency.retain(db))
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("{e:#}"))?
                }
            };
            let db = self.start_replication(db).await?;
            let mut host = ActorHost::new(db, Arc::clone(&self.sockets));
            host.replica = self.replica.clone();
            host.egress = crate::egress::EgressHooks::new(Arc::clone(&self.egress));
            host.wasi = self.guest_log.wasi(self.address.to_string());
            host.env = Arc::clone(&self.class.env);
            host.alarm_index = self
                .cluster
                .as_ref()
                .map(|c| (Arc::clone(c), self.address.clone()));
            let (mut store, meter) = self.runtime.new_metered_async_store(host);
            let started = std::time::Instant::now();
            let instance = match self.class.pre.instantiate_async(&mut store).await {
                Ok(i) => {
                    crate::metrics::METRICS
                        .instantiate_seconds
                        .observe(started.elapsed());
                    i
                }
                Err(e) => {
                    self.db = store.data_mut().db.take();
                    return Err(format!("instantiate: {e:#}"));
                }
            };
            let core = ActorCore::new(&mut store, &instance).map_err(|e| format!("{e:#}"))?;
            let proxy = Proxy::new(&mut store, &instance).map_err(|e| format!("{e:#}"))?;
            self.live = Some(Live {
                store,
                meter,
                core,
                proxy,
                event: None,
            });
            crate::metrics::inc(&crate::metrics::METRICS.activations);
        }
        // Before every event whose class's sequence this instance has not
        // recorded yet: a new instance, or the same code redeployed.
        if self
            .live
            .as_ref()
            .is_some_and(|l| l.store.data().checked_seq != Some(self.class.seq))
        {
            self.check_deploy_seq().await?;
        }

        self.install_call_context();
        let live = self.live.as_mut().ok_or("actor not ready")?;
        // Known for this instance: on-init already ran (or was found done)
        // for this version. The database does not change under a live
        // instance except through the guest, which cannot touch host meta.
        if live.store.data().init_version.as_deref() == Some(self.class.version.as_str()) {
            return Ok(Ok(()));
        }
        let current = with_db_inline(&mut live.store, |db| db.meta(INIT_VERSION_KEY))
            .map_err(|e| format!("{e:#}"))?
            .map_err(|e| e.to_string())?;
        if current.as_deref() == Some(self.class.version.as_str()) {
            live.store.data_mut().init_version = current;
            return Ok(Ok(()));
        }

        live.start_event("init");
        let init = limit(
            self.limits.wall,
            self.lease.as_ref(),
            live.core.call_on_init(&mut live.store),
        )
        .await;
        // A guest error leaves the instance usable; a trap or timeout does not.
        let trapped = !matches!(init, Ok(Ok(_)));
        live.record_event(self.address.class(), !matches!(init, Ok(Ok(Ok(())))));
        let outcome = match init {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(msg))) => Err(format!("on-init failed: {msg}")),
            Ok(Err(trap)) => Err(format!("on-init trapped: {trap:#}")),
            Err(cut) => Err(format!("on-init {cut}")),
        };
        if outcome.is_ok() {
            let version = self.class.version.clone();
            let recorded = version.clone();
            with_db(&mut live.store, move |db| {
                db.end_event()?;
                db.set_meta(INIT_VERSION_KEY, &recorded)
            })
            .await
            .map_err(|e| format!("{e:#}"))?
            .map_err(|e| e.to_string())?;
            live.store.data_mut().init_version = Some(version);
        } else {
            self.finish_event(trapped).await;
        }
        Ok(outcome)
    }

    /// Rolls back a transaction left open, re-registers the committed alarm
    /// with the timer, then drops the instance if it trapped and deletes the
    /// actor if it asked to be deleted.
    pub(super) async fn finish_event(&mut self, trapped: bool) {
        self.finish(trapped, true).await;
    }

    ///
    /// Returns the alarm index entry the replicated state names
    /// (`Some(None)`: none, as after a delete), if known: what
    /// `clear_stale_hint` must spare.
    pub(super) async fn finish(
        &mut self,
        trapped: bool,
        sync_alarm: bool,
    ) -> Option<Option<Entry>> {
        let live = self.live.as_mut()?;
        live.record_event(self.address.class(), trapped);
        // A wall-clock cut cancels the guest future, not spawn_blocking.
        // Recover its connection before rollback, replication, deletion or
        // another event can open this database again.
        if let Err(e) = live.store.data_mut().drain_db().await {
            tracing::warn!(actor = %self.address, error = %e, "database worker failed");
            self.live = None;
            self.db = None;
            self.replica = None;
            self.lease = None;
            return None;
        }
        // Host bookkeeping, microseconds on the actor's own connection:
        // inline rather than a hop to the blocking pool.
        let committed = with_db_inline(&mut live.store, |db| {
            db.end_event()?;
            db.alarm()
        });
        let delete = live.store.data().delete_requested;
        // The committed alarm, for the bucket's index (if it could be read).
        let indexed = if delete {
            Some(None)
        } else {
            match &committed {
                Ok(Ok(at)) => Some(*at),
                _ => None,
            }
        };
        if delete {
            self.alarms.clear(self.address.clone());
            // Under `existing`, a deleted actor's sockets go with it (an
            // open class keeps them: the next message recreates the actor).
            if self.rule() != crate::access::Access::Open {
                for conn in self.sockets.of(&self.address, None) {
                    self.sockets
                        .close(&self.address, conn, 1000, "actor deleted".into());
                }
            }
        } else if sync_alarm {
            match committed {
                Ok(Ok(Some(at))) => self.alarms.set(self.address.clone(), at),
                Ok(Ok(None)) => self.alarms.clear(self.address.clone()),
                _ => tracing::warn!(actor = %self.address, "could not read the alarm"),
            }
        }
        // The new entry before the event is replicated, the old one removed
        // after: a crash in between leaves an extra entry, never a missing one.
        let published = match indexed {
            Some(at) => self.publish_alarm(at).await,
            // Not known: the index may not match what is replicated.
            None => self.index_missed("the committed alarm is unreadable").await,
        };
        let (mut named, mut stale) = match published {
            Some(Published { named, replaced }) => (Some(named), replaced),
            None => (None, None),
        };
        self.replicate().await;
        if !delete {
            self.retire_alarm_entry(stale.take()).await;
        }
        if (trapped || delete)
            && let Some(mut live) = self.live.take()
        {
            self.db = live.store.data_mut().db.take();
        }
        if delete {
            self.db = None; // close before removing the files
            let storage = self.storage.clone();
            let a = self.address.clone();
            let removed = tokio::task::spawn_blocking(move || {
                storage.delete(a.tenant(), a.class(), a.name())
            })
            .await;
            let files_removed = matches!(removed, Ok(Ok(())));
            if !files_removed {
                if self.replica.is_some() {
                    tracing::warn!(actor = %self.address, "deleting the actor database failed; the bucket records it empty anyway");
                } else {
                    tracing::warn!(actor = %self.address, "deleting the actor database failed; its data stays on this node");
                }
            }
            self.replicate_delete(files_removed).await;
            // Only once the delete is in the bucket.
            self.retire_alarm_entry(stale).await;
            if !files_removed {
                // The old files are still here: give the actor up, so its
                // next event restores from the bucket, which sets them
                // aside (their sidecar no longer matches) instead of
                // serving them.
                self.replica = None;
                self.live = None;
                self.db = None;
                self.lease = None; // releases
            }
            // The bucket's state is now the empty database (or, if
            // recording the delete failed, the actor was given up and
            // nothing may be removed: `may_index` refuses).
            named = Some(None);
        }
        named
    }
}
