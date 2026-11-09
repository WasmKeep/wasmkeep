//! Moving one actor to a named node ([ADR 0017](../../../../docs/adr/0017-moving-one-actor.md)).
//!
//! A move is a drain of one actor. The owner's move job, in the actor's
//! mailbox, records the move in the node's map of moves under way and
//! releases the lease; from then on `ensure_owner` sends every event for the
//! actor to the target, whatever state the event finds (the map outlives a
//! hibernated or rebuilt state). The owner then sends the target a signed
//! take call, so the actor moves even if no event arrives: the target
//! acquires, restores and arms the alarm from the database. An actor nobody
//! holds is taken by the target itself.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::cluster::{HOLDER, LeaseReturn, TakeAnswer, parse_take};
use wasmkeep_cluster::control::MoveIntent;

/// How long a move may stay under way before it gives up.
const MOVE_DEADLINE: Duration = Duration::from_secs(30);
/// Moves run at once per node.
const MOVES_PARALLEL: usize = 16;
/// How long the owner waits for an actor's sockets to finish closing.
const SOCKETS_CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long the owner waits for the events queued before the move job.
const MOVE_WAIT: Duration = Duration::from_secs(30);

/// Move requests being acted on, by key, each with its attempt's token: a
/// move job whose token is no longer there (the attempt was abandoned)
/// does nothing.
pub(super) type Active = Arc<Mutex<HashMap<String, u64>>>;

/// A fresh token per attempt.
static ATTEMPTS: AtomicU64 = AtomicU64::new(1);

/// What a take job did.
#[derive(Debug)]
pub(super) enum Taken {
    /// This node holds the actor now.
    Held,
    /// Another node holds it.
    HeldBy(String),
    /// No such actor.
    NoSuchActor,
    /// Refused or failed (draining, secrets blocked, the bucket, the
    /// alarm unreadable).
    Refused(String),
}

/// What a move job did.
#[derive(Debug)]
enum MoveOut {
    /// Released; the actor's committed alarm, if any.
    Released { alarm: Option<u64> },
    /// The release failed: the lease expires on its own.
    Expiring,
    /// A socket is open or closing, or the alarm could not be read: tried
    /// again later, the lease kept.
    Busy,
    /// The actor held no lease here.
    NotHeld,
    /// The attempt was abandoned before the job ran.
    Kept,
}

impl ActorState {
    /// The take job (ADR 0017): forget learned owners, check the actor
    /// exists, acquire and restore, arm the committed alarm.
    pub(super) async fn take(&mut self) -> Taken {
        self.owner_hint = None;
        match self.exists().await {
            Ok(true) => {}
            Ok(false) => return Taken::NoSuchActor,
            Err(e) => return Taken::Refused(e),
        }
        match self.ensure_owner().await {
            Ok(()) => {}
            Err(OwnershipError::NotOwner { owner, .. }) => return Taken::HeldBy(owner),
            Err(OwnershipError::Elsewhere { .. }) => {
                return Taken::Refused("this node is draining or moving the actor".into());
            }
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => {
                return Taken::Refused(e);
            }
        }
        // Held now. An alarm that cannot be read is not dropped: the old
        // owner keeps its registration and asks again.
        match self.alarm_from_database().await {
            Ok(Some(at)) => self.alarms.set_if_earlier(self.address.clone(), at),
            Ok(None) => {}
            Err(e) => return Taken::Refused(format!("reading the alarm: {e}")),
        }
        Taken::Held
    }

    /// Arms the committed alarm for a path that may start no instance (the
    /// WebSocket upgrade's ownership probe): otherwise only an event's
    /// finish arms it, and it would wait for the sweep. As the sweep arms
    /// one, it never displaces a delivery in flight, an admission backoff,
    /// or an earlier time; after a failed delivery, nor its retry backoff
    /// (the longest one is the horizon then). One that cannot be read is
    /// left to the sweep.
    pub(super) async fn arm_committed_alarm(&mut self) {
        match self.alarm_from_database().await {
            // After a failed delivery, with the longest backoff as the
            // horizon, as the sweep arms one: the backoff stands. Otherwise
            // none: a later registration (one left from an earlier tenure,
            // say) must not hold back the earlier committed time.
            Ok(Some(at)) => {
                let horizon = if self.alarm_failures > 0 {
                    self.limits.retry_max
                } else {
                    std::time::Duration::ZERO
                };
                self.alarms.sweep(self.address.clone(), at, horizon);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(actor = %self.address, error = %e, "reading the alarm failed; the sweep delivers it");
            }
        }
    }

    /// The committed alarm, read from the open instance or the restored
    /// database without instantiating the component.
    pub(super) async fn alarm_from_database(&mut self) -> Result<Option<u64>, String> {
        if let Some(live) = self.live.as_mut() {
            return match with_db(&mut live.store, |db| db.alarm()).await {
                Ok(Ok(at)) => Ok(at),
                Ok(Err(e)) => Err(e.to_string()),
                Err(e) => Err(e.to_string()),
            };
        }
        let db = match self.db.take() {
            Some(db) => db,
            None => {
                let storage = self.storage.clone();
                let a = self.address.clone();
                let residency = self.residency.clone();
                let opened = tokio::task::spawn_blocking(move || {
                    if !storage.db_path(a.tenant(), a.class(), a.name()).exists() {
                        return Ok(None);
                    }
                    storage
                        .open(a.tenant(), a.class(), a.name())
                        .map(|db| Some(residency.retain(db)))
                        .map_err(|e| e.to_string())
                })
                .await
                .map_err(|e| e.to_string())??;
                match opened {
                    Some(db) => db,
                    None => return Ok(None), // no database: no alarm
                }
            }
        };
        let (db, read) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            let read = db.alarm();
            (db, read)
        })
        .await
        .map_err(|e| e.to_string())?;
        self.db = Some(db);
        read.map_err(|e| e.to_string())
    }

    /// The owner's move job: after the queued events, record the move and
    /// release the lease (the drain's hand-over steps).
    async fn move_out(
        &mut self,
        target: String,
        key: String,
        token: u64,
        active: Active,
    ) -> MoveOut {
        let current = active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .copied();
        if current != Some(token) {
            return MoveOut::Kept;
        }
        if self.sockets.reserved_by(&self.address) > 0 {
            for conn in self.sockets.of(&self.address, None) {
                self.sockets
                    .close(&self.address, conn, 1012, "actor moving; reconnect".into());
            }
            return MoveOut::Busy;
        }
        if !self.lease.as_ref().is_some_and(LeaseHold::is_valid) {
            self.lease = None;
            return MoveOut::NotHeld;
        }
        let Ok(alarm) = self.alarm_from_database().await else {
            return MoveOut::Busy; // read again on the next attempt
        };
        let until_ms =
            now_ms().saturating_add(u64::try_from(MOVE_DEADLINE.as_millis()).unwrap_or(u64::MAX));
        // Recorded before the release: from here every event goes to the
        // target, on this state or any rebuilt one.
        self.moves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(self.address.clone(), drain::Move { target, until_ms });
        self.live = None;
        self.db = None;
        self.replica = None;
        let Some(hold) = self.lease.take() else {
            self.end_move();
            return MoveOut::NotHeld;
        };
        match hold.release().await {
            LeaseReturn::Released | LeaseReturn::Superseded => MoveOut::Released { alarm },
            LeaseReturn::Pending => {
                self.end_move();
                MoveOut::Expiring
            }
        }
    }

    /// Forgets this actor's move under way.
    fn end_move(&self) {
        self.moves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.address);
    }
}

impl ActorService {
    /// A node asked this one to take `address` (a verified, single-use node
    /// call whose signature covers the marker; ADR 0017).
    pub(super) async fn accept_take(
        &self,
        address: &ActorAddress,
        body: &[u8],
    ) -> Response<HyperOutgoingBody> {
        let Some(tenant) = parse_take(body) else {
            return text_response(StatusCode::BAD_REQUEST, "malformed take\n");
        };
        if tenant != address.tenant() {
            return text_response(StatusCode::FORBIDDEN, "not this node's tenant\n");
        }
        if self.inner.cluster.as_ref().is_some_and(|c| c.draining()) {
            return crate::cluster::unavailable("this node is draining; it takes no actor\n");
        }
        match self.take_here(address).await {
            Taken::Held => text_response(StatusCode::OK, "held\n"),
            Taken::HeldBy(holder) => {
                let mut r = text_response(StatusCode::CONFLICT, "held by another node\n");
                if let Ok(v) = hyper::header::HeaderValue::from_str(&holder) {
                    r.headers_mut().insert(HOLDER, v);
                }
                r
            }
            Taken::NoSuchActor => text_response(StatusCode::NOT_FOUND, "no such actor\n"),
            Taken::Refused(why) => {
                tracing::debug!(actor = %address, %why, "take refused");
                crate::cluster::unavailable("cannot take the actor now; retry\n")
            }
        }
    }

    /// Runs the take job for `address` on this node.
    async fn take_here(&self, address: &ActorAddress) -> Taken {
        self.inner.forget_owner(address);
        let job =
            self.inner
                .mailboxes
                .submit_wait(address, ADMISSION_WAIT, |state: &mut ActorState| {
                    Box::pin(state.take())
                });
        let taken = match job.await {
            Ok(taken) => taken,
            Err(e) => Taken::Refused(e.to_string()),
        };
        // Nothing to keep for an actor that does not exist (a mistyped
        // name), or that another node holds.
        if matches!(taken, Taken::NoSuchActor | Taken::HeldBy(_)) {
            self.inner.mailboxes.hibernate(address);
        }
        taken
    }
}

/// While the node is not draining: starts acting on the move requests that
/// concern it (ADR 0017), each in its own task, so a slow move never holds
/// up the drain loop. Requests being acted on are in `active`.
pub(super) async fn moves_pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, active: &Active) {
    // One move per actor: the newest key counts, the others are deleted.
    let mut newest: BTreeMap<(String, String), MoveIntent> = BTreeMap::new();
    for intent in cluster.moves() {
        if intent.tenant != inner.tenant {
            continue;
        }
        let actor = (intent.class.clone(), intent.name.clone());
        match newest.get(&actor) {
            Some(kept) if kept.written_at_ms >= intent.written_at_ms => {
                cluster.delete_move(&intent).await;
            }
            Some(kept) => {
                cluster.delete_move(kept).await;
                newest.insert(actor, intent);
            }
            None => {
                newest.insert(actor, intent);
            }
        }
    }
    for intent in newest.into_values() {
        let key = intent.key();
        let token = ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        {
            let mut active = active.lock().unwrap_or_else(|p| p.into_inner());
            if active.contains_key(&key) || active.len() >= MOVES_PARALLEL {
                continue; // under way, or enough at once: a later pass
            }
            active.insert(key.clone(), token);
        }
        let (inner, cluster, active) = (Arc::clone(inner), Arc::clone(cluster), Arc::clone(active));
        tokio::spawn(async move {
            move_one(&inner, &cluster, &active, &intent, token).await;
            let mut active = active.lock().unwrap_or_else(|p| p.into_inner());
            if active.get(&key) == Some(&token) {
                active.remove(&key);
            }
        });
    }
}

/// Acts on one move request, as its actor's owner or as its target.
async fn move_one(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    active: &Active,
    intent: &MoveIntent,
    token: u64,
) {
    let Ok(address) = ActorAddress::new(&intent.tenant, &intent.class, &intent.name) else {
        cluster.delete_move(intent).await;
        return;
    };
    // A class this node does not serve (yet): left to the nodes that do.
    if !read(&inner.classes).contains_key(address.class()) {
        return;
    }
    let service = ActorService {
        inner: Arc::clone(inner),
    };
    // The target: take the actor unless another node holds it. A mailbox
    // here is not proof of holding it (a request forwarded during the move
    // may be waiting to acquire), so the take job decides; it returns at
    // once if this node already holds the lease.
    if intent.target == cluster.node_id {
        match cluster.other_owner(&address).await {
            Ok(None) => {}
            _ => return, // held elsewhere (that owner acts), or unknown
        }
        match service.take_here(&address).await {
            Taken::Held => {
                tracing::info!(actor = %address, "move: this node holds the actor");
                cluster.delete_move(intent).await;
            }
            Taken::NoSuchActor => {
                tracing::info!(actor = %address, "move: no such actor; dropped");
                cluster.delete_move(intent).await;
            }
            Taken::HeldBy(_) | Taken::Refused(_) => {}
        }
        return;
    }
    // The owner: only for an actor resident here.
    if !inner.mailboxes.contains(&address) {
        return;
    }
    // A target that cannot take actors now (down, draining, not seen yet,
    // another app, an older version): the request stays, tried on later
    // passes until it expires.
    if !cluster.can_take(&intent.target) {
        tracing::debug!(actor = %address, target = %intent.target, "move: the target cannot take actors now");
        return;
    }
    if move_resident(inner, cluster, active, intent, &address, token).await {
        cluster.delete_move(intent).await;
    }
}

/// The owner's side of a move. Whether the request is finished (done, or
/// another node won); `false` keeps it for a later pass.
async fn move_resident(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    active: &Active,
    intent: &MoveIntent,
    address: &ActorAddress,
    token: u64,
) -> bool {
    // Sockets first: their close events run here, with the lease.
    let open = inner.sockets.of(address, None);
    if !open.is_empty() {
        for conn in open {
            inner
                .sockets
                .close(address, conn, 1012, "actor moving; reconnect".into());
        }
        let started = tokio::time::Instant::now();
        while inner.sockets.reserved_by(address) > 0 && started.elapsed() < SOCKETS_CLOSE_WAIT {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let (target, key, active_job) = (intent.target.clone(), intent.key(), Arc::clone(active));
    let job = inner
        .mailboxes
        .submit_if_resident(address, move |state: &mut ActorState| {
            Box::pin(state.move_out(target, key, token, active_job))
        });
    let outcome = match tokio::time::timeout(MOVE_WAIT, job).await {
        Ok(Ok(outcome)) => outcome,
        // Busy past the wait, or gone. This attempt is abandoned (its token
        // leaves `active` when the task ends), so the job, if it runs
        // later, does nothing.
        Ok(Err(_)) | Err(_) => return false,
    };
    let alarm = match outcome {
        MoveOut::Released { alarm } => alarm,
        MoveOut::Expiring => {
            tracing::warn!(actor = %address, "move: the release is pending (retried in the background); the move is tried again later");
            return false;
        }
        MoveOut::Busy | MoveOut::NotHeld | MoveOut::Kept => return false,
    };
    let answer = cluster.take(&intent.target, address).await;
    let done = match &answer {
        TakeAnswer::Held => true,
        TakeAnswer::HeldBy(holder) => *holder == intent.target,
        TakeAnswer::NoSuchActor | TakeAnswer::Failed(_) => false,
    };
    if !done {
        // Events stop going to the target: the next one claims the actor
        // as usual (here, or wherever it lands).
        inner
            .moves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(address);
    }
    // The stub goes either way. After a success the lease names the
    // target, so this node forwards to it.
    inner.mailboxes.hibernate(address);
    match answer {
        TakeAnswer::Held => {}
        TakeAnswer::HeldBy(holder) if holder == intent.target => {}
        TakeAnswer::HeldBy(holder) => {
            tracing::info!(actor = %address, %holder, target = %intent.target, "move: another node took the actor first");
            return true;
        }
        TakeAnswer::NoSuchActor | TakeAnswer::Failed(_) => {
            tracing::info!(actor = %address, target = %intent.target, ?answer, "move: the target did not take the actor; tried again later");
            return false;
        }
    }
    inner
        .moves
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(address);
    if let Some(at) = alarm {
        inner.alarms.clear_if(address.clone(), at);
    }
    tracing::info!(actor = %address, target = %intent.target, "moved");
    true
}
