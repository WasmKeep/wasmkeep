//! Draining a node (ADR 0014): handing resident actors over, and the alarm
//! hand-off between nodes.
//!
//! A draining node claims nothing new (`ensure_owner`). This module empties
//! it of the actors it already holds, a few at a time: close the actor's
//! sockets (their close events run here, with their uploads), then a
//! hand-over job in the actor's own mailbox, after every event already
//! queued: it reads the committed alarm, drops the instance and database,
//! and releases the lease, waiting for the release to be written. The
//! actor is then dropped from memory; events queued behind the job find no
//! lease and go to the chosen peer, which claims. Its alarm, if any, is
//! handed to that peer, which arms it in its own timer. Rooms that
//! hibernated with sockets open (not resident, so not handed over) have
//! their parked records released (ADR 0024) and their sockets closed;
//! their close events are lost.

use std::collections::HashMap;

use super::*;
use crate::cluster::{LeaseReturn, parse_alarm_handoff};
use wasmkeep_cluster::membership::DrainState;

/// Actors handed over at once.
const DRAIN_PARALLEL: usize = 16;
/// How long a hand-over waits for an actor's sockets to finish closing
/// (their close events run on this node) before going on.
const SOCKETS_CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long a hand-over waits for the events queued before it. A hot or
/// long event past this is left resident and tried again on a later pass;
/// the job, still queued, runs when its turn comes and does nothing if the
/// node no longer drains or has no peer.
const HAND_OVER_WAIT: Duration = Duration::from_secs(30);

/// What one hand-over did.
#[derive(Debug)]
pub(super) enum HandOver {
    /// Released; `alarm` is the committed alarm to hand to a peer.
    Done { alarm: Option<u64> },
    /// The release failed: the lease expires on its own. The alarm, if
    /// any, stays in this node's timer (another node cannot take the actor
    /// before then).
    Expiring { alarm: Option<u64> },
    /// Nothing to release: the actor held no lease (a routing stub, or a
    /// lapsed hold, dropped).
    NotHeld,
    /// A socket is still open or closing: its close event must run here,
    /// with the lease. Tried again on the next pass.
    Busy,
    /// The node is no longer draining, or has no peer to hand to: the
    /// actor stays (a job that ran late does nothing).
    Kept,
}

/// A move of one actor under way (ADR 0017).
#[derive(Debug, Clone)]
pub(super) struct Move {
    /// The node it moves to.
    pub(super) target: String,
    /// When the move gives up if it has not ended (ms since the epoch).
    pub(super) until_ms: u64,
}

/// Moves under way on a node, by actor.
pub(super) type Moves = Arc<std::sync::Mutex<HashMap<ActorAddress, Move>>>;

impl ActorState {
    /// The target of a move under way, if one is and its deadline has not
    /// passed (a passed one is cleared).
    pub(super) fn moving_target(&mut self) -> Option<String> {
        let mut moves = self.moves.lock().unwrap_or_else(|p| p.into_inner());
        let target = moves
            .get(&self.address)
            .filter(|m| now_ms() < m.until_ms)
            .map(|m| m.target.clone());
        if target.is_none() {
            moves.remove(&self.address);
        }
        target
    }

    /// Runs in the actor's mailbox, after every event queued before it.
    pub(super) async fn hand_over(&mut self) -> HandOver {
        let Some(cluster) = self.cluster.clone() else {
            return HandOver::Kept;
        };
        // Checked when the job runs, not when it was queued.
        if !cluster.draining() || cluster.chosen_peer(&self.address).is_none() {
            return HandOver::Kept;
        }
        // A socket accepted since the drain closed the others, or one whose
        // client has not answered the close yet: its close event would run
        // after the release and find no lease.
        if self.sockets.reserved_by(&self.address) > 0 {
            for conn in self.sockets.of(&self.address, None) {
                self.sockets
                    .close(&self.address, conn, 1012, "actor moving; reconnect".into());
            }
            return HandOver::Busy;
        }
        if !self.lease.as_ref().is_some_and(LeaseHold::is_valid) {
            // A lapsed hold: drop it (released on drop), nothing to hand.
            self.lease = None;
            return HandOver::NotHeld;
        }
        let alarm = self.committed_alarm().await;
        // Every event before this job finished with its upload: nothing
        // is left to replicate. Close the instance and the database before
        // giving the lease up.
        self.live = None;
        self.db = None;
        self.replica = None;
        let Some(hold) = self.lease.take() else {
            return HandOver::NotHeld;
        };
        match hold.release().await {
            LeaseReturn::Released | LeaseReturn::Superseded => HandOver::Done { alarm },
            LeaseReturn::Pending => HandOver::Expiring { alarm },
        }
    }

    /// The actor's committed alarm, read from its open instance or
    /// database. `None` if it has none or it cannot be read: this node's
    /// timer still holds it then, and hands it off when it fires.
    async fn committed_alarm(&mut self) -> Option<u64> {
        if let Some(live) = self.live.as_mut() {
            return match with_db(&mut live.store, |db| db.alarm()).await {
                Ok(Ok(at)) => at,
                _ => None,
            };
        }
        let db = self.db.take()?;
        let (db, read) = tokio::task::spawn_blocking(move || {
            let mut db = db;
            let read = db.alarm();
            (db, read)
        })
        .await
        .ok()?;
        self.db = Some(db);
        read.ok().flatten()
    }
}

impl ActorService {
    /// A draining node handed `address`'s alarm to this node (a verified,
    /// single-use node call whose signature covers the marker): arm it
    /// here. The alarm path does the rest when it fires: acquire, restore,
    /// run it if still committed, clear stale index entries.
    pub(super) fn accept_alarm(
        &self,
        address: &ActorAddress,
        body: &[u8],
    ) -> Response<HyperOutgoingBody> {
        let Some((tenant, at)) = parse_alarm_handoff(body) else {
            return text_response(StatusCode::BAD_REQUEST, "malformed alarm hand-off\n");
        };
        if tenant != address.tenant() {
            return text_response(StatusCode::FORBIDDEN, "not this node's tenant\n");
        }
        if self.inner.cluster.as_ref().is_some_and(|c| c.draining()) {
            return text_response(StatusCode::CONFLICT, "this node is draining\n");
        }
        // An owner learned before the hand-over would make the alarm path
        // skip the actor as someone else's.
        self.inner.forget_owner(address);
        // Its record may still name the sender, its release pending: the
        // alarm path looks again rather than leave it to the sender.
        self.inner.note_handed(address);
        // An earlier time already registered here stays.
        self.inner.alarms.set_if_earlier(address.clone(), at);
        text_response(StatusCode::OK, "armed\n")
    }
}

/// Once per heartbeat period. While this node is asked to drain, it hands
/// over its resident actors and the alarms its timer holds for actors it
/// does not hold (failed hand-offs, alarms other nodes handed it before it
/// drained), and publishes `draining`, `drained` or `stuck` on heartbeat
/// line 9; it is `drained` only with no actor, no alarm and no socket left
/// (it closes the sockets of rooms that hibernated with them open), so
/// stopping it then leaves nothing to the sweep. Otherwise it acts on move
/// requests (ADR 0017).
pub(super) async fn drain_loop(service: std::sync::Weak<ServiceInner>, cluster: Arc<Cluster>) {
    let every = cluster.heartbeat_period();
    // Moves under way (ADR 0017), by request key.
    let moving: moving::Active = Arc::default();
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return; // shutting down: the clean stop hands everything over
        }
        // Not draining: act on move requests instead (a drain already moves
        // every actor).
        if !cluster.draining() {
            moving::moves_pass(&inner, &cluster, &moving).await;
            continue;
        }
        let addresses = inner.mailboxes.addresses();
        let any_peer = cluster.has_eligible_peer();
        if any_peer {
            // Parked records first (ADR 0024; on every pass, since a park
            // may land just after the drain began): until released, their
            // rooms' events come here and find a node that claims nothing,
            // while the peer finds them held here. The hand-overs below
            // can take a while.
            cluster.release_all_parked("drain").await;
            futures_util::StreamExt::for_each_concurrent(
                futures_util::stream::iter(addresses),
                DRAIN_PARALLEL,
                |address| hand_over_one(Arc::clone(&inner), Arc::clone(&cluster), address),
            )
            .await;
            for (address, at) in inner.alarms.registered().await {
                if !inner.mailboxes.contains(&address) {
                    hand_off(&inner, &cluster, &address, at).await;
                }
            }
            // Rooms that hibernated with sockets open are not resident, so
            // no hand-over closes them: their records were released above;
            // close their sockets, and their clients reconnect to the peer.
            // Their close events find no hold on this draining node and
            // are lost.
            for address in inner.sockets.owners() {
                if !inner.mailboxes.contains(&address) {
                    let closed =
                        inner
                            .sockets
                            .close_owner(&address, 1012, "node draining; reconnect");
                    crate::metrics::owner_closes("drain", closed);
                }
            }
        }
        // After the pass: what is still here decides the state. `resident`
        // counts every registered mailbox, including one delivering an
        // alarm whose timer entry is already gone: a node is never
        // `drained` (and stopped) in the middle of an alarm. Counting only
        // actors that hold a lease would bring that race back (found by
        // the drain simulation).
        // Sockets too, closing ones included: a node that still holds a
        // socket is not drained.
        let left = inner.mailboxes.resident()
            + inner.alarms.registered().await.len()
            + inner.sockets.len();
        // Drained only once no release is pending either: no record is left
        // naming this node while it no longer serves the actor (ADR 0020,
        // section 4).
        let state = match (left, any_peer) {
            (0, _) if cluster.nothing_pending() => DrainState::Drained,
            (0, _) => DrainState::Draining,
            (_, true) => DrainState::Draining,
            (_, false) => DrainState::Stuck,
        };
        cluster.set_drain_state(state);
        drop(inner);
    }
}

/// Hands one resident actor over: sockets first, then the job in its
/// mailbox, then its alarm to the peer. An alarm whose hand-off failed
/// stays in this node's timer, for the next pass.
async fn hand_over_one(inner: Arc<ServiceInner>, cluster: Arc<Cluster>, address: ActorAddress) {
    // Sockets close first, so their close events run here, each with its
    // upload, before the lease goes. The job checks again.
    let open = inner.sockets.of(&address, None);
    if !open.is_empty() {
        for conn in open {
            inner
                .sockets
                .close(&address, conn, 1012, "actor moving; reconnect".into());
        }
        let started = tokio::time::Instant::now();
        while inner.sockets.reserved_by(&address) > 0 && started.elapsed() < SOCKETS_CLOSE_WAIT {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    // Only to a mailbox that is still here: never creates one.
    let job = inner
        .mailboxes
        .submit_if_resident(&address, |state: &mut ActorState| {
            Box::pin(state.hand_over())
        });
    let outcome = match tokio::time::timeout(HAND_OVER_WAIT, job).await {
        Ok(Ok(outcome)) => outcome,
        // Busy past the wait, or the mailbox went away.
        Ok(Err(_)) | Err(_) => return,
    };
    match outcome {
        HandOver::Done { alarm } => {
            inner.mailboxes.hibernate(&address);
            cluster.snapshot_waits.forget(&address);
            tracing::info!(actor = %address, "handed over (draining)");
            if let Some(at) = alarm {
                hand_off(&inner, &cluster, &address, at).await;
            }
        }
        HandOver::Expiring { alarm } => {
            inner.mailboxes.hibernate(&address);
            tracing::warn!(actor = %address, "the actor's release is pending; retried in the background");
            // Nobody can take the actor before the release lands: keep its
            // alarm here, where the timer hands it off when it fires.
            if let Some(at) = alarm {
                inner.alarms.set_if_earlier(address, at);
            }
        }
        HandOver::NotHeld => inner.mailboxes.hibernate(&address),
        HandOver::Busy | HandOver::Kept => {}
    }
}

/// Hands `address`'s alarm, due at `at`, to the chosen peer. On success
/// this node forgets it (only that registration: a newer one stays); on
/// failure it keeps it registered, so its timer still fires it.
async fn hand_off(
    inner: &ServiceInner,
    cluster: &Cluster,
    address: &ActorAddress,
    at: u64,
) -> bool {
    let Some(peer) = cluster.chosen_peer(address) else {
        inner.alarms.set_if_earlier(address.clone(), at);
        return false;
    };
    match cluster.hand_off_alarm(&peer, address, at).await {
        Ok(()) => {
            inner.alarms.clear_if(address.clone(), at);
            true
        }
        Err(e) => {
            tracing::info!(actor = %address, %peer, error = %e, "alarm hand-off failed; kept here");
            cluster.suspect(&peer);
            inner.alarms.set_if_earlier(address.clone(), at);
            false
        }
    }
}
