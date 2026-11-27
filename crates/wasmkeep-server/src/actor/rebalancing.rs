//! Spread actors return home after a node rejoins ([ADR 0040, section
//! 14](../../../../docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md)).
//!
//! Once per heartbeat period, and again each time one of its moves ends,
//! the pass asks [`wasmkeep_cluster::rebalance::decide`] which spread actors
//! this node holds away from their home may move now, and moves them with
//! the owner's side of a move (ADR 0017), started from memory: no move key
//! is written or read. At most two at once, at most 500 sockets closed a
//! period, per app on this node; paused while a node drains, a live node is
//! not `serving`, or two minor versions run.
//!
//! While a move (this pass's, an operator's) or a drain's hand-over closes
//! an actor's sockets, the actor is marked closing ([`State::mark_closing`]):
//! an upgrade for it here is answered `503` with `Retry-After: 1`, so a
//! client that reconnects at once (relayed back by a node that still names
//! this one) does not reopen a socket the move must wait for. A move that
//! does not finish for a cause of this node's backs that actor off (10 s,
//! doubling); one the target refused or did not answer backs the target
//! off, for moves and for cold sockets alike.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::moving::{Awaiting, MoveEnd};
use super::*;
use wasmkeep_cluster::rebalance;

/// The first backoff, of an actor or a target; it doubles on each failure
/// in a row, up to [`BACKOFF_MAX`].
const BACKOFF_MIN: Duration = Duration::from_secs(10);
const BACKOFF_MAX: Duration = Duration::from_secs(160);

/// A run of failures: when the next try may come, and the next wait.
#[derive(Clone, Copy, Debug)]
struct Backoff {
    until: tokio::time::Instant,
    next: Duration,
}

impl Backoff {
    /// One more failure, from `before` (none: the first).
    fn after(before: Option<Self>, first: Duration) -> Self {
        let wait = before.map_or(first, |b| b.next);
        Self {
            until: tokio::time::Instant::now() + wait,
            next: wait.saturating_mul(2).min(BACKOFF_MAX.max(first)),
        }
    }

    fn active(&self) -> bool {
        self.until > tokio::time::Instant::now()
    }
}

/// The answer of a cold take shared by the upgrades that wait for it: the
/// owner to relay to, or `None` to wake the actor where it landed.
type Cold = Arc<tokio::sync::OnceCell<Option<String>>>;

/// This node's rebalance state, shared by the pass, the take path and the
/// WebSocket upgrade.
pub(super) struct State {
    /// Actors this node acquired through a take call while not their home
    /// in its own view, and when.
    taken: Mutex<HashMap<ActorAddress, tokio::time::Instant>>,
    /// The held spread actors away from their home as of the last pass;
    /// `u64::MAX` before the first.
    misplaced: AtomicU64,
    pass: Mutex<Pass>,
    /// The move jobs' attempt tokens, by a key of their own (never an
    /// operator's request key).
    active: super::moving::Active,
    /// Actors whose sockets a move or a drain's hand-over is closing, each
    /// with the number of marks held on it.
    closing: Mutex<HashMap<ActorAddress, usize>>,
    /// Targets a move or a cold take failed on.
    targets: Mutex<HashMap<String, Backoff>>,
    /// Cold takes under way, one per actor.
    cold: Mutex<HashMap<ActorAddress, Cold>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            taken: Mutex::default(),
            misplaced: AtomicU64::new(u64::MAX),
            pass: Mutex::new(Pass {
                in_flight: HashSet::new(),
                sockets_left: rebalance::SOCKETS_PER_PERIOD,
                backoff: HashMap::new(),
                paused: None,
            }),
            active: Arc::default(),
            closing: Mutex::default(),
            targets: Mutex::default(),
            cold: Mutex::default(),
        }
    }
}

/// A held closing mark ([`State::mark_closing`]).
pub(super) struct ClosingMark<'a> {
    state: &'a State,
    address: ActorAddress,
}

impl Drop for ClosingMark<'_> {
    fn drop(&mut self) {
        let mut closing = lock(&self.state.closing);
        if let Some(n) = closing.get_mut(&self.address) {
            *n -= 1;
            if *n == 0 {
                closing.remove(&self.address);
            }
        }
    }
}

struct Pass {
    in_flight: HashSet<ActorAddress>,
    sockets_left: i64,
    /// Actors a move of did not finish for a cause of this node's.
    backoff: HashMap<ActorAddress, Backoff>,
    /// The last pause logged.
    paused: Option<rebalance::Pause>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl State {
    fn pass(&self) -> std::sync::MutexGuard<'_, Pass> {
        lock(&self.pass)
    }

    /// This node acquired `address` through a take call now, while not
    /// its home in its own view.
    pub(super) fn taken(&self, address: &ActorAddress) {
        lock(&self.taken).insert(address.clone(), tokio::time::Instant::now());
    }

    /// The load key `misplaced` (ADR 0040, section 14): unknown until
    /// the first pass.
    pub(super) fn misplaced(&self) -> Option<u64> {
        let n = self.misplaced.load(Ordering::Relaxed);
        (n != u64::MAX).then_some(n)
    }

    /// Whether a move or a drain's hand-over is closing `address`'s
    /// sockets now.
    pub(super) fn closing(&self, address: &ActorAddress) -> bool {
        lock(&self.closing).contains_key(address)
    }

    /// Runs `open` (a socket's registration) unless `address` is marked
    /// closing, under the mark's lock: a mark taken after it sees the
    /// socket registered, so the move closes it.
    pub(super) fn open_unless_closing<T>(
        &self,
        address: &ActorAddress,
        open: impl FnOnce() -> Result<T, crate::websocket::OpenError>,
    ) -> Result<T, crate::websocket::OpenError> {
        let closing = lock(&self.closing);
        if closing.contains_key(address) {
            return Err(crate::websocket::OpenError::Moving);
        }
        open()
    }

    /// Marks `address` closing until the mark is dropped: a move's or a
    /// drain's hand-over, from before it closes the sockets until its
    /// release (a parked room's move: until its take), an early return or
    /// a cancelled future included. Marks nest.
    pub(super) fn mark_closing(&self, address: &ActorAddress) -> ClosingMark<'_> {
        *lock(&self.closing).entry(address.clone()).or_insert(0) += 1;
        ClosingMark {
            state: self,
            address: address.clone(),
        }
    }

    /// Whether `target` is backed off after a failed move or cold take.
    pub(super) fn target_backed_off(&self, target: &str) -> bool {
        lock(&self.targets).get(target).is_some_and(Backoff::active)
    }

    /// A move or a cold take to `target` failed: it is left out for
    /// `first` (the heartbeat horizon), doubling on each failure in a row.
    pub(super) fn target_failed(&self, target: &str, first: Duration) {
        let mut targets = lock(&self.targets);
        let before = targets.get(target).copied();
        targets.insert(target.to_owned(), Backoff::after(before, first));
    }

    /// `target` took an actor: its failures are forgotten.
    pub(super) fn target_took(&self, target: &str) {
        lock(&self.targets).remove(target);
    }

    /// Runs `take` for `address` unless a cold take of it is under way, in
    /// which case its answer is awaited instead: one take per actor.
    pub(super) async fn cold_take<F>(&self, address: &ActorAddress, take: F) -> Option<String>
    where
        F: std::future::Future<Output = Option<String>>,
    {
        let (cell, mine) = {
            let mut cold = lock(&self.cold);
            match cold.get(address) {
                Some(cell) => (Arc::clone(cell), false),
                None => {
                    let cell = Cold::default();
                    cold.insert(address.clone(), Arc::clone(&cell));
                    (cell, true)
                }
            }
        };
        let answer = cell.get_or_init(|| take).await.clone();
        if mine {
            lock(&self.cold).remove(address);
        }
        answer
    }
}

/// Once per heartbeat period, draining or not: the period's socket budget
/// is refilled, and the pass runs (a draining node only counts).
pub(super) fn tick(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, awaiting: &Awaiting) {
    let settings = cluster.rebalance_settings();
    {
        let mut pass = inner.rebalance.pass();
        pass.sockets_left = rebalance::Budget {
            in_flight: 0,
            sockets_left: pass.sockets_left,
        }
        .refilled(rebalance::SOCKETS_PER_PERIOD)
        .sockets_left;
        // Ended backoffs keep their next wait until a move succeeds, but
        // one older than the longest wait is forgotten.
        pass.backoff
            .retain(|_, b| b.until + BACKOFF_MAX > tokio::time::Instant::now());
    }
    // A take older than the stay no longer counts.
    lock(&inner.rebalance.taken).retain(|_, at| at.elapsed() < settings.stay);
    kick(inner, cluster, awaiting);
}

/// Decides, and starts the moves that may start now.
fn kick(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, awaiting: &Awaiting) {
    let peers = cluster.rebalance_peers();
    let held = held_spread(inner, cluster);
    let keys: Vec<String> = held
        .iter()
        .map(|h| crate::cluster::encode_address(&h.address))
        .collect();
    let mut pass = inner.rebalance.pass();
    let view: Vec<rebalance::Peer<'_>> = peers
        .iter()
        .map(|p| rebalance::Peer {
            node: &p.node,
            version: &p.version,
            drain: p.drain,
            drain_requested: p.drain_requested,
            reachable: p.reachable,
            serving_for: p.serving_for,
            can_take: p.can_take,
            load: p.load,
            failed: p.suspected || inner.rebalance.target_backed_off(&p.node),
        })
        .collect();
    let mine: Vec<rebalance::Held<'_>> = held
        .iter()
        .zip(&keys)
        .map(|(h, key)| rebalance::Held {
            key,
            taken: h.taken,
            busy: h.busy
                || pass.in_flight.contains(&h.address)
                || pass.backoff.get(&h.address).is_some_and(Backoff::active),
            sockets: h.sockets,
        })
        .collect();
    let local = rebalance::Local {
        node: &cluster.node_id,
        version: &cluster.own_version,
        on: cluster.rebalance_on(),
        draining: cluster.draining(),
        lease_valid: cluster.node_lease_valid(),
    };
    let budget = rebalance::Budget {
        in_flight: pass.in_flight.len(),
        sockets_left: pass.sockets_left,
    };
    let verdict = rebalance::decide(local, &view, &mine, budget, cluster.rebalance_settings());
    inner
        .rebalance
        .misplaced
        .store(verdict.misplaced, Ordering::Relaxed);
    crate::metrics::rebalance_state(&inner.app_labels, verdict.misplaced, verdict.paused);
    if verdict.paused != pass.paused {
        if verdict.misplaced > 0 || pass.paused.is_some() {
            match verdict.paused {
                Some(why) => {
                    tracing::info!(app = %crate::logging::named(&inner.app_name), misplaced = verdict.misplaced, reason = why.as_str(), "rebalance paused");
                }
                None => {
                    tracing::info!(app = %crate::logging::named(&inner.app_name), misplaced = verdict.misplaced, "rebalance resumed");
                }
            }
        }
        pass.paused = verdict.paused;
    }
    for m in &verdict.moves {
        let Some(i) = keys.iter().position(|k| k == m.key) else {
            continue;
        };
        let h = &held[i];
        // Still this node's, under the pass's lock: a hibernation, a lapse
        // or another move may have taken it since the list was read.
        if !(cluster.holds_here(&h.address) || cluster.is_parked(&h.address)) {
            continue;
        }
        pass.in_flight.insert(h.address.clone());
        let charged = i64::try_from(h.sockets).unwrap_or(i64::MAX);
        pass.sockets_left = pass.sockets_left.saturating_sub(charged);
        let (inner, cluster, awaiting) =
            (Arc::clone(inner), Arc::clone(cluster), Arc::clone(awaiting));
        let (address, target) = (h.address.clone(), m.target.to_owned());
        tokio::spawn(async move {
            let (end, closed) = move_home(&inner, &cluster, &awaiting, &address, &target).await;
            let horizon = Duration::from_millis(cluster.heartbeat_horizon());
            {
                let mut pass = inner.rebalance.pass();
                pass.in_flight.remove(&address);
                // What it closed is charged, not what it was expected to.
                let closed = i64::try_from(closed).unwrap_or(i64::MAX);
                pass.sockets_left = pass
                    .sockets_left
                    .saturating_add(charged)
                    .saturating_sub(closed);
                match end {
                    MoveEnd::Moved | MoveEnd::Elsewhere => {
                        pass.backoff.remove(&address);
                    }
                    MoveEnd::Busy => {
                        let before = pass.backoff.get(&address).copied();
                        pass.backoff
                            .insert(address.clone(), Backoff::after(before, BACKOFF_MIN));
                    }
                    MoveEnd::Refused => {}
                }
            }
            match end {
                MoveEnd::Moved => inner.rebalance.target_took(&target),
                MoveEnd::Refused => {
                    inner.rebalance.target_failed(&target, horizon);
                    cluster.suspect(&target);
                }
                MoveEnd::Elsewhere | MoveEnd::Busy => {}
            }
            // Closed loop: the next move starts when one ends.
            kick(&inner, &cluster, &awaiting);
        });
    }
}

/// One spread actor this node holds.
struct HeldHere {
    address: ActorAddress,
    taken: Option<Duration>,
    busy: bool,
    sockets: u64,
}

/// The spread actors this node holds now: resident with the hold, or
/// parked with sockets (ADR 0024).
fn held_spread(inner: &ServiceInner, cluster: &Cluster) -> Vec<HeldHere> {
    let spread: HashSet<String> = read(&inner.classes)
        .iter()
        .filter(|(_, c)| c.spread)
        .map(|(name, _)| name.clone())
        .collect();
    if spread.is_empty() {
        return Vec::new();
    }
    let mut addresses: Vec<ActorAddress> = inner
        .mailboxes
        .addresses()
        .into_iter()
        .filter(|a| spread.contains(a.class()) && cluster.holds_here(a))
        .collect();
    for (a, _) in cluster.parked() {
        if spread.contains(a.class()) && !addresses.contains(&a) {
            addresses.push(a);
        }
    }
    let requested: HashSet<(String, String)> = cluster
        .moves()
        .into_iter()
        .map(|m| (m.class, m.name))
        .collect();
    let moving = lock(&inner.moves);
    let restoring = lock(&inner.restoring);
    let taken = lock(&inner.rebalance.taken);
    addresses
        .into_iter()
        .map(|address| HeldHere {
            taken: taken.get(&address).map(|at| at.elapsed()),
            busy: moving.contains_key(&address)
                || restoring.contains_key(&address)
                || requested.contains(&(address.class().to_owned(), address.name().to_owned())),
            sockets: u64::try_from(inner.sockets.of(&address, None).len()).unwrap_or(u64::MAX),
            address,
        })
        .collect()
}

/// Moves `address` to `target` as an operator's move would, from memory:
/// how it ended, and how many sockets it closed.
async fn move_home(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    awaiting: &Awaiting,
    address: &ActorAddress,
    target: &str,
) -> (MoveEnd, u64) {
    // Parked or resident now, not when the list was read: a parked room
    // may have woken since, and a resident one hibernated with sockets.
    let parked = cluster.is_parked(address);
    if !parked && !cluster.holds_here(address) {
        return (MoveEnd::Busy, 0);
    }
    let sockets = u64::try_from(inner.sockets.of(address, None).len()).unwrap_or(u64::MAX);
    tracing::info!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), %target, sockets, parked, "rebalance: moving a spread actor to its home");
    // The closing mark is held by the move itself (`move_parked`,
    // `move_resident`).
    let (end, closed) = if parked {
        super::moving::move_parked(inner, cluster, awaiting, target, address).await
    } else {
        let key = format!("rebalance/{}", crate::cluster::encode_address(address));
        let token = super::moving::ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        let active = &inner.rebalance.active;
        lock(active).insert(key.clone(), token);
        let ended = super::moving::move_resident(
            inner,
            cluster,
            active,
            target,
            key.clone(),
            address,
            token,
        )
        .await;
        let mut active = lock(active);
        if active.get(&key) == Some(&token) {
            active.remove(&key);
        }
        ended
    };
    let outcome = match end {
        MoveEnd::Moved => "moved",
        MoveEnd::Elsewhere => "taken_elsewhere",
        MoveEnd::Busy => "busy",
        MoveEnd::Refused => "failed",
    };
    crate::metrics::rebalance_move(&inner.app_labels, outcome, closed);
    (end, closed)
}
