//! Spread actors return home after a node rejoins ([ADR 0040, section
//! 14](../../../../docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md)).
//!
//! Once per heartbeat period, and again each time one of its moves ends,
//! the pass asks [`wasmkeep_cluster::rebalance::decide`] which spread actors
//! this node holds away from their home may move now, and moves them with
//! the owner's side of a move (ADR 0017), started from memory: no move key
//! is written or read. At most two at once, at most 500 sockets closed a
//! period; paused while a node drains, a live node is not `serving`, or two
//! minor versions run.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::moving::{Awaiting, MoveEnd};
use super::*;
use wasmkeep_cluster::rebalance;

/// This node's rebalance state, shared by the pass and the take path.
pub(super) struct State {
    /// Actors this node acquired through a take call, and when.
    taken: Mutex<HashMap<ActorAddress, tokio::time::Instant>>,
    /// The held spread actors away from their home as of the last pass;
    /// `u64::MAX` before the first.
    misplaced: AtomicU64,
    pass: Mutex<Pass>,
    /// The move jobs' attempt tokens, by a key of their own (never an
    /// operator's request key).
    active: super::moving::Active,
}

impl Default for State {
    fn default() -> Self {
        Self {
            taken: Mutex::default(),
            misplaced: AtomicU64::new(u64::MAX),
            pass: Mutex::new(Pass {
                in_flight: HashSet::new(),
                sockets_left: rebalance::SOCKETS_PER_PERIOD,
                failed: HashMap::new(),
                paused: None,
            }),
            active: Arc::default(),
        }
    }
}

struct Pass {
    in_flight: HashSet<ActorAddress>,
    sockets_left: i64,
    /// Targets a move to failed, until when.
    failed: HashMap<String, tokio::time::Instant>,
    /// The last pause logged.
    paused: Option<rebalance::Pause>,
}

impl State {
    fn pass(&self) -> std::sync::MutexGuard<'_, Pass> {
        self.pass.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn taken_map(&self) -> std::sync::MutexGuard<'_, HashMap<ActorAddress, tokio::time::Instant>> {
        self.taken.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// This node acquired `address` through a take call now.
    pub(super) fn taken(&self, address: &ActorAddress) {
        self.taken_map()
            .insert(address.clone(), tokio::time::Instant::now());
    }

    /// The load key `misplaced` (ADR 0040, section 14): unknown until
    /// the first pass.
    pub(super) fn misplaced(&self) -> Option<u64> {
        let n = self.misplaced.load(Ordering::Relaxed);
        (n != u64::MAX).then_some(n)
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
        let now = tokio::time::Instant::now();
        pass.failed.retain(|_, until| *until > now);
    }
    // A take older than the stay no longer counts.
    inner
        .rebalance
        .taken_map()
        .retain(|_, at| at.elapsed() < settings.stay);
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
    let now = tokio::time::Instant::now();
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
            failed: p.suspected || pass.failed.get(&p.node).is_some_and(|u| *u > now),
        })
        .collect();
    let mine: Vec<rebalance::Held<'_>> = held
        .iter()
        .zip(&keys)
        .map(|(h, key)| rebalance::Held {
            key,
            taken: h.taken,
            busy: h.busy || pass.in_flight.contains(&h.address),
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
        pass.in_flight.insert(h.address.clone());
        pass.sockets_left = pass
            .sockets_left
            .saturating_sub(i64::try_from(h.sockets).unwrap_or(i64::MAX));
        let (inner, cluster, awaiting) =
            (Arc::clone(inner), Arc::clone(cluster), Arc::clone(awaiting));
        let (address, target, parked) = (h.address.clone(), m.target.to_owned(), h.parked);
        tokio::spawn(async move {
            let end = move_home(&inner, &cluster, &awaiting, &address, &target, parked).await;
            {
                let mut pass = inner.rebalance.pass();
                pass.in_flight.remove(&address);
                if end == MoveEnd::NotDone {
                    let horizon = Duration::from_millis(cluster.heartbeat_horizon());
                    pass.failed
                        .insert(target.clone(), tokio::time::Instant::now() + horizon);
                }
            }
            // Closed loop: the next move starts when one ends.
            kick(&inner, &cluster, &awaiting);
        });
    }
}

/// One spread actor this node holds.
struct HeldHere {
    address: ActorAddress,
    parked: bool,
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
    let mut addresses: Vec<(ActorAddress, bool)> = inner
        .mailboxes
        .addresses()
        .into_iter()
        .filter(|a| spread.contains(a.class()) && cluster.holds_here(a))
        .map(|a| (a, false))
        .collect();
    for (a, _) in cluster.parked() {
        if spread.contains(a.class()) && !addresses.iter().any(|(b, _)| *b == a) {
            addresses.push((a, true));
        }
    }
    let requested: HashSet<(String, String)> = cluster
        .moves()
        .into_iter()
        .map(|m| (m.class, m.name))
        .collect();
    let moving = inner.moves.lock().unwrap_or_else(|p| p.into_inner());
    let restoring = inner.restoring.lock().unwrap_or_else(|p| p.into_inner());
    let taken = inner.rebalance.taken_map();
    addresses
        .into_iter()
        .map(|(address, parked)| HeldHere {
            parked,
            taken: taken.get(&address).map(|at| at.elapsed()),
            busy: moving.contains_key(&address)
                || restoring.contains_key(&address)
                || requested.contains(&(address.class().to_owned(), address.name().to_owned())),
            sockets: u64::try_from(inner.sockets.of(&address, None).len()).unwrap_or(u64::MAX),
            address,
        })
        .collect()
}

/// Moves `address` to `target` as an operator's move would, from memory.
async fn move_home(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    awaiting: &Awaiting,
    address: &ActorAddress,
    target: &str,
    parked: bool,
) -> MoveEnd {
    let sockets = u64::try_from(inner.sockets.of(address, None).len()).unwrap_or(u64::MAX);
    tracing::info!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), %target, sockets, "rebalance: moving a spread actor to its home");
    let end = if parked {
        super::moving::move_parked(inner, cluster, awaiting, target, address).await
    } else {
        let key = format!("rebalance/{}", crate::cluster::encode_address(address));
        let token = super::moving::ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        let active = &inner.rebalance.active;
        active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone(), token);
        let end = super::moving::move_resident(
            inner,
            cluster,
            active,
            target,
            key.clone(),
            address,
            token,
        )
        .await;
        let mut active = active.lock().unwrap_or_else(|p| p.into_inner());
        if active.get(&key) == Some(&token) {
            active.remove(&key);
        }
        end
    };
    let outcome = match end {
        MoveEnd::Moved => "moved",
        MoveEnd::Elsewhere => "taken_elsewhere",
        MoveEnd::NotDone => "failed",
    };
    crate::metrics::rebalance_move(&inner.app_labels, outcome, sockets);
    end
}
