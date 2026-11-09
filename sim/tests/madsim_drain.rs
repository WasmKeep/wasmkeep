//! Draining a node under madsim (ADR 0014): node `d` drains, peers `p1`
//! and `p2` take its actors' alarms, and every ownership goes through node
//! leases (ADR 0020, `wasmkeep_sim::node`, the server's own hold rules) over
//! the simulated bucket: each node runs its lease on a task of its own,
//! holds act by `may_act`, a release that does not land is retried in the
//! background, and a node stopped with its lease live is fenced by a peer.
//!
//! `d` starts holding every actor, a quarter of them with no alarm and the
//! rest with a committed alarm due before, during or after the hand-over.
//! From `DRAIN_START_MS` it hands its actors over, `DRAIN_PARALLEL` at a
//! time, one pass per heartbeat period, as `actor/drain.rs` does: the job
//! waits behind the actor's earlier events, reads the committed alarm,
//! releases the lease (awaited), then sends the alarm to the chosen peer.
//! On success `d` clears its own timer entry; on failure it keeps it and
//! suspects the peer for a heartbeat horizon.
//!
//! Every node runs a timer (`AlarmHandle`: set, set-if-earlier, fire). When
//! it fires on a node that holds the actor, the alarm runs. On a draining
//! node that does not hold it (`actor/alarm_delivery.rs`), the alarm goes
//! to the chosen peer; on failure the node suspects that peer and tries
//! again in `HANDOFF_RETRY_MS`; with no eligible peer it claims the actor
//! and runs the alarm itself. Elsewhere the node acquires the lease (or
//! finds another owner and does nothing) and runs the alarm if it is still
//! committed. A peer refuses a hand-off (409) while it drains itself. The
//! bucket's alarm sweep is the last resort, modelled coarsely: every
//! `SWEEP_MS`, a serving peer delivers what is `OVERDUE_MS` past due.
//!
//! The chosen peer is the rendezvous choice (the server's hash) among the
//! nodes a node sees as eligible: not suspect, not draining in its last
//! `control/` listing (every `LISTING_MS`, so a node learns of a drain up
//! to 3 s late), not stopped longer than a heartbeat horizon. Faults,
//! drawn from the seed: hand-off messages are late now and then; a peer is
//! unreachable for node calls for a while (it refuses connections, and
//! keeps its timer and the bucket), or drains for a while and then either
//! undrains or is stopped (its timer gone); `d` may be stopped some time
//! after it reports `drained`; a peer due to stop is stopped once it
//! reports `drained`.
//!
//! A draining node, `d` or a peer, also makes a pass over its timer after
//! its resident actors: every alarm registered for an actor it does not
//! hold goes to the chosen peer (cleared only if still at the same time;
//! kept on failure). It reports `drained` only with no resident actor and
//! an empty timer, and the operator stops only a node reporting drained.
//! A release that fails keeps the alarm in `d`'s timer; a node that finds
//! the lease held by a node it sees draining looks again in 1 s.
//!
//! Nodes also lose the bucket for a while (`clogs`, drawn from the seed,
//! up to 950 ms: often long enough for a lease to lapse, which ends every
//! hold into the pending releases, too short to be fenced; a live node
//! fenced fails the run, as restarts are not modelled here). A release that does
//! not land is the server's `HandOver::Expiring`: the alarm stays in the
//! timer, and the record names `d` until the background retry lands.
//! `drained` needs, as in the server, no release pending either: the
//! operator stops a drained node by killing it, and a record a killed node
//! still names frees only once a peer fences it.
//!
//! Checked: never two nodes acting as owner at once (claims from each
//! hold's acquisition to its release, extended while it may act, as in
//! `madsim_node_lease.rs`); every committed alarm delivered (by the end of
//! the run, the sweep included); and an alarm due during or after the drain
//! delivered within `HANDOFF_BOUND_MS` of its time while a peer is up and
//! accepting over that window and no node is cut off from the bucket near
//! it, with no exception. Five unsafe variants must each be caught by some
//! seed: (a) dropping a refused hand-off instead of retrying it, (b)
//! clearing a timer entry before the peer confirmed, (c) a peer that
//! accepts a hand-off while draining, (d) the rule before the fix: no pass
//! over the timer, and `drained` with alarms still registered, (e)
//! `drained` with a release still pending.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_drain --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use std::rc::Rc;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::node_lease::{AcquireError, OwnerRecord};
use wasmkeep_sim::check::{double_ownership, stop};
use wasmkeep_sim::driver::{Claim, Env};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node};

/// Lease clocks start here (no skew: `madsim_node_lease.rs` covers skew).
const EPOCH_MS: u64 = 1_000_000;
/// Node lease objects.
const NODES: &str = "nodes/";
/// Every node's start check waits ttl plus grace: the scenario's clock
/// (`World::now`) starts this long after the processes, once `d` holds
/// its actors.
const SETUP_MS: u64 = 3_000;
/// How long ownership record writes wait for the bucket.
const RECORD_WAIT: Duration = Duration::from_millis(300);
/// How long a node loses the bucket: often past what its lease's validity
/// has left, so it lapses, and short of what a peer fences (a lease period
/// plus an attempt since its last write, and past `written_at` plus ttl
/// and grace; the last write may be a renewal period old).
const CLOG_MIN_MS: u64 = 400;
const CLOG_MAX_MS: u64 = 950;
/// After a node gets the bucket back, how long its pending releases may
/// take to land and a peer's acquire, retried every second, to follow.
const CLOG_TAIL_MS: u64 = 1_500;
const D: usize = 0;
const NAMES: [&str; 3] = ["d", "p1", "p2"];
const ACTORS: u64 = 36;
/// When `d` learns it is draining.
const DRAIN_START_MS: u64 = 1_000;
/// Heartbeat period: one drain pass, one `control/` listing, per period.
const LISTING_MS: u64 = 3_000;
/// The server's `DRAIN_PARALLEL`.
const DRAIN_PARALLEL: usize = 16;
/// Heartbeat horizon: how long a suspect stays suspect, and how long a
/// stopped node stays in the peer list.
const HORIZON_MS: u64 = 10_000;
/// The server's `HANDED_RECHECK_MS`.
const HANDED_RECHECK_MS: u64 = 30_000;
/// The server's `HANDOFF_RETRY_MS`.
const HANDOFF_RETRY_MS: u64 = 500;
/// Invariant 3's bound: 1 s plus the retry interval, plus room for one
/// refused round trip and one request that are both late (`late_ms`).
const HANDOFF_BOUND_MS: u64 = 1_000 + HANDOFF_RETRY_MS + 500;
/// The sweep's last resort (ttl + grace, and the sweep period, with the
/// server's defaults).
const OVERDUE_MS: u64 = 15_000;
const SWEEP_MS: u64 = 10_000;
/// Earlier events in an actor's mailbox before its hand-over job.
const BACKLOG_MAX_MS: u64 = 300;
const TICK_MS: u64 = 10;
/// After this, once every alarm is delivered, the run ends.
const QUIET_FROM_MS: u64 = 25_000;
const RUN_MS: u64 = 80_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// The server's rule.
    Production,
    /// Unsafe (a): a refused hand-off is dropped, not retried.
    DropRefused,
    /// Unsafe (b): a draining node clears its timer entry before the peer
    /// confirmed (and, believing it kept, does not register it again).
    ClearBeforeAck,
    /// Unsafe (c): a node accepts hand-offs while it drains.
    AcceptWhileDraining,
    /// Unsafe (d), the rule before the fix: no pass over the timer, and
    /// `drained` as soon as no actor is resident, alarms still registered.
    DrainedWithAlarms,
    /// Unsafe (e): `drained` with a release still pending.
    DrainedWithPending,
    /// Unsafe (e'), in the node: a retry pass's entries are not counted
    /// while they are out of the pending set, so a drain that asks
    /// mid-pass reads nothing pending.
    PendingUncounted,
    /// Unsafe (f), the rule before this port: a peer that finds another
    /// node named in the record looks again only if its `control/` listing
    /// shows that node draining, not after a hand-off of that actor, so it
    /// drops an alarm whose sender's release is still pending.
    ListedDrainingOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Refuses node calls (connection refused); its timer and leases work.
    Unreachable,
    /// Drains; at the end, undrains, or (`stops`) is stopped as soon as it
    /// reports drained (it drains until then).
    Draining { stops: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    from: u64,
    until: u64,
    fault: Fault,
}

impl Window {
    fn end(&self) -> u64 {
        match self.fault {
            Fault::Draining { stops: true } => u64::MAX,
            _ => self.until,
        }
    }
}

/// The seed's fault schedule, fixed before the run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Schedule {
    /// Per node (`d`'s is empty).
    windows: [Vec<Window>; 3],
    /// `d` is stopped this long after it reports drained.
    d_stop_after: Option<u64>,
    /// When each node lists `control/` (offset in the period).
    listing_offset: [u64; 3],
    /// Actor -> due time of its committed alarm.
    dues: BTreeMap<u64, u64>,
    /// (node, from, until): the node's traffic to the bucket is held.
    clogs: Vec<(usize, u64, u64)>,
}

impl Schedule {
    fn draw(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x0D2A_1A5E);
        let mut dues = BTreeMap::new();
        for actor in 0..ACTORS {
            match rng.below(8) {
                0 | 1 => {}
                // Before the drain: `d` runs it as the owner.
                2 => {
                    dues.insert(actor, 100 + rng.below(DRAIN_START_MS - 100));
                }
                // During the hand-over.
                3 | 4 => {
                    dues.insert(actor, DRAIN_START_MS + rng.below(1_500));
                }
                // After it.
                _ => {
                    dues.insert(actor, 2_500 + rng.below(12_000));
                }
            }
        }
        let mut windows: [Vec<Window>; 3] = Default::default();
        let mut stopped = false;
        for (node, list) in windows.iter_mut().enumerate().skip(1) {
            // Half the seeds start with a fault early, so it meets the
            // hand-over.
            let mut at = if rng.chance(50) {
                300 + rng.below(1_500)
            } else {
                2_000 + rng.below(6_000)
            };
            for _ in 0..1 + rng.below(3) {
                let len = 800 + rng.below(4_000);
                let fault = if rng.chance(50) {
                    Fault::Unreachable
                } else {
                    // At most one peer is stopped, so the sweep keeps a node.
                    let stops =
                        !stopped && node == 1 + usize::from(seed % 2 == 1) && rng.chance(60);
                    stopped |= stops;
                    Fault::Draining { stops }
                };
                list.push(Window {
                    from: at,
                    until: at + len,
                    fault,
                });
                if fault == (Fault::Draining { stops: true }) {
                    break;
                }
                at += len + 500 + rng.below(5_000);
            }
        }
        // Some stops follow `drained` at once, so one can land while a
        // release is still pending.
        let d_stop_after = rng.chance(40).then(|| {
            if rng.chance(50) {
                rng.below(400)
            } else {
                rng.below(3_000)
            }
        });
        let listing_offset = [
            rng.below(LISTING_MS),
            rng.below(LISTING_MS),
            rng.below(LISTING_MS),
        ];
        // `d` loses the bucket around its hand-over in half the seeds (its
        // releases then wait in the background), half of those from its
        // first drain pass on, so it can report drained while cut off; a
        // peer now and then.
        let clog_len = |rng: &mut Rng| CLOG_MIN_MS + rng.below(CLOG_MAX_MS - CLOG_MIN_MS);
        let mut clogs = Vec::new();
        if rng.chance(50) {
            let from = if rng.chance(50) {
                DRAIN_START_MS.max(listing_offset[D]) + rng.below(200)
            } else {
                DRAIN_START_MS.saturating_sub(500) + rng.below(3_500)
            };
            clogs.push((D, from, from + clog_len(&mut rng)));
        }
        for node in 1..3 {
            if rng.chance(25) {
                let from = rng.below(10_000);
                clogs.push((node, from, from + clog_len(&mut rng)));
            }
        }
        clogs.sort_by_key(|&(_, from, _)| from);
        Self {
            windows,
            d_stop_after,
            listing_offset,
            dues,
            clogs,
        }
    }

    /// When an alarm due at `due`, delivered by `by`, must be delivered
    /// by: within the hand-off bound, counted from the end of any bucket
    /// outage it meets of a node that handled it (`d`, `by`, and the nodes
    /// in `touched`: that fired it or tried to take it), plus the time a
    /// pending release takes to land, instead of from `due`. A third node
    /// counts (seed 14163 at 20,000 seeds: p2 fired the alarm, lost the
    /// bucket, drained, and handed it to p1, 65 ms past a deadline that
    /// ignored p2's outage), a node that never handled it does not
    /// (counting every node's outage would excuse deliveries no outage
    /// delayed). Outages are in start order.
    fn deadline(&self, due: u64, by: usize, touched: &BTreeSet<usize>) -> u64 {
        let mut from = due;
        for &(node, start, until) in &self.clogs {
            let handled = node == D || node == by || touched.contains(&node);
            if handled && start <= from + HANDOFF_BOUND_MS && until + CLOG_TAIL_MS > from {
                from = from.max(until + CLOG_TAIL_MS);
            }
        }
        from + HANDOFF_BOUND_MS
    }

    /// When `node` may be stopped (once it reports drained), not `d`.
    fn stop_at(&self, node: usize) -> Option<u64> {
        self.windows[node].iter().find_map(|w| match w.fault {
            Fault::Draining { stops: true } => Some(w.until),
            _ => None,
        })
    }

    /// A window that ends in a stop lasts until the end.
    fn in_window(&self, node: usize, t: u64, f: impl Fn(Fault) -> bool) -> bool {
        self.windows[node]
            .iter()
            .any(|w| w.from <= t && t < w.end() && f(w.fault))
    }

    /// Whether `node` is up, reachable and serving throughout `[t0, t1]`.
    fn accepting(&self, node: usize, t0: u64, t1: u64) -> bool {
        !self.windows[node]
            .iter()
            .any(|w| w.from <= t1 && w.end() > t0)
    }
}

/// How an entry got into a node's timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prov {
    /// The node's own (it owned the actor), or a retry of one.
    Own,
    /// A hand-off it accepted while serving.
    AcceptedServing,
    /// A hand-off it accepted while draining (only unsafe (c)).
    AcceptedDraining,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    due: u64,
    prov: Prov,
}

#[derive(Default)]
struct State {
    /// Actor -> due time, until delivered.
    committed: BTreeMap<u64, u64>,
    /// Actor -> (first delivery, node).
    delivered: BTreeMap<u64, (u64, usize)>,
    /// Actor -> the nodes that fired its alarm or tried to take it: whose
    /// bucket outages may delay its delivery.
    touched: BTreeMap<u64, BTreeSet<usize>>,
    timers: [BTreeMap<u64, Entry>; 3],
    /// What each node holds: actor -> hold (`d` keeps its actors resident
    /// until a drain pass hands them over; a peer holds one for an alarm).
    held: [BTreeMap<u64, Held>; 3],
    /// Actor mailboxes in use, per node.
    busy: BTreeSet<(usize, u64)>,
    /// Alarm events being processed: id -> (node, actor, provenance).
    in_flight: BTreeMap<u64, (usize, u64, Prov)>,
    next_id: u64,
    /// Per node: actors a draining node handed it an alarm for, until when
    /// it looks again at a record still naming another node.
    handed: [BTreeMap<u64, u64>; 3],
    /// Per observer: suspect -> until.
    suspects: [BTreeMap<usize, u64>; 3],
    claims: BTreeMap<u64, Vec<Claim>>,
    stopped: [Option<u64>; 3],
    /// Alarms left in a stopped node's timer: (actor, stopped at, prov).
    orphans: Vec<(u64, u64, Prov)>,
    /// Per node: since when it reports drained (`None`: it does not).
    drained_since: [Option<u64>; 3],
    handoffs_ok: u64,
    handoffs_failed: u64,
    self_claims: u64,
    swept: u64,
    /// Acquires that found another owner.
    not_owner: u64,
    /// Hand-overs whose release did not land (`HandOver::Expiring`).
    expiring: u64,
    /// Hand-overs of a hold a lapse had ended (nothing handed).
    lapsed: u64,
    /// `d` had not taken every actor by the scenario's start.
    setup_late: bool,
    /// A node that lost its lease while running (fenced by a peer).
    fenced_live: Option<usize>,
}

struct World {
    start: Instant,
    rule: Rule,
    schedule: Schedule,
    st: Mutex<State>,
}

impl World {
    /// The scenario's time: from `SETUP_MS` after the processes started.
    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_millis())
            .unwrap()
            .saturating_sub(SETUP_MS)
    }

    fn draining(&self, node: usize, t: u64) -> bool {
        if node == D {
            t >= DRAIN_START_MS
        } else {
            self.schedule
                .in_window(node, t, |f| matches!(f, Fault::Draining { .. }))
        }
    }

    /// Whether `observer`'s last `control/` listing (and heartbeat line 9)
    /// showed `node` draining at `t`.
    fn seen_draining(&self, observer: usize, node: usize, t: u64, st: &State) -> bool {
        let off = self.schedule.listing_offset[observer];
        let listed = if t < off {
            0
        } else {
            off + (t - off) / LISTING_MS * LISTING_MS
        };
        self.draining(node, listed) || st.stopped[node].is_some_and(|s| s <= listed)
    }

    /// The chosen peer of `observer` for `actor` at `t`, if any.
    fn chosen_peer(&self, observer: usize, actor: u64, t: u64) -> Option<usize> {
        let mut st = self.st.lock().unwrap();
        st.suspects[observer].retain(|_, until| *until > t);
        let eligible: Vec<&str> = (0..3)
            .filter(|&n| {
                n != observer
                    && !self.seen_draining(observer, n, t, &st)
                    && !st.suspects[observer].contains_key(&n)
                    && st.stopped[n].is_none_or(|s| t < s + HORIZON_MS)
            })
            .map(|n| NAMES[n])
            .collect();
        let chosen = rendezvous(&key(actor), eligible)?;
        NAMES.iter().position(|n| *n == chosen)
    }

    fn suspect(&self, observer: usize, peer: usize) {
        let until = self.now() + HORIZON_MS;
        self.st.lock().unwrap().suspects[observer].insert(peer, until);
    }

    fn set_if_earlier(&self, node: usize, actor: u64, due: u64, prov: Prov) {
        let mut st = self.st.lock().unwrap();
        if st.stopped[node].is_some() {
            return;
        }
        match st.timers[node].get(&actor) {
            Some(e) if e.due <= due => {}
            _ => {
                st.timers[node].insert(actor, Entry { due, prov });
            }
        }
    }

    /// `AlarmHandle::clear_if`: only the registration for `due`.
    fn clear_if(&self, node: usize, actor: u64, due: u64) {
        let mut st = self.st.lock().unwrap();
        if st.timers[node].get(&actor).is_some_and(|e| e.due == due) {
            st.timers[node].remove(&actor);
        }
    }

    fn clear_timer(&self, node: usize, actor: u64) {
        self.st.lock().unwrap().timers[node].remove(&actor);
    }

    /// The peer's side of `POST /_wasmkeep/alarm`.
    fn receive(&self, to: usize, actor: u64, at: u64) -> bool {
        let t = self.now();
        let draining = self.draining(to, t);
        if self.st.lock().unwrap().stopped[to].is_some()
            || self.schedule.in_window(to, t, |f| f == Fault::Unreachable)
        {
            return false;
        }
        if draining && self.rule != Rule::AcceptWhileDraining {
            return false; // 409
        }
        let prov = if draining {
            Prov::AcceptedDraining
        } else {
            Prov::AcceptedServing
        };
        // The sender drains: a record still naming it is being given up
        // (`ServiceInner::note_handed`).
        self.st.lock().unwrap().handed[to].insert(actor, t + HANDED_RECHECK_MS);
        self.set_if_earlier(to, actor, at, prov);
        true
    }

    /// `node` acts as `actor`'s owner from now until its lease's validity
    /// (extending its claim for that hold if it still runs).
    fn claim(&self, node: usize, actor: u64, held: &Held, lease: &Node) {
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        claim_in(st.claims.entry(actor).or_default(), node, held, lease, now);
    }

    fn stop_claims(&self, node: usize, actor: u64) {
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        if let Some(claims) = st.claims.get_mut(&actor) {
            stop(claims, u32::try_from(node).unwrap(), now);
        }
    }

    async fn lock(&self, node: usize, actor: u64) {
        while !self.st.lock().unwrap().busy.insert((node, actor)) {
            sleep(Duration::from_millis(1)).await;
        }
    }

    fn unlock(&self, node: usize, actor: u64) {
        self.st.lock().unwrap().busy.remove(&(node, actor));
    }

    /// Stops `node`: its tasks are gone (killed by the caller), and with
    /// them its timer and what it was processing.
    fn stopped(&self, node: usize) {
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        st.stopped[node] = Some(now);
        let mut orphans: Vec<(u64, u64, Prov)> = std::mem::take(&mut st.timers[node])
            .into_iter()
            .map(|(actor, e)| (actor, now, e.prov))
            .collect();
        orphans.extend(
            st.in_flight
                .values()
                .filter(|(n, _, _)| *n == node)
                .map(|&(_, actor, prov)| (actor, now, prov)),
        );
        st.orphans.extend(orphans);
        st.in_flight.retain(|_, (n, _, _)| *n != node);
        st.held[node].clear();
        let n = u32::try_from(node).unwrap();
        for claims in st.claims.values_mut() {
            stop(claims, n, now);
        }
    }
}

/// Records `node`'s claim through `held` at `now`, until its lease's
/// validity: the last claim for that hold is extended if it still runs, so
/// a long hold stays one claim.
fn claim_in(claims: &mut Vec<Claim>, node: usize, held: &Held, lease: &Node, now: u64) {
    let Some(until) = lease.valid_until_ms().map(scenario_ms) else {
        return;
    };
    let node = u32::try_from(node).unwrap();
    match claims.iter_mut().rev().find(|c| c.node == node) {
        Some(c) if c.generation == held.generation && c.until_ms >= now => {
            c.until_ms = c.until_ms.max(until);
        }
        _ => claims.push(Claim {
            node,
            instance: 0,
            generation: held.generation,
            from_ms: now,
            until_ms: until,
        }),
    }
}

/// A node clock's reading in the scenario's time.
fn scenario_ms(clock_ms: u64) -> u64 {
    clock_ms.saturating_sub(EPOCH_MS + SETUP_MS)
}

/// The actor's ownership record (ADR 0020).
fn key(actor: u64) -> String {
    format!("owners/t/c/actor-{actor}")
}

/// The server's rendezvous choice (`cluster::rendezvous`).
fn rendezvous<'a>(key: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let weight = |node: &str| {
        let mut h = key
            .bytes()
            .chain([0])
            .chain(node.bytes())
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        h ^ (h >> 31)
    };
    candidates
        .into_iter()
        .max_by_key(|node| (weight(node), *node))
}

/// Sleeps until the scenario's time `t` (`now` stands still at 0 during
/// the setup, so a sleep counted from it would end early).
async fn sleep_until(start: Instant, t: u64) {
    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap();
    sleep(Duration::from_millis(
        (SETUP_MS + t).saturating_sub(elapsed),
    ))
    .await;
}

thread_local! {
    /// Looks again only the hand-off rule asked for (the holder not listed
    /// as draining), in this simulation (one thread per simulation).
    static HANDED_RECHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn random_below(n: u64) -> u64 {
    madsim::rand::random::<u64>() % n.max(1)
}

/// One way of a node call: usually quick, one in twenty late.
fn late_ms() -> u64 {
    if random_below(20) == 0 {
        100 + random_below(400)
    } else {
        1 + random_below(20)
    }
}

#[derive(Debug, Clone, Copy)]
struct SimClock(Instant);

impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// The simulation's services for a node.
#[derive(Clone, Copy)]
struct SimEnv(Instant);

impl Env for SimEnv {
    fn global_ms(&self) -> u64 {
        SimClock(self.0).now_ms()
    }

    async fn sleep_ms(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }
}

type Node = LeaseNode<EpBucket, SimClock>;

/// ttl 1 s, grace 500 ms, safety 200 ms.
fn config() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
}

/// Starts `names[me]`'s process on node leases: the start check, the first
/// write, and the lease on a task of its own (it dies with the node).
async fn start_node(start: Instant, names: [&str; 3], me: usize, breaks: Breaks) -> Rc<Node> {
    let config = config();
    // Lease writes and peers' reads bounded as the server bounds a lease
    // write (a third of ttl minus safety).
    let attempt = Duration::from_millis((config.ttl_ms() - config.safety_ms()) / 3);
    let setup = NodeSetup {
        id: names[me].to_owned(),
        peers: (0..3)
            .filter(|&n| n != me)
            .map(|n| names[n].to_owned())
            .collect(),
        nodes_prefix: NODES.to_owned(),
        config,
        attempt_ms: u64::try_from(attempt.as_millis()).unwrap(),
        breaks,
    };
    let buckets = NodeBuckets {
        lease: EpBucket::connect_with_timeout(attempt).await,
        task_records: EpBucket::connect_with_timeout(RECORD_WAIT).await,
        peers: EpBucket::connect_with_timeout(attempt).await,
        records: EpBucket::connect_with_timeout(RECORD_WAIT).await,
    };
    let instance = format!("{:08x}", madsim::rand::random::<u32>());
    let (node, task) = LeaseNode::start(
        setup,
        &instance,
        SimClock(start),
        buckets,
        format!("{}\n", names[me]).into_bytes(),
        &SimEnv(start),
    )
    .await;
    madsim::task::spawn_local(task.run(SimEnv(start)));
    Rc::new(node)
}

/// The holder a `Held` error names, as a node index.
fn holder(names: [&str; 3], e: &AcquireError) -> Option<usize> {
    match e {
        AcquireError::Held { node, .. } => names.iter().position(|n| n == node),
        _ => None,
    }
}

/// A signed hand-off call from `from` to `to`. No call here outlasts the
/// server's 30 s forward timeout, so none times out.
async fn hand_off(world: &World, to: usize, actor: u64, at: u64) -> bool {
    sleep(Duration::from_millis(late_ms())).await;
    let ok = world.receive(to, actor, at);
    sleep(Duration::from_millis(late_ms())).await;
    let mut st = world.st.lock().unwrap();
    if ok {
        st.handoffs_ok += 1;
    } else {
        st.handoffs_failed += 1;
    }
    ok
}

/// Runs the alarm if it is still committed and due (the owner's event).
async fn run(world: &World, node: usize, actor: u64) {
    let now = world.now();
    let rearm = {
        let mut st = world.st.lock().unwrap();
        match st.committed.get(&actor) {
            Some(&due) if due <= now => {
                st.committed.remove(&actor);
                st.delivered.entry(actor).or_insert((now, node));
                None
            }
            Some(&due) => Some(due),
            None => None,
        }
    };
    if let Some(due) = rearm {
        world.set_if_earlier(node, actor, due, Prov::Own);
    }
    sleep(Duration::from_millis(10 + random_below(40))).await;
}

/// Acquires the actor, runs its alarm, and (except on `d`, which keeps it
/// resident until a drain pass hands it over) releases it.
async fn claim_and_run(world: &World, node: usize, lease: &Node, actor: u64, retry: Entry) {
    world
        .st
        .lock()
        .unwrap()
        .touched
        .entry(actor)
        .or_default()
        .insert(node);
    match lease.acquire(&key(actor), 1).await {
        Ok(held) => {
            world.claim(node, actor, &held, lease);
            world.st.lock().unwrap().held[node].insert(actor, held.clone());
            run(world, node, actor).await;
            if node != D {
                world.st.lock().unwrap().held[node].remove(&actor);
                world.stop_claims(node, actor);
                let _ = lease.release(&held).await;
            }
        }
        // Held by a node this one sees draining (or stopped): that node
        // may be handing it over (its release not yet written), or wait to
        // be fenced; or by any node while a hand-off of this actor is
        // recent (`recheck_if_handing_over`); or by this node, its release
        // pending. Look again.
        Err(e)
            if holder(NAMES, &e).is_some_and(|o| {
                let now = world.now();
                let st = world.st.lock().unwrap();
                let handed = world.rule != Rule::ListedDrainingOnly
                    && st.handed[node]
                        .get(&actor)
                        .is_some_and(|&until| now < until);
                let listed = world.seen_draining(node, o, now, &st);
                if handed && !listed && o != node {
                    HANDED_RECHECKS.with(|c| c.set(c.get() + 1));
                }
                o == node || handed || listed
            }) =>
        {
            world.st.lock().unwrap().not_owner += 1;
            world.set_if_earlier(node, actor, world.now() + 1_000, retry.prov);
        }
        // Another node owns it: it has the alarm.
        Err(AcquireError::Held { .. }) => {
            world.st.lock().unwrap().not_owner += 1;
        }
        Err(_) => world.set_if_earlier(node, actor, world.now() + 1_000, retry.prov),
    }
}

/// `ActorState::alarm`, on `node`, in the actor's mailbox.
async fn alarm_event(world: &World, node: usize, lease: &Node, actor: u64, entry: Entry) {
    world
        .st
        .lock()
        .unwrap()
        .touched
        .entry(actor)
        .or_default()
        .insert(node);
    let now = world.now();
    let holds = world.st.lock().unwrap().held[node]
        .get(&actor)
        .is_some_and(|h| lease.may_act(h));
    if holds {
        run(world, node, actor).await;
        return;
    }
    if world.draining(node, now) {
        if let Some(peer) = world.chosen_peer(node, actor, now) {
            if !hand_off(world, peer, actor, entry.due).await {
                world.suspect(node, peer);
                if world.rule != Rule::DropRefused {
                    let at = world.now() + HANDOFF_RETRY_MS;
                    world.set_if_earlier(node, actor, at, entry.prov);
                }
            }
            return;
        }
        world.st.lock().unwrap().self_claims += 1;
    }
    claim_and_run(world, node, lease, actor, entry).await;
}

async fn timer_loop(world: Arc<World>, node: usize, lease: Rc<Node>) {
    loop {
        sleep(Duration::from_millis(TICK_MS)).await;
        let now = world.now();
        let fired: Vec<(u64, Entry, u64)> = {
            let mut st = world.st.lock().unwrap();
            let due: Vec<u64> = st.timers[node]
                .iter()
                .filter(|(_, e)| e.due <= now)
                .map(|(a, _)| *a)
                .collect();
            due.into_iter()
                .map(|actor| {
                    let entry = st.timers[node].remove(&actor).unwrap();
                    st.next_id += 1;
                    let id = st.next_id;
                    st.in_flight.insert(id, (node, actor, entry.prov));
                    (actor, entry, id)
                })
                .collect()
        };
        for (actor, entry, id) in fired {
            let (world, lease) = (Arc::clone(&world), Rc::clone(&lease));
            madsim::task::spawn_local(async move {
                world.lock(node, actor).await;
                alarm_event(&world, node, &lease, actor, entry).await;
                world.unlock(node, actor);
                world.st.lock().unwrap().in_flight.remove(&id);
            });
        }
    }
}

/// Extends each hold's claim while it may act (the lease task renews the
/// node lease; a lapse ends every hold, and its claim with the validity).
async fn refresher(world: Arc<World>, node: usize, lease: Rc<Node>) {
    loop {
        sleep(Duration::from_millis(TICK_MS)).await;
        let now = world.now();
        let mut st = world.st.lock().unwrap();
        if lease.lost() {
            st.fenced_live.get_or_insert(node);
        }
        let State { held, claims, .. } = &mut *st;
        for (actor, h) in &held[node] {
            if lease.may_act(h) {
                claim_in(claims.entry(*actor).or_default(), node, h, &lease, now);
            }
        }
    }
}

/// `hand_over_one`: the job in the actor's mailbox, then the alarm.
async fn hand_over_one(world: &World, lease: &Node, actor: u64) {
    if world.chosen_peer(D, actor, world.now()).is_none() {
        return;
    }
    world.lock(D, actor).await;
    // The actor's earlier events, each with its upload.
    sleep(Duration::from_millis(random_below(BACKLOG_MAX_MS))).await;
    let Some(peer) = world.chosen_peer(D, actor, world.now()) else {
        world.unlock(D, actor);
        return;
    };
    let (held, alarm) = {
        let mut st = world.st.lock().unwrap();
        (st.held[D].remove(&actor), st.committed.get(&actor).copied())
    };
    let Some(held) = held else {
        world.unlock(D, actor);
        return;
    };
    world.stop_claims(D, actor);
    if !lease.may_act(&held) {
        // A lapsed hold: dropped, which releases it (or parks the release,
        // as the server's `LeaseHold` drop does), and nothing to hand (the
        // timer hands the alarm off when it fires).
        world.st.lock().unwrap().lapsed += 1;
        let _ = lease.release(&held).await;
        world.unlock(D, actor);
        return;
    }
    let released = lease.release(&held).await;
    world.unlock(D, actor);
    let Some(at) = alarm else {
        return;
    };
    if !released {
        // `HandOver::Expiring`: the release is retried in the background,
        // and nobody takes the actor before it lands; the timer hands the
        // alarm off when it fires.
        world.st.lock().unwrap().expiring += 1;
        world.set_if_earlier(D, actor, at, Prov::Own);
        return;
    }
    hand_off_alarm(world, D, peer, actor, at, Prov::Own).await;
}

/// `drain.rs`'s `hand_off`: on success forget only that registration; on
/// failure keep it (and suspect the peer).
async fn hand_off_alarm(world: &World, node: usize, peer: usize, actor: u64, at: u64, prov: Prov) {
    if world.rule == Rule::ClearBeforeAck {
        world.clear_timer(node, actor);
    }
    if hand_off(world, peer, actor, at).await {
        world.clear_if(node, actor, at);
    } else {
        world.suspect(node, peer);
        match world.rule {
            Rule::DropRefused => world.clear_timer(node, actor),
            // It believes the entry is still there: it never removed it
            // after a failure, only before sending.
            Rule::ClearBeforeAck => {}
            _ => world.set_if_earlier(node, actor, at, prov),
        }
    }
}

/// The pass over the timer: every alarm registered for an actor this node
/// does not hold (and is not processing) goes to its chosen peer.
async fn hand_off_registered(world: &World, node: usize) {
    let registered: Vec<(u64, Entry)> = {
        let st = world.st.lock().unwrap();
        st.timers[node]
            .iter()
            .filter(|(a, _)| !st.held[node].contains_key(a))
            .filter(|(a, _)| !st.busy.contains(&(node, **a)))
            .map(|(a, e)| (*a, *e))
            .collect()
    };
    for (actor, entry) in registered {
        if let Some(peer) = world.chosen_peer(node, actor, world.now()) {
            hand_off_alarm(world, node, peer, actor, entry.due, entry.prov).await;
        }
    }
}

/// `drain_loop`, on any node: one pass per heartbeat period while it
/// drains, then its state (`drained` only with nothing resident, nothing
/// pending and, but for (d), nothing registered in its timer).
async fn drain_loop(world: Arc<World>, node: usize, lease: Rc<Node>) {
    let first = DRAIN_START_MS.max(world.schedule.listing_offset[node]);
    sleep_until(world.start, first).await;
    loop {
        let now = world.now();
        if world.draining(node, now) {
            let any_peer = (0..ACTORS).any(|a| world.chosen_peer(node, a, now).is_some());
            if any_peer {
                let actors: Vec<u64> = if node == D {
                    world.st.lock().unwrap().held[D].keys().copied().collect()
                } else {
                    Vec::new()
                };
                let queue = Rc::new(Mutex::new(actors));
                let workers: Vec<_> = (0..DRAIN_PARALLEL)
                    .map(|_| {
                        let (world, lease, queue) =
                            (Arc::clone(&world), Rc::clone(&lease), Rc::clone(&queue));
                        madsim::task::spawn_local(async move {
                            loop {
                                let next = queue.lock().unwrap().pop();
                                let Some(actor) = next else { break };
                                hand_over_one(&world, &lease, actor).await;
                            }
                        })
                    })
                    .collect();
                for w in workers {
                    let _ = w.await;
                }
                if world.rule != Rule::DrainedWithAlarms {
                    hand_off_registered(&world, node).await;
                }
            }
            let now = world.now();
            let mut st = world.st.lock().unwrap();
            // Resident: what it holds, and any actor whose mailbox is in
            // use (an alarm event in progress has a mailbox too).
            let resident = if node == D { st.held[D].len() } else { 0 }
                + st.busy.iter().filter(|(n, _)| *n == node).count()
                + st.in_flight.values().filter(|(n, _, _)| *n == node).count();
            let registered = if world.rule == Rule::DrainedWithAlarms {
                0
            } else {
                st.timers[node].len()
            };
            let pending = world.rule != Rule::DrainedWithPending && !lease.nothing_pending();
            if resident + registered == 0 && !pending {
                st.drained_since[node].get_or_insert(now);
            } else {
                st.drained_since[node] = None;
            }
        } else {
            world.st.lock().unwrap().drained_since[node] = None;
        }
        sleep(Duration::from_millis(LISTING_MS)).await;
    }
}

/// The coarse sweep: a serving peer delivers what is long overdue.
async fn sweeper(world: Arc<World>, node: usize, lease: Rc<Node>) {
    sleep(Duration::from_millis(random_below(SWEEP_MS))).await;
    loop {
        let now = world.now();
        if !world.draining(node, now) {
            let overdue: Vec<u64> = world
                .st
                .lock()
                .unwrap()
                .committed
                .iter()
                .filter(|(_, due)| **due + OVERDUE_MS <= now)
                .map(|(a, _)| *a)
                .collect();
            for actor in overdue {
                let (world, lease) = (Arc::clone(&world), Rc::clone(&lease));
                madsim::task::spawn_local(async move {
                    world.lock(node, actor).await;
                    world.st.lock().unwrap().swept += 1;
                    let entry = Entry {
                        due: now,
                        prov: Prov::Own,
                    };
                    claim_and_run(&world, node, &lease, actor, entry).await;
                    world.unlock(node, actor);
                });
            }
        }
        sleep(Duration::from_millis(SWEEP_MS)).await;
    }
}

async fn node_main(world: Arc<World>, node: usize) {
    let breaks = Breaks {
        pending_uncounted: world.rule == Rule::PendingUncounted,
        ..Breaks::default()
    };
    let lease = start_node(world.start, NAMES, node, breaks).await;
    madsim::task::spawn_local(refresher(Arc::clone(&world), node, Rc::clone(&lease)));
    if node == D {
        // `d` owns every actor, and its timer holds their alarms.
        for actor in 0..ACTORS {
            let held = loop {
                match lease.acquire(&key(actor), 1).await {
                    Ok(held) => break held,
                    Err(_) => sleep(Duration::from_millis(50)).await,
                }
            };
            world.claim(D, actor, &held, &lease);
            let due = {
                let mut st = world.st.lock().unwrap();
                st.held[D].insert(actor, held);
                st.committed.get(&actor).copied()
            };
            if let Some(due) = due {
                world.set_if_earlier(D, actor, due, Prov::Own);
            }
        }
        if world.start.elapsed() >= Duration::from_millis(SETUP_MS) {
            world.st.lock().unwrap().setup_late = true;
        }
    } else {
        madsim::task::spawn_local(sweeper(Arc::clone(&world), node, Rc::clone(&lease)));
    }
    madsim::task::spawn_local(drain_loop(Arc::clone(&world), node, Rc::clone(&lease)));
    timer_loop(world, node, lease).await;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    schedule: Schedule,
    delivered: BTreeMap<u64, (u64, usize)>,
    touched: BTreeMap<u64, BTreeSet<usize>>,
    double: Option<String>,
    orphans: Vec<(u64, u64, Prov)>,
    stopped: [Option<u64>; 3],
    generations: u64,
    handoffs_ok: u64,
    handoffs_failed: u64,
    self_claims: u64,
    swept: u64,
    not_owner: u64,
    expiring: u64,
    lapsed: u64,
    fences: u64,
    setup_late: bool,
    fenced_live: Option<usize>,
    /// A record naming a node the operator stopped.
    left: Option<String>,
    handed_rechecks: u64,
}

fn simulate(seed: u64, rule: Rule) -> Outcome {
    let runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let schedule = Schedule::draw(seed);
    let _ = take_fences();
    HANDED_RECHECKS.with(|c| c.set(0));
    runtime.block_on(async move {
        let handle = Handle::current();
        let store = Store::default();
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Arc::clone(&store);
                move || bucket_node(Arc::clone(&store))
            })
            .build()
            .id();
        // What the operator relies on stopping a drained node: no
        // ownership record still names it (ADR 0020: `drained` waits for
        // pending releases).
        let named = |node: usize| -> Option<String> {
            store.lock().unwrap().iter().find_map(|(k, (body, _))| {
                let record = OwnerRecord::decode(body).filter(|_| k.starts_with("owners/"))?;
                record
                    .holder
                    .filter(|(n, _)| n == NAMES[node])
                    .map(|_| format!("{} stopped while {k} still named it", NAMES[node]))
            })
        };
        let mut left = None;
        let world = Arc::new(World {
            start: Instant::now(),
            rule,
            schedule: schedule.clone(),
            st: Mutex::new(State {
                committed: schedule.dues.clone(),
                ..State::default()
            }),
        });
        let nodes: Vec<_> = (0..3)
            .map(|node| {
                let world = Arc::clone(&world);
                handle
                    .create_node()
                    .name(NAMES[node])
                    .ip(format!("10.0.1.{}", node + 1).parse().unwrap())
                    .init(move || node_main(Arc::clone(&world), node))
                    .build()
                    .id()
            })
            .collect();
        let net = NetSim::current();
        // Per clog: applied, lifted.
        let mut clogs = vec![(false, false); schedule.clogs.len()];
        loop {
            sleep(Duration::from_millis(TICK_MS)).await;
            let now = world.now();
            let started = world.start.elapsed() >= Duration::from_millis(SETUP_MS);
            for (i, &(node, from, until)) in schedule.clogs.iter().enumerate() {
                let (on, off) = &mut clogs[i];
                if started && !*on && now >= from {
                    *on = true;
                    net.clog_link(nodes[node], bucket);
                    net.clog_link(bucket, nodes[node]);
                }
                if *on && !*off && now >= until {
                    *off = true;
                    net.unclog_link(nodes[node], bucket);
                    net.unclog_link(bucket, nodes[node]);
                }
            }
            let (drained, stopped, done) = {
                let st = world.st.lock().unwrap();
                (st.drained_since, st.stopped, st.committed.is_empty())
            };
            // The operator stops a node it sees reporting drained.
            if let (Some(since), Some(after), None) =
                (drained[D], schedule.d_stop_after, stopped[D])
                && now >= since + after
            {
                left = left.or_else(|| named(D));
                handle.kill(nodes[D]);
                world.stopped(D);
            }
            for node in 1..3 {
                if stopped[node].is_none()
                    && drained[node].is_some()
                    && schedule.stop_at(node).is_some_and(|s| now >= s)
                {
                    left = left.or_else(|| named(node));
                    handle.kill(nodes[node]);
                    world.stopped(node);
                }
            }
            if (done && now >= QUIET_FROM_MS) || now >= RUN_MS {
                break;
            }
        }
        let st = world.st.lock().unwrap();
        let double = st.claims.iter().find_map(|(actor, claims)| {
            double_ownership(claims).map(|v| format!("actor {actor}: {v:?}"))
        });
        let generations = st
            .claims
            .values()
            .flat_map(|c| c.iter().map(|c| c.generation))
            .sum();
        Outcome {
            schedule: schedule.clone(),
            delivered: st.delivered.clone(),
            touched: st.touched.clone(),
            double,
            orphans: st.orphans.clone(),
            stopped: st.stopped,
            generations,
            handoffs_ok: st.handoffs_ok,
            handoffs_failed: st.handoffs_failed,
            self_claims: st.self_claims,
            swept: st.swept,
            not_owner: st.not_owner,
            expiring: st.expiring,
            lapsed: st.lapsed,
            fences: take_fences(),
            setup_late: st.setup_late,
            fenced_live: st.fenced_live,
            left,
            handed_rechecks: HANDED_RECHECKS.with(std::cell::Cell::get),
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Broken {
    /// Invariant 1.
    TwoOwners,
    /// Invariant 2.
    Lost,
    /// Invariant 3.
    Late,
    /// Invariant 4: a stopped node left a record naming it.
    RecordLeft,
}

/// Whether invariant 3 applies to an alarm due at `due`, delivered by
/// `by`.
fn bounded(outcome: &Outcome, actor: u64, due: u64, by: usize) -> bool {
    let end = outcome.schedule.deadline(due, by, &touched(outcome, actor));
    due >= DRAIN_START_MS && (1..3).any(|p| outcome.schedule.accepting(p, due, end))
}

/// The nodes that handled `actor`'s alarm.
fn touched(outcome: &Outcome, actor: u64) -> BTreeSet<usize> {
    outcome.touched.get(&actor).cloned().unwrap_or_default()
}

fn violation(outcome: &Outcome) -> Option<(Broken, String)> {
    if let Some(double) = &outcome.double {
        return Some((Broken::TwoOwners, format!("two owners: {double}")));
    }
    if let Some(left) = &outcome.left {
        return Some((Broken::RecordLeft, left.clone()));
    }
    for (&actor, &due) in &outcome.schedule.dues {
        let Some(&(at, node)) = outcome.delivered.get(&actor) else {
            return Some((
                Broken::Lost,
                format!("actor {actor}'s alarm due at {due} ms never delivered"),
            ));
        };
        let deadline = outcome
            .schedule
            .deadline(due, node, &touched(outcome, actor));
        if bounded(outcome, actor, due, node) && at > deadline {
            return Some((
                Broken::Late,
                format!(
                    "actor {actor}'s alarm due at {due} ms delivered at {at} ms by {}, \
                     past its deadline {deadline} ms ({HANDOFF_BOUND_MS} ms bound) with a peer accepting",
                    NAMES[node]
                ),
            ));
        }
    }
    None
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200)
}

#[test]
fn a_drain_hands_every_alarm_over_in_time() {
    let seeds = seeds();
    assert!(seeds > 0, "the drain simulation needs at least one seed");
    let (mut ok, mut failed, mut self_claims, mut swept, mut bounded_alarms, mut gens) =
        (0, 0, 0, 0, 0, 0);
    let (mut not_owner, mut orphans) = (0, 0);
    let (mut expiring, mut lapsed, mut fences) = (0, 0, 0);
    // Seeds that took each node-lease path, so one busy seed cannot carry
    // a floor alone.
    let (mut s_expiring, mut s_lapsed, mut s_fences, mut s_handed) = (0, 0, 0, 0);
    for seed in 0..seeds {
        let outcome = simulate(seed, Rule::Production);
        if let Some((_, broken)) = violation(&outcome) {
            panic!("seed {seed}: {broken}");
        }
        assert!(!outcome.setup_late, "seed {seed}: d took its actors late");
        assert_eq!(
            outcome.fenced_live, None,
            "seed {seed}: a running node was fenced"
        );
        expiring += outcome.expiring;
        lapsed += outcome.lapsed;
        fences += outcome.fences;
        s_expiring += u64::from(outcome.expiring > 0);
        s_lapsed += u64::from(outcome.lapsed > 0);
        s_fences += u64::from(outcome.fences > 0);
        s_handed += u64::from(outcome.handed_rechecks > 0);
        ok += outcome.handoffs_ok;
        failed += outcome.handoffs_failed;
        self_claims += outcome.self_claims;
        swept += outcome.swept;
        not_owner += outcome.not_owner;
        orphans += outcome.orphans.len();
        gens += outcome.generations;
        bounded_alarms += outcome
            .schedule
            .dues
            .iter()
            .filter(|&(a, &d)| {
                outcome
                    .delivered
                    .get(a)
                    .is_some_and(|&(_, by)| bounded(&outcome, *a, d, by))
            })
            .count();
    }
    assert!(failed > seeds / 2, "few refused hand-offs ({failed})");
    assert!(bounded_alarms as u64 > seeds * 5, "few bounded alarms");
    // The node-lease paths were taken, each in many seeds: releases left
    // pending, holds a lapse ended, stopped nodes fenced, and looks again
    // only the hand-off rule asked for.
    assert!(
        s_expiring > seeds / 10,
        "few seeds left a release pending ({s_expiring})"
    );
    assert!(
        s_lapsed > seeds / 10,
        "few seeds had a hold lapse ({s_lapsed})"
    );
    assert!(
        s_fences > seeds / 10,
        "few seeds fenced a node ({s_fences})"
    );
    assert!(
        s_handed > seeds / 50,
        "few seeds needed the hand-off rule ({s_handed})"
    );
    eprintln!(
        "drain: {seeds} seeds: {ok} hand-offs taken, {failed} refused, {self_claims} claims by a \
         draining node with no peer, {swept} sweep attempts, {not_owner} acquires that found another owner, {bounded_alarms} alarms held to the \
         {HANDOFF_BOUND_MS} ms bound, lease generations summed {gens}; {orphans} alarms left in a stopped node's timer; \
         {expiring} hand-overs with the release pending ({s_expiring} seeds), {lapsed} holds a lapse ended ({s_lapsed} seeds), \
         {fences} fences ({s_fences} seeds), the hand-off rule needed in {s_handed} seeds"
    );
}

#[test]
fn the_unsafe_drain_rules_are_caught() {
    // Each search stops at its first catch; (e) and (f) need a bucket
    // outage at the right moment, a few seeds in a hundred.
    let seeds = seeds().max(1_000);
    for (rule, breaks) in [
        (Rule::DropRefused, Broken::Late),
        (Rule::ClearBeforeAck, Broken::Late),
        (Rule::AcceptWhileDraining, Broken::Late),
        (Rule::DrainedWithAlarms, Broken::Late),
        (Rule::DrainedWithPending, Broken::RecordLeft),
        (Rule::PendingUncounted, Broken::RecordLeft),
        (Rule::ListedDrainingOnly, Broken::Late),
    ] {
        let found = (0..seeds).find_map(|seed| {
            violation(&simulate(seed, rule))
                .filter(|(b, _)| *b == breaks)
                .map(|(_, why)| (seed, why))
        });
        let (seed, why) =
            found.unwrap_or_else(|| panic!("{rule:?} never broke {breaks:?} in {seeds} seeds"));
        // A catch past the seeds the correct rule runs proves something
        // only if the correct rule passes there.
        assert_eq!(
            violation(&simulate(seed, Rule::Production)).map(|(_, why)| why),
            None,
            "seed {seed} breaks the correct rule too"
        );
        eprintln!("drain: {rule:?} caught ({breaks:?}) at seed {seed}: {why}");
    }
}

#[test]
fn a_drain_replays_with_the_same_seed() {
    assert_eq!(
        simulate(41, Rule::Production),
        simulate(41, Rule::Production)
    );
    assert_eq!(
        simulate(41, Rule::AcceptWhileDraining),
        simulate(41, Rule::AcceptWhileDraining)
    );
}

// Moving one actor (ADR 0017): a drain of one actor, to a named target.
//
// `a` holds actor X; clients send it events through all three nodes (a
// node that does not hold X forwards to the lease's holder, once, or
// claims it if free; a forwarded event that cannot run is a 503 the client
// retries). The move key, written at `key_at`, names `b`. Every node lists
// it each heartbeat period (`moves_pass`): the holder runs the move job in
// X's mailbox after the queued events (set `moving_to`, read the committed
// alarm, release, awaited), sends `b` the take call, then hibernates the
// stub; on `Held` (or `HeldBy(b)`) it clears its alarm registration if
// still at the same time and reports the move done; on `HeldBy(other)` it
// gives up; on failure it tries again on a later pass. While `moving_to`
// is set, events at `a` go to `b` and an alarm firing at `a` is handed off
// to `b`. The take at `b` forgets learned owners, waits for X's mailbox
// (`ADMISSION_WAIT`), acquires, restores (sometimes slowly), and arms the
// committed alarm. `b`, listing the key while nobody holds X, takes it
// itself. `b` fails a quarter of its takes, and some seeds have it refuse
// node calls for a while. In some seeds X is idle during the move (no
// event would claim it and arm its alarm).
//
// Checked: one owner at a time; the alarm delivered; `b` holding X when
// the move reports done; the old owner never claiming X back while its
// move is under way (the ADR's "so the owner claims back"); the move
// finishing; and an alarm due from the key on delivered within
// `HANDOFF_BOUND_MS`. Unsafe variants: hibernating the stub right after
// the release, before the take (caught as a claim back), and clearing the
// alarm registration before the target confirmed (caught as a late
// alarm).

const MOVE_NAMES: [&str; 3] = ["a", "b", "c"];
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
/// X's ownership record.
const X: &str = "owners/t/c/moving";
const EVENTS_UNTIL_MS: u64 = 8_000;
/// How long a learned owner is trusted (the owner hint).
const HINT_MS: u64 = 1_000;
/// The server's `ADMISSION_WAIT`.
const ADMISSION_WAIT_MS: u64 = 250;
/// The server's `TAKE_TIMEOUT`.
const TAKE_TIMEOUT_MS: u64 = 10_000;
const MOVE_QUIET_MS: u64 = 12_000;
const MOVE_RUN_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveRule {
    /// The server's rule.
    Production,
    /// Unsafe: the stub hibernates (dropping `moving_to`) right after the
    /// release, before the take.
    HibernateEarly,
    /// Unsafe: the owner clears its alarm registration before the target
    /// confirmed.
    ClearAlarmEarly,
    /// Unsafe: a node acts on X while its node lease is valid, ignoring
    /// that a lapse ended its hold.
    RunOnLeaseOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MovePlan {
    key_at: u64,
    /// X's committed alarm.
    due: Option<u64>,
    /// `b` refuses node calls over this window.
    b_down: Option<(u64, u64)>,
    /// Clients stop sending events just before the key: X is idle
    /// during the move.
    quiet: bool,
    /// When each node lists `control/` (offset in the period).
    pass_offset: [u64; 3],
    /// `a` loses the bucket over this window (its lease may lapse).
    a_clog: Option<(u64, u64)>,
}

impl MovePlan {
    fn draw(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x0A0C_7E11);
        let key_at = 1_000 + rng.below(500);
        let due = match rng.below(10) {
            // Due during the move.
            0..=5 => Some(key_at + rng.below(2_500)),
            // After it.
            6 | 7 => Some(4_000 + rng.below(6_000)),
            _ => None,
        };
        let b_down = rng.chance(40).then(|| {
            let from = key_at.saturating_sub(500) + rng.below(3_000);
            (from, from + 1_000 + rng.below(3_000))
        });
        let pass_offset = [
            rng.below(LISTING_MS),
            rng.below(LISTING_MS),
            rng.below(LISTING_MS),
        ];
        let quiet = rng.chance(30);
        let a_clog = rng.chance(40).then(|| {
            let from = 200 + rng.below(key_at + 2_000);
            (
                from,
                from + CLOG_MIN_MS + rng.below(CLOG_MAX_MS - CLOG_MIN_MS),
            )
        });
        Self {
            key_at,
            due,
            b_down,
            quiet,
            pass_offset,
            a_clog,
        }
    }

    /// When the alarm due at `due` must be delivered by (see
    /// `Schedule::deadline`).
    fn deadline(&self, due: u64) -> u64 {
        match self.a_clog {
            Some((start, until))
                if start <= due + HANDOFF_BOUND_MS && until + CLOG_TAIL_MS > due =>
            {
                until + CLOG_TAIL_MS + HANDOFF_BOUND_MS
            }
            _ => due + HANDOFF_BOUND_MS,
        }
    }

    fn b_down_at(&self, t: u64) -> bool {
        self.b_down
            .is_some_and(|(from, until)| from <= t && t < until)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Held,
    HeldBy(usize),
    Failed,
}

enum Msg {
    Event { forwarded: bool, tries: u32 },
    Take(Arc<Mutex<Option<Answer>>>),
    HandOff(u64, Arc<Mutex<Option<bool>>>),
}

#[derive(Default)]
struct MoveState {
    committed: Option<u64>,
    delivered: Option<(u64, usize)>,
    held: [Option<Held>; 3],
    /// X's mailbox in use, per node.
    busy: [bool; 3],
    moving_to: [Option<usize>; 3],
    /// Learned owner and until when.
    hint: [Option<(usize, u64)>; 3],
    timers: [Option<u64>; 3],
    inbox: [Vec<Msg>; 3],
    /// The move key is in the bucket.
    key: bool,
    /// A move job under way on this node: from the release to the answer.
    under_way: [bool; 3],
    claims: Vec<Claim>,
    /// (when, owner, whether `b` held X then).
    done: Vec<(u64, usize, bool)>,
    /// (when, holder named) for `HeldBy(other)`.
    gave_up: Vec<(u64, usize)>,
    self_takes: u64,
    /// (when, node): an owner claimed X back during its own move.
    claimed_back: Option<(u64, usize)>,
    requests_ok: u64,
    requests_failed: u64,
    takes: u64,
    takes_failed: u64,
}

struct MoveWorld {
    start: Instant,
    rule: MoveRule,
    plan: MovePlan,
    st: Mutex<MoveState>,
}

impl MoveWorld {
    /// The scenario's time: from `SETUP_MS` after the processes started.
    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_millis())
            .unwrap()
            .saturating_sub(SETUP_MS)
    }

    /// Whether `node` may act on X: its hold may act (`RunOnLeaseOnly`:
    /// its node lease alone is valid).
    fn holds(&self, node: usize, lease: &Node) -> bool {
        self.st.lock().unwrap().held[node]
            .as_ref()
            .is_some_and(|h| self.acting(h, lease))
    }

    /// Whether `node` holds X now as the others can tell: a hold whose
    /// claim still runs (the node's lease is its own).
    fn held_now(&self, node: usize) -> bool {
        let now = self.now();
        let st = self.st.lock().unwrap();
        let n = u32::try_from(node).unwrap();
        st.held[node].is_some()
            && st
                .claims
                .iter()
                .rev()
                .find(|c| c.node == n)
                .is_some_and(|c| c.until_ms > now)
    }

    fn acting(&self, held: &Held, lease: &Node) -> bool {
        if self.rule == MoveRule::RunOnLeaseOnly {
            lease.lease_valid()
        } else {
            lease.may_act(held)
        }
    }

    async fn lock(&self, node: usize) {
        loop {
            {
                let mut st = self.st.lock().unwrap();
                if !st.busy[node] {
                    st.busy[node] = true;
                    return;
                }
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    /// Waits at most `ms` for the mailbox.
    async fn lock_within(&self, node: usize, ms: u64) -> bool {
        let until = self.now() + ms;
        loop {
            {
                let mut st = self.st.lock().unwrap();
                if !st.busy[node] {
                    st.busy[node] = true;
                    return true;
                }
            }
            if self.now() >= until {
                return false;
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    fn unlock(&self, node: usize) {
        self.st.lock().unwrap().busy[node] = false;
    }

    fn hold(&self, node: usize, held: Held, lease: &Node) {
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        if st.under_way[node] && st.claimed_back.is_none() {
            st.claimed_back = Some((now, node));
        }
        claim_in(&mut st.claims, node, &held, lease, now);
        st.held[node] = Some(held);
    }

    fn stop_claims(&self, node: usize) {
        let now = self.now();
        stop(
            &mut self.st.lock().unwrap().claims,
            u32::try_from(node).unwrap(),
            now,
        );
    }

    fn set_if_earlier(&self, node: usize, at: u64) {
        let mut st = self.st.lock().unwrap();
        if st.timers[node].is_none_or(|t| at < t) {
            st.timers[node] = Some(at);
        }
    }

    fn clear_if(&self, node: usize, at: u64) {
        let mut st = self.st.lock().unwrap();
        if st.timers[node] == Some(at) {
            st.timers[node] = None;
        }
    }

    /// Sends `msg` to `to`, after a network delay.
    fn send(self: &Arc<Self>, to: usize, msg: Msg) {
        let world = Arc::clone(self);
        madsim::task::spawn(async move {
            sleep(Duration::from_millis(late_ms())).await;
            world.st.lock().unwrap().inbox[to].push(msg);
        });
    }

    /// A client's retry of a failed request, through any node.
    fn retry(self: &Arc<Self>, tries: u32) {
        if tries >= 6 {
            self.st.lock().unwrap().requests_failed += 1;
            return;
        }
        let world = Arc::clone(self);
        madsim::task::spawn(async move {
            sleep(Duration::from_millis(200 + random_below(800))).await;
            let to = usize::try_from(random_below(3)).unwrap();
            world.st.lock().unwrap().inbox[to].push(Msg::Event {
                forwarded: false,
                tries: tries + 1,
            });
        });
    }
}

/// Waits for a call's reply, up to `timeout_ms`.
async fn reply<T: Clone>(slot: &Mutex<Option<T>>, timeout_ms: u64) -> Option<T> {
    for _ in 0..timeout_ms {
        if let Some(v) = slot.lock().unwrap().clone() {
            return Some(v);
        }
        sleep(Duration::from_millis(1)).await;
    }
    None
}

enum Route {
    Run,
    Forward(usize),
    Fail,
}

/// `ensure_owner` for an event at `node`.
async fn event_owner(world: &MoveWorld, node: usize, lease: &Node) -> Route {
    if world.holds(node, lease) {
        return Route::Run;
    }
    let now = world.now();
    let (hint, moving) = {
        let mut st = world.st.lock().unwrap();
        st.held[node] = None;
        (st.hint[node], st.moving_to[node])
    };
    if let Some((owner, until)) = hint
        && now < until
        && owner != node
    {
        return Route::Forward(owner);
    }
    if let Some(target) = moving {
        return Route::Forward(target);
    }
    match lease.acquire(X, 1).await {
        Ok(held) => {
            world.hold(node, held, lease);
            Route::Run
        }
        Err(e) => match holder(MOVE_NAMES, &e) {
            Some(owner) if owner != node => {
                world.st.lock().unwrap().hint[node] = Some((owner, world.now() + HINT_MS));
                Route::Forward(owner)
            }
            // Its own release pending, or the bucket: the client retries.
            _ => Route::Fail,
        },
    }
}

async fn handle_event(
    world: Arc<MoveWorld>,
    node: usize,
    lease: Rc<Node>,
    forwarded: bool,
    tries: u32,
) {
    world.lock(node).await;
    let route = event_owner(&world, node, &lease).await;
    if let Route::Run = route {
        sleep(Duration::from_millis(5 + random_below(25))).await;
        // The event's end arms the committed alarm here.
        let committed = world.st.lock().unwrap().committed;
        if let Some(at) = committed {
            world.set_if_earlier(node, at);
        }
        world.st.lock().unwrap().requests_ok += 1;
    }
    world.unlock(node);
    match route {
        Route::Run => {}
        Route::Forward(to) if !forwarded => world.send(
            to,
            Msg::Event {
                forwarded: true,
                tries,
            },
        ),
        Route::Forward(_) | Route::Fail => world.retry(tries),
    }
}

/// Runs X's alarm if it is committed and due (as its owner).
async fn move_run_alarm(world: &MoveWorld, node: usize) {
    let now = world.now();
    let later = {
        let mut st = world.st.lock().unwrap();
        match st.committed {
            Some(at) if at <= now => {
                st.committed = None;
                st.delivered.get_or_insert((now, node));
                None
            }
            other => other,
        }
    };
    if let Some(at) = later {
        world.set_if_earlier(node, at);
    }
    sleep(Duration::from_millis(5 + random_below(25))).await;
}

/// The alarm path at `node`, in X's mailbox.
async fn move_alarm_event(world: Arc<MoveWorld>, node: usize, lease: Rc<Node>) {
    world.lock(node).await;
    if world.holds(node, &lease) {
        move_run_alarm(&world, node).await;
        world.unlock(node);
        return;
    }
    let moving = world.st.lock().unwrap().moving_to[node];
    if let Some(target) = moving {
        let at = world.st.lock().unwrap().committed.unwrap_or(0);
        let slot = Arc::new(Mutex::new(None));
        world.send(target, Msg::HandOff(at, Arc::clone(&slot)));
        if reply(&slot, 2_000).await != Some(true) {
            world.set_if_earlier(node, world.now() + HANDOFF_RETRY_MS);
        }
        world.unlock(node);
        return;
    }
    match lease.acquire(X, 1).await {
        Ok(held) => {
            world.hold(node, held, &lease);
            move_run_alarm(&world, node).await;
        }
        // The owner has it.
        Err(e) if holder(MOVE_NAMES, &e).is_some_and(|o| o != node) => {}
        Err(_) => world.set_if_earlier(node, world.now() + 1_000),
    }
    world.unlock(node);
}

/// The take job at `b`.
async fn take_job(world: &MoveWorld, lease: &Node) -> Answer {
    world.st.lock().unwrap().hint[B] = None;
    if !world.lock_within(B, ADMISSION_WAIT_MS).await {
        return Answer::Failed;
    }
    let answer = if world.holds(B, lease) {
        Answer::Held
    } else {
        match lease.acquire(X, 1).await {
            Ok(held) => {
                world.hold(B, held, lease);
                // Restore, sometimes slowly.
                let slow = if random_below(10) < 3 {
                    200 + random_below(800)
                } else {
                    0
                };
                sleep(Duration::from_millis(20 + random_below(180) + slow)).await;
                let committed = world.st.lock().unwrap().committed;
                if let Some(at) = committed {
                    world.set_if_earlier(B, at);
                }
                Answer::Held
            }
            Err(e) => match holder(MOVE_NAMES, &e) {
                Some(owner) if owner != B => Answer::HeldBy(owner),
                _ => Answer::Failed,
            },
        }
    };
    world.unlock(B);
    answer
}

async fn handle_msg(world: Arc<MoveWorld>, node: usize, lease: Rc<Node>, msg: Msg) {
    let refused = node == B && world.plan.b_down_at(world.now());
    match msg {
        Msg::Event { forwarded, tries } if refused => {
            let _ = forwarded;
            world.retry(tries);
        }
        Msg::Event { forwarded, tries } => {
            handle_event(world, node, lease, forwarded, tries).await;
        }
        Msg::Take(slot) => {
            world.st.lock().unwrap().takes += 1;
            // A quarter of takes fail (secrets, the bucket).
            let answer = if refused || random_below(4) == 0 {
                Answer::Failed
            } else {
                take_job(&world, &lease).await
            };
            if answer == Answer::Failed {
                world.st.lock().unwrap().takes_failed += 1;
            }
            sleep(Duration::from_millis(late_ms())).await;
            *slot.lock().unwrap() = Some(answer);
        }
        Msg::HandOff(at, slot) => {
            if !refused {
                world.set_if_earlier(node, at);
            }
            sleep(Duration::from_millis(late_ms())).await;
            *slot.lock().unwrap() = Some(!refused);
        }
    }
}

/// The owner's move job and take call. Whether the key is finished.
async fn move_resident(world: &Arc<MoveWorld>, node: usize, lease: &Node) -> bool {
    world.lock(node).await;
    if !world.holds(node, lease) {
        world.unlock(node);
        return false;
    }
    let (held, alarm) = {
        let mut st = world.st.lock().unwrap();
        (st.held[node].take(), st.committed)
    };
    let held = held.unwrap();
    {
        let mut st = world.st.lock().unwrap();
        st.moving_to[node] = Some(B);
        st.under_way[node] = true;
    }
    world.stop_claims(node);
    // `MoveOut::Expiring`: a release that did not land is retried in the
    // background, and the move is tried again on a later pass.
    let released = lease.release(&held).await;
    if !released {
        let mut st = world.st.lock().unwrap();
        st.moving_to[node] = None;
        st.under_way[node] = false;
        drop(st);
        world.unlock(node);
        return false;
    }
    if world.rule == MoveRule::HibernateEarly {
        world.st.lock().unwrap().moving_to[node] = None;
    }
    world.unlock(node);
    if world.rule == MoveRule::ClearAlarmEarly
        && let Some(at) = alarm
    {
        world.clear_if(node, at);
    }
    let slot = Arc::new(Mutex::new(None));
    world.send(B, Msg::Take(Arc::clone(&slot)));
    let answer = reply(&slot, TAKE_TIMEOUT_MS)
        .await
        .unwrap_or(Answer::Failed);
    // However it went, the stub goes.
    {
        let mut st = world.st.lock().unwrap();
        st.moving_to[node] = None;
        st.under_way[node] = false;
    }
    match answer {
        Answer::Held | Answer::HeldBy(B) => {
            if let Some(at) = alarm {
                world.clear_if(node, at);
            }
            let (now, b_holds) = (world.now(), world.held_now(B));
            let mut st = world.st.lock().unwrap();
            st.done.push((now, node, b_holds));
            st.key = false;
            true
        }
        Answer::HeldBy(other) => {
            let now = world.now();
            let mut st = world.st.lock().unwrap();
            st.gave_up.push((now, other));
            st.key = false;
            true
        }
        Answer::Failed => false,
    }
}

async fn moves_pass(world: Arc<MoveWorld>, node: usize, lease: Rc<Node>) {
    sleep_until(
        world.start,
        world.plan.key_at + world.plan.pass_offset[node],
    )
    .await;
    loop {
        if world.st.lock().unwrap().key {
            if node == B {
                if matches!(lease.other_owner(X).await, Ok(None))
                    && take_job(&world, &lease).await == Answer::Held
                {
                    let mut st = world.st.lock().unwrap();
                    st.self_takes += 1;
                    st.key = false;
                }
            } else if world.holds(node, &lease) {
                move_resident(&world, node, &lease).await;
            }
        }
        sleep(Duration::from_millis(LISTING_MS)).await;
    }
}

/// Extends the hold's claim while it may act (through the rule's own
/// predicate, so a variant acting on a dead hold claims it too).
async fn move_refresher(world: Arc<MoveWorld>, node: usize, lease: Rc<Node>) {
    loop {
        sleep(Duration::from_millis(TICK_MS)).await;
        let now = world.now();
        let mut st = world.st.lock().unwrap();
        let MoveState { held, claims, .. } = &mut *st;
        if let Some(h) = &held[node]
            && world.acting(h, &lease)
        {
            claim_in(claims, node, h, &lease, now);
        }
    }
}

async fn move_timer(world: Arc<MoveWorld>, node: usize, lease: Rc<Node>) {
    loop {
        sleep(Duration::from_millis(TICK_MS)).await;
        let now = world.now();
        let fired = {
            let mut st = world.st.lock().unwrap();
            let due = st.timers[node].is_some_and(|t| t <= now);
            if due {
                st.timers[node] = None;
            }
            due
        };
        if fired {
            madsim::task::spawn_local(move_alarm_event(
                Arc::clone(&world),
                node,
                Rc::clone(&lease),
            ));
        }
    }
}

async fn move_node(world: Arc<MoveWorld>, node: usize) {
    let lease = start_node(world.start, MOVE_NAMES, node, Breaks::default()).await;
    madsim::task::spawn_local(move_refresher(Arc::clone(&world), node, Rc::clone(&lease)));
    if node == A {
        let held = loop {
            match lease.acquire(X, 1).await {
                Ok(held) => break held,
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        };
        world.hold(A, held, &lease);
        let committed = world.st.lock().unwrap().committed;
        if let Some(at) = committed {
            world.set_if_earlier(A, at);
        }
    }
    madsim::task::spawn_local(move_timer(Arc::clone(&world), node, Rc::clone(&lease)));
    madsim::task::spawn_local(moves_pass(Arc::clone(&world), node, Rc::clone(&lease)));
    if node == C {
        // The coarse sweep.
        let (world, lease) = (Arc::clone(&world), Rc::clone(&lease));
        madsim::task::spawn_local(async move {
            loop {
                sleep(Duration::from_millis(SWEEP_MS)).await;
                let now = world.now();
                let overdue = world
                    .st
                    .lock()
                    .unwrap()
                    .committed
                    .is_some_and(|at| at + OVERDUE_MS <= now);
                if overdue {
                    world.lock(C).await;
                    if world.holds(C, &lease) {
                        move_run_alarm(&world, C).await;
                    } else if let Ok(held) = lease.acquire(X, 1).await {
                        world.hold(C, held, &lease);
                        move_run_alarm(&world, C).await;
                    }
                    world.unlock(C);
                }
            }
        });
    }
    // Clients send X events through this node.
    {
        let world = Arc::clone(&world);
        madsim::task::spawn_local(async move {
            // From the scenario's start: `a` takes X first.
            sleep_until(world.start, 0).await;
            let gap = [50, 400, 200][node];
            let until = if world.plan.quiet {
                world.plan.key_at.saturating_sub(200)
            } else {
                EVENTS_UNTIL_MS
            };
            while world.now() < until {
                sleep(Duration::from_millis(gap / 2 + random_below(gap))).await;
                world.st.lock().unwrap().inbox[node].push(Msg::Event {
                    forwarded: false,
                    tries: 0,
                });
            }
        });
    }
    loop {
        sleep(Duration::from_millis(2)).await;
        let msgs = std::mem::take(&mut world.st.lock().unwrap().inbox[node]);
        for msg in msgs {
            madsim::task::spawn_local(handle_msg(Arc::clone(&world), node, Rc::clone(&lease), msg));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MoveOutcome {
    plan: MovePlan,
    delivered: Option<(u64, usize)>,
    double: Option<String>,
    done: Vec<(u64, usize, bool)>,
    gave_up: Vec<(u64, usize)>,
    self_takes: u64,
    claimed_back: Option<(u64, usize)>,
    key_left: bool,
    requests_ok: u64,
    requests_failed: u64,
    takes: u64,
    takes_failed: u64,
}

fn simulate_move(seed: u64, rule: MoveRule) -> MoveOutcome {
    let runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let plan = MovePlan::draw(seed);
    runtime.block_on(async move {
        let handle = Handle::current();
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Store::default();
                move || bucket_node(Arc::clone(&store))
            })
            .build()
            .id();
        let world = Arc::new(MoveWorld {
            start: Instant::now(),
            rule,
            plan: plan.clone(),
            st: Mutex::new(MoveState {
                committed: plan.due,
                key: true,
                ..MoveState::default()
            }),
        });
        let mut nodes = Vec::new();
        for (node, name) in MOVE_NAMES.iter().enumerate() {
            let world = Arc::clone(&world);
            let id = handle
                .create_node()
                .name(*name)
                .ip(format!("10.0.1.{}", node + 1).parse().unwrap())
                .init(move || move_node(Arc::clone(&world), node))
                .build()
                .id();
            nodes.push(id);
        }
        let net = NetSim::current();
        let (mut clogged, mut lifted) = (false, false);
        loop {
            sleep(Duration::from_millis(TICK_MS)).await;
            let now = world.now();
            let started = world.start.elapsed() >= Duration::from_millis(SETUP_MS);
            if let Some((from, until)) = plan.a_clog {
                if started && !clogged && now >= from {
                    clogged = true;
                    net.clog_link(nodes[A], bucket);
                    net.clog_link(bucket, nodes[A]);
                }
                if clogged && !lifted && now >= until {
                    lifted = true;
                    net.unclog_link(nodes[A], bucket);
                    net.unclog_link(bucket, nodes[A]);
                }
            }
            let settled = {
                let st = world.st.lock().unwrap();
                st.committed.is_none() && !st.key
            };
            if (settled && now >= MOVE_QUIET_MS) || now >= MOVE_RUN_MS {
                break;
            }
        }
        let st = world.st.lock().unwrap();
        MoveOutcome {
            plan: plan.clone(),
            delivered: st.delivered,
            double: double_ownership(&st.claims).map(|v| format!("{v:?}")),
            done: st.done.clone(),
            gave_up: st.gave_up.clone(),
            self_takes: st.self_takes,
            claimed_back: st.claimed_back,
            key_left: st.key,
            requests_ok: st.requests_ok,
            requests_failed: st.requests_failed,
            takes: st.takes,
            takes_failed: st.takes_failed,
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveBroken {
    TwoOwners,
    Lost,
    NotHeldWhenDone,
    ClaimedBack,
    Unfinished,
    Late,
}

fn move_violation(o: &MoveOutcome) -> Option<(MoveBroken, String)> {
    if let Some(double) = &o.double {
        return Some((MoveBroken::TwoOwners, format!("two owners: {double}")));
    }
    if let Some(&(at, node, _)) = o.done.iter().find(|d| !d.2) {
        return Some((
            MoveBroken::NotHeldWhenDone,
            format!(
                "{} reported the move done at {at} ms, b not holding X",
                MOVE_NAMES[node]
            ),
        ));
    }
    if let Some((at, node)) = o.claimed_back {
        return Some((
            MoveBroken::ClaimedBack,
            format!(
                "{} claimed X back at {at} ms during its own move",
                MOVE_NAMES[node]
            ),
        ));
    }
    if let Some(due) = o.plan.due {
        let Some((at, node)) = o.delivered else {
            return Some((
                MoveBroken::Lost,
                format!("the alarm due at {due} ms was never delivered"),
            ));
        };
        if due >= o.plan.key_at && at > o.plan.deadline(due) {
            return Some((
                MoveBroken::Late,
                format!(
                    "the alarm due at {due} ms was delivered at {at} ms by {}, past its \
                     deadline {} ms ({HANDOFF_BOUND_MS} ms bound)",
                    MOVE_NAMES[node],
                    o.plan.deadline(due)
                ),
            ));
        }
    }
    if o.key_left {
        return Some((MoveBroken::Unfinished, "the move never finished".into()));
    }
    None
}

#[test]
fn a_move_hands_the_actor_to_its_target() {
    let seeds = seeds();
    assert!(seeds > 0, "the move simulation needs at least one seed");
    let (mut done, mut gave_up, mut self_takes, mut ok, mut failed, mut takes, mut takes_failed) =
        (0, 0, 0, 0, 0, 0, 0);
    for seed in 0..seeds {
        let o = simulate_move(seed, MoveRule::Production);
        if let Some((_, broken)) = move_violation(&o) {
            panic!("seed {seed}: {broken}");
        }
        done += o.done.len();
        gave_up += o.gave_up.len();
        self_takes += o.self_takes;
        ok += o.requests_ok;
        failed += o.requests_failed;
        takes += o.takes;
        takes_failed += o.takes_failed;
    }
    assert!(
        done as u64 > seeds / 2,
        "few moves done by the owner ({done})"
    );
    eprintln!(
        "move: {seeds} seeds: {done} moves done by the owner, {gave_up} given up to another \
         holder, {self_takes} taken by the target alone; {takes} take calls, {takes_failed} \
         failed; {ok} events run, {failed} given up by clients"
    );
}

#[test]
fn the_unsafe_move_rules_are_caught() {
    // Each search stops at its first catch; clearing the alarm early shows
    // only in an idle move whose take fails (about one seed in a hundred,
    // fewer where `a`'s outage moves the deadline).
    let seeds = seeds().max(2_000);
    for (rule, breaks) in [
        (MoveRule::HibernateEarly, MoveBroken::ClaimedBack),
        (MoveRule::ClearAlarmEarly, MoveBroken::Late),
        (MoveRule::RunOnLeaseOnly, MoveBroken::TwoOwners),
    ] {
        let found = (0..seeds).find_map(|seed| {
            move_violation(&simulate_move(seed, rule))
                .filter(|(b, _)| *b == breaks)
                .map(|(_, why)| (seed, why))
        });
        let (seed, why) =
            found.unwrap_or_else(|| panic!("{rule:?} never broke {breaks:?} in {seeds} seeds"));
        assert_eq!(
            move_violation(&simulate_move(seed, MoveRule::Production)).map(|(_, why)| why),
            None,
            "seed {seed} breaks the correct rule too"
        );
        eprintln!("move: {rule:?} caught ({breaks:?}) at seed {seed}: {why}");
    }
}

#[test]
fn a_move_replays_with_the_same_seed() {
    assert_eq!(
        simulate_move(41, MoveRule::Production),
        simulate_move(41, MoveRule::Production)
    );
}
