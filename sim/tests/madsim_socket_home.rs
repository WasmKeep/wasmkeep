//! Socket home ([ADR 0024]) under madsim: three nodes on the shared node
//! (`wasmkeep_sim::node`, the server's rules), one room, one bucket, seeded
//! faults (cut links, slow writes that land late, crashes and restarts as a
//! new instance, clean stops, drains).
//!
//! The room's sockets live on the node that ran it when they opened. When
//! the room hibernates with sockets open, its node parks the hold
//! (`LeaseNode::park`): the record keeps naming it. Events land on random
//! nodes; a node that does not hold the room asks the record where it is
//! (`other_owner`) and relays the event to the node it names, if that node
//! is running (the membership view), which wakes the room (an acquisition
//! at a new generation); otherwise the event acquires where it landed. The
//! last socket closing releases the parked record, as do drains, clean
//! stops and lapses. A node whose lease is not valid closes its sockets.
//!
//! Checked, on every seed:
//! - never two processes acting as owner at once, never one generation for
//!   two holds (ADR 0020's invariants);
//! - never a room taken by another node while a node keeps it parked with
//!   its lease valid (two homes);
//! - never a room taken by another node while a live node with a valid
//!   lease, not draining, holds its sockets, unless that node's own wake
//!   could not tell what it wrote (an unknown outcome or another holder
//!   found: the room is then exposed, which the ADR accepts: the sweep
//!   closes those sockets);
//! - never a record left naming a node that neither holds, parks, nor has
//!   anything pending for it, while its lease is valid (a leak: the room
//!   would be pinned to a node that no longer serves it; a drain would
//!   report drained with it).
//!
//! Broken variants each caught on some seed: a wake that takes the parked
//! hold back without writing (ADR 0020's rejected first draft), a lapse
//! that forgets parked holds, releasing on hibernation (the behaviour
//! before ADR 0024, which loses the sockets), and a resolver that ends an
//! unresolved acquire on its first read of an unchanged record (ADR 0020
//! before its amendment: a write landing after that read is a leak), or
//! that waits for one without a bound (an unresolved acquire kept past its
//! deadline, which would hold a drain forever).
//!
//! The scenario (`a_room_wakes_where_its_sockets_are`): without faults, a
//! room with a socket on n1 is called through n2 and n3 and wakes on n1
//! each time; n1 is killed, and another node takes the room within the
//! failover bound; its new node parks it with a socket, and the last
//! socket closing releases the record.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim --test madsim_socket_home`
//!
//! [ADR 0024]: ../../docs/adr/0024-socket-home.md

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Bucket, LeaseConfig};
use wasmkeep_cluster::node_lease::{AcquireError, OwnerRecord};
use wasmkeep_sim::check::{double_ownership, stop_process};
use wasmkeep_sim::driver::{Claim, Env, SkewedClock};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node_with};

const ACTOR: &str = "owners/t/chat/lobby";
const NODES: &str = "nodes/";
const EPOCH_MS: u64 = 1_000_000;
/// The bucket client's timeout (`EpBucket::connect`): one attempt's limit.
const ATTEMPT_MS: u64 = 300;
/// The scenario's bound on a takeover after the room's node is killed:
/// ttl plus grace (1.5 s), the fence watch (a lease period plus an
/// attempt), a lease period until the fencer looks, the skew allowed, and
/// the time an event takes to arrive.
const TAKEOVER_MS: u64 = 3_500;

/// ttl 1 s, grace 500 ms, safety 200 ms: tolerates 350 ms of skew.
fn config() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    Correct,
    /// A wake takes the parked hold back without writing the record.
    WakeReusesParked,
    /// A lapse forgets the parked holds.
    LapseForgetsParked,
    /// A hibernation with sockets open releases the record (before ADR
    /// 0024).
    ReleaseOnHibernate,
    /// The resolver counts a parked hold at any generation.
    ResolverIgnoresGeneration,
    /// A wake with an unknown outcome drops the parked hold.
    UnknownWakeDropsParked,
    /// The resolver ends an unresolved acquire on its first read of an
    /// unchanged record (ADR 0020 before its 2026-11-14 amendment).
    ResolverEndsUnchanged,
    /// The resolver waits for an unchanged record without a bound.
    ResolverNeverEnds,
}

#[derive(Clone, Copy)]
struct SimEnv(Instant);

impl SimEnv {
    fn now(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
    /// Since the simulation began.
    fn elapsed(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

impl Env for SimEnv {
    fn global_ms(&self) -> u64 {
        self.now()
    }

    async fn sleep_ms(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }
}

#[derive(Clone, Debug)]
struct HoldSeen {
    node: u32,
    instance: u64,
    generation: u64,
    serial: u64,
}

type Probe = Box<dyn Fn() -> (bool, bool, u64) + Send + Sync>;

#[derive(Default)]
struct Shared {
    claims: Vec<Claim>,
    holds: Vec<HoldSeen>,
    /// The process each node runs now, while it runs.
    running: HashMap<u32, u64>,
    /// Open sockets of the room on each node's current process.
    sockets: HashMap<u32, u32>,
    /// The epoch of the node's validity its sockets were opened in: the
    /// server closes them all when it moves (a lapse), whatever the
    /// workload has seen yet.
    sockets_epoch: HashMap<u32, u64>,
    /// A node whose own wake could not tell what it wrote while it had
    /// sockets: its sockets are no longer protected (see the module doc).
    exposed: HashSet<u32>,
    draining: HashSet<u32>,
    /// Each running process's (parked here, lease valid) for the room.
    probes: HashMap<u32, Probe>,
    /// Events relayed to a node, waiting for it.
    inbox: HashSet<u32>,
    acquired: u64,
    parks: u64,
    /// Acquisitions by a node that had the room parked: wakes at home.
    wakes_home: u64,
    relays: u64,
    last_socket_releases: u64,
    drains_with_parked: u64,
    /// Acquisitions while another node held sockets, as allowed (exposed).
    exposed_takeovers: u64,
    two_homes: Vec<String>,
    lost_sockets: Vec<String>,
    leaks: Vec<String>,
    /// The scenario: when the room's first node was killed; acquisitions
    /// by others before then, and the first after.
    killed_at: Option<u64>,
    early_elsewhere: u64,
    taken_after_kill: Option<u64>,
    /// The scenario: n1 parked the room once.
    n1_home: bool,
}

/// What an acquisition by `i` finds on the other nodes; violations
/// recorded.
fn check_takeover(s: &mut Shared, i: u32, at: u64) {
    let others: Vec<(u32, u32)> = s
        .sockets
        .iter()
        .filter(|(j, n)| **j != i && **n > 0)
        .map(|(j, n)| (*j, *n))
        .collect();
    for (j, n) in others {
        let (parked, valid, epoch) = s.probes.get(&j).map_or((false, false, 0), |p| p());
        // Closed by the server at a lapse already (the epoch moved).
        let valid = valid && s.sockets_epoch.get(&j) == Some(&epoch);
        if parked && valid && !s.exposed.contains(&j) {
            s.two_homes.push(format!(
                "at {at}: n{i} took the room while n{j} keeps it parked"
            ));
        }
        if valid && !s.draining.contains(&j) {
            if s.exposed.contains(&j) {
                s.exposed_takeovers += 1;
            } else {
                s.lost_sockets.push(format!(
                    "at {at}: n{i} took the room while n{j} holds {n} sockets"
                ));
            }
        }
    }
}

/// The record's holder (node id, instance) and generation, if readable.
async fn record<B: Bucket>(bucket: &B) -> Option<(Option<(String, String)>, u64)> {
    let (body, _) = bucket.get(ACTOR).await.ok()??;
    let r = OwnerRecord::decode(&body)?;
    Some((r.holder, r.generation))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_node(
    i: u32,
    env: SimEnv,
    skew_ms: i64,
    seed: u64,
    variant: Variant,
    scenario: bool,
    shared: Arc<Mutex<Shared>>,
) {
    let cfg = config();
    let id = format!("n{i}");
    let mut rng = Rng::new(seed);
    let mut serial = 0u64;
    let breaks = Breaks {
        wake_reuses_parked: variant == Variant::WakeReusesParked,
        lapse_forgets_parked: variant == Variant::LapseForgetsParked,
        resolver_ignores_parked_generation: variant == Variant::ResolverIgnoresGeneration,
        unknown_wake_drops_parked: variant == Variant::UnknownWakeDropsParked,
        resolver_ends_unchanged: variant == Variant::ResolverEndsUnchanged,
        resolver_never_ends: variant == Variant::ResolverNeverEnds,
        ..Breaks::default()
    };
    loop {
        let instance = format!("{:08x}", madsim::rand::random::<u32>());
        let process = u64::from_str_radix(&instance, 16).unwrap();
        let clock = SkewedClock {
            global: move || env.now(),
            skew_ms,
        };
        let setup = NodeSetup {
            id: id.clone(),
            peers: (1..=3u32)
                .filter(|&p| p != i)
                .map(|p| format!("n{p}"))
                .collect(),
            nodes_prefix: NODES.to_owned(),
            config: cfg,
            attempt_ms: ATTEMPT_MS,
            breaks,
        };
        let buckets = NodeBuckets {
            lease: EpBucket::connect().await,
            task_records: EpBucket::connect().await,
            peers: EpBucket::connect().await,
            records: EpBucket::connect().await,
        };
        let (node, task) =
            LeaseNode::start(setup, &instance, clock, buckets, Vec::new(), &env).await;
        {
            let mut s = shared.lock().unwrap();
            s.running.insert(i, process);
            s.sockets.insert(i, 0);
            s.exposed.remove(&i);
            s.draining.remove(&i);
            s.probes.insert(i, Box::new(node.probe(ACTOR)));
        }
        madsim::task::spawn_local(task.run(env));
        let bucket = EpBucket::connect().await;
        let end = |shared: &Mutex<Shared>| {
            stop_process(&mut shared.lock().unwrap().claims, i, process, env.now());
        };
        let sockets = |shared: &Mutex<Shared>| shared.lock().unwrap().sockets[&i];
        let set_sockets = |shared: &Mutex<Shared>, n: u32| {
            let mut s = shared.lock().unwrap();
            if s.sockets[&i] == 0 && n > 0 {
                s.sockets_epoch.insert(i, node.epoch());
            }
            s.sockets.insert(i, n);
            if n == 0 {
                s.exposed.remove(&i);
            }
        };
        let mut hold: Option<Held> = None;
        let mut held_since = 0u64;
        let mut drain_until: Option<u64> = None;
        let mut leak_suspect: Option<u64> = None;
        let mut overdue_seen = false;
        let mut next_leak_check = 0u64;
        let gone = |s: &mut Shared| {
            s.running.remove(&i);
            s.probes.remove(&i);
            s.sockets.insert(i, 0);
            s.draining.remove(&i);
        };
        loop {
            let t = env.elapsed();
            if node.lost() {
                end(&shared);
                gone(&mut shared.lock().unwrap());
                break;
            }
            // A node whose lease is not valid closes every socket (the
            // server's close at the end of validity).
            let opened_in = shared.lock().unwrap().sockets_epoch.get(&i).copied();
            if !node.lease_valid() || opened_in != Some(node.epoch()) {
                set_sockets(&shared, 0);
            }
            let acting = hold.as_ref().is_some_and(|h| node.may_act(h));
            if hold.is_some() && !acting {
                end(&shared);
                if hold.as_ref().is_some_and(|h| !node.still_held(h)) {
                    hold = None;
                }
            }
            // A clean stop now and then: sockets close, holds and parked
            // records released, then the lease.
            if !scenario && rng.below(500) == 0 {
                set_sockets(&shared, 0);
                end(&shared);
                node.clean_stop(&env).await;
                gone(&mut shared.lock().unwrap());
                break;
            }
            // A drain now and then: sockets close (1012), the hold and the
            // parked record released, no acquisition until it ends.
            if !scenario && drain_until.is_none() && rng.below(150) == 0 {
                drain_until = Some(t + 1_000 + rng.below(2_000));
                let mut s = shared.lock().unwrap();
                s.draining.insert(i);
                if node.is_parked(ACTOR) {
                    s.drains_with_parked += 1;
                }
            }
            let draining = drain_until.is_some_and(|u| t < u);
            if drain_until.is_some() && !draining {
                drain_until = None;
                shared.lock().unwrap().draining.remove(&i);
            }
            if draining {
                set_sockets(&shared, 0);
                if let Some(h) = hold.take() {
                    end(&shared);
                    node.release(&h).await;
                }
                if node.is_parked(ACTOR) {
                    node.release_parked(ACTOR).await;
                }
                shared.lock().unwrap().inbox.remove(&i);
            }
            if acting && let Some(h) = hold.clone() {
                let until = node
                    .valid_until_ms()
                    .unwrap_or(0)
                    .saturating_add_signed(-skew_ms);
                shared.lock().unwrap().claims.push(Claim {
                    node: i,
                    instance: process,
                    generation: h.generation,
                    from_ms: env.now(),
                    until_ms: until,
                });
                let n = sockets(&shared);
                // Sockets open on the room's node while it runs.
                let open = if scenario {
                    n == 0 && t < SCENARIO_CLOSE_MS
                } else {
                    n < 3 && rng.below(10) == 0
                };
                if open && !draining {
                    set_sockets(&shared, n + 1);
                } else if !scenario && n > 0 && rng.below(25) == 0 {
                    set_sockets(&shared, n - 1);
                }
                let hibernate = if scenario {
                    t >= held_since + 200 && rng.below(4) == 0
                } else {
                    rng.below(8) == 0
                };
                if hibernate {
                    end(&shared);
                    hold = None;
                    if sockets(&shared) > 0 && !draining && variant != Variant::ReleaseOnHibernate {
                        if node.park(&h).await {
                            let mut s = shared.lock().unwrap();
                            s.parks += 1;
                            s.n1_home |= i == 1;
                        } else {
                            shared.lock().unwrap().exposed.insert(i);
                        }
                    } else {
                        node.release(&h).await;
                    }
                }
            } else if hold.is_none() {
                // Sockets of a room not in memory close now and then; the
                // last one releases its parked record.
                let n = sockets(&shared);
                let close_all = scenario && t >= SCENARIO_CLOSE_MS;
                if n > 0 && (close_all || (!scenario && rng.below(25) == 0)) {
                    set_sockets(&shared, if close_all { 0 } else { n - 1 });
                }
                if sockets(&shared) == 0
                    && node.is_parked(ACTOR)
                    && node.release_parked(ACTOR).await
                {
                    shared.lock().unwrap().last_socket_releases += 1;
                }
                // The owner sweep: sockets of a room not parked here, that
                // another node holds, close.
                if sockets(&shared) > 0
                    && !node.is_parked(ACTOR)
                    && rng.below(10) == 0
                    && matches!(node.other_owner(ACTOR).await, Ok(Some(_)))
                {
                    set_sockets(&shared, 0);
                }
                // An event: arrived here from outside, or relayed here.
                let relayed = shared.lock().unwrap().inbox.remove(&i);
                let arrives = relayed
                    || if scenario {
                        // n1 gets every event at first, so it surely takes
                        // the room; the others now and then.
                        scenario_event(i, t) && rng.below(if i == 1 { 1 } else { 6 }) == 0
                    } else {
                        rng.below(15) == 0
                    };
                if arrives && !draining {
                    // Where the record says the room is, if that node runs
                    // (the membership view); relayed events are run here.
                    let target = if relayed {
                        None
                    } else {
                        match node.other_owner(ACTOR).await {
                            Ok(Some(owner)) => {
                                let o: u32 = owner[1..].parse().unwrap();
                                let s = shared.lock().unwrap();
                                (s.running.contains_key(&o) && !s.draining.contains(&o))
                                    .then_some(o)
                            }
                            _ => None,
                        }
                    };
                    if let Some(o) = target {
                        let mut s = shared.lock().unwrap();
                        s.inbox.insert(o);
                        s.relays += 1;
                    } else {
                        let had_parked = node.is_parked(ACTOR);
                        let had_sockets = sockets(&shared) > 0;
                        let got = node.wake(ACTOR, 1).await;
                        match &got {
                            Ok(h) if node.may_act(h) => {
                                let until = node
                                    .valid_until_ms()
                                    .unwrap_or(0)
                                    .saturating_add_signed(-skew_ms);
                                serial += 1;
                                let mut s = shared.lock().unwrap();
                                let at = env.now();
                                check_takeover(&mut s, i, at);
                                s.acquired += 1;
                                s.wakes_home += u64::from(had_parked);
                                s.exposed.remove(&i);
                                if scenario {
                                    match s.killed_at {
                                        None if i != 1 && s.n1_home => s.early_elsewhere += 1,
                                        Some(k) if i != 1 && s.taken_after_kill.is_none() => {
                                            s.taken_after_kill = Some(at - k);
                                        }
                                        _ => {}
                                    }
                                }
                                s.holds.push(HoldSeen {
                                    node: i,
                                    instance: process,
                                    generation: h.generation,
                                    serial,
                                });
                                s.claims.push(Claim {
                                    node: i,
                                    instance: process,
                                    generation: h.generation,
                                    from_ms: at,
                                    until_ms: until,
                                });
                                drop(s);
                                held_since = t;
                                hold = Some(h.clone());
                            }
                            Err(AcquireError::Unresolved { .. } | AcquireError::Held { .. })
                                if had_sockets =>
                            {
                                shared.lock().unwrap().exposed.insert(i);
                            }
                            _ => {}
                        }
                    }
                }
            }
            // The leak check, about twice a second: a record naming this
            // process, with nothing here that holds, parks or releases it,
            // seen twice at one generation while the lease is valid.
            if hold.is_none() && env.now() >= next_leak_check {
                next_leak_check = env.now() + 500;
                // An unresolved acquire still pending well past its
                // deadline, seen twice while the lease is valid: the
                // resolver's wait has no bound, and a drain never ends.
                if node.overdue_unresolved(1_500) {
                    if overdue_seen {
                        shared.lock().unwrap().leaks.push(format!(
                            "at {t}: n{i} keeps an unresolved acquire past its deadline"
                        ));
                    }
                    overdue_seen = true;
                } else {
                    overdue_seen = false;
                }
                let idle = !node.is_parked(ACTOR) && node.nothing_pending() && node.lease_valid();
                let mine = match record(&bucket).await {
                    Some((Some((n, inst)), g)) if n == id && inst == instance => Some(g),
                    _ => None,
                };
                let still_idle =
                    !node.is_parked(ACTOR) && node.nothing_pending() && node.lease_valid();
                match (idle && still_idle && hold.is_none(), mine) {
                    (true, Some(g)) if leak_suspect == Some(g) => {
                        shared
                            .lock()
                            .unwrap()
                            .leaks
                            .push(format!("at {t}: the record names n{i} at {g}, unheld"));
                        leak_suspect = None;
                    }
                    (true, Some(g)) => leak_suspect = Some(g),
                    _ => leak_suspect = None,
                }
            }
            sleep(Duration::from_millis(50 + rng.below(50))).await;
        }
        sleep(Duration::from_millis(200 + rng.below(800))).await;
    }
}

/// The scenario's timeline (ms since the start): n1 alone gets events
/// first; then n2 and n3; n1 is killed at [`SCENARIO_KILL_MS`]; every
/// socket closes at [`SCENARIO_CLOSE_MS`], and no event comes after.
const SCENARIO_OTHERS_MS: u64 = 3_000;
const SCENARIO_KILL_MS: u64 = 6_000;
const SCENARIO_CLOSE_MS: u64 = 11_000;
const SCENARIO_END_MS: u64 = 14_000;

fn scenario_event(i: u32, t: u64) -> bool {
    match t {
        t if t < SCENARIO_OTHERS_MS => i == 1,
        t if t < SCENARIO_CLOSE_MS => i != 1,
        _ => false,
    }
}

struct Outcome {
    claims: Vec<Claim>,
    holds: Vec<HoldSeen>,
    acquired: u64,
    parks: u64,
    wakes_home: u64,
    relays: u64,
    last_socket_releases: u64,
    drains_with_parked: u64,
    exposed_takeovers: u64,
    two_homes: Vec<String>,
    lost_sockets: Vec<String>,
    leaks: Vec<String>,
    early_elsewhere: u64,
    taken_after_kill: Option<u64>,
    n1_home: bool,
    final_holder: Option<Option<(String, String)>>,
}

impl Outcome {
    /// The first violation, if any.
    fn violation(&self) -> Option<String> {
        if let Some(d) = double_ownership(&self.claims) {
            return Some(format!("two owners: {d:?}"));
        }
        if let Some(pair) = shared_generation(&self.holds) {
            return Some(format!("one generation, two holds: {pair:?}"));
        }
        self.two_homes
            .first()
            .or(self.lost_sockets.first())
            .or(self.leaks.first())
            .cloned()
    }
}

fn simulate(seed: u64, variant: Variant, scenario: bool) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let shared = Arc::new(Mutex::new(Shared::default()));
    let out = Arc::clone(&shared);
    let final_holder = Arc::new(Mutex::new(None));
    let final_out = Arc::clone(&final_holder);
    rt.block_on(async move {
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let mut rng = Rng::new(seed ^ 0x50C4_E7E5);
        let store = Store::default();
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Arc::clone(&store);
                let slow_one_in = if scenario { 0 } else { 25 };
                move || bucket_node_with(Arc::clone(&store), bucket::Faults { slow_one_in })
            })
            .build();
        let max_skew = config().max_skew_ms();
        let mut nodes = Vec::new();
        for i in 1..=3u32 {
            let skew_ms = i64::try_from(rng.below(max_skew * 2 + 1)).unwrap()
                - i64::try_from(max_skew).unwrap();
            let shared = Arc::clone(&shared);
            let node_seed = seed ^ (u64::from(i) << 40);
            let node = handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || {
                    run_node(
                        i,
                        env,
                        skew_ms,
                        node_seed,
                        variant,
                        scenario,
                        Arc::clone(&shared),
                    )
                })
                .build();
            nodes.push(node.id());
        }
        let net = NetSim::current();
        let kill = |s: &mut Shared, i: u32| {
            if let Some(process) = s.running.remove(&i) {
                stop_process(&mut s.claims, i, process, env.now());
            }
            s.probes.remove(&i);
            s.sockets.insert(i, 0);
            s.draining.remove(&i);
        };
        let duration_ms = if scenario { SCENARIO_END_MS } else { 30_000 };
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        for step in 1..=duration_ms / 50 {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            if scenario {
                let mut s = shared.lock().unwrap();
                if at >= SCENARIO_KILL_MS && s.killed_at.is_none() {
                    kill(&mut s, 1);
                    s.killed_at = Some(env.now());
                    drop(s);
                    handle.kill(nodes[0]);
                }
                continue;
            }
            let n = usize::try_from(rng.below(3)).unwrap();
            match rng.below(30) {
                0..=2 => {
                    net.clog_link(nodes[n], bucket.id());
                    net.clog_link(bucket.id(), nodes[n]);
                    held.push((n, at + 300 + rng.below(3_000)));
                }
                3 if rng.below(4) == 0 && !crashed.iter().any(|(c, _)| *c == n) => {
                    kill(&mut shared.lock().unwrap(), u32::try_from(n).unwrap() + 1);
                    handle.kill(nodes[n]);
                    crashed.push((n, at + 500 + rng.below(2_000)));
                }
                _ => {}
            }
            held.retain(|(h, t)| {
                if *t <= at {
                    net.unclog_link(nodes[*h], bucket.id());
                    net.unclog_link(bucket.id(), nodes[*h]);
                    false
                } else {
                    true
                }
            });
            crashed.retain(|(c, t)| {
                if *t <= at {
                    handle.restart(nodes[*c]);
                    false
                } else {
                    true
                }
            });
        }
        let body = store.lock().unwrap().get(ACTOR).map(|(b, _)| b.clone());
        *final_out.lock().unwrap() = body.and_then(|b| OwnerRecord::decode(&b)).map(|r| r.holder);
    });
    let s = out.lock().unwrap();
    let final_holder = final_holder.lock().unwrap().clone();
    Outcome {
        claims: s.claims.clone(),
        holds: s.holds.clone(),
        acquired: s.acquired,
        parks: s.parks,
        wakes_home: s.wakes_home,
        relays: s.relays,
        last_socket_releases: s.last_socket_releases,
        drains_with_parked: s.drains_with_parked,
        exposed_takeovers: s.exposed_takeovers,
        two_homes: s.two_homes.clone(),
        lost_sockets: s.lost_sockets.clone(),
        leaks: s.leaks.clone(),
        early_elsewhere: s.early_elsewhere,
        taken_after_kill: s.taken_after_kill,
        n1_home: s.n1_home,
        final_holder,
    }
}

/// Two holds (different processes, or different serials) with one
/// generation.
fn shared_generation(holds: &[HoldSeen]) -> Option<(HoldSeen, HoldSeen)> {
    let mut by: HashMap<u64, &HoldSeen> = HashMap::new();
    for h in holds {
        if let Some(prev) = by.insert(h.generation, h)
            && (prev.node != h.node || prev.instance != h.instance || prev.serial != h.serial)
        {
            return Some((prev.clone(), h.clone()));
        }
    }
    None
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn a_room_keeps_one_home_and_releases_it() {
    let mut totals = [0u64; 7];
    for seed in 0..seeds() {
        let o = simulate(seed, Variant::Correct, false);
        if let Some(v) = o.violation() {
            panic!("seed {seed}: {v}");
        }
        for (t, n) in totals.iter_mut().zip([
            o.acquired,
            o.parks,
            o.wakes_home,
            o.relays,
            o.last_socket_releases,
            o.drains_with_parked,
            o.exposed_takeovers,
        ]) {
            *t += n;
        }
    }
    let [acquired, parks, wakes, relays, last, drains, exposed] = totals;
    eprintln!(
        "{} seeds: {acquired} acquisitions, {parks} parks, {wakes} wakes at home, \
         {relays} relays, {last} releases at the last socket, {drains} drains of a parked \
         room, {exposed} exposed takeovers",
        seeds()
    );
    // The runs reach what they claim to.
    let floor = (seeds() / 10).max(1);
    for (what, n) in [
        ("parks", parks),
        ("wakes at home", wakes),
        ("relays", relays),
        ("releases at the last socket", last),
        ("drains of a parked room", drains),
    ] {
        assert!(n >= floor, "{n} {what}, want {floor}");
    }
}

/// Each broken variant is caught on some seed: the checks have teeth.
#[test]
fn each_broken_variant_is_caught() {
    // Each with the violation it must cause: one of another kind is not
    // the check catching it.
    for (variant, kind) in [
        (Variant::WakeReusesParked, "one generation, two holds"),
        (Variant::LapseForgetsParked, "unheld"),
        (Variant::ReleaseOnHibernate, " sockets"),
        (Variant::ResolverIgnoresGeneration, "unheld"),
        (Variant::UnknownWakeDropsParked, "unheld"),
        (Variant::ResolverEndsUnchanged, "unheld"),
        (Variant::ResolverNeverEnds, "past its deadline"),
    ] {
        // The resolver variants and an unknown wake that drops the parked
        // hold need a rarer fault (an unknown write of a parked room, or
        // one that never lands): up to 20 times the seeds, which CI's 200
        // seeds make 4,000 (the last ones are caught first by seed 2,000).
        let most = seeds().max(200) * 20;
        let caught = (0..most).find_map(|seed| {
            simulate(seed, variant, false)
                .violation()
                .filter(|v| v.contains(kind))
                .map(|v| (seed, v))
        });
        eprintln!("{variant:?}: caught by {caught:?}");
        assert!(caught.is_some(), "{variant:?} never caused {kind:?}");
    }
}

/// Without faults: a room with a socket on n1, called through n2 and n3,
/// wakes on n1 each time; n1 is killed and another node takes the room
/// within the failover bound; the last socket closing releases the record.
#[test]
fn a_room_wakes_where_its_sockets_are() {
    for seed in 0..seeds().min(50) {
        let o = simulate(seed, Variant::Correct, true);
        if let Some(v) = o.violation() {
            panic!("seed {seed}: {v}");
        }
        assert!(
            o.n1_home,
            "seed {seed}: n1 never parked the room: {} acquisitions, {} parks, holds {:?}",
            o.acquired, o.parks, o.holds
        );
        assert!(
            o.relays > 0 && o.wakes_home > 0,
            "seed {seed}: no wake at home"
        );
        assert_eq!(
            o.early_elsewhere, 0,
            "seed {seed}: woken away from its sockets"
        );
        let after = o
            .taken_after_kill
            .unwrap_or_else(|| panic!("seed {seed}: nobody took the room after the kill"));
        assert!(
            after <= TAKEOVER_MS,
            "seed {seed}: taken {after} ms after the kill"
        );
        assert!(o.parks >= 2, "seed {seed}: {} parks", o.parks);
        assert!(
            o.last_socket_releases > 0,
            "seed {seed}: no release at the last socket"
        );
        assert_eq!(
            o.final_holder,
            Some(None),
            "seed {seed}: the record still names someone"
        );
    }
}
