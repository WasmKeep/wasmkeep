//! `wasmkeep_cluster::node_lease` (ADR 0020) under madsim: three nodes with
//! skewed clocks plus a twin of the first (a second process configured
//! with its node id), one bucket, one actor, seeded faults (cut links that
//! drop requests and replies, slow writes that land late after their reply
//! was lost, crashes and restarts as a new instance, clean stops).
//!
//! Each process runs on the simulations' shared node (`wasmkeep_sim::node`,
//! the server's rules): the start check, its lease on a task of its own
//! (renewed every third of the ttl, a lapse ending its holds into the
//! pending releases, stale peers watched and fenced, pending releases and
//! unresolved acquires retried under the actor's lock), and the workload:
//! it acquires the actor's ownership record when it holds nothing, acts
//! while the hold may act, gives the hold up now and then, and stops
//! cleanly now and then; a lost lease ends the process. Checked: never two
//! processes acting as owner at the same global moment (the twin counts as
//! another process), and never one generation for two holds. Each broken
//! variant must be caught on some seed. An operator fences nodes at
//! random, live or not, as `wasmkeep cluster fence` may. The correct run
//! must also take the record over each way the free rule allows (from
//! nobody, from a fenced node, from a node's older instance), and the twin
//! must hold the actor now and then.
//!
//! With waiters (`a_waiter_follows_the_owner_watch`), a process refused
//! because another node holds the actor may keep asking as the server
//! keeps a request for a dead node's actor: it feeds an
//! `OwnerWatch` (`wasmkeep_cluster::failover`) from its own listings of
//! the node leases and asks again only when its `Waiter` says so, until a
//! deadline. The same checks hold with those acquisitions; a waiter never
//! asks while the holder is only stale, nor more often than its retry
//! period; and after a crash with one request per survivor and no retry
//! (`a_waiter_takes_over_after_a_crash`), a survivor holds the actor
//! within the wait. This models acquisitions, not requests: what a kept
//! request's caller sees is the cluster tests' to show.
//!
//! Ignoring the node's epoch rule is never caught here, and a test says
//! why (`ignoring_the_epoch_is_never_caught_here`).
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::failover::{OwnerWatch, Seen, Step, Verdict, Waiter};
use wasmkeep_cluster::lease::{Bucket, Clock, LeaseConfig};
use wasmkeep_cluster::node_lease::{
    AcquireError, Hold, NodeLeaseObject, NodeState, OwnerRecord, frees, operator_fence, tail,
};
use wasmkeep_sim::check::{double_ownership, stop_process};
use wasmkeep_sim::driver::{Claim, Env, SkewedClock};
use wasmkeep_sim::node::{
    Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences, take_old_epoch_acts,
};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node_with};

const ACTOR: &str = "owners/t/c/a";
/// The madsim node running node n1's twin: a second process configured
/// with n1's id (an operator's mistake ADR 0020's start check survives).
const TWIN: usize = 3;
const NODES: &str = "nodes/";
const EPOCH_MS: u64 = 1_000_000;
/// The bucket client's timeout (`EpBucket::connect`): one attempt's limit.
const ATTEMPT_MS: u64 = 300;

#[derive(Clone, Copy)]
struct SimEnv(Instant);

impl SimEnv {
    fn now(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// How long a node's lease counts as live in a waiter's view: three lease
/// periods plus the skew allowed, as the server's heartbeat horizon.
const HORIZON_MS: u64 = 1_300;
/// How often a waiting process lists the node leases.
const LISTING_MS: u64 = 300;
/// The least time between two tries of one waiter.
const RETRY_MS: u64 = 300;
/// How long a waiter keeps asking: ttl plus grace, the fence watch (a
/// lease period plus an attempt), a lease period until the fencer looks,
/// a listing, the jitter and a retry, and the skew allowed.
const WAIT_MS: u64 = 3_500;

/// ttl 1 s, grace 500 ms, safety 200 ms: tolerates 350 ms of skew.
fn config() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
}

/// What the simulation deliberately breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    Correct,
    /// The fence overwrites the node lease without its condition.
    UnconditionalFence,
    /// An acquisition writes the record's generation again.
    ReuseGeneration,
    /// A fenced node's records are free at once, without the time bound.
    FreeOnFenceAtOnce,
    /// The resolver releases what an unresolved acquire wrote even while
    /// this node holds the actor: a later acquisition that landed at the
    /// same generation is released under it (what the lock per actor and
    /// the check for a live hold close).
    ResolveWhileHeld,
    /// A lapse ends no hold: a hold acquired before it acts again once the
    /// lease is renewed (the node's epoch rule ignored).
    EpochIgnored,
    /// The correct rules, with waiters: a refused process may keep asking
    /// as its `OwnerWatch` and `Waiter` say.
    Waiters,
    /// Waiters that take the watch's `Free` for a grant: the record is
    /// written without asking whether its holder's node frees it.
    HintAsGrant,
    /// Waiters that never wait: a refusal is final, as without the wait.
    GiveUp,
}

impl Variant {
    fn waiters(self) -> bool {
        matches!(self, Self::Waiters | Self::HintAsGrant | Self::GiveUp)
    }
}

/// One process's wait for the actor another node holds.
struct Wait {
    owner: String,
    waiter: Waiter,
    /// When it began (this node's clock), and the tries made since.
    began_ms: u64,
    tries: u64,
}

/// What the waiters of a simulation did.
#[derive(Clone, Debug, Default)]
struct Waits {
    /// Waits begun, by the verdict that admitted them.
    suspect: u64,
    stale: u64,
    stopping: u64,
    /// How they ended: holding the actor, another node holding it, the
    /// holder shown alive, the deadline.
    took: u64,
    saw_other: u64,
    left: u64,
    expired: u64,
    /// Tries made while the holder was only stale, or fenced with its
    /// date not passed: none may be.
    tried_unfree: u64,
    /// Waits that tried more often than once per retry period.
    spun: u64,
}

/// One hold, as the checks see it.
#[derive(Clone, Debug)]
struct HoldSeen {
    node: u32,
    instance: String,
    generation: u64,
    serial: u64,
}

impl Env for SimEnv {
    fn global_ms(&self) -> u64 {
        self.now()
    }

    async fn sleep_ms(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }
}

#[derive(Default)]
struct Shared {
    claims: Vec<Claim>,
    holds: Vec<HoldSeen>,
    acquired: u64,
    clean_stops: u64,
    /// Node objects the operator read without their node's payload (a
    /// fence or a release that dropped the heartbeat's fields).
    lost_payloads: u64,
    /// Node objects the operator deleted before fencing them as absent.
    deletions: u64,
    /// Acquisitions by what the record and its holder's node object said
    /// just before (see [`takeover_reason`]).
    reasons: HashMap<&'static str, u64>,
    /// The process each madsim node runs now (node id, instance): a crash
    /// ends that process's claims.
    running: HashMap<usize, (u32, u64)>,
    /// Acquisitions by the twin's processes.
    twin_acquired: u64,
    waits: Waits,
    /// The crash scenario: when the holder was killed (global ms). From
    /// then on each surviving process asks once, and waits or not.
    killed_at: Option<u64>,
    killed_node: u32,
}

/// What node `i` writes after its lease lines: the heartbeat's fields,
/// which every fence and release must keep.
fn payload(i: u64) -> Vec<u8> {
    format!("n{i}\nhttp://10.0.1.{i}:8080\n").into_bytes()
}

/// What an acquisition is about to take over, read just before it.
async fn takeover_reason<B: Bucket>(bucket: &B, node: &str, instance: &str) -> &'static str {
    let Ok(Some((body, _))) = bucket.get(ACTOR).await else {
        return "absent";
    };
    let Some((n, i)) = OwnerRecord::decode(&body).and_then(|r| r.holder) else {
        return "nobody";
    };
    if n == node && i == instance {
        return "this process";
    }
    match bucket
        .get(&format!("{NODES}{n}"))
        .await
        .ok()
        .flatten()
        .and_then(|(b, _)| NodeLeaseObject::decode(&b))
    {
        Some(o) if o.state == NodeState::Fenced => "fenced",
        Some(o) if o.state == NodeState::Released => "released",
        Some(o) if o.instance.as_deref() != Some(i.as_str()) => "another instance",
        _ => "held",
    }
}

/// An acquisition with the broken variants' changes (the correct path is
/// `Owners::acquire` itself).
async fn acquire_broken<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    node: &str,
    instance: &str,
    variant: Variant,
    valid: &dyn Fn() -> bool,
) -> Result<Hold, AcquireError> {
    let cfg = config();
    let mine = |generation| {
        OwnerRecord {
            holder: Some((node.to_owned(), instance.to_owned())),
            generation,
        }
        .encode()
    };
    let read = bucket
        .get(ACTOR)
        .await
        .map_err(|_| AcquireError::Unavailable)?;
    let (generation, written) = match read {
        None => {
            if !valid() {
                return Err(AcquireError::LeaseInvalid);
            }
            (1, bucket.create(ACTOR, mine(1)).await)
        }
        Some((body, version)) => {
            let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
            if let Some((n, i)) = &record.holder
                && !(n == node && i == instance)
            {
                let obj = bucket
                    .get(&format!("{NODES}{n}"))
                    .await
                    .map_err(|_| AcquireError::Unavailable)?
                    .and_then(|(b, _)| NodeLeaseObject::decode(&b));
                let free = match (variant, &obj) {
                    (Variant::FreeOnFenceAtOnce, Some(o)) if o.state != NodeState::Live => true,
                    (Variant::HintAsGrant, _) => true,
                    _ => frees(i, obj.as_ref(), clock.now_ms(), &cfg),
                };
                if !free {
                    return Err(AcquireError::Held {
                        node: n.clone(),
                        instance: i.clone(),
                    });
                }
            }
            let generation = if variant == Variant::ReuseGeneration {
                record.generation.max(1)
            } else {
                record.generation + 1
            };
            if !valid() {
                return Err(AcquireError::LeaseInvalid);
            }
            (
                generation,
                bucket.replace(ACTOR, mine(generation), &version).await,
            )
        }
    };
    written
        .map(|version| Hold {
            key: ACTOR.to_owned(),
            generation,
            version,
            previous: None,
        })
        .map_err(|_| AcquireError::Unresolved { generation })
}

/// One process slot's lives: a process (its own instance) starts through
/// the start check on the shared node ([`LeaseNode`], its lease on a task
/// of its own as in the server), takes the actor when it holds nothing,
/// gives it up now and then, stops cleanly now and then, and exits when
/// its lease is lost; a supervisor starts the next process. `slot` is the
/// madsim node (a twin shares node `i`'s id).
#[allow(clippy::too_many_arguments)]
async fn run_node(
    slot: usize,
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
        unconditional_fence: variant == Variant::UnconditionalFence,
        resolve_while_held: variant == Variant::ResolveWhileHeld,
        epoch_ignored: variant == Variant::EpochIgnored,
        pending_uncounted: false,
        wake_reuses_parked: false,
        lapse_forgets_parked: false,
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
        let (node, task) = LeaseNode::start(
            setup,
            &instance,
            clock,
            buckets,
            payload(u64::from(i)),
            &env,
        )
        .await;
        {
            let mut s = shared.lock().unwrap();
            s.running.insert(slot, (i, process));
        }
        madsim::task::spawn_local(task.run(env));
        let bucket = EpBucket::connect().await;
        let end = |shared: &Mutex<Shared>| {
            stop_process(&mut shared.lock().unwrap().claims, i, process, env.now());
        };
        let mut hold: Option<Held> = None;
        // With waiters: what this process knows of the others' leases, its
        // wait if it has one, and (the crash scenario) whether it asked
        // since the holder was killed.
        let mut watch = OwnerWatch::new(id.clone(), &cfg, HORIZON_MS);
        let mut listed_ms = 0;
        let mut wait: Option<Wait> = None;
        let mut asked_since_kill = false;
        let ask_after_kill_ms = rng.below(1_500);
        loop {
            if variant.waiters() && clock.now_ms() >= listed_ms + LISTING_MS {
                // One listing: every other node's lease, or nothing learnt.
                let mut bodies = Vec::new();
                let mut failed = false;
                for p in (1..=3u32).filter(|&p| p != i) {
                    match bucket.get(&format!("{NODES}n{p}")).await {
                        Ok(Some((body, _))) => bodies.push((format!("n{p}"), body)),
                        Ok(None) => {}
                        Err(_) => failed = true,
                    }
                }
                if !failed {
                    let objects: Vec<(String, Option<NodeLeaseObject>)> = bodies
                        .into_iter()
                        .map(|(id, body)| (id, NodeLeaseObject::decode(&body)))
                        .collect();
                    let seen = objects.iter().map(|(id, o)| Seen {
                        id,
                        state: o.as_ref().map(|o| o.state),
                        instance: o.as_ref().and_then(|o| o.instance.as_deref()),
                        written_at_ms: o.as_ref().map_or(0, |o| o.written_at_ms),
                    });
                    listed_ms = clock.now_ms();
                    watch.observe(seen, listed_ms);
                }
            }
            if node.lost() {
                // Exit at once: no releases. The claims end now.
                end(&shared);
                break;
            }
            // The actor stays resident on its hold and acts only while the
            // hold may act; a lapse ends the hold (to the pending releases).
            let acting = hold.as_ref().is_some_and(|h| node.may_act(h));
            if hold.is_some() && !acting {
                end(&shared);
                if hold.as_ref().is_some_and(|h| !node.still_held(h)) {
                    hold = None;
                }
            }
            // A clean stop now and then: stop acting, release the hold,
            // then the node lease (`released`), and exit.
            if !scenario && rng.below(300) == 0 {
                end(&shared);
                node.clean_stop(&env).await;
                shared.lock().unwrap().clean_stops += 1;
                break;
            }
            match hold.take() {
                Some(h) if !scenario && rng.chance(8) => {
                    end(&shared);
                    node.release(&h).await;
                }
                Some(h) if !acting => hold = Some(h),
                Some(h) => {
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
                    hold = Some(h);
                }
                None => {
                    // Without waiters a process asks whenever it holds
                    // nothing. With them, a waiter asks when its `Waiter`
                    // says so, and a process without one now and then (a
                    // request arriving); in the crash scenario, once the
                    // holder was killed, once only.
                    let now = clock.now_ms();
                    let mut tried_as = None;
                    let ask = if !variant.waiters() {
                        true
                    } else if let Some(w) = &mut wait {
                        let verdict = watch.verdict(&w.owner, now, Some(w.waiter.held_since_ms()));
                        let step = w.waiter.step(verdict, now);
                        let mut s = shared.lock().unwrap();
                        match step {
                            Step::Try => {
                                w.tries += 1;
                                if matches!(
                                    verdict,
                                    Verdict::Stale { .. }
                                        | Verdict::Forward
                                        | Verdict::Stopping {
                                            released: false,
                                            ..
                                        }
                                ) {
                                    s.waits.tried_unfree += 1;
                                }
                                tried_as = Some(verdict);
                            }
                            Step::Leave => s.waits.left += 1,
                            Step::Expire => s.waits.expired += 1,
                            Step::Wait { .. } => {}
                        }
                        if matches!(step, Step::Leave | Step::Expire) {
                            if w.tries > 1 + (now - w.began_ms) / RETRY_MS {
                                s.waits.spun += 1;
                            }
                            drop(s);
                            wait = None;
                        }
                        step == Step::Try
                    } else {
                        let killed = shared.lock().unwrap().killed_at;
                        match killed {
                            Some(_) if asked_since_kill => false,
                            Some(at) if env.now() >= at + ask_after_kill_ms => {
                                asked_since_kill = true;
                                true
                            }
                            Some(_) => false,
                            None => rng.chance(30),
                        }
                    };
                    if ask {
                        let reason = takeover_reason(&bucket, &id, &instance).await;
                        let broken = match variant {
                            Variant::ReuseGeneration | Variant::FreeOnFenceAtOnce => true,
                            Variant::HintAsGrant => tried_as == Some(Verdict::Free),
                            _ => false,
                        };
                        let got = if broken {
                            let b = &bucket;
                            let (id, instance) = (&id, &instance);
                            node.acquire_via(ACTOR, async |valid: &dyn Fn() -> bool| {
                                acquire_broken(b, &clock, id, instance, variant, valid).await
                            })
                            .await
                        } else {
                            node.acquire(ACTOR, 1).await
                        };
                        if variant.waiters() {
                            let now = clock.now_ms();
                            let mut s = shared.lock().unwrap();
                            let mut ended = false;
                            match (&got, &mut wait) {
                                (Ok(_), Some(_)) => {
                                    s.waits.took += 1;
                                    ended = true;
                                }
                                (Err(AcquireError::Held { node: owner, .. }), Some(w)) => {
                                    if &w.owner == owner {
                                        w.waiter.refused(now);
                                    } else {
                                        s.waits.saw_other += 1;
                                        ended = true;
                                    }
                                }
                                // The bucket did not answer: asked again later.
                                (Err(_), Some(w)) => w.waiter.refused(now),
                                (Err(AcquireError::Held { node: owner, .. }), None)
                                    if variant != Variant::GiveUp =>
                                {
                                    // As a connection to the holder that
                                    // failed: always once it was killed; now
                                    // and then under the random faults, where
                                    // it may be alive (in the crash scenario
                                    // nothing fails before the kill).
                                    if s.killed_at.is_some() || (!scenario && rng.chance(50)) {
                                        watch.connect_failed(owner, now);
                                    }
                                    let verdict = watch.verdict(owner, now, None);
                                    let jitter = u64::from(i) * 67;
                                    if let Some(waiter) =
                                        Waiter::admit(verdict, now, now + WAIT_MS, RETRY_MS, jitter)
                                    {
                                        match verdict {
                                            Verdict::Suspect => s.waits.suspect += 1,
                                            Verdict::Stale { .. } => s.waits.stale += 1,
                                            _ => s.waits.stopping += 1,
                                        }
                                        wait = Some(Wait {
                                            owner: owner.clone(),
                                            waiter,
                                            began_ms: now,
                                            tries: 0,
                                        });
                                    }
                                }
                                _ => {}
                            }
                            if ended {
                                if let Some(w) = wait.take()
                                    && w.tries > 1 + (now - w.began_ms) / RETRY_MS
                                {
                                    s.waits.spun += 1;
                                }
                            }
                        }
                        if let Ok(h) = got
                            && node.may_act(&h)
                        {
                            // The node acts as owner from now on.
                            let until = node
                                .valid_until_ms()
                                .unwrap_or(0)
                                .saturating_add_signed(-skew_ms);
                            serial += 1;
                            let mut s = shared.lock().unwrap();
                            s.acquired += 1;
                            if slot == TWIN {
                                s.twin_acquired += 1;
                            }
                            *s.reasons.entry(reason).or_default() += 1;
                            s.holds.push(HoldSeen {
                                node: i,
                                instance: instance.clone(),
                                generation: h.generation,
                                serial,
                            });
                            s.claims.push(Claim {
                                node: i,
                                instance: process,
                                generation: h.generation,
                                from_ms: env.now(),
                                until_ms: until,
                            });
                            drop(s);
                            hold = Some(h);
                        }
                    }
                }
            }
            sleep(Duration::from_millis(50 + rng.below(50))).await;
        }
        sleep(Duration::from_millis(200 + rng.below(800))).await;
    }
}

struct Outcome {
    twin_acquired: u64,
    claims: Vec<Claim>,
    holds: Vec<HoldSeen>,
    fences: u64,
    acquired: u64,
    clean_stops: u64,
    lost_payloads: u64,
    deletions: u64,
    reasons: HashMap<&'static str, u64>,
    waits: Waits,
    /// The crash scenario: when the holder was killed, and which node it
    /// was.
    killed: Option<(u64, u32)>,
}

fn simulate(seed: u64, variant: Variant, duration: Duration) -> Outcome {
    simulate_with(seed, variant, duration, false)
}

/// `scenario`: no random fault, no twin, no operator, no slow bucket and
/// no hold given up; halfway, the node holding the actor is killed for
/// good, and each survivor asks for the actor once.
fn simulate_with(seed: u64, variant: Variant, duration: Duration, scenario: bool) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let shared = Arc::new(Mutex::new(Shared::default()));
    let out = Arc::clone(&shared);
    rt.block_on(async move {
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let mut rng = Rng::new(seed ^ 0x0DE1_EA5E);
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Store::default();
                let slow_one_in = if scenario { 0 } else { 25 };
                move || bucket_node_with(Arc::clone(&store), bucket::Faults { slow_one_in })
            })
            .build();
        let max_skew = config().max_skew_ms();
        let mut nodes = Vec::new();
        take_fences();
        // Slots 0 to 2 run n1 to n3; slot 3 runs n1's twin.
        for slot in 0..=TWIN {
            if scenario && slot == TWIN {
                continue;
            }
            let i = if slot == TWIN {
                1
            } else {
                u32::try_from(slot).unwrap() + 1
            };
            let skew_ms = i64::try_from(rng.below(max_skew * 2 + 1)).unwrap()
                - i64::try_from(max_skew).unwrap();
            let shared = Arc::clone(&shared);
            let node_seed = seed ^ (u64::try_from(slot).unwrap() + 1) << 40;
            let node = handle
                .create_node()
                .name(format!("slot{slot}"))
                .ip(format!("10.0.1.{}", slot + 1).parse().unwrap())
                .init(move || {
                    run_node(
                        slot,
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
        // An operator fences a node now and then, live or not, as
        // `wasmkeep cluster fence` does (`operator_fence`): only the time
        // bound keeps that safe. Now and then it first deletes the node's
        // object (a mistake, or a machine gone): the fence is then created at
        // the operator's clock, skewed within the bound like a node's.
        let shared_op = Arc::clone(&shared);
        // Its own stream: the nodes' fault schedule stays as without it.
        let op_skew = i64::try_from(Rng::new(seed ^ 0x05CE_E0FF).below(max_skew * 2 + 1)).unwrap()
            - i64::try_from(max_skew).unwrap();
        let _operator = handle
            .create_node()
            .name("operator")
            .ip("10.0.2.1".parse().unwrap())
            .init(move || {
                let shared_op = Arc::clone(&shared_op);
                async move {
                    let bucket = EpBucket::connect().await;
                    let clock = SkewedClock {
                        global: move || env.now(),
                        skew_ms: op_skew,
                    };
                    let mut rng = Rng::new(seed ^ 0x0FE7_CE00);
                    while !scenario {
                        sleep(Duration::from_millis(2_000 + rng.below(6_000))).await;
                        let n = 1 + rng.below(3);
                        let key = format!("{NODES}n{n}");
                        if let Ok(Some((body, _))) = bucket.get(&key).await
                            && let Some(o) = NodeLeaseObject::decode(&body)
                            // A fence the operator created names no
                            // instance and has no payload, by design.
                            && o.instance.is_some()
                            // The broken fence drops it by design.
                            && tail(&body) != payload(n)
                            && variant != Variant::UnconditionalFence
                        {
                            shared_op.lock().unwrap().lost_payloads += 1;
                        }
                        if rng.below(4) == 0 {
                            let _ = bucket.delete(&key).await;
                            shared_op.lock().unwrap().deletions += 1;
                        }
                        let _ = operator_fence(&bucket, &clock, &key).await;
                    }
                }
            })
            .build();
        let net = NetSim::current();
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            if scenario {
                // Halfway: the holder dies, and stays dead.
                let mut s = shared.lock().unwrap();
                let now = env.now();
                let holder = s
                    .claims
                    .iter()
                    .rev()
                    .find(|c| c.from_ms <= now && now < c.until_ms)
                    .map(|c| (c.node, c.instance));
                if step >= steps / 2
                    && s.killed_at.is_none()
                    && let Some((node, process)) = holder
                {
                    stop_process(&mut s.claims, node, process, now);
                    s.killed_at = Some(now);
                    s.killed_node = node;
                    drop(s);
                    handle.kill(nodes[usize::try_from(node).unwrap() - 1]);
                }
                continue;
            }
            let n = usize::try_from(rng.below(4)).unwrap();
            // Over four slots at the three-slot schedule's rate per process
            // (one in 40 steps a clog, one in 120 a crash, for each).
            match rng.below(30) {
                0..=2 => {
                    net.clog_link(nodes[n], bucket.id());
                    net.clog_link(bucket.id(), nodes[n]);
                    held.push((n, at + 300 + rng.below(3_000)));
                }
                3 if !crashed.iter().any(|(c, _)| *c == n) => {
                    // A crash drops its process's claims at once.
                    let mut s = shared.lock().unwrap();
                    if let Some(&(node, process)) = s.running.get(&n) {
                        stop_process(&mut s.claims, node, process, env.now());
                    }
                    drop(s);
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
    });
    let fences = take_fences();
    let s = out.lock().unwrap();
    Outcome {
        claims: s.claims.clone(),
        holds: s.holds.clone(),
        fences,
        twin_acquired: s.twin_acquired,
        acquired: s.acquired,
        clean_stops: s.clean_stops,
        lost_payloads: s.lost_payloads,
        deletions: s.deletions,
        reasons: s.reasons.clone(),
        waits: s.waits.clone(),
        killed: s.killed_at.map(|at| (at, s.killed_node)),
    }
}

/// Two holds (different nodes, or different serials) with one generation.
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
fn one_owner_and_one_generation_per_hold() {
    let (mut acquired, mut fences, mut clean_stops, mut deletions) = (0, 0, 0, 0);
    let mut twin = 0;
    let mut reasons: HashMap<&'static str, u64> = HashMap::new();
    for seed in 0..seeds() {
        let o = simulate(seed, Variant::Correct, Duration::from_secs(30));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners: {d:?}");
        }
        if let Some(pair) = shared_generation(&o.holds) {
            panic!("seed {seed}: one generation, two holds: {pair:?}");
        }
        assert_eq!(
            o.lost_payloads, 0,
            "seed {seed}: a node object lost its payload"
        );
        acquired += o.acquired;
        twin += o.twin_acquired;
        fences += o.fences;
        clean_stops += o.clean_stops;
        deletions += o.deletions;
        for (r, n) in o.reasons {
            *reasons.entry(r).or_default() += n;
        }
    }
    // The runs exercise what they claim to: holds change hands each way the
    // free rule allows, nodes fence, and nodes stop cleanly.
    eprintln!(
        "{} seeds: {acquired} acquisitions {reasons:?} ({twin} by n1's twin), \
         {fences} fences by nodes, {clean_stops} clean stops, \
         {deletions} objects deleted then fenced",
        seeds()
    );
    let floor = (seeds() / 20).max(1);
    assert!(acquired > seeds() * 5, "{acquired} acquisitions");
    for r in ["nobody", "fenced", "another instance"] {
        let n = reasons.get(r).copied().unwrap_or(0);
        assert!(n >= floor, "{n} acquisitions after {r:?}, want {floor}");
    }
    assert!(fences >= floor, "{fences} fences, want {floor}");
    assert!(
        clean_stops >= floor,
        "{clean_stops} clean stops, want {floor}"
    );
    assert!(deletions >= floor, "{deletions} deletions, want {floor}");
    assert!(
        twin >= floor,
        "{twin} acquisitions by the twin, want {floor}"
    );
}

/// Each broken variant is caught on some seed: the checks have teeth.
#[test]
fn each_broken_variant_is_caught() {
    for variant in [
        Variant::UnconditionalFence,
        Variant::ReuseGeneration,
        Variant::FreeOnFenceAtOnce,
        Variant::ResolveWhileHeld,
        Variant::HintAsGrant,
    ] {
        // At least 200 seeds: the rarest variant shows within that, so a
        // quick run with fewer seeds still finds it.
        let caught = (0..seeds().max(200)).find_map(|seed| {
            let o = simulate(seed, variant, Duration::from_secs(30));
            if double_ownership(&o.claims).is_some() {
                Some((seed, "two owners"))
            } else if shared_generation(&o.holds).is_some() {
                Some((seed, "one generation, two holds"))
            } else {
                None
            }
        });
        eprintln!("{variant:?}: caught by {caught:?}");
        assert!(
            caught.is_some(),
            "{variant:?} was never caught in {} seeds",
            seeds()
        );
    }
}

/// The node's epoch rule (a lapse ends every hold, even when the lease is
/// renewed after it) is not what keeps one owner: holds from before a
/// lapse act again after the renewal, and no seed shows two owners or a
/// shared generation. ADR 0020 section 1 says why: a renewal after a lapse
/// is conditional on this process's own last version, so it lands only if
/// `nodes/<id>` did not change, and a record frees only on a change of that
/// object, so nobody could take the actor meanwhile. The epoch carries out
/// the ADR's other choice, that the node then acquires afresh, at new
/// generations. Should this ever be caught, the rule
/// became load-bearing: move the variant to `each_broken_variant_is_caught`.
#[test]
fn ignoring_the_epoch_is_never_caught_here() {
    let mut acts = 0;
    take_old_epoch_acts();
    for seed in 0..seeds() {
        let o = simulate(seed, Variant::EpochIgnored, Duration::from_secs(30));
        assert!(
            double_ownership(&o.claims).is_none() && shared_generation(&o.holds).is_none(),
            "seed {seed}: ignoring the epoch was caught: the rule is load-bearing now"
        );
        acts += take_old_epoch_acts();
    }
    eprintln!(
        "epoch ignored: holds from before a lapse allowed to act {acts} times over {} seeds, \
         never caught",
        seeds()
    );
    assert!(
        acts >= seeds() / 2,
        "old holds acted {acts} times: the variant missed its case"
    );
}

/// The single-owner checks hold when waiters drive acquisitions, under
/// the same faults; a waiter asks only when its rule says so; and the runs
/// reach every way a wait begins and ends.
#[test]
fn a_waiter_follows_the_owner_watch() {
    let mut waits = Waits::default();
    let (mut acquired, mut fences) = (0, 0);
    for seed in 0..seeds() {
        let o = simulate(seed, Variant::Waiters, Duration::from_secs(30));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners: {d:?}");
        }
        if let Some(pair) = shared_generation(&o.holds) {
            panic!("seed {seed}: one generation, two holds: {pair:?}");
        }
        assert_eq!(
            o.waits.tried_unfree, 0,
            "seed {seed}: a waiter asked for an actor whose holder was only stale"
        );
        assert_eq!(
            o.waits.spun, 0,
            "seed {seed}: a waiter asked more than once per retry period"
        );
        acquired += o.acquired;
        fences += o.fences;
        waits.suspect += o.waits.suspect;
        waits.stale += o.waits.stale;
        waits.stopping += o.waits.stopping;
        waits.took += o.waits.took;
        waits.saw_other += o.waits.saw_other;
        waits.left += o.waits.left;
        waits.expired += o.waits.expired;
    }
    eprintln!(
        "{} seeds with waiters: {acquired} acquisitions, {fences} fences, {waits:?}",
        seeds()
    );
    let floor = (seeds() / 20).max(1);
    for (what, n) in [
        ("begun on a suspected holder", waits.suspect),
        ("begun on a stale holder", waits.stale),
        ("begun on a fenced or released holder", waits.stopping),
        ("ended holding the actor", waits.took),
        ("ended by another node holding it", waits.saw_other),
        ("ended by the holder shown alive", waits.left),
    ] {
        assert!(n >= floor, "{n} waits {what}, want {floor}");
    }
}

/// The holder is killed and each survivor asks once, with no retry: a
/// waiter takes the actor over within the wait, never before the dead
/// node's lease allows (the checks above), and none gives up at its
/// deadline. Without the wait (a refusal is final) nobody ever holds the
/// actor again: the check has teeth.
#[test]
fn a_waiter_takes_over_after_a_crash() {
    let taken_over = |o: &Outcome| {
        let (killed_at, dead) = o.killed.expect("the holder was never killed");
        o.claims
            .iter()
            .filter(|c| c.node != dead && c.until_ms > c.from_ms && c.from_ms >= killed_at)
            .map(|c| c.from_ms - killed_at)
            .min()
    };
    for seed in 0..seeds() {
        let o = simulate_with(seed, Variant::Waiters, Duration::from_secs(16), true);
        assert!(double_ownership(&o.claims).is_none(), "seed {seed}");
        assert!(shared_generation(&o.holds).is_none(), "seed {seed}");
        let after = taken_over(&o)
            .unwrap_or_else(|| panic!("seed {seed}: nobody took over: {:?}", o.waits));
        // Each survivor asks within 1.5 s of the kill and waits `WAIT_MS`.
        assert!(
            after <= 1_500 + WAIT_MS,
            "seed {seed}: taken over {after} ms after the kill"
        );
        assert_eq!(o.waits.expired, 0, "seed {seed}: {:?}", o.waits);
        assert_eq!(o.waits.tried_unfree + o.waits.spun, 0, "seed {seed}");
    }
    let o = simulate_with(0, Variant::GiveUp, Duration::from_secs(16), true);
    assert_eq!(
        taken_over(&o),
        None,
        "taken over though no survivor waited: the scenario shows nothing"
    );
}
