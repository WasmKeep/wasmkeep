//! `wasmkeep_cluster::node_lease` (ADR 0020) under madsim: three nodes with
//! skewed clocks plus a twin of the first (a second process configured
//! with its node id), one bucket, one actor, seeded faults (cut links that
//! drop requests and replies, slow writes that land late after their reply
//! was lost, crashes and restarts as a new instance, clean stops).
//!
//! Each process runs on the simulations' shared node (`wasmkeep_sim::node`,
//! the server's rules): the start check, its lease on a task of its own
//! (renewed on the server's schedule, `node_lease::Renewals`: a quarter
//! of `ttl - safety` after the last acknowledged write, a lapse ending its
//! holds into the
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
//! `OwnerWatch` (`wasmkeep_cluster::failover`) from its own reads of the
//! node leases (one per node; a node whose read fails is kept as last read
//! and passed as not read just now, as the server's rounds do) and asks
//! again only when its `Waiter` says so, until a
//! deadline. The same checks hold with those acquisitions; a waiter never
//! asks while the holder is only stale, nor more often than its retry
//! period; and after a crash with one request per survivor and no retry
//! (`a_waiter_takes_over_after_a_crash`), a survivor holds the actor
//! within the wait. This models acquisitions, not requests: what a kept
//! request's caller sees is the cluster tests' to show.
//!
//! With stale reads (`stale_lease_reads_never_free_a_held_record`), the
//! record clients' reads of node leases now and then return the body the
//! process was served before the newest one: the same checks hold, and
//! judging those bodies as read instead of reading them again is caught
//! (ADR 0020, amendment on lease reads). A first read that is stale, of a
//! body the process never saw past, is not modelled: the rule does not
//! close it.
//!
//! With moves (`rebalance_moves_and_cold_takes_keep_one_holder`, ADR 0040,
//! section 14), two more events run beside the others. A holder now and
//! then moves the actor as the rebalance pass does: it stops acting (its
//! sockets closed, instance and database dropped), releases its hold, and
//! once the release landed sends another node a take call, which that
//! node's process answers by acquiring, as any acquisition. And a process
//! holding nothing now and then sends a take call to another node first
//! (a cold socket asking its home), and acquires itself only if that take
//! was refused, lost or not answered in time. Take calls may be lost or
//! late, and land on whichever process runs that node id (the twin
//! included). The same checks hold; a mover that releases before it stops
//! acting, and a target that takes the call for a grant (the record
//! written whoever holds it), are caught.
//!
//! Ignoring the node's epoch rule is never caught here, and a test says
//! why (`ignoring_the_epoch_is_never_caught_here`).
//!
//! The generation floor (ADR 0045) is a bounded interleaving model, not a
//! seeded run (`support/generation_floor.rs`): first acquisitions on two
//! nodes that serve through the finalize, racing `cluster markers clean`
//! over a marker a restore before 0.10 left as the only carrier of a
//! seed, and an actor's delete and recreate. No record reuses a
//! generation the actor's history ever carried; the cleanup deleting
//! before it raises, a node reading either floor before the marker, and a
//! node keeping the upgrade floor it read at start are each caught.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::Handle;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::failover::{OwnerWatch, Seen, Shown, Step, Verdict, Waiter};
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use wasmkeep_cluster::node_lease::{
    AcquireError, Hold, NodeLeaseObject, NodeState, OwnerRecord, ReadFault, frees, operator_fence,
    tail,
};
use wasmkeep_sim::check::{double_ownership, stop_process};
use wasmkeep_sim::driver::{Claim, Env, SkewedClock};
use wasmkeep_sim::node::{
    Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences, take_old_epoch_acts,
};
use wasmkeep_sim::rng::Rng;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

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
    /// The correct rules, with a bucket whose node lease reads by the
    /// record clients now and then go back: they return the body this
    /// process was served before the newest one (a stale replica or cache).
    StaleReads,
    /// The same stale reads, judged as read: a record is freed on a body
    /// older than one of that lease the process already read (the rule
    /// before ADR 0020's amendment on lease reads).
    StaleReadsTrusted,
    /// The same stale reads, with the read back after a failed acquisition
    /// write judging its body as read.
    StaleReadBackUnchecked,
    /// The same stale reads, with a judgement whose every read was behind
    /// judging the last one anyway.
    StaleExhaustedJudged,
    /// Stale reads that may return a body this process was never served
    /// (the newest it was not given): a first stale read, which the rule
    /// does not close.
    StaleUnseen,
    /// The correct rules under stale reads, with every node's clock
    /// stepping back [`CLOCK_STEP_MS`] at [`CLOCK_STEP_AT_MS`] (the crash
    /// scenario only).
    StaleClockStep,
    /// The correct rules under stale reads, with a hot operator: it fences
    /// four times as often, deleting the lease before every fence, on a
    /// clock the full skew behind, so its created fences are dated before
    /// the bodies they follow.
    StaleHotOperator,
    /// The hot operator's stale reads, with a created fence ordered against
    /// named bodies by date alone (the rule before its window).
    StaleFenceByDate,
    /// The hot operator's stale reads, with a read that finds a lease
    /// absent leaving the unpin window running (the rule before).
    StaleAbsentKeepsWindow,
    /// The correct rules, with rebalance moves and cold takes (ADR 0040,
    /// section 14).
    Moves,
    /// Moves whose owner keeps acting a moment after its release, as if
    /// the move released before it dropped the instance. This tests the
    /// move's stop-then-release order: any acquirer in that moment catches
    /// it, the move's target or not.
    MoveActsAfterRelease,
    /// Moves whose target takes a take call for a grant: the record is
    /// written whoever holds it (the take not going through the free rule).
    TakeAsGrant,
}

/// When the clocks step back in [`Variant::StaleClockStep`] (global ms).
const CLOCK_STEP_AT_MS: u64 = EPOCH_MS + 5_000;
/// By how much.
const CLOCK_STEP_MS: u64 = 2_000;

impl Variant {
    fn moves(self) -> bool {
        matches!(
            self,
            Self::Moves | Self::MoveActsAfterRelease | Self::TakeAsGrant
        )
    }

    fn waiters(self) -> bool {
        matches!(self, Self::Waiters | Self::HintAsGrant | Self::GiveUp)
    }

    fn stale(self) -> bool {
        matches!(
            self,
            Self::StaleReads
                | Self::StaleReadsTrusted
                | Self::StaleReadBackUnchecked
                | Self::StaleExhaustedJudged
                | Self::StaleUnseen
                | Self::StaleClockStep
                | Self::StaleHotOperator
                | Self::StaleFenceByDate
                | Self::StaleAbsentKeepsWindow
        )
    }

    /// Whether the operator deletes before every fence, the full skew
    /// behind.
    fn hot_operator(self) -> bool {
        matches!(
            self,
            Self::StaleHotOperator | Self::StaleFenceByDate | Self::StaleAbsentKeepsWindow
        )
    }

    fn read_fault(self) -> ReadFault {
        match self {
            Self::StaleReadsTrusted => ReadFault::Off,
            Self::StaleReadBackUnchecked => ReadFault::ReadBackUnchecked,
            Self::StaleExhaustedJudged => ReadFault::ExhaustedJudged,
            Self::StaleFenceByDate => ReadFault::FenceByDate,
            Self::StaleAbsentKeepsWindow => ReadFault::AbsentKeepsWindow,
            _ => ReadFault::None,
        }
    }

    /// How far back the nodes' clocks are at global `now`.
    fn stepped_back(self, now: u64) -> u64 {
        if self == Self::StaleClockStep && now >= CLOCK_STEP_AT_MS {
            CLOCK_STEP_MS
        } else {
            0
        }
    }
}

/// The bodies of each `nodes/` object a process's record clients were
/// served, by the store's version (the last few).
type Served = Arc<Mutex<HashMap<String, std::collections::BTreeMap<u64, Vec<u8>>>>>;

/// A process's bucket client. With `served`, a read of a node lease now
/// and then returns the body this process was served before the newest
/// one it was served, with that body's version: a read that goes back
/// behind what the process has already seen, never one it was not served.
/// With `unseen`, the current body counts as served even when the stale
/// one is returned instead, so a stale answer may be older than anything
/// this process was given since (a first stale read).
struct StaleNodes {
    inner: EpBucket,
    served: Option<Served>,
    unseen: bool,
}

impl StaleNodes {
    async fn connect(served: Option<Served>, unseen: bool) -> Self {
        Self {
            inner: EpBucket::connect().await,
            served,
            unseen,
        }
    }
}

impl Bucket for StaleNodes {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        let got = self.inner.get(key).await?;
        let Some(served) = self.served.as_ref().filter(|_| key.starts_with(NODES)) else {
            return Ok(got);
        };
        let mut served = served.lock().unwrap();
        let bodies = served.entry(key.to_owned()).or_default();
        if self.unseen
            && let Some((body, v)) = &got
        {
            bodies.insert(v.0.parse().unwrap(), body.clone());
        }
        // Now and then the body served before the newest one served (only
        // what this process was served is recorded, never a stale answer).
        if madsim::rand::random::<u64>().is_multiple_of(3)
            && let Some((v, body)) = bodies.iter().rev().nth(1)
        {
            return Ok(Some((body.clone(), Version(v.to_string()))));
        }
        if let Some((body, v)) = &got {
            bodies.insert(v.0.parse().unwrap(), body.clone());
            while bodies.len() > 3 {
                bodies.pop_first();
            }
        }
        Ok(got)
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.inner.create(key, body).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        self.inner.replace(key, body, expected).await
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
    /// Lease reads found behind the process's newest and read again, by
    /// process (slot, instance).
    behind: HashMap<(usize, u64), u64>,
    /// Take calls sent and not yet answered.
    takes: Vec<Take>,
    moves: Moves,
}

/// What the moves and cold takes of a simulation did.
#[derive(Clone, Debug, Default)]
struct Moves {
    /// Rebalance moves whose release landed and that sent their take.
    moved: u64,
    /// Rebalance moves whose release did not land (no take sent).
    unreleased: u64,
    /// Take calls answered by acquiring the actor.
    taken: u64,
    /// Cold takes sent, answered held, and given up (refused, lost, late):
    /// the asking process then acquires itself.
    cold: u64,
    cold_held: u64,
    cold_fallback: u64,
}

/// A take call (ADR 0017) on its way to node `to`, delivered from
/// `at_ms` (global), dropped unanswered past `until_ms`. A cold one
/// carries the cell its sender waits on.
struct Take {
    to: u32,
    at_ms: u64,
    until_ms: u64,
    answer: Option<Arc<Mutex<Option<bool>>>>,
}

/// How long a cold take's sender waits for the answer before it acquires
/// itself: it stands for the server's 10 s take timeout, scaled to this
/// simulation's 1 s ttl.
const COLD_WAIT_MS: u64 = 500;

/// Sends a take call to `to`: lost one time in ten, delivered after up
/// to 200 ms otherwise. Only the call is lost or late: an answer, once a
/// process gives it, always reaches the cell its sender waits on (a lost
/// answer looks to the sender like a late one, which the wait covers).
fn send_take(
    shared: &Mutex<Shared>,
    rng: &mut Rng,
    now: u64,
    to: u32,
    answer: Option<Arc<Mutex<Option<bool>>>>,
) {
    if rng.chance(10) {
        return;
    }
    let at_ms = now + rng.below(200);
    shared.lock().unwrap().takes.push(Take {
        to,
        at_ms,
        until_ms: at_ms + COLD_WAIT_MS,
        answer,
    });
}

/// The next take call delivered to node `i` by `now`, if any; calls past
/// their time are dropped unanswered.
fn next_take(shared: &Mutex<Shared>, i: u32, now: u64) -> Option<Take> {
    let mut s = shared.lock().unwrap();
    s.takes.retain(|t| t.until_ms > now);
    let k = s.takes.iter().position(|t| t.to == i && t.at_ms <= now)?;
    Some(s.takes.remove(k))
}

/// Another node than `i`, at random.
fn other_node(rng: &mut Rng, i: u32) -> u32 {
    let k = u32::try_from(rng.below(2)).unwrap() + 1;
    (i + k - 1) % 3 + 1
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
            recorded: None,
        }
        .encode()
    };
    let read = bucket
        .get(ACTOR)
        .await
        .map_err(|_| AcquireError::Unavailable)?;
    let (generation, before, written) = match read {
        None => {
            if !valid() {
                return Err(AcquireError::LeaseInvalid);
            }
            (1, None, bucket.create(ACTOR, mine(1)).await)
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
                    (Variant::HintAsGrant | Variant::TakeAsGrant, _) => true,
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
            let written = bucket.replace(ACTOR, mine(generation), &version).await;
            (generation, Some(record), written)
        }
    };
    written
        .map(|version| Hold {
            key: ACTOR.to_owned(),
            generation,
            version,
            previous: None,
            head: Default::default(),
        })
        .map_err(|_| AcquireError::Unresolved {
            generation,
            before: before.map(Box::new),
        })
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
        resolver_ignores_parked_generation: false,
        unknown_wake_drops_parked: false,
        resolver_ends_unchanged: false,
        resolver_never_ends: false,
        release_before_stop: false,
        act_on_cell_only: false,
        read_fault: variant.read_fault(),
        start_over_mark: false,
    };
    loop {
        let instance = format!("{:08x}", madsim::rand::random::<u32>());
        let process = u64::from_str_radix(&instance, 16).unwrap();
        let clock = SkewedClock {
            global: move || {
                let now = env.now();
                now - variant.stepped_back(now)
            },
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
        // Stale reads, if any, only on the record clients: their reads
        // of node leases are the ones that judge a record free (the start
        // check's and the fence watch's are left as they are).
        let served = variant.stale().then(Served::default);
        let unseen = variant == Variant::StaleUnseen;
        let buckets = NodeBuckets {
            lease: StaleNodes::connect(None, false).await,
            task_records: StaleNodes::connect(served.clone(), unseen).await,
            peers: StaleNodes::connect(None, false).await,
            records: StaleNodes::connect(served, unseen).await,
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
        // Each other node's lease as last read (`None`: unreadable).
        let mut last_read: HashMap<String, Option<NodeLeaseObject>> = HashMap::new();
        let mut listed_ms = 0;
        let mut wait: Option<Wait> = None;
        let mut asked_since_kill = false;
        let ask_after_kill_ms = rng.below(1_500);
        loop {
            if variant.waiters() && clock.now_ms() >= listed_ms + LISTING_MS {
                // One round of reads, one per other node, as the server's:
                // a read that fails keeps that node as last read, and the
                // watch is told it was not read just now.
                let mut kept = Vec::new();
                for p in (1..=3u32).filter(|&p| p != i) {
                    let id = format!("n{p}");
                    match bucket.get(&format!("{NODES}{id}")).await {
                        Ok(Some((body, _))) => {
                            last_read.insert(id, NodeLeaseObject::decode(&body));
                        }
                        Ok(None) => {
                            last_read.remove(&id);
                        }
                        Err(_) => kept.push(id),
                    }
                }
                let seen = last_read.iter().map(|(id, o)| Seen {
                    id,
                    state: o.as_ref().map(|o| o.state),
                    instance: o.as_ref().and_then(|o| o.instance.as_deref()),
                    written_at_ms: o.as_ref().map_or(0, |o| o.written_at_ms),
                });
                listed_ms = clock.now_ms();
                watch.observe_kept(
                    seen,
                    |id| {
                        if kept.iter().any(|k| k == id) {
                            Shown::Kept
                        } else {
                            Shown::Read
                        }
                    },
                    listed_ms,
                );
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
            // A take call reaching the holder is answered "held".
            if variant.moves() && acting {
                while let Some(t) = next_take(&shared, i, env.now()) {
                    if let Some(answer) = t.answer {
                        *answer.lock().unwrap() = Some(true);
                    }
                }
            }
            match hold.take() {
                // A rebalance move (ADR 0040, section 14): the owner's side
                // of ADR 0017's move, from memory. It stops acting (sockets
                // closed, instance and database dropped), releases, and
                // only once the release landed sends the target its take.
                Some(h) if variant.moves() && acting && rng.chance(10) => {
                    end(&shared);
                    let to = other_node(&mut rng, i);
                    let released = node.release(&h).await;
                    if variant == Variant::MoveActsAfterRelease {
                        let now = env.now();
                        shared.lock().unwrap().claims.push(Claim {
                            node: i,
                            instance: process,
                            generation: h.generation,
                            from_ms: now,
                            until_ms: now + 100 + rng.below(400),
                        });
                    }
                    if released {
                        shared.lock().unwrap().moves.moved += 1;
                        send_take(&shared, &mut rng, env.now(), to, None);
                    } else {
                        shared.lock().unwrap().moves.unreleased += 1;
                    }
                }
                Some(h) if !scenario && rng.chance(8) => {
                    end(&shared);
                    node.release(&h).await;
                }
                Some(h) if !acting => hold = Some(h),
                Some(h) => {
                    let until = node
                        .valid_until_ms()
                        .unwrap_or(0)
                        .saturating_add_signed(-skew_ms)
                        .saturating_add(variant.stepped_back(env.now()));
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
                    // A take call delivered here: acquire as asked.
                    let take = if variant.moves() {
                        next_take(&shared, i, env.now())
                    } else {
                        None
                    };
                    let ask = if take.is_some() || !variant.waiters() {
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
                    // A cold socket (ADR 0040, section 14): ask another
                    // node to take the actor first; acquire here only if
                    // that take was refused, lost or not answered in time.
                    let mut asked_home = false;
                    if ask && take.is_none() && variant.moves() && rng.chance(25) {
                        let cell = Arc::new(Mutex::new(None));
                        let home = other_node(&mut rng, i);
                        shared.lock().unwrap().moves.cold += 1;
                        send_take(&shared, &mut rng, env.now(), home, Some(Arc::clone(&cell)));
                        let until = env.now() + COLD_WAIT_MS;
                        while env.now() < until && cell.lock().unwrap().is_none() {
                            sleep(Duration::from_millis(20)).await;
                        }
                        let held = *cell.lock().unwrap() == Some(true);
                        let mut s = shared.lock().unwrap();
                        if held {
                            s.moves.cold_held += 1;
                        } else {
                            s.moves.cold_fallback += 1;
                        }
                        asked_home = held;
                    }
                    if ask && !asked_home {
                        let reason = takeover_reason(&bucket, &id, &instance).await;
                        let broken = match variant {
                            Variant::ReuseGeneration | Variant::FreeOnFenceAtOnce => true,
                            Variant::HintAsGrant => tried_as == Some(Verdict::Free),
                            Variant::TakeAsGrant => take.is_some(),
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
                        shared
                            .lock()
                            .unwrap()
                            .behind
                            .insert((slot, process), node.behind_reads());
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
                            if ended
                                && let Some(w) = wait.take()
                                && w.tries > 1 + (now - w.began_ms) / RETRY_MS
                            {
                                s.waits.spun += 1;
                            }
                        }
                        if let Some(t) = take {
                            let held = got.as_ref().is_ok_and(|h| node.may_act(h));
                            if held {
                                shared.lock().unwrap().moves.taken += 1;
                            }
                            if let Some(answer) = t.answer {
                                *answer.lock().unwrap() = Some(held);
                            }
                        }
                        if let Ok(h) = got
                            && node.may_act(&h)
                        {
                            // The node acts as owner from now on.
                            let until = node
                                .valid_until_ms()
                                .unwrap_or(0)
                                .saturating_add_signed(-skew_ms)
                                .saturating_add(variant.stepped_back(env.now()));
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
    /// Lease reads found behind and read again, over every process.
    behind: u64,
    moves: Moves,
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
                move || {
                    bucket_node_with(
                        Arc::clone(&store),
                        bucket::Faults {
                            slow_one_in,
                            late_reply_one_in: 0,
                        },
                    )
                }
            })
            .build();
        let max_skew = config().max_skew_ms();
        let mut nodes = Vec::new();
        take_fences();
        // Slots 0 to 2 run n1 to n3; slot 3 runs n1's twin.
        for slot in 0..=TWIN {
            // No twin with the hot operator: it deletes a lease so often
            // that a twin's whole run can fall between the two reads of the
            // other's start check, which (unlike the server's, which polls)
            // cannot see it there.
            if (scenario || variant.hot_operator()) && slot == TWIN {
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
        let op_skew = if variant.hot_operator() {
            -i64::try_from(max_skew).unwrap()
        } else {
            op_skew
        };
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
                    // The scenario case has no operator.
                    if scenario {
                        return;
                    }
                    loop {
                        // The hot operator fences four times as often.
                        let pause = 2_000 + rng.below(6_000);
                        let pause = if variant.hot_operator() {
                            pause / 4
                        } else {
                            pause
                        };
                        sleep(Duration::from_millis(pause)).await;
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
                        if rng.below(4) == 0 || variant.hot_operator() {
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
                0..=2 if n < nodes.len() => {
                    net.clog_link(nodes[n], bucket.id());
                    net.clog_link(bucket.id(), nodes[n]);
                    held.push((n, at + 300 + rng.below(3_000)));
                }
                3 if n < nodes.len() && !crashed.iter().any(|(c, _)| *c == n) => {
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
        behind: s.behind.values().sum(),
        moves: s.moves.clone(),
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

/// `WASMKEEP_SIM_SEED`: one seed to run alone, as other simulations take it.
fn only_seed() -> Option<u64> {
    std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
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
        Variant::StaleReadsTrusted,
        Variant::StaleExhaustedJudged,
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

/// Rebalance moves and cold takes (ADR 0040, section 14) as events, under
/// the same faults: one holder at a time and one generation per hold, and
/// the runs do move the actor, have take calls acquire it, and have cold
/// takes both answered held and given up.
#[test]
fn rebalance_moves_and_cold_takes_keep_one_holder() {
    let mut moves = Moves::default();
    let mut acquired = 0;
    let only = only_seed();
    for seed in only.map_or(0..seeds(), |s| s..s + 1) {
        let o = simulate(seed, Variant::Moves, Duration::from_secs(30));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners with moves: {d:?}");
        }
        if let Some(pair) = shared_generation(&o.holds) {
            panic!("seed {seed}: one generation, two holds with moves: {pair:?}");
        }
        acquired += o.acquired;
        moves.moved += o.moves.moved;
        moves.unreleased += o.moves.unreleased;
        moves.taken += o.moves.taken;
        moves.cold += o.moves.cold;
        moves.cold_held += o.moves.cold_held;
        moves.cold_fallback += o.moves.cold_fallback;
    }
    if only.is_some() {
        return;
    }
    eprintln!(
        "{} seeds with moves: {acquired} acquisitions, {moves:?}",
        seeds()
    );
    let floor = (seeds() / 20).max(1);
    assert!(acquired > seeds() * 5, "{acquired} acquisitions");
    assert!(moves.moved >= floor * 5, "{} moves", moves.moved);
    assert!(moves.taken >= floor, "{} takes acquired", moves.taken);
    assert!(
        moves.cold_held >= floor,
        "{} cold takes held",
        moves.cold_held
    );
    assert!(
        moves.cold_fallback >= floor,
        "{} cold takes given up",
        moves.cold_fallback
    );
}

/// A move whose owner acts after its release, and a target that takes a
/// take call for a grant, are each caught on some seed.
#[test]
fn a_broken_move_or_take_is_caught() {
    for variant in [Variant::MoveActsAfterRelease, Variant::TakeAsGrant] {
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
            seeds().max(200)
        );
    }
}

/// A bucket whose node lease reads go back behind what a process has
/// already read (ADR 0020, amendment on lease reads): with the correct
/// rules a body older than one of that lease the process read before is
/// read again, never trusted, so the checks hold; the runs do meet such
/// reads, and still hand the actor over. `each_broken_variant_is_caught`
/// shows the same reads judged as read give two owners.
#[test]
fn stale_lease_reads_never_free_a_held_record() {
    let (mut acquired, mut behind) = (0, 0);
    let mut reasons: HashMap<&'static str, u64> = HashMap::new();
    // `WASMKEEP_SIM_SEED` runs that one seed's checks alone (no floors).
    let only = only_seed();
    for seed in only.map_or(0..seeds(), |s| s..s + 1) {
        let o = simulate(seed, Variant::StaleReads, Duration::from_secs(30));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners under stale reads: {d:?}");
        }
        if let Some(pair) = shared_generation(&o.holds) {
            panic!("seed {seed}: one generation, two holds under stale reads: {pair:?}");
        }
        acquired += o.acquired;
        behind += o.behind;
        for (r, n) in o.reasons {
            *reasons.entry(r).or_default() += n;
        }
    }
    if only.is_some() {
        return;
    }
    eprintln!(
        "{} seeds with stale lease reads: {acquired} acquisitions {reasons:?}, \
         {behind} reads behind the newest read again",
        seeds()
    );
    let floor = (seeds() / 20).max(1);
    assert!(acquired > seeds() * 5, "{acquired} acquisitions");
    assert!(behind >= floor, "{behind} reads behind, want {floor}");
    for r in ["fenced", "another instance"] {
        let n = reasons.get(r).copied().unwrap_or(0);
        assert!(n >= floor, "{n} acquisitions after {r:?}, want {floor}");
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

/// What the rule does not close, shown: when a stale read may return a
/// body older than one the bucket held when this process last read (but
/// never gave it), a first stale read frees a held record on some seed.
/// Closing it needs the ownership record to carry its holder's lease date
/// (ADR 0020, amendment on lease reads).
#[test]
fn a_first_stale_read_is_not_closed() {
    let caught = (0..seeds().max(200)).find(|&seed| {
        let o = simulate(seed, Variant::StaleUnseen, Duration::from_secs(30));
        double_ownership(&o.claims).is_some() || shared_generation(&o.holds).is_some()
    });
    eprintln!("first stale reads: caught by seed {caught:?}");
    assert!(
        caught.is_some(),
        "a first stale read was never caught: the model shows nothing"
    );
}

/// A hot operator under stale reads (ADR 0020, amendment on lease reads):
/// every fence is created over a deleted lease, on a clock the full skew
/// behind, so its date is before the last bodies of the process it ends.
/// With the correct rules there are never two owners nor one generation
/// for two holds; the runs do take records over from created fences.
#[test]
fn stale_lease_reads_with_a_hot_operator_never_free_a_held_record() {
    let (mut acquired, mut deletions) = (0, 0);
    // `WASMKEEP_SIM_SEED` runs that one seed's checks alone (no floor).
    let only = only_seed();
    for seed in only.map_or(0..seeds(), |s| s..s + 1) {
        let o = simulate(seed, Variant::StaleHotOperator, Duration::from_secs(30));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners with a hot operator: {d:?}");
        }
        if let Some(pair) = shared_generation(&o.holds) {
            panic!("seed {seed}: one generation, two holds with a hot operator: {pair:?}");
        }
        acquired += o.reasons.get("fenced").copied().unwrap_or(0);
        deletions += o.deletions;
    }
    if only.is_some() {
        return;
    }
    eprintln!(
        "{} seeds with a hot operator: {deletions} leases deleted and fenced, \
         {acquired} acquisitions after a fence",
        seeds()
    );
    let floor = (seeds() / 20).max(1);
    assert!(acquired >= floor, "{acquired} acquisitions after a fence");
}

/// The first seed of `variant` with two owners or one generation for two
/// holds: `known` first (seeds that showed it, so a 200-seed gate run
/// still checks it), then every seed below `WASMKEEP_SIM_SEEDS`.
fn first_caught(variant: Variant, known: &[u64]) -> Option<u64> {
    known.iter().copied().chain(0..seeds()).find(|&seed| {
        let o = simulate(seed, variant, Duration::from_secs(30));
        double_ownership(&o.claims).is_some() || shared_generation(&o.holds).is_some()
    })
}

/// A created fence ordered against named bodies by date alone, under a hot
/// operator's stale reads, is caught: a predecessor's body dated after the
/// fence stays the newest held, and served again once the node runs as
/// another instance it frees the record that instance holds. Rare (about
/// once in 5,000 seeds), so a seed that showed it is checked first; should
/// a change to the simulation move it, search again with
/// `WASMKEEP_SIM_SEEDS=20000` and replace it.
#[test]
fn a_created_fence_judged_by_date_is_caught() {
    let caught = first_caught(Variant::StaleFenceByDate, &[KNOWN_FENCE_BY_DATE]);
    eprintln!("created fence by date: caught by seed {caught:?}");
    assert!(caught.is_some(), "a created fence by date was never caught");
    assert!(
        !caught_by(KNOWN_FENCE_BY_DATE, Variant::StaleHotOperator),
        "seed {KNOWN_FENCE_BY_DATE} fails with the correct rules too"
    );
}

/// A seed where [`Variant::StaleFenceByDate`] gives two owners.
const KNOWN_FENCE_BY_DATE: u64 = 7_548;

/// A read finding a lease absent that leaves the unpin window running,
/// under a hot operator's stale reads, is caught: a deleted lease's old
/// bodies, served again with no current answer counted for the window,
/// replace the newest held and free the record the node's next process
/// holds. A seed that showed it is checked first, as above.
#[test]
fn an_absent_read_keeping_the_window_is_caught() {
    let caught = first_caught(Variant::StaleAbsentKeepsWindow, &[KNOWN_ABSENT]);
    eprintln!("absent read keeping the window: caught by seed {caught:?}");
    assert!(
        caught.is_some(),
        "an absent read keeping the window was never caught"
    );
    assert!(
        !caught_by(KNOWN_ABSENT, Variant::StaleHotOperator),
        "seed {KNOWN_ABSENT} fails with the correct rules too"
    );
}

/// A seed where [`Variant::StaleAbsentKeepsWindow`] gives two owners.
const KNOWN_ABSENT: u64 = 87_854;

/// Whether `seed` under `variant` gives two owners or one generation for
/// two holds.
fn caught_by(seed: u64, variant: Variant) -> bool {
    let o = simulate(seed, variant, Duration::from_secs(30));
    double_ownership(&o.claims).is_some() || shared_generation(&o.holds).is_some()
}

/// The read back after a failed acquisition write judging its rival's
/// lease as read, under stale reads, is caught: rarely (its case needs a
/// failed write, a rival's record and a stale read of that rival's lease
/// together), so the search runs to 1,000 seeds whatever
/// `WASMKEEP_SIM_SEEDS` says.
#[test]
fn a_read_back_judging_as_read_is_caught() {
    let caught = (0..seeds().max(1_000)).find(|&seed| {
        let o = simulate(
            seed,
            Variant::StaleReadBackUnchecked,
            Duration::from_secs(30),
        );
        double_ownership(&o.claims).is_some() || shared_generation(&o.holds).is_some()
    });
    eprintln!("read back judging as read: caught by seed {caught:?}");
    assert!(
        caught.is_some(),
        "the read back judging as read was never caught"
    );
}

/// A killed holder is taken over within a bound under stale reads, after
/// every node's clock stepped back 2 s (its live bodies after the step are
/// dated before ones read before it; its fence keeps such a date): on each
/// seed, never two owners, and a survivor holds the actor within the same
/// bound as with waiters.
#[test]
fn a_killed_holder_is_taken_over_under_stale_reads_and_a_clock_step() {
    let mut behind = 0;
    for seed in 0..seeds() {
        let o = simulate_with(seed, Variant::StaleClockStep, Duration::from_secs(16), true);
        assert!(double_ownership(&o.claims).is_none(), "seed {seed}");
        assert!(shared_generation(&o.holds).is_none(), "seed {seed}");
        let (killed_at, dead) = o.killed.expect("the holder was never killed");
        assert!(
            killed_at >= CLOCK_STEP_AT_MS,
            "seed {seed}: killed before the step"
        );
        let after = o
            .claims
            .iter()
            .filter(|c| c.node != dead && c.until_ms > c.from_ms && c.from_ms >= killed_at)
            .map(|c| c.from_ms - killed_at)
            .min()
            .unwrap_or_else(|| panic!("seed {seed}: nobody took over"));
        assert!(
            after <= 1_500 + WAIT_MS,
            "seed {seed}: taken over {after} ms after the kill"
        );
        behind += o.behind;
    }
    eprintln!(
        "{} seeds, clocks stepped back: {behind} reads behind",
        seeds()
    );
}

#[path = "support/generation_floor.rs"]
mod generation_floor_model;

/// After a restore before 0.10: actor 0's history carried 5, the restore
/// copied a plan at 3 and left a marker at 6, its only carrier; actor 1 is
/// new. Two nodes of 0.10 serve through the finalize, and a cleanup runs
/// once the floor is 0.10.
fn leftover_marker() -> generation_floor_model::Start {
    use generation_floor_model::{Start, Task};
    Start {
        finalized: false,
        floor: (0, false),
        actors: vec![(Some(6), None, 3, 5, 3), (None, None, 0, 0, 0)],
        nodes: vec![
            vec![Task::Acquire(0), Task::Delete(0), Task::Acquire(0)],
            vec![Task::Acquire(0), Task::Acquire(1)],
        ],
        restores: 0,
        clean: true,
    }
}

/// ADR 0045, section 2: across every interleaving of the finalize, the
/// cleanup and first acquisitions, no ownership record reuses a
/// generation; the runs reach a first record seeded from the generation
/// floor alone, a folded marker, and a recreated actor.
#[test]
fn a_cleanup_racing_first_acquisitions_through_a_finalize_reuses_no_generation() {
    use generation_floor_model::{Mutation, explore};
    let reached =
        explore(&leftover_marker(), Mutation::None).unwrap_or_else(|reused| panic!("{reused:?}"));
    assert!(reached.from_floor > 0, "{reached:?}");
    assert!(reached.folded > 0, "{reached:?}");
    assert!(reached.recreated > 0, "{reached:?}");
    eprintln!("generation floor: {reached:?}");
}

/// Each order the cleanup's safety rests on, broken, reuses a generation
/// on some interleaving.
#[test]
fn each_broken_generation_floor_order_is_caught() {
    use generation_floor_model::{Mutation, explore};
    for mutation in [
        Mutation::DeleteBeforeRaise,
        Mutation::UpgradeFloorFirst,
        Mutation::GenerationFloorFirst,
        Mutation::StartFloor,
    ] {
        assert!(
            explore(&leftover_marker(), mutation).is_err(),
            "{mutation:?} was never caught"
        );
    }
}
