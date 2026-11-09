//! The alarm sweep's recovery of a dead node's alarms under madsim:
//! the surviving nodes walk the bucket's alarm index with their own cursor,
//! each with its own view of the live nodes, and decide every page with the
//! real rule (`wasmkeep_cluster::alarm_index::plan_sweep`). An entry is a
//! node's own by a hash of the actor over its view; overdue three times
//! longer it is anyone's.
//!
//! Delivering an entry is the alarm event: take the actor's lease (a node
//! that finds it held by another gets "not the owner" and does nothing),
//! run, and remove the entry well after (the owner's mailbox is busy with
//! the rest of the backlog). A node never has the same actor in flight
//! twice, and does not count against the cap an actor it has in flight
//! (or keeps retrying), as the server asks its timer
//! (`AlarmHandle::pending`). The backlog is a dead node's: due before the run starts, plus
//! alarms coming due during it.
//!
//! Four scenarios: the survivors agree; one still sees the dead node for a
//! while (its heartbeat not yet stale); all see a node that is alive but
//! never delivers (it does not serve the class), so its shard goes only as
//! anyone's; and the same with half the actors unable to start on the
//! stuck node, which holds their leases: their entries never leave the
//! index, and a survivor delivering one is "not the owner" (nothing stays
//! in flight), so they count against its cap on every sweep. Checked: every entry of an actor that can start is delivered
//! (liveness, all scenarios); and with agreeing views, the backlog drains
//! at the survivors' combined rate, not one node's (the point of the fix).
//! Three unsafe rules must each be caught by some seed, for the property
//! they break: the earlier first-come walk (the rate), never delivering
//! anyone's entries (liveness), and own entries first with no share kept
//! for anyone's (liveness, behind actors that cannot start).
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_alarm_sweep --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::alarm_index::{self, SweepClaim, SweepPlan};
use wasmkeep_sim::rng::Rng;

/// Most deliveries per sweep and entries read per sweep (the server's 256
/// and 2,048, scaled down with the backlog).
const CAP: usize = 16;
const SCAN: usize = 128;
const SWEEP_MS: u64 = 1_000;
/// Lease ttl plus grace: an entry is the sweep's once this overdue.
const OVERDUE_MS: u64 = 1_500;
/// Due before the run starts (spread over the first 2 s of the epoch).
const BACKLOG: u64 = 640;
const BACKLOG_SPREAD_MS: u64 = 2_000;
/// Coming due later, up to 20 s in.
const LATER: u64 = 20;
const LATER_UNTIL_MS: u64 = 20_000;
/// Long enough for the poisoned scenario: the stuck node's shard goes
/// only through the share of the cap kept for anyone's entries, picked at
/// random, so its last entries can take many minutes here.
const DEADLINE: Duration = Duration::from_secs(1_200);

/// Actor `n`'s index key: (due time, actor), in listing order.
type Key = (u64, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// The production rule.
    Planned,
    /// Unsafe: the walk before the fix, first come first served.
    FirstCome,
    /// Unsafe: anyone's entries never delivered (shards only).
    NeverAnyones,
    /// Unsafe: own entries first, anyone's only with what they leave.
    NoReserve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Views {
    /// Both survivors see each other only.
    Agree,
    /// `a` still sees the dead node `c` until a random moment.
    Lingering,
    /// Everyone sees `c`, alive but never delivering.
    StuckOwner,
    /// `StuckOwner`, and half the actors cannot start.
    Poisoned,
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    views: Views,
    /// Entries of actors that can start never delivered by the deadline.
    left: usize,
    /// When the last entry went, in ms of the run.
    drained_ms: u64,
    /// Deliveries that found the actor's lease held by the other node.
    not_owner: u64,
}

fn fnv(actor: u64) -> u64 {
    format!("actor-{actor}")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

/// Own entries first, anyone's with what the cap leaves, for the unsafe
/// variant (the first version of the fix).
fn no_reserve(claims: &[SweepClaim], cap: usize) -> SweepPlan {
    let mut deliver = Vec::new();
    let mut walked = claims.len();
    for (i, claim) in claims.iter().enumerate() {
        if deliver.len() >= cap {
            walked = i;
            break;
        }
        if *claim == SweepClaim::Mine {
            deliver.push(i);
        }
    }
    let spare = cap.saturating_sub(deliver.len());
    let anyones: Vec<_> = (0..walked)
        .filter(|&i| claims[i] == SweepClaim::Anyones)
        .take(spare)
        .collect();
    deliver.extend(anyones);
    SweepPlan { deliver, walked }
}

/// The earlier walk, for the unsafe variant: mine or anyone's, in order,
/// stopping at the cap.
fn first_come(claims: &[SweepClaim], cap: usize) -> SweepPlan {
    let mut deliver = Vec::new();
    for (i, claim) in claims.iter().enumerate() {
        if deliver.len() >= cap {
            return SweepPlan { deliver, walked: i };
        }
        if *claim != SweepClaim::Skip {
            deliver.push(i);
        }
    }
    SweepPlan {
        deliver,
        walked: claims.len(),
    }
}

struct World {
    start: Instant,
    index: Mutex<BTreeMap<Key, ()>>,
    /// Actor -> (holder, held until ms).
    leases: Mutex<HashMap<u64, (usize, u64)>>,
    not_owner: Mutex<u64>,
    drained_ms: Mutex<u64>,
    /// Actors that cannot start: their entries never leave.
    poisoned: HashSet<u64>,
}

impl World {
    fn now(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_millis()).unwrap()
    }
}

/// Node `node`'s view of the live nodes at `now`, sorted.
fn view(views: Views, node: usize, now: u64, linger_ms: u64) -> Vec<usize> {
    const DEAD: usize = 2;
    match views {
        Views::Agree => vec![0, 1],
        Views::Lingering if node == 0 && now < linger_ms => vec![0, 1, DEAD],
        Views::Lingering => vec![0, 1],
        Views::StuckOwner | Views::Poisoned => vec![0, 1, DEAD],
    }
}

/// `alarms_due_before`: entries after `after` due before `before`, at most
/// `SCAN`, with the key to resume from (`None` once it reached entries not
/// yet due, or the end).
fn list(world: &World, before: u64, after: Option<Key>) -> (Vec<Key>, Option<Key>) {
    let index = world.index.lock().unwrap();
    let mut due = Vec::new();
    let range = match after {
        Some(after) => index.range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded)),
        None => index.range(..),
    };
    for (&key, ()) in range {
        if key.0 >= before {
            return (due, None);
        }
        due.push(key);
        if due.len() >= SCAN {
            return (due, Some(key));
        }
    }
    (due, None)
}

/// Returns whether the actor is done with (its in-flight mark can go): not
/// for one that cannot start, which the node keeps retrying on its own.
async fn deliver(world: Arc<World>, node: usize, key: Key, rng: &mut Rng) -> bool {
    // Taking the lease.
    sleep(Duration::from_millis(5 + rng.below(45))).await;
    let actor = key.1;
    let now = world.now();
    {
        let mut leases = world.leases.lock().unwrap();
        match leases.get(&actor) {
            Some(&(holder, until)) if holder != node && until > now => {
                *world.not_owner.lock().unwrap() += 1;
                return true;
            }
            _ => {
                leases.insert(actor, (node, now + 5_000));
            }
        }
    }
    // The event waits behind the rest of the node's backlog, runs, and
    // removes the entry; the owner keeps the lease while idle.
    sleep(Duration::from_millis(100 + rng.below(1_400))).await;
    if world.poisoned.contains(&actor) {
        return false;
    }
    if world.index.lock().unwrap().remove(&key).is_some() {
        let now = world.now();
        let mut drained = world.drained_ms.lock().unwrap();
        *drained = (*drained).max(now);
    }
    true
}

async fn sweeper(
    world: Arc<World>,
    node: usize,
    rule: Rule,
    views: Views,
    linger_ms: u64,
    seed: u64,
) {
    let mut rng = Rng::new(seed ^ ((node as u64 + 1) << 40));
    let in_flight: Arc<Mutex<HashSet<u64>>> = Arc::default();
    let mut cursor: Option<Key> = None;
    sleep(Duration::from_millis(rng.below(SWEEP_MS))).await;
    loop {
        // Real sweeps drift a little.
        sleep(Duration::from_millis(SWEEP_MS - 20 + rng.below(40))).await;
        let now = world.now();
        let (before, anyones) = (
            now.saturating_sub(OVERDUE_MS),
            now.saturating_sub(3 * OVERDUE_MS),
        );
        let (due, next) = list(&world, before, cursor);
        let nodes = view(views, node, now, linger_ms);
        let me = nodes.iter().position(|&n| n == node).unwrap();
        let claims: Vec<SweepClaim> = due
            .iter()
            .map(|&(at, actor)| {
                let slot = usize::try_from(fnv(actor) % nodes.len() as u64).unwrap();
                // `AlarmHandle::pending`: an actor this node already has in
                // flight (or retrying) is not counted (not before the fix).
                let counted =
                    rule == Rule::FirstCome || !in_flight.lock().unwrap().contains(&actor);
                if !counted {
                    SweepClaim::Skip
                } else if slot == me {
                    SweepClaim::Mine
                } else if at < anyones && rule != Rule::NeverAnyones {
                    SweepClaim::Anyones
                } else {
                    SweepClaim::Skip
                }
            })
            .collect();
        let plan = match rule {
            Rule::FirstCome => first_come(&claims, CAP),
            Rule::NoReserve => no_reserve(&claims, CAP),
            Rule::Planned | Rule::NeverAnyones => {
                alarm_index::plan_sweep(&claims, CAP, rng.next_u64())
            }
        };
        assert!(plan.deliver.len() <= CAP);
        assert!(plan.deliver.iter().all(|&i| i < plan.walked));
        if plan.walked < due.len() {
            if let Some(last) = plan.walked.checked_sub(1) {
                cursor = Some(due[last]);
            }
        } else {
            cursor = next;
        }
        for i in plan.deliver {
            let key = due[i];
            if !in_flight.lock().unwrap().insert(key.1) {
                continue;
            }
            let (world, in_flight) = (Arc::clone(&world), Arc::clone(&in_flight));
            let mut rng = Rng::new(rng.next_u64());
            madsim::task::spawn(async move {
                if deliver(world, node, key, &mut rng).await {
                    in_flight.lock().unwrap().remove(&key.1);
                }
            });
        }
    }
}

fn simulate(seed: u64, rule: Rule) -> Outcome {
    let runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
    runtime.block_on(async move {
        let mut rng = Rng::new(seed);
        let views = match seed % 4 {
            0 => Views::Agree,
            1 => Views::Lingering,
            2 => Views::StuckOwner,
            _ => Views::Poisoned,
        };
        let linger_ms = rng.below(10_000);
        let mut index = BTreeMap::new();
        for actor in 0..BACKLOG {
            index.insert((rng.below(BACKLOG_SPREAD_MS), actor), ());
        }
        for actor in BACKLOG..BACKLOG + LATER {
            let at = BACKLOG_SPREAD_MS + rng.below(LATER_UNTIL_MS - BACKLOG_SPREAD_MS);
            index.insert((at, actor), ());
        }
        let poisoned: HashSet<u64> = match views {
            Views::Poisoned => (0..BACKLOG).filter(|_| rng.chance(50)).collect(),
            _ => HashSet::new(),
        };
        let world = Arc::new(World {
            start: Instant::now(),
            index: Mutex::new(index),
            // The stuck node holds the leases of the actors that cannot
            // start, and keeps retrying them itself: a survivor delivering
            // one finds it taken, every sweep.
            leases: Mutex::new(
                poisoned
                    .iter()
                    .map(|&actor| (actor, (2, u64::MAX)))
                    .collect(),
            ),
            not_owner: Mutex::default(),
            drained_ms: Mutex::default(),
            poisoned,
        });
        let nodes: Vec<_> = (0..2)
            .map(|node| {
                madsim::task::spawn(sweeper(
                    Arc::clone(&world),
                    node,
                    rule,
                    views,
                    linger_ms,
                    seed,
                ))
            })
            .collect();
        let left = |world: &World| {
            let index = world.index.lock().unwrap();
            index
                .keys()
                .filter(|(_, actor)| !world.poisoned.contains(actor))
                .count()
        };
        while world.start.elapsed() < DEADLINE && left(&world) > 0 {
            sleep(Duration::from_millis(100)).await;
        }
        for node in nodes {
            node.abort();
        }
        let left = left(&world);
        let drained_ms = *world.drained_ms.lock().unwrap();
        let not_owner = *world.not_owner.lock().unwrap();
        Outcome {
            views,
            left,
            drained_ms,
            not_owner,
        }
    })
}

/// With agreeing views, how long two nodes at full rate may take: the
/// backlog comes due (2 s plus the overdue wait), then two caps a sweep,
/// with a quarter's slack and two sweeps for the last deliveries to land.
fn rate_bound_ms() -> u64 {
    let total = BACKLOG + LATER;
    let sweeps = total.div_ceil(2 * CAP as u64);
    BACKLOG_SPREAD_MS + OVERDUE_MS + sweeps * SWEEP_MS * 5 / 4 + 2 * SWEEP_MS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Broken {
    Liveness,
    Rate,
}

/// Which property `outcome` breaks, if any.
fn violation(outcome: &Outcome) -> Option<(Broken, String)> {
    if outcome.left > 0 {
        return Some((
            Broken::Liveness,
            format!(
                "{:?}: {} entries never delivered",
                outcome.views, outcome.left
            ),
        ));
    }
    if outcome.views == Views::Agree && outcome.drained_ms > rate_bound_ms() {
        return Some((
            Broken::Rate,
            format!(
                "agreeing views drained in {} ms, over {} ms ({} not-owner deliveries): one node's rate",
                outcome.drained_ms,
                rate_bound_ms(),
                outcome.not_owner
            ),
        ));
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
fn survivors_recover_a_dead_nodes_alarms_at_their_combined_rate() {
    let seeds = seeds();
    assert!(
        seeds > 0,
        "the alarm sweep simulation needs at least one seed"
    );
    for seed in 0..seeds {
        let outcome = simulate(seed, Rule::Planned);
        if let Some((_, broken)) = violation(&outcome) {
            panic!("seed {seed}: {broken}");
        }
    }
}

#[test]
fn the_unsafe_sweep_rules_are_caught() {
    // Four seeds cover each scenario once; more find it faster.
    let seeds = seeds().clamp(4, 24);
    for (rule, breaks) in [
        (Rule::FirstCome, Broken::Rate),
        (Rule::NeverAnyones, Broken::Liveness),
        (Rule::NoReserve, Broken::Liveness),
    ] {
        let caught = (0..seeds)
            .any(|seed| violation(&simulate(seed, rule)).is_some_and(|(b, _)| b == breaks));
        assert!(caught, "{rule:?} never broke {breaks:?} in {seeds} seeds");
    }
}

#[test]
fn a_sweep_schedule_replays_with_the_same_seed() {
    assert_eq!(simulate(73, Rule::Planned), simulate(73, Rule::Planned));
}
