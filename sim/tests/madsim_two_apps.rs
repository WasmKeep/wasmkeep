//! Two apps of one tenant on one bucket prefix under madsim (ADR 0022):
//! `acme/shop` and `acme/blog` deploy the same class and actor names, and
//! each app's ownership records live under its own state root
//! (`wasmkeep_cluster::state_root::StateRoot`), so the same name is two
//! actors. Nodes run on node leases as the server does
//! (`wasmkeep_sim::node`), and node leases (`nodes/`) are shared by the
//! prefix: fencing is app-blind.
//!
//! `shop-1` takes both shop actors and keeps them; the two blog nodes take
//! and release the blog actors of the same names over and over. `shop-1`
//! is killed at a time drawn from the seed, holding both. `shop-2` starts
//! some time later (drawn too): often after a blog node fenced `shop-1`,
//! sometimes before, fencing it itself.
//!
//! Checked for every seed:
//!
//! - never two nodes acting as owner of one record at once (claims from
//!   each hold's acquisition to its release, extended while it may act);
//! - a node only ever writes records under its own app's root, so each
//!   app's records name only its own nodes, and each app's generations
//!   count only its own holds (the blog's many holds never move the shop's
//!   generation);
//! - `shop-2` takes both shop actors over from the killed `shop-1`, at the
//!   next generation, whoever fenced `shop-1`, and no blog node ever holds
//!   a shop actor.
//!
//! A hop between nodes of two apps is refused before it runs (503, "not
//! this app's node"): that is the server's, tested in
//! `crates/wasmkeep-server/tests/cluster/apps.rs`.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_two_apps --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::node_lease::OwnerRecord;
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_sim::check::{double_ownership, stop};
use wasmkeep_sim::driver::{Claim, Env};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node};

const EPOCH_MS: u64 = 1_000_000;
const NODES: &str = "nodes/";
const NAMES: [&str; 4] = ["shop-1", "shop-2", "blog-1", "blog-2"];
const APPS: [&str; 4] = ["shop", "shop", "blog", "blog"];
/// The actors both apps deploy: the same class and names.
const TAILS: [&str; 2] = ["chat/a", "chat/b"];
const RECORD_WAIT: Duration = Duration::from_millis(300);
const TICK_MS: u64 = 10;
const RUN_MS: u64 = 20_000;

fn root(node: usize) -> StateRoot {
    StateRoot::new("", "acme", APPS[node])
}

fn record_key(node: usize, tail: &str) -> String {
    root(node).key(Family::Owners, tail)
}

#[derive(Debug, Clone, Copy)]
struct SimClock(Instant);

impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

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

fn config() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
}

/// What the run records.
#[derive(Debug, Default)]
struct State {
    /// Per record key, every claim on it.
    claims: BTreeMap<String, Vec<Claim>>,
    /// Every successful acquisition: node, key, generation, time.
    acquired: Vec<(usize, String, u64, u64)>,
    /// Attempts per key (an attempt may write the record).
    attempts: BTreeMap<String, u64>,
    killed_at: Option<u64>,
    /// A key some node built outside its own root.
    foreign: Option<String>,
}

struct World {
    start: Instant,
    st: Mutex<State>,
}

impl World {
    fn now(&self) -> u64 {
        SimClock(self.start).now_ms()
    }

    /// Records `node`'s claim on `key` through `held`, until its lease's
    /// validity (extending the claim of the same hold).
    fn claim(&self, node: usize, key: &str, held: &Held, lease: &Node) {
        let Some(until) = lease.valid_until_ms() else {
            return;
        };
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        let claims = st.claims.entry(key.to_owned()).or_default();
        let n = u32::try_from(node).unwrap();
        match claims.iter_mut().rev().find(|c| c.node == n) {
            Some(c) if c.generation == held.generation && c.until_ms >= now => {
                c.until_ms = c.until_ms.max(until);
            }
            _ => claims.push(Claim {
                node: n,
                instance: 0,
                generation: held.generation,
                from_ms: now,
                until_ms: until,
            }),
        }
    }
}

async fn start_node(start: Instant, me: usize) -> Rc<Node> {
    let config = config();
    let attempt = Duration::from_millis((config.ttl_ms() - config.safety_ms()) / 3);
    let setup = NodeSetup {
        id: NAMES[me].to_owned(),
        // Every node watches every other: fencing is app-blind.
        peers: (0..NAMES.len())
            .filter(|&n| n != me)
            .map(|n| NAMES[n].to_owned())
            .collect(),
        nodes_prefix: NODES.to_owned(),
        config,
        attempt_ms: u64::try_from(attempt.as_millis()).unwrap(),
        breaks: Breaks::default(),
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
        format!("{}\n{}\n", NAMES[me], APPS[me]).into_bytes(),
        &SimEnv(start),
    )
    .await;
    madsim::task::spawn_local(task.run(SimEnv(start)));
    Rc::new(node)
}

/// Acquires `tail` of `node`'s app until it gets it (or the run ends).
async fn take(world: &World, lease: &Node, node: usize, tail: &str) -> Option<(String, Held)> {
    let key = record_key(node, tail);
    if !key.starts_with(&format!("state/acme/{}/", APPS[node])) {
        world.st.lock().unwrap().foreign = Some(key.clone());
    }
    loop {
        *world
            .st
            .lock()
            .unwrap()
            .attempts
            .entry(key.clone())
            .or_default() += 1;
        match lease.acquire(&key, 1).await {
            Ok(held) => {
                world.claim(node, &key, &held, lease);
                let now = world.now();
                world
                    .st
                    .lock()
                    .unwrap()
                    .acquired
                    .push((node, key.clone(), held.generation, now));
                return Some((key, held));
            }
            Err(_) if world.now() < EPOCH_MS + RUN_MS => {
                sleep(Duration::from_millis(50)).await;
            }
            Err(_) => return None,
        }
    }
}

async fn node_main(world: Arc<World>, node: usize, seed: u64) {
    let lease = start_node(world.start, node).await;
    let mut rng = Rng::new(seed ^ (node as u64) << 20);
    if APPS[node] == "shop" {
        // Take both shop actors and keep them, acting while it may.
        let mut held = Vec::new();
        for tail in TAILS {
            if let Some(h) = take(&world, &lease, node, tail).await {
                held.push(h);
            }
        }
        loop {
            sleep(Duration::from_millis(TICK_MS)).await;
            for (key, h) in &held {
                if lease.may_act(h) {
                    world.claim(node, key, h, &lease);
                }
            }
        }
    }
    // Blog nodes: take, act a while, release, over and over.
    while world.now() < EPOCH_MS + RUN_MS {
        let tail = TAILS[usize::try_from(rng.below(2)).unwrap()];
        let Some((key, h)) = take(&world, &lease, node, tail).await else {
            break;
        };
        let until = world.now() + 20 + rng.below(300);
        while world.now() < until && lease.may_act(&h) {
            world.claim(node, &key, &h, &lease);
            sleep(Duration::from_millis(TICK_MS)).await;
        }
        let now = world.now();
        {
            let mut st = world.st.lock().unwrap();
            stop(
                st.claims.get_mut(&key).unwrap(),
                u32::try_from(node).unwrap(),
                now,
            );
        }
        lease.release(&h).await;
        sleep(Duration::from_millis(rng.below(100))).await;
    }
}

#[derive(Debug)]
struct Outcome {
    killed_at: u64,
    shop2_start: u64,
    records: BTreeMap<String, OwnerRecord>,
    st: State,
    fences: u64,
}

fn simulate(seed: u64) -> Outcome {
    let runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let _ = take_fences();
    let mut rng = Rng::new(seed);
    let kill_ms = 2_000 + rng.below(3_000);
    let shop2_after = rng.below(6_000);
    runtime.block_on(async move {
        let handle = Handle::current();
        let store = Store::default();
        handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Arc::clone(&store);
                move || bucket_node(Arc::clone(&store))
            })
            .build();
        let world = Arc::new(World {
            start: Instant::now(),
            st: Mutex::new(State::default()),
        });
        let spawn = |node: usize| {
            let world = Arc::clone(&world);
            handle
                .create_node()
                .name(NAMES[node])
                .ip(format!("10.0.1.{}", node + 1).parse().unwrap())
                .init(move || node_main(Arc::clone(&world), node, seed))
                .build()
                .id()
        };
        let shop1 = spawn(0);
        spawn(2);
        spawn(3);
        sleep(Duration::from_millis(kill_ms)).await;
        handle.kill(shop1);
        let killed_at = world.now();
        world.st.lock().unwrap().killed_at = Some(killed_at);
        sleep(Duration::from_millis(shop2_after)).await;
        let shop2_start = world.now();
        spawn(1);
        sleep(Duration::from_millis(
            RUN_MS.saturating_sub(kill_ms + shop2_after),
        ))
        .await;
        // Let the blog nodes stop.
        sleep(Duration::from_millis(1_000)).await;
        let records = store
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, (body, _))| Some((k.clone(), OwnerRecord::decode(body)?)))
            .filter(|(k, _)| k.starts_with("state/"))
            .collect();
        let st = std::mem::take(&mut *world.st.lock().unwrap());
        Outcome {
            killed_at,
            shop2_start,
            records,
            st,
            fences: take_fences(),
        }
    })
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

#[test]
fn two_apps_of_one_tenant_keep_their_actors_apart() {
    let mut fenced_by_blog = 0;
    for seed in 0..seeds() {
        let o = simulate(seed);
        assert_eq!(o.st.foreign, None, "seed {seed}");
        for (key, claims) in &o.st.claims {
            if let Some(v) = double_ownership(claims) {
                panic!("seed {seed}: two owners of {key}: {v:?}");
            }
        }
        // Each app's records name only its own nodes, and each app's
        // generations count only its own holds.
        for (key, record) in &o.records {
            let app = key.split('/').nth(2).unwrap();
            if let Some((holder, _)) = &record.holder {
                assert!(
                    holder.starts_with(&format!("{app}-")),
                    "seed {seed}: {key} names {holder}"
                );
            }
            let attempts = o.st.attempts.get(key).copied().unwrap_or(0);
            assert!(
                record.generation <= attempts,
                "seed {seed}: {key} at generation {} after {attempts} attempts of its own app",
                record.generation
            );
        }
        for (node, key, _, _) in &o.st.acquired {
            assert!(
                key.starts_with(&format!("state/acme/{}/", APPS[*node])),
                "seed {seed}: {} acquired {key}",
                NAMES[*node]
            );
        }
        // shop-2 took both shop actors over from the killed shop-1, at the
        // next generation.
        for tail in TAILS {
            let key = record_key(0, tail);
            let by_shop1 =
                o.st.acquired
                    .iter()
                    .find(|(n, k, _, _)| *n == 0 && *k == key)
                    .map(|(_, _, g, _)| *g);
            let by_shop2 =
                o.st.acquired
                    .iter()
                    .find(|(n, k, _, _)| *n == 1 && *k == key)
                    .map(|(_, _, g, _)| *g);
            let (Some(first), Some(second)) = (by_shop1, by_shop2) else {
                panic!(
                    "seed {seed}: {key} not taken over (shop-1 {by_shop1:?}, shop-2 \
                     {by_shop2:?}; killed at {}, shop-2 from {})",
                    o.killed_at, o.shop2_start
                );
            };
            assert_eq!(second, first + 1, "seed {seed}: {key}");
        }
        // Whether a blog node fenced shop-1 before shop-2 started: shop-2
        // started later than the time a fence needs.
        if o.shop2_start > o.killed_at + 3_000 && o.fences > 0 {
            fenced_by_blog += 1;
        }
    }
    assert!(
        seeds() < 20 || fenced_by_blog > 0,
        "no seed had a blog node fence shop-1"
    );
}

#[test]
fn every_family_of_two_apps_is_apart() {
    let (shop, blog) = (root(0), root(2));
    for family in [
        Family::History,
        Family::Owners,
        Family::Leases,
        Family::Alarms,
        Family::Moves,
    ] {
        let (a, b) = (shop.base(family), blog.base(family));
        assert!(
            !a.starts_with(&b) && !b.starts_with(&a),
            "{family:?}: {a} and {b}"
        );
        assert_ne!(shop.key(family, "chat/a"), blog.key(family, "chat/a"));
    }
}

#[test]
fn a_seed_replays() {
    let (a, b) = (simulate(5), simulate(5));
    assert_eq!(a.st.acquired, b.st.acquired);
}
