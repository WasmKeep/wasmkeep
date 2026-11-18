//! Two apps' node ids in one process under madsim (ADR 0038): `shop-1`
//! and `blog-1` run in one simulated process, each on its own node lease,
//! and take their own app's actors of the same names; `shop-2` and
//! `blog-2` run in another, started later.
//!
//! Two scenarios, for every seed:
//!
//! - **the process dies**: both node ids end at once, each is failed over
//!   by its own app's node, independently, at the next generation;
//! - **one runtime stops** (its lease task and actors end, as a verdict
//!   stops one app's runtime): its sibling in the same process keeps
//!   renewing and keeps its actors at their generation; the stopped node
//!   id's actors are taken over by its own app's other node; the runtime
//!   started again in place (a new token) acquires only at a newer
//!   generation.
//!
//! Checked throughout: never two owners of one record at once, and each
//! app's records name only its own node ids.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_shared_process --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Handle;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::node_lease::OwnerRecord;
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_sim::check::double_ownership;
use wasmkeep_sim::driver::{Claim, Env};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences};
use wasmkeep_sim::rng::Rng;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node};

const EPOCH_MS: u64 = 1_000_000;
const NODES: &str = "nodes/";
const NAMES: [&str; 4] = ["shop-1", "blog-1", "shop-2", "blog-2"];
const APPS: [&str; 4] = ["shop", "blog", "shop", "blog"];
const TAILS: [&str; 2] = ["chat/a", "chat/b"];
const RECORD_WAIT: Duration = Duration::from_millis(300);
const TICK_MS: u64 = 10;
const RUN_MS: u64 = 16_000;

fn record_key(node: usize, tail: &str) -> String {
    StateRoot::new("", "acme", APPS[node]).key(Family::Owners, tail)
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

#[derive(Debug, Default)]
struct State {
    claims: BTreeMap<String, Vec<Claim>>,
    /// Node, its start (0 first, 1 after an in-place restart), key,
    /// generation.
    acquired: Vec<(usize, u32, String, u64)>,
}

struct World {
    start: Instant,
    st: Mutex<State>,
}

impl World {
    fn now(&self) -> u64 {
        SimClock(self.start).now_ms()
    }

    fn claim(&self, node: usize, start: u32, key: &str, held: &Held, lease: &Node) {
        let Some(until) = lease.valid_until_ms() else {
            return;
        };
        let now = self.now();
        let mut st = self.st.lock().unwrap();
        let claims = st.claims.entry(key.to_owned()).or_default();
        let n = u32::try_from(node).unwrap();
        match claims
            .iter_mut()
            .rev()
            .find(|c| c.node == n && c.instance == u64::from(start))
        {
            Some(c) if c.generation == held.generation && c.until_ms >= now => {
                c.until_ms = c.until_ms.max(until);
            }
            _ => claims.push(Claim {
                node: n,
                instance: u64::from(start),
                generation: held.generation,
                from_ms: now,
                until_ms: until,
            }),
        }
    }
}

/// Node id `me`'s lease, started (its task spawned on the current
/// simulated process); the task's handle, to stop this runtime alone.
async fn start_node(start: Instant, me: usize) -> (Rc<Node>, madsim::task::JoinHandle<()>) {
    let config = config();
    let attempt = Duration::from_millis((config.ttl_ms() - config.safety_ms()) / 3);
    let setup = NodeSetup {
        id: NAMES[me].to_owned(),
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
    // A fresh token at every start of a runtime (ADR 0038, section 3).
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
    let handle = madsim::task::spawn_local(task.run(SimEnv(start)));
    (Rc::new(node), handle)
}

/// Aborts its task when dropped: a runtime stopped takes its lease task
/// with it.
struct Owned(madsim::task::JoinHandle<()>);

impl Drop for Owned {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A runtime of node id `node` (its `start`-th): takes both of its app's
/// actors (retrying until the run ends) and keeps them while it may act.
async fn runtime(world: Arc<World>, node: usize, start: u32) {
    let (lease, task) = start_node(world.start, node).await;
    let _task = Owned(task);
    let mut held: Vec<(String, Held)> = Vec::new();
    let mut wanted: Vec<String> = TAILS.iter().map(|t| record_key(node, t)).collect();
    while world.now() < EPOCH_MS + RUN_MS {
        let mut still = Vec::new();
        for key in wanted.drain(..) {
            match lease.acquire(&key, 1).await {
                Ok(h) => {
                    world.claim(node, start, &key, &h, &lease);
                    world.st.lock().unwrap().acquired.push((
                        node,
                        start,
                        key.clone(),
                        h.generation,
                    ));
                    held.push((key, h));
                }
                Err(_) => still.push(key),
            }
        }
        wanted = still;
        for (key, h) in &held {
            if lease.may_act(h) {
                world.claim(node, start, key, h, &lease);
            }
        }
        sleep(Duration::from_millis(TICK_MS.max(if wanted.is_empty() {
            0
        } else {
            50
        })))
        .await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    ProcessDies,
    OneRuntimeStops,
}

struct Outcome {
    records: BTreeMap<String, OwnerRecord>,
    st: State,
    stopped_at: u64,
}

fn simulate(seed: u64, scenario: Scenario) -> Outcome {
    let runtime_ = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let _ = take_fences();
    let mut rng = Rng::new(seed);
    let stop_ms = 2_000 + rng.below(2_000);
    let other_after = rng.below(3_000);
    runtime_.block_on(async move {
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
        // Process P: shop-1 and blog-1. In the second scenario blog-1's
        // runtime is stopped at `stop_ms` and started again a while after.
        let p = {
            let world = Arc::clone(&world);
            handle
                .create_node()
                .name("p")
                .ip("10.0.1.1".parse().unwrap())
                .init(move || {
                    let world = Arc::clone(&world);
                    async move {
                        let shop = madsim::task::spawn_local(runtime(Arc::clone(&world), 0, 0));
                        let blog = madsim::task::spawn_local(runtime(Arc::clone(&world), 1, 0));
                        if scenario == Scenario::OneRuntimeStops {
                            sleep(Duration::from_millis(stop_ms)).await;
                            // This runtime alone: its lease task and its
                            // actors end, without a release.
                            blog.abort();
                            sleep(Duration::from_millis(3_000)).await;
                            madsim::task::spawn_local(runtime(Arc::clone(&world), 1, 1));
                        }
                        let _ = shop.await;
                    }
                })
                .build()
                .id()
        };
        // Process Q: shop-2 and blog-2, later.
        let q = {
            let world = Arc::clone(&world);
            move || {
                let world = Arc::clone(&world);
                async move {
                    let shop = madsim::task::spawn_local(runtime(Arc::clone(&world), 2, 0));
                    let blog = madsim::task::spawn_local(runtime(Arc::clone(&world), 3, 0));
                    let _ = (shop.await, blog.await);
                }
            }
        };
        sleep(Duration::from_millis(stop_ms)).await;
        if scenario == Scenario::ProcessDies {
            handle.kill(p);
        }
        let stopped_at = world.now();
        sleep(Duration::from_millis(other_after)).await;
        handle
            .create_node()
            .name("q")
            .ip("10.0.1.2".parse().unwrap())
            .init(q)
            .build();
        sleep(Duration::from_millis(
            RUN_MS.saturating_sub(stop_ms + other_after) + 1_000,
        ))
        .await;
        let records = store
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, (body, _))| Some((k.clone(), OwnerRecord::decode(body)?)))
            .filter(|(k, _)| k.starts_with("state/"))
            .collect();
        let st = std::mem::take(&mut *world.st.lock().unwrap());
        Outcome {
            records,
            st,
            stopped_at,
        }
    })
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

fn first_by(o: &Outcome, node: usize, start: u32, key: &str) -> Option<u64> {
    o.st.acquired
        .iter()
        .find(|(n, s, k, _)| *n == node && *s == start && k == key)
        .map(|(_, _, _, g)| *g)
}

fn common_checks(seed: u64, o: &Outcome) {
    for (key, claims) in &o.st.claims {
        if let Some(v) = double_ownership(claims) {
            panic!("seed {seed}: two owners of {key}: {v:?}");
        }
    }
    for (key, record) in &o.records {
        let app = key.split('/').nth(2).unwrap();
        if let Some((holder, _)) = &record.holder {
            assert!(
                holder.starts_with(&format!("{app}-")),
                "seed {seed}: {key} names {holder}"
            );
        }
    }
}

#[test]
fn a_process_dying_ends_both_node_ids_and_each_app_fails_over_on_its_own() {
    for seed in 0..seeds() {
        let o = simulate(seed, Scenario::ProcessDies);
        common_checks(seed, &o);
        for (first, then) in [(0, 2), (1, 3)] {
            for tail in TAILS {
                let key = record_key(first, tail);
                let (Some(a), Some(b)) =
                    (first_by(&o, first, 0, &key), first_by(&o, then, 0, &key))
                else {
                    panic!(
                        "seed {seed}: {key} not taken over by {} from {} (stopped at {})",
                        NAMES[then], NAMES[first], o.stopped_at
                    );
                };
                assert_eq!(b, a + 1, "seed {seed}: {key}");
            }
        }
    }
}

#[test]
fn one_runtime_stopping_leaves_its_sibling_and_restarts_at_a_newer_generation() {
    for seed in 0..seeds() {
        let o = simulate(seed, Scenario::OneRuntimeStops);
        common_checks(seed, &o);
        for tail in TAILS {
            // The sibling's actors never moved: shop-1 still holds them,
            // at their first generation, and shop-2 never got them.
            let shop = record_key(0, tail);
            assert_eq!(first_by(&o, 0, 0, &shop), Some(1), "seed {seed}: {shop}");
            assert_eq!(first_by(&o, 2, 0, &shop), None, "seed {seed}: {shop} moved");
            let record = &o.records[&shop];
            assert_eq!(
                record.holder.as_ref().map(|(h, _)| h.as_str()),
                Some("shop-1"),
                "seed {seed}"
            );
            assert_eq!(record.generation, 1, "seed {seed}: {shop}");
            // The stopped runtime's actors were taken again, by its own
            // app's other node or by the runtime restarted in place, only at
            // a newer generation; never by another app's node.
            let blog = record_key(1, tail);
            let first = first_by(&o, 1, 0, &blog).expect("blog-1 took it first");
            let later: Vec<u64> =
                o.st.acquired
                    .iter()
                    .filter(|(n, s, k, _)| *k == blog && (*n, *s) != (1, 0))
                    .map(|(n, _, _, g)| {
                        assert_eq!(APPS[*n], "blog", "seed {seed}: {} took {blog}", NAMES[*n]);
                        *g
                    })
                    .collect();
            assert!(!later.is_empty(), "seed {seed}: {blog} never taken again");
            assert!(
                later.iter().all(|g| *g > first),
                "seed {seed}: {blog} {later:?}"
            );
        }
    }
}

#[test]
fn a_seed_replays() {
    let (a, b) = (
        simulate(3, Scenario::OneRuntimeStops),
        simulate(3, Scenario::OneRuntimeStops),
    );
    assert_eq!(a.st.acquired, b.st.acquired);
}
