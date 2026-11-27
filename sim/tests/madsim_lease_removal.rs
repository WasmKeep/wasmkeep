//! The removal of released node leases by the live nodes (ADR 0020,
//! amendment of 2026-11-27) under madsim: one node id whose process stops
//! cleanly and starts again, over and over, beside removers that run
//! `wasmkeep_cluster::lease_removal::Remover` rounds, one bucket with slow
//! requests (applied up to 2.5 s after they arrive) and late replies.
//!
//! Each process of node `a` runs on the simulations' shared node
//! (`wasmkeep_sim::node`, the server's rules): its start check, which waits
//! while the key holds a removal's mark, and its lease on a task of its own.
//! It runs for a while, stops cleanly (its lease `released`), and starts
//! again as a new instance after a pause that is often longer than the
//! removal's age. So its released lease is removed while it restarts: the
//! read-delete gap the mark closes. A remover now and then dies right after
//! its mark landed (the mark stays), and another starts after a while; now
//! and then two run at once. Node `b` stopped long ago, and an ownership
//! record still names it.
//!
//! Checked: no lease a process of `a` wrote is ever deleted (a process
//! never finds its lease gone); `b`'s lease stays while the record names
//! it; and once a remover runs alone at the end, no mark is left five
//! minutes later. The runs
//! must remove leases, end marks a dead remover left and see removers die.
//! Broken on purpose, each is caught on some seed: the delete without the
//! mark (`cluster forget` before 0.9), a start check that takes a mark for
//! an object, no read of the ownership records, and marks that stay never
//! ended.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_lease_removal --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Handle;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::app_format::floor::{self, Minor};
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use wasmkeep_cluster::lease_removal::{Fault, Remover, Round, Settings};
use wasmkeep_cluster::node_lease::{
    NodeLeaseObject, NodeState, OwnerRecord, forget_delete_bound_ms, forgotten,
};
use wasmkeep_cluster::store::ObjectMeta;
use wasmkeep_sim::driver::{Env, SkewedClock};
use wasmkeep_sim::node::{Breaks, LeaseNode, NodeBuckets, NodeSetup, take_marks_stayed};
use wasmkeep_sim::rng::Rng;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node_with};

const EPOCH_MS: u64 = 1_000_000;
const APP: &str = "acme/shop";
const NODES: &str = "nodes/";
/// A heartbeat counts as live this long: three lease periods and the skew.
const HORIZON_MS: u64 = 3_200;
/// One lease write's limit: a third of `ttl - safety`.
const ATTEMPT_MS: u64 = 833;
/// Versions of the objects written before the run: far above the bucket's.
const SEEDED: u64 = 1 << 40;

/// ttl 3 s, grace 1 s, safety 500 ms, 200 ms of skew: in the ratios of the
/// defaults to the bucket's slowest request (2.5 s here). A mark's delete
/// is given up after 2.67 s and a mark is ended once 4.2 s old, so a
/// delete the bucket holds is applied before another remover reads the
/// mark; one held longer than that is the limit the record keeps
/// (ADR 0043, "A delete that lands late").
fn config() -> LeaseConfig {
    LeaseConfig::new(3_000, 1_000, 500, 200).unwrap()
}

#[derive(Clone, Copy)]
struct SimEnv(Instant);

impl SimEnv {
    fn now(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
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

/// What the simulation breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Correct,
    Unmarked,
    StartOverMark,
    RecordsUnchecked,
    MarksKept,
}

impl Variant {
    fn fault(self) -> Fault {
        match self {
            Self::Unmarked => Fault::Unmarked,
            Self::RecordsUnchecked => Fault::RecordsUnchecked,
            Self::MarksKept => Fault::MarksKept,
            Self::Correct | Self::StartOverMark => Fault::None,
        }
    }
}

/// A heartbeat payload of `id` for the app, at 0.9.
fn payload(id: &str) -> Vec<u8> {
    format!("{id}\nhttp://{id}\n0.9.0\n{APP}\n").into_bytes()
}

/// The remover's client: the simulated bucket, with the listings and
/// plain writes a remover makes (keys, versions and last-modified times;
/// bodies read one by one). `dies`: set once its mark landed and it hangs
/// for ever, as a process killed between its two requests.
struct Removal {
    ep: EpBucket,
    die_one_in: u64,
    dies: Arc<AtomicBool>,
}

impl Removal {
    async fn listed(&self, prefix: &str) -> Result<Vec<(String, String, u64)>, BucketError> {
        Ok(self
            .ep
            .list_versions(prefix)
            .await?
            .into_iter()
            .map(|(k, v, m)| (k, v, m + EPOCH_MS))
            .collect())
    }
}

impl Bucket for Removal {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.ep.get(key).await
    }
    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.ep.create(key, body).await
    }
    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        let mark = forgotten(&body).is_some();
        let written = self.ep.replace(key, body, expected).await?;
        if mark
            && self.die_one_in > 0
            && madsim::rand::random::<u64>().is_multiple_of(self.die_one_in)
        {
            self.dies.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        Ok(written)
    }
}

impl wasmkeep_cluster::control::Keys for Removal {
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError> {
        Ok(self
            .listed(prefix)
            .await?
            .into_iter()
            .map(|(k, _, m)| (k, m))
            .collect())
    }
    async fn put_empty(&self, key: &str) -> Result<(), BucketError> {
        self.ep.put(key, Vec::new()).await
    }
    async fn put_body(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.ep.put(key, body).await
    }
    async fn get_body(&self, key: &str) -> Result<Option<Vec<u8>>, BucketError> {
        Ok(self.ep.get(key).await?.map(|(b, _)| b))
    }
    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        self.ep.delete(key).await
    }
}

impl wasmkeep_cluster::membership::Directory for Removal {
    async fn list(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, BucketError> {
        let mut out = Vec::new();
        for (key, _, _) in self.listed(prefix).await? {
            if let Some((body, _)) = self.ep.get(&key).await? {
                out.push((key, body));
            }
        }
        Ok(out)
    }
    async fn read(&self, keys: &[String]) -> Result<Vec<(String, Vec<u8>)>, BucketError> {
        let mut out = Vec::new();
        for key in keys {
            if let Some((body, _)) = self.ep.get(key).await? {
                out.push((key.clone(), body));
            }
        }
        Ok(out)
    }
}

impl wasmkeep_cluster::store::Store for Removal {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError> {
        Ok(self
            .listed(prefix)
            .await?
            .into_iter()
            .map(|(key, v, m)| ObjectMeta {
                key,
                size: 0,
                modified_ms: m,
                etag: Some(v),
            })
            .collect())
    }
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
        let mut dirs: Vec<String> = self
            .listed(prefix)
            .await?
            .into_iter()
            .filter_map(|(k, _, _)| {
                let (dir, _) = k.strip_prefix(prefix)?.split_once('/')?;
                Some(dir.to_owned())
            })
            .collect();
        dirs.dedup();
        Ok(dirs)
    }
    async fn copy(&self, _from: &str, _to: &str) -> Result<(), BucketError> {
        Err(BucketError::Unavailable("no copy here".into()))
    }
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.ep.put(key, body).await
    }
    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        self.ep.delete(key).await
    }
}

#[derive(Debug, Default)]
struct Outcome {
    /// Processes of `a` that found their lease gone or replaced.
    lost: u64,
    starts: u64,
    removed: u64,
    ended: u64,
    deaths: u64,
    /// Marks left at the end.
    marks_left: u64,
    /// `b`'s lease, whose node a record names, gone.
    named_removed: bool,
    marks_stayed: u64,
}

fn simulate(seed: u64, variant: Variant, duration: Duration) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let out = Arc::new(Mutex::new(Outcome::default()));
    let shared = Arc::clone(&out);
    rt.block_on(async move {
        let _modified = bucket::reset_modified();
        bucket::clear_delays();
        take_marks_stayed();
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let cfg = config();
        let mut rng = Rng::new(seed ^ 0x5EED_0F0F);
        let store = Store::default();
        {
            let mut s = store.lock().unwrap();
            s.insert(
                floor::relative_key("acme", "shop"),
                (floor::encode(Minor { major: 0, minor: 9 }), SEEDED),
            );
            let b = NodeLeaseObject {
                state: NodeState::Released,
                instance: Some("old".into()),
                written_at_ms: EPOCH_MS - 60_000,
            };
            s.insert(
                format!("{NODES}b"),
                (b.encode_with(&payload("b")), SEEDED + 1),
            );
            let record = OwnerRecord {
                holder: Some(("b".into(), "old".into())),
                generation: 4,
                recorded: None,
            };
            s.insert(
                "state/acme/shop/owners/c/x".into(),
                (record.encode(), SEEDED + 2),
            );
        }
        let _bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Arc::clone(&store);
                move || {
                    bucket_node_with(
                        Arc::clone(&store),
                        bucket::Faults {
                            slow_one_in: 8,
                            late_reply_one_in: 20,
                        },
                    )
                }
            })
            .build();

        // Node `a`: runs, stops cleanly, starts again.
        let breaks = Breaks {
            start_over_mark: variant == Variant::StartOverMark,
            ..Breaks::default()
        };
        let node_seed = rng.next_u64();
        let a_out = Arc::clone(&shared);
        let restarting = Arc::new(AtomicBool::new(true));
        let a_restarting = Arc::clone(&restarting);
        let _a = handle
            .create_node()
            .name("a")
            .ip("10.0.1.1".parse().unwrap())
            .init(move || {
                let out = Arc::clone(&a_out);
                let restarting = Arc::clone(&a_restarting);
                async move {
                    let mut rng = Rng::new(node_seed);
                    let skew = i64::try_from(rng.below(401)).unwrap() - 200;
                    let clock = SkewedClock {
                        global: move || env.now(),
                        skew_ms: skew,
                    };
                    sleep(Duration::from_millis(rng.below(2_000))).await;
                    while restarting.load(Ordering::SeqCst) {
                        let instance = format!("{:08x}", madsim::rand::random::<u32>());
                        let setup = NodeSetup {
                            id: "a".into(),
                            peers: Vec::new(),
                            nodes_prefix: NODES.into(),
                            config: config(),
                            attempt_ms: ATTEMPT_MS,
                            breaks,
                        };
                        let client = async || {
                            EpBucket::connect_with_timeout(Duration::from_millis(ATTEMPT_MS)).await
                        };
                        let buckets = NodeBuckets {
                            lease: client().await,
                            task_records: client().await,
                            peers: client().await,
                            records: client().await,
                        };
                        let (node, task) =
                            LeaseNode::start(setup, &instance, clock, buckets, payload("a"), &env)
                                .await;
                        out.lock().unwrap().starts += 1;
                        madsim::task::spawn_local(task.run(env));
                        let until = env.now() + 1_000 + rng.below(8_000);
                        while env.now() < until && !node.lost() {
                            sleep(Duration::from_millis(50)).await;
                        }
                        if node.lost() {
                            out.lock().unwrap().lost += 1;
                        } else {
                            node.clean_stop(&env).await;
                            if node.lost() {
                                out.lock().unwrap().lost += 1;
                            }
                        }
                        sleep(Duration::from_millis(rng.below(16_000))).await;
                    }
                }
            })
            .build();

        // Removers: one at a time mostly, now and then two; one may die
        // right after its mark landed.
        let after = Settings {
            after_ms: 0,
            horizon_ms: HORIZON_MS,
            config: cfg,
        };
        let bound = forget_delete_bound_ms(&cfg);
        let fault = variant.fault();
        let start_remover = |n: u64, die_one_in: u64, dies: Arc<AtomicBool>| {
            let out = Arc::clone(&shared);
            let remover_seed = seed ^ (n << 32) ^ 0xBEEF;
            handle
                .create_node()
                .name(format!("remover{n}"))
                .ip(format!("10.0.2.{}", n % 250 + 1).parse().unwrap())
                .init(move || {
                    let out = Arc::clone(&out);
                    let dies = Arc::clone(&dies);
                    async move {
                        let mut rng = Rng::new(remover_seed);
                        let skew = i64::try_from(rng.below(401)).unwrap() - 200;
                        let clock = SkewedClock {
                            global: move || env.now(),
                            skew_ms: skew,
                        };
                        let bucket = Removal {
                            ep: EpBucket::connect_with_timeout(Duration::from_millis(1_000)).await,
                            die_one_in,
                            dies,
                        };
                        let mut remover = Remover::with_fault(fault);
                        loop {
                            sleep(Duration::from_millis(300 + rng.below(1_500))).await;
                            let round: Round = match remover
                                .round(&bucket, clock.now_ms(), APP, &after, || {
                                    sleep(Duration::from_millis(bound))
                                })
                                .await
                            {
                                Ok(r) => r,
                                Err(_) => continue,
                            };
                            let mut o = out.lock().unwrap();
                            o.removed += round.removed.len() as u64;
                            o.ended += round.ended.len() as u64;
                        }
                    }
                })
                .build()
                .id()
        };
        let started = Instant::now();
        let mut n = 0u64;
        let mut current = None;
        let mut second = None;
        while started.elapsed() < duration {
            if current.is_none() {
                n += 1;
                let dies = Arc::new(AtomicBool::new(false));
                current = Some((start_remover(n, 3, Arc::clone(&dies)), dies));
            }
            if let Some((id, dies)) = &current
                && dies.load(Ordering::SeqCst)
            {
                handle.kill(*id);
                shared.lock().unwrap().deaths += 1;
                current = None;
                // Another node takes over after a while.
                sleep(Duration::from_millis(rng.below(3_000))).await;
                continue;
            }
            if second.is_none() && rng.below(40) == 0 {
                n += 1;
                second = Some(start_remover(n, 0, Arc::new(AtomicBool::new(false))));
            } else if let Some(id) = second
                && rng.below(20) == 0
            {
                handle.kill(id);
                second = None;
            }
            sleep(Duration::from_millis(100)).await;
        }
        // The end: `a` stops restarting, one remover runs alone and never
        // dies.
        restarting.store(false, Ordering::SeqCst);
        if let Some((id, _)) = current.take() {
            handle.kill(id);
        }
        if let Some(id) = second.take() {
            handle.kill(id);
        }
        n += 1;
        let _last = start_remover(n, 0, Arc::new(AtomicBool::new(false)));
        // Given time: a round stops at its first failed request, and one
        // request in eight here is slow.
        let marked = || {
            store
                .lock()
                .unwrap()
                .values()
                .any(|(body, _)| forgotten(body).is_some())
        };
        let quiet = Instant::now();
        sleep(Duration::from_secs(10)).await;
        while marked() && quiet.elapsed() < Duration::from_secs(300) {
            sleep(Duration::from_secs(1)).await;
        }
        let s = store.lock().unwrap();
        let mut o = shared.lock().unwrap();
        o.marks_left = s
            .values()
            .filter(|(body, _)| forgotten(body).is_some())
            .count() as u64;
        o.named_removed = !s.contains_key(&format!("{NODES}b"));
        o.marks_stayed = take_marks_stayed();
    });
    std::mem::take(&mut *out.lock().unwrap())
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

fn only_seed() -> Option<u64> {
    std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
}

/// Why a run broke the rules, if it did.
fn broken(o: &Outcome) -> Option<String> {
    if o.lost > 0 {
        return Some(format!("{} processes found their lease gone", o.lost));
    }
    if o.named_removed {
        return Some("a lease removed while a record names its node".into());
    }
    if o.marks_left > 0 {
        return Some(format!("{} marks left at the end", o.marks_left));
    }
    None
}

#[test]
fn released_leases_are_removed_and_no_lease_a_node_wrote_is_deleted() {
    let range = only_seed().map_or(0..seeds(), |s| s..s + 1);
    let (mut removed, mut ended, mut deaths, mut starts, mut stayed) = (0, 0, 0, 0, 0);
    for seed in range {
        let o = simulate(seed, Variant::Correct, Duration::from_secs(120));
        if let Some(why) = broken(&o) {
            panic!("seed {seed}: {why}: {o:?}");
        }
        removed += o.removed;
        ended += o.ended;
        deaths += o.deaths;
        starts += o.starts;
        stayed += o.marks_stayed;
    }
    eprintln!(
        "{} seeds: {starts} starts of a, {removed} leases removed, {deaths} removers died, \
         {ended} marks ended, {stayed} starts that gave up on a mark",
        seeds()
    );
    if only_seed().is_none() {
        let floor = (seeds() / 10).max(1);
        assert!(removed >= seeds(), "{removed} removals");
        assert!(deaths >= floor, "{deaths} removers died");
        assert!(ended >= floor, "{ended} marks ended");
    }
}

/// Each rule the removal rests on, broken, is caught on some seed.
#[test]
fn each_broken_removal_rule_is_caught() {
    for variant in [
        Variant::Unmarked,
        Variant::StartOverMark,
        Variant::RecordsUnchecked,
        Variant::MarksKept,
    ] {
        let caught = (0..seeds()).find_map(|seed| {
            broken(&simulate(seed, variant, Duration::from_secs(120))).map(|why| (seed, why))
        });
        eprintln!("{variant:?}: caught at {caught:?}");
        assert!(caught.is_some(), "{variant:?} was never caught");
    }
}
