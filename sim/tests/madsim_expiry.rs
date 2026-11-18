//! Idle expiry racing wakes and alarms ([ADR 0028], section 4), under
//! madsim: three nodes with skewed clocks, one bucket that applies one
//! request in 25 late and answers another in 25 only after its client gave
//! up, three actors of one class.
//!
//! Each node runs the simulations' shared node (`wasmkeep_sim::node`, the
//! server's rules). Now and then it wakes an actor as an event does: an
//! ordinary acquisition, a few events that run its code (each acknowledged
//! only while the hold may act), sometimes setting or clearing its alarm
//! (in the actor's database, then in the alarm index, best effort), then a
//! release, as a hibernation. Now and then it runs an expiry pass as the
//! collecting node does: one listing of `owners/` (versions and
//! last-modified times), one of `alarms/`, the candidates of
//! `wasmkeep_cluster::expiry::candidates`, and for each, after a pause (the
//! job waits in the actor's mailbox), the expiry's own acquisition
//! (`Owners::acquire_unchanged`: released, and the version listed), a
//! check of the restored database for an alarm, and the delete, recorded
//! only if the hold may still act. Every node runs passes, not only the
//! lowest id: the rules must hold whoever collects.
//!
//! Checked: no actor is expired while active (an event of an earlier
//! generation acknowledged less than the limit, less the clocks' skew,
//! before the expiry acquired it: a wake lost), and none whose database
//! holds an alarm. The correct run must expire actors and must see
//! expiries refused because the record changed since the listing. Two
//! broken variants must be caught on some seed: an expiry that acquires as
//! an event does (ignoring the listing), and one that ignores alarms.
//!
//! [ADR 0028]: ../../docs/adr/0028-actors-that-end.md
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Handle;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::expiry::{Listed, candidates};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::node_lease::AcquireError;
use wasmkeep_sim::driver::{Env, SkewedClock};
use wasmkeep_sim::node::{Breaks, LeaseNode, NodeBuckets, NodeSetup};
use wasmkeep_sim::rng::Rng;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node_with, reset_modified};

const OWNERS: &str = "owners/t/c/";
const ALARMS: &str = "alarms/";
const NODES: &str = "nodes/";
const EPOCH_MS: u64 = 1_000_000;
const ATTEMPT_MS: u64 = 300;
/// The class's limit.
const IDLE_MS: u64 = 1_500;
/// Actors: the first is woken often, the others seldom.
const ACTORS: [&str; 3] = ["a0", "a1", "a2"];

/// ttl 1 s, grace 500 ms, safety 200 ms: tolerates 350 ms of skew.
fn config() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
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

/// What the expiry does wrong, for the broken variants.
#[derive(Debug, Clone, Copy, Default)]
struct Wrong {
    /// It acquires as an event does, whatever the listing showed.
    plain_acquire: bool,
    /// It skips the alarm listing and the restored database's alarm.
    ignore_alarms: bool,
}

/// An event that ran the actor's code, acknowledged while its hold could
/// act (global ms).
#[derive(Debug, Clone, Copy)]
struct Act {
    generation: u64,
    at_ms: u64,
}

/// An expiry recorded: the actor, the hold's generation, when its
/// acquisition returned (global ms), and whether the database it deleted
/// held an alarm.
#[derive(Debug, Clone)]
struct Expiry {
    actor: String,
    generation: u64,
    acquired_ms: u64,
    alarm: bool,
}

#[derive(Default)]
struct Shared {
    acts: HashMap<String, Vec<Act>>,
    /// Each actor's database as its holders acknowledged it: whether an
    /// alarm is set.
    alarm: HashMap<String, bool>,
    expiries: Vec<Expiry>,
    /// Expiries whose acquisition found the record changed since the listing.
    changed: u64,
    /// Expiries that found an alarm in the restored database and kept the
    /// actor.
    kept_for_alarm: u64,
}

/// An expiry of an actor active within the limit: the event and the
/// expiry.
fn active_expired(s: &Shared) -> Option<(Act, Expiry)> {
    let slack = config().max_skew_ms();
    s.expiries.iter().find_map(|e| {
        s.acts.get(&e.actor)?.iter().find_map(|a| {
            (a.generation < e.generation && a.at_ms + IDLE_MS > e.acquired_ms + slack)
                .then(|| (*a, e.clone()))
        })
    })
}

/// One node's lives: a process starts, then wakes actors and runs expiry
/// passes until its lease is lost; the next one starts.
async fn run_node(
    i: u32,
    env: SimEnv,
    skew_ms: i64,
    seed: u64,
    wrong: Wrong,
    shared: Arc<Mutex<Shared>>,
) {
    let id = format!("n{i}");
    let mut rng = Rng::new(seed);
    loop {
        let instance = format!("{:08x}", madsim::rand::random::<u32>());
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
            config: config(),
            attempt_ms: ATTEMPT_MS,
            breaks: Breaks::default(),
        };
        let buckets = NodeBuckets {
            lease: EpBucket::connect().await,
            task_records: EpBucket::connect().await,
            peers: EpBucket::connect().await,
            records: EpBucket::connect().await,
        };
        let (node, task) =
            LeaseNode::start(setup, &instance, clock, buckets, Vec::new(), &env).await;
        let node = Rc::new(node);
        madsim::task::spawn_local(task.run(env));
        let client = EpBucket::connect().await;
        while !node.lost() {
            match rng.below(100) {
                0..=39 => {
                    // a0 three times as often as each of the others.
                    let k = match rng.below(5) {
                        0..=2 => 0,
                        n => usize::try_from(n).unwrap() - 2,
                    };
                    wake(&node, &client, ACTORS[k], env, &mut rng, &shared).await;
                }
                40..=54 => expiry_pass(&node, &client, clock, env, &mut rng, wrong, &shared).await,
                _ => {}
            }
            sleep(Duration::from_millis(50 + rng.below(400))).await;
        }
        sleep(Duration::from_millis(200 + rng.below(800))).await;
    }
}

/// An event wakes actor `name`: acquired, a few events, maybe its alarm set
/// or cleared, then released (a hibernation).
async fn wake(
    node: &LeaseNode<EpBucket, SkewedClock<impl Fn() -> u64 + Copy>>,
    client: &EpBucket,
    name: &str,
    env: SimEnv,
    rng: &mut Rng,
    shared: &Mutex<Shared>,
) {
    let Ok(h) = node.acquire(&format!("{OWNERS}{name}"), 1).await else {
        return; // held elsewhere, or no answer: the event is forwarded or refused
    };
    for _ in 0..=rng.below(3) {
        if !node.may_act(&h) {
            break;
        }
        sleep(Duration::from_millis(10 + rng.below(40))).await;
        if !node.may_act(&h) {
            break;
        }
        let at_ms = env.now();
        shared
            .lock()
            .unwrap()
            .acts
            .entry(name.to_owned())
            .or_default()
            .push(Act {
                generation: h.generation,
                at_ms,
            });
        if rng.chance(30) {
            // The alarm, in the database, then the index (best effort: an
            // entry may be missing, the database is what counts).
            let set = rng.chance(50);
            sleep(Duration::from_millis(5 + rng.below(20))).await;
            if node.may_act(&h) {
                shared.lock().unwrap().alarm.insert(name.to_owned(), set);
                let key = format!("{ALARMS}{name}");
                let _ = if set {
                    client.put(&key, b"x".to_vec()).await
                } else {
                    client.delete(&key).await
                };
            }
        }
    }
    let _ = node.release(&h).await;
}

/// An expiry pass: the listings, the candidates, then each one's expiry.
async fn expiry_pass(
    node: &LeaseNode<EpBucket, SkewedClock<impl Fn() -> u64 + Copy>>,
    client: &EpBucket,
    clock: SkewedClock<impl Fn() -> u64 + Copy>,
    env: SimEnv,
    rng: &mut Rng,
    wrong: Wrong,
    shared: &Mutex<Shared>,
) {
    let Ok(owners) = client.list_versions(OWNERS).await else {
        return;
    };
    let alarmed: BTreeSet<String> = if wrong.ignore_alarms {
        BTreeSet::new()
    } else {
        let Ok(alarms) = client.list_versions(ALARMS).await else {
            return;
        };
        alarms
            .into_iter()
            .filter_map(|(k, _, _)| k.strip_prefix(ALARMS).map(str::to_owned))
            .collect()
    };
    let listed = owners
        .into_iter()
        .filter_map(|(k, version, modified_ms)| {
            Some(Listed {
                name: k.strip_prefix(OWNERS)?.to_owned(),
                modified_ms,
                version,
                idle_since_ms: None,
            })
        })
        .collect();
    // The listing's times are the bucket's (since the start); this node's
    // clock is skewed.
    let now = clock.now_ms().saturating_sub(EPOCH_MS);
    for c in candidates(listed, &alarmed, |_| false, now, IDLE_MS, 100) {
        // The job waits in the actor's mailbox a while.
        sleep(Duration::from_millis(rng.below(300))).await;
        let key = format!("{OWNERS}{}", c.name);
        let acquired = if wrong.plain_acquire {
            node.acquire(&key, 1).await
        } else {
            node.acquire_unchanged(&key, &c.version).await
        };
        let h = match acquired {
            Ok(h) => h,
            Err(AcquireError::Changed) => {
                shared.lock().unwrap().changed += 1;
                continue;
            }
            Err(_) => continue,
        };
        let acquired_ms = env.now();
        // The restored database: an alarm keeps the actor.
        let alarm = shared
            .lock()
            .unwrap()
            .alarm
            .get(&c.name)
            .copied()
            .unwrap_or(false);
        if alarm && !wrong.ignore_alarms {
            shared.lock().unwrap().kept_for_alarm += 1;
            let _ = node.release(&h).await;
            continue;
        }
        // The reset uploaded, recorded only while the hold may act.
        sleep(Duration::from_millis(10 + rng.below(40))).await;
        if node.may_act(&h) {
            let mut s = shared.lock().unwrap();
            s.expiries.push(Expiry {
                actor: c.name.clone(),
                generation: h.generation,
                acquired_ms,
                alarm,
            });
            s.alarm.insert(c.name.clone(), false);
        }
        let _ = node.release(&h).await;
    }
}

struct Outcome {
    active_expired: Option<(Act, Expiry)>,
    alarmed_expired: Option<Expiry>,
    expiries: u64,
    changed: u64,
    kept_for_alarm: u64,
}

fn simulate(seed: u64, wrong: Wrong, duration: Duration) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let shared = Arc::new(Mutex::new(Shared::default()));
    let out = Arc::clone(&shared);
    rt.block_on(async move {
        let _timestamps = reset_modified();
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let mut rng = Rng::new(seed ^ 0x0E1D_1E57);
        let _bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Store::default();
                move || {
                    bucket_node_with(
                        Arc::clone(&store),
                        bucket::Faults {
                            slow_one_in: 25,
                            late_reply_one_in: 25,
                        },
                    )
                }
            })
            .build();
        let max_skew = config().max_skew_ms();
        for i in 1..=3u32 {
            let skew_ms = i64::try_from(rng.below(max_skew * 2 + 1)).unwrap()
                - i64::try_from(max_skew).unwrap();
            let shared = Arc::clone(&shared);
            let node_seed = seed ^ (u64::from(i) << 40);
            handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || run_node(i, env, skew_ms, node_seed, wrong, Arc::clone(&shared)))
                .build();
        }
        sleep(duration).await;
    });
    let s = out.lock().unwrap();
    Outcome {
        active_expired: active_expired(&s),
        alarmed_expired: s.expiries.iter().find(|e| e.alarm).cloned(),
        expiries: s.expiries.len() as u64,
        changed: s.changed,
        kept_for_alarm: s.kept_for_alarm,
    }
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn an_expiry_never_loses_a_wake_nor_an_alarm() {
    let (mut expiries, mut changed, mut kept) = (0, 0, 0);
    for seed in 0..seeds() {
        let o = simulate(seed, Wrong::default(), Duration::from_secs(20));
        if let Some(pair) = o.active_expired {
            panic!("seed {seed}: an active actor expired: {pair:?}");
        }
        if let Some(e) = o.alarmed_expired {
            panic!("seed {seed}: an actor with an alarm expired: {e:?}");
        }
        expiries += o.expiries;
        changed += o.changed;
        kept += o.kept_for_alarm;
    }
    eprintln!(
        "{} seeds: {expiries} expiries, {changed} refused (the record changed since the \
         listing), {kept} kept for an alarm",
        seeds()
    );
    let floor = (seeds() / 4).max(1);
    assert!(expiries >= floor, "{expiries} expiries, want {floor}");
    assert!(
        changed >= floor,
        "{changed} refused as changed, want {floor}"
    );
}

#[test]
fn an_expiry_that_acquires_as_an_event_does_is_caught() {
    caught(
        Wrong {
            plain_acquire: true,
            ..Wrong::default()
        },
        "plain acquire",
    );
}

#[test]
fn an_expiry_that_ignores_alarms_is_caught() {
    caught(
        Wrong {
            ignore_alarms: true,
            ..Wrong::default()
        },
        "alarms ignored",
    );
}

fn caught(wrong: Wrong, what: &str) {
    let caught = (0..seeds().max(200)).find_map(|seed| {
        let o = simulate(seed, wrong, Duration::from_secs(20));
        if o.active_expired.is_some() {
            Some((seed, "an active actor expired"))
        } else if o.alarmed_expired.is_some() {
            Some((seed, "an actor with an alarm expired"))
        } else {
            None
        }
    });
    eprintln!("{what}: caught by {caught:?}");
    assert!(
        caught.is_some(),
        "{what}: never caught in {} seeds",
        seeds().max(200)
    );
}
