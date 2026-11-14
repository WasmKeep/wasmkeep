//! A node whose lease writes fail while its other writes still land (ADR
//! 0020, section 1, amended 2026-11-14), under madsim: three nodes with
//! skewed clocks, one bucket, one actor. Now and then one node's lease
//! writes are refused for a while (a slow or throttled bucket timing them
//! out, an error on that one key), long enough for its lease to run out;
//! its ownership records still land. The bucket also applies one request
//! in 25 late.
//!
//! Each process runs on the simulations' shared node (`wasmkeep_sim::node`,
//! the server's rules). Its actor writes while its hold may act: each write
//! is uploaded, then acknowledged only if the hold may still act (the
//! server's `Replica::put`). Its actor gives the hold up now and then (a
//! hibernation), and an event that finds the hold no longer valid gives it
//! up too, as the server's actor drops it: after a lapse the lease task has
//! not seen yet, that release lands at once, and another node may take the
//! actor without a fence. Releases are sent in the background, as the
//! server's hold sends its own when dropped, while the actor goes on.
//!
//! Checked: never two processes acting as owner at the same global moment,
//! never one generation for two holds, and never a write acknowledged by a
//! hold after a later generation's acquisition landed (the new owner's
//! restore could miss it: an acknowledged write lost). The correct run
//! must hand the actor over that way (releases after a lapse that landed,
//! then acquisitions from nobody); the broken variant, a hold that keeps
//! acting until its release is answered, must be caught on some seed.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Bucket, BucketError, LeaseConfig, Version};
use wasmkeep_cluster::node_lease::{OwnerRecord, Released};
use wasmkeep_sim::check::{double_ownership, stop_process};
use wasmkeep_sim::driver::{Claim, Env, SkewedClock};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node_with};

const ACTOR: &str = "owners/t/c/a";
const NODES: &str = "nodes/";
const EPOCH_MS: u64 = 1_000_000;
/// The bucket client's timeout (`EpBucket::connect`): one attempt's limit.
const ATTEMPT_MS: u64 = 300;

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

/// The lease's own client: its writes are refused while `cut` is set
/// (reads still answer), the node's other clients unaffected.
struct LeaseWrites {
    inner: EpBucket,
    cut: Arc<AtomicBool>,
}

impl LeaseWrites {
    fn refused(&self) -> Option<BucketError> {
        self.cut
            .load(Ordering::SeqCst)
            .then(|| BucketError::Unavailable("lease write refused".into()))
    }
}

impl Bucket for LeaseWrites {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        if let Some(e) = self.refused() {
            return Err(e);
        }
        self.inner.create(key, body).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        if let Some(e) = self.refused() {
            return Err(e);
        }
        self.inner.replace(key, body, expected).await
    }
}

/// A write the actor acknowledged: by which hold, when (global ms).
#[derive(Clone, Copy, Debug)]
struct Ack {
    #[allow(dead_code, reason = "shown in a failure's message")]
    node: u32,
    generation: u64,
    at_ms: u64,
}

/// An acquisition that landed, by the time its caller learned it (global
/// ms): the write landed no later.
#[derive(Clone, Copy, Debug)]
struct Acquired {
    #[allow(dead_code, reason = "shown in a failure's message")]
    node: u32,
    generation: u64,
    at_ms: u64,
    /// The record named nobody just before.
    from_nobody: bool,
}

#[derive(Default)]
struct Shared {
    claims: Vec<Claim>,
    acks: Vec<Ack>,
    acquired: Vec<Acquired>,
    /// Releases sent while the node's lease had run out, and of those the
    /// ones that landed.
    lapsed_releases: u64,
    lapsed_landed: u64,
    /// Times a node's lease writes were cut.
    cuts: u64,
}

/// A write acknowledged after a later generation's acquisition landed.
fn lost_ack(s: &Shared) -> Option<(Ack, Acquired)> {
    s.acks.iter().find_map(|ack| {
        s.acquired
            .iter()
            .find(|a| a.generation > ack.generation && a.at_ms < ack.at_ms)
            .map(|a| (*ack, *a))
    })
}

/// Two acquisitions of one generation by different holds.
fn shared_generation(s: &Shared) -> Option<(Acquired, Acquired)> {
    for (i, a) in s.acquired.iter().enumerate() {
        if let Some(b) = s.acquired[i + 1..]
            .iter()
            .find(|b| b.generation == a.generation)
        {
            return Some((*a, *b));
        }
    }
    None
}

async fn names_nobody<B: Bucket>(bucket: &B) -> bool {
    matches!(bucket.get(ACTOR).await, Ok(Some((body, _)))
        if OwnerRecord::decode(&body).is_some_and(|r| r.holder.is_none()))
}

/// One node's lives: a process starts (the start check), takes the actor
/// when it holds nothing, writes while it may act, gives the hold up now
/// and then, and gives it up when an event finds it no longer valid; a
/// lost lease (a fence, after a long cut) ends the process, and the next
/// one starts.
#[allow(clippy::too_many_arguments)]
async fn run_node(
    i: u32,
    env: SimEnv,
    skew_ms: i64,
    seed: u64,
    breaks: Breaks,
    cut: Arc<AtomicBool>,
    shared: Arc<Mutex<Shared>>,
) {
    let cfg = config();
    let id = format!("n{i}");
    let mut rng = Rng::new(seed);
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
        // The lease's writes go through the cut; nothing else does. The
        // peers' client reads and fences with the node's other clients.
        let buckets = NodeBuckets {
            lease: LeaseWrites {
                inner: EpBucket::connect().await,
                cut: Arc::clone(&cut),
            },
            task_records: LeaseWrites {
                inner: EpBucket::connect().await,
                cut: Arc::new(AtomicBool::new(false)),
            },
            peers: LeaseWrites {
                inner: EpBucket::connect().await,
                cut: Arc::new(AtomicBool::new(false)),
            },
            records: LeaseWrites {
                inner: EpBucket::connect().await,
                cut: Arc::new(AtomicBool::new(false)),
            },
        };
        let (node, task) =
            LeaseNode::start(setup, &instance, clock, buckets, Vec::new(), &env).await;
        let node = Rc::new(node);
        madsim::task::spawn_local(task.run(env));
        let reader = EpBucket::connect().await;
        let end = |shared: &Mutex<Shared>| {
            stop_process(&mut shared.lock().unwrap().claims, i, process, env.now());
        };
        // Gives `h` up in the background, as the server's dropped hold
        // does, noting a release sent after a lapse.
        let give_up = |h: Held| {
            let lapsed = node.still_held(&h) && !node.lease_valid();
            let (node, shared) = (Rc::clone(&node), Arc::clone(&shared));
            madsim::task::spawn_local(async move {
                let got = node.release_outcome(&h).await;
                if lapsed {
                    let mut s = shared.lock().unwrap();
                    s.lapsed_releases += 1;
                    if got == Some(Released::Released) {
                        s.lapsed_landed += 1;
                    }
                }
            });
        };
        let mut hold: Option<Held> = None;
        loop {
            if node.lost() {
                end(&shared);
                break;
            }
            match hold.take() {
                Some(h) if node.may_act(&h) => {
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
                    if rng.chance(10) {
                        // A hibernation: given up while it may act.
                        give_up(h.clone());
                        if !breaks.release_before_stop {
                            end(&shared);
                        }
                        hold = Some(h);
                    } else {
                        // A write: uploaded, then acknowledged only if the
                        // hold may still act.
                        sleep(Duration::from_millis(10 + rng.below(70))).await;
                        if node.may_act(&h) {
                            shared.lock().unwrap().acks.push(Ack {
                                node: i,
                                generation: h.generation,
                                at_ms: env.now(),
                            });
                        }
                        hold = Some(h);
                    }
                }
                Some(h) if node.still_held(&h) => {
                    // No longer valid (a lapse the lease task has not seen
                    // yet), or given up with its release unanswered: an
                    // event that reaches the actor now gives it up.
                    end(&shared);
                    if node.lease_valid() {
                        // Released in the background, not answered yet
                        // (the broken variant keeps it meanwhile).
                        hold = Some(h);
                    } else if rng.chance(40) {
                        give_up(h);
                    } else {
                        hold = Some(h);
                    }
                }
                Some(_) => end(&shared),
                None if rng.chance(20) => {
                    let from_nobody = names_nobody(&reader).await;
                    if let Ok(h) = node.acquire(ACTOR, 1).await {
                        let mut s = shared.lock().unwrap();
                        s.acquired.push(Acquired {
                            node: i,
                            generation: h.generation,
                            at_ms: env.now(),
                            from_nobody,
                        });
                        drop(s);
                        hold = Some(h);
                    }
                }
                None => {}
            }
            sleep(Duration::from_millis(20 + rng.below(40))).await;
        }
        sleep(Duration::from_millis(200 + rng.below(800))).await;
    }
}

struct Outcome {
    claims: Vec<Claim>,
    lost_ack: Option<(Ack, Acquired)>,
    shared_generation: Option<(Acquired, Acquired)>,
    lapsed_releases: u64,
    lapsed_landed: u64,
    from_nobody: u64,
    acquired: u64,
    cuts: u64,
}

fn simulate(seed: u64, breaks: Breaks, duration: Duration) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let shared = Arc::new(Mutex::new(Shared::default()));
    let out = Arc::clone(&shared);
    rt.block_on(async move {
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let mut rng = Rng::new(seed ^ 0x0EA2_17E1);
        let _bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Store::default();
                move || bucket_node_with(Arc::clone(&store), bucket::Faults { slow_one_in: 25 })
            })
            .build();
        let max_skew = config().max_skew_ms();
        let cuts: Vec<Arc<AtomicBool>> = (0..3).map(|_| Arc::default()).collect();
        for i in 1..=3u32 {
            let skew_ms = i64::try_from(rng.below(max_skew * 2 + 1)).unwrap()
                - i64::try_from(max_skew).unwrap();
            let shared = Arc::clone(&shared);
            let cut = Arc::clone(&cuts[usize::try_from(i).unwrap() - 1]);
            let node_seed = seed ^ (u64::from(i) << 40);
            handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || {
                    run_node(
                        i,
                        env,
                        skew_ms,
                        node_seed,
                        breaks,
                        Arc::clone(&cut),
                        Arc::clone(&shared),
                    )
                })
                .build();
        }
        // Now and then one node's lease writes are refused for 0.3 to 3 s:
        // past 800 ms its lease runs out, past about 2 s it is fenced.
        let mut until: [u64; 3] = [0; 3];
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            for (n, cut) in cuts.iter().enumerate() {
                if cut.load(Ordering::SeqCst) && until[n] <= at {
                    cut.store(false, Ordering::SeqCst);
                }
            }
            if rng.below(20) == 0 {
                let n = usize::try_from(rng.below(3)).unwrap();
                if !cuts[n].load(Ordering::SeqCst) {
                    cuts[n].store(true, Ordering::SeqCst);
                    until[n] = at + 300 + rng.below(2_700);
                    shared.lock().unwrap().cuts += 1;
                }
            }
        }
    });
    let s = out.lock().unwrap();
    Outcome {
        claims: s.claims.clone(),
        lost_ack: lost_ack(&s),
        shared_generation: shared_generation(&s),
        lapsed_releases: s.lapsed_releases,
        lapsed_landed: s.lapsed_landed,
        from_nobody: s.acquired.iter().filter(|a| a.from_nobody).count() as u64,
        acquired: s.acquired.len() as u64,
        cuts: s.cuts,
    }
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn a_release_after_a_lapse_hands_the_actor_over_safely() {
    let (mut sent, mut landed, mut from_nobody, mut acquired, mut cuts) = (0, 0, 0, 0, 0);
    for seed in 0..seeds() {
        let o = simulate(seed, Breaks::default(), Duration::from_secs(20));
        if let Some(d) = double_ownership(&o.claims) {
            panic!("seed {seed}: two owners: {d:?}");
        }
        if let Some(pair) = o.shared_generation {
            panic!("seed {seed}: one generation, two holds: {pair:?}");
        }
        if let Some(pair) = o.lost_ack {
            panic!("seed {seed}: acknowledged after a later acquisition landed: {pair:?}");
        }
        sent += o.lapsed_releases;
        landed += o.lapsed_landed;
        from_nobody += o.from_nobody;
        acquired += o.acquired;
        cuts += o.cuts;
    }
    eprintln!(
        "{} seeds: {cuts} lease cuts, {sent} releases after a lapse ({landed} landed), \
         {acquired} acquisitions ({from_nobody} from nobody)",
        seeds()
    );
    let floor = (seeds() / 4).max(1);
    assert!(
        landed >= floor,
        "{landed} releases after a lapse landed, want {floor}"
    );
    assert!(
        from_nobody >= floor,
        "{from_nobody} acquisitions from nobody, want {floor}"
    );
}

/// A hold that keeps acting until its release is answered is caught: the
/// node that takes the record once it landed acts beside it, and a write
/// is acknowledged after that acquisition.
#[test]
fn a_hold_acting_until_its_release_is_answered_is_caught() {
    let breaks = Breaks {
        release_before_stop: true,
        ..Breaks::default()
    };
    let caught = (0..seeds().max(200)).find_map(|seed| {
        let o = simulate(seed, breaks, Duration::from_secs(20));
        if double_ownership(&o.claims).is_some() {
            Some((seed, "two owners"))
        } else if o.lost_ack.is_some() {
            Some((seed, "an acknowledged write lost"))
        } else {
            None
        }
    });
    eprintln!("release before stop: caught by {caught:?}");
    assert!(
        caught.is_some(),
        "never caught in {} seeds",
        seeds().max(200)
    );
}
