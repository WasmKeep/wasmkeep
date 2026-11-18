//! A node whose lease writes fail while its other writes still land (ADR
//! 0020, section 1, amended 2026-11-14), under madsim: three nodes with
//! skewed clocks, one bucket, one actor. Now and then one node's lease
//! writes are refused for a while (a slow or throttled bucket timing them
//! out, an error on that one key), long enough for its lease to run out;
//! its ownership records still land. The bucket also applies one request
//! in 25 late, and answers another in 25 only after its client gave up;
//! a release after a lapse it applies at once and answers 150 to 290 ms
//! later, within the client's timeout.
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
//! then acquisitions of the records they freed). Two broken variants must
//! each be caught on a tenth of the seeds at least: a hold that keeps
//! acting until its release is answered (on a hibernation's release,
//! while the lease is valid), and, with no hibernation, a hold that acts
//! on its cell alone and keeps acting until its release is answered: what
//! is left after a lapse if the node did not also stop acting by its own
//! clock.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Handle;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Bucket, LeaseConfig};
use wasmkeep_cluster::node_lease::{OwnerRecord, Released};
use wasmkeep_sim::check::{double_ownership, stop_process};
use wasmkeep_sim::driver::{Claim, Env, SkewedClock};
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup};
use wasmkeep_sim::rng::Rng;

#[path = "support/runtime.rs"]
mod runtime;
use runtime::Runtime;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{CutWrites, EpBucket, Store, bucket_node_with};

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
    /// The generations whose release after a lapse landed.
    lapsed_generations: Vec<u64>,
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
    hibernate: bool,
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
            lease: CutWrites {
                inner: EpBucket::connect().await,
                cut: Arc::clone(&cut),
            },
            task_records: CutWrites::uncut(EpBucket::connect().await),
            peers: CutWrites::uncut(EpBucket::connect().await),
            records: CutWrites::uncut(EpBucket::connect().await),
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
        // does (once: `given` keeps its generation), noting a release sent
        // after a lapse.
        let give_up = |h: Held, given: &mut Option<u64>| {
            *given = Some(h.generation);
            let lapsed = node.still_held(&h) && !node.lease_valid();
            let (node, shared) = (Rc::clone(&node), Arc::clone(&shared));
            madsim::task::spawn_local(async move {
                let got = node.release_outcome(&h).await;
                if lapsed {
                    let mut s = shared.lock().unwrap();
                    s.lapsed_releases += 1;
                    if got == Some(Released::Released) {
                        s.lapsed_landed += 1;
                        s.lapsed_generations.push(h.generation);
                    }
                }
            });
        };
        let mut hold: Option<Held> = None;
        let mut given: Option<u64> = None;
        loop {
            if node.lost() {
                end(&shared);
                break;
            }
            match hold.take() {
                // An event reaches the actor after a lapse the lease task
                // has not seen yet: the server's actor finds the hold no
                // longer valid by the lease, and gives it up. The bucket
                // applies that release at once and answers it a while
                // later, within the client's timeout: the window in which a
                // hold that acted until its release is answered acts beside
                // the node that took the record.
                Some(h)
                    if given != Some(h.generation)
                        && node.still_held(&h)
                        && !node.lease_valid()
                        && rng.chance(40) =>
                {
                    end(&shared);
                    bucket::late_reply_next("REPLACE", ACTOR, 150 + rng.below(140));
                    give_up(h.clone(), &mut given);
                    hold = Some(h);
                }
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
                    if hibernate && given != Some(h.generation) && rng.chance(10) {
                        // A hibernation: given up while it may act.
                        give_up(h.clone(), &mut given);
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
                    // No longer valid and no event yet, or given up with
                    // its release unanswered (the broken variants keep it
                    // meanwhile).
                    end(&shared);
                    hold = Some(h);
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
    /// Acquisitions of a record a release after a lapse freed.
    after_lapse: u64,
    acquired: u64,
    cuts: u64,
}

/// `hibernate`: holds are also given up while they may act.
fn simulate(seed: u64, breaks: Breaks, hibernate: bool, duration: Duration) -> Outcome {
    bucket::clear_delays();
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
                        hibernate,
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
        // The record named nobody at the generation a release after a
        // lapse freed: this acquisition took what that release handed over.
        after_lapse: s
            .acquired
            .iter()
            .filter(|a| {
                a.from_nobody
                    && s.lapsed_generations
                        .contains(&a.generation.saturating_sub(1))
            })
            .count() as u64,
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
    let (mut sent, mut landed, mut after_lapse, mut acquired, mut cuts) = (0, 0, 0, 0, 0);
    for seed in 0..seeds() {
        let o = simulate(seed, Breaks::default(), true, Duration::from_secs(20));
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
        after_lapse += o.after_lapse;
        acquired += o.acquired;
        cuts += o.cuts;
    }
    eprintln!(
        "{} seeds: {cuts} lease cuts, {sent} releases after a lapse ({landed} landed), \
         {acquired} acquisitions ({after_lapse} of a record such a release freed)",
        seeds()
    );
    let floor = (seeds() / 4).max(1);
    assert!(
        landed >= floor,
        "{landed} releases after a lapse landed, want {floor}"
    );
    assert!(
        after_lapse >= floor,
        "{after_lapse} acquisitions of a record a release after a lapse freed, want {floor}"
    );
}

/// A hold that keeps acting until its release is answered is caught: the
/// node that takes the record once it landed acts beside it, and a write
/// is acknowledged after that acquisition. Once with hibernations (a
/// release while the lease is valid), once without, with holds acting on
/// their cell alone: only releases after a lapse then hand the actor over.
#[test]
fn a_hold_acting_until_its_release_is_answered_is_caught() {
    let before_stop = Breaks {
        release_before_stop: true,
        ..Breaks::default()
    };
    let after_lapse = Breaks {
        act_on_cell_only: true,
        ..before_stop
    };
    let hibernating = caught(before_stop, true, "release before stop, hibernating");
    let lapsed = caught(
        after_lapse,
        false,
        "release before stop, acting on the cell alone",
    );
    // Caught often, not on one lucky seed: a tenth of the seeds at least.
    let floor = (seeds() / 10).max(1);
    for (what, n) in [("hibernating", hibernating), ("after a lapse", lapsed)] {
        assert!(n >= floor, "{what}: caught on {n} seeds, want {floor}");
    }
}

/// On how many seeds the variant is caught.
fn caught(breaks: Breaks, hibernate: bool, what: &str) -> u64 {
    let n = seeds();
    let mut first = None;
    let mut caught = 0u64;
    for seed in 0..n {
        let o = simulate(seed, breaks, hibernate, Duration::from_secs(20));
        let why = if double_ownership(&o.claims).is_some() {
            Some("two owners")
        } else if o.lost_ack.is_some() {
            Some("an acknowledged write lost")
        } else {
            None
        };
        if let Some(why) = why {
            caught += 1;
            first.get_or_insert((seed, why));
        }
    }
    eprintln!("{what}: caught on {caught} of {n} seeds, first {first:?}");
    caught
}
