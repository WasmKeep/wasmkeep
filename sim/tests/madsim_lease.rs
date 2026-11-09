//! `wasmkeep_cluster::lease` under madsim: three nodes with skewed clocks, one
//! bucket, seeded faults. Built only with `RUSTFLAGS="--cfg madsim"`:
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as StdInstant};

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Clock, LeaseConfig, LeaseError, Leases};
use wasmkeep_sim::check::{double_ownership, stop};
use wasmkeep_sim::driver::{Claim, Decision, Env, Observer, SkewedClock, run_node};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Store, bucket_node};

const KEY: &str = "leases/t/c/a";
/// Global time starts here, so negative skews never underflow.
const EPOCH_MS: u64 = 1_000_000;

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

struct Run {
    claims: Vec<Claim>,
    /// Leases given up (release decisions taken).
    releases: u64,
    /// Who released and when it began (global ms).
    stops: Vec<(u32, u64)>,
}

/// Records claims; with `release_percent` > 0, a holder gives the lease up
/// on that share of its ticks (half the time with a renewal in flight).
struct Recorder {
    claims: Arc<Mutex<Vec<Claim>>>,
    releases: Arc<Mutex<u64>>,
    stops: Arc<Mutex<Vec<(u32, u64)>>>,
    rng: Rng,
    release_percent: u64,
    /// Give up without releasing instead (the control).
    abandon: bool,
}

impl Observer for Recorder {
    fn decide(&mut self) -> Decision {
        if !self.rng.chance(self.release_percent) {
            return Decision::Keep;
        }
        *self.releases.lock().unwrap() += 1;
        if self.abandon {
            Decision::Abandon
        } else if self.rng.chance(50) {
            Decision::ReleaseWhileRenewing
        } else {
            Decision::Release
        }
    }

    fn claim(&mut self, claim: Claim) {
        self.claims.lock().unwrap().push(claim);
    }

    fn stop(&mut self, node: u32, at_ms: u64) {
        self.stops.lock().unwrap().push((node, at_ms));
        stop(&mut self.claims.lock().unwrap(), node, at_ms);
    }
}

/// Whether holders give the lease up, and how.
#[derive(Clone, Copy)]
enum Releases {
    Never,
    /// Release on this share of ticks (in percent).
    Release(u64),
    /// Stop and go away without releasing, as often (the control).
    Abandon(u64),
}

impl Releases {
    fn percent(self) -> u64 {
        match self {
            Self::Never => 0,
            Self::Release(p) | Self::Abandon(p) => p,
        }
    }
}

/// One seeded run of `duration`, nodes' clocks skewed by up to `max_skew_ms`.
/// `bucket_outage` adds periods when no node can reach the bucket;
/// `mode` makes holders give the lease up now and then.
fn simulate(
    seed: u64,
    config: LeaseConfig,
    max_skew_ms: u64,
    bucket_outage: bool,
    mode: Releases,
    duration: Duration,
) -> Run {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let claims: Arc<Mutex<Vec<Claim>>> = Arc::new(Mutex::new(Vec::new()));
    let out = Arc::clone(&claims);
    let releases: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let releases_out = Arc::clone(&releases);
    let stops: Arc<Mutex<Vec<(u32, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let stops_out = Arc::clone(&stops);
    rt.block_on(async move {
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let mut rng = Rng::new(seed ^ 0x5EED_CAFE);
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Store::default();
                move || bucket_node(Arc::clone(&store))
            })
            .build();
        let mut nodes = Vec::new();
        for i in 1..=3u32 {
            let span = max_skew_ms * 2 + 1;
            let skew_ms =
                i64::try_from(rng.below(span)).unwrap() - i64::try_from(max_skew_ms).unwrap();
            let claims = Arc::clone(&claims);
            let releases = Arc::clone(&releases);
            let stops = Arc::clone(&stops);
            // Not drawn from `rng`: existing seeds keep their fault schedules.
            let node_seed = seed ^ (u64::from(i) << 48) ^ 0x0E1E_A5E0;
            let node = handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || {
                    let mut recorder = Recorder {
                        claims: Arc::clone(&claims),
                        releases: Arc::clone(&releases),
                        stops: Arc::clone(&stops),
                        rng: Rng::new(node_seed),
                        release_percent: mode.percent(),
                        abandon: matches!(mode, Releases::Abandon(_)),
                    };
                    async move {
                        let bucket = EpBucket::connect().await;
                        let clock = SkewedClock {
                            global: move || env.now(),
                            skew_ms,
                        };
                        let leases = Leases::new(bucket, clock, format!("n{i}"), config);
                        run_node(i, KEY, &leases, &env, 100, &mut recorder).await;
                    }
                })
                .build();
            nodes.push(node.id());
        }

        let net = NetSim::current();
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        let mut outage_until = 0;
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            let i = usize::try_from(rng.below(3)).unwrap();
            match rng.below(20) {
                // Hold this node's traffic to the bucket, then deliver it late.
                0..=4 => {
                    net.clog_link(nodes[i], bucket.id());
                    net.clog_link(bucket.id(), nodes[i]);
                    held.push((i, at + 200 + rng.below(2_300)));
                }
                // Crash it; it restarts later with no memory (and must
                // acquire a new generation).
                5 if !crashed.iter().any(|(n, _)| *n == i) => {
                    handle.kill(nodes[i]);
                    crashed.push((i, at + 300 + rng.below(1_500)));
                }
                // The bucket is unreachable from everyone for a while.
                6 if bucket_outage && outage_until == 0 => {
                    net.clog_node(bucket.id());
                    outage_until = at + 500 + rng.below(3_000);
                }
                _ => {}
            }
            if outage_until != 0 && at >= outage_until {
                net.unclog_node(bucket.id());
                outage_until = 0;
            }
            held.retain(|(n, t)| {
                if *t <= at {
                    net.unclog_link(nodes[*n], bucket.id());
                    net.unclog_link(bucket.id(), nodes[*n]);
                    false
                } else {
                    true
                }
            });
            crashed.retain(|(n, t)| {
                if *t <= at {
                    handle.restart(nodes[*n]);
                    false
                } else {
                    true
                }
            });
        }
    });
    let claims = out.lock().unwrap().clone();
    let releases = *releases_out.lock().unwrap();
    let stops = stops_out.lock().unwrap().clone();
    Run {
        claims,
        releases,
        stops,
    }
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

/// ttl 1 s, grace 500 ms, safety 200 ms: tolerates 350 ms of skew.
fn safe() -> LeaseConfig {
    LeaseConfig::new(1_000, 500, 200, 300).unwrap()
}

#[test]
fn a_stale_routing_hint_cannot_authorize_the_previous_owner() {
    async fn connect(
        node: &'static str,
        env: SimEnv,
        skew_ms: i64,
    ) -> Leases<EpBucket, impl Clock> {
        Leases::new(
            EpBucket::connect().await,
            SkewedClock {
                global: move || env.now(),
                skew_ms,
            },
            node,
            safe(),
        )
    }

    let rt = Runtime::with_seed_and_config(87, madsim::Config::default());
    rt.block_on(async move {
        let handle = Handle::current();
        let env = SimEnv(Instant::now());
        let store = Store::default();
        let _bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init({
                let store = Arc::clone(&store);
                move || bucket_node(Arc::clone(&store))
            })
            .build();
        let a = handle
            .create_node()
            .name("a")
            .ip("10.0.1.1".parse().unwrap())
            .build();
        let b = handle
            .create_node()
            .name("b")
            .ip("10.0.1.2".parse().unwrap())
            .build();
        let router = handle
            .create_node()
            .name("router")
            .ip("10.0.1.3".parse().unwrap())
            .build();

        let previous = a
            .spawn(async move { connect("a", env, 300).await.acquire(KEY).await.unwrap() })
            .await
            .unwrap();
        let stored_before_hint = store.lock().unwrap().clone();
        let delayed_hint = router
            .spawn(async move {
                connect("router", env, 0)
                    .await
                    .other_owner(KEY)
                    .await
                    .unwrap()
            })
            .await
            .unwrap();
        assert_eq!(delayed_hint.as_deref(), Some("a"));
        assert_eq!(*store.lock().unwrap(), stored_before_hint);

        // The router's request is delayed past expiry and grace, including
        // the full clock-skew allowance. No node renews the old grant.
        sleep(Duration::from_secs(3)).await;
        let current = b
            .spawn(async move { connect("b", env, -300).await.acquire(KEY).await.unwrap() })
            .await
            .unwrap();
        assert_eq!(current.generation, previous.generation + 1);
        let after_takeover = store.lock().unwrap().clone();

        // Deliver the stale hint to its original target. Routing to a does
        // not turn that old grant into permission to execute another event.
        a.spawn(async move {
            assert_eq!(delayed_hint.as_deref(), Some("a"));
            let leases = connect("a", env, 300).await;
            assert!(!previous.is_valid(leases.clock().now_ms()));
            assert_eq!(leases.renew(&previous).await, Err(LeaseError::Lost));
            assert!(matches!(
                leases.acquire(KEY).await,
                Err(LeaseError::HeldBy { owner, .. }) if owner == "b"
            ));
            assert_eq!(leases.other_owner(KEY).await.unwrap().as_deref(), Some("b"));
            leases.release(&previous).await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(*store.lock().unwrap(), after_takeover);
        b.spawn(async move {
            let leases = connect("b", env, -300).await;
            let renewed = leases.renew(&current).await.unwrap();
            assert_eq!(renewed.generation, current.generation);
            assert!(renewed.is_valid(leases.clock().now_ms()));
        })
        .await
        .unwrap();
    });
}

#[test]
fn never_two_owners_with_skewed_clocks() {
    let started = StdInstant::now();
    let mut handovers = 0;
    for seed in 0..seeds() {
        let run = simulate(
            seed,
            safe(),
            300,
            false,
            Releases::Never,
            Duration::from_secs(30),
        );
        if let Some(v) = double_ownership(&run.claims) {
            panic!("seed {seed}: two owners: {v:?}");
        }
        handovers += run.claims.iter().map(|c| c.generation).max().unwrap_or(0);
    }
    assert!(
        handovers > seeds() * 3,
        "the lease rarely changed hands ({handovers})"
    );
    eprintln!(
        "lease: {} seeds x 30 s, skew +-300 ms: {handovers} generations, {:?}",
        seeds(),
        started.elapsed()
    );
}

#[test]
fn never_two_owners_through_bucket_outages() {
    for seed in 0..seeds() {
        let run = simulate(
            seed,
            safe(),
            300,
            true,
            Releases::Never,
            Duration::from_secs(30),
        );
        if let Some(v) = double_ownership(&run.claims) {
            panic!("seed {seed}: two owners: {v:?}");
        }
    }
}

/// For each time a node gave the lease up: how long until another node
/// owned the actor (the node that gave it up stays away meanwhile).
fn handover_gaps(run: &Run) -> Vec<u64> {
    run.stops
        .iter()
        .filter_map(|&(node, at)| {
            run.claims
                .iter()
                .filter(|c| c.node != node && c.from_ms >= at)
                .map(|c| c.from_ms - at)
                .min()
        })
        .collect()
}

fn median(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v.get(v.len() / 2).copied().unwrap_or(0)
}

#[test]
fn never_two_owners_when_owners_release() {
    // Holders give the lease up (hibernation, shutdown) on 3% of ticks, half
    // of the time with a renewal still in flight, on top of skew, late
    // delivery, crashes and bucket outages.
    let (mut releases, mut gaps, mut control) = (0, Vec::new(), Vec::new());
    for seed in 0..seeds() {
        let run = simulate(
            seed,
            safe(),
            300,
            true,
            Releases::Release(3),
            Duration::from_secs(30),
        );
        if let Some(v) = double_ownership(&run.claims) {
            panic!("seed {seed}: two owners: {v:?}");
        }
        releases += run.releases;
        gaps.extend(handover_gaps(&run));
        // The same seed with nodes going away without releasing.
        let run = simulate(
            seed,
            safe(),
            300,
            true,
            Releases::Abandon(3),
            Duration::from_secs(30),
        );
        control.extend(handover_gaps(&run));
    }
    assert!(releases > seeds(), "few releases ({releases})");
    let (with, without) = (median(gaps), median(control));
    eprintln!(
        "lease with releases: {} seeds x 30 s, {releases} releases; median handover {with} ms, \
         {without} ms when nodes go away without releasing",
        seeds()
    );
    // A release that did nothing would leave the next owner waiting for the
    // lease to run out, like the control.
    assert!(
        with * 3 < without * 2,
        "releasing does not speed handover up: {with} ms against {without} ms"
    );
}

#[test]
fn a_config_that_ignores_skew_is_caught() {
    // grace 0, safety 0 with clocks 300 ms apart: unsafe by construction
    // (LeaseConfig::new would refuse it).
    assert!(LeaseConfig::new(1_000, 0, 0, 300).is_err());
    let unsafe_config = LeaseConfig::new_unchecked(1_000, 0, 0, 300);
    let found = (0..seeds()).find(|&s| {
        double_ownership(
            &simulate(
                s,
                unsafe_config,
                300,
                false,
                Releases::Never,
                Duration::from_secs(30),
            )
            .claims,
        )
        .is_some()
    });
    let seed = found.expect("the unsafe config was not caught within the seed budget");
    eprintln!("lease: unsafe config caught at seed {seed}");
    let a = simulate(
        seed,
        unsafe_config,
        300,
        false,
        Releases::Never,
        Duration::from_secs(30),
    )
    .claims;
    let b = simulate(
        seed,
        unsafe_config,
        300,
        false,
        Releases::Never,
        Duration::from_secs(30),
    )
    .claims;
    assert_eq!(a, b, "seed {seed} did not reproduce");
}
