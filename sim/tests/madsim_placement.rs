//! ADR 0041 P3: real pure policy, modelled admission/hops/heartbeats, real
//! node-lease acquisition. No production routing is wired by these tests.
//! The eight numbered oracles below retain the ADR's exact bounds. Directed
//! counterexamples are expected failures of those *claims*, not relaxed bounds.
//! RUSTFLAGS='--cfg madsim' cargo test -j 4 -p wasmkeep-sim
//!   --test madsim_placement --target-dir target/madsim
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use madsim::{
    runtime::Runtime,
    time::{Instant, sleep},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use wasmkeep_cluster::{
    lease::{Bucket, BucketError, Clock, LeaseConfig, Version},
    membership::{DrainState, Load},
    node_lease::{NodeLease, Owners, operator_fence},
    placement::{Budget, Decision, Local, Peer, RoomLoad, decide},
};
use wasmkeep_sim::{check::double_ownership, driver::Claim, rng::Rng};

const PERIOD: Duration = Duration::from_secs(3);
const HORIZON_MS: u64 = 9000;
const IDLE_MS: u64 = 6000;
const NAMES: [&str; 4] = ["a", "b", "c", "joining"];
fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}
fn load(actors: u64, capacity: u64) -> RoomLoad {
    RoomLoad {
        load: Load {
            actors,
            ..Load::default()
        },
        actor_room: Some(capacity.saturating_sub(actors)),
        socket_room: Some(100),
    }
}
fn fill(l: RoomLoad) -> u64 {
    let capacity = l.load.actors + l.actor_room.unwrap();
    (l.load.actors * 1000).checked_div(capacity).unwrap_or(1000)
}
fn peer<'a>(node: &'a str, l: RoomLoad, heartbeat: &'a str, age: Duration) -> Peer<'a> {
    Peer {
        node,
        app: "app",
        live: true,
        drain: Some(DrainState::Serving),
        drain_requested: false,
        suspected: false,
        reachable: true,
        place: true,
        load: Some(l),
        heartbeat,
        age,
        backoff_remaining: Duration::ZERO,
    }
}
fn local(node: &str, l: RoomLoad) -> Local<'_> {
    Local {
        node,
        app: "app",
        place: true,
        load: l,
    }
}

/// P4's hop/admission state machine is deliberately only a model here.
/// Counters are derived from transitions, not assumed by the property oracle.
#[derive(Default)]
struct Event {
    placed: u8,
    at_receiver: bool,
    placement_off: bool,
    receiver_forwards: u8,
}
impl Event {
    fn choose<'a>(
        &mut self,
        me: Local<'_>,
        peers: &[Peer<'a>],
        key: &str,
        budget: &Budget,
    ) -> Option<&'a str> {
        if self.at_receiver || self.placement_off {
            return None;
        }
        if let Decision::Forward { node, .. } = decide(me, peers, key, PERIOD, budget) {
            self.placed += 1;
            self.receiver_forwards += u8::from(self.at_receiver);
            Some(node)
        } else {
            None
        }
    }
    fn receive(&mut self) {
        self.at_receiver = true;
    }
    fn fallback(&mut self) {
        self.at_receiver = false;
        self.placement_off = true;
    }
    fn check(&self, evidence: &mut Evidence) {
        evidence.hops(self.placed, self.receiver_forwards);
    }
}

#[derive(Clone, Debug)]
struct Sent {
    from: usize,
    to: usize,
    at: u64,
    sender_fill: u64,
    seen_fill: u64,
    age_ms: u64,
}
#[derive(Default, Debug)]
struct Evidence {
    violations: Vec<(u8, String)>,
    sent: Vec<Sent>,
    fullest: [u64; 4],
    direct: [u64; 4],
    claims: BTreeMap<usize, Vec<Claim>>,
    resident_nodes: BTreeMap<usize, usize>,
    woke: u64,
    slept: u64,
    stale: u64,
    delayed: u64,
}
impl Evidence {
    fn check(&mut self, number: u8, ok: bool, detail: impl FnOnce() -> String) {
        if !ok {
            self.violations.push((number, detail()));
        }
    }
    // 1. One hop: placement count AND any subsequent forward, separately.
    fn hops(&mut self, placed: u8, forwarded_after_placement: u8) {
        self.check(1, placed <= 1 && forwarded_after_placement == 0, || {
            "placed/forwarded twice".into()
        });
    }
    // 2. Exactly the node-lease simulation's ownership invariant, per actor.
    fn ownership(&mut self) {
        let bad = self
            .claims
            .iter()
            .find_map(|(key, c)| double_ownership(c).map(|d| (*key, d)));
        self.check(2, bad.is_none(), || format!("overlapping holders: {bad:?}"));
    }
    // 3. No resident changes node. Hibernation/failure removes its entry;
    // refusal occurs before activation and therefore has no resident entry.
    fn resident(&mut self, actor: usize, node: usize) {
        let previous = self.resident_nodes.insert(actor, node);
        self.check(3, previous.is_none_or(|old| old == node), || {
            format!("resident {actor} moved")
        });
    }
    // 4. Compare to the LAST PUBLISHED fill, not the estimated/actual fill.
    fn direction(&mut self, s: Sent) {
        self.check(
            4,
            s.sender_fill >= 200 && s.seen_fill * 5 < s.sender_fill * 4,
            || format!("wrong direction: {s:?}"),
        );
        // Freshness is additionally checked, not silently substituted for P4.
        assert!(s.age_ms < HORIZON_MS, "a stale peer was selected: {s:?}");
        self.fullest[s.to] = self.fullest[s.to].max(s.sender_fill);
        // 6. No opposite placements within three periods, including repeats
        // across distinct heartbeat identities.
        let reversed = self
            .sent
            .iter()
            .any(|p| p.from == s.to && p.to == s.from && s.at - p.at < HORIZON_MS);
        self.check(6, !reversed, || {
            format!("opposite placements within 9s: {s:?}")
        });
        self.sent.push(s);
    }
    // 5. At arrival (after adding the new resident), exact integer count at
    // 80% of the fullest sender, N-2, plus direct wakes. No extra rounding slack.
    fn arrival(&mut self, node: usize, residents: u64, capacity: u64, n: u64) {
        let bound = self.fullest[node] * 4 * capacity / 5000 + (n - 2) + self.direct[node];
        self.check(5, residents <= bound, || {
            format!("receiver {node}: {residents} > {bound}")
        });
    }
    // 7a. The actual deadline, not eventual convergence.
    fn filled(&mut self, joined_at: u64, now: u64, joining: u64, lightest: u64) {
        self.check(
            7,
            now - joined_at <= 2 * IDLE_MS + 10 * 3000 && joining * 5 >= lightest * 4,
            || {
                format!(
                    "join fill {joining}, lightest {lightest}, elapsed {}",
                    now - joined_at
                )
            },
        );
    }
    // 7b. Once filled, every pair inside the band means no placements.
    fn quiet(&mut self, fills: &[u64], placements: usize) {
        let in_band = fills.iter().max().unwrap() * 4 <= fills.iter().min().unwrap() * 5;
        self.check(7, !in_band || placements == 0, || {
            "placement inside the band".into()
        });
    }
    // 8. Idle cluster silent, checked against actual local loads.
    fn idle(&mut self, fills: &[u64], placements: usize) {
        self.check(
            8,
            !fills.iter().all(|s| *s < 200) || placements == 0,
            || "idle cluster forwarded".into(),
        );
    }
}

#[derive(Clone)]
struct Seen {
    load: RoomLoad,
    heartbeat: String,
    first_seen: u64,
}

/// A seeded event model: three loaded nodes, a fourth joining, actors sleeping
/// and waking on their ingress node; per-sender delayed/dropped observations.
/// Fixed resident population; no unmodelled CPU load or wall-clock sleeps.
async fn filling(seed: u64) -> Evidence {
    let mut rng = Rng::new(seed ^ 0x41);
    let mut out = Evidence::default();
    let mut counts = [60_u64, 60, 60, 0];
    let mut actors: Vec<(usize, u64)> = (0..180).map(|i| (i % 3, rng.below(IDLE_MS))).collect();
    for (i, (n, _)) in actors.iter().enumerate() {
        out.resident(i, *n);
    }
    let mut budgets: [Budget; 4] = std::array::from_fn(|_| Budget::default());
    let mut seen: [[Seen; 4]; 4] = std::array::from_fn(|_| {
        std::array::from_fn(|p| Seen {
            load: load(counts[p], 100),
            heartbeat: "initial".into(),
            first_seen: 0,
        })
    });
    let mut deliveries = Vec::<(u64, usize, usize, RoomLoad, String)>::new();
    let start = Instant::now();
    let mut reached = None;
    for tick in 0..=600 {
        sleep(Duration::from_millis(100)).await;
        let now = u64::try_from(start.elapsed().as_millis()).unwrap();
        if tick % 30 == 0 {
            for (s, _) in NAMES.iter().enumerate() {
                for (p, count) in counts.iter().enumerate() {
                    // A deliberate outage after the filling deadline: all
                    // cached loads become stale; repeated listings do not
                    // advance first_seen or replenish credit.
                    if (450..570).contains(&tick) || rng.below(20) == 0 {
                        continue;
                    }
                    let delay = rng.below(201);
                    deliveries.push((now + delay, s, p, load(*count, 100), format!("hb{tick}")));
                    out.delayed += u64::from(delay > 0);
                }
            }
        }
        deliveries.retain(|(at, s, p, l, h)| {
            if *at > now {
                return true;
            }
            seen[*s][*p] = Seen {
                load: *l,
                heartbeat: h.clone(),
                first_seen: now,
            };
            false
        });
        for (actor, state) in actors.iter_mut().enumerate() {
            if now < state.1 {
                continue;
            }
            let previous = state.0;
            counts[previous] -= 1;
            out.resident_nodes.remove(&actor); // hibernated, before wake-up
            out.slept += 1;
            let source = actor % 3;
            let peers: Vec<_> = (0..4)
                .map(|p| {
                    peer(
                        NAMES[p],
                        seen[source][p].load,
                        &seen[source][p].heartbeat,
                        Duration::from_millis(now - seen[source][p].first_seen),
                    )
                })
                .collect();
            out.stale += peers.iter().filter(|p| p.age >= PERIOD * 3).count() as u64;
            let me = local(NAMES[source], load(counts[source], 100));
            let mut event = Event::default();
            let decision = event.choose(
                me,
                &peers,
                &format!("tenant/class/{actor}"),
                &budgets[source],
            );
            let before = out.sent.len();
            let fills_before: Vec<_> = counts.iter().map(|c| c * 10).collect();
            let destination = if let Some(node) = decision {
                let dest = NAMES.iter().position(|n| *n == node).unwrap();
                budgets[source].charge(node, &seen[source][dest].heartbeat);
                out.direction(Sent {
                    from: source,
                    to: dest,
                    at: now,
                    sender_fill: fill(me.load),
                    seen_fill: fill(seen[source][dest].load),
                    age_ms: now - seen[source][dest].first_seen,
                });
                event.receive();
                assert!(
                    event
                        .choose(
                            local(node, load(counts[dest], 100)),
                            &peers,
                            &format!("tenant/class/{actor}"),
                            &budgets[dest]
                        )
                        .is_none()
                );
                event.check(&mut out);
                counts[dest] += 1;
                out.arrival(dest, counts[dest], 100, 4);
                dest
            } else {
                counts[source] += 1;
                out.direct[source] += 1;
                event.check(&mut out);
                source
            };
            out.resident(actor, destination);
            out.woke += 1;
            *state = (destination, now + IDLE_MS + rng.below(1000));
            let fills: Vec<_> = counts.iter().map(|c| c * 10).collect();
            out.idle(&fills, out.sent.len() - before);
            if reached.is_some() {
                out.quiet(&fills_before, out.sent.len() - before);
            }
        }
        let lightest = *counts[..3].iter().min().unwrap() * 10;
        if reached.is_none() && counts[3] * 10 * 5 >= lightest * 4 {
            reached = Some((now, counts[3] * 10, lightest));
        }
        if tick == 419 {
            let (at, joining, lightest) = reached.unwrap_or((now, counts[3] * 10, lightest));
            out.filled(0, at, joining, lightest);
        }
    }
    assert!(out.woke > 1000 && out.slept > 1000 && out.delayed > 0 && out.stale > 0);
    out
}

#[test]
fn joining_node_fills_but_cached_loads_can_flap_and_delay_quiet() {
    let mut counterexamples = [0; 8];
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let out = filling(seed).await;
            // Convergence, direction and overshoot are checked without
            // relaxation. As in the directed counterexamples, P6 and the
            // quiet half of P7 can fail: actual loads are
            // in-band while the latest heartbeat still advertises an empty
            // joiner. Retain this exact P7 failure, rather than checking only
            // published fills (which would weaken the ADR).
            assert!(
                out.violations
                    .iter()
                    .all(|(p, why)| *p == 6 || (*p == 7 && why == "placement inside the band")),
                "seed {seed}: {:?}",
                out.violations
            );
            assert!(!out.sent.is_empty(), "joining node must receive placements");
            for (property, _) in &out.violations {
                counterexamples[usize::from(*property - 1)] += 1;
            }
        });
    }
    eprintln!(
        "ADR counterexamples by property over {} seeds: {counterexamples:?}",
        seeds()
    );
    if seeds() >= 200 {
        assert!(counterexamples[5] > 0 && counterexamples[6] > 0);
    }
}

// Same arrival oracle, budget deliberately forgotten before each decision.
fn herd(no_budget: bool) -> Evidence {
    let mut out = Evidence::default();
    let mut budgets: [Budget; 3] = std::array::from_fn(|_| Budget::default());
    let mut residents = 0;
    let peers = [
        peer("joining", load(0, 100), "h1", Duration::ZERO),
        Peer {
            reachable: false,
            ..peer("a", load(60, 100), "h1", Duration::ZERO)
        },
        Peer {
            reachable: false,
            ..peer("b", load(60, 100), "h1", Duration::ZERO)
        },
        Peer {
            reachable: false,
            ..peer("c", load(60, 100), "h1", Duration::ZERO)
        },
    ];
    for round in 0..60 {
        for source in 0..3 {
            if no_budget {
                budgets[source] = Budget::default();
            }
            if let Decision::Forward { node, .. } = decide(
                local(NAMES[source], load(60, 100)),
                &peers,
                &format!("herd/{round}/{source}"),
                PERIOD,
                &budgets[source],
            ) {
                budgets[source].charge(node, "h1");
                out.direction(Sent {
                    from: source,
                    to: 3,
                    at: round,
                    sender_fill: 600,
                    seen_fill: 0,
                    age_ms: 0,
                });
                residents += 1;
                out.arrival(3, residents, 100, 4);
            }
        }
    }
    out
}
#[test]
fn removing_budget_is_caught_by_property_five() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let correct = herd(false);
            assert_eq!(correct.sent.len(), 48);
            assert!(correct.violations.is_empty());
            assert!(herd(true).violations.iter().any(|(p, _)| *p == 5));
        });
    }
}

/// Minimal conditional bucket; yielding before reads/writes lets madsim race
/// the real Owners CAS. Stored versions are unique across replacements.
#[derive(Clone, Default)]
struct Mem(Objects);
type Objects = Arc<Mutex<BTreeMap<String, (Vec<u8>, u64)>>>;
impl Bucket for Mem {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        madsim::task::yield_now().await;
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, v)| (b.clone(), Version(v.to_string()))))
    }
    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        madsim::task::yield_now().await;
        let mut map = self.0.lock().unwrap();
        if map.contains_key(key) {
            return Err(BucketError::Conflict);
        }
        map.insert(key.into(), (body, 1));
        Ok(Version("1".into()))
    }
    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        madsim::task::yield_now().await;
        let mut map = self.0.lock().unwrap();
        let Some((_, version)) = map.get(key) else {
            return Err(BucketError::Conflict);
        };
        if version.to_string() != expected.0 {
            return Err(BucketError::Conflict);
        }
        let next = version + 1;
        map.insert(key.into(), (body, next));
        Ok(Version(next.to_string()))
    }
}
#[derive(Clone, Copy)]
struct SimClock(Instant);
impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        1_000_000 + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// P2/P3 does not test real HTTP. This admission model covers the four rows
/// of section 6, and the real ownership protocol handles races and failover.
#[test]
fn one_hop_one_holder_residents_stay_and_a_failed_node_returns() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let clock = SimClock(Instant::now());
            let bucket = Mem::default();
            let cfg = LeaseConfig::new(9000, 1000, 1000, 0).unwrap();
            let mut leases = Vec::new();
            let mut owners = Vec::new();
            for name in NAMES {
                let mut lease =
                    NodeLease::new(bucket.clone(), clock, format!("nodes/{name}"), "first", cfg);
                lease.start(None).await.unwrap();
                leases.push(lease);
                owners.push(Owners::new(
                    bucket.clone(),
                    clock,
                    name,
                    "first",
                    "nodes/",
                    cfg,
                ));
            }
            let peers = [peer("joining", load(0, 100), "h1", Duration::ZERO)];
            let mut evidence = Evidence::default();
            // Success, explicit refusal, never-read, ambiguous-write; access
            // answers are final and do not activate or set an owner hint.
            for outcome in 0..5 {
                let key = format!("owners/app/{outcome}");
                let mut event = Event::default();
                assert_eq!(
                    event.choose(local("a", load(60, 100)), &peers, &key, &Budget::default()),
                    Some("joining")
                );
                event.receive();
                // Even if the receiver is fuller than another peer, a signed
                // placed hop is refused/accepted here, never placed again.
                let tempting = [peer("a", load(0, 100), "h1", Duration::ZERO)];
                assert!(
                    event
                        .choose(
                            local("joining", load(100, 100)),
                            &tempting,
                            &key,
                            &Budget::default()
                        )
                        .is_none()
                );
                event.check(&mut evidence);
                let receiver_runs = outcome == 0 || outcome == 3;
                let fallback = outcome == 1 || outcome == 2;
                let target = if fallback {
                    event.fallback();
                    assert!(
                        event
                            .choose(local("a", load(60, 100)), &peers, &key, &Budget::default())
                            .is_none()
                    );
                    event.check(&mut evidence);
                    0
                } else {
                    3
                };
                if outcome == 4 {
                    continue;
                } // access answer, no fallback/hint
                assert!(!(receiver_runs && fallback));
                let hold = owners[target]
                    .acquire(&key, || leases[target].is_valid(), async || Ok(1))
                    .await
                    .unwrap();
                evidence.resident(outcome, target);
                let began = clock.now_ms();
                // The other node cannot acquire an already resident actor.
                assert!(
                    owners[1]
                        .acquire(&key, || leases[1].is_valid(), async || Ok(1))
                        .await
                        .is_err()
                );
                sleep(Duration::from_millis(1)).await;
                evidence.claims.entry(outcome).or_default().push(Claim {
                    node: target as u32,
                    instance: 0,
                    generation: hold.generation,
                    from_ms: began,
                    until_ms: clock.now_ms(),
                });
                evidence.resident(outcome, target);
                evidence.resident_nodes.remove(&outcome); // hibernate before release
                owners[target].release(&hold).await;
                let next = owners[1]
                    .acquire(&key, || leases[1].is_valid(), async || Ok(1))
                    .await
                    .unwrap();
                assert!(next.generation > hold.generation);
                evidence.resident(outcome, 1);
                evidence.claims.entry(outcome).or_default().push(Claim {
                    node: 1,
                    instance: 0,
                    generation: next.generation,
                    from_ms: clock.now_ms(),
                    until_ms: clock.now_ms() + 1,
                });
            }
            // Concurrent placements race through real conditional ownership.
            let key = "owners/app/race";
            let a = &owners[0];
            let b = &owners[3];
            let (left, right) = join(
                a.acquire(key, || leases[0].is_valid(), async || Ok(1)),
                b.acquire(key, || leases[3].is_valid(), async || Ok(1)),
            )
            .await;
            assert_ne!(left.is_ok(), right.is_ok());
            let (dead, old) = if let Ok(h) = left {
                (0, h)
            } else {
                (3, right.unwrap())
            };
            evidence.resident(99, dead);
            evidence.claims.entry(99).or_default().push(Claim {
                node: dead as u32,
                instance: 0,
                generation: old.generation,
                from_ms: clock.now_ms(),
                until_ms: clock.now_ms() + 1,
            });
            sleep(Duration::from_millis(1)).await;
            evidence.resident_nodes.remove(&99); // process failure ends acting
            operator_fence(&bucket, &clock, &format!("nodes/{}", NAMES[dead]))
                .await
                .unwrap();
            sleep(Duration::from_millis(10_001)).await;
            leases[1].renew().await.unwrap();
            let takeover = owners[1]
                .acquire(key, || leases[1].is_valid(), async || Ok(1))
                .await
                .unwrap();
            assert!(takeover.generation > old.generation);
            evidence.resident(99, 1);
            evidence.claims.entry(99).or_default().push(Claim {
                node: 1,
                instance: 0,
                generation: takeover.generation,
                from_ms: clock.now_ms(),
                until_ms: clock.now_ms() + 1000,
            });
            let node_key = format!("nodes/{}", NAMES[dead]);
            let version = bucket.get(&node_key).await.unwrap().unwrap().1;
            let mut returning = NodeLease::new(bucket.clone(), clock, node_key, "returned", cfg);
            returning.start(Some(&version)).await.unwrap();
            let returning_owners = Owners::new(
                bucket.clone(),
                clock,
                NAMES[dead],
                "returned",
                "nodes/",
                cfg,
            );
            assert!(
                returning_owners
                    .acquire(key, || returning.is_valid(), async || Ok(1))
                    .await
                    .is_err()
            );
            let returned_peers = [peer(NAMES[dead], load(0, 100), "returned", Duration::ZERO)];
            assert!(matches!(
                decide(
                    local("b", load(60, 100)),
                    &returned_peers,
                    "new/actor",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Forward { .. }
            ));
            let fresh = returning_owners
                .acquire("owners/app/new", || returning.is_valid(), async || Ok(1))
                .await
                .unwrap();
            assert_eq!(fresh.generation, 1);
            evidence.ownership();
            assert!(
                evidence.violations.is_empty(),
                "seed {seed}: {:?}",
                evidence.violations
            );
        });
    }
}

#[test]
fn idle_cluster_is_silent() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut out = Evidence::default();
            let mut rng = Rng::new(seed);
            let loads: Vec<_> = (0..4).map(|_| load(rng.below(200), 1000)).collect();
            let peers: Vec<_> = (0..4)
                .map(|p| peer(NAMES[p], loads[p], "h", Duration::ZERO))
                .collect();
            for s in 0..4 {
                let d = decide(
                    local(NAMES[s], loads[s]),
                    &peers,
                    "idle/actor",
                    PERIOD,
                    &Budget::default(),
                );
                out.idle(
                    &loads.iter().map(|l| fill(*l)).collect::<Vec<_>>(),
                    usize::from(matches!(d, Decision::Forward { .. })),
                );
            }
            assert!(out.violations.is_empty());
        });
    }
}

#[test]
fn adr_minimum_one_budget_counterexample_to_five_and_six() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut out = Evidence::default();
            let p = [peer("b", load(0, 1), "h", Duration::ZERO)];
            let d = decide(
                local("a", load(1, 5)),
                &p,
                "tiny",
                PERIOD,
                &Budget::default(),
            );
            assert!(matches!(d, Decision::Forward { .. }));
            out.direction(Sent {
                from: 0,
                to: 1,
                at: 0,
                sender_fill: 200,
                seen_fill: 0,
                age_ms: 0,
            });
            out.arrival(1, 1, 1, 2); // floor(.8*.2*1) + (2-2) = 0
            sleep(Duration::from_millis(1)).await;
            // Steady arrivals, no departure required: b is now 1000 permille
            // and its next cold wake places back to a, still at 200 permille.
            let back = [peer("a", load(1, 5), "h", Duration::from_millis(1))];
            assert!(matches!(
                decide(
                    local("b", load(1, 1)),
                    &back,
                    "next",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Forward { .. }
            ));
            out.direction(Sent {
                from: 1,
                to: 0,
                at: 1,
                sender_fill: 1000,
                seen_fill: 200,
                age_ms: 1,
            });
            assert_eq!(
                out.violations.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
                [5, 6]
            );
        });
    }
}

#[test]
fn adr_seven_needs_an_arrival_rate_assumption() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            // Three nodes with 600 active actors of capacity 1000. A steady
            // one cold arrival/sec can provide only 42 actors by the deadline,
            // even granting the joiner EVERY arrival. Hot actors do not idle.
            let mut out = Evidence::default();
            let mut joined = 0;
            let mut budgets: [Budget; 3] = std::array::from_fn(|_| Budget::default());
            for second in 0..42 {
                sleep(Duration::from_secs(1)).await;
                let h = format!("h{}", second / 3);
                let p = [peer("joining", load(joined, 1000), &h, Duration::ZERO)];
                let s = second as usize % 3;
                if let Decision::Forward { node, .. } = decide(
                    local(NAMES[s], load(600, 1000)),
                    &p,
                    &format!("slow/{second}"),
                    PERIOD,
                    &budgets[s],
                ) {
                    budgets[s].charge(node, &h);
                    joined += 1;
                }
            }
            out.filled(0, 42_000, joined, 600);
            assert_eq!(joined, 42);
            assert_eq!(out.violations[0].0, 7); // requires at least 480
        });
    }
}

async fn join<A: std::future::Future, B: std::future::Future>(
    a: A,
    b: B,
) -> (A::Output, B::Output) {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let (mut ra, mut rb) = (None, None);
    std::future::poll_fn(|cx| {
        if ra.is_none()
            && let std::task::Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let std::task::Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        if ra.is_some() && rb.is_some() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    match (ra, rb) {
        (Some(a), Some(b)) => (a, b),
        _ => unreachable!("both finished"),
    }
}

#[test]
fn filled_cluster_with_current_in_band_loads_stays_quiet() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut evidence = Evidence::default();
            // Once the joiner reaches 80%, a steady wake/hibernate stream
            // leaves all pairs in-band. No decision may forward.
            let counts = [50, 50, 50, 40];
            evidence.filled(0, 6000, 400, 500);
            for wake in 0..120 {
                sleep(Duration::from_millis(100)).await;
                let heartbeat = format!("h{}", wake / 30);
                let peers: Vec<_> = (0..4)
                    .map(|p| {
                        peer(
                            NAMES[p],
                            load(counts[p], 100),
                            &heartbeat,
                            Duration::from_millis(wake % 30 * 100),
                        )
                    })
                    .collect();
                let sender = wake as usize % 4;
                let mut event = Event::default();
                // A resident hibernates immediately before its next wake.
                let placed = event
                    .choose(
                        local(NAMES[sender], load(counts[sender] - 1, 100)),
                        &peers,
                        &format!("steady/{wake}"),
                        &Budget::default(),
                    )
                    .is_some();
                evidence.quiet(&[500, 500, 500, 400], usize::from(placed));
                event.check(&mut evidence);
            }
            assert!(evidence.violations.is_empty());
        });
    }
}

#[test]
fn fresh_but_delayed_load_disproves_property_sevens_quiet_clause() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut evidence = Evidence::default();
            // Since its empty heartbeat, b has received 40 direct wakes.
            // Now a=50/100 and b=40/100: filled and exactly in-band.
            evidence.filled(0, 1, 400, 500);
            let peers = [peer("b", load(0, 100), "empty", Duration::from_millis(1))];
            let sent = matches!(
                decide(
                    local("a", load(50, 100)),
                    &peers,
                    "one-more",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Forward { .. }
            );
            evidence.quiet(&[500, 400], usize::from(sent));
            assert_eq!(evidence.violations.len(), 1);
            assert_eq!(evidence.violations[0].0, 7);
        });
    }
}
