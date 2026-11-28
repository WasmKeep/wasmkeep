//! ADR 0041 P3: real pure policy, modelled admission/hops/heartbeats, real
//! node-lease acquisition. No production routing is wired by these tests.
//! The eight numbered oracles below retain the ADR's exact bounds. Directed
//! mutations are expected failures of the revised properties, not relaxed bounds.
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
    placement::{Arrival, Budget, Decision, Local, Peer, RoomLoad, decide, receive},
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
        accepted_age: None,
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
    sender_fill: u64,
    seen_fill: u64,
    age_ms: u64,
}
#[derive(Default, Debug)]
struct Evidence {
    violations: Vec<(u8, String)>,
    sent: Vec<Sent>,

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

        self.sent.push(s);
    }
    // 5(a): the signed sending fill and live capacity, with no allowance.
    fn arrival(&mut self, sender_fill: u64, residents: u64, capacity: u64) {
        let bound = sender_fill * 4 * capacity / 5000;
        self.check(5, residents <= bound, || {
            format!("accepted {residents} > {bound}")
        });
    }
    // 5(b): count attempts, including refusals, under the same heartbeat.
    fn herd(&mut self, sent: u64, t_max: u64, actors: u64) {
        self.check(5, sent <= t_max.saturating_sub(actors), || {
            format!("sent {sent} > gap {}", t_max.saturating_sub(actors))
        });
    }
    // 6: independent local-clock gates on sending and accepting.
    fn no_return(&mut self, age: Option<u64>, happened: bool) {
        self.check(6, !happened || age.is_none_or(|a| a >= HORIZON_MS), || {
            format!("return at {age:?} ms")
        });
    }
    // 7(b): actual fills BEFORE admission, and accepted placements only.
    fn quiet(&mut self, fills: &[u64], accepted: usize) {
        let in_band = fills.iter().max().unwrap() * 4 <= fills.iter().min().unwrap() * 5;
        self.check(7, !in_band || accepted == 0, || {
            "accepted inside the band".into()
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
    let mut sent_at = [[None; 4]; 4];
    let mut accepted_at = [[None; 4]; 4];
    let mut backoff_until = [[0_u64; 4]; 4];
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
                    let mut pview = peer(
                        NAMES[p],
                        seen[source][p].load,
                        &seen[source][p].heartbeat,
                        Duration::from_millis(now - seen[source][p].first_seen),
                    );
                    pview.accepted_age =
                        accepted_at[source][p].map(|at| Duration::from_millis(now - at));
                    pview.backoff_remaining =
                        Duration::from_millis(backoff_until[source][p].saturating_sub(now));
                    pview
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

            let destination = if let Some(node) = decision {
                let dest = NAMES.iter().position(|n| *n == node).unwrap();
                budgets[source].charge(node, &seen[source][dest].heartbeat);
                out.direction(Sent {
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
                out.no_return(accepted_at[source][dest].map(|at| now - at), true);
                sent_at[source][dest] = Some(now);
                let accepted = receive(
                    load(counts[dest], 100),
                    fill(me.load),
                    true,
                    sent_at[dest][source].map(|at| Duration::from_millis(now - at)),
                    PERIOD,
                ) == Arrival::Accept;
                out.no_return(sent_at[dest][source].map(|at| now - at), accepted);
                out.quiet(&[fill(me.load), counts[dest] * 10], usize::from(accepted));
                if accepted {
                    accepted_at[dest][source] = Some(now);
                    counts[dest] += 1;
                    out.arrival(fill(me.load), counts[dest], 100);
                    dest
                } else {
                    backoff_until[source][dest] = now + 10_000;
                    event.fallback();
                    counts[source] += 1;
                    source
                }
            } else {
                counts[source] += 1;

                event.check(&mut out);
                source
            };
            out.resident(actor, destination);
            out.woke += 1;
            *state = (destination, now + IDLE_MS + rng.below(1000));
            let fills: Vec<_> = counts.iter().map(|c| c * 10).collect();
            out.idle(&fills, out.sent.len() - before);
        }
    }
    assert!(out.woke > 1000 && out.slept > 1000 && out.delayed > 0 && out.stale > 0);
    out
}

#[test]
fn joining_node_obeys_revised_properties() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let out = filling(seed).await;
            assert!(
                out.violations.is_empty(),
                "seed {seed}: {:?}",
                out.violations
            );
            assert!(!out.sent.is_empty());
        });
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
                    sender_fill: 600,
                    seen_fill: 0,
                    age_ms: 0,
                });
                if receive(load(residents, 100), 600, true, None, PERIOD) == Arrival::Accept {
                    residents += 1;
                    out.arrival(600, residents, 100);
                }
            }
        }
    }
    out.herd(out.sent.len() as u64, 48, 0);
    out
}
#[test]
fn removing_budget_is_caught_by_property_five() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let correct = herd(false);
            assert_eq!(correct.sent.len(), 48);
            assert!(correct.violations.is_empty());
            let mutated = herd(true);
            assert_eq!(mutated.sent.len(), 180);
            assert!(mutated.violations.iter().any(|(p, _)| *p == 5));
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
            // Over the node's lease: kept as it is, so no date ahead.
            operator_fence(
                &bucket,
                &clock,
                &format!("nodes/{}", NAMES[dead]),
                0,
                std::future::pending(),
            )
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
fn minimum_one_mutation_fails_five_b() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let peers = [peer("b", load(0, 1), "h", Duration::ZERO)];
            let decision = decide(
                local("a", load(1, 5)),
                &peers,
                "tiny",
                PERIOD,
                &Budget::default(),
            );
            assert_eq!(
                decision,
                Decision::Keep(wasmkeep_cluster::placement::Reason::Budget)
            );
            let mut correct = Evidence::default();
            correct.herd(
                u64::from(matches!(decision, Decision::Forward { .. })),
                0,
                0,
            );
            assert!(correct.violations.is_empty());
            // Restore max(1) for this zero-share schedule: the first hop passes.
            let mutated = if decision == Decision::Keep(wasmkeep_cluster::placement::Reason::Budget)
            {
                Decision::Forward {
                    node: "b",
                    reason: wasmkeep_cluster::placement::Reason::Lighter,
                }
            } else {
                decision
            };
            let mut out = Evidence::default();
            out.herd(u64::from(matches!(mutated, Decision::Forward { .. })), 0, 0);
            assert_eq!(out.violations[0].0, 5);
        });
    }
}

fn mixed_heartbeats(remove_arrival: bool) -> (Evidence, u64, u64) {
    let mut out = Evidence::default();
    let mut residents = 0;
    let mut refused = 0;
    let mut budgets = [Budget::default(), Budget::default()];
    // ADR 5(a): sender 1 sees h2 after its 24; sender 2 still sees h1.
    for (source, published, heartbeat, attempts) in
        [(0, 0, "h1", 24), (0, 24, "h2", 12), (1, 0, "h1", 24)]
    {
        let peers = [
            peer(
                "joining",
                load(published, 100),
                heartbeat,
                Duration::from_millis(1),
            ),
            Peer {
                reachable: false,
                ..peer(NAMES[1 - source], load(60, 100), "other", Duration::ZERO)
            },
        ];
        for _ in 0..attempts {
            let d = decide(
                local(NAMES[source], load(60, 100)),
                &peers,
                "mixed",
                PERIOD,
                &budgets[source],
            );
            assert!(matches!(
                d,
                Decision::Forward {
                    node: "joining",
                    ..
                }
            ));
            budgets[source].charge("joining", heartbeat);
            if remove_arrival
                || receive(load(residents, 100), 600, true, None, PERIOD) == Arrival::Accept
            {
                residents += 1;
                out.arrival(600, residents, 100);
            } else {
                refused += 1;
            }
        }
    }
    (out, residents, refused)
}

#[test]
fn arrival_mutation_fails_five_a() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            // Attempts already in flight: refusal backoff cannot retract them.
            let (correct, count, refused) = mixed_heartbeats(false);
            assert_eq!((count, refused), (48, 12));
            assert!(correct.violations.is_empty());
            let (mutated, count, _) = mixed_heartbeats(true);
            assert_eq!(count, 60);
            assert!(mutated.violations.iter().all(|(p, _)| *p == 5));
            assert_eq!(mutated.violations.len(), 12);
        });
    }
}

#[test]
fn quiet_uses_live_count_and_refusal_backs_off_for_ten_seconds() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut out = Evidence::default();
            let mut budget = Budget::default();
            let mut until: u64 = 0;
            let mut attempts = 0;
            // Fresh empty observations do not override placement backoff.
            for now in [0_u64, 1, 8999, 9999, 10_000] {
                let heartbeat = format!("h{now}");
                let mut p = peer("b", load(0, 100), &heartbeat, Duration::ZERO);
                p.backoff_remaining = Duration::from_millis(until.saturating_sub(now));
                let d = decide(local("a", load(50, 100)), &[p], "quiet", PERIOD, &budget);
                if let Decision::Forward { node, .. } = d {
                    attempts += 1;
                    budget.charge(node, &heartbeat);
                    let accepted =
                        receive(load(40, 100), 500, true, None, PERIOD) == Arrival::Accept;
                    assert!(!accepted); // 41 > 40
                    out.quiet(&[500, 400], usize::from(accepted));
                    until = now + 10_000;
                }
                assert_eq!(attempts, if now < 10_000 { 1 } else { 2 });
            }
            assert!(out.violations.is_empty());
            // Removing arrival admission makes the very same hop accepted.
            out.quiet(&[500, 400], 1);
            assert_eq!(out.violations[0].0, 7);
        });
    }
}

#[test]
fn no_return_mutation_fails_six_and_both_gates_expire_exactly() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let mut budget = Budget::default();
            let peers = [peer("b", load(0, 100), "h1", Duration::ZERO)];
            let mut b = 0;
            for _ in 0..48 {
                assert!(matches!(
                    decide(
                        local("a", load(60, 100)),
                        &peers,
                        "forward",
                        PERIOD,
                        &budget
                    ),
                    Decision::Forward { .. }
                ));
                budget.charge("b", "h1");
                assert_eq!(
                    receive(load(b, 100), 600, true, None, PERIOD),
                    Arrival::Accept
                );
                b += 1;
            }
            // A hibernates 40 residents; B now wants to send back to A=20.
            for age in [0, 1, 8999, 9000] {
                let mut p = peer("a", load(20, 100), "h2", Duration::ZERO);
                p.accepted_age = Some(Duration::from_millis(age));
                let sent = matches!(
                    decide(
                        local("b", load(b, 100)),
                        &[p],
                        "back",
                        PERIOD,
                        &Budget::default()
                    ),
                    Decision::Forward { .. }
                );
                let accepted = receive(
                    load(20, 100),
                    480,
                    true,
                    Some(Duration::from_millis(age)),
                    PERIOD,
                ) == Arrival::Accept;
                assert_eq!((sent, accepted), (age == 9000, age == 9000));
                let mut correct = Evidence::default();
                correct.no_return(Some(age), sent);
                correct.no_return(Some(age), accepted);
                assert!(correct.violations.is_empty());
                if age < 9000 {
                    // Remove both gates. The reverse hop is sent and accepted.
                    p.accepted_age = None;
                    let sent = matches!(
                        decide(
                            local("b", load(b, 100)),
                            &[p],
                            "back",
                            PERIOD,
                            &Budget::default()
                        ),
                        Decision::Forward { .. }
                    );
                    let accepted =
                        receive(load(20, 100), 480, true, None, PERIOD) == Arrival::Accept;
                    assert!(sent && accepted);
                    correct.no_return(Some(age), sent);
                    correct.no_return(Some(age), accepted);
                    assert_eq!(correct.violations.len(), 2);
                }
            }
            // Two hops cross: neither receiver has accepted yet. Sending
            // alone is sufficient to refuse the opposite in-flight hop.
            assert_eq!(
                receive(load(20, 100), 600, true, Some(Duration::ZERO), PERIOD),
                Arrival::NoReturn
            );
        });
    }
}

#[test]
fn directed_join_fills_at_the_offered_rate() {
    use wasmkeep_cluster::placement::Reason;
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            for w in [1_u64, 3, 160, 200] {
                let mut residents = 0;
                let mut budgets: [Budget; 3] = std::array::from_fn(|_| Budget::default());
                let deadline = 480_u64.div_ceil(3 * w) + 2;
                let mut reached = None;
                for period in 0..deadline {
                    // All three see this heartbeat within one period; no
                    // joiner hibernation, all senders stay at fill 600.
                    let published = residents;
                    let h = format!("h{period}");
                    for _ in 0..w {
                        for source in 0..3 {
                            if residents >= 478 {
                                break;
                            }
                            let peers: Vec<_> = (0..4)
                                .map(|p| {
                                    let mut view = peer(
                                        NAMES[p],
                                        load(
                                            if p == 3 { published } else { 60 },
                                            if p == 3 { 1000 } else { 100 },
                                        ),
                                        &h,
                                        Duration::ZERO,
                                    );
                                    view.reachable = p == 3;
                                    view
                                })
                                .collect();
                            match decide(
                                local(NAMES[source], load(60, 100)),
                                &peers,
                                "rate",
                                PERIOD,
                                &budgets[source],
                            ) {
                                Decision::Forward { node, .. } => {
                                    budgets[source].charge(node, &h);
                                    assert_eq!(
                                        receive(load(residents, 1000), 600, true, None, PERIOD),
                                        Arrival::Accept
                                    );
                                    residents += 1;
                                }
                                Decision::Keep(reason) => assert_eq!(reason, Reason::Budget),
                            }
                        }
                    }
                    if w == 1 && period == 13 {
                        assert_eq!(residents, 42);
                    }
                    if residents >= 478 {
                        reached = Some(period + 1);
                        break;
                    }
                    sleep(PERIOD).await;
                }
                assert!(
                    reached.is_some_and(|p| p <= deadline),
                    "seed {seed}, w={w}, residents={residents}, deadline={deadline}"
                );
            }
        });
    }
}

#[test]
fn floor_and_skew_mutations_are_detected() {
    for seed in 0..seeds() {
        Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            let peers = [peer("b", load(0, 1000), "h", Duration::ZERO)];
            assert_eq!(
                decide(
                    local("a", load(199, 1000)),
                    &peers,
                    "idle",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Keep(wasmkeep_cluster::placement::Reason::Floor)
            );
            // Without the floor, band and budget allow this first attempt:
            // 199*4 > 0*5; floor(199*4*1000/5000) = 159 > 0.
            let mut out = Evidence::default();
            out.idle(&[199, 0], 1);
            assert_eq!(out.violations[0].0, 8);

            let mut stale = peers[0];
            stale.age = Duration::from_millis(9000);
            assert_eq!(
                decide(
                    local("a", load(600, 1000)),
                    &[stale],
                    "skew",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Keep(wasmkeep_cluster::placement::Reason::NoCandidate)
            );
            // Mutant trusts the peer's fast wall clock and thinks age is 0.
            stale.age = Duration::ZERO;
            let sent = matches!(
                decide(
                    local("a", load(600, 1000)),
                    &[stale],
                    "skew",
                    PERIOD,
                    &Budget::default()
                ),
                Decision::Forward { .. }
            );
            assert!(sent);
            out.check(4, !sent, || {
                "skew selected a locally stale heartbeat".into()
            });
            assert_eq!(out.violations[1].0, 4);
        });
    }
}
