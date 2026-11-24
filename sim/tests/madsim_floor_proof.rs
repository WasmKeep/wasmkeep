//! ADR 0035, section 3, simulation case 8: what proves a recorded floor.
//! Each seed builds an actor's true history (segments and snapshots over
//! several generations, a frozen head H stamped `set-at`, collection
//! below the newest snapshot now and then), plus stale branches a writer
//! that skipped the check could have left at or below the stamp (the
//! lagging-listing fork), and unrelated larger transaction ids there.
//! The listing then hides a seeded subset of keys (pages out of order or
//! late, the latest snapshot, links). The real planner
//! (`replication::plan_restore`) selects a chain, the real `prove` judges
//! it with the checksums a replay would find.
//!
//! Also the interleaving of review M1: an upload that landed after H
//! froze and a snapshot at it hide H from the plan; the listing's links
//! from the base down to H prove it (`linked_back`), as the server does.
//!
//! Checked: `prove` says proven only if the selected chain is on the true
//! history and reaches H (exactly, with H's checksum, or through a true
//! descendant in a generation above the stamp); with nothing hidden it
//! always proves. Broken variants must be caught: comparing against the
//! head's generation instead of the stamp, and accepting a larger txid.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeMap;

use wasmkeep_cluster::local_heads::{Proof, Requirement, linked_back, prove};
use wasmkeep_cluster::recorded_head::{Completeness, Position};
use wasmkeep_cluster::replication::{
    Listing, Segment, SegmentId, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_sim::rng::Rng;

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

/// The checksum after an object: the true history's, or a branch's own.
fn sum(id: SegmentId, branch: u64) -> u64 {
    (id.generation << 32) ^ id.txid ^ (branch << 48)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Breaks {
    None,
    HeadGeneration,
    LargerTxid,
}

/// The rule under test, or a broken one.
fn judge(breaks: Breaks, floor: &Requirement, chain: &[(SegmentId, Option<u64>)]) -> Proof {
    match breaks {
        Breaks::None => prove(Some(floor), chain),
        Breaks::HeadGeneration => {
            let Position::Known { generation, .. } = floor.head else {
                return Proof::Proven;
            };
            let moved = Requirement {
                set_at: generation,
                ..*floor
            };
            prove(Some(&moved), chain)
        }
        Breaks::LargerTxid => {
            let Position::Known { txid, .. } = floor.head else {
                return Proof::Proven;
            };
            if chain.iter().any(|(id, _)| id.txid >= txid) {
                Proof::Proven
            } else {
                prove(Some(floor), chain)
            }
        }
    }
}

struct World {
    /// Every key, and which branch it belongs to (0: the true history).
    keys: BTreeMap<String, u64>,
    /// The true history's objects by position.
    truth: BTreeMap<SegmentId, ()>,
    floor: Requirement,
    /// True objects above the stamp (descendants of H).
    above: Vec<SegmentId>,
}

fn world(rng: &mut Rng) -> World {
    let mut keys = BTreeMap::new();
    let mut truth = BTreeMap::new();
    let mut txid = 0u64;
    let mut parent_gen = 0u64;
    let gens = 2 + rng.below(4);
    let mut floor = None;
    let mut above = Vec::new();
    let h_hold = 1 + rng.below(gens);
    for g in 1..=gens {
        // A hold that may write nothing (it froze its inherited head).
        let writes = if g == 1 {
            1 + rng.below(4)
        } else {
            rng.below(4)
        };
        for _ in 0..writes {
            txid += 1;
            let id = SegmentId {
                generation: g,
                txid,
            };
            keys.insert(
                segment_key(&Segment {
                    id,
                    parent_generation: parent_gen,
                }),
                0,
            );
            truth.insert(id, ());
            parent_gen = g;
            if rng.chance(15) {
                keys.insert(snapshot_key(id), 0);
            }
        }
        if g == h_hold {
            // H: this hold's last acknowledged position, stamped with a
            // later generation if the next holds wrote nothing yet.
            let head = truth.keys().next_back().copied().unwrap();
            floor = Some((head, g));
        }
    }
    let (head, hold) = floor.unwrap();
    // The stamp: the hold that froze H, or a later one that inherited H
    // without writing (it re-froze it unchanged).
    let set_at = hold;
    // Review M1: an upload that landed after H froze (in H's hold, at
    // H + 1), and the next owner's snapshot there, written without a new
    // segment: the snapshot hides H from the plan.
    if truth.keys().next_back() == Some(&head) && rng.chance(35) {
        let late = SegmentId {
            generation: hold,
            txid: head.txid + 1,
        };
        keys.insert(
            segment_key(&Segment {
                id: late,
                parent_generation: head.generation,
            }),
            0,
        );
        keys.insert(snapshot_key(late), 0);
        truth.insert(late, ());
    }
    for id in truth.keys() {
        if id.generation > set_at {
            above.push(*id);
        }
    }
    // Stale branches at or below the stamp: forked from below H (a
    // writer that restored an older head), with larger txids too.
    for _ in 0..rng.below(3) {
        // Often between H's own generation and the stamp: the holds that
        // inherited H without writing, where only the stamp tells a
        // stale branch from a descendant.
        let g = if rng.chance(60) {
            head.generation + rng.below(set_at - head.generation + 1)
        } else {
            1 + rng.below(set_at)
        };
        let from = rng.below(head.txid);
        let mut pg = truth
            .keys()
            .find(|id| id.txid == from)
            .map_or(0, |id| id.generation);
        let len = 1 + rng.below(6);
        for t in from + 1..=from + len {
            let id = SegmentId {
                generation: g,
                txid: t,
            };
            if truth.contains_key(&id) {
                break;
            }
            keys.insert(
                segment_key(&Segment {
                    id,
                    parent_generation: pg,
                }),
                1,
            );
            pg = g;
        }
    }
    World {
        keys,
        truth,
        floor: Requirement {
            head: Position::Known {
                generation: head.generation,
                txid: head.txid,
                checksum: sum(head, 0),
            },
            set_at,
            completeness: Completeness::Complete,
        },
        above,
    }
}

/// One seed: `Some(why)` if the rule accepted a chain that does not
/// reach H, or refused a complete listing.
fn run(seed: u64, breaks: Breaks) -> Option<String> {
    let mut rng = Rng::new(seed ^ 0x0008);
    let w = world(&mut rng);
    let Position::Known {
        generation: hg,
        txid: ht,
        ..
    } = w.floor.head
    else {
        unreachable!()
    };
    let h = SegmentId {
        generation: hg,
        txid: ht,
    };
    for hide in [false, true, true, true, true, true, true] {
        let listed: Vec<&String> = w.keys.keys().filter(|_| !hide || !rng.chance(30)).collect();
        let listing = Listing::from_keys(listed.iter().map(|k| k.as_str()));
        let Ok(plan) = plan_restore(&listing) else {
            continue; // a missing link: listed again, never activated
        };
        let ids: Vec<SegmentId> = plan.snapshot.into_iter().chain(plan.segments).collect();
        // A replay finds each object's own checksum.
        let branch_of = |id: &SegmentId| u64::from(!w.truth.contains_key(id));
        let replayed: Vec<(SegmentId, Option<u64>)> = ids
            .iter()
            .map(|id| (*id, Some(sum(*id, branch_of(id)))))
            .collect();
        let on_truth = ids.iter().all(|id| w.truth.contains_key(id));
        // The true history is one chain: a true base past H descends from it.
        let base_past_h = plan
            .snapshot
            .is_some_and(|b| w.truth.contains_key(&b) && b.txid > h.txid);
        let reaches = on_truth
            && (ids.contains(&h) || ids.iter().any(|id| w.above.contains(id)) || base_past_h);
        let mut verdict = judge(breaks, &w.floor, &replayed);
        // The server's fallback (replica.rs): the base links back to a
        // listed floor.
        if verdict == Proof::Missing
            && let Some(base) = plan.snapshot
            && linked_back(Some(&w.floor), base, &listing)
        {
            verdict = Proof::Proven;
            LINKED.with(|n| n.set(n.get() + 1));
        }
        if verdict == Proof::Proven && !reaches {
            return Some(format!("proved {h:?} from {ids:?} (hidden: {hide})"));
        }
        if !hide && breaks == Breaks::None && verdict != Proof::Proven && on_truth {
            // A complete listing whose chain is the true one always proves.
            return Some(format!("refused a complete listing {ids:?}"));
        }
    }
    None
}

thread_local! {
    /// Floors proven by the base linking back to them (review M1).
    static LINKED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[test]
fn only_exact_ancestry_or_a_later_generation_proves_the_floor() {
    LINKED.with(|n| n.set(0));
    for seed in 0..seeds() {
        if let Some(why) = run(seed, Breaks::None) {
            panic!("seed {seed}: {why}");
        }
    }
    let linked = LINKED.with(std::cell::Cell::get);
    eprintln!(
        "{} seeds: {linked} floors proven through the listing below the base",
        seeds()
    );
    assert!(linked >= (seeds() / 20).max(1), "linked {linked}");
}

#[test]
fn broken_floor_rules_are_caught() {
    // The stale branch must be the selected chain: a narrow event (about
    // 2.4% of seeds for a larger txid, 3.2% for the head's generation, at
    // 2,000 seeds). The floor is half the rarer rate, over at least 400
    // seeds (pure computation): every prefix from 400 to 2,000 seeds then
    // clears it by 1.8x or more, so it holds at every seed count.
    let seeds = seeds().max(400);
    let floor = seeds / 80;
    for breaks in [Breaks::HeadGeneration, Breaks::LargerTxid] {
        let caught = (0..seeds).filter(|&s| run(s, breaks).is_some()).count() as u64;
        eprintln!("caught on {caught} of {seeds} seeds");
        assert!(caught >= floor, "caught on {caught}, want {floor}");
    }
}

// Cases 1, 9 and 19. The planner/proof below is production code; the
// server-private discovery/cooldown scheduler is a model of replica.rs and
// cluster/lease_hold.rs. In particular it DOES NOT invent an outer timeout:
// a stream with pages below the idle limit can exceed 15 s (as-built (c)).
const RETRIES: [u64; 9] = [100, 200, 400, 800, 1600, 1600, 1600, 1600, 1600];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiscoveryFault {
    None,
    SkipFloor,
    RestartBudget,
    BrokenInsteadOfVisibility,
}

#[derive(Debug, PartialEq, Eq)]
enum DiscoveryResult {
    Active,
    Visibility,
    Broken,
    Cancelled,
}

async fn discover(
    seed: u64,
    visible_at: u64,
    page_ms: u64,
    cancel_at: Option<u64>,
    fault: DiscoveryFault,
) -> (DiscoveryResult, usize, u64) {
    use madsim::time::{Instant, sleep};
    use std::time::Duration;
    let start = Instant::now();
    let elapsed = || u64::try_from(start.elapsed().as_millis()).unwrap();
    let mut rng = Rng::new(seed);
    let h = SegmentId {
        generation: 3,
        txid: 20,
    };
    let floor = Requirement {
        head: Position::Known {
            generation: 3,
            txid: 20,
            checksum: sum(h, 0),
        },
        set_at: 4,
        completeness: Completeness::Complete,
    };
    let mut attempts = 0;
    loop {
        attempts += 1;
        // Two streamed pages, each within the 5 s idle rule. Replay can
        // return Visibility too; it consumes this same attempt/budget.
        for _ in 0..2 {
            sleep(Duration::from_millis(page_ms)).await;
            if cancel_at.is_some_and(|at| elapsed() >= at) {
                return (DiscoveryResult::Cancelled, attempts, elapsed());
            }
        }
        let keys = if elapsed() >= visible_at {
            vec![snapshot_key(h)]
        } else if seed.is_multiple_of(2) {
            Vec::new() // apparently empty, including snapshots
        } else {
            vec![snapshot_key(SegmentId {
                generation: 3,
                txid: 19,
            })]
        };
        let plan = plan_restore(&Listing::from_keys(keys.iter().map(String::as_str))).unwrap();
        let chain: Vec<_> = plan
            .snapshot
            .into_iter()
            .chain(plan.segments)
            .map(|id| (id, Some(sum(id, 0))))
            .collect();
        if fault == DiscoveryFault::SkipFloor || prove(Some(&floor), &chain) == Proof::Proven {
            return (DiscoveryResult::Active, attempts, elapsed());
        }
        if fault == DiscoveryFault::BrokenInsteadOfVisibility {
            return (DiscoveryResult::Broken, attempts, elapsed());
        }
        if attempts == 10 {
            return (DiscoveryResult::Visibility, attempts, elapsed());
        }
        let pause = RETRIES[attempts - 1];
        let pause = pause + rng.below(pause / 5 + 1);
        if fault != DiscoveryFault::RestartBudget && elapsed() + pause >= 15_000 {
            return (DiscoveryResult::Visibility, attempts, elapsed());
        }
        sleep(Duration::from_millis(pause)).await;
        if cancel_at.is_some_and(|at| elapsed() >= at) {
            return (DiscoveryResult::Cancelled, attempts, elapsed());
        }
    }
}

#[test]
fn delayed_handover_discovery_and_broken_variants() {
    for seed in 0..seeds() {
        madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            // Each release site has the same frozen bucket requirement.
            for site in ["move", "hibernate", "drain", "stop"] {
                let (result, attempts, ms) =
                    discover(seed, 5000, 1, None, DiscoveryFault::None).await;
                assert_eq!(result, DiscoveryResult::Active, "{site} seed {seed}");
                assert!((2..=10).contains(&attempts) && (5000..15000).contains(&ms));
                let (result, attempts, _) =
                    discover(seed, u64::MAX, 1, None, DiscoveryFault::None).await;
                assert_eq!(result, DiscoveryResult::Visibility, "{site}");
                assert_eq!(attempts, 10, "all nine delays exercised");
            }
            assert_eq!(
                discover(seed, 5000, 1, None, DiscoveryFault::SkipFloor)
                    .await
                    .0,
                DiscoveryResult::Active,
                "mutant activates before H is visible"
            );
            let slow = discover(seed, u64::MAX, 4000, None, DiscoveryFault::None).await;
            assert_eq!(slow.0, DiscoveryResult::Visibility);
            assert_eq!(slow.1, 2);
            assert!(
                slow.2 > 15000,
                "documented stream deadline deviation exercised"
            );
            let broken = discover(seed, u64::MAX, 4000, None, DiscoveryFault::RestartBudget).await;
            assert_eq!(
                broken.1, 10,
                "resetting retry budget is caught by the two-attempt oracle"
            );
            assert!(broken.2 > slow.2);
            assert_eq!(
                discover(
                    seed,
                    u64::MAX,
                    1,
                    None,
                    DiscoveryFault::BrokenInsteadOfVisibility
                )
                .await
                .0,
                DiscoveryResult::Broken,
                "mutant misclassifies visibility refusal"
            );
            for cancellation in [1, 150, 700, 3000] {
                assert_eq!(
                    discover(seed, u64::MAX, 1, Some(cancellation), DiscoveryFault::None)
                        .await
                        .0,
                    DiscoveryResult::Cancelled
                );
            }
        });
    }
}

// Case 19: admission state belongs to the node, surviving actor eviction.
// Coalesced contenders never acquire, list or refresh the idle origin.
fn refusal_storm(seed: u64, reset_cooldown: bool, refresh_idle: bool) -> (u64, u64, bool) {
    let mut rng = Rng::new(seed);
    let mut until = 0;
    let mut cycles = 0;
    let mut idle = 17;
    let mut active = false;
    let steps = [1000, 2000, 4000, 8000, 16000, 30000];
    // 5 ingress routes at every tick; eviction between ticks deliberately
    // retains these node-owned fields. Floor becomes visible after 90 s.
    for now in (0..180_000).step_by(50) {
        for _route in ["request", "alarm", "move", "delete", "expiry"] {
            if active {
                break;
            }
            if reset_cooldown {
                until = now;
            }
            if now < until {
                continue;
            }
            if now >= 90_000 {
                active = true;
                break;
            }
            let delay = steps[usize::try_from(cycles).unwrap().min(5)];
            cycles += 1; // one acquisition, <= 10 listings, one resolver entry
            if refresh_idle {
                idle = now;
            }
            until = now + delay + rng.below(delay / 5 + 1);
        }
    }
    (cycles, idle, active)
}

#[test]
fn eviction_and_all_ingress_routes_share_refusal_cooldown() {
    for seed in 0..seeds() {
        let (cycles, idle, active) = refusal_storm(seed, false, false);
        assert!((6..=8).contains(&cycles), "seed {seed}: {cycles} cycles");
        assert_eq!(idle, 17);
        assert!(
            active,
            "reveal must restore progress, including eligible expiry"
        );
        assert!(
            refusal_storm(seed, true, false).0 > 8,
            "cooldown-reset mutant escaped"
        );
        assert_ne!(
            refusal_storm(seed, false, true).1,
            17,
            "idle-refresh mutant escaped"
        );
    }
}

/// Case 9 cancellation at blocking boundaries. No pretend GET deadline:
/// caller cancellation drops discovery even when a permit/GET never returns.
#[test]
fn cancellation_drops_queued_hung_and_backing_off_discovery() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for seed in 0..seeds() {
        madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
            for stage in ["permit queue", "hung GET", "backoff", "replay"] {
                let entered = Arc::new(AtomicUsize::new(0));
                let dropped = Arc::new(AtomicUsize::new(0));
                let uploads = Arc::new(AtomicUsize::new(0));
                let (e, d, u) = (entered.clone(), dropped.clone(), uploads.clone());
                let work = madsim::task::spawn(async move {
                    let _guard = Dropped(d);
                    e.store(1, Ordering::SeqCst);
                    if matches!(stage, "permit queue" | "hung GET") {
                        std::future::pending::<()>().await;
                    } else {
                        madsim::time::sleep(Duration::from_secs(30)).await;
                    }
                    u.fetch_add(1, Ordering::SeqCst);
                });
                madsim::time::sleep(Duration::from_millis(1 + seed % 50)).await;
                assert_eq!(entered.load(Ordering::SeqCst), 1, "{stage} must be reached");
                work.abort();
                assert!(work.await.is_err());
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert_eq!(uploads.load(Ordering::SeqCst), 0);
            }
        });
    }
}

// ADR 0042, Tests, "Other sims": after an idle pass, an acquirer proves
// the recorded head at its exact position. Each seed writes an actor's
// history over several generations (periodic snapshots now and then),
// the last writer's hold acknowledging its head H through the real hold
// cell (`hold_cell::verify`, `acknowledge`, `end`) and releasing with
// what it froze (`Requirement::frozen`, `Requirement::stored`, encoded
// and decoded as a version 2 record, as `Owners::release_written` writes
// it). An idle snapshot is taken at H (or, on some seeds, none: the
// threshold was not met, and the pass compacts from a periodic snapshot
// of H's generation below H, with segments after it). The pass acquires
// the released record (one generation up), inherits its requirement
// (`combine` of its node's local evidence and the record), lists, applies
// `idle_collectable` with the released generation, deletes, and releases
// unverified: it freezes what it inherited. Sometimes a late segment of an
// earlier generation lands after the pass (a slow upload of an older
// owner, ADR 0012's reason for keeping the segment at P).
//
// An acquirer on another node then reads the pass's record
// (`Requirement::of_record`), lists, plans with the real planner and
// judges with the real `prove`. Checked: the record still requires H,
// complete, stamped by H's writer; the plan's head is H; and the proof is
// the exact one (H on the chain with H's checksum), never "a generation
// above the stamp" nor `linked_back`. Mutations, each caught on some
// seed: the pass also deletes the segment at P, or `S`, or a segment
// between `S` and H; the idle snapshot holds another position's
// database; the pass's release drops what it inherited.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassBreak {
    None,
    DropSegmentAtP,
    DropSnapshot,
    DropBetween,
    StaleSnapshot,
    ForgetInherited,
}

/// One seed: `Err(why)` if the acquirer did not prove H exactly; `Ok` with
/// whether the pass deleted anything and whether `S` was below H.
fn proof_after_a_pass(seed: u64, breaks: PassBreak) -> Result<(bool, bool), String> {
    use std::sync::Mutex;
    use wasmkeep_cluster::hold_cell::{self, HoldHead};
    use wasmkeep_cluster::lease::Version;
    use wasmkeep_cluster::local_heads::combine;
    use wasmkeep_cluster::node_lease::{Hold, OwnerRecord};
    use wasmkeep_cluster::replication::idle_collectable;
    let mut rng = Rng::new(seed ^ 0x0042_0035);
    // The history: (key, checksum after it); snapshots hold the database
    // at their position.
    let mut keys: BTreeMap<String, u64> = BTreeMap::new();
    let mut txid = 0u64;
    let mut parent = 0u64;
    let gens = 1 + rng.below(4);
    let mut since_snapshot = 0;
    for g in 1..=gens {
        let writes = 1 + rng.below(9);
        for _ in 0..writes {
            txid += 1;
            let id = SegmentId {
                generation: g,
                txid,
            };
            keys.insert(
                segment_key(&Segment {
                    id,
                    parent_generation: parent,
                }),
                sum(id, 0),
            );
            parent = g;
            since_snapshot += 1;
            if since_snapshot == 5 || rng.chance(10) {
                keys.insert(snapshot_key(id), sum(id, 0));
                since_snapshot = 0;
            }
        }
    }
    let head = SegmentId {
        generation: gens,
        txid,
    };
    let at = |id: SegmentId| Position::Known {
        generation: id.generation,
        txid: id.txid,
        checksum: sum(id, 0),
    };
    // The writer's hold: verified at its restore, its last upload
    // acknowledged, then ended and released (version 2).
    let cell = Mutex::new(Some(Hold {
        key: "owners/t/c/a".into(),
        generation: gens,
        version: Version("v1".into()),
        previous: None,
        head: HoldHead {
            recorded: true,
            ..HoldHead::default()
        },
    }));
    hold_cell::verify(&cell, || true, Position::Empty).unwrap();
    hold_cell::acknowledge(&cell, || true, Some(at(head))).unwrap();
    let writer = hold_cell::end(&cell).unwrap();
    let writer_floor = Requirement::frozen(&writer).unwrap();
    let released = OwnerRecord {
        holder: None,
        generation: gens,
        recorded: Some(Requirement::stored(Some(writer_floor), false, None)),
    };
    let released = OwnerRecord::decode(&released.encode()).expect("a valid record");
    // The idle snapshot at H, unless the threshold was not met (one seed
    // in three) or one is there already.
    let below = rng.chance(33)
        && keys.keys().any(|k| {
            Listing::from_keys([k.as_str()])
                .snapshots
                .first()
                .is_some_and(|s| s.generation == gens && s.txid < head.txid)
        });
    if !below {
        let body = if breaks == PassBreak::StaleSnapshot {
            sum(
                SegmentId {
                    generation: gens,
                    txid: head.txid - 1,
                },
                0,
            )
        } else {
            sum(head, 0)
        };
        keys.insert(snapshot_key(head), body);
    }
    // The pass: one generation up, unchanged record, its requirement
    // inherited from its node's local evidence (the writer's frozen head)
    // and the record.
    let inherited = combine(
        Some((writer_floor, gens)),
        Requirement::of_record(&released).map(|r| (r, released.generation)),
    );
    let pass = Hold {
        key: "owners/t/c/a".into(),
        generation: released.generation + 1,
        version: Version("v2".into()),
        previous: None,
        head: HoldHead {
            inherited,
            recorded: true,
            ..HoldHead::default()
        },
    };
    let listing = Listing::from_keys(keys.keys().map(String::as_str));
    let Some((s, mut gone)) = idle_collectable(&listing, released.generation) else {
        return Err("no snapshot of the released generation".into());
    };
    let boundary = |id: SegmentId| {
        listing
            .segments
            .iter()
            .find(|seg| seg.id == id)
            .map(segment_key)
    };
    match breaks {
        PassBreak::DropSegmentAtP => gone.extend(boundary(s)),
        PassBreak::DropSnapshot => gone.push(snapshot_key(s)),
        PassBreak::DropBetween if s.txid < head.txid => gone.extend(boundary(SegmentId {
            generation: gens,
            txid: s.txid + 1,
        })),
        _ => {}
    }
    let deleted = !gone.is_empty();
    for key in &gone {
        keys.remove(key);
    }
    // Released unverified: it freezes what it inherited.
    let frozen = if breaks == PassBreak::ForgetInherited {
        None
    } else {
        Requirement::frozen(&pass)
    };
    let after = OwnerRecord {
        holder: None,
        generation: pass.generation,
        recorded: Some(Requirement::stored(frozen, false, None)),
    };
    let after = OwnerRecord::decode(&after.encode()).expect("a valid record");
    // A late segment of an earlier generation, landing after the pass.
    if gens > 1 && rng.chance(40) {
        let late = SegmentId {
            generation: gens - 1,
            txid: head.txid + 1,
        };
        keys.insert(
            segment_key(&Segment {
                id: late,
                parent_generation: gens - 1,
            }),
            sum(late, 1),
        );
    }
    // The acquirer: the record's requirement, the plan, the proof.
    let floor = Requirement::of_record(&after).ok_or("the pass's record lost the head")?;
    if floor.head != at(head)
        || floor.set_at != gens
        || floor.completeness != Completeness::Complete
    {
        return Err(format!("the pass's record requires {floor:?}"));
    }
    let listing = Listing::from_keys(keys.keys().map(String::as_str));
    let plan = plan_restore(&listing).map_err(|e| format!("plan: {e:?}"))?;
    if plan.head() != head {
        return Err(format!("planned {:?}, not {head:?}", plan.head()));
    }
    let chain: Vec<(SegmentId, Option<u64>)> = plan
        .snapshot
        .map(|s| (s, keys[&snapshot_key(s)]))
        .into_iter()
        .chain(plan.segments.iter().map(|id| {
            let key = listing
                .segments
                .iter()
                .find(|seg| seg.id == *id)
                .map(segment_key)
                .unwrap();
            (*id, keys[&key])
        }))
        .map(|(id, sum)| (id, Some(sum)))
        .collect();
    let verdict = prove(Some(&floor), &chain);
    if verdict != Proof::Proven {
        return Err(format!("proof {verdict:?} over {chain:?}"));
    }
    // And by the exact branch: nothing above the stamp to prove it
    // otherwise, H on the chain with its checksum.
    if chain.iter().any(|(id, _)| id.generation > floor.set_at) {
        return Err("a generation above the stamp".into());
    }
    if !chain.contains(&(head, Some(sum(head, 0)))) {
        return Err(format!("H not on the chain with its checksum: {chain:?}"));
    }
    Ok((deleted, s.txid < head.txid))
}

#[test]
fn after_an_idle_pass_an_acquirer_proves_the_recorded_head_exactly() {
    let (mut deleted, mut below) = (0, 0);
    for seed in 0..seeds() {
        let (d, b) = proof_after_a_pass(seed, PassBreak::None)
            .unwrap_or_else(|why| panic!("seed {seed}: {why}"));
        deleted += u64::from(d);
        below += u64::from(b);
    }
    eprintln!(
        "{} seeds: an exact proof after every pass; {deleted} passes deleted, {below} from a \
         snapshot below the head",
        seeds()
    );
    assert!(
        deleted >= seeds() / 2 && below >= seeds() / 10,
        "deleted {deleted}, below {below}"
    );
}

#[test]
fn a_broken_idle_pass_fails_the_recorded_head_proof() {
    for breaks in [
        PassBreak::DropSegmentAtP,
        PassBreak::DropSnapshot,
        PassBreak::DropBetween,
        PassBreak::StaleSnapshot,
        PassBreak::ForgetInherited,
    ] {
        let mut first = None;
        let mut caught = 0;
        for seed in 0..seeds() {
            if let Err(why) = proof_after_a_pass(seed, breaks) {
                first.get_or_insert((seed, why));
                caught += 1;
            }
        }
        eprintln!(
            "{breaks:?}: caught on {caught} of {} seeds, first {first:?}",
            seeds()
        );
        assert!(caught > 0, "{breaks:?} not caught");
    }
}
