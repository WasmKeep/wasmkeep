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

use wasmkeep_cluster::local_heads::{Proof, Requirement, prove};
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
        let reaches = on_truth && (ids.contains(&h) || ids.iter().any(|id| w.above.contains(id)));
        let verdict = judge(breaks, &w.floor, &replayed);
        if verdict == Proof::Proven && !reaches {
            return Some(format!("proved {h:?} from {ids:?} (hidden: {hide})"));
        }
        if !hide && breaks == Breaks::None && verdict != Proof::Proven && !w.above.is_empty() {
            // The complete listing's chain is the true one, above the stamp.
            return Some(format!("refused a complete listing {ids:?}"));
        }
    }
    None
}

#[test]
fn only_exact_ancestry_or_a_later_generation_proves_the_floor() {
    for seed in 0..seeds() {
        if let Some(why) = run(seed, Breaks::None) {
            panic!("seed {seed}: {why}");
        }
    }
}

#[test]
fn broken_floor_rules_are_caught() {
    // The stale branch must be the selected chain: a narrow event.
    let floor = (seeds() / 40).max(1);
    for breaks in [Breaks::HeadGeneration, Breaks::LargerTxid] {
        let caught = (0..seeds()).filter(|&s| run(s, breaks).is_some()).count() as u64;
        eprintln!("caught on {caught} of {} seeds", seeds());
        assert!(caught >= floor, "caught on {caught}, want {floor}");
    }
}
