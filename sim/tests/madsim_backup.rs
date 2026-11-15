//! ADR 0033 destination model. Real lease owners/faults are exercised by
//! the replication reader test below; these schedules isolate archive races.
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[path = "madsim_replication.rs"]
mod replication;
use std::collections::BTreeMap;
use std::time::Duration;
use wasmkeep_cluster::replication::{
    Listing, Segment, SegmentId, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_sim::rng::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unsafe {
    None,
    OmitBoundary,
    NoReplay,
    NoRetry,
    SplitListing,
    SourceKey,
    Prune,
}
fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}
fn run(seed: u64, broken: Unsafe) -> (bool, usize, usize) {
    madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(
        async move {
            let mut rng = Rng::new(seed);
            let p = SegmentId {
                generation: 1,
                txid: 2,
            };
            let old = Segment {
                id: SegmentId {
                    generation: 1,
                    txid: 1,
                },
                parent_generation: 0,
            };
            let reset = Segment {
                id: p,
                parent_generation: 1,
            };
            // Bodies model complete states and checksum identity. Reset is empty.
            let mut source = BTreeMap::from([
                (segment_key(&old), 1u64),
                (segment_key(&reset), 0),
                (snapshot_key(p), 0),
            ]);
            let mut destination = BTreeMap::new();
            let mut manifest = BTreeMap::new();
            let mut retries = 0;
            let mut starved = 0;
            for incarnation in 0..2 {
                if incarnation == 1 {
                    for body in source.values_mut() {
                        *body += 10;
                    }
                }
                let mut copied = false;
                for attempt in 0..8 {
                    let mut listed = source.clone();
                    madsim::time::sleep(Duration::from_millis(rng.below(10))).await;
                    // Page boundary/GET races: zero retention can exhaust retries;
                    // nonzero retention permits a fresh complete second listing.
                    let zero_window = seed.is_multiple_of(7);
                    let race = zero_window || (rng.chance(40) && attempt == 0);
                    if race {
                        retries += 1;
                        if broken == Unsafe::NoRetry {
                            break; // falsely treats a collection miss as final
                        }
                        if broken == Unsafe::SplitListing {
                            listed.remove(&segment_key(&reset));
                        } else if broken != Unsafe::NoReplay {
                            continue;
                        }
                    }
                    if broken == Unsafe::OmitBoundary {
                        listed.remove(&segment_key(&reset));
                    }
                    for (key, body) in &listed {
                        let digest = if broken == Unsafe::SourceKey {
                            key.clone()
                        } else {
                            format!("content-{key}-{body}")
                        };
                        // A snapshot GET misses after collection. The unsafe reader
                        // still publishes its listing as a successful manifest.
                        if !(race && broken == Unsafe::NoReplay && key == &snapshot_key(p)) {
                            destination.entry(digest.clone()).or_insert(*body);
                        }
                        manifest.insert(key.clone(), digest);
                    }
                    if broken == Unsafe::Prune {
                        destination.clear();
                    }
                    let plan =
                        plan_restore(&Listing::from_keys(manifest.keys().map(String::as_str)))
                            .unwrap();
                    let expected = incarnation * 10;
                    let actual = plan
                        .snapshot
                        .map(snapshot_key)
                        .or_else(|| plan.segments.last().map(|_| segment_key(&old)))
                        .and_then(|key| manifest.get(&key))
                        .and_then(|digest| destination.get(digest))
                        .copied();
                    if plan.head() != p || actual != Some(expected) {
                        return (false, retries, starved);
                    }
                    copied = true;
                    break;
                }
                if !copied {
                    starved += 1;
                    if !seed.is_multiple_of(7) {
                        return (false, retries, starved);
                    }
                }
            }
            (true, retries, starved)
        },
    )
}
#[test]
fn verified_incremental_reader_and_retention() {
    let mut retries = 0;
    let mut starved = 0;
    for seed in 0..seeds() {
        let (ok, r, s) = run(seed, Unsafe::None);
        assert!(ok, "seed {seed}");
        retries += r;
        starved += s;
    }
    assert!(retries > 0, "reader never retried");
    assert!(starved > 0, "zero-window starvation was not exercised");
    eprintln!("retries={retries}, zero-window starved={starved}");
}
#[test]
fn unsafe_readers_are_caught() {
    for variant in [
        Unsafe::OmitBoundary,
        Unsafe::NoReplay,
        Unsafe::NoRetry,
        Unsafe::SplitListing,
        Unsafe::SourceKey,
        Unsafe::Prune,
    ] {
        assert!(
            (0..seeds()).any(|seed| !run(seed, variant).0),
            "{variant:?}"
        );
    }
}

#[test]
fn history_all_preserves_every_acknowledged_position_inside_the_window() {
    use wasmkeep_cluster::replication::collectable;
    // A body is (pre-checksum, post-checksum, upload time). The state/checksum
    // is abstract here; the CLI fixtures exercise the real LTX and SQLite bytes.
    for seed in 0..seeds() {
        madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(
            async {
                for window in [8u64, 32] {
                    let mut source: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
                    let mut archive = BTreeMap::new();
                    let mut expected = BTreeMap::new();
                    let mut rng = Rng::new(seed);
                    let mut state = 0;
                    for txid in 1..=24u64 {
                        let previous = state;
                        // Erasure at a delete has no window; the preceding run at
                        // txid 12 already archived every earlier acknowledged state.
                        state = if txid == 13 { 0 } else { state + 1 };
                        let id = SegmentId {
                            generation: 1,
                            txid,
                        };
                        let segment = Segment {
                            id,
                            parent_generation: if txid == 1 { 0 } else { 1 },
                        };
                        source.insert(segment_key(&segment), (previous, state, txid));
                        expected.insert(txid, state);
                        if txid == 1 || txid.is_multiple_of(5) || txid == 13 {
                            source.insert(snapshot_key(id), (0, state, txid));
                            let listed = Listing::from_keys(source.keys().map(String::as_str));
                            let deletions = collectable(&listed, 1, id);
                            // Owner clock skew and late completion of a delete.
                            madsim::time::sleep(Duration::from_millis(rng.below(5))).await;
                            for key in deletions {
                                if txid == 13 || source[&key].2 + window < txid + 1 {
                                    source.remove(&key);
                                }
                            }
                        }
                        if txid.is_multiple_of(3) || txid == 1 {
                            archive.extend(source.clone());
                            for (&target, &checksum) in &expected {
                                let listing =
                                    Listing::from_keys(archive.keys().map(String::as_str));
                                let subset = Listing {
                                    segments: listing
                                        .segments
                                        .into_iter()
                                        .filter(|s| s.id.txid <= target)
                                        .collect(),
                                    snapshots: listing
                                        .snapshots
                                        .into_iter()
                                        .filter(|s| s.txid <= target)
                                        .collect(),
                                };
                                let plan = plan_restore(&subset).unwrap();
                                let mut restored =
                                    plan.snapshot.map_or(0, |id| archive[&snapshot_key(id)].1);
                                for id in &plan.segments {
                                    let segment =
                                        subset.segments.iter().find(|s| s.id == *id).unwrap();
                                    let (pre, post, _) = archive[&segment_key(segment)];
                                    assert_eq!(restored, pre, "seed {seed}: checksum chain");
                                    restored = post;
                                }
                                assert_eq!(plan.head().txid, target);
                                assert_eq!(
                                    restored, checksum,
                                    "seed {seed}: archived position {target}"
                                );
                            }
                        }
                    }
                }
            },
        );
    }
}
