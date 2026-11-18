//! Paginated archive reader sharing the replication model's live owners.
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[path = "madsim_replication.rs"]
mod replication;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, segment_key, snapshot_key};
const PREFIX: &str = "actors/t/c/a/";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unsafe {
    None,
    OmitBoundary,
    TruncatedSnapshot,
    NoReplay,
    NoRetry,
    SplitListing,
    SourceKey,
    Prune,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Failure {
    Boundary,
    Body,
    Missing,
    Retry,
    Split,
    Content,
}
type Manifest = BTreeMap<String, String>;
type Bodies = BTreeMap<String, Vec<u8>>;
#[derive(Default)]
struct Reader {
    cursor: String,
    listed: Vec<String>,
    fetching: Option<Vec<String>>,
    manifest: BTreeMap<String, String>,
    destination: BTreeMap<String, Vec<u8>>,
    runs: Vec<(Manifest, Bodies, OwnerHistory, Vec<SegmentId>)>,
    owners: OwnerHistory,
    acked: Vec<SegmentId>,
    expected: BTreeMap<String, Vec<u8>>,
    retries: usize,
    pages: usize,
    gets: usize,
    failure: Option<Failure>,
    tick: usize,
}
fn oracle(
    manifest: &BTreeMap<String, String>,
    destination: &BTreeMap<String, Vec<u8>>,
    expected: &BTreeMap<String, Vec<u8>>,
) -> Result<(), Failure> {
    let listing = Listing::from_keys(manifest.keys().map(String::as_str));
    let plan = plan_restore(&listing).map_err(|_| Failure::Split)?;
    if let Some(snapshot) = plan.snapshot {
        // The reader records whether this boundary was listed, independently
        // of the potentially damaged manifest.
        if let Some(segment) = Listing::from_keys(expected.keys().map(String::as_str))
            .segments
            .iter()
            .find(|s| s.id == snapshot)
            && !manifest.contains_key(&segment_key(segment))
        {
            return Err(Failure::Boundary);
        }
    }
    if manifest.len() != expected.len() {
        return Err(Failure::Split);
    }
    for (key, reference) in manifest {
        let body = destination.get(reference).ok_or(Failure::Missing)?;
        let single = Listing::from_keys([key.as_str()]);
        if !single.segments.is_empty() {
            if body != b"x" && body != b"reset" {
                return Err(Failure::Body);
            }
        } else {
            let body = std::str::from_utf8(body).map_err(|_| Failure::Body)?;
            for position in body.split(',').filter(|p| !p.is_empty()) {
                let (g, t) = position.split_once(':').ok_or(Failure::Body)?;
                g.parse::<u64>().map_err(|_| Failure::Body)?;
                t.parse::<u64>().map_err(|_| Failure::Body)?;
            }
            if let Some(segment) = listing
                .segments
                .iter()
                .find(|s| Some(s.id) == single.snapshots.first().copied())
            {
                let boundary = destination
                    .get(&manifest[&segment_key(segment)])
                    .ok_or(Failure::Missing)?;
                if boundary == b"reset"
                    && body != format!("{}:{}", segment.id.generation, segment.id.txid)
                {
                    return Err(Failure::Boundary);
                }
            }
        }
        if expected.get(key) != Some(body) {
            return Err(Failure::Content);
        }
    }
    Ok(())
}
fn run(seed: u64, broken: Unsafe) -> Reader {
    let reader = Arc::new(Mutex::new(Reader::default()));
    let observer = reader.clone();
    replication::backup_simulation(
        seed,
        Box::new(move |store, acked, owners| {
            let mut r = observer.lock().unwrap();
            r.tick += 1;
            // Delayed GETs and separate LIST pages, while owners keep running.
            if !r.tick.is_multiple_of(2 + seed as usize % 3) || r.failure.is_some() {
                return;
            }
            if let Some(mut remaining) = r.fetching.take() {
                if let Some(key) = remaining.pop() {
                    r.gets += 1;
                    let body = store
                        .lock()
                        .unwrap()
                        .get(&format!("{PREFIX}{key}"))
                        .map(|(b, _)| b.clone());
                    let Some(mut body) = body else {
                        r.retries += 1;
                        if broken == Unsafe::NoRetry {
                            r.failure = Some(Failure::Retry);
                        } else if broken == Unsafe::NoReplay {
                            r.failure = Some(Failure::Missing);
                        }
                        r.cursor.clear();
                        r.listed.clear();
                        r.manifest.clear();
                        r.expected.clear();
                        return;
                    };

                    if broken == Unsafe::TruncatedSnapshot
                        && key.starts_with("snapshot/")
                        && let Some(comma) = body.iter().position(|b| *b == b',')
                    {
                        body = body[comma + 1..].to_vec();
                    }
                    if broken == Unsafe::NoReplay {
                        body = b"corrupt".to_vec();
                    }
                    let reference = if broken == Unsafe::SourceKey {
                        key.clone()
                    } else {
                        format!("{key}:{body:?}")
                    };
                    r.destination.entry(reference.clone()).or_insert(body);
                    r.manifest.insert(key, reference);
                    r.fetching = Some(remaining);
                    return;
                }
                let mut manifest = std::mem::take(&mut r.manifest);
                let listing = Listing::from_keys(manifest.keys().map(String::as_str));
                if let Ok(plan) = plan_restore(&listing) {
                    if broken == Unsafe::OmitBoundary
                        && let Some(boundary) = listing
                            .segments
                            .iter()
                            .find(|s| Some(s.id) == plan.snapshot)
                    {
                        manifest.remove(&segment_key(boundary));
                    }
                    if broken == Unsafe::Prune {
                        r.destination.clear();
                    }
                    match oracle_history(
                        &manifest,
                        &r.destination,
                        &r.expected,
                        &r.owners,
                        &r.acked,
                    ) {
                        Ok(()) => {
                            let expected = r.expected.clone();
                            let owners = r.owners.clone();
                            let acked = r.acked.clone();
                            r.runs.push((manifest, expected, owners, acked));
                        }
                        Err(e) => {
                            r.failure = Some(e);
                        }
                    }
                } else {
                    r.retries += 1;
                }
                r.expected.clear();
                r.cursor.clear();
                r.listed.clear();
            } else {
                if r.cursor.is_empty() {
                    r.acked = acked.to_vec();
                }
                r.owners = owners.clone();
                let page: Vec<_> = store
                    .lock()
                    .unwrap()
                    .keys()
                    .filter_map(|k| k.strip_prefix(PREFIX))
                    .filter(|k| *k > r.cursor.as_str())
                    .take(2)
                    .map(str::to_owned)
                    .collect();
                r.pages += 1;
                if let Some(last) = page.last() {
                    r.cursor = last.clone();
                    for name in &page {
                        if let Some(body) = owner_body(name, owners) {
                            r.expected.insert(name.clone(), body);
                        }
                    }
                    r.listed.extend(page);
                } else if !r.listed.is_empty() {
                    let mut keys = std::mem::take(&mut r.listed);
                    if broken == Unsafe::SplitListing
                        && let Some(k) = keys.iter().find(|k| k.starts_with("wal/")).cloned()
                    {
                        keys.retain(|key| *key != k);
                    }
                    r.fetching = Some(keys);
                }
            }
        }),
    );
    Arc::try_unwrap(reader).ok().unwrap().into_inner().unwrap()
}
fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}
#[test]
fn concurrent_archive_reader() {
    let mut runs = 0;
    let mut retries = 0;
    let mut retry_free = 0;
    for seed in 0..seeds() {
        let r = run(seed, Unsafe::None);
        assert_eq!(r.failure, None, "seed {seed}");
        assert!(
            r.pages > 0 && r.gets > 0 && !r.runs.is_empty(),
            "vacuous seed {seed}: runs={}",
            r.runs.len()
        );
        // A retry needs the collector to delete an object between this
        // reader's LIST and its GET: a schedule can miss that window
        // (seed 1299: 91 consistent runs, 20 objects collected, none in
        // the window). About one seed in 2,000 does, so the coverage
        // requirement is a fraction, not every seed.
        if r.retries == 0 {
            retry_free += 1;
        }
        assert!(
            r.runs.iter().any(|(_, _, _, acked)| !acked.is_empty()),
            "no listing-time acknowledgements for seed {seed}"
        );
        runs += r.runs.len();
        retries += r.retries;
        for (manifest, expected, owners, acked) in &r.runs {
            oracle_history(manifest, &r.destination, expected, owners, acked).unwrap();
        }
    }
    assert!(runs > 0 && retries > 0, "runs={runs} retries={retries}");
    assert!(
        retry_free <= (seeds() / 100).max(1),
        "{retry_free} of {} seeds never retried",
        seeds()
    );
}
#[test]
fn seed_1299_backs_up_consistently() {
    // Found at 2,000 seeds: nothing is collected between a LIST and its
    // GET, so no retry runs; every backup is still consistent.
    let r = run(1299, Unsafe::None);
    assert_eq!(r.failure, None);
    assert!(r.runs.iter().any(|(_, _, _, acked)| !acked.is_empty()));
    for (manifest, expected, owners, acked) in &r.runs {
        oracle_history(manifest, &r.destination, expected, owners, acked).unwrap();
    }
}
#[test]
fn unsafe_readers_fail_for_the_expected_reason() {
    for (variant, reason) in [
        (Unsafe::OmitBoundary, Failure::Boundary),
        (Unsafe::TruncatedSnapshot, Failure::Content),
        (Unsafe::NoReplay, Failure::Body),
        (Unsafe::NoRetry, Failure::Retry),
        (Unsafe::Prune, Failure::Missing),
        (Unsafe::SplitListing, Failure::Split),
    ] {
        assert!(
            (0..seeds()).any(|seed| run(seed, variant).failure == Some(reason.clone())),
            "{variant:?}: {reason:?}"
        );
    }
}
#[test]
fn oracle_checks_snapshot_boundary_and_segment_bodies() {
    let id = SegmentId {
        generation: 1,
        txid: 2,
    };
    let segment = wasmkeep_cluster::replication::Segment {
        id,
        parent_generation: 1,
    };
    let keys = [snapshot_key(id), segment_key(&segment)];
    let expected = BTreeMap::from([
        (keys[0].clone(), b"1:2".to_vec()),
        (keys[1].clone(), b"reset".to_vec()),
    ]);
    let manifest = BTreeMap::from([
        (keys[0].clone(), "snapshot".into()),
        (keys[1].clone(), "boundary".into()),
    ]);
    let destination = BTreeMap::from([
        ("snapshot".into(), b"1:2".to_vec()),
        ("boundary".into(), b"reset".to_vec()),
    ]);
    oracle(&manifest, &destination, &expected).unwrap();
    let next = segment_key(&wasmkeep_cluster::replication::Segment {
        id: SegmentId {
            generation: 1,
            txid: 3,
        },
        parent_generation: 1,
    });
    let mut extended = manifest.clone();
    extended.insert(next.clone(), "next".into());
    let mut bodies = destination.clone();
    bodies.insert("next".into(), b"x".to_vec());
    let mut truth = expected.clone();
    truth.insert(next, b"x".to_vec());
    oracle(&extended, &bodies, &truth).unwrap();
    bodies.insert("next".into(), b"corrupt".to_vec());
    assert_eq!(oracle(&extended, &bodies, &truth), Err(Failure::Body));
    for reference in ["snapshot", "boundary"] {
        let mut damaged = destination.clone();
        damaged.insert(reference.into(), b"corrupt".to_vec());
        assert_eq!(oracle(&manifest, &damaged, &expected), Err(Failure::Body));
    }
    let mut missing = manifest.clone();
    missing.remove(&keys[1]);
    assert_eq!(
        oracle(&missing, &destination, &expected),
        Err(Failure::Boundary)
    );
}

fn reference(key: &str, body: &[u8], broken: Unsafe) -> String {
    if broken == Unsafe::SourceKey {
        key.to_owned()
    } else {
        format!("{key}:{body:?}")
    }
}
#[test]
fn source_key_reuse_across_incarnations_is_content_failure() {
    let key = snapshot_key(SegmentId {
        generation: 1,
        txid: 1,
    });
    for variant in [Unsafe::None, Unsafe::SourceKey] {
        let mut destination = BTreeMap::new();
        let mut runs = Vec::new();
        for body in [b"1:1".to_vec(), b"1:1,1:2".to_vec()] {
            let reference = reference(&key, &body, variant);
            destination.entry(reference.clone()).or_insert(body.clone());
            runs.push((
                BTreeMap::from([(key.clone(), reference)]),
                BTreeMap::from([(key.clone(), body)]),
            ));
        }
        assert_eq!(oracle(&runs[0].0, &destination, &runs[0].1), Ok(()));
        assert_eq!(
            oracle(&runs[1].0, &destination, &runs[1].1),
            if variant == Unsafe::None {
                Ok(())
            } else {
                Err(Failure::Content)
            }
        );
    }
}

// Owner state is supplied separately from all reader LIST/GET results.
type OwnerHistory = BTreeMap<SegmentId, (std::collections::BTreeSet<SegmentId>, bool)>;
fn owner_body(name: &str, owners: &OwnerHistory) -> Option<Vec<u8>> {
    let listing = Listing::from_keys([name]);
    if let Some(id) = listing.snapshots.first() {
        let (positions, _) = owners.get(id)?;
        Some(
            positions
                .iter()
                .map(|p| format!("{}:{}", p.generation, p.txid))
                .collect::<Vec<_>>()
                .join(",")
                .into_bytes(),
        )
    } else {
        let (_, reset) = owners.get(&listing.segments.first()?.id)?;
        Some(if *reset {
            b"reset".to_vec()
        } else {
            b"x".to_vec()
        })
    }
}
fn oracle_history(
    manifest: &Manifest,
    destination: &Bodies,
    expected: &Bodies,
    owners: &OwnerHistory,
    acked: &[SegmentId],
) -> Result<(), Failure> {
    oracle(manifest, destination, expected)?;
    let listing = Listing::from_keys(manifest.keys().map(String::as_str));
    let head = plan_restore(&listing).map_err(|_| Failure::Split)?.head();
    let positions: std::collections::BTreeSet<_> = listing
        .snapshots
        .iter()
        .copied()
        .chain(listing.segments.iter().map(|s| s.id))
        .collect();
    for target in positions {
        let subset = Listing {
            snapshots: listing
                .snapshots
                .iter()
                .copied()
                .filter(|p| *p <= target)
                .collect(),
            segments: listing
                .segments
                .iter()
                .copied()
                .filter(|s| s.id <= target)
                .collect(),
        };
        let plan = if subset.snapshots.contains(&target) {
            wasmkeep_cluster::replication::Plan {
                snapshot: Some(target),
                segments: vec![],
            }
        } else {
            let Ok(plan) = plan_restore(&subset) else {
                continue;
            };
            if plan.head() != target {
                continue;
            }
            plan
        };
        let mut restored = std::collections::BTreeSet::new();
        if let Some(id) = plan.snapshot {
            let body = destination
                .get(manifest.get(&snapshot_key(id)).ok_or(Failure::Missing)?)
                .ok_or(Failure::Missing)?;
            let body = std::str::from_utf8(body).map_err(|_| Failure::Body)?;
            for position in body.split(',').filter(|p| !p.is_empty()) {
                let (g, t) = position.split_once(':').ok_or(Failure::Body)?;
                restored.insert(SegmentId {
                    generation: g.parse().map_err(|_| Failure::Body)?,
                    txid: t.parse().map_err(|_| Failure::Body)?,
                });
            }
        }
        for id in plan.segments {
            let segment = listing
                .segments
                .iter()
                .find(|s| s.id == id)
                .ok_or(Failure::Missing)?;
            let body = destination
                .get(
                    manifest
                        .get(&segment_key(segment))
                        .ok_or(Failure::Missing)?,
                )
                .ok_or(Failure::Missing)?;
            if body == b"reset" {
                restored.clear();
            }
            restored.insert(id);
        }
        let (truth, _) = owners.get(&target).ok_or(Failure::Content)?;
        if restored != *truth {
            return Err(Failure::Content);
        }
        if target == head {
            // Listing-time acknowledged writes must survive, except those
            // removed by a reset on this independently reconstructed chain.
            let reset = truth
                .iter()
                .filter(|id| owners.get(id).is_some_and(|(_, r)| *r))
                .max();
            for id in acked {
                if reset.is_none_or(|reset| id > reset) && !restored.contains(id) {
                    return Err(Failure::Content);
                }
            }
        }
    }
    Ok(())
}
#[test]
fn rereview_truncated_valid_snapshot_is_not_owner_history() {
    let first = SegmentId {
        generation: 1,
        txid: 1,
    };
    let head = SegmentId {
        generation: 1,
        txid: 2,
    };
    let name = snapshot_key(head);
    let manifest = BTreeMap::from([(name.clone(), "body".into())]);
    let bodies = BTreeMap::from([("body".into(), b"1:2".to_vec())]);
    // Both the GET and archive agree, but the owner acknowledged two writes.
    let fetched = BTreeMap::from([(name, b"1:2".to_vec())]);
    let owners = BTreeMap::from([(head, ([first, head].into(), false))]);
    assert_eq!(
        oracle_history(&manifest, &bodies, &fetched, &owners, &[first, head]),
        Err(Failure::Content)
    );
}
