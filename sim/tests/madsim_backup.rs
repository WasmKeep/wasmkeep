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

// ADR 0042, Tests, "Other sims": a backup reader racing idle passes, and
// ADR 0033's bound on its attempts. The replication model runs idle
// compaction alone (no periodic collection, no deletes: every delete is a
// pass's), its passes deleting only what the retention window allows
// (`IDLE_WINDOW_MS`). A reader with no lease lists the actor at once,
// plans with the real planner, and reads the plan (the snapshot, the
// segment at its position, the segments after it: what `--history head`
// copies), its reads spread over its deadline and its oldest object read
// last, while the owners and passes keep running under the model's
// faults. A read that finds an object gone starts the actor again (list,
// plan, read); the reader gives the actor up after
// `IDLE_READER_ATTEMPTS`.
//
// ADR 0033, section 1: with each attempt within `d` and `2d + ε < W`, a
// reader needs at most two attempts. Here `d` is held below
// `IDLE_READER_DEADLINE_MS` (asserted), and `W` is above twice it plus
// the skew bound. Oracles:
// - no reader gives up within two attempts;
// - every completed backup is the owners' history at its head, with every
//   write acknowledged before its listing (`oracle_history`; mutation: a
//   reader that skips an object it finds gone instead of starting again);
// - the replication model's own checks, per pass and final.
// In this model the race is rare; the focused model further down
// reaches it on most seeds and checks the first oracle against its
// mutations (passes ignoring the window, a window below the bound).
use replication::{IDLE_WINDOW_MS, MAX_SKEW_MS, idle_backup_simulation};

/// The attempts the reader makes for one backup: the bound under test.
const IDLE_READER_ATTEMPTS: usize = 2;
/// The longest one attempt may take, listing to last read (ms).
const IDLE_READER_DEADLINE_MS: u64 = 1_800;
/// The observer's tick (`simulate_observed` calls it every 50 ms).
const TICK_MS: u64 = 50;
const _: () = assert!(2 * IDLE_READER_DEADLINE_MS + MAX_SKEW_MS < IDLE_WINDOW_MS);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdleBreak {
    None,
    /// The reader skips an object it finds gone, keeping the rest.
    SkipMissing,
}

#[derive(Default)]
struct IdleReader {
    tick: u64,
    /// The attempt in progress: when it listed, what is left to read.
    listed_at: u64,
    /// Ticks between two reads of the attempt.
    pace: u64,
    fetching: Option<Vec<String>>,
    attempt: usize,
    manifest: Manifest,
    destination: Bodies,
    expected: Bodies,
    owners: OwnerHistory,
    acked: Vec<SegmentId>,
    /// Backups completed, and those that needed a second attempt.
    completed: usize,
    second_attempts: usize,
    /// Reads that found an object of the plan gone (a pass's delete,
    /// landed between the listing and the read).
    misses: usize,
    /// Backups given up after `IDLE_READER_ATTEMPTS`.
    exhausted: usize,
    /// The longest attempt, listing to last read (ms).
    longest_ms: u64,
    failure: Option<Failure>,
}

impl IdleReader {
    fn start(
        &mut self,
        store: &replication::ObservedStore,
        acked: &[SegmentId],
        owners: &OwnerHistory,
    ) {
        let keys: Vec<String> = store
            .lock()
            .unwrap()
            .keys()
            .filter_map(|k| k.strip_prefix(PREFIX))
            .map(str::to_owned)
            .collect();
        let listing = Listing::from_keys(keys.iter().map(String::as_str));
        let Ok(plan) = plan_restore(&listing) else {
            return; // a missing link: list again at the next tick
        };
        if plan.head() == wasmkeep_cluster::replication::EMPTY {
            return; // nothing written yet
        }
        let segment = |id: SegmentId| listing.segments.iter().find(|s| s.id == id).copied();
        let mut wanted = Vec::new();
        if let Some(s) = plan.snapshot {
            wanted.push(snapshot_key(s));
            if let Some(boundary) = segment(s) {
                wanted.push(segment_key(&boundary));
            }
        }
        for id in &plan.segments {
            wanted.push(segment_key(&segment(*id).expect("planned segment listed")));
        }
        self.expected = wanted
            .iter()
            .filter_map(|k| Some((k.clone(), owner_body(k, owners)?)))
            .collect();
        self.owners = owners.clone();
        self.acked = acked.to_vec();
        self.manifest.clear();
        self.listed_at = self.tick;
        // As slow as the deadline allows: the reads spread over it, so
        // every attempt is as exposed to the passes as the bound permits.
        let ticks = IDLE_READER_DEADLINE_MS / TICK_MS;
        self.pace = (ticks / (wanted.len() as u64 + 1)).max(1);
        // Popped from the end: the plan's first object (its oldest, the
        // one a pass deletes) is read last, the most exposed order. The
        // bound does not depend on the order; `backup create` reads the
        // plan first to last.
        self.fetching = Some(wanted);
    }

    fn step(
        &mut self,
        store: &replication::ObservedStore,
        acked: &[SegmentId],
        owners: &OwnerHistory,
        broken: IdleBreak,
    ) {
        self.tick += 1;
        if self.failure.is_some() {
            return;
        }
        let Some(mut remaining) = self.fetching.take() else {
            self.start(store, acked, owners);
            return;
        };
        if !(self.tick - self.listed_at).is_multiple_of(self.pace) {
            self.fetching = Some(remaining);
            return;
        }
        if let Some(key) = remaining.pop() {
            let body = store
                .lock()
                .unwrap()
                .get(&format!("{PREFIX}{key}"))
                .map(|(b, _)| b.clone());
            match body {
                Some(body) => {
                    let reference = format!("{key}:{body:?}");
                    self.destination.entry(reference.clone()).or_insert(body);
                    self.manifest.insert(key, reference);
                }
                None => {
                    self.misses += 1;
                    if broken == IdleBreak::SkipMissing {
                        self.expected.remove(&key);
                    } else {
                        self.attempt += 1;
                        if self.attempt >= IDLE_READER_ATTEMPTS {
                            self.exhausted += 1;
                            self.attempt = 0;
                        }
                        return; // the next tick lists again
                    }
                }
            }
            self.fetching = Some(remaining);
            return;
        }
        // Every object of the plan read: the backup is done.
        let took = (self.tick - self.listed_at) * TICK_MS;
        self.longest_ms = self.longest_ms.max(took);
        let manifest = std::mem::take(&mut self.manifest);
        if let Err(e) = oracle_history(
            &manifest,
            &self.destination,
            &self.expected,
            &self.owners,
            &self.acked,
        ) {
            self.failure = Some(e);
            return;
        }
        self.completed += 1;
        if self.attempt == 1 {
            self.second_attempts += 1;
        }
        self.attempt = 0;
    }
}

struct IdleRun {
    reader: IdleReader,
    passes_deleted: u64,
    problem: Option<String>,
}

fn idle_run(seed: u64, broken: IdleBreak) -> IdleRun {
    let reader = Arc::new(Mutex::new(IdleReader::default()));
    let observer = reader.clone();
    let observed = idle_backup_simulation(
        seed,
        true,
        std::time::Duration::from_secs(60),
        Box::new(move |store, acked, owners| {
            observer.lock().unwrap().step(store, acked, owners, broken);
        }),
    );
    IdleRun {
        reader: Arc::try_unwrap(reader).ok().unwrap().into_inner().unwrap(),
        passes_deleted: observed.passes_deleted,
        problem: observed.problem,
    }
}

#[test]
fn a_backup_reader_racing_idle_passes_finishes_within_two_attempts() {
    let (mut completed, mut second, mut misses, mut deleted, mut longest) = (0, 0, 0, 0, 0);
    let mut empty = 0;
    for seed in 0..seeds() {
        let run = idle_run(seed, IdleBreak::None);
        assert_eq!(run.problem, None, "seed {seed}: replication");
        let r = run.reader;
        assert_eq!(r.failure, None, "seed {seed}: backup content");
        assert_eq!(r.exhausted, 0, "seed {seed}: a reader gave up");
        assert!(
            r.longest_ms <= IDLE_READER_DEADLINE_MS,
            "seed {seed}: an attempt took {} ms, over the deadline the window assumes",
            r.longest_ms
        );
        // A seed whose faults keep the actor from being written has
        // nothing to back up (seed 564 of 2,000).
        if r.completed == 0 {
            empty += 1;
        }
        completed += r.completed;
        second += r.second_attempts;
        misses += r.misses;
        deleted += run.passes_deleted;
        longest = longest.max(r.longest_ms);
    }
    eprintln!(
        "backup racing idle passes: {} seeds x 60 s: {completed} backups, {second} on a second \
         attempt after {misses} reads found a pass's delete, none gave up; {deleted} passes \
         deleted; longest attempt {longest} ms (deadline {IDLE_READER_DEADLINE_MS} ms, window \
         {IDLE_WINDOW_MS} ms)",
        seeds()
    );
    assert!(
        empty <= (seeds() / 100).max(1),
        "{empty} seeds backed nothing up"
    );
    // Non-vacuity: passes deleted, and some deleted under a reader. In this
    // model that race is rare (2 reads in 200 seeds): a plan the passes
    // delete from is usually read before the pass runs, or starts at a
    // periodic snapshot younger than the window. `madsim_backup`'s
    // focused model below reaches it on most seeds.
    assert!(deleted > 0, "no pass deleted");
    if seeds() >= 200 {
        assert!(misses > 0 && second > 0, "no race reached");
    }
}

#[test]
fn a_reader_skipping_an_object_a_pass_deleted_is_caught() {
    // The content oracle against its mutation, in the replication model:
    // a reader that skips an object it finds gone instead of starting
    // again. The race is rare there (see above), so over at least 200
    // seeds.
    let seeds = seeds().max(200);
    let skip = (0..seeds).find(|&seed| {
        idle_run(seed, IdleBreak::SkipMissing)
            .reader
            .failure
            .is_some()
    });
    eprintln!("a reader skipping a missing object: caught first at seed {skip:?}");
    assert!(
        skip.is_some(),
        "a reader skipping a missing object not caught"
    );
}

// The same race in a focused model, where it is common: one writer, no
// faults, discrete 10 ms steps. Each wake is a new generation that
// writes 1 to 6 segments (a periodic snapshot every 5), then hibernates:
// an idle snapshot at the head, and a pass 0 to 3 s later unless the
// actor woke first (its record changed), with `idle_collectable` of the
// released generation, deleting what is older than the window on its
// clock (skewed by up to the bound), each delete landing up to 400 ms
// after it was sent. The pass's delay is not the window here, which is
// more adversarial than the server (whose pass waits a window after the
// release): the bound rests on the objects' age alone. The reader is the
// one above, paced to its deadline and reading the plan's oldest object
// last, and its restore is compared with every position written up to
// its head (one writer: one chain).
struct Focused {
    completed: usize,
    second_attempts: usize,
    misses: usize,
    exhausted: usize,
    longest_ms: u64,
    passes_deleted: usize,
    failure: Option<String>,
}

fn decode(body: &[u8]) -> std::collections::BTreeSet<SegmentId> {
    std::str::from_utf8(body)
        .unwrap()
        .split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (g, t) = p.split_once(':').unwrap();
            SegmentId {
                generation: g.parse().unwrap(),
                txid: t.parse().unwrap(),
            }
        })
        .collect()
}

/// The reader's attempt in progress: when it listed, its pace, what is
/// left to read, what it read, the plan's head.
type Attempt = (u64, u64, Vec<String>, BTreeMap<String, Vec<u8>>, SegmentId);

/// One seed of the focused model: `window` in ms, `None` to ignore it.
fn focused(seed: u64, window: Option<u64>, skip_missing: bool) -> Focused {
    use wasmkeep_cluster::replication::{EMPTY, Segment, idle_collectable};
    const STEP: u64 = 10;
    const END: u64 = 60_000;
    let mut rng = wasmkeep_sim::rng::Rng::new(seed ^ 0x0042_0033);
    let skew = i64::try_from(rng.below(2 * MAX_SKEW_MS + 1)).unwrap()
        - i64::try_from(MAX_SKEW_MS).unwrap();
    let mut out = Focused {
        completed: 0,
        second_attempts: 0,
        misses: 0,
        exhausted: 0,
        longest_ms: 0,
        passes_deleted: 0,
        failure: None,
    };
    // The bucket: key -> (last modified, body).
    let mut store: BTreeMap<String, (u64, Vec<u8>)> = BTreeMap::new();
    let mut written: Vec<SegmentId> = Vec::new();
    let (mut generation, mut parent, mut since_snapshot) = (0u64, 0u64, 0u64);
    // The writer: awake with writes left (and when the next is due), or
    // resting until a wake.
    let (mut writes_left, mut next_write, mut wake_at) = (0u64, 0u64, 0u64);
    // A pass due: when, and the released generation.
    let mut pass: Option<(u64, u64)> = None;
    let mut landing: Vec<(u64, String)> = Vec::new();
    // The reader.
    let mut attempt = 0usize;
    let mut reading: Option<Attempt> = None;
    let snapshot_body = |written: &[SegmentId]| {
        written
            .iter()
            .map(|p| format!("{}:{}", p.generation, p.txid))
            .collect::<Vec<_>>()
            .join(",")
            .into_bytes()
    };
    let mut now = 0;
    while now < END {
        now += STEP;
        // Deletes landing.
        landing.retain(|(at, key)| {
            if *at <= now {
                store.remove(key);
                false
            } else {
                true
            }
        });
        // The writer.
        if writes_left == 0 && now >= wake_at {
            // A wake: a new generation (a pass not yet run finds its
            // record changed, and is dropped).
            pass = None;
            generation += 1;
            writes_left = 1 + rng.below(6);
            next_write = now + 20 + rng.below(150);
        }
        if writes_left > 0 && now >= next_write {
            let id = SegmentId {
                generation,
                txid: written.len() as u64 + 1,
            };
            let key = segment_key(&Segment {
                id,
                parent_generation: parent,
            });
            store.insert(key, (now, b"x".to_vec()));
            written.push(id);
            parent = generation;
            since_snapshot += 1;
            if since_snapshot == 5 {
                store.insert(snapshot_key(id), (now, snapshot_body(&written)));
                since_snapshot = 0;
            }
            writes_left -= 1;
            next_write = now + 20 + rng.below(150);
            if writes_left == 0 {
                // Hibernation: the idle snapshot at the head, the release,
                // the pass to come, and a rest.
                if since_snapshot > 0 {
                    store.insert(snapshot_key(id), (now, snapshot_body(&written)));
                    since_snapshot = 0;
                }
                pass = Some((now + rng.below(3_000), generation));
                wake_at = now
                    + if rng.chance(50) {
                        100 + rng.below(1_500)
                    } else {
                        3_000 + rng.below(5_000)
                    };
            }
        }
        if let Some((at, released)) = pass
            && now >= at
        {
            pass = None;
            let listing = Listing::from_keys(store.keys().map(String::as_str));
            if let Some((_, gone)) = idle_collectable(&listing, released) {
                let clock = now.saturating_add_signed(skew);
                let mut sent = 0;
                for key in gone {
                    let old = window.is_none_or(|w| store[&key].0.saturating_add(w) <= clock);
                    if old {
                        landing.push((now + rng.below(400), key));
                        sent += 1;
                    }
                }
                if sent > 0 {
                    out.passes_deleted += 1;
                }
            }
        }
        // The reader.
        let Some((listed_at, pace, mut wanted, mut manifest, head)) = reading.take() else {
            let listing = Listing::from_keys(store.keys().map(String::as_str));
            let Ok(plan) = plan_restore(&listing) else {
                continue;
            };
            if plan.head() == EMPTY {
                continue;
            }
            let segment = |id: SegmentId| listing.segments.iter().find(|s| s.id == id).copied();
            let mut keys = Vec::new();
            if let Some(s) = plan.snapshot {
                keys.push(snapshot_key(s));
                if let Some(boundary) = segment(s) {
                    keys.push(segment_key(&boundary));
                }
            }
            for id in &plan.segments {
                keys.push(segment_key(&segment(*id).unwrap()));
            }
            let pace = (IDLE_READER_DEADLINE_MS / STEP / (keys.len() as u64 + 1)).max(1);
            reading = Some((now, pace, keys, BTreeMap::new(), plan.head()));
            continue;
        };
        if !((now - listed_at) / STEP).is_multiple_of(pace) {
            reading = Some((listed_at, pace, wanted, manifest, head));
            continue;
        }
        if let Some(key) = wanted.pop() {
            match store.get(&key) {
                Some((_, body)) => {
                    manifest.insert(key, body.clone());
                }
                None => {
                    out.misses += 1;
                    if !skip_missing {
                        attempt += 1;
                        if attempt >= IDLE_READER_ATTEMPTS {
                            out.exhausted += 1;
                            attempt = 0;
                        }
                        continue; // list again at the next step
                    }
                }
            }
            reading = Some((listed_at, pace, wanted, manifest, head));
            continue;
        }
        out.longest_ms = out.longest_ms.max(now - listed_at);
        // What the copy restores, against every position up to its head.
        let listing = Listing::from_keys(manifest.keys().map(String::as_str));
        let restored = plan_restore(&listing)
            .ok()
            .filter(|p| p.head() == head)
            .map(|plan| {
                let mut positions = plan
                    .snapshot
                    .map(|s| decode(&manifest[&snapshot_key(s)]))
                    .unwrap_or_default();
                positions.extend(plan.segments);
                positions
            });
        let truth: std::collections::BTreeSet<_> = written
            .iter()
            .copied()
            .filter(|p| p.txid <= head.txid)
            .collect();
        if restored.as_ref() != Some(&truth) {
            out.failure = Some(format!("backup at {head:?} restores {restored:?}"));
            return out;
        }
        out.completed += 1;
        if attempt == 1 {
            out.second_attempts += 1;
        }
        attempt = 0;
    }
    out
}

#[test]
fn a_reader_racing_idle_passes_in_the_focused_model_needs_two_attempts_at_most() {
    let (mut completed, mut second, mut misses, mut deleted, mut raced) = (0, 0, 0, 0, 0);
    for seed in 0..seeds() {
        let r = focused(seed, Some(IDLE_WINDOW_MS), false);
        assert_eq!(r.failure, None, "seed {seed}");
        assert_eq!(r.exhausted, 0, "seed {seed}: a reader gave up");
        assert!(
            r.longest_ms <= IDLE_READER_DEADLINE_MS,
            "seed {seed}: {} ms",
            r.longest_ms
        );
        completed += r.completed;
        second += r.second_attempts;
        misses += r.misses;
        deleted += r.passes_deleted;
        raced += usize::from(r.second_attempts > 0);
    }
    eprintln!(
        "focused backup racing idle passes: {} seeds x 60 s: {completed} backups, {second} on a \
         second attempt (on {raced} seeds) after {misses} reads found a pass's delete, none gave \
         up; {deleted} passes deleted",
        seeds()
    );
    assert!(deleted > 0 && second > 0, "no race reached");
    assert!(
        raced as u64 >= seeds() / 4,
        "the race on only {raced} seeds"
    );
}

#[test]
fn the_focused_reader_oracles_catch_their_mutations() {
    let count = |window, skip| {
        let mut first = None;
        let mut caught = 0;
        for seed in 0..seeds() {
            let r = focused(seed, window, skip);
            if (skip && r.failure.is_some()) || (!skip && r.exhausted > 0) {
                first.get_or_insert(seed);
                caught += 1;
            }
        }
        (first, caught)
    };
    for (what, window, skip) in [
        ("passes ignoring the window", None, false),
        (
            "a window at the read deadline, below the bound",
            Some(IDLE_READER_DEADLINE_MS),
            false,
        ),
        (
            "a reader skipping a missing object",
            Some(IDLE_WINDOW_MS),
            true,
        ),
    ] {
        let (first, caught) = count(window, skip);
        eprintln!(
            "{what}: caught on {caught} of {} seeds, first {first:?}",
            seeds()
        );
        assert!(caught > 0, "{what}: not caught");
    }
}
