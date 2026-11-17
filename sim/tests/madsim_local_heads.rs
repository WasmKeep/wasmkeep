//! ADR 0035, section 2, under madsim: a node's local evidence of the head
//! its ended holds froze (`wasmkeep_cluster::local_heads`, the server's
//! registry), carried by same-process re-acquisitions through the real
//! `Owners::acquire_prepared` and its preparation hook, over a bucket
//! whose requests are delayed and whose replies may be lost.
//!
//! The node is modelled as the server keeps it (`cluster/lease_hold.rs`):
//! live hold cells, ended holds frozen with `hold_cell::end` and
//! registered before they leave the live set, pending releases, one
//! parked hold, unresolved acquisitions resolved by `Owners::resolve`.
//! Nothing is written to the record but version 1 bytes: the requirement
//! lives only here and in the hold that inherits it.
//!
//! Cases (the ADR's numbering), one chosen per seed:
//!
//! - **2**: re-acquire while the release of H is queued, in flight or
//!   pending (a lapse), and after it landed; the new hold carries H unless
//!   the release had landed before the acquisition read the evidence.
//! - **3**: park at H, wake with an acquisition that lands but loses its
//!   reply; the resolver and the parked release run in either order; the
//!   next acquisition carries H.
//! - **16**: a lapse with H retained, another node takes over and releases
//!   at a higher generation, then the original process re-acquires: H is
//!   retired, not carried.
//! - **17**: `end_all` races an acquisition of the same key: the
//!   acquisition sees the live hold (and waits) or the frozen head, never
//!   neither.
//!
//! - **7**, with the recorded-head format enabled: a release whose reply
//!   is lost, sent again; another node inheriting H from the record
//!   alone; the first release sent late on its old version; the original
//!   process re-acquiring: every record carries H, complete, with its
//!   stamp, and no late release overwrites a newer hold.
//!
//! An unverified hold re-freezes what it inherited, stamp unchanged.
//! Broken variants must each be caught: dropping H on re-acquire,
//! keeping superseded evidence after a remote takeover, draining the live
//! set before freezing, and restamping an inherited floor with each new
//! generation. Every case must be exercised.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::sleep;
use wasmkeep_cluster::hold_cell;
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use wasmkeep_cluster::local_heads::{LocalHeads, Requirement};
use wasmkeep_cluster::node_lease::{
    AcquireError, Hold, NodeLeaseObject, NodeState, OwnerRecord, Owners, Released,
};
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_sim::rng::Rng;

const KEY: &str = "owners/t/c/a";
const EPOCH_MS: u64 = 1_000_000;

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

thread_local! {
    static START: std::cell::Cell<Option<madsim::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy)]
struct SimClock;

impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        let start = START.with(std::cell::Cell::get).expect("started");
        EPOCH_MS + u64::try_from(start.elapsed().as_millis()).unwrap_or(0)
    }
}

/// Faults aimed at the next request of a kind.
#[derive(Default)]
struct Faults {
    /// The next owner-record replace is applied, its reply lost.
    lose_replace_reply: bool,
    /// The next get of the owner record fails.
    fail_get: bool,
}

/// Key -> (body, version).
type Objects = Arc<Mutex<BTreeMap<String, (Vec<u8>, u64)>>>;

/// An in-process bucket: each request takes 0..20 ms.
#[derive(Clone)]
struct Mem {
    objects: Objects,
    faults: Arc<Mutex<Faults>>,
    seed: Arc<Mutex<Rng>>,
}

impl Mem {
    fn new(seed: u64) -> Self {
        Self {
            objects: Arc::default(),
            faults: Arc::default(),
            seed: Arc::new(Mutex::new(Rng::new(seed ^ 0xb0c))),
        }
    }

    async fn pause(&self) {
        let ms = self.seed.lock().unwrap().below(20);
        sleep(Duration::from_millis(ms)).await;
    }

    fn record(&self) -> Option<OwnerRecord> {
        self.objects
            .lock()
            .unwrap()
            .get(KEY)
            .and_then(|(b, _)| OwnerRecord::decode(b))
    }

    fn put(&self, key: &str, body: Vec<u8>) -> Version {
        let mut o = self.objects.lock().unwrap();
        let v = o.values().map(|(_, v)| *v).max().unwrap_or(0) + 1;
        o.insert(key.to_owned(), (body, v));
        Version(v.to_string())
    }
}

impl Bucket for Mem {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.pause().await;
        if key == KEY && std::mem::take(&mut self.faults.lock().unwrap().fail_get) {
            return Err(BucketError::Unavailable("no answer".into()));
        }
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, v)| (b.clone(), Version(v.to_string()))))
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.pause().await;
        if self.objects.lock().unwrap().contains_key(key) {
            return Err(BucketError::Conflict);
        }
        Ok(self.put(key, body))
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        self.pause().await;
        let found = self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(_, v)| v.to_string());
        if found.as_deref() != Some(expected.0.as_str()) {
            return Err(BucketError::Conflict);
        }
        let v = self.put(key, body);
        if key == KEY && std::mem::take(&mut self.faults.lock().unwrap().lose_replace_reply) {
            return Err(BucketError::Unavailable("reply lost".into()));
        }
        Ok(v)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Breaks {
    drop_local: bool,
    keep_superseded: bool,
    drain_before_freeze: bool,
    restamp: bool,
}

type Cell = Arc<Mutex<Option<Hold>>>;

/// One process, as the server keeps its holds.
struct Node {
    owners: Owners<Mem, SimClock>,
    live: Mutex<Option<Cell>>,
    local: Mutex<LocalHeads>,
    pending: Mutex<Vec<Hold>>,
    parked: Mutex<Option<Hold>>,
    unresolved: Mutex<Vec<(u64, Option<OwnerRecord>)>>,
    breaks: Breaks,
    /// A release of the actor landed (its entry may have ended).
    landed: Mutex<bool>,
    /// Whether one had landed when the last acquisition prepared.
    landed_at_prepare: Mutex<bool>,
}

impl Node {
    fn register(&self, hold: &Hold) {
        if self.breaks.restamp && hold.head.acknowledged.is_none() {
            let mut h = hold.clone();
            if let Some(r) = h.head.inherited.as_mut() {
                r.set_at = h.generation;
            }
            self.local.lock().unwrap().register(&h);
        } else {
            self.local.lock().unwrap().register(hold);
        }
    }

    /// `LeaseHold::acquire_with`.
    async fn acquire(&self) -> Result<Cell, AcquireError> {
        let mut prepared = false;
        let prepare = |record: Option<&OwnerRecord>, attempted: u64| {
            prepared = true;
            *self.landed_at_prepare.lock().unwrap() = *self.landed.lock().unwrap();
            let live = self.live.lock().unwrap();
            if live.as_ref().is_some_and(|c| c.lock().unwrap().is_some()) {
                return Err(AcquireError::LocalHold);
            }
            let mut local = self.local.lock().unwrap();
            let carried = local
                .prepare(KEY, record, attempted, "a", "i1")
                .zip(local.get(KEY).map(|e| e.generation));
            if self.breaks.keep_superseded && carried.is_none() {
                // Broken: a retired entry is not retired.
                return Ok(KEPT.with(|k| *k.borrow()).map(|r| (r, u64::MAX)));
            }
            Ok(if self.breaks.drop_local {
                None
            } else {
                carried
            })
        };
        let acquired = self
            .owners
            .acquire_prepared(KEY, || true, async || Ok(1), prepare)
            .await;
        if matches!(acquired, Ok(_) | Err(AcquireError::Held { .. })) {
            *self.parked.lock().unwrap() = None;
        }
        match acquired {
            Ok(hold) => {
                let generation = hold.generation;
                let cell = Arc::new(Mutex::new(Some(hold)));
                *self.live.lock().unwrap() = Some(Arc::clone(&cell));
                self.local.lock().unwrap().transferred(KEY, generation);
                Ok(cell)
            }
            Err(AcquireError::Unresolved { generation, before }) => {
                self.unresolved
                    .lock()
                    .unwrap()
                    .push((generation, before.map(|b| *b)));
                Err(AcquireError::Unavailable)
            }
            Err(e) => {
                if prepared {
                    self.local.lock().unwrap().finished(KEY);
                }
                Err(e)
            }
        }
    }

    /// The hold's own take (drop, explicit release): frozen and registered
    /// before it leaves the live set.
    fn take(&self, cell: &Cell) -> Option<Hold> {
        let mut live = self.live.lock().unwrap();
        let hold = hold_cell::end(cell);
        if let Some(h) = &hold {
            self.register(h);
        }
        if live.as_ref().is_some_and(|c| Arc::ptr_eq(c, cell)) {
            *live = None;
        }
        hold
    }

    /// `Pending::end_all(true)`: every live hold into a pending release.
    async fn end_all(&self) {
        if self.breaks.drain_before_freeze {
            let cell = self.live.lock().unwrap().take();
            sleep(Duration::from_millis(20)).await;
            if let Some(cell) = cell
                && let Some(hold) = hold_cell::end(&cell)
            {
                self.register(&hold);
                self.pending.lock().unwrap().push(hold);
            }
            return;
        }
        let mut live = self.live.lock().unwrap();
        if let Some(cell) = live.take()
            && let Some(hold) = hold_cell::end(&cell)
        {
            self.register(&hold);
            self.pending.lock().unwrap().push(hold);
        }
    }

    fn park(&self, cell: &Cell) {
        if let Some(hold) = self.take(cell) {
            *self.parked.lock().unwrap() = Some(hold);
        }
    }

    async fn release(&self, hold: &Hold) -> Released {
        let r = self.owners.release(hold).await;
        if r == Released::Released {
            *self.landed.lock().unwrap() = true;
            self.local
                .lock()
                .unwrap()
                .released(&hold.key, hold.generation);
        }
        r
    }

    async fn retry_pending(&self) {
        let entries = std::mem::take(&mut *self.pending.lock().unwrap());
        for hold in entries {
            if matches!(
                self.release(&hold).await,
                Released::Pending | Released::Unchanged
            ) {
                self.pending.lock().unwrap().push(hold);
            }
        }
    }

    async fn resolve(&self) {
        let entries = std::mem::take(&mut *self.unresolved.lock().unwrap());
        for (generation, before) in entries {
            match self
                .owners
                .resolve(KEY, generation, before.as_ref(), false)
                .await
            {
                Released::Pending | Released::Unchanged => {
                    self.unresolved.lock().unwrap().push((generation, before));
                }
                Released::Released | Released::Superseded => {
                    self.local.lock().unwrap().finished(KEY);
                }
            }
        }
    }

    async fn release_parked(&self) {
        let parked = self.parked.lock().unwrap().take();
        if let Some(hold) = parked {
            let _ = self.release(&hold).await;
        }
    }
}

thread_local! {
    /// What the broken "keep superseded" variant hands back after a
    /// retirement: the requirement it should have dropped.
    static KEPT: std::cell::RefCell<Option<Requirement>> = const { std::cell::RefCell::new(None) };
}

fn h_at(txid: u64) -> Position {
    Position::Known {
        generation: 1,
        txid,
        checksum: txid * 31,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    Queued,
    Pending,
    Parked,
    LostWake,
    Remote,
    Race,
    V2Release,
}

#[derive(Debug, Default)]
struct Outcome {
    /// A violated rule, if any.
    violation: Option<String>,
}

fn simulate(seed: u64, breaks: Breaks, case: Case) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        START.with(|s| s.set(Some(madsim::time::Instant::now())));
        KEPT.with(|k| *k.borrow_mut() = None);
        let mut rng = Rng::new(seed ^ 0x16);
        let mem = Mem::new(seed);
        let cfg = LeaseConfig::new(1_000, 500, 200, 300).unwrap();
        let node = Rc::new(Node {
            owners: Owners::new(mem.clone(), SimClock, "a", "i1", "nodes/", cfg),
            live: Mutex::new(None),
            local: Mutex::new(LocalHeads::default()),
            pending: Mutex::new(Vec::new()),
            parked: Mutex::new(None),
            unresolved: Mutex::new(Vec::new()),
            breaks,
            landed: Mutex::new(false),
            landed_at_prepare: Mutex::new(false),
        });
        // Case 7 runs with the recorded-head format enabled.
        node.owners.set_recorded_head(case == Case::V2Release);
        let mut out = Outcome::default();
        // A first hold, verified, that acknowledges up to H.
        let cell = node.acquire().await.unwrap();
        hold_cell::verify(&cell, || true, Position::Empty).unwrap();
        let txid = 1 + rng.below(40);
        hold_cell::acknowledge(&cell, || true, Some(h_at(txid))).unwrap();
        let g1 = cell.lock().unwrap().as_ref().unwrap().generation;
        let want = Requirement {
            head: h_at(txid),
            set_at: g1,
            completeness: wasmkeep_cluster::recorded_head::Completeness::Complete,
        };
        KEPT.with(|k| *k.borrow_mut() = Some(want));
        let fail = |out: &mut Outcome, why: String| {
            out.violation.get_or_insert(why);
        };
        let carried_of = |c: &Cell| c.lock().unwrap().as_ref().and_then(|h| h.head.inherited);
        match case {
            Case::Queued => {
                // Dropped: the release task runs at a seeded moment,
                // possibly landing before the re-acquire reads.
                let hold = node.take(&cell).unwrap();
                let n = Rc::clone(&node);
                let delay = rng.below(60);
                let release = madsim::task::spawn_local(async move {
                    sleep(Duration::from_millis(delay)).await;
                    let _ = n.release(&hold).await;
                });
                sleep(Duration::from_millis(rng.below(60))).await;
                match node.acquire().await {
                    Ok(c) => {
                        let landed_before = *node.landed_at_prepare.lock().unwrap();
                        let carried = carried_of(&c);
                        if !landed_before && carried != Some(want) {
                            fail(&mut out, format!("queued: carried {carried:?}, want H"));
                        }
                        count(Counter::Carried, carried == Some(want));
                    }
                    Err(_) => count(Counter::Refused, true),
                }
                release.await.unwrap();
            }
            Case::Pending | Case::Race => {
                if case == Case::Race {
                    // The lapse's end_all races the acquisition.
                    let n = Rc::clone(&node);
                    let wait = rng.below(25);
                    let ender = madsim::task::spawn_local(async move {
                        sleep(Duration::from_millis(wait)).await;
                        n.end_all().await;
                    });
                    let got = node.acquire().await;
                    ender.await.unwrap();
                    match got {
                        Ok(c) => {
                            let carried = carried_of(&c);
                            if carried != Some(want) {
                                fail(&mut out, format!("race: carried {carried:?}, want H"));
                            }
                            count(Counter::Carried, true);
                        }
                        Err(AcquireError::LocalHold) => count(Counter::SawLive, true),
                        Err(_) => count(Counter::Refused, true),
                    }
                    return out;
                }
                node.end_all().await;
                if rng.chance(50) {
                    node.retry_pending().await;
                }
                let c = node.acquire().await.unwrap();
                let landed_before = *node.landed_at_prepare.lock().unwrap();
                let carried = carried_of(&c);
                if !landed_before && carried != Some(want) {
                    fail(&mut out, format!("pending: carried {carried:?}, want H"));
                }
                // Never verified: ending again re-freezes H, stamp kept.
                node.end_all().await;
                if !landed_before {
                    let c = node.acquire().await.unwrap();
                    let again = carried_of(&c);
                    if again != Some(want) {
                        fail(&mut out, format!("re-ended unverified: {again:?}, want H"));
                    }
                }
                count(Counter::Carried, carried == Some(want));
            }
            Case::Parked | Case::LostWake => {
                node.park(&cell);
                if case == Case::LostWake {
                    // The wake lands; its reply and its read-back are lost.
                    {
                        let mut f = mem.faults.lock().unwrap();
                        f.lose_replace_reply = true;
                    }
                    let got = {
                        // Fail the read-back that follows the lost reply.
                        let m = mem.clone();
                        let watcher = madsim::task::spawn_local(async move {
                            loop {
                                if !m.faults.lock().unwrap().lose_replace_reply {
                                    m.faults.lock().unwrap().fail_get = true;
                                    return;
                                }
                                sleep(Duration::from_millis(1)).await;
                            }
                        });
                        let got = node.acquire().await;
                        watcher.abort();
                        mem.faults.lock().unwrap().fail_get = false;
                        got
                    };
                    if got.is_ok() {
                        count(Counter::Refused, true);
                        return out;
                    }
                    count(Counter::LostWake, true);
                    // The resolver and the parked release, in either order.
                    if rng.chance(50) {
                        node.resolve().await;
                        node.release_parked().await;
                    } else {
                        node.release_parked().await;
                        node.resolve().await;
                    }
                }
                let c = node.acquire().await.unwrap();
                let carried = carried_of(&c);
                if carried != Some(want) {
                    fail(&mut out, format!("{case:?}: carried {carried:?}, want H"));
                }
                count(Counter::Carried, carried == Some(want));
            }
            Case::Remote => {
                // A lapse keeps H in a pending release; this node's lease
                // is then fenced, and another node takes over, writes and
                // releases at a higher generation.
                node.end_all().await;
                let fenced = NodeLeaseObject {
                    state: NodeState::Fenced,
                    instance: Some("i1".into()),
                    written_at_ms: 0,
                };
                mem.put("nodes/a", fenced.encode());
                let b = Owners::new(mem.clone(), SimClock, "b", "j1", "nodes/", cfg);
                let theirs = b.acquire(KEY, || true, async || Ok(1)).await.unwrap();
                assert_eq!(b.release(&theirs).await, Released::Released);
                if rng.chance(50) {
                    node.retry_pending().await;
                }
                let c = node.acquire().await.unwrap();
                let carried = carried_of(&c);
                if carried.is_some() {
                    fail(&mut out, format!("remote: kept superseded {carried:?}"));
                }
                count(Counter::Retired, carried.is_none());
            }
            Case::V2Release => {
                // Case 7. The release lands with its reply lost; its retry
                // meets its own landed write. The record carries H,
                // complete, stamped with the hold that froze it.
                let hold = node.take(&cell).unwrap();
                mem.faults.lock().unwrap().lose_replace_reply = true;
                if node.release(&hold).await != Released::Pending {
                    count(Counter::Refused, true);
                    return out;
                }
                let _ = node.release(&hold).await;
                let stored = |what: &str, out: &mut Outcome| {
                    let r = mem.record().unwrap();
                    let carried = Requirement::of_record(&r);
                    if carried != Some(want) {
                        out.violation.get_or_insert(format!(
                            "{what}: the record carries {carried:?}, want H"
                        ));
                    }
                    r
                };
                stored("lost reply", &mut out);
                // Another node takes the actor: it inherits H from the
                // record alone, and releases it unverified, stamp kept.
                let b = Owners::new(mem.clone(), SimClock, "b", "j1", "nodes/", cfg);
                b.set_recorded_head(true);
                let theirs = b.acquire(KEY, || true, async || Ok(1)).await.unwrap();
                if theirs.head.inherited != Some(want) {
                    fail(&mut out, format!("b inherited {:?}", theirs.head.inherited));
                }
                // The first release, sent again late on its old version,
                // never overwrites the newer hold.
                let late = node.owners.release(&hold).await;
                let held = mem.record().unwrap();
                if held.holder.is_none() || late == Released::Released {
                    fail(&mut out, "a late release overwrote a newer hold".into());
                }
                assert_eq!(b.release(&theirs).await, Released::Released);
                let after = stored("after b", &mut out);
                if after.generation != theirs.generation {
                    fail(&mut out, "the record went back".into());
                }
                // This process again: its own entry is retired by b's
                // release, the record's floor carried instead.
                let c = node.acquire().await.unwrap();
                let carried = carried_of(&c);
                if carried != Some(want) {
                    fail(&mut out, format!("re-acquired with {carried:?}, want H"));
                }
                count(Counter::Carried, carried == Some(want));
            }
        }
        out
    })
}

#[derive(Clone, Copy)]
enum Counter {
    Carried,
    Refused,
    SawLive,
    LostWake,
    Retired,
}

thread_local! {
    static COUNTS: std::cell::RefCell<[u64; 5]> = const { std::cell::RefCell::new([0; 5]) };
}

fn count(c: Counter, yes: bool) {
    if yes {
        COUNTS.with(|k| k.borrow_mut()[c as usize] += 1);
    }
}

fn case_of(seed: u64) -> Case {
    [
        Case::Queued,
        Case::Pending,
        Case::Parked,
        Case::LostWake,
        Case::Remote,
        Case::Race,
        Case::V2Release,
    ][usize::try_from(seed % 7).unwrap()]
}

#[test]
fn local_evidence_survives_every_same_process_hand_over() {
    COUNTS.with(|k| *k.borrow_mut() = [0; 5]);
    for seed in 0..seeds() {
        let case = case_of(seed);
        if let Some(why) = simulate(seed, Breaks::default(), case).violation {
            panic!("seed {seed} ({case:?}): {why}");
        }
    }
    let [carried, refused, saw_live, lost_wake, retired] = COUNTS.with(|k| *k.borrow());
    eprintln!(
        "{} seeds: {carried} carried, {refused} refused, {saw_live} saw the live hold, \
         {lost_wake} lost wakes, {retired} retired",
        seeds()
    );
    let floor = (seeds() / 60).max(1);
    assert!(carried >= floor, "carried {carried}");
    assert!(lost_wake >= floor, "lost wakes {lost_wake}");
    assert!(retired >= floor, "retired {retired}");
    assert!(saw_live >= floor, "saw the live hold {saw_live}");
}

fn caught(breaks: Breaks, cases: &[Case]) -> u64 {
    (0..seeds())
        .filter(|&seed| cases.contains(&case_of(seed)))
        .filter(|&seed| simulate(seed, breaks, case_of(seed)).violation.is_some())
        .count() as u64
}

#[test]
fn every_broken_variant_is_caught() {
    let floor = (seeds() / 60).max(1);
    for (name, breaks, cases) in [
        (
            "drop local H on re-acquire",
            Breaks {
                drop_local: true,
                ..Breaks::default()
            },
            &[Case::Queued, Case::Pending, Case::Parked, Case::LostWake][..],
        ),
        (
            "keep superseded evidence",
            Breaks {
                keep_superseded: true,
                ..Breaks::default()
            },
            &[Case::Remote][..],
        ),
        (
            "drain before freezing",
            Breaks {
                drain_before_freeze: true,
                ..Breaks::default()
            },
            &[Case::Race][..],
        ),
        (
            "restamp an inherited floor",
            Breaks {
                restamp: true,
                ..Breaks::default()
            },
            &[Case::Pending][..],
        ),
    ] {
        let n = caught(breaks, cases);
        eprintln!("{name}: caught on {n} of {} seeds", seeds());
        assert!(n >= floor, "{name}: caught on {n}, want {floor}");
    }
}
