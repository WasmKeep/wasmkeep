//! `wasmkeep cluster migrate` (ADR 0022) under madsim: the production
//! migration (`wasmkeep_cluster::migrate`) moves a small bucket of the
//! layout before 0.4.0 into each app's root, against a bucket whose
//! requests interleave in a seeded order, with crashes, old binaries and
//! nodes that start in the middle.
//!
//! The bucket holds two tenants: `default` with no app (its state goes to
//! `default/default`), and `acme` with two apps on separate classes and a
//! class no app deploys (`--heir acme/blog`). Each actor has history (a
//! snapshot and a chain of segments), an ownership record, an old-lease
//! marker, an alarm entry and a move; one name is longer than the 200
//! bytes the segment encoding keeps, so its key segment is the lossy,
//! hashed form, which the migration must never decode.
//!
//! Cases, each for every seed:
//!
//! - **A crash after the i-th bucket request**, then a second run: the
//!   end state is the clean run's; every root's restore plan is the
//!   source's; every acknowledged transaction can be restored; every
//!   ownership record under a root is the one the actor had. Also for
//!   `--rollback`: the bare layout comes back byte for byte.
//! - **An old binary runs between two runs**: it takes an actor, bumping
//!   its generation, with a record of the same size, and stops. The second
//!   run copies the new record and segment: no generation is reused.
//! - **A node with a new id starts in the middle**: the run is refused or
//!   aborts, and nothing is committed.
//! - **Listings that leave out recent keys**: the run commits a correct
//!   state or aborts, never commits a wrong one; a later run converges.
//! - **An old node takes an actor between copy and barrier**: its
//!   tombstone's condition fails, the run aborts, and the next run copies
//!   the new generation.
//! - **`--rollback` after a node ran** is refused, and changes nothing.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_migrate --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Runtime;
use wasmkeep_cluster::alarm_index::{self, Entry};
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, Version};
use wasmkeep_cluster::migrate::{
    self, AppInfo, COMMIT_PREFIX, LOCK_KEY, MigrateError, Migrator, ObjectMeta, Options, Pause,
};
use wasmkeep_cluster::node_lease::{NodeLeaseObject, NodeState, OwnerRecord, encode_marker};
use wasmkeep_cluster::replication::{
    Listing, Segment, SegmentId, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_sim::rng::Rng;

/// The simulation's start, ms since the epoch.
const EPOCH_MS: u64 = 1_000_000_000;

/// One object: its body, version, and when it was written (ms).
#[derive(Debug, Clone)]
struct Obj {
    body: Vec<u8>,
    version: u64,
    written: u64,
}

type Map = BTreeMap<String, Obj>;
/// Something to do to the bucket just before a given request.
type Hook = Box<dyn FnOnce(&mut Map, u64) + Send>;

/// The simulated bucket.
struct Sim {
    objects: Mutex<Map>,
    /// Requests so far.
    ops: Mutex<u64>,
    /// Every request after this one fails, as after a crash of the run.
    crash_after: Mutex<Option<u64>>,
    /// Hooks by request number.
    hooks: Mutex<BTreeMap<u64, Vec<Hook>>>,
    /// Hide from listings the keys written less than this long ago, with
    /// this chance in percent (a listing that is not yet consistent).
    hide_recent: Mutex<(u64, u64)>,
    rng: Mutex<Rng>,
    next_version: Mutex<u64>,
    /// How many more listings leave out each recent key: drawn when a
    /// listing first meets it (0 to 2), counted down, and once a listing
    /// shows the key every later one does (the listing catches up).
    hidden_left: Mutex<BTreeMap<String, u64>>,
    /// The request numbers of the listings of `nodes/`.
    node_listings: Mutex<Vec<u64>>,
}

impl Sim {
    fn new(seed: u64, objects: Map) -> Self {
        Self {
            objects: Mutex::new(objects),
            ops: Mutex::new(0),
            crash_after: Mutex::new(None),
            hooks: Mutex::new(BTreeMap::new()),
            hide_recent: Mutex::new((0, 0)),
            rng: Mutex::new(Rng::new(seed)),
            next_version: Mutex::new(1_000),
            hidden_left: Mutex::new(BTreeMap::new()),
            node_listings: Mutex::new(Vec::new()),
        }
    }

    fn now() -> u64 {
        SimClock.now_ms()
    }

    /// Counts a request, runs its hooks, waits a seeded moment (so the
    /// run's concurrent requests interleave), and fails it after a crash.
    async fn op(&self) -> Result<(), BucketError> {
        let n = {
            let mut ops = self.ops.lock().unwrap();
            *ops += 1;
            *ops
        };
        let hooks = self.hooks.lock().unwrap().remove(&n).unwrap_or_default();
        for hook in hooks {
            hook(&mut self.objects.lock().unwrap(), Self::now());
        }
        let wait = self.rng.lock().unwrap().below(4);
        madsim::time::sleep(Duration::from_millis(wait)).await;
        if self.crash_after.lock().unwrap().is_some_and(|c| n > c) {
            return Err(BucketError::Unavailable("crashed".into()));
        }
        Ok(())
    }

    fn version(&self) -> u64 {
        let mut v = self.next_version.lock().unwrap();
        *v += 1;
        *v
    }

    fn write(&self, key: &str, body: Vec<u8>) -> Version {
        let version = self.version();
        self.objects.lock().unwrap().insert(
            key.to_owned(),
            Obj {
                body,
                version,
                written: Self::now(),
            },
        );
        Version(version.to_string())
    }

    fn snapshot(&self) -> Map {
        self.objects.lock().unwrap().clone()
    }
}

struct SimClock;

thread_local! {
    /// When this seed's run started (each runtime sets it first).
    static START: std::cell::Cell<Option<madsim::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// Starts this seed's clock at [`EPOCH_MS`].
fn start_clock() {
    START.with(|s| s.set(Some(madsim::time::Instant::now())));
}

impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        let start = START.with(std::cell::Cell::get).expect("start_clock first");
        EPOCH_MS + u64::try_from(start.elapsed().as_millis()).unwrap_or(0)
    }
}

impl Pause for SimClock {
    async fn pause_ms(&self, ms: u64) {
        madsim::time::sleep(Duration::from_millis(ms)).await;
    }
}

/// The migrator's host with a wall clock this far ahead of the nodes'
/// (no NTP): it judges a node lease written a moment ago as long past.
struct AheadClock(u64);

impl Clock for AheadClock {
    fn now_ms(&self) -> u64 {
        SimClock.now_ms() + self.0
    }
}

impl Pause for AheadClock {
    async fn pause_ms(&self, ms: u64) {
        madsim::time::sleep(Duration::from_millis(ms)).await;
    }
}

impl Bucket for Sim {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.op().await?;
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|o| (o.body.clone(), Version(o.version.to_string()))))
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.op().await?;
        if self.objects.lock().unwrap().contains_key(key) {
            return Err(BucketError::Conflict);
        }
        Ok(self.write(key, body))
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        self.op().await?;
        let found = self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|o| o.version.to_string());
        if found.as_deref() != Some(expected.0.as_str()) {
            return Err(BucketError::Conflict);
        }
        Ok(self.write(key, body))
    }
}

impl migrate::Store for Sim {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BucketError> {
        self.op().await?;
        if prefix == "nodes/" {
            let n = *self.ops.lock().unwrap();
            self.node_listings.lock().unwrap().push(n);
        }
        let (recent, percent) = *self.hide_recent.lock().unwrap();
        let now = Self::now();
        let objects = self.objects.lock().unwrap().clone();
        let mut rng = self.rng.lock().unwrap();
        let mut hidden = self.hidden_left.lock().unwrap();
        Ok(objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .filter(|(k, o)| {
                // Recent writes from before the run (the migration's own
                // copies are left to the head and byte checks).
                if now.saturating_sub(o.written) >= recent || o.written >= EPOCH_MS {
                    return true;
                }
                let left = hidden.entry((*k).clone()).or_insert_with(|| {
                    if rng.chance(percent) {
                        1 + rng.below(2)
                    } else {
                        0
                    }
                });
                if *left == 0 {
                    return true;
                }
                *left -= 1;
                // Shown once it is caught up: never hidden again.
                if *left == 0 {
                    hidden.insert((*k).clone(), 0);
                }
                false
            })
            .map(|(k, o)| ObjectMeta {
                key: k.clone(),
                size: u64::try_from(o.body.len()).unwrap(),
                modified_ms: o.written,
                etag: Some(o.version.to_string()),
            })
            .collect())
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
        self.op().await?;
        let objects = self.objects.lock().unwrap();
        let dirs: BTreeSet<String> = objects
            .keys()
            .filter_map(|k| {
                k.strip_prefix(prefix)?
                    .split_once('/')
                    .map(|(d, _)| d.to_owned())
            })
            .collect();
        Ok(dirs.into_iter().collect())
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError> {
        self.op().await?;
        let body = self
            .objects
            .lock()
            .unwrap()
            .get(from)
            .map(|o| o.body.clone());
        match body {
            Some(body) => {
                self.write(to, body);
                Ok(())
            }
            None => Err(BucketError::Unavailable(format!("{from}: no such key"))),
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.op().await?;
        self.write(key, body);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        self.op().await?;
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

/// An actor of the fixture.
#[derive(Debug, Clone)]
struct Actor {
    tenant: &'static str,
    class: &'static str,
    /// Its name as given.
    name: String,
    /// Its name as a key segment (lossy past 200 bytes).
    segment: String,
    /// Its heir app.
    heir: &'static str,
    generation: u64,
    txid: u64,
    /// Whether it has an ownership record: an actor last held before
    /// records (ADR 0020 §6) has history and a marker only.
    owned: bool,
}

impl Actor {
    fn bare(&self, family: Family) -> String {
        format!("{}/{}/{}", family.bare(), self.tenant, self.tail(family))
    }

    fn tail(&self, family: Family) -> String {
        match family {
            Family::History => format!("{}/{}", self.class, self.segment),
            Family::Owners | Family::Leases => format!("{}/{}", self.class, self.segment),
            Family::Alarms => alarm_index::key_tail(
                self.class,
                &self.name,
                &Entry::owned(5_000, 1, "ab").unwrap(),
            ),
            Family::Moves => format!("{}/{}/node-b", self.class, hex(&self.name)),
        }
    }

    fn root(&self) -> StateRoot {
        StateRoot::new("", self.tenant, self.heir)
    }

    fn rooted(&self, family: Family) -> String {
        self.root().key(family, &self.tail(family))
    }

    fn record(&self) -> Vec<u8> {
        OwnerRecord {
            holder: None,
            generation: self.generation,
        }
        .encode()
    }
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// What `encode_segment` makes of a 256-byte name: cut, then a hash.
fn lossy() -> String {
    format!("{}~85e62acd750c4eb56b7b6a1d66dca5bf", "x".repeat(167))
}

fn actors() -> Vec<Actor> {
    let a = |tenant, class, name: &str, segment: &str, heir, generation, txid| Actor {
        tenant,
        class,
        name: name.to_owned(),
        segment: segment.to_owned(),
        heir,
        generation,
        txid,
        owned: true,
    };
    let mut before_records = a("acme", "chat", "early", "early", "shop", 2, 2);
    before_records.owned = false;
    vec![
        a("default", "t", "a", "a", "default", 2, 3),
        a("default", "t", &"x".repeat(256), &lossy(), "default", 1, 2),
        a("acme", "chat", "room 1", "room%201", "shop", 3, 4),
        a("acme", "post", "p", "p", "blog", 1, 1),
        a("acme", "gone", "old", "old", "blog", 1, 1),
        before_records,
    ]
}

fn apps() -> Vec<AppInfo> {
    vec![
        AppInfo {
            tenant: "acme".into(),
            app: "shop".into(),
            classes: BTreeSet::from(["chat".into()]),
        },
        AppInfo {
            tenant: "acme".into(),
            app: "blog".into(),
            classes: BTreeSet::from(["post".into()]),
        },
        // A tenant with an app and no actor state yet: only its token.
        AppInfo {
            tenant: "zeta".into(),
            app: "web".into(),
            classes: BTreeSet::from(["page".into()]),
        },
    ]
}

fn options() -> Options {
    Options {
        heirs: BTreeMap::from([("acme".into(), "blog".into())]),
        concurrency: 4,
        lease_window_ms: 25_000,
        old_window_ms: 60_000,
        prefix_bytes: 0,
    }
}

fn obj(body: impl Into<Vec<u8>>) -> Obj {
    Obj {
        body: body.into(),
        version: 1,
        written: EPOCH_MS - 600_000,
    }
}

/// The segments of `a`'s history: transaction 1 under generation 1, then
/// the rest under its generation.
fn history(a: &Actor) -> Vec<(String, Vec<u8>)> {
    let mut out = vec![(
        snapshot_key(SegmentId {
            generation: 1,
            txid: 1,
        }),
        b"snapshot 1".to_vec(),
    )];
    let mut parent = 0;
    for txid in 1..=a.txid {
        let generation = if txid == 1 { 1 } else { a.generation };
        out.push((
            segment_key(&Segment {
                id: SegmentId { generation, txid },
                parent_generation: parent,
            }),
            format!("tx {txid}").into_bytes(),
        ));
        parent = generation;
    }
    out
}

/// The bucket before the migration.
fn fixture() -> Map {
    let mut m = Map::new();
    for a in actors() {
        for (key, body) in history(&a) {
            m.insert(format!("{}/{key}", a.bare(Family::History)), obj(body));
        }
        if a.owned {
            m.insert(a.bare(Family::Owners), obj(a.record()));
        }
        m.insert(a.bare(Family::Leases), obj(encode_marker(a.generation)));
        m.insert(a.bare(Family::Alarms), obj(Vec::new()));
        m.insert(a.bare(Family::Moves), obj(Vec::new()));
    }
    m.insert("apps/acme/shop/current".into(), obj("v1 4\n"));
    m.insert("apps/acme/blog/current".into(), obj("v2 7 2\n"));
    m.insert("apps/zeta/web/current".into(), obj("v1 1\n"));
    // A node that stopped cleanly, and one that crashed long ago.
    m.insert(
        "nodes/node-a".into(),
        obj(NodeLeaseObject {
            state: NodeState::Released,
            instance: Some("aa".into()),
            written_at_ms: EPOCH_MS - 1_000,
        }
        .encode()),
    );
    m.insert(
        "nodes/node-b".into(),
        obj(NodeLeaseObject {
            state: NodeState::Live,
            instance: Some("bb".into()),
            written_at_ms: EPOCH_MS - 120_000,
        }
        .encode()),
    );
    m
}

/// An old binary takes `a` (if its bare record is not a tombstone): a
/// record of the next generation, of the same size, and a segment of it;
/// with no record, it creates one seeded from the marker (v0.2.0's
/// acquire). Its node lease shows it ran, then stopped. Gives the actor
/// as it is now, or `None` if the tombstone refused it.
fn old_takes(m: &mut Map, a: &Actor, now: u64) -> Option<Actor> {
    let key = a.bare(Family::Owners);
    let generation = match m.get(&key) {
        Some(o) => OwnerRecord::decode(&o.body)?.generation,
        None => a.generation,
    };
    let mut taken = a.clone();
    taken.owned = true;
    taken.generation = generation + 1;
    taken.txid += 1;
    let version = m.values().map(|o| o.version).max().unwrap_or(0) + 1;
    let put = |m: &mut Map, k: String, body: Vec<u8>, v: u64| {
        m.insert(
            k,
            Obj {
                body,
                version: v,
                written: now,
            },
        );
    };
    put(m, key, taken.record(), version);
    let segment = segment_key(&Segment {
        id: SegmentId {
            generation: taken.generation,
            txid: taken.txid,
        },
        parent_generation: a.generation,
    });
    put(
        m,
        format!("{}/{segment}", a.bare(Family::History)),
        format!("tx {}", taken.txid).into_bytes(),
        version + 1,
    );
    put(
        m,
        "nodes/node-old".into(),
        NodeLeaseObject {
            state: NodeState::Released,
            instance: Some("oo".into()),
            written_at_ms: now,
        }
        .encode(),
        version + 2,
    );
    Some(taken)
}

/// A node of a new id, live now.
fn node_starts(m: &mut Map, now: u64) {
    m.insert(
        "nodes/node-new".into(),
        Obj {
            body: NodeLeaseObject {
                state: NodeState::Live,
                instance: Some("nn".into()),
                written_at_ms: now,
            }
            .encode(),
            version: 9_999_999,
            written: now,
        },
    );
}

/// Every check on a migrated bucket: `actors` as their latest state.
fn check_migrated(seed: u64, m: &Map, before: &Map, actors: &[Actor]) {
    let body = |k: &str| {
        m.get(k)
            .unwrap_or_else(|| panic!("seed {seed}: {k} missing"))
            .body
            .clone()
    };
    assert!(!m.contains_key(LOCK_KEY), "seed {seed}: still locked");
    for t in ["acme", "default"] {
        assert!(
            m.contains_key(&format!("{COMMIT_PREFIX}{t}")),
            "seed {seed}: {t} not committed"
        );
    }
    for app in ["acme/shop", "acme/blog", "zeta/web"] {
        let current = String::from_utf8(body(&format!("apps/{app}/current"))).unwrap();
        assert_eq!(
            current.split_whitespace().nth(2),
            Some("3"),
            "seed {seed}: {app} not rooted"
        );
    }
    let mut wanted = BTreeSet::new();
    for a in actors {
        // The record under the root is the actor's latest; the bare one a
        // tombstone naming it (one with no record: a tombstone all the
        // same, which an older binary cannot create a record over).
        let rooted = a.rooted(Family::Owners);
        if a.owned {
            assert_eq!(body(&rooted), a.record(), "seed {seed}: {rooted}");
        } else {
            assert!(!m.contains_key(&rooted), "seed {seed}: {rooted}");
        }
        assert_eq!(
            migrate::tombstone_target(&body(&a.bare(Family::Owners))),
            Some(rooted.as_str()),
            "seed {seed}: {}",
            a.name
        );
        // Every other bare key has its copy, byte for byte.
        let bare_history = format!("{}/", a.bare(Family::History));
        let rooted_history = format!("{}/", a.rooted(Family::History));
        for (k, o) in m.iter().filter(|(k, _)| k.starts_with(&bare_history)) {
            let to = k.replacen(&bare_history, &rooted_history, 1);
            assert_eq!(body(&to), o.body, "seed {seed}: {to}");
            wanted.insert(to);
        }
        // Moves stay in the old layout: a copy would renew them.
        assert!(!m.contains_key(&a.rooted(Family::Moves)), "seed {seed}");
        for family in [Family::Leases, Family::Alarms] {
            assert_eq!(
                body(&a.rooted(family)),
                body(&a.bare(family)),
                "seed {seed}"
            );
            wanted.insert(a.rooted(family));
        }
        if a.owned {
            wanted.insert(rooted);
        }
        // The same restore plan, ending at the last acknowledged write.
        let plan = |prefix: &str| {
            let keys: Vec<String> = m
                .keys()
                .filter_map(|k| k.strip_prefix(prefix).map(str::to_owned))
                .collect();
            plan_restore(&Listing::from_keys(keys.iter().map(String::as_str))).unwrap()
        };
        let (from, to) = (plan(&bare_history), plan(&rooted_history));
        assert_eq!(from, to, "seed {seed}: {}", a.name);
        assert_eq!(
            to.head(),
            SegmentId {
                generation: a.generation,
                txid: a.txid
            },
            "seed {seed}: {} lost a write",
            a.name
        );
    }
    // Nothing else under the roots.
    let roots: BTreeSet<String> = m
        .keys()
        .filter(|k| k.starts_with("state/"))
        .cloned()
        .collect();
    assert_eq!(roots, wanted, "seed {seed}: extra or missing root keys");
    // Nothing of the bare layout is deleted but the records' bodies.
    for k in before.keys() {
        assert!(m.contains_key(k), "seed {seed}: {k} deleted");
    }
}

/// The bucket without what depends on time or on the run's history
/// (commits, the lock, node leases, versions).
fn canonical(m: &Map) -> BTreeMap<String, Vec<u8>> {
    m.iter()
        .filter(|(k, _)| !k.starts_with("cluster/") && !k.starts_with("nodes/"))
        .map(|(k, o)| (k.clone(), o.body.clone()))
        .collect()
}

fn runtime(seed: u64) -> Runtime {
    Runtime::with_seed_and_config(seed, madsim::Config::default())
}

async fn apply(sim: &Sim) -> Result<migrate::Outcome, MigrateError> {
    Migrator::new(sim, &SimClock, apps(), options())
        .apply()
        .await
}

async fn rollback(sim: &Sim) -> Result<Vec<String>, MigrateError> {
    Migrator::new(sim, &SimClock, apps(), options())
        .rollback()
        .await
}

/// A clean run's requests and end state.
fn clean(seed: u64) -> (u64, BTreeMap<String, Vec<u8>>) {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Sim::new(seed, fixture());
        let before = sim.snapshot();
        let outcome = apply(&sim).await.unwrap();
        assert_eq!(outcome.exit_code(), 0);
        check_migrated(seed, &sim.snapshot(), &before, &actors());
        (*sim.ops.lock().unwrap(), canonical(&sim.snapshot()))
    })
}

/// A crash after request `i`, then a second run.
fn crash_then_rerun(seed: u64, i: u64, want: &BTreeMap<String, Vec<u8>>) {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Sim::new(seed, fixture());
        let before = sim.snapshot();
        *sim.crash_after.lock().unwrap() = Some(i);
        let first = apply(&sim).await;
        *sim.crash_after.lock().unwrap() = None;
        if first.as_ref().is_ok_and(|o| o.exit_code() == 0) {
            // Done before the crash point.
            check_migrated(seed, &sim.snapshot(), &before, &actors());
            return;
        }
        // Nothing half-committed reads as done: either no commit, or the
        // lock is still there.
        let m = sim.snapshot();
        let committed = m.keys().any(|k| k.starts_with(COMMIT_PREFIX));
        assert!(
            !committed || m.contains_key(LOCK_KEY),
            "seed {seed}, crash at {i}: a commit without its lock after a crash"
        );
        apply(&sim)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}, crash at {i}: {e}"));
        let m = sim.snapshot();
        check_migrated(seed, &m, &before, &actors());
        assert_eq!(&canonical(&m), want, "seed {seed}, crash at {i}");
    });
}

/// A migrated bucket rolled back, with a crash after request `i` of the
/// first rollback (`None`: no crash): the bare layout comes back byte for
/// byte. Gives the clean rollback's requests.
fn rollback_with_crash(seed: u64, i: Option<u64>) -> u64 {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Sim::new(seed, fixture());
        let before = canonical(&sim.snapshot());
        apply(&sim).await.unwrap();
        let ops = *sim.ops.lock().unwrap();
        *sim.crash_after.lock().unwrap() = i.map(|i| ops + i);
        let first = rollback(&sim).await;
        let used = *sim.ops.lock().unwrap() - ops;
        *sim.crash_after.lock().unwrap() = None;
        if first.is_err() {
            assert!(i.is_some(), "seed {seed}: {first:?}");
            rollback(&sim)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}, rollback crash at {i:?}: {e}"));
        }
        let m = sim.snapshot();
        assert_eq!(
            canonical(&m),
            before,
            "seed {seed}, rollback crash at {i:?}: not the bare layout again"
        );
        assert!(!m.keys().any(|k| k.starts_with("cluster/")), "seed {seed}");
        // And it migrates again.
        apply(&sim).await.unwrap();
        check_migrated(seed, &sim.snapshot(), &fixture(), &actors());
        used
    })
}

/// `--apply` crashed after request `i`, then `--rollback`: the bare layout
/// comes back byte for byte, from a lock of an unfinished run (some
/// tombstones written, some tokens changed, or none).
fn apply_crash_then_rollback(seed: u64, i: u64) {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Sim::new(seed, fixture());
        let before = canonical(&sim.snapshot());
        *sim.crash_after.lock().unwrap() = Some(i);
        let first = apply(&sim).await;
        *sim.crash_after.lock().unwrap() = None;
        let _ = first;
        rollback(&sim)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}, apply crash at {i}: {e}"));
        let m = sim.snapshot();
        assert_eq!(
            canonical(&m),
            before,
            "seed {seed}, apply crash at {i}: not the bare layout after the rollback"
        );
        assert!(!m.keys().any(|k| k.starts_with("cluster/")), "seed {seed}");
    });
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

#[test]
fn a_crash_at_every_request_then_a_rerun_ends_in_the_clean_state() {
    // Every request of one run, on the small instance.
    let (ops, want) = clean(1);
    for i in 0..=ops {
        crash_then_rerun(1, i, &want);
    }
    // Then a crash point and an interleaving per seed.
    for seed in 0..seeds() {
        let (ops, want) = clean(seed);
        let i = Rng::new(seed ^ 0x5eed).below(ops + 1);
        crash_then_rerun(seed, i, &want);
    }
}

#[test]
fn a_rollback_crashed_anywhere_restores_the_bare_layout() {
    // Every request of one rollback.
    let ops = rollback_with_crash(2, None);
    for i in 0..=ops {
        rollback_with_crash(2, Some(i));
    }
    for seed in 0..seeds() {
        let ops = rollback_with_crash(seed, None);
        rollback_with_crash(seed, Some(Rng::new(seed ^ 0xbac).below(ops + 1)));
    }
}

#[test]
fn an_apply_crashed_anywhere_then_rolled_back_restores_the_bare_layout() {
    let (ops, _) = clean(3);
    for i in 0..=ops {
        apply_crash_then_rollback(3, i);
    }
    for seed in 0..seeds() {
        let (ops, _) = clean(seed);
        apply_crash_then_rollback(seed, Rng::new(seed ^ 0xab).below(ops + 1));
    }
}

#[test]
fn an_old_binary_between_two_runs_reuses_no_generation() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let mut rng = Rng::new(seed ^ 0x01d);
            let sim = Sim::new(seed, fixture());
            let before = sim.snapshot();
            // Crash somewhere in the first run (it may finish first).
            *sim.crash_after.lock().unwrap() = Some(1 + rng.below(150));
            let first = apply(&sim).await;
            *sim.crash_after.lock().unwrap() = None;
            let mut now = actors();
            if !first.as_ref().is_ok_and(|o| o.exit_code() == 0) {
                // Some time later, an old binary takes one actor and stops.
                madsim::time::sleep(Duration::from_secs(1)).await;
                let pick = usize::try_from(rng.below(now.len() as u64)).unwrap();
                let taken = old_takes(&mut sim.objects.lock().unwrap(), &now[pick], Sim::now());
                if let Some(taken) = taken {
                    now[pick] = taken;
                }
                apply(&sim)
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            }
            check_migrated(seed, &sim.snapshot(), &before, &now);
        });
    }
}

#[test]
fn a_node_that_starts_mid_run_stops_the_commit() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let sim = Sim::new(seed, fixture());
            let before = sim.snapshot();
            let at = 1 + Rng::new(seed ^ 0x9e).below(200);
            sim.hooks
                .lock()
                .unwrap()
                .entry(at)
                .or_default()
                .push(Box::new(node_starts));
            match apply(&sim).await {
                // It started after the final proof: the run was over.
                Ok(_) => {
                    let last = sim.node_listings.lock().unwrap().last().copied().unwrap();
                    assert!(
                        at > last,
                        "seed {seed}: committed although a node started (request {at}) before \
                         the last listing of nodes/ (request {last})"
                    );
                    let m = sim.snapshot();
                    check_migrated(seed, &m, &before, &actors());
                    return;
                }
                Err(MigrateError::Refused(_) | MigrateError::Aborted(_)) => {}
                Err(e) => panic!("seed {seed}: {e}"),
            }
            let m = sim.snapshot();
            assert!(
                !m.keys().any(|k| k.starts_with(COMMIT_PREFIX)),
                "seed {seed}: committed although a node ran (hook at {at})"
            );
            // It stops cleanly; the next run commits.
            sim.write(
                "nodes/node-new",
                NodeLeaseObject {
                    state: NodeState::Released,
                    instance: Some("nn".into()),
                    written_at_ms: Sim::now(),
                }
                .encode(),
            );
            apply(&sim)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            check_migrated(seed, &sim.snapshot(), &before, &actors());
        });
    }
}

#[test]
fn listings_that_leave_out_recent_keys_never_commit_a_wrong_state() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            // The last writes of one actor landed just before the nodes
            // stopped: a listing may not show them yet.
            let mut m = fixture();
            let recent = actors()[usize::try_from(seed % 5).unwrap()].clone();
            for (k, o) in &mut m {
                if k.starts_with(&recent.bare(Family::History))
                    || k.starts_with(&recent.bare(Family::Alarms))
                {
                    o.written = EPOCH_MS - 10;
                }
            }
            let sim = Sim::new(seed, m);
            let before = sim.snapshot();
            *sim.hide_recent.lock().unwrap() = (1_000, 50);
            let first = apply(&sim).await;
            *sim.hide_recent.lock().unwrap() = (0, 0);
            match first {
                Ok(_) => check_migrated(seed, &sim.snapshot(), &before, &actors()),
                Err(MigrateError::Aborted(_)) => {
                    assert!(
                        !sim.snapshot().keys().any(|k| k.starts_with(COMMIT_PREFIX)),
                        "seed {seed}: a commit after an abort"
                    );
                    madsim::time::sleep(Duration::from_secs(1)).await;
                    apply(&sim)
                        .await
                        .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
                    check_migrated(seed, &sim.snapshot(), &before, &actors());
                }
                Err(e) => panic!("seed {seed}: {e}"),
            }
        });
    }
}

#[test]
fn an_old_node_taking_an_actor_before_its_tombstone_aborts_the_run() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let sim = Arc::new(Sim::new(seed, fixture()));
            let before = sim.snapshot();
            let pick = usize::try_from(Rng::new(seed ^ 0x7a).below(actors().len() as u64)).unwrap();
            let target = actors()[pick].clone();
            // The request number of the tombstone over the target's record
            // in a clean run of this seed: hook the old node just before it.
            let (at, _) = {
                let probe = Sim::new(seed, fixture());
                let mut at = 0;
                let key = target.bare(Family::Owners);
                // Find when the record is first replaced by a tombstone.
                let watch = Arc::new(Mutex::new(None));
                for n in 1..2_000 {
                    let watch = Arc::clone(&watch);
                    let key = key.clone();
                    probe
                        .hooks
                        .lock()
                        .unwrap()
                        .entry(n)
                        .or_default()
                        .push(Box::new(move |m: &mut Map, _| {
                            let mut w = watch.lock().unwrap();
                            if w.is_none()
                                && m.get(&key)
                                    .is_some_and(|o| migrate::tombstone_target(&o.body).is_some())
                            {
                                *w = Some(n);
                            }
                        }));
                }
                apply(&probe).await.unwrap();
                if let Some(n) = *watch.lock().unwrap() {
                    // The tombstone landed at request n - 1.
                    at = n - 1;
                }
                (at, ())
            };
            assert!(at > 0, "seed {seed}: no tombstone written");
            let taken = Arc::new(Mutex::new(None));
            {
                let taken = Arc::clone(&taken);
                let target = target.clone();
                sim.hooks
                    .lock()
                    .unwrap()
                    .entry(at)
                    .or_default()
                    .push(Box::new(move |m: &mut Map, now| {
                        *taken.lock().unwrap() = old_takes(m, &target, now);
                    }));
            }
            let first = apply(&sim).await;
            let taken = taken.lock().unwrap().clone();
            let mut now = actors();
            match (first, taken) {
                (Err(MigrateError::Aborted(why)), Some(t)) => {
                    assert!(
                        !sim.snapshot().keys().any(|k| k.starts_with(COMMIT_PREFIX)),
                        "seed {seed}: committed after {why}"
                    );
                    now[pick] = t;
                    apply(&sim)
                        .await
                        .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
                }
                (Ok(_), None) => {}
                (r, t) => panic!(
                    "seed {seed}: {:?} with the old node's take {t:?}",
                    r.map(drop)
                ),
            }
            check_migrated(seed, &sim.snapshot(), &before, &now);
        });
    }
}

#[test]
fn a_rollback_after_a_node_ran_is_refused_and_changes_nothing() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let sim = Sim::new(seed, fixture());
            let before = sim.snapshot();
            apply(&sim).await.unwrap();
            madsim::time::sleep(Duration::from_millis(1 + Rng::new(seed).below(5_000))).await;
            node_starts(&mut sim.objects.lock().unwrap(), Sim::now());
            let migrated = sim.snapshot();
            match rollback(&sim).await {
                Err(MigrateError::Refused(why)) => {
                    assert!(why.concat().contains("one-way"), "seed {seed}: {why:?}");
                }
                r => panic!("seed {seed}: {r:?}"),
            }
            assert_eq!(
                canonical(&sim.snapshot()),
                canonical(&migrated),
                "seed {seed}"
            );
            assert!(!sim.snapshot().contains_key(LOCK_KEY), "seed {seed}");
            check_migrated(seed, &sim.snapshot(), &before, &actors());
        });
    }
}

#[test]
fn a_seed_replays() {
    assert_eq!(clean(7), clean(7));
}

/// A node lease written just before the run, by a node that keeps
/// acknowledging writes until its lease would run out, then dies without
/// a word: the migrator's clock runs `ahead` ms in front, so the plan
/// judges that lease long past. Every write it made must be under the
/// root once committed.
#[test]
fn a_migrator_clock_ahead_of_the_nodes_loses_no_acknowledged_write() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let mut rng = Rng::new(seed ^ 0xc10c);
            let mut m = fixture();
            // Renewed 1 s ago on the node's clock; the next renewal never
            // comes: the node dies within 2.5 s.
            m.insert(
                "nodes/node-c".into(),
                obj(NodeLeaseObject {
                    state: NodeState::Live,
                    instance: Some("cc".into()),
                    written_at_ms: EPOCH_MS - 1_000,
                }
                .encode()),
            );
            let sim = Arc::new(Sim::new(seed, m));
            let before = sim.snapshot();
            let dies = rng.below(2_500);
            let mut now = actors();
            let writer = usize::try_from(rng.below(now.len() as u64)).unwrap();
            let acked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            {
                let (sim, acked, a) = (Arc::clone(&sim), Arc::clone(&acked), now[writer].clone());
                let mut rng = Rng::new(seed ^ 0xd1e);
                madsim::task::spawn(async move {
                    let mut txid = a.txid;
                    loop {
                        madsim::time::sleep(Duration::from_millis(1 + rng.below(300))).await;
                        if Sim::now() >= EPOCH_MS + dies {
                            return;
                        }
                        txid += 1;
                        let key = format!(
                            "{}/{}",
                            a.bare(Family::History),
                            segment_key(&Segment {
                                id: SegmentId {
                                    generation: a.generation,
                                    txid,
                                },
                                parent_generation: a.generation,
                            })
                        );
                        sim.write(&key, format!("tx {txid}").into_bytes());
                        acked.lock().unwrap().push(key);
                    }
                });
            }
            let clock = AheadClock(30_000);
            let migrator = Migrator::new(&*sim, &clock, apps(), options());
            if migrator.apply().await.is_err() {
                assert!(
                    !sim.snapshot().keys().any(|k| k.starts_with(COMMIT_PREFIX)),
                    "seed {seed}: a commit after an abort"
                );
                migrator
                    .apply()
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            }
            let m = sim.snapshot();
            let acked = acked.lock().unwrap().clone();
            now[writer].txid += u64::try_from(acked.len()).unwrap();
            for key in &acked {
                let to = key.replacen(
                    &now[writer].bare(Family::History),
                    &now[writer].rooted(Family::History),
                    1,
                );
                assert!(
                    m.contains_key(&to),
                    "seed {seed}: the acknowledged {key} is not under the root"
                );
            }
            check_migrated(seed, &m, &before, &now);
        });
    }
}

/// Two runs at once: an `--apply` with a second `--apply`, or with a
/// `--rollback` started at a seeded moment. A commit never names a copy
/// that is gone, and one more `--apply` alone ends in the clean state.
fn two_runs(seed: u64, second_rolls_back: bool) {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Arc::new(Sim::new(seed, fixture()));
        let before = sim.snapshot();
        let delay = Rng::new(seed ^ 0x2a2a).below(27_000);
        let first = {
            let sim = Arc::clone(&sim);
            madsim::task::spawn(async move { apply(&sim).await.map(drop) })
        };
        let second = {
            let sim = Arc::clone(&sim);
            madsim::task::spawn(async move {
                madsim::time::sleep(Duration::from_millis(delay)).await;
                if second_rolls_back {
                    rollback(&sim).await.map(drop)
                } else {
                    apply(&sim).await.map(drop)
                }
            })
        };
        let (a, b) = (first.await.unwrap(), second.await.unwrap());
        let m = sim.snapshot();
        for t in ["acme", "default"] {
            if !m.contains_key(&format!("{COMMIT_PREFIX}{t}")) {
                continue;
            }
            for actor in actors().iter().filter(|x| x.tenant == t && x.owned) {
                assert!(
                    m.contains_key(&actor.rooted(Family::Owners)),
                    "seed {seed} (rollback {second_rolls_back}, at {delay}): {t} committed but \
                     {} is gone ({a:?}, {b:?})",
                    actor.rooted(Family::Owners)
                );
            }
        }
        apply(&sim).await.unwrap_or_else(|e| {
            panic!("seed {seed} (rollback {second_rolls_back}, at {delay}): {e} ({a:?}, {b:?})")
        });
        check_migrated(seed, &sim.snapshot(), &before, &actors());
    });
}

#[test]
fn two_runs_at_once_never_commit_a_missing_copy() {
    for seed in 0..seeds() {
        two_runs(seed, false);
        two_runs(seed, true);
    }
}

/// `--apply` crashed after request `i`, run again to the end, then
/// `--rollback`: every token comes back, including those of a tenant with
/// apps and no actor state (its originals live only in the first lock).
fn crash_rerun_then_rollback(seed: u64, i: u64) {
    runtime(seed).block_on(async move {
        start_clock();
        let sim = Sim::new(seed, fixture());
        let before = canonical(&sim.snapshot());
        *sim.crash_after.lock().unwrap() = Some(i);
        let _ = apply(&sim).await;
        *sim.crash_after.lock().unwrap() = None;
        apply(&sim)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}, crash at {i}: {e}"));
        rollback(&sim)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}, crash at {i}: {e}"));
        assert_eq!(
            canonical(&sim.snapshot()),
            before,
            "seed {seed}, crash at {i}: not the bare layout after the rollback"
        );
    });
}

#[test]
fn a_crashed_run_finished_then_rolled_back_restores_every_token() {
    let (ops, _) = clean(4);
    for i in 0..=ops {
        crash_rerun_then_rollback(4, i);
    }
    for seed in 0..seeds() {
        let (ops, _) = clean(seed);
        crash_rerun_then_rollback(seed, Rng::new(seed ^ 0x70c).below(ops + 1));
    }
}

/// After the commit and before any node: a deploy of this version, or a
/// key written under a root (a node whose `nodes/` object is gone, a
/// restored backup), makes the migration one-way: `--rollback` refuses
/// and changes nothing.
#[test]
fn a_rollback_after_a_deploy_or_a_late_root_key_is_refused() {
    for seed in 0..seeds() {
        for deploy in [true, false] {
            runtime(seed).block_on(async move {
                start_clock();
                let sim = Sim::new(seed, fixture());
                let before = sim.snapshot();
                apply(&sim).await.unwrap();
                madsim::time::sleep(Duration::from_millis(1 + Rng::new(seed).below(5_000))).await;
                if deploy {
                    sim.write("apps/zeta/web/current", b"v2 2 3\n".to_vec());
                } else {
                    sim.write("state/acme/shop/actors/chat/new/wal/x.ltx", b"tx".to_vec());
                }
                let migrated = sim.snapshot();
                match rollback(&sim).await {
                    Err(MigrateError::Refused(why)) => {
                        assert!(why.concat().contains("one-way"), "seed {seed}: {why:?}");
                    }
                    r => panic!("seed {seed} (deploy {deploy}): {r:?}"),
                }
                assert_eq!(
                    canonical(&sim.snapshot()),
                    canonical(&migrated),
                    "seed {seed}"
                );
                if deploy {
                    check_migrated(seed, &sim.snapshot(), &before, &actors());
                }
            });
        }
    }
}

/// A deploy of an older version lands after the token barrier (its
/// `current` without token 3): the check after the final proof aborts,
/// nothing is committed, and the next run roots it and commits.
#[test]
fn a_deploy_of_an_older_version_after_the_barrier_stops_the_commit() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let sim = Sim::new(seed, fixture());
            let before = sim.snapshot();
            let done = Arc::new(Mutex::new(false));
            for n in 1..3_000 {
                let done = Arc::clone(&done);
                sim.hooks
                    .lock()
                    .unwrap()
                    .entry(n)
                    .or_default()
                    .push(Box::new(move |m: &mut Map, now| {
                        let mut d = done.lock().unwrap();
                        let barrier = m.iter().any(|(k, o)| {
                            k.starts_with("owners/") && migrate::tombstone_target(&o.body).is_some()
                        });
                        if !*d && barrier {
                            *d = true;
                            m.insert(
                                "apps/zeta/web/current".into(),
                                Obj {
                                    body: b"v5 5\n".to_vec(),
                                    version: 8_888_888,
                                    written: now,
                                },
                            );
                        }
                    }));
            }
            match apply(&sim).await {
                Err(MigrateError::Aborted(why)) => {
                    assert!(why.contains("token 3"), "seed {seed}: {why}");
                }
                r => panic!("seed {seed}: {:?}", r.map(drop)),
            }
            assert!(
                !sim.snapshot().keys().any(|k| k.starts_with(COMMIT_PREFIX)),
                "seed {seed}: committed over an unrooted current"
            );
            sim.hooks.lock().unwrap().clear();
            apply(&sim)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            check_migrated(seed, &sim.snapshot(), &before, &actors());
        });
    }
}

/// A rollback crashed after it started writing, then a node of an older
/// version ran on the old layout (its `nodes/` object changed): the next
/// `--rollback` finishes it all the same, since from its first write the
/// old layout is the truth.
#[test]
fn a_started_rollback_finishes_whatever_ran_since() {
    for seed in 0..seeds() {
        runtime(seed).block_on(async move {
            start_clock();
            let sim = Sim::new(seed, fixture());
            let before = canonical(&sim.snapshot());
            apply(&sim).await.unwrap();
            let ops = *sim.ops.lock().unwrap();
            // Past the lock and its re-check: a few requests into the
            // records.
            let i = 12 + Rng::new(seed ^ 0x5a).below(30);
            *sim.crash_after.lock().unwrap() = Some(ops + i);
            let first = rollback(&sim).await;
            *sim.crash_after.lock().unwrap() = None;
            if first.is_ok() {
                return;
            }
            if !sim
                .snapshot()
                .get(LOCK_KEY)
                .is_some_and(|o| o.body.windows(13).any(|w| w == b"mode rollback"))
            {
                return;
            }
            node_starts(&mut sim.objects.lock().unwrap(), Sim::now());
            rollback(&sim)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}, crash at {i}: {e}"));
            assert_eq!(canonical(&sim.snapshot()), before, "seed {seed}");
        });
    }
}
