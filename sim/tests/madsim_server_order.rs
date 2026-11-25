//! ADR 0035, follow-up S7, under madsim: hand-overs run in the server's
//! own order, end to end, over a bucket whose LIST lags its writes.
//!
//! The other recorded-head simulations each test one piece (the hold
//! cell, local evidence, `prove`, `linked_back`). This one composes the
//! real `wasmkeep-cluster` functions in the order the server calls them,
//! each step citing the server code it transcribes:
//!
//! - **Acquisition** (`cluster/lease_hold.rs`, `LeaseHold::acquire_with`):
//!   the real `Owners::acquire_prepared` with the recorded-head format
//!   enabled, the node's `LocalHeads::prepare` as its preparation hook,
//!   `transferred` on success and `finished` on failure.
//! - **Discovery** (`replica.rs`, `Replica::restore`): the floor read once
//!   from the hold (`LeaseHold::floor`), at most ten listings with the
//!   server's pauses and jitter inside one 15 s deadline, the hold's
//!   validity checked before each pause.
//! - **The gate** (`Replica::restore_within`): list, `plan_restore`,
//!   `prove` on the plan's chain, `linked_back` only on `Missing` with a
//!   snapshot base (the floor then dropped), refusal, the empty-plan
//!   return, the local file at the plan's head (proving an exact floor
//!   only there), else the replay (`Replica::download`: pre-apply
//!   checksums checked, then `prove` again on the replayed sums).
//! - **Activation**: `hold_cell::verify` with the verified head before
//!   any upload; a hold that lapsed meanwhile is late.
//! - **Uploads**: create-only segments acknowledged into the cell when
//!   their PUT completes (`hold_cell::acknowledge`), a periodic snapshot
//!   labelled at the head (`ship_snapshot`) that never advances it, then
//!   collection by the real `collectable`.
//! - **The end**, concurrent with uploads: a release (frozen, registered,
//!   `Owners::release` after a seeded delay, `LocalHeads::released`), a
//!   lapse (validity first, then the freeze and registration of
//!   `end_all`, a pending release retried later, the node fenced) or a
//!   process crash (nothing registered, nothing released, a new instance,
//!   the local file and sidecar kept on disk).
//!
//! Releases make their uploads visible to LIST late: at once, within 5 s
//! (recovered inside the discovery bound), or after 20 s (refused, then
//! recovered on a later cycle). A lapse or crash leaves its uploads
//! visible at once: crash-time LIST lag is the documented unsupported
//! case (ADR 0035, case 14) and is not claimed here.
//!
//! Safety, every seed: an activation descends from the last acknowledged
//! write of every earlier hold (so from its own floor); no acknowledgement
//! or verification succeeds on an ended hold, none advances an unverified
//! one; no stale release changes a successor's record. Liveness,
//! separately: every hand-over activates within its cycles, and one whose
//! lag fits the discovery bound activates on its first cycle. Counters
//! make sure the refusal, relisting, `linked_back`, local-file, replay,
//! lapse, crash, collection and late-upload paths are all exercised.
//!
//! Shapes scheduled on a share of hand-overs: a lapse or crash during
//! discovery (the validity check before each pause, and `verify` after
//! the end, are late), and review M1's interleaving (a release with an
//! upload in flight, then a read-only holder that snapshots at the head
//! it restored and crashes). Releases' objects are listed out of order
//! within their lag, so missing links are relisted.
//!
//! Broken orderings (each must be caught): using the local file at the
//! plan's head before the floor is proven, taking a snapshot base as
//! proof without `linked_back`, and labelling a snapshot at the head over
//! another database (caught on the `linked_back` route, where the base is
//! trusted on its label alone).
//!
//! The order is transcribed by hand: reordering `replica.rs` would not
//! fail this simulation. Moving the gate's ordering into the cluster crate
//! so the server and this case share it is the follow-up.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::hold_cell;
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use wasmkeep_cluster::local_heads::{LocalHeads, Proof, Requirement, linked_back, prove};
use wasmkeep_cluster::node_lease::{
    AcquireError, Hold, NodeLeaseObject, NodeState, OwnerRecord, Owners, Released,
};
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_cluster::replication::{
    EMPTY, Listing, PlanError, Segment, SegmentId, collectable, plan_restore, segment_key,
    snapshot_key,
};
use wasmkeep_sim::rng::Rng;

const KEY: &str = "owners/t/c/a";
const EPOCH_MS: u64 = 1_000_000;
const NODES: [&str; 3] = ["a", "b", "c"];

/// `replica.rs`: `DISCOVERY` and `RELIST_MS`.
const DISCOVERY: Duration = Duration::from_secs(15);
const RELIST_MS: [u64; 9] = [100, 200, 400, 800, 1_600, 1_600, 1_600, 1_600, 1_600];

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

thread_local! {
    static START: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy)]
struct SimClock;

impl Clock for SimClock {
    fn now_ms(&self) -> u64 {
        let start = START.with(std::cell::Cell::get).expect("started");
        EPOCH_MS + u64::try_from(start.elapsed().as_millis()).unwrap_or(0)
    }
}

/// A history object: a segment's pre- and post-apply database checksums,
/// or a snapshot's (both equal).
#[derive(Debug, Clone, Copy)]
struct Body {
    pre: u64,
    post: u64,
}

/// Key -> (body, version).
type Records = Arc<Mutex<BTreeMap<String, (Vec<u8>, u64)>>>;
/// Key -> (object, listed from).
type History = Arc<Mutex<BTreeMap<String, (Body, Instant)>>>;

/// The bucket: records and node leases strongly consistent; history
/// objects created once, readable at once, listed only from `visible`.
#[derive(Clone, Default)]
struct Mem {
    records: Records,
    history: History,
    versions: Arc<Mutex<u64>>,
}

impl Mem {
    fn bump(&self) -> u64 {
        let mut v = self.versions.lock().unwrap();
        *v += 1;
        *v
    }

    fn set(&self, key: &str, body: Vec<u8>) {
        let v = self.bump();
        self.records.lock().unwrap().insert(key.into(), (body, v));
    }

    fn record(&self) -> Option<OwnerRecord> {
        self.records
            .lock()
            .unwrap()
            .get(KEY)
            .and_then(|(b, _)| OwnerRecord::decode(b))
    }

    /// `PutMode::Create` (`replica.rs`, `put_bounded_by` and `put_once`).
    fn create(&self, key: String, body: Body, lag: Duration) -> bool {
        let mut h = self.history.lock().unwrap();
        if h.contains_key(&key) {
            return false;
        }
        h.insert(key, (body, Instant::now() + lag));
        true
    }

    fn list(&self) -> Vec<String> {
        let now = Instant::now();
        self.history
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn get(&self, key: &str) -> Option<Body> {
        self.history.lock().unwrap().get(key).map(|(b, _)| *b)
    }

    fn delete(&self, key: &str) {
        self.history.lock().unwrap().remove(key);
    }
}

impl Bucket for Mem {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        sleep(Duration::from_millis(2)).await;
        Ok(self
            .records
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, v)| (b.clone(), Version(v.to_string()))))
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        sleep(Duration::from_millis(2)).await;
        if self.records.lock().unwrap().contains_key(key) {
            return Err(BucketError::Conflict);
        }
        let v = self.bump();
        self.records.lock().unwrap().insert(key.into(), (body, v));
        Ok(Version(v.to_string()))
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        sleep(Duration::from_millis(2)).await;
        let found = self
            .records
            .lock()
            .unwrap()
            .get(key)
            .map(|(_, v)| v.to_string());
        if found.as_deref() != Some(expected.0.as_str()) {
            return Err(BucketError::Conflict);
        }
        let v = self.bump();
        self.records.lock().unwrap().insert(key.into(), (body, v));
        Ok(Version(v.to_string()))
    }
}

fn mix(pre: u64, id: SegmentId) -> u64 {
    let mut z = pre ^ id.generation.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ id.txid.rotate_left(17);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    (z ^ (z >> 27)) | 1
}

fn position_of(head: SegmentId, sum: u64) -> Position {
    if head == EMPTY {
        Position::Empty
    } else {
        Position::Known {
            generation: head.generation,
            txid: head.txid,
            checksum: sum,
        }
    }
}

/// Every database state ever built, by checksum: the one it was built on.
type Lineage = Arc<Mutex<HashMap<u64, u64>>>;

/// Whether state `sum` descends from (or is) state `from`.
fn descends(lineage: &Lineage, mut sum: u64, from: u64) -> bool {
    let l = lineage.lock().unwrap();
    loop {
        if sum == from {
            return true;
        }
        if sum == 0 {
            return false;
        }
        let Some(&p) = l.get(&sum) else {
            return false;
        };
        sum = p;
    }
}

/// One process of a node.
struct Proc {
    owners: Owners<Mem, SimClock>,
    instance: String,
    local: Mutex<LocalHeads>,
    /// Lapsed holds whose release has not landed (`Pending::end_all`).
    pending: Mutex<Vec<Hold>>,
}

/// One node: its process (replaced on a crash) and its disk.
struct Node {
    name: &'static str,
    proc: Mutex<Arc<Proc>>,
    restarts: Mutex<u64>,
    /// The local database's sidecar: the position it names and the
    /// checksum the file has (`replica.rs`, `local_is_at`).
    file: Mutex<Option<(SegmentId, u64)>>,
}

fn proc_for(mem: &Mem, name: &str, instance: String) -> Arc<Proc> {
    let cfg = LeaseConfig::new(1_000, 500, 200, 300).unwrap();
    let owners = Owners::new(mem.clone(), SimClock, name, &instance, "nodes/", cfg);
    owners.set_recorded_head(true);
    Arc::new(Proc {
        owners,
        instance,
        local: Mutex::new(LocalHeads::default()),
        pending: Mutex::new(Vec::new()),
    })
}

fn live(mem: &Mem, node: &str, instance: &str) {
    mem.set(
        &format!("nodes/{node}"),
        NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(instance.into()),
            written_at_ms: SimClock.now_ms(),
        }
        .encode(),
    );
}

fn fence(mem: &Mem, node: &str, instance: &str) {
    mem.set(
        &format!("nodes/{node}"),
        NodeLeaseObject {
            state: NodeState::Fenced,
            instance: Some(instance.into()),
            written_at_ms: 0,
        }
        .encode(),
    );
}

/// The rule under test, or a broken ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Breaks {
    None,
    /// The local file at the plan's head used before the floor is proven.
    LocalFirst,
    /// A snapshot base taken as proof without `linked_back`.
    SnapshotProves,
    /// A snapshot at the head labelled over another database (in review
    /// M1's shape, where only the `linked_back` route takes it as base).
    Mislabel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fail {
    Visibility,
    MissingLink,
    Restore,
    Late,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Via {
    Empty,
    Local,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Counter {
    Activations,
    FirstCycle,
    MissingLinks,
    Late,
    Refusals,
    Relists,
    LinkedBack,
    LocalFile,
    Replays,
    Lapses,
    Crashes,
    Collections,
    LateUploads,
    StaleReleases,
}

#[derive(Default)]
struct Outcome {
    violation: Option<String>,
    /// The violating activation took the `linked_back` route.
    via_linked_back: bool,
    counts: BTreeMap<Counter, u64>,
}

struct Ctx {
    mem: Mem,
    lineage: Lineage,
    breaks: Breaks,
    out: Mutex<Outcome>,
    /// Whether the last gate took the `linked_back` route.
    linked: AtomicBool,
    /// The last acknowledged write of any hold: (position, checksum).
    acked: Mutex<(SegmentId, u64)>,
}

impl Ctx {
    fn count(&self, c: Counter) {
        *self.out.lock().unwrap().counts.entry(c).or_default() += 1;
    }

    fn fail(&self, why: String) {
        self.out.lock().unwrap().violation.get_or_insert(why);
    }
}

/// `Replica::restore_within`, step by step.
async fn restore_within(
    ctx: &Ctx,
    node: &Node,
    rng: &Mutex<Rng>,
    floor: Option<&Requirement>,
) -> Result<(SegmentId, u64, Via), Fail> {
    ctx.linked.store(false, Ordering::SeqCst);
    let latency = rng.lock().unwrap().below(20);
    sleep(Duration::from_millis(1 + latency)).await;
    // `list_listed`, then `Listing::from_keys` (`restore_within`).
    let keys = ctx.mem.list();
    let listing = Listing::from_keys(keys.iter().map(String::as_str));
    // `plan_restore`; a missing link with a known floor is relisted.
    let plan = plan_restore(&listing).map_err(|e| match e {
        PlanError::Missing { .. }
            if floor.is_some_and(|f| matches!(f.head, Position::Known { .. })) =>
        {
            Fail::MissingLink
        }
        _ => Fail::Restore,
    })?;
    let file = *node.file.lock().unwrap();
    if ctx.breaks == Breaks::LocalFirst
        && plan.head() != EMPTY
        && let Some((at, sum)) = file
        && at == plan.head()
    {
        // Broken: the local file before the floor is proven.
        return Ok((at, sum, Via::Local));
    }
    // What the listing alone proves.
    let chain: Vec<(SegmentId, Option<u64>)> = plan
        .snapshot
        .into_iter()
        .chain(plan.segments.iter().copied())
        .map(|id| (id, None))
        .collect();
    let mut structural = prove(floor, &chain);
    // Review M1: `linked_back`, the floor then dropped.
    let mut floor = floor;
    if structural == Proof::Missing
        && let Some(base) = plan.snapshot
        && (ctx.breaks == Breaks::SnapshotProves || linked_back(floor, base, &listing))
    {
        structural = Proof::Proven;
        floor = None;
        ctx.count(Counter::LinkedBack);
        ctx.linked.store(true, Ordering::SeqCst);
    }
    if structural == Proof::Missing {
        return Err(Fail::Visibility);
    }
    // The empty plan: the local database becomes the
    // first snapshot. Modelled as the empty database.
    if plan.head() == EMPTY {
        return Ok((EMPTY, 0, Via::Empty));
    }
    // The local file at the plan's head (`local_is_at`).
    if let Some((at, sum)) = file
        && at == plan.head()
    {
        let at_head = [(at, Some(sum))];
        let usable = match structural {
            Proof::NeedsReplay => prove(floor, &at_head) == Proof::Proven,
            _ => true,
        };
        if usable {
            return Ok((at, sum, Via::Local));
        }
    }
    // `Replica::download`: each object applied in
    // order, its pre-apply checksum checked, then the floor proven on the
    // replayed sums before anything is installed.
    let mut chain = chain;
    let mut state = 0u64;
    for slot in &mut chain {
        let id = slot.0;
        let key = if Some(id) == plan.snapshot {
            snapshot_key(id)
        } else {
            let s = listing.segments.iter().find(|s| s.id == id).unwrap();
            segment_key(s)
        };
        let latency = rng.lock().unwrap().below(5);
        sleep(Duration::from_millis(1 + latency)).await;
        let Some(body) = ctx.mem.get(&key) else {
            return Err(Fail::Restore); // vanished
        };
        if Some(id) != plan.snapshot && body.pre != state {
            return Err(Fail::Restore);
        }
        state = body.post;
        slot.1 = Some(state);
    }
    match prove(floor, &chain) {
        Proof::Proven => {}
        Proof::Conflict => return Err(Fail::Restore),
        Proof::Missing | Proof::NeedsReplay => return Err(Fail::Visibility),
    }
    *node.file.lock().unwrap() = Some((plan.head(), state));
    Ok((plan.head(), state, Via::Replay))
}

/// `Replica::restore`: the discovery loop, then `verify` into the cell.
async fn restore(
    ctx: &Ctx,
    node: &Node,
    rng: &Mutex<Rng>,
    cell: &Arc<Mutex<Option<Hold>>>,
    valid: &AtomicBool,
) -> Result<(SegmentId, u64, Via), Fail> {
    // `LeaseHold::floor`, read once.
    let floor = cell
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|h| h.head.inherited.filter(|_| h.head.recorded));
    let deadline = Instant::now() + DISCOVERY;
    let mut attempt = 0;
    let restored = loop {
        let tried = restore_within(ctx, node, rng, floor.as_ref()).await;
        if tried == Err(Fail::MissingLink) {
            ctx.count(Counter::MissingLinks);
        }
        match tried {
            Err(Fail::Visibility | Fail::MissingLink) if attempt < RELIST_MS.len() => {
                if !valid.load(Ordering::SeqCst) || cell.lock().unwrap().is_none() {
                    break Err(Fail::Late);
                }
                let base = RELIST_MS[attempt];
                let jitter = rng.lock().unwrap().below(base / 5 + 1);
                let pause = Duration::from_millis(base + jitter);
                if Instant::now() + pause >= deadline {
                    break Err(Fail::Visibility);
                }
                attempt += 1;
                ctx.count(Counter::Relists);
                sleep(pause).await;
            }
            Err(Fail::MissingLink) => break Err(Fail::Restore),
            other => break other,
        }
    };
    let (head, sum, via) = restored?;
    hold_cell::verify(
        cell,
        || valid.load(Ordering::SeqCst),
        position_of(head, sum),
    )
    .map_err(|_| Fail::Late)?;
    Ok((head, sum, via))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Release,
    Lapse,
    Crash,
}

/// The end of a hold, as the server's paths do it.
async fn end_hold(
    ctx: &Ctx,
    node: &Node,
    proc: &Arc<Proc>,
    cell: &Arc<Mutex<Option<Hold>>>,
    valid: &AtomicBool,
    ending: Ending,
    delay: u64,
) {
    match ending {
        Ending::Release => {
            // `LeaseHold::release`: frozen and registered, then the
            // conditional release.
            let Some(hold) = hold_cell::end(cell) else {
                return;
            };
            proc.local.lock().unwrap().register(&hold);
            sleep(Duration::from_millis(delay)).await;
            if proc.owners.release(&hold).await == Released::Released {
                proc.local
                    .lock()
                    .unwrap()
                    .released(&hold.key, hold.generation);
            }
        }
        Ending::Lapse => {
            // The epoch moves, then `end_all` freezes and registers.
            valid.store(false, Ordering::SeqCst);
            sleep(Duration::from_millis(delay % 3)).await;
            if let Some(hold) = hold_cell::end(cell) {
                proc.local.lock().unwrap().register(&hold);
                proc.pending.lock().unwrap().push(hold);
            }
            fence(&ctx.mem, node.name, &proc.instance);
            ctx.count(Counter::Lapses);
        }
        Ending::Crash => {
            valid.store(false, Ordering::SeqCst);
            let _ = hold_cell::end(cell);
            fence(&ctx.mem, node.name, &proc.instance);
            let n = {
                let mut r = node.restarts.lock().unwrap();
                *r += 1;
                *r
            };
            *node.proc.lock().unwrap() = proc_for(&ctx.mem, node.name, format!("i{n}"));
            ctx.count(Counter::Crashes);
        }
    }
}

/// `end_hold` at `end_at` ms from now, on its own task.
#[allow(clippy::too_many_arguments)]
fn spawn_end(
    ctx: &Arc<Ctx>,
    node: &Arc<Node>,
    proc: &Arc<Proc>,
    cell: &Arc<Mutex<Option<Hold>>>,
    valid: &Arc<AtomicBool>,
    ending: Ending,
    end_at: u64,
    delay: u64,
) -> madsim::task::JoinHandle<()> {
    let (ctx, node, proc, cell, valid) = (
        Arc::clone(ctx),
        Arc::clone(node),
        Arc::clone(proc),
        Arc::clone(cell),
        Arc::clone(valid),
    );
    madsim::task::spawn(async move {
        sleep(Duration::from_millis(end_at)).await;
        end_hold(&ctx, &node, &proc, &cell, &valid, ending, delay).await;
    })
}

/// How a hand-over is scheduled.
#[derive(Debug, Clone, Copy, Default)]
struct Shape {
    /// Its lapse or crash comes during discovery.
    early: bool,
    /// Review M1's shape: a read-only holder snapshots at the head it
    /// restored, then crashes.
    m1: bool,
}

/// A pending release retried (`Pending` in lease_hold.rs); a stale one
/// must not change a successor's record.
async fn retry_pending(ctx: &Ctx, proc: &Arc<Proc>) {
    let entries = std::mem::take(&mut *proc.pending.lock().unwrap());
    for hold in entries {
        let before = ctx.mem.record();
        let r = proc.owners.release(&hold).await;
        let after = ctx.mem.record();
        if let (Some(b), Some(a)) = (&before, &after)
            && b.generation > hold.generation
        {
            ctx.count(Counter::StaleReleases);
            if a.generation != b.generation || a.holder != b.holder || r == Released::Released {
                ctx.fail(format!(
                    "a stale release at {} changed the record at {}",
                    hold.generation, b.generation
                ));
            }
        }
        match r {
            Released::Released => proc
                .local
                .lock()
                .unwrap()
                .released(&hold.key, hold.generation),
            Released::Pending | Released::Unchanged => proc.pending.lock().unwrap().push(hold),
            Released::Superseded => {}
        }
    }
}

/// `LeaseHold::acquire_with`.
async fn acquire(proc: &Arc<Proc>) -> Result<Hold, AcquireError> {
    let mut prepared = false;
    let acquired = proc
        .owners
        .acquire_prepared(
            KEY,
            || true,
            async || Ok(1),
            |record, attempted| {
                prepared = true;
                let mut local = proc.local.lock().unwrap();
                Ok(local
                    .prepare(KEY, record, attempted, proc.owners.node(), &proc.instance)
                    .zip(local.get(KEY).map(|e| e.generation)))
            },
        )
        .await;
    match &acquired {
        Ok(hold) => proc.local.lock().unwrap().transferred(KEY, hold.generation),
        Err(_) if prepared => proc.local.lock().unwrap().finished(KEY),
        Err(_) => {}
    }
    acquired
}

/// One hand-over: acquire, restore (cycles until activated), upload while
/// the end comes at a seeded moment.
#[allow(clippy::too_many_lines)]
async fn hand_over(
    ctx: &Arc<Ctx>,
    node: &Arc<Node>,
    rng: &Arc<Mutex<Rng>>,
    ending: Ending,
    lag: Duration,
    before: Duration,
    shape: Shape,
) {
    let mut cycles = 0;
    // A lapse or crash during discovery (`Shape::early`): the end is
    // armed before the restore, on the first cycle.
    let mut early = None;
    let mut late_cycle = false;
    let (cell, valid, proc, head, sum) = loop {
        cycles += 1;
        if cycles > 12 {
            ctx.fail(format!("node {} never activated", node.name));
            return;
        }
        let proc = Arc::clone(&node.proc.lock().unwrap());
        retry_pending(ctx, &proc).await;
        live(&ctx.mem, node.name, &proc.instance);
        let hold = match acquire(&proc).await {
            Ok(h) => h,
            Err(e) => {
                ctx.fail(format!("acquisition by {} failed: {e:?}", node.name));
                return;
            }
        };
        let generation = hold.generation;
        let cell = Arc::new(Mutex::new(Some(hold)));
        let valid = Arc::new(AtomicBool::new(true));
        // An advancing acknowledgement before verification is late.
        if hold_cell::acknowledge(&cell, || true, Some(Position::Empty)).is_ok() {
            ctx.fail("an unverified hold acknowledged an upload".into());
        }
        if cycles == 1 && shape.early {
            let end_at = rng.lock().unwrap().below(400);
            early = Some(spawn_end(
                ctx, node, &proc, &cell, &valid, ending, end_at, 0,
            ));
        }
        let restored = restore(ctx, node, rng, &cell, &valid).await;
        if restored.is_err()
            && let Some(ender) = early.take()
        {
            // Ended during discovery: late (or refused) with nothing
            // uploaded; a new cycle, its end drawn after activation.
            if restored == Err(Fail::Late) {
                ctx.count(Counter::Late);
            }
            ender.await.unwrap();
            end_hold(ctx, node, &proc, &cell, &valid, Ending::Release, 0).await;
            late_cycle = true;
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        match restored {
            Ok((head, sum, via)) => {
                let (last, last_sum) = *ctx.acked.lock().unwrap();
                if !descends(&ctx.lineage, sum, last_sum) {
                    if ctx.out.lock().unwrap().violation.is_none() {
                        ctx.out.lock().unwrap().via_linked_back = ctx.linked.load(Ordering::SeqCst);
                    }
                    ctx.fail(format!(
                        "generation {generation} activated at {head:?} ({via:?}), \
                         not descending from the acknowledged {last:?}"
                    ));
                }
                ctx.count(Counter::Activations);
                match via {
                    Via::Local => ctx.count(Counter::LocalFile),
                    Via::Replay => ctx.count(Counter::Replays),
                    Via::Empty => {}
                }
                if cycles == 1 {
                    ctx.count(Counter::FirstCycle);
                } else if before <= Duration::from_secs(5) && !late_cycle {
                    // Liveness: the predecessor's uploads became visible
                    // within the discovery bound.
                    ctx.fail(format!(
                        "generation {generation}: a lag within the bound took {cycles} cycles"
                    ));
                }
                break (cell, valid, proc, head, sum);
            }
            Err(_) => {
                ctx.count(Counter::Refusals);
                // The activation failed: the hold is dropped and released,
                // unverified, with what it inherited.
                end_hold(ctx, node, &proc, &cell, &valid, Ending::Release, 0).await;
                // The cooldown (lease_hold.rs, `refused`), then a new cycle.
                sleep(Duration::from_secs(1 << cycles.min(4))).await;
            }
        }
    };
    let generation = cell.lock().unwrap().as_ref().unwrap().generation;

    // The end, concurrent with uploads: armed during discovery, or now
    // (after a read-only holder's snapshot in the M1 shape).
    let end_at = if shape.m1 {
        100 + rng.lock().unwrap().below(200)
    } else {
        rng.lock().unwrap().below(300)
    };
    let delay = rng.lock().unwrap().below(50);
    let ender = match early.take() {
        Some(e) => e,
        None => spawn_end(ctx, node, &proc, &cell, &valid, ending, end_at, delay),
    };

    let ok = |valid: &AtomicBool| valid.load(Ordering::SeqCst);
    let (mut head, mut sum) = (head, sum);

    // A periodic snapshot due right after restore (review M1: labelled at
    // an earlier generation's head, never collected by this owner).
    // Zero writes: an activation for a read-only event.
    let snapshot_first = shape.m1 || rng.lock().unwrap().chance(30);
    let writes = if shape.m1 {
        0
    } else {
        rng.lock().unwrap().below(8)
    };
    for i in 0..=writes {
        if (i == 0 && snapshot_first) || (i > 0 && rng.lock().unwrap().chance(20)) {
            if head != EMPTY
                && ship_snapshot(
                    ctx,
                    rng,
                    &cell,
                    &valid,
                    head,
                    // Broken: the snapshot holds another database.
                    if shape.m1 && ctx.breaks == Breaks::Mislabel {
                        sum ^ 0x5a5a
                    } else {
                        sum
                    },
                    generation,
                    lag,
                )
                .await
            {
                ctx.count(Counter::Collections);
            }
            if cell.lock().unwrap().is_none() {
                break;
            }
        }
        if i == writes {
            break;
        }
        // The next transaction: a first snapshot at (g, 1) on an empty
        // plan (`SnapshotKind::First`), else a segment on the head.
        let id = SegmentId {
            generation,
            txid: head.txid + 1,
        };
        let post = mix(sum, id);
        ctx.lineage.lock().unwrap().insert(post, sum);
        let key = if head == EMPTY {
            snapshot_key(id)
        } else {
            segment_key(&Segment {
                id,
                parent_generation: head.generation,
            })
        };
        let put = rng.lock().unwrap().below(30);
        sleep(Duration::from_millis(1 + put)).await;
        if !ctx
            .mem
            .create(key, Body { pre: sum, post }, object_lag(rng, lag))
        {
            break;
        }
        *node.file.lock().unwrap() = Some((id, post));
        let acked = hold_cell::acknowledge(&cell, || ok(&valid), Some(position_of(id, post)));
        if acked.is_err() {
            // Landed after the end: a late upload, never acknowledged.
            ctx.count(Counter::LateUploads);
            break;
        }
        *ctx.acked.lock().unwrap() = (id, post);
        (head, sum) = (id, post);
        let gap = rng.lock().unwrap().below(40);
        sleep(Duration::from_millis(gap)).await;
    }
    ender.await.unwrap();
    // After the end: nothing more is acknowledged or verified.
    if hold_cell::acknowledge(&cell, || true, None).is_ok()
        || hold_cell::verify(&cell, || true, Position::Empty).is_ok()
    {
        ctx.fail("an ended hold acknowledged or verified".into());
    }
}

/// When one object becomes listed: a lag within the discovery bound is
/// drawn per object, so children are listed before parents and snapshots
/// before their segments; a long one holds every object back.
fn object_lag(rng: &Mutex<Rng>, lag: Duration) -> Duration {
    if lag >= Duration::from_secs(20) {
        return lag;
    }
    let ms = u64::try_from(lag.as_millis()).unwrap();
    Duration::from_millis(rng.lock().unwrap().below(ms + 1))
}

/// `Replica::ship_snapshot` (periodic, at the head) then collection.
#[allow(clippy::too_many_arguments)]
async fn ship_snapshot(
    ctx: &Ctx,
    rng: &Mutex<Rng>,
    cell: &Mutex<Option<Hold>>,
    valid: &AtomicBool,
    head: SegmentId,
    sum: u64,
    generation: u64,
    lag: Duration,
) -> bool {
    sleep(Duration::from_millis(3)).await;
    // `put_once`: a create; one already there is not this upload's.
    if !ctx.mem.create(
        snapshot_key(head),
        Body {
            pre: sum,
            post: sum,
        },
        object_lag(rng, lag),
    ) {
        return false;
    }
    // A snapshot at the head: a check, never an advance.
    let before = cell.lock().unwrap().as_ref().map(|h| h.head);
    if hold_cell::acknowledge(cell, || valid.load(Ordering::SeqCst), None).is_err() {
        return false;
    }
    if cell.lock().unwrap().as_ref().map(|h| h.head) != before {
        ctx.fail("a snapshot moved the head".into());
    }
    // Collection after an acknowledged snapshot (ADR 0012), from a
    // listing that may lag.
    let listing = Listing::from_keys(ctx.mem.list().iter().map(String::as_str));
    let doomed = collectable(&listing, generation, head);
    for key in &doomed {
        ctx.mem.delete(key);
    }
    !doomed.is_empty()
}

fn simulate(seed: u64, breaks: Breaks) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        START.with(|s| s.set(Some(Instant::now())));
        let mem = Mem::default();
        let ctx = Arc::new(Ctx {
            mem: mem.clone(),
            lineage: Arc::default(),
            breaks,
            out: Mutex::new(Outcome::default()),
            linked: AtomicBool::new(false),
            acked: Mutex::new((EMPTY, 0)),
        });
        let rng = Arc::new(Mutex::new(Rng::new(seed ^ 0x0035_5707)));
        let nodes: Vec<Arc<Node>> = NODES
            .iter()
            .map(|&name| {
                Arc::new(Node {
                    name,
                    proc: Mutex::new(proc_for(&mem, name, "i0".into())),
                    restarts: Mutex::new(0),
                    file: Mutex::new(None),
                })
            })
            .collect();
        let rounds = 4 + rng.lock().unwrap().below(6);
        let mut before = Duration::ZERO;
        for _ in 0..rounds {
            let pick = usize::try_from(rng.lock().unwrap().below(3)).unwrap();
            let (ending, lag, shape) = {
                let mut r = rng.lock().unwrap();
                let shape = match r.below(10) {
                    0 => Shape {
                        early: true,
                        m1: false,
                    },
                    1..=2 => Shape {
                        early: false,
                        m1: true,
                    },
                    _ => Shape::default(),
                };
                let ending = match r.below(10) {
                    _ if shape.m1 => Ending::Crash,
                    n if shape.early => {
                        if n < 5 {
                            Ending::Lapse
                        } else {
                            Ending::Crash
                        }
                    }
                    0..=5 => Ending::Release,
                    6..=7 => Ending::Lapse,
                    _ => Ending::Crash,
                };
                // Crash-time LIST lag is the unsupported case: a lapse or
                // a crash leaves its uploads visible at once.
                let lag = match (ending, r.below(3)) {
                    (Ending::Release, 1) => Duration::from_millis(r.below(5_000)),
                    (Ending::Release, 2) => Duration::from_secs(20),
                    _ => Duration::ZERO,
                };
                (ending, lag, shape)
            };
            hand_over(&ctx, &nodes[pick], &rng, ending, lag, before, shape).await;
            before = lag;
            if ctx.out.lock().unwrap().violation.is_some() {
                break;
            }
            // A lapsed process's pending releases are sent again late,
            // now and then before anyone else acquires.
            for n in &nodes {
                if rng.lock().unwrap().chance(30) {
                    let p = Arc::clone(&n.proc.lock().unwrap());
                    retry_pending(&ctx, &p).await;
                }
            }
        }
        let ctx = Arc::try_unwrap(ctx).ok().expect("one owner");
        ctx.out.into_inner().unwrap()
    })
}

#[test]
fn hand_overs_in_the_server_order_keep_every_acknowledged_write() {
    let mut totals: BTreeMap<Counter, u64> = BTreeMap::new();
    for seed in 0..seeds() {
        let o = simulate(seed, Breaks::None);
        if let Some(v) = o.violation {
            panic!("seed {seed}: {v}");
        }
        for (c, n) in o.counts {
            *totals.entry(c).or_default() += n;
        }
    }
    eprintln!("{} seeds: {totals:?}", seeds());
    for c in [
        Counter::Activations,
        Counter::FirstCycle,
        Counter::MissingLinks,
        Counter::Late,
        Counter::Refusals,
        Counter::Relists,
        Counter::LinkedBack,
        Counter::LocalFile,
        Counter::Replays,
        Counter::Lapses,
        Counter::Crashes,
        Counter::Collections,
        Counter::LateUploads,
        Counter::StaleReleases,
    ] {
        assert!(
            totals.get(&c).copied().unwrap_or(0) > 0,
            "{c:?} never exercised"
        );
    }
}

#[test]
fn broken_orderings_are_caught() {
    for breaks in [Breaks::LocalFirst, Breaks::SnapshotProves, Breaks::Mislabel] {
        let caught = (0..seeds())
            .filter(|&seed| {
                let o = simulate(seed, breaks);
                // A mislabelled snapshot must be caught where only its
                // label is trusted: after `linked_back`.
                o.violation.is_some() && (breaks != Breaks::Mislabel || o.via_linked_back)
            })
            .count() as u64;
        eprintln!("{} seeds: {breaks:?} caught on {caught}", seeds());
        let floor = (seeds() / 20).max(1);
        assert!(
            caught >= floor,
            "{breaks:?}: caught on {caught}, want {floor}"
        );
    }
}
