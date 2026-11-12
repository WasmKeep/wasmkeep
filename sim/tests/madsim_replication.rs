//! The replication protocol of ADR 0007 under madsim: three nodes take turns
//! owning one actor through node leases and its ownership record, as the
//! server runs them since ADR 0020 (`wasmkeep_sim::node`, whose holds act by
//! the server's own rule, `wasmkeep_cluster::holds`), restore with the real
//! planner (`wasmkeep_cluster::replication`), and upload one segment per
//! transaction, under the same faults as the lease simulation (requests held
//! and delivered late, crashes and restarts as a new process, skewed clocks).
//! The bucket also applies some requests late (a slow PUT that still
//! succeeds), so uploads can complete after the uploader's hold ended.
//!
//! Snapshots record the positions they hold, and are taken as the server
//! takes them (every `SNAPSHOT_EVERY` segments, counted from the restore, so
//! possibly right after a restore at the previous owner's head; one whose
//! upload failed is tried again after the next segment, then after two
//! more, four, so a snapshot can land late next to the one that retried
//! it). With
//! collection on, owners delete history by ADR 0012's rule
//! (`collectable`) after their own acknowledged snapshots, through a client
//! whose deletes can time out and land late.
//!
//! At the end the bucket is restored once more and checked: the plan exists,
//! and every write acknowledged under the ADR's rule is in the restored
//! history (the snapshot the plan starts from, or the segments after it).
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Bucket, LeaseConfig};
use wasmkeep_cluster::replication::{
    Listing, Plan, Segment, SegmentId, collectable, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_sim::driver::Env;
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{EpBucket, Faults, Store, bucket_node_with};

/// The actor's ownership record (ADR 0020).
const RECORD: &str = "owners/t/c/a";
/// Node lease objects.
const NODES: &str = "nodes/";
const PREFIX: &str = "actors/t/c/a/";
const EPOCH_MS: u64 = 1_000_000;
const SNAPSHOT_EVERY: u64 = 5;
/// How long a node waits for the bucket: longer than its slowest requests,
/// so a slow upload can succeed after the grant ran out.
const WAIT: Duration = Duration::from_secs(3);
/// How long collection's deletes wait: shorter than the slowest requests,
/// so a delete can time out and still land later, after the next owner
/// restored (an S3 client timing out and retrying).
const DELETE_WAIT: Duration = Duration::from_secs(1);

/// Whether nodes follow the acknowledgement rule (always, except to prove
/// the rule matters).
#[derive(Debug, Clone, Copy)]
struct Rules {
    /// Acknowledge only uploads that completed while the hold could act.
    ack_within_grant: bool,
    /// Unsafe, to prove the hold's own validity matters: acknowledge
    /// whenever the node lease is valid, even after a lapse ended the hold
    /// (and its release may have landed, and another node taken it).
    ack_on_lease_only: bool,
    /// Whether owners give the actor up by releasing the lease.
    releases: Releases,
    /// Whether owners collect history after their snapshots (ADR 0012).
    collect: Collect,
}

/// Garbage collection (ADR 0012), after a periodic snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Collect {
    /// Never (the protocol of ADR 0007 alone).
    Off,
    /// By the rule: after an acknowledged snapshot of one's own generation,
    /// `collectable`.
    Safe,
    /// Unsafe, to prove it matters: also delete the segment at the
    /// snapshot's position, keeping only the snapshot.
    DropSegmentAtSnapshot,
}

/// How owners release the lease (production: when an actor hibernates, the
/// node shuts down, or an upload fails).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Releases {
    /// Never: leases only run out.
    Never,
    /// As production does: after the last upload finished (and was acked
    /// or not), and at once when an upload fails.
    AfterUploads,
    /// Unsafe, to prove the check matters: release, keep working for a
    /// moment, then still upload and acknowledge one more write.
    AckAfterRelease,
}

const SAFE: Rules = Rules {
    ack_within_grant: true,
    ack_on_lease_only: false,
    releases: Releases::Never,
    collect: Collect::Off,
};

const SAFE_RELEASING: Rules = Rules {
    ack_within_grant: true,
    ack_on_lease_only: false,
    releases: Releases::AfterUploads,
    collect: Collect::Off,
};

const SAFE_COLLECTING: Rules = Rules {
    ack_within_grant: true,
    ack_on_lease_only: false,
    releases: Releases::AfterUploads,
    collect: Collect::Safe,
};

/// Keys under the actor prefix, as a listing.
fn read_listing<'a>(keys: impl IntoIterator<Item = &'a str>) -> Listing {
    Listing::from_keys(keys.into_iter().filter_map(|k| k.strip_prefix(PREFIX)))
}

/// A snapshot's body: the positions of the history it holds.
fn encode_positions(positions: &BTreeSet<SegmentId>) -> Vec<u8> {
    positions
        .iter()
        .map(|p| format!("{}:{}", p.generation, p.txid))
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

fn decode_positions(body: &str) -> BTreeSet<SegmentId> {
    body.split(',')
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

/// Every position a restore by `plan` holds: those in the snapshot it
/// starts from (its body), then the segments after it.
fn restored(plan: &Plan, objects: &[(String, String)]) -> Result<BTreeSet<SegmentId>, String> {
    let mut held = BTreeSet::new();
    if let Some(snapshot) = plan.snapshot {
        let key = format!("{PREFIX}{}", snapshot_key(snapshot));
        let body = objects
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, b)| b.as_str())
            .ok_or_else(|| format!("snapshot {snapshot:?} is not in the listing"))?;
        held = decode_positions(body);
    }
    held.extend(plan.segments.iter().copied());
    Ok(held)
}

thread_local! {
    /// Uploads in this run that succeeded after their hold ended. The
    /// simulation runs on one thread, so a thread-local counts one run.
    static LATE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Objects collected in this run.
    static COLLECTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn count_collected(n: usize) {
    COLLECTED.with(|c| c.set(c.get() + u64::try_from(n).unwrap()));
}

fn trace(msg: &str) {
    if std::env::var_os("WASMKEEP_SIM_TRACE").is_some() {
        eprintln!("[{:?}] {}", madsim::time::Instant::now(), msg);
    }
}

/// What one owner knows: its hold, the database head it has, every
/// position its database holds, the segments since its last snapshot
/// (counted from the restore, as the server does), and when a snapshot
/// that failed is tried again (the server's `Replica::snapshot_due`).
struct Owner {
    held: Held,
    head: SegmentId,
    held_positions: BTreeSet<SegmentId>,
    since_snapshot: u64,
    snapshot_failures: u32,
    snapshot_retry_at: u64,
}

type Node = LeaseNode<EpBucket, NodeClock>;

/// A node's clock: global time shifted by its skew (`Copy`, as the node
/// driver needs).
#[derive(Clone, Copy)]
struct NodeClock {
    time: GlobalTime,
    skew_ms: i64,
}

impl wasmkeep_cluster::lease::Clock for NodeClock {
    fn now_ms(&self) -> u64 {
        self.time.now().saturating_add_signed(self.skew_ms)
    }
}

/// A periodic snapshot of the owner's database at its head, taken as the
/// server does once `SNAPSHOT_EVERY` segments were applied since the last
/// one (including segments restored, so possibly at the previous owner's
/// head); then garbage collection by `rules.collect`.
async fn snapshot_and_collect(
    node: &Node,
    bucket: &EpBucket,
    deletes: &EpBucket,
    rules: Rules,
    o: &mut Owner,
) {
    if o.since_snapshot < SNAPSHOT_EVERY.max(o.snapshot_retry_at) || o.held_positions.is_empty() {
        return;
    }
    let at = o.head;
    let key = format!("{PREFIX}{}", snapshot_key(at));
    // Created and acknowledged (ADR 0012, condition 1), or no collection.
    let created = bucket
        .create(&key, encode_positions(&o.held_positions))
        .await;
    match created {
        // There (already, for a conflict: an earlier try landed late):
        // the count starts again, as the server's does.
        Ok(_) | Err(wasmkeep_cluster::lease::BucketError::Conflict) => {
            o.since_snapshot = 0;
            o.snapshot_failures = 0;
            o.snapshot_retry_at = 0;
        }
        // Failed, or unanswered and perhaps landing later: the count
        // stays, and the next try comes after the next segment, then
        // after one more, two, four, up to the interval.
        Err(_) => {
            let wait = match o.snapshot_failures {
                0 => 0,
                n => 1u64
                    .checked_shl(n - 1)
                    .map_or(SNAPSHOT_EVERY, |w| w.min(SNAPSHOT_EVERY)),
            };
            o.snapshot_failures += 1;
            o.snapshot_retry_at = o.since_snapshot + wait;
        }
    }
    let created = created.is_ok();
    if !created || !node.may_act(&o.held) || rules.collect == Collect::Off {
        return;
    }
    // Condition 2 is in `collectable`: nothing after a snapshot at the
    // previous owner's head. (Dropping it breaks a restore only in an
    // interleaving this model does not reach within thousands of seeds;
    // `wasmkeep_cluster::replication`'s unit tests script it instead.)
    let owner_generation = o.held.generation;
    let Ok(objects) = bucket.list(PREFIX).await else {
        return;
    };
    let listing = read_listing(objects.iter().map(|(k, _)| k.as_str()));
    let mut gone = collectable(&listing, owner_generation, at);
    if rules.collect == Collect::DropSegmentAtSnapshot && !gone.is_empty() {
        if let Some(s) = listing.segments.iter().find(|s| s.id == at) {
            gone.push(segment_key(s));
        }
    }
    if !gone.is_empty() {
        count_collected(gone.len());
    }
    for key in gone {
        // Condition 3: the hold, before each delete (a batch of one here).
        if !node.may_act(&o.held) {
            return;
        }
        let _ = deletes.delete(&format!("{PREFIX}{key}")).await;
    }
}

/// One process's life on node leases: until its lease is lost (fenced, or
/// replaced by its own restart), then it exits.
async fn run_node(
    node: &Node,
    bucket: &EpBucket,
    deletes: &EpBucket,
    rules: Rules,
    rng: &mut Rng,
    acked: &Mutex<Vec<SegmentId>>,
    problems: &Mutex<Vec<String>>,
) {
    let mut owner: Option<Owner> = None;
    loop {
        sleep(Duration::from_millis(100)).await;
        if node.lost() {
            trace(&format!("{} lost the node lease: exits", node.id()));
            return;
        }
        let Some(o) = owner.as_mut() else {
            let held = match node.acquire(RECORD, 1).await {
                Ok(held) => held,
                Err(e) => {
                    trace(&format!("{} acquire: {e:?}", node.id()));
                    continue;
                }
            };
            // Restore: list, plan, take the head.
            let Ok(objects) = bucket.list(PREFIX).await else {
                // Production gives the actor up: a later acquisition starts
                // a new generation.
                let _ = node.release(&held).await;
                continue;
            };
            let listing = read_listing(objects.iter().map(|(k, _)| k.as_str()));
            match plan_restore(&listing)
                .map_err(|e| e.to_string())
                .and_then(|plan| restored(&plan, &objects).map(|held| (plan, held)))
            {
                Ok((plan, positions)) => {
                    trace(&format!(
                        "{} gen {} (valid until {:?}) restored to {:?}",
                        node.id(),
                        held.generation,
                        node.valid_until_ms(),
                        plan.head()
                    ));
                    let mut o = Owner {
                        held,
                        head: plan.head(),
                        held_positions: positions,
                        since_snapshot: u64::try_from(plan.segments.len()).unwrap(),
                        snapshot_failures: 0,
                        snapshot_retry_at: 0,
                    };
                    // As after the server's first event: a snapshot is due
                    // if enough segments were restored, at the restored
                    // head, whoever wrote it.
                    snapshot_and_collect(node, bucket, deletes, rules, &mut o).await;
                    owner = Some(o);
                }
                Err(e) => problems
                    .lock()
                    .unwrap()
                    .push(format!("restore failed: {e}")),
            }
            continue;
        };

        if !node.may_act(&o.held) {
            trace(&format!(
                "{} hold gen {} may not act: dropped",
                node.id(),
                o.held.generation
            ));
            owner = None;
            continue;
        }

        // Now and then the owner gives the actor up (hibernation, shutdown).
        // Its previous upload has finished, as in production, where an
        // actor's state (and hold) is dropped only after its last event.
        let mut released = false;
        if rules.releases != Releases::Never && rng.chance(4) {
            trace(&format!("releases gen {}", o.held.generation));
            let held = o.held.clone();
            let _ = node.release(&held).await;
            if rules.releases == Releases::AfterUploads {
                owner = None;
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            // AckAfterRelease: work that goes on after the release (an
            // event still running), then one more write, wrongly acked.
            sleep(Duration::from_millis(300)).await;
            released = true;
        }

        // One transaction, built on the current head.
        let segment = Segment {
            id: SegmentId {
                generation: o.held.generation,
                txid: o.head.txid + 1,
            },
            parent_generation: o.head.generation,
        };
        let key = format!("{PREFIX}{}", segment_key(&segment));
        if bucket.create(&key, b"x".to_vec()).await.is_err() {
            trace(&format!("{} upload failed {:?}", node.id(), segment.id));
            // Unknown whether it landed: stop, and restore again later.
            // Production gives the actor up and releases at once.
            if rules.releases != Releases::Never {
                let held = o.held.clone();
                let _ = node.release(&held).await;
            }
            owner = None;
            continue;
        }
        // The rule: acknowledge only while the hold may act. The broken
        // variants ask only the node lease: after the hold was released
        // (AckAfterRelease reaches here only then), or ended by a lapse.
        let may_ack = if released || rules.ack_on_lease_only {
            node.lease_valid()
        } else {
            node.may_act(&o.held)
        };
        if !rules.ack_within_grant || may_ack {
            trace(&format!(
                "{} acked {:?} on {:?}",
                node.id(),
                segment.id,
                o.head
            ));
            acked.lock().unwrap().push(segment.id);
        } else {
            trace(&format!(
                "{} landed late {:?} on {:?}",
                node.id(),
                segment.id,
                o.head
            ));
            LATE.with(|l| l.set(l.get() + 1));
        }
        o.head = segment.id;
        o.held_positions.insert(segment.id);
        o.since_snapshot += 1;
        if released {
            owner = None;
            continue;
        }
        // Best effort: a failed snapshot only costs restore time.
        snapshot_and_collect(node, bucket, deletes, rules, o).await;
    }
}

/// The simulation's services for a node.
#[derive(Clone, Copy)]
struct SimEnv(GlobalTime);

impl Env for SimEnv {
    fn global_ms(&self) -> u64 {
        self.0.now()
    }

    async fn sleep_ms(&self, ms: u64) {
        sleep(Duration::from_millis(ms)).await;
    }
}

#[derive(Clone, Copy)]
struct GlobalTime(Instant);

impl GlobalTime {
    fn now(&self) -> u64 {
        EPOCH_MS + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

struct Outcome {
    acked: Vec<SegmentId>,
    /// Uploads that succeeded after the uploader's hold ended.
    late: u64,
    /// Objects garbage collection deleted (or sent deletes for).
    collected: u64,
    /// Fences nodes wrote over peers cut off from the bucket.
    fences: u64,
    problems: Vec<String>,
    store: Store,
}

fn simulate(seed: u64, rules: Rules, duration: Duration) -> Outcome {
    LATE.with(|l| l.set(0));
    COLLECTED.with(|c| c.set(0));
    let _ = take_fences();
    let config = LeaseConfig::new(1_000, 500, 200, 300).unwrap();
    let max_skew_ms = 300;
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let acked = Arc::new(Mutex::new(Vec::new()));
    let problems = Arc::new(Mutex::new(Vec::new()));
    let store = Store::default();
    let (acked_out, problems_out, store_out) = (
        Arc::clone(&acked),
        Arc::clone(&problems),
        Arc::clone(&store),
    );
    rt.block_on(async move {
        let handle = Handle::current();
        let time = GlobalTime(Instant::now());
        let mut rng = Rng::new(seed ^ 0x5EED_0007);
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init(move || bucket_node_with(Arc::clone(&store), Faults { slow_one_in: 15 }))
            .build();
        let mut nodes = Vec::new();
        for i in 1..=3u32 {
            let span = max_skew_ms * 2 + 1;
            let skew_ms =
                i64::try_from(rng.below(span)).unwrap() - i64::try_from(max_skew_ms).unwrap();
            let (acked, problems) = (Arc::clone(&acked), Arc::clone(&problems));
            // Not drawn from `rng`: existing seeds keep their fault schedules.
            let node_seed = seed ^ (u64::from(i) << 40) ^ 0x0E1E_A5E0;
            let node = handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || {
                    let (acked, problems) = (Arc::clone(&acked), Arc::clone(&problems));
                    async move {
                        // A new process each start (and restart): its own
                        // instance, through the start check.
                        let clock = NodeClock { time, skew_ms };
                        let instance = format!("{:08x}", madsim::rand::random::<u32>());
                        // Lease writes and peers' reads bounded as the
                        // server bounds a lease write (a third of ttl minus
                        // safety); records and uploads wait longer.
                        let attempt =
                            Duration::from_millis((config.ttl_ms() - config.safety_ms()) / 3);
                        let setup = NodeSetup {
                            id: format!("n{i}"),
                            peers: (1..=3u32)
                                .filter(|&p| p != i)
                                .map(|p| format!("n{p}"))
                                .collect(),
                            nodes_prefix: NODES.to_owned(),
                            config,
                            attempt_ms: u64::try_from(attempt.as_millis()).unwrap(),
                            breaks: Breaks::default(),
                        };
                        let buckets = NodeBuckets {
                            lease: EpBucket::connect_with_timeout(attempt).await,
                            task_records: EpBucket::connect_with_timeout(WAIT).await,
                            peers: EpBucket::connect_with_timeout(attempt).await,
                            records: EpBucket::connect_with_timeout(WAIT).await,
                        };
                        let (node, lease_task) = LeaseNode::start(
                            setup,
                            &instance,
                            clock,
                            buckets,
                            format!("n{i}\n").into_bytes(),
                            &SimEnv(time),
                        )
                        .await;
                        // The lease on a task of its own, as in the server:
                        // it dies with the node.
                        madsim::task::spawn_local(lease_task.run(SimEnv(time)));
                        let bucket = EpBucket::connect_with_timeout(WAIT).await;
                        let deletes = EpBucket::connect_with_timeout(DELETE_WAIT).await;
                        let mut rng = Rng::new(node_seed);
                        run_node(&node, &bucket, &deletes, rules, &mut rng, &acked, &problems)
                            .await;
                    }
                })
                .build();
            nodes.push(node.id());
        }

        let net = NetSim::current();
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            let i = usize::try_from(rng.below(3)).unwrap();
            // Faults at the node-lease simulation's rates, half the old
            // ones: a restarted process now spends ttl plus grace in its
            // start check before it may act, and a takeover waits for a
            // release, a restart or a fence (ADR 0020), so at the old rates
            // nodes would mostly be restarting or cut off.
            match rng.below(40) {
                // Hold the node's traffic to the bucket, deliver it late.
                0..=3 => {
                    net.clog_link(nodes[i], bucket.id());
                    net.clog_link(bucket.id(), nodes[i]);
                    // Up to 6 s, mostly longer than a lease's validity and
                    // the fence watch (a lease period plus a lease write's
                    // bound): a cut-off node is fenced and its actor taken
                    // while its own upload is still held (an upload held
                    // past the client's 3 s fails instead). With up to
                    // 2.5 s, as before ADR 0020, every variant is still
                    // caught, later (late uploads at seed 86, not 0).
                    held.push((i, at + 200 + rng.below(5_800)));
                }
                4 if !crashed.iter().any(|(n, _)| *n == i) => {
                    handle.kill(nodes[i]);
                    crashed.push((i, at + 300 + rng.below(1_500)));
                }
                _ => {}
            }
            held.retain(|(n, t)| {
                if *t <= at {
                    net.unclog_link(nodes[*n], bucket.id());
                    net.unclog_link(bucket.id(), nodes[*n]);
                    false
                } else {
                    true
                }
            });
            crashed.retain(|(n, t)| {
                if *t <= at {
                    handle.restart(nodes[*n]);
                    false
                } else {
                    true
                }
            });
        }
        // Let held traffic land: late uploads are the point.
        for (n, _) in held {
            net.unclog_link(nodes[n], bucket.id());
            net.unclog_link(bucket.id(), nodes[n]);
        }
        sleep(Duration::from_secs(3)).await;
    });
    Outcome {
        acked: acked_out.lock().unwrap().clone(),
        late: LATE.with(std::cell::Cell::get),
        collected: COLLECTED.with(std::cell::Cell::get),
        fences: take_fences(),
        problems: problems_out.lock().unwrap().clone(),
        store: store_out,
    }
}

/// Restores the final bucket state and checks it; `Err` describes a
/// violation.
fn check(outcome: &Outcome) -> Result<usize, String> {
    if let Some(p) = outcome.problems.first() {
        return Err(p.clone());
    }
    let store = outcome.store.lock().unwrap();
    let objects: Vec<(String, String)> = store
        .iter()
        .map(|(k, (b, _))| (k.clone(), String::from_utf8_lossy(b).into_owned()))
        .collect();
    let listing = read_listing(objects.iter().map(|(k, _)| k.as_str()));
    let plan = plan_restore(&listing).map_err(|e| format!("final restore: {e}"))?;
    let restored = restored(&plan, &objects)?;
    for id in &outcome.acked {
        if !restored.contains(id) {
            return Err(format!(
                "acknowledged write {id:?} lost (restored to {:?})",
                plan.head()
            ));
        }
    }
    Ok(outcome.acked.len())
}

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

#[test]
fn acknowledged_writes_survive_every_handover() {
    let (mut acked, mut generations, mut late, mut fences) = (0, 0, 0, 0);
    let only: Option<u64> = std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok());
    let seeds_run = if only.is_some() { 1 } else { seeds() };
    for seed in only.map_or(0..seeds(), |s| s..s + 1) {
        let outcome = simulate(seed, SAFE, Duration::from_secs(30));
        match check(&outcome) {
            Ok(n) => acked += u64::try_from(n).unwrap(),
            Err(v) => panic!("seed {seed}: {v}"),
        }
        late += outcome.late;
        fences += outcome.fences;
        generations += outcome
            .acked
            .iter()
            .map(|id| id.generation)
            .max()
            .unwrap_or(0);
    }
    // Guard against a vacuous pass: the runs must contain the cases the
    // rule is about (ownership changing hands, uploads landing late).
    assert!(
        acked > seeds_run * 10,
        "too few writes to mean anything ({acked})"
    );
    assert!(
        generations > seeds_run * 3,
        "ownership rarely changed ({generations})"
    );
    assert!(
        only.is_some() || late > seeds_run / 10,
        "uploads rarely landed after their hold ended ({late})"
    );
    // Takeovers by fence happened, not only releases and restarts.
    assert!(
        only.is_some() || fences > seeds_run,
        "nodes rarely fenced a cut-off peer ({fences})"
    );
    eprintln!(
        "replication: {seeds_run} seeds x 30 s: {acked} acknowledged writes, all restored; \
         {generations} generations; {late} uploads landed after their hold ended; \
         {fences} fences"
    );
}

/// Each safety rule is necessary: drop it and some seed loses data.
fn caught(rules: Rules) -> u64 {
    let seed = (0..seeds())
        .find(|&s| check(&simulate(s, rules, Duration::from_secs(30))).is_err())
        .unwrap_or_else(|| panic!("{rules:?}: no violation within the seed budget"));
    let a = check(&simulate(seed, rules, Duration::from_secs(30)));
    let b = check(&simulate(seed, rules, Duration::from_secs(30)));
    assert_eq!(a, b, "seed {seed} did not reproduce");
    seed
}

#[test]
fn acknowledging_late_uploads_is_caught() {
    let seed = caught(Rules {
        ack_within_grant: false,
        ..SAFE
    });
    eprintln!("replication: acking late uploads caught at seed {seed}");
}

#[test]
fn acknowledged_writes_survive_releases() {
    // Owners also release the lease (hibernation, shutdown, failed
    // uploads), so the next owner may take over long before the lease would
    // have run out.
    let mut acked = 0;
    for seed in 0..seeds() {
        let outcome = simulate(seed, SAFE_RELEASING, Duration::from_secs(30));
        match check(&outcome) {
            Ok(n) => acked += u64::try_from(n).unwrap(),
            Err(v) => panic!("seed {seed}: {v}"),
        }
    }
    assert!(
        acked > seeds() * 10,
        "too few writes to mean anything ({acked})"
    );
    eprintln!(
        "replication with releases: {} seeds x 30 s: {acked} acknowledged writes, all restored",
        seeds()
    );
}

#[test]
fn acknowledging_after_releasing_is_caught() {
    let seed = caught(Rules {
        releases: Releases::AckAfterRelease,
        ..SAFE
    });
    eprintln!("replication: acking after a release caught at seed {seed}");
}

#[test]
fn acknowledging_after_a_lapse_ended_the_hold_is_caught() {
    // ADR 0020: a lapse of the node lease ends every hold, into the pending
    // releases; once one lands another node may take the actor. A node that
    // goes on acknowledging because its lease is valid again, ignoring that
    // its hold ended, acknowledges writes the next owner never sees.
    let seed = caught(Rules {
        ack_on_lease_only: true,
        releases: Releases::AfterUploads,
        ..SAFE
    });
    eprintln!("replication: acking after a lapse ended the hold caught at seed {seed}");
}

#[test]
fn acknowledged_writes_survive_garbage_collection() {
    // ADR 0012: owners collect history after their snapshots, with
    // releases, late uploads and late deletes.
    let (mut acked, mut collected) = (0, 0);
    for seed in 0..seeds() {
        let outcome = simulate(seed, SAFE_COLLECTING, Duration::from_secs(30));
        match check(&outcome) {
            Ok(n) => acked += u64::try_from(n).unwrap(),
            Err(v) => panic!("seed {seed}: {v}"),
        }
        collected += outcome.collected;
    }
    assert!(
        acked > seeds() * 10,
        "too few writes to mean anything ({acked})"
    );
    assert!(
        collected > seeds() * 10,
        "collection rarely deleted anything ({collected})"
    );
    eprintln!(
        "replication with collection: {} seeds x 30 s: {acked} acknowledged writes, all \
         restored; {collected} objects collected",
        seeds()
    );
}

#[test]
fn collecting_the_segment_under_the_snapshot_is_caught() {
    let seed = caught(Rules {
        collect: Collect::DropSegmentAtSnapshot,
        ..SAFE_COLLECTING
    });
    eprintln!("replication: collecting the segment at the snapshot caught at seed {seed}");
}
