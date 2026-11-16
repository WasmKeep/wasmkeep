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
//! upload failed, a single attempt, is tried again once one more segment
//! is there and a minimum time has passed, then after eight times as many
//! segments, up to the interval, so a snapshot can land late before the
//! one that retried it). An owner here forgets that wait at every
//! restore, where a server node keeps it for an actor it restores again
//! itself: the simulation tries snapshots the server would hold back.
//! Two more differences: with an interval of 5 here, the wait after a
//! failure is 1 segment, then 5, for any growth factor of 5 or more, so
//! the simulation cannot tell the server's 8 from another such factor;
//! and a create refused as existing counts as "already there" at once,
//! where the server first looks for the object. With
//! collection on, owners delete history by ADR 0012's rule
//! (`collectable`) after their own acknowledged snapshots, through a client
//! whose deletes can time out and land late.
//!
//! With lease cuts on (ADR 0020, section 1, amended 2026-11-14), now and
//! then one node's lease writes are refused for a while, its other writes
//! landing, so its lease runs out before its lease task sees it; an event
//! that finds the hold no longer valid by the lease gives it up then, and
//! that release lands at once (answered a little later, within the
//! client's timeout, as the bucket answers one request in 25 only after
//! its client gave up). Releases are bounded as the server bounds them,
//! and uploads wait a second, so a late reply or a slow apply leaves a
//! segment that lands after its uploader gave it up, possibly after the
//! next owner restored.
//!
//! At the end the bucket is restored once more and checked: the plan exists,
//! every write acknowledged under the ADR's rule is in the restored
//! history (the snapshot the plan starts from, or the segments after it),
//! and the restored head is of the newest acknowledged write's generation
//! or a later one (no late segment of an older owner taken over a newer
//! owner's history).
//!
//! With deletes on (ADR 0028), owners also delete the actor as the server
//! does: a reset segment (the whole empty database, built on the head),
//! then a snapshot at it and the erase of everything before it by ADR
//! 0012's rule. Some deletes come right after an acquisition (a node that
//! takes the actor only to delete it, as an operator delete of an actor
//! nobody holds does, or a takeover), some from the owner in the middle of
//! its writes; owners park their hold instead of releasing it half the
//! time (ADR 0024) and wake it, a new generation, while others try to
//! acquire. Then an acknowledged write may be missing only if a delete on
//! the final chain removed it, and nothing an acknowledged delete removed
//! may be restored.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::net::NetSim;
use madsim::runtime::{Handle, Runtime};
use madsim::time::{Instant, sleep};
use wasmkeep_cluster::lease::{Bucket, LeaseConfig};
use wasmkeep_cluster::node_lease::Released;
use wasmkeep_cluster::replication::{
    Listing, Plan, Segment, SegmentId, collectable, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_sim::driver::Env;
use wasmkeep_sim::node::{Breaks, Held, LeaseNode, NodeBuckets, NodeSetup, take_fences};
use wasmkeep_sim::rng::Rng;

#[path = "support/bucket.rs"]
mod bucket;
use bucket::{CutWrites, EpBucket, Faults, Store, bucket_node_with};

/// The actor's ownership record (ADR 0020).
const RECORD: &str = "owners/t/c/a";
/// Node lease objects.
const NODES: &str = "nodes/";
const PREFIX: &str = "actors/t/c/a/";
const EPOCH_MS: u64 = 1_000_000;
const SNAPSHOT_EVERY: u64 = 5;
/// Each failure in a row multiplies the segments a failed snapshot's next
/// try waits for by this (the server's `SNAPSHOT_RETRY_GROWTH`). Capped
/// at `SNAPSHOT_EVERY` (5), so any value of 5 or more gives the same
/// waits here: 1, then 5.
const SNAPSHOT_RETRY_GROWTH: u64 = 8;
/// The least time between a snapshot that failed and its next try (the
/// server's `SNAPSHOT_RETRY_MIN`, scaled as the interval is).
const SNAPSHOT_RETRY_MIN: Duration = Duration::from_secs(1);
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
    /// Lease writes cut now and then, other writes landing; holds given
    /// up when an event finds them no longer valid by the lease; late
    /// replies; releases and uploads bounded (see the module's docs).
    lease_cuts: bool,
    /// Unsafe, to prove the order matters: a hold given up keeps acting,
    /// on its cell alone, until its release is answered
    /// (`Breaks::release_before_stop` and `Breaks::act_on_cell_only`).
    act_until_answered: bool,
    /// Whether owners delete the actor (ADR 0028).
    deletes: Deletes,
    /// Replace with a retained past state instead of deleting (ADR 0033).
    restores: bool,
}

/// Deletes (ADR 0028): a reset, the snapshot at it, then the erase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deletes {
    /// Never.
    Off,
    /// As the server does: the erase only after the snapshot at an
    /// acknowledged reset, by ADR 0012's rule.
    Safe,
    /// Unsafe, to prove it matters: everything but the reset erased as
    /// soon as it was sent, acknowledged or not.
    EraseBeforeAck,
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
    lease_cuts: false,
    act_until_answered: false,
    deletes: Deletes::Off,
    restores: false,
};

const SAFE_RELEASING: Rules = Rules {
    ack_within_grant: true,
    ack_on_lease_only: false,
    releases: Releases::AfterUploads,
    collect: Collect::Off,
    lease_cuts: false,
    act_until_answered: false,
    deletes: Deletes::Off,
    restores: false,
};

/// Lease cuts, and no hibernation: besides fences and restarts, the actor
/// changes hands only through releases after a lapse.
const SAFE_CUT: Rules = Rules {
    lease_cuts: true,
    ..SAFE
};

const SAFE_COLLECTING: Rules = Rules {
    ack_within_grant: true,
    ack_on_lease_only: false,
    releases: Releases::AfterUploads,
    collect: Collect::Safe,
    lease_cuts: false,
    act_until_answered: false,
    deletes: Deletes::Off,
    restores: false,
};

const SAFE_DELETING: Rules = Rules {
    deletes: Deletes::Safe,
    ..SAFE_RELEASING
};

const SAFE_DELETING_COLLECTING: Rules = Rules {
    deletes: Deletes::Safe,
    ..SAFE_COLLECTING
};

/// The body of a reset segment: the whole database replaced by an empty one.
const RESET: &[u8] = b"reset";

/// A delete an owner sent: its reset segment, what the deleting owner's
/// database held (what it removes), and whether it was acknowledged.
#[derive(Debug, Clone)]
struct Reset {
    /// Bucket request order and the carried restore marker, durable in state.
    request_order: u64,
    restore_marker: Option<u64>,
    id: SegmentId,
    removed: BTreeSet<SegmentId>,
    acked: bool,
}

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
    for segment in &plan.segments {
        let key = format!("{PREFIX}{}", segment_key_of(*segment, objects));
        // A reset replaces everything before it.
        if objects
            .iter()
            .any(|(k, b)| *k == key && b.as_bytes() == RESET)
        {
            held.clear();
        }
        if let Some((_, body)) = objects.iter().find(|(k, _)| *k == key)
            && let Some(saved) = body.strip_prefix("restore ")
        {
            held = decode_positions(saved);
        }
        held.insert(*segment);
    }
    Ok(held)
}

/// The key of the segment at `id` in `objects` (its parent generation is
/// part of the name).
fn segment_key_of(id: SegmentId, objects: &[(String, String)]) -> String {
    let listing = read_listing(objects.iter().map(|(k, _)| k.as_str()));
    listing
        .segments
        .iter()
        .find(|s| s.id == id)
        .map(segment_key)
        .unwrap_or_default()
}

// Independent ledger captured from owners before uploads, never from the
// backup reader's GETs. Includes late tips; acknowledgements remain separate.
pub(crate) type BackupHistory = std::collections::BTreeMap<SegmentId, (BTreeSet<SegmentId>, bool)>;
fn record_backup_state(o: &Owner, id: SegmentId, reset: bool) {
    let mut positions = if reset {
        BTreeSet::new()
    } else {
        o.held_positions.clone()
    };
    positions.insert(id);
    BACKUP_HISTORY.with(|h| {
        h.borrow_mut().insert(id, (positions, reset));
    });
}
thread_local! {
    static RESTORE_COVERAGE: std::cell::Cell<[u64; 5]> = const { std::cell::Cell::new([0; 5]) };
    static BACKUP_HISTORY: std::cell::RefCell<BackupHistory> = const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
    /// Uploads in this run that succeeded after their hold ended. The
    /// simulation runs on one thread, so a thread-local counts one run.
    static LATE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Objects collected in this run.
    static COLLECTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Releases sent after a lapse the lease task had not seen, that
    /// landed, in this run.
    static EARLY: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Wakes of a parked hold in this run.
    static WAKES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Deletes acknowledged as found carried out (after the fence).
    static READ_ACKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn count_restore(index: usize, n: u64) {
    RESTORE_COVERAGE.with(|c| {
        let mut counts = c.get();
        counts[index] += n;
        c.set(counts);
    });
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
    snapshot_retry_after: Instant,
}

type Node = LeaseNode<CutWrites, NodeClock>;

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
    if o.since_snapshot < SNAPSHOT_EVERY.max(o.snapshot_retry_at)
        || (o.snapshot_failures > 0 && Instant::now() < o.snapshot_retry_after)
        || o.held_positions.is_empty()
    {
        return;
    }
    let started = Instant::now();
    let at = o.head;
    let key = format!("{PREFIX}{}", snapshot_key(at));
    // Created and acknowledged (ADR 0012, condition 1), or no collection.
    let created = bucket
        .create(&key, encode_positions(&o.held_positions))
        .await;
    match created {
        // There (already, for a conflict): the count starts again, as
        // the server's does.
        Ok(_) | Err(wasmkeep_cluster::lease::BucketError::Conflict) => {
            o.since_snapshot = 0;
            o.snapshot_failures = 0;
            o.snapshot_retry_at = 0;
        }
        // Failed, or unanswered and perhaps landing later: the count
        // stays, and the next try comes once one more segment is there,
        // then eight times as many, up to the interval, and never before
        // the minimum time since this try started.
        Err(_) => {
            let wait = SNAPSHOT_RETRY_GROWTH
                .checked_pow(o.snapshot_failures)
                .map_or(SNAPSHOT_EVERY, |w| w.min(SNAPSHOT_EVERY));
            o.snapshot_failures += 1;
            o.snapshot_retry_at = o.since_snapshot + wait;
            o.snapshot_retry_after = started + SNAPSHOT_RETRY_MIN;
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
    if rules.collect == Collect::DropSegmentAtSnapshot
        && !gone.is_empty()
        && let Some(s) = listing.segments.iter().find(|s| s.id == at)
    {
        gone.push(segment_key(s));
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

/// The owner deletes the actor (ADR 0028), as the server's delete routine
/// does: a reset segment on its head, acknowledged by the same rule as a
/// write; then a snapshot of the empty database at it, and the erase of
/// everything before it (ADR 0012's rule). `false` if the upload failed:
/// the caller gives the actor up.
#[allow(clippy::too_many_arguments)]
async fn delete_actor(
    node: &Rc<Node>,
    bucket: &EpBucket,
    deletes: &EpBucket,
    rules: Rules,
    o: &mut Owner,
    acked: &Mutex<Vec<SegmentId>>,
    resets: &Mutex<Vec<Reset>>,
    rng: &mut Rng,
) -> bool {
    // A reset position in the replicated state represents its durable marker.
    // Snapshots carry that position; later resets replace it. Request indices
    // are immutable bucket order, so older requests cannot overwrite a marker.
    let latest = resets
        .lock()
        .unwrap()
        .iter()
        .enumerate()
        .filter(|(_, r)| o.held_positions.contains(&r.id))
        .max_by_key(|(_, r)| r.id.txid)
        .map(|(i, r)| (i, r.clone()));
    if rules.restores
        && let Some((index, marker)) = &latest
    {
        let carried = marker.restore_marker.map(|id| format!("{id} {id}"));
        let deleted = (marker.restore_marker != Some(marker.request_order))
            .then(|| format!("delete {} {}", marker.id.txid, marker.request_order));
        let request = if rng.chance(40) {
            marker.restore_marker
        } else {
            resets
                .lock()
                .unwrap()
                .iter()
                .filter_map(|r| r.restore_marker)
                .find(|id| *id < marker.request_order)
        };
        if let Some(request) = request {
            let outcome = wasmkeep_cluster::actor_restore::replay(
                carried.as_deref(),
                deleted.as_deref(),
                request,
                &request.to_string(),
            );
            if let Some(outcome) = outcome {
                let before = o.held_positions.clone();
                let ok = ack_by_read(node, bucket, o, acked, resets, *index).await;
                assert!(
                    before.is_subset(&o.held_positions),
                    "replayed or superseded request replaced state"
                );
                if ok {
                    count_restore(if outcome == "done" { 2 } else { 3 }, 1);
                }
                return ok;
            }
        }
    }
    let restoring = rules.restores && !rng.chance(20);
    let move_queued = restoring && rng.chance(35);
    let segment = Segment {
        id: SegmentId {
            generation: o.held.generation,
            txid: o.head.txid + 1,
        },
        parent_generation: o.head.generation,
    };
    let key = format!("{PREFIX}{}", segment_key(&segment));
    // The staged past state keeps the oldest event, replacing everything
    // written since. No history object is edited or erased for a restore.
    let saved: BTreeSet<_> = if restoring {
        o.held_positions.iter().take(1).copied().collect()
    } else {
        BTreeSet::new()
    };
    let at = resets.lock().unwrap().len();
    let request_order = at as u64 + 1;
    let restore_marker = if restoring {
        Some(request_order)
    } else {
        latest.as_ref().and_then(|(_, r)| r.restore_marker)
    };
    resets.lock().unwrap().push(Reset {
        request_order,
        restore_marker,
        id: segment.id,
        removed: o.held_positions.difference(&saved).copied().collect(),
        acked: false,
    });
    record_backup_state(o, segment.id, true);
    let body = if restoring {
        format!(
            "restore {}",
            String::from_utf8(encode_positions(&saved)).unwrap()
        )
        .into_bytes()
    } else {
        RESET.to_vec()
    };
    let uploaded = bucket.create(&key, body).await.is_ok();
    if rules.deletes == Deletes::EraseBeforeAck
        && let Ok(objects) = bucket.list(PREFIX).await
    {
        for (k, _) in objects {
            if k != key {
                count_collected(1);
                let _ = deletes.delete(&k).await;
            }
        }
    }
    if !uploaded {
        trace(&format!(
            "{} delete upload failed {:?}",
            node.id(),
            segment.id
        ));
        return false;
    }
    let may_ack = if rules.ack_on_lease_only {
        node.lease_valid()
    } else {
        node.may_act(&o.held)
    };
    if !rules.ack_within_grant || may_ack {
        trace(&format!("{} acked delete {:?}", node.id(), segment.id));
        acked.lock().unwrap().push(segment.id);
        resets.lock().unwrap()[at].acked = true;
    } else {
        LATE.with(|l| l.set(l.get() + 1));
    }
    o.head = segment.id;
    o.held_positions = saved;
    o.held_positions.insert(segment.id);
    o.since_snapshot = 0;
    o.snapshot_failures = 0;
    o.snapshot_retry_at = 0;
    // A move queued behind the reset releases only after its upload finishes.
    // It can invalidate the detached AfterRestore snapshot's collection grant.
    if move_queued && node.release(&o.held).await {
        count_restore(4, 1);
    }
    if !may_ack || rules.deletes != Deletes::Safe {
        return true;
    }
    if restoring {
        count_restore(0, 1);
    }
    let snapshot = format!("{PREFIX}{}", snapshot_key(segment.id));
    if bucket
        .create(&snapshot, encode_positions(&o.held_positions))
        .await
        .is_err()
        || !node.may_act(&o.held)
    {
        return true;
    }
    let Ok(objects) = bucket.list(PREFIX).await else {
        return true;
    };
    let listing = read_listing(objects.iter().map(|(k, _)| k.as_str()));
    let gone = collectable(&listing, o.held.generation, segment.id);
    if restoring {
        count_restore(1, gone.len() as u64);
    }
    count_collected(gone.len());
    for key in gone {
        if !node.may_act(&o.held) {
            return true;
        }
        let _ = deletes.delete(&format!("{PREFIX}{key}")).await;
    }
    true
}

/// A node that restored delete `resets[at]` acknowledges it as carried out
/// (ADR 0028, a request listed again): after the fence, a one-line upload
/// of this hold's that ends the history, acknowledged by the write rule.
/// `false` if the fence's upload failed: the caller gives the actor up.
async fn ack_by_read(
    node: &Node,
    bucket: &EpBucket,
    o: &mut Owner,
    acked: &Mutex<Vec<SegmentId>>,
    resets: &Mutex<Vec<Reset>>,
    at: usize,
) -> bool {
    let segment = Segment {
        id: SegmentId {
            generation: o.held.generation,
            txid: o.head.txid + 1,
        },
        parent_generation: o.head.generation,
    };
    let key = format!("{PREFIX}{}", segment_key(&segment));
    record_backup_state(o, segment.id, false);
    if bucket.create(&key, b"x".to_vec()).await.is_err() {
        return false;
    }
    o.head = segment.id;
    o.held_positions.insert(segment.id);
    o.since_snapshot += 1;
    if node.may_act(&o.held) {
        acked.lock().unwrap().push(segment.id);
        resets.lock().unwrap()[at].acked = true;
        READ_ACKS.with(|r| r.set(r.get() + 1));
    }
    true
}

/// One process's life on node leases: until its lease is lost (fenced, or
/// replaced by its own restart), then it exits.
#[allow(clippy::too_many_arguments)]
async fn run_node(
    node: &Rc<Node>,
    bucket: &EpBucket,
    deletes: &EpBucket,
    rules: Rules,
    rng: &mut Rng,
    acked: &Mutex<Vec<SegmentId>>,
    resets: &Mutex<Vec<Reset>>,
    problems: &Mutex<Vec<String>>,
) {
    let deleting = rules.deletes != Deletes::Off;
    let mut owner: Option<Owner> = None;
    // The generation of the hold given up last (once per hold, as the
    // server's dropped hold sends one release).
    let mut given: Option<u64> = None;
    loop {
        sleep(Duration::from_millis(100)).await;
        if node.lost() {
            trace(&format!("{} lost the node lease: exits", node.id()));
            return;
        }
        let Some(o) = owner.as_mut() else {
            // A parked hold is woken, a new generation (ADR 0024).
            let parked = deleting && node.is_parked(RECORD);
            let taken = if parked {
                node.wake(RECORD, 1).await
            } else {
                node.acquire(RECORD, 1).await
            };
            if parked && taken.is_ok() {
                WAKES.with(|w| w.set(w.get() + 1));
            }
            let held = match taken {
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
                        snapshot_retry_after: Instant::now(),
                    };
                    // As after the server's first event: a snapshot is due
                    // if enough segments were restored, at the restored
                    // head, whoever wrote it.
                    snapshot_and_collect(node, bucket, deletes, rules, &mut o).await;
                    // A delete found carried out by the restore (its
                    // request listed again) is acknowledged only once this
                    // hold's own upload ends the history (the server's
                    // fence). Acknowledging on the read alone is unsafe,
                    // but needs an interleaving this model does not reach
                    // within 20,000 seeds; `wasmkeep_cluster::replication`'s
                    // unit tests script it.
                    let restored_reset = resets.lock().unwrap().iter().position(|r| r.id == o.head);
                    if deleting
                        && let Some(at) = restored_reset
                        && rng.chance(50)
                        && !ack_by_read(node, bucket, &mut o, acked, resets, at).await
                    {
                        let _ = node.release(&o.held).await;
                        continue;
                    }
                    // Acquired (or woken) only to delete it, as an operator
                    // delete of an actor nobody holds, or of a parked room.
                    if deleting
                        && rng.chance(25)
                        && !delete_actor(node, bucket, deletes, rules, &mut o, acked, resets, rng)
                            .await
                    {
                        let _ = node.release(&o.held).await;
                        continue;
                    }
                    owner = Some(o);
                }
                Err(e) => problems
                    .lock()
                    .unwrap()
                    .push(format!("restore failed: {e}")),
            }
            continue;
        };

        // An event after a lapse the lease task has not seen yet: the
        // server's actor finds the hold no longer valid by the lease and
        // drops it, and the hold's release is sent at once, in the
        // background. The bucket applies it at once and answers it a while
        // later, within the client's timeout.
        if rules.lease_cuts
            && given != Some(o.held.generation)
            && node.still_held(&o.held)
            && !node.lease_valid()
        {
            given = Some(o.held.generation);
            trace(&format!(
                "{} gives gen {} up after a lapse",
                node.id(),
                o.held.generation
            ));
            bucket::late_reply_next("REPLACE", RECORD, 150 + rng.below(100));
            let (node, held) = (Rc::clone(node), o.held.clone());
            madsim::task::spawn_local(async move {
                if node.release_outcome(&held).await == Some(Released::Released) {
                    EARLY.with(|e| e.set(e.get() + 1));
                }
            });
            continue;
        }
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
        if rules.releases != Releases::Never && rng.chance(if deleting { 10 } else { 4 }) {
            trace(&format!("releases gen {}", o.held.generation));
            let held = o.held.clone();
            // Half the time it parks instead (a room hibernating with
            // sockets open, ADR 0024): its record keeps naming this node.
            if deleting && rng.chance(50) {
                let parked = node.park(&held).await;
                trace(&format!(
                    "{} parks gen {}: {parked}",
                    node.id(),
                    held.generation
                ));
            } else {
                let _ = node.release(&held).await;
            }
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

        // Now and then the owner deletes the actor instead of writing.
        if deleting && !released && rng.chance(6) {
            if !delete_actor(node, bucket, deletes, rules, o, acked, resets, rng).await {
                let held = o.held.clone();
                let _ = node.release(&held).await;
                owner = None;
            }
            continue;
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
        record_backup_state(o, segment.id, false);
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
    restore_coverage: [u64; 5],
    acked: Vec<SegmentId>,
    /// Releases after a lapse the lease task had not seen, that landed.
    early: u64,
    /// Deletes sent (ADR 0028).
    resets: Vec<Reset>,
    /// Wakes of a parked hold.
    wakes: u64,
    /// Deletes acknowledged as found carried out.
    read_acks: u64,
    /// Uploads that succeeded after the uploader's hold ended.
    late: u64,
    /// Objects garbage collection deleted (or sent deletes for).
    collected: u64,
    /// Fences nodes wrote over peers cut off from the bucket.
    fences: u64,
    problems: Vec<String>,
    exports: usize,
    export_retries: usize,
    store: Store,
}

fn simulate(seed: u64, rules: Rules, duration: Duration) -> Outcome {
    simulate_with_exports(seed, rules, duration, false)
}

fn simulate_with_exports(seed: u64, rules: Rules, duration: Duration, export: bool) -> Outcome {
    simulate_observed(seed, rules, duration, export, None)
}

type Observer = Box<dyn FnMut(&Store, &[SegmentId], &BackupHistory)>;
#[allow(dead_code)]
pub(crate) fn backup_simulation(seed: u64, observer: Observer) {
    simulate_observed(
        seed,
        SAFE_DELETING_COLLECTING,
        Duration::from_secs(90),
        false,
        Some(observer),
    );
}
fn simulate_observed(
    seed: u64,
    rules: Rules,
    duration: Duration,
    export: bool,
    mut observer: Option<Observer>,
) -> Outcome {
    RESTORE_COVERAGE.with(|c| c.set([0; 5]));
    BACKUP_HISTORY.with(|h| h.borrow_mut().clear());
    bucket::clear_delays();
    EARLY.with(|e| e.set(0));
    LATE.with(|l| l.set(0));
    COLLECTED.with(|c| c.set(0));
    WAKES.with(|w| w.set(0));
    READ_ACKS.with(|r| r.set(0));
    let _ = take_fences();
    let config = LeaseConfig::new(1_000, 500, 200, 300).unwrap();
    let max_skew_ms = 300;
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    let acked = Arc::new(Mutex::new(Vec::new()));
    let resets = Arc::new(Mutex::new(Vec::new()));
    let problems = Arc::new(Mutex::new(Vec::new()));
    let store = Store::default();
    let (acked_out, resets_out, problems_out, store_out) = (
        Arc::clone(&acked),
        Arc::clone(&resets),
        Arc::clone(&problems),
        Arc::clone(&store),
    );
    let export_counts = Arc::new(Mutex::new((0usize, 0usize)));
    let export_counts_out = Arc::clone(&export_counts);
    rt.block_on(async move {
        let export_store = Arc::clone(&store);
        let mut pending_export: Option<(Plan, Vec<SegmentId>, usize)> = None;
        let handle = Handle::current();
        let time = GlobalTime(Instant::now());
        let mut rng = Rng::new(seed ^ 0x5EED_0007);
        let bucket = handle
            .create_node()
            .name("bucket")
            .ip("10.0.0.1".parse().unwrap())
            .init(move || {
                bucket_node_with(
                    Arc::clone(&store),
                    Faults {
                        slow_one_in: 15,
                        late_reply_one_in: if rules.lease_cuts { 25 } else { 0 },
                    },
                )
            })
            .build();
        let mut nodes = Vec::new();
        let cuts: Vec<Arc<AtomicBool>> = (0..3).map(|_| Arc::default()).collect();
        for i in 1..=3u32 {
            let span = max_skew_ms * 2 + 1;
            let skew_ms =
                i64::try_from(rng.below(span)).unwrap() - i64::try_from(max_skew_ms).unwrap();
            let (acked, resets, problems) = (
                Arc::clone(&acked),
                Arc::clone(&resets),
                Arc::clone(&problems),
            );
            let cut = Arc::clone(&cuts[usize::try_from(i).unwrap() - 1]);
            // Not drawn from `rng`: existing seeds keep their fault schedules.
            let node_seed = seed ^ (u64::from(i) << 40) ^ 0x0E1E_A5E0;
            let node = handle
                .create_node()
                .name(format!("n{i}"))
                .ip(format!("10.0.1.{i}").parse().unwrap())
                .init(move || {
                    let (acked, resets, problems) = (
                        Arc::clone(&acked),
                        Arc::clone(&resets),
                        Arc::clone(&problems),
                    );
                    let cut = Arc::clone(&cut);
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
                            breaks: Breaks {
                                release_before_stop: rules.act_until_answered,
                                act_on_cell_only: rules.act_until_answered,
                                ..Breaks::default()
                            },
                        };
                        // With lease cuts, the workload's records (its
                        // acquisitions and releases) wait a lease attempt,
                        // as the server's foreground releases do.
                        let records = if rules.lease_cuts { attempt } else { WAIT };
                        let buckets = NodeBuckets {
                            lease: CutWrites {
                                inner: EpBucket::connect_with_timeout(attempt).await,
                                cut,
                            },
                            task_records: CutWrites::uncut(
                                EpBucket::connect_with_timeout(WAIT).await,
                            ),
                            peers: CutWrites::uncut(EpBucket::connect_with_timeout(attempt).await),
                            records: CutWrites::uncut(
                                EpBucket::connect_with_timeout(records).await,
                            ),
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
                        let node = Rc::new(node);
                        // With lease cuts, uploads wait a second: a slow or
                        // late-answered one is given up and may land later.
                        let uploads = if rules.lease_cuts { DELETE_WAIT } else { WAIT };
                        let bucket = EpBucket::connect_with_timeout(uploads).await;
                        let deletes = EpBucket::connect_with_timeout(DELETE_WAIT).await;
                        let mut rng = Rng::new(node_seed);
                        run_node(
                            &node, &bucket, &deletes, rules, &mut rng, &acked, &resets, &problems,
                        )
                        .await;
                    }
                })
                .build();
            nodes.push(node.id());
        }

        let net = NetSim::current();
        // Lease cuts on a schedule of their own: `rng`'s draws, and so the
        // other faults of a seed, are the same with or without them.
        let mut cut_rng = Rng::new(seed ^ 0x0C07_1EA5);
        let mut cut_until: [u64; 3] = [0; 3];
        let mut held: Vec<(usize, u64)> = Vec::new();
        let mut crashed: Vec<(usize, u64)> = Vec::new();
        let steps = u64::try_from(duration.as_millis()).unwrap() / 50;
        for step in 1..=steps {
            sleep(Duration::from_millis(50)).await;
            let at = step * 50;
            if let Some(observer) = &mut observer {
                BACKUP_HISTORY
                    .with(|h| observer(&export_store, &acked.lock().unwrap(), &h.borrow()));
            }
            // ADR 0033: an observer with no node lease or hold lists, then
            // fetches later while writers, kills and collection keep running.
            // The model checks positions; real LTX verification is covered
            // by the server's export fixtures and cluster tests.
            if export && step % 2 == 0 {
                let objects: Vec<(String, String)> = export_store
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, (body, _))| (k.clone(), String::from_utf8_lossy(body).into_owned()))
                    .collect();
                let mut attempts = 0;
                if let Some((plan, before, tried)) = pending_export.take() {
                    let complete = plan
                        .segments
                        .iter()
                        .all(|id| !segment_key_of(*id, &objects).is_empty());
                    if let Ok(found) = restored(&plan, &objects).and_then(|found| {
                        if complete {
                            Ok(found)
                        } else {
                            Err("collected segment".into())
                        }
                    }) {
                        if let Some(lost) = before.iter().find(|id| !found.contains(id)) {
                            problems
                                .lock()
                                .unwrap()
                                .push(format!("lease-free export lost acknowledged {lost:?}"));
                        }
                        export_counts.lock().unwrap().0 += 1;
                    } else {
                        attempts = tried + 1;
                        export_counts.lock().unwrap().1 += 1;
                    }
                }
                if attempts < 8 {
                    let listing = read_listing(objects.iter().map(|(key, _)| key.as_str()));
                    if let Ok(plan) = plan_restore(&listing) {
                        pending_export = Some((plan, acked.lock().unwrap().clone(), attempts));
                    }
                }
            }
            if rules.lease_cuts {
                // One node's lease writes refused for 0.3 to 1.6 s: past
                // 800 ms its lease runs out, and it is seldom fenced (ttl
                // plus grace is 1.5 s; a fenced process exits here, and
                // only a crash restarts its node).
                for (n, cut) in cuts.iter().enumerate() {
                    if cut.load(Ordering::SeqCst) && cut_until[n] <= at {
                        cut.store(false, Ordering::SeqCst);
                    }
                }
                if cut_rng.below(20) == 0 {
                    let n = usize::try_from(cut_rng.below(3)).unwrap();
                    if !cuts[n].load(Ordering::SeqCst) {
                        cuts[n].store(true, Ordering::SeqCst);
                        cut_until[n] = at + 300 + cut_rng.below(1_300);
                    }
                }
            }
            let i = usize::try_from(rng.below(3)).unwrap();
            // Faults at the node-lease simulation's rates, half the old
            // ones: a restarted process now spends ttl plus grace in its
            // start check before it may act, and a takeover waits for a
            // release, a restart or a fence (ADR 0020), so at the old rates
            // nodes would mostly be restarting or cut off.
            match rng.below(40) {
                // Hold the node's traffic to the bucket, deliver it late
                // (with lease cuts a fourth as often: the cuts take a
                // node's lease, and a node held off the bucket would mostly
                // miss the hand-overs they make).
                d @ 0..=3 if !rules.lease_cuts || d == 0 => {
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
    let (exports, export_retries) = *export_counts_out.lock().unwrap();
    Outcome {
        restore_coverage: RESTORE_COVERAGE.with(std::cell::Cell::get),
        acked: acked_out.lock().unwrap().clone(),
        early: EARLY.with(std::cell::Cell::get),
        resets: resets_out.lock().unwrap().clone(),
        wakes: WAKES.with(std::cell::Cell::get),
        read_acks: READ_ACKS.with(std::cell::Cell::get),
        late: LATE.with(std::cell::Cell::get),
        collected: COLLECTED.with(std::cell::Cell::get),
        fences: take_fences(),
        problems: problems_out.lock().unwrap().clone(),
        store: store_out,
        exports,
        export_retries,
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
    // The deletes on the final chain: those restored, then those an
    // earlier one on it removed (ADR 0028). What they removed may be
    // missing; nothing else acknowledged may.
    let mut on_chain: BTreeSet<SegmentId> = outcome
        .resets
        .iter()
        .filter(|r| restored.contains(&r.id))
        .map(|r| r.id)
        .collect();
    loop {
        let more: Vec<SegmentId> = outcome
            .resets
            .iter()
            .filter(|r| on_chain.contains(&r.id))
            .flat_map(|r| {
                outcome
                    .resets
                    .iter()
                    .filter(|d| r.removed.contains(&d.id) && !on_chain.contains(&d.id))
                    .map(|d| d.id)
            })
            .collect();
        if more.is_empty() {
            break;
        }
        on_chain.extend(more);
    }
    let gone: BTreeSet<SegmentId> = outcome
        .resets
        .iter()
        .filter(|r| on_chain.contains(&r.id))
        .flat_map(|r| r.removed.iter().copied())
        .collect();
    // Never resurrected: nothing an acknowledged delete removed is back.
    for r in outcome.resets.iter().filter(|r| r.acked) {
        if let Some(back) = r.removed.iter().find(|p| restored.contains(p)) {
            return Err(format!(
                "deleted data came back: {back:?}, removed by the acknowledged delete {:?} \
                 (restored to {:?})",
                r.id,
                plan.head()
            ));
        }
    }
    for id in &outcome.acked {
        if !restored.contains(id) && !gone.contains(id) {
            let what = if outcome.resets.iter().any(|r| r.id == *id) {
                "delete (what it removed may be back)"
            } else {
                "write"
            };
            return Err(format!(
                "acknowledged {what} {id:?} lost (restored to {:?})",
                plan.head()
            ));
        }
    }
    if let Some(newest) = outcome.acked.iter().map(|id| id.generation).max()
        && plan.head().generation < newest
    {
        return Err(format!(
            "restored to {:?}, older than generation {newest}'s acknowledged writes",
            plan.head()
        ));
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

/// ADR 0020, section 1 (amended 2026-11-14): lease writes cut, other
/// writes landing. A hold an event gives up after a lapse the lease task
/// has not seen is released at once, and the next owner restores without
/// a fence, while the old owner's uploads, slow or answered late, may
/// still land: no acknowledged write is lost, and no late segment of an
/// older owner is restored over a newer owner's history.
#[test]
fn acknowledged_writes_survive_lease_write_cuts() {
    let (mut acked, mut early, mut late, mut generations) = (0, 0, 0, 0);
    for seed in 0..seeds() {
        let outcome = simulate(seed, SAFE_CUT, Duration::from_secs(30));
        match check(&outcome) {
            Ok(n) => acked += u64::try_from(n).unwrap(),
            Err(v) => panic!("seed {seed}: {v}"),
        }
        early += outcome.early;
        late += outcome.late;
        generations += outcome
            .acked
            .iter()
            .map(|id| id.generation)
            .max()
            .unwrap_or(0);
    }
    eprintln!(
        "replication with lease cuts: {} seeds x 30 s: {acked} acknowledged writes, all \
         restored; {early} releases after a lapse landed; {late} uploads landed after their \
         hold ended; {generations} generations",
        seeds()
    );
    assert!(
        acked > seeds() * 10,
        "too few writes to mean anything ({acked})"
    );
    assert!(
        early >= (seeds() / 2).max(1),
        "releases after a lapse rarely landed ({early})"
    );
    assert!(
        late > seeds() / 10,
        "uploads rarely landed after their hold ended ({late})"
    );
}

/// With lease cuts, a hold that keeps acting on its cell alone until its
/// release is answered acknowledges writes the next owner, which took the
/// record once the release landed, never restored: caught on a twentieth
/// of the seeds at least (about a tenth: 202 of 2,000, 30 of 200).
#[test]
fn acting_until_a_release_after_a_lapse_is_answered_is_caught() {
    let rules = Rules {
        act_until_answered: true,
        ..SAFE_CUT
    };
    let caught: Vec<u64> = (0..seeds())
        .filter(|&s| check(&simulate(s, rules, Duration::from_secs(30))).is_err())
        .collect();
    eprintln!(
        "replication: acting until a release after a lapse is answered caught on {} of {} \
         seeds, first {:?}",
        caught.len(),
        seeds(),
        caught.first()
    );
    let floor = (seeds() / 20).max(1);
    assert!(
        u64::try_from(caught.len()).unwrap() >= floor,
        "caught on {} seeds, want {floor}",
        caught.len()
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

/// Deletes (ADR 0028) among takeovers, restores, releases and parked holds
/// woken by their node: acknowledged writes survive unless a delete on the
/// final chain removed them, and nothing an acknowledged delete removed
/// comes back.
fn deletes_hold(rules: Rules, what: &str) {
    let (mut acked, mut deletes, mut wakes, mut generations, mut read_acks) = (0, 0, 0, 0, 0);
    for seed in 0..seeds() {
        let outcome = simulate(seed, rules, Duration::from_secs(30));
        match check(&outcome) {
            Ok(n) => acked += u64::try_from(n).unwrap(),
            Err(v) => panic!("seed {seed}: {v}"),
        }
        deletes += u64::try_from(outcome.resets.iter().filter(|r| r.acked).count()).unwrap();
        wakes += outcome.wakes;
        read_acks += outcome.read_acks;
        generations += outcome
            .acked
            .iter()
            .map(|id| id.generation)
            .max()
            .unwrap_or(0);
    }
    assert!(acked > seeds() * 10, "too few writes ({acked})");
    assert!(
        deletes > seeds(),
        "too few acknowledged deletes ({deletes})"
    );
    assert!(wakes > seeds() / 4, "parked holds rarely woken ({wakes})");
    assert!(
        read_acks > seeds() / 4,
        "deletes rarely acknowledged on a restore ({read_acks})"
    );
    assert!(
        generations > seeds() * 3,
        "ownership rarely changed ({generations})"
    );
    eprintln!(
        "replication {what}: {} seeds x 30 s: {acked} acknowledged writes and {deletes} \
         acknowledged deletes ({read_acks} found carried out by a restore), none lost, \
         nothing deleted back; {wakes} wakes",
        seeds()
    );
}

#[test]
fn deletes_never_bring_data_back() {
    deletes_hold(SAFE_DELETING, "with deletes");
}

#[test]
fn deletes_with_collection_never_bring_data_back() {
    deletes_hold(SAFE_DELETING_COLLECTING, "with deletes and collection");
}

#[test]
fn a_delete_acked_after_its_hold_ended_can_bring_data_back() {
    // A delete acknowledged because the node lease is valid, though a lapse
    // ended the hold: another node may have taken the actor without it.
    let rules = Rules {
        ack_on_lease_only: true,
        ..SAFE_DELETING
    };
    let seed = (0..seeds())
        .find(|&s| {
            check(&simulate(s, rules, Duration::from_secs(30)))
                .is_err_and(|e| e.contains("deleted data came back"))
        })
        .unwrap_or_else(|| panic!("no resurrection within the seed budget"));
    let again = check(&simulate(seed, rules, Duration::from_secs(30)));
    assert!(again.is_err(), "seed {seed} did not reproduce");
    eprintln!("replication: a delete acked after its hold ended came back at seed {seed}");
}

#[test]
fn erasing_before_the_delete_is_acked_loses_writes() {
    let seed = caught(Rules {
        deletes: Deletes::EraseBeforeAck,
        ..SAFE_DELETING
    });
    eprintln!("replication: erasing before the delete is acked caught at seed {seed}");
}

/// A read-only observer has no ownership machinery. Its delayed reads either
/// replan on collection or contain all writes acknowledged before its listing.
#[test]
fn lease_free_exports_during_writes_kills_and_collection_keep_acknowledged_positions() {
    let (mut exports, mut retries) = (0, 0);
    for seed in 0..seeds() {
        let outcome = simulate_with_exports(seed, SAFE_COLLECTING, Duration::from_secs(30), true);
        if let Err(error) = check(&outcome) {
            panic!("seed {seed}: {error}");
        }
        exports += outcome.exports;
        retries += outcome.export_retries;
    }
    assert!(
        exports > seeds() as usize * 10,
        "too few exports: {exports}"
    );
    assert!(retries > 0, "collection never invalidated an export");
    eprintln!(
        "lease-free exports: {exports} verified, {retries} collection retries across {} seeds",
        seeds()
    );
}

#[test]
fn actor_restore_is_a_new_write_and_never_resurrects_replaced_events() {
    deletes_hold(
        Rules {
            restores: true,
            ..SAFE_DELETING_COLLECTING
        },
        "with historical replacements",
    );
}

fn restore_coverage() -> [u64; 5] {
    let mut counts = [0; 5];
    for seed in 0..seeds() {
        let out = simulate(
            seed,
            Rules {
                restores: true,
                ..SAFE_DELETING_COLLECTING
            },
            Duration::from_secs(30),
        );
        check(&out).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        for (total, n) in counts.iter_mut().zip(out.restore_coverage) {
            *total += n;
        }
    }
    counts
}

#[test]
fn actor_restore_after_restore_snapshot_and_collection_are_exercised() {
    let c = restore_coverage();
    assert!(
        c[0] > seeds() / 2 && c[1] > seeds() / 2,
        "no AfterRestore snapshot/collection: {c:?}"
    );
}

#[test]
fn actor_restore_marker_replay_and_supersession_are_exercised() {
    let c = restore_coverage();
    assert!(
        c[2] > seeds() / 4 && c[3] > seeds() / 4,
        "no marker replay/supersession: {c:?}"
    );
}

#[test]
fn actor_restore_racing_a_move_is_exercised() {
    let c = restore_coverage();
    assert!(c[4] > seeds() / 4, "no restore/move races: {c:?}");
}
