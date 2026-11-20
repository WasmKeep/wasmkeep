//! ADR 0034's split protocol model, shared by the replication and alarm tests.
//!
//! This is deliberately not a SQLite/server simulation. A database is a sequence
//! of uniquely named event/flush boundaries, with an alarm and reset tombstones.
//! Equality of those states checks provenance, not WAL checksums. Production
//! `plan_restore`, `collectable`, and alarm R1/R2/R3 helpers run on model objects.
//! Node leases are abstracted to validity intervals and unique hold generations:
//! the existing node-lease suites check how those intervals are obtained. A cut
//! invalidates all of a node's holds before release; bucket jobs outlive them.
//!
//! One tick is 10 ms. Entry PUT, segment PUT, reply/acknowledgement and completion
//! are distinct scheduled steps; capture consumes pending boundaries separately
//! from local commit. Seeded delays and event lengths reorder actors and jobs.
//! Directed workloads make rare races reachable, as in madsim_alarm_index's
//! cut_at_first_upload; the same workloads run with every rule switch, including
//! strict. Oracles never consult the switch. No mailbox (except the alarm
//! delivery model at the end of this file, ADR 0009's amendment of 2026-11-19),
//! bytes, sidecar file I/O, checksum computation, effects, or real lease network
//! is claimed here. Byte
//! caps and the node upload budget are out of scope. Cache contents/positions
//! model local_is_at, not filesystem atomicity. SidecarBeforeAck is explicitly
//! structural only (SidecarAhead), not evidence of a restore failure. This is
//! not the strict golden trace. Stalled-bucket time-span bounds apply to busy
//! actors; sparse demand is counted separately (strict event-count checks still run).

#![allow(dead_code)] // shared by two integration binaries, with different tests

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::sleep;
use wasmkeep_cluster::alarm_index::{self, Entry};
use wasmkeep_cluster::replication::{
    EMPTY, Listing, Segment, SegmentId, collectable, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_sim::rng::Rng;

const ACTORS: usize = 3;
const MAX_UNSENT: u64 = 6;
const EVENT_MAX: u64 = 3;
// Includes entry + segment + reply + completion and the next capture tick.
const ROUND_TRIP: u64 = 10;
const STOP_BUDGET: u64 = 3 * ROUND_TRIP;

/// Exactly one protocol rule is changed at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Switch {
    Safe,
    FlushCaptured,
    TwoInFlight,
    ReleaseWithoutBarrier,
    SnapshotInFlight,
    ResetCaptured,
    ResetInFlight,
    RetireBeforeAck,
    RetireOnFailure,
    SegmentBeforeEntry,
    FenceCaptured,
    CompletionOtherHold,
    SidecarBeforeAck,
    HeadBackwards,
    WithoutStateLocal,
    NoEventStartRule,
    AckLate,
    IntermediateEntry,
    NoR3,
    DeleteWithoutAck,
    MaxUnsentPlusOne,
    MaxUnsentPlusHalf,
    DoubleMaxUnsent,
    ModeWithoutBarrier,
    CaptureAfterFailure,
    /// A stale-hint LIST resumed against an acknowledged entry read before
    /// the same hold wrote on, instead of returning to the barrier and
    /// reading it again (ADR 0034, section 9).
    ClearOldAcknowledged,
    /// A periodic snapshot taken in a gap without flushing first: nothing
    /// in flight, but commits not yet captured are in it, labelled at the
    /// acknowledged head (ADR 0034, section 7).
    SnapshotUncaptured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Violation {
    DurableLost,
    MissingLink,
    NotOwnerPrefix,
    HeadBackwards,
    LossBound,
    AlarmMissing,
    DeletedReturned,
    CleanLoss,
    SidecarAhead,
    StrictAhead,
    EntryFailureUploaded,
    /// The delivery model ([`simulate_deliveries`]): a delivery an event's
    /// end registered while one for the same time was in flight, run after
    /// that round (a second delivery of it, which finds its hint stale).
    LocalDuplicate,
    /// A committed alarm an event synced, with no registration or delivery
    /// on the timer once every delivery's result came back.
    AlarmUnarmed,
    /// An index entry at a time named by no state, left after a delivery
    /// from the index (a sweep) of that time.
    OrphanLeft,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Db {
    // Unique commit identities stand for database rows, not segment positions.
    rows: Vec<u64>,
    erased: BTreeSet<u64>,
    resets: BTreeSet<u64>,
    alarm: Option<Entry>,
    boundary: u64,
}

#[derive(Debug, Clone)]
struct Boundary {
    db: Db,
    at: u64,
}

#[derive(Clone)]
struct Object {
    segment: Segment,
    before: Db,
    after: Db,
    // One per event or explicit flush point; intermediate alarm entries are
    // never published, only the entry at the segment's last boundary.
    boundaries: Vec<u64>,
}

#[derive(Clone, Copy)]
enum Stage {
    Entry,
    Put,
    Reply,
    Completion,
    Done,
}

#[derive(Clone)]
struct Upload {
    actor: usize,
    hold: u64,
    object: Object,
    retire: Option<Entry>,
    publish: Option<Entry>,
    entry_fail: bool,
    reset: bool,
    due: u64,
    stage: Stage,
    delay: u64,
    captured_at: u64,
    returned_at: Option<u64>,
    fail: bool,
    landed: bool,
    acknowledged: bool,
}

struct Hold {
    generation: u64,
    batching: bool,
    node: usize,
    local: Db,
    head: SegmentId,
    acknowledged: Db,
    captured: SegmentId,
    captured_db: Db,
    sidecar: SegmentId,
    pending: Vec<Boundary>,
    flights: Vec<usize>,
    flushes: Vec<Db>,
    commits: Vec<(u64, u64)>,
    demand: Vec<u64>,
    // The greatest observed acknowledged position for this hold, independent
    // of the mutable head under test.
    high_water: SegmentId,
}

#[derive(Default)]
struct Actor {
    hold: Option<Hold>,
    generation: u64,
    objects: BTreeMap<String, Object>,
    snapshots: BTreeMap<SegmentId, Db>,
    entries: BTreeSet<Entry>,
    promised: BTreeSet<u64>,
    durable_waiting: BTreeSet<u64>,
    deleted: BTreeSet<u64>,
    reset_acks: BTreeSet<u64>,
    // Oracle: all local event/flush states, indexed by boundary, and segment
    // endpoints before any snapshot could mislabel them.
    boundaries: BTreeMap<u64, Db>,
    positions: BTreeMap<SegmentId, Db>,
    next_delay: u64,
    next_fail: bool,
    next_entry_fail: bool,
    cache: BTreeMap<usize, (SegmentId, Db)>,
    acknowledged_positions: BTreeSet<SegmentId>,
    snapshot_paths: BTreeMap<SegmentId, BTreeSet<SegmentId>>,
    position_paths: BTreeMap<SegmentId, BTreeSet<SegmentId>>,
}

#[derive(Debug, Default)]
pub(crate) struct Counts {
    pub(crate) commits: u64,
    pub(crate) durable: u64,
    pub(crate) flushes: u64,
    pub(crate) mid_event_flushes: u64,
    pub(crate) captures: u64,
    pub(crate) batched: u64,
    pub(crate) late_batched: u64,
    pub(crate) crash_pending: u64,
    pub(crate) cuts: u64,
    pub(crate) collected: u64,
    pub(crate) resets: u64,
    pub(crate) snapshots: u64,
    pub(crate) healthy_handoffs: [u64; 5],
    pub(crate) unhealthy_handoffs: u64,
    pub(crate) blocked_events: u64,
    pub(crate) retired: u64,
    pub(crate) fenced: u64,
    pub(crate) completions: u64,
    pub(crate) batch_alarm_changes: u64,
    pub(crate) late_successor_unacked: u64,
    pub(crate) handoff_flight: u64,
    pub(crate) handoff_both: u64,
    pub(crate) busy: u64,
    pub(crate) cleared: u64,
    pub(crate) cleanup_live: u64,
    pub(crate) r3_refused: u64,
    pub(crate) healthy_loss: u64,
    pub(crate) sparse_loss: u64,
    pub(crate) unhealthy_loss: u64,
    pub(crate) stalled_at_bound: u64,
    pub(crate) entry_failed: u64,
    pub(crate) failed_hold_dropped: u64,
    pub(crate) wall_internal: u64,
    pub(crate) wall_completed: u64,
    pub(crate) mode_switched: u64,
    pub(crate) mode_inflight: u64,
    pub(crate) interrupted_waits: [u64; 8],
    pub(crate) wait_crashes: u64,
    pub(crate) wait_cuts: u64,
    pub(crate) random_ops: u64,
    pub(crate) reset_late: u64,
    pub(crate) reset_collision: u64,
    pub(crate) cache_hits: u64,
}

impl Counts {
    fn add(&mut self, other: Self) {
        self.commits += other.commits;
        self.durable += other.durable;
        self.flushes += other.flushes;
        self.mid_event_flushes += other.mid_event_flushes;
        self.captures += other.captures;
        self.batched += other.batched;
        self.late_batched += other.late_batched;
        self.crash_pending += other.crash_pending;
        self.cuts += other.cuts;
        self.collected += other.collected;
        self.resets += other.resets;
        self.snapshots += other.snapshots;
        for (a, b) in self.healthy_handoffs.iter_mut().zip(other.healthy_handoffs) {
            *a += b;
        }
        self.unhealthy_handoffs += other.unhealthy_handoffs;
        self.blocked_events += other.blocked_events;
        self.retired += other.retired;
        self.fenced += other.fenced;
        self.completions += other.completions;
        self.batch_alarm_changes += other.batch_alarm_changes;
        self.late_successor_unacked += other.late_successor_unacked;
        self.handoff_flight += other.handoff_flight;
        self.handoff_both += other.handoff_both;
        self.busy += other.busy;
        self.cleared += other.cleared;
        self.cleanup_live += other.cleanup_live;
        self.r3_refused += other.r3_refused;
        self.healthy_loss += other.healthy_loss;
        self.sparse_loss += other.sparse_loss;
        self.unhealthy_loss += other.unhealthy_loss;
        self.stalled_at_bound += other.stalled_at_bound;
        self.entry_failed += other.entry_failed;
        self.failed_hold_dropped += other.failed_hold_dropped;
        self.wall_internal += other.wall_internal;
        self.wall_completed += other.wall_completed;
        self.mode_switched += other.mode_switched;
        self.mode_inflight += other.mode_inflight;
        for (a, b) in self
            .interrupted_waits
            .iter_mut()
            .zip(other.interrupted_waits)
        {
            *a += b;
        }
        self.wait_crashes += other.wait_crashes;
        self.wait_cuts += other.wait_cuts;
        self.random_ops += other.random_ops;
        self.reset_late += other.reset_late;
        self.reset_collision += other.reset_collision;
        self.cache_hits += other.cache_hits;
    }
}

#[derive(Debug, Default)]
pub(crate) struct Outcome {
    pub(crate) counts: Counts,
    pub(crate) violations: BTreeSet<Violation>,
    pub(crate) per: Vec<BTreeSet<Violation>>,
}

// Each actor can wait independently; the random workload polls all three
// once per tick. A slow flush never pauses another actor's operation stream.
enum Operation {
    Event(u64, Option<u64>),
    Flush(u64, Db),
    Handover(usize, u64, Vec<u64>),
    ResetBarrier,
    ResetUpload(u64, usize, u64),
    CleanupAdmission(bool),
    CleanupFlush(bool, bool),
}
struct Task {
    generation: u64,
    operation: Operation,
}

struct World {
    rule: Switch,
    batching: bool,
    now: u64,
    serial: u64,
    rng: Rng,
    actors: Vec<Actor>,
    uploads: Vec<Upload>,
    entry_puts: Vec<(u64, usize, Entry)>,
    retires: Vec<(u64, Upload)>,
    random_faults: bool,
    fault_rng: Rng,
    // Directed interruptions also execute inside advance(), including waits.
    interrupt: Option<(u64, usize, bool)>,
    healthy_delay: u64,
    waiting: Option<(usize, usize)>,
    // The delivery model clears one stale hint at a time; `None` (the
    // other workloads) clears every time, as before.
    clear_only: Option<u64>,

    // Intervals include deliberately slow/failed object requests. Lease-only
    // cuts are independent: those can happen while the bucket accepts PUTs.
    unhealthy: Vec<(u64, u64)>,
    outcome: Outcome,
}

impl World {
    fn new(seed: u64, rule: Switch, batching: bool) -> Self {
        let mut w = Self {
            rule,
            batching,
            now: 0,
            serial: 0,
            rng: Rng::new(seed),
            actors: (0..ACTORS).map(|_| Actor::default()).collect(),
            uploads: Vec::new(),
            entry_puts: Vec::new(),
            retires: Vec::new(),
            random_faults: false,
            fault_rng: Rng::new(seed ^ 0x0C07_1EA5),
            interrupt: None,
            healthy_delay: 0,
            waiting: None,
            clear_only: None,

            unhealthy: Vec::new(),
            outcome: Outcome::default(),
        };
        for a in 0..ACTORS {
            w.acquire(a);
        }
        w
    }

    fn fault(&mut self, actor: usize, fail: bool) {
        let delay = self.rng.below(41);
        self.actors[actor].next_delay = delay;
        self.actors[actor].next_fail = fail;
    }

    fn stall(&mut self, actor: usize, fail: bool) {
        self.actors[actor].next_delay = 40;
        self.actors[actor].next_fail = fail;
    }

    /// Inclusive windows: being healthy at the end alone proves nothing.
    fn healthy(&self, from: u64, to: u64) -> bool {
        self.unhealthy.iter().all(|&(a, b)| b < from || a > to)
    }

    fn listing(&self, actor: usize) -> Listing {
        let a = &self.actors[actor];
        let mut keys: Vec<String> = a.objects.keys().cloned().collect();
        keys.extend(a.snapshots.keys().copied().map(snapshot_key));
        Listing::from_keys(keys.iter().map(String::as_str))
    }

    /// Replays the real planner's selected chain. Comparing exact boundary
    /// states stands for provenance; it does not pretend to test WAL checksums.
    fn restore(&self, actor: usize) -> Result<(SegmentId, Db), Violation> {
        let a = &self.actors[actor];
        let plan = plan_restore(&self.listing(actor)).map_err(|_| Violation::MissingLink)?;
        let mut db = match plan.snapshot {
            Some(id) => {
                let snapshot = &a.snapshots[&id];
                if a.positions.get(&id) != Some(snapshot) {
                    return Err(Violation::NotOwnerPrefix);
                }
                snapshot.clone()
            }
            None => Db::default(),
        };
        for id in &plan.segments {
            let obj = a.objects.values().find(|o| o.segment.id == *id).unwrap();
            if db != obj.before || a.boundaries.get(&obj.after.boundary) != Some(&obj.after) {
                return Err(Violation::NotOwnerPrefix);
            }
            db = obj.after.clone();
        }
        Ok((plan.head(), db))
    }

    fn restored_positions(&self, actor: usize) -> Result<BTreeSet<SegmentId>, Violation> {
        let a = &self.actors[actor];
        let plan = plan_restore(&self.listing(actor)).map_err(|_| Violation::MissingLink)?;
        let mut positions = plan
            .snapshot
            .and_then(|id| a.snapshot_paths.get(&id).cloned())
            .unwrap_or_default();
        positions.extend(plan.segments);
        Ok(positions)
    }

    fn observe_restore(&mut self, actor: usize) -> Result<(SegmentId, Db), Violation> {
        let result = self.restore(actor);
        match &result {
            Err(e) => {
                self.outcome.violations.insert(*e);
                if !self.actors[actor].promised.is_empty() {
                    self.outcome.violations.insert(Violation::DurableLost);
                }
            }
            Ok(_) => {
                let positions = self.restored_positions(actor).unwrap();
                if !self.actors[actor]
                    .acknowledged_positions
                    .is_subset(&positions)
                {
                    self.outcome.violations.insert(Violation::HeadBackwards);
                }
            }
        }
        result
    }

    fn acquire(&mut self, actor: usize) {
        self.acquire_at(actor, None);
    }

    fn acquire_at(&mut self, actor: usize, node: Option<usize>) {
        if self.actors[actor].hold.is_some() {
            return;
        }
        let Ok((head, mut db)) = self.observe_restore(actor) else {
            return;
        };
        let node = node.unwrap_or_else(|| self.rng.below(3) as usize);
        if let Some((sidecar, local)) = self.actors[actor].cache.get(&node)
            && *sidecar == head
        {
            self.outcome.counts.cache_hits += 1;
            if local != &db {
                self.outcome.violations.insert(Violation::NotOwnerPrefix);
            }
            db = local.clone(); // local_is_at trusts the matching sidecar.
        }
        let a = &mut self.actors[actor];
        assert!(a.hold.is_none());
        a.generation += 1;
        a.hold = Some(Hold {
            generation: a.generation,
            batching: self.batching,
            node,
            local: db.clone(),
            head,
            acknowledged: db.clone(),
            captured: head,
            captured_db: db,
            sidecar: head,
            pending: Vec::new(),
            flights: Vec::new(),
            flushes: Vec::new(),
            commits: Vec::new(),
            demand: Vec::new(),
            high_water: head,
        });
    }

    fn can_start(&mut self, actor: usize) -> bool {
        let Some(h) = &mut self.actors[actor].hold else {
            return false;
        };
        h.demand.push(self.now);
        let ready = if h.batching {
            self.rule == Switch::NoEventStartRule
                || h.pending.first().is_none_or(|p| {
                    self.now - p.at
                        < match self.rule {
                            Switch::MaxUnsentPlusOne => MAX_UNSENT + 1,
                            Switch::MaxUnsentPlusHalf => MAX_UNSENT + MAX_UNSENT / 2,
                            Switch::DoubleMaxUnsent => 2 * MAX_UNSENT,
                            _ => MAX_UNSENT,
                        }
                })
        } else {
            h.pending.is_empty() && h.flights.is_empty()
        };
        if !ready {
            self.outcome.counts.blocked_events += 1;
        }
        ready
    }

    /// A completed local event (or a mid-event flush boundary). Caller tested
    /// event admission before running for up to EVENT_MAX ticks.
    fn commit(&mut self, actor: usize, alarm: Option<u64>) {
        let a = &mut self.actors[actor];
        let Some(h) = a.hold.as_mut() else {
            return;
        };
        if !h.batching && (!h.pending.is_empty() || !h.flights.is_empty()) {
            self.outcome.violations.insert(Violation::StrictAhead);
        }
        self.serial += 1;
        h.local.rows.push(self.serial);
        h.local.boundary = self.serial;
        if alarm_index::needs_publish(h.local.alarm.as_ref(), alarm) {
            h.local.alarm = alarm
                .map(|at| Entry::owned(at, h.generation, format!("{:x}", self.serial)).unwrap());
        }
        h.pending.push(Boundary {
            db: h.local.clone(),
            at: self.now,
        });
        h.commits.push((self.serial, self.now));
        a.boundaries.insert(self.serial, h.local.clone());
        self.outcome.counts.commits += 1;
    }

    async fn event(&mut self, actor: usize, alarm: Option<u64>) {
        if self.can_start(actor) {
            let duration = 1 + self.rng.below(EVENT_MAX);
            self.advance(duration).await;
            self.commit(actor, alarm);
            self.check();
        } else {
            self.advance(1).await;
        }
    }

    fn capture(&mut self, actor: usize) {
        let a = &mut self.actors[actor];
        let Some(h) = a.hold.as_mut() else { return };
        let parallel = self.rule == Switch::TwoInFlight;
        if h.pending.is_empty() || (!h.flights.is_empty() && !parallel) {
            return;
        }
        let boundaries = std::mem::take(&mut h.pending);
        let after = boundaries.last().unwrap().db.clone();
        let parent = h.captured;
        let object = Object {
            segment: Segment {
                id: SegmentId {
                    generation: h.generation,
                    txid: parent.txid + 1,
                },
                parent_generation: parent.generation,
            },
            before: h.captured_db.clone(),
            after: after.clone(),
            boundaries: boundaries.iter().map(|b| b.db.boundary).collect(),
        };
        let retire = (h.acknowledged.alarm != after.alarm)
            .then(|| h.acknowledged.alarm.clone())
            .flatten();
        if boundaries.len() > 1 && boundaries.iter().any(|b| b.db.alarm != after.alarm) {
            self.outcome.counts.batch_alarm_changes += 1;
        }
        let publish = if self.rule == Switch::IntermediateEntry {
            boundaries.first().unwrap().db.alarm.clone()
        } else {
            after.alarm.clone()
        };
        let needs_entry = publish.is_some() && publish != h.captured_db.alarm;
        if needs_entry && self.rule == Switch::SegmentBeforeEntry {
            // Start the entry request, but wrongly launch the segment before
            // it returns. The entry still lands later: this is an ordering
            // mutation, not an omitted publication.
            self.entry_puts.push((
                self.now + ROUND_TRIP + 2,
                actor,
                after.alarm.clone().unwrap(),
            ));
        }
        h.captured = object.segment.id;
        h.captured_db = after.clone();
        let index = self.uploads.len();
        h.flights.push(index);
        a.positions.insert(object.segment.id, after);
        let mut path = a.position_paths.get(&parent).cloned().unwrap_or_default();
        path.insert(object.segment.id);
        a.position_paths.insert(object.segment.id, path);
        let fault_delay = std::mem::take(&mut a.next_delay);
        let delay = fault_delay + self.healthy_delay;
        let fail = std::mem::take(&mut a.next_fail);
        let entry_fail = std::mem::take(&mut a.next_entry_fail);
        if fault_delay > 0 || fail || entry_fail {
            self.unhealthy
                .push((self.now, self.now + delay + ROUND_TRIP));
        }
        self.uploads.push(Upload {
            actor,
            hold: h.generation,
            object,
            retire: retire.clone(),
            publish,
            entry_fail,
            reset: false,
            due: self.now + 1 + self.rng.below(2),
            stage: if needs_entry && self.rule != Switch::SegmentBeforeEntry {
                Stage::Entry
            } else {
                Stage::Put
            },
            delay,
            captured_at: self.now,
            returned_at: None,
            fail,
            landed: false,
            acknowledged: false,
        });
        if self.rule == Switch::SidecarBeforeAck {
            h.sidecar = h.captured;
        }
        if self.rule == Switch::RetireBeforeAck
            && let Some(entry) = retire
        {
            a.entries.remove(&entry);
        }
        self.outcome.counts.captures += 1;
        if boundaries.len() > 1 {
            self.outcome.counts.batched += 1;
        }
    }

    fn promise(&mut self, actor: usize, db: &Db) {
        let a = &mut self.actors[actor];
        a.promised.extend(&db.rows);
        for row in &db.rows {
            if a.durable_waiting.remove(row) {
                self.outcome.counts.durable += 1;
            }
        }
        self.outcome.counts.flushes += 1;
    }

    fn request_flush(&mut self, actor: usize) {
        // A restore can observe a late, unacknowledged PUT while a later
        // departed hold still has a conflicting PUT outstanding. Before a
        // read-only durable promise, anchor the observed prefix in THIS hold.
        // This is the read-ack fence used by madsim_replication, not a promise
        // based merely on what an earlier LIST happened to return.
        if let Some(h) = &self.actors[actor].hold
            && h.head != EMPTY
            && h.head.generation != h.generation
            && h.pending.is_empty()
            && h.flights.is_empty()
        {
            let alarm = h.local.alarm.as_ref().map(|e| e.at);
            self.commit(actor, alarm);
        }
        let Some(h) = self.actors[actor].hold.as_mut() else {
            return;
        };
        let db = h.local.clone();
        if h.acknowledged == db || (self.rule == Switch::FlushCaptured && h.captured_db == db) {
            self.promise(actor, &db);
        } else {
            h.flushes.push(db);
        }
    }

    async fn flush(&mut self, actor: usize) {
        self.flush_until(actor, 100).await;
    }

    async fn flush_until(&mut self, actor: usize, budget: u64) -> bool {
        let previous = self.waiting;
        if previous.is_none() {
            self.waiting = Some((0, actor));
        }
        let result = self.flush_until_wait(actor, budget).await;
        self.waiting = previous;
        result
    }

    async fn flush_until_wait(&mut self, actor: usize, budget: u64) -> bool {
        let Some(h) = &self.actors[actor].hold else {
            return false;
        };
        let generation = h.generation;
        let wanted = h.local.clone();
        self.request_flush(actor);
        for _ in 0..budget {
            let Some(h) = &self.actors[actor].hold else {
                return false;
            };
            if h.generation != generation {
                return false;
            }
            if h.flights.is_empty() && h.pending.is_empty() {
                self.check();
                return true;
            }
            self.advance(1).await;
        }
        let Some(h) = self.actors[actor].hold.as_mut() else {
            return false;
        };
        if h.flights.is_empty() && h.pending.is_empty() {
            return true;
        }
        h.flushes.retain(|db| db != &wanted);
        self.outcome.counts.wall_internal += 1;
        self.check();
        false // internal: still uploading, not a replica failure
    }

    async fn switch_to_strict(&mut self, actor: usize) {
        self.outcome.counts.mode_inflight += u64::from(
            self.actors[actor]
                .hold
                .as_ref()
                .is_some_and(|h| !h.flights.is_empty()),
        );
        if (self.rule == Switch::ModeWithoutBarrier || self.flush_until(actor, STOP_BUDGET).await)
            && let Some(h) = self.actors[actor].hold.as_mut()
        {
            h.batching = false;
            self.outcome.counts.mode_switched += 1;
        }
    }

    fn valid(&self, u: &Upload) -> bool {
        self.actors[u.actor]
            .hold
            .as_ref()
            .is_some_and(|h| h.generation == u.hold)
    }

    fn retire(&mut self, u: &Upload) {
        let Some(entry) = &u.retire else { return };
        let a = &mut self.actors[u.actor];
        // Exact key captured from the acknowledged predecessor. Fresh nonces
        // ensure a later segment/hold never reuses this retired identity.
        // LIST-based stale-hint clearing is a separate barrier operation.
        if a.entries.remove(entry) {
            self.outcome.counts.retired += 1;
        }
    }

    fn network_step(&mut self, index: usize) {
        let mut u = self.uploads[index].clone();
        match u.stage {
            Stage::Entry => {
                if u.entry_fail {
                    self.outcome.counts.entry_failed += 1;
                    u.stage = Stage::Completion;
                    u.due = self.now + 1;
                    self.uploads[index] = u;
                    return;
                }
                // The entry request returned before starting the segment PUT.
                self.actors[u.actor]
                    .entries
                    .insert(u.publish.clone().unwrap());
                u.stage = Stage::Put;
                u.due = self.now + 1 + self.rng.below(2);
            }
            Stage::Put => {
                // A no-entry upload incurs its fault delay here, once.
                if u.delay > 0 {
                    u.due = self.now + u.delay;
                    u.delay = 0;
                    self.uploads[index] = u;
                    return;
                }
                if !u.fail {
                    let key = segment_key(&u.object.segment);
                    if let std::collections::btree_map::Entry::Vacant(slot) =
                        self.actors[u.actor].objects.entry(key)
                    {
                        slot.insert(u.object.clone());
                        u.landed = true;
                    }
                    if u.reset && !u.landed {
                        self.outcome.counts.reset_collision += 1;
                    }
                    if u.reset && u.landed && !self.valid(&u) {
                        self.outcome.counts.reset_late += 1;
                    }
                    if u.landed
                        && !self.valid(&u)
                        && self.actors[u.actor]
                            .hold
                            .as_ref()
                            .is_some_and(|h| h.head.generation < h.generation)
                    {
                        self.outcome.counts.late_successor_unacked += 1;
                    }
                    if u.landed && !self.valid(&u) && u.object.boundaries.len() > 1 {
                        self.outcome.counts.late_batched += 1;
                    }
                }
                u.stage = Stage::Reply;
                u.due = self.now + 1 + self.rng.below(2);
            }
            Stage::Reply => {
                // Outcome recorded at PUT return, not at mailbox completion.
                u.returned_at = Some(self.now);
                u.acknowledged = u.landed && (self.valid(&u) || self.rule == Switch::AckLate);
                if u.acknowledged {
                    self.actors[u.actor]
                        .acknowledged_positions
                        .insert(u.object.segment.id);
                }
                if !u.acknowledged && self.rule == Switch::RetireOnFailure {
                    self.retire(&u);
                }
                u.stage = Stage::Completion;
                // Search must reach completions delayed across ownerships, as
                // widely as retire jobs. Directed healthy timings stay bounded.
                u.due = self.now + 1 + self.rng.below(if self.random_faults { 41 } else { 2 });
            }
            Stage::Completion => {
                let valid = self.valid(&u);
                if u.acknowledged
                    && (valid
                        || self.rule == Switch::CompletionOtherHold
                        || self.rule == Switch::AckLate)
                {
                    if !valid && self.rule == Switch::AckLate {
                        self.promise(u.actor, &u.object.after);
                    } else if let Some(h) = self.actors[u.actor].hold.as_mut() {
                        let old = h.head;
                        h.head = u.object.segment.id;
                        h.acknowledged = u.object.after.clone();
                        h.sidecar = h.head;
                        h.flights.retain(|&i| i != index);
                        let fulfilled: Vec<_> = h
                            .flushes
                            .iter()
                            .filter(|db| {
                                db.rows.iter().all(|r| {
                                    h.acknowledged.rows.contains(r)
                                        || h.acknowledged.erased.contains(r)
                                })
                            })
                            .cloned()
                            .collect();
                        h.flushes.retain(|db| !fulfilled.contains(db));

                        for db in fulfilled {
                            self.promise(u.actor, &db);
                        }
                        self.actors[u.actor]
                            .acknowledged_positions
                            .insert(u.object.segment.id);
                        self.retires
                            .push((self.now + self.rng.below(41), u.clone()));
                        self.outcome.counts.completions += 1;
                        self.check();
                        // A stale completion overwrites a head that was already
                        // published; the observer saw both transitions.
                        if self.rule == Switch::HeadBackwards {
                            self.actors[u.actor].hold.as_mut().unwrap().head = old;
                        }
                    }
                } else if valid {
                    // A failed replica gives up before any release: no further
                    // event or completion can act using that hold.
                    if self.rule == Switch::CaptureAfterFailure {
                        self.actors[u.actor]
                            .hold
                            .as_mut()
                            .unwrap()
                            .flights
                            .retain(|&i| i != index);
                    } else {
                        self.outcome.counts.failed_hold_dropped += 1;
                        self.drop_hold(u.actor, false);
                    }
                }
                u.stage = Stage::Done;
            }
            Stage::Done => {}
        }
        self.uploads[index] = u;
    }

    async fn advance(&mut self, ticks: u64) {
        for _ in 0..ticks {
            sleep(Duration::from_millis(10)).await;
            self.now += 1;
            if self.random_faults && self.fault_rng.below(25) == 0 {
                let actor = self.fault_rng.below(ACTORS as u64) as usize;
                let crash = self.fault_rng.below(2) == 0;
                self.interrupt = Some((self.now, actor, crash));
            }
            if let Some((at, actor, crash)) = self.interrupt
                && at <= self.now
            {
                self.interrupt = None;
                if let Some((kind, waiting_actor)) = self.waiting
                    && let (Some(target), Some(waiting)) =
                        (&self.actors[actor].hold, &self.actors[waiting_actor].hold)
                    && target.node == waiting.node
                {
                    self.outcome.counts.interrupted_waits[kind * 2 + usize::from(crash)] += 1;
                }
                if crash {
                    self.outcome.counts.wait_crashes += 1;
                    self.crash_node(actor);
                } else {
                    self.outcome.counts.wait_cuts += 1;
                    self.cut_node(actor);
                }
            }
            let jobs = std::mem::take(&mut self.retires);
            for (due, u) in jobs {
                if due <= self.now {
                    self.retire(&u);
                } else {
                    self.retires.push((due, u));
                }
            }
            self.entry_puts.retain(|(due, actor, entry)| {
                if *due <= self.now {
                    self.actors[*actor].entries.insert(entry.clone());
                    false
                } else {
                    true
                }
            });
            // Rotate ties using the seed, so actors and old/new completions
            // do not always get the same ordering.
            let count = self.uploads.len();
            let start = self.rng.below(count as u64) as usize;
            for n in 0..count {
                let i = (start + n) % count;
                if self.uploads[i].due <= self.now && !matches!(self.uploads[i].stage, Stage::Done)
                {
                    self.network_step(i);
                    self.check();
                }
            }
            for a in 0..ACTORS {
                self.capture(a);
            }
            self.check();
        }
    }

    fn drop_hold(&mut self, actor: usize, crash: bool) {
        let Some(h) = self.actors[actor].hold.take() else {
            return;
        };
        if crash && !h.flights.is_empty() && !h.pending.is_empty() {
            self.outcome.counts.crash_pending += 1;
        }
        self.actors[actor]
            .cache
            .insert(h.node, (h.sidecar, h.local.clone()));
        // A safe sidecar is invalidated by local writes beyond its position.
        if h.local != h.acknowledged {
            self.actors[actor].cache.remove(&h.node);
        }
        self.actors[actor].durable_waiting.clear();
        // Time-span loss bound, not "time since the oldest write" (an outage
        // can last forever). Compare actual local commits absent at takeover.
        let Ok((_, restored)) = self.observe_restore(actor) else {
            return;
        };
        let lost: Vec<_> = h
            .commits
            .iter()
            .filter(|(id, _)| !restored.rows.contains(id) && !restored.erased.contains(id))
            .collect();
        if !h.batching && lost.len() > 1 {
            self.outcome.violations.insert(Violation::StrictAhead);
        }
        if let (Some(first), Some(last)) = (lost.first(), lost.last()) {
            // The predecessor of the upload containing the first lost commit,
            // not the slow/lost upload itself and not an unrelated maximum RTT.
            let predecessor = self
                .uploads
                .iter()
                .rev()
                .filter(|u| u.actor == actor && u.hold == h.generation)
                .find(|u| {
                    !u.object.after.rows.contains(&first.0)
                        && u.captured_at <= first.1
                        && u.returned_at.is_some()
                });
            let r = predecessor.map_or(ROUND_TRIP, |u| u.returned_at.unwrap() - u.captured_at + 3);
            let healthy = self.healthy(first.1.saturating_sub(r), self.now);
            if healthy {
                self.outcome.counts.healthy_loss += 1;
            } else {
                self.outcome.counts.unhealthy_loss += 1;
            }
            // The ADR's stalled-bucket time-span bound is for a busy actor.
            // With idle gaps, a single stalled event plus one event an hour
            // later has a one-hour span even though admission is correct.
            // Check strict's event count above regardless; check healthy loss
            // regardless; for a stall require continuous demand, not dense
            // successful commits (blocking itself creates commit gaps).
            let mut demand = vec![first.1];
            demand.extend(
                h.demand
                    .iter()
                    .copied()
                    .filter(|t| *t >= first.1 && *t <= last.1),
            );
            demand.push(last.1);
            // Continuous means the next request is offered at each completed
            // boundary (including blocked attempts). Merely bounding gaps
            // between requests admits idle time before the first uncaptured
            // commit, which cannot be charged to the post-capture age bound.
            let continuous = h
                .commits
                .iter()
                .filter(|(_, at)| *at >= first.1 && *at < last.1)
                .all(|(_, at)| h.demand.contains(at));
            if !healthy && (!continuous || demand.windows(2).any(|p| p[1] - p[0] > EVENT_MAX + 1)) {
                self.outcome.counts.sparse_loss += 1;
                return;
            }
            // The stalled upload's capture anchors the post-stall span. Its
            // predecessor RTT describes earlier captured work, not extra time
            // in which admission may keep accepting new work after the stall.
            let stalled = self.uploads.iter().find(|u| {
                u.actor == actor && u.hold == h.generation && u.object.boundaries.contains(&first.0)
            });
            let from = if healthy {
                first.1
            } else {
                stalled.map_or(first.1, |u| u.captured_at)
            };
            let bound = if healthy {
                2 * r + EVENT_MAX
            } else {
                MAX_UNSENT + EVENT_MAX + 1 // capture tick
            };
            if !healthy && last.1.saturating_sub(from) == bound {
                self.outcome.counts.stalled_at_bound += 1;
            }
            if last.1.saturating_sub(from) > bound || (healthy && self.now - first.1 > bound) {
                if std::env::var_os("WASMKEEP_SIM_TRACE").is_some() {
                    eprintln!(
                        "loss at {}, actor {actor}, gen {}: {}..{}, r={r}, bound={bound}, healthy={healthy}",
                        self.now, h.generation, first.1, last.1
                    );
                    eprintln!(
                        "capture_from={from}, demand={demand:?}, commits={:?}",
                        h.commits
                    );
                }
                self.outcome.violations.insert(Violation::LossBound);
            }
        }
    }

    fn crash_node(&mut self, actor: usize) {
        let Some(node) = self.actors[actor].hold.as_ref().map(|h| h.node) else {
            return;
        };
        let affected: Vec<_> = (0..ACTORS)
            .filter(|&a| self.actors[a].hold.as_ref().is_some_and(|h| h.node == node))
            .collect();
        for a in affected {
            self.drop_hold(a, true);
        }
    }

    fn cut_node(&mut self, actor: usize) {
        let Some(node) = self.actors[actor].hold.as_ref().map(|h| h.node) else {
            return;
        };
        let affected: Vec<_> = (0..ACTORS)
            .filter(|&a| self.actors[a].hold.as_ref().is_some_and(|h| h.node == node))
            .collect();
        for a in affected {
            self.drop_hold(a, false);
        }
        self.outcome.counts.cuts += 1;
    }

    /// Drain, move, hibernate, park, stop use the same Flushed barrier, but
    /// only stop gives up at its deadline. Other sites remain resident/Busy.
    async fn handover(&mut self, actor: usize, kind: usize) {
        let previous = self.waiting;
        if previous.is_none() {
            self.waiting = Some((1, actor));
        }
        let result = self.handover_wait(actor, kind).await;
        self.waiting = previous;
        result
    }

    async fn handover_wait(&mut self, actor: usize, kind: usize) {
        let Some(h) = &self.actors[actor].hold else {
            return;
        };
        let generation = h.generation;
        self.outcome.counts.handoff_flight += u64::from(!h.flights.is_empty());
        self.outcome.counts.handoff_both +=
            u64::from(!h.flights.is_empty() && !h.pending.is_empty());
        let wanted = h.local.rows.clone();
        let start = self.now;
        if self.rule != Switch::ReleaseWithoutBarrier {
            for _ in 0..STOP_BUDGET {
                if self.actors[actor]
                    .hold
                    .as_ref()
                    .is_none_or(|h| h.pending.is_empty() && h.flights.is_empty())
                {
                    break;
                }
                self.advance(1).await;
            }
            if kind != 4
                && self.actors[actor]
                    .hold
                    .as_ref()
                    .is_some_and(|h| !h.pending.is_empty() || !h.flights.is_empty())
            {
                self.outcome.counts.busy += 1;
                return;
            }
        }
        if self.actors[actor]
            .hold
            .as_ref()
            .is_none_or(|h| h.generation != generation)
        {
            return;
        }
        self.finish_handover(actor, kind, start, &wanted);
    }

    fn finish_handover(&mut self, actor: usize, kind: usize, start: u64, wanted: &[u64]) {
        self.drop_hold(actor, false);
        let healthy = self.healthy(start.saturating_sub(ROUND_TRIP), self.now);
        if healthy {
            self.outcome.counts.healthy_handoffs[kind] += 1;
            if let Ok((_, db)) = self.observe_restore(actor)
                && wanted
                    .iter()
                    .any(|r| !db.rows.contains(r) && !db.erased.contains(r))
            {
                self.outcome.violations.insert(Violation::CleanLoss);
            }
        } else {
            self.outcome.counts.unhealthy_handoffs += 1;
        }
        self.acquire(actor);
        self.check();
    }

    fn snapshot(&mut self, actor: usize) {
        let a = &mut self.actors[actor];
        let Some(h) = &a.hold else { return };
        let unflushed = self.rule == Switch::SnapshotUncaptured && h.flights.is_empty();
        if h.head == EMPTY || (!h.pending.is_empty() && !unflushed) {
            return;
        }
        if self.rule != Switch::SnapshotInFlight
            && !unflushed
            && (!h.flights.is_empty() || h.local != h.acknowledged)
        {
            return;
        }
        // A conditional create, never overwrite an existing snapshot.
        if let std::collections::btree_map::Entry::Vacant(slot) = a.snapshots.entry(h.head) {
            slot.insert(h.local.clone());
            // Use this acknowledged position's ancestry, not a newer late
            // PUT that a fresh bucket LIST might have observed meanwhile.
            a.snapshot_paths
                .insert(h.head, a.position_paths[&h.head].clone());
            self.outcome.counts.snapshots += 1;
        } else {
            return;
        }
        let generation = h.generation;
        let head = h.head;
        // Retention is configured to zero (ADR 0012 permits this).
        let gone = collectable(&self.listing(actor), generation, head);
        for key in gone {
            self.actors[actor].objects.remove(&key);
            let snapshots = &mut self.actors[actor].snapshots;
            snapshots.retain(|id, _| snapshot_key(*id) != key);
            self.outcome.counts.collected += 1;
        }
        self.check();
    }

    async fn reset(&mut self, actor: usize) {
        let previous = self.waiting;
        if previous.is_none() {
            self.waiting = Some((2, actor));
        }
        let result = self.reset_wait(actor).await;
        self.waiting = previous;
        result
    }

    async fn reset_wait(&mut self, actor: usize) {
        if !matches!(self.rule, Switch::ResetCaptured | Switch::ResetInFlight)
            && !self.flush_until(actor, 100).await
        {
            return;
        }
        if let Some((i, reset_id)) = self.start_reset(actor) {
            self.advance(8).await;
            self.finish_reset(actor, i, reset_id);
        }
    }

    fn start_reset(&mut self, actor: usize) -> Option<(usize, u64)> {
        let a = &mut self.actors[actor];
        let h = a.hold.as_mut()?;
        let parent = if self.rule == Switch::ResetCaptured {
            h.captured
        } else {
            h.head
        };
        let before = if self.rule == Switch::ResetCaptured {
            h.captured_db.clone()
        } else {
            h.acknowledged.clone()
        };
        self.serial += 1;
        let mut after = before.clone();
        after.erased.extend(after.rows.drain(..));
        after.alarm = None;
        after.boundary = self.serial;
        after.resets.insert(self.serial);
        let obj = Object {
            segment: Segment {
                id: SegmentId {
                    generation: h.generation,
                    txid: parent.txid + 1,
                },
                parent_generation: parent.generation,
            },
            before,
            after: after.clone(),
            boundaries: vec![self.serial],
        };
        // The reset is a separate upload; unsafe variants skip the barrier,
        // thereby either referring to an absent parent or colliding with it.
        let i = self.uploads.len();
        h.flights.push(i);
        h.local = after.clone();
        h.captured = obj.segment.id;
        h.captured_db = after.clone();
        a.boundaries.insert(self.serial, after.clone());
        a.positions.entry(obj.segment.id).or_insert(after);
        let mut path = a.position_paths.get(&parent).cloned().unwrap_or_default();
        path.insert(obj.segment.id);
        a.position_paths.entry(obj.segment.id).or_insert(path);
        self.uploads.push(Upload {
            actor,
            hold: h.generation,
            retire: h.acknowledged.alarm.clone(),
            publish: None,
            entry_fail: false,
            reset: true,
            object: obj,
            due: self.now + 1,
            stage: Stage::Put,
            delay: std::mem::take(&mut a.next_delay),
            captured_at: self.now,
            returned_at: None,
            fail: false,
            landed: false,
            acknowledged: false,
        });
        let delay = self.uploads[i].delay;
        if delay > 0 {
            self.unhealthy
                .push((self.now, self.now + delay + ROUND_TRIP));
        }
        Some((i, self.serial))
    }

    fn finish_reset(&mut self, actor: usize, i: usize, reset_id: u64) {
        // The caller waits for this position's upload receipt. Serialization
        // makes the position unique; skipping the reset barrier lets the older
        // in-flight segment satisfy that waiter after the reset loses CREATE.
        // Consult task receipts, not the oracle's acknowledged-position set.
        let position = self.uploads[i].object.segment.id;
        let acknowledged = self
            .uploads
            .iter()
            .any(|u| u.actor == actor && u.object.segment.id == position && u.acknowledged);
        if acknowledged || self.rule == Switch::DeleteWithoutAck {
            self.actors[actor]
                .deleted
                .extend(self.uploads[i].object.after.erased.iter().copied());
            self.actors[actor].reset_acks.insert(reset_id);
            self.outcome.counts.resets += 1;
            self.snapshot(actor);
        }
        self.check();
    }

    /// A stale alarm's cleanup, including without_state after a trapped event.
    /// If the head belongs to a previous owner, first write a fence boundary.
    async fn cleanup(&mut self, actor: usize, without_state: bool) {
        let previous = self.waiting;
        if previous.is_none() {
            self.waiting = Some((3, actor));
        }
        let result = self.cleanup_wait(actor, without_state).await;
        self.waiting = previous;
        result
    }

    async fn cleanup_wait(&mut self, actor: usize, without_state: bool) {
        let Some(h) = &self.actors[actor].hold else {
            return;
        };
        let generation = h.generation;
        let fence_head = if self.rule == Switch::FenceCaptured {
            h.captured
        } else {
            h.head
        };
        if fence_head.generation != generation {
            if h.head == EMPTY {
                return;
            }
            // Fence is an event too: strict admission must finish the previous upload.
            while !self.can_start(actor) && self.actors[actor].hold.is_some() {
                self.advance(1).await;
            }
            let Some(h) = &self.actors[actor].hold else {
                return;
            };
            let alarm = h.local.alarm.as_ref().map(|e| e.at);
            self.commit(actor, alarm);
            self.flush(actor).await;
            if self.actors[actor]
                .hold
                .as_ref()
                .is_some_and(|h| h.head.generation == generation)
            {
                self.outcome.counts.fenced += 1;
            }
        } else if self.rule != Switch::FenceCaptured
            && !(without_state && self.rule == Switch::WithoutStateLocal)
        {
            self.flush(actor).await;
        }
        self.cleanup_entries(actor, without_state);
    }

    fn cleanup_entries(&mut self, actor: usize, without_state: bool) {
        let a = &mut self.actors[actor];
        let Some(h) = &a.hold else { return };
        self.outcome.counts.cleanup_live += 1;
        // FenceCaptured changes only the fence decision. R2 stays acknowledged.
        let db = if without_state && self.rule == Switch::WithoutStateLocal {
            &h.local
        } else {
            &h.acknowledged
        };
        let indexed = db.alarm.clone();
        let generation = h.generation;
        self.clear_stale_hints(actor, indexed.as_ref(), generation);
        self.check();
    }

    fn clear_stale_hints(&mut self, actor: usize, indexed: Option<&Entry>, generation: u64) {
        let a = &mut self.actors[actor];
        let listed: Vec<_> = a.entries.iter().cloned().collect();
        for hint in [1000, 2000, 3000] {
            if self.clear_only.is_some_and(|only| only != hint) {
                continue;
            }
            if indexed.is_some_and(|e| e.at == hint) {
                continue;
            }
            self.outcome.counts.r3_refused += listed
                .iter()
                .filter(|e| e.at == hint && !alarm_index::may_delete(e, generation))
                .count() as u64;
            // NoR3 changes only the generation fence; R2 and candidate
            // selection still run through the production helper.
            let generation = if self.rule == Switch::NoR3 {
                u64::MAX
            } else {
                generation
            };
            for entry in alarm_index::entries_to_clear(hint, &listed, indexed, generation) {
                if a.entries.remove(&entry) {
                    self.outcome.counts.cleared += 1;
                }
            }
        }
    }

    fn idle(&self, actor: usize) -> bool {
        self.actors[actor]
            .hold
            .as_ref()
            .is_some_and(|h| h.pending.is_empty() && h.flights.is_empty())
    }

    fn poll_operation(&mut self, actor: usize, operation: Operation) -> Option<Operation> {
        match operation {
            Operation::Event(at, alarm) if self.now >= at => {
                self.commit(actor, alarm);
                None
            }
            Operation::Flush(deadline, wanted) => {
                if self.idle(actor) {
                    return None;
                }
                if self.now >= deadline {
                    if let Some(h) = self.actors[actor].hold.as_mut() {
                        h.flushes.retain(|db| *db != wanted);
                    }
                    self.outcome.counts.wall_internal += 1;
                    None
                } else {
                    Some(Operation::Flush(deadline, wanted))
                }
            }
            Operation::Handover(kind, start, wanted) => {
                if self.idle(actor)
                    || self.rule == Switch::ReleaseWithoutBarrier
                    || (kind == 4 && self.now - start >= STOP_BUDGET)
                {
                    self.finish_handover(actor, kind, start, &wanted);
                    None
                } else if self.now - start >= STOP_BUDGET {
                    self.outcome.counts.busy += 1;
                    None
                } else {
                    Some(Operation::Handover(kind, start, wanted))
                }
            }
            Operation::ResetBarrier => {
                if self.idle(actor)
                    || matches!(self.rule, Switch::ResetCaptured | Switch::ResetInFlight)
                {
                    self.start_reset(actor)
                        .map(|(i, id)| Operation::ResetUpload(self.now + 8, i, id))
                } else {
                    Some(Operation::ResetBarrier)
                }
            }
            Operation::ResetUpload(at, i, id) if self.now >= at => {
                self.finish_reset(actor, i, id);
                None
            }
            Operation::CleanupAdmission(without) => {
                let h = self.actors[actor].hold.as_ref()?;
                let head = if self.rule == Switch::FenceCaptured {
                    h.captured
                } else {
                    h.head
                };
                let fence = head.generation != h.generation;
                if fence && h.head == EMPTY {
                    return None;
                }
                if fence {
                    let alarm = h.local.alarm.as_ref().map(|e| e.at);
                    if !self.can_start(actor) {
                        return Some(Operation::CleanupAdmission(without));
                    }
                    self.commit(actor, alarm);
                } else if self.rule == Switch::FenceCaptured
                    || (without && self.rule == Switch::WithoutStateLocal)
                {
                    self.cleanup_entries(actor, without);
                    return None;
                }
                self.request_flush(actor);
                Some(Operation::CleanupFlush(without, fence))
            }
            Operation::CleanupFlush(without, fence) => {
                if self.idle(actor) {
                    if fence {
                        self.outcome.counts.fenced += 1;
                    }
                    self.cleanup_entries(actor, without);
                    None
                } else {
                    Some(Operation::CleanupFlush(without, fence))
                }
            }
            other => Some(other),
        }
    }

    async fn random_workload(&mut self) {
        let mut tasks: [Option<Task>; ACTORS] = std::array::from_fn(|_| None);
        // Sustained arrivals complement the sparse mixed operation stream.
        // Random event lengths and crash timing search the admission edge.
        let actor = self.rng.below(ACTORS as u64) as usize;
        self.stall(actor, true);
        self.event(actor, Some(1000)).await;
        let end = self.now + 15 + self.rng.below(15);
        while self.now < end {
            self.event(actor, Some(1000)).await;
        }
        self.crash_node(actor);
        for actor in 0..ACTORS {
            self.acquire(actor);
        }
        self.random_faults = true;
        for _ in 0..240 {
            for (actor, task) in tasks.iter_mut().enumerate() {
                if let Some(old) = task.take()
                    && self.actors[actor]
                        .hold
                        .as_ref()
                        .is_some_and(|h| h.generation == old.generation)
                {
                    *task = self
                        .poll_operation(actor, old.operation)
                        .map(|operation| Task {
                            generation: old.generation,
                            operation,
                        });
                }
                // Operation choices and bucket faults use the workload stream;
                // node crashes/cuts use advance's independent fault stream.
                let op = self.rng.below(24);
                self.outcome.counts.random_ops += 1;
                match op {
                    17 => {
                        let fail = self.rng.below(3) == 0;
                        self.fault(actor, fail);
                    }
                    18 => self.cut_node(actor),
                    19 => self.crash_node(actor),
                    20..=21 if self.actors[actor].hold.is_none() => self.acquire(actor),
                    22 => self.actors[actor].next_entry_fail = true,
                    _ => {}
                }
                if task.is_some() {
                    continue;
                }
                let Some(h) = self.actors[actor].hold.as_ref() else {
                    continue;
                };
                let generation = h.generation;
                let operation = match op {
                    0..=9 if self.can_start(actor) => {
                        let alarm = match self.rng.below(4) {
                            0 => None,
                            n => Some(n * 1000),
                        };
                        Some(Operation::Event(
                            self.now + 1 + self.rng.below(EVENT_MAX),
                            alarm,
                        ))
                    }
                    10 | 23 => {
                        let wanted = self.actors[actor].hold.as_ref().unwrap().local.clone();
                        self.request_flush(actor);
                        Some(Operation::Flush(
                            self.now + if op == 23 { 2 } else { 100 },
                            wanted,
                        ))
                    }
                    11 => {
                        self.snapshot(actor);
                        None
                    }
                    12 => {
                        self.request_flush(actor);
                        Some(Operation::ResetBarrier)
                    }
                    13 => Some(Operation::CleanupAdmission(self.rng.below(2) == 0)),
                    14..=16 => {
                        let h = self.actors[actor].hold.as_ref().unwrap();
                        self.outcome.counts.handoff_flight += u64::from(!h.flights.is_empty());
                        self.outcome.counts.handoff_both +=
                            u64::from(!h.flights.is_empty() && !h.pending.is_empty());
                        Some(Operation::Handover(
                            self.rng.below(5) as usize,
                            self.now,
                            h.local.rows.clone(),
                        ))
                    }
                    _ => None,
                };
                *task = operation.map(|operation| Task {
                    generation,
                    operation,
                });
            }
            self.advance(1).await;
        }
        self.random_faults = false;
    }

    fn check(&mut self) {
        for actor in 0..ACTORS {
            match self.observe_restore(actor) {
                Err(e) => {
                    self.outcome.violations.insert(e);
                }
                Ok((_, db)) => {
                    let a = &self.actors[actor];
                    if a.promised
                        .iter()
                        .any(|id| !db.rows.contains(id) && !db.erased.contains(id))
                    {
                        if std::env::var_os("WASMKEEP_SIM_TRACE").is_some()
                            && !self.outcome.violations.contains(&Violation::DurableLost)
                        {
                            eprintln!(
                                "durable loss t={} actor={actor} promised={:?} restored={db:?}",
                                self.now, a.promised
                            );
                            for u in self.uploads.iter().filter(|u| u.actor == actor) {
                                eprintln!(
                                    "upload {:?} cap={} return={:?} landed={} ack={} before={:?} after={:?} erased={:?}",
                                    u.object.segment,
                                    u.captured_at,
                                    u.returned_at,
                                    u.landed,
                                    u.acknowledged,
                                    u.object.before.rows,
                                    u.object.after.rows,
                                    u.object.after.erased
                                );
                            }
                            if let Some(h) = &a.hold {
                                eprintln!(
                                    "hold={} head={:?} local={:?} acknowledged={:?}",
                                    h.generation, h.head, h.local, h.acknowledged
                                );
                            }
                        }
                        self.outcome.violations.insert(Violation::DurableLost);
                    }
                    if a.deleted.iter().any(|id| db.rows.contains(id))
                        || !a.reset_acks.is_subset(&db.resets)
                    {
                        self.outcome.violations.insert(Violation::DeletedReturned);
                    }
                    if let Some(entry) = &db.alarm
                        && !a.entries.contains(entry)
                    {
                        self.outcome.violations.insert(Violation::AlarmMissing);
                    }
                }
            }
            if let Some(h) = self.actors[actor].hold.as_mut() {
                // Structural only: this abstract cache lacks the sidecar/file
                // crash-atomicity model needed to prove a restore consequence.
                if h.sidecar != h.head {
                    self.outcome.violations.insert(Violation::SidecarAhead);
                }
                if h.head < h.high_water || h.head.txid < h.high_water.txid {
                    self.outcome.violations.insert(Violation::HeadBackwards);
                }
                h.high_water = h.high_water.max(h.head);
            }
        }
    }
}

pub(crate) fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

/// Each seed exercises all workloads; random actor order, node placement,
/// event lengths and individual request/completion delays vary the races.
pub(crate) fn simulate(seed: u64, rule: Switch, batching: bool) -> Outcome {
    Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(async {
        let mut result = Outcome::default();
        for scenario in 0..32 {
            let mut w = World::new(seed.wrapping_mul(31) + scenario, rule, batching);
            // Start with acknowledged data and an alarm in all three actors.
            for a in 0..ACTORS {
                w.event(a, Some(1000)).await;
                w.flush(a).await;
            }
            let a = w.rng.below(ACTORS as u64) as usize;
            match scenario {
                0 => {
                    // Batched late PUT: hold the first segment, build a pending
                    // window, then let it capture as a multi-event segment.
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    for _ in 0..5 {
                        w.event(a, Some(1000)).await;
                    }
                    w.advance(50).await;
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    for _ in 0..5 {
                        w.event(a, Some(1000)).await;
                    }
                    // Delay the *next* capture as well, crash after it starts.
                    w.stall(a, false);
                    for _ in 0..50 {
                        if w.uploads.iter().any(|u| {
                            u.actor == a
                                && u.object.boundaries.len() > 1
                                && !matches!(u.stage, Stage::Done)
                        }) {
                            break;
                        }
                        w.advance(1).await;
                    }
                    for _ in 0..3 {
                        w.event(a, Some(1000)).await;
                    }
                    w.crash_node(a);
                    w.acquire(a);
                    w.event(a, Some(2000)).await;
                    w.flush(a).await;
                    w.snapshot(a); // GC while the old batched PUT is still outstanding.
                }
                1 => {
                    // Lower segment lost; a durable write behind it must wait.
                    w.stall(a, true);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    w.event(a, Some(2000)).await;
                    if let Some(row) = w.actors[a]
                        .hold
                        .as_ref()
                        .and_then(|h| h.local.rows.last())
                        .copied()
                    {
                        w.actors[a].durable_waiting.insert(row);
                    }
                    w.request_flush(a);
                    w.advance(50).await;
                }
                2 => {
                    w.stall(a, true);
                    w.event(a, Some(2000)).await;
                    w.advance(1).await;
                    w.request_flush(a);
                    w.snapshot(a);
                    w.advance(50).await;
                }
                3 => {
                    w.stall(a, true);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    w.reset(a).await;
                    w.advance(50).await;
                }
                4 => {
                    // Every clean site, with ordinary healthy outstanding work.
                    for kind in 0..5 {
                        w.event(a, Some(2000)).await;
                        w.handover(a, kind).await;
                        w.advance(ROUND_TRIP).await;
                    }
                    // Healthy predicate must exclude this whole stop window.
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.handover(a, 4).await;
                }
                5 => {
                    w.stall(a, true);
                    w.event(a, None).await;
                    w.advance(1).await;
                    w.cleanup(a, true).await;
                    w.advance(50).await;
                }
                6 => {
                    // A former owner's PUT can still become the head until
                    // this owner's fence lands. Capturing a fence is not one.
                    w.stall(a, false);
                    w.event(a, Some(2000)).await;
                    w.advance(1).await;
                    w.cut_node(a);
                    w.acquire(a);
                    w.stall(a, true);
                    w.event(a, None).await;
                    w.advance(1).await;
                    w.cleanup(a, false).await;
                    w.advance(60).await;
                }
                7 => {
                    // A PUT acknowledged while valid, with the completion
                    // still queued when a new hold replaces it.
                    w.event(a, Some(2000)).await;
                    for _ in 0..20 {
                        if w.uploads.iter().any(|u| {
                            u.actor == a && matches!(u.stage, Stage::Completion) && u.acknowledged
                        }) {
                            break;
                        }
                        w.advance(1).await;
                    }
                    for u in &mut w.uploads {
                        if u.actor == a && matches!(u.stage, Stage::Completion) {
                            u.due += 30;
                        }
                    }
                    w.drop_hold(a, false);
                    w.acquire(a);
                    w.event(a, None).await;
                    w.flush(a).await;
                    w.advance(40).await;
                }
                8 => {
                    // Long stall: admission must stop commits, even though
                    // the node lease remains valid and callers keep arriving.
                    w.stall(a, true);
                    w.event(a, Some(1000)).await;
                    for _ in 0..20 {
                        w.event(a, Some(1000)).await;
                    }
                    w.crash_node(a);
                }
                9 => {
                    // Successful flush/reset/GC, then crash and restore; old
                    // data must remain erased through subsequent ownerships.
                    for _ in 0..5 {
                        w.event(a, Some(2000)).await;
                    }
                    if let Some(row) = w.actors[a]
                        .hold
                        .as_ref()
                        .and_then(|h| h.local.rows.last())
                        .copied()
                    {
                        w.actors[a].durable_waiting.insert(row);
                    }
                    w.flush(a).await;
                    w.snapshot(a);
                    // One long event with an explicit flush point, then more
                    // work: each piece is an independently restorable prefix.
                    w.commit(a, Some(1000));
                    w.flush(a).await;
                    w.outcome.counts.mid_event_flushes += 1;
                    w.commit(a, None);
                    w.flush(a).await;
                    w.reset(a).await;
                    w.crash_node(a);
                    w.acquire(a);
                    w.event(a, None).await;
                    w.flush(a).await;
                    w.snapshot(a);
                }
                10 => {
                    // Section 9: several distinct alarm identities inside ONE segment.
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    for alarm in [Some(1000), Some(2000), None, Some(3000)] {
                        if w.can_start(a) {
                            w.commit(a, alarm);
                        }
                        w.advance(1).await;
                    }
                    w.flush(a).await;
                }
                11 => {
                    // Live cleanup: fence an inherited head, then clear stale hints.
                    w.drop_hold(a, false);
                    w.acquire(a);
                    w.actors[a].entries.insert(Entry::legacy(2000));
                    w.cleanup(a, false).await;
                    w.event(a, None).await;
                    w.actors[a].entries.insert(Entry::legacy(3000));
                    w.cleanup(a, true).await;
                }
                12 => {
                    // A stale-hint LIST admitted at a drained barrier resumes
                    // after ownership changes. R3 protects the successor entry.
                    w.event(a, Some(2000)).await;
                    w.flush(a).await;
                    let h = w.actors[a].hold.as_ref().unwrap();
                    let generation = h.generation;
                    let indexed = h.acknowledged.alarm.clone();
                    w.drop_hold(a, false);
                    w.acquire(a);
                    w.event(a, Some(1000)).await;
                    w.flush(a).await;
                    w.clear_stale_hints(a, indexed.as_ref(), generation);
                    w.check();
                    w.advance(45).await;
                }
                13 => {
                    // In-flight segment already landed; reset's conditional create
                    // loses the same key if it skips the acknowledgement barrier.
                    w.event(a, Some(1000)).await;
                    loop {
                        w.advance(1).await;
                        if w.uploads
                            .iter()
                            .any(|u| u.actor == a && u.landed && matches!(u.stage, Stage::Reply))
                        {
                            break;
                        }
                    }
                    w.reset(a).await;
                }
                14 => {
                    // Reset's PUT survives a cut inside its wait. A successor
                    // anchors history before it lands: recording the delete early
                    // would resurrect all rows it claimed erased.
                    w.actors[a].next_delay = 40;
                    w.interrupt = Some((w.now + 2, a, false));
                    w.reset(a).await;
                    w.acquire(a);
                    w.event(a, Some(1000)).await;
                    w.flush(a).await;
                    w.advance(50).await;
                }
                15 => {
                    // Healthy but slow bucket, random crash tick under continuous load.
                    w.healthy_delay = 10;
                    w.event(a, Some(1000)).await;
                    w.flush(a).await; // measured predecessor RTT in this regime
                    w.event(a, Some(1000)).await;
                    let crash_at = w.now + 1 + w.rng.below(8);
                    w.interrupt = Some((crash_at, a, true));
                    while w.now <= crash_at {
                        w.event(a, Some(1000)).await;
                    }
                }
                16 => {
                    // Busy handover with both a segment and uncaptured work.
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    if w.can_start(a) {
                        w.commit(a, Some(2000));
                    }
                    w.handover(a, 0).await;
                    w.advance(50).await;
                    w.handover(a, 0).await;
                }
                17 => {
                    // Same-node cache lookup after a PUT lands before its reply.
                    // Sidecar ordering is structural here; see the module header.
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    if w.can_start(a) {
                        w.commit(a, Some(2000));
                    }
                    loop {
                        w.advance(1).await;
                        if w.uploads
                            .iter()
                            .any(|u| u.actor == a && u.landed && matches!(u.stage, Stage::Reply))
                        {
                            break;
                        }
                    }
                    let node = w.actors[a].hold.as_ref().unwrap().node;
                    w.drop_hold(a, false);
                    w.acquire_at(a, Some(node));
                    w.event(a, None).await;
                    w.flush(a).await;
                }
                18 => {
                    // Entry failure must never launch this segment; after failure
                    // a fresh capture from the consumed position leaves a hole.
                    w.actors[a].next_entry_fail = true;
                    w.event(a, Some(2000)).await;
                    w.advance(1).await;
                    if w.can_start(a) {
                        w.commit(a, None);
                    }
                    w.advance(10).await;
                    w.event(a, Some(3000)).await;
                    w.advance(20).await;
                    let failed: Vec<_> = w.uploads.iter().filter(|u| u.entry_fail).collect();
                    assert!(!failed.is_empty());
                    // Entry failure is checked structurally separately from
                    // the alarm oracle, which replays actual bucket contents.
                    if failed.iter().any(|u| u.landed) {
                        w.outcome.violations.insert(Violation::EntryFailureUploaded);
                    }
                }
                19 => {
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    let generation = w.actors[a].hold.as_ref().unwrap().generation;
                    assert!(!w.flush_until(a, 2).await);
                    assert_eq!(w.actors[a].hold.as_ref().unwrap().generation, generation);
                    w.flush(a).await;
                    w.outcome.counts.wall_completed += 1;
                    w.actors[a].next_delay = 10;
                    w.event(a, Some(2000)).await;
                    w.advance(1).await;
                    // Keep several local commits absent from the bucket so
                    // strict's lost-event count judges the switch itself.
                    if w.batching {
                        w.event(a, Some(3000)).await;
                        w.event(a, None).await;
                    }
                    w.switch_to_strict(a).await;
                    w.crash_node(a);
                }
                20 => {
                    // A late PUT lands while the successor is live, BEFORE its
                    // first acknowledgement (not merely with no owner).
                    w.stall(a, false);
                    w.event(a, Some(2000)).await;
                    w.advance(3).await;
                    w.cut_node(a);
                    w.acquire(a);
                    w.advance(50).await;
                    w.event(a, None).await;
                    w.flush(a).await;
                }
                30 => {
                    // Slow but healthy handover with both kinds of work.
                    w.healthy_delay = 10;
                    w.event(a, Some(1000)).await;
                    w.advance(8).await;
                    if w.can_start(a) {
                        w.commit(a, Some(2000));
                    }
                    let before = w.outcome.counts.healthy_handoffs[0];
                    if w.batching {
                        let h = w.actors[a].hold.as_ref().unwrap();
                        assert!(!h.flights.is_empty() && !h.pending.is_empty());
                    }
                    w.handover(a, 0).await;
                    assert_eq!(w.outcome.counts.healthy_handoffs[0], before + 1);
                }
                21 => w.random_workload().await,
                31 => {
                    // Clearing waits for a drained barrier and judges R2 on
                    // the entry the acknowledged head names there. One read
                    // before the same hold wrote on is stale: the head now
                    // names another entry, which that read would clear.
                    w.event(a, Some(2000)).await;
                    w.flush(a).await;
                    let Some(h) = w.actors[a].hold.as_ref() else {
                        continue;
                    };
                    let (generation, earlier) = (h.generation, h.acknowledged.alarm.clone());
                    w.event(a, Some(1000)).await;
                    w.flush(a).await;
                    let Some(h) = w.actors[a].hold.as_ref() else {
                        continue;
                    };
                    let indexed = if w.rule == Switch::ClearOldAcknowledged {
                        earlier
                    } else {
                        h.acknowledged.alarm.clone()
                    };
                    w.clear_stale_hints(a, indexed.as_ref(), generation);
                    w.check();
                }
                _ => {
                    // Every waiting path is interrupted by both node death and
                    // lease loss. These rare seeds supplement the random stream.
                    let kind = (scenario - 22) as usize / 2;
                    let crash = scenario % 2 == 1;
                    w.stall(a, false);
                    w.event(a, Some(1000)).await;
                    w.advance(1).await;
                    w.interrupt = Some((w.now + 2, a, crash));
                    match kind {
                        0 => w.flush(a).await,
                        1 => w.handover(a, 0).await,
                        2 => w.reset(a).await,
                        _ => w.cleanup(a, true).await,
                    }
                    w.acquire(a);
                    w.advance(50).await;
                }
            }
            // Settle independent bucket work after all holds end, including
            // late PUTs: never cancel network work just because its node died.
            for a in 0..ACTORS {
                w.drop_hold(a, false);
            }
            w.advance(100).await;
            w.check();
            result.counts.add(w.outcome.counts);
            result.per.push(w.outcome.violations.clone());
            result.violations.extend(w.outcome.violations);
        }
        result
    })
}

pub(crate) fn caught(rule: Switch, expected: Violation) {
    caught_mode(rule, expected, true);
}

pub(crate) fn caught_mode(rule: Switch, expected: Violation, batching: bool) {
    let (seed, outcome) = (0..seeds())
        .find_map(|seed| {
            let out = simulate(seed, rule, batching);
            out.violations.contains(&expected).then_some((seed, out))
        })
        .unwrap_or_else(|| {
            panic!(
                "{rule:?} batching={batching}: no {expected:?} within {} seeds",
                seeds()
            )
        });
    let again = simulate(seed, rule, batching);
    assert_eq!(outcome.per, again.per, "seed {seed} did not reproduce");
    let table: Vec<_> = outcome
        .per
        .iter()
        .enumerate()
        .filter(|(_, v)| !v.is_empty())
        .collect();
    eprintln!(
        "caught: {rule:?} batching={batching} seed={seed} expected={expected:?}; scenarios={table:?}"
    );
}

pub(crate) fn safe(batching: bool) {
    let only = std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let range = only.map_or(0..seeds(), |seed| seed..seed + 1);
    let count = range.end - range.start;
    let mut totals = Counts::default();
    for seed in range {
        let out = simulate(seed, Switch::Safe, batching);
        assert!(
            out.violations.is_empty(),
            "seed {seed}, batching={batching}: {:?}; scenarios={:?}",
            out.violations,
            out.per
        );
        totals.add(out.counts);
    }
    assert!(count > 0);
    assert!(totals.commits > count * 30);
    assert!(totals.flushes > count * 10);
    assert!(
        totals.durable >= count,
        "no successful durable writes: {totals:?}"
    );
    assert!(totals.mid_event_flushes >= count);
    assert!(totals.cuts >= count);
    assert!(totals.collected > count);
    assert!(totals.resets >= count);
    assert!(totals.healthy_handoffs.iter().all(|&n| n >= count));
    assert!(totals.unhealthy_handoffs >= count);
    for (name, value) in [
        ("cleared", totals.cleared),
        ("fenced", totals.fenced),
        ("live cleanup", totals.cleanup_live),
        ("R3", totals.r3_refused),
        ("late before successor ack", totals.late_successor_unacked),
        ("handoff in flight", totals.handoff_flight),
        ("Busy", totals.busy),
        ("healthy loss", totals.healthy_loss),
        ("entry failure", totals.entry_failed),
        ("wall completion", totals.wall_completed),
        ("mode switch", totals.mode_switched),
        ("wait crash", totals.wait_crashes),
        ("wait cut", totals.wait_cuts),
        ("late reset", totals.reset_late),
    ] {
        assert!(value >= count, "{name}: {value} < {count}; {totals:?}");
    }
    assert!(totals.random_ops >= count * 240 * ACTORS as u64);
    assert!(totals.mode_inflight >= count, "{totals:?}");
    assert!(
        totals.interrupted_waits.iter().all(|n| *n >= count),
        "{totals:?}"
    );
    if batching {
        assert!(totals.batched > count, "no batching: {totals:?}");
        assert!(
            totals.late_batched >= count,
            "no late batched segments: {totals:?}"
        );
        assert!(
            totals.crash_pending >= count,
            "no crash with both in-flight and uncaptured commits: {totals:?}"
        );
        assert!(totals.blocked_events > count);
        assert!(totals.batch_alarm_changes >= count, "{totals:?}");
        assert!(totals.handoff_both >= count, "{totals:?}");
        if only.is_none() && count >= 200 {
            assert!(
                totals.stalled_at_bound >= 3,
                "stalled bound never reached: {totals:?}"
            );
        }
    } else {
        assert_eq!(totals.batched, 0, "strict captured several events");
        assert_eq!(totals.crash_pending, 0, "strict ran ahead of its upload");
    }
    eprintln!("batched model: {count} seeds, batching={batching}: {totals:?}");
}

pub(crate) fn healthy_window_checks_the_whole_interval() {
    let mut w = World::new(0, Switch::Safe, true);
    w.unhealthy.push((10, 20));
    assert!(w.healthy(0, 9));
    assert!(w.healthy(21, 40));
    assert!(
        !w.healthy(0, 30),
        "recovered at the end is not healthy throughout"
    );
    assert!(!w.healthy(10, 10));
    assert!(!w.healthy(20, 20));
}

/// Require multiple independent seeds in the named workload, not a lucky
/// directed witness elsewhere in the simulation.
pub(crate) fn caught_in_scenario(rule: Switch, expected: Violation, scenario: usize) {
    let mut witnesses = Vec::new();
    for seed in 0..seeds() {
        let out = simulate(seed, rule, true);
        if out.per[scenario].contains(&expected) {
            assert_eq!(out.per, simulate(seed, rule, true).per);
            witnesses.push(seed);
            if witnesses.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(
        witnesses.len(),
        3,
        "{rule:?}: insufficient {expected:?} witnesses in scenario {scenario}: {witnesses:?}"
    );
    eprintln!("caught: {rule:?} expected={expected:?} scenario={scenario} seeds={witnesses:?}");
}

// The node's alarm timer and mailbox deliveries (ADR 0009, amendment of
// 2026-11-19), on the batched hold above. The timer mirrors the server's
// (`alarm.rs`): one registration per actor, waiting or handed to the
// mailbox; the decisions are the production ones in
// `wasmkeep_cluster::alarm_index` (`delivery_covers`, `deliver_again`,
// `stale_clear_skippable`). Alarm times are abstract: every committed
// alarm is due. The mailbox runs jobs in order: fetch events that keep the
// alarm, events that move it, and deliveries. A delivery is refused at
// admission, dropped before it answers, or deferred (it runs and registers
// nothing, as for another owner) at random; results reach the timer up to
// three ticks late, racing the next events' syncs. A delivery whose time
// is committed runs the round (moves the alarm on); one whose time is not
// clears the stale hint at the barrier (`cleanup`, that one time), unless
// the belt skips it. Holds end at random (crashes, cuts); a new hold
// registers its restored alarm, as a restore does. At the end, with
// faults off, every entry left at a time no state names is delivered from
// the index (a sweep reaching a quiet actor), and none may remain.

/// One rule of the timer and delivery changed at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryRule {
    Safe,
    /// An event's end re-registers even when the delivery in flight is for
    /// the same time (the timer before 0.7.0).
    ResyncReplaces,
    /// A completion forgets a delivery an event's sync was left to.
    CompleteForgets,
    /// A dropped delivery is completed (forgotten), not delivered again.
    DropForgotten,
    /// The belt also skips stale hints a sweep delivers.
    BeltOnSweep,
}

#[derive(Clone, Copy)]
struct Reg {
    waiting: bool,
    ready_at: u64,
    hint: u64,
    id: u64,
    swept: bool,
    covered: bool,
    refusals: u32,
    /// Made by an event's end while a delivery of the same time was in
    /// flight: a second delivery of one round.
    second: bool,
}

enum Job {
    /// An event that leaves the alarm as it is.
    Fetch,
    /// An event that sets the alarm (or clears it).
    Move(Option<u64>),
    Deliver {
        hint: u64,
        id: u64,
        swept: bool,
        second: bool,
    },
}

enum Answer {
    Complete(u64),
    /// Refused at admission (`false`) or dropped by the mailbox (`true`).
    Retry(u64, bool),
}

#[derive(Default)]
struct Local {
    hold: u64,
    reg: Option<Reg>,
    ids: u64,
    queue: std::collections::VecDeque<Job>,
    answers: Vec<(u64, Answer)>,
    /// The latest time this hold ran the round for.
    delivered: Option<u64>,
    /// The last job to sync the timer was an event that committed an alarm.
    synced_by_event: bool,
}

#[derive(Debug, Default)]
pub(crate) struct DeliveryCounts {
    pub(crate) rounds: u64,
    pub(crate) covered: u64,
    pub(crate) delivered_again: u64,
    pub(crate) dropped: u64,
    pub(crate) refused: u64,
    pub(crate) deferred: u64,
    pub(crate) moves_in_flight: u64,
    pub(crate) stale_clears: u64,
    pub(crate) belt_skips: u64,
    pub(crate) swept_clears: u64,
    pub(crate) orphans_cleared: u64,
    pub(crate) holds: u64,
}

impl DeliveryCounts {
    fn add(&mut self, o: &Self) {
        self.rounds += o.rounds;
        self.covered += o.covered;
        self.delivered_again += o.delivered_again;
        self.dropped += o.dropped;
        self.refused += o.refused;
        self.deferred += o.deferred;
        self.moves_in_flight += o.moves_in_flight;
        self.stale_clears += o.stale_clears;
        self.belt_skips += o.belt_skips;
        self.swept_clears += o.swept_clears;
        self.orphans_cleared += o.orphans_cleared;
        self.holds += o.holds;
    }
}

struct Deliveries {
    rule: DeliveryRule,
    local: Local,
    counts: DeliveryCounts,
}

const TIMES: [u64; 3] = [1000, 2000, 3000];

impl Deliveries {
    fn set(&mut self, now: u64, at: u64, swept: bool) {
        let l = &mut self.local;
        l.ids += 1;
        l.reg = Some(Reg {
            waiting: true,
            ready_at: now,
            hint: at,
            id: l.ids,
            swept,
            covered: false,
            refusals: 0,
            second: false,
        });
    }

    /// An event's end: `AlarmHandle::sync`, or `clear` for none.
    fn sync(&mut self, now: u64, committed: Option<u64>) {
        let Some(at) = committed else {
            self.local.reg = None;
            return;
        };
        let in_flight = self.local.reg.and_then(|r| (!r.waiting).then_some(r.hint));
        if in_flight.is_some_and(|hint| hint != at) {
            self.counts.moves_in_flight += 1;
        }
        if self.rule != DeliveryRule::ResyncReplaces && alarm_index::delivery_covers(in_flight, at)
        {
            self.counts.covered += 1;
            if let Some(r) = self.local.reg.as_mut() {
                r.covered = true;
            }
        } else {
            self.set(now, at, false);
            if let Some(r) = self.local.reg.as_mut() {
                r.second = in_flight == Some(at);
            }
        }
    }

    /// A delivery's result reaching the timer.
    fn answer(&mut self, now: u64, answer: Answer) {
        let (id, retry) = match answer {
            Answer::Complete(id) => (id, None),
            Answer::Retry(id, dropped) => (id, Some(dropped)),
        };
        let Some(r) = self.local.reg else { return };
        if r.id != id || r.waiting {
            return;
        }
        match retry {
            Some(true) if self.rule == DeliveryRule::DropForgotten => self.complete(now, r),
            Some(_) => {
                self.local.ids += 1;
                self.local.reg = Some(Reg {
                    waiting: true,
                    ready_at: now + 1 + u64::from(r.refusals.min(3)),
                    id: self.local.ids,
                    covered: false,
                    refusals: r.refusals + 1,
                    ..r
                });
            }
            None => self.complete(now, r),
        }
    }

    fn complete(&mut self, now: u64, r: Reg) {
        let covered = r.covered && self.rule != DeliveryRule::CompleteForgets;
        if alarm_index::deliver_again(covered) {
            self.counts.delivered_again += 1;
            self.set(now, r.hint, false);
        } else {
            self.local.reg = None;
        }
    }
}

impl World {
    fn committed(&self, actor: usize) -> Option<u64> {
        self.actors[actor]
            .hold
            .as_ref()
            .and_then(|h| h.local.alarm.as_ref().map(|e| e.at))
    }

    fn named(&self, actor: usize) -> Option<u64> {
        self.actors[actor]
            .hold
            .as_ref()
            .and_then(|h| h.acknowledged.alarm.as_ref().map(|e| e.at))
    }

    fn hold_generation(&self, actor: usize) -> Option<u64> {
        self.actors[actor].hold.as_ref().map(|h| h.generation)
    }

    /// Runs an event to its commit, waiting for admission like a mailbox
    /// job does; `false` if the hold ended first.
    async fn admitted_event(&mut self, actor: usize, alarm: Option<u64>) -> bool {
        let Some(generation) = self.hold_generation(actor) else {
            return false;
        };
        for _ in 0..200 {
            if self.hold_generation(actor) != Some(generation) {
                return false;
            }
            if self.can_start(actor) {
                let duration = 1 + self.rng.below(EVENT_MAX);
                self.advance(duration).await;
                if self.hold_generation(actor) != Some(generation) {
                    return false;
                }
                self.commit(actor, alarm);
                self.check();
                return true;
            }
            self.advance(1).await;
        }
        false
    }

    /// Clears the stale hint `hint` as `clear_stale_hint` does: at a
    /// `Flushed` barrier, on the acknowledged entry (ADR 0034, section 9).
    async fn clear_hint(&mut self, actor: usize, hint: u64) -> u64 {
        let before = self.actors[actor].entries.len();
        self.clear_only = Some(hint);
        self.cleanup(actor, false).await;
        self.clear_only = None;
        before.saturating_sub(self.actors[actor].entries.len()) as u64
    }

    /// One mailbox job of the delivery model.
    async fn run_job(&mut self, actor: usize, d: &mut Deliveries, job: Job) {
        match job {
            Job::Fetch => {
                let keep = self.committed(actor);
                self.synced_event(actor, d, keep).await;
            }
            Job::Move(alarm) => self.synced_event(actor, d, alarm).await,
            Job::Deliver {
                hint,
                id,
                swept,
                second,
            } => {
                if self.rng.chance(8) {
                    d.counts.dropped += 1;
                    let due = self.now + self.rng.below(4);
                    d.local.answers.push((due, Answer::Retry(id, true)));
                    return;
                }
                if self.rng.chance(8) {
                    // Ran and registered nothing (another owner, say): a
                    // committed alarm left unarmed then is not this node's.
                    d.counts.deferred += 1;
                    d.local.synced_by_event = false;
                    let due = self.now + self.rng.below(4);
                    d.local.answers.push((due, Answer::Complete(id)));
                    return;
                }
                let generation = d.local.hold;
                if second && self.committed(actor) != Some(hint) {
                    // It runs after the round it repeats: a stale hint
                    // cleared for nothing (before 0.7.0, in the mailbox).
                    self.outcome.violations.insert(Violation::LocalDuplicate);
                }
                self.deliver(actor, d, hint, swept).await;
                if self.hold_generation(actor) == Some(generation) {
                    let due = self.now + self.rng.below(4);
                    d.local.answers.push((due, Answer::Complete(id)));
                }
            }
        }
    }

    async fn synced_event(&mut self, actor: usize, d: &mut Deliveries, alarm: Option<u64>) {
        if self.admitted_event(actor, alarm).await {
            let committed = self.committed(actor);
            d.sync(self.now, committed);
            d.local.synced_by_event = committed.is_some();
        }
    }

    /// `ActorState::alarm`, abstracted: the committed alarm, if any, is due
    /// and runs its round (moving the alarm on); a hint that is not
    /// committed is cleared as stale, or skipped by the belt.
    async fn deliver(&mut self, actor: usize, d: &mut Deliveries, hint: u64, swept: bool) {
        let committed = self.committed(actor);
        let delivered = d.local.delivered;
        let stale = committed != Some(hint);
        d.local.synced_by_event = false;
        let generation = d.local.hold;
        if let Some(at) = committed {
            d.local.delivered = Some(at);
            d.counts.rounds += 1;
            let next = if self.rng.below(4) == 0 {
                None
            } else {
                let others: Vec<_> = TIMES.iter().copied().filter(|t| *t != at).collect();
                Some(others[self.rng.below(others.len() as u64) as usize])
            };
            if !self.admitted_event(actor, next).await {
                return;
            }
            d.sync(self.now, self.committed(actor));
        } else {
            d.local.reg = None;
        }
        if !stale || self.hold_generation(actor) != Some(generation) {
            return;
        }
        let as_swept = swept && d.rule != DeliveryRule::BeltOnSweep;
        if alarm_index::stale_clear_skippable(hint, as_swept, delivered) {
            d.counts.belt_skips += 1;
            return;
        }
        d.counts.stale_clears += 1;
        let cleared = self.clear_hint(actor, hint).await;
        if swept {
            d.counts.swept_clears += 1;
            d.counts.orphans_cleared += cleared;
        }
    }

    /// The timer's own steps for one tick: answers that came back, and a
    /// waiting registration handed to the mailbox (or refused).
    fn timer_tick(&mut self, actor: usize, d: &mut Deliveries) {
        let now = self.now;
        let answers = std::mem::take(&mut d.local.answers);
        for (due, answer) in answers {
            if due <= now {
                d.answer(now, answer);
            } else {
                d.local.answers.push((due, answer));
            }
        }
        if let Some(r) = d.local.reg.as_mut()
            && r.waiting
            && r.ready_at <= now
        {
            r.waiting = false;
            let (hint, id, swept, second) = (r.hint, r.id, r.swept, r.second);
            if self.rng.chance(10) {
                d.counts.refused += 1;
                d.local.answers.push((now, Answer::Retry(id, false)));
            } else {
                d.local.queue.push_back(Job::Deliver {
                    hint,
                    id,
                    swept,
                    second,
                });
            }
        }
        // Once every result is in, an alarm an event committed is armed.
        if d.local.answers.is_empty()
            && d.local.synced_by_event
            && self.committed(actor).is_some()
            && d.local.reg.is_none()
        {
            self.outcome.violations.insert(Violation::AlarmUnarmed);
        }
    }

    /// A new hold: its mailbox and timer state start over, and the restored
    /// alarm is registered (a restore's `Set`).
    fn new_local(&mut self, actor: usize, d: &mut Deliveries) {
        let Some(generation) = self.hold_generation(actor) else {
            return;
        };
        if d.local.hold == generation {
            return;
        }
        d.counts.holds += 1;
        d.local = Local {
            hold: generation,
            ids: d.local.ids,
            ..Local::default()
        };
        if let Some(at) = self.committed(actor) {
            d.set(self.now, at, false);
        }
    }

    async fn delivery_workload(&mut self, d: &mut Deliveries) {
        let actor = 0;
        // Crashes and cuts are aimed here, also inside a job's waits, rather
        // than `random_faults`: that also delays upload completions across
        // ownerships, which the replication workloads above judge.
        for _ in 0..300 {
            if self.interrupt.is_none() && self.rng.chance(3) {
                let at = self.now + self.rng.below(6);
                let crash = self.rng.below(2) == 0;
                self.interrupt = Some((at, actor, crash));
            }
            if self.actors[actor].hold.is_none() && self.rng.chance(30) {
                self.acquire(actor);
            }
            self.new_local(actor, d);
            if self.actors[actor].hold.is_none() {
                self.advance(1).await;
                continue;
            }
            // Arrivals: bursts of fetches (a busy room), and some moves.
            match self.rng.below(10) {
                0..=4 => {
                    for _ in 0..1 + self.rng.below(4) {
                        d.local.queue.push_back(Job::Fetch);
                    }
                }
                5 => {
                    let alarm = match self.rng.below(4) {
                        0 => None,
                        n => Some(n * 1000),
                    };
                    d.local.queue.push_back(Job::Move(alarm));
                }
                // A failed segment (its retire request dropped, an entry
                // left), or a failed entry PUT. Not stalled: a long stall
                // is the replication workloads' (its loss bound is judged
                // for their event pattern, not this mailbox's).
                6 => {
                    if self.rng.below(2) == 0 {
                        self.actors[actor].next_fail = true;
                    } else {
                        self.actors[actor].next_entry_fail = true;
                    }
                }
                _ => {}
            }
            self.timer_tick(actor, d);
            if let Some(job) = d.local.queue.pop_front() {
                let generation = d.local.hold;
                self.run_job(actor, d, job).await;
                if self.hold_generation(actor) != Some(generation) {
                    continue;
                }
            } else {
                self.advance(1).await;
            }
            self.timer_tick(actor, d);
        }
        self.interrupt = None;
    }

    /// Faults off, everything settled: each entry at a time no state names
    /// is delivered from the index, and none may be left after.
    async fn final_sweep(&mut self, d: &mut Deliveries) {
        let actor = 0;
        if self.actors[actor].hold.is_none() {
            self.acquire(actor);
        }
        self.new_local(actor, d);
        self.flush(actor).await;
        self.advance(100).await;
        if self.hold_generation(actor) != Some(d.local.hold) || !self.idle(actor) {
            return;
        }
        let named = self.named(actor);
        let times: BTreeSet<u64> = self.actors[actor]
            .entries
            .iter()
            .map(|e| e.at)
            .filter(|at| Some(*at) != named)
            .collect();
        for hint in times {
            if self.committed(actor) == Some(hint) {
                continue;
            }
            // The sweep reaches a quiet actor: no registration of its own
            // holds the entry back, and the round is not due.
            let as_swept = d.rule != DeliveryRule::BeltOnSweep;
            if alarm_index::stale_clear_skippable(hint, as_swept, d.local.delivered) {
                d.counts.belt_skips += 1;
                continue;
            }
            let cleared = self.clear_hint(actor, hint).await;
            d.counts.swept_clears += 1;
            d.counts.orphans_cleared += cleared;
        }
        self.flush(actor).await;
        self.advance(100).await;
        let named = self.named(actor);
        if self.hold_generation(actor) == Some(d.local.hold)
            && self.actors[actor]
                .entries
                .iter()
                .any(|e| Some(e.at) != named)
        {
            self.outcome.violations.insert(Violation::OrphanLeft);
        }
    }
}

/// The delivery model under `rule`, seeded.
pub(crate) fn simulate_deliveries(
    seed: u64,
    rule: DeliveryRule,
) -> (BTreeSet<Violation>, DeliveryCounts) {
    Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(async {
        let mut w = World::new(seed ^ 0xDE11_7E2E, Switch::Safe, true);
        for a in 0..ACTORS {
            w.event(a, Some(1000)).await;
            w.flush(a).await;
        }
        let mut d = Deliveries {
            rule,
            local: Local::default(),
            counts: DeliveryCounts::default(),
        };
        w.delivery_workload(&mut d).await;
        w.final_sweep(&mut d).await;
        for a in 0..ACTORS {
            w.drop_hold(a, false);
        }
        w.advance(100).await;
        w.check();
        (w.outcome.violations, d.counts)
    })
}

pub(crate) fn deliveries_safe() {
    let mut totals = DeliveryCounts::default();
    let only = std::env::var("WASMKEEP_SIM_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let range = only.map_or(0..seeds(), |seed| seed..seed + 1);
    let count = range.end - range.start;
    for seed in range {
        let (violations, counts) = simulate_deliveries(seed, DeliveryRule::Safe);
        assert!(
            violations.is_empty(),
            "seed {seed}: {violations:?}; {counts:?}"
        );
        totals.add(&counts);
    }
    for (name, value) in [
        ("rounds", totals.rounds),
        ("covered syncs", totals.covered),
        ("delivered again", totals.delivered_again),
        ("dropped", totals.dropped),
        ("refused", totals.refused),
        ("deferred", totals.deferred),
        ("moves in flight", totals.moves_in_flight),
        ("stale clears", totals.stale_clears),
        ("swept clears", totals.swept_clears),
        ("holds", totals.holds),
    ] {
        assert!(value >= count, "{name}: {value} < {count}; {totals:?}");
    }
    assert!(totals.orphans_cleared > 0, "{totals:?}");
    eprintln!("delivery model: {count} seeds: {totals:?}");
}

pub(crate) fn deliveries_caught(rule: DeliveryRule, expected: Violation) {
    let seed = (0..seeds())
        .find(|&seed| simulate_deliveries(seed, rule).0.contains(&expected))
        .unwrap_or_else(|| panic!("{rule:?}: no {expected:?} within {} seeds", seeds()));
    assert_eq!(
        simulate_deliveries(seed, rule).0,
        simulate_deliveries(seed, rule).0,
        "seed {seed} did not reproduce"
    );
    eprintln!("caught: {rule:?} seed={seed} expected={expected:?}");
}
