//! The idle pass ([ADR 0042](../../../../docs/adr/0042-compacting-an-idle-actors-history.md),
//! section 3): one retention window after a hold that wrote released an
//! actor, the node that released it takes the record again, only if it is
//! still the one that release wrote, and deletes the history the idle
//! snapshot made unnecessary.
//!
//! A release that lands remembers the actor here
//! ([`Cluster::note_idle_release`]): its record key, the generation
//! released, the version the release wrote, and when it is due (release +
//! retention window + clock skew bound). The queue is in memory, keyed by
//! actor, a later release replacing an earlier one; a release that did not
//! write, or any acquisition here, forgets the actor. It is bounded
//! ([`QUEUE_MAX`]): a full queue refuses a new actor, counted in
//! `wasmkeep_idle_passes_dropped_total`. A loop takes what is due as soon as
//! one of its [`PASSES_AT_ONCE`] slots is free, each a job in the actor's
//! mailbox that runs no guest code ([`ActorState::idle_pass`]):
//!
//! 1. skipped if the actor is resident, held, parked or has a socket here,
//!    if the node drains or moves it, if its class has an idle limit or is
//!    not deployed, if the record is not the remembered version (one read),
//!    or if the app is deleted, restoring or of another incarnation (its
//!    markers read). A node behind the deployed version, an app gate not yet
//!    checked, a bucket that did not answer and a node at its resident
//!    limit put the entry back a few seconds later, a few times
//!    ([`RETRY_AFTER`], [`RETRIES`]);
//! 2. the record acquired with `Owners::acquire_unchanged` on the
//!    remembered version (a wake since, an expiry, a move, an app delete
//!    or a whole-app restore changed it: dropped);
//! 3. the released generation `g` read from the new hold (its generation
//!    less one), and the app's markers read again: a recreated or restored
//!    app, or one deleted or restoring, compacts nothing (the incarnation
//!    check, below);
//! 4. one listing, and the deletes of
//!    [`idle_collectable`](wasmkeep_cluster::replication::idle_collectable)`(listing, g)`
//!    older than the window, in batches, the new hold's lease checked
//!    before each ([`Collector::collect_idle`](crate::replica::Collector::collect_idle)),
//!    and the pass's stop too: past [`PASS_DEADLINE`], an event waiting in
//!    the mailbox behind the job, or a drain begun, it deletes no more;
//! 5. the release, which keeps the requirement the hold inherited (it is
//!    never verified) and the idle origin. Then the job's empty state is
//!    let go, unless an event that came after it holds the actor.
//!
//! Nothing is restored, activated or uploaded.
//!
//! **A pass stopped at its deadline continues.** What it did not delete
//! is queued again ([`CONTINUE_AFTER`] later) as a continuation: the
//! entry the pass's own release leaves, with the version that release
//! wrote and its generation, and the writer's generation `g` the first
//! pass compacted for. The continuation acquires that record unchanged
//! (any acquisition since, a write above all, changed it: `changed`,
//! dropped), so every hold after `g` was one of this node's passes, which
//! upload nothing, and the snapshot of `g` still has no other uploader.
//! It deletes for `g` with the same rule, window and bounds. A chain ends
//! when a pass finishes (`done`), after [`FRUITLESS_MAX`] passes in a row
//! that deleted nothing, or after [`CONTINUATIONS_MAX`] passes (`stopped`).
//! A pass stopped by a waiting event, a drain or its hold's end is not
//! continued (`stopped`): the event's hold or the drain decides what comes
//! next. One entry per actor and the spacing keep a giant history to one
//! of the node's slots at a time, behind every entry due before it.
//!
//! A pass whose release does not land within a second leaves its
//! continuation armed ([`Queue::detach`]): the release, retried in the
//! background, queues it when it lands with its version, and the pass is
//! counted then (`continued`). A release that comes back `Superseded`, in
//! the pass or in the background, reads the record back once: naming
//! nobody at the hold's generation, it is that release, landed with its
//! answer lost, and the chain continues on the version read
//! ([`Cluster::idle_release_superseded`]). Any other record, a read with
//! no answer, a lapse of this node's lease, or any acquisition of the
//! actor here clears it (`stopped`), so no armed entry outlives the
//! record its release would have written.
//!
//! For a first pass the entry is only a hint of when to try: what it
//! deletes rests on the record and the listing it reads itself. A
//! continuation's `g` comes from this node's memory of its own pass; the
//! record it acquires unchanged is what proves no other hold came since.
//!
//! **The incarnation.** After `app delete` and `deploy --recreate`,
//! generations start again at 1, and on S3 a version 1 released record at
//! the same generation has the same bytes, so the same `ETag`: the version
//! check alone cannot tell the recreated app's record from the remembered
//! one. The pass therefore compares the app's epoch (the `recreated` and
//! `restored` markers, as the blob sweep does, ADR 0030) with the one this
//! runtime started with, before its acquisition (so nothing is written)
//! and again under its hold (what the deletes rest on), and compacts
//! nothing on a difference. A node that sees the app's tombstone also
//! drops its whole queue.

use std::collections::{BTreeSet, HashMap};

use object_store::ObjectStoreExt as _;

use super::*;
use crate::cluster::{LeaseReturn, RECORD_CHANGED};
use crate::replica::{Collector, IdleCollected};
use wasmkeep_cluster::lease::Version;
use wasmkeep_cluster::node_lease::Hold;
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_cluster::state_root::Family;

/// Entries the queue keeps at most; then a new actor is refused.
pub(crate) const QUEUE_MAX: usize = 100_000;

/// Passes a node runs at once (as idle expiry's
/// [`PARALLEL`](wasmkeep_cluster::expiry::PARALLEL)).
pub(crate) const PASSES_AT_ONCE: usize = 4;

/// How long the loop waits for one pass's job (the events queued before
/// it, an acquisition, a listing, the deletes and a release). Its slot is
/// kept until the job ends, whatever this wait.
const PASS_WAIT: Duration = Duration::from_secs(60);

/// How long a pass deletes, from its acquisition: then it stops, and what
/// is left is continued by another pass ([`CONTINUE_AFTER`] later).
const PASS_DEADLINE: Duration = Duration::from_secs(5);

/// When a pass stopped at its deadline is continued: the same spacing
/// for every continuation, so a history of any length keeps one slot
/// at most about half the time, behind the entries due before it.
pub(crate) const CONTINUE_AFTER: Duration = Duration::from_secs(5);

/// Passes of a chain in a row that deleted nothing (a listing that alone
/// takes the deadline, say) after which it is given up.
pub(crate) const FRUITLESS_MAX: u32 = 3;

/// Passes of one chain at most (about 100,000 objects at one batch a
/// pass); then what is left waits for the actor's next write and rest.
pub(crate) const CONTINUATIONS_MAX: u32 = 1_000;

/// Attempts at a pass's listing and deletes inside its hold, with doubling
/// waits from one second, while the hold is valid and the pass not stopped.
const PASS_ATTEMPTS: u32 = 3;

/// When a pass that met a passing obstacle is tried again.
pub(crate) const RETRY_AFTER: Duration = Duration::from_secs(5);

/// How many times an entry is put back for a passing obstacle.
pub(crate) const RETRIES: u32 = 3;

/// The longest the loop sleeps between looks at the queue.
const TICK_MAX: Duration = Duration::from_secs(5);

/// One actor to compact: what its release left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    /// The ownership record's key.
    pub(crate) key: String,
    /// The generation the release kept: the writer's, or for a
    /// continuation the previous pass's hold.
    pub(crate) generation: u64,
    /// The generation whose snapshot the pass compacts from: the writer's
    /// (`generation` itself, except for a continuation).
    pub(crate) snapshot_generation: u64,
    /// The version the release wrote.
    pub(crate) version: Version,
    /// When the release landed (Unix ms): the idle origin the pass's
    /// acquisition gives a version 2 record that carries none.
    pub(crate) released_ms: u64,
    /// When the pass is due (Unix ms).
    pub(crate) due_ms: u64,
    /// Times it was put back for a passing obstacle.
    pub(crate) attempts: u32,
    /// Passes of this chain before this one (0: the first pass).
    pub(crate) continued: u32,
    /// Passes of this chain in a row, before this one, that stopped at
    /// the deadline having deleted nothing.
    pub(crate) fruitless: u32,
}

/// A continuation armed by a pass for its own release
/// ([`Queue::arm`], [`Queue::continue_from`]).
#[derive(Debug)]
enum Continuing {
    /// The pass's hold's generation, and its next entry (version,
    /// generation and due time filled in when the release lands).
    /// `detached`: the pass's release came back pending and the pass
    /// ended without counting itself; whatever ends this entry (its
    /// release landing in the background, or its clearing) counts it.
    Armed {
        hold_generation: u64,
        next: Entry,
        detached: bool,
    },
    /// The release landed: whether the queue took the next entry.
    Queued(bool),
}

/// Counts a pass whose release was retried in the background, once its
/// continuation was queued (`continued`) or could not be (`stopped`).
fn count_detached(key: &str, continued: bool) {
    if continued {
        tracing::debug!(key = %crate::logging::text(key), "idle pass: its release landed in the background, continued later");
    } else {
        tracing::debug!(key = %crate::logging::text(key), "idle pass: its release, retried in the background, did not land with its version: not continued");
    }
    crate::metrics::idle_compaction(if continued { "continued" } else { "stopped" });
}

/// The idle passes due on this node, by record key.
#[derive(Debug, Default)]
pub(crate) struct Queue {
    inner: std::sync::Mutex<QueueInner>,
    /// Tests: while set, the loop takes nothing.
    paused: std::sync::atomic::AtomicBool,
    /// Wakes the loop: an entry added, a pass's slot given back.
    changed: tokio::sync::Notify,
    /// The continuations armed by passes now releasing, by record key.
    continuing: std::sync::Mutex<HashMap<String, Continuing>>,
    /// Tests: a pass's deletes in batches of `.0`, and its deadline after
    /// `.1` batches.
    limit: std::sync::Mutex<Option<(usize, usize)>>,
}

#[derive(Debug, Default)]
struct QueueInner {
    by_key: HashMap<String, Entry>,
    by_due: BTreeSet<(u64, String)>,
}

impl QueueInner {
    fn remove(&mut self, key: &str) -> Option<Entry> {
        let old = self.by_key.remove(key)?;
        self.by_due.remove(&(old.due_ms, old.key.clone()));
        Some(old)
    }
}

impl Queue {
    fn lock(&self) -> std::sync::MutexGuard<'_, QueueInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Adds `entry`, replacing one of the same key. A full queue refuses
    /// a new actor (`false`, counted): the entries already there are
    /// older and closer to running.
    pub(crate) fn push(&self, entry: Entry) -> bool {
        {
            let mut q = self.lock();
            let replaced = q.remove(&entry.key).is_some();
            if !replaced && q.by_key.len() >= QUEUE_MAX {
                drop(q);
                crate::metrics::inc(&crate::metrics::METRICS.idle_passes_dropped);
                return false;
            }
            q.by_due.insert((entry.due_ms, entry.key.clone()));
            q.by_key.insert(entry.key.clone(), entry);
        }
        self.changed.notify_one();
        true
    }

    /// Forgets `key`'s entry, if any: the actor was acquired here, or
    /// released by a hold that wrote nothing, so the version remembered is
    /// gone.
    pub(crate) fn remove(&self, key: &str) {
        self.lock().remove(key);
    }

    /// Takes up to `most` entries due at `now_ms`, due first.
    pub(crate) fn take_due(&self, now_ms: u64, most: usize) -> Vec<Entry> {
        let mut q = self.lock();
        let mut taken = Vec::new();
        while taken.len() < most {
            let Some((due, _)) = q.by_due.first() else {
                break;
            };
            if *due > now_ms {
                break;
            }
            let Some((_, key)) = q.by_due.pop_first() else {
                break;
            };
            if let Some(entry) = q.by_key.remove(&key) {
                taken.push(entry);
            }
        }
        taken
    }

    /// When the next entry is due (Unix ms), if any.
    fn next_due(&self) -> Option<u64> {
        self.lock().by_due.first().map(|(due, _)| *due)
    }

    /// Forgets every entry (the app's tombstone was seen, or compaction is
    /// off).
    pub(crate) fn clear(&self) {
        {
            let mut q = self.lock();
            q.by_key.clear();
            q.by_due.clear();
        }
        self.end_all_armed();
    }

    /// An acquisition of `key` here: the version any entry of it
    /// remembered is gone, and so is the record any pass's release still
    /// retried in the background would have written (ADR 0042).
    pub(crate) fn acquired(&self, key: &str) {
        // Cleared first: a background release landing in between then
        // finds nothing armed, or has queued an entry `remove` forgets.
        self.end_armed(key, None);
        self.remove(key);
    }

    /// How many entries wait (tests).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().by_key.len()
    }

    /// The entries waiting, due first (tests).
    pub(crate) fn entries(&self) -> Vec<Entry> {
        let q = self.lock();
        q.by_due
            .iter()
            .filter_map(|(_, k)| q.by_key.get(k).cloned())
            .collect()
    }

    /// Tests: stops (or lets again) the loop taking entries.
    pub(crate) fn pause(&self, paused: bool) {
        self.paused
            .store(paused, std::sync::atomic::Ordering::SeqCst);
        self.changed.notify_one();
    }

    fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A pass of `next.key` under a hold of `hold_generation` stopped at
    /// its deadline: its release, if it lands with its version, queues
    /// `next`.
    pub(crate) fn arm(&self, hold_generation: u64, next: Entry) {
        self.continuing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                next.key.clone(),
                Continuing::Armed {
                    hold_generation,
                    next,
                    detached: false,
                },
            );
    }

    fn continuing(&self) -> std::sync::MutexGuard<'_, HashMap<String, Continuing>> {
        self.continuing.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A release of `hold` that wrote nothing landed, with `version` (its
    /// own conditional write's answer): if it is the hold of a pass that
    /// armed a continuation, that entry is queued, on this version and
    /// generation. A pass whose release was retried in the background is
    /// counted here: `Some(queued)`.
    fn continue_from(&self, hold: &Hold, version: Version, now_ms: u64) -> Option<bool> {
        self.continue_at(&hold.key, hold.generation, version, now_ms)
    }

    /// [`Self::continue_from`] for the hold of `key` at `generation`.
    fn continue_at(
        &self,
        key: &str,
        generation: u64,
        version: Version,
        now_ms: u64,
    ) -> Option<bool> {
        let mut continuing = self.continuing();
        let Some(Continuing::Armed {
            hold_generation,
            next,
            detached,
        }) = continuing.get(key)
        else {
            return None;
        };
        if *hold_generation != generation {
            return None;
        }
        let detached = *detached;
        let mut next = next.clone();
        next.generation = generation;
        next.version = version;
        next.due_ms = now_ms.saturating_add(u64::try_from(CONTINUE_AFTER.as_millis()).unwrap_or(0));
        let queued = self.push(next);
        if detached {
            continuing.remove(key);
            drop(continuing);
            count_detached(key, queued);
            return Some(queued);
        }
        continuing.insert(key.to_owned(), Continuing::Queued(queued));
        None
    }

    /// Whether a continuation is armed for the hold of `key` at
    /// `generation`.
    pub(crate) fn armed_for(&self, key: &str, generation: u64) -> bool {
        matches!(
            self.continuing().get(key),
            Some(Continuing::Armed { hold_generation, .. }) if *hold_generation == generation
        )
    }

    /// Ends `key`'s continuation once its pass released (or its release
    /// was superseded): whether the next pass is queued.
    fn disarm(&self, key: &str) -> bool {
        matches!(
            self.continuing().remove(key),
            Some(Continuing::Queued(true))
        )
    }

    /// The release of the pass under a hold of `hold_generation` came back
    /// pending: retried in the background. If its release already landed
    /// there (or the entry was cleared), whether the next pass is queued,
    /// as [`Self::disarm`]. Otherwise `None`: the entry stays armed, for
    /// the background release to queue when it lands with its version
    /// ([`Self::continue_from`]), or for a superseded release, a lapse or
    /// an acquisition of the key to clear ([`Self::end_armed`]); either
    /// counts the pass.
    pub(crate) fn detach(&self, key: &str, hold_generation: u64) -> Option<bool> {
        let mut continuing = self.continuing();
        match continuing.get_mut(key) {
            Some(Continuing::Armed {
                hold_generation: armed,
                detached,
                ..
            }) if *armed == hold_generation => {
                *detached = true;
                None
            }
            Some(Continuing::Queued(queued)) => {
                let queued = *queued;
                continuing.remove(key);
                Some(queued)
            }
            Some(Continuing::Armed { .. }) | None => Some(false),
        }
    }

    /// Clears `key`'s armed continuation (of the hold of `generation`, or
    /// any): its release was superseded, the release wrote no version, or
    /// the key was acquired here. A continuation already queued is left
    /// to the queue. Whether a pass detached from it was counted
    /// (`stopped`).
    pub(crate) fn end_armed(&self, key: &str, generation: Option<u64>) -> bool {
        let mut continuing = self.continuing();
        let Some(Continuing::Armed {
            hold_generation,
            detached,
            ..
        }) = continuing.get(key)
        else {
            return false;
        };
        if generation.is_some_and(|g| g != *hold_generation) {
            return false;
        }
        let detached = *detached;
        continuing.remove(key);
        drop(continuing);
        if detached {
            count_detached(key, false);
        }
        detached
    }

    /// Clears every armed continuation: this node's lease lapsed or ended,
    /// or the queue was cleared. How many passes detached from them were
    /// counted (`stopped`).
    pub(crate) fn end_all_armed(&self) -> usize {
        let mut ended = Vec::new();
        self.continuing().retain(|key, c| match c {
            Continuing::Armed { detached, .. } => {
                if *detached {
                    ended.push(key.clone());
                }
                false
            }
            Continuing::Queued(_) => true,
        });
        for key in &ended {
            count_detached(key, false);
        }
        ended.len()
    }

    /// Whether `key` has an armed continuation (tests).
    #[cfg(test)]
    pub(crate) fn armed(&self, key: &str) -> bool {
        matches!(self.continuing().get(key), Some(Continuing::Armed { .. }))
    }

    /// Tests: deletes in batches of `.0`, and each pass's deadline after
    /// `.1` batches (`None`: as built).
    pub(crate) fn limit(&self, limit: Option<(usize, usize)>) {
        *self.limit.lock().unwrap_or_else(|p| p.into_inner()) = limit;
    }

    fn limits(&self) -> Option<(usize, usize)> {
        *self.limit.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Whether a hold that ended wrote history: its acknowledged head is a
/// position of its own generation (only a segment it uploaded puts one
/// there). A hold that only read, and the idle pass's own hold, did not:
/// their release queues nothing, except the continuation a pass stopped
/// at its deadline arms for its own release ([`Queue::arm`]).
pub(crate) fn wrote(hold: &Hold) -> bool {
    matches!(
        hold.head.acknowledged,
        Some(Position::Known { generation, .. }) if generation == hold.generation
    )
}

/// Why the app's markers stop a pass ([`Cluster::fresh_app_epoch`]).
enum Markers {
    /// Deleted or restoring: the entry is dropped.
    Gone(String),
    /// Not read (the bucket did not answer): tried again later.
    Unread(String),
}

impl Cluster {
    /// A release of `hold` landed with `written` its answer's version.
    /// With idle compaction on, a hold that wrote is remembered for its
    /// idle pass; one that did not write makes this node forget any entry
    /// of the actor (the version it remembered is gone). A release whose
    /// answer was lost (it comes back `Superseded`) never reaches here:
    /// that actor is not queued. A pass's own release (it wrote nothing)
    /// queues the continuation that pass armed, if any, whether the
    /// release landed in the pass or later in the background; one that
    /// cannot (no version, compaction off, the app refused) clears it.
    pub(crate) fn note_idle_release(&self, hold: &Hold, written: Option<Version>) {
        let Some(version) = written else {
            self.idle_passes.end_armed(&hold.key, Some(hold.generation));
            return;
        };
        if !wrote(hold) {
            self.idle_passes.remove(&hold.key);
            if self.idle_compaction && self.app_refusal().is_none() {
                self.idle_passes.continue_from(hold, version, now_ms());
            } else {
                self.idle_passes.end_armed(&hold.key, Some(hold.generation));
            }
            return;
        }
        if !self.idle_compaction || self.app_refusal().is_some() {
            return;
        }
        let now = now_ms();
        let wait = u64::try_from(self.history_retention.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(self.lease_config.max_skew_ms());
        self.idle_passes.push(Entry {
            key: hold.key.clone(),
            generation: hold.generation,
            snapshot_generation: hold.generation,
            version,
            released_ms: now,
            due_ms: now.saturating_add(wait),
            attempts: 0,
            continued: 0,
            fruitless: 0,
        });
    }

    /// A release of the hold of `key` at `generation` came back
    /// `Superseded` (in the pass, or retried in the background) while an
    /// idle pass's continuation is armed for that hold: the record is read
    /// back once. Naming nobody at `generation`, it is that release, landed
    /// with its answer lost (ADR 0042, "As built: a release retried in the
    /// background"): the continuation is queued on the version read, as a
    /// release that landed would have. Anything else, or no answer, clears
    /// it. Nothing armed for that hold: nothing read.
    pub(crate) async fn idle_release_superseded(&self, key: &str, generation: u64) {
        if !self.idle_passes.armed_for(key, generation) {
            return;
        }
        let read =
            tokio::time::timeout(self.lease_attempt, self.owners.released_at(key, generation))
                .await;
        match read {
            Ok(Ok(Some(version))) if self.idle_compaction && self.app_refusal().is_none() => {
                tracing::debug!(key = %crate::logging::text(key), generation, "idle pass: its release landed with its answer lost; continued on the record read back");
                // As `note_idle_release` for a release that did not write.
                self.idle_passes.remove(key);
                self.idle_passes
                    .continue_at(key, generation, version, now_ms());
            }
            _ => {
                self.idle_passes.end_armed(key, Some(generation));
            }
        }
    }

    /// The app's epoch as its markers give it now, read again from the
    /// bucket (ADR 0030, section 2: `recreated` and `restored`), or why the
    /// pass compacts nothing: the app has a tombstone or is restoring, or
    /// the bucket did not answer.
    async fn fresh_app_epoch(&self) -> Result<String, Markers> {
        let root = self.state_root();
        let mut bodies = Vec::new();
        for marker in ["deleted", "restoring", "recreated", "restored"] {
            let path = object_store::path::Path::from(format!(
                "{}apps/{}/{}/{marker}",
                root.prefix(),
                root.tenant(),
                root.app()
            ));
            let read = tokio::time::timeout(self.lease_attempt, async {
                match self.store.get(&path).await {
                    Ok(got) => got.bytes().await.map(|b| Some(b.to_vec())),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await;
            let body: Option<Vec<u8>> = match read {
                Ok(Ok(body)) => body,
                Ok(Err(e)) => {
                    return Err(Markers::Unread(format!(
                        "reading the app's {marker} marker: {e}"
                    )));
                }
                Err(_) => {
                    return Err(Markers::Unread(format!(
                        "reading the app's {marker} marker: no answer"
                    )));
                }
            };
            if body.is_some() && matches!(marker, "deleted" | "restoring") {
                return Err(Markers::Gone(format!(
                    "the app is {}",
                    if marker == "deleted" {
                        "deleted"
                    } else {
                        "restoring"
                    }
                )));
            }
            bodies.push(body);
        }
        Ok(crate::blobs::epoch(
            bodies[2].as_deref(),
            bodies[3].as_deref(),
        ))
    }

    /// Whether this node holds `address` for an idle pass now (such a hold
    /// is not the actor in use: placement does not count it).
    pub(crate) fn idle_pass_holds(&self, address: &ActorAddress) -> bool {
        let key = self.owner_key(address);
        self.idle_pass_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&key)
    }

    /// Whether this node holds `address` for its events: [`Self::holds_here`]
    /// less an idle pass's hold. What placement asks of a companion.
    pub(crate) fn holds_for_events(&self, address: &ActorAddress) -> bool {
        self.holds_here(address) && !self.idle_pass_holds(address)
    }
}

/// The key of a hold an idle pass has, registered while it lasts.
struct PassHold<'a> {
    cluster: &'a Cluster,
    key: String,
}

impl<'a> PassHold<'a> {
    fn new(cluster: &'a Cluster, key: String) -> Self {
        cluster
            .idle_pass_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone());
        Self { cluster, key }
    }
}

impl Drop for PassHold<'_> {
    fn drop(&mut self) {
        self.cluster
            .idle_pass_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.key);
    }
}

/// Why a pass stopped before deleting all it could.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// Its deadline ([`PASS_DEADLINE`]): continued, within the bounds.
    Deadline,
    /// An event waits behind its job: that event's hold comes next.
    Waiting,
    /// The node drains.
    Drain,
    /// Its hold's lease ended.
    Lease,
}

impl Stop {
    fn text(self) -> &'static str {
        match self {
            Self::Deadline => "its deadline",
            Self::Waiting => "an event waiting",
            Self::Drain => "a drain",
            Self::Lease => "its hold ending",
        }
    }
}

/// What an idle pass did, a label of `wasmkeep_idle_compactions_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Compacted {
    /// Compacted: `deleted` objects gone (0 if the history was already
    /// as compact as the window allows), nothing old enough left.
    Done { deleted: usize },
    /// Stopped at its deadline with `left` objects old enough still to
    /// delete, and the next pass queued (on its own release's record,
    /// [`CONTINUE_AFTER`] later).
    Continued { deleted: usize, left: usize },
    /// Stopped at its deadline with `left` objects old enough still to
    /// delete, its continuation armed, and its release left to the
    /// background: not counted here. When that release lands with its
    /// version it queues the next pass and counts this one `continued`;
    /// superseded, or cleared first by a lapse or an acquisition of the
    /// key here, it counts `stopped` ([`Queue::detach`]).
    Deferred { deleted: usize, left: usize },
    /// Stopped with `left` objects old enough still to delete, and not
    /// continued: stopped by an event, a drain or its hold's end, the
    /// chain's bounds reached, or the continuation not queued.
    Stopped {
        deleted: usize,
        left: usize,
        why: String,
    },
    /// The record is not the one the release wrote: dropped.
    Changed,
    /// No snapshot of the released generation with its segment.
    Nothing,
    /// Not compacted, and why.
    Refused(String),
    /// A passing obstacle, and why: the entry is put back a few seconds
    /// later, a few times, then counted refused.
    Retry(String),
    /// The actor was in use on this node.
    Busy,
}

impl Compacted {
    /// Its label, `None` for a pass counted later ([`Self::Deferred`]).
    fn label(&self) -> Option<&'static str> {
        Some(match self {
            Self::Deferred { .. } => return None,
            Self::Done { .. } => "done",
            Self::Continued { .. } => "continued",
            Self::Stopped { .. } => "stopped",
            Self::Changed => "changed",
            Self::Nothing => "nothing",
            Self::Refused(_) | Self::Retry(_) => "refused",
            Self::Busy => "busy",
        })
    }
}

/// Whether an event waits behind the pass's job, asked from inside it.
type Waiting = Arc<dyn Fn() -> bool + Send + Sync>;

impl ActorState {
    /// The idle pass's job (ADR 0042, section 3): acquire the actor only
    /// if its record is still the version `entry` remembers, check the
    /// app's incarnation against `app_epoch`, compact, release. Runs no
    /// guest code, restores nothing, uploads nothing; the hold it takes is
    /// never this state's (`self.held` stays empty), so no event runs
    /// under it, and an event queued behind the job finds the actor
    /// released and acquires as usual. `waiting` says whether one is
    /// queued: the deletes then stop.
    pub(super) async fn idle_pass(
        &mut self,
        entry: Entry,
        app_epoch: String,
        waiting: Waiting,
    ) -> Compacted {
        let Some(cluster) = self.cluster.clone() else {
            return Compacted::Refused("not in a cluster".into());
        };
        // Held or running here: in use. Never taken as the pass's hold.
        if self.held.is_some() || self.live.is_some() {
            return Compacted::Busy;
        }
        if cluster.is_parked(&self.address) || self.sockets.reserved_by(&self.address) > 0 {
            return Compacted::Busy;
        }
        if self.moving_target().is_some() || cluster.draining() {
            return Compacted::Refused("this node is draining or moving the actor".into());
        }
        self.owner_hint = None;
        // An app deleted, restoring or of another incarnation: skipped
        // before anything is written (section 3, step 1). Checked again
        // under the hold, which is what the deletes rest on.
        if let Some(refused) = other_incarnation(&cluster, &app_epoch).await {
            return refused;
        }
        let hold = match LeaseHold::acquire_with(
            &cluster,
            &self.address,
            Some((&entry.version.0, entry.released_ms)),
        )
        .await
        {
            Ok(hold) => hold,
            Err(OwnershipError::Refused(why)) if why == RECORD_CHANGED => {
                return Compacted::Changed;
            }
            Err(OwnershipError::NotOwner { .. }) => return Compacted::Changed,
            Err(OwnershipError::Unavailable(e)) => return Compacted::Retry(e),
            Err(OwnershipError::Refused(e) | OwnershipError::Broken(e)) => {
                return Compacted::Refused(e);
            }
            Err(OwnershipError::Elsewhere { .. }) => {
                return Compacted::Refused("this node is draining or moving the actor".into());
            }
        };
        let registered = PassHold::new(&cluster, cluster.owner_key(&self.address));
        let (outcome, stop) =
            compact_under(&cluster, &self.address, &hold, &entry, &app_epoch, waiting).await;
        drop(registered);
        // Stopped at its deadline: the rest is for a continuation, which
        // this hold's release queues if it lands with its version.
        let mut bound = None;
        let mut armed = None;
        if let (Compacted::Stopped { deleted, .. }, Some(Stop::Deadline), Some(generation)) =
            (&outcome, stop, hold.generation())
        {
            let fruitless = if *deleted == 0 {
                entry.fruitless + 1
            } else {
                0
            };
            if fruitless >= FRUITLESS_MAX {
                bound = Some("passes in a row deleted nothing");
            } else if entry.continued + 1 >= CONTINUATIONS_MAX {
                bound = Some("the chain's last pass");
            } else {
                cluster.idle_passes.arm(
                    generation,
                    Entry {
                        continued: entry.continued + 1,
                        fruitless,
                        attempts: 0,
                        ..entry.clone()
                    },
                );
                armed = Some(generation);
            }
        }
        // Unverified: it releases with the requirement it inherited, and
        // with the idle origin its acquisition carried.
        let returned = hold.release().await;
        let key = cluster.owner_key(&self.address);
        // Superseded in the pass: a try cut off at its bound may have
        // landed, its answer lost, and a later try conflicted with it.
        if let (LeaseReturn::Superseded, Some(generation)) = (returned, armed) {
            cluster.idle_release_superseded(&key, generation).await;
        }
        // Left to the background: the continuation stays armed for that
        // release to queue when it lands with its version, which counts
        // the pass then (unless it already landed, or was cleared).
        let queued = match (returned, armed) {
            (LeaseReturn::Pending, Some(generation)) => {
                cluster.idle_passes.detach(&key, generation)
            }
            _ => Some(cluster.idle_passes.disarm(&key)),
        };
        match outcome {
            Compacted::Stopped { deleted, left, .. } if queued.is_none() => {
                Compacted::Deferred { deleted, left }
            }
            Compacted::Stopped { deleted, left, .. } if queued == Some(true) => {
                Compacted::Continued { deleted, left }
            }
            Compacted::Stopped { deleted, left, why } => Compacted::Stopped {
                deleted,
                left,
                why: match (stop, bound) {
                    (_, Some(bound)) => format!("{why}; not continued: {bound}"),
                    (Some(Stop::Deadline), None) => format!(
                        "{why}; not continued: the release did not land with its version (superseded, \
                         a lapse, the actor acquired here), or the queue is full"
                    ),
                    _ => why,
                },
            },
            outcome => outcome,
        }
    }
}

/// Why the pass compacts nothing of the app now, if it does not: its
/// markers, read again, give another epoch than `app_epoch` (this
/// runtime's: the app was recreated or restored since), or it is deleted
/// or restoring (refused), or the bucket did not answer (tried again).
async fn other_incarnation(cluster: &Cluster, app_epoch: &str) -> Option<Compacted> {
    match cluster.fresh_app_epoch().await {
        Ok(fresh) if fresh == app_epoch => None,
        Ok(_) => Some(Compacted::Refused(
            "the app was recreated or restored since this node started".into(),
        )),
        Err(Markers::Gone(why)) => Some(Compacted::Refused(why)),
        Err(Markers::Unread(why)) => Some(Compacted::Retry(why)),
    }
}

/// The pass's work under its own hold `hold` of `address`, and why a
/// pass that stopped did.
async fn compact_under(
    cluster: &Arc<Cluster>,
    address: &ActorAddress,
    hold: &LeaseHold,
    entry: &Entry,
    app_epoch: &str,
    waiting: Waiting,
) -> (Compacted, Option<Stop>) {
    // The released generation, from the record this hold replaced
    // (every acquisition writes generation + 1). The entry's own must
    // agree: an equal version with another generation is another
    // record (another incarnation's, whose `ETag` is the same).
    let Some(generation) = hold.generation().and_then(|g| g.checked_sub(1)) else {
        return (
            Compacted::Refused("the hold ended before the pass began".into()),
            None,
        );
    };
    if generation != entry.generation {
        return (Compacted::Changed, None);
    }
    // The incarnation: this runtime's epoch, read again now that the
    // pass holds the actor. A recreated or restored app compacts
    // nothing of what this node remembered.
    if let Some(refused) = other_incarnation(cluster, app_epoch).await {
        return (refused, None);
    }
    let deadline = std::time::Instant::now() + PASS_DEADLINE;
    let limits = cluster.idle_passes.limits();
    let batch = limits.map_or(crate::replica::COLLECT_BATCH, |(batch, _)| batch);
    let stopping = Arc::clone(cluster);
    let reason = Arc::new(std::sync::Mutex::new(None::<Stop>));
    let said = Arc::clone(&reason);
    let batches = std::sync::atomic::AtomicUsize::new(0);
    // Asked before each batch: an event or a drain first (neither is
    // continued), then the deadline (or the tests' batch budget).
    let stop = move || {
        let why = if waiting() {
            Some(Stop::Waiting)
        } else if stopping.draining() {
            Some(Stop::Drain)
        } else if std::time::Instant::now() >= deadline
            || limits
                .is_some_and(|(_, most)| batches.load(std::sync::atomic::Ordering::SeqCst) >= most)
        {
            Some(Stop::Deadline)
        } else {
            batches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            None
        };
        if why.is_some() {
            *said.lock().unwrap_or_else(|p| p.into_inner()) = why;
        }
        why.is_some()
    };
    // The writer's generation: the released one for a first pass, the one
    // the chain began with for a continuation.
    let collector = Collector::idle(cluster, address, entry.snapshot_generation, hold.view());
    let mut wait = Duration::from_secs(1);
    for attempt in 1..=PASS_ATTEMPTS {
        match collector
            .collect_idle(cluster.history_retention, batch, &stop)
            .await
        {
            Ok(IdleCollected::NoSnapshot) => return (Compacted::Nothing, None),
            Ok(IdleCollected::Collected {
                deleted, left: 0, ..
            }) => {
                return (Compacted::Done { deleted }, None);
            }
            Ok(IdleCollected::Collected {
                deleted,
                left,
                stopped,
                ..
            }) => {
                let said = *reason.lock().unwrap_or_else(|p| p.into_inner());
                // Not stopped, and some left: the lease ended first.
                let why = said.filter(|_| stopped).unwrap_or(Stop::Lease);
                return (
                    Compacted::Stopped {
                        deleted,
                        left,
                        why: format!("stopped by {}", why.text()),
                    },
                    Some(why),
                );
            }
            Err(e) => {
                crate::metrics::inc(&crate::metrics::METRICS.history_collect_failures);
                // Failed at or past the deadline under a valid hold: the
                // chain goes on, as for a stop there (its deletes so far
                // are counted in `wasmkeep_history_collected_total`).
                if collector.lease_valid() && std::time::Instant::now() >= deadline {
                    return (
                        Compacted::Stopped {
                            deleted: 0,
                            left: 0,
                            why: format!("a bucket error at its deadline: {e}"),
                        },
                        Some(Stop::Deadline),
                    );
                }
                if attempt == PASS_ATTEMPTS || !collector.lease_valid() || stop() {
                    return (Compacted::Refused(e.to_string()), None);
                }
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
        }
    }
    (Compacted::Refused("no attempt left".into()), None)
}

/// The actor an ownership record key names, if it can be told (a name
/// shortened with a hash cannot: never compacted).
fn address_of(inner: &ServiceInner, cluster: &Cluster, key: &str) -> Option<ActorAddress> {
    let base = cluster.state_root().key(Family::Owners, "");
    let (class, encoded) = key.strip_prefix(&base)?.split_once('/')?;
    let name = wasmkeep_storage::decode_segment(encoded)?;
    ActorAddress::new(&inner.tenant, class, &name).ok()
}

/// One of the loop's [`PASSES_AT_ONCE`] slots, held until the pass's job
/// ends (not only the loop's wait for it); giving it back wakes the loop.
struct Slot {
    _permit: tokio::sync::OwnedSemaphorePermit,
    cluster: Arc<Cluster>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.cluster.idle_passes.changed.notify_one();
    }
}

/// Takes due entries whenever a slot is free and runs each one's pass.
/// `app_epoch` is the epoch this runtime started with. Wakes when an entry
/// is added, a slot is given back, or the next entry comes due (at most
/// every [`TICK_MAX`]). Runs until the service is dropped or stops.
pub(super) async fn compaction_loop(
    service: std::sync::Weak<ServiceInner>,
    cluster: Arc<Cluster>,
    app_epoch: String,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(PASSES_AT_ONCE));
    loop {
        {
            let Some(inner) = service.upgrade() else {
                return;
            };
            if inner.mailboxes.is_closed() {
                return;
            }
            if !cluster.idle_compaction {
                cluster.idle_passes.clear();
            } else if !cluster.idle_passes.is_paused() {
                while let Ok(permit) = Arc::clone(&slots).try_acquire_owned() {
                    let Some(entry) = cluster.idle_passes.take_due(now_ms(), 1).pop() else {
                        break;
                    };
                    let slot = Slot {
                        _permit: permit,
                        cluster: Arc::clone(&cluster),
                    };
                    let (inner, cluster, epoch) =
                        (Arc::clone(&inner), Arc::clone(&cluster), app_epoch.clone());
                    tokio::spawn(async move {
                        let outcome = pass_one(&inner, &cluster, entry.clone(), epoch, slot).await;
                        settle(&cluster, entry, outcome);
                    });
                }
            }
        }
        // Until something changes, or the next entry comes due.
        let wait = cluster
            .idle_passes
            .next_due()
            .map_or(TICK_MAX, |due| {
                Duration::from_millis(due.saturating_sub(now_ms()).max(10))
            })
            .min(TICK_MAX);
        tokio::select! {
            () = cluster.idle_passes.changed.notified() => {}
            () = tokio::time::sleep(wait) => {}
        }
    }
}

/// Counts a pass's outcome, or puts its entry back for a passing obstacle
/// while it has tries left.
fn settle(cluster: &Cluster, mut entry: Entry, outcome: Compacted) {
    if let Compacted::Retry(why) = &outcome
        && entry.attempts < RETRIES
    {
        tracing::debug!(key = %crate::logging::text(&entry.key), %why, "idle pass: tried again later");
        entry.attempts += 1;
        entry.due_ms = now_ms().saturating_add(u64::try_from(RETRY_AFTER.as_millis()).unwrap_or(0));
        cluster.idle_passes.push(entry);
        return;
    }
    if let Some(label) = outcome.label() {
        crate::metrics::idle_compaction(label);
    }
}

/// One entry: its job in the actor's mailbox, unless it is skipped first.
/// `slot` is given back when the job ends, or when it is not submitted.
async fn pass_one(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    entry: Entry,
    app_epoch: String,
    slot: Slot,
) -> Compacted {
    let Some(address) = address_of(inner, cluster, &entry.key) else {
        return Compacted::Refused("not an actor address that can be told back".into());
    };
    // ADR 0042, constraint 8: the pass rewrites the record twice, and an
    // idle expiry judges by the record's last-modified time. A version 1
    // record carries no idle origin; a version 2 one does, but the node
    // that collects reads it only for actors it refused or judged itself.
    // Either way the pass would restart the actor's idle clock: a class
    // with an idle limit is never compacted.
    if inner
        .expiry
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains_key(address.class())
    {
        return Compacted::Refused("the class has an idle limit".into());
    }
    if !read(&inner.classes).contains_key(address.class()) {
        return Compacted::Refused("the class is not deployed".into());
    }
    if cluster.draining() {
        return Compacted::Refused("this node is draining".into());
    }
    // As idle expiry: no pass under rules the node could not apply (a
    // deploy it is behind on), nor before the app's gate was checked;
    // both pass.
    if inner
        .deploy_behind
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Compacted::Retry("this node is behind the deployed version".into());
    }
    if cluster.app_unchecked() {
        return Compacted::Retry("the app's tombstone is not checked yet".into());
    }
    if let Some(why) = cluster.app_refusal() {
        return Compacted::Refused(why.into());
    }
    if inner.mailboxes.contains(&address)
        || cluster.holds_here(&address)
        || cluster.is_parked(&address)
        || inner.sockets.has_open(&address)
    {
        return Compacted::Busy;
    }
    // A stale entry costs one read, not a resident slot.
    match current_version(cluster, &entry.key).await {
        Ok(Some(now)) if crate::cluster::same_version(&now, &entry.version.0) => {}
        Ok(_) => return Compacted::Changed,
        Err(why) => return Compacted::Retry(why),
    }
    // Admitted without evicting anyone: a node at its resident limit
    // tries again later.
    let share = inner.mailboxes.resident_share();
    if share.residents >= share.cap || share.process >= share.limit {
        return Compacted::Retry("this node is at its resident limit".into());
    }
    let waiting: Waiting = {
        let (weak, address) = (Arc::downgrade(inner), address.clone());
        Arc::new(move || {
            weak.upgrade()
                .is_some_and(|i| i.mailboxes.waiting(&address) > 0)
        })
    };
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(async move {
                    let _slot = slot;
                    state.idle_pass(entry, app_epoch, waiting).await
                })
            });
    let outcome = match tokio::time::timeout(PASS_WAIT, job).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(e)) => Compacted::Refused(e.to_string()),
        Err(_) => Compacted::Refused("the idle pass did not finish in time".into()),
    };
    match &outcome {
        Compacted::Done { deleted } => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), deleted, "idle history compacted");
        }
        Compacted::Continued { deleted, left } => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), deleted, left, "idle pass stopped at its deadline: continued later");
        }
        Compacted::Deferred { deleted, left } => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), deleted, left, "idle pass stopped at its deadline: its release is retried in the background, continued once it lands");
        }
        Compacted::Stopped { deleted, left, why } => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), deleted, left, %why, "idle pass stopped: not continued");
        }
        Compacted::Refused(why) | Compacted::Retry(why) => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %why, "idle pass: not compacted");
        }
        Compacted::Changed | Compacted::Nothing | Compacted::Busy => {}
    }
    // The job's state let go, unless an event that came after it holds the
    // actor now: asked in a job of its own, behind any such event.
    if outcome != Compacted::Busy {
        let weak = Arc::downgrade(inner);
        let _ = inner
            .mailboxes
            .submit_if_resident(&address, move |state: &mut ActorState| {
                Box::pin(async move {
                    if state.held.is_none()
                        && state.live.is_none()
                        && let Some(inner) = weak.upgrade()
                    {
                        inner.mailboxes.hibernate(&state.address);
                    }
                })
            })
            .await;
    }
    outcome
}

/// The record's version now (one read of its metadata), `None` without a
/// record.
async fn current_version(cluster: &Cluster, key: &str) -> Result<Option<String>, String> {
    let path = object_store::path::Path::from(format!("{}{key}", cluster.state_root().prefix()));
    match tokio::time::timeout(cluster.lease_attempt, cluster.store.head(&path)).await {
        Ok(Ok(meta)) => Ok(Some(format!(
            "{}\n{}",
            meta.e_tag.unwrap_or_default(),
            meta.version.unwrap_or_default()
        ))),
        Ok(Err(object_store::Error::NotFound { .. })) => Ok(None),
        Ok(Err(e)) => Err(format!("reading the ownership record: {e}")),
        Err(_) => Err("reading the ownership record: no answer".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, due_ms: u64) -> Entry {
        Entry {
            key: key.into(),
            generation: 1,
            snapshot_generation: 1,
            version: Version(format!("v-{key}")),
            released_ms: due_ms,
            due_ms,
            attempts: 0,
            continued: 0,
            fruitless: 0,
        }
    }

    #[test]
    fn the_queue_takes_what_is_due_first_and_keeps_one_per_actor() {
        let q = Queue::default();
        q.push(entry("b", 20));
        q.push(entry("a", 10));
        q.push(entry("c", 30));
        // A later release of `a` replaces its entry.
        q.push(entry("a", 25));
        assert_eq!(q.len(), 3);
        assert_eq!(q.next_due(), Some(20));
        assert!(q.take_due(19, 10).is_empty());
        let keys = |v: Vec<Entry>| v.into_iter().map(|e| e.key).collect::<Vec<_>>();
        assert_eq!(keys(q.take_due(30, 2)), ["b", "a"]);
        q.remove("c");
        assert!(q.take_due(30, 2).is_empty());
        assert_eq!(q.len(), 0);
    }

    fn hold(key: &str, generation: u64) -> Hold {
        Hold {
            key: key.into(),
            generation,
            version: Version(format!("held-{generation}")),
            previous: None,
            head: Default::default(),
        }
    }

    #[test]
    fn an_armed_continuation_is_queued_only_by_its_own_holds_release() {
        let q = Queue::default();
        let next = Entry {
            continued: 1,
            ..entry("k", 0)
        };
        // Another hold's release (another generation): nothing queued, and
        // the pass that armed it learns it was not.
        q.arm(7, next.clone());
        q.continue_from(&hold("k", 8), Version("v8".into()), 1_000);
        assert_eq!(q.len(), 0);
        assert!(!q.disarm("k"));
        // Its own release: queued on the version and generation it wrote,
        // for the writer's generation, `CONTINUE_AFTER` later.
        q.arm(7, next.clone());
        q.continue_from(&hold("k", 7), Version("v7".into()), 1_000);
        assert!(q.disarm("k"));
        let queued = q.entries();
        assert_eq!(queued.len(), 1);
        assert_eq!(
            (
                queued[0].generation,
                queued[0].snapshot_generation,
                queued[0].version.0.as_str(),
                queued[0].due_ms,
                queued[0].continued
            ),
            (7, 1, "v7", 1_000 + CONTINUE_AFTER.as_millis() as u64, 1)
        );
        // Disarmed: a release landing later (in the background) queues
        // nothing more, and a release with nothing armed neither.
        q.clear();
        q.continue_from(&hold("k", 7), Version("v7".into()), 2_000);
        assert_eq!(q.len(), 0);
        assert!(!q.disarm("k"));
    }

    fn counted(outcome: &str) -> u64 {
        crate::metrics::METRICS
            .idle_compactions
            .get(&[("outcome", outcome)])
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn continued_stopped() -> (u64, u64) {
        (counted("continued"), counted("stopped"))
    }

    /// A pass whose release came back pending: its continuation stays
    /// armed, and the background release landing queues it, on the
    /// version it wrote and its hold's generation, counting the pass
    /// `continued` there and only there.
    #[test]
    fn a_release_pending_then_landed_queues_the_continuation() {
        let q = Queue::default();
        let next = Entry {
            continued: 1,
            ..entry("k", 0)
        };
        q.arm(7, next);
        let (continued, stopped) = continued_stopped();
        assert_eq!(q.detach("k", 7), None, "deferred");
        assert!(q.armed("k"));
        assert_eq!(continued_stopped(), (continued, stopped));
        // Another hold's release (another generation) queues nothing.
        assert_eq!(
            q.continue_from(&hold("k", 8), Version("v8".into()), 1_000),
            None
        );
        assert_eq!(q.len(), 0);
        assert!(q.armed("k"));
        // Its own, landing in the background.
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 1_000),
            Some(true)
        );
        assert!(!q.armed("k"), "nobody else ends it");
        let queued = q.entries();
        assert_eq!(queued.len(), 1);
        assert_eq!(
            (
                queued[0].generation,
                queued[0].snapshot_generation,
                queued[0].version.0.as_str(),
                queued[0].due_ms,
                queued[0].continued
            ),
            (7, 1, "v7", 1_000 + CONTINUE_AFTER.as_millis() as u64, 1)
        );
        assert_eq!(continued_stopped(), (continued + 1, stopped));
        // Landing again (it cannot, but) queues nothing more.
        q.clear();
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 2_000),
            None
        );
        assert_eq!(q.len(), 0);
    }

    /// A release that landed in the background before the pass learned it
    /// was pending: the pass finds it queued, as for a release that
    /// landed at once, and counts itself.
    #[test]
    fn a_release_landed_before_the_pass_detached_counts_in_the_pass() {
        let q = Queue::default();
        q.arm(7, entry("k", 0));
        let counts = continued_stopped();
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 1_000),
            None
        );
        assert_eq!(q.detach("k", 7), Some(true));
        assert_eq!(q.len(), 1);
        assert_eq!(continued_stopped(), counts, "the pass counts it");
        // Cleared before the pass detached: not continued.
        q.arm(9, entry("j", 0));
        assert!(
            !q.end_armed("j", Some(9)),
            "not detached: the pass counts it"
        );
        assert_eq!(q.detach("j", 9), Some(false));
        // Nothing armed at all.
        assert_eq!(q.detach("i", 3), Some(false));
        assert_eq!(continued_stopped(), counts);
    }

    /// Pending, then superseded: the armed entry is cleared (only for its
    /// own hold's generation), nothing is queued, the pass counts
    /// `stopped`.
    #[test]
    fn a_release_pending_then_superseded_clears_the_continuation() {
        let q = Queue::default();
        q.arm(7, entry("k", 0));
        assert_eq!(q.detach("k", 7), None);
        let (continued, stopped) = continued_stopped();
        assert!(!q.end_armed("k", Some(8)), "another hold's");
        assert!(q.armed("k"));
        assert!(q.end_armed("k", Some(7)));
        assert!(!q.armed("k"));
        assert_eq!(continued_stopped(), (continued, stopped + 1));
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 1_000),
            None
        );
        assert_eq!(q.len(), 0);
        assert!(!q.end_armed("k", Some(7)), "counted once");
        assert_eq!(continued_stopped(), (continued, stopped + 1));
    }

    /// Pending, then an acquisition of the key here: cleared, whatever
    /// generation armed it, along with any entry of the key; a
    /// continuation already queued and not yet collected by its pass is
    /// the queue's (the acquisition removed its entry).
    #[test]
    fn a_release_pending_then_an_acquisition_clears_the_continuation() {
        let q = Queue::default();
        q.arm(7, entry("k", 0));
        assert_eq!(q.detach("k", 7), None);
        q.push(entry("k", 5));
        let (continued, stopped) = continued_stopped();
        q.acquired("k");
        assert!(!q.armed("k"));
        assert_eq!(q.len(), 0);
        assert_eq!(continued_stopped(), (continued, stopped + 1));
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 1_000),
            None
        );
        assert_eq!(q.len(), 0);
    }

    /// A lapse clears every armed continuation, counting only the passes
    /// detached from theirs (the others count themselves, `stopped`).
    #[test]
    fn a_lapse_clears_every_armed_continuation() {
        let q = Queue::default();
        q.arm(7, entry("k", 0));
        q.arm(3, entry("j", 0));
        assert_eq!(q.detach("k", 7), None);
        let (continued, stopped) = continued_stopped();
        assert_eq!(q.end_all_armed(), 1);
        assert!(!q.armed("k") && !q.armed("j"));
        assert_eq!(continued_stopped(), (continued, stopped + 1));
        assert_eq!(q.detach("j", 3), Some(false));
        assert_eq!(
            q.continue_from(&hold("k", 7), Version("v7".into()), 1_000),
            None
        );
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn a_continuation_the_full_queue_refuses_is_said_not_queued() {
        let q = Queue::default();
        for i in 0..QUEUE_MAX as u64 {
            assert!(q.push(entry(&format!("k{i}"), 1_000 + i)));
        }
        q.arm(2, entry("new", 0));
        q.continue_from(&hold("new", 2), Version("v2".into()), 1_000);
        assert!(!q.disarm("new"), "refused, so not continued");
        assert_eq!(q.len(), QUEUE_MAX);
        // Its release landing in the background: counted `stopped` there.
        q.arm(2, entry("late", 0));
        assert_eq!(q.detach("late", 2), None);
        let (continued, stopped) = continued_stopped();
        assert_eq!(
            q.continue_from(&hold("late", 2), Version("v2".into()), 1_000),
            Some(false)
        );
        assert!(!q.armed("late"));
        assert_eq!(continued_stopped(), (continued, stopped + 1));
    }

    #[test]
    fn a_full_queue_refuses_a_new_actor_and_keeps_the_older_entries() {
        let q = Queue::default();
        for i in 0..QUEUE_MAX as u64 {
            assert!(q.push(entry(&format!("k{i}"), 1_000 + i)));
        }
        let dropped = crate::metrics::METRICS
            .idle_passes_dropped
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(!q.push(entry("new", 5_000_000)), "refused");
        assert_eq!(
            crate::metrics::METRICS
                .idle_passes_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            dropped + 1
        );
        assert_eq!(q.len(), QUEUE_MAX);
        // An actor already there is still replaced.
        assert!(q.push(entry("k0", 2_000_000)));
        let first = q.take_due(u64::MAX, 1);
        assert_eq!(first[0].key, "k1", "k0 moved behind the others");
        q.clear();
        assert_eq!(q.len(), 0);
    }
}
