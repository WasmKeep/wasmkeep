//! The idle pass ([ADR 0042](../../../../docs/adr/0042-compacting-an-idle-actors-history.md),
//! section 3): one retention window after a hold that wrote released an
//! actor, the node that released it takes the record again, only if it is
//! still the one that release wrote, and deletes the history the idle
//! snapshot made unnecessary.
//!
//! The release remembers the actor here ([`Queue::note`]): its record key,
//! the generation released, the version the release wrote, and when it is
//! due (release + retention window + clock skew bound). The queue is in
//! memory, bounded ([`QUEUE_MAX`], the entry due first dropped first) and
//! keyed by actor, a later release replacing an earlier one. A loop takes
//! what is due, [`PASSES_AT_ONCE`] at a time, each a job in the actor's
//! mailbox that runs no guest code ([`ActorState::idle_pass`]):
//!
//! 1. skipped if the actor is resident, held, parked or has a socket here,
//!    if the node drains or moves it or is behind the deployed version, if
//!    its class has an idle limit or is not deployed, or if the app is
//!    deleted, restoring or of another incarnation (its markers read);
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
//!    before each ([`Collector::collect_idle`](crate::replica::Collector::collect_idle));
//! 5. the release, which keeps the requirement the hold inherited (it is
//!    never verified) and the idle origin.
//!
//! Nothing is restored, activated or uploaded. The entry is only a hint of
//! when to try: what the pass deletes rests on the record and the listing
//! it reads itself.
//!
//! **The incarnation.** After `app delete` and `deploy --recreate`,
//! generations start again at 1, and on S3 a version 1 released record at
//! the same generation has the same bytes, so the same `ETag`: the version
//! check alone cannot tell the recreated app's record from the remembered
//! one. The pass therefore compares the app's epoch (the `recreated` and
//! `restored` markers, as the blob sweep does, ADR 0030) read after its
//! acquisition with the one this runtime started with, and compacts
//! nothing on a difference. A node that sees the app's tombstone also
//! drops its whole queue.

use std::collections::{BTreeSet, HashMap};

use object_store::ObjectStoreExt as _;

use super::*;
use crate::cluster::RECORD_CHANGED;
use crate::replica::{Collector, IdleCollected};
use wasmkeep_cluster::lease::Version;
use wasmkeep_cluster::node_lease::Hold;
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_cluster::state_root::Family;

/// Entries the queue keeps at most; then the one due first is dropped.
pub(crate) const QUEUE_MAX: usize = 100_000;

/// Passes a node runs at once (as idle expiry's
/// [`PARALLEL`](wasmkeep_cluster::expiry::PARALLEL)).
pub(crate) const PASSES_AT_ONCE: usize = 4;

/// How long the loop waits for one pass (the events queued before it, an
/// acquisition, a listing, the deletes and a release).
const PASS_WAIT: Duration = Duration::from_secs(60);

/// Attempts at a pass's listing and deletes inside its hold, with doubling
/// waits from one second, while the hold is valid.
const PASS_ATTEMPTS: u32 = 3;

/// The longest the loop sleeps between looks at the queue.
const TICK_MAX: Duration = Duration::from_secs(5);

/// One actor to compact: what its release left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    /// The ownership record's key.
    pub(crate) key: String,
    /// The generation the release kept.
    pub(crate) generation: u64,
    /// The version the release wrote.
    pub(crate) version: Version,
    /// When the release landed (Unix ms): the idle origin the pass's
    /// acquisition gives a version 2 record that carries none.
    pub(crate) released_ms: u64,
    /// When the pass is due (Unix ms).
    pub(crate) due_ms: u64,
}

/// The idle passes due on this node, by record key.
#[derive(Debug, Default)]
pub(crate) struct Queue {
    inner: std::sync::Mutex<QueueInner>,
    /// Tests: while set, the loop takes nothing.
    paused: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Default)]
struct QueueInner {
    by_key: HashMap<String, Entry>,
    by_due: BTreeSet<(u64, String)>,
}

impl Queue {
    fn lock(&self) -> std::sync::MutexGuard<'_, QueueInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Adds `entry`, replacing one of the same key. A full queue drops the
    /// entry due first (the oldest release).
    pub(crate) fn push(&self, entry: Entry) {
        let mut q = self.lock();
        if let Some(old) = q.by_key.remove(&entry.key) {
            q.by_due.remove(&(old.due_ms, old.key));
        }
        while q.by_key.len() >= QUEUE_MAX {
            let Some(first) = q.by_due.pop_first() else {
                break;
            };
            q.by_key.remove(&first.1);
        }
        q.by_due.insert((entry.due_ms, entry.key.clone()));
        q.by_key.insert(entry.key.clone(), entry);
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

    /// Forgets every entry (the app's tombstone was seen, or compaction is
    /// off).
    pub(crate) fn clear(&self) {
        let mut q = self.lock();
        q.by_key.clear();
        q.by_due.clear();
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
    }

    fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Whether a hold that ended wrote history: its acknowledged head is a
/// position of its own generation (only a segment it uploaded puts one
/// there). A hold that only read, and the idle pass's own hold, did not:
/// their release queues nothing, so a pass never queues another.
pub(crate) fn wrote(hold: &Hold) -> bool {
    matches!(
        hold.head.acknowledged,
        Some(Position::Known { generation, .. }) if generation == hold.generation
    )
}

impl Cluster {
    /// A release of `hold` landed with `written` its answer's version:
    /// with idle compaction on, a hold that wrote is remembered for its
    /// idle pass. A release whose answer was lost (it comes back
    /// `Superseded`) never reaches here: that actor is not queued.
    pub(crate) fn note_idle_release(&self, hold: &Hold, written: Option<Version>) {
        let Some(version) = written else {
            return;
        };
        if !self.idle_compaction || !wrote(hold) || self.app_refusal().is_some() {
            return;
        }
        let now = now_ms();
        let wait = u64::try_from(self.history_retention.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(self.lease_config.max_skew_ms());
        self.idle_passes.push(Entry {
            key: hold.key.clone(),
            generation: hold.generation,
            version,
            released_ms: now,
            due_ms: now.saturating_add(wait),
        });
    }

    /// The app's epoch as its markers give it now, read again from the
    /// bucket (ADR 0030, section 2: `recreated` and `restored`), or why the
    /// pass compacts nothing: the app has a tombstone or is restoring, or
    /// the bucket did not answer.
    async fn fresh_app_epoch(&self) -> Result<String, String> {
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
                Ok(Err(e)) => return Err(format!("reading the app's {marker} marker: {e}")),
                Err(_) => return Err(format!("reading the app's {marker} marker: no answer")),
            };
            if body.is_some() && matches!(marker, "deleted" | "restoring") {
                return Err(format!(
                    "the app is {}",
                    if marker == "deleted" {
                        "deleted"
                    } else {
                        "restoring"
                    }
                ));
            }
            bodies.push(body);
        }
        Ok(crate::blobs::epoch(
            bodies[2].as_deref(),
            bodies[3].as_deref(),
        ))
    }
}

/// What an idle pass did, a label of `wasmkeep_idle_compactions_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Compacted {
    /// Compacted: `deleted` objects gone (0 if the history was already
    /// as compact as the window allows).
    Done { deleted: usize },
    /// The record is not the one the release wrote: dropped.
    Changed,
    /// No snapshot of the released generation with its segment.
    Nothing,
    /// Not compacted, and why.
    Refused(String),
    /// The actor was in use on this node.
    Busy,
}

impl Compacted {
    fn label(&self) -> &'static str {
        match self {
            Self::Done { .. } => "done",
            Self::Changed => "changed",
            Self::Nothing => "nothing",
            Self::Refused(_) => "refused",
            Self::Busy => "busy",
        }
    }
}

impl ActorState {
    /// The idle pass's job (ADR 0042, section 3): acquire the actor only
    /// if its record is still the version `entry` remembers, check the
    /// app's incarnation against `app_epoch`, compact, release. Runs no
    /// guest code, restores nothing, uploads nothing; the hold it takes is
    /// never this state's (`self.held` stays empty), so no event runs
    /// under it, and an event queued behind the job finds the actor
    /// released and acquires as usual.
    pub(super) async fn idle_pass(&mut self, entry: Entry, app_epoch: String) -> Compacted {
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
        let etag = entry
            .version
            .0
            .split('\n')
            .next()
            .unwrap_or_default()
            .to_owned();
        // An app deleted, restoring or of another incarnation: skipped
        // before anything is written (section 3, step 1). Checked again
        // under the hold, which is what the deletes rest on.
        if let Some(refused) = other_incarnation(&cluster, &app_epoch).await {
            return refused;
        }
        let hold = match LeaseHold::acquire_with(
            &cluster,
            &self.address,
            Some((&etag, entry.released_ms)),
        )
        .await
        {
            Ok(hold) => hold,
            Err(OwnershipError::Refused(why)) if why == RECORD_CHANGED => {
                return Compacted::Changed;
            }
            Err(OwnershipError::NotOwner { .. }) => return Compacted::Changed,
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => return Compacted::Refused(e),
            Err(OwnershipError::Elsewhere { .. }) => {
                return Compacted::Refused("this node is draining or moving the actor".into());
            }
        };
        let outcome = compact_under(&cluster, &self.address, &hold, &entry, &app_epoch).await;
        // Unverified: it releases with the requirement it inherited, and
        // with the idle origin its acquisition carried.
        hold.release().await;
        outcome
    }
}

/// Why the pass compacts nothing of the app now, if it does not: its
/// markers, read again, give another epoch than `app_epoch` (this
/// runtime's: the app was recreated or restored since), or it is deleted
/// or restoring, or the bucket did not answer.
async fn other_incarnation(cluster: &Cluster, app_epoch: &str) -> Option<Compacted> {
    match cluster.fresh_app_epoch().await {
        Ok(fresh) if fresh == app_epoch => None,
        Ok(_) => Some(Compacted::Refused(
            "the app was recreated or restored since this node started".into(),
        )),
        Err(why) => Some(Compacted::Refused(why)),
    }
}

/// The pass's work under its own hold `hold` of `address`.
async fn compact_under(
    cluster: &Arc<Cluster>,
    address: &ActorAddress,
    hold: &LeaseHold,
    entry: &Entry,
    app_epoch: &str,
) -> Compacted {
    // The released generation, from the record this hold replaced
    // (every acquisition writes generation + 1). The entry's own must
    // agree: an equal version with another generation is another
    // record (another incarnation's, whose `ETag` is the same).
    let Some(generation) = hold.generation().and_then(|g| g.checked_sub(1)) else {
        return Compacted::Refused("the hold ended before the pass began".into());
    };
    if generation != entry.generation {
        return Compacted::Changed;
    }
    // The incarnation: this runtime's epoch, read again now that the
    // pass holds the actor. A recreated or restored app compacts
    // nothing of what this node remembered.
    if let Some(refused) = other_incarnation(cluster, app_epoch).await {
        return refused;
    }
    let collector = Collector::idle(cluster, address, generation, hold.view());
    let mut wait = Duration::from_secs(1);
    for attempt in 1..=PASS_ATTEMPTS {
        match collector.collect_idle(cluster.history_retention).await {
            Ok(IdleCollected::NoSnapshot) => return Compacted::Nothing,
            Ok(IdleCollected::Collected { deleted, .. }) => {
                return Compacted::Done { deleted };
            }
            Err(e) => {
                crate::metrics::inc(&crate::metrics::METRICS.history_collect_failures);
                if attempt == PASS_ATTEMPTS || !collector.lease_valid() {
                    return Compacted::Refused(e.to_string());
                }
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
        }
    }
    Compacted::Refused("no attempt left".into())
}

/// The actor an ownership record key names, if it can be told (a name
/// shortened with a hash cannot: never compacted).
fn address_of(inner: &ServiceInner, cluster: &Cluster, key: &str) -> Option<ActorAddress> {
    let base = cluster.state_root().key(Family::Owners, "");
    let (class, encoded) = key.strip_prefix(&base)?.split_once('/')?;
    let name = wasmkeep_storage::decode_segment(encoded)?;
    ActorAddress::new(&inner.tenant, class, &name).ok()
}

/// Takes due entries, [`PASSES_AT_ONCE`] at a time, and runs each one's
/// pass. `app_epoch` is the epoch this runtime started with. Runs until
/// the service is dropped or stops.
pub(super) async fn compaction_loop(
    service: std::sync::Weak<ServiceInner>,
    cluster: Arc<Cluster>,
    app_epoch: String,
) {
    let tick = (cluster.history_retention / 10).clamp(Duration::from_millis(50), TICK_MAX);
    let slots = Arc::new(tokio::sync::Semaphore::new(PASSES_AT_ONCE));
    loop {
        tokio::time::sleep(tick).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return;
        }
        if !cluster.idle_compaction {
            cluster.idle_passes.clear();
            continue;
        }
        if cluster.idle_passes.is_paused() {
            continue;
        }
        let free = slots.available_permits();
        if free == 0 {
            continue;
        }
        for entry in cluster.idle_passes.take_due(now_ms(), free) {
            let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
                // Not reached: no more were taken than were free.
                cluster.idle_passes.push(entry);
                break;
            };
            let (inner, cluster, epoch) =
                (Arc::clone(&inner), Arc::clone(&cluster), app_epoch.clone());
            tokio::spawn(async move {
                let _permit = permit;
                let outcome = pass_one(&inner, &cluster, entry, epoch).await;
                crate::metrics::idle_compaction(outcome.label());
            });
        }
    }
}

/// One entry: its job in the actor's mailbox, unless it is skipped first.
pub(super) async fn pass_one(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    entry: Entry,
    app_epoch: String,
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
    // As idle expiry: no pass under rules the node could not apply (a
    // deploy it is behind on), nor of a class no longer deployed.
    if inner
        .deploy_behind
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Compacted::Refused("this node is behind the deployed version".into());
    }
    if !read(&inner.classes).contains_key(address.class()) {
        return Compacted::Refused("the class is not deployed".into());
    }
    if cluster.draining() {
        return Compacted::Refused("this node is draining".into());
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
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.idle_pass(entry, app_epoch))
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
        Compacted::Refused(why) => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %why, "idle pass: not compacted");
        }
        Compacted::Changed | Compacted::Nothing | Compacted::Busy => {}
    }
    // Released (or never acquired): the next event, wherever it lands,
    // restores the actor. One found in use is left as it is.
    if outcome != Compacted::Busy {
        inner.mailboxes.hibernate(&address);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, due_ms: u64) -> Entry {
        Entry {
            key: key.into(),
            generation: 1,
            version: Version(format!("v-{key}")),
            released_ms: due_ms,
            due_ms,
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
        assert!(q.take_due(19, 10).is_empty());
        let keys = |v: Vec<Entry>| v.into_iter().map(|e| e.key).collect::<Vec<_>>();
        assert_eq!(keys(q.take_due(30, 2)), ["b", "a"]);
        assert_eq!(keys(q.take_due(30, 2)), ["c"]);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn a_full_queue_drops_the_entry_due_first() {
        let q = Queue::default();
        for i in 0..QUEUE_MAX as u64 {
            q.push(entry(&format!("k{i}"), 1_000 + i));
        }
        assert_eq!(q.len(), QUEUE_MAX);
        q.push(entry("new", 5_000_000));
        assert_eq!(q.len(), QUEUE_MAX);
        let first = q.take_due(u64::MAX, 1);
        assert_eq!(first[0].key, "k1", "k0, due first, was dropped");
        q.clear();
        assert_eq!(q.len(), 0);
    }
}
