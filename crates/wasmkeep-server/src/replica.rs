//! Replicating one actor's database to the bucket (ADR 0007).
//!
//! While a node holds an actor's lease, every committed transaction is
//! uploaded as an LTX segment under
//! `actors/<tenant>/<class>/<name>/wal/<txid>-<generation>-<parent>.ltx`,
//! and every [`SNAPSHOT_EVERY`] segments a snapshot of the whole database.
//! A segment is durable only if its upload completed while the lease was
//! still valid. On acquiring an actor, the node restores the chain the
//! highest generation wrote, unless its local file already is that head.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use wasmkeep_cluster::replication::{
    EMPTY, Listing, Plan, Segment, SegmentId, collectable, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_runtime::ActorAddress;
use wasmkeep_storage::ltx::DbChecksum;
use wasmkeep_storage::wal::{Changes, WalCapture};

use crate::cluster::{Cluster, LeaseView};

/// Upload a snapshot after this many segments, so restores stay short.
const SNAPSHOT_EVERY: u64 = 64;

/// A restore's reads are bounded, so one request the bucket leaves hanging
/// cannot hold a takeover (one held a restore for 19.8 s in the
/// real-bucket soak, under the client's 30 s per-request timeout). A read
/// is given up when the bucket goes this long without answering: no
/// headers, no next chunk of a body, no next page of a listing. A segment
/// answered in 82 ms at the median and 1.7 s at worst next to the bucket;
/// the margin leaves the client's own retries on throttling some room. An
/// object's read is also given up once it has taken this long plus a
/// second per started megabyte of the object, from its request, however it
/// keeps moving (the 12-hour soak: a restore of 55 objects took 56 s, each
/// read slow and none silent for 5 s).
const READ_IDLE: std::time::Duration = std::time::Duration::from_secs(5);
/// A restore reads up to this many objects ahead of the one it applies
/// (each on a task of its own), and up to [`READ_AHEAD_BYTES`] of them by
/// the listing's sizes: one slow route no longer makes a chain's reads add
/// up. Always at least one, however large.
const READ_AHEAD: usize = 8;
const READ_AHEAD_BYTES: u64 = 32 << 20;
/// One node's restores together read at most this many objects at once,
/// and hold at most [`RESTORE_READ_MIB`] of them: one kill starts a
/// takeover of every actor its node held, each restore reading ahead, and
/// before read-ahead each read one object at a time. A read waits for its
/// slot, then for its megabytes (by the listing's size, at least one, at
/// most all), before its time starts. Process-wide.
const RESTORE_READS: usize = 32;
const RESTORE_READ_MIB: u32 = 256;
/// The listed bytes of the restore reads under way on this node (permits
/// count whole megabytes; a read's time counts bytes). Each restore's own
/// part ([`OwnInFlight`]) changes under the same lock, so the two are
/// always read as one: a restore alone on the node sees exactly its own
/// bytes here.
static RESTORE_READ_IN_FLIGHT: std::sync::Mutex<u64> = std::sync::Mutex::new(0);

/// One restore's part of [`RESTORE_READ_IN_FLIGHT`].
type OwnInFlight = Arc<std::sync::atomic::AtomicU64>;

/// The bytes in flight on the node, and those of them that are `restore`'s.
fn in_flight(restore: &OwnInFlight) -> (u64, u64) {
    let node = RESTORE_READ_IN_FLIGHT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    (*node, restore.load(std::sync::atomic::Ordering::Relaxed))
}

/// Counts a read's bytes in [`RESTORE_READ_IN_FLIGHT`], and in its
/// restore's part of it, while it lives.
struct InFlightBytes(u64, OwnInFlight);

impl InFlightBytes {
    fn new(bytes: u64, restore: &OwnInFlight) -> Self {
        let mut node = RESTORE_READ_IN_FLIGHT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *node = node.saturating_add(bytes);
        restore.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        Self(bytes, Arc::clone(restore))
    }
}

impl Drop for InFlightBytes {
    fn drop(&mut self) {
        let mut node = RESTORE_READ_IN_FLIGHT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *node = node.saturating_sub(self.0);
        self.1
            .fetch_sub(self.0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// The limit of one try of a read: [`READ_IDLE`] plus a second per
/// started megabyte of `bytes`, the bytes read over the same link
/// alongside it, its own included.
fn read_limit(bytes: u64) -> std::time::Duration {
    let megabytes = u32::try_from(bytes.div_ceil(1 << 20)).unwrap_or(u32::MAX);
    READ_IDLE.saturating_add(std::time::Duration::from_secs(1).saturating_mul(megabytes))
}

/// The time one read has, shared by the read ([`bounded_get`]) and by the
/// restore that waits for it, so the two never disagree.
///
/// A try's limit is [`read_limit`] of the most bytes seen in flight
/// alongside it: what its restore had ahead when it was asked for, or the
/// node's bytes in flight if more, looked at when the read starts, at
/// every wait for the body, and when its restore looks at its deadline.
/// Those bytes are either its own restore's or other restores'. The limit
/// the same try would have with no other restore on the node is
/// [`read_limit`] of the most of its own restore's bytes seen; the
/// difference, whole seconds, is what the other restores' bytes added:
/// the read's *lift*. It is exactly zero for a restore alone on the node.
struct ReadTime {
    /// Its restore's bytes in flight; `None` for a read outside a restore,
    /// whose time counts `ahead` alone.
    restore: Option<OwnInFlight>,
    /// The most bytes seen in flight alongside it, and the most of its own
    /// restore's: never under `ahead`, the listed bytes its restore was
    /// reading ahead when it was asked for, its own included.
    all: std::sync::atomic::AtomicU64,
    own: std::sync::atomic::AtomicU64,
    /// The lifts of the tries that ended, in ms.
    ended: std::sync::atomic::AtomicU64,
}

impl ReadTime {
    fn new(ahead: u64, restore: Option<OwnInFlight>) -> Self {
        Self {
            restore,
            all: ahead.into(),
            own: ahead.into(),
            ended: 0.into(),
        }
    }

    /// Looks at the bytes in flight now: the current try's limit, and its
    /// lift.
    fn look(&self) -> (std::time::Duration, std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let (node, own) = self.restore.as_ref().map_or((0, 0), in_flight);
        let all = self.all.fetch_max(node, Relaxed).max(node);
        let own = self.own.fetch_max(own, Relaxed).max(own);
        let limit = read_limit(all);
        (limit, limit.saturating_sub(read_limit(own)))
    }

    /// A try ended and another follows: its lift stays counted.
    fn next_try(&self) {
        let lift = u64::try_from(self.look().1.as_millis()).unwrap_or(u64::MAX);
        self.ended
            .fetch_add(lift, std::sync::atomic::Ordering::Relaxed);
    }

    /// The lifts of its tries so far, the current one included.
    fn lifted(&self) -> std::time::Duration {
        let ended = self.ended.load(std::sync::atomic::Ordering::Relaxed);
        std::time::Duration::from_millis(ended).saturating_add(self.look().1)
    }
}

/// The most of [`RESTORE_READ_MIB`] one read takes, however large its object.
const RESTORE_READ_ONE_MIB: u32 = (READ_AHEAD_BYTES >> 20) as u32;
static RESTORE_READ_SLOTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(RESTORE_READS);
static RESTORE_READ_BYTES: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(RESTORE_READ_MIB as usize);
/// Attempts per read before the restore fails (the next event restores
/// again, from a fresh listing).
const READ_ATTEMPTS: u32 = 3;

/// A read the bucket refuses with an error a resend may get past (a 5xx
/// or a reset connection once the client's own retries are spent, a body
/// cut half-way) is sent again after this pause, doubled each time, and
/// counts among the read's [`READ_ATTEMPTS`]: a GET changes nothing, so
/// sending it again is always safe. Without it one such answer among a
/// chain's reads failed the whole restore and the event that woke the
/// actor.
const READ_PAUSE: std::time::Duration = std::time::Duration::from_millis(500);

/// A restore's reads together (every object, every resend) end within
/// one deadline, as an upload does: each read above is bounded, but a
/// chain of reads that each stay under their limit added up without one
/// (three tries of 5 s or more per object), while every caller of the
/// actor waited behind it.
///
/// The rule. A restore has a time *allowed*, sized from what it restores
/// ([`restore_time`]):
///
/// - this much whatever is restored: six [`READ_IDLE`]s, 30 s, the wait
///   of a caller whose wait is bounded at all (`wall_per_event`,
///   `FORWARD_TIMEOUT`). Restores of up to 63 segments took 1 to 6 s next
///   to a real bucket, and 17.5 s at worst with 19 of them sharing the
///   node (`docs/benchmarks.md`);
/// - [`RESTORE_PER_MIB`] per started megabyte of the objects' listed
///   sizes, the rate below which a single read is given up as too slow
///   when nothing is read alongside it: a database of 1 GiB has 17
///   minutes more;
/// - [`RESTORE_PER_AHEAD`] per [`READ_AHEAD`] objects, for a chain grown
///   long because its periodic snapshots failed: eight reads side by
///   side, each answered in 82 ms at the median and 1.7 s at worst next
///   to the bucket.
///
/// Against it counts the time the restore waits for the bucket to give
/// it the object it applies next, less, for each object, that read's
/// lift ([`ReadTime`]): the seconds the bytes of *other* restores in
/// flight on the node added to the limits of its tries, and never more
/// than the restore waited for that object. The restore is ended when
/// what counts passes what is allowed.
///
/// The lift is why the allowed time can be sized from the restore's own
/// bytes while a read's limit counts every byte in flight on the node
/// (a second per megabyte, up to [`RESTORE_READ_MIB`]): after a kill one
/// node restores every actor it takes over at once over one link, and 32
/// actors of 8 MiB each then give every read 261 s where each restore is
/// allowed 39 s. Without the lift every one of them was ended at 39 s
/// with each read inside its limit, nothing was kept, and the callers
/// that tried again met the same end together. With it, what a read
/// takes past the limit it would have with no other restore on the node
/// is never counted: the time counted for an object is at most what its
/// tries are allowed alone (three of [`READ_IDLE`] plus a second per
/// megabyte of the restore's own bytes ahead, and the pauses), so other
/// restores' bytes never end a restore. A restore alone on the node has
/// no lift at all, and slow reads that add up end it as before.
///
/// The worst case. Bytes in flight on the node never pass
/// [`RESTORE_READ_MIB`] megabytes (a read counts for at most
/// [`RESTORE_READ_ONE_MIB`], as its permits do), so a try's lift is at
/// most 256 s and an object's, three tries, 768 s. A restore's waits for
/// the bucket therefore end within its allowed time plus 768 s per object
/// it reads:
/// reached only while other restores keep 256 MiB in flight and every
/// one of its reads uses three tries to the end of their lifted limits.
/// That is no more than the reads' own bounds give, which is what bounded
/// such a restore before there was a deadline; on a node restoring
/// nothing else it is the allowed time exactly. A bucket that never
/// answers ends a restore sooner than either: a read is given up after
/// three silences of [`READ_IDLE`], 15 s, whatever is in flight.
///
/// Not counted at all, since nothing above sizes it:
///
/// - the listing, whose length is not known before it ends: it keeps
///   its own rule ([`list_with`]: given up when no key comes for
///   [`READ_IDLE`], never while pages keep coming), so its time follows
///   the pages listed;
/// - the time a read waits for the node's shared limit on reads
///   ([`RESTORE_READS`], [`RESTORE_READ_MIB`]) before it is even asked
///   for, as it is not in a read's own time. The queue always moves,
///   since every read holding a place in it is bounded; while it lasts,
///   the deadline does not bound what a caller waits;
/// - checking the local file, applying an object to it or putting the
///   finished database in place: local work, sized by the database and not by the compressed
///   bytes read, and never cut. The reads ahead go on meanwhile.
pub(crate) const RESTORE_BUDGET: std::time::Duration = READ_IDLE.saturating_mul(6);
const RESTORE_PER_MIB: std::time::Duration = std::time::Duration::from_secs(1);
const RESTORE_PER_AHEAD: std::time::Duration = std::time::Duration::from_secs(1);

/// The time a restore's reads are allowed, counted from `budget`: see
/// [`RESTORE_BUDGET`].
fn restore_time(budget: std::time::Duration, objects: usize, bytes: u64) -> std::time::Duration {
    let megabytes = u32::try_from(bytes.div_ceil(1 << 20)).unwrap_or(u32::MAX);
    let rounds = u32::try_from(objects.div_ceil(READ_AHEAD)).unwrap_or(u32::MAX);
    budget
        .saturating_add(RESTORE_PER_MIB.saturating_mul(megabytes))
        .saturating_add(RESTORE_PER_AHEAD.saturating_mul(rounds))
}

/// A segment upload (one transaction, usually a few pages) is bounded too:
/// an attempt the bucket has not answered within this, plus a second per
/// started megabyte, is given up and sent again, the next attempt allowed
/// twice as long, up to [`UPLOAD_ATTEMPTS`] times. Without it one hung PUT
/// waited for the client's own 30 s timeout, the event was cut and the
/// actor given up (seen in a smoke run against a real bucket).
///
/// An attempt here is one call to the bucket's client, which may send
/// more than one request within it: after an error answer (a 5xx, 429 or
/// 408) or a request it could not send, it pauses and sends again. It
/// has a timeout of its own on each request, 30 s from connecting to the
/// last byte of the answer (its default, which `wasmkeep serve` leaves
/// as the environment has it), and a conditional create that times out,
/// or whose connection breaks, it does not send again: the call fails.
/// So the bound above is the only limit on an attempt (one answered with
/// errors can hold several requests and last past 30 s where the bound
/// allows), and 30 s is the limit on a request: the second per megabyte
/// gives a first attempt's request more time up to 25 MB and no further,
/// and an object the bucket cannot take within 30 s in one request is
/// never uploaded, whatever the deadline allows.
///
/// An actor's first snapshot and the one after a delete (the whole
/// database) are uploaded under these bounds; a periodic snapshot under
/// shorter ones, see [`periodic_snapshot_bound`]. An upload holds the
/// actor's replica, and with it the actor, until it ends.
const UPLOAD_IDLE: std::time::Duration = std::time::Duration::from_secs(5);
/// Attempts per upload before it fails (a segment's stops replication;
/// a periodic snapshot has one, and is tried again later).
const UPLOAD_ATTEMPTS: u32 = 3;
/// The whole upload (every attempt and any read-back) ends within this
/// many [`UPLOAD_IDLE`]s, plus the same second per started megabyte: 21 s
/// for a segment under a megabyte, so one stalled upload leaves an event
/// (30 s) some room; the bound is per upload, not per event. The last
/// attempt has whatever is left. Attempts before it are cut at their
/// limit even while moving, so a multi-megabyte upload slower than about
/// a megabyte a second still fails, as it did under the client's 30 s.
const UPLOAD_BUDGET_IDLES: u32 = 4;

/// A periodic snapshot is an optimisation (the segments are the history),
/// taken after an event while the actor is held: it must cost the events
/// behind it little even when the bucket leaves every snapshot upload
/// hanging. A caller queued behind the actor waits 30 s at most where its
/// wait is bounded at all (`wall_per_event` for `actors.call`,
/// `FORWARD_TIMEOUT` for a request another node forwards; a request the
/// owner received itself has no limit of its own). So a periodic snapshot
/// gets one attempt (one call to the client, see [`UPLOAD_IDLE`]), which
/// this code does not send again, given up after [`UPLOAD_IDLE`] plus a
/// second per started megabyte and at most this many [`UPLOAD_IDLE`]s: 6 s
/// under a megabyte, 10 s from 5 MB on, a third of that wait and under
/// the bucket client's own 30 s per request. Whatever the client sends
/// again after an error answer, it sends within that bound.
///
/// After a failure the next try waits for one more segment, then
/// [`SNAPSHOT_RETRY_GROWTH`] times as many each time up to
/// [`SNAPSHOT_EVERY`] (1, 8, 64, 64...), and in any case for
/// [`SNAPSHOT_RETRY_MIN`] from the start of the try that failed.
///
/// Worst case, the bucket hanging every snapshot upload: a try holds the
/// actor for 10 s (6 s under a megabyte); the tries come at the segment
/// the snapshot was due at, 1 later, 9 later, 73 later and then every 64,
/// so at most three in any 64 segments, 30 s of hold (18 s), then one per
/// 64 segments, 10 s (6 s); never two starting within 30 s. Before
/// uploads had bounds, one hung snapshot per 64 segments held the actor
/// for the client's 30 s.
///
/// That holds on one node for as long as its process lives: the wait is
/// the node's ([`SnapshotWaits`]), so an actor dropped from memory when
/// idle and restored at its next event keeps it. A restore counts every
/// segment since the last snapshot, so without a wait on record (another
/// node took the actor over, this one restarted, or the record was full)
/// a chain of [`SNAPSHOT_EVERY`] segments or more makes a snapshot due
/// at the first event, as it always did: one more try, and the waits
/// start from there.
///
/// The price: a database the bucket cannot take within 10 s (slower than
/// a megabyte a second up to 5 MB, than its size in 10 s above) gets no
/// periodic snapshot. Each try is one counted, bounded attempt, and a
/// restore replays every segment since the last snapshot there is.
const PERIODIC_SNAPSHOT_IDLES: u32 = 2;
/// See [`PERIODIC_SNAPSHOT_IDLES`]: each failure in a row multiplies the
/// segments the next try waits for by this.
const SNAPSHOT_RETRY_GROWTH: u64 = 8;
/// See [`PERIODIC_SNAPSHOT_IDLES`]: the least time between a periodic
/// snapshot that failed and the next try, however fast segments follow:
/// as long as the longest bounded wait of a queued caller.
pub(crate) const SNAPSHOT_RETRY_MIN: std::time::Duration = std::time::Duration::from_secs(30);

/// Each actor's wait after a periodic snapshot that failed, kept by the
/// node ([`Cluster::snapshot_waits`]) so that it outlives the actor's
/// [`Replica`]: an idle actor is dropped from memory and restored at its
/// next event with every segment since the last snapshot counted again,
/// and without this a snapshot would be due at that first event, wait or
/// no wait. An entry is written when a try starts, removed when a
/// snapshot is there, when the actor is deleted and when this node sees
/// the actor go to another; a restore that finds a later snapshot drops
/// it. Only this process knows it: another node that takes the actor
/// over, or this one after a restart, starts with none.
///
/// At most [`SNAPSHOT_WAITS_MAX`] entries: one more pushes out the one
/// whose time comes first, whose actor may then try again at its next
/// restore, as it did before the wait was kept.
#[derive(Debug, Default)]
pub(crate) struct SnapshotWaits(
    std::sync::Mutex<std::collections::HashMap<ActorAddress, SnapshotWait>>,
);

/// See [`SnapshotWaits`].
const SNAPSHOT_WAITS_MAX: usize = 10_000;

/// One actor's wait: no periodic snapshot is tried before `after`, nor
/// before the actor's head has reached transaction `txid` (each segment
/// is one transaction, whoever uploads it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotWait {
    /// Periodic snapshots that failed in a row.
    failures: u32,
    txid: u64,
    after: tokio::time::Instant,
}

impl SnapshotWaits {
    fn entries(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<ActorAddress, SnapshotWait>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn get(&self, address: &ActorAddress) -> Option<SnapshotWait> {
        self.entries().get(address).copied()
    }

    fn set(&self, address: &ActorAddress, wait: SnapshotWait) {
        let mut entries = self.entries();
        if entries.len() >= SNAPSHOT_WAITS_MAX && !entries.contains_key(address) {
            let first = entries
                .iter()
                .min_by_key(|(_, w)| w.after)
                .map(|(a, _)| a.clone());
            if let Some(first) = first {
                entries.remove(&first);
            }
        }
        entries.insert(address.clone(), wait);
    }

    /// The actor has a snapshot, was deleted, or is another node's now.
    pub(crate) fn forget(&self, address: &ActorAddress) {
        self.entries().remove(address);
    }
}

/// How long a periodic snapshot's one attempt may take, for an idle bound
/// of `idle` and a snapshot of `bytes`: see [`PERIODIC_SNAPSHOT_IDLES`].
fn periodic_snapshot_bound(idle: std::time::Duration, bytes: usize) -> std::time::Duration {
    let megabytes = u32::try_from(bytes.div_ceil(1 << 20)).unwrap_or(u32::MAX);
    idle.saturating_add(std::time::Duration::from_secs(1).saturating_mul(megabytes))
        .min(idle.saturating_mul(PERIODIC_SNAPSHOT_IDLES))
}

/// Which snapshot an upload is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotKind {
    /// An actor's first, at a new position: its history starts there, so
    /// a failure stops replication. Sent again as a segment is.
    First,
    /// The periodic one, at the head: one short attempt, tried again
    /// later ([`Replica::snapshot_due`]).
    Periodic,
    /// The one after a delete, at the head, so the history before it can
    /// be erased: sent again as a segment is; a failure is not tried
    /// again, and leaves the periodic snapshot's schedule as it was.
    AfterDelete,
}

/// Why replication cannot continue. The actor must stop: the node gives up
/// its lease, and the next event restores from the bucket.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplicaError {
    /// The bucket did not answer, or refused a write.
    #[error("bucket: {0}")]
    Bucket(String),
    /// The object already exists (conditional create refused).
    #[error("already exists")]
    Exists,
    /// The upload completed, but after the lease ran out: not durable.
    #[error("upload completed after the lease ran out")]
    Late,
    /// The bucket's history cannot be restored without losing data.
    #[error("restore: {0}")]
    Restore(String),
    /// Local file trouble.
    #[error("local: {0}")]
    Local(String),
}

/// One actor's replication state, owned by the actor while it holds the
/// lease.
pub(crate) struct Replica {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    generation: u64,
    lease: LeaseView,
    /// The position the bucket and the local database agree on.
    head: SegmentId,
    /// Segments since the last snapshot. A periodic snapshot that fails
    /// leaves it as it is.
    since_snapshot: u64,
    /// The actor, and the node's record of its wait after a periodic
    /// snapshot that failed: what the three fields below hold is written
    /// there too, and read back by the actor's next `Replica`
    /// ([`SnapshotWaits`]).
    address: ActorAddress,
    snapshot_waits: Arc<SnapshotWaits>,
    /// Periodic snapshots that failed since the last snapshot taken.
    snapshot_failures: u32,
    /// After a periodic snapshot failed, none is tried again before
    /// `since_snapshot` reaches this, nor before `snapshot_retry_after`:
    /// see [`Self::snapshot_due`].
    snapshot_retry_at: u64,
    snapshot_retry_after: tokio::time::Instant,
    /// [`SNAPSHOT_RETRY_MIN`] unless the node's configuration says
    /// otherwise (tests shorten it).
    snapshot_retry_min: std::time::Duration,
    /// The local database file (for the `.replica` sidecar).
    db_path: PathBuf,
    /// Set when the bucket holds nothing yet: the first upload is a
    /// snapshot of the local database.
    needs_snapshot: bool,
    /// Database checksum at `head`.
    checksum: DbChecksum,
    /// The size cap: a restore that would build a larger database (or
    /// download a larger object) is refused.
    max_db_bytes: u64,
    /// Why replication stopped, if it did. Once set, nothing more is shipped:
    /// the local database may be ahead of the bucket, so the actor must be
    /// given up and restored. Also set while an upload is in flight, so an
    /// upload cancelled half-way (the event was cut) counts as failed.
    failed: Option<String>,
    /// An upload attempt's base bound ([`UPLOAD_IDLE`]; shorter in tests).
    upload_idle: std::time::Duration,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("prefix", &self.prefix)
            .field("generation", &self.generation)
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

/// The sidecar recording which bucket position the local file is at.
fn sidecar(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(".replica");
    p.into()
}

fn read_sidecar(db_path: &Path) -> Option<(SegmentId, u64)> {
    let text = std::fs::read_to_string(sidecar(db_path)).ok()?;
    let mut parts = text.split_whitespace().map(|s| s.parse::<u64>().ok());
    let (Some(Some(generation)), Some(Some(txid)), Some(Some(sum))) =
        (parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    Some((SegmentId { generation, txid }, sum))
}

fn write_sidecar(db_path: &Path, head: SegmentId, sum: DbChecksum) -> Result<(), ReplicaError> {
    let tmp = {
        let mut p = sidecar(db_path).into_os_string();
        p.push(".tmp");
        PathBuf::from(p)
    };
    std::fs::write(
        &tmp,
        format!("{} {} {}\n", head.generation, head.txid, sum.value()),
    )
    .and_then(|()| std::fs::rename(&tmp, sidecar(db_path)))
    .map_err(|e| ReplicaError::Local(format!("sidecar: {e}")))
}

/// Moves a database the bucket is about to replace out of the way, as
/// `<db>.replaced` (with its WAL as `<db>.replaced-wal`), instead of
/// deleting it: it may hold data from before the node joined a cluster.
/// One generation of it is kept.
fn set_aside(db_path: &Path) -> std::io::Result<()> {
    let with = |suffix: &str| {
        let mut p = db_path.as_os_str().to_owned();
        p.push(suffix);
        PathBuf::from(p)
    };
    for (from, to) in [("", ".replaced"), ("-wal", ".replaced-wal")] {
        match std::fs::rename(with(from), with(to)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    for gone in ["-shm", ".replica"] {
        match std::fs::remove_file(with(gone)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

impl Replica {
    /// Prepares the actor's local database for generation `generation`:
    /// restores the bucket's current chain unless the local file is already
    /// at its head. The database must be closed.
    ///
    /// Its reads end within one deadline ([`RESTORE_BUDGET`]). A
    /// restore that fails, for the bucket or anything else, leaves the
    /// local file and its sidecar as they were (the database is built
    /// beside it and renamed over it at the very end), so the next one
    /// starts from a fresh listing and trusts nothing of this one; it is
    /// counted in `wasmkeep_restore_failures_total`.
    pub(crate) async fn restore(
        cluster: &Cluster,
        address: &ActorAddress,
        lease: LeaseView,
        generation: u64,
        db_path: PathBuf,
        max_db_bytes: u64,
    ) -> Result<Self, ReplicaError> {
        let restored =
            Self::restore_within(cluster, address, lease, generation, db_path, max_db_bytes).await;
        if restored.is_err() {
            crate::metrics::inc(&crate::metrics::METRICS.restore_failures);
        }
        restored
    }

    async fn restore_within(
        cluster: &Cluster,
        address: &ActorAddress,
        lease: LeaseView,
        generation: u64,
        db_path: PathBuf,
        max_db_bytes: u64,
    ) -> Result<Self, ReplicaError> {
        let store = Arc::clone(&cluster.store);
        let prefix = format!(
            "{}actors/{}/{}/{}/",
            cluster.store_prefix(),
            address.tenant(),
            address.class(),
            wasmkeep_storage::encode_segment(address.name())
        );
        let sized = list_sized(&store, &prefix, Some(READ_IDLE)).await?;
        let keys: Vec<String> = sized.iter().map(|(k, _)| k.clone()).collect();
        let sizes: std::collections::HashMap<String, u64> = sized.into_iter().collect();
        let listing = Listing::from_keys(keys.iter().map(String::as_str));
        let plan = plan_restore(&listing).map_err(|e| ReplicaError::Restore(e.to_string()))?;
        let mut replica = Self {
            store,
            prefix,
            generation,
            lease,
            head: plan.head(),
            since_snapshot: plan.segments.len() as u64,
            address: address.clone(),
            snapshot_waits: Arc::clone(&cluster.snapshot_waits),
            snapshot_failures: 0,
            snapshot_retry_at: 0,
            snapshot_retry_after: tokio::time::Instant::now(),
            snapshot_retry_min: cluster.snapshot_retry_min,
            db_path,
            needs_snapshot: plan.head() == EMPTY,
            checksum: DbChecksum::default(),
            max_db_bytes,
            failed: None,
            upload_idle: UPLOAD_IDLE,
        };
        replica.resume_snapshot_wait();
        if plan.head() == EMPTY {
            // Nothing in the bucket yet: the local database (possibly from
            // before the node joined a cluster) becomes the first snapshot.
            return Ok(replica);
        }
        if let Some(sum) = replica.local_is_at(&plan).await {
            tracing::debug!(prefix = %replica.prefix, head = ?plan.head(), "local database is current");
            replica.checksum = sum;
            return Ok(replica);
        }
        replica.checksum = replica
            .download(&plan, &keys, &sizes, cluster.restore_budget)
            .await?;
        Ok(replica)
    }

    /// Whether the local file is exactly the plan's head: the sidecar names
    /// it, no WAL is pending, and the file's checksum matches.
    async fn local_is_at(&self, plan: &Plan) -> Option<DbChecksum> {
        let path = self.db_path.clone();
        let head = plan.head();
        tokio::task::spawn_blocking(move || {
            let (at, sum) = read_sidecar(&path)?;
            let mut wal = path.as_os_str().to_owned();
            wal.push("-wal");
            let wal_empty = std::fs::metadata(PathBuf::from(wal)).map_or(true, |m| m.len() == 0);
            if at != head || !wal_empty {
                return None;
            }
            let page_size = page_size_of(&path).unwrap_or(4096);
            let found = WalCapture::start(&path, page_size).ok()?.checksum();
            (found.value() == sum).then_some(found)
        })
        .await
        .ok()
        .flatten()
    }

    // Keys from `list` are in the store's encoding and go through
    // `StorePath::from` again here, which is only right because segment and
    // snapshot keys contain nothing it escapes (`[0-9a-f-./]`).
    //
    // Applied one object at a time, in order, straight into the file, then
    // dropped; read up to [`READ_AHEAD`] objects and [`READ_AHEAD_BYTES`]
    // ahead (always at least one). Memory stays at the read-ahead's
    // compressed objects and one page, however long the chain or large the
    // database (a chain of highly compressible segments once decoded to
    // many times the database size).
    //
    // `listed` is the listing the plan was made from: the segment keys come
    // from it, not from a second listing.
    async fn download(
        &mut self,
        plan: &Plan,
        listed: &[String],
        sizes: &std::collections::HashMap<String, u64>,
        budget: std::time::Duration,
    ) -> Result<DbChecksum, ReplicaError> {
        let mut keys = Vec::with_capacity(plan.segments.len() + 1);
        if let Some(id) = plan.snapshot {
            keys.push(snapshot_key(id));
        }
        if !plan.segments.is_empty() {
            let listed: std::collections::HashMap<SegmentId, &String> = listed
                .iter()
                .filter_map(|k| {
                    let l = Listing::from_keys([k.as_str()]);
                    l.segments.first().map(|s| (s.id, k))
                })
                .collect();
            for id in &plan.segments {
                let key = listed
                    .get(id)
                    .ok_or_else(|| ReplicaError::Restore(format!("segment {id:?} vanished")))?;
                keys.push((*key).clone());
            }
        }

        // The time its reads are allowed; the deadline is moved back,
        // below, by what does not count against it ([`RESTORE_BUDGET`]).
        let allowed = restore_time(
            budget,
            keys.len(),
            keys.iter()
                .map(|key| sizes.get(key).copied().unwrap_or(0))
                .fold(0, u64::saturating_add),
        );
        let mut deadline = tokio::time::Instant::now() + allowed;
        let objects = keys.len();
        // What other restores' bytes added to its reads' limits, so far.
        let mut lifted = std::time::Duration::ZERO;
        let in_flight: OwnInFlight = Arc::default();

        let path = self.db_path.clone();
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".restore");
        let tmp = PathBuf::from(tmp);
        // Removed on every way out but success (errors, a cancelled event).
        let mut cleanup = RemoveOnDrop(Some(tmp.clone()));
        // Twice the node's cap: the bound is against hostile chains, while
        // an honest database may exceed this node's cap (the cap lowered
        // since, a node configured differently, a pre-cluster database).
        let max = self.max_db_bytes.saturating_mul(2).max(64 * 1024);
        // An object larger than this cannot hold such a database: LTX adds
        // a few bytes per page to incompressible data.
        let max_object = max.saturating_add(max / 16).saturating_add(1 << 20);
        let mut restorer = {
            let (path, tmp) = (path.clone(), tmp.clone());
            tokio::task::spawn_blocking(move || {
                // A node that never ran this actor has no directory for it yet.
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir).map_err(|e| ReplicaError::Local(e.to_string()))?;
                }
                let _ = std::fs::remove_file(&tmp);
                wasmkeep_storage::wal::Restorer::create(&tmp, max)
                    .map_err(|e| ReplicaError::Restore(e.to_string()))
            })
            .await
            .map_err(|e| ReplicaError::Local(e.to_string()))??
        };
        // Reads on tasks of their own, so they go on while an object is
        // applied; aborted if the restore ends early.
        let mut pending = keys.into_iter().peekable();
        let mut reading: std::collections::VecDeque<Reading> = std::collections::VecDeque::new();
        let mut ahead = 0u64;
        loop {
            while let Some(key) = pending.peek() {
                let listed = sizes.get(key).copied();
                let size = listed.unwrap_or(0);
                if !reading.is_empty()
                    && (reading.len() >= READ_AHEAD
                        || ahead.saturating_add(size) > READ_AHEAD_BYTES)
                {
                    break;
                }
                let key = pending.next().unwrap_or_default();
                ahead = ahead.saturating_add(size);
                let store = Arc::clone(&self.store);
                let location = StorePath::from(format!("{}{key}", self.prefix));
                // Its time counts every byte read alongside it.
                let time = Arc::new(ReadTime::new(ahead, Some(Arc::clone(&in_flight))));
                let (turn, has_turn) = tokio::sync::oneshot::channel();
                let (its_time, in_flight) = (Arc::clone(&time), Arc::clone(&in_flight));
                let task = tokio::spawn(async move {
                    // Never closed: a failed acquire would be a bug.
                    // One read takes at most a read-ahead's worth: a larger
                    // object would take the whole budget, and the fair
                    // semaphore would hold every other restore behind it.
                    let mib = u32::try_from(size.div_ceil(1 << 20))
                        .unwrap_or(u32::MAX)
                        .clamp(1, RESTORE_READ_ONE_MIB);
                    let (Ok(_slot), Ok(_bytes)) = (
                        RESTORE_READ_SLOTS.acquire().await,
                        RESTORE_READ_BYTES.acquire_many(mib).await,
                    ) else {
                        return Err(ReplicaError::Local("restore read limit closed".into()));
                    };
                    let _ = turn.send(());
                    // Its time counts every byte read alongside it on this
                    // node, other restores' included, as it is while the
                    // read lasts (reads that join later count too).
                    // Counted as its permits are: at most a read-ahead's
                    // worth, or one large object would lift every read's
                    // limit past any bound.
                    let _counted = InFlightBytes::new(
                        size.min(u64::from(RESTORE_READ_ONE_MIB) << 20),
                        &in_flight,
                    );
                    let read = std::time::Instant::now();
                    let got =
                        bounded_get(&store, &location, max_object, max, listed, &its_time).await;
                    crate::metrics::METRICS
                        .restore_read_seconds
                        .observe(read.elapsed());
                    got
                });
                reading.push_back(Reading {
                    key,
                    size,
                    has_turn,
                    time,
                    task: AbortOnDrop(task),
                });
            }
            let Some(Reading {
                key,
                size,
                has_turn,
                time,
                mut task,
            }) = reading.pop_front()
            else {
                break;
            };
            // Waiting for the node's limit on reads is not the restore's
            // time ([`RESTORE_BUDGET`]). An error means the task ended
            // without its turn, which the wait below reports.
            let queued = tokio::time::Instant::now();
            let _ = has_turn.await;
            deadline += queued.elapsed();
            // Looked at here only, waiting for the bucket: never while an
            // object is applied or the file put in place (whose time is
            // not counted either, below). What other restores' bytes have
            // added to this read's limits does not count: looked at again
            // when the time is up, as it grows while the read lasts. The
            // reads still under way are aborted as `reading` is dropped.
            let waited = tokio::time::Instant::now();
            let read = loop {
                let until = deadline + time.lifted();
                match tokio::time::timeout_at(until, &mut task.0).await {
                    Ok(read) => break read,
                    Err(_) if deadline + time.lifted() > tokio::time::Instant::now() => {}
                    Err(_) => {
                        let lifted = lifted.saturating_add(time.lifted());
                        tracing::warn!(prefix = %self.prefix, object = %key, ?allowed, ?lifted, objects, "a restore ran out of time");
                        return Err(ReplicaError::Bucket(format!(
                            "{}: the restore ran out of time ({allowed:?} for {objects} objects, {lifted:?} for other restores' reads) reading {key}",
                            self.prefix
                        )));
                    }
                }
            };
            // Never more than it waited for this object: a read that
            // was quick leaves nothing for the next ones.
            let lift = time.lifted().min(waited.elapsed());
            deadline += lift;
            lifted = lifted.saturating_add(lift);
            let bytes = read.map_err(|e| ReplicaError::Local(e.to_string()))??;
            ahead = ahead.saturating_sub(size);
            let applying = tokio::time::Instant::now();
            let applied = tokio::task::spawn_blocking(move || {
                restorer
                    .apply(&bytes)
                    .map(|()| restorer)
                    .map_err(|e| ReplicaError::Restore(format!("{key}: {e}")))
            })
            .await
            .map_err(|e| ReplicaError::Local(e.to_string()))?;
            restorer = applied?;
            deadline += applying.elapsed();
        }
        let head = plan.head();
        let sum = tokio::task::spawn_blocking(move || {
            let sum = restorer
                .finish()
                .map_err(|e| ReplicaError::Restore(e.to_string()))?;
            set_aside(&path).map_err(|e| ReplicaError::Local(e.to_string()))?;
            std::fs::rename(&tmp, &path).map_err(|e| ReplicaError::Local(e.to_string()))?;
            write_sidecar(&path, head, sum)?;
            Ok::<_, ReplicaError>(sum)
        })
        .await
        .map_err(|e| ReplicaError::Local(e.to_string()))??;
        cleanup.0 = None; // renamed into place
        tracing::info!(prefix = %self.prefix, head = ?plan.head(), segments = plan.segments.len(), "restored from the bucket");
        Ok(sum)
    }

    /// The next transaction id; a head at `u64::MAX` (only a corrupt
    /// bucket could hold one) stops replication instead of wrapping.
    fn next_txid(&self) -> Result<u64, ReplicaError> {
        self.head
            .txid
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Restore("transaction id overflow".into()))
    }

    /// Whether the first upload must be a snapshot (the bucket was empty).
    pub(crate) fn needs_snapshot(&self) -> bool {
        self.needs_snapshot
    }

    /// Uploads a snapshot of the whole database: an actor's first at a new
    /// position, the others at the current one. The caller captures it
    /// right after `start_capture`, or with nothing pending.
    ///
    /// `Ok(true)`: this call created the snapshot, and it was acknowledged
    /// (completed while the lease was valid), as garbage collection
    /// requires (ADR 0012); `Ok(false)`: one at the head that was already
    /// there.
    ///
    /// The first one and the one after a delete are bounded and sent again
    /// as a segment is ([`Self::put_bounded`]); a periodic one gets a
    /// single short attempt ([`periodic_snapshot_bound`]). A snapshot at
    /// the head that fails stops nothing: the segments already uploaded
    /// are the history. For a periodic one the count of segments since the
    /// last snapshot stays, and [`Self::snapshot_due`] says when it is
    /// tried again; the one after a delete changes nothing of that. Every
    /// upload not acknowledged is counted
    /// (`wasmkeep_snapshot_upload_failures_total`), whether or not the
    /// bucket ends up holding it: an attempt given up may still land.
    pub(crate) async fn ship_snapshot(
        &mut self,
        all: &Changes,
        kind: SnapshotKind,
    ) -> Result<bool, ReplicaError> {
        let position = match kind {
            SnapshotKind::First => SegmentId {
                generation: self.generation,
                txid: self.next_txid()?,
            },
            SnapshotKind::Periodic | SnapshotKind::AfterDelete => self.head,
        };
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        let bytes = all
            .to_ltx(1, position.txid.max(1), crate::alarm::now_ms_i64())
            .map_err(|e| ReplicaError::Local(e.to_string()))?;
        let key = snapshot_key(position);
        if kind != SnapshotKind::First {
            let sent = if kind == SnapshotKind::Periodic {
                // Counted as failed until it is known to have succeeded,
                // so an upload cancelled half-way waits like one that
                // failed.
                self.snapshot_failed();
                let bound = periodic_snapshot_bound(self.upload_idle, bytes.len());
                self.put_once(&key, bytes, bound).await
            } else {
                self.put_bounded(&key, bytes, "snapshot").await
            };
            return match sent {
                Ok(()) => {
                    self.snapshot_taken();
                    Ok(true)
                }
                // The same position holds the same database: already there.
                Err(ReplicaError::Exists) => {
                    self.snapshot_taken();
                    Ok(false)
                }
                Err(e) => {
                    crate::metrics::inc(&crate::metrics::METRICS.snapshot_upload_failures);
                    Err(e)
                }
            };
        }
        self.failed = Some("an upload was interrupted".into());
        if let Err(e) = self.put_bounded(&key, bytes, "snapshot").await {
            if !matches!(e, ReplicaError::Exists) {
                crate::metrics::inc(&crate::metrics::METRICS.snapshot_upload_failures);
            }
            self.failed = Some(e.to_string());
            return Err(e);
        }
        self.head = position;
        self.needs_snapshot = false;
        self.snapshot_taken();
        if let Err(e) = self.record(all.post_apply).await {
            self.failed = Some(e.to_string());
            return Err(e);
        }
        self.failed = None;
        Ok(true)
    }

    /// The lease generation this replica uploads under: the owner's, for
    /// as long as this replica is used (a new hold builds a new replica).
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The generation of the upload the history ends in, as this replica
    /// knows it: the restored head until this owner uploads.
    pub(crate) fn head_generation(&self) -> u64 {
        self.head.generation
    }

    /// The position the bucket and the local database agree on.
    pub(crate) fn head(&self) -> SegmentId {
        self.head
    }

    /// What a garbage-collection pass of this hold needs, detached so it
    /// can run in the background (ADR 0012).
    pub(crate) fn collector(&self) -> Collector {
        Collector {
            store: Arc::clone(&self.store),
            prefix: self.prefix.clone(),
            generation: self.generation,
            lease: self.lease.clone(),
        }
    }

    /// Why replication stopped, if it did.
    pub(crate) fn failed(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    /// Stops replication; the actor must be given up.
    pub(crate) fn fail(&mut self, reason: impl Into<String>) {
        self.failed.get_or_insert_with(|| reason.into());
    }

    /// Uploads one transaction. `Ok` means durable. Refuses changes that do
    /// not follow the current head, and anything after a failure.
    pub(crate) async fn ship(&mut self, changes: &Changes) -> Result<(), ReplicaError> {
        if let Some(reason) = &self.failed {
            return Err(ReplicaError::Local(format!(
                "replication stopped: {reason}"
            )));
        }
        if changes.pre_apply != self.checksum {
            // Something committed locally was never shipped: building on
            // the bucket's head now would fork the history.
            self.fail("local changes do not follow the replicated head");
            return Err(ReplicaError::Local(
                "local database is ahead of the bucket".into(),
            ));
        }
        let segment = Segment {
            id: SegmentId {
                generation: self.generation,
                txid: self.next_txid()?,
            },
            parent_generation: self.head.generation,
        };
        let bytes = changes
            .to_ltx(segment.id.txid, segment.id.txid, crate::alarm::now_ms_i64())
            .map_err(|e| ReplicaError::Local(e.to_string()))?;
        // Marked failed until the upload is known to have succeeded, so a
        // cancelled upload (the event was cut mid-way) stops replication.
        self.failed = Some("an upload was interrupted".into());
        if let Err(e) = self
            .put_bounded(&segment_key(&segment), bytes, "segment")
            .await
        {
            self.failed = Some(e.to_string());
            return Err(e);
        }
        self.head = segment.id;
        self.since_snapshot += 1;
        if let Err(e) = self.record(changes.post_apply).await {
            self.failed = Some(e.to_string());
            return Err(e);
        }
        self.failed = None;
        Ok(())
    }

    /// Replaces the whole database in the history (after `delete-self`):
    /// one segment holding every page of the new database, applied on top
    /// of the current head, so the current chain ends in the new state.
    pub(crate) async fn ship_reset(&mut self, all: Changes) -> Result<(), ReplicaError> {
        let changes = Changes {
            pre_apply: self.checksum,
            ..all
        };
        self.ship(&changes).await
    }

    /// Whether a periodic snapshot is due: [`SNAPSHOT_EVERY`] segments
    /// since the last one. One that failed is tried again once the actor
    /// has shipped one more segment and [`SNAPSHOT_RETRY_MIN`] has passed
    /// since that try started; if that fails too, after
    /// [`SNAPSHOT_RETRY_GROWTH`] times as many segments, up to
    /// [`SNAPSHOT_EVERY`]. An actor that only reads, or is idle, does not
    /// try again, and neither does one restored on this node after a
    /// hibernation: its wait is taken up again
    /// ([`Self::resume_snapshot_wait`]). Each try captures the whole
    /// database and holds the actor for its upload:
    /// [`PERIODIC_SNAPSHOT_IDLES`] has the sums.
    pub(crate) fn snapshot_due(&self) -> bool {
        self.since_snapshot >= SNAPSHOT_EVERY.max(self.snapshot_retry_at)
            && (self.snapshot_failures == 0
                || tokio::time::Instant::now() >= self.snapshot_retry_after)
    }

    /// A snapshot holds the database at the head: the count starts again.
    fn snapshot_taken(&mut self) {
        self.since_snapshot = 0;
        self.snapshot_failures = 0;
        self.snapshot_retry_at = 0;
        self.snapshot_waits.forget(&self.address);
    }

    /// After a restore: takes up the wait an earlier `Replica` of this
    /// actor left on this node ([`SnapshotWaits`]), so a snapshot that
    /// failed is not tried again by the first event after a hibernation.
    /// Fewer than [`SNAPSHOT_EVERY`] segments restored means a snapshot
    /// landed since (an upload given up, or another node's): nothing to
    /// wait for.
    fn resume_snapshot_wait(&mut self) {
        let Some(wait) = self.snapshot_waits.get(&self.address) else {
            return;
        };
        if self.since_snapshot < SNAPSHOT_EVERY {
            self.snapshot_waits.forget(&self.address);
            return;
        }
        self.snapshot_failures = wait.failures;
        self.snapshot_retry_at = self
            .since_snapshot
            .saturating_add(wait.txid.saturating_sub(self.head.txid));
        self.snapshot_retry_after = wait.after;
    }

    /// A periodic snapshot failed (or is starting): when the next try is
    /// allowed.
    fn snapshot_failed(&mut self) {
        // 1 segment after the first failure in a row, then 8, then 64.
        let wait = SNAPSHOT_RETRY_GROWTH
            .checked_pow(self.snapshot_failures)
            .map_or(SNAPSHOT_EVERY, |w| w.min(SNAPSHOT_EVERY));
        self.snapshot_failures = self.snapshot_failures.saturating_add(1);
        self.snapshot_retry_at = self.since_snapshot.saturating_add(wait);
        self.snapshot_retry_after = tokio::time::Instant::now() + self.snapshot_retry_min;
        self.snapshot_waits.set(
            &self.address,
            SnapshotWait {
                failures: self.snapshot_failures,
                txid: self.head.txid.saturating_add(wait),
                after: self.snapshot_retry_after,
            },
        );
    }

    /// A periodic snapshot's upload: one conditional create (one call
    /// to the client, which may send it again after an error answer),
    /// given up after `bound` and not called again, then the
    /// acknowledgement rule as
    /// in [`Self::put_bounded`]. What is given up may still land; nothing
    /// depends on it. A create refused as existing counts as "already
    /// there" only if the object is seen within the same bound.
    async fn put_once(
        &self,
        key: &str,
        bytes: Vec<u8>,
        bound: std::time::Duration,
    ) -> Result<(), ReplicaError> {
        use tokio::time::timeout_at;
        let location = StorePath::from(format!("{}{key}", self.prefix));
        let until = tokio::time::Instant::now() + bound;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let sent = self
            .store
            .put_opts(&location, PutPayload::from(bytes), opts);
        let reason = match timeout_at(until, sent).await {
            Ok(Ok(_)) => return self.acknowledged(),
            Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                match timeout_at(until, self.store.head(&location)).await {
                    Ok(Ok(_)) => return Err(ReplicaError::Exists),
                    Ok(Err(e)) => format!("refused as existing, then: {e}"),
                    Err(_) => "refused as existing, then no answer in time".into(),
                }
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => format!("no answer within {bound:?}"),
        };
        tracing::warn!(object = %location, what = "snapshot", %reason, "a periodic snapshot's upload was given up");
        Err(ReplicaError::Bucket(format!("{location}: {reason}")))
    }

    async fn record(&mut self, sum: DbChecksum) -> Result<(), ReplicaError> {
        self.checksum = sum;
        let (path, head) = (self.db_path.clone(), self.head);
        tokio::task::spawn_blocking(move || write_sidecar(&path, head, sum))
            .await
            .map_err(|e| ReplicaError::Local(e.to_string()))?
    }

    /// A segment's upload, or a first or after-delete snapshot's (`what`,
    /// for the log): a
    /// conditional create, each attempt bounded and sent again when the
    /// bucket does not answer, the whole upload within one deadline, then
    /// the acknowledgement rule: the upload counts only if it completed
    /// while the lease was still valid. A resend that finds the object (an
    /// earlier attempt landed with its reply lost, or the client's own
    /// retry did) reads it back: this upload's own bytes are acknowledged,
    /// as any landed upload is; anything else is a conflict. Nobody else
    /// writes these bytes at this key, which names this hold's generation
    /// and the transaction, and every attempt sends the same bytes (a
    /// snapshot's carry the time it was taken, so one taken again at the
    /// same position is a conflict, which its caller reads as "already
    /// there"). A refusal with nothing there yet (a bucket answering 409
    /// to a create racing an earlier attempt still in flight) is sent
    /// again.
    async fn put_bounded(
        &self,
        key: &str,
        bytes: Vec<u8>,
        what: &'static str,
    ) -> Result<(), ReplicaError> {
        use tokio::time::{Instant, timeout_at};
        let location = StorePath::from(format!("{}{key}", self.prefix));
        // Shared, not copied, by each attempt.
        let bytes = bytes::Bytes::from(bytes);
        let megabytes = u32::try_from(bytes.len().div_ceil(1 << 20)).unwrap_or(u32::MAX);
        let per_size = std::time::Duration::from_secs(1).saturating_mul(megabytes);
        let first = self.upload_idle.saturating_add(per_size);
        let deadline = Instant::now()
            + self
                .upload_idle
                .saturating_mul(UPLOAD_BUDGET_IDLES)
                .saturating_add(per_size);
        let mut last: String;
        // Attempts given up unanswered; a refusal with nothing there does
        // not count (the attempt it raced may be about to land), it is
        // sent again after a pause that grows, until the deadline.
        let mut stalled = 0;
        let mut pause = self.upload_idle / 10;
        loop {
            let until = if stalled + 1 >= UPLOAD_ATTEMPTS {
                deadline
            } else {
                deadline.min(Instant::now() + first.saturating_mul(1 << stalled))
            };
            let opts = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            let sent = self
                .store
                .put_opts(&location, PutPayload::from(bytes.clone()), opts);
            match timeout_at(until, sent).await {
                Ok(Ok(_)) => return self.acknowledged(),
                Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                    match self.read_back(&location, &bytes, deadline).await {
                        ReadBack::Ours => {
                            tracing::info!(object = %location, what, "an upload had landed unanswered; acknowledged");
                            return self.acknowledged();
                        }
                        ReadBack::Other => return Err(ReplicaError::Exists),
                        ReadBack::Refused(e) => return Err(e),
                        ReadBack::Missing => {
                            last = "refused as existing, yet not there".into();
                            if !self.lease.is_valid() {
                                last = format!("{last}; the lease ran out meanwhile");
                                break;
                            }
                            tracing::info!(object = %location, what, reason = %last, "an upload raced an earlier attempt; sending it again");
                            let left = deadline.saturating_duration_since(Instant::now());
                            tokio::time::sleep(pause.min(left)).await;
                            pause = pause.saturating_mul(2).min(self.upload_idle);
                        }
                        // Maybe this upload's own: asked again until the
                        // deadline, as a refusal with nothing there is.
                        ReadBack::Failed(e) => {
                            last = format!("reading back an existing object: {e}");
                            if !self.lease.is_valid() {
                                last = format!("{last}; the lease ran out meanwhile");
                                break;
                            }
                            tracing::info!(object = %location, what, reason = %last, "an upload's read-back failed; trying again");
                            let left = deadline.saturating_duration_since(Instant::now());
                            tokio::time::sleep(pause.min(left)).await;
                            pause = pause.saturating_mul(2).min(self.upload_idle);
                        }
                        ReadBack::OutOfTime => {
                            last = "reading back an existing object ran out of time".into();
                            break;
                        }
                    }
                }
                Ok(Err(e)) if definite(&e) => return Err(ReplicaError::Bucket(e.to_string())),
                // A failure a resend may get past (a reset connection, a
                // 5xx once the client's own retries are spent): an attempt
                // used, sent again after a pause; a resend that meets an
                // object it had landed reads it back.
                Ok(Err(e)) => {
                    stalled += 1;
                    last = e.to_string();
                    if stalled >= UPLOAD_ATTEMPTS {
                        break;
                    }
                    if !self.lease.is_valid() {
                        last = format!("{last}; the lease ran out meanwhile");
                        break;
                    }
                    tracing::info!(object = %location, what, reason = %last, "an upload failed; sending it again");
                    let left = deadline.saturating_duration_since(Instant::now());
                    tokio::time::sleep(pause.min(left)).await;
                    pause = pause.saturating_mul(2).min(self.upload_idle);
                }
                Err(_) => {
                    stalled += 1;
                    last = "no answer in time".into();
                    if stalled >= UPLOAD_ATTEMPTS {
                        break;
                    }
                    tracing::info!(object = %location, what, reason = %last, "an upload stalled; sending it again");
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            // No resend once the hold may not act: what lands then would
            // not be acknowledged anyway (`Late`), and the event ends.
            if !self.lease.is_valid() {
                last = format!("{last}; the lease ran out meanwhile");
                break;
            }
        }
        tracing::warn!(object = %location, what, reason = %last, "an upload kept failing");
        Err(ReplicaError::Bucket(format!("{location}: {last}")))
    }
}

/// A failure no resend changes: the bucket refused the request itself (on
/// an upload, `NotFound` is the bucket missing, and a precondition cannot
/// fail a create). An attempt in flight, or a pause, still runs out
/// before a lapse is seen: the bound is one attempt or one pause.
fn definite(e: &object_store::Error) -> bool {
    matches!(
        e,
        object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::NotFound { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
    )
}

impl Replica {
    /// What sits at `location` after a create was refused, read before
    /// `deadline`.
    async fn read_back(
        &self,
        location: &StorePath,
        bytes: &[u8],
        deadline: tokio::time::Instant,
    ) -> ReadBack {
        use tokio::time::timeout_at;
        match timeout_at(deadline, self.store.head(location)).await {
            Err(_) => return ReadBack::OutOfTime,
            Ok(Err(object_store::Error::NotFound { .. })) => return ReadBack::Missing,
            Ok(Err(e)) if definite(&e) => {
                return ReadBack::Refused(ReplicaError::Bucket(e.to_string()));
            }
            Ok(Err(e)) => return ReadBack::Failed(ReplicaError::Bucket(e.to_string())),
            Ok(Ok(meta)) if meta.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) => {
                return ReadBack::Other;
            }
            Ok(Ok(_)) => {}
        }
        let max = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        match timeout_at(
            deadline,
            bounded_get(
                &self.store,
                location,
                max,
                max,
                Some(max),
                &ReadTime::new(max, None),
            ),
        )
        .await
        {
            Err(_) => ReadBack::OutOfTime,
            Ok(Ok(found)) if found.as_ref() == bytes => ReadBack::Ours,
            // Other bytes, or more of them than this upload sent.
            Ok(Ok(_) | Err(ReplicaError::Restore(_))) => ReadBack::Other,
            Ok(Err(e)) => ReadBack::Failed(e),
        }
    }

    /// The acknowledgement rule: an upload counts only if it completed
    /// while the hold could still act.
    fn acknowledged(&self) -> Result<(), ReplicaError> {
        if self.lease.is_valid() {
            Ok(())
        } else {
            Err(ReplicaError::Late)
        }
    }
}

/// What a refused create found at its key.
enum ReadBack {
    /// This upload's own bytes: an earlier attempt landed.
    Ours,
    /// Anything else: a conflict.
    Other,
    /// Nothing yet (the refusal raced an attempt still in flight).
    Missing,
    /// The bucket failed the read; a resend may get past it.
    Failed(ReplicaError),
    /// The bucket refused the read itself (permission, authentication):
    /// no resend changes it.
    Refused(ReplicaError),
    /// The upload's deadline passed first.
    OutOfTime,
}

/// Whether the bucket holds anything for the actor: it was created (and
/// perhaps deleted since: a delete leaves an empty snapshot). One listing
/// request, read no further than its first key; no lease needed.
pub(crate) async fn exists_in_bucket(
    cluster: &Cluster,
    address: &ActorAddress,
) -> Result<bool, ReplicaError> {
    use futures_util::TryStreamExt;
    let prefix = format!(
        "{}actors/{}/{}/{}",
        cluster.store_prefix(),
        address.tenant(),
        address.class(),
        wasmkeep_storage::encode_segment(address.name())
    );
    let base = StorePath::from(prefix);
    let mut listing = cluster.store.list(Some(&base));
    listing
        .try_next()
        .await
        .map(|first| first.is_some())
        .map_err(|e| ReplicaError::Bucket(e.to_string()))
}

/// The highest generation `address`'s keys in the bucket carry (its
/// segments, their parents and its snapshots; 0 for none): an actor's
/// first ownership record goes above it (ADR 0020, section 6).
pub(crate) async fn highest_generation_in_bucket(
    cluster: &Cluster,
    address: &ActorAddress,
) -> Result<u64, ReplicaError> {
    let prefix = format!(
        "{}actors/{}/{}/{}/",
        cluster.store_prefix(),
        address.tenant(),
        address.class(),
        wasmkeep_storage::encode_segment(address.name())
    );
    let keys = list_with(&cluster.store, &prefix, Some(READ_IDLE)).await?;
    Ok(Listing::from_keys(keys.iter().map(String::as_str)).highest_generation())
}

/// Removes a file when dropped, unless taken out first.
struct RemoveOnDrop(Option<PathBuf>);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Deletes per batch in a collection pass; the lease is checked before each.
const COLLECT_BATCH: usize = 100;

/// Garbage collection of one actor's history by its owner (ADR 0012),
/// detached from the replica: the owner's generation, and a view of its
/// lease, checked before every batch of deletes.
#[derive(Clone)]
pub(crate) struct Collector {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    generation: u64,
    lease: LeaseView,
}

impl Collector {
    /// The owner's generation.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the hold this collector belongs to is still valid.
    pub(crate) fn lease_valid(&self) -> bool {
        self.lease.is_valid()
    }

    /// One pass after the owner's created and acknowledged snapshot at
    /// `snapshot` (the caller checks that): lists the prefix, and deletes
    /// what `collectable` allows that is older than `retention` (by the
    /// bucket's `last_modified`), in batches, while the lease is valid.
    /// Returns how many objects were deleted (objects already gone count
    /// as deleted).
    pub(crate) async fn collect(
        &self,
        snapshot: SegmentId,
        retention: std::time::Duration,
    ) -> Result<usize, ReplicaError> {
        use futures_util::{StreamExt, TryStreamExt};
        if !self.lease.is_valid() {
            return Ok(0);
        }
        let base = StorePath::from(self.prefix.trim_end_matches('/'));
        // As in `list`: compare in the store's encoding, and delete the
        // exact locations the listing returned.
        let encoded = format!("{}/", base.as_ref());
        let metas: Vec<object_store::ObjectMeta> = self
            .store
            .list(Some(&base))
            .try_collect()
            .await
            .map_err(|e| ReplicaError::Bucket(e.to_string()))?;
        let relative: Vec<(String, &object_store::ObjectMeta)> = metas
            .iter()
            .filter_map(|m| {
                let rel = m.location.as_ref().strip_prefix(encoded.as_str())?;
                Some((rel.to_owned(), m))
            })
            .collect();
        let listing = Listing::from_keys(relative.iter().map(|(k, _)| k.as_str()));
        let gone: std::collections::HashSet<String> =
            collectable(&listing, self.generation, snapshot)
                .into_iter()
                .collect();
        let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        let cutoff = crate::alarm::now_ms_i64().saturating_sub(retention_ms);
        let doomed: Vec<StorePath> = relative
            .iter()
            .filter(|(k, m)| {
                gone.contains(k)
                    && (retention.is_zero() || m.last_modified.timestamp_millis() <= cutoff)
            })
            .map(|(_, m)| m.location.clone())
            .collect();
        let mut deleted = 0;
        for batch in doomed.chunks(COLLECT_BATCH) {
            // ADR 0012, condition 3.
            if !self.lease.is_valid() {
                break;
            }
            // The stream outlives the batch: it takes its own copies.
            let owned: Vec<StorePath> = batch.to_vec();
            let locations = futures_util::stream::iter(owned.into_iter().map(Ok)).boxed();
            let results: Vec<object_store::Result<StorePath>> =
                self.store.delete_stream(locations).collect().await;
            for r in results {
                match r {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                        deleted += 1;
                        // Counted as it happens: a pass that fails later
                        // still counts what it deleted.
                        crate::metrics::inc(&crate::metrics::METRICS.history_collected);
                    }
                    Err(e) => return Err(ReplicaError::Bucket(e.to_string())),
                }
            }
        }
        Ok(deleted)
    }
}

/// A restore's read in flight.
struct Reading {
    key: String,
    /// Its listed size.
    size: u64,
    /// The word that it has its turn under the node's limit on reads.
    has_turn: tokio::sync::oneshot::Receiver<()>,
    /// Its time, as the read counts it.
    time: Arc<ReadTime>,
    task: AbortOnDrop<Result<bytes::Bytes, ReplicaError>>,
}

/// A task aborted when dropped (a restore that ends early, or is cut).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads one object for a restore, giving up an attempt when the bucket
/// is silent for [`READ_IDLE`] (no headers, or no next chunk), up to
/// [`READ_ATTEMPTS`] times. An attempt given up is dropped whole: nothing
/// of it is applied, and the next attempt reads the object again. An
/// attempt the bucket refuses, or whose body breaks, is sent again too,
/// after a pause ([`READ_PAUSE`]), unless no resend would change the
/// answer ([`definite`]: the object is gone, say, collected since the
/// listing, and the next restore lists again). Objects
/// larger than `max_object` are refused before their body is read.
/// An attempt is also given up once it has taken [`READ_IDLE`] plus a
/// second per started megabyte of the bytes read over the same link
/// alongside it, this object's included: the most `time` has seen,
/// looked at again at every wait, so a read served last among them still
/// has its time ([`ReadTime`]). `listed` is the size
/// the listing gave: an object of another size is not the one the plan
/// names, and is refused before its body is read (the read-ahead's memory
/// bound counts listed sizes).
async fn bounded_get(
    store: &Arc<dyn ObjectStore>,
    location: &StorePath,
    max_object: u64,
    cap: u64,
    listed: Option<u64>,
    time: &ReadTime,
) -> Result<bytes::Bytes, ReplicaError> {
    use futures_util::StreamExt;
    let mut last = String::new();
    let mut pause = READ_PAUSE;
    'attempts: for attempt in 1..=READ_ATTEMPTS {
        if attempt > 1 {
            time.next_try();
        }
        // A failure a resend may get past: an attempt used, then a pause.
        let refused = async |e: object_store::Error, pause: &mut std::time::Duration| {
            if attempt < READ_ATTEMPTS {
                tracing::info!(object = %location, reason = %e, "a restore's read failed; sending it again");
                tokio::time::sleep(*pause).await;
                *pause = pause.saturating_mul(2);
            }
            e.to_string()
        };
        // The attempt's whole time, from its request, grown as reads join.
        let start = tokio::time::Instant::now();
        let mut limit = time.look().0;
        let got = match tokio::time::timeout_at(
            (start + limit).min(start + READ_IDLE),
            store.get(location),
        )
        .await
        {
            Ok(Ok(got)) => got,
            Ok(Err(e)) if definite(&e) => return Err(ReplicaError::Bucket(e.to_string())),
            Ok(Err(e)) => {
                last = refused(e, &mut pause).await;
                continue;
            }
            Err(_) => {
                last = format!("no answer within {READ_IDLE:?}");
                tracing::info!(object = %location, reason = %last, "a restore's read stalled; sending it again");
                continue;
            }
        };
        let size = got.meta.size;
        if let Some(listed) = listed
            && size != listed
        {
            return Err(ReplicaError::Restore(format!(
                "{location}: {size} bytes, where the listing said {listed}"
            )));
        }
        if size > max_object {
            return Err(ReplicaError::Restore(format!(
                "{location}: {size} bytes, more than a database under the {cap}-byte cap can need"
            )));
        }
        let mut body = got.into_stream();
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        loop {
            let now = tokio::time::Instant::now();
            limit = time.look().0;
            let until = start + limit;
            match tokio::time::timeout_at(until.min(now + READ_IDLE), body.next()).await {
                Ok(Some(Ok(chunk))) => bytes.extend_from_slice(&chunk),
                Ok(Some(Err(e))) if definite(&e) => {
                    return Err(ReplicaError::Bucket(e.to_string()));
                }
                Ok(Some(Err(e))) => {
                    last = refused(e, &mut pause).await;
                    continue 'attempts;
                }
                Ok(None) => return Ok(bytes.into()),
                Err(_) if tokio::time::Instant::now() >= start + limit => {
                    last = format!(
                        "read too slowly ({} of {size} bytes in {limit:?})",
                        bytes.len()
                    );
                    tracing::info!(object = %location, reason = %last, "a restore's read was too slow; sending it again");
                    continue 'attempts;
                }
                Err(_) => {
                    last = format!("body stalled after {} of {size} bytes", bytes.len());
                    tracing::info!(object = %location, reason = %last, "a restore's read stalled; sending it again");
                    continue 'attempts;
                }
            }
        }
    }
    tracing::warn!(object = %location, reason = %last, "a restore's read kept failing");
    Err(ReplicaError::Bucket(format!(
        "{location}: {last}, {READ_ATTEMPTS} times"
    )))
}

/// Keys under `prefix`, relative to it; with `idle`, giving up an attempt when the bucket is
/// silent that long (no next key: a page of up to 1,000 is one request),
/// up to [`READ_ATTEMPTS`] times. A long history lists slowly but is
/// never cut while pages keep coming.
async fn list_with(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    idle: Option<std::time::Duration>,
) -> Result<Vec<String>, ReplicaError> {
    Ok(list_sized(store, prefix, idle)
        .await?
        .into_iter()
        .map(|(key, _)| key)
        .collect())
}

/// [`list_with`], with each object's size.
async fn list_sized(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    idle: Option<std::time::Duration>,
) -> Result<Vec<(String, u64)>, ReplicaError> {
    use futures_util::StreamExt;
    let base = StorePath::from(prefix.trim_end_matches('/'));
    // Listed locations come back in the store's encoding, where a `%` in a
    // segment (as in an escaped actor name) is escaped again: compare with
    // the prefix in that same encoding, not with the raw string.
    let encoded = format!("{}/", base.as_ref());
    let attempts = if idle.is_some() { READ_ATTEMPTS } else { 1 };
    'attempts: for _ in 0..attempts {
        let mut listing = store.list(Some(&base));
        let mut keys = Vec::new();
        loop {
            let next = match idle {
                Some(idle) => match tokio::time::timeout(idle, listing.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        tracing::info!(prefix, "a restore's listing stalled; listing again");
                        continue 'attempts;
                    }
                },
                None => listing.next().await,
            };
            match next {
                Some(Ok(meta)) => {
                    if let Some(key) = meta.location.as_ref().strip_prefix(encoded.as_str()) {
                        keys.push((key.to_owned(), meta.size));
                    }
                }
                Some(Err(e)) => return Err(ReplicaError::Bucket(e.to_string())),
                None => return Ok(keys),
            }
        }
    }
    tracing::warn!(prefix, "a restore's listing kept stalling");
    Err(ReplicaError::Bucket(format!(
        "{prefix}: the listing stalled ({:?} without a key), {attempts} times",
        idle.unwrap_or_default()
    )))
}

/// Reads the page size from a database header (offset 16, 1 means 65536).
fn page_size_of(path: &Path) -> Option<u32> {
    use std::io::Read;
    let mut h = [0u8; 18];
    std::fs::File::open(path).ok()?.read_exact(&mut h).ok()?;
    let raw = u16::from_be_bytes([h[16], h[17]]);
    Some(if raw == 1 { 65_536 } else { u32::from(raw) })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use std::collections::BTreeMap;

    use object_store::memory::InMemory;

    use super::*;

    async fn replica(dir: &Path) -> Replica {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Replica {
            store,
            prefix: "actors/t/c/a/".into(),
            generation: 1,
            lease: LeaseView::always_valid(1),
            head: SegmentId {
                generation: 1,
                txid: 1,
            },
            since_snapshot: 0,
            address: ActorAddress::new("t", "c", "a").unwrap(),
            snapshot_waits: Arc::default(),
            snapshot_failures: 0,
            snapshot_retry_at: 0,
            snapshot_retry_after: tokio::time::Instant::now(),
            snapshot_retry_min: SNAPSHOT_RETRY_MIN,
            db_path: dir.join("a.db"),
            needs_snapshot: false,
            checksum: DbChecksum::from_value(7),
            max_db_bytes: 1 << 30,
            failed: None,
            upload_idle: UPLOAD_IDLE,
        }
    }

    fn changes(pre_apply: DbChecksum) -> Changes {
        Changes {
            commit: 1,
            page_size: 512,
            pages: BTreeMap::from([(1, vec![1u8; 512])]),
            pre_apply,
            post_apply: DbChecksum::from_value(9),
        }
    }

    #[tokio::test]
    async fn changes_that_do_not_follow_the_head_are_refused_for_good() {
        let dir = tempfile::tempdir().unwrap();
        let mut r = replica(dir.path()).await;
        // Something committed locally was never shipped: the next capture's
        // pre-apply checksum is not the head's.
        assert!(r.ship(&changes(DbChecksum::from_value(8))).await.is_err());
        assert!(r.failed().is_some());
        // Not even a change that does follow the head goes out now.
        assert!(r.ship(&changes(DbChecksum::from_value(7))).await.is_err());
        assert!(
            list_with(&r.store, &r.prefix, None)
                .await
                .unwrap()
                .is_empty(),
            "nothing uploaded"
        );
    }

    /// A store whose writes land after this node's lease lapsed: each makes
    /// it lapse while the upload is in flight.
    struct LapseOnWrite {
        inner: InMemory,
        validity: Arc<crate::cluster::NodeValidity>,
    }

    impl std::fmt::Debug for LapseOnWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("LapseOnWrite")
        }
    }

    impl std::fmt::Display for LapseOnWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("LapseOnWrite")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LapseOnWrite {
        async fn put_opts(
            &self,
            location: &StorePath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.validity.invalidate();
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &StorePath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &StorePath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&StorePath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&StorePath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &StorePath,
            to: &StorePath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A store whose next segment uploads (`/wal/` keys) misbehave: hang
    /// without being applied, be applied and then hang (the reply lost),
    /// or be refused as existing without being applied (a bucket's 409 to
    /// a create racing an attempt still in flight).
    struct PutFaults {
        inner: InMemory,
        hang_next: std::sync::atomic::AtomicU32,
        land_then_hang_next: std::sync::atomic::AtomicU32,
        refuse_next: std::sync::atomic::AtomicU32,
        /// The next segment uploads fail at once, not applied (a reset
        /// connection, a 500 after the client's own retries).
        fail_next: std::sync::atomic::AtomicU32,
        /// The next segment reads (a read-back's HEAD or GET) fail.
        fail_get_next: std::sync::atomic::AtomicU32,
        /// Runs `on_put` right before the segment upload of this number
        /// (1 is the first; 0: never): the lease lapsing as a resend goes.
        call_at_put: std::sync::atomic::AtomicU32,
        puts: std::sync::atomic::AtomicU32,
        on_put: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    }

    impl std::fmt::Debug for PutFaults {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("PutFaults")
        }
    }

    impl std::fmt::Display for PutFaults {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("PutFaults")
        }
    }

    fn take(n: &std::sync::atomic::AtomicU32) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        n.try_update(SeqCst, SeqCst, |v| v.checked_sub(1)).is_ok()
    }

    #[async_trait::async_trait]
    impl ObjectStore for PutFaults {
        async fn put_opts(
            &self,
            location: &StorePath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location.as_ref().contains("/wal/") {
                let n = self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if n == self.call_at_put.load(std::sync::atomic::Ordering::SeqCst)
                    && let Some(f) = self.on_put.lock().unwrap().as_ref()
                {
                    f();
                }
            }
            if location.as_ref().contains("/wal/") || location.as_ref().contains("/snapshot/") {
                if take(&self.hang_next) {
                    std::future::pending::<()>().await;
                }
                if take(&self.land_then_hang_next) {
                    self.inner.put_opts(location, payload, opts).await?;
                    return std::future::pending().await;
                }
                if take(&self.refuse_next) {
                    return Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "409 ConditionalRequestConflict".into(),
                    });
                }
                if take(&self.fail_next) {
                    return Err(object_store::Error::Generic {
                        store: "PutFaults",
                        source: "connection reset".into(),
                    });
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &StorePath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &StorePath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location.as_ref().contains("/wal/") && take(&self.fail_get_next) {
                return Err(object_store::Error::Generic {
                    store: "PutFaults",
                    source: "connection reset".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&StorePath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&StorePath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &StorePath,
            to: &StorePath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A replica over a [`PutFaults`] store.
    async fn faulty(dir: &Path, hang: u32, land_then_hang: u32) -> (Replica, Arc<PutFaults>) {
        faulty_with(dir, hang, land_then_hang, 0).await
    }

    async fn faulty_with(
        dir: &Path,
        hang: u32,
        land_then_hang: u32,
        refuse: u32,
    ) -> (Replica, Arc<PutFaults>) {
        let store = Arc::new(PutFaults {
            inner: InMemory::new(),
            hang_next: std::sync::atomic::AtomicU32::new(hang),
            land_then_hang_next: std::sync::atomic::AtomicU32::new(land_then_hang),
            refuse_next: std::sync::atomic::AtomicU32::new(refuse),
            fail_next: std::sync::atomic::AtomicU32::new(0),
            fail_get_next: std::sync::atomic::AtomicU32::new(0),
            call_at_put: std::sync::atomic::AtomicU32::new(0),
            puts: std::sync::atomic::AtomicU32::new(0),
            on_put: std::sync::Mutex::new(None),
        });
        let mut r = replica(dir).await;
        r.store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        r.upload_idle = std::time::Duration::from_millis(200);
        (r, store)
    }

    /// A segment upload that fails at once (a reset connection; the
    /// client's own retries spent) is sent again within the upload's
    /// deadline, not given up.
    #[tokio::test]
    async fn a_segment_upload_that_fails_at_once_is_sent_again() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        store
            .fail_next
            .store(2, std::sync::atomic::Ordering::SeqCst);
        r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
        assert_eq!(r.head.txid, 2);
        assert_eq!(store.fail_next.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A read-back that fails (its HEAD or GET) after an attempt landed
    /// unanswered is tried again within the deadline: the object may be
    /// this upload's own.
    #[tokio::test]
    async fn a_read_back_that_fails_is_tried_again() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 1).await;
        store
            .fail_get_next
            .store(1, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(
            std::time::Duration::from_secs(25),
            r.ship(&changes(DbChecksum::from_value(7))),
        )
        .await
        .expect("the upload waited past the event's limit")
        .unwrap();
        assert_eq!(r.head.txid, 2);
        assert_eq!(
            store
                .fail_get_next
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    /// A refusal no resend can change (permission, authentication) still
    /// ends the upload at once.
    #[tokio::test]
    async fn a_definite_refusal_ends_the_upload() {
        let err = object_store::Error::PermissionDenied {
            path: "x".into(),
            source: "403".into(),
        };
        assert!(definite(&err));
        let err = object_store::Error::Generic {
            store: "s",
            source: "reset".into(),
        };
        assert!(!definite(&err));
    }

    /// A segment upload the bucket leaves hanging is given up and sent
    /// again, well within an event's 30 s, as a restore's reads are: it
    /// used to wait for the client's 30 s timeout (seen in a smoke run
    /// against a real bucket: one actor stalled 30 s, its event cut, the
    /// actor given up).
    #[tokio::test]
    async fn a_hung_segment_upload_is_sent_again() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty(dir.path(), 1, 0).await;
        let started = tokio::time::Instant::now();
        tokio::time::timeout(
            std::time::Duration::from_secs(25),
            r.ship(&changes(DbChecksum::from_value(7))),
        )
        .await
        .expect("the upload waited past the event's limit")
        .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(r.head.txid, 2);
        assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
    }

    /// An upload that landed with its reply lost: the resend's conditional
    /// create finds the object; read back, it is this upload's own bytes,
    /// so the write is acknowledged (not failed, the actor not given up).
    #[tokio::test]
    async fn an_upload_that_landed_unanswered_is_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty(dir.path(), 0, 1).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(25),
            r.ship(&changes(DbChecksum::from_value(7))),
        )
        .await
        .expect("the upload waited past the event's limit")
        .unwrap();
        assert_eq!(r.head.txid, 2);
        assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
    }

    /// A create refused as existing with nothing there yet (AWS answers a
    /// conditional PUT racing one still in flight with 409, which the
    /// client reports as existing) is sent again, not taken for a conflict.
    #[tokio::test]
    async fn a_create_refused_with_nothing_there_is_sent_again() {
        let dir = tempfile::tempdir().unwrap();
        // Three refusals in a row: more than the attempts a stall may use,
        // still resent (they do not count against them).
        let (mut r, store) = faulty_with(dir.path(), 0, 0, 3).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(25),
            r.ship(&changes(DbChecksum::from_value(7))),
        )
        .await
        .expect("the upload waited past the event's limit")
        .unwrap();
        assert_eq!(r.head.txid, 2);
        assert!(r.failed().is_none());
        assert_eq!(
            store.refuse_next.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            list_with(&r.store, &r.prefix, None).await.unwrap().len(),
            1,
            "sent again, and landed"
        );
    }

    /// A read-back that finds this upload's own bytes after the lease
    /// lapsed is still under the acknowledgement rule: `Late`, not durable.
    /// The first attempt lands with its reply lost; the lease lapses as
    /// the resend goes out, which meets the object and reads it back.
    #[tokio::test]
    async fn a_read_back_after_a_lapse_is_late() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 1).await;
        let (view, validity) = LeaseView::lapsable(1);
        r.lease = view;
        *store.on_put.lock().unwrap() = Some(Box::new(move || validity.invalidate()));
        store
            .call_at_put
            .store(2, std::sync::atomic::Ordering::SeqCst);
        let head = r.head;
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Late)
        ));
        assert_eq!(
            store.puts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "resent once"
        );
        assert_eq!(r.head, head, "the head did not move");
        assert!(r.failed().is_some());
    }

    /// After the lease lapsed, nothing is acknowledged and nothing more is
    /// sent: the first attempt lands with its reply lost, and no resend
    /// follows (it would only read back a write the rule refuses, `Late`).
    #[tokio::test]
    async fn an_upload_after_a_lapse_is_not_sent_again() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty(dir.path(), 0, 1).await;
        let (view, validity) = LeaseView::lapsable(1);
        r.lease = view;
        // Lapsed already: `ship` does not look before uploading, so the
        // first attempt lands and hangs; the upload then stops.
        validity.invalidate();
        let head = r.head;
        let Err(ReplicaError::Bucket(why)) = r.ship(&changes(DbChecksum::from_value(7))).await
        else {
            panic!("acknowledged, or another error");
        };
        assert!(why.contains("the lease ran out"), "{why}");
        assert_eq!(
            list_with(&r.store, &r.prefix, None).await.unwrap().len(),
            1,
            "it landed"
        );
        assert_eq!(r.head, head, "the head did not move");
        assert!(r.failed().is_some());
    }

    /// The process's count of snapshot uploads not acknowledged. Other
    /// tests of this binary may add to it meanwhile: compare, never match.
    fn snapshot_failures_counted() -> u64 {
        crate::metrics::METRICS
            .snapshot_upload_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A periodic snapshot gets one attempt, cut short: when the bucket
    /// leaves it hanging it is given up within its bound and not sent
    /// again (it used to get a segment's three attempts and whole
    /// deadline), replication goes on, the count of segments since the
    /// last snapshot is kept, and no next event tries at once.
    #[tokio::test]
    async fn a_hung_periodic_snapshot_is_given_up_after_one_short_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), u32::MAX, 0).await;
        r.since_snapshot = SNAPSHOT_EVERY;
        let before = snapshot_failures_counted();
        let started = std::time::Instant::now();
        let shipped = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            r.ship_snapshot(&changes(DbChecksum::from_value(7)), SnapshotKind::Periodic),
        )
        .await
        .expect("a snapshot upload is bounded");
        let took = started.elapsed();
        assert!(
            matches!(shipped, Err(ReplicaError::Bucket(_))),
            "{shipped:?}"
        );
        // Twice the 200 ms the test's replica allows an idle bucket, not
        // the 1.8 s of a segment's whole upload.
        assert!(took >= std::time::Duration::from_millis(390), "{took:?}");
        assert!(took < std::time::Duration::from_millis(1_200), "{took:?}");
        assert_eq!(
            store.hang_next.load(std::sync::atomic::Ordering::SeqCst),
            u32::MAX - 1,
            "one attempt, not sent again"
        );
        assert_eq!(r.since_snapshot, SNAPSHOT_EVERY, "not reset by a failure");
        assert!(!r.snapshot_due(), "not tried again by the next event");
        assert!(r.failed().is_none(), "replication goes on");
        assert!(snapshot_failures_counted() > before, "counted");
        // Segments still ship; once one has and the wait has passed, the
        // next snapshot is due, and lands.
        store
            .hang_next
            .store(0, std::sync::atomic::Ordering::SeqCst);
        r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
        r.snapshot_retry_after = tokio::time::Instant::now();
        assert!(r.snapshot_due());
        let all = changes(r.checksum);
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
        assert_eq!(r.since_snapshot, 0);
        assert!(!r.snapshot_due());
    }

    /// An actor's first snapshot is bounded too: one hung attempt is given
    /// up and sent again, and the snapshot becomes the head.
    #[tokio::test]
    async fn a_hung_first_snapshot_upload_is_sent_again() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty(dir.path(), 1, 0).await;
        r.needs_snapshot = true;
        let shipped = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            r.ship_snapshot(&changes(DbChecksum::from_value(7)), SnapshotKind::First),
        )
        .await
        .expect("a snapshot upload is bounded");
        assert!(shipped.unwrap(), "created and acknowledged");
        assert_eq!(r.head.txid, 2);
        assert!(!r.needs_snapshot());
        assert!(r.failed().is_none());
    }

    /// A periodic snapshot that keeps failing is tried again only after
    /// one more segment, then eight, then every [`SNAPSHOT_EVERY`]: three
    /// tries at most in any [`SNAPSHOT_EVERY`] segments.
    #[tokio::test]
    async fn a_snapshot_that_keeps_failing_is_tried_less_and_less_often() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        r.snapshot_retry_min = std::time::Duration::ZERO;
        store
            .fail_next
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = SNAPSHOT_EVERY;
        // How many segments pass before each next attempt.
        let mut waits = Vec::new();
        for _ in 0..6 {
            assert!(r.snapshot_due());
            assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
            let mut waited = 0;
            while !r.snapshot_due() {
                r.since_snapshot += 1; // as a shipped segment does
                waited += 1;
            }
            waits.push(waited);
        }
        assert_eq!(waits, [1, 8, 64, 64, 64, 64]);
        // A success starts over: the count, and the waits.
        store
            .fail_next
            .store(0, std::sync::atomic::Ordering::SeqCst);
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
        assert_eq!(r.since_snapshot, 0);
        r.since_snapshot = SNAPSHOT_EVERY;
        store
            .fail_next
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        r.head.txid += 1;
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        assert!(!r.snapshot_due());
        r.since_snapshot += 1;
        assert!(r.snapshot_due(), "one segment later");
    }

    /// The wait after a failed periodic snapshot is the node's, not the
    /// `Replica`'s: the actor's next one, restored after a hibernation
    /// with every segment counted again, takes it up (it used to start
    /// with a snapshot due at once), in time and in segments.
    #[tokio::test]
    async fn a_restored_replica_takes_up_the_wait_of_the_one_before() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        store
            .fail_next
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = SNAPSHOT_EVERY;
        r.head.txid = 100;
        // Two failures in a row: the next try waits for 8 segments.
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        r.since_snapshot += 1;
        r.head.txid += 1;
        r.snapshot_retry_after = tokio::time::Instant::now();
        assert!(r.snapshot_due());
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        let after = r.snapshot_retry_after;
        // As a restore builds it: three segments further, all counted.
        let waits = Arc::clone(&r.snapshot_waits);
        let restored = async |txid: u64, since_snapshot: u64| {
            let (mut next, _) = faulty(dir.path(), 0, 0).await;
            next.snapshot_waits = Arc::clone(&waits);
            next.head.txid = txid;
            next.since_snapshot = since_snapshot;
            next.resume_snapshot_wait();
            next
        };
        let mut next = restored(104, SNAPSHOT_EVERY + 4).await;
        assert_eq!(next.snapshot_failures, 2);
        assert_eq!(next.snapshot_retry_after, after, "the time is kept");
        next.snapshot_retry_after = tokio::time::Instant::now();
        assert!(!next.snapshot_due(), "3 of the 8 segments");
        next.since_snapshot += 4;
        assert!(!next.snapshot_due(), "7 of the 8 segments");
        next.since_snapshot += 1;
        assert!(next.snapshot_due(), "8 segments after the failure");
        // Every segment was there already, but not the time.
        let next = restored(200, SNAPSHOT_EVERY + 99).await;
        assert!(!next.snapshot_due(), "before the minimum time");
        // A snapshot landed meanwhile: nothing left to wait for.
        let next = restored(200, 3).await;
        assert_eq!(next.snapshot_failures, 0);
        assert!(r.snapshot_waits.get(&r.address).is_none());
    }

    /// A snapshot taken removes the node's record of the wait, and the
    /// record holds a bounded number of actors: one more pushes out the
    /// one whose time comes first.
    #[tokio::test]
    async fn the_nodes_record_of_waits_is_cleared_and_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = SNAPSHOT_EVERY;
        store
            .fail_next
            .store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        assert_eq!(
            r.snapshot_waits.get(&r.address).map(|w| w.failures),
            Some(1)
        );
        r.since_snapshot += 1;
        r.snapshot_retry_after = tokio::time::Instant::now();
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
        assert!(r.snapshot_waits.get(&r.address).is_none(), "cleared");

        let waits = SnapshotWaits::default();
        let now = tokio::time::Instant::now();
        let actor = |i: usize| ActorAddress::new("t", "c", format!("a{i}")).unwrap();
        let wait = |i: usize| SnapshotWait {
            failures: 1,
            txid: 1,
            after: now + std::time::Duration::from_secs(i as u64),
        };
        for i in 0..SNAPSHOT_WAITS_MAX {
            waits.set(&actor(i), wait(i + 1));
        }
        // One already there is replaced; nothing is pushed out.
        waits.set(&actor(5), wait(6));
        assert_eq!(waits.entries().len(), SNAPSHOT_WAITS_MAX);
        assert!(waits.get(&actor(0)).is_some());
        waits.set(&actor(SNAPSHOT_WAITS_MAX), wait(SNAPSHOT_WAITS_MAX + 1));
        assert_eq!(waits.entries().len(), SNAPSHOT_WAITS_MAX);
        assert!(waits.get(&actor(0)).is_none(), "the earliest went");
        assert!(waits.get(&actor(1)).is_some());
        assert!(waits.get(&actor(SNAPSHOT_WAITS_MAX)).is_some());
        waits.forget(&actor(1));
        assert!(waits.get(&actor(1)).is_none());
    }

    /// A failed periodic snapshot also waits a minimum time, however many
    /// segments the actor ships meanwhile.
    #[tokio::test]
    async fn a_failed_periodic_snapshot_waits_a_minimum_time_too() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        // An hour: no machine is slow enough for it to pass between the
        // try and the check after it. The test does not wait for it: it
        // moves the end of the wait instead.
        let min = std::time::Duration::from_secs(3600);
        let soon = std::time::Duration::from_millis(50);
        r.snapshot_retry_min = min;
        store
            .fail_next
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = SNAPSHOT_EVERY;
        let before = tokio::time::Instant::now();
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        r.since_snapshot += 10; // a busy actor
        assert!(!r.snapshot_due(), "the segments alone are not enough");
        assert!(
            r.snapshot_retry_after >= before + min,
            "the whole minimum, from the try"
        );
        // Due once the clock is past the end of the wait.
        r.snapshot_retry_after = tokio::time::Instant::now() + soon;
        tokio::time::sleep(soon).await;
        assert!(r.snapshot_due());
        // And the time alone is not enough either.
        store
            .fail_next
            .store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.is_err());
        r.snapshot_retry_after = tokio::time::Instant::now();
        assert!(!r.snapshot_due(), "no segment since: an idle actor waits");
    }

    /// The snapshot after a delete is not a periodic one: when it fails
    /// (every attempt, as a segment's) it is counted, and the periodic
    /// snapshot's schedule is as it was.
    #[tokio::test]
    async fn a_failed_snapshot_after_a_delete_leaves_the_periodic_schedule_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        store
            .fail_next
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = 10;
        let schedule = |r: &Replica| {
            (
                r.since_snapshot,
                r.snapshot_failures,
                r.snapshot_retry_at,
                r.snapshot_retry_after,
            )
        };
        let was = schedule(&r);
        let before = snapshot_failures_counted();
        let shipped = r.ship_snapshot(&all, SnapshotKind::AfterDelete).await;
        assert!(
            matches!(shipped, Err(ReplicaError::Bucket(_))),
            "{shipped:?}"
        );
        assert_eq!(
            store.fail_next.load(std::sync::atomic::Ordering::SeqCst),
            u32::MAX - UPLOAD_ATTEMPTS,
            "sent again, as a segment is"
        );
        assert!(snapshot_failures_counted() > before, "counted");
        assert_eq!(schedule(&r), was);
        assert!(r.failed().is_none(), "replication goes on");
        // One that lands is a snapshot at the head: the count starts again.
        store
            .fail_next
            .store(0, std::sync::atomic::Ordering::SeqCst);
        r.head.txid += 1;
        assert!(
            r.ship_snapshot(&all, SnapshotKind::AfterDelete)
                .await
                .unwrap()
        );
        assert_eq!(r.since_snapshot, 0);
    }

    /// A periodic snapshot's create refused as existing with nothing
    /// there is a failure, tried again later: "already there" is only
    /// said of an object the bucket shows.
    #[tokio::test]
    async fn a_periodic_snapshot_refused_with_nothing_there_is_not_taken() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty_with(dir.path(), 0, 0, 1).await;
        let all = changes(DbChecksum::from_value(7));
        r.since_snapshot = SNAPSHOT_EVERY;
        let shipped = r.ship_snapshot(&all, SnapshotKind::Periodic).await;
        assert!(
            matches!(shipped, Err(ReplicaError::Bucket(_))),
            "{shipped:?}"
        );
        assert_eq!(r.since_snapshot, SNAPSHOT_EVERY);
        assert_eq!(r.snapshot_failures, 1);
        // One that is there: nothing created, and the count starts again.
        r.snapshot_retry_after = tokio::time::Instant::now();
        r.since_snapshot += 1;
        assert!(r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
        r.since_snapshot = SNAPSHOT_EVERY;
        assert!(!r.ship_snapshot(&all, SnapshotKind::Periodic).await.unwrap());
        assert_eq!(r.since_snapshot, 0);
    }

    /// A periodic snapshot's one attempt is allowed a second per megabyte
    /// only up to twice the idle bound: a database too large to upload
    /// within that costs one short attempt per try, never the client's
    /// own timeout.
    #[test]
    fn a_periodic_snapshots_attempt_is_short_whatever_the_database() {
        use std::time::Duration;
        let idle = UPLOAD_IDLE;
        assert_eq!(
            periodic_snapshot_bound(idle, 100 << 10),
            Duration::from_secs(6)
        );
        assert_eq!(
            periodic_snapshot_bound(idle, 3 << 20),
            Duration::from_secs(8)
        );
        assert_eq!(
            periodic_snapshot_bound(idle, 5 << 20),
            Duration::from_secs(10)
        );
        assert_eq!(
            periodic_snapshot_bound(idle, 100 << 20),
            Duration::from_secs(10)
        );
        assert_eq!(
            periodic_snapshot_bound(idle, usize::MAX),
            Duration::from_secs(10)
        );
    }

    /// A read's limit counts every byte in flight on the node; its lift is
    /// the part of it other restores' bytes account for, exactly zero for
    /// a restore alone, and kept for each try that ended.
    #[test]
    fn a_reads_lift_is_what_other_restores_bytes_add_to_its_limit() {
        let s = std::time::Duration::from_secs;
        let (mine, theirs) = (OwnInFlight::default(), OwnInFlight::default());
        let _own = InFlightBytes::new(3 << 20, &mine);
        let time = ReadTime::new(3 << 20, Some(Arc::clone(&mine)));
        assert_eq!(time.look(), (s(8), s(0)), "alone on the node");
        {
            let _other = InFlightBytes::new(10 << 20, &theirs);
            assert_eq!(time.look(), (s(18), s(10)));
        }
        // The most seen stays, as for the limit itself.
        assert_eq!(time.look(), (s(18), s(10)));
        assert_eq!(time.lifted(), s(10));
        time.next_try();
        assert_eq!(time.lifted(), s(20), "a second try has its own");
        // Its own restore's later reads lift the limit, never the lift.
        let _more = InFlightBytes::new(20 << 20, &mine);
        assert_eq!(time.look(), (s(28), s(0)));
        // A read outside a restore counts what it is given, alone.
        assert_eq!(ReadTime::new(1, None).look(), (s(6), s(0)));
    }

    /// A restore's deadline is sized from what it reads: never under the
    /// budget, a second more per started megabyte and per eight objects.
    #[test]
    fn a_restores_deadline_grows_with_what_it_reads() {
        let s = std::time::Duration::from_secs;
        assert_eq!(restore_time(RESTORE_BUDGET, 1, 1), s(32));
        // A snapshot and 63 segments, a megabyte in all.
        assert_eq!(restore_time(RESTORE_BUDGET, 64, 1 << 20), s(39));
        // A database of 1 GiB in one snapshot.
        assert_eq!(restore_time(RESTORE_BUDGET, 1, 1 << 30), s(30 + 1024 + 1));
        assert_eq!(
            restore_time(RESTORE_BUDGET, usize::MAX, u64::MAX),
            RESTORE_BUDGET
                .saturating_add(RESTORE_PER_MIB.saturating_mul(u32::MAX))
                .saturating_add(RESTORE_PER_AHEAD.saturating_mul(u32::MAX))
        );
    }

    /// When every attempt hangs, the upload fails within its deadline
    /// (4 idles plus a second per started megabyte), replication stops,
    /// and nothing more is shipped.
    #[tokio::test]
    async fn an_upload_that_keeps_hanging_fails_within_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, _store) = faulty(dir.path(), u32::MAX, 0).await;
        let started = tokio::time::Instant::now();
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Bucket(_))
        ));
        let took = started.elapsed();
        // 4 × 200 ms + 1 s for the one started megabyte.
        assert!(took >= std::time::Duration::from_millis(1_700), "{took:?}");
        assert!(took < std::time::Duration::from_secs(3), "{took:?}");
        assert!(r.failed().is_some());
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Local(_))
        ));
        assert!(
            list_with(&r.store, &r.prefix, None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// Other bytes already at the segment's key are never taken for this
    /// upload's own: still a conflict, replication stops.
    #[tokio::test]
    async fn other_bytes_at_the_segment_key_stay_a_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        let key = segment_key(&Segment {
            id: SegmentId {
                generation: 1,
                txid: 2,
            },
            parent_generation: 1,
        });
        store
            .inner
            .put(
                &StorePath::from(format!("{}{key}", r.prefix)),
                PutPayload::from(b"someone else's".to_vec()),
            )
            .await
            .unwrap();
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Exists)
        ));
        assert!(r.failed().is_some());

        // The same length, other bytes (another timestamp in the LTX
        // header): read back and compared, still a conflict.
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        let same_length = changes(DbChecksum::from_value(7)).to_ltx(2, 2, 1).unwrap();
        store
            .inner
            .put(
                &StorePath::from(format!("{}{key}", r.prefix)),
                PutPayload::from(same_length),
            )
            .await
            .unwrap();
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Exists)
        ));

        // More bytes than this upload sends: a conflict too, not a
        // restore's size error.
        let dir = tempfile::tempdir().unwrap();
        let (mut r, store) = faulty(dir.path(), 0, 0).await;
        store
            .inner
            .put(
                &StorePath::from(format!("{}{key}", r.prefix)),
                PutPayload::from(vec![0u8; 1 << 16]),
            )
            .await
            .unwrap();
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Exists)
        ));
    }

    /// ADR 0007's acknowledgement rule under node leases: an upload that
    /// completes after this node's lease lapsed (its epoch moved, so the
    /// hold may no longer act) is not durable. It lands in the bucket, but `ship` answers `Late`, the
    /// head does not move, and replication stops until the actor is
    /// restored by its next owner.
    #[tokio::test]
    async fn an_upload_that_lands_after_a_lapse_is_late() {
        let dir = tempfile::tempdir().unwrap();
        let (view, validity) = LeaseView::lapsable(1);
        let mut r = replica(dir.path()).await;
        r.store = Arc::new(LapseOnWrite {
            inner: InMemory::new(),
            validity,
        });
        r.lease = view;
        let head = r.head;
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Late)
        ));
        assert_eq!(r.head, head, "the head did not move");
        assert_eq!(
            r.checksum,
            DbChecksum::from_value(7),
            "nothing recorded as replicated"
        );
        assert!(
            r.failed()
                .is_some_and(|why| why.contains("after the lease ran out")),
            "replication stopped, saying why: {:?}",
            r.failed()
        );
        assert_eq!(
            list_with(&r.store, &r.prefix, None).await.unwrap().len(),
            1,
            "the upload landed, unacknowledged"
        );
        // Nothing after it goes out.
        assert!(matches!(
            r.ship(&changes(DbChecksum::from_value(7))).await,
            Err(ReplicaError::Local(m)) if m.contains("replication stopped")
        ));
    }

    #[tokio::test]
    async fn a_change_that_follows_the_head_is_shipped_and_becomes_the_head() {
        let dir = tempfile::tempdir().unwrap();
        let mut r = replica(dir.path()).await;
        r.ship(&changes(DbChecksum::from_value(7))).await.unwrap();
        assert_eq!(r.head.txid, 2);
        assert_eq!(r.checksum, DbChecksum::from_value(9));
        assert_eq!(list_with(&r.store, &r.prefix, None).await.unwrap().len(), 1);
    }
}
