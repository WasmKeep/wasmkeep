//! Restoring an actor's database from the bucket: the plan, the bounded reads
//! and the limits they share, and the listings a restore uses.
//!
//! The reads and the listings are the replica's one reader and one lister
//! ([`bucket`]) under a restore's rules: [`bounded_get`] and
//! [`list_listed`].

use super::*;

/// The encoded bytes of the segments a restore replays after its snapshot,
/// from the listing's sizes: what a batched hold counts toward a snapshot
/// by bytes when it is acquired again (ADR 0034, section 7).
pub(super) fn history_bytes(
    plan: &wasmkeep_cluster::replication::Plan,
    keys: &[String],
    sizes: &std::collections::HashMap<String, u64>,
) -> u64 {
    let wanted: std::collections::HashSet<SegmentId> = plan.segments.iter().copied().collect();
    keys.iter()
        .filter(|k| {
            Listing::from_keys([k.as_str()])
                .segments
                .first()
                .is_some_and(|s| wanted.contains(&s.id))
        })
        .map(|k| sizes.get(k).copied().unwrap_or(0))
        .fold(0, u64::saturating_add)
}

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
pub(super) const READ_IDLE: std::time::Duration = std::time::Duration::from_secs(5);
/// A restore reads up to this many objects ahead of the one it applies
/// (each on a task of its own), and up to [`READ_AHEAD_BYTES`] of them by
/// the listing's sizes: one slow route no longer makes a chain's reads add
/// up. Always at least one, however large.
pub(super) const READ_AHEAD: usize = 8;
pub(super) const READ_AHEAD_BYTES: u64 = 32 << 20;
/// One node's restores together read at most this many objects at once,
/// and hold at most [`RESTORE_READ_MIB`] of them: one kill starts a
/// takeover of every actor its node held, each restore reading ahead, and
/// before read-ahead each read one object at a time. A read waits for its
/// slot, then for its megabytes (by the listing's size, at least one, at
/// most all), before its time starts. Process-wide.
pub(super) const RESTORE_READS: usize = 32;
pub(super) const RESTORE_READ_MIB: u32 = 256;
/// The listed bytes of the restore reads under way on this node (permits
/// count whole megabytes; a read's time counts bytes). Each restore's own
/// part ([`OwnInFlight`]) changes under the same lock, so the two are
/// always read as one: a restore alone on the node sees exactly its own
/// bytes here.
pub(super) static RESTORE_READ_IN_FLIGHT: std::sync::Mutex<u64> = std::sync::Mutex::new(0);

/// One restore's part of [`RESTORE_READ_IN_FLIGHT`].
pub(super) type OwnInFlight = Arc<std::sync::atomic::AtomicU64>;

/// The bytes in flight on the node, and those of them that are `restore`'s.
fn in_flight(restore: &OwnInFlight) -> (u64, u64) {
    let node = RESTORE_READ_IN_FLIGHT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    (*node, restore.load(std::sync::atomic::Ordering::Relaxed))
}

/// Counts a read's bytes in [`RESTORE_READ_IN_FLIGHT`], and in its
/// restore's part of it, while it lives.
pub(super) struct InFlightBytes(u64, OwnInFlight);

impl InFlightBytes {
    pub(super) fn new(bytes: u64, restore: &OwnInFlight) -> Self {
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

/// The started megabytes of `bytes`, saturating at `u32::MAX`: what the
/// per-megabyte time and permit allowances count.
pub(crate) fn started_mib(bytes: impl TryInto<u64>) -> u32 {
    u32::try_from(bytes.try_into().unwrap_or(u64::MAX).div_ceil(1 << 20)).unwrap_or(u32::MAX)
}

/// The limit of one try of a read: [`READ_IDLE`] plus a second per
/// started megabyte of `bytes`, the bytes read over the same link
/// alongside it, its own included.
fn read_limit(bytes: u64) -> std::time::Duration {
    let megabytes = started_mib(bytes);
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
pub(super) struct ReadTime {
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
    pub(super) fn new(ahead: u64, restore: Option<OwnInFlight>) -> Self {
        Self {
            restore,
            all: ahead.into(),
            own: ahead.into(),
            ended: 0.into(),
        }
    }

    /// Looks at the bytes in flight now: the current try's limit, and its
    /// lift.
    pub(super) fn look(&self) -> (std::time::Duration, std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let (node, own) = self.restore.as_ref().map_or((0, 0), in_flight);
        let all = self.all.fetch_max(node, Relaxed).max(node);
        let own = self.own.fetch_max(own, Relaxed).max(own);
        let limit = read_limit(all);
        (limit, limit.saturating_sub(read_limit(own)))
    }

    /// A try ended and another follows: its lift stays counted.
    pub(super) fn next_try(&self) {
        let lift = u64::try_from(self.look().1.as_millis()).unwrap_or(u64::MAX);
        self.ended
            .fetch_add(lift, std::sync::atomic::Ordering::Relaxed);
    }

    /// The lifts of its tries so far, the current one included.
    pub(super) fn lifted(&self) -> std::time::Duration {
        let ended = self.ended.load(std::sync::atomic::Ordering::Relaxed);
        std::time::Duration::from_millis(ended).saturating_add(self.look().1)
    }
}

/// A read's place under the node's limits on reads: one of
/// [`RESTORE_READS`], its megabytes of [`RESTORE_READ_MIB`], and its
/// bytes counted in flight. Held while the bucket is asked and answers,
/// and given up for the pause before a resend ([`READ_PAUSE`]): a read
/// that only waits keeps no other restore's read waiting, and its bytes,
/// which nobody is reading, add nothing to the other reads' limits. A
/// read keeps nothing of a failed try's answer while it is without its
/// megabytes, which are what bounds the memory of the node's reads.
pub(super) struct Turn {
    /// The app reading (`tenant/app`), for its share of the limits.
    app: Arc<str>,
    megabytes: u32,
    counted: u64,
    restore: OwnInFlight,
    /// Whether the read has its place, for the restore that waits for
    /// it: waiting for a place is not the restore's time.
    has: tokio::sync::watch::Sender<bool>,
    /// Its app's shares of the slots and megabytes (taken first, given
    /// back last), the slot, the megabytes, and its bytes in flight.
    #[allow(clippy::type_complexity)]
    held: Option<(
        crate::shares::SharePermit,
        crate::shares::SharePermit,
        tokio::sync::SemaphorePermit<'static>,
        tokio::sync::SemaphorePermit<'static>,
        InFlightBytes,
    )>,
}

impl Turn {
    /// The place of a read of `size` listed bytes, not taken yet.
    pub(super) fn new(
        size: u64,
        restore: OwnInFlight,
        has: tokio::sync::watch::Sender<bool>,
        app: Arc<str>,
    ) -> Self {
        // One read takes at most a read-ahead's worth: a larger object
        // would take the whole budget, and the fair semaphore would hold
        // every other restore behind it. Its bytes are counted as its
        // permits are, or one large object would lift every read's limit
        // past any bound.
        Self {
            app,
            megabytes: started_mib(size).clamp(1, RESTORE_READ_ONE_MIB),
            counted: size.min(u64::from(RESTORE_READ_ONE_MIB) << 20),
            restore,
            has,
            held: None,
        }
    }

    /// Waits for its place: a slot, then its megabytes.
    pub(super) async fn take(&mut self) -> Result<(), ReplicaError> {
        self.has.send_replace(false);
        // Its app's shares first (ADR 0038, section 8): an app that fills
        // its own never keeps another's read from its slots.
        let slot_share = RESTORE_SLOT_SHARES.acquire(&self.app, 1).await;
        let bytes_share = RESTORE_BYTE_SHARES
            .acquire(&self.app, u64::from(self.megabytes))
            .await;
        // Never closed: a failed acquire would be a bug.
        let (Ok(slot), Ok(bytes)) = (
            RESTORE_READ_SLOTS.acquire().await,
            RESTORE_READ_BYTES.acquire_many(self.megabytes).await,
        ) else {
            return Err(ReplicaError::Local("restore read limit closed".into()));
        };
        // Its time counts every byte read alongside it on this node,
        // other restores' included, as it is while the read lasts (reads
        // that join later count too).
        let counted = InFlightBytes::new(self.counted, &self.restore);
        self.held = Some((slot_share, bytes_share, slot, bytes, counted));
        self.has.send_replace(true);
        Ok(())
    }

    /// Pauses for `pause` without its place, then waits for one again.
    /// The pause is the restore's time, as the failed try was; the wait
    /// for a place after it is not, as the first was not.
    async fn pause(&mut self, pause: std::time::Duration) -> Result<(), ReplicaError> {
        self.held = None;
        tokio::time::sleep(pause).await;
        self.take().await
    }
}

/// The most of [`RESTORE_READ_MIB`] one read takes, however large its object.
const RESTORE_READ_ONE_MIB: u32 = (READ_AHEAD_BYTES >> 20) as u32;
pub(super) static RESTORE_READ_SLOTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(RESTORE_READS);
pub(super) static RESTORE_READ_BYTES: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(RESTORE_READ_MIB as usize);
/// [`RESTORE_READ_SLOTS`] and [`RESTORE_READ_BYTES`] divided between the
/// apps a process serves (ADR 0038, section 8), checked before them.
pub(crate) static RESTORE_SLOT_SHARES: std::sync::LazyLock<Arc<crate::shares::Shares>> =
    std::sync::LazyLock::new(|| Arc::new(crate::shares::Shares::new(RESTORE_READS as u64)));
pub(crate) static RESTORE_BYTE_SHARES: std::sync::LazyLock<Arc<crate::shares::Shares>> =
    std::sync::LazyLock::new(|| Arc::new(crate::shares::Shares::new(u64::from(RESTORE_READ_MIB))));
/// Attempts per read before the restore fails (the next event restores
/// again, from a fresh listing).
pub(super) const READ_ATTEMPTS: u32 = 3;

/// A read the bucket refuses with an error a resend may get past (a 5xx
/// or a reset connection once the client's own retries are spent, a body
/// cut half-way) is sent again after this pause, doubled each time, and
/// counts among the read's [`READ_ATTEMPTS`]: a GET changes nothing, so
/// sending it again is always safe. The read gives up its place under
/// the node's limits on reads while it pauses ([`Turn`]). Without a
/// resend one such answer among a chain's reads failed the whole restore
/// and the event that woke the actor.
pub(super) const READ_PAUSE: std::time::Duration = std::time::Duration::from_millis(500);

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
/// most 256 s and an object's, three tries, 768 s. The waits that count
/// (for the bucket to give the next object, the read having its place)
/// therefore end within the allowed time plus 768 s per object read:
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
///   for, as it is not in a read's own time, or waits for it again after
///   the pause before a resend ([`Turn`]; the pause itself counts). The
///   queue always moves, since every read holding a place in it is
///   bounded; while it lasts, the deadline does not bound what a caller
///   waits;
/// - checking the local file, creating the file the database is built
///   in, applying an object to it or putting the finished database in
///   place: local work, sized by the database and not by the compressed
///   bytes read, and never cut. The reads ahead go on meanwhile.
pub(crate) const RESTORE_BUDGET: std::time::Duration = READ_IDLE.saturating_mul(6);
pub(super) const RESTORE_PER_MIB: std::time::Duration = std::time::Duration::from_secs(1);
pub(super) const RESTORE_PER_AHEAD: std::time::Duration = std::time::Duration::from_secs(1);

/// The time a restore's reads are allowed, counted from `budget`: see
/// [`RESTORE_BUDGET`].
pub(super) fn restore_time(
    budget: std::time::Duration,
    objects: usize,
    bytes: u64,
) -> std::time::Duration {
    let megabytes = started_mib(bytes);
    let rounds = u32::try_from(objects.div_ceil(READ_AHEAD)).unwrap_or(u32::MAX);
    budget
        .saturating_add(RESTORE_PER_MIB.saturating_mul(megabytes))
        .saturating_add(RESTORE_PER_AHEAD.saturating_mul(rounds))
}

/// How long discovery of a recorded head may take, all listings and
/// back-offs included (ADR 0035, section 3).
const DISCOVERY: std::time::Duration = std::time::Duration::from_secs(15);

/// The pauses between the at most ten listings of that discovery (ms).
const RELIST_MS: [u64; 9] = [100, 200, 400, 800, 1_600, 1_600, 1_600, 1_600, 1_600];

/// What a discovery that used all ten listings reports: a link every one
/// of them showed missing is a corruption (500); otherwise a retryable
/// refusal (503) (ADR 0035, S6).
fn gave_up(why: String, missing_link: bool) -> ReplicaError {
    if missing_link {
        ReplicaError::Restore(format!(
            "{why}, still after the discovery bound: the history is incomplete"
        ))
    } else {
        ReplicaError::Visibility(why)
    }
}

/// Whether the bucket holds anything for the actor: it was created (and
/// perhaps deleted since: a delete leaves an empty snapshot). One listing
/// request, read no further than its first key; no lease needed.
pub(crate) async fn exists_in_bucket(
    cluster: &Cluster,
    address: &ActorAddress,
) -> Result<bool, ReplicaError> {
    use futures_util::TryStreamExt;
    let prefix = cluster.history_key(address);
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
    let prefix = format!("{}/", cluster.history_key(address));
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

/// A restore's read in flight.
struct Reading {
    key: String,
    /// Its listed size.
    size: u64,
    /// Whether it has its place under the node's limits on reads
    /// ([`Turn`]).
    has_turn: tokio::sync::watch::Receiver<bool>,
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
/// after a pause ([`READ_PAUSE`]) during which `turn`, the read's place
/// under the node's limits, is given up, unless no resend would change the
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
/// bound counts listed sizes). A body is as long as its answer declared:
/// a longer one is refused as it passes that size, a shorter one is an
/// attempt given up.
///
/// These are a restore's rules ([`RestoreRead`]) for the replica's one
/// reader ([`bucket::read`]); the request is unconditional.
pub(super) async fn bounded_get(
    store: &Arc<dyn ObjectStore>,
    location: &StorePath,
    max_object: u64,
    cap: u64,
    listed: Option<u64>,
    time: &ReadTime,
    turn: Option<&mut Turn>,
) -> Result<bytes::Bytes, ReplicaError> {
    let rules = RestoreRead {
        location,
        max_object,
        cap,
        listed,
        time,
        turn,
        pause: READ_PAUSE,
        last: String::new(),
    };
    bucket::read(store.as_ref(), location, rules).await
}

/// A restore's rules for a read ([`bounded_get`]).
struct RestoreRead<'a> {
    location: &'a StorePath,
    max_object: u64,
    cap: u64,
    listed: Option<u64>,
    time: &'a ReadTime,
    turn: Option<&'a mut Turn>,
    /// The pause before the next resend of a refused attempt: twice as
    /// long each time.
    pause: std::time::Duration,
    /// Why the last attempt failed.
    last: String,
}

impl bucket::ReadRules for RestoreRead<'_> {
    type Error = ReplicaError;

    fn options(&self) -> object_store::GetOptions {
        object_store::GetOptions::default()
    }

    fn next_try(&mut self) {
        self.time.next_try();
    }

    fn limit(&self) -> std::time::Duration {
        self.time.look().0
    }

    fn ends(&self, refusal: &object_store::Error, _: bucket::Phase) -> Option<ReplicaError> {
        definite(refusal).then(|| ReplicaError::Bucket(refusal.to_string()))
    }

    fn accept(&self, answered: &object_store::ObjectMeta) -> Result<(), ReplicaError> {
        let (location, size) = (self.location, answered.size);
        if let Some(listed) = self.listed
            && size != listed
        {
            return Err(ReplicaError::Restore(format!(
                "{location}: {size} bytes, where the listing said {listed}"
            )));
        }
        if size > self.max_object {
            return Err(ReplicaError::Restore(format!(
                "{location}: {size} bytes, more than a database under the {}-byte cap can need",
                self.cap
            )));
        }
        Ok(())
    }

    fn too_long(&self, size: u64) -> ReplicaError {
        ReplicaError::Restore(format!(
            "{}: a body longer than the {size} bytes it declared",
            self.location
        ))
    }

    async fn failed(&mut self, fault: bucket::Fault, more: bool) -> Result<(), ReplicaError> {
        let location = self.location;
        self.last = match fault {
            // A failure a resend may get past: an attempt used, then a
            // pause, without its place under the node's limits on reads
            // if it has one ([`Turn`]).
            bucket::Fault::Refused(e) => {
                if more {
                    tracing::info!(object = %location, reason = %e, "a restore's read failed; sending it again");
                    match self.turn.as_deref_mut() {
                        Some(turn) => turn.pause(self.pause).await?,
                        None => tokio::time::sleep(self.pause).await,
                    }
                    self.pause = self.pause.saturating_mul(2);
                }
                e.to_string()
            }
            bucket::Fault::NoAnswer => {
                let last = format!("no answer within {READ_IDLE:?}");
                tracing::info!(object = %location, reason = %last, "a restore's read stalled; sending it again");
                last
            }
            bucket::Fault::Waited {
                read,
                size,
                limit,
                spent: true,
                ..
            } => {
                let last = format!("read too slowly ({read} of {size} bytes in {limit:?})");
                tracing::info!(object = %location, reason = %last, "a restore's read was too slow; sending it again");
                last
            }
            bucket::Fault::Waited { read, size, .. } => {
                let last = format!("body stalled after {read} of {size} bytes");
                tracing::info!(object = %location, reason = %last, "a restore's read stalled; sending it again");
                last
            }
            bucket::Fault::Short { read, size } => {
                let last = format!("body ended after {read} of {size} bytes");
                tracing::info!(object = %location, reason = %last, "a restore's read ended short; sending it again");
                last
            }
        };
        Ok(())
    }

    fn gave_up(self) -> ReplicaError {
        let (location, last) = (self.location, self.last);
        tracing::warn!(object = %location, reason = %last, "a restore's read kept failing");
        ReplicaError::Bucket(format!("{location}: {last}, {READ_ATTEMPTS} times"))
    }
}

/// Keys under `prefix`, relative to it; with `idle`, giving up an attempt when the bucket is
/// silent that long (no next key: a page of up to 1,000 is one request),
/// up to [`READ_ATTEMPTS`] times. A long history lists slowly but is
/// never cut while pages keep coming.
pub(super) async fn list_with(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    idle: Option<std::time::Duration>,
) -> Result<Vec<String>, ReplicaError> {
    Ok(list_listed(store, prefix, idle)
        .await?
        .into_iter()
        .map(|(key, _)| key)
        .collect())
}

/// [`list_with`], with what the listing said of each object (its exact
/// location, its size, its last-modified time), in the order listed: the
/// replica's one lister ([`bucket::list`]) for those that hold or are
/// taking the actor's lease (a restore, the listing that seeds a first
/// generation, a collection pass). An empty prefix names no actor and
/// lists nothing: what a collection pass lists, it may delete.
pub(super) async fn list_listed(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    idle: Option<std::time::Duration>,
) -> Result<Vec<(String, object_store::ObjectMeta)>, ReplicaError> {
    if prefix.trim_end_matches('/').is_empty() {
        return Ok(Vec::new());
    }
    let limits = bucket::ListLimits {
        idle,
        max: None,
        log: true,
    };
    bucket::list(store.as_ref(), prefix, limits)
        .await
        .map_err(|e| match e {
            bucket::ListError::Refused(e) => ReplicaError::Bucket(e.to_string()),
            bucket::ListError::Over(max) => {
                ReplicaError::Bucket(format!("{prefix}: more than {max} objects listed"))
            }
            bucket::ListError::Stalled { idle, attempts } => ReplicaError::Bucket(format!(
                "{prefix}: the listing stalled ({idle:?} without a key), {attempts} times"
            )),
        })
}

impl Replica {
    /// Prepares the actor's local database for generation `generation`:
    /// restores the bucket's current chain unless the local file is already
    /// at its head. The database must be closed.
    ///
    /// Its reads end within one deadline ([`RESTORE_BUDGET`]). The
    /// database is built beside the local file, so a restore that fails
    /// before it is complete (a read, the deadline, a history that cannot
    /// be restored, an error writing the file being built) leaves the
    /// local file and its sidecar as they were, and removes what it was
    /// building. A complete one is put in place in three local steps: the
    /// file that was there is set aside and its sidecar removed
    /// ([`set_aside`]), the new file is renamed in, its sidecar is
    /// written. A restore that fails in one of these (a local disk
    /// error) has changed the local files: the old file is at
    /// `<db>.replaced`, and either no file is in place or the new one is,
    /// without a sidecar. Neither is used as it is: a local file spares
    /// the reads only when a sidecar names the chain's head and the
    /// file's checksum is the one the sidecar records
    /// ([`Self::local_is_at`]). So the next restore reads the bucket
    /// again, as after any failure: it starts from a fresh listing and
    /// trusts nothing of this one. If the new file was in place, that
    /// next restore sets it aside in turn, over the file set aside
    /// before (one generation is kept). A failed restore is counted in
    /// `wasmkeep_restore_failures_total`.
    pub(crate) async fn restore(
        cluster: &Cluster,
        address: &ActorAddress,
        lease: LeaseView,
        generation: u64,
        db_path: PathBuf,
        max_db_bytes: u64,
    ) -> Result<Self, ReplicaError> {
        // ADR 0035, section 3: the floor the hold inherited (an app that
        // writes the recorded-head format) must be proven by the selected
        // chain; a listing that does not show it is listed again, at most
        // ten times within the discovery deadline, then refused.
        let (floor, key) = lease.floor();
        let deadline = tokio::time::Instant::now() + DISCOVERY;
        let mut attempt = 0;
        let started = std::time::Instant::now();
        // Whether every listing so far showed a link missing: only all ten
        // listings the bound allows, each so, make it a corruption.
        let mut all_missing = true;
        let restored = loop {
            let tried = Self::restore_within(
                cluster,
                address,
                lease.clone(),
                generation,
                db_path.clone(),
                max_db_bytes,
                floor.as_ref(),
            )
            .await;
            all_missing &= matches!(tried, Err(ReplicaError::MissingLink(_)));
            match tried {
                Err(ReplicaError::Visibility(why) | ReplicaError::MissingLink(why))
                    if attempt < RELIST_MS.len() =>
                {
                    // The hold ended: late, whatever the listing showed.
                    if !lease.is_valid() {
                        break Err(ReplicaError::Late);
                    }
                    let pause = jittered(std::time::Duration::from_millis(RELIST_MS[attempt]));
                    // The deadline, spent before the ten listings: nothing
                    // proven either way, retryable.
                    if tokio::time::Instant::now() + pause >= deadline {
                        break Err(ReplicaError::Visibility(why));
                    }
                    attempt += 1;
                    tokio::time::sleep(pause).await;
                }
                // The tenth listing.
                Err(ReplicaError::MissingLink(why)) => break Err(gave_up(why, all_missing)),
                Err(ReplicaError::Visibility(why)) => break Err(ReplicaError::Visibility(why)),
                other => break other,
            }
        };
        if let (Err(ReplicaError::Visibility(why)), Some(key)) = (&restored, &key) {
            cluster.pending.refused(key);
            tracing::warn!(app = %crate::logging::named(cluster.serves()), actor = %crate::logging::actor(address), generation, required = ?floor, attempts = attempt + 1, elapsed = ?started.elapsed(), reason = %why, "refused to activate: the recorded head is not visible");
        }
        let restored = restored.and_then(|replica| {
            // The verified head goes into the hold cell, under its
            // lock and validity check, before activation (ADR 0035,
            // section 2): a hold that lapsed meanwhile is late.
            replica
                .lease
                .verify(replica.verified_head())
                .map_err(|_| ReplicaError::Late)?;
            Ok(replica)
        });
        if let (Ok(_), Some(key)) = (&restored, &key) {
            cluster.pending.clear_cooldown(key);
        }
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
        floor: Option<&Requirement>,
    ) -> Result<Self, ReplicaError> {
        let store = Arc::clone(&cluster.store);
        let prefix = format!("{}/", cluster.history_key(address));
        let listed = list_listed(&store, &prefix, Some(READ_IDLE)).await?;
        let keys: Vec<String> = listed.iter().map(|(k, _)| k.clone()).collect();
        let sizes: std::collections::HashMap<String, u64> = listed
            .into_iter()
            .map(|(key, meta)| (key, meta.size))
            .collect();
        let listing = Listing::from_keys(keys.iter().map(String::as_str));
        let plan = plan_restore(&listing).map_err(|e| match e {
            // ADR 0035, section 3: with a recorded floor to reach, a missing
            // link is a listing behind its writes, listed again; without
            // one, as before.
            wasmkeep_cluster::replication::PlanError::Missing { .. }
                if floor.is_some_and(|f| matches!(f.head, Position::Known { .. })) =>
            {
                ReplicaError::MissingLink(e.to_string())
            }
            e => ReplicaError::Restore(e.to_string()),
        })?;
        // ADR 0035, section 3: what the listing alone proves of the floor,
        // before any local file is used or changed.
        let chain: Vec<(SegmentId, Option<u64>)> = plan
            .snapshot
            .into_iter()
            .chain(plan.segments.iter().copied())
            .map(|id| (id, None))
            .collect();
        let mut structural = prove(floor, &chain);
        // Review M1: a snapshot above an exact floor hides it from the
        // plan; the listing proves descent if it shows the floor and the
        // links from the plan's base down to it. Nothing below the base is
        // replayed, so the later checks have no floor left to see.
        let mut floor = floor;
        if structural == Proof::Missing
            && let Some(base) = plan.snapshot
            && linked_back(floor, base, &listing)
        {
            structural = Proof::Proven;
            floor = None;
        }
        if structural == Proof::Missing {
            return Err(ReplicaError::Visibility(format!(
                "the listing's chain ends at {:?} and shows neither {:?} nor a later generation",
                plan.head(),
                floor.map(|f| f.head)
            )));
        }
        let mut replica = Self {
            store,
            prefix,
            generation,
            lease,
            head: plan.head(),
            since_snapshot: plan.segments.len() as u64,
            address: address.clone(),
            app: Arc::from(cluster.serves()),
            snapshot_waits: Arc::clone(&cluster.snapshot_waits),
            periodic_flights: Arc::clone(&cluster.periodic_flights),
            snapshot_failures: 0,
            snapshot_retry_at: 0,
            snapshot_retry_after: tokio::time::Instant::now(),
            snapshot_retry_min: cluster.snapshot_retry_min,
            db_path,
            needs_snapshot: plan.head() == EMPTY,
            checksum: DbChecksum::default(),
            max_db_bytes,
            failed: None,
            in_flight: None,
            batched: batched::Uploads::default(),
            upload_idle: UPLOAD_IDLE,
        };
        replica.batched.bytes_since_snapshot = history_bytes(&plan, &keys, &sizes);
        replica.resume_snapshot_wait();
        if plan.head() == EMPTY {
            // Nothing in the bucket yet: the local database (possibly from
            // before the node joined a cluster) becomes the first snapshot.
            return Ok(replica);
        }
        if let Some(sum) = replica.local_is_at(&plan).await {
            // The local file is at the chain's head: it proves an exact
            // floor only there (an earlier exact floor needs the replay).
            let at_head = vec![(plan.head(), Some(sum.value()))];
            let usable = match structural {
                Proof::NeedsReplay => match prove(floor, &at_head) {
                    Proof::Proven => true,
                    // A file changed outside replication proves nothing of
                    // the bucket (ADR 0035, S4): set aside, as the download
                    // installs its result; the replay judges the history.
                    Proof::Conflict => {
                        tracing::warn!(prefix = %crate::logging::text(&replica.prefix), head = ?plan.head(), "the local database at the recorded head has another checksum; downloading instead");
                        false
                    }
                    Proof::Missing | Proof::NeedsReplay => false,
                },
                _ => true,
            };
            if usable {
                tracing::debug!(prefix = %crate::logging::text(&replica.prefix), head = ?plan.head(), "local database is current");
                replica.checksum = sum;
                return Ok(replica);
            }
        }
        replica.checksum = replica
            .download(&plan, &keys, &sizes, cluster.restore_budget, floor)
            .await?;
        Ok(replica)
    }

    /// Whether the local file is exactly the plan's head: the sidecar names
    /// it, no WAL is pending, and the file's checksum matches.
    async fn local_is_at(&self, plan: &Plan) -> Option<DbChecksum> {
        let path = self.db_path.clone();
        let head = plan.head();
        tokio::task::spawn_blocking(move || {
            wasmkeep_storage::local_is_at(&path, head.generation, head.txid)
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
        floor: Option<&Requirement>,
    ) -> Result<DbChecksum, ReplicaError> {
        let mut keys = Vec::with_capacity(plan.segments.len() + 1);
        // Each object's position, and the database checksum after it once
        // applied: the replay's proof of an exact floor (ADR 0035).
        let mut chain: Vec<(SegmentId, Option<u64>)> = plan
            .snapshot
            .into_iter()
            .chain(plan.segments.iter().copied())
            .map(|id| (id, None))
            .collect();
        let mut applied_objects = 0usize;
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
        // Its clock starts here, with its first read: creating the file
        // above is local work, not a wait for the bucket.
        let mut deadline = tokio::time::Instant::now() + allowed;
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
                let (has, has_turn) = tokio::sync::watch::channel(false);
                let its_time = Arc::clone(&time);
                let mut turn = Turn::new(size, Arc::clone(&in_flight), has, Arc::clone(&self.app));
                let task = tokio::spawn(async move {
                    turn.take().await?;
                    let read = std::time::Instant::now();
                    let got = bounded_get(
                        &store,
                        &location,
                        max_object,
                        max,
                        listed,
                        &its_time,
                        Some(&mut turn),
                    )
                    .await;
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
                mut has_turn,
                time,
                mut task,
            }) = reading.pop_front()
            else {
                break;
            };
            // What it waits for this object, its read having its place.
            let mut waited = std::time::Duration::ZERO;
            // The read ended, or is about to: its place says no more.
            let mut ended = false;
            let read = loop {
                // Waiting for the node's limit on reads is not the
                // restore's time ([`RESTORE_BUDGET`]): before the read is
                // asked for, or again after the pause before a resend
                // ([`Turn`]). An error means the task ended, which the
                // wait below reports.
                if !ended {
                    let queued = tokio::time::Instant::now();
                    ended = has_turn.wait_for(|has| *has).await.is_err();
                    deadline += queued.elapsed();
                }
                // The deadline is looked at here only, waiting for the
                // bucket: never while an object is applied or the file
                // put in place (whose time is not counted either, below).
                // What other restores' bytes have added to this read's
                // limits does not count: looked at again when the time is
                // up, as it grows while the read lasts. The reads still
                // under way are aborted as `reading` is dropped.
                let since = tokio::time::Instant::now();
                let until = deadline + time.lifted();
                let read = tokio::select! {
                    biased;
                    read = &mut task.0 => Some(read),
                    lost = has_turn.wait_for(|has| !*has), if !ended => {
                        ended = lost.is_err();
                        None
                    }
                    () = tokio::time::sleep_until(until) => None,
                };
                waited = waited.saturating_add(since.elapsed());
                if let Some(read) = read {
                    break read;
                }
                if deadline + time.lifted() <= tokio::time::Instant::now() {
                    let lifted = lifted.saturating_add(time.lifted());
                    tracing::warn!(prefix = %crate::logging::text(&self.prefix), object = %key, ?allowed, ?lifted, objects, "a restore ran out of time");
                    return Err(ReplicaError::Bucket(format!(
                        "{}: the restore ran out of time ({allowed:?} for {objects} objects, {lifted:?} for other restores' reads) reading {key}",
                        self.prefix
                    )));
                }
            };
            // Never more than it waited for this object: a read that
            // was quick leaves nothing for the next ones.
            let lift = time.lifted().min(waited);
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
            if let Some(slot) = chain.get_mut(applied_objects) {
                slot.1 = Some(restorer.checksum().value());
            }
            applied_objects += 1;
            deadline += applying.elapsed();
        }
        // Proven before anything is installed: a replay that does not
        // reach the floor is discarded.
        match prove(floor, &chain) {
            Proof::Proven => {}
            Proof::Conflict => {
                return Err(ReplicaError::Restore(
                    "the history at the recorded head has another checksum".into(),
                ));
            }
            Proof::Missing | Proof::NeedsReplay => {
                return Err(ReplicaError::Visibility(
                    "the replayed chain does not reach the recorded head".into(),
                ));
            }
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
        tracing::info!(prefix = %crate::logging::text(&self.prefix), head = ?plan.head(), segments = plan.segments.len(), "restored from the bucket");
        Ok(sum)
    }
}
