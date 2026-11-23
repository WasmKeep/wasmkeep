//! Lease-free history reads. Only LIST and GET touch the bucket; publication
//! uses a verified private file without replacing a destination.

use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use wasmkeep_cluster::replication::{
    EMPTY, Listing, Plan, SegmentId, plan_restore, segment_key, snapshot_key,
};
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_runtime::ActorAddress;
use wasmkeep_storage::{ltx, readonly, wal};

/// Requested boundary on the current ancestry, never an abandoned branch.
#[derive(Debug, Clone, Copy, Default)]
pub enum Point {
    /// Current listed head.
    #[default]
    Head,
    /// Exact transaction boundary.
    Txid(u64),
    /// Longest retained ancestry prefix with all capture stamps <= this time.
    At(i64),
}

/// What was read and verified. The checksum describes raw replay bytes,
/// before SQLite changes the journal-mode header of the portable output.
#[derive(Debug)]
pub struct Export {
    /// Selected generation and transaction.
    pub position: SegmentId,
    /// Head of the listing used for this export.
    pub listed_head: SegmentId,
    /// Segment capture stamp (never the snapshot or object-store timestamp).
    pub timestamp_ms: i64,
    /// Ownership-record hint; absence or unreadability is unknown, not settled.
    pub owner_generation: Option<u64>,
    /// The ownership record's recorded head (ADR 0035), when it is in
    /// version 2: a hint like the generation, not proof.
    pub owner_recorded: Option<wasmkeep_cluster::recorded_head::RecordedHead>,
    /// LTX checksum of the raw replay.
    pub raw_checksum: u64,
    /// Objects read in the successful attempt, including timestamp lookups.
    pub objects: usize,
    /// Attempts used, including retries after collection.
    pub attempts: usize,
    /// Output length.
    pub bytes: u64,
    /// SQLite checks and application tables.
    pub verification: readonly::Verification,
}

/// Why no export was published.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Ownership hint did not match the listed head.
    #[error(
        "owner generation unavailable or differs from listed head; --require-owner-match refused publication"
    )]
    OwnerMismatch,
    /// Local file failure.
    #[error("export file: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid LTX or broken checksum chain: terminal, never retried.
    #[error("corrupt history: {0}")]
    Corrupt(String),
    /// SQLite verification failed.
    #[error("export SQLite: {0}")]
    Sql(#[from] wasmkeep_storage::SqlError),
    /// Bucket access failure.
    #[error("reading history: {0}")]
    Bucket(String),
    /// Collection or an incomplete listing removed a needed link/object.
    #[error("missing history segment or object: {0}")]
    Missing(String),
    /// Object identity changed after listing.
    #[error("history listing went stale: {0}")]
    Stale(String),
    /// Requested point has no retained base on the current chain.
    #[error("{0}")]
    Point(String),
}

/// Exports one actor without acquiring ownership, contacting nodes, or writing
/// bucket objects. `output` must not exist. `max_bytes` bounds replay size.
/// Scratch files are removed on errors or when the future is dropped. Process
/// termination needs a caller-installed signal handler; SIGKILL cannot clean up.
pub async fn export(
    store: &Arc<dyn ObjectStore>,
    root: &StateRoot,
    actor: &ActorAddress,
    point: Point,
    output: &Path,
    max_bytes: u64,
) -> Result<Export, Error> {
    export_with_options(
        store,
        root,
        actor,
        point,
        output,
        Options {
            max_bytes,
            ..Default::default()
        },
    )
    .await
}

/// Local restore and publication policy.
#[derive(Debug)]
pub struct Options<'a> {
    /// Maximum replay size.
    pub max_bytes: u64,
    /// Scratch parent; defaults to the output parent.
    pub tmpdir: Option<&'a Path>,
    /// Refuse publication unless the owner generation matches the listed head.
    pub require_owner_match: bool,
}
impl Default for Options<'_> {
    fn default() -> Self {
        Self {
            max_bytes: 2 << 30,
            tmpdir: None,
            require_owner_match: false,
        }
    }
}

/// Whether a record one generation past the listed head still names that
/// head as its owner's (`--require-owner-match`): released, in version 2
/// (ADR 0035), with its recorded head at exactly the listed head's
/// generation and txid. What an idle pass (ADR 0042) or a wake that only
/// read leaves: a hold that wrote nothing above the head. A version 1
/// record carries no head, so it cannot.
fn vouches_for(record: &wasmkeep_cluster::node_lease::OwnerRecord, head: SegmentId) -> bool {
    use wasmkeep_cluster::recorded_head::Position;
    record.holder.is_none()
        && head.generation.checked_add(1) == Some(record.generation)
        && record.recorded.is_some_and(|r| {
            matches!(r.head, Position::Known { generation, txid, .. }
                if generation == head.generation && txid == head.txid)
        })
}

/// Export with scratch placement and optional ownership-hint enforcement.
pub async fn export_with_options(
    store: &Arc<dyn ObjectStore>,
    root: &StateRoot,
    actor: &ActorAddress,
    point: Point,
    output: &Path,
    options: Options<'_>,
) -> Result<Export, Error> {
    export_mode(store, root, actor, point, output, options, false).await
}

/// Verified WAL-mode replay for an operator restore, before SQLite can convert
/// its journal header. Uses the same bounded reads and retries as export.
pub async fn export_raw(
    store: &Arc<dyn ObjectStore>,
    root: &StateRoot,
    actor: &ActorAddress,
    point: Point,
    output: &Path,
    max_bytes: u64,
) -> Result<Export, Error> {
    export_mode(
        store,
        root,
        actor,
        point,
        output,
        Options {
            max_bytes,
            ..Default::default()
        },
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn export_mode(
    store: &Arc<dyn ObjectStore>,
    root: &StateRoot,
    actor: &ActorAddress,
    point: Point,
    output: &Path,
    options: Options<'_>,
    raw: bool,
) -> Result<Export, Error> {
    if output.try_exists()? {
        return Err(Error::Point(format!(
            "destination {} already exists; choose a new file",
            output.display()
        )));
    }
    if root.tenant() != actor.tenant() {
        return Err(Error::Point("actor tenant differs from state root".into()));
    }
    let tail = format!(
        "{}/{}",
        actor.class(),
        wasmkeep_storage::encode_segment(actor.name())
    );
    let prefix = format!("{}/", root.bucket_key(Family::History, &tail));
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let scratch = tempfile::Builder::new()
        .prefix(".wasmkeep-export-")
        .tempdir_in(options.tmpdir.unwrap_or(parent))?;
    let staged = scratch.path().join("verified.db");
    let mut last = String::new();
    for attempt in 1..=8 {
        match attempt_export(store, &prefix, point, &staged, options.max_bytes, raw).await {
            Ok(mut result) => {
                result.attempts = attempt;
                // A hint only. Read-only credentials may deliberately omit owners/.
                let owner = StorePath::from(root.bucket_key(Family::Owners, &tail));
                let record = get(store, &owner, None, 16 * 1024)
                    .await
                    .ok()
                    .and_then(|bytes| wasmkeep_cluster::node_lease::OwnerRecord::decode(&bytes));
                result.owner_generation = record.as_ref().map(|owner| owner.generation);
                let vouched = record
                    .as_ref()
                    .is_some_and(|r| vouches_for(r, result.listed_head));
                result.owner_recorded = record.and_then(|owner| owner.recorded);
                if options.require_owner_match
                    && result.owner_generation != Some(result.listed_head.generation)
                    && !vouched
                {
                    return Err(Error::OwnerMismatch);
                }
                publish(&staged, output, |from, to| std::fs::hard_link(from, to))?;
                std::fs::File::open(parent)?.sync_all()?;
                return Ok(result);
            }
            Err(e @ (Error::Missing(_) | Error::Stale(_))) => last = e.to_string(),
            Err(Error::Bucket(message))
                if matches!(point, Point::At(_))
                    && message.contains("deadline")
                    && !message.contains("--txid") =>
            {
                return Err(Error::Bucket(format!(
                    "{message}; try --txid to avoid scanning capture timestamps"
                )));
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::Missing(format!(
        "{last}; refused after 8 attempts: history is incomplete or collected faster than read; raise --history-retention-secs"
    )))
}

async fn attempt_export(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    point: Point,
    output: &Path,
    max: u64,
    raw: bool,
) -> Result<Export, Error> {
    let objects = list_objects(store, prefix).await?;
    attempt_from_objects(store, prefix, point, output, max, raw, &objects, false).await
}

/// Read a head from the caller's fresh app listing. No ownership record is read.
/// Missing links/objects and stale identities retry with an actor listing, at
/// most eight attempts in total. Returns the listing actually verified.
pub async fn export_from_listing(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    mut objects: BTreeMap<String, ObjectMeta>,
    output: &Path,
    max: u64,
) -> Result<(Export, BTreeMap<String, ObjectMeta>), Error> {
    let mut last = String::new();
    for attempt in 1..=8 {
        if attempt > 1 {
            objects = list_objects(store, prefix).await?;
        }
        match attempt_from_objects(
            store,
            prefix,
            Point::Head,
            output,
            max,
            false,
            &objects,
            true,
        )
        .await
        {
            Ok(mut result) => {
                result.attempts = attempt;
                return Ok((result, objects));
            }
            Err(e @ (Error::Missing(_) | Error::Stale(_))) => last = e.to_string(),
            Err(e) => return Err(e),
        }
    }
    Err(Error::Missing(format!("{last}; refused after 8 attempts")))
}

#[allow(clippy::too_many_arguments)]
async fn attempt_from_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    point: Point,
    output: &Path,
    max: u64,
    raw: bool,
    objects: &BTreeMap<String, ObjectMeta>,
    head_from_replay: bool,
) -> Result<Export, Error> {
    let sizes: BTreeMap<_, _> = objects
        .iter()
        .map(|(key, meta)| (key.clone(), meta.size))
        .collect();
    let listing = Listing::from_keys(sizes.keys().map(String::as_str));
    let current = plan_restore(&listing).map_err(|e| Error::Missing(e.to_string()))?;
    let head = current.head();
    if head == EMPTY {
        return Err(Error::Point(
            "actor has no retained history in this bucket".into(),
        ));
    }
    let chain = ancestry(&listing, head);
    let max_object = max.saturating_add(max / 16).saturating_add(1 << 20);
    let mut reader = Reader {
        store,
        prefix,
        objects,
        max_object,
        read_count: 0,
    };
    let allowed = super::restore_time(
        Duration::from_secs(30),
        sizes.len(),
        sizes.values().copied().fold(0, u64::saturating_add),
    );
    let (plan, mut stamp) = if head_from_replay && (!current.segments.is_empty() || head.txid == 1)
    {
        // A head segment (or the first snapshot) is already in the replay.
        // Take its capture stamp there, without fetching it twice. A later
        // snapshot alone still needs its head segment to date the commit.
        (current.clone(), 0)
    } else {
        tokio::time::timeout(
            allowed,
            select(&listing, &current, &chain, point, &mut reader),
        )
        .await
        .map_err(|_| read_deadline(point, "history selection"))??
    };
    let position = plan.head();
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut scratch = tempfile::Builder::new();
    scratch.prefix(".wasmkeep-export-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        scratch.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let scratch = scratch.tempdir_in(parent)?;
    let path = scratch.path().join("actor.db");
    let mut restorer =
        wal::Restorer::create_private(&path, max).map_err(|e| Error::Corrupt(e.to_string()))?;
    let mut keys = Vec::new();
    if let Some(id) = plan.snapshot {
        keys.push((snapshot_key(id), id, true));
    }
    for id in &plan.segments {
        keys.push((key_for(&listing, *id)?, *id, false));
    }
    let mut remaining = allowed;
    // Batches preserve the restore's eight-object / 32 MiB read-ahead bounds.
    // Only compressed objects in this batch and the restorer's page are retained.
    let mut pending = keys.into_iter().peekable();
    while pending.peek().is_some() {
        let mut batch = Vec::new();
        let mut bytes = 0u64;
        while let Some((key, _, _)) = pending.peek() {
            let size = sizes.get(key).copied().unwrap_or(0);
            if !batch.is_empty()
                && (batch.len() >= super::READ_AHEAD
                    || bytes.saturating_add(size) > super::READ_AHEAD_BYTES)
            {
                break;
            }
            bytes = bytes.saturating_add(size);
            if let Some(item) = pending.next() {
                batch.push(item);
            }
        }
        let start = std::time::Instant::now();
        let reads = futures_util::stream::iter(batch.iter().map(|(key, id, snapshot)| {
            let reader = &reader;
            async move { Ok::<_, Error>((reader.read(key).await?, *id, *snapshot)) }
        }))
        .buffered(super::READ_AHEAD)
        .try_collect::<Vec<_>>();
        let data = tokio::time::timeout(remaining, reads)
            .await
            .map_err(|_| read_deadline(point, "export"))??;
        remaining = remaining.saturating_sub(start.elapsed());
        reader.read_count += data.len();
        for (bytes, id, snapshot) in data {
            let header = checked_header(&bytes, id, snapshot)?;
            if head_from_replay && id == head && (!snapshot || id.txid == 1) {
                stamp = header.timestamp_ms;
            }
            let expected = u64::from(header.commit) * u64::from(header.page_size);
            let present = std::fs::metadata(&path)?.len();
            check_space(scratch.path(), expected.saturating_sub(present))?;
            restorer
                .apply(&bytes)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
        }
    }
    let checksum = restorer
        .finish()
        .map_err(|e| Error::Corrupt(e.to_string()))?;
    // Re-read every raw page independently before SQLite can change any byte.
    let capture = wal::WalCapture::start(&path, page_size(&path)?)
        .map_err(|e| Error::Corrupt(e.to_string()))?;
    if capture.checksum() != checksum {
        return Err(Error::Corrupt(
            "final file checksum does not match the replay".into(),
        ));
    }
    drop(capture);
    let verify_path = path.clone();
    let (verification, _scratch) = tokio::task::spawn_blocking(move || {
        let verification = if raw {
            readonly::verify(&verify_path)
        } else {
            readonly::verify_and_make_portable(&verify_path)
        };
        verification.map(|verification| (verification, scratch))
    })
    .await
    .map_err(|e| Error::Corrupt(format!("verification worker: {e}")))??;
    let file = std::fs::File::open(&path)?;
    file.sync_all()?;
    let bytes = file.metadata()?.len();
    publish(&path, output, |from, to| std::fs::hard_link(from, to))?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(Export {
        position,
        listed_head: head,
        timestamp_ms: stamp,
        owner_generation: None,
        owner_recorded: None,
        raw_checksum: checksum.value(),
        objects: reader.read_count,
        attempts: 1,
        bytes,
        verification,
    })
}

fn read_deadline(point: Point, phase: &str) -> Error {
    Error::Bucket(format!(
        "{phase} read deadline exceeded{}",
        if matches!(point, Point::At(_)) {
            "; try --txid to avoid scanning capture timestamps"
        } else {
            ""
        }
    ))
}

fn check_space(path: &Path, needed: u64) -> Result<(), Error> {
    // Advisory: quotas, concurrent writers and SQLite sidecars may still exhaust disk.
    #[cfg(unix)]
    if let Ok(stat) = rustix::fs::statvfs(path) {
        let available = stat.f_bavail.saturating_mul(stat.f_frsize);
        if available < needed {
            return Err(Error::Io(std::io::Error::other(format!(
                "insufficient free space: need {needed} bytes, have {available}; choose --tmpdir on a larger filesystem"
            ))));
        }
    }
    Ok(())
}

fn publish(
    from: &Path,
    to: &Path,
    link: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
) -> Result<(), Error> {
    match link(from, to) {
        Ok(()) => return Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::Unsupported
                    | std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::CrossesDevices
            ) => {}
        Err(e) => return Err(e.into()),
    }
    let source = std::fs::File::open(from)?;
    check_space(
        to.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
        source.metadata()?.len(),
    )?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut dest = options.open(to)?;
    let result = std::io::copy(&mut &source, &mut dest).and_then(|_| dest.sync_all());
    if result.is_err() {
        drop(dest);
        let _ = std::fs::remove_file(to);
    }
    result.map_err(Into::into)
}

fn page_size(path: &Path) -> Result<u32, Error> {
    use std::io::Read;
    let mut header = [0u8; 18];
    std::fs::File::open(path)?.read_exact(&mut header)?;
    Ok(match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65_536,
        n => u32::from(n),
    })
}

/// Walk past newer snapshots too: filtering keys globally by txid could pick
/// an orphan generation. A missing parent bounds retained ancestry.
pub fn ancestry(listing: &Listing, head: SegmentId) -> Vec<SegmentId> {
    let parents: BTreeMap<_, _> = listing
        .segments
        .iter()
        .map(|s| (s.id, s.parent()))
        .collect();
    let mut chain = Vec::new();
    let mut at = head;
    while at != EMPTY {
        chain.push(at);
        let Some(parent) = parents.get(&at) else {
            break;
        };
        at = *parent;
    }
    chain.reverse();
    chain
}

fn key_for(listing: &Listing, id: SegmentId) -> Result<String, Error> {
    listing
        .segments
        .iter()
        .find(|s| s.id == id)
        .map(segment_key)
        .ok_or_else(|| {
            Error::Missing(format!(
                "segment at {id:?}; its LTX capture stamp is required"
            ))
        })
}

async fn select(
    listing: &Listing,
    current: &Plan,
    chain: &[SegmentId],
    point: Point,
    reader: &mut Reader<'_>,
) -> Result<(Plan, i64), Error> {
    let oldest = chain
        .iter()
        .copied()
        .find(|id| {
            listing.snapshots.contains(id)
                || (id.txid == 1
                    && listing
                        .segments
                        .iter()
                        .any(|s| s.id == *id && s.parent() == EMPTY))
        })
        .ok_or_else(|| {
            Error::Missing("no retained snapshot or initial segment on the current chain".into())
        })?;
    let unavailable = || {
        Error::Point(format!(
            "requested point is older than retained history or not on the current chain (replayable txids {}..={}); earlier data may have been erased by a delete/reset; restore it from a backup",
            oldest.txid,
            current.head().txid
        ))
    };
    let mut selected = None;
    match point {
        Point::Head => {
            let id = current.head();
            let stamp = reader.stamp_position(listing, id).await?;
            return Ok((current.clone(), stamp));
        }
        Point::Txid(txid) => {
            let id = chain
                .iter()
                .copied()
                .find(|id| id.txid == txid && id.txid >= oldest.txid)
                .ok_or_else(unavailable)?;
            selected = Some((id, reader.stamp_position(listing, id).await?));
        }
        Point::At(time) => {
            // Visit oldest to newest; stop at the FIRST stamp above T even
            // when a successor's clock moved backwards. Snapshot stamps do
            // not date positions; the segment at the snapshot does.
            for &id in chain {
                if !listing.segments.iter().any(|s| s.id == id)
                    && !(id.txid == 1 && listing.snapshots.contains(&id))
                {
                    continue;
                }
                let stamp = reader.stamp_position(listing, id).await?;
                if stamp > time {
                    break;
                }
                if id.txid >= oldest.txid {
                    selected = Some((id, stamp));
                }
            }
        }
    }
    let (target, stamp) = selected.ok_or_else(unavailable)?;
    let retained: BTreeSet<_> = chain
        .iter()
        .copied()
        .filter(|id| id.txid <= target.txid)
        .collect();
    let subset = Listing {
        segments: listing
            .segments
            .iter()
            .copied()
            .filter(|s| retained.contains(&s.id))
            .collect(),
        snapshots: listing
            .snapshots
            .iter()
            .copied()
            .filter(|id| retained.contains(id))
            .collect(),
    };
    let plan = plan_restore(&subset).map_err(|_| unavailable())?;
    if plan.head() != target {
        return Err(unavailable());
    }
    Ok((plan, stamp))
}

struct Reader<'a> {
    store: &'a Arc<dyn ObjectStore>,
    prefix: &'a str,
    objects: &'a BTreeMap<String, ObjectMeta>,
    max_object: u64,
    read_count: usize,
}
impl Reader<'_> {
    async fn read(&self, key: &str) -> Result<bytes::Bytes, Error> {
        get(
            self.store,
            &StorePath::from(format!("{}{key}", self.prefix)),
            self.objects.get(key),
            self.max_object,
        )
        .await
    }
    async fn stamp_position(&mut self, listing: &Listing, id: SegmentId) -> Result<i64, Error> {
        // The first upload is a snapshot at txid 1, with no segment. It
        // creates the position itself; later snapshots do not date commits.
        let (key, snapshot) = match key_for(listing, id) {
            Ok(key) => (key, false),
            Err(_) if id.txid == 1 && listing.snapshots.contains(&id) => (snapshot_key(id), true),
            Err(e) => return Err(e),
        };
        let bytes = self.read(&key).await?;
        self.read_count += 1;
        let h = checked_header(&bytes, id, snapshot)?;
        // Validate the trailer even when this object only dates a snapshot.
        ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
            .map_err(|e| Error::Corrupt(format!("{key}: {e:?}")))?;
        Ok(h.timestamp_ms)
    }
}
fn checked_header(bytes: &[u8], id: SegmentId, snapshot: bool) -> Result<ltx::Header, Error> {
    let h = ltx::Header::decode(
        bytes
            .get(..ltx::HEADER_SIZE)
            .ok_or_else(|| Error::Corrupt("truncated LTX header".into()))?,
    )
    .map_err(|e| Error::Corrupt(e.to_string()))?;
    if h.no_checksum() || h.max_txid != id.txid || h.min_txid != if snapshot { 1 } else { id.txid }
    {
        return Err(Error::Corrupt(format!(
            "LTX header does not match position {id:?}"
        )));
    }
    Ok(h)
}

/// Keep identity from the same listing as the keys. An app recreation can
/// reuse a history key with different bytes, even at the same length.
pub async fn list_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<BTreeMap<String, ObjectMeta>, Error> {
    list_objects_with_limit(store, prefix, None).await
}

/// List at most `max` objects, returning an error rather than partial results.
/// Intended for bounded operator views; history readers remain unbounded.
pub async fn list_objects_bounded(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    max: usize,
) -> Result<BTreeMap<String, ObjectMeta>, Error> {
    list_objects_with_limit(store, prefix, Some(max)).await
}

async fn list_objects_with_limit(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    max: Option<usize>,
) -> Result<BTreeMap<String, ObjectMeta>, Error> {
    let base = StorePath::from(prefix.trim_end_matches('/'));
    let encoded = if base.as_ref().is_empty() {
        String::new()
    } else {
        format!("{}/", base.as_ref())
    };
    'attempts: for _ in 0..super::READ_ATTEMPTS {
        let mut stream = store.list(Some(&base));
        let mut objects = BTreeMap::new();
        let mut count = 0;
        loop {
            match tokio::time::timeout(super::READ_IDLE, stream.next()).await {
                Ok(Some(Ok(meta))) => {
                    count += 1;
                    if let Some(max) = max
                        && count > max
                    {
                        return Err(Error::Bucket(format!("listing exceeds {max} object limit")));
                    }
                    if let Some(key) = meta.location.as_ref().strip_prefix(&encoded) {
                        objects.insert(key.to_owned(), meta);
                    }
                }
                Ok(Some(Err(e))) => return Err(Error::Bucket(e.to_string())),
                Ok(None) => return Ok(objects),
                Err(_) => continue 'attempts,
            }
        }
    }
    Err(Error::Bucket("history listing kept stalling".into()))
}

/// Bounded read with typed missing/stale errors for replanning. A lying body
/// cannot allocate past the cap, even if its metadata understates its length.
///
/// With `listed`, the read is conditional on the listed ETag. Some buckets
/// report a different ETag in LIST than in GET for the same bytes (OVHcloud,
/// for objects made by a server-side copy: a SHA-256-like tag listed, the
/// MD5 on GET, and a 412 to If-Match with the listed one). Refusing those
/// would make every copied object unreadable, so a 412 or a different GET
/// ETag is not taken as a change on its own: the object is read once more
/// without the condition and accepted only when it is proven to be the
/// listed object. Its size and its last-modified time (to the second)
/// equal the listing's, its body is a valid LTX file with its checksum, and
/// a new listing of its key, made after the read, still reports the listed
/// ETag. Anything else is [`Error::Stale`], as before. Only LTX history
/// objects are read with a listing, hence the LTX check.
pub async fn get(
    store: &Arc<dyn ObjectStore>,
    path: &StorePath,
    listed: Option<&ObjectMeta>,
    cap: u64,
) -> Result<bytes::Bytes, Error> {
    match get_once(store, path, listed, cap, true).await {
        Err(Error::Stale(_)) if listed.is_some_and(|meta| meta.e_tag.is_some()) => {
            let Some(listed) = listed else {
                return Err(Error::Stale(path.to_string()));
            };
            let bytes = get_once(store, path, Some(listed), cap, false).await?;
            prove_listed(store, path, listed, &bytes).await?;
            tracing::debug!(object = %path, "listed and read ETags differ; identity proven by size, time, LTX checksum and a new listing");
            Ok(bytes)
        }
        other => other,
    }
}

/// The fallback's proof, after the unconditional read checked size and time.
async fn prove_listed(
    store: &Arc<dyn ObjectStore>,
    path: &StorePath,
    listed: &ObjectMeta,
    bytes: &[u8],
) -> Result<(), Error> {
    let stale = || Error::Stale(path.to_string());
    match ltx::visit(bytes, |_| Ok::<_, ()>(()), |_, _| Ok(())) {
        Ok((header, _)) if !header.no_checksum() => {}
        _ => return Err(stale()),
    }
    // Listed again, after the read: a replacement since the first listing
    // shows a new listed ETag (or none). The offset is just before the key,
    // so this is one page on S3, not the whole directory.
    let full = path.as_ref();
    let (parent, _) = full.rsplit_once('/').unwrap_or(("", full));
    let parent = StorePath::parse(parent).map_err(|e| Error::Bucket(e.to_string()))?;
    let parent = (!parent.as_ref().is_empty()).then_some(parent);
    let before = full
        .char_indices()
        .last()
        .and_then(|(at, _)| StorePath::parse(&full[..at]).ok());
    let mut stream = match &before {
        Some(offset) => store.list_with_offset(parent.as_ref(), offset),
        None => store.list(parent.as_ref()),
    };
    loop {
        let next = tokio::time::timeout(super::READ_IDLE, stream.next())
            .await
            .map_err(|_| Error::Bucket("listing to confirm an object stalled".into()))?;
        match next {
            Some(Ok(meta)) if meta.location == *path => {
                return if meta.e_tag == listed.e_tag && meta.size == listed.size {
                    Ok(())
                } else {
                    Err(stale())
                };
            }
            Some(Ok(meta)) if meta.location.as_ref() > full => return Err(stale()),
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(Error::Bucket(e.to_string())),
            None => return Err(stale()),
        }
    }
}

/// One bounded read. `strict`: conditional on the listed ETag, which the
/// response must carry too. Otherwise unconditional, with the listed size,
/// version and last-modified second still required.
async fn get_once(
    store: &Arc<dyn ObjectStore>,
    path: &StorePath,
    listed: Option<&ObjectMeta>,
    cap: u64,
    strict: bool,
) -> Result<bytes::Bytes, Error> {
    let mut last = String::new();
    for _ in 0..super::READ_ATTEMPTS {
        let read = async {
            let options = object_store::GetOptions {
                if_match: listed.filter(|_| strict).and_then(|m| m.e_tag.clone()),
                version: listed.and_then(|m| m.version.clone()),
                ..Default::default()
            };
            let got = tokio::time::timeout(super::READ_IDLE, store.get_opts(path, options))
                .await
                .map_err(|_| Error::Bucket("object headers stalled".into()))?
                .map_err(|e| match e {
                    object_store::Error::NotFound { .. } => Error::Missing(path.to_string()),
                    object_store::Error::Precondition { .. }
                    | object_store::Error::NotModified { .. } => Error::Stale(path.to_string()),
                    _ => Error::Bucket(e.to_string()),
                })?;
            let size = got.meta.size;
            if listed.is_some_and(|meta| {
                meta.size != size
                    || (strict
                        && meta
                            .e_tag
                            .as_ref()
                            .is_some_and(|tag| got.meta.e_tag.as_ref() != Some(tag)))
                    || (!strict
                        && meta.last_modified.timestamp() != got.meta.last_modified.timestamp())
                    || meta
                        .version
                        .as_ref()
                        .is_some_and(|version| got.meta.version.as_ref() != Some(version))
            }) {
                return Err(Error::Stale(path.to_string()));
            }
            if size > cap {
                return Err(Error::Corrupt(format!(
                    "{path}: object exceeds {cap} byte cap"
                )));
            }
            let mut stream = got.into_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = tokio::time::timeout(super::READ_IDLE, stream.next())
                .await
                .map_err(|_| Error::Bucket("object body stalled".into()))?
            {
                let chunk = chunk.map_err(|e| Error::Bucket(e.to_string()))?;
                if (bytes.len() as u64).saturating_add(chunk.len() as u64) > size {
                    return Err(Error::Corrupt(format!(
                        "{path}: body exceeds declared size"
                    )));
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() as u64 != size {
                return Err(Error::Bucket(format!("{path}: truncated object body")));
            }
            Ok(bytes.into())
        };
        let allowed = super::READ_IDLE
            + Duration::from_secs(listed.map_or(cap, |m| m.size).div_ceil(1 << 20));
        match tokio::time::timeout(allowed, read).await {
            Ok(Ok(bytes)) => return Ok(bytes),
            Ok(Err(Error::Bucket(e))) => last = e,
            Err(_) => last = "object read deadline exceeded".into(),
            Ok(Err(e)) => return Err(e),
        }
    }
    Err(Error::Bucket(format!("{path}: {last}")))
}

/// Replay an isolated manifest-backed store. The caller must populate only
/// the named actor objects and compare the returned head with its manifest.
pub async fn replay(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    output: &Path,
    max: u64,
) -> Result<Export, Error> {
    attempt_export(store, prefix, Point::Head, output, max, true).await
}

/// Snapshot, boundary segment and ascending replay segments, without duplicates.
pub fn copy_keys(listing: &Listing, plan: &Plan) -> Result<Vec<String>, Error> {
    let mut keys = Vec::new();
    if let Some(id) = plan.snapshot {
        keys.push(snapshot_key(id));
        if let Ok(key) = key_for(listing, id) {
            keys.push(key);
        }
    }
    for id in &plan.segments {
        keys.push(key_for(listing, *id)?);
    }
    Ok(keys)
}

#[cfg(test)]
mod tests;
