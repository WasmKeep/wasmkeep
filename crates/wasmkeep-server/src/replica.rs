//! Replicating one actor's database to the bucket (ADR 0007).
//!
//! While a node holds an actor's lease, every committed transaction is
//! uploaded as an LTX segment under
//! `actors/<tenant>/<class>/<name>/wal/<txid>-<generation>-<parent>.ltx`,
//! and every [`SNAPSHOT_EVERY`] segments a snapshot of the whole database.
//! A segment is durable only if its upload completed while the lease was
//! still valid. On acquiring an actor, the node restores the chain the
//! highest generation wrote, unless its local file already is that head.

pub mod history;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use wasmkeep_cluster::local_heads::{Proof, Requirement, linked_back, prove};
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_cluster::replication::{
    EMPTY, Listing, Plan, Segment, SegmentId, collectable, idle_collectable, plan_restore,
    segment_key, snapshot_key,
};
use wasmkeep_runtime::ActorAddress;
use wasmkeep_storage::ltx::DbChecksum;
use wasmkeep_storage::wal::Changes;

use crate::cluster::{Cluster, LeaseView};

mod batched;
mod bucket;
pub(crate) use batched::{
    AlarmIndex, BudgetPermit, NODE_UPLOAD_BYTES, Stop, UploadBudget, stopped,
};
#[cfg(test)]
pub(crate) use batched::{Applied, UploadFailure};
mod collect;
mod restore;
mod snapshots;
mod upload;
pub(crate) use collect::*;
pub(crate) use restore::*;
pub(crate) use snapshots::*;
pub(crate) use upload::*;

/// What `failed` says while an inline upload is under way, so a cut that
/// interrupts it stops replication.
const INTERRUPTED: &str = "an upload was interrupted";

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
    /// A snapshot or a reset that would not be labelled at the position it
    /// holds (ADR 0034, section 7): refused before anything is uploaded,
    /// and replication stops, as for a fork.
    #[error("refused: {0}")]
    Refused(String),
    /// The recorded head the hold inherited is not visible in the bucket's
    /// listing within the bounded re-list (ADR 0035, section 3): nothing
    /// activated, retryable (503).
    #[error("the actor's recorded head is not visible yet: {0}")]
    Visibility(String),
    /// A link of the chain is missing while a recorded floor is to be
    /// reached: a listing behind its writes, listed again like
    /// [`Self::Visibility`]; still missing once the discovery bound is
    /// spent, it is reported as [`Self::Restore`] (ADR 0035, S6). Never
    /// returned by [`Replica::restore`].
    #[error("restore: {0}")]
    MissingLink(String),
}

/// `d` plus a uniform 0 to 20 percent (ADR 0035, section 3).
pub(crate) fn jittered(d: std::time::Duration) -> std::time::Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |t| t.subsec_nanos());
    d + d.mul_f64(f64::from(nanos % 1_000) / 5_000.0)
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
    /// Its app, `tenant/app`: for the app's shares of the node's budgets
    /// (ADR 0038, section 8).
    app: Arc<str>,
    snapshot_waits: Arc<SnapshotWaits>,
    /// The node's periodic snapshots uploading ([`PeriodicFlights`]).
    periodic_flights: Arc<PeriodicFlights>,
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
    /// The segment whose upload has started and not ended, if any. A
    /// snapshot or a reset is refused while one is (ADR 0034, section 7):
    /// its key would name a position the bucket may not hold yet. In a
    /// class with strict writes an upload runs inside [`Self::ship`], which
    /// holds the replica until it ends, so no snapshot or reset ever sees
    /// one, except after a `ship` cancelled half-way, which leaves `failed`
    /// set as well. A batched segment (ADR 0034, section 2) is in flight
    /// from [`Self::prepare`] until its outcome is applied
    /// ([`Self::apply`]), while the actor runs on.
    in_flight: Option<SegmentId>,
    /// Batched writes' uploader: the captured position, the outcome cell
    /// and the waiters (ADR 0034, sections 2 and 5). Idle in a strict
    /// class.
    batched: batched::Uploads,
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
/// One generation of it is kept: the previous copy's WAL and shared
/// memory are removed first, so an older WAL never sits beside a newer
/// copy (SQLite would apply it when the copy is opened).
fn set_aside(db_path: &Path) -> std::io::Result<()> {
    let with = |suffix: &str| {
        let mut p = db_path.as_os_str().to_owned();
        p.push(suffix);
        PathBuf::from(p)
    };
    for gone in [".replaced-wal", ".replaced-shm"] {
        match std::fs::remove_file(with(gone)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
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
    /// Its app, `tenant/app`.
    pub(crate) fn app(&self) -> Arc<str> {
        Arc::clone(&self.app)
    }

    /// The next transaction id; a head at `u64::MAX` (only a corrupt
    /// bucket could hold one) stops replication instead of wrapping.
    fn next_txid(&self) -> Result<u64, ReplicaError> {
        self.head
            .txid
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Restore("transaction id overflow".into()))
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

    /// The head with its database checksum, as the hold cell records it
    /// (ADR 0035): the explicit pre-history position for an empty history.
    pub(crate) fn verified_head(&self) -> Position {
        position_of(self.head, self.checksum)
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
        // An inline upload's provisional interruption marker is replaced by
        // its actual error; an established failure keeps its original cause.
        if self.failed.as_deref() == Some(INTERRUPTED) {
            self.failed = None;
        }
        let reason = self.failed.get_or_insert_with(|| reason.into());
        // The provisional marker of an inline upload under way (a reset or
        // a snapshot) is not a stop: the upload replaces it with its
        // outcome. Published, it would cut a batched hold's running event
        // for an upload that then succeeds. Left behind by a cancelled
        // upload, `failed` still stops the hold at its next boundary.
        if reason == INTERRUPTED {
            return;
        }
        // Only batched waiters listen (ADR 0034, section 4).
        self.batched.stopped(reason);
    }

    /// Resolve a delete's ordering from the immutable reset, never a node clock.
    /// A collected reset predates every live request when retention covers
    /// the request TTL plus the caller's clock/precision margin.
    pub(crate) async fn delete_time(
        &self,
        label: &str,
        retention: std::time::Duration,
        minimum_retention: std::time::Duration,
    ) -> Result<Option<u64>, ReplicaError> {
        use futures_util::StreamExt as _;
        let fields: Vec<_> = label.split_whitespace().collect();
        let invalid = || ReplicaError::Local("delete ordering evidence unavailable".into());
        let txid = fields
            .get(1)
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(invalid)?;
        let generation = fields
            .get(3)
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(invalid)?;
        let base = StorePath::from(self.prefix.trim_end_matches('/'));
        let encoded = format!("{}/", base.as_ref());
        let mut listing = self.store.list(Some(&base));
        while let Some(meta) = tokio::time::timeout(self.upload_idle, listing.next())
            .await
            .map_err(|_| invalid())?
        {
            let meta = meta.map_err(|e| ReplicaError::Bucket(e.to_string()))?;
            let Some(key) = meta.location.as_ref().strip_prefix(&encoded) else {
                continue;
            };
            if Listing::from_keys([key])
                .segments
                .iter()
                .any(|s| s.id == (SegmentId { generation, txid }))
            {
                return u64::try_from(meta.last_modified.timestamp_millis())
                    .map(Some)
                    .map_err(|_| invalid());
            }
        }
        if retention >= minimum_retention {
            Ok(None)
        } else {
            Err(ReplicaError::Local(format!(
                "delete ordering evidence unavailable: history retention {} ms is shorter than the request TTL plus clock-skew margin ({} ms required)",
                retention.as_millis(),
                minimum_retention.as_millis(),
            )))
        }
    }
}

#[cfg(test)]
mod tests;
