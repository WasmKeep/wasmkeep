//! Collecting the history a snapshot makes unneeded.

use super::*;

/// Deletes per batch in a collection pass; the lease is checked before each.
pub(crate) const COLLECT_BATCH: usize = 100;

/// Garbage collection of one actor's history by its owner (ADR 0012),
/// detached from the replica: the owner's generation, and a view of its
/// lease, checked before every batch of deletes.
#[derive(Clone)]
pub(crate) struct Collector {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) prefix: String,
    pub(super) generation: u64,
    pub(super) lease: LeaseView,
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
        if !self.lease.is_valid() {
            return Ok(0);
        }
        let listed = self.list().await?;
        let listing = Listing::from_keys(listed.iter().map(|(k, _)| k.as_str()));
        let gone = collectable(&listing, self.generation, snapshot);
        Ok(self
            .delete_older(&listed, gone, retention, COLLECT_BATCH, &|| false)
            .await?
            .deleted)
    }

    /// The collector of an idle pass (ADR 0042, section 3): `generation`
    /// is the released record's, which the pass's own hold acquired
    /// unchanged, and `lease` is the view of that new hold. Built from the
    /// cluster: the pass restores nothing and has no replica.
    pub(crate) fn idle(
        cluster: &Cluster,
        address: &ActorAddress,
        generation: u64,
        lease: LeaseView,
    ) -> Self {
        Self {
            store: Arc::clone(&cluster.store),
            prefix: format!("{}/", cluster.history_key(address)),
            generation,
            lease,
        }
    }

    /// An idle pass's collection (ADR 0042, section 3, steps 4 and 5): one
    /// listing; the snapshot and keys [`idle_collectable`] gives for this
    /// collector's generation (the released one); and the deletes of
    /// those older than `retention` by `last_modified`, in batches, the
    /// lease checked before each, and `stop` too: the pass stops deleting
    /// once it says so (its deadline, an event waiting behind it, a drain),
    /// and what is left is counted in `left`, for the caller to continue in
    /// a later pass. Deletes go `batch` objects at a time
    /// ([`COLLECT_BATCH`], except in tests). A lease already invalid is
    /// [`ReplicaError::Late`], nothing listed.
    pub(crate) async fn collect_idle(
        &self,
        retention: std::time::Duration,
        batch: usize,
        stop: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<IdleCollected, ReplicaError> {
        if !self.lease.is_valid() {
            return Err(ReplicaError::Late);
        }
        let listed = self.list().await?;
        let listing = Listing::from_keys(listed.iter().map(|(k, _)| k.as_str()));
        let Some((snapshot, gone)) = idle_collectable(&listing, self.generation) else {
            return Ok(IdleCollected::NoSnapshot);
        };
        let d = self
            .delete_older(&listed, gone, retention, batch, stop)
            .await?;
        Ok(IdleCollected::Collected {
            snapshot,
            deleted: d.deleted,
            kept: d.kept,
            left: d.left,
            stopped: d.stopped,
        })
    }

    /// The prefix, listed once: each key relative to it, with the exact
    /// location and last-modified time the listing returned. A restore's
    /// lister ([`list_listed`]) without an idle limit: one attempt, which
    /// waits as long as the bucket takes; the deletes use the exact
    /// locations listed.
    async fn list(&self) -> Result<Vec<(String, object_store::ObjectMeta)>, ReplicaError> {
        list_listed(&self.store, &self.prefix, None).await
    }

    /// Deletes the listed objects `gone` names that are older than
    /// `retention`, in batches of `batch`, while the lease is valid and
    /// `stop` says nothing.
    async fn delete_older(
        &self,
        listed: &[(String, object_store::ObjectMeta)],
        gone: Vec<String>,
        retention: std::time::Duration,
        batch: usize,
        stop: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Deleted, ReplicaError> {
        use futures_util::StreamExt;
        let gone: std::collections::HashSet<String> = gone.into_iter().collect();
        let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        let cutoff = crate::alarm::now_ms_i64().saturating_sub(retention_ms);
        let (mut doomed, mut kept) = (Vec::new(), 0);
        for (k, m) in listed {
            if !gone.contains(k) {
                continue;
            }
            if retention.is_zero() || m.last_modified.timestamp_millis() <= cutoff {
                doomed.push(m.location.clone());
            } else {
                kept += 1;
            }
        }
        let (mut deleted, mut stopped) = (0, false);
        for batch in doomed.chunks(batch.max(1)) {
            // ADR 0012, condition 3.
            if !self.lease.is_valid() {
                break;
            }
            if stop() {
                stopped = true;
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
        Ok(Deleted {
            deleted,
            kept,
            left: doomed.len().saturating_sub(deleted),
            stopped,
        })
    }
}

/// What [`Collector::delete_older`] did.
struct Deleted {
    /// Deleted (already gone counts).
    deleted: usize,
    /// Kept for being younger than the retention window.
    kept: usize,
    /// Old enough and not deleted: the lease ended or `stop` said so
    /// before their batch.
    left: usize,
    /// `stop` cut the deletes short.
    stopped: bool,
}

/// What an idle pass's collection did ([`Collector::collect_idle`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleCollected {
    /// The listing has no snapshot of the released generation with its
    /// segment (a read-only wake's release, say): nothing deleted.
    NoSnapshot,
    /// Compacted below the snapshot at `snapshot`: `deleted` objects gone
    /// (already gone counts), `kept` collectable ones left for being
    /// younger than the retention window.
    Collected {
        snapshot: SegmentId,
        deleted: usize,
        kept: usize,
        /// Collectable, old enough, and not deleted: the lease ended or
        /// the pass's stop came before their batch. 0 when the pass
        /// finished.
        left: usize,
        /// The pass's stop condition cut the deletes short.
        stopped: bool,
    },
}
