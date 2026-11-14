//! Garbage collection of an actor's history in the bucket, in the
//! background (ADR 0012).

use super::*;

/// An actor's garbage collection (ADR 0012): at most one pass at a time,
/// and one pending request while it runs. A newer request replaces the
/// pending one: its snapshot is newer, so what it collects includes what
/// the older one would have (nothing is skipped); the shorter window and
/// an erasure win.
#[derive(Default)]
pub(super) struct Collection {
    pub(super) state: std::sync::Mutex<CollectionState>,
}

#[derive(Default)]
pub(super) struct CollectionState {
    pub(super) running: bool,
    pub(super) next: Option<CollectRequest>,
}

pub(super) struct CollectRequest {
    pub(super) collector: crate::replica::Collector,
    pub(super) snapshot: wasmkeep_cluster::replication::SegmentId,
    pub(super) retention: Duration,
    /// After a delete: retried a few times, since no later pass would come.
    pub(super) erase: bool,
}

impl CollectRequest {
    pub(super) fn merge(self, other: Self) -> Self {
        let (retention, erase) = (
            self.retention.min(other.retention),
            self.erase || other.erase,
        );
        let newer = if (other.snapshot.generation, other.snapshot.txid)
            >= (self.snapshot.generation, self.snapshot.txid)
        {
            other
        } else {
            self
        };
        Self {
            retention,
            erase,
            ..newer
        }
    }
}

/// Attempts at an erasure pass (after a delete), with doubling waits from
/// one second, while the lease is held.
pub(super) const ERASE_ATTEMPTS: u32 = 5;

/// Asks for a garbage-collection pass of an actor's history after a
/// snapshot that qualifies (the caller checked it was created and
/// acknowledged by this hold; its generation is checked here), run in the
/// background (ADR 0012).
pub(super) fn spawn_collection(
    address: ActorAddress,
    collection: Arc<Collection>,
    request: CollectRequest,
) {
    if request.snapshot.generation != request.collector.generation() {
        return;
    }
    {
        let mut state = collection.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.running {
            state.next = Some(match state.next.take() {
                Some(pending) => pending.merge(request),
                None => request,
            });
            return;
        }
        state.running = true;
    }
    tokio::spawn(async move {
        let mut request = request;
        loop {
            run_collection(&address, &request).await;
            let mut state = collection.state.lock().unwrap_or_else(|p| p.into_inner());
            match state.next.take() {
                Some(next) => request = next,
                None => {
                    state.running = false;
                    return;
                }
            }
        }
    });
}

pub(super) async fn run_collection(address: &ActorAddress, request: &CollectRequest) {
    let attempts = if request.erase { ERASE_ATTEMPTS } else { 1 };
    let mut wait = Duration::from_secs(1);
    for attempt in 1..=attempts {
        match request
            .collector
            .collect(request.snapshot, request.retention)
            .await
        {
            Ok(n) => {
                tracing::debug!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), snapshot = ?request.snapshot, deleted = n, "history collected");
                return;
            }
            Err(e) => {
                crate::metrics::inc(&crate::metrics::METRICS.history_collect_failures);
                let last = attempt == attempts || !request.collector.lease_valid();
                if last {
                    tracing::warn!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), error = %e, erase = request.erase, "collecting history failed; it is tried again after the next snapshot");
                    return;
                }
                tracing::debug!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), error = %e, "collecting history failed; retrying");
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
        }
    }
}

/// A snapshot of an actor's database as a fresh one is created (its
/// schema, no data), made in a scratch directory under the data directory
/// and removed after: what a delete records when the actor's own files
/// could not be removed.
pub(super) fn empty_database_snapshot(
    storage: &Storage,
    address: &ActorAddress,
) -> Result<wasmkeep_storage::wal::Changes, String> {
    // Unique per call in this process (two actors may be deleted at the
    // same instant), and per process.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scratch = storage
        .scratch_dir()
        .join(format!("{}-{n}", std::process::id()));
    let result = (|| {
        let fresh = Storage::new(
            wasmkeep_storage::StorageConfig {
                data_dir: scratch.clone(),
                ..storage.config().clone()
            },
            storage.state().clone(),
        );
        let mut db = fresh
            .open(address.class(), address.name())
            .map_err(|e| format!("{e:#}"))?;
        db.start_capture().map_err(|e| e.to_string())?;
        db.snapshot().map_err(|e| e.to_string())
    })();
    let _ = std::fs::remove_dir_all(&scratch);
    result
}
