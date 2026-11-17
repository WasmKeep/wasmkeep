//! The sweep pass of deleted actors' files (ADR 0030, sections 5 and 7):
//! one node of the app (the one that collects idle actors) lists
//! `blob-sweeps/` once a minute and acts on each record by its age, and
//! once an hour removes the files of epochs other than the app's own.
//!
//! A `pending` record is judged by a job in the actor's mailbox that runs
//! no guest code: an acquisition only of a record naming nobody, as idle
//! expiry acquires (or the hold already here), a restore, the fence, and a
//! read of the incarnation. Only that answers "not deleted", which drops
//! the record and touches no file; anything short of it judges nothing.

use super::*;
use wasmkeep_cluster::blob_sweep::{self, Action, Judged, State};

/// Records a pass acts on, at most.
const PER_PASS: usize = 1_000;
/// How long a pass waits for one judging job.
const JUDGE_WAIT: Duration = Duration::from_secs(60);

impl ActorState {
    /// Judges a `pending` sweep record of `recorded` incarnation: `None`
    /// when this job could not tell (held elsewhere, a refused or failed
    /// acquisition or restore, the fence not uploaded).
    pub(super) async fn judge_files(&mut self, recorded: String) -> Option<Judged> {
        let cluster = self.cluster.clone()?;
        let held_here = self.held.as_ref().is_some_and(Held::is_valid);
        let mut kept_origin = None;
        if !held_here {
            if self.held.is_some() || self.live.is_some() {
                return None;
            }
            // Only a record naming nobody, as listed, as idle expiry takes
            // it; its idle origin is kept at the release (decision 8).
            let listed = cluster.owner_record_listed(&self.address).await.ok()??;
            let origin = listed.idle_origin_ms();
            self.owner_hint = None;
            if self
                .ensure_owner_with(Some((&listed.version, origin)))
                .await
                .is_err()
            {
                return None;
            }
            kept_origin = Some(origin);
        }
        self.replica()?;
        // The restored database, opened (and kept for the fence) without
        // instantiating the component.
        self.delete_marks().await.ok()?;
        let current = self.restore_meta(BLOB_INCARNATION_KEY).await.ok()?;
        let current = blob_sweep::incarnation_of(current.as_deref()).ok()?;
        // What was restored is the state only once this hold's own upload
        // ends the history: until then a late reset could still land.
        let fenced = self.fence().await;
        // Last, after the restore's verification and the fence: the
        // release writes the idle origin the record had (decision 8).
        if let (Some(origin), Some(h)) = (kept_origin, self.held.as_ref()) {
            h.lease().keep_idle_since(origin);
            cluster.note_judged(&self.address);
        }
        if !fenced {
            return None;
        }
        Some(blob_sweep::judge(&recorded, &current))
    }
}

/// Every pass period, while this node collects. Runs until the service is
/// dropped or stops.
pub(super) async fn sweep_loop(service: std::sync::Weak<ServiceInner>, cluster: Arc<Cluster>) {
    let every = cluster.blob_sweep_every;
    let mut epochs_due = tokio::time::Instant::now() + cluster.blob_epochs_every;
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return;
        }
        if !inner.blobs.on_a_bucket()
            || cluster.app_refusal().is_some()
            || !cluster.collects_blob_sweeps()
        {
            continue;
        }
        pass(&inner, &cluster).await;
        if tokio::time::Instant::now() >= epochs_due {
            epochs_due = tokio::time::Instant::now() + cluster.blob_epochs_every;
            epochs_pass(&inner, &cluster).await;
        }
    }
}

/// One pass over the sweep records.
pub(super) async fn pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>) {
    let blobs = Arc::clone(&inner.blobs);
    let Some(epoch) = blobs.epoch().map(str::to_owned) else {
        return;
    };
    let records = match blobs.records().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "blob sweep: listing the sweep records failed");
            return;
        }
    };
    let now = now_ms();
    for l in records.into_iter().take(PER_PASS) {
        let age = now.saturating_sub(l.modified_ms);
        let Ok(Some(record)) = blobs.read_record(&l).await else {
            continue;
        };
        // Another epoch's files go with it (the epochs pass); a record
        // whose name is not its key's is not acted on.
        if record.epoch != epoch || crate::blobs::keys::name_id(&record.name) != l.name_id {
            continue;
        }
        let outcome = match blob_sweep::action_at(record.state, age, cluster.blob_sweep_ages) {
            Action::Wait => continue,
            Action::Judge => match judge_one(inner, &l.class, &record.name, &l.incarnation).await {
                Some(Judged::Deleted) => match blobs
                    .write_record(&l.class, &record.name, &l.incarnation, State::Swept)
                    .await
                {
                    Ok(()) => blobs
                        .remove_incarnation(&l.class, &l.name_id, &l.incarnation)
                        .await
                        .map(|_| ()),
                    Err(e) => Err(e),
                },
                Some(Judged::NotDeleted) => blobs.drop_record(&l).await,
                None => continue,
            },
            Action::Relist => match blobs
                .remove_incarnation(&l.class, &l.name_id, &l.incarnation)
                .await
            {
                Ok(_) => {
                    blobs
                        .write_record(&l.class, &record.name, &l.incarnation, State::Listed)
                        .await
                }
                Err(e) => Err(e),
            },
            Action::Finish => match blobs
                .remove_incarnation(&l.class, &l.name_id, &l.incarnation)
                .await
            {
                Ok(_) => blobs.drop_record(&l).await,
                Err(e) => Err(e),
            },
        };
        if let Err(e) = outcome {
            tracing::warn!(class = %l.class, error = %e, "blob sweep: a record was not acted on; the next pass tries again");
        }
    }
}

/// A `pending` record's judging job, in the actor's mailbox.
async fn judge_one(
    inner: &Arc<ServiceInner>,
    class: &str,
    name: &str,
    incarnation: &str,
) -> Option<Judged> {
    let address = ActorAddress::new(&inner.tenant, class, name).ok()?;
    if !read(&inner.classes).contains_key(class) {
        return None;
    }
    let recorded = incarnation.to_owned();
    let resident = inner.mailboxes.contains(&address);
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.judge_files(recorded))
            });
    let judged = match tokio::time::timeout(JUDGE_WAIT, job).await {
        Ok(Ok(j)) => j,
        _ => None,
    };
    // A hold the job took is given back; an actor resident before is left.
    if !resident {
        inner.mailboxes.hibernate(&address);
    }
    judged
}

/// Removes the files of every epoch other than the app's own, once the
/// markers, read again, still give this runtime's epoch: a runtime that
/// started before a re-creation or an in-place restore removes nothing.
pub(super) async fn epochs_pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>) {
    let blobs = Arc::clone(&inner.blobs);
    let Some(epoch) = blobs.epoch().map(str::to_owned) else {
        return;
    };
    let root = cluster.state_root();
    let fresh = match blobs
        .fresh_epoch(root.prefix(), root.tenant(), root.app())
        .await
    {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "blob sweep: reading the app's markers failed; no epoch removed");
            return;
        }
    };
    if fresh != epoch || !cluster.lease_valid_now() {
        return;
    }
    let others = match blobs.other_epochs().await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(error = %e, "blob sweep: listing the epochs failed");
            return;
        }
    };
    for other in others {
        if !cluster.lease_valid_now() {
            return;
        }
        match blobs.remove_epoch(&other).await {
            Ok(n) if n > 0 => {
                tracing::info!(epoch = %other, removed = n, "removed the files of an earlier epoch of the app")
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(epoch = %other, error = %e, "removing an earlier epoch's files failed")
            }
        }
    }
}
