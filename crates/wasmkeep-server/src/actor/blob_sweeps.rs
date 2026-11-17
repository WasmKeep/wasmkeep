//! The sweep pass of deleted actors' files (ADR 0030, sections 5 and 7):
//! one node of the app (the one that collects idle actors) lists
//! `blob-sweeps/` once a minute and acts on each record by its age, and
//! once an hour removes the files of epochs other than the app's own.
//!
//! Every removal is judged first, at the second and last listings too: a
//! job in the actor's mailbox that runs no guest code (an acquisition only
//! of a record naming nobody, as idle expiry acquires, or the hold already
//! here; a restore; the fence; a read of the incarnation an instance would
//! use). Another incarnation sweeps; the same one drops the record in the
//! mailbox, on the version listed, and touches no file; no judgement for
//! 25 hours drops the record and touches no file either.

use super::*;
use wasmkeep_cluster::blob_sweep::{self, Action, Judged};

/// Records a pass acts on, at most.
const PER_PASS: usize = 1_000;
/// How long a pass waits for one judging job.
const JUDGE_WAIT: Duration = Duration::from_secs(60);

impl ActorState {
    /// Judges a sweep record of `recorded` incarnation (`listed`, as the
    /// pass listed it): `None` when this job could not tell (held
    /// elsewhere, a refused or failed acquisition or restore, the fence not
    /// uploaded). A `NotDeleted` record is dropped here, in the mailbox,
    /// and only if it is still the version listed: a delete that rewrote
    /// it since keeps it.
    pub(super) async fn judge_files(
        &mut self,
        listed: crate::blobs::sweep::Listed,
    ) -> Option<Judged> {
        let cluster = self.cluster.clone()?;
        let held_here = self.held.as_ref().is_some_and(Held::is_valid);
        let mut kept_origin = None;
        if !held_here {
            if self.held.is_some() || self.live.is_some() {
                return None;
            }
            // Without the recorded-head format a release always rewrites
            // the record, and with it the idle age: an idle actor is not
            // acquired to judge it (decision 8); its record ages out.
            if !cluster.owners.recorded_head() {
                return None;
            }
            // Only a record naming nobody, as listed, as idle expiry takes
            // it; its idle origin is kept at the release (decision 8).
            let owner = cluster.owner_record_listed(&self.address).await.ok()??;
            let origin = owner.idle_origin_ms();
            self.owner_hint = None;
            if self
                .ensure_owner_with(Some((&owner.version, origin)))
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
        let incarnation = self.restore_meta(BLOB_INCARNATION_KEY).await.ok()?;
        let replaced = self.restore_meta("replaced").await.ok()?;
        // The incarnation an instance would use; `None`: it draws a fresh
        // one, so no file of it is under any record.
        let current = match blob_sweep::start(incarnation.as_deref(), replaced.as_deref()).ok()? {
            blob_sweep::Start::Use(i) => Some(i),
            blob_sweep::Start::Draw => None,
        };
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
        let judged = blob_sweep::judge(&listed.incarnation, current.as_deref());
        if judged == Judged::NotDeleted {
            let blobs = self.blobs()?;
            match blobs.drop_record_if_listed(&listed).await {
                Ok(true) => crate::metrics::blob_sweep(blobs.labels(), "not_deleted"),
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "blob sweep: dropping a record failed");
                    crate::metrics::blob_sweep(blobs.labels(), "failed");
                }
            }
        }
        Some(judged)
    }
}

/// A record's body, by its key, as last read at a version: read again
/// only when the listing shows another version.
type Bodies = std::collections::HashMap<String, (Option<String>, blob_sweep::Record)>;

/// Every pass period, while this node collects. Runs until the service is
/// dropped or stops.
pub(super) async fn sweep_loop(service: std::sync::Weak<ServiceInner>, cluster: Arc<Cluster>) {
    let every = cluster.blob_sweep_every;
    let mut epochs_due = tokio::time::Instant::now() + cluster.blob_epochs_every;
    let mut bodies = Bodies::new();
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return;
        }
        if !inner.blobs.on_a_bucket() || cluster.app_refusal().is_some() {
            continue;
        }
        // Every node takes the app's last count (section 7).
        match inner.blobs.read_usage().await {
            Ok(Some(count)) => inner.blobs.note_usage(&count),
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "reading the app's file count failed"),
        }
        if !cluster.collects_blob_sweeps() {
            continue;
        }
        pass(&inner, &cluster, &mut bodies).await;
        if tokio::time::Instant::now() >= epochs_due {
            epochs_due = tokio::time::Instant::now() + cluster.blob_epochs_every;
            epochs_pass(&inner, &cluster).await;
            count_pass(&inner, &cluster).await;
        }
    }
}

/// One pass over the sweep records: one listing; a body read only for a
/// record old enough to act on whose version changed; then at most
/// [`PER_PASS`] records acted on, every removal judged first.
pub(super) async fn pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, bodies: &mut Bodies) {
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
    let ages = cluster.blob_sweep_ages;
    let now = now_ms();
    let listed: std::collections::HashSet<String> = records.iter().map(|l| l.key()).collect();
    bodies.retain(|k, _| listed.contains(k));
    let mut due = Vec::new();
    for l in records {
        let age = now.saturating_sub(l.modified_ms);
        // No state acts on a record younger than this: no read.
        if age <= ages.judge_after {
            continue;
        }
        let key = l.key();
        let record = match bodies.get(&key) {
            Some((etag, r)) if *etag == l.etag => r.clone(),
            _ => match blobs.read_record(&l).await {
                Ok(Some(r)) => {
                    bodies.insert(key, (l.etag.clone(), r.clone()));
                    r
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(error = %e, "blob sweep: reading a record failed");
                    continue;
                }
            },
        };
        // Another epoch's files go with it (the epochs pass); a record
        // whose name is not its key's is not acted on.
        if record.epoch != epoch || crate::blobs::keys::name_id(&record.name) != l.name_id {
            continue;
        }
        let action = blob_sweep::action_at(record.state, age, ages);
        if action != Action::Wait {
            due.push((l, record, action, age));
        }
        if due.len() >= PER_PASS {
            break;
        }
    }
    for (l, record, action, age) in due {
        let judged = judge_one(inner, &l.class, &record.name, &l).await;
        let done = match blob_sweep::outcome(action, judged, age, ages) {
            blob_sweep::Outcome::Skip | blob_sweep::Outcome::Drop => Ok(()),
            blob_sweep::Outcome::AgeOut => {
                crate::metrics::blob_sweep(inner.blobs.labels(), "aged_out");
                tracing::warn!(class = %l.class, "blob sweep: a record nobody could judge for 25 hours is dropped; its files, if any, stay");
                blobs.drop_record(&l).await
            }
            blob_sweep::Outcome::Sweep(then) => {
                match blobs
                    .remove_incarnation(&l.class, &l.name_id, &l.incarnation)
                    .await
                {
                    Ok(_) => match then {
                        Some(state) => {
                            crate::metrics::blob_sweep(inner.blobs.labels(), "swept");
                            blobs
                                .write_record(&l.class, &record.name, &l.incarnation, state)
                                .await
                        }
                        None => {
                            crate::metrics::blob_sweep(inner.blobs.labels(), "final");
                            blobs.drop_record(&l).await
                        }
                    },
                    Err(e) => Err(e),
                }
            }
        };
        if let Err(e) = done {
            crate::metrics::blob_sweep(inner.blobs.labels(), "failed");
            tracing::warn!(class = %l.class, error = %e, "blob sweep: a record was not acted on; the next pass tries again");
        }
    }
}

/// A record's judging job, in the actor's mailbox.
async fn judge_one(
    inner: &Arc<ServiceInner>,
    class: &str,
    name: &str,
    listed: &crate::blobs::sweep::Listed,
) -> Option<Judged> {
    let address = ActorAddress::new(&inner.tenant, class, name).ok()?;
    if !read(&inner.classes).contains_key(class) {
        return None;
    }
    let listed = listed.clone();
    let resident = inner.mailboxes.contains(&address);
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.judge_files(listed))
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

/// The hourly count of the app's files, written for every node, by the
/// node that collects: only while its epoch is still the app's.
async fn count_pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>) {
    let blobs = Arc::clone(&inner.blobs);
    let root = cluster.state_root();
    match blobs
        .fresh_epoch(root.prefix(), root.tenant(), root.app())
        .await
    {
        Ok(fresh) if Some(fresh.as_str()) == blobs.epoch() => {}
        _ => return,
    }
    match blobs.count().await {
        Ok(count) => {
            if let Err(e) = blobs.publish_usage(&count).await {
                tracing::warn!(error = %e, "writing the app's file count failed");
            }
            blobs.note_usage(&count);
        }
        Err(e) => tracing::warn!(error = %e, "counting the app's files failed"),
    }
}

/// On a node without a bucket: counts the app's files at start, then every
/// `every`. Runs until the service is dropped.
pub(super) async fn disk_count_loop(service: std::sync::Weak<ServiceInner>, every: Duration) {
    loop {
        {
            let Some(inner) = service.upgrade() else {
                return;
            };
            if inner.mailboxes.is_closed() {
                return;
            }
            match inner.blobs.count().await {
                Ok(count) => inner.blobs.note_usage(&count),
                Err(e) => tracing::debug!(error = %e, "counting the app's files failed"),
            }
        }
        tokio::time::sleep(every).await;
    }
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
                crate::metrics::blob_sweep(blobs.labels(), "old_epoch");
                tracing::info!(epoch = %other, removed = n, "removed the files of an earlier epoch of the app")
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(epoch = %other, error = %e, "removing an earlier epoch's files failed")
            }
        }
    }
}
