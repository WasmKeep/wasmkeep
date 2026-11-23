//! Idle expiry ([ADR 0028](../../../../docs/adr/0028-actors-that-end.md),
//! section 4): a class may say how long its actors may stay idle before
//! they are deleted.
//!
//! One node of the app collects: the lowest-id live node that serves and
//! announces `ends`, once every pass period (ten minutes). Per class with a
//! limit it lists `owners/<class>/` once (keys, last-modified times and
//! `ETag`s) and the app's `alarms/` once, and takes as candidates the
//! records last written more than the limit ago whose actor has no alarm
//! entry, oldest first, at most [`MAX_PER_PASS`](wasmkeep_cluster::expiry::MAX_PER_PASS)
//! per pass, [`PARALLEL`](wasmkeep_cluster::expiry::PARALLEL) at a time.
//! Each candidate's job runs in the actor's mailbox and no guest code: an
//! acquisition that takes the record only if it names nobody and is still
//! the version listed (a wake since then rewrote it, so the expiry loses),
//! a restore, a check of the restored database for a pending alarm, then
//! the delete routine of `delete-self`. Once the job holds the actor, an
//! event arriving elsewhere is forwarded here and queued after it: it finds
//! a new, empty actor. An actor resident here, parked here, or with a
//! socket here is never a candidate's job's victim: those are activity.

use std::collections::{BTreeSet, HashSet};

use super::*;
use crate::cluster::RECORD_CHANGED;
use wasmkeep_cluster::expiry::{Listed, MAX_PER_PASS, PARALLEL, candidates};

/// How long a pass waits for one job (the events queued before it, an
/// acquire and a restore).
const EXPIRE_WAIT: Duration = Duration::from_secs(60);

/// Records an earlier pass found changed or held, kept at most this many
/// (then forgotten, and listed again).
const SKIPPED_MAX: usize = 100_000;

/// What an expiry job did.
#[derive(Debug)]
pub(super) enum Expired {
    /// Deleted: the reset is in the bucket.
    Done,
    /// Not deleted: the record changed since the listing (a wake), or
    /// names a holder.
    Changed,
    /// Not deleted, and why (`alarm`, `socket`, `active`): a label of
    /// `wasmkeep_expiry_kept_total`.
    Kept(&'static str),
    /// Not deleted: refused or failed (the bucket, the restore).
    Refused(String),
}

impl ActorState {
    /// The expiry job: acquire the actor only as the listing showed its
    /// record (`listed`, its `ETag`), restore it, and delete it unless its
    /// database holds an alarm or it has a socket. Runs no guest code.
    pub(super) async fn expire_idle(&mut self, listed: String, idle_origin_ms: u64) -> Expired {
        let Some(cluster) = self.cluster.clone() else {
            return Expired::Refused("not in a cluster".into());
        };
        // Held or running here: in use, whatever the listing said. Never
        // taken as a hold for the expiry: only its own acquisition counts.
        if self.held.is_some() || self.live.is_some() {
            return Expired::Kept("active");
        }
        if cluster.is_parked(&self.address) || self.sockets.reserved_by(&self.address) > 0 {
            return Expired::Kept("socket");
        }
        self.owner_hint = None;
        match self
            .ensure_owner_with(Some((&listed, idle_origin_ms)), false)
            .await
        {
            Ok(()) => {}
            Err(OwnershipError::Refused(why)) if why == RECORD_CHANGED => {
                return Expired::Changed;
            }
            Err(OwnershipError::NotOwner { .. }) => return Expired::Changed,
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => return Expired::Refused(e),
            Err(OwnershipError::Elsewhere { .. }) => {
                return Expired::Refused("this node is draining or moving the actor".into());
            }
        }
        if self.replica().is_none() {
            return Expired::Refused("no replication for the actor".into());
        }
        let (marker, stale, alarm) = match self.delete_marks().await {
            Ok(read) => read,
            Err(e) => return Expired::Refused(format!("reading the database: {e}")),
        };
        // An alarm is activity to come: the alarm listing missed it (an
        // entry not written, or written since), the database did not.
        if alarm.is_some() {
            return Expired::Kept("alarm");
        }
        // A socket opened while the job acquired (its upgrade waits behind
        // the job in the mailbox, but a reservation shows it).
        if self.sockets.reserved_by(&self.address) > 0 {
            return Expired::Kept("socket");
        }
        // The operator delete's marker is carried over, as `delete-self`
        // carries it: a request left behind never deletes a later actor.
        match self.delete_recorded(stale, marker, true).await {
            Ok(()) => {
                crate::metrics::deleted("expired");
                Expired::Done
            }
            Err(e) => {
                crate::metrics::delete_failed("expired");
                Expired::Refused(format!("recording the delete: {e}"))
            }
        }
    }
}

/// Every pass period, while this node collects: one pass per class with a
/// limit. Runs until the service is dropped or stops.
pub(super) async fn expiry_loop(service: std::sync::Weak<ServiceInner>, cluster: Arc<Cluster>) {
    let every = cluster.expiry_every;
    // `(class, name, ETag)` of records an earlier pass found changed or
    // held: the same version is never tried again, so old records of
    // long-resident actors do not take a pass's whole budget.
    let mut skipped: HashSet<(String, String, String)> = HashSet::new();
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = service.upgrade() else {
            return;
        };
        if inner.mailboxes.is_closed() {
            return;
        }
        let limits: Vec<(String, Duration)> = inner
            .expiry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(c, d)| (c.clone(), *d))
            .collect();
        if limits.is_empty()
            || cluster.app_refusal().is_some()
            || inner
                .deploy_behind
                .load(std::sync::atomic::Ordering::Acquire)
            || !cluster.collects_expiry()
        {
            continue;
        }
        if skipped.len() > SKIPPED_MAX {
            skipped.clear();
        }
        crate::metrics::inc(&crate::metrics::METRICS.expiry_passes);
        pass(&inner, &cluster, &limits, &mut skipped).await;
    }
}

/// One pass: the alarm index once, then each class's records.
async fn pass(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    limits: &[(String, Duration)],
    skipped: &mut HashSet<(String, String, String)>,
) {
    let alarmed = match cluster.alarmed_actors().await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(error = %e, "idle expiry: listing the alarm index failed; the pass is skipped");
            return;
        }
    };
    let mut budget = MAX_PER_PASS;
    for (class, idle) in limits {
        if budget == 0 {
            break;
        }
        if !read(&inner.classes).contains_key(class) {
            continue;
        }
        // Before the listing: a listing slow to come back never makes a
        // record look idler than it was when it was taken.
        let now = now_ms();
        let mut listed = match cluster.list_owner_records(class).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(%class, error = %e, "idle expiry: listing the ownership records failed");
                continue;
            }
        };
        let idle_ms = u64::try_from(idle.as_millis()).unwrap_or(u64::MAX);
        // ADR 0035: a record this node refused was rewritten by its refused
        // cycles; its carried `idle-since` is its idle origin.
        for l in &mut listed {
            if now.saturating_sub(l.modified_ms) > idle_ms {
                continue;
            }
            let Some(name) = wasmkeep_storage::decode_segment(&l.name) else {
                continue;
            };
            if let Ok(address) = ActorAddress::new(&inner.tenant, class, &name) {
                l.idle_since_ms = cluster.carried_idle_since(&address).await;
            }
        }
        let with_alarm: BTreeSet<String> = alarmed
            .iter()
            .filter(|(c, _)| c == class)
            .map(|(_, n)| n.clone())
            .collect();
        let chosen = candidates(
            listed,
            &with_alarm,
            |l| skipped.contains(&(class.clone(), l.name.clone(), l.version.clone())),
            now,
            idle_ms,
            budget,
        );
        budget -= chosen.len();
        crate::metrics::METRICS
            .expiry_candidates
            .fetch_add(chosen.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let outcomes: Vec<(Listed, Expired)> =
            futures_util::StreamExt::collect(futures_util::StreamExt::buffer_unordered(
                futures_util::stream::iter(chosen.into_iter().map(|l| {
                    let (inner, cluster, class) =
                        (Arc::clone(inner), Arc::clone(cluster), class.clone());
                    async move {
                        let outcome = expire_one(&inner, &cluster, &class, &l).await;
                        (l, outcome)
                    }
                })),
                PARALLEL,
            ))
            .await;
        for (l, outcome) in outcomes {
            let reason = match &outcome {
                Expired::Done => continue,
                Expired::Changed => "changed",
                Expired::Kept(reason) => reason,
                Expired::Refused(_) => "failed",
            };
            crate::metrics::expiry_kept(reason);
            if matches!(
                outcome,
                Expired::Changed | Expired::Kept("active" | "socket")
            ) {
                skipped.insert((class.clone(), l.name, l.version));
            }
        }
    }
}

/// One candidate: its job in the actor's mailbox, unless the actor is in
/// use here.
async fn expire_one(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    class: &str,
    listed: &Listed,
) -> Expired {
    // A name shortened with a hash cannot be told back: never expired.
    let Some(name) = wasmkeep_storage::decode_segment(&listed.name) else {
        return Expired::Kept("active");
    };
    let Ok(address) = ActorAddress::new(&inner.tenant, class, &name) else {
        return Expired::Refused("not an actor address".into());
    };
    if inner.mailboxes.contains(&address)
        || cluster.holds_here(&address)
        || cluster.is_parked(&address)
        || inner.sockets.has_open(&address)
    {
        return Expired::Kept("active");
    }
    let etag = listed.version.clone();
    let origin = listed.idle_origin_ms();
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.expire_idle(etag, origin))
            });
    let outcome = match tokio::time::timeout(EXPIRE_WAIT, job).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(e)) => Expired::Refused(e.to_string()),
        Err(_) => Expired::Refused("the expiry did not finish in time".into()),
    };
    match &outcome {
        Expired::Done => {
            tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), "deleted after being idle");
        }
        Expired::Refused(why) => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %why, "idle expiry: not deleted");
        }
        Expired::Changed | Expired::Kept(_) => {}
    }
    // Released (or never acquired): the next event, wherever it lands,
    // restores the actor. One found in use is left as it is.
    if !matches!(outcome, Expired::Kept("active")) {
        inner.mailboxes.hibernate(&address);
    }
    outcome
}
