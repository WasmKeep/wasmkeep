//! Deleting one actor on an operator's request
//! ([ADR 0028](../../../../docs/adr/0028-actors-that-end.md), section 3).
//!
//! `wasmkeep actor delete` writes a request under the app's `deletes/`.
//! Every node of the app lists it with the moves. The node that holds the
//! actor (or keeps its record parked, or has its sockets) closes the
//! sockets with 1000 and queues the delete job in the actor's mailbox; if
//! nobody holds it, any node serving the class queues the job, and the
//! ordinary conditional acquire picks one. The job runs no guest code: it
//! acquires and restores, then writes the delete as `delete-self` does,
//! with the request's time and id as a marker in the empty database, so
//! that a request listed again is never carried out twice. The node writes
//! the request's outcome beside it, then deletes it, only once the reset
//! is acknowledged (or, for a request found carried out, once this hold's
//! own upload ends the history).

use std::collections::HashSet;
use std::sync::Mutex;

use super::*;
use wasmkeep_cluster::control::{DeleteIntent, DeleteOutcome};

/// Requests acted on at once per node.
const DELETES_PARALLEL: usize = 16;
/// How long the owner waits for an actor's sockets to finish closing.
const SOCKETS_CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long the pass waits for the job (the events queued before it, an
/// acquire and a restore).
const DELETE_WAIT: Duration = Duration::from_secs(60);
/// What a deleted actor's sockets are closed with: not 1012, which would
/// bring clients straight back to recreate an open class's actor.
const CLOSE_CODE: u16 = 1000;
const CLOSE_REASON: &str = "actor deleted";

/// Request keys being acted on.
pub(super) type Active = Arc<Mutex<HashSet<String>>>;

/// What a delete job did.
#[derive(Debug)]
pub(super) enum Deleted {
    /// The delete is in the bucket (now, or by an earlier run of this
    /// request or a later one).
    Done,
    /// No such actor: nothing to delete, nothing created.
    NoSuchActor,
    /// Another node holds the actor: it acts.
    HeldBy(String),
    /// A socket is open or closing: tried again on a later pass.
    Busy,
    /// Refused or failed (draining or moving, the bucket, the restore):
    /// tried again on a later pass.
    Refused(String),
}

/// Whether the marker an actor's database holds (`<ms> <id>`, or `<ms>`
/// alone) says request `id`, written at `at_ms`, is already carried out:
/// by itself, or by a request written after it.
fn carried_out(marker: Option<&str>, at_ms: u64, id: &str) -> bool {
    let Some(marker) = marker else {
        return false;
    };
    let (ms, done_id) = marker.split_once(' ').unwrap_or((marker, ""));
    match ms.parse::<u64>() {
        Ok(ms) => ms > at_ms || (ms == at_ms && done_id == id),
        Err(_) => false,
    }
}

impl ActorState {
    /// The delete job: forget learned owners, check the actor exists,
    /// acquire and restore, then delete it unless this request (or a later
    /// one) already did. `at_ms` is the request's time, the bucket's clock,
    /// and `id` its own id. Sockets are closed (and the job answers
    /// `Busy`) only once the delete is known to be needed.
    pub(super) async fn operator_delete(&mut self, at_ms: u64, id: String) -> Deleted {
        self.owner_hint = None;
        match self.exists().await {
            Ok(true) => {}
            Ok(false) => return Deleted::NoSuchActor,
            Err(e) => return Deleted::Refused(e),
        }
        match self.ensure_owner().await {
            Ok(()) => {}
            Err(OwnershipError::NotOwner { owner, .. }) => return Deleted::HeldBy(owner),
            Err(OwnershipError::Elsewhere { .. }) => {
                return Deleted::Refused("this node is draining or moving the actor".into());
            }
            Err(
                OwnershipError::Unavailable(e)
                | OwnershipError::Refused(e)
                | OwnershipError::Broken(e),
            ) => return Deleted::Refused(e),
        }
        // Held: only a holder's reset counts, and only an acknowledged one
        // makes the request done.
        if self.cluster.is_some() && self.replica.is_none() {
            return Deleted::Refused("no replication for the actor".into());
        }
        let (marker, stale, _) = match self.delete_marks().await {
            Ok(read) => read,
            Err(e) => return Deleted::Refused(format!("reading the database: {e}")),
        };
        if carried_out(marker.as_deref(), at_ms, &id) {
            // What was restored is the state only once this hold's own
            // upload ends the history: until then an earlier holder's
            // upload landing late could still become it (ADR 0007), and
            // the delete read here would be off the chain.
            if self.cluster.is_some() && !self.fence().await {
                return Deleted::Refused("the restored delete is not this hold's yet".into());
            }
            return Deleted::Done;
        }
        if self.sockets.reserved_by(&self.address) > 0 {
            for conn in self.sockets.of(&self.address, None) {
                self.sockets
                    .close(&self.address, conn, CLOSE_CODE, CLOSE_REASON.into());
            }
            return Deleted::Busy;
        }
        self.alarms.clear(self.address.clone());
        match self
            .delete_recorded(stale, Some(format!("{at_ms} {id}")), true)
            .await
        {
            Ok(()) => {
                crate::metrics::deleted("operator");
                Deleted::Done
            }
            Err(e) => {
                crate::metrics::delete_failed("operator");
                Deleted::Refused(format!("recording the delete: {e}"))
            }
        }
    }

    /// The operator delete marker, the alarm index entry and the pending
    /// alarm the actor's database holds, read from the open instance or the
    /// restored database (opened without instantiating the component).
    pub(super) async fn delete_marks(
        &mut self,
    ) -> Result<(Option<String>, Option<Entry>, Option<u64>), String> {
        type Read = (Option<String>, Option<String>, Option<u64>);
        let read = |db: &mut ActorDb| -> Result<Read, String> {
            let marker = db.meta(OPERATOR_DELETE_KEY).map_err(|e| e.to_string())?;
            let indexed = db.meta(ALARM_INDEXED_KEY).map_err(|e| e.to_string())?;
            let alarm = db.alarm().map_err(|e| e.to_string())?;
            Ok((marker, indexed, alarm))
        };
        let (marker, indexed, alarm) = if let Some(live) = self.live.as_mut() {
            with_db(&mut live.store, read)
                .await
                .map_err(|e| format!("{e:#}"))??
        } else {
            let db = match self.db.take() {
                Some(db) => db,
                None => {
                    let storage = self.storage.clone();
                    let a = self.address.clone();
                    let residency = self.residency.clone();
                    tokio::task::spawn_blocking(move || {
                        storage
                            .open(a.class(), a.name())
                            .map(|db| residency.retain(db))
                            .map_err(|e| format!("{e:#}"))
                    })
                    .await
                    .map_err(|e| e.to_string())??
                }
            };
            let (db, out) = tokio::task::spawn_blocking(move || {
                let mut db = db;
                let out = read(&mut db);
                (db, out)
            })
            .await
            .map_err(|e| e.to_string())?;
            self.db = Some(db);
            out?
        };
        Ok((
            marker,
            indexed.as_deref().and_then(alarm_index::parse_meta),
            alarm,
        ))
    }
}

/// Starts acting on the delete requests that concern this node, each in
/// its own task, at most [`DELETES_PARALLEL`] at once.
pub(super) fn deletes_pass(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, active: &Active) {
    for intent in cluster.deletes() {
        let key = intent.key();
        {
            let mut active = active.lock().unwrap_or_else(|p| p.into_inner());
            if active.contains(&key) || active.len() >= DELETES_PARALLEL {
                continue; // under way, or enough at once: a later pass
            }
            active.insert(key.clone());
        }
        let (inner, cluster, active) = (Arc::clone(inner), Arc::clone(cluster), Arc::clone(active));
        tokio::spawn(async move {
            delete_one(&inner, &cluster, &intent).await;
            active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
        });
    }
}

/// Acts on one delete request, as the actor's owner or, if nobody holds
/// it, as any node serving its class.
async fn delete_one(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, intent: &DeleteIntent) {
    // Listed under this node's own root: its app, so its tenant.
    if intent.tenant != inner.tenant {
        return;
    }
    let Ok(address) = ActorAddress::new(&intent.tenant, &intent.class, &intent.name) else {
        // Not an address: dropped, with no outcome (the command says so).
        cluster.end_delete_request(intent, None).await;
        return;
    };
    // A class this node does not serve (yet): left to the nodes that do.
    if !read(&inner.classes).contains_key(address.class()) {
        return;
    }
    let ours = cluster.holds_here(&address)
        || cluster.is_parked(&address)
        || inner.sockets.has_open(&address);
    if !ours {
        // A draining node takes no actor; another live owner acts itself.
        if cluster.draining() {
            return;
        }
        match cluster.other_owner(&address).await {
            Ok(None) => {}
            Ok(Some(_)) | Err(_) => return,
        }
    }
    inner.forget_owner(&address);
    let (at, id) = (intent.written_at_ms, intent.id.clone());
    let run = || {
        let id = id.clone();
        let job =
            inner
                .mailboxes
                .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                    Box::pin(state.operator_delete(at, id))
                });
        async move {
            match tokio::time::timeout(DELETE_WAIT, job).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(e)) => Deleted::Refused(e.to_string()),
                Err(_) => Deleted::Refused("the delete job did not finish in time".into()),
            }
        }
    };
    let mut outcome = run().await;
    if matches!(outcome, Deleted::Busy) {
        // The job closed the sockets: their close events run in the
        // mailbox, then the job once more.
        let started = tokio::time::Instant::now();
        while inner.sockets.reserved_by(&address) > 0 && started.elapsed() < SOCKETS_CLOSE_WAIT {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        outcome = run().await;
    }
    match outcome {
        Deleted::Done => {
            tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), request_ms = at, "deleted on request");
            cluster
                .end_delete_request(intent, Some(DeleteOutcome::Done))
                .await;
            // Released: the next event, wherever it lands, restores the
            // empty database.
            inner.mailboxes.hibernate(&address);
        }
        Deleted::NoSuchActor => {
            tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), "delete request: no such actor; dropped");
            cluster
                .end_delete_request(intent, Some(DeleteOutcome::Absent))
                .await;
            inner.mailboxes.hibernate(&address);
        }
        Deleted::HeldBy(owner) => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %owner, "delete request: another node holds the actor");
            inner.mailboxes.hibernate(&address);
        }
        Deleted::Busy => {}
        Deleted::Refused(why) => {
            tracing::debug!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), %why, "delete request: tried again later");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::carried_out;

    #[test]
    fn a_marker_covers_its_own_request_and_older_ones_only() {
        assert!(!carried_out(None, 5, "a"));
        assert!(carried_out(Some("5 a"), 5, "a"));
        assert!(carried_out(Some("6 b"), 5, "a"));
        // The same millisecond (a bucket with second precision): only the
        // same request.
        assert!(!carried_out(Some("5 b"), 5, "a"));
        assert!(!carried_out(Some("4 a"), 5, "a"));
        assert!(!carried_out(Some("junk"), 5, "a"));
        assert!(carried_out(Some("6"), 5, "a"));
    }
}
