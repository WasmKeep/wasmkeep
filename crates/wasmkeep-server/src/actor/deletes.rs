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
//! with the request's time as a marker in the empty database, so that a
//! request listed again is never carried out twice. The request's key is
//! deleted only once the reset is acknowledged.

use std::collections::HashSet;
use std::sync::Mutex;

use super::*;
use wasmkeep_cluster::control::DeleteIntent;

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

impl ActorState {
    /// The delete job: forget learned owners, check the actor exists,
    /// acquire and restore, then delete it unless this request (or a later
    /// one) already did. `at_ms` is the request's time, the bucket's clock.
    pub(super) async fn operator_delete(&mut self, at_ms: u64) -> Deleted {
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
        if self.sockets.reserved_by(&self.address) > 0 {
            for conn in self.sockets.of(&self.address, None) {
                self.sockets
                    .close(&self.address, conn, CLOSE_CODE, CLOSE_REASON.into());
            }
            return Deleted::Busy;
        }
        let (marker, stale) = match self.delete_marks().await {
            Ok(read) => read,
            Err(e) => return Deleted::Refused(format!("reading the database: {e}")),
        };
        if marker.is_some_and(|done| done >= at_ms) {
            return Deleted::Done;
        }
        self.alarms.clear(self.address.clone());
        match self
            .delete_recorded(stale, Some(at_ms.to_string()), true)
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

    /// The operator delete marker and the alarm index entry the actor's
    /// database holds, read from the open instance or the restored database
    /// (opened without instantiating the component).
    async fn delete_marks(&mut self) -> Result<(Option<u64>, Option<Entry>), String> {
        let read = |db: &mut ActorDb| -> Result<(Option<String>, Option<String>), String> {
            let marker = db.meta(OPERATOR_DELETE_KEY).map_err(|e| e.to_string())?;
            let indexed = db.meta(ALARM_INDEXED_KEY).map_err(|e| e.to_string())?;
            Ok((marker, indexed))
        };
        let (marker, indexed) = if let Some(live) = self.live.as_mut() {
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
            marker.and_then(|v| v.parse().ok()),
            indexed.as_deref().and_then(alarm_index::parse_meta),
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
        cluster.delete_request(intent).await;
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
    // Sockets first: their close events run before the delete, here.
    let open = inner.sockets.of(&address, None);
    if !open.is_empty() {
        for conn in open {
            inner
                .sockets
                .close(&address, conn, CLOSE_CODE, CLOSE_REASON.into());
        }
        let started = tokio::time::Instant::now();
        while inner.sockets.reserved_by(&address) > 0 && started.elapsed() < SOCKETS_CLOSE_WAIT {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    inner.forget_owner(&address);
    let at = intent.written_at_ms;
    let job =
        inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.operator_delete(at))
            });
    let outcome = match tokio::time::timeout(DELETE_WAIT, job).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(e)) => Deleted::Refused(e.to_string()),
        Err(_) => Deleted::Refused("the delete job did not finish in time".into()),
    };
    match outcome {
        Deleted::Done => {
            tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), request_ms = at, "deleted on request");
            cluster.delete_request(intent).await;
            // Released: the next event, wherever it lands, restores the
            // empty database.
            inner.mailboxes.hibernate(&address);
        }
        Deleted::NoSuchActor => {
            tracing::info!(app = %crate::logging::app(&address), actor = %crate::logging::actor(&address), "delete request: no such actor; dropped");
            cluster.delete_request(intent).await;
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
