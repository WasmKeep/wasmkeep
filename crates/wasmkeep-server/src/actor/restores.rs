//! Live single-actor replacement: a new owner-fenced write, never a history edit.
use super::*;
use wasmkeep_cluster::{
    actor_restore::{Request as RestoreRequest, replay},
    control::RestoreIntent,
};
use wasmkeep_storage::wal::{Changes, Restorer};

impl ActorState {
    /// Read host metadata without invoking guest code. Also used before deletes
    /// close the old database, so every reset carries the last restore marker.
    pub(super) async fn restore_meta(
        &mut self,
        key: &'static str,
    ) -> Result<Option<String>, String> {
        if let Some(live) = self.live.as_mut() {
            return with_db(&mut live.store, move |db| db.meta(key))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string());
        }
        if let Some(db) = self.db.as_mut() {
            return db.meta(key).map_err(|e| e.to_string());
        }
        let storage = self.storage.clone();
        let address = self.address.clone();
        tokio::task::spawn_blocking(move || {
            storage
                .open(address.class(), address.name())
                .map_err(|e| e.to_string())?
                .meta(key)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }
    async fn retire_restore_alarms(
        &mut self,
        cluster: &Cluster,
        generation: u64,
    ) -> Result<(), String> {
        let debt = self
            .restore_meta("restore_stale_alarms")
            .await?
            .unwrap_or_default();
        for entry in debt.lines().filter_map(alarm_index::parse_meta) {
            if !self.fence().await
                || !self
                    .replica()
                    .ok_or("no replica")?
                    .lock()
                    .await
                    .collector()
                    .lease_valid()
                || !alarm_index::may_delete(&entry, generation)
            {
                return Err("restore cleanup lost its fence".into());
            }
            cluster
                .delete_alarm_entry(&self.address, &entry)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Restored {
    End(&'static str, String),
    Retry,
    Busy,
}

impl ActorState {
    async fn operator_restore(&mut self, intent: RestoreIntent) -> Restored {
        let Some(cluster) = self.cluster.clone() else {
            return Restored::Retry;
        };
        if !cluster.actor_restore_allowed().await {
            return Restored::Retry;
        }
        self.owner_hint = None;
        if self.ensure_owner().await.is_err() {
            return Restored::Retry;
        }
        let Some(cluster) = self.cluster.clone() else {
            return Restored::Retry;
        };
        let Some(slot) = self.replica().cloned() else {
            return Restored::Retry;
        };
        if slot.lock().await.needs_snapshot() {
            return Restored::End("absent", "no such actor".into());
        }
        if self.batched_replica().is_some() {
            let deadline = self.upload_deadline().await;
            if self.flush_to_barrier(Some(deadline)).await != Some(Barrier::Flushed) {
                self.abandon("restore flush did not reach Flushed");
                return Restored::Retry;
            }
        }
        let marker = match self.restore_meta(OPERATOR_RESTORE_KEY).await {
            Ok(v) => v,
            Err(_) => return Restored::Retry,
        };
        let replaced = match self.restore_meta("replaced").await {
            Ok(v) => v,
            Err(_) => return Restored::Retry,
        };
        let replaced = if replay(marker.as_deref(), None, intent.written_at_ms, &intent.id)
            .is_none()
            && replaced
                .as_deref()
                .is_some_and(|v| v.starts_with("delete "))
        {
            match slot
                .lock()
                .await
                .delete_time(replaced.as_deref().unwrap_or_default())
                .await
            {
                Ok(at) => Some(format!("delete 0 {at}")),
                Err(e) => return Restored::End("refused", e.to_string()),
            }
        } else {
            replaced
        };
        if let Some(outcome) = replay(
            marker.as_deref(),
            replaced.as_deref(),
            intent.written_at_ms,
            &intent.id,
        ) {
            let marks = self.delete_marks().await;
            if marks.is_err() || !self.fence().await {
                return Restored::Retry;
            }
            if let Ok((_, _, alarm)) = marks {
                self.alarms.clear(self.address.clone());
                if let Some(at) = alarm {
                    self.alarms.set(self.address.clone(), at);
                }
            }
            let head = slot.lock().await.head();
            if !cluster
                .restore_outcome(
                    &intent,
                    outcome,
                    &format!("{}:{}", head.generation, head.txid),
                )
                .await
            {
                return Restored::Retry;
            }
            let generation = slot.lock().await.generation();
            if self
                .retire_restore_alarms(&cluster, generation)
                .await
                .is_err()
            {
                return Restored::Retry;
            }
            let head = slot.lock().await.head();
            return Restored::End(outcome, format!("{}:{}", head.generation, head.txid));
        }
        if self.sockets.reserved_by(&self.address) > 0 {
            for conn in self.sockets.of(&self.address, None) {
                self.sockets
                    .close(&self.address, conn, 1012, "actor restored".into());
            }
            return Restored::Busy;
        }
        let request = match cluster.restore_object(&intent.key(), 4096).await {
            Ok(bytes) => match RestoreRequest::decode(&bytes) {
                Some(r) => r,
                None => return Restored::End("refused", "invalid restore request".into()),
            },
            Err(_) => return Restored::Retry,
        };
        let head = slot.lock().await.head();
        if request.if_head.is_some_and(|expected| expected != head) {
            return Restored::End("conflict", format!("{}:{}", head.generation, head.txid));
        }
        let cap = self.storage.config().max_db_bytes;
        if request.size > cap.saturating_mul(2) {
            return Restored::End("refused", "staged object exceeds limit".into());
        }
        let bytes = match cluster
            .restore_object(&format!("{}.ltx", intent.key()), request.size)
            .await
        {
            Ok(v) => v,
            Err(e) if e.contains("exceeds limit") => return Restored::End("refused", e),
            Err(_) => return Restored::Retry,
        };
        let (delete, stale, _) = match self.delete_marks().await {
            Ok(v) => v,
            Err(_) => return Restored::Retry,
        };
        if head.txid == u64::MAX {
            return Restored::End("refused", "transaction id exhausted".into());
        }
        let mut stale_debt = String::new();
        if let Some(stale) = &stale {
            stale_debt.push_str(&alarm_index::to_meta(Some(stale)));
            stale_debt.push('\n');
        }
        // Scratch files are unique per attempt, including concurrent requests.
        let scratch = self.storage.scratch_dir().join(format!(
            "restore-{}",
            wasmkeep_cluster::control::new_request_id()
        ));
        let storage = Storage::new(
            wasmkeep_storage::StorageConfig {
                data_dir: scratch.clone(),
                ..self.storage.config().clone()
            },
            self.storage.state().clone(),
        );
        let a = self.address.clone();
        let current_path = self.storage.db_path(a.class(), a.name());
        let request_copy = request.clone();
        let staged = tokio::task::spawn_blocking(move || -> Result<ActorDb, String> {
            use std::io::Read as _;
            let mut header = [0; 100];
            std::fs::File::open(current_path)
                .and_then(|mut f| f.read_exact(&mut header))
                .map_err(|e| e.to_string())?;
            let current_size = u16::from_be_bytes([header[16], header[17]]);
            let current_size = if current_size == 1 {
                65536
            } else {
                u32::from(current_size)
            };
            if current_size != request_copy.page_size {
                return Err("page size differs from current database".into());
            }
            let path = storage.db_path(a.class(), a.name());
            std::fs::create_dir_all(path.parent().ok_or("scratch path has no parent")?)
                .map_err(|e| e.to_string())?;
            let file = wasmkeep_storage::ltx::decode(&bytes).map_err(|e| e.to_string())?;
            if bytes.len() as u64 != request_copy.size
                || !file.header.is_snapshot()
                || file.header.no_checksum()
                || file.header.page_size != request_copy.page_size
                || file.trailer.post_apply_checksum != request_copy.checksum
                || file
                    .pages
                    .first()
                    .is_none_or(|p| p.1.get(18..20) != Some(&[2, 2]))
            {
                return Err("staged snapshot metadata or WAL header mismatch".into());
            }
            let mut restorer = Restorer::create(&path, cap).map_err(|e| e.to_string())?;
            restorer.apply_file(&file).map_err(|e| e.to_string())?;
            if restorer.finish().map_err(|e| e.to_string())?.value() != request_copy.checksum {
                return Err("staged checksum mismatch".into());
            }
            storage.open(a.class(), a.name()).map_err(|e| e.to_string())
        })
        .await;
        let mut db = match staged {
            Ok(Ok(db)) => db,
            other => {
                let reason = match other {
                    Ok(Err(e)) => e,
                    Err(e) => e.to_string(),
                    _ => unreachable!(),
                };
                let _ = std::fs::remove_dir_all(&scratch);
                return Restored::End("refused", reason);
            }
        };
        let alarm = match db.alarm() {
            Ok(a) => a,
            Err(_) => {
                drop(db);
                let _ = std::fs::remove_dir_all(&scratch);
                return Restored::Retry;
            }
        };
        let generation = slot.lock().await.generation();
        // The previous attempt may have uploaded without receiving an answer.
        // Fence the restored head before retiring entries not named by that head.
        if !self.fence().await
            || self
                .retire_restore_alarms(&cluster, generation)
                .await
                .is_err()
        {
            drop(db);
            let _ = std::fs::remove_dir_all(&scratch);
            return Restored::Retry;
        }
        let Some(txid) = slot.lock().await.head().txid.checked_add(1) else {
            drop(db);
            let _ = std::fs::remove_dir_all(&scratch);
            return Restored::End("refused", "transaction id exhausted".into());
        };
        let entry = if let Some(at) = alarm {
            let candidates = match cluster.alarm_entries_at(&self.address, at).await {
                Ok(v) => v,
                Err(_) => {
                    drop(db);
                    let _ = std::fs::remove_dir_all(&scratch);
                    return Restored::Retry;
                }
            };
            for old in candidates {
                if !slot.lock().await.collector().lease_valid() {
                    drop(db);
                    let _ = std::fs::remove_dir_all(&scratch);
                    return Restored::Retry;
                }
                if Some(&old) != stale.as_ref()
                    && alarm_index::may_delete(&old, generation)
                    && cluster
                        .delete_alarm_entry(&self.address, &old)
                        .await
                        .is_err()
                {
                    drop(db);
                    let _ = std::fs::remove_dir_all(&scratch);
                    return Restored::Retry;
                }
            }
            match cluster.name_alarm_entry(at, generation) {
                Ok(entry) => {
                    if cluster
                        .put_alarm_entry(&self.address, &entry)
                        .await
                        .is_err()
                    {
                        let _ = cluster.delete_alarm_entry(&self.address, &entry).await;
                        drop(db);
                        let _ = std::fs::remove_dir_all(&scratch);
                        return Restored::Retry;
                    }
                    Some(entry)
                }
                Err(_) => {
                    drop(db);
                    let _ = std::fs::remove_dir_all(&scratch);
                    return Restored::Retry;
                }
            }
        } else {
            None
        };
        let mark = format!("{} {}", intent.written_at_ms, intent.id);
        let fresh_entry = entry.clone();
        let cleanup_debt = stale_debt.clone();
        let replacement = format!("restore {txid} {mark}");
        let built = tokio::task::spawn_blocking(move || -> Result<Changes, String> {
            for (key, value) in [
                (OPERATOR_DELETE_KEY, delete.unwrap_or_default()),
                (OPERATOR_RESTORE_KEY, mark),
                ("restore_stale_alarms", stale_debt),
                ("replaced", replacement),
                (ALARM_INDEXED_KEY, alarm_index::to_meta(entry.as_ref())),
                (ALARM_FAILURES_KEY, "0".into()),
            ] {
                db.set_meta(key, &value).map_err(|e| e.to_string())?;
            }
            db.start_capture().map_err(|e| e.to_string())?;
            db.snapshot().map_err(|e| e.to_string())
        })
        .await;
        let _ = std::fs::remove_dir_all(&scratch);
        let all = match built {
            Ok(Ok(all)) => all,
            _ => {
                if let Some(entry) = &fresh_entry {
                    let _ = cluster.delete_alarm_entry(&self.address, entry).await;
                }
                return Restored::Retry;
            }
        };
        // Recheck the app fence immediately before the write. Lease validity
        // also guards the upload acknowledgement and a concurrent owner change.
        if !cluster.actor_restore_allowed().await {
            if let Some(entry) = &fresh_entry {
                let _ = cluster.delete_alarm_entry(&self.address, entry).await;
            }
            return Restored::Retry;
        }
        self.end_capture_at_barrier().await;
        let mut replica = slot.lock().await;
        if replica.ship_reset(all.clone()).await.is_err() {
            drop(replica);
            self.abandon("restore replacement was not acknowledged");
            return Restored::Retry;
        }
        let head = replica.head();
        let body = format!("{}:{}", head.generation, head.txid);
        // Publish done before retiring anything. A failed outcome leaves the
        // request for the next holder, which recognizes its durable marker.
        let mut ended = cluster.restore_outcome(&intent, "done", &body).await;
        if ended {
            for stale in cleanup_debt.lines().filter_map(alarm_index::parse_meta) {
                if !replica.collector().lease_valid()
                    || !alarm_index::may_delete(&stale, generation)
                    || cluster
                        .delete_alarm_entry(&self.address, &stale)
                        .await
                        .is_err()
                {
                    ended = false;
                    break;
                }
            }
            self.alarms.clear(self.address.clone());
            if let Some(at) = alarm {
                self.alarms.set(self.address.clone(), at);
            }
        }
        if ended {
            cluster
                .end_restore_request(&intent, "done", body.clone())
                .await;
        }
        drop(replica);
        self.live = None;
        self.db = None;
        // The actor's local state is discarded now. Keep only the old hold
        // with the detached snapshot task. A takeover invalidates that hold,
        // so a late snapshot never authorizes collection on another owner.
        let held = self.held.take();
        let address = self.address.clone();
        tokio::spawn(async move {
            let mut replica = slot.lock().await;
            if matches!(
                replica
                    .ship_snapshot(&all, crate::replica::SnapshotKind::AfterRestore)
                    .await,
                Ok(true)
            ) {
                let collection = CollectRequest {
                    collector: replica.collector(),
                    snapshot: head,
                    retention: cluster.history_retention,
                    erase: false,
                };
                run_collection(&address, &collection).await;
            }
            drop(replica);
            if let Some(held) = held {
                let barrier = held.barrier();
                held.release(barrier);
            }
        });
        if ended {
            Restored::End("done", body)
        } else {
            Restored::Retry
        }
    }
}

pub(super) fn restores_pass(
    inner: &Arc<ServiceInner>,
    cluster: &Arc<Cluster>,
    active: &deletes::Active,
) {
    for intent in cluster.restores() {
        let key = intent.key();
        {
            let mut running = active.lock().unwrap_or_else(|p| p.into_inner());
            if running.len() >= 16 || !running.insert(key.clone()) {
                continue;
            }
        }
        let (inner, cluster, active) = (inner.clone(), cluster.clone(), active.clone());
        tokio::spawn(async move {
            restore_one(&inner, &cluster, intent).await;
            active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
        });
    }
}
async fn restore_one(inner: &Arc<ServiceInner>, cluster: &Arc<Cluster>, intent: RestoreIntent) {
    let Ok(address) = ActorAddress::new(&intent.tenant, &intent.class, &intent.name) else {
        return;
    };
    if intent.tenant != inner.tenant || !read(&inner.classes).contains_key(address.class()) {
        return;
    }
    if !cluster.holds_here(&address)
        && !cluster.is_parked(&address)
        && !inner.sockets.has_open(&address)
        && (cluster.draining() || !matches!(cluster.other_owner(&address).await, Ok(None)))
    {
        return;
    }
    if !cluster.actor_restore_allowed().await {
        return;
    }
    struct Admission<'a>(&'a ServiceInner, ActorAddress);
    impl Drop for Admission<'_> {
        fn drop(&mut self) {
            let mut active = self.0.restoring.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(count) = active.get_mut(&self.1) {
                *count -= 1;
                if *count == 0 {
                    active.remove(&self.1);
                }
            }
        }
    }
    *inner
        .restoring
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(address.clone())
        .or_default() += 1;
    let _admission = Admission(inner, address.clone());
    inner.forget_owner(&address);
    for attempt in 0..=3 {
        let request = intent.clone();
        // Do not cancel an admitted job: timeout would allow another pass to
        // race its outcome. The mailbox and bucket operations are bounded.
        let result = inner
            .mailboxes
            .submit_wait(&address, ADMISSION_WAIT, move |state: &mut ActorState| {
                Box::pin(state.operator_restore(request))
            })
            .await;
        match result {
            Ok(Restored::End(suffix, body)) => {
                cluster.end_restore_request(&intent, suffix, body).await;
                return;
            }
            Ok(Restored::Busy) if attempt < 3 => {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Ok(Restored::Busy) => {
                cluster
                    .end_restore_request(&intent, "refused", "sockets did not close".into())
                    .await;
                return;
            }
            _ => {
                drop(_admission);
                // Keep the request in the active set during backoff, but reopen
                // socket admission; a failing request cannot churn every heartbeat.
                tokio::time::sleep(Duration::from_secs(2)).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actor_restore_replay_and_delete_supersession() {
        assert_eq!(replay(Some("10 a"), None, 10, "a"), Some("done"));
        assert_eq!(replay(Some("11 b"), None, 10, "a"), Some("superseded"));
        assert_eq!(replay(Some("10 b"), None, 10, "a"), None);
        assert_eq!(
            replay(None, Some("delete 8 11"), 10, "a"),
            Some("superseded")
        );
        assert_eq!(replay(None, Some("delete 8 9"), 10, "a"), None);
        // Every delete carries this marker: a leftover request cannot undo it.
        assert_eq!(
            replay(Some("10 a"), Some("delete 8 11"), 10, "a"),
            Some("done")
        );
    }
    #[test]
    fn actor_restore_empty_marker_is_not_carried_by_delete() {
        for marker in [None, Some("")] {
            let tmp = tempfile::tempdir().unwrap();
            let storage = Storage::new(
                wasmkeep_storage::StorageConfig::new(tmp.path()),
                wasmkeep_storage::StateDir {
                    dir: "actors".into(),
                    scratch: "scratch".into(),
                    tenant: "t".into(),
                },
            );
            let address = ActorAddress::new("t", "c", "a").unwrap();
            let snapshot =
                empty_database_snapshot(&storage, &address, None, marker, "delete 42 10").unwrap();
            let path = storage.db_path("c", "a");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut restorer = Restorer::create(&path, 1 << 20).unwrap();
            restorer
                .apply(&snapshot.to_ltx(1, 42, 10).unwrap())
                .unwrap();
            restorer.finish().unwrap();
            let mut db = storage.open("c", "a").unwrap();
            assert_eq!(db.meta(OPERATOR_RESTORE_KEY).unwrap(), None);
            let mut fresh = storage.open("c", "fresh").unwrap();
            mark_deleted(&mut fresh, None, marker, "delete 42 10").unwrap();
            assert_eq!(fresh.meta(OPERATOR_RESTORE_KEY).unwrap(), None);
        }
    }

    #[test]
    fn actor_restore_marker_survives_both_delete_snapshot_build_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new(
            wasmkeep_storage::StorageConfig::new(tmp.path()),
            wasmkeep_storage::StateDir {
                dir: "actors".into(),
                scratch: "scratch".into(),
                tenant: "t".into(),
            },
        );
        let address = ActorAddress::new("t", "c", "a").unwrap();
        let snapshot =
            empty_database_snapshot(&storage, &address, Some("9 d"), Some("8 r"), "delete 42 10")
                .unwrap();
        let file = snapshot.to_ltx(1, 42, 10).unwrap();
        let path = storage.db_path("c", "a");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut restorer = Restorer::create(&path, 1 << 20).unwrap();
        restorer.apply(&file).unwrap();
        restorer.finish().unwrap();
        let mut db = storage.open("c", "a").unwrap();
        assert_eq!(
            db.meta(OPERATOR_RESTORE_KEY).unwrap().as_deref(),
            Some("8 r")
        );
        assert_eq!(
            db.meta("replaced").unwrap().as_deref(),
            Some("delete 42 10")
        );
        mark_deleted(&mut db, Some("12 d2"), Some("8 r"), "delete 43 12").unwrap();
        assert_eq!(
            db.meta(OPERATOR_RESTORE_KEY).unwrap().as_deref(),
            Some("8 r")
        );
    }
}
