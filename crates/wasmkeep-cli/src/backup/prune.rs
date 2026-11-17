//! Destination-only retention. Permanent markers close the create/prune ABA
//! window; a separate CAS lock prevents overlapping deletion passes.
use super::*;
use object_store::ObjectStoreExt;

const DAY_MS: u128 = 24 * 60 * 60 * 1000;

/// Completed markers stay forever: a create spanning an entire prune must
/// still observe that the archive changed, even after the pruner exited.
pub(super) async fn markers(store: &Store, prefix: &str) -> anyhow::Result<BTreeSet<String>> {
    let markers = list_bounded(store, &format!("{}/", key(prefix, "prunes"))).await?;
    for id in markers.keys() {
        safe_id(id)?;
        ensure!(
            optional(store, &key(prefix, &format!("prune-done/{id}")))
                .await?
                .as_deref()
                == Some(id.as_bytes()),
            "destination has active prune markers; retry after prune recovery"
        );
    }
    Ok(markers.into_keys().collect())
}

/// Pins are deliberately permanent, including after an interrupted restore.
/// This handshake happens before reading the run or changing the target.
/// A racing prune either lists this pin or makes the handshake fail.
pub(super) async fn pin(store: &Store, prefix: &str, id: &str) -> anyhow::Result<()> {
    safe_id(id)?;
    let before = markers(store, prefix).await?;
    let path = key(prefix, &format!("restoring/{id}"));
    match optional(store, &path).await? {
        Some(bytes) => ensure!(bytes == id.as_bytes(), "invalid restore pin"),
        None => restore::same_or_create(store, &path, id.as_bytes().to_vec()).await?,
    }
    ensure!(
        before == markers(store, prefix).await?,
        "prune raced restore pin; retry"
    );
    Ok(())
}

pub(super) async fn execute(
    store: &Store,
    prefix: &str,
    keep: usize,
    now: u128,
) -> anyhow::Result<()> {
    let lock = key(prefix, "prune-lock");
    let (version, nonce) = restore::acquire_lock(store, &lock, &json!({"operation":"prune"}))
        .await
        .context(
            "prune locked: stop the old pruner and wait 40 seconds before clearing only prune-lock",
        )?;
    let result: anyhow::Result<()> = async {
        // Publish before listing any runs/pins. An unsuccessful pass also leaves
        // its marker: creates that overlap it must refuse publication.
        json_put(
            store,
            &key(prefix, &format!("prunes/{nonce}")),
            &json!({"started_ms":now}),
            false,
        )
        .await?;
        collect(store, prefix, keep, now).await?;
        Ok(())
    }
    .await;
    // Drain potentially late deletes before allowing another pass or a create.
    #[cfg(not(test))]
    tokio::time::sleep(wasmkeep_server::apps::LATE_WRITES).await;
    // After draining I/O even a refused/failed pass is safe to close. Permanent
    // markers still make every overlapping create fail its final comparison.
    let finish: anyhow::Result<()> = async {
        // A retry recomputes reachability, never replays a stale deletion plan.
        // Complete all prior interrupted markers while still holding the lock.
        for id in list_bounded(store, &format!("{}/", key(prefix, "prunes")))
            .await?
            .keys()
        {
            safe_id(id)?;
            restore::same_or_create(
                store,
                &key(prefix, &format!("prune-done/{id}")),
                id.as_bytes().to_vec(),
            )
            .await?;
        }
        Ok(())
    }
    .await;
    let unlock = store
        .put_opts(
            &Key::parse(lock)?,
            format!("unlocked {nonce}").into_bytes().into(),
            PutOptions {
                mode: PutMode::Update(version),
                ..Default::default()
            },
        )
        .await;
    result?;
    unlock?;
    finish?;
    Ok(())
}

async fn collect(store: &Store, prefix: &str, keep: usize, now: u128) -> anyhow::Result<()> {
    let root = format!("{}/", key(prefix, "runs"));
    let listed = list_bounded(store, &root).await?;
    let mut runs = BTreeSet::new();
    let mut complete = BTreeSet::new();
    let mut protected = BTreeSet::new();
    if let Some(id) = latest(store, prefix).await? {
        protected.insert(id);
    }
    for family in ["before-restore", "restoring"] {
        for meta in list_bounded(store, &format!("{}/", key(prefix, family)))
            .await?
            .values()
        {
            let bytes = read(store, meta.location.as_ref(), META_MAX).await?;
            let id = std::str::from_utf8(&bytes)?.trim();
            safe_id(id)?;
            protected.insert(id.to_owned());
        }
    }
    for (path, meta) in &listed {
        let (id, tail) = path.split_once('/').context("invalid run key")?;
        safe_id(id)?;
        runs.insert(id.to_owned());
        if tail == "complete" {
            complete.insert(id.to_owned());
        }
        if tail == "started" {
            let stamp = u128::try_from(meta.last_modified.timestamp_millis())
                .context("invalid started time")?;
            if now.saturating_sub(stamp) <= DAY_MS {
                ensure!(
                    listed.contains_key(&format!("{id}/complete")),
                    "backup create in progress: {id}"
                );
                protected.insert(id.to_owned());
            }
        }
    }
    protected.extend(complete.iter().rev().take(keep).cloned());
    // A missing protected run may be a failed pin handshake. Its remaining
    // fragments are retained conservatively; it is never made restorable.
    let mut hashes = BTreeSet::new();
    let mut used = 0;
    for id in complete.intersection(&protected) {
        let run = load(store, prefix, id).await?;
        for path in &run.actors {
            let actor = disk_actor(path)?;
            if let Some(objects) = actor["objects"].as_object() {
                for object in objects.values() {
                    retain(&mut hashes, &mut used, object)?;
                }
            } else {
                ensure!(actor["status"] == "failed", "missing actor objects");
            }
        }
        for app in &run.apps {
            for object in array(app, "objects")? {
                retain(&mut hashes, &mut used, object)?;
            }
        }
    }
    // Finish every read/validation before deleting anything. Data listing is
    // bounded too; never partially collect after a truncated listing.
    let data = list_bounded(store, &format!("{}/", key(prefix, "data"))).await?;
    for id in runs.difference(&protected) {
        for tail in ["complete", "verified"] {
            remove(store, &key(&root, &format!("{id}/{tail}"))).await?;
        }
        let start = format!("{id}/");
        for path in listed.keys().filter(|p| {
            p.starts_with(&start)
                && !p.ends_with("/started")
                && !p.ends_with("/complete")
                && !p.ends_with("/verified")
        }) {
            remove(store, &key(&root, path)).await?;
        }
        remove(store, &key(&root, &format!("{id}/started"))).await?;
    }
    for (hash, meta) in data {
        let stamp =
            u128::try_from(meta.last_modified.timestamp_millis()).context("invalid data time")?;
        if !hashes.contains(&hash) && now.saturating_sub(stamp) > DAY_MS {
            remove(store, meta.location.as_ref()).await?;
        }
    }
    Ok(())
}

fn retain(hashes: &mut BTreeSet<String>, used: &mut u64, object: &Value) -> anyhow::Result<()> {
    let hash = text(object, "sha256")?;
    ensure!(
        hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid content hash"
    );
    if hashes.insert(hash.to_owned()) {
        charge(used, 128, INDEX_MAX, "prune references")?;
    }
    Ok(())
}

async fn remove(store: &Store, path: &str) -> anyhow::Result<()> {
    match store.delete(&Key::parse(path)?).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}
