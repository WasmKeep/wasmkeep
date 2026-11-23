//! Whole-app restore. The journal is the start fence; its phase and generation
//! floors survive failures. The separate exclusive lock is released by CAS;
//! after a killed CLI the operator waits for pending requests and clears it.
//! Neither wait is fixed: the quiet wait counts from the bucket's stamps of
//! the app's node leases (`restore_fence::quiet_wait_ms`); the lock is held
//! only while a request of this command may still land (`restore_fence::release_at_ms`).
use super::*;
use object_store::ObjectStoreExt;
use std::time::Duration;
use wasmkeep_cluster::node_lease::{OwnerRecord, decode_marker, encode_marker};
use wasmkeep_server::apps;
fn now_ms() -> anyhow::Result<u64> {
    Ok(u64::try_from(super::now_ms()?)?)
}

pub(super) async fn same_or_create(
    store: &Store,
    path: &str,
    bytes: Vec<u8>,
) -> anyhow::Result<()> {
    let path = Key::parse(path)?;
    match store
        .put_opts(
            &path,
            bytes.clone().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            let found = history::get(store, &path, None, MAX + (MAX / 16) + (1 << 20)).await?;
            ensure!(
                found.as_ref() == bytes.as_slice(),
                "restore conflict at {path}: {e}"
            );
            Ok(())
        }
    }
}

// Conditional replacements prevent a timed-out request from landing late
// and regressing the journal phase (or a newer restore's disk marker).
async fn replace_conditional(store: &Store, path: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
    let path = Key::parse(path)?;
    let mode = match store.get(&path).await {
        Ok(current) => PutMode::Update(object_store::UpdateVersion {
            e_tag: current.meta.e_tag,
            version: current.meta.version,
        }),
        Err(object_store::Error::NotFound { .. }) => PutMode::Create,
        Err(e) => return Err(e.into()),
    };
    store
        .put_opts(
            &path,
            bytes.into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

pub(super) async fn acquire_lock(
    store: &Store,
    path: &str,
    binding: &Value,
) -> anyhow::Result<(object_store::UpdateVersion, String)> {
    let path = Key::parse(path)?;
    let mode = match store.get(&path).await {
        Ok(current) => {
            let version = object_store::UpdateVersion {
                e_tag: current.meta.e_tag.clone(),
                version: current.meta.version.clone(),
            };
            let bytes = current.bytes().await?;
            ensure!(
                bytes.starts_with(b"unlocked "),
                "operation already locked at {path}; stop the old CLI and wait 40 seconds before clearing only this lock"
            );
            PutMode::Update(version)
        }
        Err(object_store::Error::NotFound { .. }) => PutMode::Create,
        Err(e) => return Err(e.into()),
    };
    let nonce = wasmkeep_cluster::control::new_request_id();
    let body = encode_bounded(&json!({"holder":nonce,"binding":binding}), META_MAX - 1)?;
    let result = store
        .put_opts(
            &path,
            body.into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await?;
    Ok((
        object_store::UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        },
        nonce,
    ))
}

async fn save_journal(store: &Store, path: &str, journal: &Value) -> anyhow::Result<()> {
    replace_conditional(store, path, encode_bounded(journal, META_MAX - 1)?).await
}

async fn nodes(args: &Restore, store: &Store, prefix: &str) -> anyhow::Result<()> {
    let (tenant, app) = &args.app;
    ensure!(
        apps::live_nodes(Arc::clone(store), prefix, tenant, app)
            .await?
            .is_empty(),
        "live nodes still serve {tenant}/{app}; stop them before restoring"
    );
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
        Arc::clone(store),
        if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        },
    );
    for node in wasmkeep_cluster::membership::all_heartbeats(&bucket).await? {
        ensure!(
            node.app != format!("{tenant}/{app}") || node.can_restore || args.force,
            "node {} version {} ignores restore fences (including offline nodes); upgrade or use --force after stopping it",
            node.id,
            node.version
        );
    }
    ensure!(
        optional(store, &key(prefix, &format!("apps/{tenant}/{app}/deleted")))
            .await?
            .is_none(),
        "app has a tombstone; restore refused"
    );
    Ok(())
}

/// The node lease objects a restore's quiet wait looks at, from one listing
/// of `nodes/` made after the restore fence landed.
#[derive(Debug, PartialEq, Eq)]
struct AppNodes {
    /// The listing proves that no node of the app can act on it unseen
    /// (`restore_fence::quiet_wait_ms`). Fails closed: an object under
    /// `nodes/` that does not decode or names no app, or a node of the app
    /// without a node lease or the `restore` capability, makes it incomplete.
    complete: bool,
    /// The newest `Last-Modified` (the bucket's clock) among the app's node
    /// objects and any object whose app is unknown.
    newest_ms: Option<u64>,
    /// Those objects, each with its version and stamp: a node that writes
    /// its lease again changes them.
    seen: BTreeMap<String, (Option<String>, i64)>,
}

async fn app_nodes(store: &Store, prefix: &str, served: &str) -> anyhow::Result<AppNodes> {
    use wasmkeep_cluster::membership::{NODES_PREFIX, NodeInfo, serves};
    let root = key(prefix, NODES_PREFIX);
    let mut nodes = AppNodes {
        complete: true,
        newest_ms: None,
        seen: BTreeMap::new(),
    };
    for (id, meta) in list_bounded(store, &root).await? {
        let body = read(store, meta.location.as_ref(), META_MAX).await?;
        let node = NodeInfo::decode(&body).filter(|node| node.id == id);
        let relevant = match &node {
            // Its app is unknown: it may be this one.
            None => {
                nodes.complete = false;
                true
            }
            Some(node) if node.app.is_empty() => {
                nodes.complete = false;
                true
            }
            Some(node) if serves(node, served) => {
                nodes.complete &= node.lease.is_some() && node.can_restore;
                true
            }
            Some(_) => false,
        };
        if relevant {
            let stored = meta.last_modified.timestamp_millis();
            let stored_ms = u64::try_from(stored).unwrap_or(0);
            nodes.newest_ms = Some(nodes.newest_ms.map_or(stored_ms, |n| n.max(stored_ms)));
            nodes.seen.insert(id, (meta.e_tag.clone(), stored));
        }
    }
    Ok(nodes)
}

/// The `Last-Modified` of `path`, in ms on the bucket's clock.
async fn stored_ms(store: &Store, path: &str) -> anyhow::Result<u64> {
    let meta = store.head(&Key::parse(path)?).await?;
    Ok(u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0))
}

fn actor_name<'a>(prefix: &'a str, root: &str) -> anyhow::Result<&'a str> {
    let name = prefix
        .strip_prefix(root)
        .context("actor outside selected app")?
        .trim_end_matches('/');
    let parts: Vec<_> = name.split('/').collect();
    ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|p| !p.is_empty() && *p != "." && *p != ".."),
        "invalid actor address"
    );
    ensure!(
        Key::parse(name)?.as_ref() == name,
        "noncanonical actor address"
    );
    Ok(name)
}

// History/owner keys were already escaped once by object_store. Preserve
// their exact bytes on write; decode that layer only to recover an address.
pub(crate) fn object_segment(encoded: &str) -> anyhow::Result<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex =
                std::str::from_utf8(bytes.get(i + 1..i + 3).context("invalid object escape")?)?;
            out.push(u8::from_str_radix(hex, 16)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    let decoded = String::from_utf8(out)?;
    ensure!(
        Key::from(decoded.as_str()).as_ref() == encoded,
        "noncanonical object segment"
    );
    Ok(decoded)
}

async fn alarm_address(
    archive: &Store,
    ap: &str,
    actor: &Value,
    address: &str,
    tenant: &str,
) -> anyhow::Result<(String, String)> {
    let (class, name) = address.split_once('/').context("invalid actor address")?;
    if let (Some(class), Some(name)) = (
        Some(object_segment(class)?),
        wasmkeep_storage::decode_segment(&object_segment(name)?),
    ) {
        return Ok((class, name));
    }
    // Long names are shortened in history keys. The host's address row in
    // the verified database retains the original, including any slashes.
    let tmp = crate::private_tempdir()?;
    let local: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        tmp.path(),
    )?);
    for item in array(actor, "plan")? {
        let tail = item.as_str().context("invalid plan")?;
        put(
            &local,
            tail,
            blob(archive, ap, &actor["objects"][tail]).await?,
            false,
        )
        .await?;
    }
    let db = tmp.path().join("address.db");
    history::replay(&local, "", &db, MAX).await?;
    let original = wasmkeep_storage::readonly::backup_address(&db)?
        .context("hashed actor lacks original address")?;
    let mut parts = original.splitn(3, '/');
    ensure!(
        parts.next() == Some(tenant),
        "alarm address tenant mismatch"
    );
    let original_class = parts.next().context("missing class")?;
    let original_name = parts.next().context("missing name")?;
    ensure!(
        Key::from(original_class).as_ref() == class
            && Key::from(wasmkeep_storage::encode_segment(original_name)).as_ref() == name,
        "alarm address differs from history key"
    );
    Ok((original_class.into(), original_name.into()))
}

async fn verify_undo(archive: &Store, ap: &str, id: &str, force: bool) -> anyhow::Result<()> {
    let run = load(archive, ap, id).await?;
    let missing = array(&run.complete, "failed")?
        .iter()
        .map(|f| text(f, "actor"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if !missing.is_empty() {
        let warning = format!("partial undo: actors not covered: {}", missing.join(", "));
        ensure!(
            force,
            "{warning}; use --force to proceed without their undo coverage"
        );
        eprintln!("WARNING: {warning}; these actors cannot be recovered by undo (--force)");
    }
    verify_mode(archive, ap, id, None, force, false).await
}

async fn verify_target(
    run: &Run,
    source_root: &str,
    target: &Store,
    state: &str,
) -> anyhow::Result<()> {
    let mut expected = BTreeSet::new();
    for path in &run.actors {
        let actor = disk_actor(path)?;
        if actor["status"] == "failed" {
            continue;
        }
        let prefix = text(&actor, "prefix")?;
        if !prefix.starts_with(source_root) {
            continue;
        }
        let name = actor_name(prefix, source_root)?;
        expected.insert(name.to_owned());
        let prefix = format!("{}/", key(state, &format!("actors/{name}")));
        let listing = list_bounded(target, &prefix).await?;
        let plan = plan_restore(&Listing::from_keys(listing.keys().map(String::as_str)))?;
        ensure!(
            plan.head().generation == number(&actor, "generation")?
                && plan.head().txid == number(&actor, "txid")?,
            "restored actor {name} has an unexpected head"
        );
        let tmp = crate::private_tempdir()?;
        let local: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
            tmp.path(),
        )?);
        for tail in history::copy_keys(
            &Listing::from_keys(listing.keys().map(String::as_str)),
            &plan,
        )? {
            let bytes = history::get(
                target,
                &Key::parse(key(&prefix, &tail))?,
                None,
                MAX + (MAX / 16) + (1 << 20),
            )
            .await?;
            put(&local, &tail, bytes.to_vec(), false).await?;
        }
        let result = history::replay(&local, "", &tmp.path().join("verify.db"), MAX).await?;
        ensure!(
            result.position == plan.head() && result.raw_checksum == number(&actor, "checksum")?,
            "restored actor {name} checksum differs"
        );
    }
    let root = format!("{}/", key(state, "actors"));
    let mut groups = actor_groups(target, &root);
    while let Some((prefix, _)) = groups.try_next().await? {
        ensure!(
            expected.contains(actor_name(&prefix, &root)?),
            "unexpected restored actor: {prefix}"
        );
    }
    Ok(())
}

pub(super) async fn execute(
    args: &Restore,
    archive: &Store,
    ap: &str,
    target: &Store,
    tp: &str,
    late: Duration,
) -> anyhow::Result<()> {
    execute_settled(
        args,
        archive,
        ap,
        target,
        tp,
        late,
        wasmkeep_server::settle::Settle::new(),
    )
    .await
}

/// [`execute`], recording this command's writes in `settle`, which the
/// caller may also feed from the target's HTTP client
/// (`wasmkeep_server::settle::observe`): every attempt, retries included.
pub(super) async fn execute_settled(
    args: &Restore,
    archive: &Store,
    ap: &str,
    target: &Store,
    tp: &str,
    late: Duration,
    settle: Arc<wasmkeep_server::settle::Settle>,
) -> anyhow::Result<()> {
    let target = &wasmkeep_server::settle::tracked(Arc::clone(target), Arc::clone(&settle));
    let archive = &wasmkeep_server::settle::tracked(Arc::clone(archive), Arc::clone(&settle));
    let late_ms = u64::try_from(late.as_millis())?;
    let name = format!("{}/{}", args.app.0, args.app.1);
    ensure!(
        name != "default/default",
        "implicit app does not follow restore fences"
    );
    if let Some(id) = &args.resume {
        let root = wasmkeep_cluster::state_root::StateRoot::new("", &args.app.0, &args.app.1);
        wasmkeep_cluster::control::checked_restore_key(&root, "app", "app", id)?;
        println!("Operation ID: {id}");
    }
    let base = key(tp, &format!("apps/{name}"));
    let fence = key(&base, "restoring");
    // Pin the selected run across retries even when the undo backup moves latest.
    let existing = optional(target, &fence).await?;
    let mut journal: Value = match existing.as_deref() {
        Some(bytes) => serde_json::from_slice(bytes).context("invalid restore journal")?,
        None => Value::Null,
    };
    if let Some(id) = &args.resume {
        if existing.is_none()
            && optional(target, &key(&base, "restored")).await?.as_deref() == Some(id.as_bytes())
        {
            println!("Restore {id} done: {name}");
            return Ok(());
        }
        ensure!(
            journal["nonce"].as_str() == Some(id),
            "operation {id} is unknown or no longer active; nothing changed"
        );
    }
    if let Some(id) = journal["nonce"].as_str() {
        println!("Operation ID: {id}");
    }
    if args.abort {
        // Trust the persisted mode, never the invocation's --in-place flag.
        let empty_root = journal["binding"]["in_place"] == false;
        ensure!(
            existing.is_some()
                && (journal["phase"] == "prepare"
                    || (empty_root
                        && ["clear", "copy", "abort"]
                            .iter()
                            .any(|phase| journal["phase"] == *phase))),
            "--abort for in-place restores is only safe in prepare; clear/copy must restore the pinned before_restore run with --in-place --deploy backup"
        );
        nodes(args, target, tp).await?;
        let lock = key(&base, "restore-lock");
        let (version, nonce) = acquire_lock(target, &lock, &journal["binding"]).await?;
        let result = async {
            ensure!(
                optional(target, &fence).await? == existing,
                "restore changed while acquiring lock"
            );
            tokio::time::sleep(late).await;
            nodes(args, target, tp).await?;
            if empty_root {
                // Persist cancellation before deleting anything: a failed cleanup
                // must be retried with --abort, never resumed as a partial copy.
                journal["phase"] = json!("abort");
                save_journal(target, &fence, &journal).await?;
                let state = format!("{}/", key(tp, &format!("state/{name}")));
                for _ in 0..10 {
                    let keys = list_bounded(target, &state).await?;
                    if keys.is_empty() {
                        break;
                    }
                    for meta in keys.values() {
                        target.delete(&meta.location).await?;
                    }
                }
                ensure!(
                    list_bounded(target, &state).await?.is_empty(),
                    "state keeps appearing; fence retained"
                );
                if journal["binding"]["deploy"] == "Backup" {
                    for (tail, meta) in list_bounded(target, &format!("{base}/")).await? {
                        if ![
                            "restoring",
                            "restored",
                            "restore-lock",
                            "recreated",
                            "deleted",
                        ]
                        .contains(&tail.as_str())
                        {
                            target.delete(&meta.location).await?;
                        }
                    }
                }
                nodes(args, target, tp).await?;
            }
            target.delete(&Key::from(fence.as_str())).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        tokio::time::sleep(late).await;
        let unlocked = target
            .put_opts(
                &Key::from(lock),
                format!("unlocked {nonce}").into_bytes().into(),
                PutOptions {
                    mode: PutMode::Update(version),
                    ..Default::default()
                },
            )
            .await;
        result?;
        unlocked?;
        println!(
            "Aborted restore for {name}; {}",
            if empty_root {
                "app state root emptied"
            } else {
                "original bucket state retained"
            }
        );
        return Ok(());
    }
    ensure!(
        journal["phase"] != "abort",
        "cleanup in progress; rerun --abort"
    );
    let id = match (&args.run, journal.get("run").and_then(Value::as_str)) {
        (Some(id), _) => id.clone(),
        (None, Some(id)) => id.to_owned(),
        (None, None) => latest(archive, ap).await?.context("no latest run")?,
    };
    prune::pin(archive, ap, &id).await?;
    // Full verification is mandatory even on resume, before any target write.
    let root_key = key(ap, &format!("runs/{id}/root"));
    let root = read(archive, &root_key, META_MAX).await?;
    let partial_undo = journal["before_restore"].as_str() == Some(id.as_str());
    if partial_undo {
        verify_undo(archive, ap, &id, args.force).await?;
    } else {
        verify_mode(archive, ap, &id, None, false, false).await?;
    }
    let run = load(archive, ap, &id).await?;
    ensure!(
        read(archive, &root_key, META_MAX).await? == root,
        "run changed during verification"
    );
    let app = run
        .apps
        .iter()
        .find(|a| a["app"] == name)
        .context("app absent from run")?;
    let (_, sp) = crate::parse_bucket(text(&run.complete, "source")?)?;
    let source_root = format!("{}/", key(sp, &format!("state/{name}/actors")));
    let state = format!("{}/", key(tp, &format!("state/{name}")));
    let choice = args.deploy.unwrap_or(if args.in_place {
        DeployChoice::Keep
    } else {
        DeployChoice::Backup
    });
    if choice == DeployChoice::Keep {
        ensure!(
            optional(target, &key(&base, "current")).await?.is_some(),
            "--deploy keep needs an existing deploy"
        );
    }
    // A verified app record may contain extra content-addressed objects.
    // Only deploy payloads and secrets are allowed to become target metadata;
    // in particular, a trailing slash must not normalize into a fence key.
    let source_app = format!("{}/", key(sp, &format!("apps/{name}")));
    if app.get("files").is_none() {
        let imports = imports_blobs(archive, ap, app, &source_app, target, &base, choice).await?;
        if let Some(warning) = missing_files_warning(imports, app) {
            eprintln!("{warning}");
        }
    }
    for object in array(app, "objects")? {
        let tail = text(object, "key")?
            .strip_prefix(&source_app)
            .context("app object outside app")?;
        ensure!(
            Key::parse(tail)?.as_ref() == tail,
            "noncanonical app object"
        );
        let parts: Vec<_> = tail.split('/').collect();
        ensure!(
            tail == "current"
                || (parts.len() == 3
                    && parts[0] == "_assets"
                    && (parts[1] == "index" || parts[1] == "content")
                    && wasmkeep_server::assets::valid_digest(parts[2]))
                || (parts.len() == 2
                    && (parts[0] == "secrets"
                        || parts[1] == "manifest"
                        || parts[1].ends_with(".wasm")
                        || parts[1].ends_with(".config"))),
            "unexpected app object: {tail}"
        );
    }
    let mut alarm_addresses = BTreeMap::new();
    let mut address_bytes = 0;
    for path in &run.actors {
        let actor = disk_actor(path)?;
        if actor["status"] == "failed" {
            continue;
        }
        let prefix = text(&actor, "prefix")?;
        if prefix.starts_with(&source_root) {
            let address = actor_name(prefix, &source_root)?;
            if !actor["alarm"].is_null() {
                let original = alarm_address(archive, ap, &actor, address, &args.app.0).await?;
                charge(
                    &mut address_bytes,
                    (address.len() + original.0.len() + original.1.len() + 256) as u64,
                    INDEX_MAX,
                    "restore alarm addresses",
                )?;
                alarm_addresses.insert(address.to_owned(), original);
                u64::try_from(actor["alarm"].as_i64().context("invalid alarm")?)
                    .context("negative alarm")?;
            }
        }
    }
    let undoing = existing.is_some()
        && journal["before_restore"].as_str() == Some(id.as_str())
        && journal["run"] != id;
    let binding = json!({"run":id,"root":digest(&root),"from":args.from,"bucket":args.bucket,
        "app":name,"in_place":args.in_place,"deploy":format!("{choice:?}")});
    nodes(args, target, tp).await?;
    if existing.is_some() {
        ensure!(
            journal["binding"] == binding
                || (undoing
                    && args.in_place
                    && choice == DeployChoice::Backup
                    && ["from", "bucket", "app"]
                        .iter()
                        .all(|k| journal["binding"][k] == binding[k])),
            "another restore is fenced here; resume its exact options"
        );
    } else {
        ensure!(
            args.in_place || list_bounded(target, &state).await?.is_empty(),
            "non-empty app root requires --in-place"
        );
    }
    let lock = key(&base, "restore-lock");
    let (lock_version, lock_nonce) = acquire_lock(target, &lock, &binding).await?;
    let result = async {
        nodes(args, target, tp).await?;
        ensure!(optional(target, &fence).await? == existing, "restore changed while acquiring lock; retry");
        if existing.is_none() {
            ensure!(args.in_place || list_bounded(target, &state).await?.is_empty(),
                "non-empty app root requires --in-place");
        }
        if undoing {
            ensure!(journal["phase"] == "clear" || journal["phase"] == "copy", "use --abort to cancel prepare");
            journal["binding"] = binding.clone();
            journal["run"] = json!(id);
            journal["phase"] = json!("clear");
            journal["undoing"] = json!(true);
            save_journal(target, &fence, &journal).await?;
        }
        if existing.is_none() {
            // The epoch before this operation's `restored`: the undo run
            // copies the files there.
            journal = json!({"run":id,"binding":binding,"phase":"prepare",
                "nonce":wasmkeep_cluster::control::new_request_id(),"started_ms":now_ms()?,
                "files_epoch":files::epoch_of(target, &base).await?});
            json_put(target, &fence, &journal, false).await?;
        }
        println!("Operation ID: {}", text(&journal, "nonce")?);
        // A distinct marker for every operation, stable across that operation's
        // retries. It is written before any state is removed or copied.
        replace_conditional(target, &key(&base, "restored"), text(&journal, "nonce")?.as_bytes().to_vec()).await?;
        nodes(args, target, tp).await?;
        // The quiet wait (ADR 0033): until no write an old holder of the app
        // sent can still land. Every time is a bucket stamp, never a node's
        // clock against this one's. Expired live leases are fenced first, so
        // the listing after it carries the fence's stamp.
        apps::fence_app_nodes(target, tp, &args.app.0, &args.app.1, &mut |_| {}).await?;
        // The bucket's clock: `restored` was written just above, so its stamp
        // plus the time since this read is at most the bucket's now.
        let probe = stored_ms(target, &key(&base, "restored")).await?;
        let probed = tokio::time::Instant::now();
        let fence_stored = stored_ms(target, &fence).await?;
        let mut listed = app_nodes(target, tp, &name).await?;
        let mut settled = false;
        for _ in 0..wasmkeep_cluster::restore_fence::QUIET_ROUNDS {
            let bucket_now = probe.saturating_add(u64::try_from(probed.elapsed().as_millis())?);
            let wait = wasmkeep_cluster::restore_fence::quiet_wait_ms(
                listed.complete && !args.force, listed.newest_ms, fence_stored, bucket_now, late_ms);
            if wait > 0 {
                println!("Waiting {} s for writes of the app's earlier nodes that may still land.", wait.div_ceil(1000));
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
            // A node that wrote its lease since is still acting: wait again
            // from that write.
            let again = app_nodes(target, tp, &name).await?;
            if again.seen == listed.seen {
                settled = true;
                break;
            }
            listed = again;
        }
        ensure!(settled, "a node of {name} still writes its node lease ({}); stop it before restoring",
            listed.seen.keys().cloned().collect::<Vec<_>>().join(", "));
        nodes(args, target, tp).await?;

        if journal["phase"] == "prepare" {
            if args.in_place {
                if journal["before_restore"].is_null() {
                    let create_args = Create { bucket: args.bucket.clone(), to: args.from.clone(),
                        app: Some(args.app.clone()), all: false, history: History::Head,
                        require_continuous: Some(true), cache: None, to_env: None };
                    let fence_bytes = read(target, &fence, META_MAX).await?;
                    let undo = create_undo(&create_args, target, tp, archive, ap, Some(&fence_bytes)).await?;
                    verify_undo(archive, ap, &undo, args.force).await?;
                    journal["before_restore"] = json!(undo);
                    // Persist before publishing the undo label: a retry must
                    // never replace the label with a different run.
                    save_journal(target, &fence, &journal).await?;
                }
                let undo = text(&journal, "before_restore")?;
                verify_undo(archive, ap, undo, args.force).await?;
                same_or_create(archive, &key(ap, &format!("before-restore/{}", text(&journal, "nonce")?)),
                    undo.as_bytes().to_vec()).await?;
            }
            let mut seeds = BTreeMap::<String, u64>::new();
            if args.in_place {
                for (rel, _) in list_bounded(target, &state).await? {
                    if let Some(tail) = rel.strip_prefix("actors/") {
                        let mut parts = tail.splitn(3, '/');
                        let class = parts.next().context("invalid history class")?;
                        let actor = parts.next().context("invalid history actor")?;
                        let history = parts.next().context("invalid history object")?;
                        let listing = Listing::from_keys([history]);
                        let generation = listing.snapshots.iter().map(|s| s.generation)
                            .chain(listing.segments.iter().map(|s| s.id.generation)).max()
                            .context("unrecognized history key; cannot preserve its generation")?;
                        seeds.entry(format!("{class}/{actor}")).and_modify(|v| *v = (*v).max(generation)).or_insert(generation);
                    } else if let Some(actor) = rel.strip_prefix("owners/").or_else(|| rel.strip_prefix("leases/")) {
                        actor_name(actor, "")?;
                        let body = history::get(target, &Key::parse(key(&state, &rel))?, None, META_MAX).await?;
                        let generation = if rel.starts_with("owners/") {
                            OwnerRecord::decode(&body).map(|r| r.generation)
                        } else {
                            decode_marker(&body).or_else(|| wasmkeep_cluster::lease::LeaseRecord::decode(&body).map(|r| r.generation))
                        }.context("invalid ownership/lease record")?;
                        seeds.entry(actor.into()).and_modify(|v| *v = (*v).max(generation)).or_insert(generation);
                    }
                }
            }
            for path in &run.actors {
                let actor = disk_actor(path)?;
        if actor["status"] == "failed" { continue; }
                let prefix = text(&actor, "prefix")?;
                if !prefix.starts_with(&source_root) { continue; }
                let name = actor_name(prefix, &source_root)?;
                let plan = array(&actor, "plan")?;
                let listing = Listing::from_keys(plan.iter().filter_map(Value::as_str));
                let generation = listing.snapshots.iter().map(|s| s.generation)
                    .chain(listing.segments.iter().map(|s| s.id.generation)).max().context("empty plan")?;
                seeds.entry(name.into()).and_modify(|v| *v = (*v).max(generation)).or_insert(generation);
            }
            if args.in_place {
                for value in seeds.values_mut() { *value = value.checked_add(1).context("generation exhausted")?; }
            }
            journal["seeds"] = json!(seeds);
            journal["phase"] = json!(if args.in_place { "clear" } else { "copy" });
            save_journal(target, &fence, &journal).await?;
        }
        if journal["phase"] == "clear" {
            nodes(args, target, tp).await?;
            if journal["undoing"] == true {
                for (tail, meta) in list_bounded(target, &format!("{base}/")).await? {
                    if !["restoring", "restored", "restore-lock", "recreated", "deleted"].contains(&tail.as_str()) {
                        target.delete(&meta.location).await?;
                    }
                }
            }
            for _ in 0..10 {
                let keys = list_bounded(target, &state).await?;
                if keys.is_empty() { break; }
                for meta in keys.values() { target.delete(&meta.location).await?; }
            }
            ensure!(list_bounded(target, &state).await?.is_empty(), "state keeps appearing; fence retained");
            journal["phase"] = json!("copy");
            save_journal(target, &fence, &journal).await?;
        }
        ensure!(journal["phase"] == "copy", "unknown restore phase");
        nodes(args, target, tp).await?;
        // History first. Only the head plan (including its boundary segment),
        // never the rest of an --history all archive.
        for path in &run.actors {
            let actor = disk_actor(path)?;
        if actor["status"] == "failed" { continue; }
            let prefix = text(&actor, "prefix")?;
            if !prefix.starts_with(&source_root) { continue; }
            let name = actor_name(prefix, &source_root)?;
            for item in array(&actor, "plan")? {
                let tail = item.as_str().context("invalid plan")?;
                let bytes = blob(archive, ap, &actor["objects"][tail]).await?;
                same_or_create(target, &key(&state, &format!("actors/{name}/{tail}")), bytes).await?;
            }
        }
        // Then all generation floors, including actors absent from the run.
        for (name, seed) in journal["seeds"].as_object().context("missing seeds")? {
            same_or_create(target, &key(&state, &format!("leases/{name}")),
                encode_marker(seed.as_u64().context("invalid seed")?)).await?;
        }
        // Then the alarm index derived from the verified committed databases.
        for path in &run.actors {
            let actor = disk_actor(path)?;
        if actor["status"] == "failed" { continue; }
            let prefix = text(&actor, "prefix")?;
            if !prefix.starts_with(&source_root) { continue; }
            let name = actor_name(prefix, &source_root)?;
            let Some(at) = actor["alarm"].as_i64() else { continue; };
            let at = u64::try_from(at).context("negative alarm")?;
            let indexed = actor["alarm_indexed"].as_str().and_then(wasmkeep_cluster::alarm_index::parse_meta);
            let (class, decoded) = alarm_addresses.get(name).context("missing alarm address")?;
            let mut entries = Vec::new();
            if let Some(entry) = indexed.clone() { entries.push(entry); }
            if indexed.as_ref().is_none_or(|entry| entry.at != at) {
                entries.push(wasmkeep_cluster::alarm_index::Entry::owned(at, 0, text(&journal, "nonce")?)
                    .context("invalid alarm nonce")?);
            }
            for entry in entries {
                let tail = wasmkeep_cluster::alarm_index::key_tail(class, decoded, &entry);
                same_or_create(target, &key(&state, &format!("alarms/{tail}")), Vec::new()).await?;
            }
        }
        // Then the files of the incarnation each restored database names,
        // and the front's, under the epoch `restored` just gave the app.
        let mut namespaces = BTreeSet::new();
        for path in &run.actors {
            let actor = disk_actor(path)?;
            if actor["status"] == "failed" || !text(&actor, "prefix")?.starts_with(&source_root) { continue; }
            if let Some(ns) = actor["blob_namespace"].as_str() { namespaces.insert(ns.to_owned()); }
        }
        let files = files::restore_app(archive, ap, app, namespaces, target, &base, &state).await?;
        if files > 0 { println!("Restored {files} files of {name}; each written after its actor's row, so a file may be newer than the rows that name it."); }
        nodes(args, target, tp).await?;
        if choice == DeployChoice::Backup {
            let source_app = format!("{}/", key(sp, &format!("apps/{name}")));
            // Versions and sealed payloads before either current pointer.
            let objects = array(app, "objects")?;
            for current in [false, true] {
                for object in objects {
                    let tail = text(object, "key")?.strip_prefix(&source_app).context("app object outside app")?;
                    ensure!(tail != "deleted" && tail != "restoring" && tail != "restored"
                        && tail != "restore-lock" && tail != "recreated", "invalid app metadata");
                    if (tail == "current" || tail == "secrets/current") != current { continue; }
                    let bytes = blob(archive, ap, object).await?;
                    if current { put(target, &key(&base, tail), bytes, true).await?; }
                    else { same_or_create(target, &key(&base, tail), bytes).await?; }
                }
            }
            if !objects.iter().any(|object| object["key"] == key(&source_app, "secrets/current")) {
                let current = key(&base, "secrets/current");
                if optional(target, &current).await?.is_some() {
                    target.delete(&Key::from(current)).await?;
                }
            }
            if sp != tp {
                println!("Sealed secrets are bound to their prefix: run wasmkeep secrets reset for {name}.");
            }
        }
        nodes(args, target, tp).await?;
        verify_target(&run, &source_root, target, &state).await?;
        // Last: any failure above leaves the fence and journal for a retry.
        target.delete(&Key::from(fence.as_str())).await?;
        println!("Restored {name} from {id}; each actor at its own moment. Due alarms may fire again; no ownership or intents restored.");
        println!("Start: wasmkeep serve --app {name} --bucket {}", args.bucket);
        Ok(())
    }.await;
    // A timed-out write/delete of this command may still land. Keep the
    // exclusive lock across its deadline, including after removing the start
    // fence, so a late fence deletion cannot open the next restore. A command
    // whose every write was answered has nothing left in flight.
    let now = settle.now_ms();
    let release =
        wasmkeep_cluster::restore_fence::release_at_ms(now, settle.unsettled_ms(), late_ms);
    if release > now {
        eprintln!(
            "A write of this restore got no answer: holding the restore lock {} s in case it lands late.",
            (release - now).div_ceil(1000)
        );
        tokio::time::sleep(Duration::from_millis(release - now)).await;
    }
    let unlock = target
        .put_opts(
            &Key::parse(lock)?,
            format!("unlocked {lock_nonce}").into_bytes().into(),
            PutOptions {
                mode: PutMode::Update(lock_version),
                ..Default::default()
            },
        )
        .await;
    result?;
    unlock?;
    Ok(())
}

pub(super) fn missing_files_warning(imports_blobs: bool, app: &Value) -> Option<&'static str> {
    (imports_blobs && app.get("files").is_none()).then_some(
        "warning: app version imports blobs but this backup run has no file manifest; files cannot be restored",
    )
}

pub(super) async fn imports_blobs(
    archive: &Store,
    ap: &str,
    app: &Value,
    source_app: &str,
    target: &Store,
    base: &str,
    choice: DeployChoice,
) -> anyhow::Result<bool> {
    use wasmkeep_server::apps::{Current, Manifest, StorageImports};
    let objects: BTreeMap<_, _> = array(app, "objects")?
        .iter()
        .map(|o| Ok((text(o, "key")?, o)))
        .collect::<anyhow::Result<_>>()?;
    let get = async |tail: &str| -> anyhow::Result<Vec<u8>> {
        if choice == DeployChoice::Keep {
            read(target, &key(base, tail), MAX).await
        } else {
            blob(
                archive,
                ap,
                objects
                    .get(key(source_app, tail).as_str())
                    .context("deploy object missing")?,
            )
            .await
        }
    };
    let current = Current::parse(&get("current").await?).context("invalid current")?;
    let manifest = Manifest::parse(std::str::from_utf8(
        &get(&format!("{}/manifest", current.version)).await?,
    )?)
    .context("invalid manifest")?;
    let runtime = wasmkeep_runtime::Runtime::new(&wasmkeep_runtime::RuntimeConfig::default())?;
    for class in manifest
        .classes
        .keys()
        .map(String::as_str)
        .chain(manifest.front.as_ref().map(|_| "_front"))
    {
        let bytes = get(&format!("{}/{class}.wasm", current.version)).await?;
        let component = wasmtime::component::Component::new(runtime.engine(), &bytes)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if StorageImports::of(&runtime, &component).blobs {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_late_unlock_cannot_release_the_next_holder() {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        let binding = json!({"run":"same-run"});
        let (old_version, old_nonce) = acquire_lock(&store, "lock", &binding).await.unwrap();
        let unlock = format!("unlocked {old_nonce}").into_bytes();
        store
            .put_opts(
                &Key::from("lock"),
                unlock.clone().into(),
                PutOptions {
                    mode: PutMode::Update(old_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (_, new_nonce) = acquire_lock(&store, "lock", &binding).await.unwrap();
        assert_ne!(new_nonce, old_nonce);
        assert!(
            store
                .put_opts(
                    &Key::from("lock"),
                    unlock.into(),
                    PutOptions {
                        mode: PutMode::Update(old_version),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert!(acquire_lock(&store, "lock", &binding).await.is_err());
    }

    #[tokio::test]
    async fn restore_late_journal_write_cannot_regress_phase() {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        json_put(&store, "restoring", &json!({"phase":"prepare"}), false)
            .await
            .unwrap();
        let current = store.get(&Key::from("restoring")).await.unwrap();
        let pending = object_store::UpdateVersion {
            e_tag: current.meta.e_tag,
            version: current.meta.version,
        };
        save_journal(&store, "restoring", &json!({"phase":"copy"}))
            .await
            .unwrap();
        assert!(
            store
                .put_opts(
                    &Key::from("restoring"),
                    b"clear".to_vec().into(),
                    PutOptions {
                        mode: PutMode::Update(pending),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&read(&store, "restoring", META_MAX).await.unwrap())
                .unwrap()["phase"],
            "copy"
        );
    }
}
