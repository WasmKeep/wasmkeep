//! Bucket-only operator restore staging and outcome waiting (ADR 0033).
use anyhow::{Context, bail, ensure};
use clap::Args;
use wasmkeep_cluster::{
    control::{self, Keys as _},
    replication::SegmentId,
    state_root::StateRoot,
};
use wasmkeep_server::history::{self, Point};

#[derive(Debug, Args)]
pub(crate) struct RestoreArgs {
    /// Resume this operation ID without submitting another request.
    #[arg(long, conflicts_with_all = ["txid", "at", "from", "run", "if_head"])]
    resume: Option<String>,
    /// CLASS/NAME (or default/CLASS/NAME without --app).
    actor: String,
    #[arg(long, value_parser = crate::parse_app)]
    app: Option<(String, String)>,
    #[arg(long)]
    bucket: String,
    #[arg(long, conflicts_with = "at", value_parser = clap::value_parser!(u64).range(1..))]
    txid: Option<u64>,
    #[arg(long, allow_hyphen_values = true)]
    at: Option<String>,
    /// Backup destination instead of live history.
    #[arg(long)]
    from: Option<String>,
    #[arg(long, requires = "from")]
    run: Option<String>,
    /// Refuse if the owner's flushed head changed (GEN:TXID).
    #[arg(long)]
    if_head: Option<String>,
    /// Seconds to wait for an outcome (maximum 3000); requests expire after 50 minutes.
    #[arg(long, default_value_t = 60, conflicts_with = "no_wait", value_parser = clap::value_parser!(u64).range(0..=control::RESTORE_TTL_MS / 1000))]
    wait: u64,
    /// Return immediately; the request remains live for 50 minutes.
    #[arg(long)]
    no_wait: bool,
}

pub(crate) fn run(args: &RestoreArgs) -> anyhow::Result<()> {
    let ((tenant, app), class, name) = crate::move_target(&args.actor, args.app.as_ref())?;
    ensure!(
        args.resume.is_some() || args.txid.is_some() || args.at.is_some() || args.from.is_some(),
        "choose --txid, --at, or --from"
    );
    let point = match (&args.at, args.txid) {
        (Some(at), _) => Point::At(crate::actor_read::parse_at(at)?),
        (_, Some(txid)) => Point::Txid(txid),
        _ => Point::Head,
    };
    let if_head = args
        .if_head
        .as_ref()
        .map(|s| -> anyhow::Result<SegmentId> {
            let (generation, txid) = s.split_once(':').context("--if-head needs GEN:TXID")?;
            Ok(SegmentId {
                generation: generation.parse()?,
                txid: txid.parse()?,
            })
        })
        .transpose()?;
    let (store, prefix) = crate::open_bucket(&args.bucket)?;
    crate::check_layout(&args.bucket, &store, &prefix, &tenant)?;
    if args.resume.is_none() {
        crate::refuse_deleted(&store, &prefix, &tenant, &app)?;
    }
    crate::actor_read::run(execute(
        args, store, prefix, tenant, app, class, name, point, if_head,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    args: &RestoreArgs,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: String,
    tenant: String,
    app: String,
    class: String,
    name: String,
    point: Point,
    if_head: Option<SegmentId>,
) -> anyhow::Result<()> {
    let root = StateRoot::new(&prefix, &tenant, &app);
    let address = wasmkeep_runtime::ActorAddress::new(&tenant, &class, &name)?;
    let keys =
        wasmkeep_cluster::object_store::ObjectStoreBucket::new(store.clone(), prefix.clone());
    if let Some(id) = &args.resume {
        let key = control::checked_restore_key(&root, &class, &name, id)?;
        println!("Operation ID: {id}");
        return wait(args, &keys, &key, id, None).await;
    }
    // Every registered node of the app, offline ones included, through
    // the one test of "announces these words" (ADR 0043, section 2).
    let nodes = wasmkeep_cluster::membership::all_heartbeats(&keys).await?;
    let served = format!("{tenant}/{app}");
    let lacking = wasmkeep_cluster::upgrade::without(
        &["actor-restore", "blobs"],
        nodes.iter().filter(|n| n.app == served),
    );
    if let Some(node) = lacking.first() {
        bail!(
            "node {node} lacks actor-restore or blobs capability (including offline nodes); upgrade it \
             before restoring (a node without blobs would bring back an earlier blob incarnation)"
        );
    }
    ensure!(
        !keys
            .list_keys(&wasmkeep_server::history_folder(&root, &class, &name))
            .await?
            .is_empty(),
        "no such actor"
    );
    let temp = wasmkeep_cli::private_tempdir()?;
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(temp.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: tenant.clone(),
        },
    );
    let path = storage.db_path(&class, &name);
    std::fs::create_dir_all(path.parent().context("scratch path has no parent")?)?;
    let (export, source) = if let Some(from) = &args.from {
        let (export, id) = wasmkeep_cli::backup::export_actor(
            from,
            args.run.as_deref(),
            &tenant,
            &app,
            &class,
            &name,
            point,
            &path,
        )
        .await?;
        (export, format!("backup {id}"))
    } else {
        (
            history::export_raw(&store, &root, &address, point, &path, 2 << 30).await?,
            "live".to_owned(),
        )
    };
    let (bytes, page_size, checksum) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let mut db = storage.open(&class, &name)?;
        db.start_capture()?;
        let all = db.snapshot()?;
        Ok((
            all.to_ltx(1, export.position.txid, export.timestamp_ms)?,
            all.page_size,
            all.post_apply.value(),
        ))
    })
    .await??;
    let id = control::new_request_id();
    let key = control::checked_restore_key(&root, address.class(), address.name(), &id)?;
    let request = wasmkeep_cluster::actor_restore::Request {
        source: format!(
            "{source} {}:{}",
            export.position.generation, export.position.txid
        ),
        size: bytes.len() as u64,
        page_size,
        checksum,
        if_head,
    };
    if bytes.len() >= 8 * 1024 * 1024 {
        let path = object_store::path::Path::from(format!("{prefix}{key}.ltx"));
        let mut upload = store.put_multipart_opts(&path, Default::default()).await?;
        for chunk in bytes.chunks(8 * 1024 * 1024) {
            if let Err(error) = upload.put_part(chunk.to_vec().into()).await {
                let _ = upload.abort().await;
                return Err(error.into());
            }
        }
        if let Err(error) = upload.complete().await {
            let _ = upload.abort().await;
            return Err(error.into());
        }
    } else {
        keys.put_body(&format!("{key}.ltx"), bytes).await?;
    }
    keys.put_body(&key, request.encode()).await?;
    let app_ref = wasmkeep_server::apps::AppRef::new(store.clone(), &prefix, &tenant, &app);
    if let Err(e) = app_ref.check_not_deleted().await {
        let _ = keys.delete(&key).await;
        let _ = keys.delete(&format!("{key}.ltx")).await;
        return Err(e.into());
    }
    // Written again after a conflict, as for a move (`announce_with_retry`).
    if let Err(e) = crate::announce_with_retry(&keys, crate::ANNOUNCE_RETRY).await {
        let _ = keys.delete(&key).await;
        let _ = keys.delete(&format!("{key}.ltx")).await;
        bail!("announcing restore failed: {e}");
    }
    println!("Operation ID: {id}");
    wait(args, &keys, &key, &id, Some(export.position)).await
}

async fn wait(
    args: &RestoreArgs,
    keys: &impl control::Keys,
    key: &str,
    id: &str,
    source: Option<wasmkeep_cluster::replication::SegmentId>,
) -> anyhow::Result<()> {
    let until = std::time::Instant::now()
        + std::time::Duration::from_secs(if args.no_wait { 0 } else { args.wait });
    loop {
        // The request first, then its outcomes. A node publishes the outcome
        // before it deletes the request, so a request seen gone has its
        // outcome visible to the reads below. The other order reads a
        // restore that ended between the two as dropped.
        let pending = keys.get_body(key).await?.is_some();
        for suffix in ["done", "conflict", "refused", "absent", "superseded"] {
            let outcome = format!("{key}.{suffix}");
            let body = keys.get_body(&outcome).await?;
            if let Some(body) = body {
                if suffix != "done" {
                    bail!("restore {suffix}: {}", String::from_utf8_lossy(&body));
                }
                if let Some(source) = source {
                    println!(
                        "Restored to txid {} of generation {}, now {}",
                        source.txid,
                        source.generation,
                        String::from_utf8_lossy(&body)
                    );
                } else {
                    println!("Restore {id} done: now {}", String::from_utf8_lossy(&body));
                }
                return Ok(());
            }
        }
        if !pending {
            bail!(
                "restore {id}: the request is gone and left no outcome: it expired ({} minutes \
                 after its bucket timestamp) or was removed, so nothing says the actor was \
                 restored. Check the actor's data; if it still needs restoring, stage a new \
                 request (--resume cannot revive this one)",
                control::RESTORE_TTL_MS / 60_000
            );
        }
        if std::time::Instant::now() >= until {
            println!(
                "Restore requested ({id}); the cluster retries until {} minutes after the request's bucket timestamp.",
                control::RESTORE_TTL_MS / 60_000
            );
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[tokio::test]
    async fn actor_restore_cli_stages_verified_wal_before_announcing() {
        use object_store::{ObjectStoreExt as _, path::Path};
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let temp = tempfile::tempdir().unwrap();
        let storage = wasmkeep_storage::Storage::new(
            wasmkeep_storage::StorageConfig::new(temp.path()),
            wasmkeep_storage::StateDir {
                dir: "actors".into(),
                scratch: "scratch".into(),
                tenant: "t".into(),
            },
        );
        let mut db = storage.open("c", "n").unwrap();
        db.exec("CREATE TABLE data (n INTEGER)", &[]).unwrap();
        db.exec("INSERT INTO data VALUES (42)", &[]).unwrap();
        db.start_capture().unwrap();
        let bytes = db.snapshot().unwrap().to_ltx(1, 1, 1).unwrap();
        store
            .put(
                &Path::from("state/t/a/actors/c/n/snapshot/0000000000000001-0000000000000001.ltx"),
                bytes.into(),
            )
            .await
            .unwrap();
        let args = RestoreArgs {
            resume: None,
            actor: "c/n".into(),
            app: Some(("t".into(), "a".into())),
            bucket: "s3://test".into(),
            txid: Some(1),
            at: None,
            from: None,
            run: None,
            if_head: None,
            wait: 0,
            no_wait: true,
        };
        let old = Path::from("nodes/old");
        store.put(&old, b"wasmkeep-node 2\nold\nhttp://old\n0.4.2\n0\nt/a\n\n\nserving\n\ntake ends restore\n".to_vec().into()).await.unwrap();
        let refused = execute(
            &args,
            store.clone(),
            String::new(),
            "t".into(),
            "a".into(),
            "c".into(),
            "n".into(),
            Point::Head,
            None,
        )
        .await;
        assert!(refused.is_err(), "old nodes must prevent staging a restore");
        assert!(refused.unwrap_err().to_string().contains("actor-restore"));
        store.delete(&old).await.unwrap();
        execute(
            &args,
            store.clone(),
            String::new(),
            "t".into(),
            "a".into(),
            "c".into(),
            "n".into(),
            Point::Head,
            None,
        )
        .await
        .unwrap();
        let keys =
            wasmkeep_cluster::object_store::ObjectStoreBucket::new(store.clone(), String::new());
        let listed = keys.list_keys("state/t/a/restores/").await.unwrap();
        let (control, _) = control::parse(&listed, 0);
        assert_eq!(control.restores.len(), 1);
        let key = control.restores[0].key();
        let request = wasmkeep_cluster::actor_restore::Request::decode(
            &keys.get_body(&key).await.unwrap().unwrap(),
        )
        .unwrap();
        let bytes = keys.get_body(&format!("{key}.ltx")).await.unwrap().unwrap();
        assert_eq!(request.size, bytes.len() as u64);
        let file = wasmkeep_storage::ltx::decode(&bytes).unwrap();
        assert_eq!(file.trailer.post_apply_checksum, request.checksum);
        assert_eq!(&file.pages[0].1[18..20], &[2, 2]);
        assert!(keys.get_body(control::CHANGED).await.unwrap().is_some());
        assert!(request.source.starts_with("live "));
    }

    #[tokio::test]
    async fn stable_id_actor_restore_timeout_finish_recreate_resume() {
        use object_store::{ObjectStoreExt as _, path::Path};
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let id = "0123456789abcdef";
        let key = "state/t/a/restores/c/6e/0123456789abcdef";
        store
            .put(&Path::from(key), b"request".to_vec().into())
            .await
            .unwrap();
        let args = RestoreArgs {
            resume: Some(id.into()),
            actor: "c/n".into(),
            app: Some(("t".into(), "a".into())),
            bucket: "s3://b".into(),
            txid: None,
            at: None,
            from: None,
            run: None,
            if_head: None,
            wait: 0,
            no_wait: false,
        };
        let run = || {
            execute(
                &args,
                store.clone(),
                String::new(),
                "t".into(),
                "a".into(),
                "c".into(),
                "n".into(),
                Point::Head,
                None,
            )
        };
        // A request whose wait ended before the node publishes its outcome.
        assert!(run().await.is_ok());
        store
            .put(&Path::from(format!("{key}.done")), b"2:7".to_vec().into())
            .await
            .unwrap();
        store.delete(&Path::from(key)).await.unwrap();
        let current = Path::from("state/t/a/actors/c/n/new-incarnation");
        store.put(&current, b"keep".to_vec().into()).await.unwrap();
        for _ in 0..2 {
            run().await.unwrap();
            assert_eq!(
                store.get(&current).await.unwrap().bytes().await.unwrap(),
                "keep"
            );
            assert!(store.get(&Path::from(key)).await.is_err());
            assert!(store.get(&Path::from(format!("{key}.done"))).await.is_ok());
        }
    }

    /// The node's ending (`end_restore_request`: the outcome, then the
    /// request's deletion) landing between the command's outcome reads and
    /// its read of the request, as on a real bucket where each read is a
    /// round trip: a restore that is done must not read as dropped.
    #[tokio::test]
    async fn actor_restore_wait_sees_an_outcome_written_while_it_polls() {
        use wasmkeep_cluster::lease::BucketError;
        struct EndsDuringPoll {
            inner: wasmkeep_cluster::object_store::ObjectStoreBucket,
            request: String,
            ended: std::sync::atomic::AtomicBool,
        }
        impl EndsDuringPoll {
            async fn node_ends(&self) {
                if !self.ended.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    let done = format!("{}.done", self.request);
                    self.inner.put_body(&done, b"2:8".to_vec()).await.unwrap();
                    self.inner.delete(&self.request).await.unwrap();
                }
            }
        }
        impl control::Keys for EndsDuringPoll {
            async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError> {
                self.inner.list_keys(prefix).await
            }
            async fn put_empty(&self, key: &str) -> Result<(), BucketError> {
                self.inner.put_empty(key).await
            }
            async fn put_body(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
                self.inner.put_body(key, body).await
            }
            async fn get_body(&self, key: &str) -> Result<Option<Vec<u8>>, BucketError> {
                // The node ends the request right after the command's
                // last outcome read of a poll.
                let got = self.inner.get_body(key).await;
                if key.ends_with(".superseded") {
                    self.node_ends().await;
                }
                got
            }
            async fn delete(&self, key: &str) -> Result<(), BucketError> {
                self.inner.delete(key).await
            }
        }
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let request = "state/t/a/restores/c/6e/0123456789abcdef".to_owned();
        let keys = EndsDuringPoll {
            inner: wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, String::new()),
            request: request.clone(),
            ended: Default::default(),
        };
        keys.put_body(&request, b"request".to_vec()).await.unwrap();
        let args = RestoreArgs {
            resume: Some("0123456789abcdef".into()),
            actor: "c/n".into(),
            app: Some(("t".into(), "a".into())),
            bucket: "s3://b".into(),
            txid: None,
            at: None,
            from: None,
            run: None,
            if_head: None,
            wait: 5,
            no_wait: false,
        };
        wait(&args, &keys, &request, "0123456789abcdef", None)
            .await
            .expect("a done restore read as dropped");
    }

    /// A request gone with no outcome says why and what to do.
    #[tokio::test]
    async fn actor_restore_wait_explains_a_request_gone_without_outcome() {
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, String::new());
        let args = RestoreArgs {
            resume: Some("0123456789abcdef".into()),
            actor: "c/n".into(),
            app: Some(("t".into(), "a".into())),
            bucket: "s3://b".into(),
            txid: None,
            at: None,
            from: None,
            run: None,
            if_head: None,
            wait: 5,
            no_wait: false,
        };
        let error = wait(
            &args,
            &keys,
            "state/t/a/restores/c/6e/0123456789abcdef",
            "0123456789abcdef",
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("no outcome"), "{error}");
        assert!(error.contains("50 minutes"), "{error}");
        assert!(error.contains("new request"), "{error}");
    }

    #[tokio::test]
    async fn expired_actor_restore_resume_files_nothing() {
        use object_store::{ObjectStoreExt as _, path::Path};
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let args = RestoreArgs {
            resume: Some("0123456789abcdef".into()),
            actor: "c/n".into(),
            app: Some(("t".into(), "a".into())),
            bucket: "s3://b".into(),
            txid: None,
            at: None,
            from: None,
            run: None,
            if_head: None,
            wait: 0,
            no_wait: true,
        };
        assert!(
            execute(
                &args,
                store.clone(),
                String::new(),
                "t".into(),
                "a".into(),
                "c".into(),
                "n".into(),
                Point::Head,
                None
            )
            .await
            .is_err()
        );
        assert!(
            store
                .get(&Path::from("state/t/a/restores/c/6e/0123456789abcdef"))
                .await
                .is_err()
        );
    }

    #[test]
    fn actor_restore_cli_accepts_live_and_backup_requests() {
        for args in [
            vec![
                "wasmkeep",
                "actor",
                "restore",
                "--app",
                "acme/shop",
                "cart/a",
                "--bucket",
                "s3://b",
                "--at",
                "-15m",
                "--if-head",
                "3:4",
                "--no-wait",
            ],
            vec![
                "wasmkeep",
                "actor",
                "restore",
                "--app",
                "acme/shop",
                "cart/a",
                "--bucket",
                "s3://b",
                "--from",
                "s3://backup",
                "--run",
                "run-1",
            ],
        ] {
            assert!(crate::Cli::try_parse_from(args).is_ok());
        }
        assert!(
            crate::Cli::try_parse_from([
                "wasmkeep",
                "actor",
                "restore",
                "default/cart/a",
                "--bucket",
                "s3://b",
                "--txid",
                "1",
                "--at",
                "-15m"
            ])
            .is_err()
        );
    }
}
