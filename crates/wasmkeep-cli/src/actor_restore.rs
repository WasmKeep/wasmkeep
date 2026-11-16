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
    #[arg(long, default_value_t = 60, conflicts_with = "no_wait", value_parser = clap::value_parser!(u64).range(0..=3000))]
    wait: u64,
    #[arg(long)]
    no_wait: bool,
}

pub(crate) fn run(args: &RestoreArgs) -> anyhow::Result<()> {
    let ((tenant, app), class, name) = crate::move_target(&args.actor, args.app.as_ref())?;
    ensure!(
        args.txid.is_some() || args.at.is_some() || args.from.is_some(),
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
    crate::refuse_deleted(&store, &prefix, &tenant, &app)?;
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
    for node in wasmkeep_cluster::membership::all_heartbeats(&keys).await? {
        ensure!(
            node.app != format!("{tenant}/{app}") || node.can_actor_restore,
            "node {} lacks actor-restore capability (including offline nodes); upgrade it before restoring",
            node.id
        );
    }
    ensure!(
        !keys
            .list_keys(&wasmkeep_server::history_folder(&root, &class, &name))
            .await?
            .is_empty(),
        "no such actor"
    );
    let temp = tempfile::tempdir()?;
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
    if let Err(e) = control::announce(&keys, &id).await {
        let _ = keys.delete(&key).await;
        let _ = keys.delete(&format!("{key}.ltx")).await;
        bail!("announcing restore failed: {e}");
    }
    let until = std::time::Instant::now()
        + std::time::Duration::from_secs(if args.no_wait { 0 } else { args.wait });
    loop {
        for suffix in ["done", "conflict", "refused", "absent", "superseded"] {
            let outcome = format!("{key}.{suffix}");
            if let Some(body) = keys.get_body(&outcome).await? {
                let _ = keys.delete(&outcome).await;
                if suffix != "done" {
                    bail!("restore {suffix}: {}", String::from_utf8_lossy(&body));
                }
                println!(
                    "Restored to txid {} of generation {}, now {}",
                    export.position.txid,
                    export.position.generation,
                    String::from_utf8_lossy(&body)
                );
                return Ok(());
            }
        }
        if keys.get_body(&key).await?.is_none() {
            bail!("restore request dropped");
        }
        if std::time::Instant::now() >= until {
            println!(
                "Restore requested ({id}); the cluster retries until one hour after the request's bucket timestamp."
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
