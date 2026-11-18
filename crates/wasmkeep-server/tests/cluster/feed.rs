use super::*;
use futures_util::TryStreamExt;

use object_store::{ObjectStoreExt, path::Path as StorePath};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    List,
    Get,
    Put,
    Delete,
}

/// One request: what, the key or prefix.
#[derive(Debug, Clone)]
struct Logged {
    op: Op,
    key: String,
}

/// A bucket that logs every request a node sends it.
#[derive(Debug)]
struct Counted {
    inner: Arc<dyn ObjectStore>,
    log: Arc<std::sync::Mutex<Vec<Logged>>>,
}

impl Counted {
    fn over(
        inner: &Arc<dyn ObjectStore>,
    ) -> (Arc<dyn ObjectStore>, Arc<std::sync::Mutex<Vec<Logged>>>) {
        let log: Arc<std::sync::Mutex<Vec<Logged>>> = Arc::default();
        let store: Arc<dyn ObjectStore> = Arc::new(Self {
            inner: Arc::clone(inner),
            log: Arc::clone(&log),
        });
        (store, log)
    }

    fn note(&self, op: Op, key: &str) {
        self.log.lock().unwrap().push(Logged {
            op,
            key: key.to_owned(),
        });
    }
}

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counted({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.note(Op::Put, location.as_ref());
        {
            let _ = (location, payload, opts);
            panic!("feed PUT")
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.note(Op::Put, location.as_ref());
        {
            let _ = (location, opts);
            panic!("feed multipart")
        }
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.note(Op::Get, location.as_ref());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.note(Op::Delete, "delete_stream");
        let _ = locations;
        panic!("feed DELETE")
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.note(Op::Put, to.as_ref());
        {
            let _ = (from, to, options);
            panic!("feed copy")
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_one_poll_projection_cost_and_read_only() {
    feed_one_poll_projection_cost_and_read_only_on(Arc::new(InMemory::new())).await;
}

async fn feed_one_poll_projection_cost_and_read_only_on(store: Arc<dyn ObjectStore>) {
    use clap::Parser;
    use wasmkeep_cli::feed::{Args, poll};

    let dir = tempfile::tempdir().unwrap();
    let node = node_with(
        "feed",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    for (name, count) in [("a", 2), ("b", 3)] {
        for n in 1..=count {
            assert_eq!(
                send(node.addr, "POST", &format!("/actor/t/{name}/durable"), "")
                    .await
                    .body
                    .trim(),
                n.to_string()
            );
        }
    }
    let output = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: Args,
    }
    let args = Cli::parse_from([
        "feed",
        "default/default",
        "--bucket",
        "s3://local",
        "--to",
        "parquet",
        "--out",
        output.path().to_str().unwrap(),
        "--state",
        state.path().to_str().unwrap(),
    ])
    .args;
    let (reader, log) = Counted::over(&store);
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    let first = log.lock().unwrap().clone();
    assert_eq!(first.iter().filter(|e| e.op == Op::List).count(), 1);
    assert!(
        first
            .iter()
            .filter(|e| e.op == Op::Get)
            .all(|e| e.key.starts_with("state/default/default/actors/")
                || e.key.starts_with("apps/default/default/"))
    );
    let keys = wasmkeep_server::history::list_objects(&store, "state/default/default/actors/t/a/")
        .await
        .unwrap();
    let listing =
        wasmkeep_cluster::replication::Listing::from_keys(keys.keys().map(String::as_str));
    let plan_a = wasmkeep_cluster::replication::plan_restore(&listing).unwrap();
    let keys = wasmkeep_server::history::list_objects(&store, "state/default/default/actors/t/b/")
        .await
        .unwrap();
    let listing =
        wasmkeep_cluster::replication::Listing::from_keys(keys.keys().map(String::as_str));
    let plan_b = wasmkeep_cluster::replication::plan_restore(&listing).unwrap();
    assert_eq!(
        first.iter().filter(|e| e.op == Op::Get).count(),
        4 + plan_a.segments.len()
            + usize::from(plan_a.snapshot.is_some())
            + plan_b.segments.len()
            + usize::from(plan_b.snapshot.is_some())
    );
    let rows = read_projection(output.path(), "t", "config").unwrap();
    assert_eq!(rows, vec![("a".into(), 2), ("b".into(), 3)]);
    log.lock().unwrap().clear();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    let second = log.lock().unwrap().clone();
    assert_eq!(second.iter().filter(|e| e.op == Op::List).count(), 1);
    assert_eq!(second.iter().filter(|e| e.op == Op::Get).count(), 4);
    assert_eq!(
        std::fs::read_dir(output.path().join("_wk_states"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_dir(state.path().join("scratch"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "3"
    );
    log.lock().unwrap().clear();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        read_projection(output.path(), "t", "config").unwrap(),
        vec![("a".into(), 3), ("b".into(), 3)]
    );
    let third = log.lock().unwrap().clone();
    assert_eq!(third.iter().filter(|e| e.op == Op::List).count(), 1);
    assert!(
        third
            .iter()
            .filter(|e| e.op == Op::Get)
            .all(|e| !e.key.contains("/actors/t/b/"))
    );
    assert_eq!(
        std::fs::read_dir(output.path().join("_wk_states"))
            .unwrap()
            .count(),
        2
    );
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(state.path().join("lock"))
        .unwrap();
    lock.lock().unwrap();
    log.lock().unwrap().clear();
    assert!(
        poll(&args, &reader, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("locked")
    );
    assert!(log.lock().unwrap().is_empty());
    drop(lock);
    // A second class is written through its own real node in the same app.
    let dir2 = tempfile::tempdir().unwrap();
    let node2 = node_with(
        "feed-other",
        dir2.path(),
        &store,
        Duration::from_secs(30),
        "u",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(
        send(node2.addr, "POST", "/actor/u/c/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    let selected_out = tempfile::tempdir().unwrap();
    let selected_state = tempfile::tempdir().unwrap();
    let mut selected = vec![
        "feed",
        "default/default",
        "--bucket",
        "s3://local",
        "--to",
        "parquet",
        "--out",
        selected_out.path().to_str().unwrap(),
        "--state",
        selected_state.path().to_str().unwrap(),
        "--classes",
        "u",
        "--max-bytes",
        "1",
    ];
    log.lock().unwrap().clear();
    assert_eq!(
        poll(&Cli::parse_from(&selected).args, &reader, "")
            .await
            .unwrap(),
        2
    );
    assert!(!selected_out.path().join("_wk_states").exists());
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| e.op == Op::Get)
            .all(|e| !e.key.contains("/actors/t/"))
    );
    selected.pop();
    selected.push("2097152");
    assert_eq!(
        poll(&Cli::parse_from(&selected).args, &reader, "")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        read_projection(selected_out.path(), "u", "config").unwrap(),
        vec![("c".into(), 1)]
    );
    assert!(!selected_out.path().join("tables/t").exists());
    // Poll the original selection (all classes): only the newly seen class reads.
    log.lock().unwrap().clear();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        read_projection(output.path(), "u", "config").unwrap(),
        vec![("c".into(), 1)]
    );
    assert_eq!(
        read_projection(output.path(), "t", "config").unwrap(),
        vec![("a".into(), 3), ("b".into(), 3)]
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| e.op == Op::Get)
            .all(|e| !e.key.contains("/actors/t/"))
    );
}

fn read_projection(
    out: &Path,
    class: &str,
    table: &str,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::Field;
    let mut latest = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(out.join("_wk_states"))? {
        let reader = SerializedFileReader::new(std::fs::File::open(entry?.path())?)?;
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let cols: std::collections::BTreeMap<_, _> = row.get_column_iter().collect();
            if cols[&"_wk_class".to_string()] == &Field::Str(class.into())
                && let (Field::Str(name), Field::ULong(seq)) =
                    (cols[&"_wk_name".to_string()], cols[&"_wk_seq".to_string()])
            {
                latest
                    .entry(name.clone())
                    .and_modify(|v: &mut u64| *v = (*v).max(*seq))
                    .or_insert(*seq);
            }
        }
    }
    let mut result = Vec::new();
    for entry in std::fs::read_dir(out.join("tables").join(class).join(table))? {
        let reader = SerializedFileReader::new(std::fs::File::open(entry?.path())?)?;
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let cols: std::collections::BTreeMap<_, _> = row.get_column_iter().collect();
            if let (Field::Str(name), Field::ULong(seq), Field::Str(key), Field::Long(value)) = (
                cols[&"_wk_name".to_string()],
                cols[&"_wk_seq".to_string()],
                cols[&"key".to_string()],
                cols[&"value".to_string()],
            ) && latest.get(name) == Some(seq)
                && key == "durable"
            {
                result.push((name.clone(), *value));
            }
        }
    }
    result.sort();
    Ok(result)
}

// ADR 0033 dates a commit with its segment, not the later snapshot's stamp.
// ADR 0039's 1+s formula misses this one timestamp-only GET when s is zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_snapshot_head_preserves_commit_stamp() {
    use clap::Parser;
    use wasmkeep_cli::feed::{Args, poll};
    use wasmkeep_cluster::replication::{Listing, plan_restore};
    use wasmkeep_server::history;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let node = node_with(
        "feed-snapshot",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let mut count = 0;
    for n in 1..=70 {
        assert_eq!(
            send(node.addr, "POST", "/actor/t/snapshot/durable", "")
                .await
                .body
                .trim(),
            n.to_string()
        );
        // Periodic work runs before the next event. A read lets the node
        // take the due snapshot without advancing the head again.
        assert_eq!(
            send(node.addr, "GET", "/actor/t/snapshot/durable", "")
                .await
                .body
                .trim(),
            n.to_string()
        );
        let objects = history::list_objects(&store, "state/default/default/actors/t/snapshot/")
            .await
            .unwrap();
        let plan = plan_restore(&Listing::from_keys(objects.keys().map(String::as_str))).unwrap();
        if plan.head().txid > 1 && plan.snapshot == Some(plan.head()) {
            count = n;
            break;
        }
    }
    assert!(count > 0, "node did not take its periodic snapshot");
    let objects = history::list_objects(&store, "state/default/default/actors/t/snapshot/")
        .await
        .unwrap();
    let listing = Listing::from_keys(objects.keys().map(String::as_str));
    let head = plan_restore(&listing).unwrap().head();
    let segment = listing.segments.iter().find(|s| s.id == head).unwrap();
    let meta = &objects[&wasmkeep_cluster::replication::segment_key(segment)];
    let bytes = history::get(&store, &meta.location, Some(meta), 2 << 20)
        .await
        .unwrap();
    let (header, _) =
        wasmkeep_storage::ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(())).unwrap();
    let expected_stamp = header.timestamp_ms;

    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: Args,
    }
    let args = Cli::parse_from([
        "feed",
        "default/default",
        "--bucket",
        "s3://local",
        "--to",
        "parquet",
        "--out",
        out.path().to_str().unwrap(),
        "--state",
        state.path().to_str().unwrap(),
    ])
    .args;
    let (reader, log) = Counted::over(&store);
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    let log = log.lock().unwrap();
    assert_eq!(log.iter().filter(|e| e.op == Op::List).count(), 1);
    assert_eq!(log.iter().filter(|e| e.op == Op::Get).count(), 6);
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::Field;
    let file = std::fs::read_dir(out.path().join("_wk_states"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let parquet = SerializedFileReader::new(std::fs::File::open(file).unwrap()).unwrap();
    let row = parquet.get_row_iter(None).unwrap().next().unwrap().unwrap();
    assert!(
        row.get_column_iter()
            .any(|(name, value)| name == "_wk_committed_at"
                && value == &Field::TimestampMillis(expected_stamp))
    );

    assert_eq!(
        read_projection(out.path(), "t", "config").unwrap(),
        vec![("snapshot".into(), count)]
    );
}

fn feed_args(out: &Path, state: &Path, app: &str) -> wasmkeep_cli::feed::Args {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: wasmkeep_cli::feed::Args,
    }
    Cli::parse_from([
        "feed",
        app,
        "--bucket",
        "s3://local",
        "--to",
        "parquet",
        "--out",
        out.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
    ])
    .args
}

fn latest_kind(out: &Path, name: &str) -> (String, Option<String>) {
    use parquet::{
        file::reader::{FileReader, SerializedFileReader},
        record::Field,
    };
    let mut latest = None;
    for entry in std::fs::read_dir(out.join("_wk_states")).unwrap() {
        let reader =
            SerializedFileReader::new(std::fs::File::open(entry.unwrap().path()).unwrap()).unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row = row.unwrap();
            let cols: std::collections::BTreeMap<_, _> = row
                .get_column_iter()
                .map(|(k, v)| (k.as_str(), v))
                .collect();
            if cols["_wk_name"] != &Field::Str(name.into()) {
                continue;
            }
            let Field::ULong(seq) = cols["_wk_seq"] else {
                panic!()
            };
            let Field::Str(kind) = cols["_wk_kind"] else {
                panic!()
            };
            let reason = match cols["_wk_reason"] {
                Field::Str(s) => Some(s.clone()),
                Field::Null => None,
                _ => panic!(),
            };
            if latest.as_ref().is_none_or(|(s, _, _)| seq > s) {
                latest = Some((*seq, kind.clone(), reason));
            }
        }
    }
    let (_, kind, reason) = latest.unwrap();
    (kind, reason)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_f4_delete_restore_and_absent() {
    use wasmkeep_cli::feed::poll;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let node = node_with(
        "feed-kinds",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = feed_args(out.path(), state.path(), "default/default");
    let (reader, log) = Counted::over(&store);
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    let stage = super::restore::actor_restore_stage(&store, "a").await;
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    let before_restore = copy_bucket(&store).await;
    let key = super::restore::actor_restore_submit(&store, "a", "1234567890abcdef", &stage).await;
    super::restore::actor_restore_outcome(&store, &key, "done").await;
    resume_kind(
        &before_restore,
        &copy_bucket(&store).await,
        "default/default",
        "a",
        "rebase",
        Some("restore"),
    )
    .await;
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("rebase".into(), Some("restore".into()))
    );
    assert_eq!(
        read_projection(out.path(), "t", "config").unwrap(),
        vec![("a".into(), 1)]
    );
    let before_delete = copy_bucket(&store).await;
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/delete-durable", "")
            .await
            .status,
        200
    );
    resume_kind(
        &before_delete,
        &copy_bucket(&store).await,
        "default/default",
        "a",
        "deleted",
        Some("delete"),
    )
    .await;
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("deleted".into(), Some("delete".into()))
    );
    assert!(
        read_projection(out.path(), "t", "config")
            .unwrap()
            .is_empty()
    );
    log.lock().unwrap().clear();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| e.op == Op::Get)
            .count(),
        4
    );
    assert_eq!(
        write_after_delete(node.addr, "/actor/t/a/durable").await,
        "1"
    );
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(latest_kind(out.path(), "a").0, "state");
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/delete-durable", "")
            .await
            .status,
        200
    );
    assert_eq!(
        write_after_delete(node.addr, "/actor/t/a/durable").await,
        "1"
    );
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("rebase".into(), Some("delete".into()))
    );
    let before_absent = copy_bucket(&store).await;
    drop(node);
    for meta in wasmkeep_server::history::list_objects(&store, "state/default/default/actors/t/a/")
        .await
        .unwrap()
        .values()
    {
        store.delete(&meta.location).await.unwrap();
    }
    resume_kind(
        &before_absent,
        &copy_bucket(&store).await,
        "default/default",
        "a",
        "deleted",
        Some("absent"),
    )
    .await;
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("deleted".into(), Some("absent".into()))
    );
    log.lock().unwrap().clear();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| e.op == Op::List)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_f4_batched_delete_only_uploaded() {
    use wasmkeep_cli::feed::poll;
    for endpoint in ["delete", "delete-durable", "operator", "lost"] {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(Flaky::new(inner.clone(), Arc::new(AtomicBool::new(false))));
        let store: Arc<dyn ObjectStore> = flaky.clone();
        let dir = tempfile::tempdir().unwrap();
        let node = node_configured(
            "feed-batched",
            dir.path(),
            &store,
            Duration::from_secs(30),
            &["t"],
            test_actor_wasm(),
            Duration::from_secs(30),
            "",
            |c| {
                c.batched.insert("t".into());
                c.max_unsent_age = Duration::from_secs(5);
            },
        )
        .await;
        let out = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let args = feed_args(out.path(), state.path(), "default/default");
        let (reader, _) = Counted::over(&inner);
        assert_eq!(
            send(node.addr, "POST", "/actor/t/a/durable", "")
                .await
                .body
                .trim(),
            "1"
        );
        assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
        let before_pending = feed_files(out.path());
        let before_delete = copy_bucket(&inner).await;
        if endpoint == "lost" {
            flaky.hang_wal_puts.store(100, Ordering::SeqCst);
        } else {
            flaky.delay_wal_ms.store(800, Ordering::SeqCst);
        }
        assert_eq!(
            send(node.addr, "POST", "/actor/t/a/plain", "")
                .await
                .body
                .trim(),
            "1"
        );
        assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
        assert_eq!(
            read_projection(out.path(), "t", "config").unwrap(),
            vec![("a".into(), 1)]
        );
        assert_eq!(
            before_pending,
            feed_files(out.path()),
            "unuploaded commit became visible"
        );
        if endpoint == "lost" {
            node.service.shutdown(Duration::ZERO).await;
            assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
            assert_eq!(
                before_pending,
                feed_files(out.path()),
                "lost commit became visible"
            );
            continue;
        }
        if endpoint == "operator" {
            request_delete_of(&store, "t", "a").await;
        } else {
            assert_eq!(
                send(node.addr, "POST", &format!("/actor/t/a/{endpoint}"), "")
                    .await
                    .status,
                200
            );
        }
        eventually("delete reset uploaded", async || {
            let temp = tempfile::tempdir().unwrap();
            let result = wasmkeep_server::history::export_raw(
                &inner,
                &StateRoot::new("", "default", "default"),
                &wasmkeep_runtime::ActorAddress::new("default", "t", "a").unwrap(),
                wasmkeep_server::history::Point::Head,
                &temp.path().join("a.db"),
                2 << 20,
            )
            .await;
            result.is_ok_and(|e| {
                e.verification.replaced.is_some_and(|l| {
                    l.starts_with("delete ")
                        && l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap()
                            == e.position.txid
                })
            })
        })
        .await;
        assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
        resume_kind(
            &before_delete,
            &copy_bucket(&inner).await,
            "default/default",
            "a",
            "deleted",
            Some("delete"),
        )
        .await;
        assert_eq!(
            latest_kind(out.path(), "a"),
            ("deleted".into(), Some("delete".into()))
        );
        assert!(
            read_projection(out.path(), "t", "config")
                .unwrap()
                .is_empty()
        );
        assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_f4_whole_app_restore_and_recreate() {
    use super::app_delete::{app_node_following, deploy};
    use wasmkeep_cli::feed::poll;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (node, follows) = app_node_following("feed-app", dir.path(), &store)
        .await
        .unwrap();
    assert_eq!(super::deletes::read(node.addr, "a").await, "0");
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    let run = archive_command(
        &[
            "create",
            "--bucket",
            "s3://live",
            "--to",
            "s3://archive",
            "--app",
            "default/shop",
        ],
        &store,
        &archive,
    )
    .await
    .unwrap();
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    assert_eq!(
        send(node.addr, "POST", "/actor/t/b/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = feed_args(out.path(), state.path(), "default/shop");
    let (reader, _) = Counted::over(&store);
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    let before_restore = copy_bucket(&store).await;
    store
        .put(
            &StorePath::from("apps/default/shop/restoring"),
            b"feed-fence".to_vec().into(),
        )
        .await
        .unwrap();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 2);
    tokio::time::timeout(Duration::from_secs(10), node.service.app_deleted())
        .await
        .unwrap();
    follows.await.unwrap();
    drop(node);
    store
        .delete(&StorePath::from("apps/default/shop/restoring"))
        .await
        .unwrap();
    eventually("nodes stopped for restore", async || {
        wasmkeep_server::apps::live_nodes(store.clone(), "", "default", "shop")
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    archive_command(
        &[
            "restore",
            "--from",
            "s3://archive",
            "--bucket",
            "s3://live",
            "--app",
            "default/shop",
            "--in-place",
            "--run",
            &run,
        ],
        &archive,
        &store,
    )
    .await;
    resume_kind(
        &before_restore,
        &copy_bucket(&store).await,
        "default/shop",
        "a",
        "rebase",
        Some("incarnation"),
    )
    .await;
    resume_kind(
        &before_restore,
        &copy_bucket(&store).await,
        "default/shop",
        "b",
        "deleted",
        Some("absent"),
    )
    .await;
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("rebase".into(), Some("incarnation".into()))
    );
    assert_eq!(
        latest_kind(out.path(), "b"),
        ("deleted".into(), Some("absent".into()))
    );
    assert_eq!(
        read_projection(out.path(), "t", "config").unwrap(),
        vec![("a".into(), 1)]
    );
    wasmkeep_server::apps::delete_app(
        store.clone(),
        "",
        "default",
        "shop",
        &wasmkeep_server::apps::DeleteApp {
            resume: None,
            wait: Duration::from_secs(10),
            force: false,
            who: "feed-test".into(),
            late: Duration::from_millis(500),
        },
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 2);
    let app = AppRef::new(store.clone(), "", "default", "shop");
    assert!(app.recreate(Duration::from_millis(500)).await.unwrap());
    deploy(&store).await.unwrap();
    let fresh = tempfile::tempdir().unwrap();
    let (node, follows) = app_node_following("feed-new", fresh.path(), &store)
        .await
        .unwrap();
    assert_eq!(super::deletes::read(node.addr, "a").await, "0");
    for _ in 0..3 {
        assert_eq!(
            send(node.addr, "POST", "/actor/t/a/durable", "")
                .await
                .status,
            200
        );
    }
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(
        latest_kind(out.path(), "a"),
        ("rebase".into(), Some("incarnation".into()))
    );
    assert_eq!(
        read_projection(out.path(), "t", "config").unwrap(),
        vec![("a".into(), 3)]
    );
    follows.abort();
}

async fn archive_command(
    args: &[&str],
    source: &Arc<dyn ObjectStore>,
    target: &Arc<dyn ObjectStore>,
) -> Option<String> {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: wasmkeep_cli::backup::Command,
    }
    let cli = Cli::parse_from(std::iter::once("backup").chain(args.iter().copied()));
    wasmkeep_cli::backup::execute_with_stores(
        &cli.command,
        source,
        "",
        target,
        "",
        Duration::from_millis(10),
    )
    .await
    .unwrap()
}

async fn copy_bucket(source: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for meta in source.list(None).try_collect::<Vec<_>>().await.unwrap() {
        // The source is live: an object listed may be gone before its GET
        // (a restore's staging object is removed once the restore ends).
        let bytes = match source.get(&meta.location).await {
            Ok(got) => got.bytes().await.unwrap(),
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => panic!("copying {}: {e}", meta.location),
        };
        target.put(&meta.location, bytes.into()).await.unwrap();
    }
    target
}

fn feed_files(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    if root.exists() {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(feed_files(&path));
            } else {
                files.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
    }
    files
}

/// F3's injection points, now over states produced by real nodes. Every
/// committed batch and data file stays byte-identical after resuming.
async fn resume_kind(
    before: &Arc<dyn ObjectStore>,
    after: &Arc<dyn ObjectStore>,
    app: &str,
    name: &str,
    kind: &str,
    reason: Option<&str>,
) {
    use wasmkeep_cli::feed::{poll, poll_with_checkpoint};
    for step in [
        "listed",
        "reserved",
        "data",
        "data_complete",
        "states",
        "before_state",
        "head",
        "advanced",
    ] {
        if step == "data" && kind == "deleted" {
            continue;
        }
        let out = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let args = feed_args(out.path(), state.path(), app);
        let (before, _) = Counted::over(before);
        let (after, _) = Counted::over(after);
        assert_eq!(poll(&args, &before, "").await.unwrap(), 0);
        let mut reached = false;
        let result = poll_with_checkpoint(&args, &after, "", &mut |s| {
            if s == step {
                reached = true;
                return Err(std::io::Error::other(format!("injected {step}")).into());
            }
            Ok(())
        })
        .await;
        assert!(
            reached && result.is_err(),
            "{kind}/{reason:?}: {step} not reached"
        );
        let files = feed_files(out.path());
        let committed = feed_files(&out.path().join("_wk_states"));
        assert_eq!(poll(&args, &after, "").await.unwrap(), 0);
        assert_eq!(
            latest_kind(out.path(), name),
            (kind.into(), reason.map(str::to_owned))
        );
        for (path, bytes) in files {
            if path.extension().is_some_and(|s| s == "parquet")
                && committed.contains_key(
                    &out.path()
                        .join("_wk_states")
                        .join(path.file_name().unwrap()),
                )
            {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
        }
        assert!(
            feed_files(out.path())
                .keys()
                .all(|p| p.extension().is_none_or(|e| e != "partial"))
        );
        assert!(feed_files(&state.path().join("scratch")).is_empty());
        let settled = feed_files(out.path());
        assert_eq!(poll(&args, &after, "").await.unwrap(), 0);
        assert_eq!(settled, feed_files(out.path()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_f4_late_tip_collection_and_generation_zero() {
    use wasmkeep_cli::feed::poll;
    use wasmkeep_cluster::replication::{Listing, plan_restore, snapshot_key};
    use wasmkeep_server::history;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let node = node_with(
        "feed-tip",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .status,
        200
    );
    // A second node sees the base, but not the first node's next upload.
    let branch = copy_bucket(&store).await;
    // Independent ownership in the branch, history generations still seed it.
    for meta in branch.list(None).try_collect::<Vec<_>>().await.unwrap() {
        if !meta
            .location
            .as_ref()
            .starts_with("state/default/default/actors/")
        {
            branch.delete(&meta.location).await.unwrap();
        }
    }
    assert_eq!(
        send(node.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    let tip = copy_bucket(&store).await;
    let dir2 = tempfile::tempdir().unwrap();
    let successor = node_with(
        "feed-successor",
        dir2.path(),
        &branch,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(
        send(successor.addr, "POST", "/actor/t/a/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    let fork = copy_bucket(&branch).await;
    resume_kind(&tip, &fork, "default/default", "a", "rebase", Some("fork")).await;
    for _ in 0..3 {
        assert_eq!(
            send(successor.addr, "POST", "/actor/t/a/durable", "")
                .await
                .status,
            200
        );
    }
    // Collect to a real snapshot of the head; retain its dating segment.
    let collected = copy_bucket(&branch).await;
    let (bytes, _) = super::restore::actor_restore_stage(&branch, "a").await;
    let prefix = "state/default/default/actors/t/a/";
    let objects = history::list_objects(&branch, prefix).await.unwrap();
    let listing = Listing::from_keys(objects.keys().map(String::as_str));
    let head = plan_restore(&listing).unwrap().head();
    let boundary = wasmkeep_cluster::replication::segment_key(
        listing.segments.iter().find(|s| s.id == head).unwrap(),
    );
    for (key, meta) in &objects {
        if *key != boundary {
            collected.delete(&meta.location).await.unwrap();
        }
    }
    collected
        .put(
            &StorePath::from(format!("{prefix}{}", snapshot_key(head))),
            bytes.into(),
        )
        .await
        .unwrap();
    resume_kind(
        &tip,
        &collected,
        "default/default",
        "a",
        "rebase",
        Some("unproven"),
    )
    .await;
    resume_kind(&fork, &collected, "default/default", "a", "state", None).await;

    // Generation 0 is not an ownership generation. Use node-produced LTX
    // under zero-generation keys to test the conservative offline answer.
    async fn zero(source: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let result = copy_bucket(source).await;
        let objects = history::list_objects(source, "state/default/default/actors/t/a/")
            .await
            .unwrap();
        let listing = Listing::from_keys(objects.keys().map(String::as_str));
        for (key, meta) in &objects {
            let renamed = if let Some(s) = listing
                .segments
                .iter()
                .find(|s| wasmkeep_cluster::replication::segment_key(s) == *key)
            {
                let mut s = *s;
                s.id.generation = 0;
                s.parent_generation = 0;
                wasmkeep_cluster::replication::segment_key(&s)
            } else if let Some(s) = listing.snapshots.iter().find(|s| snapshot_key(**s) == *key) {
                let mut s = *s;
                s.generation = 0;
                snapshot_key(s)
            } else {
                continue;
            };
            let bytes = source
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            result.delete(&meta.location).await.unwrap();
            result
                .put(
                    &StorePath::from(format!("state/default/default/actors/t/a/{renamed}")),
                    bytes.into(),
                )
                .await
                .unwrap();
        }
        result
    }
    let zold = zero(&fork).await;
    let znew = zero(&collected).await;
    resume_kind(
        &zold,
        &znew,
        "default/default",
        "a",
        "rebase",
        Some("unproven"),
    )
    .await;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let args = feed_args(out.path(), state.path(), "default/default");
    let (reader, _) = Counted::over(&znew);
    assert_eq!(poll(&args, &reader, "").await.unwrap(), 0);
    assert_eq!(latest_kind(out.path(), "a"), ("state".into(), None));
}

/// A write right after the actor's own delete may get the retryable 503
/// ("this node cannot run the actor now; retry") while the deleted actor is
/// torn down; a client retries it, as here, for up to 10 s.
async fn write_after_delete(addr: std::net::SocketAddr, path: &str) -> String {
    let started = std::time::Instant::now();
    loop {
        let r = send(addr, "POST", path, "").await;
        if r.status == 503 && started.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert_eq!(r.status, 200, "{}", r.body);
        return r.body.trim().to_owned();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_feed() {
    super::s3::run("feed", feed_one_poll_projection_cost_and_read_only_on).await;
}
