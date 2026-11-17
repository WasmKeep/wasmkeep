use super::*;

use object_store::path::Path as StorePath;

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
    use clap::Parser;
    use wasmkeep_cli::feed::{Args, poll};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
