use super::*;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
use wasmkeep_cluster::replication::Segment;
use wasmkeep_storage::{SqlValue, StateDir, Storage, StorageConfig};

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<dyn ObjectStore>,
    root: StateRoot,
    actor: ActorAddress,
    db: wasmkeep_storage::ActorDb,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(
            StorageConfig::new(dir.path()),
            StateDir {
                dir: "actors".into(),
                scratch: "scratch".into(),
                tenant: "acme".into(),
            },
        );
        let mut db = storage.open("c", "a% b").unwrap();
        db.exec("CREATE TABLE items(n INTEGER)", &[]).unwrap();
        db.exec("INSERT INTO items VALUES(1)", &[]).unwrap();
        db.start_capture().unwrap();
        let mut f = Self {
            _dir: dir,
            store: Arc::new(InMemory::new()),
            root: StateRoot::new("prefix", "acme", "shop"),
            actor: ActorAddress::new("acme", "c", "a% b").unwrap(),
            db,
        };
        let snapshot = f.db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
        f.put(&snapshot_key(id(1, 1)), snapshot).await;
        f.db.take_changes().unwrap();
        f
    }
    fn key(&self, key: &str) -> StorePath {
        StorePath::from(format!(
            "{}/{key}",
            self.root.bucket_key(Family::History, "c/a%25%20b")
        ))
    }
    async fn put(&self, key: &str, bytes: Vec<u8>) {
        self.store
            .put(&self.key(key), PutPayload::from(bytes))
            .await
            .unwrap();
    }
    async fn increment(&mut self, generation: u64, txid: u64, parent: u64, stamp: i64) {
        self.db.exec("UPDATE items SET n=n+1", &[]).unwrap();
        let bytes = self
            .db
            .take_changes()
            .unwrap()
            .unwrap()
            .to_ltx(txid, txid, stamp)
            .unwrap();
        self.put(
            &segment_key(&Segment {
                id: id(generation, txid),
                parent_generation: parent,
            }),
            bytes,
        )
        .await;
    }
    async fn read(&self, point: Point) -> (Export, i64) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.db");
        let result = export(&self.store, &self.root, &self.actor, point, &out, 1 << 20)
            .await
            .unwrap();
        let rows = readonly::query(&out, "SELECT n FROM items").unwrap();
        let SqlValue::Integer(n) = rows.values[0][0] else {
            panic!("not integer")
        };
        let bytes = std::fs::read(&out).unwrap();
        assert_eq!(&bytes[18..20], &[1, 1]);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        (result, n)
    }
}
fn id(generation: u64, txid: u64) -> SegmentId {
    SegmentId { generation, txid }
}

#[tokio::test]
async fn initial_snapshot_and_historical_points_use_capture_stamps_and_current_ancestry() {
    let mut f = Fixture::new().await;
    assert_eq!(f.read(Point::Head).await.1, 1);
    assert_eq!(f.read(Point::At(100)).await.1, 1);
    f.increment(1, 2, 1, 200).await;
    let snap = f.db.snapshot().unwrap().to_ltx(1, 2, 9999).unwrap();
    f.put(&snapshot_key(id(1, 2)), snap).await;
    f.increment(1, 3, 1, 300).await;
    f.increment(2, 4, 1, 250).await; // skew: cannot skip txid 3 to reach 4
    assert_eq!(f.read(Point::At(275)).await.1, 2);
    assert_eq!(f.read(Point::At(300)).await.1, 4);
    assert_eq!(f.read(Point::Txid(3)).await.1, 3);
    // Abandoned generation with a higher txid cannot become a historical head.
    f.increment(1, 5, 1, 150).await;
    let (result, n) = f.read(Point::Head).await;
    assert_eq!(n, 4);
    assert_eq!(result.position, id(2, 4));
    assert_eq!(f.read(Point::Txid(2)).await.0.timestamp_ms, 200);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.db");
    for point in [Point::Txid(5), Point::At(99)] {
        let error = export(&f.store, &f.root, &f.actor, point, &out, 1 << 20)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("retained history"), "{error}");
        assert!(!out.exists());
    }
}

#[tokio::test]
async fn retained_snapshot_bounds_old_points_and_corrupt_or_missing_segments_fail_closed() {
    let mut f = Fixture::new().await;
    f.increment(1, 2, 1, 200).await;
    f.increment(1, 3, 1, 300).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.db");
    let key = segment_key(&Segment {
        id: id(1, 2),
        parent_generation: 1,
    });
    let original = f
        .store
        .get(&f.key(&key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut corrupt = original.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    f.put(&key, corrupt).await;
    let e = export(&f.store, &f.root, &f.actor, Point::Head, &out, 1 << 20)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("corrupt history"), "{e}");
    assert!(!out.exists());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    f.store.delete(&f.key(&key)).await.unwrap();
    let e = export(&f.store, &f.root, &f.actor, Point::Head, &out, 1 << 20)
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("missing") && e.to_string().contains("8 attempts"),
        "{e}"
    );
    f.put(&key, original.to_vec()).await;
    let snap = f.db.snapshot().unwrap().to_ltx(1, 3, 10000).unwrap();
    f.put(&snapshot_key(id(1, 3)), snap).await;
    f.store
        .delete(&f.key(&snapshot_key(id(1, 1))))
        .await
        .unwrap();
    f.store.delete(&f.key(&key)).await.unwrap();
    assert_eq!(f.read(Point::Head).await.1, 3);
    for point in [Point::At(299), Point::Txid(2)] {
        let e = export(&f.store, &f.root, &f.actor, point, &out, 1 << 20)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("replayable txids 3..=3"), "{e}");
    }
}

#[tokio::test]
async fn existing_destinations_and_size_caps_are_preserved() {
    let f = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.db");
    std::fs::write(&out, b"keep me").unwrap();
    assert!(
        export(&f.store, &f.root, &f.actor, Point::Head, &out, 1 << 20)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&out).unwrap(), b"keep me");
    std::fs::remove_file(&out).unwrap();
    assert!(
        export(&f.store, &f.root, &f.actor, Point::Head, &out, 1)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

/// The reader is only allowed LIST/GET, even when a collector changes the
/// underlying store between those operations. Any write/control access panics.
#[derive(Debug)]
struct ReadOnlyRace {
    inner: Arc<dyn ObjectStore>,
    collect: std::sync::Mutex<Option<(StorePath, StorePath, Vec<u8>)>>,
    lists: std::sync::atomic::AtomicUsize,
    short_reads: std::sync::Mutex<u32>,
}
impl std::fmt::Display for ReadOnlyRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadOnlyRace")
    }
}
#[async_trait::async_trait]
impl ObjectStore for ReadOnlyRace {
    async fn put_opts(
        &self,
        _: &StorePath,
        _: PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        panic!("reader wrote to bucket")
    }
    async fn put_multipart_opts(
        &self,
        _: &StorePath,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        panic!("reader wrote multipart")
    }
    async fn get_opts(
        &self,
        path: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        assert!(
            path.as_ref().contains("/actors/") || path.as_ref().contains("/owners/"),
            "unexpected GET {path}"
        );
        let collect = self.collect.lock().unwrap().take();
        if let Some((old, new, bytes)) = collect {
            self.inner.put(&new, PutPayload::from(bytes)).await?;
            if old != new {
                self.inner.delete(&old).await?;
            }
        }
        let mut result = self.inner.get_opts(path, options).await?;
        // S3 LIST omits version IDs even when a versioned GET supplies one.
        if result.meta.version.is_none() {
            result.meta.version = Some("get-only-version".into());
        }
        let shorten = {
            let mut remaining = self.short_reads.lock().unwrap();
            let shorten = *remaining > 0;
            *remaining = remaining.saturating_sub(1);
            shorten
        };
        if shorten {
            result.payload = object_store::GetResultPayload::Stream(Box::pin(
                futures_util::stream::once(async { Ok(bytes::Bytes::new()) }),
            ));
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        _: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        panic!("reader deleted bucket data")
    }
    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        assert!(prefix.unwrap().as_ref().contains("/actors/"));
        self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        _: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        panic!("unexpected listing")
    }
    async fn copy_opts(
        &self,
        _: &StorePath,
        _: &StorePath,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        panic!("reader copied bucket data")
    }
}

#[tokio::test]
async fn a_collection_between_list_and_get_replans_without_bucket_writes() {
    let mut f = Fixture::new().await;
    f.increment(1, 2, 1, 200).await;
    let snapshot = f.db.snapshot().unwrap().to_ltx(1, 2, 9999).unwrap();
    let reader = Arc::new(ReadOnlyRace {
        inner: Arc::clone(&f.store),
        collect: std::sync::Mutex::new(Some((
            f.key(&snapshot_key(id(1, 1))),
            f.key(&snapshot_key(id(1, 2))),
            snapshot,
        ))),
        lists: std::sync::atomic::AtomicUsize::new(0),
        short_reads: std::sync::Mutex::new(0),
    });
    f.store = reader.clone();
    let (result, n) = f.read(Point::Head).await;
    assert_eq!(n, 2);
    assert_eq!(result.attempts, 2);
    assert_eq!(reader.lists.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_recreated_key_with_the_same_size_is_replanned_by_object_identity() {
    let mut f = Fixture::new().await;
    let key = f.key(&snapshot_key(id(1, 1)));
    let newer = f.db.snapshot().unwrap().to_ltx(1, 1, 900).unwrap();
    assert_eq!(f.store.head(&key).await.unwrap().size, newer.len() as u64);
    let reader = Arc::new(ReadOnlyRace {
        inner: Arc::clone(&f.store),
        collect: std::sync::Mutex::new(Some((key.clone(), key, newer))),
        lists: std::sync::atomic::AtomicUsize::new(0),
        short_reads: std::sync::Mutex::new(0),
    });
    f.store = reader.clone();
    let (result, _) = f.read(Point::Head).await;
    assert_eq!(result.timestamp_ms, 900);
    assert_eq!(result.attempts, 2);
}

#[test]
fn publication_fallback_is_private_and_never_clobbers() {
    use std::io::ErrorKind;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"verified database").unwrap();
    for kind in [
        ErrorKind::Unsupported,
        ErrorKind::PermissionDenied,
        ErrorKind::CrossesDevices,
    ] {
        let dest = dir.path().join(format!("{kind:?}"));
        publish(&source, &dest, |_, _| Err(kind.into())).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"verified database");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::write(&source, b"replacement").unwrap();
        assert!(publish(&source, &dest, |_, _| Err(kind.into())).is_err());
        assert_eq!(std::fs::read(&dest).unwrap(), b"verified database");
        std::fs::write(&source, b"verified database").unwrap();
    }
}

#[tokio::test]
async fn time_selection_skips_undated_retained_snapshot_base() {
    let mut f = Fixture::new().await;
    f.increment(1, 2, 1, 200).await;
    let snapshot = f.db.snapshot().unwrap().to_ltx(1, 2, 9999).unwrap();
    f.put(&snapshot_key(id(1, 2)), snapshot).await;
    f.store
        .delete(&f.key(&snapshot_key(id(1, 1))))
        .await
        .unwrap();
    f.store
        .delete(&f.key(&segment_key(&Segment {
            id: id(1, 2),
            parent_generation: 1,
        })))
        .await
        .unwrap();
    f.increment(1, 3, 1, 300).await;
    assert_eq!(f.read(Point::At(300)).await.1, 3);
    let dir = tempfile::tempdir().unwrap();
    let err = export(
        &f.store,
        &f.root,
        &f.actor,
        Point::At(299),
        &dir.path().join("out"),
        1 << 20,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Point(_)), "{err}");
    assert!(
        read_deadline(Point::At(300), "selection")
            .to_string()
            .contains("--txid")
    );
}

#[tokio::test]
async fn owner_match_is_enforced_before_publication() {
    let f = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let owner_key = StorePath::from(f.root.bucket_key(Family::Owners, "c/a%25%20b"));
    for owner in [None, Some(2), Some(1)] {
        if let Some(generation) = owner {
            let bytes = wasmkeep_cluster::node_lease::OwnerRecord {
                generation,
                holder: None,
                recorded: None,
            }
            .encode();
            f.store
                .put(&owner_key, PutPayload::from(bytes))
                .await
                .unwrap();
        }
        let result = export_with_options(
            &f.store,
            &f.root,
            &f.actor,
            Point::Head,
            &out,
            Options {
                max_bytes: 1 << 20,
                tmpdir: Some(dir.path()),
                require_owner_match: true,
            },
        )
        .await;
        if owner == Some(1) {
            assert!(result.is_ok(), "{result:?}");
            assert!(out.exists());
        } else {
            assert!(matches!(result, Err(Error::OwnerMismatch)), "{result:?}");
            assert!(!out.exists());
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }
}

#[tokio::test]
async fn short_bodies_retry_and_exhaust_as_read_errors() {
    let f = Fixture::new().await;
    for short in [1, super::super::READ_ATTEMPTS] {
        let reader = Arc::new(ReadOnlyRace {
            inner: f.store.clone(),
            collect: std::sync::Mutex::new(None),
            lists: std::sync::atomic::AtomicUsize::new(0),
            short_reads: std::sync::Mutex::new(short),
        });
        let store: Arc<dyn ObjectStore> = reader.clone();
        let result = get(&store, &f.key(&snapshot_key(id(1, 1))), None, 1 << 20).await;
        if short == 1 {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(matches!(result, Err(Error::Bucket(_))), "{result:?}");
        }
        assert_eq!(*reader.short_reads.lock().unwrap(), 0);
    }
}

#[tokio::test]
async fn shared_listing_returns_more_than_ten_thousand_objects() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for i in 0..10_001 {
        store
            .put(
                &StorePath::from(format!("large/{i}")),
                bytes::Bytes::new().into(),
            )
            .await
            .unwrap();
    }
    assert_eq!(list_objects(&store, "large").await.unwrap().len(), 10_001);
}
