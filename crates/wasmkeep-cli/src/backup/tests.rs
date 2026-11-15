use super::*;
use object_store::ObjectStoreExt;
use wasmkeep_cluster::replication::{Segment, SegmentId, segment_key, snapshot_key};
use wasmkeep_storage::{StateDir, Storage, StorageConfig};

struct Fixture {
    _dir: tempfile::TempDir,
    source: Store,
    dest: Store,
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
        let mut db = storage.open("counter", "alice").unwrap();
        db.exec("CREATE TABLE counts(n INTEGER)", &[]).unwrap();
        db.exec("INSERT INTO counts VALUES(1)", &[]).unwrap();
        db.set_alarm(123456).unwrap();
        db.start_capture().unwrap();
        let source: Store = Arc::new(object_store::memory::InMemory::new());
        let dest: Store = Arc::new(object_store::memory::InMemory::new());
        let bytes = db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
        put(
            &source,
            &format!(
                "state/acme/shop/actors/counter/alice/{}",
                snapshot_key(SegmentId {
                    generation: 1,
                    txid: 1
                })
            ),
            bytes,
            false,
        )
        .await
        .unwrap();
        db.take_changes().unwrap();
        put(
            &source,
            "apps/acme/shop/current",
            b"v1 1 3\n".to_vec(),
            false,
        )
        .await
        .unwrap();
        put(
            &source,
            "apps/acme/shop/v1/manifest",
            b"wasmkeep-manifest 1\n".to_vec(),
            false,
        )
        .await
        .unwrap();
        Self {
            _dir: dir,
            source,
            dest,
            db,
        }
    }
    fn args(&self) -> Create {
        Create {
            bucket: "s3://source".into(),
            to: "s3://destination".into(),
            app: Some(("acme".into(), "shop".into())),
            all: false,
            history: History::All,
            cache: Some(self._dir.path().join("cache")),
            to_env: None,
        }
    }
    async fn create(&self) -> String {
        create(&self.args(), &self.source, "", &self.dest, "")
            .await
            .unwrap();
        String::from_utf8(read(&self.dest, "latest", 1024).await.unwrap()).unwrap()
    }
    async fn increment(&mut self) {
        self.db.exec("UPDATE counts SET n=2", &[]).unwrap();
        let bytes = self
            .db
            .take_changes()
            .unwrap()
            .unwrap()
            .to_ltx(2, 2, 200)
            .unwrap();
        let segment = Segment {
            id: SegmentId {
                generation: 1,
                txid: 2,
            },
            parent_generation: 1,
        };
        put(
            &self.source,
            &format!(
                "state/acme/shop/actors/counter/alice/{}",
                segment_key(&segment)
            ),
            bytes,
            false,
        )
        .await
        .unwrap();
    }
}
#[tokio::test]
async fn incremental_archive_replays_and_verification_refuses_missing_data() {
    let mut f = Fixture::new().await;
    let before = history::list_objects(&f.source, "").await.unwrap();
    let first = f.create().await;
    assert_eq!(
        before,
        history::list_objects(&f.source, "").await.unwrap(),
        "backup mutated source"
    );
    verify(&f.dest, "", &first, None).await.unwrap();
    f.increment().await;
    let second = f.create().await;
    verify(&f.dest, "", &first, None).await.unwrap();
    verify(&f.dest, "", &second, None).await.unwrap();
    let run = load(&f.dest, "", &second).await.unwrap();
    assert_eq!(run.actors[0]["txid"], 2);
    assert_eq!(
        run.actors[0]["replayed_objects"], 1,
        "cache should apply only the new segment"
    );
    assert_eq!(run.actors[0]["alarm"], 123456);
    let actor = &run.actors[0];
    let name = actor["plan"][0].as_str().unwrap();
    let hash = actor["objects"][name]["sha256"].as_str().unwrap();
    f.dest
        .delete(&Key::from(format!("data/{hash}")))
        .await
        .unwrap();
    assert!(verify(&f.dest, "", &second, None).await.is_err());
}
#[tokio::test]
async fn incomplete_partial_and_tampered_manifests_are_refused() {
    let f = Fixture::new().await;
    assert!(verify(&f.dest, "", "unfinished", None).await.is_err());
    let id = f.create().await;
    let run = load(&f.dest, "", &id).await.unwrap();
    let mut complete = run.complete.clone();
    complete["failed"] = json!([{"actor":"missing","error":"collected"}]);
    json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    assert!(verify(&f.dest, "", &id, Some(1)).await.is_err());
    json_put(&f.dest, &format!("runs/{id}/complete"), &run.complete, true)
        .await
        .unwrap();
    put(
        &f.dest,
        text(&run.complete["chunks"][0], "key").unwrap(),
        b"[]".to_vec(),
        true,
    )
    .await
    .unwrap();
    assert!(verify(&f.dest, "", &id, None).await.is_err());
}
#[tokio::test]
async fn recreated_source_keys_do_not_overwrite_an_earlier_run() {
    let f = Fixture::new().await;
    let first = f.create().await;
    let mut replacement = Fixture::new().await;
    put(
        &f.source,
        "apps/acme/shop/recreated",
        b"incarnation two".to_vec(),
        false,
    )
    .await
    .unwrap();
    // Same key and txid, different LTX stamp/digest, as after recreation.
    let bytes = replacement
        .db
        .snapshot()
        .unwrap()
        .to_ltx(1, 1, 999)
        .unwrap();
    let name = format!(
        "state/acme/shop/actors/counter/alice/{}",
        snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })
    );
    put(&f.source, &name, bytes, true).await.unwrap();
    let second = f.create().await;
    let a = load(&f.dest, "", &first).await.unwrap();
    let b = load(&f.dest, "", &second).await.unwrap();
    assert_ne!(a.actors[0]["objects"], b.actors[0]["objects"]);
    verify(&f.dest, "", &first, None).await.unwrap();
    verify(&f.dest, "", &second, None).await.unwrap();
}
#[tokio::test]
async fn tombstone_and_prune_refuse_completion() {
    let f = Fixture::new().await;
    put(
        &f.source,
        "apps/acme/shop/deleted",
        b"deleted".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
    f.source
        .delete(&Key::from("apps/acme/shop/deleted"))
        .await
        .unwrap();
    put(&f.dest, "prunes/active", b"active".to_vec(), false)
        .await
        .unwrap();
    assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
    assert!(optional(&f.dest, "latest").await.unwrap().is_none());
}
#[test]
fn carry_requires_size_and_etag_and_boundary_segment_is_kept() {
    let snapshot = SegmentId {
        generation: 1,
        txid: 2,
    };
    let segment = Segment {
        id: snapshot,
        parent_generation: 1,
    };
    let listing = Listing {
        snapshots: vec![snapshot],
        segments: vec![segment],
    };
    let keys = history::copy_keys(&listing, &plan_restore(&listing).unwrap()).unwrap();
    assert_eq!(keys, vec![snapshot_key(snapshot), segment_key(&segment)]);
    let mut meta = ObjectMeta {
        location: Key::from("test"),
        last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        size: 3,
        e_tag: Some("new".into()),
        version: None,
    };
    let old = json!({"size":3,"etag":"old","version":null});
    assert!(!same(&meta, &old));
    meta.e_tag = Some("old".into());
    assert!(same(&meta, &old));
    meta.size = 4;
    assert!(!same(&meta, &old));
}

#[derive(Debug)]
struct ReadOnly {
    inner: Store,
    gets: std::sync::atomic::AtomicUsize,
    collect: std::sync::Mutex<Option<(Key, Key, Vec<u8>)>>,
}
impl std::fmt::Display for ReadOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("read-only backup source")
    }
}
#[async_trait::async_trait]
impl ObjectStore for ReadOnly {
    async fn put_opts(
        &self,
        _: &Key,
        _: object_store::PutPayload,
        _: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        panic!("source PUT")
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        panic!("source multipart")
    }
    async fn get_opts(
        &self,
        path: &Key,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        assert!(path.as_ref().starts_with("state/") || path.as_ref().starts_with("apps/"));
        self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let collect = self.collect.lock().unwrap().take();
        if let Some((old, new, bytes)) = collect {
            self.inner
                .put_opts(&new, bytes.into(), PutOptions::default())
                .await?;
            self.inner.delete(&old).await?;
        }
        self.inner.get_opts(path, opts).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn delete_stream(
        &self,
        _: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        panic!("source DELETE")
    }
    async fn list_with_delimiter(
        &self,
        _: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        panic!("unexpected delimiter list")
    }
    async fn copy_opts(
        &self,
        _: &Key,
        _: &Key,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        panic!("source COPY")
    }
}
#[tokio::test]
async fn source_is_list_get_only_and_idle_actor_costs_no_gets() {
    use std::sync::atomic::Ordering;
    let f = Fixture::new().await;
    let readonly = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: Default::default(),
    });
    let source: Store = readonly.clone();
    let dest = f.dest.clone();
    let copier = Copier {
        source: &source,
        dest: &dest,
        dp: "",
        cache: None,
        all: false,
    };
    let prefix = "state/acme/shop/actors/counter/alice/";
    let first = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap();
    readonly.gets.store(0, Ordering::SeqCst);
    let second = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            Some(&first),
        )
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(readonly.gets.load(Ordering::SeqCst), 0);
    create(&f.args(), &source, "", &dest, "").await.unwrap();
}

#[tokio::test]
async fn corrupt_raw_cache_falls_back_and_deleted_boundary_replays_empty() {
    let mut f = Fixture::new().await;
    let first = f.create().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    let cache = f.args().cache.unwrap();
    std::fs::write(
        cache.join(format!("{}.db", digest(prefix.as_bytes()))),
        b"broken",
    )
    .unwrap();
    f.increment().await;
    let second = f.create().await;
    verify(&f.dest, "", &second, None).await.unwrap();
    let run = load(&f.dest, "", &second).await.unwrap();
    assert!(run.actors[0]["replayed_objects"].as_u64().unwrap() > 1);
    f.db.exec("DROP TABLE counts", &[]).unwrap();
    f.db.clear_alarm().unwrap();
    let reset = Segment {
        id: SegmentId {
            generation: 1,
            txid: 3,
        },
        parent_generation: 1,
    };
    let bytes =
        f.db.take_changes()
            .unwrap()
            .unwrap()
            .to_ltx(3, 3, 300)
            .unwrap();
    put(
        &f.source,
        &format!("{prefix}{}", segment_key(&reset)),
        bytes,
        false,
    )
    .await
    .unwrap();
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 3, 400).unwrap();
    put(
        &f.source,
        &format!("{prefix}{}", snapshot_key(reset.id)),
        bytes,
        false,
    )
    .await
    .unwrap();
    let deleted = f.create().await;
    let run = load(&f.dest, "", &deleted).await.unwrap();
    assert!(
        run.actors[0]["plan"]
            .as_array()
            .unwrap()
            .contains(&json!(segment_key(&reset)))
    );
    assert!(
        replay(&f.dest, "", &run.actors[0])
            .await
            .unwrap()
            .verification
            .tables
            .is_empty()
    );
    verify(&f.dest, "", &first, None).await.unwrap();
    verify(&f.dest, "", &deleted, None).await.unwrap();
}

#[tokio::test]
async fn collector_between_listing_and_get_forces_a_fresh_plan() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    let old = Key::from(format!(
        "{prefix}{}",
        snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })
    ));
    let new = Key::from(format!(
        "{prefix}{}",
        snapshot_key(SegmentId {
            generation: 1,
            txid: 2
        })
    ));
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 2, 500).unwrap();
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: std::sync::Mutex::new(Some((old, new, bytes))),
    });
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: false,
    };
    let actor = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(actor["txid"], 2);
    assert_eq!(
        actor["plan"][0],
        snapshot_key(SegmentId {
            generation: 1,
            txid: 2
        })
    );
    replay(&f.dest, "", &actor).await.unwrap();
}
#[tokio::test]
async fn corrupt_ltx_records_partial_and_never_verifies() {
    let f = Fixture::new().await;
    let path = format!(
        "state/acme/shop/actors/counter/alice/{}",
        snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })
    );
    let mut bytes = read(&f.source, &path, MAX).await.unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    put(&f.source, &path, bytes, true).await.unwrap();
    assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
    let id = String::from_utf8(read(&f.dest, "latest", 1024).await.unwrap()).unwrap();
    let run = load(&f.dest, "", &id).await.unwrap();
    assert_eq!(array(&run.complete, "failed").unwrap().len(), 1);
    assert!(verify(&f.dest, "", &id, None).await.is_err());
}

#[tokio::test]
async fn snapshot_must_match_its_boundary_even_when_both_ltx_files_are_valid() {
    let mut f = Fixture::new().await;
    f.increment().await;
    f.create().await;
    let mut other = Fixture::new().await;
    let bytes = other.db.snapshot().unwrap().to_ltx(1, 2, 300).unwrap();
    let prefix = "state/acme/shop/actors/counter/alice/";
    put(
        &f.source,
        &format!(
            "{prefix}{}",
            snapshot_key(SegmentId {
                generation: 1,
                txid: 2
            })
        ),
        bytes,
        false,
    )
    .await
    .unwrap();
    for cache in [None, f.args().cache] {
        let copier = Copier {
            source: &f.source,
            dest: &f.dest,
            dp: "",
            cache: cache.as_deref(),
            all: false,
        };
        let error = copier
            .actor(
                prefix,
                history::list_objects(&f.source, prefix).await.unwrap(),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("boundary segment checksum"),
            "{error}"
        );
    }
}
