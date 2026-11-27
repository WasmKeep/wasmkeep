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
            require_continuous: None,
            history_retention_secs: 3600,
            cache: Some(self._dir.path().join("cache")),
            to_env: None,
        }
    }
    async fn create(&self) -> String {
        create(&self.args(), &self.source, "", &self.dest, "")
            .await
            .unwrap();
        latest(&self.dest, "").await.unwrap().unwrap()
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
    assert_eq!(disk_actor(&run.actors[0]).unwrap()["txid"], 2);
    assert_eq!(
        disk_actor(&run.actors[0]).unwrap()["replayed_objects"],
        1,
        "cache should apply only the new segment"
    );
    assert_eq!(disk_actor(&run.actors[0]).unwrap()["alarm"], 123456);
    let actor = &disk_actor(&run.actors[0]).unwrap();
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
/// ADR 0043, section 2: a backup run is the CLI's own format. A run of a
/// later format, whole and with its digests right, is refused by this
/// `wasmkeep`, never read as format 1.
#[tokio::test]
async fn a_run_of_a_later_format_is_refused() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let run = load(&f.dest, "", &id).await.unwrap();
    assert_eq!(run.complete["format"], 1);
    let root_key = format!("runs/{id}/root");
    let root: Value =
        serde_json::from_slice(&read(&f.dest, &root_key, META_MAX).await.unwrap()).unwrap();
    // The run as a later version would write it: its root names the new
    // manifest's digest.
    let write = async |complete: &Value| {
        let hash = json_put(&f.dest, &format!("runs/{id}/complete"), complete, true)
            .await
            .unwrap();
        let mut root = root.clone();
        root["sha256"] = json!(hash);
        json_put(&f.dest, &root_key, &root, true).await.unwrap();
    };
    let mut later = run.complete.clone();
    later["format"] = json!(3);
    write(&later).await;
    let refused = load(&f.dest, "", &id).await.map(drop).unwrap_err();
    assert!(
        format!("{refused:#}").contains("unknown or inconsistent backup format"),
        "{refused:#}"
    );
    write(&run.complete).await;
    load(&f.dest, "", &id).await.unwrap();
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
    assert_ne!(
        disk_actor(&a.actors[0]).unwrap()["objects"],
        disk_actor(&b.actors[0]).unwrap()["objects"]
    );
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
    collect_after: usize,
    fail_collect: bool,
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
        let count = self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let collect = if path.as_ref().starts_with("state/") && count >= self.collect_after {
            self.collect.lock().unwrap().take()
        } else {
            None
        };
        if let Some((old, new, bytes)) = collect {
            self.inner
                .put_opts(&new, bytes.into(), PutOptions::default())
                .await?;
            self.inner.delete(&old).await?;
            if self.fail_collect {
                return Err(object_store::Error::NotFound {
                    path: path.to_string(),
                    source: "injected later GET failure".into(),
                });
            }
        }
        self.inner.get_opts(path, opts).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Like the S3 paginated stream, this must not be polled after EOF.
        futures_util::stream::unfold(self.inner.list(prefix), |mut stream| async {
            stream.next().await.map(|meta| (meta, stream))
        })
        .boxed()
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
/// A source that lists `key` but answers every read of it as missing,
/// counting them: a key that keeps changing under the reader.
#[derive(Debug)]
struct Unreadable {
    inner: Store,
    key: Key,
    misses: std::sync::atomic::AtomicUsize,
}
impl std::fmt::Display for Unreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source with an unreadable key")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Unreadable {
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
        if *path == self.key {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: "listed, never readable".into(),
            });
        }
        self.inner.get_opts(path, opts).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures_util::stream::unfold(self.inner.list(prefix), |mut stream| async {
            stream.next().await.map(|meta| (meta, stream))
        })
        .boxed()
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
/// A retained key still listed whose every read misses: the actor is read
/// again a bounded number of times (`ATTEMPTS`), then kept with the key
/// as a gap, never retried for ever and never given up as a whole.
#[tokio::test]
async fn a_listed_key_that_keeps_missing_is_retried_then_a_gap() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    // A snapshot at the head, so the first one is off the plan.
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 2, 500).unwrap();
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
    let first = snapshot_key(SegmentId {
        generation: 1,
        txid: 1,
    });
    let unreadable = Arc::new(Unreadable {
        inner: f.source.clone(),
        key: Key::from(format!("{prefix}{first}")),
        misses: Default::default(),
    });
    let source: Store = unreadable.clone();
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: true,
        retention: std::time::Duration::ZERO,
    };
    let actor = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(actor["gaps"], json!([first]));
    assert_eq!(actor["collected"], json!([]));
    assert_eq!(
        unreadable.misses.load(std::sync::atomic::Ordering::SeqCst),
        ATTEMPTS
    );
}
#[tokio::test]
async fn source_is_list_get_only_and_idle_actor_costs_no_gets() {
    use std::sync::atomic::Ordering;
    let f = Fixture::new().await;
    let readonly = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: Default::default(),
        collect_after: 0,
        fail_collect: false,
    });
    let source: Store = readonly.clone();
    let dest = f.dest.clone();
    let copier = Copier {
        source: &source,
        dest: &dest,
        dp: "",
        cache: None,
        all: false,
        retention: std::time::Duration::ZERO,
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
    assert!(
        disk_actor(&run.actors[0]).unwrap()["replayed_objects"]
            .as_u64()
            .unwrap()
            > 1
    );
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
        disk_actor(&run.actors[0]).unwrap()["plan"]
            .as_array()
            .unwrap()
            .contains(&json!(segment_key(&reset)))
    );
    assert!(
        replay(&f.dest, "", &disk_actor(&run.actors[0]).unwrap())
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
        collect_after: 0,
        fail_collect: false,
    });
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: false,
        retention: std::time::Duration::ZERO,
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
    let id = latest(&f.dest, "").await.unwrap().unwrap();
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
            retention: std::time::Duration::ZERO,
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

#[tokio::test]
async fn destination_rejects_every_bucket_alias() {
    let f = Fixture::new().await;
    for name in ["AWS_BUCKET", "AWS_BUCKET_NAME", "bucket", "bucket_name"] {
        let file = f._dir.path().join("destination.env");
        std::fs::write(&file, format!("{name}=source\nAWS_REGION=us-east-1\n")).unwrap();
        let mut args = f.args();
        args.to_env = Some(file);
        assert!(destination(&args).is_err(), "accepted {name}");
    }
}

#[tokio::test]
async fn editing_complete_cannot_hide_failed_membership() {
    let f = Fixture::new().await;
    put(
        &f.source,
        &format!(
            "state/acme/shop/actors/counter/bob/{}",
            snapshot_key(SegmentId {
                generation: 1,
                txid: 1
            })
        ),
        b"broken".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
    let id = latest(&f.dest, "").await.unwrap().unwrap();
    let run = load(&f.dest, "", &id).await.unwrap();
    let mut complete = run.complete;
    complete["failed"] = json!([]);
    json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    assert!(
        verify(&f.dest, "", &id, None).await.is_err(),
        "failure hidden by complete-only edit"
    );
}

#[tokio::test]
async fn history_all_reports_listed_get_miss() {
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
    put(&f.source, new.as_ref(), bytes.clone(), false)
        .await
        .unwrap();
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: std::sync::Mutex::new(Some((old, new, bytes))),
        collect_after: 0,
        fail_collect: false,
    });
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: true,
        retention: std::time::Duration::ZERO,
    };
    let actor = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap();
    // The first snapshot went from below the second and its segment: what
    // collection deletes, so recorded as collected, not as a gap.
    assert_eq!(array(&actor, "gaps").unwrap().len(), 0);
    assert_eq!(
        actor["collected"],
        json!([snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })])
    );
}

/// A key gone between the listing and its read that no collection could
/// have deleted (no listed snapshot with its segment above it) stays a
/// gap: `--require-continuous` (the default) refuses the run and its
/// verification; `--require-continuous false` completes with the gap.
#[tokio::test]
async fn a_miss_no_collection_explains_stays_a_gap() {
    for continuous in [None, Some(false)] {
        let mut f = Fixture::new().await;
        f.increment().await;
        let prefix = "state/acme/shop/actors/counter/alice/";
        // Generation 2 takes over at txid 3; a late segment of generation
        // 1 at txid 3 is off the chain. No snapshot has its segment listed.
        f.db.exec("UPDATE counts SET n=3", &[]).unwrap();
        let bytes =
            f.db.take_changes()
                .unwrap()
                .unwrap()
                .to_ltx(3, 3, 300)
                .unwrap();
        let at = |generation, parent_generation| Segment {
            id: SegmentId {
                generation,
                txid: 3,
            },
            parent_generation,
        };
        put(
            &f.source,
            &format!("{prefix}{}", segment_key(&at(2, 1))),
            bytes.clone(),
            false,
        )
        .await
        .unwrap();
        let late = Key::from(format!("{prefix}{}", segment_key(&at(1, 1))));
        put(&f.source, late.as_ref(), bytes.clone(), false)
            .await
            .unwrap();
        let source: Store = Arc::new(ReadOnly {
            inner: f.source.clone(),
            gets: Default::default(),
            collect: std::sync::Mutex::new(Some((late, Key::from("elsewhere/unrelated"), bytes))),
            collect_after: 0,
            fail_collect: false,
        });
        let mut args = f.args();
        args.require_continuous = continuous;
        // Old enough: only the shape is wrong.
        args.history_retention_secs = 0;
        let raced = create(&args, &source, "", &f.dest, "").await;
        let refused = continuous.is_none();
        assert_eq!(raced.is_err(), refused, "{continuous:?}");
        let run_id = latest(&f.dest, "").await.unwrap().unwrap();
        let run = load(&f.dest, "", &run_id).await.unwrap();
        let actor = disk_actor(&run.actors[0]).unwrap();
        assert_eq!(actor["status"], if refused { "failed" } else { "copied" });
        assert_eq!(actor["gaps"], json!([segment_key(&at(1, 1))]), "{actor}");
        assert_eq!(actor["collected"], json!([]), "{actor}");
        assert_eq!(verify(&f.dest, "", &run_id, None).await.is_err(), refused);
    }
}

/// ADR 0042, Tests, "Readers": `backup create` racing an idle pass. The
/// actor rests with an idle snapshot at its head; the pass deletes
/// `idle_collectable` of the released generation (here the first
/// snapshot) after the backup listed and before its first read, as any
/// collection may:
/// - `--history head` completes and verifies at the head;
/// - `--history all`, continuous (the default) or not, lists again after
///   the miss, finds the first snapshot gone from below the idle snapshot
///   and its segment, as the rule deletes it, and completes in the same
///   run: the key recorded as collected, no gap, and the backup verifies.
///   (Before 0.8.1 the continuous run was refused as partial, and the
///   next one completed.)
#[tokio::test]
async fn backup_create_during_an_idle_pass() {
    for (history, continuous) in [
        (History::Head, None),
        (History::All, Some(false)),
        (History::All, None),
    ] {
        let mut f = Fixture::new().await;
        f.increment().await;
        let prefix = "state/acme/shop/actors/counter/alice/";
        let at_head = SegmentId {
            generation: 1,
            txid: 2,
        };
        let idle = Key::from(format!("{prefix}{}", snapshot_key(at_head)));
        let bytes = f.db.snapshot().unwrap().to_ltx(1, 2, 500).unwrap();
        put(&f.source, idle.as_ref(), bytes.clone(), false)
            .await
            .unwrap();
        let listed = history::list_objects(&f.source, prefix).await.unwrap();
        let listing = wasmkeep_cluster::replication::Listing::from_keys(
            listed.keys().map(|k| k.strip_prefix(prefix).unwrap_or(k)),
        );
        let (snapshot, gone) =
            wasmkeep_cluster::replication::idle_collectable(&listing, 1).unwrap();
        assert_eq!(snapshot, at_head);
        assert_eq!(
            gone,
            [snapshot_key(SegmentId {
                generation: 1,
                txid: 1
            })]
        );
        // At the first read: the pass's delete. The hook also writes an
        // object; one outside the app, so no listed object changes.
        let racing = Arc::new(ReadOnly {
            inner: f.source.clone(),
            gets: Default::default(),
            collect: std::sync::Mutex::new(Some((
                Key::from(format!("{prefix}{}", gone[0])),
                Key::from("elsewhere/unrelated"),
                bytes,
            ))),
            collect_after: 0,
            fail_collect: false,
        });
        let source: Store = racing.clone();
        let mut args = f.args();
        args.history = history;
        args.require_continuous = continuous;
        // The objects here were written a moment ago: a window of 0 lets
        // the pass's delete be older than it, as a pass's deletes are.
        args.history_retention_secs = 0;
        let run_id = create(&args, &source, "", &f.dest, "")
            .await
            .unwrap_or_else(|e| panic!("{history:?} {continuous:?}: {e:#}"));
        assert!(
            racing.collect.lock().unwrap().is_none(),
            "the pass's delete ran"
        );
        assert_eq!(latest(&f.dest, "").await.unwrap().unwrap(), run_id);
        verify(&f.dest, "", &run_id, None).await.unwrap();
        let run = load(&f.dest, "", &run_id).await.unwrap();
        let actor = disk_actor(&run.actors[0]).unwrap();
        assert_eq!(actor["status"], "copied");
        assert_eq!(actor["txid"], 2, "{history:?} {continuous:?}");
        assert_eq!(array(&actor, "gaps").unwrap().len(), 0);
        if history == History::All {
            assert_eq!(actor["collected"], json!([gone[0]]));
        }
    }
}

/// The same race with the key younger than the window (the default hour):
/// no collection by the rule could have deleted it, whatever the shape, so
/// a continuous run refuses it as a gap; the next run, listed after the
/// delete, completes.
#[tokio::test]
async fn a_key_younger_than_the_window_gone_below_a_snapshot_stays_a_gap() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    let at_head = SegmentId {
        generation: 1,
        txid: 2,
    };
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 2, 500).unwrap();
    put(
        &f.source,
        &format!("{prefix}{}", snapshot_key(at_head)),
        bytes.clone(),
        false,
    )
    .await
    .unwrap();
    let first = snapshot_key(SegmentId {
        generation: 1,
        txid: 1,
    });
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: std::sync::Mutex::new(Some((
            Key::from(format!("{prefix}{first}")),
            Key::from("elsewhere/unrelated"),
            bytes,
        ))),
        collect_after: 0,
        fail_collect: false,
    });
    let args = f.args();
    assert_eq!(args.history_retention_secs, 3600);
    assert!(create(&args, &source, "", &f.dest, "").await.is_err());
    let run_id = latest(&f.dest, "").await.unwrap().unwrap();
    let actor = disk_actor(&load(&f.dest, "", &run_id).await.unwrap().actors[0]).unwrap();
    assert_eq!(actor["status"], "failed");
    assert_eq!(actor["gaps"], json!([first]));
    assert_eq!(actor["collected"], json!([]));
    create(&args, &source, "", &f.dest, "").await.unwrap();
}

#[tokio::test]
async fn chunks_stay_below_loader_cap() {
    let dest: Store = Arc::new(object_store::memory::InMemory::new());
    let mut chunks = Vec::new();
    let mut actors = vec![
        json!({"prefix":"large", "objects": {"one": "x".repeat(3 << 20), "two": "y".repeat(3 << 20)}}),
    ];
    flush_chunk(&dest, "runs/test", &mut chunks, &mut actors)
        .await
        .unwrap();
    for chunk in chunks {
        checked_json(
            &dest,
            text(&chunk, "key").unwrap(),
            text(&chunk, "sha256").unwrap(),
        )
        .await
        .unwrap();
    }
}

#[test]
fn effective_store_identity_refuses_prefix_overlap() {
    assert!(
        disjoint(
            ("https://s3.example", "live", "prod//actors"),
            ("https://s3.example", "live", "prod/actors")
        )
        .is_err()
    );
    assert_eq!(
        endpoint_identity("https://S3.EXAMPLE:443/").unwrap(),
        endpoint_identity("https://s3.example").unwrap()
    );
    assert_eq!(
        endpoint_identity("https://s3.amazonaws.com").unwrap(),
        endpoint_identity("https://s3.us-east-1.amazonaws.com/").unwrap()
    );
    for alias in ["AWS_BUCKET", "AWS_BUCKET_NAME", "bucket", "bucket_name"] {
        assert!(
            setting(alias)
                .unwrap_err()
                .to_string()
                .contains("bucket override")
        );
    }
    for prefix in ["", "prod", "prod/archive"] {
        assert!(
            disjoint(
                ("https://s3.example", "live", "prod"),
                ("https://s3.example", "live", prefix)
            )
            .is_err()
        );
    }
    disjoint(
        ("https://s3.example", "live", "prod"),
        ("https://s3.example", "live", "other"),
    )
    .unwrap();
    assert!(
        disjoint(
            ("https://one.example", "live", "prod"),
            ("https://two.example", "live", "prod"),
        )
        .is_err()
    );
}

#[tokio::test]
async fn latest_digest_and_protected_app_membership_are_checked() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let run = load(&f.dest, "", &id).await.unwrap();
    let mut complete = run.complete.clone();
    complete["source"] = json!("s3://another");
    json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    assert!(
        latest(&f.dest, "")
            .await
            .unwrap_err()
            .to_string()
            .contains("digest mismatch")
    );
    // Even if both mutable roots are refreshed, chunk membership cannot be
    // removed by editing just the completion record.
    complete = run.complete;
    complete["actors"] = json!(0);
    complete["chunks"] = json!([]);
    let hash = json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    json_put(&f.dest, "latest", &json!({"run":id,"sha256":hash}), true)
        .await
        .unwrap();
    json_put(
        &f.dest,
        &format!("runs/{id}/root"),
        &json!({"run":id,"sha256":hash}),
        true,
    )
    .await
    .unwrap();
    assert!(
        verify(&f.dest, "", &id, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("membership")
    );
}

#[tokio::test]
async fn a_collected_historical_segment_splits_ranges_without_a_gap() {
    for continuous in [true, false] {
        let mut f = Fixture::new().await;
        f.increment().await;
        let prefix = "state/acme/shop/actors/counter/alice/";
        for txid in 3..=4 {
            f.db.exec(&format!("UPDATE counts SET n={txid}"), &[])
                .unwrap();
            let bytes =
                f.db.take_changes()
                    .unwrap()
                    .unwrap()
                    .to_ltx(txid, txid, txid as i64 * 100)
                    .unwrap();
            let segment = Segment {
                id: SegmentId {
                    generation: 1,
                    txid,
                },
                parent_generation: 1,
            };
            put(
                &f.source,
                &format!("{prefix}{}", segment_key(&segment)),
                bytes,
                false,
            )
            .await
            .unwrap();
        }
        let snapshot = Key::from(format!(
            "{prefix}{}",
            snapshot_key(SegmentId {
                generation: 1,
                txid: 4
            })
        ));
        let bytes = f.db.snapshot().unwrap().to_ltx(1, 4, 500).unwrap();
        put(&f.source, snapshot.as_ref(), bytes.clone(), false)
            .await
            .unwrap();
        let missing = Key::from(format!(
            "{prefix}{}",
            segment_key(&Segment {
                id: SegmentId {
                    generation: 1,
                    txid: 2
                },
                parent_generation: 1
            })
        ));
        let source: Store = Arc::new(ReadOnly {
            inner: f.source.clone(),
            gets: Default::default(),
            collect: std::sync::Mutex::new(Some((missing, snapshot, bytes))),
            collect_after: 0,
            fail_collect: false,
        });
        let mut args = f.args();
        args.require_continuous = Some(continuous);
        args.history_retention_secs = 0;
        // Segment 2 went from below the snapshot at 4 and its segment: a
        // collection (cut short, or its deletes landing one at a time) may
        // leave exactly this. Not a gap, so the continuous run completes;
        // the ranges still record what was archived.
        create(&args, &source, "", &f.dest, "").await.unwrap();
        let id = latest(&f.dest, "").await.unwrap().unwrap();
        let run = load(&f.dest, "", &id).await.unwrap();
        let actor = disk_actor(&run.actors[0]).unwrap();
        assert_eq!(
            actor["ranges"],
            json!([{"generation":1,"start":1,"end":1},{"generation":1,"start":4,"end":4}])
        );
        assert_eq!(array(&actor, "gaps").unwrap().len(), 0);
        assert_eq!(array(&actor, "collected").unwrap().len(), 1);
        verify(&f.dest, "", &id, None).await.unwrap();
    }
}

#[tokio::test]
async fn newer_snapshot_cannot_hide_a_broken_historical_chain() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 2, 300).unwrap();
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
    let mut other = Fixture::new().await;
    other.db.exec("INSERT INTO counts VALUES(99)", &[]).unwrap();
    other.db.take_changes().unwrap();
    let bytes = other.db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
    put(
        &f.source,
        &format!(
            "{prefix}{}",
            snapshot_key(SegmentId {
                generation: 1,
                txid: 1
            })
        ),
        bytes,
        true,
    )
    .await
    .unwrap();
    let copier = Copier {
        source: &f.source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: true,
        retention: std::time::Duration::ZERO,
    };
    let error = copier
        .actor(
            prefix,
            history::list_objects(&f.source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error:#}");
}

#[tokio::test]
async fn rereview_destination_refuses_ambiguous_addressing() {
    let f = Fixture::new().await;
    for settings in [
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true\nAWS_ENDPOINT=https://source.s3.us-east-1.amazonaws.com",
        "AWS_ENDPOINT=https://source.s3.us-east-1.amazonaws.com",
        "AWS_ENDPOINT=https://destination.storage.example.com",
        "AWS_ENDPOINT=https://source.storage.example.com",
        "AWS_ENDPOINT=https://SOURCE.storage.example.com./",
        "AWS_ENDPOINT=https://other.s3.us-east-1.amazonaws.com",
        "AWS_ENDPOINT=https://minio.example.com/bucket",
        "AWS_ENDPOINT=https://minio.example.com/?bucket=source",
        "AWS_ENDPOINT=https://user:password@minio.example.com",
        "AWS_ENDPOINT=https://minio.example.com/#bucket",
        "AWS_ENDPOINT=https://s3.s3.us-east-1.amazonaws.com",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false",
    ] {
        let file = f._dir.path().join("destination.env");
        std::fs::write(&file, format!("AWS_REGION=us-east-1\n{settings}\n")).unwrap();
        let mut args = f.args();
        args.to_env = Some(file);
        assert!(destination(&args).is_err(), "accepted {settings}");
    }
}

#[tokio::test]
async fn destination_accepts_path_style_custom_dns_endpoints() {
    let f = Fixture::new().await;
    for endpoint in [
        "https://account.r2.cloudflarestorage.com",
        "https://s3.ca-east-tor.io.cloud.ovh.net",
        "https://minio.example.com",
        "https://source-backup.example.com",
    ] {
        let file = f._dir.path().join("destination.env");
        std::fs::write(
            &file,
            format!("AWS_REGION=us-east-1\nAWS_ENDPOINT={endpoint}\n"),
        )
        .unwrap();
        let mut args = f.args();
        args.to_env = Some(file.clone());
        destination(&args).unwrap_or_else(|e| panic!("refused {endpoint}: {e:#}"));
        let (_, _, source_endpoint) = configured(&args.bucket, Some(&file), None).unwrap();
        let (_, _, dest_endpoint) = configured(&args.to, Some(&file), Some("source")).unwrap();
        disjoint(
            (&source_endpoint, "source", ""),
            (&dest_endpoint, "destination", ""),
        )
        .unwrap();
        for prefix in ["", "prod", "prod/archive"] {
            assert!(
                disjoint(
                    (&source_endpoint, "source", "prod"),
                    (&dest_endpoint, "source", prefix),
                )
                .is_err()
            );
        }
        disjoint(
            (&source_endpoint, "source", "prod"),
            (&dest_endpoint, "source", "archive"),
        )
        .unwrap();
    }
    assert_eq!(
        endpoint_identity("https://MINIO.example.com.:443/").unwrap(),
        endpoint_identity("https://minio.example.com").unwrap(),
    );
}

#[tokio::test]
async fn rereview_old_run_complete_has_own_digest() {
    let f = Fixture::new().await;
    let first = f.create().await;
    let mut complete = load(&f.dest, "", &first).await.unwrap().complete;
    f.create().await;
    complete["incarnations"] = json!({"acme/other":null});
    json_put(&f.dest, &format!("runs/{first}/complete"), &complete, true)
        .await
        .unwrap();
    assert!(verify(&f.dest, "", &first, None).await.is_err());
}

#[test]
fn rereview_snapshot_without_boundary_is_recoverable() {
    let objects = BTreeMap::from([
        (
            snapshot_key(SegmentId {
                generation: 1,
                txid: 64,
            }),
            json!({}),
        ),
        (
            segment_key(&Segment {
                id: SegmentId {
                    generation: 1,
                    txid: 2,
                },
                parent_generation: 1,
            }),
            json!({}),
        ),
    ]);
    assert_eq!(
        recoverable_ranges(&json!({"objects":objects})).unwrap(),
        vec![json!({"generation":1,"start":64,"end":64})]
    );
}

#[tokio::test]
async fn rereview_actor_memory_budget_is_enforced() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let mut complete = load(&f.dest, "", &id).await.unwrap().complete;
    let path = text(&complete["chunks"][0], "key").unwrap().to_owned();
    let mut actor = disk_actor(&load(&f.dest, "", &id).await.unwrap().actors[0]).unwrap();
    actor["oversize"] = json!("x".repeat(8 * 1024 * 1024));
    let encoded = serde_json::to_string(&actor).unwrap();
    let fragments: Vec<_> = encoded.as_bytes().chunks(1024 * 1024).enumerate().map(|(part, bytes)|
        json!({"part":part,"last":(part+1)*1024*1024>=encoded.len(),"json":std::str::from_utf8(bytes).unwrap()})).collect();
    let hash = json_put(&f.dest, &path, &json!(fragments), true)
        .await
        .unwrap();
    complete["chunks"][0]["sha256"] = json!(hash);
    let hash = json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    json_put(
        &f.dest,
        &format!("runs/{id}/root"),
        &json!({"run":id,"sha256":hash}),
        true,
    )
    .await
    .unwrap();
    let result = load(&f.dest, "", &id).await;
    assert!(result.is_err(), "unbounded actor accepted");
    assert!(result.err().unwrap().to_string().contains("budget"));
}

#[tokio::test]
async fn rereview_retry_observations_include_vanished_non_head() {
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
    put(&f.source, new.as_ref(), bytes.clone(), false)
        .await
        .unwrap();
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: std::sync::Mutex::new(Some((old, new, bytes))),
        collect_after: 0,
        fail_collect: false,
    });
    let missing = segment_key(&Segment {
        id: SegmentId {
            generation: 1,
            txid: 3,
        },
        parent_generation: 1,
    });
    let initial = BTreeMap::from([(
        missing.clone(),
        ObjectMeta {
            location: Key::from(format!("{prefix}{missing}")),
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 1,
            e_tag: None,
            version: None,
        },
    )]);
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: true,
        retention: std::time::Duration::ZERO,
    };
    let actor = copier.actor(prefix, initial, None).await.unwrap();
    // Every key observed by any attempt is accounted for: the first
    // snapshot, gone from below the second and its segment, as collected;
    // the head the first listing named, which nothing explains, as a gap.
    assert_eq!(
        actor["collected"],
        json!([snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })])
    );
    assert_eq!(actor["gaps"], json!([missing]));
}

#[tokio::test]
async fn rereview_spool_and_index_budgets_fail_explicitly() {
    let f = Fixture::new().await;
    let id = f.create().await;
    for (disk, memory, label) in [
        (1, INDEX_MAX, "spool disk"),
        (SPOOL_MAX, 1, "resident index"),
    ] {
        let result = load_with_budget(&f.dest, "", &id, disk, memory).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains(label));
    }
}

#[tokio::test]
async fn rereview_old_two_app_run_cannot_drop_failed_app() {
    let f = Fixture::new().await;
    for (tail, body) in [
        ("current", b"v1 1 3\n".to_vec()),
        ("v1/manifest", b"wasmkeep-manifest 1\n".to_vec()),
    ] {
        put(&f.source, &format!("apps/acme/other/{tail}"), body, false)
            .await
            .unwrap();
    }
    put(
        &f.source,
        &format!(
            "state/acme/other/actors/counter/bob/{}",
            snapshot_key(SegmentId {
                generation: 1,
                txid: 1
            })
        ),
        b"broken".to_vec(),
        false,
    )
    .await
    .unwrap();
    let mut args = f.args();
    args.all = true;
    args.app = None;
    assert!(create(&args, &f.source, "", &f.dest, "").await.is_err());
    let old = latest(&f.dest, "").await.unwrap().unwrap();
    let mut complete = load(&f.dest, "", &old).await.unwrap().complete;
    assert!(create(&args, &f.source, "", &f.dest, "").await.is_err());
    assert_ne!(latest(&f.dest, "").await.unwrap().unwrap(), old);
    let mut retained = Vec::new();
    for chunk in array(&complete, "chunks").unwrap() {
        let value = checked_json(
            &f.dest,
            text(chunk, "key").unwrap(),
            text(chunk, "sha256").unwrap(),
        )
        .await
        .unwrap();
        if text(&value[0], "json")
            .unwrap()
            .contains("state/acme/shop/")
        {
            retained.push(chunk.clone());
        }
    }
    complete["chunks"] = json!(retained);
    complete["apps"]
        .as_array_mut()
        .unwrap()
        .retain(|app| text(app, "key").unwrap().ends_with("/acme/shop"));
    complete["actors"] = json!(1);
    complete["failed"] = json!([]);
    let hash = json_put(&f.dest, &format!("runs/{old}/complete"), &complete, true)
        .await
        .unwrap();
    assert!(
        verify(&f.dest, "", &old, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("digest mismatch")
    );
    json_put(
        &f.dest,
        &format!("runs/{old}/root"),
        &json!({"run":old,"sha256":hash}),
        true,
    )
    .await
    .unwrap();
    assert!(
        verify(&f.dest, "", &old, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("incarnation membership")
    );
    complete["incarnations"]
        .as_object_mut()
        .unwrap()
        .remove("acme/other");
    let hash = json_put(&f.dest, &format!("runs/{old}/complete"), &complete, true)
        .await
        .unwrap();
    json_put(
        &f.dest,
        &format!("runs/{old}/root"),
        &json!({"run":old,"sha256":hash}),
        true,
    )
    .await
    .unwrap();
    assert!(
        verify(&f.dest, "", &old, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("started membership")
    );
}

#[test]
fn fix4_unproven_endpoint_aliases_are_refused() {
    for (a, b) in [
        ("https://one.example", "https://two.example"),
        ("aws", "https://custom.example"),
        ("aws", "aws"),
    ] {
        for tail in ["", "prod", "prod/archive"] {
            let error = disjoint((a, "live", "prod"), (b, "live", tail)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("different bucket name or a non-overlapping prefix")
            );
        }
        disjoint((a, "live", "prod"), (b, "live", "archive")).unwrap();
        disjoint((a, "live", "prod"), (b, "backup", "prod")).unwrap();
    }
    disjoint(("aws", "live", "prod"), ("aws-cn", "live", "prod")).unwrap();
}

#[tokio::test]
async fn fix4_retry_preserves_copied_reference_after_expiry() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    let first = snapshot_key(SegmentId {
        generation: 1,
        txid: 1,
    });
    let new = snapshot_key(SegmentId {
        generation: 1,
        txid: 2,
    });
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect_after: 1,
        fail_collect: true,
        collect: std::sync::Mutex::new(Some((
            Key::from(format!("{prefix}{first}")),
            Key::from(format!("{prefix}{new}")),
            f.db.snapshot().unwrap().to_ltx(1, 2, 200).unwrap(),
        ))),
    });
    let copier = Copier {
        source: &source,
        dest: &f.dest,
        dp: "",
        cache: None,
        all: true,
        retention: std::time::Duration::ZERO,
    };
    let actor = copier
        .actor(prefix, list_bounded(&source, prefix).await.unwrap(), None)
        .await
        .unwrap();
    assert!(
        actor["objects"].get(&first).is_some(),
        "validated copy lost on retry"
    );
    assert!(!array(&actor, "gaps").unwrap().contains(&json!(first)));
    assert!(array(&actor, "gaps").unwrap().is_empty());
}

#[tokio::test]
async fn fix4_range_reads_are_linear() {
    let mut f = Fixture::new().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    for txid in 2..=24 {
        f.db.exec("UPDATE counts SET n=n+1", &[]).unwrap();
        let bytes =
            f.db.take_changes()
                .unwrap()
                .unwrap()
                .to_ltx(txid, txid, txid as i64)
                .unwrap();
        let name = segment_key(&Segment {
            id: SegmentId {
                generation: 1,
                txid,
            },
            parent_generation: 1,
        });
        put(&f.source, &format!("{prefix}{name}"), bytes, false)
            .await
            .unwrap();
    }
    let id = f.create().await;
    let actor = disk_actor(&load(&f.dest, "", &id).await.unwrap().actors[0]).unwrap();
    let counted = Arc::new(ReadOnly {
        inner: f.dest.clone(),
        gets: Default::default(),
        collect: Default::default(),
        collect_after: 0,
        fail_collect: false,
    });
    let store: Store = counted.clone();
    verify_ranges(&store, "", &actor).await.unwrap();
    let reads = counted.gets.load(std::sync::atomic::Ordering::SeqCst);
    assert!(reads <= 50, "24 positions needed {reads} reads");
}

#[tokio::test]
async fn fix4_retry_references_require_current_size_etag_and_version() {
    for field in ["size", "etag", "version"] {
        let f = Fixture::new().await;
        let prefix = "state/acme/shop/actors/counter/alice/";
        let name = snapshot_key(SegmentId {
            generation: 1,
            txid: 1,
        });
        let listed = list_bounded(&f.source, prefix).await.unwrap();
        let meta = &listed[&name];
        let bytes = history::get(&f.source, &meta.location, Some(meta), MAX)
            .await
            .unwrap();
        let mut entry = save_blob(
            &f.dest,
            "",
            meta.location.as_ref(),
            bytes.to_vec(),
            Some(meta),
            None,
        )
        .await
        .unwrap();
        let expected = entry.clone();
        entry[field] = if field == "size" {
            json!(0)
        } else {
            json!("different")
        };
        let mut validated = serde_json::Map::from_iter([(name.clone(), entry)]);
        let counted = Arc::new(ReadOnly {
            inner: f.source.clone(),
            gets: Default::default(),
            collect: Default::default(),
            collect_after: 0,
            fail_collect: false,
        });
        let source: Store = counted.clone();
        let copier = Copier {
            source: &source,
            dest: &f.dest,
            dp: "",
            cache: None,
            all: true,
            retention: std::time::Duration::ZERO,
        };
        let actor = copier
            .attempt(prefix, &listed, None, &mut validated)
            .await
            .unwrap();
        assert!(
            counted.gets.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "reused changed {field}"
        );
        assert_eq!(actor["objects"][&name][field], expected[field]);
    }
}

#[tokio::test]
async fn fix4_range_verification_follows_generation_branches() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let prefix = "state/acme/shop/actors/counter/alice/";
    // Two children of the same retained base, one after an ownership change.
    let original = segment_key(&Segment {
        id: SegmentId {
            generation: 1,
            txid: 2,
        },
        parent_generation: 1,
    });
    let branch = segment_key(&Segment {
        id: SegmentId {
            generation: 2,
            txid: 2,
        },
        parent_generation: 1,
    });
    let bytes = history::get(
        &f.source,
        &Key::from(format!("{prefix}{original}")),
        None,
        MAX,
    )
    .await
    .unwrap();
    put(
        &f.source,
        &format!("{prefix}{branch}"),
        bytes.to_vec(),
        false,
    )
    .await
    .unwrap();
    let id = f.create().await;
    let actor = disk_actor(&load(&f.dest, "", &id).await.unwrap().actors[0]).unwrap();
    assert_eq!(
        actor["ranges"],
        json!([{"generation":1,"start":1,"end":2},{"generation":2,"start":2,"end":2}])
    );
    verify_ranges(&f.dest, "", &actor).await.unwrap();
}

fn restore_args(id: &str, in_place: bool) -> Restore {
    Restore {
        resume: None,
        from: "s3://destination".into(),
        run: Some(id.into()),
        app: ("acme".into(), "shop".into()),
        bucket: "s3://target".into(),
        in_place,
        abort: false,
        deploy: None,
        force: false,
        writer: None,
    }
}

#[tokio::test]
async fn restore_empty_refuses_nonempty_and_tombstone() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(&target, "state/acme/shop/unknown", b"keep".to_vec(), false)
        .await
        .unwrap();
    assert!(
        restore::execute(
            &restore_args(&id, false),
            &f.dest,
            "",
            &target,
            "",
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    assert_eq!(
        read(&target, "state/acme/shop/unknown", MAX).await.unwrap(),
        b"keep"
    );
    target
        .delete(&Key::from("state/acme/shop/unknown"))
        .await
        .unwrap();
    put(
        &target,
        "apps/acme/shop/deleted",
        b"deleted".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(
        restore::execute(
            &restore_args(&id, true),
            &f.dest,
            "",
            &target,
            "",
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    assert!(
        optional(&target, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn restore_replays_head_seeds_generation_and_rebuilds_alarm() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let result = history::replay(
        &target,
        "state/acme/shop/actors/counter/alice/",
        &tmp.path().join("actor.db"),
        MAX,
    )
    .await
    .unwrap();
    assert_eq!(result.position.txid, 2);
    assert_eq!(
        wasmkeep_cluster::node_lease::decode_marker(
            &read(&target, "state/acme/shop/leases/counter/alice", MAX)
                .await
                .unwrap()
        ),
        Some(1)
    );
    assert!(
        !list_bounded(&target, "state/acme/shop/alarms/")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        optional(&target, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        optional(&target, "apps/acme/shop/restored")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn restore_in_place_keeps_old_only_actor_generation_and_undo() {
    let f = Fixture::new().await;
    let id = f.create().await;
    put(
        &f.source,
        "state/acme/shop/leases/counter/absent",
        wasmkeep_cluster::node_lease::encode_marker(42),
        false,
    )
    .await
    .unwrap();
    restore::execute(
        &restore_args(&id, true),
        &f.dest,
        "",
        &f.source,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(
        wasmkeep_cluster::node_lease::decode_marker(
            &read(&f.source, "state/acme/shop/leases/counter/absent", MAX)
                .await
                .unwrap()
        ),
        Some(43)
    );
    assert!(
        list_bounded(&f.dest, "before-restore/")
            .await
            .unwrap()
            .len()
            == 1
    );
}

#[derive(Debug)]
struct FailMutation {
    inner: Store,
    count: Arc<std::sync::atomic::AtomicUsize>,
    fail: usize,
    lost: bool,
    stray: bool,
}
impl std::fmt::Display for FailMutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("restore fault injection")
    }
}
fn mutation(count: &std::sync::atomic::AtomicUsize, fail: usize) -> object_store::Result<()> {
    if count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == fail {
        Err(object_store::Error::Generic {
            store: "test",
            source: "injected mutation failure".into(),
        })
    } else {
        Ok(())
    }
}
#[async_trait::async_trait]
impl ObjectStore for FailMutation {
    async fn put_opts(
        &self,
        key: &Key,
        payload: object_store::PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if !self.lost {
            mutation(&self.count, self.fail)?;
        }
        let result = self.inner.put_opts(key, payload, opts).await;
        if self.stray && key.as_ref().starts_with("state/acme/shop/leases/") {
            let original = list_bounded(&self.inner, "state/acme/shop/actors/counter/alice/")
                .await
                .unwrap();
            let bytes = read(
                &self.inner,
                original.values().next().unwrap().location.as_ref(),
                MAX,
            )
            .await
            .unwrap();
            put(
                &self.inner,
                &format!(
                    "state/acme/shop/actors/counter/alice/{}",
                    snapshot_key(SegmentId {
                        generation: 99,
                        txid: 1
                    })
                ),
                bytes,
                true,
            )
            .await
            .unwrap();
        }
        if self.lost {
            mutation(&self.count, self.fail)?;
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        unreachable!()
    }
    async fn get_opts(
        &self,
        key: &Key,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(key, opts).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn delete_stream(
        &self,
        keys: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        let inner = self.inner.clone();
        let count = self.count.clone();
        let fail = self.fail;
        let lost = self.lost;
        keys.then(move |key| {
            let inner = inner.clone();
            let count = count.clone();
            async move {
                let key = key?;
                if !lost {
                    mutation(&count, fail)?;
                }
                inner.delete(&key).await?;
                if lost {
                    mutation(&count, fail)?;
                }
                Ok(key)
            }
        })
        .boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        _: &Key,
        _: &Key,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn restore_failure_at_each_mutation_can_resume_without_losing_generation_floor() {
    resume_at_each_mutation(false).await;
}

/// The same at the floor 0.10 (ADR 0045, section 3): whatever write fails,
/// the resumed restore leaves the absent actor's seed in the app's
/// generation floor, and no marker.
#[tokio::test]
async fn a_restore_at_the_floor_0_10_resumes_at_each_mutation_with_the_generation_floor() {
    resume_at_each_mutation(true).await;
}

async fn resume_at_each_mutation(ten: bool) {
    let mut failures = 0;
    for (lost, fail) in [false, true]
        .into_iter()
        .flat_map(|lost| (0..100).map(move |fail| (lost, fail)))
    {
        let f = Fixture::new().await;
        put(
            &f.source,
            "apps/acme/shop/secrets/current",
            b"0123456789abcdef".to_vec(),
            false,
        )
        .await
        .unwrap();
        put(
            &f.source,
            "apps/acme/shop/secrets/0123456789abcdef",
            b"sealed".to_vec(),
            false,
        )
        .await
        .unwrap();
        if ten {
            set_floor(&f.source, TEN).await;
        }
        let id = f.create().await;
        put(
            &f.source,
            "state/acme/shop/leases/counter/absent",
            wasmkeep_cluster::node_lease::encode_marker(42),
            false,
        )
        .await
        .unwrap();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let target: Store = Arc::new(FailMutation {
            inner: f.source.clone(),
            count: count.clone(),
            fail,
            lost,
            stray: false,
        });
        let archive: Store = Arc::new(FailMutation {
            inner: f.dest.clone(),
            count: count.clone(),
            fail,
            lost,
            stray: false,
        });
        let mut args = restore_args(&id, true);
        args.deploy = Some(DeployChoice::Backup);
        if ten {
            args.writer = Some(TEN);
        }
        let result =
            restore::execute(&args, &archive, "", &target, "", std::time::Duration::ZERO).await;
        let total = count.load(std::sync::atomic::Ordering::SeqCst);
        if result.is_err() {
            failures += 1;
            // Undo publication also uses the destination's prune lock.
            f.dest.delete(&Key::from("prune-lock")).await.unwrap();
            // Clearing an orphaned exclusive lock is the documented recovery
            // after a failure to release it. Never remove the fence/journal.
            if optional(&f.source, "apps/acme/shop/restore-lock")
                .await
                .unwrap()
                .is_some()
            {
                f.source
                    .delete(&Key::from("apps/acme/shop/restore-lock"))
                    .await
                    .unwrap();
            }
            if optional(&f.source, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_some()
                || optional(&f.source, "apps/acme/shop/restored")
                    .await
                    .unwrap()
                    .is_none()
            {
                restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
                    .await
                    .unwrap_or_else(|e| panic!("failure {fail}: {e:#}"));
            }
        }
        if ten {
            let floor = generation_floor_at(&f.source).await;
            assert_eq!(
                floor.map(|f| (f.generation, f.markers)),
                Some((43, wasmkeep_cluster::generation_floor::Markers::None)),
                "failure {fail}"
            );
            assert!(
                list_bounded(&f.source, "state/acme/shop/leases/")
                    .await
                    .unwrap()
                    .is_empty(),
                "failure {fail}"
            );
        } else {
            assert_eq!(
                wasmkeep_cluster::node_lease::decode_marker(
                    &read(&f.source, "state/acme/shop/leases/counter/absent", MAX)
                        .await
                        .unwrap()
                ),
                Some(43),
                "failure {fail}"
            );
        }
        assert!(
            optional(&f.source, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_none(),
            "failure {fail}"
        );
        let _ = total;
    }
    assert!(failures >= 15, "did not exercise all phases: {failures}");
}

#[tokio::test]
async fn restore_corruption_is_refused_before_target_writes() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let run = load(&f.dest, "", &id).await.unwrap();
    let actor = disk_actor(&run.actors[0]).unwrap();
    let object = actor["objects"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    put(
        &f.dest,
        &format!("data/{}", text(object, "sha256").unwrap()),
        b"corrupt".to_vec(),
        true,
    )
    .await
    .unwrap();
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    assert!(
        restore::execute(
            &restore_args(&id, false),
            &f.dest,
            "",
            &target,
            "",
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    assert_eq!(
        target
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn restore_refuses_live_nodes_even_with_force_and_offline_old_versions_without_it() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    for (stamp, force) in [(now_ms().unwrap() / 1000 * 1000, true), (1, false)] {
        let body = format!("wasmkeep-node 2\nold\nhttp://127.0.0.1:9\n0.4.2\n{stamp}\nacme/shop\n");
        put(&target, "nodes/old", body.into_bytes(), true)
            .await
            .unwrap();
        let mut args = restore_args(&id, false);
        args.force = force;
        assert!(
            restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
                .await
                .is_err()
        );
        assert!(
            optional(&target, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn restore_preserves_named_alarm_and_adds_committed_time() {
    for entry in [
        wasmkeep_cluster::alarm_index::Entry::legacy(111),
        wasmkeep_cluster::alarm_index::Entry::owned(111, 1, "abc").unwrap(),
    ] {
        let mut f = Fixture::new().await;
        f.db.set_meta(
            "alarm_indexed",
            &wasmkeep_cluster::alarm_index::to_meta(Some(&entry)),
        )
        .unwrap();
        f.increment().await;
        let id = f.create().await;
        let target: Store = Arc::new(object_store::memory::InMemory::new());
        restore::execute(
            &restore_args(&id, false),
            &f.dest,
            "",
            &target,
            "",
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        let alarms = list_bounded(&target, "state/acme/shop/alarms/")
            .await
            .unwrap();
        assert_eq!(alarms.len(), 2);
        assert!(
            alarms.contains_key(&wasmkeep_cluster::alarm_index::key_tail(
                "counter", "alice", &entry
            ))
        );
        assert!(alarms.keys().any(|k| {
            wasmkeep_cluster::alarm_index::parse_listed(k)
                .unwrap()
                .entry
                .at
                == 123456
        }));
    }
}

#[tokio::test]
async fn restore_deploy_choices_and_before_restore_run_are_usable() {
    let mut f = Fixture::new().await;
    let id = f.create().await;
    f.increment().await;
    put(
        &f.source,
        "apps/acme/shop/v2/manifest",
        b"wasmkeep-manifest 1\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    put(
        &f.source,
        "apps/acme/shop/current",
        b"v2 2 3\n".to_vec(),
        true,
    )
    .await
    .unwrap();
    restore::execute(
        &restore_args(&id, true),
        &f.dest,
        "",
        &f.source,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(
        read(&f.source, "apps/acme/shop/current", MAX)
            .await
            .unwrap(),
        b"v2 2 3\n"
    );
    let labels = list_bounded(&f.dest, "before-restore/").await.unwrap();
    let undo = String::from_utf8(
        read(
            &f.dest,
            labels.values().next().unwrap().location.as_ref(),
            MAX,
        )
        .await
        .unwrap(),
    )
    .unwrap();
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&undo, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let result = history::replay(
        &target,
        "state/acme/shop/actors/counter/alice/",
        &tmp.path().join("undo.db"),
        MAX,
    )
    .await
    .unwrap();
    assert_eq!(result.position.txid, 2);
    let mut args = restore_args(&id, true);
    args.deploy = Some(DeployChoice::Backup);
    restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        read(&f.source, "apps/acme/shop/current", MAX)
            .await
            .unwrap(),
        b"v1 1 3\n"
    );
}

#[tokio::test]
async fn restore_alarm_recovers_hashed_name_from_host_address() {
    let mut f = Fixture::new().await;
    let name = "z".repeat(250);
    let encoded = wasmkeep_storage::encode_segment(&name);
    f.db.set_meta("address", &format!("acme/counter/{name}"))
        .unwrap();
    f.db.take_changes().unwrap();
    let bytes = f.db.snapshot().unwrap().to_ltx(1, 1, 100).unwrap();
    let root = "state/acme/shop/actors/counter/";
    for meta in list_bounded(&f.source, root).await.unwrap().values() {
        f.source.delete(&meta.location).await.unwrap();
    }
    put(
        &f.source,
        &format!(
            "{root}{encoded}/{}",
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
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let alarms = list_bounded(&target, "state/acme/shop/alarms/")
        .await
        .unwrap();
    assert_eq!(
        wasmkeep_cluster::alarm_index::parse_listed(alarms.keys().next().unwrap())
            .unwrap()
            .name,
        name
    );
}

#[tokio::test]
async fn review1_backup_refuses_restore_fence_and_changed_marker() {
    let f = Fixture::new().await;
    put(
        &f.source,
        "apps/acme/shop/restoring",
        b"busy".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
    assert!(latest(&f.dest, "").await.unwrap().is_none());
    f.source
        .delete(&Key::from("apps/acme/shop/restoring"))
        .await
        .unwrap();
    let before = incarnation(&f.source, "apps/acme/shop").await.unwrap();
    put(&f.source, "apps/acme/shop/restored", b"new".to_vec(), false)
        .await
        .unwrap();
    assert_ne!(
        before,
        incarnation(&f.source, "apps/acme/shop").await.unwrap()
    );
}

#[tokio::test]
async fn review2_force_allows_partial_undo() {
    let f = Fixture::new().await;
    let id = f.create().await;
    put(
        &f.source,
        "state/acme/shop/actors/counter/broken/snapshot/0000000000000001-0000000000000001.ltx",
        b"corrupt".to_vec(),
        false,
    )
    .await
    .unwrap();
    let mut args = restore_args(&id, true);
    assert!(
        restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    args.force = true;
    restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
        .await
        .unwrap();
}

#[tokio::test]
async fn review3_pinned_undo_recovers_copy_conflict() {
    let f = Fixture::new().await;
    let id = f.create().await;
    put(
        &f.source,
        "apps/acme/shop/v1/manifest",
        b"wasmkeep-manifest 1".to_vec(),
        true,
    )
    .await
    .unwrap();
    let mut args = restore_args(&id, true);
    args.deploy = Some(DeployChoice::Backup);
    assert!(
        restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    let journal: Value = serde_json::from_slice(
        &read(&f.source, "apps/acme/shop/restoring", META_MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(journal["phase"], "copy");
    args.run = Some(text(&journal, "before_restore").unwrap().into());
    let read_only: Store = Arc::new(ReadOnly {
        inner: f.dest.clone(),
        gets: 0.into(),
        collect: std::sync::Mutex::new(None),
        collect_after: usize::MAX,
        fail_collect: false,
    });
    restore::execute(
        &args,
        &read_only,
        "",
        &f.source,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        optional(&f.source, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn review5_delete_and_recreate_refuse_restore() {
    let f = Fixture::new().await;
    put(
        &f.source,
        "apps/acme/shop/restoring",
        b"busy".to_vec(),
        false,
    )
    .await
    .unwrap();
    let app = wasmkeep_server::apps::AppRef::new(f.source.clone(), "", "acme", "shop");
    assert!(app.recreate(std::time::Duration::ZERO).await.is_err());
    let args = wasmkeep_server::apps::DeleteApp {
        resume: None,
        wait: std::time::Duration::ZERO,
        force: true,
        who: "test".into(),
        late: std::time::Duration::ZERO,
    };
    assert!(
        wasmkeep_server::apps::delete_app(
            f.source.clone(),
            "",
            "acme",
            "shop",
            &args,
            &mut |_| {},
            std::time::Duration::ZERO,
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    assert!(
        optional(&f.source, "apps/acme/shop/deleted")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        optional(&f.source, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn review6_capability_not_version_metadata() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(
        &target,
        "nodes/old",
        b"wasmkeep-node 2\nold\nhttp://127.0.0.1:9\n0.4.2\n1\nacme/shop\n\n\n\nsecrets\nrestore\n"
            .to_vec(),
        false,
    )
    .await
    .unwrap();
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn review8_deploy_check_reads_each_fence_once() {
    let dir = tempfile::tempdir().unwrap();
    let counted = Arc::new(ReadOnly {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gets: 0.into(),
        collect: std::sync::Mutex::new(None),
        collect_after: usize::MAX,
        fail_collect: false,
    });
    let store: Store = counted.clone();
    let app = wasmkeep_server::apps::AppRef::new(store, "", "acme", "shop");
    let service = wasmkeep_server::ActorService::new(
        wasmkeep_runtime::Runtime::new(&Default::default()).unwrap(),
        wasmkeep_server::ActorConfig::new(dir.path()),
        vec![],
    )
    .unwrap();
    assert!(!app.update(&service).await.unwrap());
    assert_eq!(counted.gets.load(std::sync::atomic::Ordering::SeqCst), 5);
}

#[tokio::test]
async fn review4_stray_higher_generation_keeps_fence() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let inner: Store = Arc::new(object_store::memory::InMemory::new());
    let target: Store = Arc::new(FailMutation {
        inner: inner.clone(),
        count: Arc::new(0.into()),
        fail: usize::MAX,
        lost: false,
        stray: true,
    });
    assert!(
        restore::execute(
            &restore_args(&id, false),
            &f.dest,
            "",
            &target,
            "",
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    assert!(
        optional(&inner, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn review7_fault_applies_before_losing_response() {
    let inner: Store = Arc::new(object_store::memory::InMemory::new());
    let store: Store = Arc::new(FailMutation {
        inner: inner.clone(),
        count: Arc::new(0.into()),
        fail: 0,
        lost: true,
        stray: false,
    });
    assert!(
        put(&store, "key", b"applied".to_vec(), false)
            .await
            .is_err()
    );
    assert_eq!(
        optional(&inner, "key").await.unwrap(),
        Some(b"applied".to_vec())
    );
}

#[tokio::test]
async fn review3_abort_prepare_without_archive_access() {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: Restore,
    }
    let cli = Cli::try_parse_from([
        "test",
        "--from",
        "s3://archive",
        "--bucket",
        "s3://target",
        "--app",
        "acme/shop",
        "--abort",
    ]);
    assert!(
        cli.is_ok(),
        "prepare abort must be a supported CLI operation"
    );
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let archive: Store = Arc::new(object_store::memory::InMemory::new());
    json_put(
        &target,
        "apps/acme/shop/restoring",
        &json!({"phase":"prepare", "run":"unreadable"}),
        false,
    )
    .await
    .unwrap();
    restore::execute(
        &cli.unwrap().args,
        &archive,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        optional(&target, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn review7_nonzero_late_wait_and_two_restores() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let start = tokio::time::Instant::now();
    let late = wasmkeep_server::apps::LATE_WRITES;
    restore::execute(&restore_args(&id, false), &f.dest, "", &target, "", late)
        .await
        .unwrap();
    // No node of the app ever wrote here and every write was answered:
    // neither the quiet wait nor the lock's settle has anything to wait for.
    assert!(
        start.elapsed() < std::time::Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    let old = optional(&target, "apps/acme/shop/restored").await.unwrap();
    restore::execute(&restore_args(&id, true), &f.dest, "", &target, "", late)
        .await
        .unwrap();
    assert_ne!(
        old,
        optional(&target, "apps/acme/shop/restored").await.unwrap()
    );
}

/// A heartbeat of `acme/shop`'s node `id`, in `state`, last written live at
/// `written_at_ms`.
fn app_node_lease(
    id: &str,
    state: wasmkeep_cluster::node_lease::NodeState,
    written_at_ms: u64,
) -> Vec<u8> {
    node_lease_of("acme/shop", id, state, written_at_ms)
}

/// [`app_node_lease`] for a node of `app` (empty: one naming no app).
fn node_lease_of(
    app: &str,
    id: &str,
    state: wasmkeep_cluster::node_lease::NodeState,
    written_at_ms: u64,
) -> Vec<u8> {
    use wasmkeep_cluster::membership::NodeInfo;
    let info = NodeInfo {
        id: id.into(),
        address: "http://n:8080".into(),
        version: "0.0.0".into(),
        written_at_ms,
        app: app.into(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain: None,
        reads_secrets: true,
        can_take: true,
        can_front: true,
        can_handoff: true,
        can_end: true,
        can_flush: true,
        can_rate_limits: true,
        can_assets: true,
        can_blobs: true,
        can_restore: true,
        can_actor_restore: true,
        instance: None,
        lease: None,
        load: None,
        can_recorded_head: true,
        can_multi_app: true,
        can_place: false,
        can_apart: false,
        can_genfloor: false,
        can_trace: false,
        process: None,
    };
    wasmkeep_cluster::node_lease::NodeLeaseObject {
        state,
        instance: Some("i1".into()),
        written_at_ms,
    }
    .encode_with(&info.payload())
}

/// A store whose listing stamps each `nodes/` object with the time it was
/// written on the test's (paused) clock, `age` earlier: the bucket's clock
/// moves with the test's, and a heartbeat may be one it stored long ago.
#[derive(Debug)]
struct AgedNodes {
    inner: Store,
    age: chrono::Duration,
    start: tokio::time::Instant,
    wall: chrono::DateTime<chrono::Utc>,
    writes: std::sync::Mutex<BTreeMap<String, tokio::time::Instant>>,
}
impl AgedNodes {
    fn store(age: chrono::Duration) -> Store {
        Arc::new(Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            age,
            start: tokio::time::Instant::now(),
            wall: chrono::Utc::now(),
            writes: Default::default(),
        })
    }
}
impl std::fmt::Display for AgedNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("aged heartbeats")
    }
}
#[async_trait::async_trait]
impl ObjectStore for AgedNodes {
    async fn put_opts(
        &self,
        key: &Key,
        payload: object_store::PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let result = self.inner.put_opts(key, payload, opts).await;
        if result.is_ok() && key.as_ref().starts_with("nodes/") {
            self.writes
                .lock()
                .unwrap()
                .insert(key.as_ref().to_owned(), tokio::time::Instant::now());
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        key: &Key,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    async fn get_opts(
        &self,
        key: &Key,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(key, opts).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let stamps: BTreeMap<String, chrono::DateTime<chrono::Utc>> = self
            .writes
            .lock()
            .unwrap()
            .iter()
            .map(|(k, at)| {
                let since = chrono::Duration::from_std(*at - self.start).unwrap();
                (k.clone(), self.wall + since - self.age)
            })
            .collect();
        self.inner
            .list(prefix)
            .map(move |meta| {
                meta.map(|mut meta| {
                    if let Some(at) = stamps.get(meta.location.as_ref()) {
                        meta.last_modified = *at;
                    }
                    meta
                })
            })
            .boxed()
    }
    fn delete_stream(
        &self,
        keys: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        self.inner.delete_stream(keys)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Key,
        to: &Key,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Restores `id` into `target` and returns how long it took.
async fn timed_restore(f: &Fixture, id: &str, target: &Store) -> std::time::Duration {
    let start = tokio::time::Instant::now();
    restore::execute(
        &restore_args(id, false),
        &f.dest,
        "",
        target,
        "",
        wasmkeep_server::apps::LATE_WRITES,
    )
    .await
    .unwrap();
    start.elapsed()
}

#[tokio::test(start_paused = true)]
async fn restore_waits_for_a_stopped_nodes_last_write_not_for_the_fence() {
    use wasmkeep_cluster::node_lease::NodeState;
    let late = wasmkeep_server::apps::LATE_WRITES;
    let f = Fixture::new().await;
    let id = f.create().await;
    let now = u64::try_from(super::now_ms().unwrap()).unwrap();
    // A node of the app stopped moments ago: a write it sent may still land.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(
        &target,
        "nodes/n1",
        app_node_lease("n1", NodeState::Released, now),
        false,
    )
    .await
    .unwrap();
    let waited = timed_restore(&f, &id, &target).await;
    assert!(
        waited >= late - std::time::Duration::from_secs(1) && waited < late * 2,
        "{waited:?}"
    );
    // The bucket stored its last write long ago: no wait, whatever the
    // node's clock wrote in it.
    let target = AgedNodes::store(chrono::Duration::hours(1));
    put(
        &target,
        "nodes/n1",
        app_node_lease("n1", NodeState::Fenced, now),
        false,
    )
    .await
    .unwrap();
    let waited = timed_restore(&f, &id, &target).await;
    assert!(waited < std::time::Duration::from_secs(1), "{waited:?}");
    // A heartbeat this version cannot read proves nothing: the fence's time
    // counts again, as before 0.7.0.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(&target, "nodes/n2", b"wasmkeep-node 9\n".to_vec(), false)
        .await
        .unwrap();
    let waited = timed_restore(&f, &id, &target).await;
    assert!(
        waited >= late - std::time::Duration::from_secs(1),
        "{waited:?}"
    );
}

/// The node's clock is 45 s behind the operator's: the `written_at` it
/// wrote looks older than the bound, but the bucket stored the write just
/// now. The quiet wait is measured on the bucket's clock.
#[tokio::test(start_paused = true)]
async fn restore_measures_a_stopped_nodes_lease_on_the_buckets_clock() {
    use wasmkeep_cluster::node_lease::NodeState;
    let late = wasmkeep_server::apps::LATE_WRITES;
    let f = Fixture::new().await;
    let id = f.create().await;
    let behind = u64::try_from(super::now_ms().unwrap()).unwrap() - 45_000;
    for state in [NodeState::Released, NodeState::Fenced, NodeState::Live] {
        let target: Store = Arc::new(object_store::memory::InMemory::new());
        put(
            &target,
            "nodes/n1",
            app_node_lease("n1", state, behind),
            false,
        )
        .await
        .unwrap();
        let waited = timed_restore(&f, &id, &target).await;
        assert!(
            waited >= late - std::time::Duration::from_secs(1),
            "{state:?}: {waited:?}"
        );
    }
}

/// A heartbeat that names no app (format 1 serves `<tenant>/default`) cannot
/// show which app it serves: the listing proves nothing.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_naming_no_app_makes_the_listing_prove_nothing() {
    use wasmkeep_cluster::node_lease::NodeState;
    let late = wasmkeep_server::apps::LATE_WRITES;
    let f = Fixture::new().await;
    let id = f.create().await;
    let target = AgedNodes::store(chrono::Duration::hours(1));
    let now = u64::try_from(super::now_ms().unwrap()).unwrap();
    put(
        &target,
        "nodes/n1",
        node_lease_of("", "n1", NodeState::Released, now - 3_600_000),
        false,
    )
    .await
    .unwrap();
    let waited = timed_restore(&f, &id, &target).await;
    assert!(
        waited >= late - std::time::Duration::from_secs(1),
        "{waited:?}"
    );
}

/// The mark `cluster forget` leaves at `nodes/<id>` is not a node: unlike
/// an object that does not decode, it does not make the listing prove
/// nothing, and the restore does not wait for it.
#[tokio::test(start_paused = true)]
async fn a_forget_mark_is_no_node_and_does_not_force_the_long_quiet_wait() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let now = u64::try_from(super::now_ms().unwrap()).unwrap();
    let marked = AgedNodes::store(chrono::Duration::hours(1));
    put(
        &marked,
        "nodes/n1",
        wasmkeep_cluster::node_lease::encode_forgotten(now - 3_600_000),
        false,
    )
    .await
    .unwrap();
    let waited = timed_restore(&f, &id, &marked).await;
    assert!(waited < std::time::Duration::from_secs(1), "{waited:?}");
    // Anything else that does not decode still does.
    let junk = AgedNodes::store(chrono::Duration::hours(1));
    put(
        &junk,
        "nodes/n1",
        b"wasmkeep-node-forgotten 2\n1\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    let waited = timed_restore(&f, &id, &junk).await;
    assert!(
        waited >= wasmkeep_server::apps::LATE_WRITES - std::time::Duration::from_secs(1),
        "{waited:?}"
    );
}

/// A node of the app that keeps writing its lease through the quiet wait
/// (here unconditionally, over the restore's fence) is still acting: the
/// restore waits again from its newest write, then refuses.
#[tokio::test(start_paused = true)]
async fn a_node_still_writing_its_lease_through_the_quiet_wait_is_refused() {
    use wasmkeep_cluster::node_lease::NodeState;
    let f = Fixture::new().await;
    let id = f.create().await;
    let target = AgedNodes::store(chrono::Duration::zero());
    // Its clock is a minute behind: by the operator's it looks stale.
    let behind = || u64::try_from(super::now_ms().unwrap()).unwrap() - 60_000;
    put(
        &target,
        "nodes/n1",
        app_node_lease("n1", NodeState::Live, behind()),
        false,
    )
    .await
    .unwrap();
    let writer = {
        let target = target.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                put(
                    &target,
                    "nodes/n1",
                    app_node_lease("n1", NodeState::Live, behind()),
                    true,
                )
                .await
                .unwrap();
            }
        })
    };
    let result = restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        wasmkeep_server::apps::LATE_WRITES,
    )
    .await;
    writer.abort();
    let err = result.unwrap_err().to_string();
    assert!(err.contains("still writes its node lease"), "{err}");
    assert!(
        list_bounded(&target, "state/").await.unwrap().is_empty(),
        "state written while a node acts"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_answered_only_after_it_landed_keeps_the_lock_although_the_restore_succeeded() {
    let late = wasmkeep_server::apps::LATE_WRITES;
    let f = Fixture::new().await;
    let id = f.create().await;
    let run = async |fail: usize| {
        let empty: Store = Arc::new(object_store::memory::InMemory::new());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Every mutation lands; the answer to mutation `fail` is lost.
        let target: Store = Arc::new(FailMutation {
            inner: empty.clone(),
            count: count.clone(),
            fail,
            lost: true,
            stray: false,
        });
        let start = tokio::time::Instant::now();
        let result =
            restore::execute(&restore_args(&id, false), &f.dest, "", &target, "", late).await;
        let lock = optional(&empty, "apps/acme/shop/restore-lock")
            .await
            .unwrap()
            .unwrap();
        (
            result,
            start.elapsed(),
            count.load(std::sync::atomic::Ordering::SeqCst),
            lock,
        )
    };
    let (result, waited, total, _) = run(usize::MAX).await;
    result.unwrap();
    assert!(waited < std::time::Duration::from_secs(1), "{waited:?}");
    assert!(total > 10, "{total}");
    let mut recovered = 0;
    // Mutation 0 is the lock's own write and the last its release: neither
    // has a lock to hold afterwards. Every one in between does.
    for fail in 1..total - 1 {
        let (result, waited, _, lock) = run(fail).await;
        assert!(
            waited >= late - std::time::Duration::from_secs(1),
            "mutation {fail}: {waited:?} ({result:?})"
        );
        assert!(lock.starts_with(b"unlocked "), "mutation {fail}");
        // Recovered: a create that finds the same bytes is not an error.
        if result.is_ok() {
            recovered += 1;
        }
    }
    assert!(recovered > 0, "no restore recovered from a lost answer");
}

#[tokio::test]
async fn review1_backup_changed_restore_marker_refuses_publication() {
    let f = Fixture::new().await;
    let old = f.create().await;
    put(&f.source, "apps/acme/shop/restored", b"old".to_vec(), false)
        .await
        .unwrap();
    put(&f.source, "race-trigger", b"trigger".to_vec(), false)
        .await
        .unwrap();
    let source: Store = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: 0.into(),
        collect_after: 0,
        fail_collect: false,
        collect: std::sync::Mutex::new(Some((
            Key::from("race-trigger"),
            Key::from("apps/acme/shop/restored"),
            b"new".to_vec(),
        ))),
    });
    assert!(create(&f.args(), &source, "", &f.dest, "").await.is_err());
    assert_eq!(latest(&f.dest, "").await.unwrap(), Some(old));
}

#[tokio::test]
async fn review7_prepare_local_to_app_check_gap_refuses_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let store: Store = Arc::new(object_store::memory::InMemory::new());
    let app = wasmkeep_server::apps::AppRef::new(store.clone(), "", "acme", "shop");
    app.prepare_local(dir.path()).await.unwrap();
    let service = wasmkeep_server::ActorService::new(
        wasmkeep_runtime::Runtime::new(&Default::default()).unwrap(),
        wasmkeep_server::ActorConfig::new(dir.path()),
        vec![],
    )
    .unwrap();
    put(
        &store,
        "apps/acme/shop/restored",
        b"new incarnation".to_vec(),
        false,
    )
    .await
    .unwrap();
    assert!(app.update(&service).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn review7_concurrent_restore_is_refused_during_quiet_window() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let args = restore_args(&id, false);
    let late = wasmkeep_server::apps::LATE_WRITES;
    let (first, second) = tokio::join!(
        restore::execute(&args, &f.dest, "", &target, "", late),
        restore::execute(&args, &f.dest, "", &target, "", late)
    );
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "exactly one restore may hold the lock"
    );
}

#[tokio::test]
async fn review2_partial_pinned_undo_recovers_only_covered_actors() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let broken = format!(
        "state/acme/shop/actors/counter/broken/{}",
        snapshot_key(SegmentId {
            generation: 1,
            txid: 1
        })
    );
    put(&f.source, &broken, b"corrupt".to_vec(), false)
        .await
        .unwrap();
    let target: Store = Arc::new(FailMutation {
        inner: f.source.clone(),
        count: Arc::new(0.into()),
        fail: usize::MAX,
        lost: false,
        stray: true,
    });
    let mut args = restore_args(&id, true);
    args.force = true;
    assert!(
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    let journal: Value = serde_json::from_slice(
        &read(&f.source, "apps/acme/shop/restoring", META_MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(journal["phase"], "copy");
    args.run = Some(text(&journal, "before_restore").unwrap().into());
    args.deploy = Some(DeployChoice::Backup);
    args.force = false;
    assert!(
        restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    args.force = true;
    restore::execute(&args, &f.dest, "", &f.source, "", std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(optional(&f.source, &broken).await.unwrap().is_none());
    assert!(
        optional(&f.source, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn restore_abort_empty_root_every_phase_without_archive() {
    for phase in ["prepare", "clear", "copy", "abort"] {
        for fail in [usize::MAX, 1, 3] {
            let target: Store = Arc::new(object_store::memory::InMemory::new());
            let archive: Store = Arc::new(object_store::memory::InMemory::new());
            json_put(
                &target,
                "apps/acme/shop/restoring",
                &json!({
                    "phase":phase, "run":"lost", "binding":{"in_place":false, "deploy":"Backup"}
                }),
                false,
            )
            .await
            .unwrap();
            for path in [
                "state/acme/shop/actors/counter/a/snapshot",
                "state/acme/shop/leases/counter/a",
                "state/acme/shop/alarms/a",
                "apps/acme/shop/current",
                "apps/acme/shop/v1/manifest",
                "apps/acme/shop/secrets/current",
                "apps/acme/shop/restored",
                "state/acme/shop2/keep",
                "apps/acme/shop2/current",
                "apps/acme/shop/recreated",
            ] {
                put(&target, path, b"keep-or-clean".to_vec(), false)
                    .await
                    .unwrap();
            }
            let faulty: Store = Arc::new(FailMutation {
                inner: target.clone(),
                count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                fail,
                lost: false,
                stray: false,
            });
            let mut args = restore_args("lost", false);
            args.run = None;
            args.abort = true;
            let started = tokio::time::Instant::now();
            let result = restore::execute(
                &args,
                &archive,
                "",
                &faulty,
                "",
                std::time::Duration::from_secs(40),
            )
            .await;
            if fail != usize::MAX {
                assert!(result.is_err());
                assert!(
                    optional(&target, "apps/acme/shop/restoring")
                        .await
                        .unwrap()
                        .is_some(),
                    "cleanup failure must retain fence"
                );
                restore::execute(
                    &args,
                    &archive,
                    "",
                    &target,
                    "",
                    std::time::Duration::from_secs(40),
                )
                .await
                .unwrap();
            } else {
                result.unwrap();
            }
            assert!(started.elapsed() >= std::time::Duration::from_secs(80));
            assert!(
                list_bounded(&target, "state/acme/shop/")
                    .await
                    .unwrap()
                    .is_empty()
            );
            for path in [
                "apps/acme/shop/current",
                "apps/acme/shop/v1/manifest",
                "apps/acme/shop/secrets/current",
                "apps/acme/shop/restoring",
            ] {
                assert!(
                    optional(&target, path).await.unwrap().is_none(),
                    "{phase}: {path}"
                );
            }
            for path in [
                "apps/acme/shop/restored",
                "apps/acme/shop/recreated",
                "state/acme/shop2/keep",
                "apps/acme/shop2/current",
            ] {
                assert!(
                    optional(&target, path).await.unwrap().is_some(),
                    "{phase}: {path}"
                );
            }
        }
    }
}

#[tokio::test]
async fn restore_abort_in_place_after_prepare_refuses_even_without_flag() {
    for phase in ["clear", "copy"] {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        json_put(
            &store,
            "apps/acme/shop/restoring",
            &json!({"phase":phase, "binding":{"in_place":true}}),
            false,
        )
        .await
        .unwrap();
        let mut args = restore_args("lost", false);
        args.abort = true;
        let err = restore::execute(&args, &store, "", &store, "", std::time::Duration::ZERO)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("pinned before_restore"), "{err}");
        assert!(
            optional(&store, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn restore_abort_checks_live_nodes_after_quiet_and_excludes_competing_restore() {
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    json_put(
        &target,
        "apps/acme/shop/restoring",
        &json!({"phase":"copy", "binding":{"in_place":false,"deploy":"Backup"}}),
        false,
    )
    .await
    .unwrap();
    put(
        &target,
        "state/acme/shop/partial",
        b"retain".to_vec(),
        false,
    )
    .await
    .unwrap();
    let mut args = restore_args("lost", false);
    args.abort = true;
    args.force = true;
    let late = std::time::Duration::from_secs(40);
    let intruder = async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let err = restore::execute(&args, &target, "", &target, "", late)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("locked"), "{err}");
        let stamp = now_ms().unwrap();
        put(
            &target,
            "nodes/live",
            format!("wasmkeep-node 2\nlive\nhttp://127.0.0.1:9\n0.4.2\n{stamp}\nacme/shop\n")
                .into_bytes(),
            false,
        )
        .await
        .unwrap();
    };
    let (result, ()) = tokio::join!(
        restore::execute(&args, &target, "", &target, "", late),
        intruder
    );
    assert!(result.unwrap_err().to_string().contains("live nodes"));
    assert!(
        optional(&target, "state/acme/shop/partial")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        optional(&target, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn restore_abort_failed_copy_with_stray_history_and_lost_archive() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let faulty: Store = Arc::new(FailMutation {
        inner: target.clone(),
        count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        fail: usize::MAX,
        lost: false,
        stray: true,
    });
    let mut args = restore_args(&id, false);
    let err = restore::execute(&args, &f.dest, "", &faulty, "", std::time::Duration::ZERO)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unexpected head"), "{err}");
    let journal: Value = serde_json::from_slice(
        &read(&target, "apps/acme/shop/restoring", META_MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(journal["phase"], "copy");
    let missing: Store = Arc::new(object_store::memory::InMemory::new());
    args.abort = true;
    args.run = None;
    restore::execute(&args, &missing, "", &target, "", std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(
        list_bounded(&target, "state/acme/shop/")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        optional(&target, "apps/acme/shop/current")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        optional(&target, "apps/acme/shop/restoring")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn actor_restore_from_backup_replays_selected_position_and_rejects_tampering() {
    let mut f = Fixture::new().await;
    f.increment().await;
    let id = f.create().await;
    let output = f._dir.path().join("actor-restore.db");
    let (export, run) = export_actor_from_store(
        &f.dest,
        "",
        Some(&id),
        "acme",
        "shop",
        "counter",
        "alice",
        history::Point::Txid(1),
        &output,
    )
    .await
    .unwrap();
    assert_eq!(run, id);
    assert_eq!(export.position.txid, 1);
    assert_eq!(&std::fs::read(&output).unwrap()[18..20], &[2, 2]);
    let loaded = load(&f.dest, "", &id).await.unwrap();
    let actor = disk_actor(&loaded.actors[0]).unwrap();
    let object = actor["objects"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    let digest = text(object, "sha256").unwrap();
    put(
        &f.dest,
        &format!("data/{digest}"),
        b"corrupt".to_vec(),
        true,
    )
    .await
    .unwrap();
    let refused = f._dir.path().join("refused.db");
    assert!(
        export_actor_from_store(
            &f.dest,
            "",
            Some(&id),
            "acme",
            "shop",
            "counter",
            "alice",
            history::Point::Head,
            &refused
        )
        .await
        .is_err()
    );
    assert!(!refused.exists());
}

fn prune_time() -> u128 {
    now_ms().unwrap() + 25 * 60 * 60 * 1000
}

#[tokio::test]
async fn prune_retention_latest_shared_objects_and_destination_only() {
    let f = Fixture::new().await;
    let first = f.create().await;
    let second = f.create().await;
    let before = list_bounded(&f.source, "").await.unwrap();
    // Keep latest even if it is not the lexicographically newest run.
    let root = read(&f.dest, &format!("runs/{first}/root"), META_MAX)
        .await
        .unwrap();
    put(&f.dest, "latest", root, true).await.unwrap();
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    assert!(load(&f.dest, "", &first).await.is_ok());
    assert!(load(&f.dest, "", &second).await.is_err());
    verify(&f.dest, "", &first, None).await.unwrap();
    assert_eq!(before, list_bounded(&f.source, "").await.unwrap());
}

#[tokio::test]
async fn prune_pins_undo_and_restoring_runs() {
    let f = Fixture::new().await;
    let undo = f.create().await;
    let restoring = f.create().await;
    let latest = f.create().await;
    put(
        &f.dest,
        "before-restore/nonce",
        undo.as_bytes().to_vec(),
        false,
    )
    .await
    .unwrap();
    put(
        &f.dest,
        &format!("restoring/{restoring}"),
        restoring.as_bytes().to_vec(),
        false,
    )
    .await
    .unwrap();
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    for id in [undo, restoring, latest] {
        verify(&f.dest, "", &id, None).await.unwrap();
    }
}

#[tokio::test]
async fn prune_refuses_running_create_and_collects_abandoned_runs() {
    let f = Fixture::new().await;
    f.create().await;
    json_put(
        &f.dest,
        "runs/abandoned/started",
        &json!({"started_ms":now_ms().unwrap()}),
        false,
    )
    .await
    .unwrap();
    assert!(
        prune::execute(&f.dest, "", 0, now_ms().unwrap())
            .await
            .is_err()
    );
    assert!(
        optional(&f.dest, "runs/abandoned/started")
            .await
            .unwrap()
            .is_some()
    );
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    assert!(
        optional(&f.dest, "runs/abandoned/started")
            .await
            .unwrap()
            .is_none()
    );
    // Finished markers must allow a subsequent create.
    f.create().await;
}

#[tokio::test]
async fn prune_age_floor_and_newest_n() {
    let f = Fixture::new().await;
    let a = f.create().await;
    let b = f.create().await;
    let c = f.create().await;
    let orphan = format!("data/{}", digest(b"orphan"));
    put(&f.dest, &orphan, b"orphan".to_vec(), false)
        .await
        .unwrap();
    prune::execute(&f.dest, "", 0, now_ms().unwrap())
        .await
        .unwrap();
    assert!(optional(&f.dest, &orphan).await.unwrap().is_some());
    for id in [&a, &b, &c] {
        assert!(load(&f.dest, "", id).await.is_ok());
    }
    prune::execute(&f.dest, "", 2, prune_time()).await.unwrap();
    let mut ids = [a, b, c.clone()];
    ids.sort();
    for id in &ids[1..] {
        assert!(load(&f.dest, "", id).await.is_ok());
    }
    if ids[0] != c {
        assert!(load(&f.dest, "", &ids[0]).await.is_err());
    }
    assert!(optional(&f.dest, &orphan).await.unwrap().is_none());
}

#[tokio::test]
async fn prune_interruption_each_mutation_and_lost_response() {
    for lost in [false, true] {
        let mut failures = 0;
        for fail in 0..100 {
            let f = Fixture::new().await;
            let old = f.create().await;
            let latest = f.create().await;
            put(&f.dest, "data/orphan", b"orphan".to_vec(), false)
                .await
                .unwrap();
            let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let faulty: Store = Arc::new(FailMutation {
                inner: f.dest.clone(),
                count: count.clone(),
                fail,
                lost,
                stray: false,
            });
            let result = prune::execute(&faulty, "", 0, prune_time()).await;
            let mutations = count.load(std::sync::atomic::Ordering::SeqCst);
            let exhausted = fail >= mutations;
            if exhausted {
                result.unwrap();
            } else {
                // An uncertain PUT can recover by reading back identical
                // bytes (same_or_create); pre-write failures must propagate.
                if !lost {
                    assert!(result.is_err(), "mutation {fail} must report failure");
                }
                failures += 1;
            }
            // Simulate recovery after stopping the crashed process and draining its I/O.
            f.dest.delete(&Key::from("prune-lock")).await.unwrap();
            prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
            assert!(load(&f.dest, "", &old).await.is_err());
            verify(&f.dest, "", &latest, None).await.unwrap();
            if exhausted {
                break;
            }
            assert!(fail < 99, "mutation sweep must reach a fault-free pass");
        }
        assert!(failures > 0, "mutation sweep must inject failures");
    }
}

#[tokio::test]
async fn prune_invalid_kept_manifest_fails_before_deleting() {
    let f = Fixture::new().await;
    let old = f.create().await;
    let kept = f.create().await;
    put(
        &f.dest,
        &format!("runs/{kept}/complete"),
        b"{}".to_vec(),
        true,
    )
    .await
    .unwrap();
    assert!(prune::execute(&f.dest, "", 0, prune_time()).await.is_err());
    assert!(load(&f.dest, "", &old).await.is_ok());
}

#[derive(Debug)]
struct PrunePause {
    inner: Store,
    at: &'static str,
    paused: std::sync::atomic::AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl std::fmt::Display for PrunePause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("prune interleaving")
    }
}
#[async_trait::async_trait]
impl ObjectStore for PrunePause {
    async fn put_opts(
        &self,
        path: &Key,
        payload: object_store::PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let result = self.inner.put_opts(path, payload, opts).await?;
        let matches = path.as_ref().starts_with(self.at) || path.as_ref().ends_with(self.at);
        if matches && !self.paused.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        unreachable!()
    }
    async fn get_opts(
        &self,
        path: &Key,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        // pin() has completed its first marker snapshot when it asks for
        // the pin. Pause before that GET, not before marker publication.
        if self.at == "@pin-snapshot"
            && path.as_ref().starts_with("restoring/")
            && !self.paused.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
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
        keys: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        if self.at != "@delete" || self.paused.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return self.inner.delete_stream(keys);
        }
        let inner = self.inner.clone();
        let entered = self.entered.clone();
        let release = self.release.clone();
        use futures_util::StreamExt;
        keys.then(move |key| {
            let inner = inner.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let key = key?;
                // The request has left the client: dropping its future does
                // not cancel the remote DELETE. Explicitly drain this task
                // before recovery clears the lock.
                let task = tokio::spawn(async move {
                    entered.notify_one();
                    release.notified().await;
                    inner.delete(&key).await?;
                    Ok(key)
                });
                task.await.unwrap()
            }
        })
        .boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Key,
        to: &Key,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn prune_concurrent_create_both_start_orders() {
    for at_started in [true, false] {
        let f = Fixture::new().await;
        let latest = f.create().await;
        let pause = Arc::new(PrunePause {
            inner: f.dest.clone(),
            at: if at_started { "/started" } else { "prunes/" },
            paused: false.into(),
            entered: Default::default(),
            release: Default::default(),
        });
        let store: Store = pause.clone();
        if at_started {
            let args = f.args();
            let creating = create(&args, &f.source, "", &store, "");
            let racing = async {
                pause.entered.notified().await;
                assert!(
                    prune::execute(&f.dest, "", 0, now_ms().unwrap())
                        .await
                        .is_err()
                );
                pause.release.notify_one();
            };
            let (result, ()) = tokio::join!(creating, racing);
            // The marker snapshot precedes started: even a fully finished
            // refused prune must invalidate this create (the ABA case).
            assert!(result.is_err());
        } else {
            let pruning = prune::execute(&store, "", 0, now_ms().unwrap());
            let racing = async {
                pause.entered.notified().await;
                assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
                pause.release.notify_one();
            };
            let (result, ()) = tokio::join!(pruning, racing);
            result.unwrap();
        }
        verify(&f.dest, "", &latest, None).await.unwrap();
    }
}

#[tokio::test]
async fn prune_removes_unshared_content_but_keeps_shared_app_objects() {
    let f = Fixture::new().await;
    let old = f.create().await;
    put(
        &f.source,
        "apps/acme/shop/current",
        b"v2 1 3\n".to_vec(),
        true,
    )
    .await
    .unwrap();
    put(
        &f.source,
        "apps/acme/shop/v2/manifest",
        b"wasmkeep-manifest 1\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    let kept = f.create().await;
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    assert!(load(&f.dest, "", &old).await.is_err());
    assert!(
        optional(&f.dest, &format!("data/{}", digest(b"v1 1 3\n")))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        optional(
            &f.dest,
            &format!("data/{}", digest(b"wasmkeep-manifest 1\n"))
        )
        .await
        .unwrap()
        .is_some()
    );
    verify(&f.dest, "", &kept, None).await.unwrap();
}

#[tokio::test]
async fn prune_protects_run_of_real_interrupted_restore_fence() {
    let f = Fixture::new().await;
    let restoring = f.create().await;
    f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let fault: Store = Arc::new(FailMutation {
        inner: target.clone(),
        count: Arc::new(0.into()),
        fail: 3,
        lost: false,
        stray: false,
    });
    assert!(
        restore::execute(
            &restore_args(&restoring, false),
            &f.dest,
            "",
            &fault,
            "",
            std::time::Duration::ZERO
        )
        .await
        .is_err()
    );
    let bytes = optional(&target, "apps/acme/shop/restoring")
        .await
        .unwrap()
        .expect("restore must have reached its fence");
    let journal: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(journal["run"], restoring);
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    verify(&f.dest, "", &restoring, None).await.unwrap();
}

#[tokio::test]
async fn prune_excludes_other_pruners_and_restore_pin_during_scan() {
    let f = Fixture::new().await;
    let id = f.create().await;
    let pause = Arc::new(PrunePause {
        inner: f.dest.clone(),
        at: "prunes/",
        paused: false.into(),
        entered: Default::default(),
        release: Default::default(),
    });
    let store: Store = pause.clone();
    let pruning = prune::execute(&store, "", 0, now_ms().unwrap());
    let racing = async {
        pause.entered.notified().await;
        assert!(
            prune::execute(&f.dest, "", 0, now_ms().unwrap())
                .await
                .is_err()
        );
        assert!(prune::pin(&f.dest, "", &id).await.is_err());
        assert!(
            optional(&f.dest, &format!("restoring/{id}"))
                .await
                .unwrap()
                .is_none()
        );
        pause.release.notify_one();
    };
    let (result, ()) = tokio::join!(pruning, racing);
    result.unwrap();
    prune::pin(&f.dest, "", &id).await.unwrap();
}

#[tokio::test]
async fn prune_prefix_isolation() {
    let f = Fixture::new().await;
    let outside = f.create().await;
    let old = create(&f.args(), &f.source, "", &f.dest, "archive")
        .await
        .unwrap();
    let kept = create(&f.args(), &f.source, "", &f.dest, "archive")
        .await
        .unwrap();
    prune::execute(&f.dest, "archive", 0, prune_time())
        .await
        .unwrap();
    assert!(load(&f.dest, "archive", &old).await.is_err());
    verify(&f.dest, "archive", &kept, None).await.unwrap();
    verify(&f.dest, "", &outside, None).await.unwrap();
}

#[tokio::test]
async fn prune_cannot_enter_between_create_complete_and_latest() {
    let f = Fixture::new().await;
    let previous = f.create().await;
    let pause = Arc::new(PrunePause {
        inner: f.dest.clone(),
        at: "/complete",
        paused: false.into(),
        entered: Default::default(),
        release: Default::default(),
    });
    let store: Store = pause.clone();
    let args = f.args();
    let creating = create(&args, &f.source, "", &store, "");
    let racing = async {
        pause.entered.notified().await;
        assert_eq!(latest(&f.dest, "").await.unwrap(), Some(previous));
        // Even treating all data as old cannot prune the unpublished run.
        let error = prune::execute(&f.dest, "", 0, prune_time())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("prune locked"));
        pause.release.notify_one();
    };
    let (result, ()) = tokio::join!(creating, racing);
    let id = result.unwrap();
    assert_eq!(latest(&f.dest, "").await.unwrap(), Some(id.clone()));
    verify(&f.dest, "", &id, None).await.unwrap();
}

#[tokio::test]
async fn prune_exact_age_boundary_preserves_then_collects() {
    let f = Fixture::new().await;
    f.create().await;
    put(&f.dest, "runs/abandoned/started", b"{}".to_vec(), false)
        .await
        .unwrap();
    let stamp = f
        .dest
        .head(&Key::from("runs/abandoned/started"))
        .await
        .unwrap()
        .last_modified
        .timestamp_millis() as u128;
    let boundary = stamp + 24 * 60 * 60 * 1000;
    assert!(prune::execute(&f.dest, "", 0, boundary).await.is_err());
    prune::execute(&f.dest, "", 0, boundary + 1).await.unwrap();
    assert!(
        optional(&f.dest, "runs/abandoned/started")
            .await
            .unwrap()
            .is_none()
    );

    put(&f.dest, "data/orphan", b"orphan".to_vec(), false)
        .await
        .unwrap();
    let stamp = f
        .dest
        .head(&Key::from("data/orphan"))
        .await
        .unwrap()
        .last_modified
        .timestamp_millis() as u128;
    let boundary = stamp + 24 * 60 * 60 * 1000;
    prune::execute(&f.dest, "", 0, boundary).await.unwrap();
    assert!(optional(&f.dest, "data/orphan").await.unwrap().is_some());
    prune::execute(&f.dest, "", 0, boundary + 1).await.unwrap();
    assert!(optional(&f.dest, "data/orphan").await.unwrap().is_none());
}

#[tokio::test]
async fn actor_groups_does_not_repoll_an_exhausted_s3_listing() {
    let fixture = Fixture::new().await;
    let source: Store = Arc::new(ReadOnly {
        inner: fixture.source,
        gets: Default::default(),
        collect: Default::default(),
        collect_after: 0,
        fail_collect: false,
    });
    let groups: Vec<_> = actor_groups(&source, "state/acme/shop/actors/")
        .try_collect()
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].0, "state/acme/shop/actors/counter/alice/");
    assert_eq!(groups[0].1.len(), 1);
}

/// Unlike an injected error, cancellation never reaches execute's finish
/// block. Test a landed marker and an accepted but still pending DELETE.
#[tokio::test]
async fn prune_dropped_future_leaves_barrier_until_late_delete_is_drained() {
    for at in ["prunes/", "@delete"] {
        let f = Fixture::new().await;
        let old = f.create().await;
        let latest = f.create().await;
        let pause = Arc::new(PrunePause {
            inner: f.dest.clone(),
            at,
            paused: false.into(),
            entered: Default::default(),
            release: Default::default(),
        });
        let store: Store = pause.clone();
        let mut pruning = Box::pin(prune::execute(&store, "", 0, prune_time()));
        tokio::select! {
            biased;
            () = pause.entered.notified() => {},
            result = &mut pruning => panic!("prune completed before pause: {result:?}"),
        }
        drop(pruning);
        assert!(prune::markers(&f.dest, "").await.is_err());
        assert!(prune::execute(&f.dest, "", 0, prune_time()).await.is_err());
        assert!(prune::pin(&f.dest, "", &latest).await.is_err());
        assert!(create(&f.args(), &f.source, "", &f.dest, "").await.is_err());
        if at == "@delete" {
            // The first DELETE targets old/complete. It has not landed yet.
            let complete = format!("runs/{old}/complete");
            assert!(optional(&f.dest, &complete).await.unwrap().is_some());
            pause.release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while optional(&f.dest, &complete).await.unwrap().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        verify(&f.dest, "", &latest, None).await.unwrap();
        // Model the documented operator recovery only after remote I/O drains.
        f.dest.delete(&Key::from("prune-lock")).await.unwrap();
        prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
        assert!(load(&f.dest, "", &old).await.is_err());
        verify(&f.dest, "", &latest, None).await.unwrap();
        prune::markers(&f.dest, "").await.unwrap();
    }
}

#[tokio::test]
async fn pin_paused_after_first_marker_snapshot_detects_a_whole_prune() {
    let f = Fixture::new().await;
    let old = f.create().await;
    let latest = f.create().await;
    let pause = Arc::new(PrunePause {
        inner: f.dest.clone(),
        at: "@pin-snapshot",
        paused: false.into(),
        entered: Default::default(),
        release: Default::default(),
    });
    let store: Store = pause.clone();
    let pinning = prune::pin(&store, "", &old);
    let interleave = async {
        pause.entered.notified().await;
        prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
        assert!(load(&f.dest, "", &old).await.is_err());
        pause.release.notify_one();
    };
    let (result, ()) = tokio::join!(pinning, interleave);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("prune raced restore pin")
    );
    assert!(
        optional(&f.dest, &format!("restoring/{old}"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        load(&f.dest, "", &old).await.is_err(),
        "failed pin cannot resurrect a run"
    );
    verify(&f.dest, "", &latest, None).await.unwrap();
}

#[tokio::test]
async fn prune_waits_for_delayed_delete_before_finalizing() {
    let f = Fixture::new().await;
    let old = f.create().await;
    let latest = f.create().await;
    let pause = Arc::new(PrunePause {
        inner: f.dest.clone(),
        at: "@delete",
        paused: false.into(),
        entered: Default::default(),
        release: Default::default(),
    });
    let store: Store = pause.clone();
    let pruning = prune::execute(&store, "", 0, prune_time());
    let interleave = async {
        pause.entered.notified().await;
        assert!(
            optional(&f.dest, &format!("runs/{old}/complete"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(prune::markers(&f.dest, "").await.is_err());
        assert!(prune::pin(&f.dest, "", &old).await.is_err());
        assert!(prune::execute(&f.dest, "", 0, prune_time()).await.is_err());
        verify(&f.dest, "", &latest, None).await.unwrap();
        pause.release.notify_one();
    };
    let (result, ()) = tokio::join!(pruning, interleave);
    result.unwrap();
    prune::markers(&f.dest, "").await.unwrap();
    assert!(load(&f.dest, "", &old).await.is_err());
    verify(&f.dest, "", &latest, None).await.unwrap();
}

#[tokio::test]
async fn stable_id_backup_restore_timeout_finish_recreate_resume() {
    let f = Fixture::new().await;
    let run = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    // A node of the app stopped just now: the quiet wait holds the restore
    // past the timeout below.
    put(
        &target,
        "nodes/n1",
        app_node_lease(
            "n1",
            wasmkeep_cluster::node_lease::NodeState::Released,
            u64::try_from(super::now_ms().unwrap()).unwrap(),
        ),
        false,
    )
    .await
    .unwrap();
    let mut args = restore_args(&run, false);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(30),
            restore::execute(
                &args,
                &f.dest,
                "",
                &target,
                "",
                std::time::Duration::from_secs(10)
            )
        )
        .await
        .is_err()
    );
    let journal: Value = serde_json::from_slice(
        &read(&target, "apps/acme/shop/restoring", META_MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let id = text(&journal, "nonce").unwrap().to_owned();
    args.resume = Some("deadbeefdeadbeef".into());
    assert!(
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    assert_eq!(
        serde_json::from_slice::<Value>(
            &read(&target, "apps/acme/shop/restoring", META_MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        journal
    );
    args.resume = Some(id.clone());
    // Simulate operator recovery of the stopped CLI's exclusive lock.
    put(
        &target,
        "apps/acme/shop/restore-lock",
        b"unlocked recovery".to_vec(),
        true,
    )
    .await
    .unwrap();
    restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
        .await
        .unwrap();
    for (_, meta) in list_bounded(&target, "state/acme/shop/").await.unwrap() {
        target.delete(&meta.location).await.unwrap();
    }
    put(
        &target,
        "state/acme/shop/new-incarnation",
        b"keep".to_vec(),
        false,
    )
    .await
    .unwrap();
    args.resume = Some(id);
    for _ in 0..2 {
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            read(&target, "state/acme/shop/new-incarnation", MAX)
                .await
                .unwrap(),
            b"keep"
        );
        assert!(
            optional(&target, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_none()
        );
    }
    put(
        &target,
        "apps/acme/shop/restored",
        b"different nonce".to_vec(),
        true,
    )
    .await
    .unwrap();
    assert!(
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    assert_eq!(
        read(&target, "state/acme/shop/new-incarnation", MAX)
            .await
            .unwrap(),
        b"keep"
    );
}

#[tokio::test]
async fn backup_restore_unknown_resume_files_nothing() {
    let f = Fixture::new().await;
    let run = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let mut args = restore_args(&run, false);
    args.resume = Some("0123456789abcdef".into());
    assert!(
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO)
            .await
            .is_err()
    );
    assert!(list_bounded(&target, "").await.unwrap().is_empty());
}

// Files and assets (ADR 0030, section 5, "Backup and restore of a whole
// app"; slice B6).

/// The source's epoch: neither marker written.
fn fresh_epoch() -> String {
    wasmkeep_server::blobs::epoch(None, None)
}
fn alice_ns(incarnation: &str) -> String {
    format!("counter/{}/{incarnation}", digest(b"alice"))
}
async fn put_file(store: &Store, epoch: &str, ns: &str, name: &str, bytes: &[u8], ctype: &str) {
    let mut attributes = object_store::Attributes::new();
    attributes.insert(
        object_store::Attribute::ContentType,
        ctype.to_owned().into(),
    );
    store
        .put_opts(
            &Key::parse(format!("state/acme/shop/blobs/{epoch}/{ns}/{name}")).unwrap(),
            bytes.to_vec().into(),
            PutOptions {
                attributes,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}
async fn file_at(store: &Store, path: &str) -> Option<(Vec<u8>, Option<String>)> {
    let got = match store
        .get_opts(&Key::parse(path).unwrap(), Default::default())
        .await
    {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return None,
        Err(e) => panic!("{e}"),
    };
    let ctype = got
        .attributes
        .get(&object_store::Attribute::ContentType)
        .map(|v| v.as_ref().to_owned());
    Some((got.bytes().await.unwrap().to_vec(), ctype))
}
/// A second version with one asset, made current.
async fn deploy_assets(source: &Store) -> (String, String) {
    let content = b"<h1>hello</h1>".to_vec();
    let content_digest = digest(&content);
    let index = format!(
        "wasmkeep-assets 1\nnot-found front\nfile index.html text/html identity:{content_digest}:{}\n",
        content.len()
    );
    let parsed = wasmkeep_server::assets::Index::parse(index.as_bytes()).unwrap();
    assert_eq!(parsed.bytes(), index.as_bytes());
    let index_digest = digest(index.as_bytes());
    put(
        source,
        &format!("apps/acme/shop/_assets/index/{index_digest}"),
        index.into_bytes(),
        false,
    )
    .await
    .unwrap();
    put(
        source,
        &format!("apps/acme/shop/_assets/content/{content_digest}"),
        content,
        false,
    )
    .await
    .unwrap();
    // Markers a deploy leaves: never copied.
    put(
        source,
        "apps/acme/shop/_assets/deploying/x",
        b"m".to_vec(),
        false,
    )
    .await
    .unwrap();
    put(
        source,
        "apps/acme/shop/v2/manifest",
        format!("wasmkeep-manifest 7\nprevious v1\nassets {index_digest}\n").into_bytes(),
        false,
    )
    .await
    .unwrap();
    put(source, "apps/acme/shop/current", b"v2 2 3\n".to_vec(), true)
        .await
        .unwrap();
    (index_digest, content_digest)
}
async fn restored_epoch(target: &Store) -> String {
    let recreated = optional(target, "apps/acme/shop/recreated").await.unwrap();
    let restored = optional(target, "apps/acme/shop/restored").await.unwrap();
    wasmkeep_server::blobs::epoch(recreated.as_deref(), restored.as_deref())
}

#[tokio::test]
async fn files_and_assets_come_back_under_the_new_epoch() {
    let f = Fixture::new().await;
    let epoch = fresh_epoch();
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "photos/a.jpg",
        b"jpeg",
        "image/jpeg",
    )
    .await;
    put_file(
        &f.source,
        &epoch,
        "_front/main/0",
        "logo.png",
        b"png",
        "image/png",
    )
    .await;
    // Another incarnation of alice (deleted after her copy), and an actor
    // the run holds no database for: neither comes back.
    put_file(
        &f.source,
        &epoch,
        &alice_ns("00000000deadbeef"),
        "old",
        b"old",
        "text/plain",
    )
    .await;
    let bob = format!("counter/{}/0", digest(b"bob"));
    put_file(&f.source, &epoch, &bob, "b", b"bob", "text/plain").await;
    // Cluster state that is never copied.
    put(
        &f.source,
        &format!("state/acme/shop/blob-sweeps/counter/{}/0", digest(b"alice")),
        b"wasmkeep-blob-sweep 1\nswept\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    put(
        &f.source,
        "state/acme/shop/blob-usage",
        b"1".to_vec(),
        false,
    )
    .await
    .unwrap();
    let (index_digest, content_digest) = deploy_assets(&f.source).await;
    let id = f.create().await;
    verify(&f.dest, "", &id, None).await.unwrap();
    let run = load(&f.dest, "", &id).await.unwrap();
    assert_eq!(
        disk_actor(&run.actors[0]).unwrap()["blob_namespace"],
        json!(alice_ns("0"))
    );
    assert_eq!(run.apps[0]["blob_epoch"], json!(epoch));
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let new = restored_epoch(&target).await;
    assert_ne!(new, epoch);
    let root = format!("state/acme/shop/blobs/{new}");
    assert_eq!(
        file_at(&target, &format!("{root}/{}/photos/a.jpg", alice_ns("0"))).await,
        Some((b"jpeg".to_vec(), Some("image/jpeg".into())))
    );
    assert_eq!(
        file_at(&target, &format!("{root}/_front/main/0/logo.png")).await,
        Some((b"png".to_vec(), Some("image/png".into())))
    );
    let written: Vec<_> = list_bounded(&target, "state/acme/shop/")
        .await
        .unwrap()
        .into_keys()
        .collect();
    assert!(
        !written
            .iter()
            .any(|k| k.contains("deadbeef") || k.contains(&digest(b"bob"))),
        "{written:?}"
    );
    assert!(
        !written
            .iter()
            .any(|k| k.starts_with("blob-sweeps") || k == "blob-usage"),
        "{written:?}"
    );
    assert!(
        !written
            .iter()
            .any(|k| k.starts_with(&format!("blobs/{epoch}"))),
        "{written:?}"
    );
    assert_eq!(
        read(
            &target,
            &format!("apps/acme/shop/_assets/content/{content_digest}"),
            MAX
        )
        .await
        .unwrap(),
        b"<h1>hello</h1>"
    );
    assert!(
        optional(
            &target,
            &format!("apps/acme/shop/_assets/index/{index_digest}")
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        optional(&target, "apps/acme/shop/_assets/deploying/x")
            .await
            .unwrap()
            .is_none()
    );
    // Nor did the archive take any of it.
    for object in array(&run.apps[0], "objects").unwrap() {
        let k = text(object, "key").unwrap();
        assert!(!k.contains("deploying") && !k.contains("blob-"), "{k}");
    }
}

#[tokio::test]
async fn a_second_run_reads_no_unchanged_file_and_verify_needs_every_file() {
    use std::sync::atomic::Ordering;
    let f = Fixture::new().await;
    let epoch = fresh_epoch();
    put_file(&f.source, &epoch, &alice_ns("0"), "a", b"one", "text/plain").await;
    let readonly = Arc::new(ReadOnly {
        inner: f.source.clone(),
        gets: Default::default(),
        collect: Default::default(),
        collect_after: usize::MAX,
        fail_collect: false,
    });
    let source: Store = readonly.clone();
    create(&f.args(), &source, "", &f.dest, "").await.unwrap();
    let first = latest(&f.dest, "").await.unwrap().unwrap();
    // Count only GETs under blobs/ in the second run.
    let counting = Arc::new(CountBlobGets {
        inner: f.source.clone(),
        gets: Default::default(),
        late_change: None,
    });
    let source: Store = counting.clone();
    create(&f.args(), &source, "", &f.dest, "").await.unwrap();
    assert_eq!(
        counting.gets.load(Ordering::SeqCst),
        0,
        "an unchanged file was read again"
    );
    let second = latest(&f.dest, "").await.unwrap().unwrap();
    assert_ne!(first, second);
    verify(&f.dest, "", &second, None).await.unwrap();
    f.dest
        .delete(&Key::from(format!("data/{}", digest(b"one"))))
        .await
        .unwrap();
    assert!(
        verify(&f.dest, "", &second, None).await.is_err(),
        "verify passed without the file's object"
    );
}

#[tokio::test]
async fn a_large_file_goes_through_the_spool() {
    let f = Fixture::new().await;
    let epoch = fresh_epoch();
    let big: Vec<u8> = (0..20u32 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "big.bin",
        &big,
        "application/octet-stream",
    )
    .await;
    let id = f.create().await;
    let stored = read(&f.dest, &format!("data/{}", digest(&big)), MAX)
        .await
        .unwrap();
    assert_eq!(stored.len(), big.len());
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let new = restored_epoch(&target).await;
    let (bytes, _) = file_at(
        &target,
        &format!("state/acme/shop/blobs/{new}/{}/big.bin", alice_ns("0")),
    )
    .await
    .unwrap();
    assert_eq!(digest(&bytes), digest(&big));
}

#[tokio::test]
async fn prune_keeps_a_named_file_and_drops_an_unnamed_one() {
    let f = Fixture::new().await;
    let epoch = fresh_epoch();
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "kept",
        b"kept",
        "text/plain",
    )
    .await;
    let first = f.create().await;
    f.source
        .delete(
            &Key::parse(format!(
                "state/acme/shop/blobs/{epoch}/{}/kept",
                alice_ns("0")
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "new",
        b"new",
        "text/plain",
    )
    .await;
    let second = f.create().await;
    // Keep latest (the second run) only: `kept` is named by no kept run.
    prune::execute(&f.dest, "", 0, prune_time()).await.unwrap();
    assert!(load(&f.dest, "", &first).await.is_err());
    assert!(
        optional(&f.dest, &format!("data/{}", digest(b"new")))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        optional(&f.dest, &format!("data/{}", digest(b"kept")))
            .await
            .unwrap()
            .is_none()
    );
    verify(&f.dest, "", &second, None).await.unwrap();
}

#[tokio::test]
async fn an_in_place_restore_moves_files_to_a_new_epoch() {
    let f = Fixture::new().await;
    let epoch = fresh_epoch();
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "a",
        b"before",
        "text/plain",
    )
    .await;
    let id = f.create().await;
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "a",
        b"after",
        "text/plain",
    )
    .await;
    restore::execute(
        &restore_args(&id, true),
        &f.dest,
        "",
        &f.source,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let new = restored_epoch(&f.source).await;
    assert_ne!(new, epoch);
    assert_eq!(
        file_at(
            &f.source,
            &format!("state/acme/shop/blobs/{new}/{}/a", alice_ns("0"))
        )
        .await
        .map(|f| f.0),
        Some(b"before".to_vec())
    );
    // An upload through a link issued before the restore names the old
    // epoch: it lands there, and nothing reads it.
    put_file(
        &f.source,
        &epoch,
        &alice_ns("0"),
        "late",
        b"late",
        "text/plain",
    )
    .await;
    assert!(
        file_at(
            &f.source,
            &format!("state/acme/shop/blobs/{new}/{}/late", alice_ns("0"))
        )
        .await
        .is_none()
    );
    // The undo run copied the files the restore replaced.
    let labels = list_bounded(&f.dest, "before-restore/").await.unwrap();
    let undo = String::from_utf8(
        read(
            &f.dest,
            labels.values().next().unwrap().location.as_ref(),
            MAX,
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(
        optional(&f.dest, &format!("data/{}", digest(b"after")))
            .await
            .unwrap()
            .is_some(),
        "undo run {undo} lacks the replaced file"
    );
}

/// Counts GETs of files (`blobs/`) and passes everything to `inner`.
#[derive(Debug)]
struct CountBlobGets {
    inner: Store,
    gets: std::sync::atomic::AtomicUsize,
    late_change: Option<(Store, String, Arc<std::sync::atomic::AtomicBool>)>,
}
impl std::fmt::Display for CountBlobGets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counting source")
    }
}
#[async_trait::async_trait]
impl ObjectStore for CountBlobGets {
    async fn put_opts(
        &self,
        p: &Key,
        b: object_store::PutPayload,
        o: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Key,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &Key,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if p.as_ref().contains("/blobs/") {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.get_opts(p, o).await
    }
    fn list(
        &self,
        prefix: Option<&Key>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix.is_some_and(|p| p.as_ref().contains("/blobs/"))
            && let Some((archive, copied, changed)) = self.late_change.clone()
        {
            let inner = self.inner.clone();
            let prefix = prefix.cloned();
            return futures_util::stream::once(async move {
                // The actor's snapshot is already in the archive. No sleep
                // or race with a scheduler decides which bytes are backed up.
                assert!(archive.head(&Key::from(copied)).await.is_ok());
                put_file(
                    &inner,
                    &fresh_epoch(),
                    &alice_ns("0"),
                    "value",
                    b"2",
                    "text/plain",
                )
                .await;
                changed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, object_store::Error>(inner.list(prefix.as_ref()))
            })
            .try_flatten()
            .boxed();
        }
        self.inner.list(prefix)
    }
    fn delete_stream(
        &self,
        s: futures_util::stream::BoxStream<'static, object_store::Result<Key>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Key>> {
        self.inner.delete_stream(s)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&Key>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &Key,
        b: &Key,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

#[test]
fn missing_file_manifest_warning_is_specific_to_blob_imports() {
    assert!(restore::missing_files_warning(true, &json!({})).is_some());
    assert!(restore::missing_files_warning(false, &json!({})).is_none());
    assert!(restore::missing_files_warning(true, &json!({"files": []})).is_none());
}

#[tokio::test]
async fn file_changed_after_actor_copy_restores_newer_than_its_row() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let f = Fixture::new().await;
    put_file(
        &f.source,
        &fresh_epoch(),
        &alice_ns("0"),
        "value",
        b"1",
        "text/plain",
    )
    .await;
    let objects = history::list_objects(&f.source, "state/acme/shop/actors/counter/alice/")
        .await
        .unwrap();
    let snapshot = f
        .source
        .get(&objects.values().next().unwrap().location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let changed = Arc::new(AtomicBool::new(false));
    let source: Store = Arc::new(CountBlobGets {
        inner: f.source.clone(),
        gets: Default::default(),
        late_change: Some((
            f.dest.clone(),
            format!("data/{}", digest(&snapshot)),
            changed.clone(),
        )),
    });
    create(&f.args(), &source, "", &f.dest, "").await.unwrap();
    assert!(changed.load(Ordering::SeqCst));
    let id = latest(&f.dest, "").await.unwrap().unwrap();
    verify(&f.dest, "", &id, None).await.unwrap();
    let run = load(&f.dest, "", &id).await.unwrap();
    assert!(array(&run.complete, "failed").unwrap().is_empty());
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    let db = f._dir.path().join("restored.db");
    history::replay(&target, "state/acme/shop/actors/counter/alice/", &db, MAX)
        .await
        .unwrap();
    let conn = rusqlite::Connection::open(db).unwrap();
    let row: i64 = conn
        .query_row("SELECT n FROM counts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(row, 1);
    let epoch = restored_epoch(&target).await;
    assert_eq!(
        file_at(
            &target,
            &format!("state/acme/shop/blobs/{epoch}/{}/value", alice_ns("0"))
        )
        .await,
        Some((b"2".to_vec(), Some("text/plain".into())))
    );
}

#[tokio::test]
async fn legacy_backup_with_blob_imports_restores_successfully() {
    const CHILD: &str = "WASMKEEP_TEST_LEGACY_BLOB_FRONT";
    if std::env::var_os(CHILD).is_none() {
        for front in ["false", "true"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "backup::tests::legacy_backup_with_blob_imports_restores_successfully",
                    "--nocapture",
                ])
                .env(CHILD, front)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "{}\n{stderr}",
                String::from_utf8_lossy(&output.stdout)
            );
            assert_eq!(
                stderr
                    .lines()
                    .filter(|line| line.starts_with("warning:"))
                    .count(),
                1,
                "{stderr}"
            );
        }
        return;
    }
    let front = std::env::var(CHILD).unwrap() == "true";
    let f = Fixture::new().await;
    let wasm = wat::parse_str(
        r#"(component
            (import "wasmkeep:actor/blobs@0.2.0" (instance)))"#,
    )
    .unwrap();
    let class = if front { "_front" } else { "counter" };
    let line = if front {
        format!("front main {}", digest(&wasm))
    } else {
        format!("class counter {}", digest(&wasm))
    };
    put(
        &f.source,
        "apps/acme/shop/v1/manifest",
        format!("wasmkeep-manifest 3\n{line}\n").into_bytes(),
        true,
    )
    .await
    .unwrap();
    put(
        &f.source,
        &format!("apps/acme/shop/v1/{class}.wasm"),
        wasm,
        false,
    )
    .await
    .unwrap();
    let id = f.create().await;
    let run = load(&f.dest, "", &id).await.unwrap();
    let mut app = run.apps[0].clone();
    for field in ["files", "file_count", "blob_epoch"] {
        app.as_object_mut().unwrap().remove(field);
    }
    let mut complete = run.complete.clone();
    let app_key = complete["apps"][0]["key"].as_str().unwrap().to_owned();
    complete["apps"][0]["sha256"] = json!(json_put(&f.dest, &app_key, &app, true).await.unwrap());
    let hash = json_put(&f.dest, &format!("runs/{id}/complete"), &complete, true)
        .await
        .unwrap();
    json_put(
        &f.dest,
        &format!("runs/{id}/root"),
        &json!({"run":id,"sha256":hash}),
        true,
    )
    .await
    .unwrap();
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let imported = restore::imports_blobs(
        &f.dest,
        "",
        &app,
        "apps/acme/shop/",
        &target,
        "apps/acme/shop",
        DeployChoice::Backup,
    )
    .await
    .unwrap();
    assert!(imported);
    assert_eq!(
        restore::missing_files_warning(imported, &app)
            .unwrap()
            .lines()
            .count(),
        1
    );
    restore::execute(
        &restore_args(&id, false),
        &f.dest,
        "",
        &target,
        "",
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        restore::imports_blobs(
            &f.dest,
            "",
            &app,
            "apps/acme/shop/",
            &target,
            "apps/acme/shop",
            DeployChoice::Keep
        )
        .await
        .unwrap()
    );
}

// ADR 0043, section 3, "A restore": the upgrade floor in a backup, and the
// gate a whole-app restore goes through.

use wasmkeep_cluster::app_format::floor::{self as upgrade_floor, Minor};

const FLOOR_KEY: &str = "formats/acme/shop/floor";
const NINE: Minor = Minor { major: 0, minor: 9 };
const TEN: Minor = Minor {
    major: 0,
    minor: 10,
};

async fn set_floor(store: &Store, floor: Minor) {
    put(store, FLOOR_KEY, upgrade_floor::encode(floor), true)
        .await
        .unwrap();
}

async fn floor_at(store: &Store) -> Option<Minor> {
    upgrade_floor::decode(optional(store, FLOOR_KEY).await.unwrap().as_deref()).unwrap()
}

/// The fixture's app on a version with `spread` (manifest format 8) that
/// keeps the first version, as a deploy of 0.9 leaves it.
async fn deploy_format_8(source: &Store) {
    let wasm = b"not read by a backup".to_vec();
    let manifest = format!(
        "wasmkeep-manifest 8\nclass counter {}\nprevious v1\nspread counter on\n",
        digest(&wasm)
    );
    assert_eq!(
        wasmkeep_server::apps::Manifest::parse(&manifest)
            .unwrap()
            .format_number(),
        8
    );
    put(source, "apps/acme/shop/v8/counter.wasm", wasm, false)
        .await
        .unwrap();
    put(
        source,
        "apps/acme/shop/v8/manifest",
        manifest.into_bytes(),
        false,
    )
    .await
    .unwrap();
    put(source, "apps/acme/shop/current", b"v8 2 3\n".to_vec(), true)
        .await
        .unwrap();
}

/// A live node of another app under the prefix, on `version`.
async fn live_neighbour(store: &Store, version: &str) {
    let stamp = now_ms().unwrap() / 1000 * 1000;
    let body = format!("wasmkeep-node 2\nold\nhttp://127.0.0.1:9\n{version}\n{stamp}\nacme/blog\n");
    put(store, "nodes/old", body.into_bytes(), true)
        .await
        .unwrap();
}

async fn restore_as(
    f: &Fixture,
    id: &str,
    target: &Store,
    in_place: bool,
    writer: Option<Minor>,
) -> anyhow::Result<()> {
    let mut args = restore_args(id, in_place);
    args.writer = writer;
    restore::execute(&args, &f.dest, "", target, "", std::time::Duration::ZERO).await
}

async fn keys_of(store: &Store) -> Vec<String> {
    list_bounded(store, "").await.unwrap().into_keys().collect()
}

/// A run carries the floor of an app that has one, and only then is format
/// 2: a `wasmkeep` of 0.8 reads every other run, and refuses this one
/// instead of restoring the app without its floor.
#[tokio::test]
async fn a_run_with_a_floor_is_format_2_and_one_without_stays_format_1() {
    let f = Fixture::new().await;
    let plain = f.create().await;
    let run = load(&f.dest, "", &plain).await.unwrap();
    assert_eq!(run.complete["format"], 1);
    assert!(run.apps[0].get("floor").is_none());

    set_floor(&f.source, NINE).await;
    let id = f.create().await;
    verify(&f.dest, "", &id, None).await.unwrap();
    verify(&f.dest, "", &plain, None).await.unwrap();
    let run = load(&f.dest, "", &id).await.unwrap();
    assert_eq!(run.complete["format"], 2);
    assert_eq!(run.apps[0]["floor"], "0.9");
    assert_eq!(archived_floor(&run.apps[0]).unwrap(), Some(NINE));

    // The number and the field go together: neither is read without the
    // other, whatever the digests say.
    let root_key = format!("runs/{id}/root");
    let root: Value =
        serde_json::from_slice(&read(&f.dest, &root_key, META_MAX).await.unwrap()).unwrap();
    let mut as_one = run.complete.clone();
    as_one["format"] = json!(1);
    let hash = json_put(&f.dest, &format!("runs/{id}/complete"), &as_one, true)
        .await
        .unwrap();
    let mut edited = root.clone();
    edited["sha256"] = json!(hash);
    json_put(&f.dest, &root_key, &edited, true).await.unwrap();
    let refused = load(&f.dest, "", &id).await.map(drop).unwrap_err();
    assert!(
        format!("{refused:#}").contains("unknown or inconsistent backup format"),
        "{refused:#}"
    );
    for (value, refused) in [
        (json!("0.9"), false),
        (json!("0.09"), true),
        (json!("0.9.0"), true),
        (json!(9), true),
        (Value::Null, false),
    ] {
        assert_eq!(archived_floor(&json!({ "floor": value })).is_err(), refused);
    }

    json_put(&f.dest, &format!("runs/{id}/complete"), &run.complete, true)
        .await
        .unwrap();
    json_put(&f.dest, &root_key, &root, true).await.unwrap();
    load(&f.dest, "", &id).await.unwrap();

    // An unreadable record is not archived as "no floor".
    put(
        &f.source,
        FLOOR_KEY,
        b"wasmkeep-floor 2\n1\n".to_vec(),
        true,
    )
    .await
    .unwrap();
    let e = create(&f.args(), &f.source, "", &f.dest, "")
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("unreadable"), "{e:#}");
}

/// The case of ADR 0043, "Not settled" 4: an app on manifest 8 restored
/// into another prefix gets its floor there before anything of it is
/// written, so a node of 0.8 stops at the floor instead of at a manifest it
/// cannot read.
#[tokio::test]
async fn restore_of_a_format_8_app_into_an_empty_prefix_creates_the_floor() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    deploy_format_8(&f.source).await;
    let id = f.create().await;

    // A `wasmkeep` older than the app's floor writes nothing.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let e = restore_as(&f, &id, &target, false, Some(upgrade_floor::ABSENT))
        .await
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("older than the app in the backup")
            && format!("{e:#}").contains("0.9"),
        "{e:#}"
    );
    assert_eq!(keys_of(&target).await, Vec::<String>::new());

    restore_as(&f, &id, &target, false, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&target).await, Some(NINE));
    assert_eq!(
        read(&target, "apps/acme/shop/current", MAX).await.unwrap(),
        b"v8 2 3\n"
    );
    assert!(
        read(&target, "apps/acme/shop/v8/manifest", MAX)
            .await
            .unwrap()
            .starts_with(b"wasmkeep-manifest 8\n")
    );
    // A newer `wasmkeep` restores the floor the backup carries, not its own.
    let newer: Store = Arc::new(object_store::memory::InMemory::new());
    restore_as(&f, &id, &newer, false, Some(TEN)).await.unwrap();
    assert_eq!(floor_at(&newer).await, Some(NINE));
}

/// The floor is written before the app's state and deploy: a restore that
/// fails later has left the floor, and its retry writes it once.
#[tokio::test]
async fn the_restored_floor_precedes_the_apps_state_and_deploy() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    deploy_format_8(&f.source).await;
    let id = f.create().await;
    for fail in 1..200 {
        let inner: Store = Arc::new(object_store::memory::InMemory::new());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failing: Store = Arc::new(FailMutation {
            inner: Arc::clone(&inner),
            count: Arc::clone(&count),
            fail,
            lost: false,
            stray: false,
        });
        let done = restore_as(&f, &id, &failing, false, Some(NINE)).await;
        let keys = keys_of(&inner).await;
        let app_written = keys.iter().any(|k| {
            k.starts_with("state/") || k == "apps/acme/shop/current" || k.contains("/v8/")
        });
        assert!(
            !app_written || floor_at(&inner).await == Some(NINE),
            "mutation {fail}: {keys:?}"
        );
        if done.is_ok() {
            assert_eq!(floor_at(&inner).await, Some(NINE));
            return;
        }
        // Only its unlock failed: the restore is done.
        if app_written
            && optional(&inner, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_none()
        {
            continue;
        }
        // The retry goes through, on the floor it finds or creates.
        restore_as(&f, &id, &inner, false, Some(NINE))
            .await
            .unwrap();
        assert_eq!(floor_at(&inner).await, Some(NINE));
    }
    panic!("the restore never ran out of mutations");
}

/// The floor is the app's own: beside live nodes of 0.8 under the target
/// prefix that serve another app, it is raised all the same (as `finalize`
/// raises it), and the restore names them: they cannot serve this app.
#[tokio::test]
async fn restore_beside_older_nodes_of_another_app_raises_the_floor_and_names_them() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    deploy_format_8(&f.source).await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    live_neighbour(&target, "0.8.1").await;
    let check = wasmkeep_server::apps::AppRef::new(Arc::clone(&target), "", "acme", "shop")
        .as_version(NINE)
        .check_restore(Some(NINE), &[])
        .await
        .unwrap();
    assert_eq!(check.raise, Some(NINE));
    assert_eq!(check.shut_out, ["old (acme/blog, 0.8.1)"]);
    assert_eq!(check.left_behind, None);
    assert_eq!(
        keys_of(&target).await,
        ["nodes/old"],
        "the check reads only"
    );
    restore_as(&f, &id, &target, false, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&target).await, Some(NINE));
    assert!(
        read(&target, "apps/acme/shop/v8/manifest", MAX)
            .await
            .unwrap()
            .starts_with(b"wasmkeep-manifest 8\n")
    );
}

/// `--deploy keep` installs no version, and the floor still goes with the
/// state: raised to the backup's, and refused from a `wasmkeep` below it.
#[tokio::test]
async fn restore_with_deploy_keep_still_carries_the_floor() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    deploy_format_8(&f.source).await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(
        &target,
        "apps/acme/shop/current",
        b"v1 1 3\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    let keep = async |writer| {
        let mut args = restore_args(&id, false);
        args.writer = Some(writer);
        args.deploy = Some(DeployChoice::Keep);
        restore::execute(&args, &f.dest, "", &target, "", std::time::Duration::ZERO).await
    };
    let e = keep(upgrade_floor::ABSENT).await.unwrap_err();
    let text = format!("{e:#}");
    assert!(
        text.contains("older than the app in the backup")
            && text.contains("finalized at 0.9 and this is 0.8"),
        "{text}"
    );
    assert_eq!(keys_of(&target).await, ["apps/acme/shop/current"]);
    keep(NINE).await.unwrap();
    assert_eq!(floor_at(&target).await, Some(NINE));
    assert_eq!(
        read(&target, "apps/acme/shop/current", MAX).await.unwrap(),
        b"v1 1 3\n",
        "the target's deploy is kept"
    );
    assert!(
        optional(&target, "apps/acme/shop/v8/manifest")
            .await
            .unwrap()
            .is_none()
    );
}

/// A restore in place that has to raise the floor lists the app's
/// registered nodes first, as `finalize` does. An older one whose lease
/// was never released nor fenced is not stopped for sure: refused, naming
/// it, before the undo backup or any other write. Released, the restore
/// goes on, and the floor is raised.
#[tokio::test]
async fn an_in_place_raise_beside_an_older_node_not_stopped_for_sure_is_refused_first() {
    use wasmkeep_cluster::node_lease::NodeState;
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    let id = f.create().await;
    // Another prefix that holds the app, never finalized.
    let other = Fixture::new().await;
    let stale = u64::try_from(now_ms().unwrap()).unwrap() - 3_600_000;
    put(
        &other.source,
        "nodes/n1",
        app_node_lease("n1", NodeState::Live, stale),
        true,
    )
    .await
    .unwrap();
    let before = keys_of(&other.source).await;
    let archive_before = keys_of(&f.dest).await;
    let e = restore_as(&f, &id, &other.source, true, Some(NINE))
        .await
        .unwrap_err();
    let text = format!("{e:#}");
    assert!(
        text.contains("would raise the app's upgrade floor to 0.9")
            && text.contains("n1 (0.0.0)")
            && text.contains("cluster fence")
            && text.contains("cluster forget"),
        "{text}"
    );
    assert_eq!(keys_of(&other.source).await, before, "nothing written");
    assert_eq!(floor_at(&other.source).await, None);
    // The archive got its pin, and no undo run.
    let added: Vec<String> = keys_of(&f.dest)
        .await
        .into_iter()
        .filter(|k| !archive_before.contains(k))
        .collect();
    assert!(added.iter().all(|k| !k.starts_with("runs/")), "{added:?}");

    put(
        &other.source,
        "nodes/n1",
        app_node_lease("n1", NodeState::Released, stale),
        true,
    )
    .await
    .unwrap();
    let check = wasmkeep_server::apps::AppRef::new(Arc::clone(&other.source), "", "acme", "shop")
        .as_version(NINE)
        .check_restore(Some(NINE), &[])
        .await
        .unwrap();
    assert_eq!(check.left_behind.as_deref(), Some("n1 (0.0.0)"));
    restore_as(&f, &id, &other.source, true, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&other.source).await, Some(NINE));
    // The undo run was made before the floor was raised: it carries none.
    let undo = list_bounded(&f.dest, "before-restore/").await.unwrap();
    let undo = read(
        &f.dest,
        undo.values().next().unwrap().location.as_ref(),
        MAX,
    )
    .await
    .unwrap();
    let undo = load(&f.dest, "", std::str::from_utf8(&undo).unwrap())
        .await
        .unwrap();
    assert_eq!(undo.complete["format"], 1);
}

/// A rerun after the listing failed finds the floor written, and lists
/// again: it does not go on beside the older node.
#[tokio::test]
async fn a_rerun_lists_the_older_nodes_again_although_the_floor_is_written() {
    use wasmkeep_cluster::node_lease::NodeState;
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    // The floor as an earlier run left it, and an older node of the app
    // that is not stopped for sure.
    set_floor(&target, NINE).await;
    let stale = u64::try_from(now_ms().unwrap()).unwrap() - 3_600_000;
    put(
        &target,
        "nodes/n1",
        node_lease_of("acme/shop", "n1", NodeState::Fenced, stale + 3_600_000),
        true,
    )
    .await
    .unwrap();
    let e = restore_as(&f, &id, &target, false, Some(NINE))
        .await
        .unwrap_err();
    let text = format!("{e:#}");
    assert!(
        text.contains("upgrade floor is 0.9")
            && text.contains("n1 (0.0.0)")
            && text.contains("cluster fence"),
        "{text}"
    );
    assert!(
        !keys_of(&target)
            .await
            .iter()
            .any(|k| k.starts_with("state/") || k == "apps/acme/shop/current"),
        "nothing of the app written"
    );
    // Fenced for long enough: stopped for sure, and the same operation
    // goes on.
    put(
        &target,
        "nodes/n1",
        node_lease_of("acme/shop", "n1", NodeState::Fenced, stale),
        true,
    )
    .await
    .unwrap();
    restore_as(&f, &id, &target, false, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&target).await, Some(NINE));
}

/// An in-place restore of a run without a floor into an app that has one
/// since: the floor stays, and the undo run it makes first carries it, so
/// that run is format 2 and becomes the archive's latest.
#[tokio::test]
async fn an_in_place_restore_into_a_finalized_app_writes_a_format_2_undo_run() {
    let f = Fixture::new().await;
    let id = f.create().await;
    assert_eq!(load(&f.dest, "", &id).await.unwrap().complete["format"], 1);
    set_floor(&f.source, NINE).await;
    restore_as(&f, &id, &f.source, true, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&f.source).await, Some(NINE));
    let newest = latest(&f.dest, "").await.unwrap().unwrap();
    assert_ne!(newest, id);
    let undo = load(&f.dest, "", &newest).await.unwrap();
    assert_eq!(undo.complete["format"], 2);
    assert_eq!(archived_floor(&undo.apps[0]).unwrap(), Some(NINE));
}

/// A backup without a floor (one made by 0.8, or of an app never
/// finalized) restores as before: no floor is written, beside older nodes
/// or not, by this build or a later one.
#[tokio::test]
async fn an_old_backup_without_a_floor_restores_as_before() {
    let f = Fixture::new().await;
    let id = f.create().await;
    assert_eq!(load(&f.dest, "", &id).await.unwrap().complete["format"], 1);
    for writer in [None, Some(NINE)] {
        let target: Store = Arc::new(object_store::memory::InMemory::new());
        live_neighbour(&target, "0.8.1").await;
        restore_as(&f, &id, &target, false, writer).await.unwrap();
        assert_eq!(
            read(&target, "apps/acme/shop/current", MAX).await.unwrap(),
            b"v1 1 3\n"
        );
        assert!(
            !keys_of(&target)
                .await
                .iter()
                .any(|k| k.starts_with("formats/")),
            "no floor"
        );
    }
}

/// A run without a floor that holds a format 8 manifest (the record was
/// absent when the run was made): the gate applies on the floor a first
/// deploy would create, for a new app only.
#[tokio::test]
async fn a_backup_without_a_floor_that_holds_format_8_needs_the_floor_of_a_first_deploy() {
    let f = Fixture::new().await;
    deploy_format_8(&f.source).await;
    let id = f.create().await;
    assert_eq!(load(&f.dest, "", &id).await.unwrap().complete["format"], 1);

    // This build is 0.8: it may not write the format at all.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let e = restore_as(&f, &id, &target, false, Some(upgrade_floor::ABSENT))
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("wasmkeep-manifest 8"), "{e:#}");
    assert_eq!(keys_of(&target).await, Vec::<String>::new());

    // Beside an older live node under the prefix: no floor, so refused.
    live_neighbour(&target, "0.8.1").await;
    let e = restore_as(&f, &id, &target, false, Some(NINE))
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("old (acme/blog, 0.8.1)"), "{e:#}");
    assert_eq!(keys_of(&target).await, ["nodes/old"]);

    // A target app that is not new gets no floor from a restore.
    let deployed: Store = Arc::new(object_store::memory::InMemory::new());
    put(
        &deployed,
        "apps/acme/shop/current",
        b"v1 1 3\n".to_vec(),
        false,
    )
    .await
    .unwrap();
    let e = restore_as(&f, &id, &deployed, false, Some(NINE))
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("not new"), "{e:#}");
    assert_eq!(keys_of(&deployed).await, ["apps/acme/shop/current"]);
    // With a floor that allows the format, it is not asked to be new.
    set_floor(&deployed, NINE).await;
    restore_as(&f, &id, &deployed, false, Some(NINE))
        .await
        .unwrap();

    // A new app: the floor of a first deploy, then the manifests.
    let fresh: Store = Arc::new(object_store::memory::InMemory::new());
    restore_as(&f, &id, &fresh, false, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&fresh).await, Some(NINE));
}

/// The floor is only ever raised: a restore where one exists keeps the
/// higher of the target's and the backup's.
#[tokio::test]
async fn a_restore_where_a_floor_exists_keeps_the_higher_floor() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    let id = f.create().await;

    // In place, in the prefix the backup was made from: the floor has
    // since been raised, and stays.
    set_floor(&f.source, TEN).await;
    let e = restore_as(&f, &id, &f.source, true, Some(NINE))
        .await
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("older than the cluster"),
        "a `wasmkeep` below the target's floor: {e:#}"
    );
    assert_eq!(floor_at(&f.source).await, Some(TEN));
    restore_as(&f, &id, &f.source, true, Some(TEN))
        .await
        .unwrap();
    assert_eq!(floor_at(&f.source).await, Some(TEN));

    // The target's floor is the backup's already: nothing to write, so a
    // live node of 0.8 serving another app there does not matter.
    let same: Store = Arc::new(object_store::memory::InMemory::new());
    set_floor(&same, NINE).await;
    live_neighbour(&same, "0.8.1").await;
    let before = same.head(&Key::from(FLOOR_KEY)).await.unwrap().e_tag;
    restore_as(&f, &id, &same, false, Some(NINE)).await.unwrap();
    assert_eq!(
        same.head(&Key::from(FLOOR_KEY)).await.unwrap().e_tag,
        before,
        "the record was not written again"
    );

    // The target's floor is lower: raised to the backup's.
    let lower: Store = Arc::new(object_store::memory::InMemory::new());
    set_floor(&lower, upgrade_floor::ABSENT).await;
    restore_as(&f, &id, &lower, false, Some(NINE))
        .await
        .unwrap();
    assert_eq!(floor_at(&lower).await, Some(NINE));

    // An unreadable floor in the target: nothing is written.
    let unreadable: Store = Arc::new(object_store::memory::InMemory::new());
    put(&unreadable, FLOOR_KEY, b"9\n".to_vec(), false)
        .await
        .unwrap();
    let e = restore_as(&f, &id, &unreadable, false, Some(NINE))
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("unreadable"), "{e:#}");
    assert_eq!(keys_of(&unreadable).await, [FLOOR_KEY]);
}

const GENERATION_FLOOR_KEY: &str = "state/acme/shop/generation-floor";

async fn generation_floor_at(
    store: &Store,
) -> Option<wasmkeep_cluster::generation_floor::GenerationFloor> {
    optional(store, GENERATION_FLOOR_KEY)
        .await
        .unwrap()
        .map(|body| wasmkeep_cluster::generation_floor::decode(Some(&body)).unwrap())
}

/// ADR 0045, section 3: once the target's upgrade floor is 0.10 a restore
/// in place writes one generation floor, the highest generation collected
/// plus one, with `markers none`, and no marker; a second restore in place
/// with no acquisition between reads the first one's floor and keeps it.
#[tokio::test]
async fn a_restore_at_the_floor_0_10_writes_the_generation_floor_and_no_marker() {
    use wasmkeep_cluster::generation_floor::{GenerationFloor, Markers};
    let f = Fixture::new().await;
    set_floor(&f.source, TEN).await;
    let id = f.create().await;
    // A marker left by a restore before the floor: the only carrier of an
    // absent actor's seed.
    put(
        &f.source,
        "state/acme/shop/leases/counter/absent",
        wasmkeep_cluster::node_lease::encode_marker(42),
        false,
    )
    .await
    .unwrap();
    restore_as(&f, &id, &f.source, true, Some(TEN))
        .await
        .unwrap();
    assert_eq!(
        generation_floor_at(&f.source).await,
        Some(GenerationFloor {
            generation: 43,
            markers: Markers::None
        })
    );
    assert!(
        list_bounded(&f.source, "state/acme/shop/leases/")
            .await
            .unwrap()
            .is_empty(),
        "no marker"
    );
    // Again, nothing acquired between: the floor is read and kept above.
    restore_as(&f, &id, &f.source, true, Some(TEN))
        .await
        .unwrap();
    assert_eq!(
        generation_floor_at(&f.source).await,
        Some(GenerationFloor {
            generation: 44,
            markers: Markers::None
        })
    );
    // An unreadable one stops the next restore before it clears anything.
    put(&f.source, GENERATION_FLOOR_KEY, b"junk".to_vec(), true)
        .await
        .unwrap();
    let e = restore_as(&f, &id, &f.source, true, Some(TEN))
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains(GENERATION_FLOOR_KEY), "{e:#}");
    assert!(
        !list_bounded(&f.source, "state/acme/shop/actors/")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Into another prefix that held nothing, at 0.10: the object carries the
/// plans' highest generation plus one, `markers none`.
#[tokio::test]
async fn a_restore_at_the_floor_0_10_into_an_empty_target_writes_the_generation_floor() {
    use wasmkeep_cluster::generation_floor::{GenerationFloor, Markers};
    let f = Fixture::new().await;
    set_floor(&f.source, TEN).await;
    let id = f.create().await;
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    restore_as(&f, &id, &target, false, Some(TEN))
        .await
        .unwrap();
    assert_eq!(floor_at(&target).await, Some(TEN));
    assert_eq!(
        generation_floor_at(&target).await,
        Some(GenerationFloor {
            generation: 2,
            markers: Markers::None
        }),
        "alice's plan carries generation 1"
    );
    assert!(
        list_bounded(&target, "state/acme/shop/leases/")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Below the floor 0.10, by this build or one of 0.10: markers, byte for
/// byte as 0.9 writes them, and no generation floor.
#[tokio::test]
async fn a_restore_below_the_floor_0_10_writes_markers_as_before() {
    let f = Fixture::new().await;
    set_floor(&f.source, NINE).await;
    let id = f.create().await;
    for writer in [None, Some(NINE), Some(TEN)] {
        let target: Store = Arc::new(object_store::memory::InMemory::new());
        restore_as(&f, &id, &target, false, writer).await.unwrap();
        assert_eq!(
            read(&target, "state/acme/shop/leases/counter/alice", MAX)
                .await
                .unwrap(),
            b"wasmkeep-lease 2\n1\n",
            "{writer:?}"
        );
        assert_eq!(generation_floor_at(&target).await, None, "{writer:?}");
    }
}
