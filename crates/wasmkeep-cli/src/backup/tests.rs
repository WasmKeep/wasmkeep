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
    };
    let actor = copier
        .actor(
            prefix,
            history::list_objects(&source, prefix).await.unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(!array(&actor, "gaps").unwrap().is_empty());
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
async fn historical_get_gap_splits_ranges_and_requires_opt_out() {
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
        let result = create(&args, &source, "", &f.dest, "").await;
        assert_eq!(result.is_err(), continuous);
        let id = latest(&f.dest, "").await.unwrap().unwrap();
        let run = load(&f.dest, "", &id).await.unwrap();
        let actor = disk_actor(&run.actors[0]).unwrap();
        assert_eq!(
            actor["ranges"],
            json!([{"generation":1,"start":1,"end":1},{"generation":1,"start":4,"end":4}])
        );
        assert_eq!(array(&actor, "gaps").unwrap().len(), 1);
        assert_eq!(verify(&f.dest, "", &id, None).await.is_err(), continuous);
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
    };
    let actor = copier.actor(prefix, initial, None).await.unwrap();
    assert!(
        array(&actor, "gaps")
            .unwrap()
            .contains(&json!(snapshot_key(SegmentId {
                generation: 1,
                txid: 1
            })))
    );
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
        from: "s3://destination".into(),
        run: Some(id.into()),
        app: ("acme".into(), "shop".into()),
        bucket: "s3://target".into(),
        in_place,
        deploy: None,
        force: false,
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
        mutation(&self.count, self.fail)?;
        self.inner.put_opts(key, payload, opts).await
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
        keys.then(move |key| {
            let inner = inner.clone();
            let count = count.clone();
            async move {
                let key = key?;
                mutation(&count, fail)?;
                inner.delete(&key).await?;
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
    let mut failures = 0;
    for fail in 0..80 {
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
        });
        let archive: Store = Arc::new(FailMutation {
            inner: f.dest.clone(),
            count: count.clone(),
            fail,
        });
        let mut args = restore_args(&id, true);
        args.deploy = Some(DeployChoice::Backup);
        let result =
            restore::execute(&args, &archive, "", &target, "", std::time::Duration::ZERO).await;
        let total = count.load(std::sync::atomic::Ordering::SeqCst);
        if result.is_err() {
            failures += 1;
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
        assert_eq!(
            wasmkeep_cluster::node_lease::decode_marker(
                &read(&f.source, "state/acme/shop/leases/counter/absent", MAX)
                    .await
                    .unwrap()
            ),
            Some(43),
            "failure {fail}"
        );
        assert!(
            optional(&f.source, "apps/acme/shop/restoring")
                .await
                .unwrap()
                .is_none(),
            "failure {fail}"
        );
        if fail >= total {
            break;
        }
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
