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
        resume: None,
        from: "s3://destination".into(),
        run: Some(id.into()),
        app: ("acme".into(), "shop".into()),
        bucket: "s3://target".into(),
        in_place,
        abort: false,
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
    use wasmkeep_cluster::membership::NodeInfo;
    let info = NodeInfo {
        id: id.into(),
        address: "http://n:8080".into(),
        version: "0.0.0".into(),
        written_at_ms,
        app: "acme/shop".into(),
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
        process: None,
    };
    wasmkeep_cluster::node_lease::NodeLeaseObject {
        state,
        instance: Some("i1".into()),
        written_at_ms,
    }
    .encode_with(&info.payload())
}

#[tokio::test(start_paused = true)]
async fn restore_waits_for_a_stopped_nodes_last_write_not_for_the_fence() {
    use wasmkeep_cluster::node_lease::NodeState;
    let late = wasmkeep_server::apps::LATE_WRITES;
    let f = Fixture::new().await;
    let id = f.create().await;
    // A node of the app stopped moments ago: a write it sent may still land.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let now = u64::try_from(super::now_ms().unwrap()).unwrap();
    put(
        &target,
        "nodes/n1",
        app_node_lease("n1", NodeState::Released, now),
        false,
    )
    .await
    .unwrap();
    let start = tokio::time::Instant::now();
    restore::execute(&restore_args(&id, false), &f.dest, "", &target, "", late)
        .await
        .unwrap();
    let waited = start.elapsed();
    assert!(
        waited >= late - std::time::Duration::from_secs(1) && waited < late * 2,
        "{waited:?}"
    );
    // Stopped long ago (its last write is older than the bound): no wait.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    let old = now - u64::try_from(late.as_millis()).unwrap() - 1_000;
    put(
        &target,
        "nodes/n1",
        app_node_lease("n1", NodeState::Fenced, old),
        false,
    )
    .await
    .unwrap();
    let start = tokio::time::Instant::now();
    restore::execute(&restore_args(&id, false), &f.dest, "", &target, "", late)
        .await
        .unwrap();
    assert!(
        start.elapsed() < std::time::Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    // A heartbeat this version cannot read proves nothing: the fence's time
    // counts again, as before 0.7.0.
    let target: Store = Arc::new(object_store::memory::InMemory::new());
    put(&target, "nodes/n2", b"wasmkeep-node 9\n".to_vec(), false)
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    restore::execute(&restore_args(&id, false), &f.dest, "", &target, "", late)
        .await
        .unwrap();
    assert!(
        start.elapsed() >= late - std::time::Duration::from_secs(1),
        "{:?}",
        start.elapsed()
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
