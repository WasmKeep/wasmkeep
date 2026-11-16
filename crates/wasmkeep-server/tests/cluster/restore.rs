//! Restore fences and stale local files (ADR 0033 section 5).
use super::app_delete::{app_node_following, deploy};
use super::deletes::read;
use super::*;
use object_store::{ObjectStoreExt, path::Path as Key};

#[tokio::test(flavor = "multi_thread")]
async fn restore_fence_stops_nodes_and_invalidates_offline_files() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let archive: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let offline = tempfile::tempdir().unwrap();
    let (old, old_follow) = app_node_following("offline", offline.path(), &store)
        .await
        .unwrap();
    assert_eq!(read(old.addr, "saved").await, "0");
    assert_eq!(
        send(old.addr, "POST", "/actor/t/saved/durable", "")
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
    assert_eq!(read(old.addr, "absent").await, "0");
    assert_eq!(
        send(old.addr, "POST", "/actor/t/absent/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    old_follow.abort();
    drop(old);
    let other_dir = tempfile::tempdir().unwrap();
    let other = node_with(
        "other-app",
        other_dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(read(other.addr, "unrelated").await, "0");
    let dir = tempfile::tempdir().unwrap();
    let (node, follows) = app_node_following("restore-node", dir.path(), &store)
        .await
        .unwrap();
    assert_eq!(read(node.addr, "old").await, "0");
    store
        .put(
            &Key::from("apps/default/shop/restoring"),
            b"run".to_vec().into(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), node.service.app_deleted())
        .await
        .expect("restore did not stop node");
    assert_eq!(
        send(node.addr, "GET", "/actor/t/old/durable", "")
            .await
            .status,
        503
    );
    follows.await.unwrap();
    let local = wasmkeep_server::local_app_dir(offline.path(), "default", "shop");
    std::fs::create_dir_all(&local).unwrap();
    std::fs::write(local.join("stale.db"), b"must disappear").unwrap();
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    assert!(app.prepare_local(&local).await.is_err());
    // The hand-written fence above tests a running node's stop path. Remove
    // that synthetic request, then drive the real CLI executor end to end.
    store
        .delete(&Key::from("apps/default/shop/restoring"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !wasmkeep_server::apps::live_nodes(store.clone(), "", "default", "shop")
            .await
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
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
    assert!(
        store
            .get(&Key::from("apps/default/shop/restoring"))
            .await
            .is_err()
    );
    app.prepare_local(&local).await.unwrap();
    assert!(!local.join("stale.db").exists());
    std::fs::write(local.join("keep"), b"same incarnation").unwrap();
    app.prepare_local(&local).await.unwrap();
    assert!(local.join("keep").exists());
    let (restarted, follow) = app_node_following("offline", offline.path(), &store)
        .await
        .unwrap();
    assert_eq!(
        read(restarted.addr, "saved").await,
        "1",
        "archived actor lost"
    );
    assert_eq!(
        read(restarted.addr, "absent").await,
        "0",
        "stale actor resurrected from disk"
    );
    assert_eq!(
        read(other.addr, "unrelated").await,
        "0",
        "other app was interrupted"
    );
    follow.abort();
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

/// A staged historical state is written at a new position by the live owner.
#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_replaces_at_a_new_head_and_survives_a_second_node() {
    use wasmkeep_cluster::{actor_restore::Request, control, state_root::StateRoot};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(store.clone(), Arc::new(AtomicBool::new(false))));
    let seen: Arc<dyn ObjectStore> = flaky.clone();
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let a = node_configured(
        "restore-a",
        dirs[0].path(),
        &seen,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |c| {
            #[cfg(feature = "batched-writes")]
            {
                c.batched.insert("t".into());
                c.max_unsent_age = Duration::from_secs(5);
            }
            let _ = c;
        },
    )
    .await;
    let b = node_with(
        "restore-b",
        dirs[1].path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "rewind", 1).await;
    let root = StateRoot::new("", "default", "default");
    let address = wasmkeep_runtime::ActorAddress::new("default", "t", "rewind").unwrap();
    let saved = tempfile::tempdir().unwrap();
    let path = saved.path().join("saved.db");
    let export = wasmkeep_server::history::export(
        &store,
        &root,
        &address,
        wasmkeep_server::history::Point::Head,
        &path,
        1024 * 1024,
    )
    .await
    .unwrap();
    // Snapshot the raw export using the same WAL capture as an owner.
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(saved.path().join("staged")),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "default".into(),
        },
    );
    let staged_path = storage.db_path("t", "rewind");
    std::fs::create_dir_all(staged_path.parent().unwrap()).unwrap();
    std::fs::copy(&path, &staged_path).unwrap();
    let mut db = storage.open("t", "rewind").unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    let bytes = all.to_ltx(1, export.position.txid, 1).unwrap();
    drop(db);
    assert_eq!(
        send(a.addr, "POST", "/actor/t/rewind/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    // Leave plain events pending on a batched hold. The restore must flush
    // them before it can select its reset position.
    #[cfg(feature = "batched-writes")]
    flaky.delay_wal_ms.store(1500, Ordering::SeqCst);
    for n in 1..=3 {
        assert_eq!(
            send(a.addr, "POST", "/actor/t/rewind/plain", "")
                .await
                .body
                .trim(),
            n.to_string()
        );
    }
    let key = control::checked_restore_key(&root, "t", "rewind", "0123456789abcdef").unwrap();
    let request = Request {
        source: "live history".into(),
        size: bytes.len() as u64,
        page_size: all.page_size,
        checksum: all.post_apply.value(),
        if_head: None,
    };
    store
        .put(&Key::from(format!("{key}.ltx")), bytes.into())
        .await
        .unwrap();
    store
        .put(&Key::from(key.clone()), request.encode().into())
        .await
        .unwrap();
    store
        .put(
            &Key::from(control::CHANGED),
            b"restore-test".to_vec().into(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while store.head(&Key::from(format!("{key}.done"))).await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("restore never acknowledged");
    let done = store
        .get(&Key::from(format!("{key}.done")))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let done = std::str::from_utf8(&done).unwrap();
    let (generation, txid) = done.split_once(':').unwrap();
    assert_eq!(
        generation.parse::<u64>().unwrap(),
        export.position.generation,
        "restore abandoned a valid hold instead of flushing it"
    );
    flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    assert!(
        txid.parse::<u64>().unwrap() > export.position.txid,
        "history was rewound"
    );
    let old_path = saved.path().join("old-still-replayable.db");
    wasmkeep_server::history::export_raw(
        &store,
        &root,
        &address,
        wasmkeep_server::history::Point::Txid(export.position.txid),
        &old_path,
        1024 * 1024,
    )
    .await
    .unwrap();
    assert_eq!(read(b.addr, "rewind").await, "1");
    assert_eq!(
        send(b.addr, "POST", "/actor/t/rewind/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    assert_eq!(read(a.addr, "rewind").await, "2");
    assert!(store.head(&Key::from(key)).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_refuses_absent_conflicting_corrupt_and_other_page_size() {
    use wasmkeep_cluster::{
        actor_restore::Request, control, replication::SegmentId, state_root::StateRoot,
    };
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "restore-refusals",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "present", 1).await;
    let root = StateRoot::new("", "default", "default");
    for (i, name, expected, page_size, if_head) in [
        (1, "missing", "absent", 4096, None),
        (
            2,
            "present",
            "conflict",
            4096,
            Some(SegmentId {
                generation: u64::MAX,
                txid: 1,
            }),
        ),
        (3, "present", "refused", 512, None),
        (4, "present", "refused", 4096, None),
    ] {
        let id = format!("{i:016x}");
        let key = control::checked_restore_key(&root, "t", name, &id).unwrap();
        let request = Request {
            source: "test".into(),
            size: 1,
            page_size,
            checksum: 1,
            if_head,
        };
        store
            .put(&Key::from(format!("{key}.ltx")), vec![0].into())
            .await
            .unwrap();
        store
            .put(&Key::from(key.clone()), request.encode().into())
            .await
            .unwrap();
        store
            .put(&Key::from(control::CHANGED), id.into_bytes().into())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            while store
                .head(&Key::from(format!("{key}.{expected}")))
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("missing {expected} outcome for case {i}"));
        if i == 3 {
            let body = store
                .get(&Key::from(format!("{key}.refused")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert!(String::from_utf8_lossy(&body).contains("page size differs"));
        }
        assert_eq!(read(a.addr, "present").await, "1", "refusal changed data");
    }
}

/// Construct a real, checksum-valid historical snapshot, also returning its head.
async fn actor_restore_stage(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> (Vec<u8>, wasmkeep_cluster::actor_restore::Request) {
    let tmp = tempfile::tempdir().unwrap();
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(tmp.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "default".into(),
        },
    );
    let path = storage.db_path("t", name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let export = wasmkeep_server::history::export_raw(
        store,
        &StateRoot::new("", "default", "default"),
        &wasmkeep_runtime::ActorAddress::new("default", "t", name).unwrap(),
        wasmkeep_server::history::Point::Head,
        &path,
        1 << 20,
    )
    .await
    .unwrap();
    let mut db = storage.open("t", name).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    let bytes = all
        .to_ltx(1, export.position.txid, export.timestamp_ms)
        .unwrap();
    let request = wasmkeep_cluster::actor_restore::Request {
        source: "live test history".into(),
        size: bytes.len() as u64,
        page_size: all.page_size,
        checksum: all.post_apply.value(),
        if_head: None,
    };
    (bytes, request)
}

async fn actor_restore_submit(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    id: &str,
    stage: &(Vec<u8>, wasmkeep_cluster::actor_restore::Request),
) -> String {
    let key = wasmkeep_cluster::control::checked_restore_key(
        &StateRoot::new("", "default", "default"),
        "t",
        name,
        id,
    )
    .unwrap();
    store
        .put(&Key::from(format!("{key}.ltx")), stage.0.clone().into())
        .await
        .unwrap();
    store
        .put(&Key::from(key.clone()), stage.1.encode().into())
        .await
        .unwrap();
    store
        .put(
            &Key::from(wasmkeep_cluster::control::CHANGED),
            id.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    key
}

async fn actor_restore_outcome(store: &Arc<dyn ObjectStore>, key: &str, suffix: &str) -> String {
    let outcome = Key::from(format!("{key}.{suffix}"));
    eventually("restore outcome", async || {
        if suffix != "refused"
            && let Ok(got) = store.get(&Key::from(format!("{key}.refused"))).await
        {
            panic!(
                "unexpected restore refusal: {:?}",
                got.bytes().await.unwrap()
            );
        }
        store.head(&outcome).await.is_ok()
    })
    .await;
    String::from_utf8(
        store
            .get(&outcome)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

async fn actor_restore_meta(store: &Arc<dyn ObjectStore>, name: &str, key: &str) -> Option<String> {
    let (bytes, _) = actor_restore_stage(store, name).await;
    let tmp = tempfile::tempdir().unwrap();
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(tmp.path()),
        wasmkeep_storage::StateDir {
            dir: "actors".into(),
            scratch: "scratch".into(),
            tenant: "default".into(),
        },
    );
    let path = storage.db_path("t", name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut replay = wasmkeep_storage::wal::Restorer::create(&path, 1 << 20).unwrap();
    replay.apply(&bytes).unwrap();
    replay.finish().unwrap();
    storage.open("t", name).unwrap().meta(key).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_marker_survives_every_delete_and_replay_does_not_resurrect() {
    for mode in ["delete", "delete-durable", "operator", "expiry"] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(Flaky::new(store.clone(), Arc::new(AtomicBool::new(false))));
        flaky.fail_request_deletes.store(true, Ordering::SeqCst);
        let seen: Arc<dyn ObjectStore> = flaky.clone();
        let dir = tempfile::tempdir().unwrap();
        let a = node_configured(
            "restore-delete",
            dir.path(),
            &seen,
            Duration::from_millis(200),
            &["t"],
            test_actor_wasm(),
            Duration::from_secs(30),
            "",
            |c| {
                if mode == "expiry" {
                    c.expire_after_idle
                        .insert("t".into(), Duration::from_secs(2));
                    c.cluster.as_mut().unwrap().expiry_every = Duration::from_millis(100);
                }
            },
        )
        .await;
        super::deletes::write(a.addr, mode, 1).await;
        let stage = actor_restore_stage(&store, mode).await;
        let key = actor_restore_submit(&store, mode, "1111111111111111", &stage).await;
        actor_restore_outcome(&store, &key, "done").await;
        let marker = actor_restore_meta(&store, mode, "operator_restore")
            .await
            .unwrap();
        assert!(marker.ends_with("1111111111111111"));
        assert_eq!(read(a.addr, mode).await, "1");
        if mode == "operator" {
            request_delete_of(&store, "t", mode).await;
        } else if mode != "expiry" {
            assert_eq!(
                send(a.addr, "POST", &format!("/actor/t/{mode}/{mode}"), "")
                    .await
                    .status,
                200
            );
        }
        eventually("delete recorded", async || {
            actor_restore_meta(&store, mode, "replaced")
                .await
                .is_some_and(|m| m.starts_with("delete "))
        })
        .await;
        assert_eq!(
            actor_restore_meta(&store, mode, "operator_restore").await,
            Some(marker.clone()),
            "{mode} lost restore marker"
        );
        // Keep the original request and bucket timestamp, then restart the
        // owner so its in-memory ended-request suppression cannot hide replay.
        assert!(store.head(&Key::from(key.clone())).await.is_ok());
        store
            .delete(&Key::from(format!("{key}.done")))
            .await
            .unwrap();
        drop(a);
        flaky.fail_request_deletes.store(false, Ordering::SeqCst);
        let next_dir = tempfile::tempdir().unwrap();
        let b = node_with(
            "restore-next",
            next_dir.path(),
            &store,
            Duration::from_secs(30),
            "t",
            test_actor_wasm(),
        )
        .await;
        actor_restore_outcome(&store, &key, "done").await;
        assert_eq!(
            read(b.addr, mode).await,
            "0",
            "{mode}: leftover restore resurrected data"
        );
        // A newly requested undo is allowed, including after a delete.
        let new_key = actor_restore_submit(&store, mode, "2222222222222222", &stage).await;
        actor_restore_outcome(&store, &new_key, "done").await;
        assert_eq!(read(b.addr, mode).await, "1");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_closes_sockets_and_restores_alarm_with_fresh_index() {
    use futures_util::StreamExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(store.clone(), Arc::new(AtomicBool::new(false))));
    let seen: Arc<dyn ObjectStore> = flaky.clone();
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "restore-socket",
        dir.path(),
        &seen,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "socket", 1).await;
    assert_eq!(
        send(a.addr, "POST", "/actor/t/socket/alarm?in=30000", "")
            .await
            .status,
        200
    );
    assert_eq!(
        send(a.addr, "POST", "/actor/t/socket/flush", "")
            .await
            .status,
        200
    );
    let stage = actor_restore_stage(&store, "socket").await;
    assert_eq!(
        send(a.addr, "POST", "/actor/t/socket/alarm?in=60000", "")
            .await
            .status,
        200
    );
    assert_eq!(
        send(a.addr, "POST", "/actor/t/socket/flush", "")
            .await
            .status,
        200
    );
    let old_entry = actor_restore_meta(&store, "socket", "alarm_indexed").await;
    assert!(old_entry.as_ref().is_some_and(|e| !e.is_empty()));
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/socket", a.addr))
        .await
        .unwrap();
    let key = actor_restore_submit(&store, "socket", "3333333333333333", &stage).await;
    *flaky.fail_puts_of.lock().unwrap() = Some(format!("{key}.done"));
    let close = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame))) = ws.next().await
            {
                break frame.unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(u16::from(close.code), 1012);
    let upgrade = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/socket", a.addr)).await;
    match upgrade {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status().as_u16(), 503);
            assert_eq!(response.headers()["retry-after"], "1");
        }
        other => panic!("upgrade admitted during restore: {other:?}"),
    }
    // Close acknowledgement permits the mailbox close callback to finish.
    let _ = ws.close(None).await;
    drop(ws); // Finish the transport so the server can retire its reservation.
    // Busy retries are five seconds apart. Include all three retries and
    // scheduling time when this test runs with the full cluster suite.
    tokio::time::timeout(Duration::from_secs(25), async {
        while actor_restore_meta(&store, "socket", "operator_restore")
            .await
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restore reset never acknowledged despite failed outcome");
    assert!(store.head(&Key::from(format!("{key}.done"))).await.is_err());
    *flaky.fail_puts_of.lock().unwrap() = None;
    actor_restore_outcome(&store, &key, "done").await;
    assert_ne!(
        actor_restore_meta(&store, "socket", "alarm_indexed").await,
        old_entry
    );
    assert_eq!(
        actor_restore_meta(&store, "socket", "alarm_failures")
            .await
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        send(a.addr, "POST", "/actor/t/socket/ws-closed", "")
            .await
            .body
            .trim(),
        "0",
        "close callback ran on restored state"
    );
    assert_eq!(read(a.addr, "socket").await, "1");
    assert_eq!(
        index_entries_for(&store, "socket").await.len(),
        1,
        "stale alarm entry retained"
    );
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            if send(a.addr, "GET", "/actor/t/socket/fired", "")
                .await
                .body
                .trim()
                == "1"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("restored alarm did not fire");
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_predating_delete_is_superseded_after_app_gate_opens() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "restore-gate",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "ordered", 1).await;
    let stage = actor_restore_stage(&store, "ordered").await;
    let gate = Key::from("apps/default/default/restoring");
    store
        .put(&gate, b"whole-app-restore".to_vec().into())
        .await
        .unwrap();
    let key = actor_restore_submit(&store, "ordered", "4444444444444444", &stage).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(store.head(&Key::from(format!("{key}.done"))).await.is_err());
    request_delete_of(&store, "t", "ordered").await;
    eventually("delete before restore", async || {
        actor_restore_meta(&store, "ordered", "replaced")
            .await
            .is_some_and(|m| m.starts_with("delete "))
    })
    .await;
    store.delete(&gate).await.unwrap();
    actor_restore_outcome(&store, &key, "superseded").await;
    assert_eq!(read(a.addr, "ordered").await, "0");
    assert!(store.head(&Key::from(format!("{key}.ltx"))).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_retries_on_a_new_owner_after_upload_failure_and_moves() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(store.clone(), Arc::new(AtomicBool::new(false))));
    let seen: Arc<dyn ObjectStore> = flaky.clone();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let a = node_with(
        "a",
        dirs[0].path(),
        &seen,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "retry", 1).await;
    let stage = actor_restore_stage(&store, "retry").await;
    assert_eq!(
        send(a.addr, "POST", "/actor/t/retry/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    flaky.fail_wal.store(100, Ordering::SeqCst);
    let key = actor_restore_submit(&store, "retry", "5555555555555555", &stage).await;
    eventually("restore upload attempted", async || {
        flaky.fail_wal.load(Ordering::SeqCst) < 100
    })
    .await;
    assert!(store.head(&Key::from(format!("{key}.done"))).await.is_err());
    drop(a);
    let b = node_with(
        "b",
        dirs[1].path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    actor_restore_outcome(&store, &key, "done").await;
    assert_eq!(read(b.addr, "retry").await, "1");
    let c = node_with(
        "c",
        dirs[2].path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let moved = request_move(&store, "retry", "c").await;
    let next = actor_restore_submit(&store, "retry", "6666666666666666", &stage).await;
    actor_restore_outcome(&store, &next, "done").await;
    eventually("move completed", async || store.head(&moved).await.is_err()).await;
    assert_eq!(read(c.addr, "retry").await, "1");
    assert_eq!(
        send(c.addr, "POST", "/actor/t/retry/durable", "")
            .await
            .body
            .trim(),
        "2"
    );
    assert_eq!(read(b.addr, "retry").await, "2");
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_unexecutable_request_does_not_block_websocket() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "restore-gate",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "gate", 1).await;
    let stage = actor_restore_stage(&store, "gate").await;
    store.put(&Key::from("nodes/old"), b"wasmkeep-node 2\nold\nhttp://old\n0.4.2\n0\ndefault/default\n\n\nserving\n\ntake ends restore\n".to_vec().into()).await.unwrap();
    let key = actor_restore_submit(&store, "gate", "7777777777777777", &stage).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(store.head(&Key::from(format!("{key}.done"))).await.is_err());
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/gate", a.addr))
        .await
        .expect("an unexecutable request must not gate sockets");
    let _ = ws.close(None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_build_failure_cleans_fresh_alarm_entries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "restore-cleanup",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    super::deletes::write(a.addr, "cleanup", 1).await;
    assert_eq!(
        send(a.addr, "POST", "/actor/t/cleanup/alarm?in=120000", "")
            .await
            .status,
        200
    );
    assert_eq!(
        send(a.addr, "POST", "/actor/t/cleanup/flush", "")
            .await
            .status,
        200
    );
    let stage = actor_restore_stage(&store, "cleanup").await;
    for _ in 0..100 {
        wasmkeep_storage::faults::fail_next("set_meta:operator_restore");
    }
    actor_restore_submit(&store, "cleanup", "8888888888888888", &stage).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        index_entries_for(&store, "cleanup").await.len(),
        1,
        "failed builds leaked fresh entries"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_marker_read_error_does_not_break_strict_durable_delete() {
    for mode in ["delete", "delete-durable"] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let a = node_with(
            "delete-meta-error",
            dir.path(),
            &store,
            Duration::from_secs(30),
            "t",
            test_actor_wasm(),
        )
        .await;
        super::deletes::write(a.addr, "meta-error", 1).await;
        let stage = actor_restore_stage(&store, "meta-error").await;
        let gate = Key::from("apps/default/default/restoring");
        store.put(&gate, vec![1].into()).await.unwrap();
        let key = actor_restore_submit(&store, "meta-error", "9999999999999999", &stage).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        wasmkeep_storage::faults::fail_next("meta:operator_restore");
        let reply = send(a.addr, "POST", &format!("/actor/t/meta-error/{mode}"), "").await;
        assert_eq!(reply.status, 200, "{}", reply.body);
        assert_eq!(read(a.addr, "meta-error").await, "0");
        store.delete(&gate).await.unwrap();
        actor_restore_outcome(&store, &key, "superseded").await;
        assert_eq!(
            read(a.addr, "meta-error").await,
            "0",
            "unreadable marker allowed replay"
        );
    }
}

// Age only the old history's bucket timestamps: no hour-long wall-clock wait,
// and collection itself must remove the reset and after-delete snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn actor_restore_after_delete_reset_collected() {
    for retention_secs in [3600, 3002] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(Flaky::new(store.clone(), Arc::new(AtomicBool::new(false))));
        let seen: Arc<dyn ObjectStore> = flaky.clone();
        let dir = tempfile::tempdir().unwrap();
        let a = node_configured(
            "collected",
            dir.path(),
            &seen,
            Duration::from_millis(200),
            &["t"],
            test_actor_wasm(),
            Duration::from_secs(30),
            "",
            |c| {
                let cluster = c.cluster.as_mut().unwrap();
                cluster.history_retention = Duration::from_secs(retention_secs);
                // Default skew: bound = 3000 s + 2 * 1 s + 1 s = 3003 s.
                cluster.lease = LeaseConfig::default_safe();
            },
        )
        .await;
        super::deletes::write(a.addr, "aged", 1).await;
        let stage = actor_restore_stage(&store, "aged").await;
        assert_eq!(
            send(a.addr, "POST", "/actor/t/aged/delete-durable", "")
                .await
                .status,
            200
        );
        eventually("delete snapshot", async || {
            let (snapshots, segments) = history_of(&store, "aged").await;
            snapshots.len() == 1 && segments.len() == 1 && snapshots == segments
        })
        .await;
        let old = actor_keys(&store, "aged").await;
        flaky
            .aged_history
            .lock()
            .unwrap()
            .extend(old.iter().cloned());
        super::deletes::write(a.addr, "aged", 64).await;
        eventually("ordinary collection removed delete evidence", async || {
            let keys = actor_keys(&store, "aged").await;
            old.iter().all(|key| !keys.contains(key))
        })
        .await;
        assert!(
            actor_restore_meta(&store, "aged", "replaced")
                .await
                .unwrap()
                .starts_with("delete ")
        );
        let key = actor_restore_submit(&store, "aged", "7777777777777777", &stage).await;
        if retention_secs == 3600 {
            actor_restore_outcome(&store, &key, "done").await;
            assert_eq!(read(a.addr, "aged").await, "1");
        } else {
            let error = actor_restore_outcome(&store, &key, "refused").await;
            assert!(error.contains("history retention"), "{error}");
            assert!(error.contains("3003000"), "{error}");
        }
    }
}
