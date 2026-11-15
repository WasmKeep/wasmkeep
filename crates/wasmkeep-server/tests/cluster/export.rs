//! Verified exports while durable writes run, across a killed owner, and resets.
use super::*;
use wasmkeep_runtime::ActorAddress;
use wasmkeep_server::history::{self, Point};
use wasmkeep_storage::{SqlValue, readonly};

async fn read_export(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    point: Point,
) -> (history::Export, Option<i64>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("export.db");
    let result = history::export(
        store,
        &StateRoot::new("", "default", "default"),
        &ActorAddress::new("default", "t", name).unwrap(),
        point,
        &path,
        1 << 20,
    )
    .await
    .unwrap();
    let n = if result.verification.tables.iter().any(|t| t == "config") {
        let rows = readonly::query(&path, "SELECT value FROM config WHERE key='durable'").unwrap();
        rows.values.first().map(|r| {
            let SqlValue::Integer(n) = r[0] else {
                panic!("not integer")
            };
            n
        })
    } else {
        None
    };
    assert_eq!(&std::fs::read(&path).unwrap()[18..20], &[1, 1]);
    (result, n)
}

#[tokio::test(flavor = "multi_thread")]
async fn exports_during_writes_and_after_a_kill_keep_every_acknowledged_durable_write() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let da = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &seen_by_a,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let first = send(a.addr, "POST", "/actor/t/export/durable", "").await;
    assert_eq!((first.status, first.body.trim()), (200, "1"));
    let acked = Arc::new(AtomicUsize::new(1));
    let sent = Arc::new(AtomicUsize::new(1));
    let writer = {
        let acked = Arc::clone(&acked);
        let sent = Arc::clone(&sent);
        let addr = a.addr;
        tokio::spawn(async move {
            for i in 2..=30 {
                sent.store(i, Ordering::SeqCst);
                let r = send(addr, "POST", "/actor/t/export/durable", "").await;
                assert_eq!((r.status, r.body.trim()), (200, i.to_string().as_str()));
                acked.store(i, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let mut exports = 0;
    while !writer.is_finished() || exports < 3 {
        let before = acked.load(Ordering::SeqCst) as i64;
        let (result, n) = read_export(&shared, "export", Point::Head).await;
        let n = n.unwrap();
        assert!(
            n >= before && n <= sent.load(Ordering::SeqCst) as i64,
            "{before} <= {n}; {result:?}"
        );
        exports += 1;
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    writer.await.unwrap();
    // Same kill model as node_leases: deny the old process all further bucket
    // writes BEFORE stopping its listener, so shutdown cannot release a lease.
    down.store(true, Ordering::SeqCst);
    stop_listening(a).await;
    assert_eq!(
        read_export(&shared, "export", Point::Head).await.1,
        Some(30)
    );
    let db = tempfile::tempdir().unwrap();
    let b = node_with(
        "b",
        db.path(),
        &shared,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(deletes::read(b.addr, "export").await, "30");
    let r = send(b.addr, "POST", "/actor/t/export/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "31"));
    let (result, n) = read_export(&shared, "export", Point::Head).await;
    assert_eq!(n, Some(31));
    assert_eq!(result.owner_generation, Some(result.position.generation));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_actor_exports_empty_and_a_reset_never_resurrects_its_old_rows() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        dir.path(),
        &shared,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    deletes::write(a.addr, "gone", 3).await;
    let (before, _) = read_export(&shared, "gone", Point::Head).await;
    let r = send(a.addr, "POST", "/actor/t/gone/delete-durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"));
    let (deleted, n) = read_export(&shared, "gone", Point::Head).await;
    assert_eq!(n, None);
    assert!(deleted.verification.tables.is_empty());
    assert!(deleted.position.txid > before.position.txid);
    // Wait for asynchronous erasure, then old positions must be refused.
    let scratch = tempfile::tempdir().unwrap();
    let root = StateRoot::new("", "default", "default");
    let actor = ActorAddress::new("default", "t", "gone").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let path = scratch.path().join("old.db");
        match history::export(
            &shared,
            &root,
            &actor,
            Point::Txid(before.position.txid),
            &path,
            1 << 20,
        )
        .await
        {
            Err(e) => {
                assert!(e.to_string().contains("retained history"), "{e}");
                break;
            }
            Ok(_) => {
                std::fs::remove_file(path).unwrap();
            }
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let r = send(a.addr, "POST", "/actor/t/gone/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    assert_eq!(read_export(&shared, "gone", Point::Head).await.1, Some(1));
}
