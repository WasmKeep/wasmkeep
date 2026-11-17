//! R2: the SDK room itself loses unsent batched rows on takeover. The HTTP
//! front reconnects using the position returned before the failed upload.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_reconnect_announces_unsent_numbers_void() {
    let wasm = guests::build_guest("../wasmkeep-sdk/tests/room-guest", "room_guest");
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(shared.clone(), Arc::new(AtomicBool::new(false))));
    let seen: Arc<dyn ObjectStore> = flaky.clone();
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        da.path(),
        &seen,
        Duration::from_secs(30),
        &["t"],
        &wasm,
        Duration::from_secs(30),
        "",
        |c| {
            c.batched.insert("t".into());
            c.max_unsent_age = Duration::from_secs(5);
        },
    )
    .await;
    let b = node_with("b", db.path(), &shared, Duration::from_secs(30), "t", &wasm).await;
    settle().await;
    // The cold-start epoch is durable before we obstruct uploads. No messages yet.
    let warm = send(a.addr, "GET", "/actor/t/front/warm", "").await;
    assert_eq!((warm.status, warm.body.trim()), (200, "1.0"));
    let kept = send(a.addr, "GET", "/actor/t/front/publish", "").await;
    assert_eq!((kept.status, kept.body.trim()), (200, "1.1"));
    let flushed = send(a.addr, "GET", "/actor/t/room/flush", "").await;
    assert_eq!((flushed.status, flushed.body.trim()), (200, "ok"));
    // Freeze the upload, rather than racing a timed delay. These messages have
    // been numbered and answered but cannot reach the bucket.
    flaky.hold_wal_puts.send_replace(true);
    let first_lost = send(a.addr, "GET", "/actor/t/front/publish", "").await;
    assert_eq!((first_lost.status, first_lost.body.trim()), (200, "1.2"));
    let published = send(a.addr, "GET", "/actor/t/front/publish", "").await;
    assert_eq!((published.status, published.body.trim()), (200, "1.3"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while flaky.held_wal_puts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    flaky.down.store(true, Ordering::SeqCst);
    // The old owner is fenced by the real cluster; the new owner restores only
    // uploaded history. Do not gracefully stop/flush the old owner.
    let path = format!(
        "/actor/t/reconnecting-front/reconnect?after={}",
        published.body.trim()
    );
    let started = std::time::Instant::now();
    loop {
        let r = poll_get(b.addr, &path).await;
        // Before its lease expires, the old node can still answer a forwarded
        // read from memory. Only a newer epoch proves the room was restored.
        if r.status == 200 && !r.body.starts_with("head=1.") {
            assert_eq!(r.body.trim(), "head=2.1 lost=2.1 reset=false messages=0");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "reconnect: {} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let reused = send(b.addr, "GET", "/actor/t/reconnecting-front/publish", "").await;
    assert_eq!((reused.status, reused.body.trim()), (200, "2.2"));
}
