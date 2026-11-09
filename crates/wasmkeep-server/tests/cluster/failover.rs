//! Actors are taken over or handed over when their node stops or loses its lease.

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_hibernated_actor_can_move_to_another_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_millis(200);
    let a = node("a", da.path(), &store, idle).await;
    let b = node("b", db.path(), &store, idle).await;
    settle().await;

    for expected in ["1", "2"] {
        let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
        assert_eq!((r.body.trim(), r.node.as_deref()), (expected, Some("a")));
    }

    // a hibernates alice and releases the lease; b takes it at once and
    // restores its database from the bucket.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let r = send(b.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "the released actor moved");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "3"),
        "and its data with it"
    );

    // And back again: a's local copy is stale now, so a restores too.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.body.trim(), r.node.as_deref()), ("4", Some("a")));

    // Each object those restores read was timed (both nodes run in this
    // test's process, which has the metrics to itself).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let (status, page) = get_raw(metrics, "/metrics").await;
    assert_eq!(status, 200);
    let reads: u64 = page
        .lines()
        .find_map(|l| l.strip_prefix("wasmkeep_restore_read_seconds_count "))
        .unwrap_or_else(|| panic!("no restore reads in\n{page}"))
        .trim()
        .parse()
        .unwrap();
    assert!(reads >= 2, "{reads} objects read by two restores");
}

#[tokio::test(flavor = "multi_thread")]
async fn names_that_need_escaping_survive_failover_too() {
    // Uppercase letters, spaces and `%` are escaped in file names and keys.
    for name in ["Alice", "room%201", "a.b~c"] {
        durable_write_survives_failover(name).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_stops_when_the_lease_runs_out() {
    let down = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::new(InMemory::new()), Arc::clone(&down)));
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;

    // A 5 s event; the bucket becomes unreachable 300 ms in, so renewals
    // fail and the grant (ttl 1 s - safety 200 ms) runs out long before.
    let started = std::time::Instant::now();
    let event = tokio::spawn(send(a.addr, "POST", "/actor/t/x/sleep?ms=5000", ""));
    tokio::time::sleep(Duration::from_millis(300)).await;
    down.store(true, Ordering::SeqCst);
    let r = event.await.unwrap();
    let took = started.elapsed();
    assert_eq!(r.status, 500, "the event was stopped, not completed");
    assert!(
        took < Duration::from_millis(2_500),
        "stopped when the lease ran out, not at the end of the event ({took:?})"
    );

    // Its write never committed. Another node could own the actor meanwhile;
    // this one did nothing after its grant ended. Once the bucket is back,
    // the node renews its lease (it acquires nothing before, ADR 0020).
    down.store(false, Ordering::SeqCst);
    let started = std::time::Instant::now();
    while a.service.not_ready().is_some() {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "never ready again"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let r = send(a.addr, "GET", "/actor/t/x/slept", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_shutdown_hands_actors_over_at_once() {
    // With 10 s leases, a node that stops without releasing its actors
    // leaves them unreachable for ~11 s (ttl + grace). A clean shutdown
    // releases them: the other node serves at once, with the data.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, stop_a, served_a) = long_lease_node("a", da.path(), &store).await;
    let (b, _stop_b, _served_b) = long_lease_node("b", db.path(), &store).await;
    settle().await;

    for expected in ["1", "2"] {
        let r = send(a, "POST", "/actor/counter/alice", "").await;
        assert_eq!((r.status, r.body.trim()), (200, expected));
        assert_eq!(r.node.as_deref(), Some("a"));
    }

    drop(stop_a);
    tokio::time::timeout(Duration::from_secs(15), served_a)
        .await
        .expect("a did not finish shutting down")
        .unwrap()
        .unwrap();

    let stopped = std::time::Instant::now();
    loop {
        let r = send(b, "POST", "/actor/counter/alice", "").await;
        if r.status == 200 {
            assert_eq!(r.body.trim(), "3", "the count survived the handover");
            assert_eq!(r.node.as_deref(), Some("b"));
            break;
        }
        assert!(
            stopped.elapsed() < Duration::from_secs(3),
            "b could not serve alice {:?} after a stopped (last: {} {})",
            stopped.elapsed(),
            r.status,
            r.body.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `wasmkeep_failovers_total` counts an actor taken over from a node whose
/// lease expired, not one handed over when its owner released it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failover_is_counted_and_a_hand_off_is_not() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_millis(200);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let failovers = || async {
        let (_, page) = get_raw(metrics, "/metrics").await;
        page.lines()
            .find_map(|l| l.strip_prefix("wasmkeep_failovers_total "))
            .unwrap_or_else(|| panic!("no failovers in\n{page}"))
            .trim()
            .parse::<u64>()
            .unwrap()
    };

    // Hand-off: a hibernates the actor and releases its lease; b takes it.
    let r = send(a.addr, "POST", "/actor/t/handed/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    tokio::time::sleep(Duration::from_millis(600)).await;
    let r = send(b.addr, "GET", "/actor/t/handed/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("b")));
    assert_eq!(failovers().await, 0, "a released the lease");

    // Failover: a is cut off from the bucket while holding an actor, so its
    // lease expires unreleased and b takes the actor over.
    let r = send(a.addr, "POST", "/actor/t/stranded/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/stranded/durable", "").await;
        if r.node.as_deref() == Some("b") {
            assert_eq!(r.status, 200, "{}", r.body);
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "b never took over"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(failovers().await, 1);
}
