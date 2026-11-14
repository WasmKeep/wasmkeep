//! Draining a node and moving actors hand them, their sockets and alarms to a peer.

use super::*;

/// ADR 0014: the operator's drain marker reaches the node's heartbeat
/// (`drained` at once: it holds no actors); a failed listing of `control/`
/// keeps the node drained (it never undrains on its own); removing the
/// marker returns it to serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_marker_reaches_the_heartbeat_and_a_failed_listing_keeps_it() {
    use object_store::ObjectStoreExt as _;
    let down = Arc::new(AtomicBool::new(false));
    let flaky = Arc::new(Flaky::new(Arc::new(InMemory::new()), down));
    let fail_list = Arc::clone(&flaky.fail_control_list);
    let store: Arc<dyn ObjectStore> = flaky;
    let data = tempfile::tempdir().unwrap();
    let _a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    wait_drain_line(&store, "a", "serving").await;

    let marker = object_store::path::Path::from("control/drain/a");
    store.put(&marker, Vec::new().into()).await.unwrap();
    wait_drain_line(&store, "a", "drained").await;

    fail_list.store(true, Ordering::SeqCst);
    store.delete(&marker).await.unwrap();
    // Several heartbeats with the listing failing: still draining.
    tokio::time::sleep(Duration::from_millis(600)).await;
    wait_drain_line(&store, "a", "drained").await;

    fail_list.store(false, Ordering::SeqCst);
    wait_drain_line(&store, "a", "serving").await;
}

/// ADR 0014: a draining node claims nothing new. A request and a
/// WebSocket upgrade for actors nobody holds go to the other node, which
/// claims them; a request already forwarded gets 503 with Retry-After.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_node_sends_new_actors_to_a_peer() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let _b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    drain(&store, "a").await;
    let r = send(a.addr, "GET", "/actor/t/new/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    assert_eq!(lease_holder(&store, "new").await.as_deref(), Some("b"));

    // Already forwarded once: not forwarded again, not claimed.
    let r = send(
        a.addr,
        "GET",
        "/actor/t/fwd/fired",
        "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n",
    )
    .await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "{}",
        r.body
    );
    assert_eq!(lease_holder(&store, "fwd").await, None);

    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/sock", a.addr))
        .await
        .unwrap();
    assert_eq!(lease_holder(&store, "sock").await.as_deref(), Some("b"));
    drop(ws);
}

/// ADR 0014: a draining node hands every resident actor over. Their data
/// is on the peer, the node reports `drained`, and an alarm set before the
/// drain fires on the peer within a second of its time, not after the
/// sweep's margin.
#[tokio::test(flavor = "multi_thread")]
async fn draining_hands_resident_actors_over_with_their_alarms() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    // A slow sweep: only the hand-over can deliver the alarm on time.
    let slow_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().alarm_sweep = Duration::from_secs(60);
    };
    let wall = Duration::from_secs(30);
    let wasm = test_actor_wasm();
    let a = node_configured(
        "a",
        da.path(),
        &store,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    let b = node_configured(
        "b",
        db.path(),
        &store,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    for i in 0..5 {
        let r = send(a.addr, "POST", &format!("/actor/t/x{i}/durable"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref(), r.body.trim()),
            (200, Some("a"), "1")
        );
    }
    // Due well after the hand-over, before the sweep would deliver it.
    let due = now_ms() + 3_000;
    let r = send(a.addr, "POST", &format!("/actor/t/al/alarm?at={due}"), "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    drain(&store, "a").await;
    wait_drain_line(&store, "a", "drained").await;
    for i in 0..5 {
        assert_eq!(
            lease_holder(&store, &format!("x{i}")).await,
            None,
            "x{i} released"
        );
    }
    // The operator stops the drained node: what it handed over must not
    // depend on it any more (its timer held the alarm).
    drop(a);
    for i in 0..5 {
        let r = send(b.addr, "GET", &format!("/actor/t/x{i}/durable"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref(), r.body.trim()),
            (200, Some("b"), "1")
        );
    }
    // The alarm fires on b, from b's timer, on time. Not asked before:
    // a request to the actor would restore it on b and arm the alarm there
    // whatever the hand-over did.
    tokio::time::sleep(Duration::from_millis(due + 800 - now_ms())).await;
    let r = send(b.addr, "GET", "/actor/t/al/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1"),
        "the alarm did not fire within 800 ms of its time"
    );
}

/// ADR 0014: a node that was handed an alarm while serving, then drains,
/// hands that alarm on before it reports `drained`: stopping it then
/// leaves nothing to the sweep (found by the drain simulation).
#[tokio::test(flavor = "multi_thread")]
async fn a_drained_node_holds_no_alarm_it_was_handed() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let slow_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().alarm_sweep = Duration::from_secs(60);
    };
    let (wall, wasm) = (Duration::from_secs(30), test_actor_wasm());
    let a = node_configured(
        "a",
        da.path(),
        &store,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    let b = node_configured(
        "b",
        db.path(),
        &store,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let due = now_ms() + 5_000;
    let r = send(a.addr, "POST", &format!("/actor/t/al/alarm?at={due}"), "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    // a hands the actor and its alarm to b, and comes back.
    drain(&store, "a").await;
    wait_drain_line(&store, "a", "drained").await;
    store
        .delete(&object_store::path::Path::from("control/drain/a"))
        .await
        .unwrap();
    wait_drain_line(&store, "a", "serving").await;
    settle().await;
    // b, holding only the alarm (not the actor), drains and is stopped.
    drain(&store, "b").await;
    wait_drain_line(&store, "b", "drained").await;
    drop(b);
    assert!(now_ms() < due, "the test took too long to prove anything");

    tokio::time::sleep(Duration::from_millis(due + 800 - now_ms())).await;
    let r = send(a.addr, "GET", "/actor/t/al/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("a"), "1"),
        "the alarm did not fire within 800 ms of its time"
    );
}

/// ADR 0014, 0020: a draining node whose hold a bucket outage ended hands
/// its alarm off while the actor's record still names it (the release
/// waits in the background). The peer, which has not listed the drain
/// yet, looks again instead of leaving the alarm to that node, and runs it
/// once the release lands (found by the drain simulation).
#[tokio::test(flavor = "multi_thread")]
async fn a_handed_alarm_waits_for_the_senders_release() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down_a = Arc::new(AtomicBool::new(false));
    let flaky_a = Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down_a)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let (seen_by_a, seen_by_b): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) =
        (flaky_a.clone(), flaky_b.clone());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    // A slow sweep: only the hand-off can deliver the alarm in time.
    let slow_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().alarm_sweep = Duration::from_secs(60);
    };
    let (wall, wasm) = (Duration::from_secs(30), test_actor_wasm());
    let a = node_configured(
        "a",
        da.path(),
        &seen_by_a,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    let b = node_configured(
        "b",
        db.path(),
        &seen_by_b,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    wait_drain_line(&shared, "b", "serving").await;
    settle().await;
    // a holds the actor, with an alarm due while a long event keeps it
    // busy: the drain's hand-over job waits behind that event.
    let due = now_ms() + 2_500;
    let r = send(a.addr, "POST", &format!("/actor/t/al/alarm?at={due}"), "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let busy = tokio::spawn(send(a.addr, "POST", "/actor/t/al/sleep?ms=3500", ""));
    // b keeps its last control/ listing: it never learns that a drains.
    flaky_b.fail_control_list.store(true, Ordering::SeqCst);
    drain(&shared, "a").await;
    // a loses the bucket past its lease's validity (ttl - safety), which
    // ends its hold; then only its ownership-record writes keep failing:
    // its node lease is live again, its release waits in the background.
    // Short of what b fences (a lease period and an attempt since a's
    // last write, and past its written_at plus ttl and grace).
    down_a.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(900)).await;
    flaky_a.fail_owner_puts.store(true, Ordering::SeqCst);
    down_a.store(false, Ordering::SeqCst);
    // The event ends; the hand-over finds the hold lapsed; the alarm,
    // due meanwhile, goes from a to b, which finds the record naming a.
    let _ = busy.await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        lease_holder(&shared, "al").await.as_deref(),
        Some("a"),
        "a's release should still be pending"
    );
    // The release lands: b, looking again, takes the actor and runs the
    // alarm. Not asked before: a request would restore the actor on b and
    // arm the alarm whatever the hand-off did.
    flaky_a.fail_owner_puts.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(6)).await;
    let r = send(b.addr, "GET", "/actor/t/al/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1"),
        "the handed alarm was left to a, whose release had not landed"
    );
    drop(a);
}

/// ADR 0016: a node that cannot open the app's secrets drains: its actors
/// move, with their data, to a node that can, new ones go there too, and
/// its heartbeat stops saying `serving`, so no peer sends it any. Open
/// again, it serves again; when no node can, every event is 503.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_cannot_open_the_secrets_hands_its_actors_over() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("a"), "1")
    );

    a.service.set_secrets_blocked(true);
    wait_drain_line(&store, "a", "drained").await;
    let r = send(a.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );
    let r = send(a.addr, "GET", "/actor/t/new/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );

    // Open again: a serves and takes actors again.
    a.service.set_secrets_blocked(false);
    wait_drain_line(&store, "a", "serving").await;
    settle().await;
    let r = send(a.addr, "GET", "/actor/t/back/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    // Neither can: nobody runs the app's actors, and every event is 503.
    a.service.set_secrets_blocked(true);
    b.service.set_secrets_blocked(true);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = send(a.addr, "GET", "/actor/t/other/fired", "").await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "{}",
        r.body
    );
}

/// ADR 0017: `wasmkeep actor move` moves a resident actor, under requests,
/// to the named node: its data goes with it, requests that reach the old
/// owner are forwarded, the request is deleted, and an alarm set before
/// fires on the new owner on time even once the old one is stopped.
#[tokio::test(flavor = "multi_thread")]
async fn a_resident_actor_moves_to_the_named_node() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    // A slow sweep: only the move can deliver the alarm on time.
    let slow_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().alarm_sweep = Duration::from_secs(60);
    };
    let (wall, wasm) = (Duration::from_secs(30), test_actor_wasm());
    let a = node_configured(
        "a",
        da.path(),
        &store,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    // b sees the bucket through a store whose lease reads can be slowed:
    // its take then leaves a window after a's release, where a must not
    // claim the actor back.
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&store),
        Arc::new(AtomicBool::new(false)),
    ));
    let slow_leases = Arc::clone(&flaky.delay_lease_get_ms);
    let seen_by_b: Arc<dyn ObjectStore> = flaky;
    let b = node_configured(
        "b",
        db.path(),
        &seen_by_b,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/m/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("a"), "1")
    );
    // A second actor, idle during its move, with an alarm: only the take
    // can arm it on b (no event runs there to register it).
    let r = send(a.addr, "POST", "/actor/t/q/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    let due = now_ms() + 6_000;
    let r = send(a.addr, "POST", &format!("/actor/t/q/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);

    // Six clients keep sending to a while m moves: some land after a's
    // release and before b holds it, where a must not claim m back.
    let traffic: Vec<_> = (0..6)
        .map(|_| {
            let addr = a.addr;
            tokio::spawn(async move {
                let mut statuses = Vec::new();
                for _ in 0..30 {
                    let r = send(addr, "GET", "/actor/t/m/durable", "").await;
                    statuses.push((r.status, r.retry_after));
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                statuses
            })
        })
        .collect();
    slow_leases.store(400, Ordering::SeqCst);
    let key = request_move(&store, "m", "b").await;
    eventually("the move request is deleted", async || {
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        )
    })
    .await;
    slow_leases.store(0, Ordering::SeqCst);
    assert_eq!(lease_holder(&store, "m").await.as_deref(), Some("b"));
    for client in traffic {
        for (status, retry_after) in client.await.unwrap() {
            assert!(
                status == 200 || (status == 503 && retry_after.as_deref() == Some("1")),
                "{status} {retry_after:?}"
            );
        }
    }
    assert_eq!(
        lease_holder(&store, "m").await.as_deref(),
        Some("b"),
        "still b's after the traffic"
    );
    let r = send(a.addr, "GET", "/actor/t/m/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );

    // q moves with no traffic at all.
    let key = request_move(&store, "q", "b").await;
    eventually("q's move request is deleted", async || {
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        )
    })
    .await;
    assert_eq!(lease_holder(&store, "q").await.as_deref(), Some("b"));

    // The old owner stops; q's alarm is b's now, armed by the take.
    drop(a);
    assert!(now_ms() < due, "the test took too long to prove anything");
    tokio::time::sleep(Duration::from_millis(due + 800 - now_ms())).await;
    let r = send(b.addr, "GET", "/actor/t/q/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1"),
        "the alarm did not fire within 800 ms of its time"
    );
}

/// ADR 0017: an actor nobody holds is taken by the named node; a name that
/// was never an actor is not created; a request to move an actor to its
/// current owner is deleted; one to a node that cannot take it is kept
/// (until it expires) and the actor stays.
#[tokio::test(flavor = "multi_thread")]
async fn moves_of_idle_unknown_and_unmovable_actors() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // Short idle timeout: the actor hibernates (releasing its lease) on a.
    let idle = Duration::from_millis(300);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/idle/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    eventually("a releases the idle actor", async || {
        lease_holder(&store, "idle").await.is_none()
    })
    .await;

    // Nobody holds it: b takes it.
    let key = request_move(&store, "idle", "b").await;
    eventually("b takes the idle actor", async || {
        lease_holder(&store, "idle").await.as_deref() == Some("b")
    })
    .await;
    eventually("the request is deleted", async || {
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        )
    })
    .await;

    // Never an actor: not created, request dropped.
    let key = request_move(&store, "typo", "b").await;
    eventually("the typo request is deleted", async || {
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        )
    })
    .await;
    assert_eq!(lease_holder(&store, "typo").await, None);

    // To the node that holds it: dropped, the actor stays.
    let key = request_move(&store, "idle", "b").await;
    eventually("a move to the owner is deleted", async || {
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        )
    })
    .await;
    assert_eq!(lease_holder(&store, "idle").await.as_deref(), Some("b"));

    // To a node that cannot take it (draining): kept, the actor stays.
    drain(&store, "a").await;
    let key = request_move(&store, "idle", "a").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(store.head(&key).await.is_ok(), "kept until it expires");
    let r = send(b.addr, "GET", "/actor/t/idle/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );
}

/// ADR 0014: a socket on an actor being handed over closes with 1012 and
/// its close event runs on the draining node, before the lease goes.
#[tokio::test(flavor = "multi_thread")]
async fn draining_closes_sockets_and_runs_their_close_events_first() {
    use futures_util::StreamExt;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let _b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/s", a.addr))
        .await
        .unwrap();
    assert_eq!(lease_holder(&store, "s").await.as_deref(), Some("a"));

    drain(&store, "a").await;
    let code = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(msg) = ws.next().await {
            if let Ok(tokio_tungstenite::tungstenite::Message::Close(Some(frame))) = msg {
                return u16::from(frame.code);
            }
        }
        0
    })
    .await
    .unwrap();
    assert_eq!(code, 1012);
    // Keep reading, as a client does: that sends the close reply, after
    // which the server runs the close event.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while ws.next().await.is_some() {}
    })
    .await;
    wait_drain_line(&store, "a", "drained").await;
    let r = send(a.addr, "POST", "/actor/t/s/ws-closed", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );
}

/// ADR 0014: a chosen peer that stops is not chosen again: after at most
/// one failed request, the draining node (with no other peer) claims.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_node_stops_choosing_a_peer_that_is_down() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    drain(&store, "a").await;
    let r = send(a.addr, "GET", "/actor/t/first/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );

    stop_listening(b).await; // its heartbeat is still live for seconds
    let mut failed = 0;
    for i in 0..5 {
        let r = send(a.addr, "GET", &format!("/actor/t/later-{i}/fired"), "").await;
        if r.status == 200 {
            assert_eq!(r.node.as_deref(), Some("a"), "{}", r.body);
        } else {
            failed += 1;
        }
    }
    assert!(failed <= 1, "{failed} of 5 requests failed");
}

/// ADR 0014: with no other node to take its actors, a draining node claims
/// as usual (draining never makes an actor unreachable) and reports `stuck`.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_node_alone_still_serves() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    drain(&store, "a").await;
    let r = send(a.addr, "GET", "/actor/t/new/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    wait_drain_line(&store, "a", "stuck").await;
}

/// Payload line 10 is read as each node lease write is built: the
/// actors a node took show by its next write, and a heartbeat that says
/// `drained` never carries actors counted before the hand-over (load and
/// drain state of one moment).
#[tokio::test(flavor = "multi_thread")]
async fn a_heartbeat_carries_load_and_drain_state_of_one_moment() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let _b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    wait_drain_line(&store, "b", "serving").await;
    settle().await;
    let heartbeat = async |id: &str| {
        let key = object_store::path::Path::from(format!("nodes/{id}"));
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let actors = body
            .lines()
            .find_map(|l| l.strip_prefix("load actors="))
            .and_then(|rest| rest.split(' ').next())
            .map(|n| n.parse::<u64>().unwrap());
        (drain_line(&body).map(str::to_owned), actors)
    };
    for i in 0..3 {
        let r = send(a.addr, "POST", &format!("/actor/t/x{i}/durable"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "{}",
            r.body
        );
    }
    // Within a write or two (100 ms apart here), not a sampler period on.
    eventually("a's heartbeat shows its 3 actors", async || {
        heartbeat("a").await.1 == Some(3)
    })
    .await;

    drain(&store, "a").await;
    let started = std::time::Instant::now();
    loop {
        let (state, actors) = heartbeat("a").await;
        if state.as_deref() == Some("drained") {
            assert_eq!(actors, Some(0), "drained with actors");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a never drained: {state:?} {actors:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
