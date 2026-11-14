//! Node identity, the start check, twins, restarts, fences and the node lease.

use super::*;

/// ADR 0020, section 4: a node shut down cleanly whose first `released`
/// write is refused tries again within the stop's limit, so its node lease
/// ends `released` (peers need not fence it) and its actor is free.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_releases_the_node_lease_after_a_refused_try() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = flaky.clone();
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
    let r = send(a.addr, "POST", "/actor/t/held/durable", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    flaky.fail_released.store(1, Ordering::SeqCst);
    a.service.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        flaky.fail_released.load(Ordering::SeqCst),
        0,
        "a try was refused"
    );
    let body = shared
        .get(&object_store::path::Path::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("\nreleased "),
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        lease_holder(&shared, "held").await,
        None,
        "the actor is free"
    );
    drop(a);
}

/// A store that accepts conditional writes and ignores their conditions
/// would let two nodes own one actor. A node proves the conditions hold on
/// a probe object before anything else, and refuses to run otherwise: no
/// heartbeat, no lease, not ready, and `serve` exits with why.
#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_that_ignores_conditional_writes_is_refused() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    flaky.ignore_conditions.store(true, Ordering::SeqCst);
    let lax: Arc<dyn ObjectStore> = flaky;
    let dir = tempfile::tempdir().unwrap();
    let a = node("a", dir.path(), &lax, Duration::from_secs(30)).await;
    let why = a.service.node_id_checked().await.unwrap_err();
    assert!(why.contains("does not enforce conditional writes"), "{why}");
    assert!(why.contains("If-None-Match"), "{why}");
    assert_eq!(
        a.service.not_ready(),
        Some("the bucket does not enforce conditional writes")
    );
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_ne!(r.status, 200, "{}", r.body);
    // Nothing written but the probe, which was removed.
    use futures_util::TryStreamExt as _;
    let left: Vec<_> = shared.list(None).try_collect().await.unwrap();
    assert!(left.is_empty(), "{left:?}");
}

/// The same store with its conditions enforced passes, and the probe
/// object is gone afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_that_enforces_conditional_writes_is_accepted() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node("a", dir.path(), &shared, Duration::from_secs(30)).await;
    a.service.node_id_checked().await.unwrap();
    use futures_util::TryStreamExt as _;
    let probes: Vec<_> = shared
        .list(Some(&object_store::path::Path::from("probe")))
        .try_collect()
        .await
        .unwrap();
    assert!(probes.is_empty(), "{probes:?}");
}

/// Leases name their holder by node id: a second process started with the
/// id of a live node must not run its actors. It is refused at start, takes
/// no lease and writes no heartbeat; the first node keeps serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_node_with_a_live_nodes_id_runs_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    let first = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((first.status, first.body.trim()), (200, "1"));

    let twin = node("a", db.path(), &store, Duration::from_secs(30)).await;
    let why = twin.service.node_id_checked().await.unwrap_err();
    assert!(
        why.starts_with("another process runs with this node id: ") && why.contains("node a"),
        "{why}"
    );
    assert_eq!(
        twin.service.not_ready(),
        Some("another process runs with this node id")
    );
    // Neither alice (a's) nor a new actor runs on the twin.
    for name in ["alice", "bob"] {
        let r = send(twin.addr, "POST", &format!("/actor/counter/{name}"), "").await;
        assert_ne!(r.status, 200, "{name}: {}", r.body);
    }
    // a still owns alice, and its count went on from where it was.
    let again = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((again.status, again.body.trim()), (200, "2"));
    assert_eq!(again.node.as_deref(), Some("a"));
}

/// A node killed and started again with its id (a restart) is not taken
/// for a second process: its old heartbeat stops changing, and the new
/// process takes the actors back at once (the lease names the same id).
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_node_keeps_its_id_and_its_actors() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_old: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let da = tempfile::tempdir().unwrap();
    let a = node("a", da.path(), &seen_by_old, Duration::from_secs(30)).await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    // A plain write's upload follows its answer: let it land.
    settle().await;
    // Killed: it writes nothing more to the bucket (in this test process
    // its tasks would otherwise go on heartbeating), then serves nothing.
    // The bucket is cut first: dropping the node starts a clean stop, whose
    // `released` write, landing first under load, ends the start check
    // early, as it should for a stop (it failed so in the full suite).
    down.store(true, Ordering::SeqCst);
    stop_listening(a).await;

    let started = std::time::Instant::now();
    let again = node("a", da.path(), &shared, Duration::from_secs(30)).await;
    again.service.node_id_checked().await.unwrap();
    // Its predecessor's heartbeat was still live, so it watched it for a
    // lease's ttl plus grace (1.2 s in these timings) first.
    assert!(
        started.elapsed() >= Duration::from_millis(1_200),
        "{:?}",
        started.elapsed()
    );
    let r = send(again.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));
}

/// ADR 0020, section 6: an actor's first ownership record goes above every
/// generation it already used, whether its keys carry it or its per-actor
/// lease from before does. Here a node wrote two actors, then their records
/// are removed, as for a bucket written before ADR 0020: `x` keeps an old
/// lease at generation 9, `y` only its keys. A new node takes both at fresh
/// generations, with their data.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_ownership_record_goes_above_every_generation_used() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
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
    for name in ["x", "y"] {
        for _ in 0..3 {
            let r = send(a.addr, "POST", &format!("/actor/t/{name}/durable"), "").await;
            assert_eq!(r.status, 200, "{}", r.body);
        }
    }
    stop_listening(a).await;
    down.store(true, Ordering::SeqCst);
    for name in ["x", "y"] {
        shared
            .delete(&object_store::path::Path::from(test_key(
                Family::Owners,
                &format!("t/{name}"),
            )))
            .await
            .unwrap();
    }
    shared
        .put(
            &object_store::path::Path::from(test_key(Family::Leases, "t/x")),
            b"wasmkeep-lease 1\na\n9\n0\n".to_vec().into(),
        )
        .await
        .unwrap();
    let y_keys = highest_key_generation(&shared, "y").await;

    let b = node_with(
        "b",
        db.path(),
        &shared,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    for name in ["x", "y"] {
        let r = send(b.addr, "GET", &format!("/actor/t/{name}/durable"), "").await;
        assert_eq!((r.status, r.body.trim()), (200, "3"), "{name}: {}", r.body);
    }
    let (holder, x_generation) = owner_record(&shared, "x").await.unwrap();
    assert_eq!(holder.as_deref(), Some("b"));
    assert_eq!(x_generation, 10, "above the old lease's generation 9");
    // Both old leases are markers an old binary never takes over: x's from
    // b (seeded from its lease's 9), y's still the one a wrote at its very
    // first acquisition (no keys yet: 0), kept, since b seeds from the keys
    // too.
    for (name, seed) in [("x", 9), ("y", 0)] {
        let body = shared
            .get(&object_store::path::Path::from(test_key(
                Family::Leases,
                &format!("t/{name}"),
            )))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&body),
            format!("wasmkeep-lease 2\n{seed}\n"),
            "{name}"
        );
    }
    let (_, y_generation) = owner_record(&shared, "y").await.unwrap();
    assert!(
        y_generation > y_keys,
        "y's record at {y_generation}, its keys up to {y_keys}"
    );
}

/// ADR 0020, section 2: with the heartbeat object gone (deleted, the
/// machine lost), `wasmkeep cluster fence` writes a fence at the operator's
/// clock; the actors its node held are free ttl plus grace (1.2 s here)
/// later.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_over_a_deleted_heartbeat_frees_its_actors() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    store
        .delete(&object_store::path::Path::from("nodes/a"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), a.service.node_id_lost())
        .await
        .expect("a never noticed");
    stop_listening(a).await;

    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(&store), "");
    let fenced_at = std::time::Instant::now();
    let fenced = wasmkeep_cluster::node_lease::operator_fence(&bucket, &WallClock, "nodes/a")
        .await
        .unwrap();
    assert!(
        matches!(
            fenced,
            wasmkeep_cluster::node_lease::OperatorFence::Fenced {
                created: true,
                instance: None,
                ..
            }
        ),
        "{fenced:?}"
    );
    loop {
        let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
        if r.status == 200 {
            assert_eq!(r.body.trim(), "1");
            break;
        }
        assert!(fenced_at.elapsed() < Duration::from_secs(4), "{}", r.body);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        fenced_at.elapsed() >= Duration::from_millis(1_100),
        "freed before ttl plus grace: {:?}",
        fenced_at.elapsed()
    );
}

/// ADR 0020, section 6: an actor whose per-actor lease from before is
/// still live by the old rules is held, whatever node it names (this
/// node's own id included: an old record has no instance), and is not
/// taken until that lease runs out; then it is marked and taken.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_old_lease_refuses_until_it_runs_out() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = tempfile::tempdir().unwrap();
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    // Live for 2 s more, plus the 200 ms grace; one names b itself.
    let until = now_ms() + 2_000;
    for (name, owner) in [("old", "a"), ("mine", "b")] {
        store
            .put(
                &object_store::path::Path::from(test_key(Family::Leases, &format!("t/{name}"))),
                format!("wasmkeep-lease 1\n{owner}\n4\n{until}\n")
                    .into_bytes()
                    .into(),
            )
            .await
            .unwrap();
        let r = send(b.addr, "GET", &format!("/actor/t/{name}/durable"), "").await;
        assert_ne!(
            r.status, 200,
            "{name} taken from a live old lease: {}",
            r.body
        );
        assert!(
            owner_record(&store, name).await.is_none(),
            "{name}: a record was written"
        );
    }
    // Once it ran out (and its grace passed): marked, taken at 5.
    while now_ms() <= until + 200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for name in ["old", "mine"] {
        let started = std::time::Instant::now();
        loop {
            let r = send(b.addr, "GET", &format!("/actor/t/{name}/durable"), "").await;
            if r.status == 200 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "{name}: {}",
                r.body
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            owner_record(&store, name).await,
            Some((Some("b".into()), 5))
        );
    }
}

/// ADR 0020, section 2: an absent `nodes/<id>` frees nothing. Its process
/// may have been valid when the object was deleted, and without the object
/// nothing tells how long ago it last wrote; the actors stay held until a
/// process starts with that id (its start check waits, then its new
/// instance frees its predecessor's records) or an operator fences it.
#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_heartbeat_frees_nothing() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    store
        .delete(&object_store::path::Path::from("nodes/a"))
        .await
        .unwrap();
    // a stops at its next write; b takes nothing.
    tokio::time::timeout(Duration::from_secs(3), a.service.node_id_lost())
        .await
        .expect("a never noticed");
    stop_listening(a).await;
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
        assert_ne!(
            r.status, 200,
            "b took x from a deleted heartbeat: {}",
            r.body
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("a"));

    // A new process with id a frees its predecessor's records: b may take
    // x from then on (or a may, wherever the request lands).
    let again = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    again.service.node_id_checked().await.unwrap();
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
        if r.status == 200 {
            assert_eq!(r.body.trim(), "1", "{}", r.body);
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "x never served again"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Two processes started with one id at the same moment both pass the
/// start check. The running check finds them: each heartbeat carries its
/// process's token, and a listing showing this id with another token stops
/// the process. Here the twin's heartbeat is written into the bucket as a
/// twin would write it.
#[tokio::test(flavor = "multi_thread")]
async fn a_twin_started_at_the_same_moment_is_found_while_running() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));

    // The twin's node lease (format 3): this id, another process, written
    // just now.
    let twin = format!(
        "wasmkeep-node 3\nlive feedface\n{}\na\nhttp://127.0.0.1:1\n0\ndefault/\n\n\nserving\n\ntake\n",
        now_ms()
    );
    store
        .put(
            &object_store::path::Path::from("nodes/a"),
            twin.into_bytes().into(),
        )
        .await
        .unwrap();

    let why = tokio::time::timeout(Duration::from_secs(5), a.service.node_id_lost())
        .await
        .expect("the twin was never noticed");
    assert!(
        why.starts_with("another process runs with this node id: ") && why.contains("node a"),
        "{why}"
    );
    assert_eq!(
        a.service.not_ready(),
        Some("another process runs with this node id")
    );
    // The twin takes this id's leases at once: alice, resident here with a
    // lease still valid, is no longer served either, right away.
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_ne!(r.status, 200, "alice still served: {}", r.body);
    // No new actor runs here, and the heartbeat is no longer rewritten.
    let r = send(a.addr, "POST", "/actor/counter/bob", "").await;
    assert_ne!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let body = store
        .get(&object_store::path::Path::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("\nlive feedface\n"),
        "a kept writing its heartbeat"
    );
}

/// ADR 0020: the heartbeat is the node's lease, written conditionally. A
/// fence over it (here written as a peer or `wasmkeep cluster fence`
/// would: the state line changed, the rest kept) fails the node's next
/// write; it stops serving at once and its service reports why.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_node_stops_serving_its_actors() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));

    let key = object_store::path::Path::from("nodes/a");
    // As `wasmkeep cluster fence a` does.
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(&store), "");
    let fenced = wasmkeep_cluster::node_lease::operator_fence(&bucket, &WallClock, "nodes/a")
        .await
        .unwrap();
    assert!(
        matches!(
            fenced,
            wasmkeep_cluster::node_lease::OperatorFence::Fenced { created: false, .. }
        ),
        "{fenced:?}"
    );

    let why = tokio::time::timeout(Duration::from_secs(5), a.service.node_id_lost())
        .await
        .expect("the fence was never noticed");
    assert!(why.contains("fenced"), "{why}");
    let r = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_ne!(r.status, 200, "alice still served: {}", r.body);
    // The fence stays: the node writes nothing more.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("\nfenced "));
}

/// ADR 0020, section 2: after a bucket outage longer than the lease, every
/// node renews before it judges the others. The others' objects are still
/// the ones from before the outage, older than ttl plus grace, but no node
/// fences another: each waits a lease period plus an attempt timeout after
/// its own lease is valid again, by which time the others renewed.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_bucket_outage_no_node_fences_another_before_renewing() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (down_a, down_b) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let store_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down_a)));
    let store_b: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down_b)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store_a, Duration::from_secs(30)).await;
    let b = node("b", db.path(), &store_b, Duration::from_secs(30)).await;
    settle().await;
    // Longer than ttl plus grace (1.2 s): every object is stale after it.
    down_a.store(true, Ordering::SeqCst);
    down_b.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    // a is back first. b's object is stale, and a has watched it unchanged
    // since before the outage; b is back 200 ms later, within the lease
    // period plus attempt timeout (600 ms) a waits after renewing.
    down_a.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    down_b.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    for id in ["a", "b"] {
        let body = shared
            .get(&object_store::path::Path::from(format!("nodes/{id}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("\nlive "),
            "{id} was fenced after the outage"
        );
    }
    for (id, n) in [("a", &a), ("b", &b)] {
        assert_eq!(n.service.not_ready(), None, "{id}");
    }
}

/// ADR 0020, section 2: a node that stops renewing (here, its process is
/// gone without a clean stop) is fenced by a peer; its heartbeat then
/// says `fenced` and no listing counts it live.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_stops_renewing_is_fenced_by_a_peer() {
    use object_store::ObjectStoreExt as _;
    let down = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let b_store: Arc<dyn ObjectStore> = Arc::new(Flaky::new(Arc::clone(&store), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    let b = node("b", db.path(), &b_store, Duration::from_secs(30)).await;
    settle().await;
    // b loses the bucket (a does not): its lease runs out, a fences it.
    down.store(true, Ordering::SeqCst);
    let key = object_store::path::Path::from("nodes/b");
    let started = std::time::Instant::now();
    loop {
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        if String::from_utf8_lossy(&body).contains("\nfenced ") {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "b never fenced");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Back on the bucket, b's next write fails its condition: it stops.
    down.store(false, Ordering::SeqCst);
    let why = tokio::time::timeout(Duration::from_secs(5), b.service.node_id_lost())
        .await
        .expect("b never noticed the fence");
    assert!(why.contains("fenced"), "{why}");
    assert_eq!(a.service.not_ready(), None);
}

/// A twin of an older version writes no token and does no check of its
/// own: once this node's own heartbeat has landed, a heartbeat under its id
/// without a token can only be another writer's.
#[tokio::test(flavor = "multi_thread")]
async fn a_twin_of_an_older_version_is_found_too() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    // A heartbeat as a node from before the token writes it.
    let twin = format!(
        "wasmkeep-node 2\na\nhttp://127.0.0.1:1\n0\n{}\ndefault/\n\n\nserving\n\ntake\n",
        now_ms()
    );
    store
        .put(
            &object_store::path::Path::from("nodes/a"),
            twin.into_bytes().into(),
        )
        .await
        .unwrap();
    let why = tokio::time::timeout(Duration::from_secs(5), a.service.node_id_lost())
        .await
        .expect("the older twin was never noticed");
    // Whichever finds it first (the listing or the next lease write's
    // re-read), the same verdict.
    assert!(
        why.starts_with("an older version of WasmKeep runs with this node id: "),
        "{why}"
    );
    assert!(why.contains("the other goes on"), "{why}");
    assert_eq!(
        a.service.not_ready(),
        Some("an older version of WasmKeep runs with this node id")
    );
}

/// `id`'s heartbeat as the bucket holds it, decoded, once `ok` holds for
/// it (within 5 s).
async fn wait_heartbeat(
    store: &Arc<dyn ObjectStore>,
    id: &str,
    ok: impl Fn(&wasmkeep_cluster::membership::NodeInfo) -> bool,
) -> wasmkeep_cluster::membership::NodeInfo {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(format!("nodes/{id}"));
    let started = std::time::Instant::now();
    loop {
        let read = match store.get(&key).await {
            Ok(r) => wasmkeep_cluster::membership::NodeInfo::decode(&r.bytes().await.unwrap()),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => panic!("{e}"),
        };
        if let Some(info) = read.as_ref().filter(|i| ok(i)) {
            return info.clone();
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{id}'s heartbeat never matched: {read:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Payload line 10: a node's heartbeat says how many actors and WebSockets
/// it holds, kept current with every write; events per minute stay unknown
/// until a minute of totals exists.
#[tokio::test(flavor = "multi_thread")]
async fn the_heartbeat_carries_the_nodes_load() {
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
    let idle = wait_heartbeat(&store, "a", |i| i.load.is_some()).await;
    assert_eq!(idle.load.unwrap().actors, 0, "{idle:?}");
    for name in ["one", "two"] {
        let r = send(a.addr, "GET", &format!("/actor/t/{name}/fired"), "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let busy = wait_heartbeat(&store, "a", |i| i.load.is_some_and(|l| l.actors == 2)).await;
    let load = busy.load.unwrap();
    assert_eq!((load.sockets, load.events_per_min), (0, None), "{busy:?}");
    assert_eq!(
        busy.lease,
        Some(wasmkeep_cluster::node_lease::NodeState::Live)
    );

    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/sock", a.addr))
        .await
        .unwrap();
    wait_heartbeat(&store, "a", |i| i.load.is_some_and(|l| l.sockets == 1)).await;
    drop(ws);
    drop(a);
}
