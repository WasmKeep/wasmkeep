//! Actors that end (ADR 0028): the operator delete (`wasmkeep actor
//! delete`, a request under the app's `deletes/`) and the durable delete
//! (`storage.delete-self-durable`).

use super::*;

/// Asks for actor `t/<name>` to be deleted, as `wasmkeep actor delete`
/// does: the request's key.
async fn request_delete(store: &Arc<dyn ObjectStore>, name: &str) -> object_store::path::Path {
    request_delete_of(store, "t", name).await
}

async fn exists(store: &Arc<dyn ObjectStore>, key: &object_store::path::Path) -> bool {
    use object_store::ObjectStoreExt as _;
    match store.head(key).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => panic!("{e}"),
    }
}

/// Waits (up to `within`) until the request at `key` is done: its key gone,
/// its outcome `.done` written beside it.
async fn done_within(
    store: &Arc<dyn ObjectStore>,
    key: &object_store::path::Path,
    within: Duration,
) {
    ended_within(store, key, ".done", within).await;
}

/// Waits (up to `within`) until the request at `key` is gone, its outcome
/// `outcome` beside it.
async fn ended_within(
    store: &Arc<dyn ObjectStore>,
    key: &object_store::path::Path,
    outcome: &str,
    within: Duration,
) {
    let started = std::time::Instant::now();
    while exists(store, key).await {
        assert!(
            started.elapsed() < within,
            "the delete request is still there after {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let written = object_store::path::Path::from(format!("{key}{outcome}"));
    assert!(
        exists(store, &written).await,
        "no outcome {outcome} for {key}"
    );
}

/// `n` durable increments of actor `t/<name>` on the node at `addr`.
pub(super) async fn write(addr: SocketAddr, name: &str, n: u32) {
    for i in 1..=n {
        let r = send(addr, "POST", &format!("/actor/t/{name}/durable"), "").await;
        assert_eq!((r.status, r.body.trim()), (200, i.to_string().as_str()));
    }
}

/// What actor `t/<name>` reads as on the node at `addr` (its `durable`
/// counter), once it answers.
pub(super) async fn read(addr: SocketAddr, name: &str) -> String {
    let started = std::time::Instant::now();
    loop {
        let r = poll_get(addr, &format!("/actor/t/{name}/durable")).await;
        if r.status == 200 {
            return r.body.trim().to_owned();
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn two_nodes(idle: Duration) -> (Arc<dyn ObjectStore>, Node, Node, [tempfile::TempDir; 2]) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let a = node_with("a", dirs[0].path(), &store, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", dirs[1].path(), &store, idle, "t", test_actor_wasm()).await;
    settle().await;
    (store, a, b, dirs)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_delete_of_a_resident_actor_is_done_once_recorded() {
    let (store, a, b, _dirs) = two_nodes(Duration::from_secs(30)).await;
    write(a.addr, "res", 3).await;
    assert_eq!(a.service.resident(), 1);
    let key = request_delete(&store, "res").await;
    done_within(&store, &key, Duration::from_secs(10)).await;
    // Its history is erased down to the reset and the snapshot at it, and
    // before the request is done: the operator delete erases before its
    // job returns. Only uploads after the reset may follow it, such as
    // the fence of a node that read the same request before it was done,
    // took the actor once it was released and found the delete already
    // carried out (ADR 0007).
    let (snapshots, segments) = history_of(&store, "res").await;
    let [reset] = snapshots[..] else {
        panic!("one snapshot, at the reset: {snapshots:?} {segments:?}");
    };
    assert!(
        segments.contains(&reset) && segments.iter().all(|&txid| txid >= reset),
        "history below the reset {reset} left: {segments:?}"
    );
    // Done means recorded: whichever node runs it next restores it empty.
    assert_eq!(read(b.addr, "res").await, "0", "the deleted data came back");
    assert_eq!(read(a.addr, "res").await, "0");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_delete_of_a_hibernated_actor() {
    let (store, a, b, _dirs) = two_nodes(Duration::from_millis(300)).await;
    write(a.addr, "hib", 2).await;
    eventually("hibernated", async || a.service.resident() == 0).await;
    let key = request_delete(&store, "hib").await;
    done_within(&store, &key, Duration::from_secs(10)).await;
    assert_eq!(read(b.addr, "hib").await, "0", "the deleted data came back");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_delete_of_an_actor_whose_node_died() {
    // a holds the actor, then loses the bucket for good: b deletes it once
    // the record a left is free (ADR 0020: ttl plus grace), no fence
    // needed.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(p.a.addr, "x", 3).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    p.flaky.down.store(true, Ordering::SeqCst);
    let shared = Arc::clone(&p.flaky.inner);
    let key = request_delete(&shared, "x").await;
    done_within(&shared, &key, Duration::from_secs(20)).await;
    assert_eq!(read(p.b.addr, "x").await, "0", "the deleted data came back");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_delete_clears_the_alarm() {
    let (store, a, b, _dirs) = two_nodes(Duration::from_secs(30)).await;
    write(a.addr, "alarmed", 1).await;
    let r = send(a.addr, "POST", "/actor/t/alarmed/alarm?in=2500", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    eventually("the alarm indexed", async || {
        !index_entries_for(&store, "alarmed").await.is_empty()
    })
    .await;
    let key = request_delete(&store, "alarmed").await;
    done_within(&store, &key, Duration::from_secs(10)).await;
    wait_unindexed(&store, "alarmed", Duration::from_secs(5)).await;
    // Past the time it was due: nothing fired, nothing pending.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for node in [&a, &b] {
        let r = send_reachable(node.addr, "GET", "/actor/t/alarmed/alarm", "").await;
        assert_eq!((r.status, r.body.trim()), (200, "none"));
        let r = send_reachable(node.addr, "GET", "/actor/t/alarmed/fired", "").await;
        assert_eq!((r.status, r.body.trim()), (200, "0"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_request_left_behind_is_not_carried_out_twice() {
    // The request's key cannot be deleted: every pass lists it again. Data
    // written after the delete was carried out must stay.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let d = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        d.path(),
        &seen,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    write(a.addr, "again", 3).await;
    flaky.fail_request_deletes.store(true, Ordering::SeqCst);
    let key = request_delete(&shared, "again").await;
    eventually("deleted", async || read(a.addr, "again").await == "0").await;
    // Written after the delete, and a socket opened to the new actor: the
    // request, still listed, must take neither.
    write(a.addr, "again", 2).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/again", a.addr))
        .await
        .unwrap();
    {
        use futures_util::StreamExt as _;
        let open = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        assert!(open.is_err(), "the new actor's socket was closed: {open:?}");
    }
    assert!(exists(&shared, &key).await, "the key was deleted after all");
    assert_eq!(read(a.addr, "again").await, "2", "deleted twice");
    // Hibernated and restored: the same.
    drop(a);
    let d2 = tempfile::tempdir().unwrap();
    let b = node_with(
        "b",
        d2.path(),
        &seen,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(read(b.addr, "again").await, "2", "deleted twice");
    flaky.fail_request_deletes.store(false, Ordering::SeqCst);
    done_within(&shared, &key, Duration::from_secs(10)).await;
    assert_eq!(read(b.addr, "again").await, "2", "deleted twice");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_request_for_no_such_actor_creates_nothing() {
    let (store, _a, _b, _dirs) = two_nodes(Duration::from_secs(30)).await;
    let key = request_delete(&store, "ghost").await;
    ended_within(&store, &key, ".absent", Duration::from_secs(10)).await;
    assert!(actor_keys(&store, "ghost").await.is_empty());
    assert_eq!(owner_record(&store, "ghost").await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_delete_answers_once_the_bucket_holds_it() {
    // a is cut off from the bucket right after the answer: whatever it had
    // not uploaded by then is lost with it. A plain `delete-self` uploads
    // its reset after the answer; the durable one before.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(p.a.addr, "x", 2).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/delete-durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"), "{}", r.body);
    p.flaky.down.store(true, Ordering::SeqCst);
    assert_eq!(
        read(p.b.addr, "x").await,
        "0",
        "an acknowledged delete was lost"
    );
    // b writes on top of the delete: a new actor.
    write(p.b.addr, "x", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_delete_whose_upload_fails_says_so_and_keeps_the_data() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(p.a.addr, "x", 2).await;
    // Every upload of the reset fails, the client's resends included.
    p.flaky.fail_wal.store(100, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/delete-durable", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert!(r.body.contains("Internal"), "{}", r.body);
    p.flaky.fail_wal.store(0, Ordering::SeqCst);
    // Nothing was recorded: the data is still the bucket's.
    let restored = b_restores(&p).await;
    assert_eq!(
        restored.body.trim(),
        "2",
        "an unrecorded delete removed the data"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_is_refused_after_a_durable_delete_in_the_same_event() {
    let (_store, a, b, _dirs) = two_nodes(Duration::from_secs(30)).await;
    write(a.addr, "then", 2).await;
    let r = send(a.addr, "POST", "/actor/t/then/delete-durable?then=sql", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "err:SqlErrorKind::NotAllowed"),
        "{}",
        r.body
    );
    assert_eq!(read(b.addr, "then").await, "0");
    assert_eq!(read(a.addr, "then").await, "0");
}
