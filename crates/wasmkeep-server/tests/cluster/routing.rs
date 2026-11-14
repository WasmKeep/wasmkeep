//! Requests that reach a node other than the owner are forwarded to it.

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn forwarding_does_not_require_a_local_resident_slot() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

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
    let b = node_configured(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.mailbox.max_resident = 0,
    )
    .await;
    settle().await;
    assert_eq!(
        send(a.addr, "GET", "/actor/t/remote/fired", "")
            .await
            .status,
        200
    );

    let response = send(b.addr, "GET", "/actor/t/remote/fired", "").await;
    assert_eq!(
        (response.status, response.node.as_deref()),
        (200, Some("a"))
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/remote", b.addr))
        .await
        .unwrap();
    ws.send(Message::text("through a full relay"))
        .await
        .unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(
        echo.unwrap().unwrap(),
        Message::text("through a full relay")
    );
    assert_eq!(
        b.service.resident(),
        0,
        "forwarding created a resident stub"
    );
    assert!(!db.path().join(test_dir("t").join("remote.db")).exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_calls_do_not_evict_or_compete_with_the_local_caller() {
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
    let b = node_configured(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.mailbox.max_resident = 1,
    )
    .await;
    settle().await;
    assert_eq!(
        send(a.addr, "GET", "/actor/t/remote/fired", "")
            .await
            .status,
        200
    );
    let response = send(
        b.addr,
        "POST",
        "/actor/t/local/call?to=t/remote&path=/caller",
        "",
    )
    .await;
    assert_eq!(
        (response.status, response.body.trim()),
        (200, "200:default/t/local")
    );
    assert_eq!(b.service.resident(), 1, "only the local caller is resident");
    assert!(!db.path().join(test_dir("t").join("remote.db")).exists());
}

/// A client's `Connection: close` is for its own hop: the forwarding node
/// keeps its connection to the owner for the next request instead of
/// opening one (over TLS, a full handshake) per request. Bodies still
/// arrive whole, framed by length or chunked.
#[tokio::test(flavor = "multi_thread")]
async fn a_clients_connection_close_does_not_close_the_connection_to_the_owner() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, accepted) = node_counting_peers("a", da.path(), &store, "t", test_actor_wasm()).await;
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
    assert_eq!(
        send(a.addr, "GET", "/actor/t/x/fired", "").await.status,
        200
    );

    for _ in 0..5 {
        pooled().await;
        let r = send(b.addr, "GET", "/actor/t/x/fired", "").await;
        assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    }
    pooled().await;
    let sized = exchange(
        b.addr,
        "POST /actor/t/x/body-len HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\
         Connection: close\r\n\r\nhello",
    )
    .await;
    assert_eq!((sized.status, sized.body.trim()), (200, "5"));
    pooled().await;
    let chunked = exchange(
        b.addr,
        "POST /actor/t/x/body-len HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\
         Connection: close\r\n\r\n3\r\nhel\r\n4\r\nlo w\r\n0\r\n\r\n",
    )
    .await;
    assert_eq!((chunked.status, chunked.body.trim()), (200, "7"));
    // HTTP/1.0 closes after each response unless asked not to: the owner
    // must not see the client's version either.
    for _ in 0..3 {
        pooled().await;
        let r = exchange(b.addr, "GET /actor/t/x/fired HTTP/1.0\r\nHost: t\r\n\r\n").await;
        assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    }
    pooled().await;
    let old_client = exchange(
        b.addr,
        "POST /actor/t/x/body-len HTTP/1.0\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello",
    )
    .await;
    assert_eq!((old_client.status, old_client.body.trim()), (200, "5"));
    // Eleven requests, one connection. A second is tolerated: the pooled
    // connection goes back to the pool from a background task, and a
    // request that checks out just before that starts a connection anyway.
    // `pooled` makes that rare under load; closing per request would show
    // eleven.
    let opened = accepted.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&opened),
        "{opened} connections to the owner for 11 forwarded requests"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_reaching_another_node_are_forwarded_to_the_owner() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    let b = node("b", db.path(), &store, Duration::from_secs(30)).await;
    settle().await;

    let first = send(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((first.status, first.body.trim()), (200, "1"));
    assert_eq!(first.node.as_deref(), Some("a"));

    // Through b: forwarded to a, which owns alice, so the count continues.
    for expected in ["2", "3"] {
        let r = send(b.addr, "POST", "/actor/counter/alice", "").await;
        assert_eq!((r.status, r.body.trim()), (200, expected));
        assert_eq!(r.node.as_deref(), Some("a"), "served by the owner");
    }

    // A different actor first reached through b is owned by b.
    let bob = send(b.addr, "POST", "/actor/counter/bob", "").await;
    assert_eq!(bob.node.as_deref(), Some("b"));
    let bob = send(a.addr, "GET", "/actor/counter/bob", "").await;
    assert_eq!((bob.body.trim(), bob.node.as_deref()), ("1", Some("b")));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forwarded_request_is_never_forwarded_again() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Duration::from_secs(30)).await;
    let b = node("b", db.path(), &store, Duration::from_secs(30)).await;
    settle().await;

    send(a.addr, "POST", "/actor/counter/alice", "").await;
    // b believes a forwarded this to it, yet a owns the actor: two nodes
    // disagree, so b answers 503 instead of bouncing it back.
    let r = send(
        b.addr,
        "POST",
        "/actor/counter/alice",
        "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n",
    )
    .await;
    assert_eq!(r.status, 503);
    let r = send(a.addr, "GET", "/actor/counter/alice", "").await;
    assert_eq!(r.body.trim(), "1", "the refused request did not run");
}

#[tokio::test(flavor = "multi_thread")]
async fn awkward_names_work_across_nodes() {
    async fn admitted(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
        let mut reply = send(addr, method, path, extra).await;
        // Names and forwarding stubs exceed the 14-resident budget. Only
        // retry explicit admission refusals, which ran no actor event.
        for _ in 0..5 {
            if reply.status != 503
                || reply.retry_after.as_deref() != Some("1")
                || reply.body.trim() != "actor capacity unavailable"
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            reply = send(addr, method, path, extra).await;
        }
        reply
    }

    // Forwarding, calls between nodes (and the caller header), failover
    // with restore, and the alarm sweep, for every awkward name.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;

    for name in awkward_names() {
        let enc = url_encode(&name);
        let path = |rest: &str| format!("/actor/t/{enc}/{rest}");
        // a takes the actor; b forwards to it.
        let r = admitted(a.addr, "POST", &path("durable"), "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, "1", Some("a")),
            "{name:?}"
        );
        let r = admitted(b.addr, "POST", &path("durable"), "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, "2", Some("a")),
            "{name:?} forwarded"
        );
        // An actor first reached on b calls it on a; the callee sees who called.
        let caller = format!("caller-{}", url_encode(&name).len());
        let r = admitted(
            b.addr,
            "POST",
            &format!(
                "/actor/t/{caller}/call?to={}&path=%2Fcaller",
                url_encode(&format!("t/{name}"))
            ),
            "",
        )
        .await;
        assert_eq!(
            (r.status, r.body.trim().to_owned()),
            (200, format!("200:default/t/{caller}")),
            "{name:?}: call across nodes"
        );
        // And the other way: the awkward name calls; its caller header must
        // decode back to its name.
        let r = admitted(
            a.addr,
            "POST",
            &format!("{}?to=t%2Fsink&path=%2Fcaller", path("call")),
            "",
        )
        .await;
        assert_eq!(r.status, 200, "{name:?}");
        let header = r
            .body
            .trim()
            .strip_prefix("200:")
            .unwrap_or_default()
            .to_owned();
        let decoded = header
            .strip_prefix("default/t/")
            .map(percent_decode_str)
            .unwrap_or_default();
        assert_eq!(decoded, name, "caller header {header:?}");
        // A pending alarm, due soon.
        let r = admitted(a.addr, "POST", &format!("{}?in=1500", path("alarm")), "").await;
        assert_eq!(r.status, 200, "{name:?}");
    }

    // a loses the bucket: b restores every actor with its writes and fires
    // its alarm (before any request reaches it).
    tokio::time::sleep(Duration::from_millis(300)).await;
    down.store(true, Ordering::SeqCst);
    let names = awkward_names();
    wait_delivered(
        &shared,
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    for name in awkward_names() {
        let enc = url_encode(&name);
        let r = admitted(b.addr, "GET", &format!("/actor/t/{enc}/fired"), "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, "1", Some("b")),
            "{name:?}: alarm"
        );
        let r = admitted(b.addr, "GET", &format!("/actor/t/{enc}/durable"), "").await;
        assert_eq!((r.status, r.body.trim()), (200, "2"), "{name:?}: writes");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_prefix_that_needs_escaping_works_end_to_end() {
    // The prefix comes from `--bucket s3://BUCKET/PREFIX` as typed: it must
    // not break heartbeats, leases, restore or the alarm sweep.
    let prefix = "team~1 x";
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (idle, wall) = (Duration::from_secs(30), Duration::from_secs(30));
    let a = node_prefixed(
        "a",
        da.path(),
        &seen_by_a,
        idle,
        "t",
        test_actor_wasm(),
        wall,
        prefix,
    )
    .await;
    let b = node_prefixed(
        "b",
        db.path(),
        &shared,
        idle,
        "t",
        test_actor_wasm(),
        wall,
        prefix,
    )
    .await;
    settle().await;

    let r = send(a.addr, "POST", "/actor/t/Room%201/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("a"))
    );
    // Forwarded: b sees a's heartbeat and lease under the prefix.
    let r = send(b.addr, "POST", "/actor/t/Room%201/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "2", Some("a"))
    );
    let r = send(a.addr, "POST", "/actor/t/Room%201/alarm?in=1500", "").await;
    assert_eq!(r.status, 200);
    tokio::time::sleep(Duration::from_millis(300)).await;
    down.store(true, Ordering::SeqCst);

    // (The index is under the prefix here: `wait_delivered` reads the
    // unprefixed one, so this test waits the worst case.)
    tokio::time::sleep(Duration::from_secs(6)).await;
    let r = send(b.addr, "GET", "/actor/t/Room%201/fired", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("b")),
        "alarm swept"
    );
    let r = send(b.addr, "GET", "/actor/t/Room%201/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "writes restored");
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarding_reuses_the_owner_it_learned_instead_of_reading_the_bucket_each_time() {
    // b relays requests for an actor a owns. It must learn the owner once
    // and reuse that for a while (as the per-actor hint did), not read the
    // actor's lease from the bucket for every request it relays.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seen_by_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &shared, idle, "t", test_actor_wasm()).await;
    let b = node_with(
        "b",
        db.path(),
        &(Arc::clone(&seen_by_b) as Arc<dyn ObjectStore>),
        idle,
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/fired", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));

    let before = seen_by_b.lease_gets.load(Ordering::SeqCst);
    for _ in 0..20 {
        let r = send(b.addr, "GET", "/actor/t/x/fired", "").await;
        assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    }
    let reads = seen_by_b.lease_gets.load(Ordering::SeqCst) - before;
    assert!(reads <= 2, "{reads} lease reads for 20 relayed requests");
}

/// A request whose client hung up while it waited in the actor's queue is
/// not run: nobody would get its answer (the 12-hour soak: 11 durable
/// writes whose clients had timed out ran one after another behind a slow
/// restore, 16 s before the first live answer). Here an event holds the
/// actor for 1.5 s; a durable write queued behind it is abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_abandoned_while_queued_is_not_run() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    let r = send(a.addr, "GET", "/actor/t/q/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "0"));
    let busy = tokio::spawn(send(a.addr, "POST", "/actor/t/q/sleep?ms=1500", ""));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Sent while the sleep holds the actor, then abandoned.
    let mut stream = TcpStream::connect(a.addr).await.unwrap();
    stream
        .write_all(b"POST /actor/t/q/durable HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(stream);
    assert_eq!(busy.await.unwrap().status, 200);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = send(a.addr, "GET", "/actor/t/q/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "0"),
        "the abandoned write ran"
    );
    // Skipped by the check, not lost on the way: counted once (this test's
    // process has the metrics to itself).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let (_, page) = get_raw(metrics, "/metrics").await;
    assert!(
        page.lines()
            .any(|l| l == "wasmkeep_requests_abandoned_total 1"),
        "{page}"
    );
}

/// Node a behind a proxy the test controls, owning actor `x`, and node b
/// holding one pooled connection to it for forwarded requests.
async fn b_with_a_pooled_connection_to_a() -> (Node, Node, PeerProxy, [tempfile::TempDir; 2]) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let (a, proxy) = node_behind_proxy("a", dirs[0].path(), &store, "t", test_actor_wasm()).await;
    let b = node_with(
        "b",
        dirs[1].path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    assert_eq!(
        send(a.addr, "GET", "/actor/t/x/slept", "").await.status,
        200
    );
    let r = send_reachable(b.addr, "GET", "/actor/t/x/slept", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    pooled().await;
    (a, b, proxy, dirs)
}

/// The owner closes a connection it kept idle (its own idle limit, or a
/// restart) while the forwarding node still has it in its pool. The next
/// request goes out on a new connection and the client never notices: no
/// 503.
///
/// A characterization test of the HTTP client's pool, not of anything
/// this crate does: the test waits until the close reached the forwarding
/// node, so the pooled connection's task has ended and the pool drops the
/// entry when the next request takes a connection (its checkout skips
/// connections that are no longer open). The client's other mechanism,
/// sending again a request it got back unsent from a reused connection
/// (a close that arrives between the checkout and the write), is not what
/// passes here, and no test in this suite reaches it on purpose.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_after_the_owner_closed_the_idle_connection_goes_on_a_new_one() {
    let (_a, b, proxy, _dirs) = b_with_a_pooled_connection_to_a().await;
    for round in 1..=3 {
        let before = proxy.accepted.load(Ordering::SeqCst);
        proxy.close_connections();
        // The close reaches b before the request does.
        pooled().await;
        let r = send(b.addr, "GET", "/actor/t/x/slept", "").await;
        assert_eq!(
            (r.status, r.node.as_deref(), r.body.trim()),
            (200, Some("a"), "0"),
            "round {round}"
        );
        assert_eq!(
            proxy.accepted.load(Ordering::SeqCst),
            before + 1,
            "round {round}: one new connection"
        );
        pooled().await;
    }
}

/// A request written into a connection that then closes without an answer
/// is not sent again: the forwarding node cannot tell an owner that never
/// read it from one that ran it and lost the connection (its client
/// reports the same error for both), and an event must not run twice. The
/// client gets a 503 with `Retry-After` and decides. Here the request is
/// first lost on the way (the event does not run), then delivered with its
/// answer lost (the event runs exactly once); the same for `actors.call`.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_written_before_its_connection_closed_is_not_sent_again() {
    let (a, b, proxy, _dirs) = b_with_a_pooled_connection_to_a().await;
    let slept = async || {
        // Longer than the event, which outlives its lost answer.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let r = send(a.addr, "GET", "/actor/t/x/slept", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
        r.body.trim().to_owned()
    };
    let unanswered = |r: &Reply| {
        assert_eq!(
            (r.status, r.body.trim(), r.retry_after.as_deref()),
            (503, "the actor's node did not answer; retry", Some("1"))
        );
    };

    // Lost before the owner read it.
    let before = proxy.accepted.load(Ordering::SeqCst);
    proxy.swallow_next_request();
    let r = send(b.addr, "POST", "/actor/t/x/sleep?ms=100", "").await;
    unanswered(&r);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), before, "sent once");
    assert_eq!(slept().await, "0");

    // Read by the owner, the answer lost.
    let r = send(b.addr, "POST", "/actor/t/x/sleep?ms=100", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "slept"),
        "a new connection"
    );
    pooled().await;
    let before = proxy.accepted.load(Ordering::SeqCst);
    proxy.cut_next_answer();
    let r = send(b.addr, "POST", "/actor/t/x/sleep?ms=100", "").await;
    unanswered(&r);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), before, "sent once");
    assert_eq!(slept().await, "2", "the event ran once, not twice");

    // `actors.call` from an actor on b: the same rule.
    let call = "/actor/t/local/call?to=t/x&path=%2Fsleep%3Fms%3D100";
    let r = send_reachable(b.addr, "POST", call, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "200:slept"));
    pooled().await;
    let before = proxy.accepted.load(Ordering::SeqCst);
    proxy.cut_next_answer();
    let r = send(b.addr, "POST", call, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.starts_with("failed:node a: "), "{}", r.body);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), before, "sent once");
    assert_eq!(slept().await, "4", "the callee ran once, not twice");
}

/// Sends a POST with `len` bytes of body, all of it whatever the node
/// answers meanwhile, and reads the reply. A write or a read the node cut
/// short is the failure these tests look for, so it is reported, not
/// unwrapped.
async fn post_body(addr: SocketAddr, path: &str, len: usize) -> Result<Reply, String> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    // In two parts with a pause, so the answer of a handler that does not
    // read the body is on its way while most of the body is still to
    // send, whatever the socket buffers hold.
    let body = vec![b'x'; len];
    let (first, rest) = body.split_at(len.min(64 << 10));
    let wrote = async {
        stream.write_all(first).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(rest).await
    }
    .await;
    let mut raw = Vec::new();
    let read = stream.read_to_end(&mut raw).await;
    let raw = String::from_utf8_lossy(&raw);
    if let Err(e) = wrote {
        return Err(format!("the body was cut short ({e}); answer: {raw:?}"));
    }
    let Some((head, body)) = raw.split_once("\r\n\r\n") else {
        return Err(format!("no answer ({read:?}): {raw:?}"));
    };
    let header = |name: &str| {
        head.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
        })
    };
    Ok(Reply {
        status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        node: header("x-wasmkeep-node"),
        retry_after: header("retry-after"),
        head: head.to_owned(),
        body: if header("transfer-encoding").as_deref() == Some("chunked") {
            dechunk(body)
        } else {
            body.to_owned()
        },
    })
}

/// A handler may answer without reading the request body. The client
/// (and, for a forwarded request, the forwarding node) is then still
/// sending it: the owner must let it finish, or the sender sees its
/// connection reset and can lose the answer. Each request runs once.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_body_the_handler_does_not_read_still_gets_its_answer() {
    let p = x_on_a_y_on_b().await;
    let mut durable: u64 = send(p.a.addr, "GET", "/actor/t/x/durable", "")
        .await
        .body
        .trim()
        .parse()
        .unwrap();
    for (via, addr) in [("the other node", p.b.addr), ("the owner", p.a.addr)] {
        for len in [256 << 10, 4 << 20, 8 << 20] {
            for attempt in 0..3 {
                let what = format!("{len} bytes sent to {via}, attempt {attempt}");
                // `/durable` counts its runs and never reads the body.
                let reply = post_body(addr, "/actor/t/x/durable", len)
                    .await
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                durable += 1;
                assert_eq!(
                    (reply.status, reply.node.as_deref(), reply.body.trim()),
                    (200, Some("a"), durable.to_string().as_str()),
                    "{what}"
                );
            }
        }
    }
    // Nothing ran a second time behind an answer.
    let r = send(p.a.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(r.body.trim(), durable.to_string());
}

/// The same bodies to a handler that reads them whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_body_the_handler_reads_arrives_whole() {
    let p = x_on_a_y_on_b().await;
    for (via, addr) in [("the other node", p.b.addr), ("the owner", p.a.addr)] {
        for len in [256 << 10, 4 << 20, 8 << 20] {
            let reply = post_body(addr, "/actor/t/x/body-len", len)
                .await
                .unwrap_or_else(|e| panic!("{len} bytes sent to {via}: {e}"));
            assert_eq!(
                (reply.status, reply.node.as_deref(), reply.body.trim()),
                (200, Some("a"), len.to_string().as_str()),
                "{len} bytes sent to {via}"
            );
        }
    }
}

/// A client that sent `Expect: 100-continue` is waiting to be told whether
/// to send its body. A handler that answers without reading the body must
/// not have the server ask for it: the answer comes alone. Through the
/// other node, which asks for the body as it forwards the request, the
/// client sends it and still gets the answer.
#[tokio::test(flavor = "multi_thread")]
async fn an_expected_body_the_handler_does_not_read_is_not_asked_for() {
    let p = x_on_a_y_on_b().await;
    let len = 4 << 20;
    let head = format!(
        "POST /actor/t/x/durable HTTP/1.1\r\nHost: t\r\nContent-Length: {len}\r\n\
         Expect: 100-continue\r\nConnection: close\r\n\r\n"
    );
    let mut durable: u64 = send(p.a.addr, "GET", "/actor/t/x/durable", "")
        .await
        .body
        .trim()
        .parse()
        .unwrap();

    for _ in 0..5 {
        let mut stream = TcpStream::connect(p.a.addr).await.unwrap();
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut raw = String::new();
        stream.read_to_string(&mut raw).await.unwrap();
        durable += 1;
        assert!(raw.starts_with("HTTP/1.1 200 "), "{raw:?}");
        assert!(raw.contains(&format!("\r\n{durable}\n\r\n")), "{raw:?}");
    }

    for _ in 0..5 {
        let mut stream = TcpStream::connect(p.b.addr).await.unwrap();
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut interim = [0u8; 25];
        stream.read_exact(&mut interim).await.unwrap();
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
        // In two parts with a pause, as in `post_body`: the owner's answer
        // is on its way while this node still streams the body to it. An
        // owner told that the sender waits (an `Expect` passed on) would
        // leave that body unread and close on the forwarding node.
        let body = vec![b'x'; len];
        let (first, rest) = body.split_at(64 << 10);
        let wrote = async {
            stream.write_all(first).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            stream.write_all(rest).await
        }
        .await;
        let mut raw = Vec::new();
        let read = stream.read_to_end(&mut raw).await;
        let raw = String::from_utf8_lossy(&raw);
        assert!(wrote.is_ok(), "the body was cut short ({wrote:?}): {raw:?}");
        assert!(read.is_ok(), "the answer was cut short ({read:?}): {raw:?}");
        durable += 1;
        assert!(raw.starts_with("HTTP/1.1 200 "), "{raw:?}");
        assert!(raw.contains("x-wasmkeep-node: a\r\n"), "{raw:?}");
        assert!(raw.contains(&format!("\r\n{durable}\n\r\n")), "{raw:?}");
    }
    let r = send(p.a.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(r.body.trim(), durable.to_string());
}
