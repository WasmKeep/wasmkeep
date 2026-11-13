//! A request for a dead node's actor is kept until a survivor took the
//! actor over, instead of answered 503 at once: only a GET or HEAD without
//! a body, or a WebSocket upgrade, from outside, and never past its
//! deadline.

use super::*;

/// The wait these tests ask for: well past what a takeover takes with the
/// tests' timing (ttl 1 s, grace 200 ms, a fence within about 2 s of the
/// owner's last write), on a loaded machine too.
const KEPT: Duration = Duration::from_secs(10);

/// A lease period plus grace, with the tests' timing (ms): how long after
/// its node's last lease write an actor may be taken at the earliest.
const FREE_AFTER_MS: u64 = 1_200;

const NOT_REACHABLE: &str = "the actor's node is not reachable; retry";

/// Node a, which the test kills, and node b, which clients reach. Each
/// sees the bucket through its own `Flaky`.
struct Duo {
    a: Option<Node>,
    b: Node,
    fa: Arc<Flaky>,
    fb: Arc<Flaky>,
    shared: Arc<dyn ObjectStore>,
    /// What b reaches a through, if the test asked for one.
    proxy: Option<PeerProxy>,
    metrics: SocketAddr,
    _dirs: [tempfile::TempDir; 2],
}

async fn duo(wait: Duration, max: usize) -> Duo {
    duo_with(wait, max, "t", test_actor_wasm(), false).await
}

/// `proxied`: b reaches a through a [`PeerProxy`].
async fn duo_with(wait: Duration, max: usize, class: &str, wasm: &[u8], proxied: bool) -> Duo {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = || {
        Arc::new(Flaky::new(
            Arc::clone(&shared),
            Arc::new(AtomicBool::new(false)),
        ))
    };
    let (fa, fb) = (flaky(), flaky());
    let dirs = [(); 2].map(|()| tempfile::tempdir().unwrap());
    let start = async |id: &str, store: Arc<dyn ObjectStore>, dir: &Path, proxied: bool| {
        start_node(
            id,
            dir,
            &store,
            Duration::from_secs(30),
            &[class],
            wasm,
            Duration::from_secs(30),
            "",
            |c| {
                let cluster = c.cluster.as_mut().unwrap();
                cluster.failover_wait = Some(wait);
                cluster.failover_wait_max = max;
            },
            proxied,
        )
        .await
    };
    let (a, proxy) = start(
        "a",
        Arc::clone(&fa) as Arc<dyn ObjectStore>,
        dirs[0].path(),
        proxied,
    )
    .await;
    let (b, _) = start(
        "b",
        Arc::clone(&fb) as Arc<dyn ObjectStore>,
        dirs[1].path(),
        false,
    )
    .await;
    settle().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    Duo {
        a: Some(a),
        b,
        fa,
        fb,
        shared,
        proxy,
        metrics,
        _dirs: dirs,
    }
}

impl Duo {
    fn a(&self) -> SocketAddr {
        self.a.as_ref().unwrap().addr
    }

    /// Kills a: nothing more of it reaches the bucket (no release of its
    /// actors, no `released` lease), then its ports refuse connections.
    async fn kill_a(&mut self) {
        self.fa.down.store(true, Ordering::SeqCst);
        stop_listening(self.a.take().unwrap()).await;
    }

    /// Waits until a left b's view: a POST, which is never kept, is
    /// answered that a is not reachable.
    async fn a_left_the_view(&self, path: &str) {
        let started = std::time::Instant::now();
        loop {
            let r = send(self.b.addr, "POST", path, "").await;
            if r.status == 503 && r.body.trim() == NOT_REACHABLE {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "a never left b's view (last: {} {})",
                r.status,
                r.body.trim()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Whether b can fence a: while it cannot, a's actors are never free.
    fn b_can_fence_a(&self, can: bool) {
        *self.fb.fail_puts_of.lock().unwrap() = (!can).then(|| "nodes/a".to_owned());
    }

    /// When a last wrote its lease (ms), as the bucket has it.
    async fn a_last_wrote_ms(&self) -> u64 {
        use object_store::ObjectStoreExt as _;
        let body = self
            .shared
            .get(&object_store::path::Path::from("nodes/a"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        wasmkeep_cluster::node_lease::NodeLeaseObject::decode(&body)
            .unwrap()
            .written_at_ms
    }

    async fn metric(&self, line: &str) -> u64 {
        let (_, page) = get_raw(self.metrics, "/metrics").await;
        page.lines()
            .find_map(|l| l.strip_prefix(line)?.strip_prefix(' '))
            .map_or(0, |v| v.trim().parse().unwrap())
    }

    /// Requests kept so far whose wait ended with `outcome`.
    async fn waits(&self, kind: &str, outcome: &str) -> u64 {
        self.metric(&format!(
            "wasmkeep_failover_waits_total{{kind=\"{kind}\",outcome=\"{outcome}\"}}"
        ))
        .await
    }

    /// Requests kept so far, however their wait ended.
    async fn kept(&self) -> u64 {
        let (_, page) = get_raw(self.metrics, "/metrics").await;
        page.lines()
            .filter(|l| l.starts_with("wasmkeep_failover_waits_total{"))
            .filter(|l| !l.contains("outcome=\"over_cap\""))
            .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_get_for_a_dead_nodes_actor_waits_and_is_served() {
    let mut d = duo(KEPT, 100).await;
    let r = send(d.a(), "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    d.kill_a().await;
    d.a_left_the_view("/actor/t/x/durable").await;
    let last_write = d.a_last_wrote_ms().await;

    // One GET, never sent again: kept until b took x over.
    let r = send(d.b.addr, "GET", "/actor/t/x/durable", "").await;
    let answered = now_ms();
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1"),
        "kept and served with the committed write"
    );
    // The takeover itself came no sooner than the lease allows: a lease
    // period plus grace after a's last write.
    assert!(
        answered > last_write + FREE_AFTER_MS,
        "served {} ms after a's last lease write",
        answered - last_write
    );
    assert_eq!(d.waits("request", "served").await, 1);
    assert_eq!(d.kept().await, 1);
    assert_eq!(d.metric("wasmkeep_failovers_total").await, 1);
    assert_eq!(d.metric("wasmkeep_node_fences_total").await, 1);
}

/// Before a left b's view, b only learns that a is gone when a connection
/// to it fails: the request that finds out gets the 503 as ever (it may
/// have been sent), and the ones after it are kept.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_refused_connection_later_requests_are_kept() {
    let mut d = duo(KEPT, 100).await;
    let r = send(d.a(), "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    d.kill_a().await;

    let mut found_out = 0;
    let r = loop {
        let r = send(d.b.addr, "GET", "/actor/t/x/durable", "").await;
        if r.status == 200 {
            break r;
        }
        // Only the requests that find a's ports closed: never "a is not
        // reachable", which is what a request gets once a left the view,
        // without the wait.
        assert_eq!(
            (r.status, r.body.trim(), r.retry_after.as_deref()),
            (503, "the actor's node did not answer; retry", Some("1")),
        );
        found_out += 1;
        assert!(found_out <= 3, "{found_out} requests were not kept");
    };
    assert_eq!((r.node.as_deref(), r.body.trim()), (Some("b"), "1"));
    assert_eq!(d.waits("request", "served").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_for_a_dead_nodes_actor_waits_and_opens_on_the_new_owner() {
    use futures_util::StreamExt;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    let mut d = duo_with(KEPT, 100, "chat", chat_wasm(), false).await;
    let (mut alice, _) = connect_async(format!("ws://{}/actor/chat/lobby", d.a()))
        .await
        .unwrap();
    assert!(matches!(alice.next().await, Some(Ok(Message::Text(_)))));
    assert_eq!(
        lease_holder_of(&d.shared, "chat", "lobby").await.as_deref(),
        Some("a")
    );
    d.kill_a().await;
    // Until a left b's view (an upgrade that finds a's ports closed gets
    // the 503 as ever).
    d.a_left_the_view("/actor/chat/lobby").await;
    let last_write = d.a_last_wrote_ms().await;

    // One upgrade, never sent again: 101 once b took the room over.
    let (mut bob, response) = connect_async(format!("ws://{}/actor/chat/lobby", d.b.addr))
        .await
        .expect("the upgrade was kept and accepted");
    assert_eq!(response.status().as_u16(), 101);
    assert!(now_ms() > last_write + FREE_AFTER_MS);
    // The socket lives on b: its `on-websocket-open` ran there.
    match tokio::time::timeout(Duration::from_secs(5), bob.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => {
            assert_eq!(t.to_string(), "* someone joined #lobby (1 online)");
        }
        other => panic!("no message from the room on b: {other:?}"),
    }
    assert_eq!(
        lease_holder_of(&d.shared, "chat", "lobby").await.as_deref(),
        Some("b")
    );
    assert_eq!(d.waits("websocket", "served").await, 1);
}

/// The node an actor's ownership record names.
async fn lease_holder_of(store: &Arc<dyn ObjectStore>, class: &str, name: &str) -> Option<String> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("{class}/{name}")));
    let body = store.get(&key).await.ok()?.bytes().await.ok()?;
    wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)?
        .holder
        .map(|(node, _)| node)
}

/// What is not kept is answered at once, with the wait on as with it off:
/// a POST, a GET with a body, a request another node forwarded, and an
/// actor's call. b cannot fence a here, so a's actors are never free and
/// every answer is the one for a dead node.
async fn unkept_requests_get_todays_answer_at_once(wait: Duration) {
    let mut d = duo(wait, 100).await;
    let r = send(d.a(), "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send(d.b.addr, "POST", "/actor/t/caller/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    d.b_can_fence_a(false);
    d.kill_a().await;
    d.a_left_the_view("/actor/t/x/durable").await;

    let at_once = async |what: &str, req: String| {
        let started = std::time::Instant::now();
        let r = exchange(d.b.addr, &req).await;
        let took = started.elapsed();
        assert!(took < Duration::from_secs(1), "{what}: kept for {took:?}");
        (r.status, r.body.trim().to_owned(), r.retry_after)
    };
    let request = |method: &str, path: &str, extra: &str| {
        format!("{method} {path} HTTP/1.1\r\nHost: t\r\n{extra}Connection: close\r\n\r\n")
    };
    let unavailable = |text: &str| (503, text.to_owned(), Some("1".to_owned()));
    let x = "/actor/t/x/durable";
    assert_eq!(
        at_once("POST", request("POST", x, "Content-Length: 0\r\n")).await,
        unavailable(NOT_REACHABLE)
    );
    assert_eq!(
        at_once("DELETE", request("DELETE", x, "")).await,
        unavailable(NOT_REACHABLE)
    );
    assert_eq!(
        at_once(
            "GET with a body",
            request("GET", x, "Content-Length: 3\r\n") + "abc"
        )
        .await,
        unavailable(NOT_REACHABLE)
    );
    assert_eq!(
        at_once(
            "forwarded GET",
            request(
                "GET",
                x,
                "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n"
            )
        )
        .await,
        unavailable("actor ownership is changing; retry")
    );
    // The calling actor runs on b and gets the call's error: that a is
    // not reachable, or, between nodes over TLS, that the heartbeat naming
    // a's address expired.
    let call = "/actor/t/caller/call?to=t/x&path=%2Fdurable";
    let (status, body, _) = at_once("call", request("POST", call, "Content-Length: 0\r\n")).await;
    assert!(
        status == 200 && body.starts_with("failed:node a"),
        "{status} {body}"
    );
    if wait.is_zero() {
        // And the requests the wait would keep.
        assert_eq!(
            at_once("GET", request("GET", x, "")).await,
            unavailable(NOT_REACHABLE)
        );
        assert_eq!(upgrade_status(d.b.addr, "/actor/t/x").await, 503);
    }
    assert_eq!(d.kept().await, 0, "nothing was kept");
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_bodies_calls_and_forwarded_requests_are_not_kept() {
    unkept_requests_get_todays_answer_at_once(KEPT).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn with_the_wait_off_every_request_is_answered_at_once() {
    unkept_requests_get_todays_answer_at_once(Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wait_ends_at_its_deadline_with_the_same_503() {
    // Past the moment a's actors could be free (1.2 s after its last
    // write), so the request is kept; b cannot fence a, so they never are.
    let wait = Duration::from_secs(2);
    let mut d = duo(wait, 100).await;
    let r = send(d.a(), "GET", "/actor/t/x/visit", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    d.b_can_fence_a(false);
    d.kill_a().await;
    d.a_left_the_view("/actor/t/x/durable").await;

    let started = std::time::Instant::now();
    let r = send(d.b.addr, "GET", "/actor/t/x/visit", "").await;
    let took = started.elapsed();
    assert_eq!(
        (r.status, r.body.trim(), r.retry_after.as_deref()),
        (503, NOT_REACHABLE, Some("1")),
        "what a request for a dead node's actor gets without the wait"
    );
    assert!(
        took >= wait && took < wait + Duration::from_secs(2),
        "answered after {took:?}"
    );
    assert_eq!(d.waits("request", "expired").await, 1);
    // It never ran: once b has the actor, the count is the one a left.
    d.b_can_fence_a(true);
    let r = send(d.b.addr, "GET", "/actor/t/x/visit", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "2")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_leaves_while_its_request_is_kept_is_never_run() {
    let mut d = duo(KEPT, 100).await;
    let r = send(d.a(), "GET", "/actor/t/x/visit", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    d.b_can_fence_a(false);
    d.kill_a().await;
    d.a_left_the_view("/actor/t/x/durable").await;

    // A GET that would count a visit, kept; its client hangs up.
    let mut stream = TcpStream::connect(d.b.addr).await.unwrap();
    stream
        .write_all(b"GET /actor/t/x/visit HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(d.kept().await, 0, "still kept: no wait has ended");
    drop(stream);
    eventually("the kept request is dropped", async || {
        d.waits("request", "left").await == 1
    })
    .await;

    // b takes x over for the next request: one visit more than a counted,
    // not two.
    d.b_can_fence_a(true);
    let r = send(d.b.addr, "GET", "/actor/t/x/visit", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "2"),
        "the abandoned request ran"
    );
    assert_eq!(
        d.metric("wasmkeep_events_total{class=\"t\",kind=\"request\",outcome=\"ok\"}")
            .await,
        2,
        "a's visit and b's: no event for the request whose client left"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_over_the_cap_get_the_503_at_once() {
    // One place on b.
    let mut d = duo(KEPT, 1).await;
    for name in ["x", "y"] {
        let r = send(d.a(), "POST", &format!("/actor/t/{name}/durable"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "{}",
            r.body
        );
    }
    d.b_can_fence_a(false);
    d.kill_a().await;
    d.a_left_the_view("/actor/t/x/durable").await;

    let b = d.b.addr;
    let kept = tokio::spawn(async move { send(b, "GET", "/actor/t/x/durable", "").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    let r = send(d.b.addr, "GET", "/actor/t/y/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.retry_after.as_deref()),
        (503, NOT_REACHABLE, Some("1"))
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(d.waits("request", "over_cap").await, 1);

    // The one that had the place is served.
    d.b_can_fence_a(true);
    let r = kept.await.unwrap();
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );
}

/// A node that is alive and only slow to answer is not waited out, and
/// loses nothing: the request goes to it as ever.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_owner_that_is_only_slow_is_not_treated_as_dead() {
    let d = duo(KEPT, 100).await;
    let a = d.a();
    let r = send(a, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let generation = owner_record(&d.shared, "x").await.unwrap().1;

    // x is busy for 1.5 s, longer than a lease period plus grace.
    let busy = tokio::spawn(async move { send(a, "POST", "/actor/t/x/sleep?ms=1500", "").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    let r = send(d.b.addr, "GET", "/actor/t/x/slept", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("a"), "1"),
        "forwarded to a, and answered behind its event"
    );
    assert!(started.elapsed() >= Duration::from_millis(1_000));
    assert_eq!(busy.await.unwrap().status, 200);
    assert_eq!(d.kept().await, 0, "nothing was kept");
    assert_eq!(d.metric("wasmkeep_node_fences_total").await, 0);
    assert_eq!(
        owner_record(&d.shared, "x").await.unwrap(),
        (Some("a".to_owned()), generation),
        "a still holds x, never taken over"
    );
}

/// A node that is alive, and renews its lease, but refuses this node's
/// connections: the requests after the one that found it out are kept only
/// until its next lease write shows it alive, then answered as without
/// the wait. Nobody waits out the bound, and nobody takes its actors.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_that_is_alive_but_refuses_connections_is_not_waited_out() {
    const DID_NOT_ANSWER: &str = "the actor's node did not answer; retry";
    let d = duo_with(KEPT, 100, "t", test_actor_wasm(), true).await;
    let r = send(d.a(), "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let generation = owner_record(&d.shared, "x").await.unwrap().1;
    d.proxy.as_ref().unwrap().refuse_connections().await;

    // The first GET finds the refusal out and marks a; one after it is
    // kept, until b lists a lease write of a later than the refusal. Each
    // gets the answer of a refused connection, long before the wait's
    // bound.
    let mut asked = 0;
    while d.waits("request", "owner_alive").await == 0 {
        asked += 1;
        assert!(asked <= 20, "no request was kept and then released");
        let started = std::time::Instant::now();
        let r = send(d.b.addr, "GET", "/actor/t/x/durable", "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.retry_after.as_deref()),
            (503, DID_NOT_ANSWER, Some("1")),
        );
        assert!(
            started.elapsed() < KEPT / 2,
            "a live owner was waited for {:?}",
            started.elapsed()
        );
    }
    assert!(
        asked >= 2,
        "the request that found the refusal out was kept"
    );
    for outcome in ["served", "forwarded", "expired", "answered_other"] {
        assert_eq!(d.waits("request", outcome).await, 0, "{outcome}");
    }

    // a lost nothing: it still holds x at the same generation, was never
    // fenced, and serves what reaches it.
    assert_eq!(d.metric("wasmkeep_node_fences_total").await, 0);
    assert_eq!(d.metric("wasmkeep_failovers_total").await, 0);
    assert_eq!(
        owner_record(&d.shared, "x").await.unwrap(),
        (Some("a".to_owned()), generation),
        "a still holds x, never taken over"
    );
    let r = send(d.a(), "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("a"), "1")
    );
}

/// A node an operator fenced is waited for like a dead one: its actors are
/// free a lease period plus grace after its last lease write.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_nodes_actor_is_waited_for() {
    let d = duo(KEPT, 100).await;
    let r = send(d.a(), "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    // As `wasmkeep cluster fence a` does.
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(&d.shared), "");
    wasmkeep_cluster::node_lease::operator_fence(&bucket, &WallClock, "nodes/a")
        .await
        .unwrap();
    let last_write = d.a_last_wrote_ms().await;
    // b lists the fence.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let r = send(d.b.addr, "GET", "/actor/t/x/durable", "").await;
    let answered = now_ms();
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1")
    );
    assert!(
        answered > last_write + FREE_AFTER_MS,
        "served {} ms after a's last lease write",
        answered - last_write
    );
    assert_eq!(d.waits("request", "served").await, 1);
}
