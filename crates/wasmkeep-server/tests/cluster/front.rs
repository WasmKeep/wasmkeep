//! The front (ADR 0023) on a cluster: its calls reach actors on another
//! node over a signed hop, with its own caller value, an empty chain, and
//! nothing of its own in the host's header range. The front runs on the
//! node that received the request, never forwarded, and is not served on
//! the cluster port.

use super::*;

fn front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-front", "test_front"))
}

/// A node serving class `t` (the test actor) and, with `front`, the test
/// front beside it, in the cluster sharing `store`.
async fn front_node(id: &str, data: &Path, store: &Arc<dyn ObjectStore>, front: bool) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, test_actor_wasm());
    let mut config = ActorConfig::new(data);
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.alarm_sweep = Duration::from_millis(200);
    cluster.failover_wait = Some(Duration::ZERO);
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(
        runtime.clone(),
        config,
        vec![("t".into(), component, test_actor_wasm().to_vec())],
    )
    .unwrap();
    let front = front.then(|| {
        let component = guests::compile(&runtime, front_wasm());
        wasmkeep_server::HttpComponent::new(runtime.clone(), &component)
            .unwrap()
            .with_max_requests(wasmkeep_server::fds::default_front_requests(16))
    });
    let app = wasmkeep_server::App {
        front,
        actors: Some(service.clone()),
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = async {
        let _ = stopped.await;
    };
    match cluster_port {
        Some(cluster) => tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
            listener,
            cluster,
            app,
            stopped,
            wasmkeep_server::ServeLimits::default(),
        )),
        None => tokio::spawn(wasmkeep_server::serve(listener, app, stopped)),
    };
    let _ = service.node_id_checked().await;
    Node {
        addr,
        service,
        _stop: stop,
    }
}

/// `/call` on the test front: `method path` on `t/<name>`.
fn front_call(name: &str, method: &str, path: &str) -> String {
    format!(
        "/call?to=t/{name}&method={method}&path={}",
        path.replace('%', "%25")
            .replace('?', "%3F")
            .replace('=', "%3D")
            .replace('&', "%26")
    )
}

/// [`send`] to a front path, sent again while the call failed before
/// reaching the owner (a peer briefly out of the view): see
/// [`send_reachable`].
async fn front_get(addr: SocketAddr, path: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = send(addr, "GET", path, "").await;
        let body = reply.body.trim();
        let unreachable =
            body.starts_with("err:failed:node ") && body.ends_with(" is not reachable");
        if !unreachable || tokio::time::Instant::now() >= deadline {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_front_calls_an_actor_on_another_node_as_the_front() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, true).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;

    // b owns t/x before the front calls it.
    let r = send_runnable(b.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("b"));

    // The front on a reaches it on b, with its caller value.
    let r = front_get(a.addr, &front_call("x", "POST", "/caller")).await;
    assert_eq!(
        (r.status, r.body.as_str()),
        (200, "200:default/_front/main")
    );
    // What the front set in the host's range did not cross the hop: not
    // a forged caller, nor a chain, nor any other name in the range.
    let r = front_get(
        a.addr,
        &format!("{}&spoof", front_call("x", "POST", "/caller")),
    )
    .await;
    assert_eq!(r.body, "200:default/_front/main");
    for header in [
        "x-wasmkeep-chain",
        "x-wasmkeep-future",
        "x-wasmkeep-internal",
    ] {
        let path = format!("/header?name={header}");
        let r = front_get(a.addr, &format!("{}&spoof", front_call("x", "GET", &path))).await;
        assert_eq!(r.body, "200:none", "{header}");
    }
    // The actor stayed where it was: the calls were hops, not takes.
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("b"));
    let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("b"))
    );

    // A callee of the front calls on (the test actor's calls are POSTs):
    // the front is not in the chain, which starts empty, so t/y on a
    // reaches t/x on b.
    let inner = "/call?to=t/x&path=%2Fdurable".to_owned();
    let r = front_get(a.addr, &front_call("y", "POST", &inner)).await;
    assert_eq!(r.body, "200:200:2", "{}", r.body);

    // A front's own request never leaves a: the front is not served on b,
    // nor answered by a's front for a path under /actor/.
    let r = send(b.addr, "GET", "/self", "").await;
    assert_eq!(r.status, 404, "{}", r.body);
    let r = send(a.addr, "GET", "/self", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "default/_front/main"));
}

/// A client cannot pose as the front on a cluster either: a forged caller
/// sent to a node that forwards it to the owner is not believed there, nor
/// one sent straight to the owner as a forward (the mark and the app, as
/// a node relays a request).
#[tokio::test(flavor = "multi_thread")]
async fn a_forged_front_caller_is_not_believed_across_nodes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, true).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;
    let r = send_runnable(b.addr, "POST", "/actor/t/z/durable", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "{}", r.body);
    let r = send_reachable(
        a.addr,
        "POST",
        "/actor/t/z/caller",
        "x-wasmkeep-caller: default/_front/main\r\n",
    )
    .await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "none", Some("b")),
        "{}",
        r.body
    );
    let r = send(
        b.addr,
        "POST",
        "/actor/t/z/caller",
        "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n\
         x-wasmkeep-caller: default/_front/main\r\n",
    )
    .await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "none", Some("b")),
        "{}",
        r.body
    );
}

/// A front passing on the answer of an actor on another node (status,
/// every header, body) gets it through: the host's own headers on the
/// hop's answer never reach the front.
#[tokio::test(flavor = "multi_thread")]
async fn a_front_passes_a_remote_callees_answer_on() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, true).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;
    let r = send_runnable(b.addr, "POST", "/actor/t/w/durable", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "{}", r.body);
    let r = front_get(
        a.addr,
        &format!("{}&copy", front_call("w", "POST", "/durable")),
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    assert_eq!(
        r.header("x-callee-range").as_deref(),
        Some(""),
        "{}",
        r.body
    );
    assert_eq!(lease_holder(&store, "w").await.as_deref(), Some("b"));
}

/// [`front_get`] for a batch: sent again while an entry failed before
/// reaching its owner.
async fn front_get_batch(addr: SocketAddr, path: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = send(addr, "GET", path, "").await;
        if !reply.body.contains(" is not reachable") || tokio::time::Instant::now() >= deadline {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// ADR 0025 on a cluster: one batch from the front reaches callees on two
/// nodes at once, each entry a signed call with the front's caller value;
/// from an actor, each entry carries its chain across the hop, so a callee
/// on the other node calling the batch's caller back is `reentrant`.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_reaches_callees_on_two_nodes_at_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, true).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;

    // t/x lives on b, t/w on a.
    let r = send_runnable(b.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    let r = send_runnable(a.addr, "POST", "/actor/t/w/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    let batch = |path: &str| {
        front_call("x", "POST", path).replacen("/call?to=t/x", "/call-many?to=t/x,t/w", 1)
    };
    let r = front_get_batch(a.addr, &batch("/caller")).await;
    assert_eq!(r.body, "200:default/_front/main | 200:default/_front/main");
    // At once: two 800 ms calls, one over the hop, in about one.
    let started = std::time::Instant::now();
    let r = front_get_batch(a.addr, &batch("/sleep?ms=800")).await;
    let took = started.elapsed();
    assert_eq!(r.body, "200:slept | 200:slept");
    assert!(took < Duration::from_millis(1_500), "took {took:?}");
    // The calls were hops, not takes.
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("b"));
    assert_eq!(lease_holder(&store, "w").await.as_deref(), Some("a"));

    // From actor t/w on a: t/x on b calls t/w back, which waits in the
    // chain the hop carried; t/v (on a) is not in it.
    let inner = "%2Fcall%3Fto%3Dt%2Fw";
    let r = send_runnable(
        a.addr,
        "POST",
        &format!("/actor/t/w/call-many?to=t/x,t/v&path={inner}"),
        "",
    )
    .await;
    assert_eq!(r.body.trim(), "200:reentrant | 200:reentrant", "{}", r.body);
    let r = send_runnable(
        a.addr,
        "POST",
        "/actor/t/w/call-many?to=t/x,t/v&path=%2Fcaller",
        "",
    )
    .await;
    assert_eq!(
        r.body.trim(),
        "200:default/t/w | 200:default/t/w",
        "{}",
        r.body
    );
}
