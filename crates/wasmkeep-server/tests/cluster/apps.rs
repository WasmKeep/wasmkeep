//! The app between nodes (ADR 0022): every hop names its app in
//! `x-wasmkeep-app`, a node refuses a hop for an app it does not serve, and
//! nodes given classes on the command line run as `default/default`.

use super::*;

/// A node of `app` (`tenant/app`) running the test actor as class `t`
/// (`Internal` when `internal`), its actors of `tenant`.
async fn app_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    app: &str,
    internal: bool,
) -> Node {
    let app = app.to_owned();
    node_configured(
        id,
        data,
        store,
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        move |config| {
            config.tenant = app.split('/').next().unwrap().to_owned();
            config.cluster.as_mut().unwrap().app = app;
            if internal {
                let mut rules = wasmkeep_server::AccessRules::default();
                rules.set("t", wasmkeep_server::Access::Internal);
                config.access = rules;
            }
        },
    )
    .await
}

/// The cluster secret, once the first node has written it and node `id`
/// (at `addr`) has loaded it: a signed `GET` naming an app it does not
/// serve, with no forwarded mark, is a hop (503) only once the signature
/// checks out; before, it is a request from outside, served.
async fn cluster_secret(store: &Arc<dyn ObjectStore>, id: &str, addr: SocketAddr) -> String {
    let key = object_store::path::Path::from("cluster/secret");
    let secret = loop {
        if let Ok(got) = object_store::ObjectStoreExt::get(store.as_ref(), &key).await {
            break String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let secret = secret.trim().to_owned();
    let path = "/actor/t/secret-probe/durable";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let probe = sign(
            &secret,
            id,
            "GET",
            path,
            Some("probe/none"),
            Some("probe/none"),
            false,
        );
        let r = send(addr, "GET", path, &probe).await;
        if r.status == 503 {
            break secret;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{id} never loaded the cluster secret: {} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The headers of a call signed as node calls are, for node `to`, `method
/// path` with an empty body, naming `signed_app` in the signature (no app
/// line when `None`), sending `sent_app` (no header when `None`) and the
/// forwarded mark nodes put on their calls when `forwarded`.
fn sign(
    secret: &str,
    to: &str,
    method: &str,
    path: &str,
    signed_app: Option<&str>,
    sent_app: Option<&str>,
    forwarded: bool,
) -> String {
    static SENT: AtomicUsize = AtomicUsize::new(0);
    let ts = now_ms();
    let nonce = format!("{ts:x}0{:x}", SENT.fetch_add(1, Ordering::Relaxed));
    let hash = sha256_hex(b"");
    let mut msg = format!("wasmkeep-call v4\n{ts}\n{nonce}\n{to}\n{method}\n{path}\n{hash}\n");
    if let Some(app) = signed_app {
        msg.push_str(&format!("x-wasmkeep-app:{app}\n"));
    }
    let mac = hmac_hex(secret.as_bytes(), msg.as_bytes());
    let mut headers = format!("x-wasmkeep-internal: v4 {ts} {nonce} {hash} {mac}\r\n");
    if forwarded {
        headers.push_str("x-wasmkeep-forwarded: 1\r\n");
    }
    if let Some(app) = sent_app {
        headers.push_str(&format!("x-wasmkeep-app: {app}\r\n"));
    }
    headers
}

/// [`sign`] for `POST path`, with the forwarded mark, as nodes call.
fn signed_headers(
    secret: &str,
    to: &str,
    path: &str,
    signed_app: Option<&str>,
    sent_app: Option<&str>,
) -> String {
    sign(secret, to, "POST", path, signed_app, sent_app, true)
}

/// Two apps of one tenant on one prefix each have their own actor of a
/// name (ADR 0022); a hop naming another app is refused before the actor
/// runs, so no node ever serves another app's actors.
#[tokio::test(flavor = "multi_thread")]
async fn a_hop_for_another_app_gets_503_and_nothing_runs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let shop = app_node("shop-1", dirs.0.path(), &store, "acme/shop", false).await;
    let admin = app_node("admin-1", dirs.1.path(), &store, "acme/admin", false).await;
    settle().await;
    let r = send_runnable(shop.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("shop-1"))
    );
    // Through the other app's node: its own actor of that name, in its own
    // state root (ADR 0022), never relayed to the first app's owner.
    let r = send(admin.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("admin-1")),
        "{}",
        r.body
    );
    // A request relayed by a node of the other app (the forwarded mark and
    // its app, as a node relays a client's request): refused before it
    // runs, as is a relayed WebSocket upgrade.
    let relayed = "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: acme/admin\r\n";
    let r = send(shop.addr, "POST", "/actor/t/x/durable", relayed).await;
    assert_eq!(
        (r.status, r.body.trim(), r.retry_after.as_deref()),
        (503, "not this app's node", Some("1")),
        "{}",
        r.body
    );
    {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
        let mut req = format!("ws://{}/actor/t/x", shop.addr)
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("x-wasmkeep-forwarded", "1".parse().unwrap());
        req.headers_mut()
            .insert("x-wasmkeep-app", "acme/admin".parse().unwrap());
        match tokio_tungstenite::connect_async(req).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => {
                assert_eq!(r.status().as_u16(), 503);
            }
            other => panic!("a relayed upgrade for another app: {:?}", other.map(drop)),
        }
    }
    // A call signed by a node of the other app: refused before its body.
    let secret = cluster_secret(&store, "shop-1", shop.addr).await;
    let path = "/actor/t/x/durable";
    let signed = signed_headers(
        &secret,
        "shop-1",
        path,
        Some("acme/admin"),
        Some("acme/admin"),
    );
    let r = send(shop.addr, "POST", path, &signed).await;
    assert_eq!(
        (r.status, r.body.trim()),
        (503, "not this app's node"),
        "{}",
        r.body
    );
    // Nothing ran: the counter is where the first request left it.
    let r = send(shop.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    // Each refusal counted with its own reason (this process's metrics:
    // nextest runs each test alone): the three above and the probe that
    // found the secret, which names an app no node serves.
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.lines()
            .any(|l| l == "wasmkeep_admission_refused_total{reason=\"wrong_app\"} 4"),
        "{page}"
    );
}

/// The app is in the signature: a call signed for one app and sent naming
/// another is not a node's call, so an internal class refuses it and the
/// actor does not run.
#[tokio::test(flavor = "multi_thread")]
async fn a_signed_app_header_cannot_be_altered() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let shop = app_node("shop-1", dir.path(), &store, "acme/shop", true).await;
    let secret = cluster_secret(&store, "shop-1", shop.addr).await;
    let path = "/actor/t/x/durable";
    // As signed: a node's call, so the internal class answers.
    let signed = signed_headers(
        &secret,
        "shop-1",
        path,
        Some("acme/shop"),
        Some("acme/shop"),
    );
    let r = send_runnable(shop.addr, "POST", path, &signed).await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the test signs like a node: {}",
        r.body
    );
    // Signed for acme/admin, the header changed on the way to acme/shop:
    // the signature no longer holds, so it is a request from outside.
    let altered = signed_headers(
        &secret,
        "shop-1",
        path,
        Some("acme/admin"),
        Some("acme/shop"),
    );
    let r = send(shop.addr, "POST", path, &altered).await;
    assert_eq!(r.status, 403, "{}", r.body);
    let r = send_runnable(
        shop.addr,
        "POST",
        path,
        &signed_headers(
            &secret,
            "shop-1",
            "/actor/t/x/durable",
            Some("acme/shop"),
            Some("acme/shop"),
        ),
    )
    .await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "the altered call did not run"
    );
}

/// Nodes of an older version send no `x-wasmkeep-app`: their hops are
/// refused with a message saying so (upgrades stop every node).
#[tokio::test(flavor = "multi_thread")]
async fn a_hop_without_the_app_is_refused_as_an_older_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let secret = cluster_secret(&store, "a", a.addr).await;
    let path = "/actor/t/x/durable";
    // Signed as an older node signs: no app line, no header; once more
    // without the forwarded mark, so that only the signature makes it a hop.
    let old_call = signed_headers(&secret, "a", path, None, None);
    let old_unmarked = sign(&secret, "a", "POST", path, None, None, false);
    // Relayed as an older node relays a client's request.
    for extra in [
        old_call.as_str(),
        old_unmarked.as_str(),
        "x-wasmkeep-forwarded: 1\r\n",
    ] {
        let r = send(a.addr, "POST", path, extra).await;
        assert_eq!(r.status, 503, "{extra:?}: {}", r.body);
        assert!(r.body.contains("older version"), "{extra:?}: {}", r.body);
        // Only an operator can fix it: no invitation to retry every second.
        assert_eq!(r.retry_after, None, "{extra:?}");
    }
    let r = send(a.addr, "GET", path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "0"), "nothing ran");
}

/// A client cannot name an app: the header never reaches an actor, nor
/// rides a forward to the owner (which would refuse the request).
#[tokio::test(flavor = "multi_thread")]
async fn an_app_header_from_outside_is_stripped() {
    let p = x_on_a_y_on_b().await;
    let path = "/actor/t/x/header?name=x-wasmkeep-app";
    let forged = "x-wasmkeep-app: evil/app\r\n";
    // Straight to the owner, then through the other node.
    for node in [p.a.addr, p.b.addr] {
        let r = send_reachable(node, "GET", path, forged).await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, "none", Some("a")),
            "{}",
            r.body
        );
    }
}

/// `serve --actor` (no `--app`): the nodes run as the implicit app
/// `default/default`, say so in their heartbeats, and serve each other.
#[tokio::test(flavor = "multi_thread")]
async fn actor_nodes_form_a_cluster_as_default_default() {
    let p = x_on_a_y_on_b().await;
    let r = send_reachable(p.b.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("a")),
        "forwarded to the owner"
    );
    let r = send(
        p.a.addr,
        "POST",
        "/actor/t/x/call?to=t/y&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:2"), "a call to b");
    // The bucket as a sees it (passed through while not cut off).
    let store: Arc<dyn ObjectStore> = Arc::clone(&p.flaky) as Arc<dyn ObjectStore>;
    for id in ["a", "b"] {
        let got = object_store::ObjectStoreExt::get(
            store.as_ref(),
            &object_store::path::Path::from(format!("nodes/{id}")),
        )
        .await
        .unwrap();
        let body = String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap();
        assert_eq!(body.lines().nth(6), Some("default/default"), "{id}: {body}");
    }
}

/// Two apps of one tenant on one prefix, the same class and name, each
/// with an alarm: each alarm is indexed under its own app's root and fires
/// once, in its own app (ADR 0022: no tenant filter on the alarm scan, and
/// none needed).
#[tokio::test(flavor = "multi_thread")]
async fn alarms_of_two_apps_with_the_same_names_stay_apart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let shop = app_node("shop-1", dirs.0.path(), &store, "acme/shop", false).await;
    let blog = app_node("blog-1", dirs.1.path(), &store, "acme/blog", false).await;
    settle().await;
    for node in [&shop, &blog] {
        let r = send_runnable(node.addr, "POST", "/actor/t/y/alarm?in=1500", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    for node in [&shop, &blog] {
        loop {
            let r = send_runnable(node.addr, "GET", "/actor/t/y/fired", "").await;
            if r.body.trim() == "1" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "an alarm never fired: {}",
                r.body
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // Once each, never twice in one app (a sweep of the other app's root).
    tokio::time::sleep(Duration::from_secs(2)).await;
    for node in [&shop, &blog] {
        let r = send_runnable(node.addr, "GET", "/actor/t/y/fired", "").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    }
    // Each app's history is under its own root.
    let all = bucket_keys(&store).await;
    for app in ["shop", "blog"] {
        assert!(
            all.iter()
                .any(|k| k.starts_with(&format!("state/acme/{app}/actors/t/y/"))),
            "no history under {app}'s root: {all:?}"
        );
    }
}
