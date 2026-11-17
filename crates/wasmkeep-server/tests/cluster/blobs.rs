//! Files of actors and of the front (ADR 0030, slice B1) on a cluster: the
//! calls of `wasmkeep:actor/blobs` reach the bucket under the app's root and
//! epoch, each caller in its own namespace, from whichever node runs it.

use super::*;
use object_store::ObjectStoreExt as _;

fn blob_actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/blob-actor", "blob_actor"))
}

pub(crate) fn blob_actor() -> &'static [u8] {
    blob_actor_wasm()
}

fn front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-front", "test_front"))
}

/// A node serving class `blob` (the blob test actor) and the test front,
/// in the cluster sharing `store`.
async fn blob_node(id: &str, data: &Path, store: &Arc<dyn ObjectStore>) -> Node {
    blob_node_with(Some((id, store)), data).await
}

/// [`blob_node`], or with `None` a node alone: no bucket, its files on its
/// own disk.
async fn blob_node_with(cluster: Option<(&str, &Arc<dyn ObjectStore>)>, data: &Path) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, blob_actor_wasm());
    let mut config = ActorConfig::new(data);
    if let Some((id, store)) = cluster {
        let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
        cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        cluster.heartbeat = Duration::from_millis(100);
        cluster.failover_wait = Some(Duration::ZERO);
        config.cluster = Some(cluster);
    }
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(
        runtime.clone(),
        config,
        vec![("blob".into(), component, blob_actor_wasm().to_vec())],
    )
    .unwrap();
    let component = guests::compile(&runtime, front_wasm());
    let front = wasmkeep_server::HttpComponent::new(runtime.clone(), &component)
        .unwrap()
        .with_max_requests(wasmkeep_server::fds::default_front_requests(16));
    let app = wasmkeep_server::App {
        front: Some(front),
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

/// `method path` with `headers` (whole lines) and `body`, sent again while
/// the actor's node is not in view or cannot run it yet (refusals made
/// before the actor's code runs).
async fn call(addr: SocketAddr, method: &str, path: &str, headers: &str, body: &[u8]) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        req.extend_from_slice(body);
        let reply = exchange_bytes(addr, &req).await;
        let refused = not_in_view(&reply)
            || (reply.status == 503
                && reply.body.trim() == "this node cannot run the actor now; retry");
        if !refused || tokio::time::Instant::now() >= deadline {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn exchange_bytes(addr: SocketAddr, req: &[u8]) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(req).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let chunked = head.lines().any(|l| {
        l.to_ascii_lowercase()
            .starts_with("transfer-encoding: chunked")
    });
    let header = |name: &str| {
        head.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
        })
    };
    Reply {
        status,
        node: header("x-wasmkeep-node"),
        retry_after: header("retry-after"),
        head: head.to_owned(),
        body: if chunked {
            dechunk(body)
        } else {
            body.to_owned()
        },
    }
}

/// The ETag in a `put` answer, `ok ETAG SIZE TYPE`.
fn etag_of(reply: &Reply) -> String {
    let words: Vec<&str> = reply.body.split_whitespace().collect();
    assert_eq!(words.first(), Some(&"ok"), "{}", reply.body);
    words[1].to_owned()
}

fn listed(reply: &Reply) -> (Vec<String>, String) {
    assert_eq!(reply.status, 200, "{}", reply.body);
    let mut keys = Vec::new();
    let mut next = String::new();
    for line in reply.body.lines() {
        match line.strip_prefix("next=") {
            Some(n) => next = n.to_owned(),
            None => keys.push(line.split(' ').next().unwrap().to_owned()),
        }
    }
    (keys, next)
}

const A: &str = "/actor/blob/alice";

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_keeps_files_in_the_bucket_with_conditions_and_listings() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = blob_node("a", d1.path(), &store).await;
    let b = blob_node("b", d2.path(), &store).await;
    settle().await;

    let put = call(
        a.addr,
        "PUT",
        &format!("{A}/b/photos/1.txt?type=text/plain"),
        "If-None-Match: *\r\n",
        b"hello",
    )
    .await;
    assert_eq!(put.status, 200, "{}", put.body);
    let first = etag_of(&put);
    // Read from either node: the owner runs it, the files are the bucket's.
    for node in [&a, &b] {
        let r = call(node.addr, "GET", &format!("{A}/b/photos/1.txt"), "", b"").await;
        assert_eq!((r.status, r.body.as_str()), (200, "hello"));
        let r = call(node.addr, "GET", &format!("{A}/head/photos/1.txt"), "", b"").await;
        assert_eq!(r.body, format!("5 text/plain {first}"));
    }
    // Each condition.
    let r = call(
        a.addr,
        "PUT",
        &format!("{A}/b/photos/1.txt"),
        "If-None-Match: *\r\n",
        b"x",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (409, "err:conflict"));
    let r = call(
        a.addr,
        "PUT",
        &format!("{A}/b/photos/1.txt"),
        "If-Match: \"other\"\r\n",
        b"x",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (409, "err:conflict"));
    let r = call(
        a.addr,
        "PUT",
        &format!("{A}/b/photos/1.txt"),
        &format!("If-Match: {first}\r\n"),
        b"second",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_ne!(etag_of(&r), first);
    let r = call(b.addr, "GET", &format!("{A}/b/photos/1.txt"), "", b"").await;
    assert_eq!(r.body, "second");
    let r = call(a.addr, "PUT", &format!("{A}/b/photos/1.txt"), "", b"third").await;
    assert_eq!(r.status, 200, "{}", r.body);

    // Listings by prefix and by page.
    for k in ["photos/2.txt", "photos/3.txt", "docs/a", "photo"] {
        let r = call(a.addr, "PUT", &format!("{A}/b/{k}"), "", k.as_bytes()).await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = call(b.addr, "GET", &format!("{A}/list?prefix=photos/"), "", b"").await;
    assert_eq!(
        listed(&r),
        (
            vec![
                "photos/1.txt".into(),
                "photos/2.txt".into(),
                "photos/3.txt".into()
            ],
            "-".into()
        )
    );
    let r = call(b.addr, "GET", &format!("{A}/list?prefix=photo"), "", b"").await;
    assert_eq!(listed(&r).0.len(), 4);
    let r = call(a.addr, "GET", &format!("{A}/list?limit=2"), "", b"").await;
    assert_eq!(
        listed(&r),
        (vec!["docs/a".into(), "photo".into()], "photo".into())
    );
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/list?limit=2&after=photo"),
        "",
        b"",
    )
    .await;
    assert_eq!(
        listed(&r),
        (
            vec!["photos/1.txt".into(), "photos/2.txt".into()],
            "photos/2.txt".into()
        )
    );
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/list?limit=2&after=photos/2.txt"),
        "",
        b"",
    )
    .await;
    assert_eq!(listed(&r), (vec!["photos/3.txt".into()], "-".into()));

    // Delete, twice.
    for _ in 0..2 {
        let r = call(b.addr, "DELETE", &format!("{A}/b/docs/a"), "", b"").await;
        assert_eq!((r.status, r.body.as_str()), (200, "ok"));
    }
    let r = call(a.addr, "GET", &format!("{A}/b/docs/a"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (404, "none"));

    // Keys outside the alphabet or the segment rule never reach the bucket.
    for key in ["a//b", "..", "a/../b", "%2E%2E"] {
        let r = call(a.addr, "PUT", &format!("{A}/b/{key}"), "", b"x").await;
        assert_eq!(r.status, 400, "{key}: {}", r.body);
        assert!(r.body.starts_with("err:invalid-key:"), "{}", r.body);
    }

    // Where they are: the app's root, the default epoch, the hashed name.
    let keys = bucket_keys(&store).await;
    let epoch = wasmkeep_server::blobs::epoch(None, None);
    let id = sha256_hex(b"alice");
    let root = format!("state/default/default/blobs/{epoch}/blob/{id}/0/");
    let files: Vec<&String> = keys.iter().filter(|k| k.contains("/blobs/")).collect();
    assert_eq!(files.len(), 4, "{files:?}");
    assert!(files.iter().all(|k| k.starts_with(&root)), "{files:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn whole_file_calls_stop_at_16_mib_and_streams_go_further() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = blob_node("a", d.path(), &store).await;
    settle().await;
    let n: u64 = 16 * 1024 * 1024 + 10;
    let r = call(
        a.addr,
        "POST",
        &format!("{A}/stream/big?n={n}&chunk=1048576"),
        "",
        b"",
    )
    .await;
    assert_eq!((r.status, r.body.clone()), (200, format!("ok {n}")));
    let r = call(a.addr, "GET", &format!("{A}/b/big"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (413, "err:too-large"));
    let sum = |range: std::ops::Range<u64>| range.map(|i| i % 251).sum::<u64>();
    let r = call(a.addr, "GET", &format!("{A}/stream/big"), "", b"").await;
    assert_eq!(r.body, format!("size={n} read={n} sum={}", sum(0..n)));
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/stream/big?start=1000&len=5000"),
        "",
        b"",
    )
    .await;
    assert_eq!(
        r.body,
        format!("size={n} read=5000 sum={}", sum(1000..6000))
    );
    let r = call(a.addr, "GET", &format!("{A}/head/big"), "", b"").await;
    assert!(
        r.body
            .starts_with(&format!("{n} application/octet-stream ")),
        "{}",
        r.body
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_front_and_actors_each_see_only_their_own_files() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = blob_node("a", d1.path(), &store).await;
    let b = blob_node("b", d2.path(), &store).await;
    settle().await;
    // The same key in four namespaces: the front (on either node: its
    // requests run where they land), and three actors.
    let writers = [
        (a.addr, "/b/k".to_owned(), "front"),
        (a.addr, "/actor/blob/alice/b/k".to_owned(), "alice"),
        (b.addr, "/actor/blob/bob/b/k".to_owned(), "bob"),
        (b.addr, "/actor/blob/al%20ice/b/k".to_owned(), "spaced"),
    ];
    for (addr, path, body) in &writers {
        let r = call(*addr, "PUT", path, "", body.as_bytes()).await;
        assert_eq!(r.status, 200, "{path}: {}", r.body);
    }
    for (_, path, body) in &writers {
        for node in [&a, &b] {
            let r = call(node.addr, "GET", path, "", b"").await;
            assert_eq!((r.status, r.body.as_str()), (200, *body), "{path}");
        }
        let list = path.replace("/b/k", "/list");
        let r = call(a.addr, "GET", &list, "", b"").await;
        assert_eq!(listed(&r), (vec!["k".into()], "-".into()), "{list}");
    }
    // The front's namespace is its own, never a class.
    let keys = bucket_keys(&store).await;
    let epoch = wasmkeep_server::blobs::epoch(None, None);
    assert!(
        keys.contains(&format!(
            "state/default/default/blobs/{epoch}/_front/main/0/k"
        )),
        "{keys:?}"
    );
    assert_eq!(keys.iter().filter(|k| k.contains("/blobs/")).count(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_epoch_follows_the_apps_markers() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let app = AppRef::new(Arc::clone(&store), "", "acme", "shop");
    let local = wasmkeep_server::local_app_dir(d.path(), "acme", "shop");
    let none = app.prepare_local(&local).await.unwrap();
    assert_eq!(none, wasmkeep_server::blobs::epoch(None, None));
    for (marker, body) in [("recreated", "r1"), ("restored", "s1")] {
        store
            .put(
                &object_store::path::Path::from(format!("apps/acme/shop/{marker}")),
                body.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
    }
    let both = app.prepare_local(&local).await.unwrap();
    assert_eq!(
        both,
        wasmkeep_server::blobs::epoch(Some(b"r1"), Some(b"s1"))
    );
    assert_ne!(both, none);
    // Read again, the same markers give the same epoch.
    assert_eq!(app.prepare_local(&local).await.unwrap(), both);
}

/// Writes outside a request: from `on-init` and `on-alarm`, on `node`.
async fn writes_without_a_request(node: &Node) {
    let r = call(node.addr, "GET", "/actor/blob/init-x/b/from-init", "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (200, "from-init"));
    let r = call(node.addr, "POST", "/actor/blob/alarmed/alarm", "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (200, "set"));
    eventually("on-alarm wrote its file", async || {
        let r = call(
            node.addr,
            "GET",
            "/actor/blob/alarmed/b/from-alarm",
            "",
            b"",
        )
        .await;
        r.status == 200 && r.body == "from-alarm"
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_writes_from_on_init_and_on_alarm() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = blob_node("a", d.path(), &store).await;
    settle().await;
    writes_without_a_request(&a).await;
}

/// A node without a bucket keeps files on its own disk, under the app's
/// folder, and finds them again when it starts again.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_alone_keeps_files_on_its_disk_across_a_restart() {
    if over_tls() {
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let node = blob_node_with(None, d.path()).await;
    writes_without_a_request(&node).await;
    for (path, body) in [("/actor/blob/alice/b/k", "alice"), ("/b/k", "front")] {
        let r = call(
            node.addr,
            "PUT",
            path,
            "If-None-Match: *\r\n",
            body.as_bytes(),
        )
        .await;
        assert_eq!(r.status, 200, "{path}: {}", r.body);
        let r = call(node.addr, "PUT", path, "If-None-Match: *\r\n", b"again").await;
        assert_eq!(r.status, 409, "{path}: {}", r.body);
    }
    let r = call(
        node.addr,
        "POST",
        "/actor/blob/alice/stream/s?n=300000",
        "",
        b"",
    )
    .await;
    assert_eq!(r.body, "ok 300000");
    stop_listening(node).await;

    let node = blob_node_with(None, d.path()).await;
    for (path, body) in [("/actor/blob/alice/b/k", "alice"), ("/b/k", "front")] {
        let r = call(node.addr, "GET", path, "", b"").await;
        assert_eq!((r.status, r.body.as_str()), (200, body), "{path}");
    }
    let r = call(node.addr, "GET", "/actor/blob/alice/list", "", b"").await;
    assert_eq!(listed(&r), (vec!["k".into(), "s".into()], "-".into()));
    let sum: u64 = (0..300_000u64).map(|i| i % 251).sum();
    let r = call(node.addr, "GET", "/actor/blob/alice/stream/s", "", b"").await;
    assert_eq!(r.body, format!("size=300000 read=300000 sum={sum}"));
    // On disk under the app's folder and epoch, by namespace; no file is
    // named after a key.
    let epoch = wasmkeep_server::blobs::epoch(None, None);
    let dir = wasmkeep_server::local_app_dir(d.path(), "default", "default")
        .join("blobs")
        .join(&epoch)
        .join("blob")
        .join(sha256_hex(b"alice"))
        .join("0");
    assert!(dir.join("index.db").exists(), "{}", dir.display());
    let files: Vec<_> = std::fs::read_dir(dir.join("f")).unwrap().collect();
    assert_eq!(files.len(), 2);
    assert!(
        walk(d.path())
            .iter()
            .all(|p| p.file_name().is_none_or(|n| n != "k" && n != "s"))
    );
}
