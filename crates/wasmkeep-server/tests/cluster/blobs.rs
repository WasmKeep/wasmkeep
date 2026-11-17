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

/// [`blob_node_with`], or with `None` a node alone.
async fn blob_node_with(cluster: Option<(&str, &Arc<dyn ObjectStore>)>, data: &Path) -> Node {
    class_node(cluster, data, "blob", blob_actor_wasm()).await
}

/// A node serving `class` from `wasm` and the test front; with `None`
/// alone: no bucket, its files on its own disk.
async fn class_node(
    cluster: Option<(&str, &Arc<dyn ObjectStore>)>,
    data: &Path,
    class: &str,
    wasm: &[u8],
) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, wasm);
    let mut config = ActorConfig::new(data);
    // Node links name this node: from configuration, never a request.
    config.blob_links.base = Some(format!("http://{addr}"));
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
        vec![(class.into(), component, wasm.to_vec())],
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

fn files_rust_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/files-rust", "files_rust"))
}

/// A TypeScript guest built beforehand (`npm run build` and `npm run
/// build:starlingmonkey` in tests/guests/files-ts); with
/// `WASMKEEP_REQUIRE_LANGS=1` a missing one fails instead of being skipped.
fn files_ts_wasm(file: &str) -> Option<Vec<u8>> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/guests/files-ts")
        .join(file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!("{} not built; skipping", path.display());
            None
        }
        Err(e) => panic!("{}: {e}", path.display()),
    }
}

/// What an SDK's files calls do against the host, the same in each SDK:
/// conditions, types, a paged listing over several pages, deletes, key
/// refusals and links.
async fn sdk_contract(wasm: &[u8]) -> (Node, tempfile::TempDir) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let node = class_node(Some(("a", &store)), d.path(), "files", wasm).await;
    let f = |p: &str| format!("/actor/files/alice{p}");
    let r = call(
        node.addr,
        "PUT",
        &f("/files/photos/1.txt?type=text/plain"),
        "If-None-Match: *\r\n",
        b"hello",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let etag = etag_of(&r);
    assert!(r.body.ends_with(" 5 text/plain"), "{}", r.body);
    let r = call(
        node.addr,
        "PUT",
        &f("/files/photos/1.txt"),
        "If-None-Match: *\r\n",
        b"x",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (409, "err:conflict"));
    let r = call(
        node.addr,
        "PUT",
        &f("/files/photos/1.txt"),
        "If-Match: \"no\"\r\n",
        b"x",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (409, "err:conflict"));
    let r = call(
        node.addr,
        "PUT",
        &f("/files/photos/1.txt?type=text/plain"),
        &format!("If-Match: {etag}\r\n"),
        b"hello!",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = call(node.addr, "GET", &f("/files/photos/1.txt"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (200, "hello!"));
    let r = call(node.addr, "GET", &f("/info/photos/1.txt"), "", b"").await;
    assert_eq!(r.body, "6 text/plain");
    for k in [
        "photos/2.txt",
        "photos/3.txt",
        "photos/4.txt",
        "photos/5.txt",
        "docs/a",
    ] {
        let r = call(
            node.addr,
            "PUT",
            &f(&format!("/files/{k}")),
            "",
            k.as_bytes(),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    // The iterator over pages of two.
    let r = call(node.addr, "GET", &f("/all?prefix=photos/&page=2"), "", b"").await;
    assert_eq!(
        r.body,
        "photos/1.txt\nphotos/2.txt\nphotos/3.txt\nphotos/4.txt\nphotos/5.txt"
    );
    let r = call(node.addr, "GET", &f("/all"), "", b"").await;
    assert_eq!(r.body.lines().count(), 6);
    let r = call(
        node.addr,
        "GET",
        &f("/page?prefix=photos/&limit=2"),
        "",
        b"",
    )
    .await;
    assert_eq!(r.body, "photos/1.txt\nphotos/2.txt\nnext=photos/2.txt");
    let r = call(
        node.addr,
        "GET",
        &f("/page?prefix=photos/&limit=2&after=photos/4.txt"),
        "",
        b"",
    )
    .await;
    assert_eq!(r.body, "photos/5.txt\nnext=-");
    let r = call(node.addr, "DELETE", &f("/files/docs/a"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (200, "ok"));
    let r = call(node.addr, "GET", &f("/files/docs/a"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (404, "none"));
    let r = call(node.addr, "GET", &f("/info/docs/a"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (404, "none"));
    let r = call(node.addr, "PUT", &f("/files/a//b"), "", b"x").await;
    assert_eq!((r.status, r.body.as_str()), (400, "err:invalid-key"));
    // Links: a download of a file there, and an upload of a new one.
    for link in ["/link/photos/1.txt", "/upload-link/u/1"] {
        let r = call(node.addr, "GET", &f(link), "", b"").await;
        assert_eq!(r.status, 200, "{link}: {}", r.body);
        link_path(&node, r.body.trim());
    }
    let r = call(node.addr, "GET", &f("/link/a//b"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (400, "err:invalid-key"));
    (node, d)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rust_sdk_keeps_files_and_streams_them() {
    let (node, _data) = sdk_contract(files_rust_wasm()).await;
    let f = |p: &str| format!("/actor/files/alice{p}");
    let n: u64 = 16 * 1024 * 1024 + 7;
    let r = call(
        node.addr,
        "POST",
        &f(&format!("/stream/big?n={n}")),
        "",
        b"",
    )
    .await;
    assert_eq!((r.status, r.body.clone()), (200, format!("ok {n}")));
    let r = call(node.addr, "GET", &f("/files/big"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (413, "err:too-large"));
    let sum = |range: std::ops::Range<u64>| range.map(|i| i % 251).sum::<u64>();
    let r = call(node.addr, "GET", &f("/stream/big"), "", b"").await;
    assert_eq!(r.body, format!("read={n} sum={}", sum(0..n)));
    let r = call(
        node.addr,
        "GET",
        &f("/stream/big?start=100&len=70000"),
        "",
        b"",
    )
    .await;
    assert_eq!(r.body, format!("read=70000 sum={}", sum(100..70_100)));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_typescript_sdk_keeps_files_on_quickjs() {
    if let Some(wasm) = files_ts_wasm("files.wasm") {
        sdk_contract(&wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_typescript_sdk_keeps_files_on_starlingmonkey() {
    if let Some(wasm) = files_ts_wasm("files-starlingmonkey.wasm") {
        sdk_contract(&wasm).await;
    }
}

/// The path of a node link made for `node`, checked to start with its
/// configured base.
fn link_path(node: &Node, url: &str) -> String {
    let base = format!("http://{}", node.addr);
    let path = url
        .strip_prefix(&base)
        .unwrap_or_else(|| panic!("{url} is not under {base}"));
    assert!(path.starts_with("/_wasmkeep/blobs/v1/"), "{path}");
    path.to_owned()
}

async fn make_link(node: &Node, query: &str) -> String {
    // A forged Host never reaches a link.
    let r = call(
        node.addr,
        "GET",
        &format!("{A}{query}"),
        "Host: evil.example\r\n",
        b"",
    )
    .await;
    assert_eq!(r.status, 200, "{query}: {}", r.body);
    let url = r
        .body
        .split_whitespace()
        .find(|w| w.starts_with("http"))
        .unwrap()
        .to_owned();
    link_path(node, &url)
}

#[tokio::test(flavor = "multi_thread")]
async fn download_links_serve_the_file_with_the_headers_of_section_8() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = blob_node("a", d1.path(), &store).await;
    let b = blob_node("b", d2.path(), &store).await;
    settle().await;
    for (key, ty, body) in [
        ("doc.html", "text/html", "<script>alert(1)</script>"),
        ("pic.png", "image/png", "PNG"),
        ("pic.svg", "image/svg+xml", "<svg/>"),
    ] {
        let r = call(
            a.addr,
            "PUT",
            &format!("{A}/b/{key}?type={ty}"),
            "",
            body.as_bytes(),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    // An attachment: always bytes, under the name asked for.
    let path = make_link(&a, "/link/doc.html?name=report.html").await;
    for node in [&a, &b] {
        let r = call(node.addr, "GET", &path, "", b"").await;
        assert_eq!(
            (r.status, r.body.as_str()),
            (200, "<script>alert(1)</script>")
        );
        for (name, want) in [
            ("content-type", "application/octet-stream"),
            ("x-content-type-options", "nosniff"),
            ("content-security-policy", "sandbox; default-src 'none'"),
            ("cross-origin-resource-policy", "same-origin"),
        ] {
            assert_eq!(r.header(name).as_deref(), Some(want), "{name}");
        }
        assert_eq!(
            r.header("content-disposition").as_deref(),
            Some("attachment; filename=\"report.html\"; filename*=UTF-8''report.html")
        );
        let cache = r.header("cache-control").unwrap();
        assert!(
            cache.starts_with("private, max-age=") && !cache.ends_with("=0"),
            "{cache}"
        );
    }
    // Inline: an image, with its type; never HTML or SVG.
    let path = make_link(&a, "/link/pic.png?inline").await;
    let r = call(b.addr, "GET", &path, "", b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type").as_deref(), Some("image/png"));
    assert_eq!(r.header("content-disposition").as_deref(), Some("inline"));
    for key in ["doc.html", "pic.svg"] {
        let r = call(a.addr, "GET", &format!("{A}/link/{key}?inline"), "", b"").await;
        assert_eq!(r.status, 403, "{key}: {}", r.body);
        assert!(r.body.starts_with("err:not-allowed:"), "{}", r.body);
    }
    // GET only.
    for method in ["HEAD", "PUT", "POST", "DELETE"] {
        let r = call(a.addr, method, &path, "", b"").await;
        assert_eq!(r.status, 405, "{method}");
    }
    // A changed token, a query, or another path is refused.
    let mut tampered = path.clone().into_bytes();
    let last = tampered.len() - 1;
    tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).unwrap();
    for bad in [
        tampered.as_str(),
        &format!("{path}?x=1"),
        "/_wasmkeep/blobs/v1/",
        "/_wasmkeep/blobs/v1/abc",
    ] {
        let r = call(a.addr, "GET", bad, "", b"").await;
        assert_eq!(r.status, 403, "{bad}: {}", r.body);
    }
    // Expired.
    let path = make_link(&a, "/link/pic.png?secs=1").await;
    assert_eq!(call(a.addr, "GET", &path, "", b"").await.status, 200);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let r = call(a.addr, "GET", &path, "", b"").await;
    assert_eq!(
        (r.status, r.body.as_str()),
        (403, "this link has expired\n")
    );
    // Bounds on a link's life.
    for secs in [0, 3_601] {
        let r = call(
            a.addr,
            "GET",
            &format!("{A}/link/pic.png?secs={secs}"),
            "",
            b"",
        )
        .await;
        assert_eq!(r.status, 403, "{secs}");
    }
    // A link to the file is the file's: deleted, it is gone.
    let path = make_link(&a, "/link/pic.png").await;
    call(a.addr, "DELETE", &format!("{A}/b/pic.png"), "", b"").await;
    assert_eq!(call(b.addr, "GET", &path, "", b"").await.status, 404);
}

async fn put_raw(addr: SocketAddr, path: &str, headers: &str, body: &[u8]) -> Reply {
    let mut req = format!("PUT {path} HTTP/1.1\r\nHost: t\r\n{headers}Connection: close\r\n\r\n")
        .into_bytes();
    req.extend_from_slice(body);
    exchange_bytes(addr, &req).await
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_links_create_one_file_of_the_size_and_type_allowed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = blob_node("a", d1.path(), &store).await;
    let b = blob_node("b", d2.path(), &store).await;
    settle().await;
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/upload-link/up/1?size=5&type=image/png"),
        "",
        b"",
    )
    .await;
    assert!(r.body.starts_with("PUT http://"), "{}", r.body);
    assert!(r.body.ends_with(" 1"), "one header to send: {}", r.body);
    let path = make_link(&a, "/upload-link/up/1?size=5&type=image/png").await;
    // Refused before the body: another size, another type, none, chunked.
    for (headers, body) in [
        (
            "Content-Type: image/png\r\nContent-Length: 4\r\n",
            &b"PNGx"[..],
        ),
        (
            "Content-Type: image/png\r\nContent-Length: 6\r\n",
            b"PNGxyz",
        ),
        ("Content-Type: text/html\r\nContent-Length: 5\r\n", b"PNGxy"),
        ("Content-Length: 5\r\n", b"PNGxy"),
        (
            "Content-Type: image/png\r\nTransfer-Encoding: chunked\r\n",
            b"5\r\nPNGxy\r\n0\r\n\r\n",
        ),
    ] {
        let r = put_raw(b.addr, &path, headers, body).await;
        assert_eq!(r.status, 400, "{headers:?}: {}", r.body);
    }
    // GET is not what the link allows.
    assert_eq!(call(a.addr, "GET", &path, "", b"").await.status, 405);
    // The right one lands, on any node of the app, once.
    let ok = "Content-Type: image/png\r\nContent-Length: 5\r\n";
    let r = put_raw(b.addr, &path, ok, b"PNGxy").await;
    assert_eq!(r.status, 201, "{}", r.body);
    let r = put_raw(a.addr, &path, ok, b"PNGxy").await;
    assert_eq!(r.status, 412, "{}", r.body);
    let r = call(a.addr, "GET", &format!("{A}/head/up/1"), "", b"").await;
    assert!(r.body.starts_with("5 image/png "), "{}", r.body);
    // Not single-use: deleted, the key can be created again by the link.
    call(a.addr, "DELETE", &format!("{A}/b/up/1"), "", b"").await;
    assert_eq!(put_raw(a.addr, &path, ok, b"PNGxy").await.status, 201);
    // Past its expiry.
    let path = make_link(&a, "/upload-link/up/2?size=1&type=text/plain&secs=1").await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let r = put_raw(
        a.addr,
        &path,
        "Content-Type: text/plain\r\nContent-Length: 1\r\n",
        b"x",
    )
    .await;
    assert_eq!(r.status, 403, "{}", r.body);
    // What a link may allow: a checked type, a size within the limit.
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/upload-link/up/3?size=1&type=bad"),
        "",
        b"",
    )
    .await;
    assert_eq!(r.status, 403, "{}", r.body);
    let big = 16 * 1024 * 1024 + 1;
    let r = call(
        a.addr,
        "GET",
        &format!("{A}/upload-link/up/3?size={big}&type=image/png"),
        "",
        b"",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (413, "err:too-large"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_front_links_its_own_files_and_an_actors_token_names_the_actors() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = blob_node("a", d.path(), &store).await;
    settle().await;
    call(a.addr, "PUT", "/b/k", "", b"front").await;
    call(a.addr, "PUT", &format!("{A}/b/k"), "", b"actor").await;
    let r = call(a.addr, "GET", "/link/k", "", b"").await;
    let front = link_path(&a, r.body.trim());
    let actor = make_link(&a, "/link/k").await;
    assert_eq!(call(a.addr, "GET", &front, "", b"").await.body, "front");
    assert_eq!(call(a.addr, "GET", &actor, "", b"").await.body, "actor");
}
