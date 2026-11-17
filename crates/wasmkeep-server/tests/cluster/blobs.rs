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

pub(crate) fn test_front() -> &'static [u8] {
    front_wasm()
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
    class_node_with(cluster, data, class, wasm, |_| {}).await
}

/// [`class_node`], its configuration changed by `configure` last.
async fn class_node_with(
    cluster: Option<(&str, &Arc<dyn ObjectStore>)>,
    data: &Path,
    class: &str,
    wasm: &[u8],
    configure: impl FnOnce(&mut ActorConfig),
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
    configure(&mut config);
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
    let actor = wasmkeep_server::local_app_dir(d.path(), "default", "default")
        .join("blobs")
        .join(&epoch)
        .join("blob")
        .join(sha256_hex(b"alice"));
    // One incarnation, the one the actor's database names.
    let incarnations: Vec<_> = std::fs::read_dir(&actor).unwrap().flatten().collect();
    assert_eq!(incarnations.len(), 1);
    let dir = incarnations[0].path();
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

/// The keys under `blobs/` and `blob-sweeps/` of the implicit app.
async fn file_keys(store: &Arc<dyn ObjectStore>) -> (Vec<String>, Vec<String>) {
    let keys = bucket_keys(store).await;
    let files = keys
        .iter()
        .filter(|k| k.contains("/blobs/"))
        .cloned()
        .collect();
    let records = keys
        .iter()
        .filter(|k| k.contains("/blob-sweeps/"))
        .cloned()
        .collect();
    (files, records)
}

/// After a delete of `name` (by `how`), the next actor at the address
/// finds none of the old files, the old ones go, and a link to one fails.
async fn a_delete_ends_the_files(how: &str) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = blob_node("a", d.path(), &store).await;
    settle().await;
    let n = "/actor/blob/doomed";
    for k in ["k", "dir/k2"] {
        let r = call(a.addr, "PUT", &format!("{n}/b/{k}"), "", b"old").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = call(a.addr, "GET", &format!("{n}/link/k"), "", b"").await;
    let link = link_path(&a, r.body.trim());
    assert_eq!(call(a.addr, "GET", &link, "", b"").await.status, 200);
    match how {
        "delete-self" => {
            let r = call(a.addr, "POST", &format!("{n}/delete"), "", b"").await;
            assert_eq!(r.body, "deleted");
        }
        "delete-self-durable" => {
            let r = call(a.addr, "POST", &format!("{n}/delete-durable"), "", b"").await;
            // Nothing is written by the deleted actor after its delete.
            assert_eq!(r.body, "deleted refused");
        }
        "operator" => {
            request_delete_of(&store, "blob", "doomed").await;
        }
        _ => unreachable!(),
    }
    // The next actor at the address starts in another namespace.
    eventually("the next actor lists none of the old files", async || {
        let r = call(a.addr, "GET", &format!("{n}/list"), "", b"").await;
        r.status == 200 && r.body == "next=-"
    })
    .await;
    let r = call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (404, "none"));
    // And the old files go, with any link to them.
    eventually("the old files are swept", async || {
        file_keys(&store).await.0.is_empty()
    })
    .await;
    assert_eq!(call(a.addr, "GET", &link, "", b"").await.status, 404);
    let (_, records) = file_keys(&store).await;
    assert_eq!(records.len(), 1, "one record, swept: {records:?}");
    let body = store
        .get(&object_store::path::Path::from(records[0].as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("\nswept\n"),
        "{body:?}"
    );
    // The new actor's own files are its own.
    let r = call(a.addr, "PUT", &format!("{n}/b/k"), "", b"new").await;
    assert_eq!(r.status, 200);
    assert_eq!(
        call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await.body,
        "new"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_self_ends_the_actors_files() {
    a_delete_ends_the_files("delete-self").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_self_durable_ends_the_actors_files() {
    a_delete_ends_the_files("delete-self-durable").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_delete_ends_the_actors_files() {
    a_delete_ends_the_files("operator").await;
}

/// A bucket that refuses, on demand, writes of the actors' history (a
/// reset or segment that fails) and rewrites of sweep records after their
/// first write.
#[derive(Debug, Default)]
struct Faulty {
    inner: InMemory,
    fail_history: AtomicBool,
    fail_record_rewrites: AtomicBool,
}

impl std::fmt::Display for Faulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Faulty")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        use object_store::ObjectStoreExt as _;
        let key = location.as_ref();
        let refuse = (self.fail_history.load(Ordering::SeqCst) && key.contains("/actors/"))
            || (self.fail_record_rewrites.load(Ordering::SeqCst)
                && key.contains("/blob-sweeps/")
                && self.inner.head(location).await.is_ok());
        if refuse {
            return Err(object_store::Error::Generic {
                store: "Faulty",
                source: "refused".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A node whose sweep pass runs every 100 ms and acts at ages of
/// 300 ms (judging), 600 ms (the second listing) and 900 ms (the last).
async fn sweeping_node(data: &Path, store: &Arc<dyn ObjectStore>, batched: bool) -> Node {
    sweeping_node_judging_at(data, store, batched, 300).await
}

/// [`sweeping_node`], judging `pending` records at `judge_after` ms.
async fn sweeping_node_judging_at(
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    batched: bool,
    judge_after: u64,
) -> Node {
    class_node_with(Some(("a", store)), data, "blob", blob_actor_wasm(), |c| {
        let cluster = c.cluster.as_mut().unwrap();
        cluster.blob_sweep_every = Duration::from_millis(100);
        cluster.blob_epochs_every = Duration::from_millis(100);
        cluster.blob_sweep_ages = wasmkeep_cluster::blob_sweep::Ages {
            judge_after,
            relist_after: 600,
            last_after: 900,
            age_out: 3_000,
        };
        if batched {
            c.batched.insert("blob".into());
        }
    })
    .await
}

async fn record_body(store: &Arc<dyn ObjectStore>) -> Option<String> {
    let (_, records) = file_keys(store).await;
    let key = records.first()?;
    let body = store
        .get(&object_store::path::Path::from(key.as_str()))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    Some(String::from_utf8_lossy(&body).into_owned())
}

/// A `pending` record of the incarnation the actor still has (a delete
/// that never happened): judged from a fenced restore, dropped, and no
/// file is touched; in a strict class and in a batched one.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_record_of_the_live_incarnation_is_dropped_and_keeps_every_file() {
    for batched in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let d = tempfile::tempdir().unwrap();
        let a = sweeping_node(d.path(), &store, batched).await;
        settle().await;
        let n = "/actor/blob/kept";
        call(a.addr, "PUT", &format!("{n}/b/k"), "", b"mine").await;
        // A judgement needs the actor's history in the bucket (the fence).
        assert_eq!(
            call(a.addr, "POST", &format!("{n}/touch"), "", b"")
                .await
                .body,
            "touched"
        );
        let id = sha256_hex(b"kept");
        let record = wasmkeep_cluster::blob_sweep::Record {
            state: wasmkeep_cluster::blob_sweep::State::Pending,
            epoch: wasmkeep_server::blobs::epoch(None, None),
            name: "kept".into(),
        };
        store
            .put(
                &object_store::path::Path::from(format!(
                    "state/default/default/blob-sweeps/blob/{id}/0"
                )),
                record.encode().into_bytes().into(),
            )
            .await
            .unwrap();
        eventually("the record is dropped", async || {
            file_keys(&store).await.1.is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (files, _) = file_keys(&store).await;
        assert_eq!(files.len(), 1, "batched {batched}: {files:?}");
        let r = call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await;
        assert_eq!(
            (r.status, r.body.as_str()),
            (200, "mine"),
            "batched {batched}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_left_pending_is_judged_deleted_and_swept_then_listed_twice() {
    let faulty = Arc::new(Faulty::default());
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let d = tempfile::tempdir().unwrap();
    let a = sweeping_node(d.path(), &store, false).await;
    settle().await;
    let n = "/actor/blob/gone";
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"old").await;
    // The reset lands; the record is never rewritten `swept`.
    faulty.fail_record_rewrites.store(true, Ordering::SeqCst);
    call(a.addr, "POST", &format!("{n}/delete"), "", b"").await;
    eventually("the next actor starts empty", async || {
        call(a.addr, "GET", &format!("{n}/list"), "", b"")
            .await
            .body
            == "next=-"
    })
    .await;
    assert!(record_body(&store).await.unwrap().contains("\npending\n"));
    assert_eq!(file_keys(&store).await.0.len(), 1, "not swept yet");
    faulty.fail_record_rewrites.store(false, Ordering::SeqCst);
    // Judged deleted: swept.
    eventually("the old files are swept", async || {
        file_keys(&store).await.0.is_empty()
    })
    .await;
    // A late upload under the old incarnation is removed by the next
    // listing; then the record goes.
    let (_, records) = file_keys(&store).await;
    let old = records[0].replace(
        "/blob-sweeps/",
        &format!("/blobs/{}/", wasmkeep_server::blobs::epoch(None, None)),
    );
    store
        .put(
            &object_store::path::Path::from(format!("{old}/late")),
            b"late".to_vec().into(),
        )
        .await
        .unwrap();
    eventually(
        "the late file is removed and the record dropped",
        async || {
            let (files, records) = file_keys(&store).await;
            files.is_empty() && records.is_empty()
        },
    )
    .await;
    // The new actor's file was never touched.
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"new").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await.body,
        "new"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batched_delete_whose_flush_fails_writes_no_record() {
    let faulty = Arc::new(Faulty::default());
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let d = tempfile::tempdir().unwrap();
    let a = sweeping_node(d.path(), &store, true).await;
    settle().await;
    let n = "/actor/blob/batched";
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"mine").await;
    faulty.fail_history.store(true, Ordering::SeqCst);
    // A plain write that cannot be sent: the delete's flush fails, once
    // its upload budget (about 21 s) runs out.
    assert_eq!(
        call(a.addr, "POST", &format!("{n}/touch"), "", b"")
            .await
            .body,
        "touched"
    );
    call(a.addr, "POST", &format!("{n}/delete"), "", b"").await;
    tokio::time::sleep(Duration::from_secs(25)).await;
    assert!(
        file_keys(&store).await.1.is_empty(),
        "no record: nothing was decided"
    );
    faulty.fail_history.store(false, Ordering::SeqCst);
    let r = call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await;
    assert_eq!((r.status, r.body.as_str()), (200, "mine"));
}

#[tokio::test(flavor = "multi_thread")]
async fn other_epochs_files_go_unless_this_runtime_is_stale() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = sweeping_node(d.path(), &store, false).await;
    settle().await;
    call(a.addr, "PUT", &format!("{A}/b/k"), "", b"mine").await;
    let old =
        object_store::path::Path::from("state/default/default/blobs/00000000000000ee/blob/x/0/k");
    store.put(&old, b"old".to_vec().into()).await.unwrap();
    eventually("the other epoch's file goes", async || {
        store.head(&old).await.is_err()
    })
    .await;
    assert_eq!(
        call(a.addr, "GET", &format!("{A}/b/k"), "", b"").await.body,
        "mine"
    );
    // The markers moved: this runtime is stale, and removes nothing.
    store
        .put(
            &object_store::path::Path::from("apps/default/default/restored"),
            b"r".to_vec().into(),
        )
        .await
        .unwrap();
    store.put(&old, b"old".to_vec().into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(
        store.head(&old).await.is_ok(),
        "a stale runtime removes no epoch"
    );
    assert_eq!(file_keys(&store).await.0.len(), 2);
}

/// Judging a record runs no code and leaves the actor's idle age as it
/// found it (ADR 0030, decision 8): an idle actor with a `pending` record
/// judged late still expires on schedule, by its own idle origin.
#[tokio::test(flavor = "multi_thread")]
async fn judging_a_record_leaves_an_idle_actor_to_expire_on_schedule() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &object_store::path::Path::from(wasmkeep_cluster::app_format::key(
                "", "default", "default",
            )),
            wasmkeep_cluster::app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let d = tempfile::tempdir().unwrap();
    let limit = Duration::from_secs(4);
    let a = class_node_with(
        Some(("a", &store)),
        d.path(),
        "blob",
        blob_actor_wasm(),
        |c| {
            c.mailbox.idle_timeout = Duration::from_millis(200);
            c.expire_after_idle.insert("blob".into(), limit);
            let cluster = c.cluster.as_mut().unwrap();
            cluster.expiry_every = Duration::from_millis(300);
            cluster.blob_sweep_every = Duration::from_millis(100);
            cluster.blob_sweep_ages = wasmkeep_cluster::blob_sweep::Ages {
                judge_after: 2_000,
                relist_after: 60_000,
                last_after: 60_000,
                age_out: 120_000,
            };
        },
    )
    .await;
    settle().await;
    let n = "/actor/blob/sleepy";
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"mine").await;
    // A judgement needs the actor's history in the bucket (the fence).
    assert_eq!(
        call(a.addr, "POST", &format!("{n}/touch"), "", b"")
            .await
            .body,
        "touched"
    );
    let id = sha256_hex(b"sleepy");
    let record = wasmkeep_cluster::blob_sweep::Record {
        state: wasmkeep_cluster::blob_sweep::State::Pending,
        epoch: wasmkeep_server::blobs::epoch(None, None),
        name: "sleepy".into(),
    };
    let idle_from = std::time::Instant::now();
    store
        .put(
            &object_store::path::Path::from(format!(
                "state/default/default/blob-sweeps/blob/{id}/0"
            )),
            record.encode().into_bytes().into(),
        )
        .await
        .unwrap();
    // Judged about 2 s in (not deleted: the record goes, the file stays).
    eventually("the record is judged", async || {
        file_keys(&store).await.1.is_empty()
    })
    .await;
    assert_eq!(file_keys(&store).await.0.len(), 1);
    // Expired by its own origin (about 4 s), not 4 s after the judging.
    let deadline = idle_from + Duration::from_millis(5_500);
    loop {
        if file_keys(&store).await.0.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the judging hold made the idle actor look active"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Without a bucket the actor's database names its incarnation: a delete
/// removes the files, the next actor draws another incarnation, and files
/// of any other incarnation left by a crash between the two are removed
/// when the actor starts.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_alone_ends_a_deleted_actors_files() {
    if over_tls() {
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let node = blob_node_with(None, d.path()).await;
    let n = "/actor/blob/local";
    call(node.addr, "PUT", &format!("{n}/b/k"), "", b"old").await;
    let actor_dir = wasmkeep_server::local_app_dir(d.path(), "default", "default")
        .join("blobs")
        .join(wasmkeep_server::blobs::epoch(None, None))
        .join("blob")
        .join(sha256_hex(b"local"));
    let incarnations = || {
        std::fs::read_dir(&actor_dir)
            .map(|r| {
                r.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let first = incarnations();
    assert_eq!(first.len(), 1, "{first:?}");
    assert_ne!(first[0], "0", "a node alone draws its first incarnation");
    for how in ["/delete", "/delete-durable"] {
        call(node.addr, "POST", &format!("{n}{how}"), "", b"").await;
        let r = call(node.addr, "GET", &format!("{n}/list"), "", b"").await;
        assert_eq!(r.body, "next=-", "{how}");
        call(node.addr, "PUT", &format!("{n}/b/k"), "", b"new").await;
        let now = incarnations();
        assert_eq!(now.len(), 1, "{how}: {now:?}");
        assert!(!first.contains(&now[0]));
    }
    // A crash left another incarnation's files: gone at the next start.
    let stray = actor_dir.join("0123456789abcdef").join("f");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("x"), b"stray").unwrap();
    stop_listening(node).await;
    let node = blob_node_with(None, d.path()).await;
    assert_eq!(
        call(node.addr, "GET", &format!("{n}/b/k"), "", b"")
            .await
            .body,
        "new"
    );
    assert_eq!(incarnations().len(), 1);
}

/// A record for the incarnation the actor is at again (the review's
/// scenario: a node without `blobs` reset it to `0`, then files came back
/// under `0`): every listing judges first, and the live files stay; the
/// record goes.
#[tokio::test(flavor = "multi_thread")]
async fn a_swept_record_of_the_live_incarnation_never_removes_its_files() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = sweeping_node(d.path(), &store, false).await;
    settle().await;
    let n = "/actor/blob/back";
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"live").await;
    // History in the bucket, for the judgement's fence.
    assert_eq!(
        call(a.addr, "POST", &format!("{n}/touch"), "", b"")
            .await
            .body,
        "touched"
    );
    let id = sha256_hex(b"back");
    for state in [
        wasmkeep_cluster::blob_sweep::State::Swept,
        wasmkeep_cluster::blob_sweep::State::Listed,
    ] {
        let record = wasmkeep_cluster::blob_sweep::Record {
            state,
            epoch: wasmkeep_server::blobs::epoch(None, None),
            name: "back".into(),
        };
        store
            .put(
                &object_store::path::Path::from(format!(
                    "state/default/default/blob-sweeps/blob/{id}/0"
                )),
                record.encode().into_bytes().into(),
            )
            .await
            .unwrap();
        eventually("the record is judged and dropped", async || {
            file_keys(&store).await.1.is_empty()
        })
        .await;
        assert_eq!(
            file_keys(&store).await.0.len(),
            1,
            "{state:?}: the live file stays"
        );
        let r = call(a.addr, "GET", &format!("{n}/b/k"), "", b"").await;
        assert_eq!((r.status, r.body.as_str()), (200, "live"));
    }
}

/// Without the recorded-head format, judging would rewrite an idle
/// actor's record (its idle age): such an actor is not acquired to judge,
/// and its record ages out, touching no file (decision 8).
#[tokio::test(flavor = "multi_thread")]
async fn without_the_recorded_head_an_idle_actor_is_not_acquired_and_its_record_ages_out() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let a = class_node_with(
        Some(("a", &store)),
        d.path(),
        "blob",
        blob_actor_wasm(),
        |c| {
            c.mailbox.idle_timeout = Duration::from_millis(200);
            let cluster = c.cluster.as_mut().unwrap();
            cluster.blob_sweep_every = Duration::from_millis(100);
            cluster.blob_sweep_ages = wasmkeep_cluster::blob_sweep::Ages {
                judge_after: 300,
                relist_after: 60_000,
                last_after: 60_000,
                age_out: 2_000,
            };
        },
    )
    .await;
    settle().await;
    let n = "/actor/blob/idle";
    call(a.addr, "PUT", &format!("{n}/b/k"), "", b"mine").await;
    assert_eq!(
        call(a.addr, "POST", &format!("{n}/touch"), "", b"")
            .await
            .body,
        "touched"
    );
    // Hibernated: its record names nobody.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let owner = object_store::path::Path::from("state/default/default/owners/blob/idle");
    let before = store.head(&owner).await.unwrap().last_modified;
    let id = sha256_hex(b"idle");
    let record = wasmkeep_cluster::blob_sweep::Record {
        state: wasmkeep_cluster::blob_sweep::State::Pending,
        epoch: wasmkeep_server::blobs::epoch(None, None),
        name: "idle".into(),
    };
    store
        .put(
            &object_store::path::Path::from(format!(
                "state/default/default/blob-sweeps/blob/{id}/0"
            )),
            record.encode().into_bytes().into(),
        )
        .await
        .unwrap();
    eventually("the record ages out", async || {
        file_keys(&store).await.1.is_empty()
    })
    .await;
    assert_eq!(
        store.head(&owner).await.unwrap().last_modified,
        before,
        "never acquired"
    );
    assert_eq!(file_keys(&store).await.0.len(), 1, "no file touched");
}
