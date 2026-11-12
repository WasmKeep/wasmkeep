//! Revocation between nodes (ADR 0011): a node stops being trusted within
//! the heartbeat horizon (its heartbeat's ttl, three periods, plus the
//! allowed clock skew) once its heartbeat is deleted, once it stops
//! heartbeating, or once it restarts with a new key; and a node that can no
//! longer list the heartbeats trusts the pins it knew no longer than that.
//!
//! Two real nodes, `a` and `b`, over one in-memory bucket, each with a
//! cluster listener and a plain address nobody listens on (so everything
//! between them goes over mutual TLS). Each test warms up every kind of
//! connection between them in both directions, then makes `a` stop trusting
//! `b` and checks, timing each against the horizon:
//! - `a` stops sending to `b` (its pooled connection is not reused);
//! - `b`'s kept-alive connection to `a`'s cluster port is refused on its
//!   next request, and `b`'s new handshakes with `a` are refused;
//! - a WebSocket `a` relays to `b` is cut on `a`'s side, and one `b`
//!   relays to `a` is cut on `a`'s (the owner's) side;
//! - an `actors.call` in flight between them when trust lapses does not
//!   deliver its response, in either direction, and the next one fails.
//!
//! `b` keeps trusting `a` throughout, so each cut is made by `a`'s own
//! mechanism, never by `b` closing its side.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, ClusterConfig, ClusterTlsConfig, LeaseConfig};

#[path = "support/guests.rs"]
mod guests;

fn test_actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

/// The heartbeat period of the test nodes.
const PERIOD: Duration = Duration::from_millis(100);

/// The heartbeat horizon, as the node computes it: a heartbeat's ttl
/// (three periods) plus the lease config's allowed skew (100 ms below).
const HORIZON: Duration = Duration::from_millis(400);

/// How often an upgraded connection re-checks its peer (the server's
/// `cluster_tls::REVOCATION_CHECK`).
const CHECK: Duration = Duration::from_millis(500);

/// Margin for scheduling on a loaded machine. The measured times are
/// printed (run with `--no-capture`); the bound only catches a mechanism
/// that does not work at all.
const SLACK: Duration = Duration::from_millis(1_500);

/// How long the actor called in flight takes to answer: well after trust
/// lapsed.
const SLOW_CALL_MS: u64 = 2_500;

/// One node's view of the shared bucket, with switches the tests flip:
/// its own heartbeat writes, and its listings of the heartbeats.
#[derive(Debug)]
struct Gate {
    inner: Arc<dyn ObjectStore>,
    /// `nodes/<id>`: this node's heartbeat.
    heartbeat: String,
    beats: AtomicBool,
    lists: AtomicBool,
}

impl Gate {
    fn new(inner: &Arc<dyn ObjectStore>, id: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            heartbeat: format!("nodes/{id}"),
            beats: AtomicBool::new(true),
            lists: AtomicBool::new(true),
        })
    }

    fn refused(what: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "gate",
            source: format!("{what} refused by the test").into(),
        }
    }
}

impl std::fmt::Display for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gate({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Gate {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref() == self.heartbeat && !self.beats.load(Ordering::SeqCst) {
            return Err(Self::refused("heartbeat"));
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        let heartbeats = prefix.is_some_and(|p| p.as_ref().starts_with("nodes"));
        if heartbeats && !self.lists.load(Ordering::SeqCst) {
            return futures_util::stream::once(async { Err(Self::refused("listing")) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Node {
    addr: SocketAddr,
    gate: Arc<Gate>,
    _service: ActorService,
    _stop: oneshot::Sender<()>,
}

/// A node serving class `t`, with a cluster listener, publishing a plain
/// address where nothing listens: peers reach it over TLS or not at all.
async fn node(id: &str, data: &Path, shared: &Arc<dyn ObjectStore>) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(test_actor_wasm()).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_secs(30);
    let gate = Gate::new(shared, id);
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut cluster = ClusterConfig::new(id, "http://127.0.0.1:9", store);
    // Max skew 100 ms: with the 100 ms period, the horizon is 400 ms.
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = PERIOD;
    cluster.alarm_sweep = Duration::from_millis(200);
    cluster.cluster_tls = Some(ClusterTlsConfig::new(format!(
        "https://{}",
        cluster_listener.local_addr().unwrap()
    )));
    config.cluster = Some(cluster);
    let service = ActorService::new(
        runtime,
        config,
        vec![("t".into(), component, test_actor_wasm().to_vec())],
    )
    .unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
        listener,
        cluster_listener,
        service.clone(),
        async {
            let _ = stopped.await;
        },
        wasmkeep_server::ServeLimits::default(),
    ));
    // The start check (ttl plus grace, ADR 0020) and the first heartbeat.
    let _ = service.node_id_checked().await;
    Node {
        addr,
        gate,
        _service: service,
        _stop: stop,
    }
}

#[derive(Debug)]
struct Reply {
    status: u16,
    node: Option<String>,
    body: String,
}

/// A request to a node's public port, on a connection of its own. No
/// `Connection: close`: a node forwards the client's headers, and that one
/// would keep it from pooling its connection to the owner.
async fn send(addr: SocketAddr, method: &str, path: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    // Until the whole response arrived (or the connection ended).
    while !complete(&raw) {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
        }
    }
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let node = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("x-wasmkeep-node")
            .then(|| v.trim().to_owned())
    });
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_owned()
    };
    Reply {
        status,
        node,
        body: body.trim().to_owned(),
    }
}

/// Whether `raw` holds a whole response: its head, and a body of the
/// length it gives (or chunked, up to the last chunk).
fn complete(raw: &[u8]) -> bool {
    let raw = String::from_utf8_lossy(raw);
    let Some((head, body)) = raw.split_once("\r\n\r\n") else {
        return false;
    };
    let head = head.to_ascii_lowercase();
    if head.contains("transfer-encoding: chunked") {
        return body.starts_with("0\r\n\r\n") || body.contains("\r\n0\r\n\r\n");
    }
    let length = head.lines().find_map(|l| {
        l.strip_prefix("content-length:")
            .and_then(|n| n.trim().parse::<usize>().ok())
    });
    // Neither a length nor chunks: until the connection ends.
    length.is_some_and(|n| body.len() >= n)
}

/// The body of a chunked response, decoded (what arrived of it).
fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        if n == 0 || tail.len() < n {
            break;
        }
        out.push_str(&tail[..n]);
        rest = tail.get(n + 2..).unwrap_or_default();
    }
    out
}

/// Polls `check` every 20 ms until it gives something: that, and how long
/// after `since` it first did. Panics past `bound`.
async fn within<T, F, Fut>(
    what: &str,
    since: Instant,
    bound: Duration,
    mut check: F,
) -> (T, Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    loop {
        if let Some(got) = check().await {
            return (got, since.elapsed());
        }
        assert!(
            since.elapsed() <= bound,
            "{what}: not revoked {:?} after the event (bound {bound:?})",
            since.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A WebSocket to `name` through `relay`'s public port, echoing.
async fn websocket(
    relay: SocketAddr,
    name: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{relay}/actor/t/{name}"))
        .await
        .unwrap();
    ws.send(Message::text("ping")).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(echo.unwrap().unwrap(), Message::text("ping"));
    ws
}

/// Resolves with the time the WebSocket was closed or failed.
fn cut_at(
    mut ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) -> tokio::task::JoinHandle<Instant> {
    tokio::spawn(async move {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_)) | Err(_)) | None => return Instant::now(),
                Some(Ok(_)) => {}
            }
        }
    })
}

/// An `actors.call` from `caller` (on `via`) to `to`'s `/sleep`, taking
/// [`SLOW_CALL_MS`]: what the caller got (`status:body` or `failed:...`).
fn slow_call(via: SocketAddr, caller: &str, to: &str) -> tokio::task::JoinHandle<String> {
    let path = format!("/actor/t/{caller}/call?to=t/{to}&path=%2Fsleep%3Fms%3D{SLOW_CALL_MS}");
    tokio::spawn(async move { send(via, "POST", &path).await.body })
}

/// How `a` comes to stop trusting `b`.
#[derive(Debug, Clone, Copy)]
enum Revoke {
    /// `b`'s heartbeat is deleted (and `b` cannot write it again): the
    /// node was removed from the cluster.
    Deleted,
    /// `b` can no longer write its heartbeat (cut off from the bucket):
    /// the last one ages out.
    Silent,
    /// `b`'s heartbeat names a new key (it restarted), while the old
    /// process, still holding its connections, keeps running.
    NewKey,
    /// `b` is fine, but `a` can no longer list the heartbeats: what it
    /// knew lapses with the heartbeats it last listed.
    ListingFails,
}

/// Makes `a` stop trusting `b`; returns when the revoking event happened
/// (the deadline runs from there), and a task to stop at the end.
async fn revoke(
    how: Revoke,
    store: &Arc<dyn ObjectStore>,
    a: &Node,
    b: &Node,
) -> (Instant, Option<tokio::task::JoinHandle<()>>) {
    let key = StorePath::from("nodes/b");
    match how {
        Revoke::ListingFails => {
            a.gate.lists.store(false, Ordering::SeqCst);
            (Instant::now(), None)
        }
        Revoke::Silent => {
            b.gate.beats.store(false, Ordering::SeqCst);
            (Instant::now(), None)
        }
        Revoke::Deleted => {
            b.gate.beats.store(false, Ordering::SeqCst);
            // Let a write already past the gate land first.
            tokio::time::sleep(Duration::from_millis(30)).await;
            store.delete(&key).await.unwrap();
            (Instant::now(), None)
        }
        Revoke::NewKey => {
            b.gate.beats.store(false, Ordering::SeqCst);
            // The old process keeps running unaware of the new one: it does
            // not list the heartbeats (seeing another pin at its own
            // address, it would drop its own pools, and the kept-alive
            // connection this test checks on a's side would go unused).
            b.gate.lists.store(false, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(30)).await;
            let old = store.get(&key).await.unwrap().bytes().await.unwrap();
            let old = String::from_utf8(old.to_vec()).unwrap();
            let pin = new_pin();
            let beat = move || {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .to_string();
                let mut lines: Vec<&str> = old.lines().collect();
                // Format 3 (ADR 0020): the node lease's three lines (the
                // third is written_at), then the heartbeat's fields: id,
                // address, version, app, cluster address, pins, drain
                // state, reads secrets, take.
                assert_eq!(lines.len(), 12, "{old}");
                lines[2] = &now;
                lines[8] = &pin;
                format!("{}\n", lines.join("\n"))
            };
            store.put(&key, beat().into_bytes().into()).await.unwrap();
            let since = Instant::now();
            let store = Arc::clone(store);
            let writer = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    store.put(&key, beat().into_bytes().into()).await.unwrap();
                }
            });
            (since, Some(writer))
        }
    }
}

/// The pin of a fresh key: `b` restarted.
fn new_pin() -> String {
    use sha2::{Digest, Sha256};
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = rcgen::CertificateParams::new(vec!["b.test".to_owned()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    Sha256::digest(cert.der())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The scenario every test runs: see the module docs.
async fn a_stops_trusting_b(how: Revoke) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store).await;
    let b = node("b", db.path(), &store).await;

    // Actors on each node: one to request, one to call slowly, one that
    // calls.
    for (n, name) in [
        (&a, "on-a"),
        (&a, "slow-a"),
        (&a, "caller-a"),
        (&b, "on-b"),
        (&b, "slow-b"),
        (&b, "caller-b"),
    ] {
        let r = send(n.addr, "GET", &format!("/actor/t/{name}/fired")).await;
        assert_eq!(r.status, 200, "{name}: {}", r.body);
    }
    // Both nodes reach each other: requests relayed both ways (each node
    // now has a connection to the other in its pool), and calls.
    let started = Instant::now();
    loop {
        let ab = send(a.addr, "GET", "/actor/t/on-b/fired").await;
        let ba = send(b.addr, "GET", "/actor/t/on-a/fired").await;
        if (ab.status, ab.node.as_deref(), ba.status, ba.node.as_deref())
            == (200, Some("b"), 200, Some("a"))
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the nodes never reached each other: {ab:?} {ba:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = send(
        a.addr,
        "POST",
        "/actor/t/caller-a/call?to=t/on-b&path=/fired",
    )
    .await;
    assert!(r.body.starts_with("200:"), "{}", r.body);
    let r = send(
        b.addr,
        "POST",
        "/actor/t/caller-b/call?to=t/on-a&path=/fired",
    )
    .await;
    assert!(r.body.starts_with("200:"), "{}", r.body);
    // WebSockets relayed both ways.
    let a_relays = cut_at(websocket(a.addr, "on-b").await);
    let b_relays = cut_at(websocket(b.addr, "on-a").await);
    // Calls in flight both ways when trust lapses.
    let a_calls = slow_call(a.addr, "caller-a", "slow-b");
    let b_calls = slow_call(b.addr, "caller-b", "slow-a");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (since, writer) = revoke(how, &store, &a, &b).await;
    let bound = HORIZON + SLACK;

    // a no longer sends to b: the connection it pooled is not reused (b
    // would answer on it; it trusts a). The 503 is a's own, made before
    // sending: an answer refused once it came back ("no longer trusted")
    // means the request went out, so polling goes on.
    let (a_addr, b_addr) = (a.addr, b.addr);
    let (refusal, sends) = within("a sending to b", since, bound, move || async move {
        let r = send(a_addr, "GET", "/actor/t/on-b/fired").await;
        let sent = r.status != 503 || r.body == "the actor's node is no longer trusted; retry";
        (!sent).then_some(r.body)
    })
    .await;
    let expected = match how {
        // A new key at b's address: the pool is dropped, and the handshake
        // with the old key fails.
        Revoke::NewKey => "the actor's node did not answer; retry",
        // Gone from the peer list, or its heartbeat expired: not dialled.
        _ => "the actor's node is not reachable; retry",
    };
    assert_eq!(refusal, expected);
    // b's kept-alive connection to a's cluster port: its next request is
    // refused (a's own refusal, passed on by b), and the connection
    // closed...
    // (An answer withheld because trust lapsed while its request ran
    // means the request was admitted: polling goes on.)
    let (first, kept_alive) = within(
        "b's kept-alive connection",
        since,
        bound,
        move || async move {
            let r = send(b_addr, "GET", "/actor/t/on-a/fired").await;
            let admitted = r.status == 200
                || r.body == "the node's certificate stopped being live during the request";
            (!admitted).then_some(r)
        },
    )
    .await;
    assert_eq!(
        (first.status, first.body.as_str()),
        (503, "not a live node's certificate"),
        "not refused on the kept-alive connection"
    );
    // ...and b's new handshakes with a are refused (b's own 503, as a
    // refused its certificate).
    let r = send(b.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!(
        (r.status, r.body.as_str()),
        (503, "the actor's node did not answer; retry"),
        "a new handshake from b was not refused"
    );
    let handshake = since.elapsed();

    // The WebSocket a relays to b is cut on a's side, the one b relays to
    // a on a's (the owner's) side.
    let ws_bound = bound + CHECK;
    let relay_cut = tokio::time::timeout(ws_bound, a_relays)
        .await
        .expect("the WebSocket a relays to b outlived b's trust")
        .unwrap()
        - since;
    let owner_cut = tokio::time::timeout(ws_bound, b_relays)
        .await
        .expect("the WebSocket b relays to a outlived b's trust")
        .unwrap()
        - since;

    // Calls in flight: the slow callee answers well after trust lapsed;
    // its answer is not delivered, either way.
    let got = a_calls.await.unwrap();
    assert!(
        got.starts_with("failed:"),
        "a's call to b in flight delivered b's answer: {got}"
    );
    let got = b_calls.await.unwrap();
    assert!(
        !got.starts_with("200:"),
        "a answered b's call in flight: {got}"
    );
    // And the next calls fail.
    let r = send(
        a.addr,
        "POST",
        "/actor/t/caller-a/call?to=t/on-b&path=/fired",
    )
    .await;
    assert!(r.body.starts_with("failed:"), "a called b: {}", r.body);
    let r = send(
        b.addr,
        "POST",
        "/actor/t/caller-b/call?to=t/on-a&path=/fired",
    )
    .await;
    assert!(!r.body.starts_with("200:"), "b called a: {}", r.body);

    eprintln!(
        "revocation ({how:?}, horizon {HORIZON:?}): a stops sending {sends:?} ({refusal}); \
         b's kept-alive request refused {kept_alive:?}; new handshake refused by {handshake:?}; \
         relayed WebSocket cut {relay_cut:?} (relay side), {owner_cut:?} (owner side)"
    );
    if let Some(w) = writer {
        w.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_heartbeat_revokes_its_node() {
    a_stops_trusting_b(Revoke::Deleted).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_stops_heartbeating_is_revoked_within_the_horizon() {
    a_stops_trusting_b(Revoke::Silent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_restarted_with_a_new_key_revokes_the_old_key() {
    a_stops_trusting_b(Revoke::NewKey).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_cannot_list_heartbeats_trusts_pins_no_longer_than_the_horizon() {
    a_stops_trusting_b(Revoke::ListingFails).await;
}

/// A WebSocket upgrade on `addr`'s public port, on a connection of its
/// own: the response's status and body (the head only, for a 101).
async fn upgrade(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: t\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let switching =
            raw.starts_with(b"HTTP/1.1 101") && raw.windows(4).any(|w| w == b"\r\n\r\n");
        if switching || complete(&raw) {
            break;
        }
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
        }
    }
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.trim().to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relayed_upgrade_accepted_by_an_owner_no_longer_trusted_is_refused() {
    // `a` relays a client's WebSocket upgrade to `b`, which owns the actor.
    // The actor is busy, so `b` answers late (its admission check runs in
    // the actor's mailbox), with a 101; by then `a` stopped trusting `b`.
    // `a` must not answer the client's upgrade with `b`'s 101: it refuses
    // (checked once before answering; afterwards the relay is cut every
    // 500 ms check).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store).await;
    let b = node("b", db.path(), &store).await;
    let actor = "/actor/t/busy";
    let r = send(b.addr, "GET", &format!("{actor}/fired")).await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    let started = Instant::now();
    while upgrade(a.addr, &format!("{actor}/ws")).await.0 != 101 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a never relayed an upgrade to b"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let busy = |addr: SocketAddr| {
        tokio::spawn(async move { send(addr, "POST", &format!("{actor}/sleep?ms=1500")).await })
    };

    // While a trusts b: the upgrade waits for the busy actor, then gets b's
    // 101, relayed.
    let slow = busy(b.addr);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let asked = Instant::now();
    let (status, _) = upgrade(a.addr, &format!("{actor}/ws")).await;
    let waited = asked.elapsed();
    assert_eq!(status, 101);
    assert!(
        waited > Duration::from_millis(800),
        "b answered before the busy event ended ({waited:?}): the test would not cover a late 101"
    );
    assert_eq!(slow.await.unwrap().status, 200);

    // The same, with a no longer able to list the heartbeats while the
    // upgrade waits: its trust in b lapses at b's horizon. (Deleting b's
    // heartbeat would now also end b's node lease, and with it the busy
    // event, ADR 0020: trust is what this test is about, so b stays well.)
    let slow = busy(b.addr);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let relayed = tokio::spawn({
        let addr = a.addr;
        async move { upgrade(addr, &format!("{actor}/ws")).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    revoke(Revoke::ListingFails, &store, &a, &b).await;
    let (status, body) = relayed.await.unwrap();
    assert_eq!(slow.await.unwrap().status, 200);
    assert!(
        status == 503 && body.contains("no longer trusted"),
        "a answered with the 101 of a node it no longer trusts: {status} {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_refused_at_the_handshake_is_sent_again_once_the_caller_is_live() {
    // Under load, `b`'s heartbeat ages out in `a`'s view for a moment
    // although `b` is fine. `b`'s first `actors.call` to `a` opens a new
    // connection, and `a` refuses `b`'s certificate. Over TLS 1.3 `b`'s
    // side of the handshake is already done when `a` checks it, so the
    // refusal (a fatal alert) only arrives once `b` has written the request:
    // hyper reports `client error (SendRequest)`, and does not retry. `a`
    // never read the request, so `b` may send it again once its heartbeat
    // is live there; it must not fail the call.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store).await;
    let b = node("b", db.path(), &store).await;
    for (n, name, at) in [(&a, "on-a", "a"), (&b, "caller-b", "b")] {
        let r = send(n.addr, "GET", &format!("/actor/t/{name}/fired")).await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some(at)),
            "{name}: {}",
            r.body
        );
    }
    // b reaches a (a forwarded request; b's call pool stays empty).
    let started = Instant::now();
    while send(b.addr, "GET", "/actor/t/on-a/fired")
        .await
        .node
        .as_deref()
        != Some("a")
    {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "b never reached a"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // b's heartbeat stops reaching the bucket until a refuses b.
    b.gate.beats.store(false, Ordering::SeqCst);
    let (b_addr, since) = (b.addr, Instant::now());
    within("a refusing b", since, HORIZON + SLACK, move || async move {
        let r = send(b_addr, "GET", "/actor/t/on-a/fired").await;
        (r.status == 503 && r.body == "the actor's node did not answer; retry").then_some(())
    })
    .await;

    // The call goes out while a refuses b; b's heartbeat lands right after.
    let asked = Instant::now();
    let call = tokio::spawn(async move {
        send(
            b_addr,
            "POST",
            "/actor/t/caller-b/call?to=t/on-a&path=/fired",
        )
        .await
        .body
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    b.gate.beats.store(true, Ordering::SeqCst);
    let body = call.await.unwrap();
    let took = asked.elapsed();
    assert!(body.starts_with("200:"), "b's call to a: {body}");
    // Sent again: after b's heartbeat landed, two periods.
    assert!(
        took >= 2 * PERIOD,
        "answered in {took:?}: the first attempt was not refused, the retry not covered"
    );

    // While b's heartbeat stays out of the bucket, the call fails, and
    // says why. (b's pooled connection to a is refused first, with a's
    // 503; then b handshakes again.)
    b.gate.beats.store(false, Ordering::SeqCst);
    let since = Instant::now();
    let (body, _) = within(
        "b's calls refused",
        since,
        HORIZON + SLACK,
        move || async move {
            let r = send(
                b_addr,
                "POST",
                "/actor/t/caller-b/call?to=t/on-a&path=/fired",
            )
            .await;
            r.body.starts_with("failed:").then_some(r.body)
        },
    )
    .await;
    assert_eq!(
        body,
        "failed:node a refused this node's certificate (this node's heartbeat is not live there)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_refused_at_the_handshake_is_sent_again_once_the_relay_is_live() {
    // As for the call above: `b`'s heartbeat ages out in `a`'s view for a
    // moment, and `b`'s first WebSocket upgrade to `a` opens a new
    // connection on which `a` refuses `b`'s certificate. `a` never read
    // the upgrade, and it has no body, so `b` sends it again once its
    // heartbeat is live there instead of answering the client 503.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store).await;
    let b = node("b", db.path(), &store).await;
    let r = send(a.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    // b reaches a (a forwarded request; b's upgrade pool stays empty, so
    // its first upgrade opens a connection).
    let started = Instant::now();
    while send(b.addr, "GET", "/actor/t/on-a/fired")
        .await
        .node
        .as_deref()
        != Some("a")
    {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "b never reached a"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A refusal usually reaches b as a's alert, or as the upgrade handed
    // back unsent; b then sends the upgrade again. But a drops a refused
    // connection without reading what b wrote on it, and the reset that
    // can follow may reach b in place of the alert: b cannot tell that
    // from a connection lost after a read the upgrade, and answers 503 at
    // once. Both stages below allow for that 503 and try again, within a
    // bound.
    const UNREAD: &str = "the actor's node did not answer; retry";
    let b_addr = b.addr;
    let mut tries = 0;
    let took = loop {
        tries += 1;
        // b's heartbeat stops reaching the bucket until a refuses b.
        b.gate.beats.store(false, Ordering::SeqCst);
        let since = Instant::now();
        within("a refusing b", since, HORIZON + SLACK, move || async move {
            let r = send(b_addr, "GET", "/actor/t/on-a/fired").await;
            (r.status == 503 && r.body == UNREAD).then_some(())
        })
        .await;

        // The upgrade goes out while a refuses b; b's heartbeat lands
        // right after.
        let asked = Instant::now();
        let relayed = tokio::spawn(async move { upgrade(b_addr, "/actor/t/on-a/ws").await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        b.gate.beats.store(true, Ordering::SeqCst);
        let (status, body) = relayed.await.unwrap();
        let took = asked.elapsed();
        if status == 101 {
            break took;
        }
        // Anything but the 503 for a refusal b could not recognise fails
        // at once; so does that 503 every time.
        assert_eq!(
            (status, body.as_str()),
            (503, UNREAD),
            "b's upgrade to a, try {tries}"
        );
        assert!(
            tries < 5,
            "b's upgrade to a was answered 503 {tries} times, in {took:?} the last time: \
             not sent again"
        );
        eprintln!("try {tries}: 503 in {took:?}; trying again");
    };
    eprintln!("101 after {took:?}, try {tries}");
    // Sent again: after b's heartbeat landed, two periods.
    assert!(
        took >= 2 * PERIOD,
        "answered in {took:?}: the first attempt was not refused, the retry not covered"
    );

    // While b's heartbeat stays out of the bucket, the client gets the
    // 503, and only once b waited two periods for a heartbeat that did not
    // land. Until a refuses b again the answer is still 101, and a refusal
    // that reached b as a reset is a 503 at once: so each try is timed,
    // and the stage ends on a 503 that took the two periods. (That b then
    // gives up without sending the upgrade again is not seen from here:
    // a would refuse it too.) Every upgrade opens a connection: one that
    // carried a WebSocket never goes back to b's pool.
    b.gate.beats.store(false, Ordering::SeqCst);
    let since = Instant::now();
    let (waited, at) = within(
        "b's upgrades refused after the wait",
        since,
        HORIZON + SLACK + 2 * PERIOD,
        move || async move {
            let asked = Instant::now();
            let (status, body) = upgrade(b_addr, "/actor/t/on-a/ws").await;
            let waited = asked.elapsed();
            (status == 503 && body == UNREAD && waited >= 2 * PERIOD).then_some(waited)
        },
    )
    .await;
    eprintln!("503 after a wait of {waited:?}, {at:?} after b's heartbeat stopped");
}
