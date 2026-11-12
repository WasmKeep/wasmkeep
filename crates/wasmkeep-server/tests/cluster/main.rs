//! Two nodes sharing one bucket: each actor runs on the node holding its
//! lease, and requests reaching the other node are forwarded there.
//!
//! The suite runs twice: as is (plain HTTP between nodes), and from
//! `cluster_over_tls.rs` with every node behind a cluster listener (mutual
//! TLS, ADR 0011) and a plain address nobody listens on, so every relay
//! and call between nodes must go over TLS.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::apps::AppRef;
use wasmkeep_server::{ActorConfig, ActorService, ClusterConfig, ClusterTlsConfig, LeaseConfig};

#[path = "../support/guests.rs"]
mod guests;

// The tests, by theme; this file keeps what they share.
#[path = "alarms.rs"]
mod alarms;
#[path = "bucket.rs"]
mod bucket;
#[path = "deploys.rs"]
mod deploys;
#[path = "draining.rs"]
mod draining;
#[path = "durable_writes.rs"]
mod durable_writes;
#[path = "failover.rs"]
mod failover;
#[path = "failover_wait.rs"]
mod failover_wait;
#[path = "node_leases.rs"]
mod node_leases;
#[path = "routing.rs"]
mod routing;
#[path = "signed_calls.rs"]
mod signed_calls;
#[path = "snapshots.rs"]
mod snapshots;
#[path = "websockets.rs"]
mod websockets;

fn counter_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/counter", "counter"))
}

fn test_actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

/// Whether this copy of the suite runs between nodes over mutual TLS: this
/// file is compiled a second time as a module of `cluster_over_tls.rs`.
pub(crate) fn over_tls() -> bool {
    module_path!().starts_with("cluster_over_tls")
}

/// The plain address a node publishes for clients at `addr`: over TLS, one
/// where nothing listens (port 9 is discard), so that a peer reaching it
/// in plaintext fails the test.
fn advertised(addr: SocketAddr) -> String {
    if over_tls() {
        "http://127.0.0.1:9".to_owned()
    } else {
        format!("http://{addr}")
    }
}

/// Over TLS: binds a cluster listener and configures `config`'s cluster to
/// publish it.
async fn cluster_listener(config: &mut ActorConfig) -> Option<TcpListener> {
    let cluster = config.cluster.as_mut().filter(|_| over_tls())?;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let advertise = format!("https://{}", listener.local_addr().unwrap());
    cluster.cluster_tls = Some(ClusterTlsConfig::new(advertise));
    Some(listener)
}

/// Serves `service` on `listener`, and on its cluster listener if it has
/// one, until `stop` resolves.
fn spawn_serve(
    listener: TcpListener,
    cluster: Option<TcpListener>,
    service: ActorService,
    stop: impl std::future::Future<Output = ()> + Send + 'static,
) -> tokio::task::JoinHandle<Result<(), wasmkeep_server::Error>> {
    match cluster {
        Some(cluster) => tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
            listener,
            cluster,
            service,
            stop,
            wasmkeep_server::ServeLimits::default(),
        )),
        None => tokio::spawn(wasmkeep_server::serve(listener, service, stop)),
    }
}

struct Node {
    addr: SocketAddr,
    service: ActorService,
    _stop: oneshot::Sender<()>,
}

/// Starts a node `id` on a fresh port, in the cluster sharing `store`.
async fn node(id: &str, data: &Path, store: &Arc<dyn ObjectStore>, idle: Duration) -> Node {
    node_with(id, data, store, idle, "counter", counter_wasm()).await
}

async fn node_with(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    class: &str,
    wasm: &[u8],
) -> Node {
    node_full(id, data, store, idle, class, wasm, Duration::from_secs(30)).await
}

async fn node_full(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    class: &str,
    wasm: &[u8],
    wall: Duration,
) -> Node {
    node_prefixed(id, data, store, idle, class, wasm, wall, "").await
}

/// Like `node_full`, the cluster under `prefix` in the bucket.
#[allow(clippy::too_many_arguments)]
async fn node_prefixed(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    class: &str,
    wasm: &[u8],
    wall: Duration,
    prefix: &str,
) -> Node {
    node_configured(id, data, store, idle, &[class], wasm, wall, prefix, |_| {}).await
}

/// Like `node_prefixed`, with a last change to the configuration.
#[allow(clippy::too_many_arguments)]
async fn node_configured(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    classes: &[&str],
    wasm: &[u8],
    wall: Duration,
    prefix: &str,
    adjust: impl FnOnce(&mut ActorConfig),
) -> Node {
    start_node(
        id, data, store, idle, classes, wasm, wall, prefix, adjust, false,
    )
    .await
    .0
}

/// Like `node_with`, with other nodes reaching it through a TCP proxy that
/// counts the connections they open (over TLS, each one a handshake).
async fn node_counting_peers(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    class: &str,
    wasm: &[u8],
) -> (Node, Arc<AtomicUsize>) {
    let (node, proxy) = node_behind_proxy(id, data, store, class, wasm).await;
    (node, proxy.accepted)
}

/// Like `node_with`, with other nodes reaching it through a [`PeerProxy`].
async fn node_behind_proxy(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    class: &str,
    wasm: &[u8],
) -> (Node, PeerProxy) {
    let (node, proxy) = start_node(
        id,
        data,
        store,
        Duration::from_secs(30),
        &[class],
        wasm,
        Duration::from_secs(30),
        "",
        |_| {},
        true,
    )
    .await;
    (node, proxy.unwrap())
}

/// What a [`PeerProxy`] does with the next bytes a peer sends: the first
/// chunk it reads, after the mode is set, on any of its connections. Two
/// assumptions the tests using a fault mode must meet, since the proxy
/// cannot see where a request starts or ends (over TLS it cannot read it
/// at all):
/// - one chunk: the fault takes effect at that first read, taken to be
///   the start of the request the test sends next. A request that arrives
///   in several reads is still handled whole (see each mode); bytes of an
///   earlier request still on their way would take the fault instead.
/// - one peer: the mode is one value for every connection, whichever node
///   opened it. With a second peer, or anything else a peer sends on its
///   own (a call, an alarm handed over), the fault can land on another
///   request than the one the test meant.
const PROXY_PASS: u8 = 0;
/// Closes the connection at that chunk without passing it on: the node
/// behind never sees the request, nor whatever of it was still to come.
const PROXY_SWALLOW: u8 = 1;
/// Passes that chunk and all the peer sends after it on to the node, so a
/// request split across reads still reaches it whole, and passes nothing
/// back: the peer's side is closed at the first bytes of the answer. The
/// node behind runs the request, and its answer is lost.
const PROXY_CUT_ANSWER: u8 = 2;

/// A TCP proxy in front of a node's port for its peers, which the test
/// controls. It never reads what it carries, so it works over TLS too.
struct PeerProxy {
    /// How many connections it accepted (over TLS, each one a handshake).
    accepted: Arc<AtomicUsize>,
    /// What happens to the next bytes a peer sends: one of the `PROXY_`
    /// constants, back to [`PROXY_PASS`] once it happened.
    next: Arc<AtomicU8>,
    /// Bumped to close every connection open now.
    close: tokio::sync::watch::Sender<u64>,
    /// Where it listens.
    addr: SocketAddr,
    /// Set to stop listening: connections to it are refused from then on.
    refuse: tokio::sync::watch::Sender<bool>,
}

impl PeerProxy {
    /// Closes every connection open now, both sides, as the node behind
    /// does to one it kept idle too long.
    fn close_connections(&self) {
        self.close.send_modify(|n| *n += 1);
    }

    /// Stops listening and closes every connection: from now on a peer's
    /// connection to the node behind is refused, as by a firewall or a
    /// listener that is gone, while the node itself runs on.
    async fn refuse_connections(&self) {
        self.refuse.send_replace(true);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(self.addr).await.is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "the proxy at {} still accepts connections",
                self.addr
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.close_connections();
    }

    /// The next bytes a peer sends are dropped and their connection closed.
    fn swallow_next_request(&self) {
        self.next.store(PROXY_SWALLOW, Ordering::SeqCst);
    }

    /// The next bytes a peer sends reach the node; the peer's side of the
    /// connection is closed before the answer.
    fn cut_next_answer(&self) {
        self.next.store(PROXY_CUT_ANSWER, Ordering::SeqCst);
    }
}

/// Listens on a fresh port and pipes every connection to `target`.
async fn peer_proxy(target: SocketAddr) -> (SocketAddr, PeerProxy) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let next = Arc::new(AtomicU8::new(PROXY_PASS));
    let (close, closed) = tokio::sync::watch::channel(0u64);
    let (refuse, mut refused) = tokio::sync::watch::channel(false);
    let (count, mode) = (Arc::clone(&accepted), Arc::clone(&next));
    tokio::spawn(async move {
        loop {
            let inbound = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((inbound, _)) => inbound,
                    Err(_) => break,
                },
                // Dropping the listener is what refuses connections.
                Ok(()) = refused.changed() => break,
            };
            count.fetch_add(1, Ordering::SeqCst);
            let (mode, mut closed) = (Arc::clone(&mode), closed.clone());
            closed.mark_unchanged();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                tokio::select! {
                    () = proxy_pipe(inbound, outbound, &mode) => {}
                    // A proxy nobody controls any more keeps carrying.
                    Ok(()) = closed.changed() => {}
                }
            });
        }
    });
    (
        addr,
        PeerProxy {
            accepted,
            next,
            close,
            addr,
            refuse,
        },
    )
}

/// Carries bytes both ways until either side closes, or `mode` says to
/// drop the peer's next bytes or the answer to them.
async fn proxy_pipe(mut inbound: TcpStream, mut outbound: TcpStream, mode: &AtomicU8) {
    let (mut from_peer, mut from_node) = (vec![0u8; 16 << 10], vec![0u8; 16 << 10]);
    // Set by `PROXY_CUT_ANSWER`: nothing more goes back to the peer.
    let mut cut_answer = false;
    loop {
        tokio::select! {
            n = inbound.read(&mut from_peer) => {
                let Ok(n @ 1..) = n else { break };
                if !cut_answer {
                    match mode.swap(PROXY_PASS, Ordering::SeqCst) {
                        PROXY_SWALLOW => return,
                        PROXY_CUT_ANSWER => cut_answer = true,
                        _ => {}
                    }
                }
                if outbound.write_all(&from_peer[..n]).await.is_err() {
                    return;
                }
            }
            n = outbound.read(&mut from_node) => {
                let Ok(n @ 1..) = n else { return };
                if cut_answer {
                    break;
                }
                if inbound.write_all(&from_node[..n]).await.is_err() {
                    return;
                }
            }
        }
    }
    if cut_answer {
        // The node behind read its request whole and answers; the peer's
        // side closes, and nobody hears the rest of the answer.
        drop(inbound);
        while matches!(outbound.read(&mut from_node).await, Ok(1..)) {}
    }
}

/// [`PROXY_CUT_ANSWER`] on a request the proxy reads in two chunks: the
/// node behind gets both, and the peer gets none of the answer.
#[tokio::test]
async fn the_proxy_cutting_an_answer_still_passes_a_split_request_whole() {
    let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (addr, proxy) = peer_proxy(node.local_addr().unwrap()).await;
    let node = tokio::spawn(async move {
        let (mut stream, _) = node.accept().await.unwrap();
        let mut request = [0u8; 8];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(b"answer").await.unwrap();
        request
    });
    let mut peer = TcpStream::connect(addr).await.unwrap();
    proxy.cut_next_answer();
    peer.write_all(b"requ").await.unwrap();
    // Long enough for the proxy to read the first half on its own.
    tokio::time::sleep(Duration::from_millis(100)).await;
    peer.write_all(b"est!").await.unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), node)
        .await
        .expect("the second half reaches the node");
    assert_eq!(&request.unwrap(), b"request!");
    let mut answer = Vec::new();
    let _ = peer.read_to_end(&mut answer).await;
    assert_eq!(answer, b"", "the answer was cut");
}

#[allow(clippy::too_many_arguments)]
async fn start_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    classes: &[&str],
    wasm: &[u8],
    wall: Duration,
    prefix: &str,
    adjust: impl FnOnce(&mut ActorConfig),
    count_peers: bool,
) -> (Node, Option<PeerProxy>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = idle;
    config.wall_per_event = wall;
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.prefix = prefix.to_owned();
    // Short timings so takeover is quick; same machine, so no real skew.
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.alarm_sweep = Duration::from_millis(200);
    // Off unless a test asks (`failover_wait.rs`): every other test sees
    // the answer a node gives at once.
    cluster.failover_wait = Some(Duration::ZERO);
    config.cluster = Some(cluster);
    adjust(&mut config);
    let cluster_port = cluster_listener(&mut config).await;
    let accepted = if count_peers {
        let target = cluster_port
            .as_ref()
            .map_or(addr, |l| l.local_addr().unwrap());
        let (proxy, accepted) = peer_proxy(target).await;
        let cluster = config.cluster.as_mut().unwrap();
        match cluster.cluster_tls.as_mut() {
            Some(tls) => tls.advertise = format!("https://{proxy}"),
            None => cluster.advertise = format!("http://{proxy}"),
        }
        Some(accepted)
    } else {
        None
    };
    let classes = classes
        .iter()
        .map(|c| ((*c).to_owned(), component.clone(), wasm.to_vec()))
        .collect();
    let service = ActorService::new(runtime, config, classes).unwrap();

    let (stop, stopped) = oneshot::channel::<()>();
    spawn_serve(listener, cluster_port, service.clone(), async {
        let _ = stopped.await;
    });
    // The start check (ttl plus grace, ADR 0020) and the first node lease
    // write, before the test goes on: a test that then cuts the bucket
    // would otherwise hold the check back. A twin's refusal is the test's
    // to see.
    let _ = service.node_id_checked().await;
    (
        Node {
            addr,
            service,
            _stop: stop,
        },
        accepted,
    )
}

struct Reply {
    status: u16,
    node: Option<String>,
    retry_after: Option<String>,
    body: String,
}

async fn send(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n{extra}Connection: close\r\n\r\n"
    );
    exchange(addr, &req).await
}

/// [`send`], sent again (for up to 5 s) while the reply says the actor's
/// node was not in this node's view. A peer drops out of the view when its
/// heartbeat is older than the horizon (three heartbeats plus the allowed
/// skew: 400 ms in these tests), which a stalled heartbeat writer or lister
/// on a loaded machine can cause. Only the two failures that happen before
/// anything is sent to the owner are retried: a forward refused with
/// "the actor's node is not reachable" (503), and a `/call` whose
/// `Cluster::call` failed with "node X is not reachable", which the test
/// actor answers as `failed:...` (the calling actor ran an event; the
/// callee did not). Never broaden this to other 503s: "did not answer" can
/// follow a request the owner processed.
async fn send_reachable(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = send(addr, method, path, extra).await;
        if !not_in_view(&reply) || tokio::time::Instant::now() >= deadline {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// [`send`], sent again (for up to 10 s) while the node refuses the event
/// before running it: "this node cannot run the actor now; retry" (503).
/// A node whose own lease lapsed takes no actor until its next lease write
/// lands (ADR 0020); a stall past the tests' 1 s ttl causes that, such as
/// a debug build compiling a new deploy on a loaded machine. Every such
/// refusal is sent before the actor's handler runs, so a retried write is
/// not applied twice. Only for a node expected to run the actor: where
/// the refusal is the expected answer, use [`send`].
async fn send_runnable(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let reply = send(addr, method, path, extra).await;
        let refused =
            reply.status == 503 && reply.body.trim() == "this node cannot run the actor now; retry";
        if !refused || tokio::time::Instant::now() >= deadline {
            return reply;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn not_in_view(reply: &Reply) -> bool {
    let body = reply.body.trim();
    (reply.status == 503 && body == "the actor's node is not reachable; retry")
        || (reply.status == 200
            && body.starts_with("failed:node ")
            && body.ends_with(" is not reachable"))
}

/// Sends a raw request (which must close the connection) and reads the reply.
async fn exchange(addr: SocketAddr, req: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let node = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("x-wasmkeep-node")
            .then(|| v.trim().to_owned())
    });
    let retry_after = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("retry-after")
            .then(|| v.trim().to_owned())
    });
    let chunked = head.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding") && value.trim() == "chunked"
        })
    });
    Reply {
        status,
        node,
        retry_after,
        body: if chunked {
            dechunk(body)
        } else {
            body.to_owned()
        },
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            return body.to_owned();
        };
        if n == 0 {
            break;
        }
        out.push_str(&tail[..n]);
        rest = &tail[n + 2..];
    }
    out
}

/// Waits until both nodes have seen each other's heartbeat.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(400)).await;
}

/// Stops `node` and waits until its port refuses connections: dropping
/// it only signals the server, which a loaded machine (the full test
/// suite) can let accept a request or two more.
async fn stop_listening(node: Node) {
    let addr = node.addr;
    drop(node);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(addr).await.is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "{addr} still accepts connections"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Gives the forwarding node's pool time to take back the connection a
/// response just finished on. It goes back from a background task, which a
/// loaded machine (the full test suite) can run late.
async fn pooled() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

async fn durable_write_survives_failover(name: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep_server=debug")
        .with_test_writer()
        .try_init();
    let path = format!("/actor/t/{name}/durable");
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;

    for expected in ["1", "2"] {
        let r = send(a.addr, "POST", &path, "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, expected, Some("a"))
        );
    }
    // a is cut off from the bucket right after acknowledging: it can no
    // longer renew, so after ttl + grace b may take the actor over.
    down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = loop {
        let r = send(b.addr, "GET", &path, "").await;
        if r.node.as_deref() == Some("b") || started.elapsed() > Duration::from_secs(10) {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(r.node.as_deref(), Some("b"), "b took over");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "both acknowledged writes survived"
    );
    eprintln!("failover took {:?}", started.elapsed());
}

/// A bucket that can be cut off, as seen from one node.
#[derive(Debug)]
struct Flaky {
    inner: Arc<dyn ObjectStore>,
    down: Arc<AtomicBool>,
    /// Fail this many segment (`/wal/`) uploads.
    fail_wal: Arc<std::sync::atomic::AtomicU32>,
    /// Delay segment uploads by this many ms (applied before writing).
    delay_wal_ms: Arc<std::sync::atomic::AtomicU64>,
    /// The next this-many segment (`/wal/`) reads never answer: a request
    /// the bucket accepted and then left hanging.
    hang_wal_gets: Arc<std::sync::atomic::AtomicU32>,
    /// Every segment (`/wal/`) read waits this many ms before answering: a
    /// slow bucket, each request answered in time but late.
    delay_wal_get_ms: Arc<std::sync::atomic::AtomicU64>,
    /// The next this-many segment reads answer at once, then send their
    /// body in 20 pieces a second apart: slow, never silent for 5 s.
    trickle_wal_gets: Arc<std::sync::atomic::AtomicU32>,
    /// Segment reads waiting out `delay_wal_get_ms` now, and the most at once.
    wal_gets_now: Arc<std::sync::atomic::AtomicU32>,
    wal_gets_peak: Arc<std::sync::atomic::AtomicU32>,
    /// Segment reads, per key: how many times each was read.
    wal_gets: Arc<std::sync::Mutex<std::collections::HashMap<String, u32>>>,
    /// Every conditional write is applied as a plain overwrite: a store
    /// that accepts `If-None-Match` and `If-Match` and ignores them.
    ignore_conditions: Arc<AtomicBool>,
    /// Alarm-index deletes are acknowledged at once but held back until
    /// `release` turns true: a DELETE that lands late, after a retry or a
    /// slow network. `held` and `applied` count them.
    hold_alarm_deletes: Arc<AtomicBool>,
    held: Arc<std::sync::atomic::AtomicU32>,
    applied: Arc<std::sync::atomic::AtomicU32>,
    release: tokio::sync::watch::Sender<bool>,
    /// Reads of ownership records (`owners/...`).
    lease_gets: Arc<std::sync::atomic::AtomicU32>,
    /// Hold the lease read that brings `lease_gets` to this count (0:
    /// none) for 800 ms; `lease_held` is set while it waits.
    hold_lease_get_at: Arc<std::sync::atomic::AtomicU32>,
    lease_held: Arc<AtomicBool>,
    /// Listings of `control/` fail while set; everything else works.
    fail_control_list: Arc<AtomicBool>,
    /// Every read of a lease object waits this many ms first.
    delay_lease_get_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Writes of ownership records (`owners/...`) fail while set.
    fail_owner_puts: Arc<AtomicBool>,
    /// The next this-many segment (`/wal/`) uploads never answer and are
    /// not applied: a PUT the bucket left hanging.
    hang_wal_puts: Arc<std::sync::atomic::AtomicU32>,
    /// The next this-many `released` writes of a node lease fail.
    fail_released: Arc<std::sync::atomic::AtomicU32>,
    /// Writes of this key fail while set (`nodes/a`: this node cannot
    /// fence a).
    fail_puts_of: Arc<std::sync::Mutex<Option<String>>>,
    /// The next this-many snapshot (`/snapshot/`) uploads never answer and
    /// are not applied.
    hang_snapshot_puts: Arc<std::sync::atomic::AtomicU32>,
    /// The next this-many snapshot (`/snapshot/`) uploads fail at once,
    /// not applied.
    fail_snapshot_puts: Arc<std::sync::atomic::AtomicU32>,
    /// The next this-many snapshot (`/snapshot/`) uploads never answer,
    /// and are applied 8 s later: after their sender gave them up.
    late_snapshot_puts: Arc<std::sync::atomic::AtomicU32>,
}

impl Flaky {
    fn new(inner: Arc<dyn ObjectStore>, down: Arc<AtomicBool>) -> Self {
        Self {
            lease_gets: Arc::default(),
            hold_lease_get_at: Arc::default(),
            lease_held: Arc::default(),
            fail_control_list: Arc::default(),
            delay_lease_get_ms: Arc::default(),
            fail_owner_puts: Arc::default(),
            hang_wal_puts: Arc::default(),
            fail_released: Arc::default(),
            fail_puts_of: Arc::default(),
            hang_snapshot_puts: Arc::default(),
            fail_snapshot_puts: Arc::default(),
            late_snapshot_puts: Arc::default(),
            inner,
            down,
            fail_wal: Arc::default(),
            delay_wal_ms: Arc::default(),
            hang_wal_gets: Arc::default(),
            delay_wal_get_ms: Arc::default(),
            trickle_wal_gets: Arc::default(),
            wal_gets: Arc::default(),
            wal_gets_now: Arc::default(),
            wal_gets_peak: Arc::default(),
            ignore_conditions: Arc::default(),
            hold_alarm_deletes: Arc::default(),
            held: Arc::default(),
            applied: Arc::default(),
            release: tokio::sync::watch::channel(false).0,
        }
    }

    /// Lets the held alarm-index deletes land, and waits until all have.
    async fn release_held_deletes(&self) {
        self.release.send_replace(true);
        let started = std::time::Instant::now();
        while self.applied.load(Ordering::SeqCst) < self.held.load(Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "held deletes never landed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until `n` alarm-index deletes are being held.
    async fn wait_held(&self, n: u32) {
        let started = std::time::Instant::now();
        while self.held.load(Ordering::SeqCst) < n {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "only {} of {n} deletes were held",
                self.held.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl std::fmt::Display for Flaky {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flaky({})", self.inner)
    }
}

impl Flaky {
    fn check(&self) -> object_store::Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: "bucket unreachable".into(),
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for Flaky {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        mut opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.check()?;
        if location.as_ref().starts_with("nodes/")
            && self.fail_released.load(Ordering::SeqCst) > 0
            && String::from_utf8_lossy(&bytes::Bytes::from(payload.clone())).contains("\nreleased ")
        {
            self.fail_released.fetch_sub(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: "release refused".into(),
            });
        }
        if self.fail_puts_of.lock().unwrap().as_deref() == Some(location.as_ref()) {
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: "write refused".into(),
            });
        }
        if location.as_ref().starts_with("owners/") && self.fail_owner_puts.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: "ownership record write failed".into(),
            });
        }
        if self.ignore_conditions.load(Ordering::SeqCst) {
            opts.mode = object_store::PutMode::Overwrite;
        }
        if location.as_ref().contains("/wal/")
            && self
                .hang_wal_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            std::future::pending::<()>().await;
        }
        if location.as_ref().contains("/snapshot/") {
            if self
                .late_snapshot_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                // On a task of its own: the sender drops this call when
                // it gives the upload up.
                let (inner, location) = (Arc::clone(&self.inner), location.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    let _ = inner.put_opts(&location, payload, opts).await;
                });
                std::future::pending::<()>().await;
                unreachable!();
            }
            if self
                .hang_snapshot_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                std::future::pending::<()>().await;
            }
            if self
                .fail_snapshot_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(object_store::Error::Generic {
                    store: "flaky",
                    source: "snapshot upload failed".into(),
                });
            }
        }
        if location.as_ref().contains("/wal/") {
            let delay = self.delay_wal_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            // Tests set this before the upload they target; no races.
            let failing = self.fail_wal.load(Ordering::SeqCst) > 0;
            if failing {
                self.fail_wal.fetch_sub(1, Ordering::SeqCst);
                return Err(object_store::Error::Generic {
                    store: "flaky",
                    source: "upload failed".into(),
                });
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.check()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.check()?;
        if location.as_ref().contains("/wal/")
            && self
                .hang_wal_gets
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            std::future::pending::<()>().await;
        }
        if location.as_ref().contains("/wal/") {
            *self
                .wal_gets
                .lock()
                .unwrap()
                .entry(location.to_string())
                .or_default() += 1;
            let delay = self.delay_wal_get_ms.load(Ordering::SeqCst);
            if delay > 0 {
                let now = self.wal_gets_now.fetch_add(1, Ordering::SeqCst) + 1;
                self.wal_gets_peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                self.wal_gets_now.fetch_sub(1, Ordering::SeqCst);
            }
            if self
                .trickle_wal_gets
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                let got = self.inner.get_opts(location, options).await?;
                let (meta, range, attributes) =
                    (got.meta.clone(), got.range.clone(), got.attributes.clone());
                let extensions = got.extensions.clone();
                let body = got.bytes().await?;
                let piece = body.len().div_ceil(20).max(1);
                let pieces: Vec<bytes::Bytes> = body
                    .chunks(piece)
                    .map(bytes::Bytes::copy_from_slice)
                    .collect();
                let stream =
                    futures_util::stream::unfold(pieces.into_iter(), |mut rest| async move {
                        let next = rest.next()?;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Some((Ok(next), rest))
                    });
                return Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(stream)),
                    meta,
                    range,
                    attributes,
                    extensions,
                });
            }
        }
        if location.as_ref().starts_with("owners/") {
            let delay = self.delay_lease_get_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            let n = self.lease_gets.fetch_add(1, Ordering::SeqCst) + 1;
            if n == self.hold_lease_get_at.load(Ordering::SeqCst) {
                self.lease_held.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(800)).await;
                self.lease_held.store(false, Ordering::SeqCst);
            }
        }
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
        use futures_util::StreamExt as _;
        let (inner, hold, held, applied, release) = (
            Arc::clone(&self.inner),
            Arc::clone(&self.hold_alarm_deletes),
            Arc::clone(&self.held),
            Arc::clone(&self.applied),
            self.release.subscribe(),
        );
        locations
            .then(move |location| {
                let (inner, hold, held, applied, mut release) = (
                    Arc::clone(&inner),
                    Arc::clone(&hold),
                    Arc::clone(&held),
                    Arc::clone(&applied),
                    release.clone(),
                );
                async move {
                    let path = location?;
                    let one = |p: object_store::path::Path| {
                        futures_util::stream::once(async move { Ok(p) }).boxed()
                    };
                    if hold.load(Ordering::SeqCst) && path.as_ref().starts_with("alarms/") {
                        held.fetch_add(1, Ordering::SeqCst);
                        let late = path.clone();
                        tokio::spawn(async move {
                            let _ = release.wait_for(|go| *go).await;
                            let _ = inner.delete_stream(one(late)).next().await;
                            applied.fetch_add(1, Ordering::SeqCst);
                        });
                        return Ok(path);
                    }
                    match inner.delete_stream(one(path.clone())).next().await {
                        Some(r) => r,
                        None => Ok(path),
                    }
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let control = prefix.is_some_and(|p| p.as_ref().starts_with("control"));
        if control && self.fail_control_list.load(Ordering::SeqCst) {
            return futures_util::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "flaky",
                    source: "listing failed".into(),
                })
            })
            .boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.check()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.check()?;
        self.inner.copy_opts(from, to, options).await
    }
}

/// The drain state of a heartbeat: line 9, or line 10 in format 3 (the
/// node lease's three lines, then the heartbeat's fields, ADR 0020).
fn drain_line(body: &str) -> Option<&str> {
    let line = if body.starts_with("wasmkeep-node 3\n") {
        9
    } else {
        8
    };
    body.lines().nth(line)
}

/// The drain line of `id`'s heartbeat in `store`, once it reads `want`
/// (within 5 s).
async fn wait_drain_line(store: &Arc<dyn ObjectStore>, id: &str, want: &str) {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(format!("nodes/{id}"));
    let started = std::time::Instant::now();
    loop {
        // Not written yet counts as not yet.
        let body = match store.get(&key).await {
            Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
            Err(object_store::Error::NotFound { .. }) => String::new(),
            Err(e) => panic!("{e}"),
        };
        if drain_line(&body) == Some(want) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the drain line never read {want:?}: {body:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The node holding actor `t/<name>`, if one does: its ownership record
/// (`wasmkeep-owner 1`, then `<node> <instance>`, or `- -` once released,
/// then the generation; ADR 0020).
async fn lease_holder(store: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    let (holder, _) = owner_record(store, name).await?;
    holder
}

/// Actor `t/<name>`'s ownership record: the node it names (none once
/// released) and its generation; `None` if there is none.
async fn owner_record(store: &Arc<dyn ObjectStore>, name: &str) -> Option<(Option<String>, u64)> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(format!("owners/default/t/{name}"));
    let body = match store.get(&key).await {
        Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
        Err(object_store::Error::NotFound { .. }) => return None,
        Err(e) => panic!("{e}"),
    };
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines[0], "wasmkeep-owner 1", "{body}");
    let node = lines[1].split(' ').next().unwrap();
    let generation = lines[2].parse().unwrap();
    Some(((node != "-").then(|| node.to_owned()), generation))
}

/// Asks `id` to drain and waits until its heartbeat says so (`draining`,
/// or already `drained` or `stuck`).
async fn drain(store: &Arc<dyn ObjectStore>, id: &str) {
    use object_store::ObjectStoreExt as _;
    let marker = object_store::path::Path::from(format!("control/drain/{id}"));
    store.put(&marker, Vec::new().into()).await.unwrap();
    let key = object_store::path::Path::from(format!("nodes/{id}"));
    let started = std::time::Instant::now();
    loop {
        let body = match store.get(&key).await {
            Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
            Err(object_store::Error::NotFound { .. }) => String::new(),
            Err(e) => panic!("{e}"),
        };
        if matches!(drain_line(&body), Some("draining" | "drained" | "stuck")) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{id} never drained: {body:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Writes a move request for actor `t/<name>` to `target` (ADR 0017).
async fn request_move(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    target: &str,
) -> object_store::path::Path {
    use object_store::ObjectStoreExt as _;
    let key = wasmkeep_cluster::control::checked_move_key("default", "t", name, target).unwrap();
    let path = object_store::path::Path::from(key);
    store.put(&path, Vec::new().into()).await.unwrap();
    path
}

/// Waits (10 s at most) until `cond` holds.
async fn eventually(what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let started = std::time::Instant::now();
    while !cond().await {
        assert!(started.elapsed() < Duration::from_secs(10), "never: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Two nodes; a reaches the bucket through a `Flaky` the test controls.
struct Pair {
    a: Node,
    b: Node,
    flaky: Arc<Flaky>,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn pair(wall: Duration, idle: Duration) -> Pair {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node_full(
        "a",
        dirs.0.path(),
        &seen_by_a,
        idle,
        "t",
        test_actor_wasm(),
        wall,
    )
    .await;
    let b = node_full(
        "b",
        dirs.1.path(),
        &shared,
        idle,
        "t",
        test_actor_wasm(),
        wall,
    )
    .await;
    settle().await;
    Pair {
        a,
        b,
        flaky,
        _dirs: dirs,
    }
}

/// a hands the actor over (hibernating, or cut off from the bucket so its
/// lease runs out); b must restore it.
async fn b_restores(p: &Pair) -> Reply {
    tokio::time::sleep(Duration::from_millis(600)).await;
    p.flaky.down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    loop {
        let r = send(p.b.addr, "GET", "/actor/t/x/durable", "").await;
        if r.node.as_deref() == Some("b") || started.elapsed() > Duration::from_secs(10) {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The status a WebSocket upgrade was answered with (101 if accepted).
async fn upgrade_status(addr: SocketAddr, path: &str) -> u16 {
    match tokio_tungstenite::connect_async(format!("ws://{addr}{path}")).await {
        Ok(_) => 101,
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => r.status().as_u16(),
        Err(e) => panic!("{path}: {e}"),
    }
}

/// [`upgrade_status`], tried again (for up to 5 s) while it is 503: on a
/// node that does not own the actor, an upgrade is forwarded, and refused
/// with 503 while the owner is out of this node's view (see
/// [`send_reachable`]). For tests that expect another status.
async fn upgrade_status_reachable(addr: SocketAddr, path: &str) -> u16 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = upgrade_status(addr, path).await;
        if status != 503 || tokio::time::Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn bucket_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    use futures_util::TryStreamExt;
    store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap()
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Two nodes running the test actor; `x` lives on a, `y` on b.
async fn x_on_a_y_on_b() -> Pair {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.node.as_deref(), Some("a"));
    let r = send(p.b.addr, "POST", "/actor/t/y/durable", "").await;
    assert_eq!(r.node.as_deref(), Some("b"));
    p
}

fn chat_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/chat-room", "chat_room"))
}

/// A node serving whatever is deployed for app `default/shop`, checking for
/// new deploys every `follow` (or when an actor needs a newer one).
async fn node_app(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    follow: Duration,
) -> (Node, ActorService) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let app = AppRef::new(Arc::clone(store), "", "default", "shop");
    let current = app.current().await.unwrap().expect("something is deployed");
    // As `serve --app` starts: the version's classes, rules and values.
    let version = app.version(&current.version).await.unwrap();
    let classes = version
        .classes
        .into_iter()
        .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
        .collect();
    let mut config = ActorConfig::new(data);
    config.access = version.access;
    config.config = version.config;
    config.mailbox.idle_timeout = idle;
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(runtime, config, classes).unwrap();
    service.set_deploy_seq(current.seq);
    tokio::spawn(app.follow(service.clone(), follow));
    let (stop, stopped) = oneshot::channel::<()>();
    spawn_serve(listener, cluster_port, service.clone(), async {
        let _ = stopped.await;
    });
    // The start check (ttl plus grace, ADR 0020) and the first node lease
    // write, before the test goes on: a test that then cuts the bucket
    // would otherwise hold the check back. A twin's refusal is the test's
    // to see.
    let _ = service.node_id_checked().await;
    (
        Node {
            addr,
            service: service.clone(),
            _stop: stop,
        },
        service,
    )
}

fn deploy_runtime() -> Runtime {
    Runtime::new(&RuntimeConfig::default()).unwrap()
}

async fn never_runs_older_code(fault_first: bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    app.deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_millis(200);
    // a follows deploys quickly; b would take a minute.
    let (a, a_service) = node_app("a", da.path(), &store, idle, Duration::from_millis(100)).await;
    let (b, b_service) = node_app("b", db.path(), &store, idle, Duration::from_secs(60)).await;
    settle().await;

    app.deploy(&rt, vec![("c".into(), test_actor_wasm().to_vec())])
        .await
        .unwrap();
    runs_deploy(&a_service, 2).await;
    // Through `send_runnable` until x hibernates: the compile just before
    // can stall the process past a's lease, which a refuses for a moment.
    if fault_first {
        wasmkeep_storage::faults::fail_next("set_meta:deploy_seq");
        let r = send_runnable(a.addr, "GET", "/actor/c/x/durable", "").await;
        assert_eq!(
            r.status, 500,
            "the injected fault fails the event: {}",
            r.body
        );
    }
    // Only v2 (the test actor) answers `GET /durable` with 0 at first.
    let r = send_runnable(a.addr, "GET", "/actor/c/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "0"), "a runs v2");
    let r = send_runnable(a.addr, "POST", "/actor/c/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("a"))
    );

    // x hibernates on a; b, still on v1, takes it over. It must not run v1
    // on data v2 already ran on: it refuses, and updates at once. Wait for
    // the hibernation itself rather than a fixed time (slow under load),
    // then for the lease release that follows it.
    let started = std::time::Instant::now();
    while a_service.resident() > 0 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "x never hibernated on a"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(b_service.deploy_seq(), 1);
    let r = send(b.addr, "GET", "/actor/c/x/durable", "").await;
    // Refused for now (503, retry), not an actor error (review finding D4).
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "refused rather than running older code: {}",
        r.body
    );

    // b catches up when woken by the refusal, well before its one-minute
    // poll; compiling the new version takes a while in a debug build under
    // load, so allow up to 30 s.
    let started = std::time::Instant::now();
    let r = loop {
        let r = send(b.addr, "GET", "/actor/c/x/durable", "").await;
        if r.status == 200 || started.elapsed() > Duration::from_secs(30) {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("b"))
    );
    assert_eq!(
        b_service.deploy_seq(),
        2,
        "b caught up without waiting a minute"
    );
    // A route only v2 has.
    let r = send(b.addr, "POST", "/actor/c/x/durable-twice", "").await;
    assert_eq!(r.body.trim(), "first=ok second=ok", "b runs v2");
}

/// Waits (up to 30 s: compiling is slow in debug builds under load) until
/// the node runs deploy `seq`.
async fn runs_deploy(service: &ActorService, seq: u64) {
    let started = std::time::Instant::now();
    while service.deploy_seq() < seq {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the node never followed deploy {seq}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A node with production-like 10 s leases, whose `serve` task can be
/// awaited after stopping it.
async fn long_lease_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), wasmkeep_server::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(counter_wasm()).unwrap();
    let mut config = ActorConfig::new(data);
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(10_000, 1_000, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(
        runtime,
        config,
        vec![("counter".into(), component, counter_wasm().to_vec())],
    )
    .unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let served = spawn_serve(listener, cluster_port, service, async {
        let _ = stopped.await;
    });
    (addr, stop, served)
}

async fn orphaned_alarm_fires(stuck: usize) {
    // a owns "Room 1" (a name the bucket keys escape) with an alarm pending;
    // then a is cut off from the bucket for good. b has never seen the
    // actor, yet the alarm must fire there.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        use object_store::ObjectStoreExt as _;
        for i in 0..stuck {
            let key = format!("alarms/default/{:015}/ghost/{:04x}", 1_000_000 + i, i);
            shared
                .put(
                    &object_store::path::Path::from(key),
                    object_store::PutPayload::new(),
                )
                .await
                .unwrap();
        }
    }
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;

    let r = send(a.addr, "POST", "/actor/t/Room%201/alarm?in=1500", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    // The event that set the alarm replicates (and indexes it) right after
    // its response, like any plain write: let that finish, then cut a off.
    tokio::time::sleep(Duration::from_millis(300)).await;
    down.store(true, Ordering::SeqCst);

    // Due in 1.5 s; overdue once a's lease could have run out (ttl 1 s +
    // grace 200 ms); then the sweep delivers it. Then one request: its
    // event reads the count before anything it does could arm the alarm,
    // so "1" means the alarm had already fired.
    wait_delivered(&shared, &["Room 1"]).await;
    let r = send(b.addr, "GET", "/actor/t/Room%201/fired", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "b took the actor over");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the alarm fired on b"
    );
}

/// The bucket's alarm index once `done` holds for it (index updates follow
/// the response, like replication), or as it is after 2 s.
async fn alarm_index(
    store: &Arc<dyn ObjectStore>,
    done: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    let started = std::time::Instant::now();
    loop {
        let listed: Vec<_> = store
            .list(Some(&object_store::path::Path::from("alarms")))
            .try_collect()
            .await
            .unwrap();
        let keys: Vec<String> = listed.into_iter().map(|m| m.location.to_string()).collect();
        if done(&keys) || started.elapsed() > Duration::from_secs(2) {
            return keys;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The generation in `t/x`'s ownership record.
async fn lease_generation_of_x(store: &Arc<dyn ObjectStore>) -> u64 {
    owner_record(store, "x").await.unwrap().1
}

/// Actor names every path that encodes or decodes a name must survive:
/// escaped in file names and bucket keys, percent-encoded in URLs and
/// headers, hashed in file names when long.
fn awkward_names() -> Vec<String> {
    let mut names: Vec<String> = [
        "Alice", "room 1", "50%", "a~b", "a+b", "é漢字", ".dot", "a?b", "a#b", "%41",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    names.push("L".repeat(256)); // the longest name; hashed in file names
    names
}

/// Percent-encodes a name for a URL path segment or query value.
fn url_encode(name: &str) -> String {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn percent_decode_str(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && let Some(v) = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// HMAC-SHA256, hex, for signing a call the way a node does.
fn hmac_hex(key: &[u8], msg: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    block[..key.len()].copy_from_slice(key);
    let pad = |b: u8| block.iter().map(|k| k ^ b).collect::<Vec<u8>>();
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(msg)
        .finalize();
    let outer = Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize();
    outer.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Status line and body of a plain GET, whatever the framing.
async fn get_raw(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    (
        status,
        if chunked {
            dechunk(body)
        } else {
            body.to_owned()
        },
    )
}

/// Polls `path` until it answers `status`, for up to 10 s.
async fn wait_for(addr: SocketAddr, path: &str, status: u16) -> String {
    for _ in 0..200 {
        let (s, body) = get_raw(addr, path).await;
        if s == status {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{path} never answered {status}");
}

/// The index entries for actor `t/x` (`78` in hex).
async fn index_entries_for_x(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    index_entries_for(store, "x").await
}

/// The index entries for actor `t/<name>`: legacy (`…/t/<hex>`) and tagged
/// (`…/t.t/<hex>/<tag>`).
async fn index_entries_for(store: &Arc<dyn ObjectStore>, name: &str) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    let hex: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    let listed: Vec<_> = store
        .list(Some(&object_store::path::Path::from("alarms")))
        .try_collect()
        .await
        .unwrap();
    listed
        .into_iter()
        .map(|m| m.location.to_string())
        .filter(|k| {
            let parts: Vec<&str> = k.split('/').collect();
            matches!(parts.get(3), Some(&"t" | &"t.t")) && parts.get(4) == Some(&hex.as_str())
        })
        .collect()
}

/// Whether the index has an entry for `t/x` due at `at`.
async fn indexed_at(store: &Arc<dyn ObjectStore>, at: u128) -> bool {
    let due = format!("{at:015}");
    index_entries_for_x(store)
        .await
        .iter()
        .any(|k| k.split('/').nth(2) == Some(due.as_str()))
}

async fn wait_indexed_at(store: &Arc<dyn ObjectStore>, at: u128) {
    let started = std::time::Instant::now();
    while !indexed_at(store, at).await {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never indexed at {at}: {:?}",
            index_entries_for_x(store).await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Three nodes sharing one bucket: a and b each reach it through their own
/// `Flaky`, c directly.
struct Trio {
    a: Node,
    b: Node,
    c: Node,
    fa: Arc<Flaky>,
    fb: Arc<Flaky>,
    shared: Arc<dyn ObjectStore>,
    _dirs: [tempfile::TempDir; 3],
}

async fn trio() -> Trio {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = || {
        Arc::new(Flaky::new(
            Arc::clone(&shared),
            Arc::new(AtomicBool::new(false)),
        ))
    };
    let (fa, fb) = (flaky(), flaky());
    let dirs = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let start = |id: &'static str, store: Arc<dyn ObjectStore>, dir: &Path| {
        let dir = dir.to_owned();
        async move { node_with(id, &dir, &store, idle, "t", test_actor_wasm()).await }
    };
    let a = start("a", Arc::clone(&fa) as Arc<dyn ObjectStore>, dirs[0].path()).await;
    let b = start("b", Arc::clone(&fb) as Arc<dyn ObjectStore>, dirs[1].path()).await;
    let c = start("c", Arc::clone(&shared), dirs[2].path()).await;
    settle().await;
    Trio {
        a,
        b,
        c,
        fa,
        fb,
        shared,
        _dirs: dirs,
    }
}

/// Polls `node` until it serves `t/x` itself (the previous owner was cut
/// off, so its lease runs out).
async fn wait_owns_x(node: &Node, id: &str) {
    let started = std::time::Instant::now();
    loop {
        let r = send(node.addr, "GET", "/actor/t/x/durable", "").await;
        if r.node.as_deref() == Some(id) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{id} never took x over (last: {} {:?})",
            r.status,
            r.node
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The late delete: a's delete of its own index entry lands late, after b took the
/// actor over and indexed an alarm for the same due time. `clear` or move:
/// how a leaves the alarm behind before handing over. b's entry must
/// survive, and once b is gone too, c (which never ran x) delivers it.
async fn late_delete_after_takeover(clear: bool) {
    let n = trio().await;
    let t = epoch_ms() + 8_000;
    let r = send(n.a.addr, "POST", &format!("/actor/t/x/alarm?at={t}"), "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    wait_indexed_at(&n.shared, t).await;

    // a's next change retires the entry at t: that DELETE is held.
    n.fa.hold_alarm_deletes.store(true, Ordering::SeqCst);
    let r = if clear {
        send(n.a.addr, "DELETE", "/actor/t/x/alarm", "").await
    } else {
        let u = epoch_ms() + 60_000;
        send(n.a.addr, "POST", &format!("/actor/t/x/alarm?at={u}"), "").await
    };
    assert_eq!(r.status, 200);
    n.fa.wait_held(1).await;

    // a is cut off; b takes x over and sets an alarm for the same time.
    n.fa.down.store(true, Ordering::SeqCst);
    wait_owns_x(&n.b, "b").await;
    let r = send(n.b.addr, "POST", &format!("/actor/t/x/alarm?at={t}"), "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("b")));
    // Index updates follow the response; the actor's next event runs once
    // they are done (a's entry at t is still there, its delete held, so
    // the listing alone would not tell).
    let r = send(n.b.addr, "GET", "/actor/t/x/alarm", "").await;
    assert_eq!(
        (r.node.as_deref(), r.body.trim()),
        (Some("b"), t.to_string().as_str())
    );
    wait_indexed_at(&n.shared, t).await;

    // a's old DELETE lands now; b's entry survives it.
    n.fa.release_held_deletes().await;
    assert_eq!(n.fa.applied.load(Ordering::SeqCst), 1, "the delete landed");
    assert!(
        indexed_at(&n.shared, t).await,
        "clear={clear}: the late delete removed b's entry: {:?}",
        index_entries_for_x(&n.shared).await
    );

    // b goes too, before t. c finds the alarm in the index and delivers
    // it. One read, after the fact: any event on c re-arms the alarm in
    // its timer, which would hide a missing entry.
    n.fb.down.store(true, Ordering::SeqCst);
    wait_delivered(&n.shared, &["x"]).await;
    let r = send(n.c.addr, "GET", "/actor/t/x/fired", "").await;
    assert_eq!(r.node.as_deref(), Some("c"), "c took x over");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "clear={clear}: the alarm fired on c"
    );
}

/// Waits, up to 15 s, until none of `names` (actors of class `t`) has an
/// index entry left: whichever node delivered their alarms has cleared
/// them. Reads only the bucket, so it cannot arm an alarm itself; the
/// caller then checks what ran (and fails there if nothing did).
async fn wait_delivered(store: &Arc<dyn ObjectStore>, names: &[&str]) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        let mut left = false;
        for name in names {
            left |= !index_entries_for(store, name).await.is_empty();
        }
        if !left {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until `t/<name>` has no index entry left, for at most `within`.
async fn wait_unindexed(store: &Arc<dyn ObjectStore>, name: &str, within: Duration) {
    let started = std::time::Instant::now();
    loop {
        let left = index_entries_for(store, name).await;
        if left.is_empty() {
            return;
        }
        assert!(started.elapsed() < within, "left in the index: {left:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The keys under `t/<name>`'s state in the bucket.
async fn actor_keys(store: &Arc<dyn ObjectStore>, name: &str) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    let mut keys: Vec<String> = store
        .list(Some(&object_store::path::Path::from(format!(
            "actors/default/t/{name}"
        ))))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    keys.sort();
    keys
}

/// The snapshot and segment positions `(txid, kind)` in `t/<name>`'s prefix.
async fn history_of(store: &Arc<dyn ObjectStore>, name: &str) -> (Vec<u64>, Vec<u64>) {
    let (mut snapshots, mut segments) = (Vec::new(), Vec::new());
    for key in actor_keys(store, name).await {
        let file = key.rsplit('/').next().unwrap();
        let txid = u64::from_str_radix(&file[..16], 16).unwrap();
        if key.contains("/snapshot/") {
            snapshots.push(txid);
        } else if key.contains("/wal/") {
            segments.push(txid);
        }
    }
    (snapshots, segments)
}

/// A node of class `t` whose history retention is `retention` (ADR 0012).
async fn node_retaining(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    retention: Option<Duration>,
) -> Node {
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
            if let (Some(r), Some(c)) = (retention, config.cluster.as_mut()) {
                c.history_retention = r;
            }
        },
    )
    .await
}

/// The highest generation `t/<name>`'s keys in `store` carry.
async fn highest_key_generation(store: &Arc<dyn ObjectStore>, name: &str) -> u64 {
    use futures_util::TryStreamExt;
    let prefix = object_store::path::Path::from(format!("actors/default/t/{name}"));
    let metas: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    let keys: Vec<String> = metas
        .iter()
        .filter_map(|m| {
            m.location
                .as_ref()
                .strip_prefix(&format!("{}/", prefix.as_ref()))
                .map(str::to_owned)
        })
        .collect();
    assert!(!keys.is_empty(), "{name} has no keys");
    wasmkeep_cluster::replication::Listing::from_keys(keys.iter().map(String::as_str))
        .highest_generation()
}

/// This machine's clock, for the lease library.
struct WallClock;

impl wasmkeep_cluster::lease::Clock for WallClock {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}
