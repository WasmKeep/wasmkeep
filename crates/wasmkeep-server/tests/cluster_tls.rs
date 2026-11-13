//! Mutual TLS between nodes (ADR 0011, accept mode): nodes with a cluster
//! listener reach each other over TLS with the certificates pinned in their
//! heartbeats, plain HTTP stays for nodes without one, and the cluster port
//! trusts only live pins and never forwards what it receives. In refuse
//! mode (phase B), only the cluster port carries node calls and peers
//! without a cluster listener are not used.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{
    ActorConfig, ActorService, ClusterConfig, ClusterPlaintext, ClusterTlsConfig, LeaseConfig,
};

#[path = "support/guests.rs"]
mod guests;

fn test_actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

/// How a test node talks to its peers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// No cluster listener: plain HTTP only, as before ADR 0011.
    Plain,
    /// A cluster listener; the plain address it publishes is one where
    /// nothing listens, so peers reaching it in plaintext fail.
    TlsOnly,
    /// A cluster listener, and a working plain address (for peers without
    /// TLS).
    Both,
    /// A cluster listener served on one port, but another published: the
    /// node's public (plain HTTP) port, where a TLS handshake fails.
    WrongTlsAddress,
}

struct Node {
    addr: SocketAddr,
    /// The cluster listener, if any.
    cluster: Option<SocketAddr>,
    _service: ActorService,
    _stop: oneshot::Sender<()>,
}

/// A node serving class `t` (open) and class `u` (`internal`: only node
/// calls and local calls reach it).
async fn node(id: &str, data: &Path, store: &Arc<dyn ObjectStore>, mode: Mode) -> Node {
    node_with(id, data, store, mode, ClusterPlaintext::Accept).await
}

async fn node_with(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    mode: Mode,
    plaintext: ClusterPlaintext,
) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(test_actor_wasm()).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_secs(30);
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("u", wasmkeep_server::Access::Internal);
    config.access = rules;
    let plain = if mode == Mode::TlsOnly {
        "http://127.0.0.1:9".to_owned()
    } else {
        format!("http://{addr}")
    };
    let mut cluster = ClusterConfig::new(id, plain, Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.alarm_sweep = Duration::from_millis(200);
    // These tests are about what a node answers at once.
    cluster.failover_wait = Some(Duration::ZERO);
    cluster.cluster_plaintext = plaintext;
    let cluster_listener = if mode == Mode::Plain {
        None
    } else {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let published = match mode {
            Mode::WrongTlsAddress => addr,
            _ => l.local_addr().unwrap(),
        };
        cluster.cluster_tls = Some(ClusterTlsConfig::new(format!("https://{published}")));
        Some(l)
    };
    config.cluster = Some(cluster);
    let service = ActorService::new(
        runtime,
        config,
        vec![
            ("t".into(), component.clone(), test_actor_wasm().to_vec()),
            ("u".into(), component, test_actor_wasm().to_vec()),
        ],
    )
    .unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = async {
        let _ = stopped.await;
    };
    let cluster_addr = cluster_listener.as_ref().map(|l| l.local_addr().unwrap());
    match cluster_listener {
        Some(cl) => tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
            listener,
            cl,
            service.clone(),
            stopped,
            wasmkeep_server::ServeLimits::default(),
        )),
        None => tokio::spawn(wasmkeep_server::serve(listener, service.clone(), stopped)),
    };
    // The start check (ttl plus grace, ADR 0020) and the first heartbeat.
    let _ = service.node_id_checked().await;
    Node {
        addr,
        cluster: cluster_addr,
        _service: service,
        _stop: stop,
    }
}

/// Waits until the nodes have seen each other's heartbeats (and pins).
async fn settle() {
    tokio::time::sleep(Duration::from_millis(400)).await;
}

struct Reply {
    status: u16,
    node: Option<String>,
    body: String,
}

fn parse(raw: &str) -> Reply {
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw, ""));
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
    Reply {
        status,
        node,
        body: body.to_owned(),
    }
}

/// A request to a node's public port.
async fn send(addr: SocketAddr, method: &str, path: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    parse(&raw)
}

/// Makes `name` (class `t`) resident on the node at `owner`.
async fn place(owner: SocketAddr, name: &str) {
    let r = send(owner, "GET", &format!("/actor/t/{name}/fired")).await;
    assert_eq!(r.status, 200, "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_calls_and_websockets_cross_nodes_over_tls() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // Neither node can be reached in plaintext: only TLS gets through.
    let a = node("a", da.path(), &store, Mode::TlsOnly).await;
    let b = node("b", db.path(), &store, Mode::TlsOnly).await;
    settle().await;
    place(b.addr, "on-b").await;
    place(a.addr, "on-a").await;

    // A request relayed a -> b, and one b -> a.
    let r = send(a.addr, "GET", "/actor/t/on-b/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    let r = send(b.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );

    // actors.call from an actor on a to one on b: signed, over TLS.
    let r = send(a.addr, "POST", "/actor/t/on-a/call?to=t/on-b&path=/caller").await;
    assert_eq!(
        (r.status, dechunk(&r.body).trim()),
        (200, "200:default/t/on-a")
    );

    // A WebSocket relayed a -> b.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/on-b", a.addr))
        .await
        .unwrap();
    ws.send(Message::text("over tls")).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(echo.unwrap().unwrap(), Message::text("over tls"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_cluster_works_both_ways() {
    // Phase A: a has a cluster listener, c does not. c cannot present a
    // certificate, so it reaches a in plaintext; a reaches c in plaintext
    // since c publishes no TLS address.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, dc) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Mode::Both).await;
    let c = node("c", dc.path(), &store, Mode::Plain).await;
    settle().await;
    place(a.addr, "on-a").await;
    place(c.addr, "on-c").await;
    let r = send(c.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send(a.addr, "GET", "/actor/t/on-c/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("c")),
        "{}",
        r.body
    );
    let r = send(a.addr, "POST", "/actor/t/on-a/call?to=t/on-c&path=/caller").await;
    assert_eq!(
        (r.status, dechunk(&r.body).trim()),
        (200, "200:default/t/on-a")
    );
    let r = send(c.addr, "POST", "/actor/t/on-c/call?to=t/on-a&path=/caller").await;
    assert_eq!(
        (r.status, dechunk(&r.body).trim()),
        (200, "200:default/t/on-c")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_plaintext_after_a_tls_failure() {
    // b publishes a TLS address where no TLS is spoken (its public port),
    // and a working plain address. a must not fall back to it: 503.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Mode::Both).await;
    let b = node("b", db.path(), &store, Mode::WrongTlsAddress).await;
    settle().await;
    place(b.addr, "on-b").await;
    let r = send(a.addr, "GET", "/actor/t/on-b/fired").await;
    assert_eq!(r.status, 503, "{}", r.body);
    let r = send(a.addr, "POST", "/actor/t/x/call?to=t/on-b&path=/caller").await;
    assert!(dechunk(&r.body).starts_with("failed:"), "{}", r.body);
    // b itself serves the actor: only the hop was refused.
    assert_eq!(send(b.addr, "GET", "/actor/t/on-b/fired").await.status, 200);
}

/// A node identity made by the test, as another node of the cluster would
/// have one.
struct Stranger {
    cert: rustls::pki_types::CertificateDer<'static>,
    key: Vec<u8>,
    pin: String,
}

fn stranger() -> Stranger {
    use sha2::{Digest, Sha256};
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = rcgen::CertificateParams::new(vec!["x.test".to_owned()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let cert = cert.der().clone();
    let pin = Sha256::digest(&cert)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Stranger {
        cert,
        key: key.serialize_der(),
        pin,
    }
}

/// Publishes a heartbeat for node `x` with `pin` every 50 ms while `alive`.
fn heartbeats(store: &Arc<dyn ObjectStore>, pin: String, alive: Arc<AtomicBool>) {
    let store = Arc::clone(store);
    tokio::spawn(async move {
        while alive.load(Ordering::SeqCst) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let body = format!(
                "wasmkeep-node 2\nx\nhttp://127.0.0.1:9\n0.0.0\n{now}\ndefault/\nhttps://127.0.0.1:9\n{pin}\n"
            );
            store
                .put(&StorePath::from("nodes/x"), body.into_bytes().into())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        store.delete(&StorePath::from("nodes/x")).await.unwrap();
    });
}

/// Accepts any server certificate: these tests check the server's side.
#[derive(Debug)]
struct AnyServer;

impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A TLS connection to a cluster port, presenting `who`'s certificate.
async fn connect_as(
    addr: SocketAddr,
    who: &Stranger,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyServer))
        .with_client_auth_cert(
            vec![who.cert.clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(who.key.clone().into()),
        )
        .unwrap();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
    tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await
        .map_err(|e| e.to_string())
}

/// GET `path` on a cluster port as `who`: the raw response, or why the
/// connection failed.
async fn get_as(addr: SocketAddr, who: &Stranger, path: &str) -> Result<String, String> {
    let mut tls = connect_as(addr, who).await?;
    // As a node of the app relays a request (ADR 0022).
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: n\r\nx-wasmkeep-app: default/default\r\n\
         Connection: close\r\n\r\n"
    );
    tls.write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    tls.read_to_end(&mut raw).await.map_err(|e| e.to_string())?;
    let raw = String::from_utf8_lossy(&raw).into_owned();
    if raw.starts_with("HTTP/") {
        Ok(raw)
    } else {
        Err(format!("no response: {raw:?}"))
    }
}

/// A WebSocket upgrade of `url` as a node of the app relays one: naming
/// the app (ADR 0022).
fn relayed_upgrade(url: &str) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        "x-wasmkeep-app",
        hyper::header::HeaderValue::from_static("default/default"),
    );
    req
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cluster_port_trusts_live_pins_only_and_never_forwards() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node("a", da.path(), &store, Mode::TlsOnly).await;
    let b = node("b", db.path(), &store, Mode::TlsOnly).await;
    let port = b.cluster.unwrap();

    // Not in any heartbeat: refused at the handshake.
    let x = stranger();
    assert!(get_as(port, &x, "/_wasmkeep/health").await.is_err());

    // Once x's heartbeat lists its pin, b lets it in.
    let alive = Arc::new(AtomicBool::new(true));
    heartbeats(&store, x.pin.clone(), Arc::clone(&alive));
    settle().await;
    let r = parse(&get_as(port, &x, "/_wasmkeep/health").await.unwrap());
    assert_eq!(r.status, 200);

    // An actor b owns is served; one a owns is not forwarded again from
    // the cluster port (no forwarded header needed): 503, the caller
    // retries.
    place(b.addr, "on-b").await;
    place(a.addr, "on-a").await;
    let r = parse(&get_as(port, &x, "/actor/t/on-b/fired").await.unwrap());
    assert_eq!((r.status, r.node.as_deref()), (200, Some("b")));
    let r = parse(&get_as(port, &x, "/actor/t/on-a/fired").await.unwrap());
    assert_eq!(r.status, 503, "{}", r.body);
    // A WebSocket x opens on the cluster port (as when relaying for a
    // client) works while x is trusted...
    let (mut ws, _) = tokio_tungstenite::client_async(
        relayed_upgrade("ws://n/actor/t/on-b"),
        connect_as(port, &x).await.unwrap(),
    )
    .await
    .unwrap();
    ws.send(Message::text("hi")).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(echo.unwrap().unwrap(), Message::text("hi"));
    assert!(r.body.contains("ownership is changing"), "{}", r.body);
    // The same request on b's public port is relayed to a.
    let r = send(b.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));

    // x's key changes (a new pin in its heartbeat): the old certificate is
    // refused.
    alive.store(false, Ordering::SeqCst);
    let y = stranger();
    let alive = Arc::new(AtomicBool::new(true));
    tokio::time::sleep(Duration::from_millis(100)).await;
    heartbeats(&store, y.pin.clone(), Arc::clone(&alive));
    settle().await;
    assert!(get_as(port, &x, "/_wasmkeep/health").await.is_err());
    assert!(get_as(port, &y, "/_wasmkeep/health").await.is_ok());
    // ...and is cut once its pin is gone, not left open.
    let cut = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(cut.is_ok(), "the WebSocket outlived its peer's pin");

    // Its heartbeat gone: refused at the TLS layer.
    alive.store(false, Ordering::SeqCst);
    settle().await;
    assert!(get_as(port, &y, "/_wasmkeep/health").await.is_err());
    drop(a);
}

/// The cluster secret, once the first node has stored it.
async fn cluster_secret(store: &Arc<dyn ObjectStore>) -> String {
    let key = StorePath::from("cluster/secret");
    loop {
        if let Ok(got) = store.get(&key).await {
            let bytes = got.bytes().await.unwrap();
            return String::from_utf8(bytes.to_vec()).unwrap().trim().to_owned();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// HMAC-SHA256, hex.
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

static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A POST of `path` (empty body) signed the way node calls are, for node
/// `to`, from the actor `caller` (`tenant/class/name`), with the forwarded
/// header a calling node adds.
fn signed_post(secret: &str, to: &str, path: &str, caller: &str) -> String {
    use sha2::{Digest, Sha256};
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let nonce = format!("{:x}", NONCE.fetch_add(1, Ordering::SeqCst));
    let hash: String = Sha256::digest(b"")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let msg = format!(
        "wasmkeep-call v4\n{ts}\n{nonce}\n{to}\nPOST\n{path}\n{hash}\nx-wasmkeep-caller:{caller}\n\
         x-wasmkeep-app:default/default\n"
    );
    let mac = hmac_hex(secret.as_bytes(), msg.as_bytes());
    format!(
        "POST {path} HTTP/1.1\r\nHost: n\r\nContent-Length: 0\r\n\
         x-wasmkeep-caller: {caller}\r\n\
         x-wasmkeep-internal: v4 {ts} {nonce} {hash} {mac}\r\n\
         x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n\
         Connection: close\r\n\r\n"
    )
}

/// Sends a raw request on `stream`: status and (dechunked) body.
async fn exchange<S>(mut stream: S, req: &str) -> (u16, String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    // A TLS peer may close without close_notify: keep what arrived.
    let _ = stream.read_to_end(&mut raw).await;
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let r = parse(&raw);
    let chunked = raw.split_once("\r\n\r\n").is_some_and(|(h, _)| {
        h.to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
    });
    let body = if chunked { dechunk(&r.body) } else { r.body };
    (r.status, body.trim().to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn refuse_mode_honours_node_calls_on_the_cluster_port_only() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // a accepts plaintext, b refuses it; both serve their public port.
    let a = node("a", da.path(), &store, Mode::Both).await;
    let b = node_with("b", db.path(), &store, Mode::Both, ClusterPlaintext::Refuse).await;
    let secret = cluster_secret(&store).await;
    // x: another node's identity, so the test can use b's cluster port.
    let x = stranger();
    let alive = Arc::new(AtomicBool::new(true));
    heartbeats(&store, x.pin.clone(), Arc::clone(&alive));
    settle().await;
    let caller = "default/t/someone";
    let public = |addr: SocketAddr, req: String| async move {
        exchange(TcpStream::connect(addr).await.unwrap(), &req).await
    };

    // Accept mode, unchanged: a correctly signed call on the public port
    // is a node's call (the internal class answers, the caller is believed).
    let r = public(
        a.addr,
        signed_post(&secret, "a", "/actor/u/on-a/fired", caller),
    )
    .await;
    assert_eq!(r, (200, "0".to_owned()));
    let r = public(
        a.addr,
        signed_post(&secret, "a", "/actor/t/on-a/caller", caller),
    )
    .await;
    assert_eq!(r, (200, caller.to_owned()));

    // Refuse mode: the same call on b's public port comes from outside.
    let r = public(
        b.addr,
        signed_post(&secret, "b", "/actor/u/on-b/fired", caller),
    )
    .await;
    assert_eq!(r.0, 403, "{}", r.1);
    assert!(r.1.contains("not reachable from outside"), "{}", r.1);
    let r = public(
        b.addr,
        signed_post(&secret, "b", "/actor/t/on-b/caller", caller),
    )
    .await;
    assert_eq!(r, (200, "none".to_owned()), "the caller is not believed");

    // Over b's cluster port it is a node's call.
    let port = b.cluster.unwrap();
    let tls = |req: String| {
        let x = &x;
        async move { exchange(connect_as(port, x).await.unwrap(), &req).await }
    };
    let r = tls(signed_post(&secret, "b", "/actor/u/on-b/fired", caller)).await;
    assert_eq!(r, (200, "0".to_owned()));
    let r = tls(signed_post(&secret, "b", "/actor/t/on-b/caller", caller)).await;
    assert_eq!(r, (200, caller.to_owned()));

    // A call first seen on the public port (as one sent in plaintext during
    // a rollout) is spent there: replayed on the cluster port, refused.
    let copy = signed_post(&secret, "b", "/actor/u/on-b/fired", caller);
    let r = public(b.addr, copy.clone()).await;
    assert_eq!(r.0, 403, "{}", r.1);
    let r = tls(copy).await;
    assert_eq!(r.0, 403, "{}", r.1);
    assert!(r.1.contains("already used"), "{}", r.1);

    // A request with the forwarded header (as a client could send, or a
    // plain node forwarding) is any client's request on the public port:
    // the internal class stays closed, an open one is served, and it is
    // not forwarded again (b does not own on-a: 503).
    let forwarded = |path: &str| {
        format!(
            "POST {path} HTTP/1.1\r\nHost: n\r\nContent-Length: 0\r\nx-wasmkeep-forwarded: 1\r\n\
             x-wasmkeep-app: default/default\r\n\
             x-wasmkeep-caller: {caller}\r\nConnection: close\r\n\r\n"
        )
    };
    let r = public(b.addr, forwarded("/actor/u/on-b/fired")).await;
    assert_eq!(r.0, 403, "{}", r.1);
    let r = public(b.addr, forwarded("/actor/t/on-b/caller")).await;
    assert_eq!(r, (200, "none".to_owned()));
    place(a.addr, "on-a").await;
    let r = public(b.addr, forwarded("/actor/t/on-a/fired")).await;
    assert_eq!(r.0, 503, "{}", r.1);
    alive.store(false, Ordering::SeqCst);
}

#[tokio::test(flavor = "multi_thread")]
async fn refuse_mode_does_not_use_peers_without_a_cluster_listener() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // b refuses plaintext; a has a cluster listener only (its plain address
    // leads nowhere); c has none.
    let a = node("a", dirs[0].path(), &store, Mode::TlsOnly).await;
    let b = node_with(
        "b",
        dirs[1].path(),
        &store,
        Mode::Both,
        ClusterPlaintext::Refuse,
    )
    .await;
    let c = node("c", dirs[2].path(), &store, Mode::Plain).await;
    settle().await;
    place(a.addr, "on-a").await;
    place(b.addr, "on-b").await;
    place(c.addr, "on-c").await;
    // An internal actor on b, created by a local call.
    let r = send(b.addr, "POST", "/actor/t/on-b/call?to=u/on-b&path=/fired").await;
    assert_eq!(dechunk(&r.body).trim(), "200:0");

    // b reaches a over TLS: requests, calls, WebSockets.
    let r = send(b.addr, "GET", "/actor/t/on-a/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send(b.addr, "POST", "/actor/t/on-b/call?to=t/on-a&path=/caller").await;
    assert_eq!(dechunk(&r.body).trim(), "200:default/t/on-b");
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/actor/t/on-a", b.addr))
        .await
        .unwrap();
    ws.send(Message::text("to a")).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(echo.unwrap().unwrap(), Message::text("to a"));

    // b never reaches c (plain only): 503, a failed call, no WebSocket.
    let r = send(b.addr, "GET", "/actor/t/on-c/fired").await;
    assert_eq!(r.status, 503, "{}", r.body);
    let r = send(b.addr, "POST", "/actor/t/on-b/call?to=t/on-c&path=/caller").await;
    assert!(dechunk(&r.body).starts_with("failed:"), "{}", r.body);
    match tokio_tungstenite::connect_async(format!("ws://{}/actor/t/on-c", b.addr)).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 503),
        other => panic!("relayed to a plain peer: {:?}", other.map(|_| ())),
    }
    // c itself still serves its actor.
    assert_eq!(send(c.addr, "GET", "/actor/t/on-c/fired").await.status, 200);

    // c still reaches b's public port in plaintext, but only as an outside
    // client: its forwarded request is served, its signed calls are not
    // node calls (no caller; the internal class is closed to it).
    let r = send(c.addr, "GET", "/actor/t/on-b/fired").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    let r = send(c.addr, "POST", "/actor/t/on-c/call?to=t/on-b&path=/caller").await;
    assert_eq!(dechunk(&r.body).trim(), "200:none");
    let r = send(c.addr, "POST", "/actor/t/on-c/call?to=u/on-b&path=/fired").await;
    assert!(dechunk(&r.body).starts_with("403:"), "{}", r.body);
}

/// The body of a chunked response, decoded.
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
