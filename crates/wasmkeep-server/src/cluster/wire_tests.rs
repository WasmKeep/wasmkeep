//! The bytes of a hop, pinned: what a call, a forward and a hello sign and
//! send. A node of the previous patch release verifies what this one sends
//! and the other way round only while these stay the same, byte for byte.
//! The expected values were captured from 0.8.0's code; a change here is
//! a change of the wire between nodes.
//!
//! Each hop is sent to a listener that records the request as it arrives
//! on the socket. The timestamp, the call nonce (a random seed) and the
//! port are the only fields that vary between runs: they are replaced by
//! placeholders before the comparison, and every MAC is checked against
//! an HMAC this test computes itself over the message it spells out.

use super::*;
use signing::hmac_sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "abababababababababababababababababababababababababababababababab";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// HMAC-SHA256 over `msg` under the cluster secret `key`, hex.
fn hmac_hex(key: &str, msg: &str) -> String {
    hex(&hmac_sha256(key.as_bytes(), msg.as_bytes()))
}

/// The HMAC itself (RFC 4231, test case 2).
#[test]
fn the_hmac_is_rfc_4231() {
    assert_eq!(
        hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

/// The v4 signature over fixed inputs, every signed header present, two
/// chain values: the MAC of the message spelled out here, and the value
/// the release before computed.
#[test]
fn a_v4_signature_signs_these_bytes() {
    let mut headers = hyper::HeaderMap::new();
    // Out of order and with unsigned ones between: the order signed is
    // the list's, each header's values in the order they came.
    for (name, value) in [
        ("x-wasmkeep-placed", "1"),
        ("x-other", "unsigned"),
        ("x-wasmkeep-app", "t/default"),
        ("x-wasmkeep-chain", "t/room/one"),
        ("x-wasmkeep-caller", "t/room/caller"),
        ("x-wasmkeep-chain", "t/room/two"),
        ("x-wasmkeep-take", "1"),
        ("x-wasmkeep-upgrade", "websocket"),
        ("x-wasmkeep-alarm-handoff", "1"),
        ("x-wasmkeep-forwarded", "1"),
    ] {
        headers.append(name, value.parse().unwrap());
    }
    let body = body_sha256(b"hello");
    let msg = format!(
        "wasmkeep-call v4\n1700000000000\n0a1b\nowner\nPOST\n/actor/room/a%20b?x=1\n{body}\n\
         x-wasmkeep-caller:t/room/caller\n\
         x-wasmkeep-chain:t/room/one\n\
         x-wasmkeep-chain:t/room/two\n\
         x-wasmkeep-alarm-handoff:1\n\
         x-wasmkeep-take:1\n\
         x-wasmkeep-app:t/default\n\
         x-wasmkeep-upgrade:websocket\n\
         x-wasmkeep-placed:1\n"
    );
    let signed = signature(
        SECRET,
        1_700_000_000_000,
        "0a1b",
        "owner",
        &hyper::Method::POST,
        "/actor/room/a%20b?x=1",
        &body,
        &headers,
    );
    assert_eq!(signed, hmac_hex(SECRET, &msg));
    assert_eq!(
        signed,
        "444427f2ae2bc5d6d2d348765217dc6d7c63e21514665a5906e22df98e4f4640"
    );
    // Without any signed header: the seven lines are simply absent.
    let bare = signature(
        SECRET,
        1_700_000_000_000,
        "0a1b",
        "owner",
        &hyper::Method::GET,
        "/",
        &body,
        &hyper::HeaderMap::new(),
    );
    assert_eq!(
        bare,
        hmac_hex(
            SECRET,
            &format!("wasmkeep-call v4\n1700000000000\n0a1b\nowner\nGET\n/\n{body}\n")
        )
    );
    // The sender, bound to a hop's signature.
    assert_eq!(
        sender_value(SECRET, "internal", &signed, "me"),
        format!(
            "me {}",
            hmac_hex(
                SECRET,
                &format!("wasmkeep-sender v1\ninternal\n{signed}\nme\n")
            )
        )
    );
}

/// What a listener got: the request head and body as they came.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<Vec<u8>>>>);

impl Captured {
    fn take(&self) -> String {
        let mut all = self.0.lock().unwrap();
        assert_eq!(all.len(), 1, "one request");
        String::from_utf8(all.remove(0)).unwrap()
    }
}

/// A listener that records each request (head and body) and answers 200.
async fn recorder() -> (String, Captured) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Captured::default();
    let into = captured.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let into = into.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .map_or(0, |v| v.trim().parse().unwrap());
                while buf.len() < head_end + length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                into.0.lock().unwrap().push(buf);
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    (format!("http://{addr}"), captured)
}

/// Node `me` of `t/default`, the secret loaded, `owner` listed at `base`.
async fn sender(base: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(SECRET_KEY),
            SECRET.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new("me", "http://me", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    c.load_secret().await;
    *c.peers.write().unwrap() = HashMap::from([(
        "owner".to_owned(),
        Peer {
            plain: Some(base.to_owned()),
            tls: None,
            app: "t/default".into(),
            drain: None,
            can_take: true,
            can_handoff: true,
            can_end: true,
            can_blobs: true,
            instance: None,
            can_place: false,
            seen: None,
        },
    )]);
    c
}

/// The fields of a v4 value (`v4 <ts> <nonce> <hash> <mac>`).
fn fields(value: &str) -> (u64, String, String, String) {
    let f: Vec<&str> = value.split(' ').collect();
    assert_eq!(f.len(), 5, "{value}");
    assert_eq!(f[0], "v4");
    (
        f[1].parse().unwrap(),
        f[2].to_owned(),
        f[3].to_owned(),
        f[4].to_owned(),
    )
}

/// The value of header `name` in a recorded head.
fn header<'a>(raw: &'a str, name: &str) -> &'a str {
    raw.lines()
        .find_map(|l| l.strip_prefix(&format!("{name}: ")))
        .unwrap_or_else(|| panic!("no {name} in {raw}"))
}

/// `raw` with the timestamp, the call nonce, the MACs and the port
/// replaced, so the rest compares exactly.
fn redact(raw: &str, base: &str, nonce: Option<&str>) -> String {
    let port = base.rsplit(':').next().unwrap();
    let mut out = raw.replace(&format!("127.0.0.1:{port}"), "127.0.0.1:PORT");
    for name in [INTERNAL, ADMITTED] {
        if let Some(value) = raw
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name}: ")))
        {
            let (ts, _, _, mac) = fields(value);
            out = out.replace(&ts.to_string(), "TS").replace(&mac, "MAC");
        }
    }
    if let Some(sender) = raw
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{SENDER}: ")))
    {
        let (_, sender_mac) = sender.split_once(' ').unwrap();
        out = out.replace(sender_mac, "SENDER-MAC");
    }
    if let Some(nonce) = nonce {
        out = out.replace(nonce, "NONCE");
    }
    out
}

/// A call (`actors.call`) from `me` to an actor `owner` holds: the request
/// as sent, and the MACs over the message spelled out.
#[tokio::test]
async fn a_call_sends_these_bytes() {
    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    let target = ActorAddress::new("t", "room", "a b").unwrap();
    let chain = [
        ActorAddress::new("t", "room", "one").unwrap(),
        ActorAddress::new("t", "room", "caller").unwrap(),
    ];
    let req = Request::post("/inner?x=1")
        .header("x-guest", "kept")
        .header(CALLER, "t/room/caller")
        .header(CHAIN, "stale")
        .body(http_body_util::Full::new(bytes::Bytes::from_static(
            b"payload",
        )))
        .unwrap();
    let response = me.call("owner", &target, &chain, req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let raw = captured.take();
    let (ts, nonce, hash, mac) = fields(header(&raw, INTERNAL));
    assert_eq!(hash, body_sha256(b"payload"));
    let msg = format!(
        "wasmkeep-call v4\n{ts}\n{nonce}\nowner\nPOST\n/actor/room/a%20b/inner?x=1\n{hash}\n\
         x-wasmkeep-caller:t/room/caller\n\
         x-wasmkeep-chain:t/room/one\n\
         x-wasmkeep-chain:t/room/caller\n\
         x-wasmkeep-app:t/default\n"
    );
    assert_eq!(mac, hmac_hex(SECRET, &msg));
    assert_eq!(
        header(&raw, SENDER),
        format!(
            "me {}",
            hmac_hex(
                SECRET,
                &format!("wasmkeep-sender v1\ninternal\n{mac}\nme\n")
            )
        )
    );
    assert_eq!(redact(&raw, &base, Some(&nonce)), GOLDEN_CALL, "{raw}");
}

/// A hello from `me` to `owner`.
#[tokio::test]
async fn a_hello_sends_these_bytes() {
    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    assert_eq!(me.hello("owner").await.unwrap(), StatusCode::OK);
    let raw = captured.take();
    let (ts, nonce, hash, mac) = fields(header(&raw, INTERNAL));
    assert_eq!(hash, body_sha256(b""));
    let msg = format!(
        "wasmkeep-call v4\n{ts}\n{nonce}\nowner\nPOST\n/_wasmkeep/hello\n{hash}\n\
         x-wasmkeep-app:t/default\n"
    );
    assert_eq!(mac, hmac_hex(SECRET, &msg));
    assert_eq!(
        header(&raw, SENDER),
        format!(
            "me {}",
            hmac_hex(
                SECRET,
                &format!("wasmkeep-sender v1\ninternal\n{mac}\nme\n")
            )
        )
    );
    assert_eq!(redact(&raw, &base, Some(&nonce)), GOLDEN_HELLO, "{raw}");
}

/// A client's request, as a front node forwards it to `owner`: the proof
/// of the client's identity over its headers, and the headers sent.
fn forwarded<B>(body: B) -> Request<B> {
    let mut req = Request::post("/inner?q=1")
        .header("host", "app.test")
        .header("x-client-id", "alice")
        .header("accept", "text/plain")
        .header("connection", "keep-alive, x-dropped")
        .header("x-dropped", "per hop")
        .header("expect", "100-continue")
        .header("content-length", "7")
        .header(CALLER, "t/room/forged")
        .header(APP, "t/forged")
        .body(body)
        .unwrap();
    req.extensions_mut()
        .insert(OriginalPath("/actor/room/a%20b/inner?q=1".to_owned()));
    req.extensions_mut()
        .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
    req
}

/// What a forward of [`forwarded`] must carry, `raw` as recorded.
fn check_forward(raw: &str, base: &str, golden: &str) {
    let (ts, nonce, hash, mac) = fields(header(raw, ADMITTED));
    // Uncharged, the IPv4 address mapped into IPv6.
    assert_eq!(nonce, "000000000000000000000ffffc0000201");
    // The client's identity: every header outside the range, not
    // hop-by-hop, not host or expect, sorted, `name\0value\0`.
    assert_eq!(
        hash,
        body_sha256(b"accept\0text/plain\0x-client-id\0alice\0")
    );
    let msg = format!(
        "wasmkeep-call v4\n{ts}\n{nonce}\nowner\nPOST\n/actor/room/a%20b/inner?q=1\n{hash}\n\
         x-wasmkeep-caller:t/room/forged\n\
         x-wasmkeep-app:t/default\n"
    );
    assert_eq!(
        mac,
        hmac_hex(&format!("wasmkeep-ingress-identity-v2:{SECRET}"), &msg)
    );
    // The sender's MAC is under the cluster secret itself.
    assert_eq!(
        header(raw, SENDER),
        format!(
            "me {}",
            hmac_hex(
                SECRET,
                &format!("wasmkeep-sender v1\nadmitted\n{mac}\nme\n")
            )
        )
    );
    assert_eq!(redact(raw, base, None), golden, "{raw}");
}

/// A forward of a body read into memory.
#[tokio::test]
async fn a_buffered_forward_sends_these_bytes() {
    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    let req = forwarded(http_body_util::Full::new(bytes::Bytes::from_static(
        b"payload",
    )));
    let response = me.forward("owner", req).await;
    assert_eq!(response.status(), StatusCode::OK);
    check_forward(&captured.take(), &base, GOLDEN_FORWARD);
}

/// A forward of a client's streamed body: the request arrives at a
/// listener of its own, which forwards it as the public port does.
#[tokio::test]
async fn a_streamed_forward_sends_these_bytes() {
    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    let front = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front_addr = front.local_addr().unwrap();
    let forwarder = Arc::clone(&me);
    tokio::spawn(async move {
        let (socket, _) = front.accept().await.unwrap();
        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
            let me = Arc::clone(&forwarder);
            async move {
                let (parts, body) = req.into_parts();
                let mut req = forwarded(body);
                // As the client sent it, framing and all.
                *req.headers_mut() = parts.headers;
                Ok::<_, std::convert::Infallible>(me.forward("owner", req).await)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
            .await;
    });
    let mut client = tokio::net::TcpStream::connect(front_addr).await.unwrap();
    client
        .write_all(
            b"POST /inner?q=1 HTTP/1.1\r\n\
              host: app.test\r\n\
              x-client-id: alice\r\n\
              accept: text/plain\r\n\
              connection: keep-alive, x-dropped\r\n\
              x-dropped: per hop\r\n\
              content-length: 7\r\n\
              x-wasmkeep-caller: t/room/forged\r\n\
              x-wasmkeep-app: t/forged\r\n\
              \r\n\
              payload",
        )
        .await
        .unwrap();
    let mut answer = [0u8; 12];
    client.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"HTTP/1.1 200");
    check_forward(&captured.take(), &base, GOLDEN_FORWARD);
}

/// Captured from the release before.
const GOLDEN_CALL: &str = "POST /actor/room/a%20b/inner?x=1 HTTP/1.1\r\nx-guest: kept\r\nx-wasmkeep-caller: t/room/caller\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-forwarded: 1\r\nx-wasmkeep-chain: t/room/one\r\nx-wasmkeep-chain: t/room/caller\r\nx-wasmkeep-internal: v4 TS NONCE 239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\nhost: 127.0.0.1:PORT\r\ncontent-length: 7\r\n\r\npayload";
const GOLDEN_HELLO: &str = "POST /_wasmkeep/hello HTTP/1.1\r\nhost: node.wasmkeep.internal\r\nconnection: close\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-internal: v4 TS NONCE e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\n\r\n";
/// Both body types, the same bytes.
const GOLDEN_FORWARD: &str = "POST /actor/room/a%20b/inner?q=1 HTTP/1.1\r\nhost: app.test\r\nx-client-id: alice\r\naccept: text/plain\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-caller: t/room/forged\r\ncontent-length: 7\r\nx-wasmkeep-forwarded: 1\r\nx-wasmkeep-admitted: v4 TS 000000000000000000000ffffc0000201 62a92262abe143250b1a5c93d6b447e0fc89c558c87f1bfbf119d57f0a538d81 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\n\r\npayload";
