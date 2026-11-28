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
        // Outside the signed set, as any header outside the host's range
        // (ADR 0044, section 3): the MAC below is the one captured
        // without it.
        (
            "traceparent",
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
        ),
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
    answering("200 OK").await
}

/// [`recorder`], answering every request with `status` (`404 Not Found`).
async fn answering(status: &'static str) -> (String, Captured) {
    answering_in_turn(vec![status]).await
}

/// [`answering`] with each status of `statuses` in turn, the last one for
/// every request after.
async fn answering_in_turn(statuses: Vec<&'static str>) -> (String, Captured) {
    let statuses = Arc::new(statuses);
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
            let statuses = Arc::clone(&statuses);
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
                let status = {
                    let mut all = into.0.lock().unwrap();
                    all.push(buf);
                    statuses[(all.len() - 1).min(statuses.len() - 1)]
                };
                let answer = format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\n\r\n");
                let _ = socket.write_all(answer.as_bytes()).await;
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
            traces: false,
            seen: None,
            version: String::new(),
            serving_since: None,
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

/// A hello a peer answers 404 was not taken, and another round is sent
/// (ADR 0043, section 6). Until 0.9 a 404 passed for the answer of a node
/// of 0.4.1 and before, which knew no hello: no node this version runs
/// beside is one, and a node of 0.8.1 answers 200 or 403.
/// A take answered `503` is a node alive that cannot take the actor at
/// that moment (ADR 0040, section 14, amendment of 2026-11-28): signed and
/// sent again until it is answered, within the take's time; one answered
/// `503` to the end is `Unavailable` (neither suspected nor backed off by
/// the callers), not `Failed`. Any other refusal is not sent again.
#[tokio::test]
async fn a_take_answered_503_is_sent_again_within_its_time() {
    let target = ActorAddress::new("t", "room", "a b").unwrap();
    let (base, captured) = answering_in_turn(vec![
        "503 Service Unavailable",
        "503 Service Unavailable",
        "200 OK",
    ])
    .await;
    let me = sender(&base).await;
    assert_eq!(me.take("owner", &target).await, TakeAnswer::Held);
    let sent = captured.0.lock().unwrap().clone();
    assert_eq!(sent.len(), 3, "sent until answered");
    let macs: std::collections::HashSet<String> = sent
        .iter()
        .map(|raw| fields(header(&String::from_utf8_lossy(raw), INTERNAL)).3)
        .collect();
    assert_eq!(macs.len(), 3, "each try signed anew");

    let (base, captured) = answering("503 Service Unavailable").await;
    let me = sender(&base).await;
    let started = std::time::Instant::now();
    let answer = me
        .take_within("owner", &target, Duration::from_millis(1_200))
        .await;
    assert!(matches!(answer, TakeAnswer::Unavailable(_)), "{answer:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(captured.0.lock().unwrap().len() >= 3);

    // A try after a `503` that fails otherwise (no answer in time, an
    // error) still says the node asked for a retry: not `Failed`.
    let (base, _) =
        answering_in_turn(vec!["503 Service Unavailable", "500 Internal Server Error"]).await;
    let me = sender(&base).await;
    let answer = me.take("owner", &target).await;
    assert!(matches!(answer, TakeAnswer::Unavailable(_)), "{answer:?}");

    let (base, captured) = answering("500 Internal Server Error").await;
    let me = sender(&base).await;
    assert!(matches!(
        me.take("owner", &target).await,
        TakeAnswer::Failed(_)
    ));
    assert_eq!(captured.0.lock().unwrap().len(), 1, "not sent again");
}

#[tokio::test]
async fn a_hello_answered_404_is_not_taken() {
    for (status, fails) in [("200 OK", false), ("404 Not Found", true)] {
        let (base, captured) = answering(status).await;
        let me = sender(&base).await;
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        me.say_hello(Arc::clone(&failed));
        // The hello was sent and its answer read.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while captured.0.lock().unwrap().is_empty() {
            assert!(tokio::time::Instant::now() < deadline, "{status}: no hello");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            failed.load(std::sync::atomic::Ordering::Acquire),
            fails,
            "{status}"
        );
    }
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

/// The spans a test's tracer made.
#[derive(Debug, Clone, Default)]
struct Spans(Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
impl opentelemetry_sdk::trace::SpanExporter for Spans {
    async fn export(
        &self,
        spans: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(spans);
        Ok(())
    }
}

/// The `traceparent` a traced hop carried in `raw`: this node's `hop`
/// span in the trace of [`crate::traces::test_context`], sampled. And
/// `raw` without that line.
fn traced(raw: &str) -> (String, String) {
    let value = header(raw, "traceparent").to_owned();
    let context = crate::traces::test_context(true);
    let (trace, parent) = (
        "00-1234567890abcdef1234567890abcdef-",
        "1234567890abcdef-01",
    );
    assert!(
        value.len() == 55 && value.starts_with(trace) && value.ends_with("-01"),
        "{value}"
    );
    assert!(
        !value.ends_with(parent),
        "{value}: the hop span's id, not {context:?}"
    );
    let line = format!("traceparent: {value}\r\n");
    assert_eq!(raw.matches("traceparent").count(), 1, "{raw}");
    (value, raw.replace(&line, ""))
}

/// With tracing on, a call and a forward sent inside a sampled trace
/// (ADR 0044, section 3; ADR 0043, section 7): what is on the wire is
/// what 0.8.1 sends, byte for byte, and one header outside the host's
/// range.
///
/// - A call: its signature is over the same message, the header in no
///   line of it; without the `traceparent` line the request is the one
///   captured from the release before.
/// - A forward: the proof's grammar, key and message are the same, and
///   its hash of the client's headers now lists `traceparent` among them,
///   as it does for a client that sent one to 0.8.1. Without the line, and
///   with the hash of the two other headers, the request is the one
///   captured.
#[tokio::test]
async fn a_traced_call_and_forward_add_one_header_and_sign_as_before() {
    let spans = Spans::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(spans.clone())
        .build();
    let _guard = crate::traces::install(provider, 1.0, false).unwrap();
    let context = Some(crate::traces::test_context(true));

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
    let response = crate::traces::scope(context, me.call("owner", &target, &chain, req))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let raw = captured.take();
    let (_, without) = traced(&raw);
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
    assert_eq!(redact(&without, &base, Some(&nonce)), GOLDEN_CALL, "{raw}");

    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    let req = forwarded(http_body_util::Full::new(bytes::Bytes::from_static(
        b"payload",
    )));
    let response = crate::traces::scope(context, me.forward("owner", req)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let raw = captured.take();
    let (value, without) = traced(&raw);
    let (ts, nonce, hash, mac) = fields(header(&raw, ADMITTED));
    assert_eq!(nonce, "000000000000000000000ffffc0000201");
    // The client's identity, as 0.8.1 hashes it: every header outside the
    // range, sorted, `name\0value\0`, the node's `traceparent` among them.
    assert_eq!(
        hash,
        body_sha256(
            format!("accept\0text/plain\0traceparent\0{value}\0x-client-id\0alice\0").as_bytes()
        )
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
    let plain = body_sha256(b"accept\0text/plain\0x-client-id\0alice\0");
    assert_eq!(
        redact(&without, &base, None).replace(&hash, &plain),
        GOLDEN_FORWARD,
        "{raw}"
    );

    // An alarm hand-off and a take sent inside a trace are hops of it
    // too: signed as every call, one header more.
    for kind in ["hand_off", "take"] {
        let (base, captured) = recorder().await;
        let me = sender(&base).await;
        let marker = if kind == "take" { TAKE } else { ALARM_HANDOFF };
        crate::traces::scope(context, async {
            if kind == "take" {
                assert_eq!(me.take("owner", &target).await, TakeAnswer::Held);
            } else {
                me.hand_off_alarm("owner", &target, 1_700_000_000_000)
                    .await
                    .unwrap();
            }
        })
        .await;
        let raw = captured.take();
        traced(&raw);
        let (ts, nonce, hash, mac) = fields(header(&raw, INTERNAL));
        let msg = format!(
            "wasmkeep-call v4\n{ts}\n{nonce}\nowner\nPOST\n/actor/room/a%20b/\n{hash}\n\
             {marker}:1\n\
             x-wasmkeep-app:t/default\n"
        );
        assert_eq!(mac, hmac_hex(SECRET, &msg), "{kind}");
        // Outside any trace (a drain, a sweep, a move): no header.
        let (base, captured) = recorder().await;
        let me = sender(&base).await;
        if kind == "take" {
            assert_eq!(me.take("owner", &target).await, TakeAnswer::Held);
        } else {
            me.hand_off_alarm("owner", &target, 1_700_000_000_000)
                .await
                .unwrap();
        }
        assert!(!captured.take().contains("traceparent"), "{kind}");
    }
    // One hop span for each hop sent inside the trace, and none for the
    // others.
    let hops: Vec<(String, String)> = spans
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.name == "hop")
        .map(|s| {
            let attribute = |key: &str| {
                s.attributes
                    .iter()
                    .find(|a| a.key.as_str() == key)
                    .map(|a| a.value.to_string())
                    .unwrap_or_default()
            };
            assert_eq!(
                (attribute("node"), attribute("peer"), attribute("status")),
                ("me".into(), "owner".into(), "200".into())
            );
            assert_eq!(s.parent_span_id.to_string(), "1234567890abcdef");
            (attribute("kind"), attribute("carried"))
        })
        .collect();
    let carried = |kind: &str| (kind.to_owned(), "true".to_owned());
    assert_eq!(
        hops,
        [
            carried("call"),
            carried("forward"),
            carried("hand_off"),
            carried("take")
        ]
    );

    // Outside any trace (a sweep's hand-off, a move's take): nothing is
    // added, and a client's header does not go on either.
    let (base, captured) = recorder().await;
    let me = sender(&base).await;
    let mut req = forwarded(http_body_util::Full::new(bytes::Bytes::from_static(
        b"payload",
    )));
    req.headers_mut().insert(
        "traceparent",
        "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01"
            .parse()
            .unwrap(),
    );
    assert_eq!(me.forward("owner", req).await.status(), StatusCode::OK);
    let raw = captured.take();
    assert!(!raw.contains("traceparent"), "{raw}");
    let (_, _, hash, _) = fields(header(&raw, ADMITTED));
    assert_eq!(hash, plain);
    // The same lines as captured; removing a header moves the others.
    let lines = |head: &str| {
        let mut lines: Vec<String> = head.split("\r\n").map(str::to_owned).collect();
        lines.sort();
        lines
    };
    assert_eq!(
        lines(&redact(&raw, &base, None)),
        lines(GOLDEN_FORWARD),
        "{raw}"
    );
}

/// Captured from the release before.
const GOLDEN_CALL: &str = "POST /actor/room/a%20b/inner?x=1 HTTP/1.1\r\nx-guest: kept\r\nx-wasmkeep-caller: t/room/caller\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-forwarded: 1\r\nx-wasmkeep-chain: t/room/one\r\nx-wasmkeep-chain: t/room/caller\r\nx-wasmkeep-internal: v4 TS NONCE 239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\nhost: 127.0.0.1:PORT\r\ncontent-length: 7\r\n\r\npayload";
const GOLDEN_HELLO: &str = "POST /_wasmkeep/hello HTTP/1.1\r\nhost: node.wasmkeep.internal\r\nconnection: close\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-internal: v4 TS NONCE e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\n\r\n";
/// Both body types, the same bytes.
const GOLDEN_FORWARD: &str = "POST /actor/room/a%20b/inner?q=1 HTTP/1.1\r\nhost: app.test\r\nx-client-id: alice\r\naccept: text/plain\r\nx-wasmkeep-app: t/default\r\nx-wasmkeep-caller: t/room/forged\r\ncontent-length: 7\r\nx-wasmkeep-forwarded: 1\r\nx-wasmkeep-admitted: v4 TS 000000000000000000000ffffc0000201 62a92262abe143250b1a5c93d6b447e0fc89c558c87f1bfbf119d57f0a538d81 MAC\r\nx-wasmkeep-sender: me SENDER-MAC\r\n\r\npayload";
