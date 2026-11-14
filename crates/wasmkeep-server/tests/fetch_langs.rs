//! Outbound HTTP from the guest SDKs that wrap `wasi:http/outgoing-handler`
//! themselves: the same requests through the Rust SDK's `http::fetch`
//! (tests/guests/test-actor), the Go SDK's `Fetch` (tests/guests/go-fetch,
//! built by its `build.sh`) and the TypeScript SDK's `http.fetch`
//! (tests/guests/ts-fetch, `npm run build` and `npm run
//! build:starlingmonkey` there), against a local server, must give the
//! same answers. Each SDK's batch helper
//! (`http::fetch_all`, `FetchAll`, `http.fetchAll` on both engines) gives
//! those answers too, in order, and keeps its
//! requests open at once: the server sees them in flight together.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, EgressPolicy};

#[path = "support/guests.rs"]
mod guests;

fn rust_guest() -> Vec<u8> {
    guests::build_guest("tests/guests/test-actor", "test_actor")
}

fn go_guest() -> Option<Vec<u8>> {
    prebuilt("go-fetch", "go_fetch.wasm")
}

/// Requests the [`server`] has open (from the end of the request head to
/// the end of the answer), and the most it had at once.
#[derive(Default)]
struct InFlight {
    now: AtomicUsize,
    peak: AtomicUsize,
}

/// How long `/slow` and `/slow-body` wait.
const SLOW: Duration = Duration::from_millis(500);

/// A prebuilt guest in tests/guests (`dir/file`), if built; with
/// `WASMKEEP_REQUIRE_LANGS` set (CI) a missing build fails instead.
fn prebuilt(dir: &str, file: &str) -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/guests")
        .join(dir)
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

/// A plain HTTP server on 127.0.0.1:
/// - `GET /ok`: `200 hello`
/// - `GET /slow`: `200 slow`, after [`SLOW`]
/// - `GET /slow-body`: the head at once, then the body (`slow body`) after
///   [`SLOW`]
/// - `POST /echo`: `201 got N` (N bytes of body received)
/// - `GET /big`: 200 with 10,000 bytes of `y`
/// - `GET /short`: announces 100 bytes, sends 3 and closes
async fn server() -> SocketAddr {
    server_counting().await.0
}

/// [`server`], and what it has in flight.
async fn server_counting() -> (SocketAddr, Arc<InFlight>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let flight = Arc::new(InFlight::default());
    let counts = Arc::clone(&flight);
    tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _)) = listener.accept().await else {
                return;
            };
            let flight = Arc::clone(&counts);
            tokio::spawn(async move {
                let mut got = Vec::new();
                let mut buf = vec![0; 8192];
                let head_end = loop {
                    if let Some(i) = got.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    match tcp.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got.extend_from_slice(&buf[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&got[..head_end]).to_ascii_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while got.len() < head_end + length {
                    match tcp.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got.extend_from_slice(&buf[..n]),
                    }
                }
                let path = head.split_whitespace().nth(1).unwrap_or("").to_owned();
                let open = flight.now.fetch_add(1, Ordering::SeqCst) + 1;
                flight.peak.fetch_max(open, Ordering::SeqCst);
                let reply = match path.as_str() {
                    "/slow" => {
                        tokio::time::sleep(SLOW).await;
                        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nslow"
                            .to_vec()
                    }
                    "/slow-body" => {
                        let _ = tcp
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n")
                            .await;
                        let _ = tcp.flush().await;
                        tokio::time::sleep(SLOW).await;
                        b"slow body".to_vec()
                    }
                    "/ok" => {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
                            .to_vec()
                    }
                    "/echo" => {
                        let body = format!("got {length}");
                        format!(
                            "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .into_bytes()
                    }
                    "/big" => {
                        let mut r =
                            b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\nConnection: close\r\n\r\n".to_vec();
                        r.extend(std::iter::repeat_n(b'y', 10_000));
                        r
                    }
                    "/short" => {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nabc"
                            .to_vec()
                    }
                    _ => {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    }
                };
                let _ = tcp.write_all(&reply).await;
                let _ = tcp.shutdown().await;
                flight.now.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    (addr, flight)
}

async fn start(data: &Path, wasm: &[u8], egress: EgressPolicy) -> SocketAddr {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, wasm);
    let mut config = ActorConfig::new(data);
    config.egress = egress;
    let service = ActorService::new(
        runtime,
        config,
        vec![("t".into(), component, wasm.to_vec())],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    addr
}

/// Percent-encodes `s` for a query string.
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The body of a GET of `path` from actor `t/a`, and how long it took.
async fn get_from_guest(addr: SocketAddr, path: &str) -> (String, Duration) {
    let started = Instant::now();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET /actor/t/a{path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let took = started.elapsed();
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or(("", ""));
    assert!(head.starts_with("HTTP/1.1 200"), "{path}: {head}");
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = String::new();
        let mut rest = body;
        while let Some((size, after)) = rest.split_once("\r\n") {
            let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            if n == 0 {
                break;
            }
            out.push_str(&after[..n]);
            rest = &after[n + 2..];
        }
        return (out, took);
    }
    (body.to_owned(), took)
}

/// The guest's batch of `(send, url)` requests through its SDK's helper
/// (`GET /fetch-all?u=N%20URL&u=...`; with `seq`, one `fetch` after the
/// other instead): one answer per request, in order, as
/// [`fetch_from_guest`] words them; and how long the batch took.
async fn fetch_all_from_guest(
    addr: SocketAddr,
    batch: &[(usize, String)],
    seq: bool,
) -> (Vec<String>, Duration) {
    let mut path = String::from("/fetch-all?");
    if seq {
        path.push_str("seq=1&");
    }
    let items: Vec<String> = batch
        .iter()
        .map(|(send, url)| format!("u={}", encode(&format!("{send} {url}"))))
        .collect();
    path.push_str(&items.join("&"));
    let (body, took) = get_from_guest(addr, &path).await;
    let answers = body
        .split('\n')
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect();
    (answers, took)
}

/// What the guest's fetch of `url` returned: `STATUS:BODY` or `error: ...`
/// (without the newline the Rust guest ends it with); a POST of `send`
/// bytes when not 0.
async fn fetch_from_guest(addr: SocketAddr, url: &str, send: usize) -> String {
    let encoded: String = url
        .bytes()
        .map(|b| match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /actor/t/a/fetch?url={encoded}&send={send} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or(("", ""));
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = String::new();
        let mut rest = body;
        while let Some((size, after)) = rest.split_once("\r\n") {
            let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            if n == 0 {
                break;
            }
            out.push_str(&after[..n]);
            rest = &after[n + 2..];
        }
        return out.strip_suffix('\n').unwrap_or(&out).to_owned();
    }
    body.strip_suffix('\n').unwrap_or(body).to_owned()
}

/// The answers to the same requests, as one guest gives them.
async fn answers(wasm: &[u8]) -> Vec<String> {
    let server = server().await;
    let dir = tempfile::tempdir().unwrap();
    let local = EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    let addr = start(dir.path(), wasm, local).await;
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let mut out = vec![
        fetch_from_guest(addr, &format!("http://{server}/ok"), 0).await,
        fetch_from_guest(addr, &format!("http://{server}/echo"), 5000).await,
        fetch_from_guest(addr, &format!("http://{server}/big"), 0).await,
        fetch_from_guest(addr, &format!("http://{server}/short"), 0).await,
        // Nothing listens there any more.
        fetch_from_guest(addr, &format!("http://{closed}/ok"), 0).await,
        fetch_from_guest(addr, "ftp://example.com/", 0).await,
    ];
    // Loopback is not public: refused by the default egress rules.
    let dir = tempfile::tempdir().unwrap();
    let strict = start(dir.path(), wasm, EgressPolicy::default()).await;
    out.push(fetch_from_guest(strict, &format!("http://{server}/ok"), 0).await);
    out
}

fn check(lang: &str, got: &[String]) {
    assert_eq!(got[0], "200:hello", "{lang}");
    assert_eq!(got[1], "201:got 5000", "{lang}");
    assert_eq!(
        got[2],
        format!("200:{}", "y".repeat(4096)),
        "{lang}: kept to the limit"
    );
    assert!(
        got[3].starts_with("error: ") && got[3].contains(": incomplete body:"),
        "{lang}: a body cut short is an error, got {:?}",
        got[3]
    );
    assert!(
        got[4].starts_with("error: "),
        "{lang}: refused, got {:?}",
        got[4]
    );
    assert!(
        got[5].starts_with("error: ") && got[5].contains("unsupported scheme ftp"),
        "{lang}: got {:?}",
        got[5]
    );
    assert!(
        got[6].starts_with("error: "),
        "{lang}: egress refused, got {:?}",
        got[6]
    );
}

/// The batch helper of one guest: the same answers as one fetch at a time
/// (in order, failures beside successes), and its requests in flight at
/// once, past the host's 16 per actor too (the helper keeps to that many
/// open and starts the rest as they finish).
async fn check_batches(lang: &str, wasm: &[u8]) {
    let (server, _) = server_counting().await;
    let dir = tempfile::tempdir().unwrap();
    let local = EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    let addr = start(dir.path(), wasm, local).await;
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let batch = vec![
        (0, format!("http://{server}/ok")),
        (5000, format!("http://{server}/echo")),
        (0, format!("http://{server}/big")),
        (0, format!("http://{server}/short")),
        (0, format!("http://{closed}/ok")),
        (0, "ftp://example.com/".to_owned()),
    ];
    let (got, _) = fetch_all_from_guest(addr, &batch, false).await;
    assert_eq!(got.len(), 6, "{lang}: {got:?}");
    let mut alone = Vec::new();
    for (send, url) in &batch {
        alone.push(fetch_from_guest(addr, url, *send).await);
    }
    let mut with_refused = got.clone();
    let dir = tempfile::tempdir().unwrap();
    let strict = start(dir.path(), wasm, EgressPolicy::default()).await;
    let (refused, _) =
        fetch_all_from_guest(strict, &[(0, format!("http://{server}/ok"))], false).await;
    with_refused.extend(refused);
    check(&format!("{lang} batch"), &with_refused);
    assert_eq!(got, alone, "{lang}: a batch answers as one fetch at a time");
    let empty = get_from_guest(addr, "/fetch-all").await.0;
    assert_eq!(empty.trim(), "", "{lang}: an empty batch");

    // Four slow answers: open together.
    for path in ["slow", "slow-body"] {
        let (slow, flight) = server_counting().await;
        let four = vec![(0, format!("http://{slow}/{path}")); 4];
        let (got, took) = fetch_all_from_guest(addr, &four, false).await;
        let want = if path == "slow" {
            "200:slow"
        } else {
            "200:slow body"
        };
        assert_eq!(got, vec![want; 4], "{lang} /{path}");
        assert_eq!(
            flight.peak.load(Ordering::SeqCst),
            4,
            "{lang} /{path}: in flight together"
        );
        eprintln!(
            "{lang}: 4 x /{path} ({} ms each) at once: {} ms",
            SLOW.as_millis(),
            took.as_millis()
        );
        // Each body waits too: read in turn, they would take four times as long.
        assert!(took < SLOW * 3, "{lang} /{path}: {took:?}");
    }
    // Twenty: sixteen open at once, then the other four.
    let (slow, flight) = server_counting().await;
    let twenty = vec![(0, format!("http://{slow}/slow")); 20];
    let (got, took) = fetch_all_from_guest(addr, &twenty, false).await;
    assert_eq!(got, vec!["200:slow"; 20], "{lang}");
    assert_eq!(
        flight.peak.load(Ordering::SeqCst),
        16,
        "{lang}: the host's limit per actor"
    );
    eprintln!("{lang}: 20 x /slow at once: {} ms", took.as_millis());
}

#[tokio::test(flavor = "multi_thread")]
async fn rust() {
    check("rust", &answers(&rust_guest()).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn rust_batches() {
    let wasm = rust_guest();
    check_batches("rust", &wasm).await;
    // One `fetch` after the other, for comparison: never two open.
    let (slow, flight) = server_counting().await;
    let dir = tempfile::tempdir().unwrap();
    let local = EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    let addr = start(dir.path(), &wasm, local).await;
    let four = vec![(0, format!("http://{slow}/slow")); 4];
    let (got, took) = fetch_all_from_guest(addr, &four, true).await;
    assert_eq!(got, vec!["200:slow"; 4]);
    assert_eq!(flight.peak.load(Ordering::SeqCst), 1);
    eprintln!(
        "rust: 4 x /slow one after the other: {} ms",
        took.as_millis()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn go() {
    if let Some(wasm) = go_guest() {
        let go = answers(&wasm).await;
        check("go", &go);
        // Errors are worded by each SDK; the answers that succeed match.
        let rust = answers(&rust_guest()).await;
        assert_eq!(go[..3], rust[..3]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn go_batches() {
    if let Some(wasm) = go_guest() {
        check_batches("go", &wasm).await;
    }
}

/// The TypeScript SDK's `http.fetch` and `http.fetchAll`, on one engine:
/// the same answers as Rust's, alone and in a batch.
async fn typescript(lang: &str, file: &str) {
    if let Some(wasm) = prebuilt("ts-fetch", file) {
        let ts = answers(&wasm).await;
        check(lang, &ts);
        let rust = answers(&rust_guest()).await;
        assert_eq!(ts[..3], rust[..3], "{lang}");
        check_batches(lang, &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn typescript_quickjs() {
    typescript("typescript (QuickJS)", "ts_fetch.wasm").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typescript_starlingmonkey() {
    typescript(
        "typescript (StarlingMonkey)",
        "ts_fetch-starlingmonkey.wasm",
    )
    .await;
}
