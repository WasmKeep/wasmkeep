//! Outbound HTTP from the guest SDKs that wrap `wasi:http/outgoing-handler`
//! themselves: the same requests through the Rust SDK's `http::fetch`
//! (tests/guests/test-actor) and the Go SDK's `Fetch`
//! (tests/guests/go-fetch, built by its `build.sh`), against a local
//! server, must give the same answers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, EgressPolicy};

#[path = "support/guests.rs"]
mod guests;

fn rust_guest() -> Vec<u8> {
    guests::build_guest("tests/guests/test-actor", "test_actor")
}

/// The Go guest, if built; with `WASMKEEP_REQUIRE_LANGS` set (CI) a
/// missing build fails instead.
fn go_guest() -> Option<Vec<u8>> {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/guests/go-fetch/go_fetch.wasm");
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
/// - `POST /echo`: `201 got N` (N bytes of body received)
/// - `GET /big`: 200 with 10,000 bytes of `y`
/// - `GET /short`: announces 100 bytes, sends 3 and closes
async fn server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _)) = listener.accept().await else {
                return;
            };
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
                let reply = match path.as_str() {
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
            });
        }
    });
    addr
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

#[tokio::test(flavor = "multi_thread")]
async fn rust() {
    check("rust", &answers(&rust_guest()).await);
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
