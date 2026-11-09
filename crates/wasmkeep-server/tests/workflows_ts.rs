//! examples/ts-workflows end to end: the TypeScript workflow runner (ADR
//! 0019), on QuickJS and on StarlingMonkey. Its steps call a ledger over
//! HTTP with `http.fetch` (a mock server here, reached through the egress
//! allow-list), and the Rust `app` class of examples/workflows starts and
//! follows it: the two SDKs speak the same protocol.
//!
//! Needs the example built (`npm run build` and `npm run
//! build:starlingmonkey` in examples/ts-workflows); skipped otherwise, or
//! failed with `WASMKEEP_REQUIRE_LANGS=1` (CI).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService, EgressPolicy};

#[path = "support/guests.rs"]
mod guests;

fn prebuilt(file: &str) -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ts-workflows")
        .join(file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!("{} not built; skipping (see its README)", path.display());
            None
        }
        Err(e) => panic!("{}: {e}", path.display()),
    }
}

/// The ledger: records `(what, key)` per call; fails the next calls whose
/// `what` starts with a prefix, as told.
#[derive(Default)]
struct Ledger {
    entries: Vec<(String, String)>,
    failing: Vec<(String, u32)>,
    /// Body lengths received, in order.
    bodies: Vec<usize>,
}

type Shared = Arc<Mutex<Ledger>>;

fn param(query: &str, name: &str) -> String {
    let raw = query
        .split('&')
        .find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_default();
    // Percent-decoding of what the example's encodeURIComponent writes.
    let bytes = raw.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(u8::from_str_radix(&raw[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

async fn ledger_server(ledger: Shared) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let end = loop {
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                };
                let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                while buf.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                ledger.lock().unwrap().bodies.push(buf.len() - end);
                let target = head.split_whitespace().nth(1).unwrap_or("/").to_owned();
                let query = target.split_once('?').map_or("", |(_, q)| q).to_owned();
                let (what, key) = (param(&query, "what"), param(&query, "key"));
                let (status, body) = {
                    let mut l = ledger.lock().unwrap();
                    let failing = l
                        .failing
                        .iter_mut()
                        .find(|(prefix, times)| *times > 0 && what.starts_with(prefix.as_str()));
                    if let Some((_, times)) = failing {
                        *times -= 1;
                        (503, format!("{what} failed on purpose"))
                    } else {
                        l.entries.push((what.clone(), key.clone()));
                        let n = l.entries.iter().filter(|(_, k)| *k == key).count();
                        (200, format!("{what}#{n}"))
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    addr
}

/// A node with the TypeScript `orders` (`orders` wasm) and the Rust `app`.
async fn start(
    data: &Path,
    orders: &[u8],
    ledger: SocketAddr,
    egress: EgressPolicy,
) -> (SocketAddr, oneshot::Sender<()>) {
    // A generous CPU budget: these tests are about workflow steps and the
    // egress refusal, and a loaded machine must not cut a step that sends a
    // 64 KiB body (the budget has tests of its own).
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        cpu_budget: Duration::from_millis(500),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let app = guests::build_guest("../../examples/workflows/app", "workflows_app");
    let classes = vec![
        (
            "orders".to_owned(),
            guests::compile(&runtime, orders),
            orders.to_vec(),
        ),
        ("app".to_owned(), guests::compile(&runtime, &app), app),
    ];
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_millis(100);
    config.egress = egress;
    let mut vars = ActorEnv::default();
    vars.extend(
        "orders",
        wasmkeep_server::env::parse(&format!(
            "LEDGER_URL=http://{ledger}\nBACKOFF_MS=50\nATTEMPTS=3"
        ))
        .unwrap(),
    );
    config.env = vars;
    let svc = ActorService::new(runtime, config, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, body.to_owned())
}

const APP: &str = "/actor/app/main";

async fn until(addr: SocketAddr, id: &str, want: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body) = request(addr, "GET", &format!("{APP}/orders/{id}"), "").await;
        if body.contains(want) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "order {id} never had {want:?}; last:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn local() -> EgressPolicy {
    EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()])
}

/// The order through both paths (approved, and timed out), with a failing
/// charge retried, on one engine's build.
async fn scenario(file: &str) {
    let Some(orders) = prebuilt(file) else {
        return;
    };
    let ledger = Shared::default();
    let ledger_addr = ledger_server(Arc::clone(&ledger)).await;
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), &orders, ledger_addr, local()).await;

    // Approved, with two charge failures retried. The input is over 4 KiB,
    // and the reservation sends it as its body: written in chunks.
    ledger.lock().unwrap().failing.push(("charge".into(), 2));
    let input = format!(
        "item=book;cooloff_ms=300;approve_within_ms=10000;note={}",
        "n".repeat(6_000)
    );
    let (status, body) = request(addr, "POST", &format!("{APP}/orders/o1"), &input).await;
    assert_eq!(status, 200, "{body}");
    until(addr, "o1", "waiting_for: approved").await;
    request(addr, "POST", &format!("{APP}/orders/o1/approve"), "").await;
    let done = until(addr, "o1", "state: completed").await;
    assert!(done.contains("output: shipped book"), "{done}");
    assert!(done.contains("positions: 6"), "{done}");
    assert_eq!(ledger.lock().unwrap().bodies.first(), Some(&input.len()));
    let entries = ledger.lock().unwrap().entries.clone();
    // A step may run more than once (the guide says so: a step cut after
    // its write sends it again), with the same idempotency key: the
    // ledger counts each key once, as a real one would. Under load a
    // `notify:warehouse` was sent twice, and the exact list failed.
    let mut seen = std::collections::HashSet::new();
    let once: Vec<&(String, String)> = entries
        .iter()
        .filter(|(_, k)| seen.insert(k.as_str()))
        .collect();
    let whats: Vec<&str> = once.iter().map(|(w, _)| w.as_str()).collect();
    assert_eq!(
        whats,
        [
            "reserve:book",
            "charge:book",
            "notify:buyer",
            "notify:warehouse"
        ],
        "{entries:?}"
    );
    // Sent again only as itself: a key never names another step.
    assert!(
        entries
            .iter()
            .all(|(w, k)| once.iter().any(|(w2, k2)| k2 == k && w2 == w)),
        "{entries:?}"
    );

    // Never approved: released, failed, and no more events taken.
    request(
        addr,
        "POST",
        &format!("{APP}/orders/late"),
        "item=lamp;cooloff_ms=0;approve_within_ms=300",
    )
    .await;
    let failed = until(addr, "late", "state: failed").await;
    assert!(failed.contains("reserve:lamp#1 was released"), "{failed}");
    assert_eq!(
        request(addr, "POST", &format!("{APP}/orders/late/approve"), "")
            .await
            .0,
        409
    );
}

/// A step whose request is refused while its body is being sent fails
/// with the refusal, not with the body's closed stream.
async fn refused_while_sending(file: &str) {
    let Some(orders) = prebuilt(file) else {
        return;
    };
    let ledger = Shared::default();
    let ledger_addr = ledger_server(Arc::clone(&ledger)).await;
    let data = tempfile::tempdir().unwrap();
    // The default rules: the ledger, on 127.0.0.1, is refused.
    let (addr, _stop) = start(data.path(), &orders, ledger_addr, EgressPolicy::default()).await;
    let input = format!("item=book;note={}", "n".repeat(64 << 10));
    let (status, body) = request(addr, "POST", &format!("{APP}/orders/big"), &input).await;
    assert_eq!(status, 200, "{body}");
    let failed = until(addr, "big", "state: failed").await;
    assert!(failed.contains("reserve%3Abook"), "{failed}");
    assert!(failed.contains("destination-IP-prohibited"), "{failed}");
    assert!(!failed.contains("sending the body"), "{failed}");
    assert!(ledger.lock().unwrap().entries.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn quickjs_refused_while_sending() {
    refused_while_sending("orders.wasm").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn starlingmonkey_refused_while_sending() {
    refused_while_sending("orders-starlingmonkey.wasm").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn quickjs() {
    scenario("orders.wasm").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn starlingmonkey() {
    scenario("orders-starlingmonkey.wasm").await;
}
