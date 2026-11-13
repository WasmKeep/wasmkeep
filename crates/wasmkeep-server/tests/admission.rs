//! Actor admission returns retryable HTTP responses before opening a database.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start(data: &Path, max_resident: usize) -> (SocketAddr, oneshot::Sender<()>) {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    let wasm = WASM.get_or_init(|| guests::build_guest("../../examples/counter", "counter"));
    start_with_wasm(data, max_resident, wasm).await
}

async fn start_with_wasm(
    data: &Path,
    max_resident: usize,
    wasm: &[u8],
) -> (SocketAddr, oneshot::Sender<()>) {
    start_ruled(
        data,
        max_resident,
        wasm,
        wasmkeep_server::AccessRules::default(),
    )
    .await
}

async fn start_ruled(
    data: &Path,
    max_resident: usize,
    wasm: &[u8],
    access: wasmkeep_server::AccessRules,
) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 64,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.max_resident = max_resident;
    config.mailbox.idle_timeout = Duration::from_secs(600);
    config.access = access;
    let service = ActorService::new(
        runtime,
        config,
        vec![("counter".into(), component, wasm.to_vec())],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

#[derive(Debug)]
struct Reply {
    status: u16,
    retry_after: Option<String>,
    body: String,
}

async fn request(addr: SocketAddr, method: &str, name: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} /actor/counter/{name} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_string(&mut raw))
        .await
        .expect("HTTP request timed out")
        .unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").expect("HTTP response headers");
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    let retry_after = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("retry-after")
            .then(|| value.trim().to_owned())
    });
    let chunked = headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding") && value.trim() == "chunked"
        })
    });
    let mut decoded = String::new();
    if chunked {
        let mut rest = body;
        loop {
            let (length, tail) = rest.split_once("\r\n").expect("chunk header");
            let length = usize::from_str_radix(length, 16).unwrap();
            if length == 0 {
                break;
            }
            decoded.push_str(&tail[..length]);
            rest = &tail[length + 2..];
        }
    } else {
        decoded.push_str(body);
    }
    Reply {
        status,
        retry_after,
        body: decoded,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_resident_capacity_returns_retryable_503_without_creating_a_database() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), 0).await;
    let reply = request(addr, "POST", "rejected").await;
    assert_eq!(reply.status, 503, "{reply:?}");
    assert_eq!(reply.retry_after.as_deref(), Some("1"), "{reply:?}");
    assert!(
        !data
            .path()
            .join("state/default/default/actors/counter/rejected.db")
            .exists(),
        "a rejected admission created an actor database"
    );
}

async fn value(addr: SocketAddr, method: &str, name: &str) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for _ in 0..1000 {
        let reply = request(addr, method, name).await;
        match reply.status {
            200 => return reply.body.trim().parse().expect("counter value"),
            503 => {
                assert_eq!(reply.retry_after.as_deref(), Some("1"), "{reply:?}");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "admission did not recover"
                );
                // Exercise the admission race sooner than the advisory
                // Retry-After delay; only explicit refusals are retried.
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            _ => panic!("expected success or a retryable admission refusal: {reply:?}"),
        }
    }
    panic!("bounded admission retries exhausted")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_resident_cap_preserves_state_across_repeated_evictions() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), 2).await;
    for expected in 1..=2 {
        for actor in 0..12 {
            assert_eq!(
                value(addr, "POST", &format!("a{actor}")).await,
                expected,
                "actor {actor} after repeated eviction"
            );
        }
    }
    for actor in 0..12 {
        assert_eq!(value(addr, "GET", &format!("a{actor}")).await, 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_resident_capacity_refuses_websocket_upgrade_before_switching_protocols() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), 0).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"GET /actor/counter/rejected HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !raw.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "closed before response headers");
            raw.extend_from_slice(&chunk[..count]);
        }
    })
    .await
    .expect("WebSocket upgrade timed out");
    let reply = String::from_utf8(raw).unwrap();
    assert!(reply.starts_with("HTTP/1.1 503"), "{reply}");
    assert!(
        reply
            .lines()
            .any(|line| line.eq_ignore_ascii_case("retry-after: 1")),
        "{reply}"
    );
    assert!(
        !data
            .path()
            .join("state/default/default/actors/counter/rejected.db")
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_due_alarm_is_delivered_while_new_names_keep_the_node_at_capacity() {
    // Two slots, both kept busy by a steady stream of new names, each of
    // which waits for a slot. An alarm that comes due must get one too, not
    // keep losing the slot its eviction frees to the next new name.
    let test_actor = guests::build_guest("tests/guests/test-actor", "test_actor");
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start_with_wasm(data.path(), 2, &test_actor).await;
    let r = request(addr, "POST", "timer/alarm?in=500").await;
    assert_eq!(r.status, 200, "{}", r.body);

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let traffic: Vec<_> = (0..6)
        .map(|t| {
            let stop = std::sync::Arc::clone(&stop);
            tokio::spawn(async move {
                let mut i = 0;
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let _ = request(addr, "POST", &format!("cold-{t}-{i}/fired")).await;
                    i += 1;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(4)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    for t in traffic {
        t.await.unwrap();
    }
    // `/fired` counts successful on-alarm runs, read before anything this
    // request does could arm the alarm again.
    let r = request(addr, "GET", "timer/fired").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the alarm did not run within 3.5 s of being due"
    );
}

/// The status a WebSocket upgrade to `counter/<name>` gets.
async fn upgrade_status(addr: SocketAddr, name: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /actor/counter/{name} HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !raw.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "closed before response headers");
            raw.extend_from_slice(&chunk[..count]);
        }
    })
    .await
    .expect("WebSocket upgrade timed out");
    let reply = String::from_utf8(raw).unwrap();
    reply.split_whitespace().nth(1).unwrap().parse().unwrap()
}

/// `wasmkeep_admission_refused_total{reason="..."}` on the metrics page
/// (this process's: nextest runs each test alone).
fn refused(reason: &str) -> u64 {
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    let series = format!("wasmkeep_admission_refused_total{{reason=\"{reason}\"}} ");
    page.lines()
        .find_map(|l| l.strip_prefix(&series))
        .unwrap_or_else(|| panic!("no {series}in:\n{page}"))
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn admission_refusals_are_counted_by_reason() {
    // Every reason is on the page from the start, at 0.
    for reason in [
        "access",
        "capacity",
        "overloaded",
        "unavailable",
        "wrong_app",
    ] {
        assert_eq!(refused(reason), 0, "{reason}");
    }
    let data = tempfile::tempdir().unwrap();
    let (full, _stop) = start(data.path(), 0).await;
    assert_eq!(request(full, "POST", "a").await.status, 503);
    assert_eq!(request(full, "GET", "b").await.status, 503);
    assert_eq!(upgrade_status(full, "c").await, 503);
    assert_eq!(refused("capacity"), 3);

    // An internal class refuses outside requests; an `existing` one
    // refuses to create a name from outside.
    let wasm = guests::build_guest("../../examples/counter", "counter");
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("counter", wasmkeep_server::Access::Internal);
    let other = tempfile::tempdir().unwrap();
    let (internal, _stop2) = start_ruled(other.path(), 8, &wasm, rules).await;
    assert_eq!(request(internal, "POST", "a").await.status, 403);
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("counter", wasmkeep_server::Access::Existing);
    let third = tempfile::tempdir().unwrap();
    let (existing, _stop3) = start_ruled(third.path(), 8, &wasm, rules).await;
    assert_eq!(request(existing, "POST", "nobody").await.status, 404);
    assert_eq!(upgrade_status(internal, "a").await, 403);
    assert_eq!(upgrade_status(existing, "nobody").await, 404);
    assert_eq!(refused("access"), 4);
    assert_eq!(refused("capacity"), 3, "nothing else counted as capacity");
    assert_eq!(refused("overloaded") + refused("unavailable"), 0);
}
