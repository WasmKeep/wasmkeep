//! Every guest SDK's workflow client against the Rust workflow class
//! (ADR 0019): `examples/workflows/orders` and `examples/workflows/ledger`,
//! built here, serve a jobs `app` in each language (`examples/<lang>-jobs`)
//! whose `/orders` routes (as examples/workflows/app's) start, approve,
//! query and cancel orders over `actors.call`. Drives one order to
//! completion and checks an unknown id.
//!
//! The components are prebuilt (each example's README says how); a missing
//! one is skipped, or fails with `WASMKEEP_REQUIRE_LANGS=1` (CI).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// The Rust classes, built once per test binary: (class, wasm file stem).
const RUST: [(&str, &str); 2] = [
    ("orders", "workflows_orders"),
    ("ledger", "workflows_ledger"),
];

/// The `orders` class's timings: retries from 50 ms, three attempts.
const ORDERS_ENV: &str = "BACKOFF_MS=50\nATTEMPTS=3";

fn rust_wasm() -> &'static [(String, Vec<u8>)] {
    static BUILT: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    BUILT.get_or_init(|| {
        RUST.into_iter()
            .map(|(dir, name)| {
                let path = format!("../../examples/workflows/{dir}");
                (dir.to_owned(), guests::build_guest(&path, name))
            })
            .collect()
    })
}

/// A prebuilt app (`examples/<example>/<file>`), or `None` to skip.
fn prebuilt(example: &str, file: &str) -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(example)
        .join(file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!(
                "{example}: {} not built; skipping (see its README)",
                path.display()
            );
            None
        }
        Err(e) => panic!("{example}: {}: {e}", path.display()),
    }
}

/// A node with `orders` and `ledger`, and `app` as class `app`.
async fn start(data: &Path, app: &[u8]) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut classes: Vec<_> = rust_wasm()
        .iter()
        .map(|(class, bytes)| {
            let component = guests::compile(&runtime, bytes);
            (class.clone(), component, bytes.clone())
        })
        .collect();
    classes.push((
        "app".to_owned(),
        guests::compile(&runtime, app),
        app.to_vec(),
    ));
    let mut config = ActorConfig::new(data);
    let mut vars = ActorEnv::default();
    vars.extend("orders", wasmkeep_server::env::parse(ORDERS_ENV).unwrap());
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
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_owned()
    };
    (status, body)
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = s.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        s = &rest[n + 2..];
    }
    out
}

const APP: &str = "/actor/app/main";

/// Waits until order `id`'s status (as the app shows it) contains `want`;
/// returns it.
async fn until(addr: SocketAddr, engine: &str, id: &str, want: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body) = request(addr, "GET", &format!("{APP}/orders/{id}"), "").await;
        if body.contains(want) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{engine}: order {id} never had {want:?}; last:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An order started, approved and completed through the app's client; an
/// unknown id answers 404.
async fn scenario(engine: &str, app: &[u8]) {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), app).await;
    let order = format!("{APP}/orders/o-1");

    let (status, body) = request(
        addr,
        "POST",
        &order,
        "item=book;cooloff_ms=0;approve_within_ms=10000",
    )
    .await;
    assert_eq!(status, 200, "{engine}: {body}");
    assert!(body.starts_with("state: running\n"), "{engine}: {body}");
    let waiting = until(addr, engine, "o-1", "waiting_for: approved").await;
    assert!(waiting.contains("state: waiting\n"), "{engine}: {waiting}");
    assert!(waiting.contains("wake_at: "), "{engine}: {waiting}");

    let (status, body) = request(addr, "POST", &format!("{order}/approve"), "").await;
    assert_eq!(status, 200, "{engine}: {body}");
    let done = until(addr, engine, "o-1", "state: completed").await;
    assert!(done.contains("output: shipped book\n"), "{engine}: {done}");
    assert!(!done.contains("error: "), "{engine}: {done}");

    // A finished order takes no more events, and cancelling it changes
    // nothing.
    let (status, body) = request(addr, "POST", &format!("{order}/approve"), "").await;
    assert_eq!(
        (status, body.as_str()),
        (409, "the order has finished\n"),
        "{engine}"
    );
    let (status, body) = request(addr, "POST", &format!("{order}/cancel"), "").await;
    assert_eq!(status, 200, "{engine}: {body}");
    assert!(body.contains("state: completed\n"), "{engine}: {body}");

    // An unknown id: 404 for its status, an event and a cancel.
    for (method, path) in [
        ("GET", format!("{APP}/orders/nope")),
        ("POST", format!("{APP}/orders/nope/approve")),
        ("POST", format!("{APP}/orders/nope/cancel")),
    ] {
        let (status, body) = request(addr, method, &path, "").await;
        assert_eq!(
            (status, body.as_str()),
            (404, "no such order\n"),
            "{engine}: {method} {path}"
        );
    }

    // A cancelled order reports it.
    let other = format!("{APP}/orders/o-2");
    let (status, body) = request(addr, "POST", &other, "item=pen;cooloff_ms=60000").await;
    assert_eq!(status, 200, "{engine}: {body}");
    let (status, body) = request(addr, "POST", &format!("{other}/cancel"), "").await;
    assert_eq!(status, 200, "{engine}: {body}");
    assert!(body.contains("state: cancelled\n"), "{engine}: {body}");
}

/// C, with the C SDK (`./build.sh` in examples/c-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn c() {
    if let Some(app) = prebuilt("c-jobs", "jobs.wasm") {
        scenario("c", &app).await;
    }
}

/// Zig, with the Zig SDK over the C SDK (`./build.sh` in examples/zig-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn zig() {
    if let Some(app) = prebuilt("zig-jobs", "jobs.wasm") {
        scenario("zig", &app).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn go() {
    if let Some(app) = prebuilt("go-jobs", "jobs.wasm") {
        scenario("tinygo", &app).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn python() {
    if let Some(app) = prebuilt("py-jobs", "jobs.wasm") {
        scenario("python", &app).await;
    }
}
