//! examples/rate-limiter end to end: limits, windows closed by alarms, and
//! idle keys deleting themselves.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start(data: &std::path::Path) -> (SocketAddr, oneshot::Sender<()>) {
    let wasm = guests::build_guest("../../examples/rate-limiter", "rate_limiter");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let svc = ActorService::new(
        runtime,
        ActorConfig::new(data),
        vec![("limit".into(), component, wasm)],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

async fn status(addr: SocketAddr, method: &str, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    raw.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn limits_per_key_and_reopens_after_the_window() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let take = "/actor/limit/key-a?limit=3&window_ms=400";
    let opened = Instant::now();
    for _ in 0..3 {
        assert_eq!(status(addr, "POST", take).await, 200);
    }
    assert_eq!(status(addr, "POST", take).await, 429);
    // Another key has its own budget.
    assert_eq!(
        status(addr, "POST", "/actor/limit/key-b?limit=3&window_ms=400").await,
        200
    );

    // After the window's alarm, key-a may send again.
    let deadline = opened + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if status(addr, "POST", take).await == 200 {
            break;
        }
        assert!(Instant::now() < deadline, "window never reopened");
    }
    assert!(
        opened.elapsed() >= Duration::from_millis(400),
        "reopened early"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_key_deletes_itself() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    assert_eq!(
        status(addr, "POST", "/actor/limit/once?window_ms=200").await,
        200
    );
    let db = data
        .path()
        .join("state/default/default/actors/limit/once.db");
    assert!(db.exists());
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.exists() {
        assert!(Instant::now() < deadline, "idle key was not deleted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
