//! The counter contract every supported language must pass (see
//! docs/languages.md): the same black-box test, against the same HTTP API,
//! whatever language the actor is written in.
//!
//! Rust always runs (it is built here). Other languages run when their
//! component has been built (`examples/<lang>-counter`, see each README);
//! with `WASMKEEP_REQUIRE_LANGS=1` (CI) a missing one fails instead of being
//! skipped.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start(data: &Path, wasm: &[u8]) -> (SocketAddr, ActorService, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, wasm);
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_millis(100);
    let service = ActorService::new(
        runtime,
        config,
        vec![("counter".into(), component, wasm.to_vec())],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    (addr, service, stop)
}

async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    // Plain or one chunk.
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("")
        }
        _ => body,
    };
    (status, body.trim().to_owned())
}

/// The contract: GET reads, POST increments, names are separate actors,
/// other methods are refused, and the value survives hibernation and a
/// restart of the server.
async fn counter_contract(lang: &str, wasm: &[u8]) {
    let data = tempfile::tempdir().unwrap();
    let (addr, service, stop) = start(data.path(), wasm).await;

    assert_eq!(
        send(addr, "GET", "/actor/counter/a").await,
        (200, "0".into()),
        "{lang}: starts at 0"
    );
    assert_eq!(
        send(addr, "POST", "/actor/counter/a").await,
        (200, "1".into()),
        "{lang}: POST"
    );
    assert_eq!(
        send(addr, "POST", "/actor/counter/a").await,
        (200, "2".into()),
        "{lang}: POST"
    );
    assert_eq!(
        send(addr, "GET", "/actor/counter/b").await,
        (200, "0".into()),
        "{lang}: separate actors"
    );
    assert_eq!(
        send(addr, "PUT", "/actor/counter/a").await.0,
        405,
        "{lang}: other methods"
    );

    // Hibernation: the instance leaves memory; the database stays.
    let started = std::time::Instant::now();
    while service.resident() > 0 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{lang}: never hibernated"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        send(addr, "GET", "/actor/counter/a").await,
        (200, "2".into()),
        "{lang}: after hibernation"
    );

    // A restart: a new server on the same data directory.
    drop(stop);
    drop(service);
    let (addr, _service, _stop) = start(data.path(), wasm).await;
    assert_eq!(
        send(addr, "POST", "/actor/counter/a").await,
        (200, "3".into()),
        "{lang}: after a restart"
    );
}

/// A prebuilt component of another language, or `None` to skip.
fn prebuilt(lang: &str, relative: &str) -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!(
                "{lang}: {} not built; skipping (see examples/{lang}-counter)",
                path.display()
            );
            None
        }
        Err(e) => panic!("{lang}: {}: {e}", path.display()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rust() {
    counter_contract(
        "rust",
        &guests::build_guest("../../examples/counter", "counter"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
/// TypeScript on QuickJS, the default engine (`npm run build`).
async fn typescript() {
    if let Some(wasm) = prebuilt("ts", "examples/ts-counter/counter.wasm") {
        counter_contract("typescript", &wasm).await;
    }
}

/// The same TypeScript counter on StarlingMonkey
/// (`npm run build:starlingmonkey`).
#[tokio::test(flavor = "multi_thread")]
async fn typescript_starlingmonkey() {
    if let Some(wasm) = prebuilt("ts", "examples/ts-counter/counter-starlingmonkey.wasm") {
        counter_contract("typescript-starlingmonkey", &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn c() {
    if let Some(wasm) = prebuilt("c", "examples/c-counter/counter.wasm") {
        counter_contract("c", &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zig() {
    if let Some(wasm) = prebuilt("zig", "examples/zig-counter/counter.wasm") {
        counter_contract("zig", &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn python() {
    if let Some(wasm) = prebuilt("py", "examples/py-counter/counter.wasm") {
        counter_contract("python", &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn go() {
    if let Some(wasm) = prebuilt("go", "examples/go-counter/counter.wasm") {
        counter_contract("go", &wasm).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn moonbit() {
    if let Some(wasm) = prebuilt("moonbit", "examples/moonbit-counter/counter.wasm") {
        counter_contract("moonbit", &wasm).await;
    }
}
