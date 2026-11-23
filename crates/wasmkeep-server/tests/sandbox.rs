//! The SQL sandbox from inside a real guest: attacks found by the
//! security review (docs/security.md#audits) fail safely, and the node
//! keeps serving.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start(data: &std::path::Path) -> SocketAddr {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.storage.max_statement_time = Duration::from_millis(500);
    let service = ActorService::new(runtime, config, vec![("t".into(), component, wasm)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    addr
}

/// The guest's answer to `POST /actor/t/<name>/sql<query>` with `sql`.
async fn sql(addr: SocketAddr, name: &str, query: &str, sql: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /actor/t/{name}/sql{query} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sql}",
        sql.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("")
        }
        _ => body,
    };
    body.trim_end().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn hostile_sql_fails_safely_and_the_node_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path()).await;

    // fts3_tokenizer with bound parameters: it used to leak a host pointer,
    // and with two arguments install one the host would then call.
    assert_eq!(
        sql(addr, "a", "", "SELECT fts3_tokenizer('simple')").await,
        "err:SqlErrorKind::NotAllowed"
    );
    assert_eq!(
        sql(addr, "a", "?blob=8", "SELECT fts3_tokenizer('evil', ?1)").await,
        "err:SqlErrorKind::NotAllowed"
    );

    // A statement that never ends is stopped at its deadline, and the
    // actor answers its next request at once.
    let started = Instant::now();
    let runaway =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";
    assert_eq!(
        sql(addr, "a", "", runaway).await,
        "err:SqlErrorKind::QuotaExceeded"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(sql(addr, "a", "", "SELECT 1").await, "ok:1");

    // A result too big to hold.
    let huge = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT zeroblob(1000000) FROM c";
    assert_eq!(
        sql(addr, "a", "", huge).await,
        "err:SqlErrorKind::QuotaExceeded"
    );

    // Shadowing the host's tables through the temp schema.
    assert_eq!(
        sql(
            addr,
            "a",
            "",
            "CREATE TEMP TABLE x(key TEXT PRIMARY KEY, value TEXT)"
        )
        .await,
        "err:SqlErrorKind::NotAllowed"
    );

    // Other actors on the node were never affected.
    assert_eq!(sql(addr, "b", "", "SELECT 1").await, "ok:1");
}

/// `POST /actor/t/<name><path>`: the status and the body.
async fn post(addr: SocketAddr, name: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /actor/t/{name}{path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {raw:?}"));
    (status, raw)
}

/// An actor that holds more host resources than its store allows, or
/// builds headers past the size allowed, fails that event and loses its
/// instance (`/memory` counts in the instance's memory: 1 is a new one);
/// the next event runs on a fresh instance. The table already holds the
/// event's own resources, so the passing case leaves a margin.
#[tokio::test(flavor = "multi_thread")]
async fn guest_held_resources_and_header_size_are_capped() {
    use wasmkeep_server::{MAX_GUEST_FIELDS_BYTES, MAX_GUEST_RESOURCES};
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path()).await;
    // The first line of the (chunked) body.
    let memory = |text: &str| {
        let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
        let mut lines = body.split("\r\n");
        let first = lines.next().unwrap_or("");
        let line = if usize::from_str_radix(first.trim(), 16).is_ok() {
            lines.next().unwrap_or("")
        } else {
            first
        };
        line.trim_end().to_owned()
    };

    assert_eq!(memory(&post(addr, "f", "/memory").await.1), "1");
    let under = MAX_GUEST_RESOURCES - 32;
    let (status, text) = post(addr, "f", &format!("/fields?n={under}&bytes=16")).await;
    assert_eq!(status, 200, "{text}");
    assert!(text.contains(&format!("held {under}\n")), "{text}");
    assert_eq!(memory(&post(addr, "f", "/memory").await.1), "2");

    let over = MAX_GUEST_RESOURCES + 1;
    let (status, text) = post(addr, "f", &format!("/fields?n={over}&bytes=16")).await;
    assert_eq!(status, 500, "{text}");
    assert_eq!(memory(&post(addr, "f", "/memory").await.1), "1");

    let (status, text) = post(addr, "f", "/fields?n=1&bytes=16384").await;
    assert_eq!(status, 200, "{text}");
    let path = format!("/fields?n=1&bytes={MAX_GUEST_FIELDS_BYTES}");
    let (status, text) = post(addr, "f", &path).await;
    assert_eq!(status, 500, "{text}");
    assert_eq!(memory(&post(addr, "f", "/memory").await.1), "1");

    // Other actors on the node were never affected.
    assert_eq!(sql(addr, "g", "", "SELECT 1").await, "ok:1");
}
