//! Front-door limits from the security review (docs/security.md#audits):
//! slow or idle clients are cut off, bodies are capped, connections are
//! capped.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, MAX_BODY_BYTES, ServeLimits};

#[path = "support/guests.rs"]
mod guests;

async fn start(limits: ServeLimits) -> (SocketAddr, tempfile::TempDir) {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let service = ActorService::new(
        runtime,
        ActorConfig::new(dir.path()),
        vec![("t".into(), component, wasm)],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve_with(
        listener,
        service,
        async {
            let _ = stopped.await;
        },
        limits,
    ));
    (addr, dir)
}

/// Waits until the server closes `stream` (a read returns 0 or fails),
/// up to `within`; returns how long that took.
async fn closed_within(stream: &mut TcpStream, within: Duration) -> Option<Duration> {
    let started = Instant::now();
    let mut buf = [0u8; 4096];
    loop {
        match tokio::time::timeout(
            within.saturating_sub(started.elapsed()),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(0) | Err(_)) => return Some(started.elapsed()),
            Ok(Ok(_)) => {}
            Err(_) => return None,
        }
    }
}

fn status(raw: &[u8]) -> u16 {
    String::from_utf8_lossy(raw)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_and_idle_clients_are_cut_off() {
    let (addr, _dir) = start(ServeLimits {
        header_read_timeout: Duration::from_millis(400),
        ..ServeLimits::default()
    })
    .await;
    // Sends nothing at all.
    let mut silent = TcpStream::connect(addr).await.unwrap();
    // Trickles a header and never finishes it.
    let mut slow = TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /actor/t/a/inits HTTP/1.1\r\nHost: t\r\n")
        .await
        .unwrap();
    // Finishes a request, then idles on the kept-alive connection.
    let mut idle = TcpStream::connect(addr).await.unwrap();
    idle.write_all(b"GET /actor/t/a/inits HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    let n = idle.read(&mut buf).await.unwrap();
    assert_eq!(status(&buf[..n]), 200);

    for (what, stream) in [
        ("silent", &mut silent),
        ("slow", &mut slow),
        ("idle", &mut idle),
    ] {
        let took = closed_within(stream, Duration::from_secs(5)).await;
        assert!(took.is_some(), "{what} connection was never closed");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bodies_over_the_cap_are_refused() {
    let (addr, _dir) = start(ServeLimits::default()).await;

    // Declared too large: refused before anything reads it.
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /actor/t/a/body-len HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\n\r\n",
        MAX_BODY_BYTES + 1
    );
    s.write_all(head.as_bytes()).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(status(&buf[..n]), 413);

    // Chunked, with no length: the guest's read fails at the cap.
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"POST /actor/t/a/body-len HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let chunk = vec![b'x'; 1 << 20];
    let mut sent = 0u64;
    let writer = async {
        while sent <= MAX_BODY_BYTES + (1 << 20) {
            let head = format!("{:x}\r\n", chunk.len());
            if s.write_all(head.as_bytes()).await.is_err()
                || s.write_all(&chunk).await.is_err()
                || s.write_all(b"\r\n").await.is_err()
            {
                break;
            }
            sent += chunk.len() as u64;
        }
        let _ = s.write_all(b"0\r\n\r\n").await;
        s
    };
    let mut s = tokio::time::timeout(Duration::from_secs(30), writer)
        .await
        .unwrap();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut raw)).await;
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.contains("unreadable") || status(&raw) >= 400,
        "an oversized chunked body was read in full: {text}"
    );

    // Under the cap, as before.
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"POST /actor/t/a/body-len HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
        .await
        .unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw);
    assert!(text.contains("\r\n2\r\n5\n"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_beyond_the_cap_wait_for_a_free_slot() {
    let (addr, _dir) = start(ServeLimits {
        max_connections: 2,
        ..ServeLimits::default()
    })
    .await;
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /actor/t/a/inits HTTP/1.1\r\nHost: t\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 4096];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(status(&buf[..n]), 200);
        held.push(s); // kept alive: holds its slot
    }
    // The third connects (kernel backlog) but is not served yet.
    let mut third = TcpStream::connect(addr).await.unwrap();
    third
        .write_all(b"GET /actor/t/a/inits HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(500), third.read(&mut buf))
            .await
            .is_err(),
        "a connection over the cap was served"
    );
    // A slot frees up: it is served.
    drop(held.pop());
    let n = tokio::time::timeout(Duration::from_secs(5), third.read(&mut buf))
        .await
        .expect("never served after a slot freed")
        .unwrap();
    assert_eq!(status(&buf[..n]), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn new_names_cannot_exhaust_the_instance_pool() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    // A pool of 10 instances and actors that would stay resident for a
    // minute: before, the 11th new name failed (and every one after it,
    // for everyone). Now idle actors make room.
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 10,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.mailbox.idle_timeout = Duration::from_secs(60);
    let service = ActorService::new(runtime, config, vec![("t".into(), component, wasm)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop);
    tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    let get = |i: usize| async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let req =
                format!("GET /actor/t/n{i}/inits HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
            s.write_all(req.as_bytes()).await.unwrap();
            let mut raw = Vec::new();
            s.read_to_end(&mut raw).await.unwrap();
            raw
        })
        .await
        .expect("actor admission deadline exceeded")
    };
    for i in 0..40 {
        let mut raw = get(i).await;
        // The admission wait is 250 ms of real time: on a loaded machine an
        // eviction can take longer, and the answer is then the documented
        // retryable 503 (with Retry-After), which a client retries. Retry
        // that once; anything else (a 500 above all) still fails.
        if status(&raw) == 503 && String::from_utf8_lossy(&raw).contains("retry-after") {
            tokio::time::sleep(Duration::from_millis(100)).await;
            raw = get(i).await;
        }
        assert!(service.resident() <= 9, "{} resident", service.resident());
        let text = String::from_utf8_lossy(&raw);
        assert_eq!(status(&raw), 200, "actor n{i}: {text}");
        let (_, body) = text.split_once("\r\n\r\n").expect("HTTP response");
        assert!(
            body == "1\n" || body == "2\r\n1\n\r\n0\r\n\r\n",
            "actor n{i}: unexpected initialization count: {body:?}"
        );
    }
    assert!(service.resident() <= 9, "{} resident", service.resident());
}
