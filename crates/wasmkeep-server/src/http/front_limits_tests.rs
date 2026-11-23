//! Limits on the public port's request heads and stalled writers, and on
//! the metrics listener.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A public port serving no app (the health check only), with `limits`.
async fn start(limits: ServeLimits) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with(
        listener,
        App::default(),
        std::future::pending(),
        limits,
    ));
    addr
}

/// Sends `head` in one write (tolerating a server that closes early) and
/// returns the status it answers, 0 if none.
async fn status_of(addr: std::net::SocketAddr, head: &str) -> u16 {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let _ = s.write_all(head.as_bytes()).await;
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut raw)).await;
    String::from_utf8_lossy(&raw)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn health_with(headers: &str) -> String {
    format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n{headers}\r\n")
}

#[tokio::test]
async fn a_request_head_over_the_cap_is_refused_with_431() {
    let addr = start(ServeLimits::default()).await;
    let under = format!("x-big: {}\r\n", "a".repeat(60 * 1024));
    assert_eq!(status_of(addr, &health_with(&under)).await, 200);
    let over = format!("x-big: {}\r\n", "a".repeat(MAX_REQUEST_HEAD_BYTES + 1024));
    assert_eq!(status_of(addr, &health_with(&over)).await, 431);
}

#[tokio::test]
async fn more_headers_than_the_cap_are_refused_with_431() {
    let addr = start(ServeLimits::default()).await;
    // Host and Connection count too.
    let some: String = (0..MAX_REQUEST_HEADERS - 2)
        .map(|i| format!("x-h{i}: v\r\n"))
        .collect();
    assert_eq!(status_of(addr, &health_with(&some)).await, 200);
    let many: String = (0..MAX_REQUEST_HEADERS - 1)
        .map(|i| format!("x-h{i}: v\r\n"))
        .collect();
    assert_eq!(status_of(addr, &health_with(&many)).await, 431);
}

/// Serves one public connection over an in-memory pipe whose buffer holds
/// `buffer` bytes, with a write-idle limit of `idle`.
fn serve_pipe(
    buffer: usize,
    idle: Duration,
) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<()>) {
    let (client, server) = tokio::io::duplex(buffer);
    let done = tokio::spawn(async move {
        let (_keep, stop) = tokio::sync::watch::channel(false);
        serve_connection(
            stall::WriteIdle::new(server, idle),
            Served::One(App::default()),
            None,
            Duration::from_secs(30),
            stop,
            "127.0.0.1:1".parse().unwrap(),
        )
        .await;
    });
    (client, done)
}

#[tokio::test]
async fn a_client_that_stops_reading_is_cut_after_the_write_idle_limit() {
    let (mut client, done) = serve_pipe(8, Duration::from_millis(300));
    let request = format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: t\r\n\r\n");
    client.write_all(request.as_bytes()).await.unwrap();
    // Reads nothing: the server's answer (more than the pipe holds) stalls.
    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .expect("a stalled connection was never cut")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_port_client_that_stops_reading_gives_its_slot_back() {
    let addr = start(ServeLimits {
        max_connections: 1,
        write_idle_timeout: Duration::from_millis(500),
        ..ServeLimits::default()
    })
    .await;
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let stalled = socket.connect(addr).await.unwrap();
    // Pipelines requests and never reads an answer: once the socket
    // buffers fill, the server's writes wait.
    let (_unread, mut writer) = stalled.into_split();
    let _pipelining = tokio::spawn(async move {
        let request = format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: t\r\n\r\n").repeat(256);
        while writer.write_all(request.as_bytes()).await.is_ok() {}
    });
    // The only slot is the stalled connection's until the limit cuts it.
    let served =
        tokio::time::timeout(Duration::from_secs(20), status_of(addr, &health_with(""))).await;
    assert_eq!(served, Ok(200), "the stalled connection kept its slot");
}

#[tokio::test(start_paused = true)]
async fn a_slow_reader_that_keeps_reading_is_not_cut() {
    let (mut client, done) = serve_pipe(8, Duration::from_millis(300));
    let request = format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    client.write_all(request.as_bytes()).await.unwrap();
    let started = tokio::time::Instant::now();
    let mut raw = Vec::new();
    let mut buf = [0u8; 8];
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        match client.read(&mut buf).await.unwrap() {
            0 => break,
            n => raw.extend_from_slice(&buf[..n]),
        }
    }
    let text = String::from_utf8_lossy(&raw);
    // Longer than the limit in all, yet it finished.
    assert!(started.elapsed() > Duration::from_millis(600), "{text}");
    assert!(
        text.starts_with("HTTP/1.1 200") && text.ends_with("ok\n"),
        "{text}"
    );
    done.await.unwrap();
}

#[tokio::test]
async fn an_idle_connection_is_not_cut_by_the_write_idle_limit() {
    let (mut client, done) = serve_pipe(1024, Duration::from_millis(100));
    // Idle for longer than the write-idle limit (under the header-read
    // timeout): nothing waits to be written, so nothing is cut.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let request = format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    client.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    client.read_to_end(&mut raw).await.unwrap();
    assert!(String::from_utf8_lossy(&raw).starts_with("HTTP/1.1 200"));
    done.await.unwrap();
}

/// Serves metrics on a loopback port; returns its address.
async fn start_metrics() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_metrics(listener, None, std::future::pending()));
    addr
}

fn scrape(host: Option<&str>) -> String {
    let host = host.map_or(String::new(), |h| format!("Host: {h}\r\n"));
    format!("GET /metrics HTTP/1.1\r\n{host}Connection: close\r\n\r\n")
}

#[tokio::test]
async fn the_metrics_page_answers_only_addresses_and_local_names() {
    let addr = start_metrics().await;
    let bound = addr.to_string();
    for host in [
        bound.as_str(),
        "127.0.0.1",
        "localhost",
        "localhost:9100",
        "LocalHost.",
        "prometheus.localhost",
        "[::1]:9100",
        "10.0.0.7:9100",
        "wasmkeep:9100",
    ] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 200, "{host}");
    }
    for host in [
        "rebound.example",
        "rebound.example:9100",
        "metrics.internal.",
        "localhost.example",
        "",
    ] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 421, "{host}");
    }
    assert_eq!(status_of(addr, &scrape(None)).await, 421);
    let twice = "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nHost: rebound.example\r\nConnection: close\r\n\r\n";
    assert_eq!(status_of(addr, twice).await, 421);
}
