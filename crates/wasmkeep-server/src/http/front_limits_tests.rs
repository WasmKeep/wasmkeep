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

/// A head exactly at the client caps, as the wire counts it, passes; one
/// byte or one header more is refused.
#[tokio::test]
async fn the_head_caps_count_the_request_line_and_each_header() {
    let addr = start(ServeLimits::default()).await;
    let base = health_with("");
    // Host and Connection are 2 of the 100 headers; the final CRLF is not
    // counted.
    let fill = |n: usize, total: usize| {
        let named: usize = (0..n).map(|i| format!("x-h{i:02}: \r\n").len()).sum();
        let left = total - (base.len() - 2) - named;
        (0..n)
            .map(|i| {
                let v = left / n + usize::from(i < left % n);
                format!("x-h{i:02}: {}\r\n", "v".repeat(v))
            })
            .collect::<String>()
    };
    let at = fill(MAX_REQUEST_HEADERS - 2, MAX_REQUEST_HEAD_BYTES);
    assert_eq!(status_of(addr, &health_with(&at)).await, 200);
    let over = fill(MAX_REQUEST_HEADERS - 2, MAX_REQUEST_HEAD_BYTES + 1);
    assert_eq!(status_of(addr, &health_with(&over)).await, 431);
}

/// Node headers (`x-wasmkeep-*`, a hop's) are not the client's: they fit
/// in the hop reserve on top of a head at the client cap.
#[tokio::test]
async fn node_headers_fit_in_the_hop_reserve_on_top_of_the_client_cap() {
    let addr = start(ServeLimits::default()).await;
    let client = format!("x-big: {}\r\n", "a".repeat(63 * 1024));
    let node: String = (0..30)
        .map(|i| format!("x-wasmkeep-chain: {i}{}\r\n", "c".repeat(600)))
        .collect();
    let head = health_with(&format!("{client}{node}"));
    assert!(head.len() > MAX_REQUEST_HEAD_BYTES + 16 * 1024);
    assert_eq!(status_of(addr, &head).await, 200);
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

/// A public port serving one static asset of `size` bytes at `/big.bin`.
async fn start_with_asset(size: usize, limits: ServeLimits) -> std::net::SocketAddr {
    use crate::assets::{Content, Deployed, File, Index, digest};
    use object_store::ObjectStoreExt;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let content = vec![b'x'; size];
    let hash = digest(&content);
    store
        .put(
            &format!("apps/default/default/_assets/content/{hash}").into(),
            content.into(),
        )
        .await
        .unwrap();
    let mut index = Index::default();
    index.files.insert(
        "big.bin".into(),
        File {
            content_type: "application/octet-stream".into(),
            identity: Content {
                digest: hash,
                size: size as u64,
            },
            br: None,
            gzip: None,
        },
    );
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::ActorConfig::new(dir.path());
    std::mem::forget(dir);
    config.assets = Some(Deployed {
        index: Arc::new(index),
        previous: None,
        app: crate::apps::AppRef::new(store, "", "default", "default"),
    });
    let service = ActorService::new(
        wasmkeep_runtime::Runtime::new(&Default::default()).unwrap(),
        config,
        vec![],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with(
        listener,
        App::from(service),
        std::future::pending(),
        limits,
    ));
    addr
}

/// On a real socket with the kernel's default buffers, a reader that keeps
/// taking bytes slowly is not cut, though the socket reports room to
/// write only once a third of its (autotuned, several MiB) send buffer is
/// free: at the limit, the bytes it takes show the reader is there. Here
/// 16 KiB per 100 ms against a 1 s limit, for 6 s past the point where
/// the buffers are full (60 times slower than a minute's limit asks, the
/// same as about 2.7 KiB/s; loopback acknowledges in 64 KiB segments).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_steady_slow_reader_on_a_real_socket_is_not_cut() {
    let size = 16 << 20;
    let addr = start_with_asset(
        size,
        ServeLimits {
            write_idle_timeout: Duration::from_secs(1),
            ..ServeLimits::default()
        },
    )
    .await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /big.bin HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let mut buf = vec![0u8; 32 * 1024];
    let mut taken = 0usize;
    while started.elapsed() < Duration::from_secs(8) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut chunk = 0;
        while chunk < buf.len() {
            match s.read(&mut buf[chunk..]).await {
                Ok(0) | Err(_) => panic!("the slow reader was cut after {taken} bytes"),
                Ok(n) => chunk += n,
            }
        }
        taken += chunk;
    }
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

/// Serves metrics bound to `bind`, answering the names in `hosts` too;
/// returns an address that reaches it.
async fn start_metrics(bind: &str, hosts: &[&str]) -> std::net::SocketAddr {
    let listener = TcpListener::bind(bind).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hosts: Arc<[String]> = hosts.iter().map(|h| (*h).to_owned()).collect();
    tokio::spawn(serve_metrics_of(
        listener,
        Metered::One(None),
        hosts,
        std::future::pending(),
    ));
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

fn scrape(host: Option<&str>) -> String {
    let host = host.map_or(String::new(), |h| format!("Host: {h}\r\n"));
    format!("GET /metrics HTTP/1.1\r\n{host}Connection: close\r\n\r\n")
}

/// Answered on every bind: IP addresses (any, with any port), `localhost`
/// and names under it. Refused on every bind: a missing or doubled Host,
/// one with user information, and a dotted name nobody configured.
async fn assert_common(addr: std::net::SocketAddr) {
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
    ] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 200, "{host}");
    }
    for host in [
        "rebound.example",
        "rebound.example:9100",
        "metrics.internal.",
        "localhost.example",
        "user@127.0.0.1",
        "a@localhost:9100",
        "",
    ] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 421, "{host}");
    }
    assert_eq!(status_of(addr, &scrape(None)).await, 421);
    let twice = "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nHost: rebound.example\r\nConnection: close\r\n\r\n";
    assert_eq!(status_of(addr, twice).await, 421);
}

#[tokio::test]
async fn a_loopback_metrics_bind_answers_only_addresses_and_localhost() {
    let addr = start_metrics("127.0.0.1:0", &[]).await;
    assert_common(addr).await;
    // A single-label name could be pointed at loopback by whoever answers
    // for it on the network.
    assert_eq!(status_of(addr, &scrape(Some("wasmkeep:9100"))).await, 421);
}

#[tokio::test]
async fn a_metrics_bind_on_all_addresses_also_answers_single_label_names() {
    let addr = start_metrics("0.0.0.0:0", &[]).await;
    assert_common(addr).await;
    assert_eq!(status_of(addr, &scrape(Some("wasmkeep:9100"))).await, 200);
}

#[tokio::test]
async fn configured_metrics_names_are_answered_on_any_bind() {
    let addr = start_metrics("127.0.0.1:0", &["metrics.internal", "node1"]).await;
    for host in ["metrics.internal", "Metrics.Internal.:9100", "node1"] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 200, "{host}");
    }
    for host in ["other.internal", "node2", "x.metrics.internal"] {
        assert_eq!(status_of(addr, &scrape(Some(host))).await, 421, "{host}");
    }
}
