//! Actors' outbound HTTPS through the host's sender (`egress.rs`), against a
//! local TLS server: the certificate is checked against the host name and
//! the trusted roots, and nothing else gets through.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, EgressPolicy, egress};

#[path = "support/guests.rs"]
mod guests;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/support/tls")
        .join(name)
}

/// A TLS server for `localhost` (signed by the test CA) answering every
/// request with `200 secure`.
async fn tls_server() -> SocketAddr {
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(fixture("localhost.pem"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
    let key = PrivateKeyDer::from_pem_file(fixture("localhost.key")).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                // A failed handshake (an untrusting client) just ends here.
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut buf = vec![0; 4096];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    match tls.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got.extend_from_slice(&buf[..n]),
                    }
                }
                let _ = tls
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecure",
                    )
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    addr
}

async fn start(data: &Path, egress: EgressPolicy) -> SocketAddr {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.egress = egress;
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

/// What the guest's GET of `url` returned: `STATUS:BODY` or `error: ...`.
async fn fetch_from_guest(addr: SocketAddr, url: &str) -> String {
    fetch_from_guest_sending(addr, url, 0).await
}

/// Like `fetch_from_guest`, a POST with `send` bytes of body when not 0.
async fn fetch_from_guest_sending(addr: SocketAddr, url: &str, send: usize) -> String {
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
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
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
async fn https_is_verified_against_the_name_and_the_trusted_roots() {
    let server = tls_server().await;
    let port = server.port();
    let loopback = vec!["127.0.0.1".parse().unwrap()];
    let ca = egress::read_ca_file(&fixture("ca.pem")).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let trusting = start(
        &dir.path().join("a"),
        EgressPolicy::allowing(loopback.clone()).trusting(ca),
    )
    .await;
    let public_only = start(&dir.path().join("b"), EgressPolicy::allowing(loopback)).await;

    // Trusted CA, matching name: the request goes through, over TLS.
    assert_eq!(
        fetch_from_guest(trusting, &format!("https://localhost:{port}/")).await,
        "200:secure"
    );
    // The same server by address: the certificate names `localhost` only.
    let by_ip = fetch_from_guest(trusting, &format!("https://127.0.0.1:{port}/")).await;
    assert_eq!(by_ip, "error: ErrorCode::TlsCertificateError");
    // Without the private CA, the public roots do not vouch for it.
    let untrusted = fetch_from_guest(public_only, &format!("https://localhost:{port}/")).await;
    assert_eq!(untrusted, "error: ErrorCode::TlsCertificateError");
    // Plain HTTP to a TLS port gets no answer it could mistake for success.
    let plain = fetch_from_guest(trusting, &format!("http://localhost:{port}/")).await;
    assert!(!plain.starts_with("200"), "{plain}");
}

#[test]
fn ca_files_must_hold_a_certificate() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "not a certificate\n").unwrap();
    assert!(egress::read_ca_file(&empty).is_err());
    assert!(egress::read_ca_file(&dir.path().join("missing.pem")).is_err());
    assert_eq!(egress::read_ca_file(&fixture("ca.pem")).unwrap().len(), 1);
}

/// A request whose body is still being written when the request fails
/// (here, refused by the egress rules) reports why it failed, not only that
/// the body's stream closed: the cause of an agent test's flake was lost
/// that way.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_failing_while_its_body_is_sent_says_why() {
    let data = tempfile::tempdir().unwrap();
    let addr = start(data.path(), EgressPolicy::default()).await;
    // 127.0.0.1 is not a public address: refused.
    for send in [0, 10, 1 << 20] {
        let answer = fetch_from_guest_sending(addr, "http://127.0.0.1:9/x", send).await;
        eprintln!("send={send}: {answer}");
        assert!(answer.starts_with("error: "), "{answer}");
        assert!(
            !answer.contains("sending the body"),
            "send={send}: {answer}"
        );
    }
}

/// A plain HTTP server answering by path: `/short` promises 100 bytes,
/// sends 10 and closes; `/chunk-cut` stops in the middle of a chunk and
/// closes; `/whole` sends its 10 bytes; `/trailers` sends them chunked,
/// ending with a trailer; `/until-close` has no length and
/// ends by closing (a complete body); `/empty` and `/no-content` have no
/// body (Content-Length 0, and a 204); `/exact` sends exactly 4096 bytes
/// (the test guest's limit); `/big` sends 10,000 bytes.
async fn cutting_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&buf);
                let path = head.split_whitespace().nth(1).unwrap_or("/").to_owned();
                let response = match path.as_str() {
                    "/short" => {
                        "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789".to_owned()
                    }
                    "/chunk-cut" => {
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n64\r\n0123456789"
                            .to_owned()
                    }
                    "/trailers" => "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\n0123456789\r\n0\r\nx-checksum: 1\r\n\r\n".to_owned(),
                    "/whole" => {
                        "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789".to_owned()
                    }
                    "/until-close" => {
                        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n0123456789".to_owned()
                    }
                    "/empty" => "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_owned(),
                    "/no-content" => "HTTP/1.1 204 No Content\r\n\r\n".to_owned(),
                    "/exact" => format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n{}",
                        "y".repeat(4096)
                    ),
                    "/big" => format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\n{}",
                        "y".repeat(10_000)
                    ),
                    _ => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_owned(),
                };
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
                // Dropped: the connection closes, whatever was promised.
            });
        }
    });
    addr
}

/// A response whose body breaks before its end (the connection closes
/// short of its Content-Length, or inside a chunk) is an error naming the
/// incomplete body, not its status with the bytes that came: a workflow
/// step checking the status recorded a cut answer as a success.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_cut_short_is_an_error_not_a_partial_success() {
    let server = cutting_server().await;
    let data = tempfile::tempdir().unwrap();
    let addr = start(
        data.path(),
        EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]),
    )
    .await;
    for path in ["/short", "/chunk-cut"] {
        let answer = fetch_from_guest(addr, &format!("http://{server}{path}")).await;
        eprintln!("{path}: {answer}");
        assert!(answer.starts_with("error: "), "{path}: {answer}");
        assert!(answer.contains("incomplete body"), "{path}: {answer}");
    }
    // Complete bodies still succeed: a known length, chunks with trailers,
    // or the end of a response without a length.
    for path in ["/whole", "/trailers", "/until-close"] {
        let answer = fetch_from_guest(addr, &format!("http://{server}{path}")).await;
        assert_eq!(answer, "200:0123456789", "{path}");
    }
    // No body at all is a complete (empty) body, not a cut one.
    for (path, want) in [("/empty", "200:"), ("/no-content", "204:")] {
        let answer = fetch_from_guest(addr, &format!("http://{server}{path}")).await;
        assert_eq!(answer, want, "{path}");
    }
    // A body of exactly the limit (4096 here) is whole.
    let exact = fetch_from_guest(addr, &format!("http://{server}/exact")).await;
    assert_eq!(exact, format!("200:{}", "y".repeat(4096)));
    // The limit still keeps the start of a longer body.
    let big = fetch_from_guest(addr, &format!("http://{server}/big")).await;
    assert_eq!(big, format!("200:{}", "y".repeat(4096)));
}
