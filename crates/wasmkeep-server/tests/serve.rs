//! End to end: a real wasi:http component (examples/hello) behind the server.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{Error, HttpComponent};

#[path = "support/guests.rs"]
mod guests;
use guests::build_guest;

/// examples/hello, built once per test binary.
fn hello_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| build_guest("../../examples/hello", "hello"))
}

/// tests/guests/misbehave, built once per test binary.
fn misbehave_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| build_guest("tests/guests/misbehave", "misbehave"))
}

fn runtime() -> Runtime {
    Runtime::new(&RuntimeConfig {
        max_instances: 64,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Starts a server on an ephemeral port; dropping the sender stops it.
async fn start(component: HttpComponent) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, component, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

/// Minimal HTTP/1.1 client: returns (status code, full response text).
async fn request(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await.unwrap();
    let status = buf
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {buf:?}"));
    (status, buf)
}

fn hello_component() -> HttpComponent {
    let runtime = runtime();
    let component = runtime.compile(hello_wasm()).unwrap();
    HttpComponent::new(runtime, &component).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_health_endpoint_answers_without_reaching_the_component() {
    // For load balancers: a fixed path, answered by the server itself.
    let (addr, _stop) = start(hello_component()).await;
    let (status, text) = request(addr, "GET", "/_wasmkeep/health").await;
    assert_eq!(status, 200, "{text}");
    assert!(text.ends_with("ok\n"), "{text}");
    assert!(
        !text.contains("GET /_wasmkeep"),
        "reached the component: {text}"
    );
    let (status, _) = request(addr, "HEAD", "/_wasmkeep/health").await;
    assert_eq!(status, 200);
    // The prefix is reserved: nothing under it reaches the component.
    let (status, text) = request(addr, "GET", "/_wasmkeep/other").await;
    assert_eq!(status, 404, "{text}");

    // An app without a stateless component answers too.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_stop2, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(
        listener,
        wasmkeep_server::App::default(),
        async {
            let _ = stopped.await;
        },
    ));
    let (status, text) = request(addr, "GET", "/_wasmkeep/health").await;
    assert_eq!(status, 200, "{text}");
    let (status, _) = request(addr, "GET", "/elsewhere").await;
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_root_is_served_by_the_component() {
    let (addr, _stop) = start(hello_component()).await;
    let (status, text) = request(addr, "GET", "/").await;
    assert_eq!(status, 200, "{text}");
    assert!(
        text.contains("Hello from WasmKeep! You sent GET /\n"),
        "{text}"
    );
    assert!(
        text.to_ascii_lowercase()
            .contains("content-type: text/plain"),
        "{text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn method_path_and_query_reach_the_guest() {
    let (addr, _stop) = start(hello_component()).await;
    let (status, text) = request(addr, "POST", "/actor/room-1?x=1").await;
    assert_eq!(status, 200, "{text}");
    assert!(text.contains("You sent POST /actor/room-1?x=1"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_requests_each_get_an_instance() {
    let (addr, _stop) = start(hello_component()).await;
    let tasks: Vec<_> = (0..50)
        .map(|i| tokio::spawn(async move { request(addr, "GET", &format!("/n/{i}")).await }))
        .collect();
    for (i, task) in tasks.into_iter().enumerate() {
        let (status, text) = task.await.unwrap();
        assert_eq!(status, 200, "{text}");
        assert!(text.contains(&format!("GET /n/{i}")), "{text}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exhausted_pool_returns_500_and_server_recovers() {
    // One component slot, held by a store outside the server: requests
    // cannot instantiate until it is released.
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(hello_wasm()).unwrap();
    let http = HttpComponent::new(runtime.clone(), &component).unwrap();
    let (addr, _stop) = start(http).await;

    let blocker = runtime
        .compile(&wat::parse_str("(component)").unwrap())
        .unwrap();
    let linker = wasmtime::component::Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &blocker).unwrap();
    let mut held = runtime.new_store(());
    pre.instantiate(&mut held).unwrap();

    let (status, _) = request(addr, "GET", "/").await;
    assert_eq!(status, 500);

    drop(held);
    let (status, text) = request(addr, "GET", "/").await;
    assert_eq!(status, 200, "{text}");
}

#[test]
fn rejects_a_component_that_is_not_an_http_proxy() {
    let runtime = runtime();
    let wasm = wat::parse_str("(component)").unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let err = HttpComponent::new(runtime, &component).unwrap_err();
    assert!(matches!(err, Error::NotAnHttpComponent(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_stops_accepting_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(listener, hello_component(), async {
        let _ = stopped.await;
    }));
    assert_eq!(request(addr, "GET", "/").await.0, 200);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert!(TcpStream::connect(addr).await.is_err());
}

fn misbehave_component(cpu_budget: Duration) -> HttpComponent {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        cpu_budget,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(misbehave_wasm()).unwrap();
    HttpComponent::new(runtime, &component).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn infinite_loop_is_killed_and_server_keeps_serving() {
    let (addr, _stop) = start(misbehave_component(Duration::from_millis(50))).await;

    let started = Instant::now();
    let (status, text) = request(addr, "GET", "/spin").await;
    let elapsed = started.elapsed();
    assert_eq!(status, 500, "{text}");
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");

    let (status, text) = request(addr, "GET", "/").await;
    assert_eq!(status, 200, "{text}");
    assert!(text.contains("ok\n"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn guest_panic_returns_500_and_server_keeps_serving() {
    let (addr, _stop) = start(misbehave_component(Duration::from_millis(50))).await;
    let (status, text) = request(addr, "GET", "/panic").await;
    assert_eq!(status, 500, "{text}");
    let (status, _) = request(addr, "GET", "/").await;
    assert_eq!(status, 200);
}

/// Sends a POST (`framing`: the header that frames its body) and its body
/// in two parts with a pause, so the component's answer is on its way
/// while most of the body is still to send, and reads the reply. A write
/// or a read the server cut short is the failure looked for, so it is
/// reported, not unwrapped.
async fn post_in_two_parts(
    addr: SocketAddr,
    framing: &str,
    first: &[u8],
    rest: &[u8],
) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head =
        format!("POST /upload HTTP/1.1\r\nHost: test\r\n{framing}\r\nConnection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    let wrote = async {
        stream.write_all(first).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(rest).await
    }
    .await;
    let mut raw = Vec::new();
    let read = stream.read_to_end(&mut raw).await;
    let raw = String::from_utf8_lossy(&raw).into_owned();
    if let Err(e) = wrote {
        return Err(format!("the body was cut short ({e}); answer: {raw:?}"));
    }
    if let Err(e) = read {
        return Err(format!("the answer was cut short ({e}): {raw:?}"));
    }
    Ok(raw)
}

/// examples/hello answers without reading the request body. A client
/// still sending a large one must be able to finish and read the answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_body_the_component_does_not_read_still_gets_its_answer() {
    let (addr, _stop) = start(hello_component()).await;
    for len in [256 << 10, 4 << 20, 8 << 20] {
        for attempt in 0..3 {
            let what = format!("{len} bytes, attempt {attempt}");
            let body = vec![b'x'; len];
            let (first, rest) = body.split_at(64 << 10);
            let framing = format!("Content-Length: {len}");
            let text = post_in_two_parts(addr, &framing, first, rest)
                .await
                .unwrap_or_else(|e| panic!("{what}: {e}"));
            assert!(text.starts_with("HTTP/1.1 200 "), "{what}: {text}");
            assert!(text.contains("You sent POST /upload\n"), "{what}: {text}");
        }
    }
}

/// The same for a body sent in chunks, whose length the server does not
/// know beforehand.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_chunked_body_the_component_does_not_read_still_gets_its_answer() {
    let (addr, _stop) = start(hello_component()).await;
    // 64 chunks of 64 KiB (4 MiB), then the last, empty one.
    let mut chunk = format!("{:x}\r\n", 64 << 10).into_bytes();
    chunk.extend(std::iter::repeat_n(b'x', 64 << 10));
    chunk.extend(b"\r\n");
    let mut rest = chunk.repeat(63);
    rest.extend(b"0\r\n\r\n");
    for attempt in 0..3 {
        let text = post_in_two_parts(addr, "Transfer-Encoding: chunked", &chunk, &rest)
            .await
            .unwrap_or_else(|e| panic!("attempt {attempt}: {e}"));
        assert!(
            text.starts_with("HTTP/1.1 200 "),
            "attempt {attempt}: {text}"
        );
        assert!(
            text.contains("You sent POST /upload\n"),
            "attempt {attempt}: {text}"
        );
    }
}
