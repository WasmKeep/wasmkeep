//! `actors.call` end to end, with tests/guests/test-actor.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

fn wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

async fn start(data: &std::path::Path) -> (SocketAddr, oneshot::Sender<()>) {
    start_with_limit(data, usize::MAX).await
}

async fn start_with_limit(
    data: &std::path::Path,
    max_resident: usize,
) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(wasm()).unwrap();
    let mut config = ActorConfig::new(data);
    config.wall_per_event = Duration::from_secs(5);
    config.mailbox.max_resident = max_resident;
    let svc = ActorService::new(
        runtime,
        config,
        vec![("t".into(), component, wasm().to_vec())],
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

async fn post(addr: SocketAddr, path: &str) -> (u16, String) {
    post_with(addr, path, "").await
}

async fn post_with(addr: SocketAddr, path: &str, headers: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n{headers}Connection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    // Single small chunk: "<hex>\r\n<data>\r\n0\r\n\r\n".
    let text = body
        .split("\r\n")
        .nth(1)
        .unwrap_or(body)
        .trim_end()
        .to_owned();
    (status, text)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_can_call_another() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let (status, body) = post(addr, "/actor/t/a/call?to=t/b&path=/fired").await;
    assert_eq!(status, 200);
    assert_eq!(body, "200:0");
    // The callee is a real, persistent actor.
    assert!(
        data.path()
            .join("state/default/default/actors/t/b.db")
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_capacity_refusal_is_an_http_response_and_keeps_the_caller_usable() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start_with_limit(data.path(), 1).await;
    // The caller occupies the only slot while waiting for this fresh callee.
    let response = post(addr, "/actor/t/a/call?to=t/b&path=/fired").await;
    assert_eq!(response, (200, "503:actor capacity unavailable".into()));
    assert!(
        !data
            .path()
            .join("state/default/default/actors/t/b.db")
            .exists()
    );
    assert_eq!(post(addr, "/actor/t/a/caller").await, (200, "none".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn only_actors_can_set_the_caller_header() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let (_, body) = post(addr, "/actor/t/a/call?to=t/b&path=/caller").await;
    assert_eq!(body, "200:default/t/a");
    // A client claiming to be an actor is not believed.
    let (status, body) = post_with(
        addr,
        "/actor/t/b/caller",
        "x-wasmkeep-caller: default/t/a\r\n",
    )
    .await;
    assert_eq!((status, body.as_str()), (200, "none"));
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_that_would_deadlock_are_refused() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    // a -> a
    assert_eq!(post(addr, "/actor/t/a/call?to=t/a").await.1, "reentrant");
    // a -> b -> a: b's call back to a is refused, b answers with that.
    let (_, body) = post(addr, "/actor/t/a/call?to=t/b&path=%2Fcall%3Fto%3Dt%2Fa").await;
    assert_eq!(body, "200:reentrant");
    // a -> b -> c is fine.
    let (_, body) = post(addr, "/actor/t/a/call?to=t/b&path=%2Fcall%3Fto%3Dt%2Fc").await;
    assert_eq!(body, "200:200:0");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_addresses_and_failing_callees_are_reported() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let body = post(addr, "/actor/t/a/call?to=nope/x").await.1;
    assert!(body.starts_with("invalid-address:"), "{body}");
    let body = post(addr, "/actor/t/a/call?to=just-a-name").await.1;
    assert!(body.starts_with("invalid-address:"), "{body}");
    // The callee traps: the caller gets an error, not a hang, and both
    // actors keep working.
    let body = post(addr, "/actor/t/a/call?to=t/b&path=%2Ftrap").await.1;
    assert!(body.starts_with("failed:"), "{body}");
    assert_eq!(
        post(addr, "/actor/t/a/call?to=t/b&path=/fired").await.1,
        "200:0"
    );
}
