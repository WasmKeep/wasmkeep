//! Actors end to end: examples/counter behind `/actor/counter/<name>`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, Error};

#[path = "support/guests.rs"]
mod guests;

fn counter_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/counter", "counter"))
}

fn runtime() -> Runtime {
    Runtime::new(&RuntimeConfig {
        max_instances: 64,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn service(data: &Path, wasm: &[u8], idle: Duration) -> ActorService {
    let runtime = runtime();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = idle;
    ActorService::new(
        runtime,
        config,
        vec![("counter".into(), component, wasm.to_vec())],
    )
    .unwrap()
}

async fn start(service: ActorService) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

/// Returns (status, body) for a request with `Connection: close`.
async fn request(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, dechunk(body))
}

fn dechunk(body: &str) -> String {
    // Chunked bodies here are small: "<hex>\r\n<data>\r\n...0\r\n\r\n".
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            return body.to_owned(); // not chunked
        };
        if n == 0 {
            break;
        }
        out.push_str(&tail[..n]);
        rest = &tail[n + 2..];
    }
    out
}

async fn value(addr: SocketAddr, method: &str, path: &str) -> u64 {
    let (status, body) = request(addr, method, path).await;
    assert_eq!(status, 200, "{body}");
    body.trim()
        .parse()
        .unwrap_or_else(|_| panic!("not a number: {body:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn each_name_is_its_own_counter() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(
        data.path(),
        counter_wasm(),
        Duration::from_secs(30),
    ))
    .await;

    assert_eq!(value(addr, "GET", "/actor/counter/a").await, 0);
    for expected in 1..=3 {
        assert_eq!(value(addr, "POST", "/actor/counter/a").await, expected);
    }
    assert_eq!(value(addr, "GET", "/actor/counter/a").await, 3);
    assert_eq!(value(addr, "GET", "/actor/counter/b").await, 0);
    // Percent-encoded names are decoded and distinct.
    assert_eq!(value(addr, "POST", "/actor/counter/room%201").await, 1);
    assert_eq!(value(addr, "GET", "/actor/counter/room").await, 0);
    assert!(
        data.path()
            .join("actors/default/counter/room%201.db")
            .exists()
    );

    assert_eq!(request(addr, "GET", "/actor/nope/a").await.0, 404);
    assert_eq!(request(addr, "GET", "/actor/counter/").await.0, 400);
    assert_eq!(request(addr, "PUT", "/actor/counter/a").await.0, 405);
    assert_eq!(request(addr, "GET", "/not-an-actor").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_requests_to_one_actor_are_serialized() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(
        data.path(),
        counter_wasm(),
        Duration::from_secs(30),
    ))
    .await;
    let tasks: Vec<_> = (0..100)
        .map(|_| tokio::spawn(async move { value(addr, "POST", "/actor/counter/hot").await }))
        .collect();
    let mut seen = Vec::new();
    for t in tasks {
        seen.push(t.await.unwrap());
    }
    seen.sort_unstable();
    assert_eq!(
        seen,
        (1..=100).collect::<Vec<u64>>(),
        "lost or duplicated increments"
    );
    assert_eq!(value(addr, "GET", "/actor/counter/hot").await, 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn state_survives_a_restart() {
    let data = tempfile::tempdir().unwrap();
    {
        let (addr, stop) = start(service(
            data.path(),
            counter_wasm(),
            Duration::from_secs(30),
        ))
        .await;
        for _ in 0..5 {
            value(addr, "POST", "/actor/counter/a").await;
        }
        drop(stop);
    }
    let (addr, _stop) = start(service(
        data.path(),
        counter_wasm(),
        Duration::from_secs(30),
    ))
    .await;
    assert_eq!(value(addr, "GET", "/actor/counter/a").await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_actors_hibernate_and_keep_their_data() {
    let data = tempfile::tempdir().unwrap();
    // The idle timeout must comfortably exceed the time two requests take on
    // a slow CI runner (b's first request creates its database and runs
    // on-init); with 50 ms, `a` sometimes hibernated before `b` answered.
    let svc = service(data.path(), counter_wasm(), Duration::from_millis(500));
    let (addr, _stop) = start(svc.clone()).await;
    value(addr, "POST", "/actor/counter/a").await;
    value(addr, "POST", "/actor/counter/b").await;
    assert_eq!(svc.resident(), 2);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(svc.resident(), 0, "actors did not hibernate");

    assert_eq!(value(addr, "POST", "/actor/counter/a").await, 2);
    assert_eq!(svc.resident(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_init_runs_again_for_a_new_component_version() {
    let data = tempfile::tempdir().unwrap();
    let v1 = counter_wasm().to_vec();
    // Same code, different bytes: a custom section appended to the component.
    let mut v2 = v1.clone();
    let payload = b"\x07versionv2";
    v2.push(0x00);
    v2.push(u8::try_from(payload.len()).unwrap());
    v2.extend_from_slice(payload);

    let init_version = |data: &Path| {
        let storage = wasmkeep_storage::Storage::new(wasmkeep_storage::StorageConfig::new(data));
        storage
            .open("default", "counter", "a")
            .unwrap()
            .meta("init_version")
            .unwrap()
    };

    {
        let (addr, stop) = start(service(data.path(), &v1, Duration::from_secs(30))).await;
        assert_eq!(value(addr, "POST", "/actor/counter/a").await, 1);
        drop(stop);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let after_v1 = init_version(data.path()).expect("on-init recorded its version");

    {
        let (addr, stop) = start(service(data.path(), &v2, Duration::from_secs(30))).await;
        // on-init ran again (it is idempotent, so the count is kept).
        assert_eq!(value(addr, "POST", "/actor/counter/a").await, 2);
        drop(stop);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let after_v2 = init_version(data.path()).unwrap();
    assert_ne!(after_v1, after_v2);
}

#[test]
fn a_plain_http_component_is_not_an_actor() {
    let hello = guests::build_guest("../../examples/hello", "hello");
    let runtime = runtime();
    let component = runtime.compile(&hello).unwrap();
    let data = tempfile::tempdir().unwrap();
    let err = ActorService::new(
        runtime,
        ActorConfig::new(data.path()),
        vec![("hello".into(), component, hello)],
    )
    .unwrap_err();
    assert!(matches!(err, Error::NotAnActor(..)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn replacing_a_class_switches_resident_actors_at_the_next_event() {
    let data = tempfile::tempdir().unwrap();
    let svc = service(data.path(), counter_wasm(), Duration::from_secs(30));
    let (addr, _stop) = start(svc.clone()).await;
    assert_eq!(value(addr, "POST", "/actor/counter/a").await, 1);
    assert_eq!(value(addr, "POST", "/actor/counter/a").await, 2);
    assert_eq!(svc.resident(), 1);

    // v2: same code, different bytes, so on-init must run again; data stays.
    let mut v2 = counter_wasm().to_vec();
    v2.extend_from_slice(b"\x00\x0a\x07versionv2"); // custom section, 10-byte payload
    let runtime = svc.runtime().clone();
    let component = runtime.compile(&v2).unwrap();
    svc.replace_class("counter", &component, &v2).unwrap();
    assert_eq!(value(addr, "POST", "/actor/counter/a").await, 3);

    // A different component under the same class: the resident actor now
    // runs the new code (rate-limiter answers GET with its window state).
    let limiter = guests::build_guest("../../examples/rate-limiter", "rate_limiter");
    let component = runtime.compile(&limiter).unwrap();
    svc.replace_class("counter", &component, &limiter).unwrap();
    let (status, body) = request(addr, "GET", "/actor/counter/a").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("no open window"), "{body}");

    // An invalid component is rejected and the current version keeps serving.
    let hello = guests::build_guest("../../examples/hello", "hello");
    let component = runtime.compile(&hello).unwrap();
    assert!(svc.replace_class("counter", &component, &hello).is_err());
    assert!(
        request(addr, "GET", "/actor/counter/a")
            .await
            .1
            .contains("no open window")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn more_than_a_thousand_actors_can_be_resident_at_once() {
    // Each resident actor's store keeps an async stack for reuse; the
    // stack pool must be as large as the instance pool, not Wasmtime's
    // default of 1000. (Residents are capped at 90% of the pool, 1,080.)
    // 1,080 residents hold about 3,240 open files: raise the soft limit as
    // `wasmkeep serve` does, rather than depend on the machine's.
    let limit = wasmkeep_server::fds::raise_open_files_limit(1 << 20);
    assert!(
        limit.is_none_or(|l| l >= 4_000),
        "this test needs a hard open-files limit of at least 4,000 (it is {limit:?})"
    );
    let data = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 1_200,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let wasm = counter_wasm();
    let component = runtime.compile(wasm).unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.idle_timeout = Duration::from_secs(600);
    let svc = ActorService::new(
        runtime,
        config,
        vec![("counter".into(), component, wasm.to_vec())],
    )
    .unwrap();
    let (addr, _stop) = start(svc.clone()).await;
    for i in 0..1_050 {
        let (status, body) = request(addr, "POST", &format!("/actor/counter/n{i}")).await;
        assert_eq!(status, 200, "actor {i}: {body}");
    }
    assert_eq!(svc.resident(), 1_050);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_reports_not_ready_once_it_starts_shutting_down() {
    let dir = tempfile::tempdir().unwrap();
    let svc = service(dir.path(), counter_wasm(), Duration::from_secs(30));
    let (addr, _stop) = start(svc.clone()).await;
    // Ready once the disk has been sampled.
    let mut status = 0;
    for _ in 0..100 {
        status = request(addr, "GET", "/_wasmkeep/ready").await.0;
        if status == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status, 200);
    assert_eq!(svc.not_ready(), None);
    svc.begin_shutdown();
    assert_eq!(svc.not_ready(), Some("shutting down"));
    assert_eq!(request(addr, "GET", "/_wasmkeep/ready").await.0, 503);
    // Still alive, still serving.
    assert_eq!(request(addr, "GET", "/_wasmkeep/health").await.0, 200);
}
