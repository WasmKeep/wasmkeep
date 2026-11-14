//! The front (ADR 0023) on one node: the component given to `serve`, linked
//! with `actors` and `env`, calling the node's actors as an inside caller,
//! behind the host's header hygiene and its own limits.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, HttpComponent, ServeLimits};

#[path = "support/guests.rs"]
mod guests;

fn front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-front", "test_front"))
}

fn actor_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

/// How a test node is set up.
struct Setup {
    /// Serve class `t` (the test actor) beside the front.
    actors: bool,
    /// The front's request slots.
    max_front_requests: Option<usize>,
    /// The front's wall per request.
    front_wall: Duration,
    /// The front's values (`--front-env`).
    env: Vec<(&'static str, &'static str)>,
    /// The front's CPU budget, and the actors'.
    front_cpu: Option<Duration>,
    actor_cpu: Duration,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            actors: true,
            max_front_requests: None,
            front_wall: Duration::from_secs(30),
            env: Vec::new(),
            front_cpu: None,
            actor_cpu: Duration::from_millis(50),
        }
    }
}

struct Running {
    addr: SocketAddr,
    _data: tempfile::TempDir,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start(setup: Setup) -> Running {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 40,
        cpu_budget: setup.actor_cpu,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let data = tempfile::tempdir().unwrap();
    let component = guests::compile(&runtime, front_wasm());
    let mut front = HttpComponent::new(runtime.clone(), &component).unwrap();
    if let Some(n) = setup.max_front_requests {
        front = front.with_max_requests(n);
    }
    if let Some(budget) = setup.front_cpu {
        front = front.with_cpu_budget(budget);
    }
    if !setup.env.is_empty() {
        front = front.with_env(
            setup
                .env
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        );
    }
    let actors = setup.actors.then(|| {
        let component = guests::compile(&runtime, actor_wasm());
        let mut config = ActorConfig::new(data.path());
        config.max_front_requests = setup.max_front_requests;
        ActorService::new(
            runtime.clone(),
            config,
            vec![("t".into(), component, actor_wasm().to_vec())],
        )
        .unwrap()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        front: Some(front),
        actors,
    };
    let limits = ServeLimits {
        front_wall: setup.front_wall,
        ..ServeLimits::default()
    };
    tokio::spawn(wasmkeep_server::serve_with(
        listener,
        app,
        async {
            let _ = stopped.await;
        },
        limits,
    ));
    Running {
        addr,
        _data: data,
        _stop: stop,
    }
}

struct Reply {
    status: u16,
    head: String,
    body: String,
}

impl Reply {
    fn header(&self, name: &str) -> Option<String> {
        self.head.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
        })
    }
}

async fn exchange(addr: SocketAddr, req: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {raw:?}"));
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    Reply {
        status,
        head: head.to_owned(),
        body: if chunked {
            dechunk(body)
        } else {
            body.to_owned()
        },
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            return body.to_owned();
        };
        if n == 0 {
            break;
        }
        out.push_str(&tail[..n]);
        rest = &tail[n + 2..];
    }
    out
}

async fn get(addr: SocketAddr, path: &str, extra: &str) -> Reply {
    exchange(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: shop.example\r\n{extra}Connection: close\r\n\r\n"),
    )
    .await
}

/// `/call` on the test front: `method path` on `to`.
fn call(to: &str, method: &str, path: &str) -> String {
    format!(
        "/call?to={to}&method={method}&path={}",
        path.replace('%', "%25")
            .replace('?', "%3F")
            .replace('=', "%3D")
            .replace('&', "%26")
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn the_front_calls_an_actor_with_its_own_caller_value() {
    let node = start(Setup::default()).await;
    let r = get(node.addr, &call("t/a", "POST", "/caller"), "").await;
    assert_eq!(
        (r.status, r.body.as_str()),
        (200, "200:default/_front/main")
    );
    // The call created the actor, which is reachable from outside after.
    let r = get(node.addr, "/actor/t/a/inits", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let r = get(node.addr, "/self", "").await;
    assert_eq!(r.body, "default/_front/main");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_front_cannot_be_called_or_reached_under_actor() {
    let node = start(Setup::default()).await;
    let r = get(node.addr, &call("_front/main", "GET", "/"), "").await;
    assert!(r.body.starts_with("err:invalid-address:"), "{}", r.body);
    let r = get(node.addr, "/actor/_front/main/", "").await;
    assert_eq!(r.status, 404, "{}", r.body);
    // An actor calling the front gets the same.
    let r = exchange(
        node.addr,
        "POST /actor/t/b/call?to=_front/main&path=%2F HTTP/1.1\r\nHost: t\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(r.body.contains("invalid-address"), "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_cannot_pose_as_the_front() {
    let node = start(Setup::default()).await;
    let r = exchange(
        node.addr,
        "POST /actor/t/a/caller HTTP/1.1\r\nHost: t\r\nx-wasmkeep-caller: default/_front/main\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "none"), "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_actors_calls_fail_and_actor_paths_answer_404() {
    let node = start(Setup {
        actors: false,
        ..Setup::default()
    })
    .await;
    let r = get(node.addr, &call("t/a", "GET", "/"), "").await;
    assert!(r.body.starts_with("err:invalid-address:"), "{}", r.body);
    // `/actor/` never reaches the front.
    let r = get(node.addr, "/actor/t/a/headers", "").await;
    assert_eq!(r.status, 404, "{}", r.body);
    assert!(!r.body.contains("GET"), "{}", r.body);
    let r = get(node.addr, "/self", "").await;
    assert_eq!(r.body, "default/_front/main");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_front_reads_its_own_values() {
    let node = start(Setup {
        env: vec![("SESSION_KEY", "s3cret")],
        ..Setup::default()
    })
    .await;
    let r = get(node.addr, "/env?name=SESSION_KEY", "").await;
    assert_eq!(r.body, "s3cret");
    let r = get(node.addr, "/env?name=OTHER", "").await;
    assert_eq!(r.body, "none");
}

/// What a client sends in the `x-wasmkeep-` range never reaches the front,
/// and what the front answers in it never reaches the client.
#[tokio::test(flavor = "multi_thread")]
async fn wasmkeep_headers_are_stripped_both_ways() {
    let node = start(Setup::default()).await;
    let r = get(
        node.addr,
        "/headers",
        "x-wasmkeep-caller: default/admin/root\r\nX-WasmKeep-Actor: default/t/a\r\n\
         x-wasmkeep-internal: v4 1 2 3 4\r\nx-wasmkeep-forwarded: 1\r\n\
         x-wasmkeep-chain: default/t/a\r\nx-wasmkeep-anything: x\r\nx-other: kept\r\n",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.contains("x-other: kept"), "{}", r.body);
    assert!(
        !r.body.to_ascii_lowercase().contains("x-wasmkeep-"),
        "{}",
        r.body
    );
    // The host passes the client's Host on as the authority, not a header
    // the guest can forge.
    let r = get(node.addr, "/respond?spoof", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "responded"));
    assert!(
        !r.head.to_ascii_lowercase().contains("x-wasmkeep-"),
        "{}",
        r.head
    );
}

/// A guest's own `x-wasmkeep-` headers on `actors.call` never reach the
/// callee: neither the forwarded mark nor a forged caller.
#[tokio::test(flavor = "multi_thread")]
async fn guest_set_wasmkeep_headers_do_not_reach_the_callee() {
    let node = start(Setup::default()).await;
    for header in [
        "x-wasmkeep-forwarded",
        "x-wasmkeep-node",
        "x-wasmkeep-chain",
        "x-wasmkeep-internal",
        "x-wasmkeep-future",
    ] {
        let r = get(
            node.addr,
            &format!(
                "{}&spoof",
                call("t/a", "GET", &format!("/header?name={header}"))
            ),
            "",
        )
        .await;
        assert_eq!(r.body, "200:none", "{header}");
    }
    let r = get(
        node.addr,
        &format!("{}&spoof", call("t/a", "POST", "/caller")),
        "",
    )
    .await;
    assert_eq!(r.body, "200:default/_front/main");
    let r = get(
        node.addr,
        &format!(
            "{}&spoof",
            call("t/a", "GET", "/header?name=x-wasmkeep-actor")
        ),
        "",
    )
    .await;
    assert_eq!(r.body, "200:default/t/a");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_informational_status_from_the_front_becomes_500() {
    let node = start(Setup::default()).await;
    for status in [101, 103, 199] {
        let r = get(node.addr, &format!("/respond?status={status}&spoof"), "").await;
        assert_eq!(r.status, 500, "{status}: {}", r.head);
        assert!(!r.body.contains("responded"), "{}", r.body);
        assert!(
            !r.head.to_ascii_lowercase().contains("x-wasmkeep-"),
            "{}",
            r.head
        );
    }
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.contains(
            "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"guest_error\"} 3\n"
        ),
        "{page}"
    );
}

/// An upgrade outside `/actor/` reaches the front as a plain GET, and is
/// never answered with an upgrade.
#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_outside_actor_paths_reaches_the_front_as_a_get() {
    let node = start(Setup::default()).await;
    let r = get(
        node.addr,
        "/headers",
        "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.head);
    assert!(r.body.starts_with("GET\n"), "{}", r.body);
    assert!(
        !r.body.to_ascii_lowercase().contains("upgrade:"),
        "{}",
        r.body
    );
    let r = get(
        node.addr,
        "/respond?status=101",
        "Upgrade: websocket\r\nConnection: Upgrade\r\n",
    )
    .await;
    assert_eq!(r.status, 500, "{}", r.head);
}

#[tokio::test(flavor = "multi_thread")]
async fn past_its_slots_the_front_answers_503_without_running() {
    let node = start(Setup {
        max_front_requests: Some(1),
        ..Setup::default()
    })
    .await;
    let addr = node.addr;
    let busy = tokio::spawn(async move { get(addr, "/sleep?ms=1500", "").await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = get(addr, "/", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(r.header("retry-after").as_deref(), Some("1"));
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(page.contains("\nwasmkeep_front_slots_in_use 1\n"), "{page}");
    assert!(
        page.contains(
            "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"busy\"} 1\n"
        ),
        "{page}"
    );
    assert_eq!(busy.await.unwrap().body, "slept");
    // The slot is free again once the request ended.
    let r = get(addr, "/", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "front ok"));
}

/// Four slots: a quarter, one, may wait on one actor at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_quarter_of_the_slots_may_wait_on_one_actor() {
    let node = start(Setup {
        max_front_requests: Some(4),
        ..Setup::default()
    })
    .await;
    let addr = node.addr;
    let slow =
        tokio::spawn(async move { get(addr, &call("t/hot", "POST", "/sleep?ms=1500"), "").await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = get(addr, &call("t/hot", "GET", "/slept"), "").await;
    assert_eq!(
        r.body,
        "err:failed:front busy: too many front requests waiting on this actor"
    );
    // Another actor is not held back.
    let r = get(addr, &call("t/cold", "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:0");
    assert!(slow.await.unwrap().body.starts_with("200:"));
    // Once the first call ended, the actor takes calls again.
    let r = get(addr, &call("t/hot", "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:1");
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.contains(
            "wasmkeep_front_calls_total{tenant=\"default\",app=\"default\",outcome=\"busy\"} 1\n"
        ),
        "{page}"
    );
}

/// A call from the front gives up while 5 s of the front's wall remain,
/// so the front can still answer; with less than that left, at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_from_the_front_times_out_before_its_wall() {
    let node = start(Setup {
        front_wall: Duration::from_secs(7),
        ..Setup::default()
    })
    .await;
    let started = Instant::now();
    let r = get(node.addr, &call("t/slow", "POST", "/sleep?ms=10000"), "").await;
    let took = started.elapsed();
    assert_eq!((r.status, r.body.as_str()), (200, "err:timeout"));
    assert!(
        took >= Duration::from_millis(1_800) && took < Duration::from_secs(4),
        "took {took:?}"
    );
    let started = Instant::now();
    let r = get(
        node.addr,
        &format!("{}&delay=2500", call("t/other", "GET", "/slept")),
        "",
    )
    .await;
    let took = started.elapsed();
    assert_eq!((r.status, r.body.as_str()), (200, "err:timeout"));
    assert!(took < Duration::from_secs(4), "took {took:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_front_has_its_own_cpu_budget() {
    // The actors' budget is far larger: the front's alone stops it.
    let node = start(Setup {
        front_cpu: Some(Duration::from_millis(50)),
        actor_cpu: Duration::from_secs(60),
        ..Setup::default()
    })
    .await;
    let started = Instant::now();
    let r = get(node.addr, "/spin", "").await;
    assert_eq!(r.status, 500);
    assert!(started.elapsed() < Duration::from_secs(5));
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.contains(
            "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"cpu_budget\"} 1\n"
        ),
        "{page}"
    );
    let r = get(node.addr, "/", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "front ok"));
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.contains(
            "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"ok\"} 1\n"
        ),
        "{page}"
    );
    assert!(
        page.contains(
            "wasmkeep_front_request_seconds_count{tenant=\"default\",app=\"default\"} 2\n"
        ),
        "{page}"
    );
}

/// The log lines this test process wrote, for the `wasmkeep::guest` target.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_guest_log_names_the_front() {
    // Each test runs in its own process (nextest): the subscriber is this
    // test's alone.
    let captured = Captured::default();
    let writer = captured.clone();
    tracing_subscriber::fmt()
        .with_env_filter("wasmkeep::guest=info")
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .init();
    let node = start(Setup::default()).await;
    let r = get(node.addr, "/print", "").await;
    assert_eq!(r.body, "printed");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let text = String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();
        if let Some(line) = text.lines().find(|l| l.contains("front says hi")) {
            assert!(line.contains("actor=_front"), "{line}");
            break;
        }
        assert!(Instant::now() < deadline, "no line in {text}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::io::stderr().flush();
}
