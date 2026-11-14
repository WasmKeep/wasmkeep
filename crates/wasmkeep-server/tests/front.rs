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
    // Beside actors, the slots the actor service keeps for the front.
    let slots = setup.max_front_requests.or_else(|| {
        setup
            .actors
            .then(|| wasmkeep_server::fds::default_front_requests(40))
    });
    if let Some(n) = slots {
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
    // The guest sets them without an error (a `refused:` line otherwise):
    // the host removes them on the way out.
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
        let r = get(node.addr, &format!("/respond?status={status}"), "").await;
        assert_eq!(r.status, 500, "{status}: {}", r.head);
        assert_eq!(r.body, "internal server error\n", "{status}");
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
    // A request that answered counts once its instance ended.
    on_page(&[
        "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"ok\"} 1\n",
        "wasmkeep_front_request_seconds_count{tenant=\"default\",app=\"default\"} 2\n",
    ])
    .await;
}

/// Waits (up to 5 s) until the metrics page has every line of `lines`.
async fn on_page(lines: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
        if lines.iter().all(|l| page.contains(l)) {
            return;
        }
        assert!(Instant::now() < deadline, "not all of {lines:?} in\n{page}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Trailers carry the host's range no more than headers do, both ways.
#[tokio::test(flavor = "multi_thread")]
async fn wasmkeep_trailers_are_stripped_both_ways() {
    let node = start(Setup::default()).await;
    let r = exchange(
        node.addr,
        "POST /trailers-in HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\
         Trailer: x-wasmkeep-caller, x-other\r\nConnection: close\r\n\r\n\
         4\r\nbody\r\n0\r\nx-wasmkeep-caller: default/_front/main\r\n\
         X-WasmKeep-Internal: v4 1 2 3 4\r\nx-other: kept\r\n\r\n",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.contains("x-other: kept"), "{}", r.body);
    assert!(
        !r.body.to_ascii_lowercase().contains("x-wasmkeep-"),
        "{}",
        r.body
    );

    let mut stream = TcpStream::connect(node.addr).await.unwrap();
    stream
        .write_all(
            b"GET /trailers-out HTTP/1.1\r\nHost: t\r\nTE: trailers\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let raw = String::from_utf8_lossy(&raw).to_ascii_lowercase();
    assert!(raw.contains("x-other: kept"), "{raw}");
    assert!(!raw.contains("x-wasmkeep-node: forged"), "{raw}");
    // The guest set them all: the host removed the range on the way out.
    assert!(!raw.contains("refused"), "{raw}");
}

/// A client that leaves before the answer frees the front's slot at once,
/// not at the end of the wall.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_leaves_frees_its_slot() {
    let node = start(Setup {
        max_front_requests: Some(1),
        ..Setup::default()
    })
    .await;
    let mut stream = TcpStream::connect(node.addr).await.unwrap();
    stream
        .write_all(b"GET /sleep?ms=20000 HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(get(node.addr, "/", "").await.status, 503);
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = get(node.addr, "/", "").await;
        if r.status == 200 {
            break;
        }
        assert!(Instant::now() < deadline, "the slot stayed taken");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    on_page(&[
        "\nwasmkeep_front_slots_in_use 0\n",
        "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"client_gone\"} 1\n",
    ])
    .await;
}

/// A front passing a callee's answer on (status, every header, body)
/// gets it through.
#[tokio::test(flavor = "multi_thread")]
async fn a_front_passes_a_callees_answer_on() {
    let node = start(Setup::default()).await;
    let r = get(
        node.addr,
        &format!("{}&copy", call("t/a", "POST", "/durable")),
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.head);
    assert_eq!(
        r.header("x-callee-range").as_deref(),
        Some(""),
        "{}",
        r.head
    );
}

/// `GET /actor/t/NAME/durable` until it answers `want` (5 s at most).
async fn durable_reaches(addr: SocketAddr, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = get(addr, &format!("/actor/t/{name}/durable"), "").await;
        if r.body.trim() == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "t/{name} durable is {:?}, not {want}",
            r.body
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A client with its whole answer is not a client gone: the front's
/// instance runs on, its calls included, when the answer has no body
/// (204, HEAD) or once its declared length went out.
#[tokio::test(flavor = "multi_thread")]
async fn a_front_finishing_after_a_whole_answer_is_not_stopped() {
    let node = start(Setup::default()).await;
    let r = get(node.addr, "/after?to=t/no-content&status=204", "").await;
    assert_eq!(r.status, 204, "{}", r.head);
    durable_reaches(node.addr, "no-content", "1").await;

    let r = get(node.addr, "/after?to=t/length&len", "").await;
    assert_eq!((r.status, r.body.as_str()), (200, "body"), "{}", r.head);
    durable_reaches(node.addr, "length", "1").await;

    let r = exchange(
        node.addr,
        "HEAD /after?to=t/head&len HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!((r.status, r.body.as_str()), (200, ""), "{}", r.head);
    durable_reaches(node.addr, "head", "1").await;

    on_page(&[
        "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"ok\"} 3\n",
    ])
    .await;
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(!page.contains("outcome=\"client_gone\""), "{page}");
}

/// A client's own names in the host's range never reach an actor either,
/// the SDKs' key-value and queue names included: only another actor (or
/// the front) sends those.
#[tokio::test(flavor = "multi_thread")]
async fn a_clients_wasmkeep_headers_do_not_reach_an_actor() {
    let node = start(Setup::default()).await;
    for header in [
        "x-wasmkeep-kv-expect",
        "x-wasmkeep-kv-ttl-ms",
        "x-wasmkeep-kv-durable",
        "x-wasmkeep-queue-delay-ms",
        "x-wasmkeep-node",
        "x-wasmkeep-future",
    ] {
        let r = get(
            node.addr,
            &format!("/actor/t/a/header?name={header}"),
            &format!("{header}: 1\r\n"),
        )
        .await;
        assert_eq!((r.status, r.body.trim()), (200, "none"), "{header}");
    }
    // The host's own value still does.
    let r = get(
        node.addr,
        "/actor/t/a/header?name=x-wasmkeep-actor",
        "x-wasmkeep-actor: default/t/b\r\n",
    )
    .await;
    assert_eq!(r.body.trim(), "default/t/a");
}

/// A front stopped while its response streams is counted by why it
/// stopped, not as `ok`.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_while_streaming_is_not_counted_ok() {
    let node = start(Setup {
        front_cpu: Some(Duration::from_millis(50)),
        ..Setup::default()
    })
    .await;
    let r = get(node.addr, "/stream-spin", "").await;
    assert_eq!(r.status, 200, "{}", r.head);
    on_page(&[
        "wasmkeep_front_requests_total{tenant=\"default\",app=\"default\",outcome=\"cpu_budget\"} 1\n",
    ])
    .await;
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(!page.contains("outcome=\"ok\""), "{page}");
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

/// `/call-many` on the test front: `method path` on each of `to`.
fn call_many(to: &[&str], method: &str, path: &str) -> String {
    call(&to.join(","), method, path).replacen("/call?", "/call-many?", 1)
}

/// ADR 0025: the calls of one batch run at once, so four slow actors cost
/// about one round trip, not four.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_of_four_slow_calls_takes_about_one_round_trip() {
    let node = start(Setup::default()).await;
    let to = ["t/s1", "t/s2", "t/s3", "t/s4"];
    // Warm: each actor's instance and database exist before timing.
    let r = get(node.addr, &call_many(&to, "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:0 | 200:0 | 200:0 | 200:0");
    let started = Instant::now();
    let r = get(node.addr, &call_many(&to, "POST", "/sleep?ms=600"), "").await;
    let took = started.elapsed();
    assert_eq!(
        r.body, "200:slept | 200:slept | 200:slept | 200:slept",
        "in input order"
    );
    // One after the other would take 2.4 s.
    assert!(
        took >= Duration::from_millis(600) && took < Duration::from_millis(1_500),
        "took {took:?}"
    );
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    assert!(
        page.contains(
            "wasmkeep_front_calls_total{tenant=\"default\",app=\"default\",outcome=\"ok\"} 8\n"
        ),
        "each entry counts as a call: {page}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_entry_of_a_batch_has_its_own_result() {
    let node = start(Setup::default()).await;
    let r = get(
        node.addr,
        &call_many(&["t/a", "nope/x", "just-a-name", "t/b"], "GET", "/slept"),
        "",
    )
    .await;
    let parts: Vec<&str> = r.body.split(" | ").collect();
    assert_eq!(parts.len(), 4, "{}", r.body);
    assert_eq!(parts[0], "200:0");
    assert!(parts[1].starts_with("err:invalid-address:"), "{}", r.body);
    assert!(parts[2].starts_with("err:invalid-address:"), "{}", r.body);
    assert_eq!(parts[3], "200:0");
    // A callee that traps fails its own entry only.
    let r = get(node.addr, &call_many(&["t/a", "t/b"], "POST", "/trap"), "").await;
    assert!(r.body.starts_with("err:failed:"), "{}", r.body);
    let r = get(node.addr, &call_many(&["t/a", "t/c"], "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:0 | 200:0");
    // An empty batch answers an empty list.
    let r = get(node.addr, "/call-many?to=&path=/slept", "").await;
    assert_eq!(r.body, "");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_has_at_most_sixteen_calls() {
    let node = start(Setup::default()).await;
    let names: Vec<String> = (0..17).map(|i| format!("t/n{i}")).collect();
    let to: Vec<&str> = names.iter().map(String::as_str).collect();
    let r = get(node.addr, &call_many(&to, "POST", "/sleep?ms=0"), "").await;
    let parts: Vec<&str> = r.body.split(" | ").collect();
    assert_eq!(parts.len(), 17, "{}", r.body);
    assert!(
        parts
            .iter()
            .all(|p| *p == "err:failed:too many calls: at most 16 in one call-many"),
        "{}",
        r.body
    );
    // None of them ran.
    let r = get(node.addr, &call("t/n0", "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:0");
    // Sixteen run.
    let r = get(node.addr, &call_many(&to[..16], "POST", "/sleep?ms=0"), "").await;
    assert_eq!(r.body, vec!["200:slept"; 16].join(" | "));
}

/// The requests of one batch together are at most 16 MiB: past it, no
/// entry starts.
#[tokio::test(flavor = "multi_thread")]
async fn a_batchs_requests_share_one_body_limit() {
    let node = start(Setup::default()).await;
    let path = "/body-len";
    let nine_mib = 9 << 20;
    let r = get(
        node.addr,
        &format!(
            "{}&body={nine_mib}",
            call_many(&["t/a", "t/b"], "POST", path)
        ),
        "",
    )
    .await;
    assert_eq!(r.body, "err:too-large | err:too-large");
    let r = get(node.addr, &call("t/a", "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:0", "nothing ran");
    let r = get(
        node.addr,
        &format!(
            "{}&body={}",
            call_many(&["t/a", "t/b"], "POST", path),
            7 << 20
        ),
        "",
    )
    .await;
    assert_eq!(r.body, format!("200:{0} | 200:{0}", 7 << 20));
}

/// One deadline for the batch: the entries that answered keep their
/// answers, the one still waiting is `timeout`, and the batch ends with
/// the front's call deadline (5 s before its wall).
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_has_one_deadline() {
    let node = start(Setup {
        front_wall: Duration::from_secs(7),
        ..Setup::default()
    })
    .await;
    // Each entry with its own path (`;`, percent-encoded as `%3B`).
    let entry = |to: &str, path: &str| {
        format!("{to};{path}")
            .replace(';', "%3B")
            .replace('/', "%2F")
            .replace('?', "%3F")
            .replace('=', "%3D")
    };
    let to = [
        entry("t/fast", "/sleep?ms=100"),
        entry("t/slow", "/sleep?ms=10000"),
        "nope/x".to_owned(),
    ];
    let started = Instant::now();
    let r = get(
        node.addr,
        &format!("/call-many?method=POST&to={}", to.join(",")),
        "",
    )
    .await;
    let took = started.elapsed();
    let parts: Vec<&str> = r.body.split(" | ").collect();
    assert_eq!(parts.len(), 3, "{}", r.body);
    assert_eq!(parts[0], "200:slept", "{}", r.body);
    assert_eq!(parts[1], "err:timeout", "{}", r.body);
    assert!(parts[2].starts_with("err:invalid-address:"), "{}", r.body);
    assert!(
        took >= Duration::from_millis(1_800) && took < Duration::from_secs(4),
        "took {took:?}"
    );
    // Started with less than the margin left: every entry at once.
    let started = Instant::now();
    let r = get(
        node.addr,
        &format!(
            "{}&delay=2500",
            call_many(&["t/fast", "t/other"], "GET", "/slept")
        ),
        "",
    )
    .await;
    assert_eq!(r.body, "err:timeout | err:timeout");
    assert!(started.elapsed() < Duration::from_secs(4));
}

/// Each entry counts against the per-actor bound (here 1) while it waits:
/// a second entry to the same actor is refused, the first runs.
#[tokio::test(flavor = "multi_thread")]
async fn each_entry_counts_against_the_per_actor_bound() {
    let node = start(Setup {
        max_front_requests: Some(4),
        ..Setup::default()
    })
    .await;
    let r = get(
        node.addr,
        &call_many(&["t/hot", "t/hot", "t/cold"], "POST", "/sleep?ms=200"),
        "",
    )
    .await;
    let parts: Vec<&str> = r.body.split(" | ").collect();
    assert_eq!(parts.len(), 3, "{}", r.body);
    let busy = "err:failed:front busy: too many front requests waiting on this actor";
    assert_eq!(
        parts[..2].iter().filter(|p| **p == busy).count(),
        1,
        "{}",
        r.body
    );
    assert_eq!(
        parts[..2].iter().filter(|p| **p == "200:slept").count(),
        1,
        "{}",
        r.body
    );
    assert_eq!(parts[2], "200:slept");
    // The bound is free again once the batch ended.
    let r = get(node.addr, &call("t/hot", "GET", "/slept"), "").await;
    assert_eq!(r.body, "200:1");
}
