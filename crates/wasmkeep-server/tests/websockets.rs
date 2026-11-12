//! WebSocket limits from the security review (docs/security.md#audits).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start() -> (SocketAddr, tempfile::TempDir) {
    let (addr, dir, _) = start_with_limits(7, 256).await;
    (addr, dir)
}

async fn start_with_limits(
    max_resident: usize,
    queue_capacity: usize,
) -> (SocketAddr, tempfile::TempDir, ActorService) {
    start_configured(max_resident, queue_capacity, |_| {}).await
}

async fn start_configured(
    max_resident: usize,
    queue_capacity: usize,
    configure: impl FnOnce(&mut ActorConfig),
) -> (SocketAddr, tempfile::TempDir, ActorService) {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    let wasm = WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"));
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(wasm).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.mailbox.max_resident = max_resident;
    config.mailbox.queue_capacity = queue_capacity;
    config.mailbox.idle_timeout = Duration::from_secs(600);
    config.egress = wasmkeep_server::EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    configure(&mut config);
    let service =
        ActorService::new(runtime, config, vec![("t".into(), component, wasm.clone())]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    (addr, dir, service)
}

/// Runs a closure when dropped (also when a test panics).
struct Defer<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for Defer<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

async fn post(addr: SocketAddr, path: &str) -> String {
    request(addr, "POST", path).await
}

async fn request(addr: SocketAddr, method: &str, path: &str) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).await.unwrap();
    raw
}

async fn ready_actor(addr: SocketAddr, actor: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = post(addr, &format!("/actor/t/{actor}/fired")).await;
            if response.starts_with("HTTP/1.1 200") {
                return;
            }
            assert!(response.starts_with("HTTP/1.1 503"), "{response}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actor never became ready");
}

async fn echo(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    text: &str,
) {
    ws.send(Message::text(text.to_owned())).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("message was not delivered")
        .unwrap()
        .unwrap();
    assert_eq!(got, Message::text(text.to_owned()));
}

/// An outbound HTTP gate proves the guest is busy before testing admission.
async fn block_actor(
    addr: SocketAddr,
    actor: &str,
) -> (TcpStream, tokio::task::JoinHandle<String>) {
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gate_addr = gate.local_addr().unwrap();
    let path = format!("/actor/t/{actor}/fetch?url=http%3A%2F%2F{gate_addr}%2F");
    let request = tokio::spawn(async move { request(addr, "GET", &path).await });
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), gate.accept())
        .await
        .expect("actor did not reach its outbound gate")
        .unwrap();
    (stream, request)
}

async fn release_actor(mut gate: TcpStream, request: tokio::task::JoinHandle<String>) {
    gate.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n0")
        .await
        .unwrap();
    let response = request.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

async fn one_close_callback(addr: SocketAddr, service: &ActorService) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.websockets() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("close event never finished");
    let response = post(addr, "/actor/t/room/ws-closed").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.ends_with("1\n") || response.contains("\r\n1\n"),
        "expected exactly one close callback: {response}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_socket_actor_receives_the_original_message_after_admission() {
    let (addr, _dir, _service) = start_with_limits(1, 8).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
        .await
        .unwrap();
    echo(&mut ws, "before eviction").await;
    ready_actor(addr, "other").await;
    echo(&mut ws, "preserved after eviction").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_event_waiting_for_capacity_keeps_its_socket_slot() {
    let (addr, _dir, service) = start_with_limits(1, 8).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
        .await
        .unwrap();
    echo(&mut ws, "ready").await;
    ready_actor(addr, "other").await;
    let (gate, request) = block_actor(addr, "other").await;
    ws.close(None).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("writer did not close while admission waited");
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "{closed:?}"
    );
    assert_eq!(
        service.websockets(),
        1,
        "pending close lost its capacity slot"
    );
    release_actor(gate, request).await;
    one_close_callback(addr, &service).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_event_retries_a_full_mailbox_without_being_lost() {
    let (addr, _dir, service) = start_with_limits(1, 1).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
        .await
        .unwrap();
    echo(&mut ws, "ready").await;
    let (gate, request) = block_actor(addr, "room").await;
    let mut fillers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        fillers.spawn(post(addr, "/actor/t/room/fired"));
    }
    // The active guest is held by the gate. An overload response proves
    // the sole queue slot has been filled by one of the other requests.
    let overload = tokio::time::timeout(Duration::from_secs(5), fillers.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        overload.starts_with("HTTP/1.1 503") || overload.starts_with("HTTP/1.1 429"),
        "{overload}"
    );
    ws.close(None).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("writer did not close while mailbox was full");
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "{closed:?}"
    );
    assert_eq!(
        service.websockets(),
        1,
        "pending close lost its capacity slot"
    );
    release_actor(gate, request).await;
    while let Some(response) = fillers.join_next().await {
        let _ = response.unwrap();
    }
    one_close_callback(addr, &service).await;
}

/// A socket is open, registered and past its open hook, by the time the
/// client sees 101: a broadcast sent right after connecting reaches it.
/// The upgrade used to answer first and open the socket in a task after,
/// so a loaded node lost the broadcasts sent in between.
#[tokio::test(flavor = "multi_thread")]
async fn a_socket_is_open_when_the_upgrade_answers() {
    let (addr, _dir, _service) = start_with_limits(7, 256).await;
    ready_actor(addr, "room").await;
    // Load, as the full suite makes: the window used to open under it.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let spinners: Vec<_> = (0..2 * std::thread::available_parallelism().map_or(4, |n| n.get()))
        .map(|_| {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();
    let _stop_spinning = Defer(Some(move || {
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for s in spinners {
            let _ = s.join();
        }
    }));
    let mut sockets = Vec::new();
    for i in 0..40 {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
            .await
            .unwrap();
        sockets.push(ws);
        let sent = post(addr, &format!("/actor/t/room/broadcast?text=m{i}")).await;
        // One chunk: its size, then the count of sockets the actor saw.
        let body = sent.split_once("\r\n\r\n").map_or("", |(_, b)| b);
        let seen = body.lines().nth(1).unwrap_or_default();
        assert_eq!(seen, (i + 1).to_string(), "socket {i}: {sent:?}");
    }
    // The newest socket got every broadcast since it connected: just its own.
    let last = sockets.last_mut().unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), last.next())
        .await
        .unwrap();
    assert_eq!(got.unwrap().unwrap(), Message::text("m39"));
}

/// The 101 waits for the open hook, as a Durable Object accepts a socket
/// before answering: on a busy actor, the hook (and so the upgrade) waits
/// for the events ahead of it, and the socket sees what comes after.
#[tokio::test(flavor = "multi_thread")]
async fn the_upgrade_answers_after_the_open_hook_ran() {
    let (addr, _dir, _service) = start_with_limits(7, 256).await;
    ready_actor(addr, "busy").await;
    let sleep = tokio::spawn(post(addr, "/actor/t/busy/sleep?ms=500"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/busy"))
        .await
        .unwrap();
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(300),
        "the upgrade answered after {waited:?}, before the busy actor ran its open hook"
    );
    sleep.await.unwrap();
    let sent = post(addr, "/actor/t/busy/broadcast?text=after").await;
    assert!(sent.contains("\r\n1\n"), "{sent:?}");
    let got = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(got.unwrap().unwrap(), Message::text("after"));
}

/// A client that hangs up while its open hook waits behind a busy actor
/// leaves nothing behind: the hook still runs (it was queued), then the
/// socket gets its close event and its slot back.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_gone_before_the_open_hook_ran_is_closed_and_forgotten() {
    let (addr, _dir, service) = start_with_limits(7, 256).await;
    ready_actor(addr, "room").await;
    // The upgrade's admission check waits behind a first blocked event,
    // and a second one is queued behind that check, so the open hook
    // itself waits behind the second.
    let (first_gate, first) = block_actor(addr, "room").await;
    let client = tokio::spawn(tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/t/room"
    )));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = tokio::spawn(block_actor(addr, "room"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    release_actor(first_gate, first).await;
    let (second_gate, second) = second.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.websockets() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the socket was never registered");
    client.abort(); // hangs up before the 101
    let _ = client.await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    release_actor(second_gate, second).await;
    one_close_callback(addr, &service).await;
    // A live socket now sees only itself.
    let (_ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
        .await
        .unwrap();
    let sent = post(addr, "/actor/t/room/broadcast?text=x").await;
    assert!(
        sent.contains("\r\n1\n"),
        "a socket nobody holds is still listed: {sent:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_socket_the_actor_refuses_never_sees_its_traffic() {
    let (addr, _dir) = start().await;
    // An accepted member.
    let (mut member, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room"))
        .await
        .unwrap();
    // The actor is busy; a broadcast queues behind; then a socket it will
    // refuse connects, so it is registered while the broadcast runs.
    let sleep = tokio::spawn(post(addr, "/actor/t/room/sleep?ms=600"));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let broadcast = tokio::spawn(post(addr, "/actor/t/room/broadcast?text=secret"));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (mut intruder, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/room/refuse"))
            .await
            .unwrap();
    sleep.await.unwrap();
    let sent = broadcast.await.unwrap();
    assert!(
        sent.ends_with("1\n") || sent.contains("\r\n1\n"),
        "the broadcast saw the pending socket: {sent}"
    );

    // The member got it; the intruder only its refusal, with the guest's
    // own reason.
    let got = tokio::time::timeout(Duration::from_secs(5), member.next())
        .await
        .unwrap();
    assert_eq!(got.unwrap().unwrap(), Message::text("secret"));
    let first = tokio::time::timeout(Duration::from_secs(5), intruder.next())
        .await
        .unwrap();
    match first {
        Some(Ok(Message::Close(Some(frame)))) => {
            assert_eq!(u16::from(frame.code), 1008);
            assert_eq!(frame.reason.as_str(), "go away");
        }
        Some(Ok(Message::Text(t))) => panic!("a refused socket received {t:?}"),
        other => panic!("expected a close, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_client_messages_close_the_socket() {
    let (addr, _dir) = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/big"))
        .await
        .unwrap();
    ws.send(Message::text("small")).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert_eq!(echo.unwrap().unwrap(), Message::text("small"));
    // Over the 1 MiB message limit: the server closes (1009), it does not
    // buffer it.
    let _ = ws.send(Message::binary(vec![0u8; 2 << 20])).await;
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|f| u16::from(f.code)),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return None,
            }
        }
    })
    .await
    .expect("the socket stayed open after an oversized message");
    assert!(matches!(ended, Some(1009) | None), "{ended:?}");
}

/// A silent client is pinged, by default after 25 s, so the socket carries
/// traffic before a load balancer's usual idle timeout (60 s for nginx and
/// AWS ALB) cuts it. It used to wait 60 s, exactly that timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_client_is_pinged() {
    let default = ActorConfig::new("unused").websocket_ping_after;
    assert_eq!(default, Duration::from_secs(25));
    let (addr, _dir, _service) = start_configured(7, 256, |c| {
        c.websocket_ping_after = Duration::from_millis(200);
    })
    .await;
    ready_actor(addr, "quiet").await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/quiet"))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("never pinged");
    assert!(
        matches!(frame, Some(Ok(Message::Ping(_)))),
        "first frame: {frame:?}"
    );
}
