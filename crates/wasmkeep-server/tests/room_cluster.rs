//! ADR 0040 R6. Real in-process cluster, sockets and bucket ownership.
//! Separate runtimes let the test kill a node without a graceful release.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use futures_util::{SinkExt, StreamExt};
use object_store::{ObjectStore, memory::InMemory};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::tungstenite::Message;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, ClusterConfig, HttpComponent, LeaseConfig};
#[path = "support/fuel.rs"]
mod fuel;
#[path = "support/guests.rs"]
mod guests;
struct Node {
    addr: SocketAddr,
    service: ActorService,
    kill: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    _data: tempfile::TempDir,
}
impl Node {
    fn kill(&mut self) {
        let _ = self.kill.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap();
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if self.kill.is_some() {
            self.kill();
        }
    }
}
async fn node(
    id: &'static str,
    store: Arc<dyn ObjectStore>,
    wasm: Vec<u8>,
    front: Vec<u8>,
) -> Node {
    node_with(id, store, wasm, front, false).await
}
/// [`node`], its pusher class batched (ADR 0034) when `batched`.
async fn node_with(
    id: &'static str,
    store: Arc<dyn ObjectStore>,
    wasm: Vec<u8>,
    front: Vec<u8>,
    batched: bool,
) -> Node {
    let data = tempfile::tempdir().unwrap();
    let path = data.path().to_owned();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (kill, killed) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            // Fuel per event (independent of CPU speed): the pusher tests
            // bound every room event, the 513-row pull included.
            let runtime = Runtime::new(&RuntimeConfig {
                max_instances: 16,
                measure_fuel: true,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let component = guests::compile(&runtime, &wasm);
            let front =
                HttpComponent::new(runtime.clone(), &guests::compile(&runtime, &front)).unwrap();
            let mut config = ActorConfig::new(path);
            config.mailbox.idle_timeout = Duration::from_secs(180);
            // The slow client never reads, so it never answers the server's
            // pings: with the default ping timings its edge closes it 1001
            // (idle) once it has been silent for 55 s, and on a slow runner
            // the 350 rounds before it speaks take longer than that. This
            // test is about the 4802 slow close, not the idle one.
            config.websocket_ping_after = Duration::from_secs(600);
            for class in ["room", "pusher", "edge", "directory", "relay"] {
                config.access.set(class, wasmkeep_server::Access::Internal);
            }
            if batched {
                config.batched.insert("pusher".into());
            }
            let mut cluster = ClusterConfig::new(id, format!("http://{addr}"), store);
            cluster.lease = LeaseConfig::new(2000, 400, 200, 100).unwrap();
            cluster.heartbeat = Duration::from_millis(200);
            cluster.alarm_sweep = Duration::from_millis(200);
            cluster.failover_wait = Some(Duration::ZERO);
            // The test places edges itself (prewarmed on the nodes it names,
            // below). Placement at wake-up (ADR 0041, newer than this test)
            // may send an edge that wakes again on a loaded runner to a
            // lighter node, and the sockets then miss a node.
            cluster.placement = false;
            config.cluster = Some(cluster);
            let service = ActorService::new(
                runtime,
                config,
                ["room", "pusher", "edge", "directory", "relay"]
                    .into_iter()
                    .map(|c| (c.into(), component.clone(), wasm.clone()))
                    .collect(),
            )
            .unwrap();
            service.node_id_checked().await.unwrap();
            tokio::spawn(wasmkeep_server::serve(
                listener,
                App {
                    front: Some(front),
                    actors: Some(service.clone()),
                },
                std::future::pending(),
            ));
            started.send((addr, service)).ok().unwrap();
            let _ = killed.await;
        });
        // No service.shutdown, drain or lease release. All I/O and background
        // tasks stop with this runtime, just as on process death.
        rt.shutdown_timeout(Duration::from_secs(2));
    });
    let (addr, service) = ready.await.unwrap();
    Node {
        addr,
        service,
        kill: Some(kill),
        thread: Some(thread),
        _data: data,
    }
}
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    // Bound concurrent HTTP catch-up after a split; no busy request storm.
    static HTTP: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);
    let _permit = HTTP.acquire().await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let mut raw = String::new();
        s.read_to_string(&mut raw).await.unwrap();
        let (h, b) = raw.split_once("\r\n\r\n").unwrap();
        let status = h.split_whitespace().nth(1).unwrap().parse().unwrap();
        let mut body = String::new();
        if h.to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            let mut rest = b;
            loop {
                let (n, tail) = rest.split_once("\r\n").unwrap();
                let n = usize::from_str_radix(n, 16).unwrap();
                if n == 0 {
                    break;
                }
                body.push_str(&tail[..n]);
                rest = &tail[n + 2..];
            }
        } else {
            body = b.into();
        }
        (status, body)
    })
    .await
    .unwrap()
}
async fn history(addr: SocketAddr, room: &str, last: &mut u64) {
    for _ in 0..100 {
        let (status, h) = get(addr, &format!("/r6/history?room={room}&n={last}")).await;
        if status != 200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if h.is_empty() {
            return;
        }
        for n in h.split(',').map(|n| n.parse::<u64>().unwrap()) {
            assert_eq!(n, *last + 1, "HTTP gap");
            *last = n;
        }
    }
    panic!("history did not finish")
}
fn apply(f: &str, last: &mut u64, allow_repeat: bool) -> bool {
    let j: serde_json::Value = serde_json::from_str(f).unwrap();
    if j["t"] == "behind" {
        return true;
    }
    if j["t"] == "m" {
        assert_eq!(j["e"], 1, "only the edge node dies");
        let first = j["n"].as_u64().unwrap();
        if first > *last + 1 {
            return true;
        }
        for n in first..first + j["m"].as_array().unwrap().len() as u64 {
            if n <= *last {
                assert!(allow_repeat, "repeat without recovery");
                continue;
            }
            assert_eq!(n, *last + 1);
            *last = n;
        }
    }
    false
}
/// Why an actor may have started cold (a new room epoch): instances that
/// left memory, and leases lost or fenced, from the process's metrics.
fn restarts() -> String {
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    page.lines()
        .filter(|l| {
            !l.starts_with('#')
                && (l.starts_with("wasmkeep_hibernations_total")
                    || l.contains("lease_lost")
                    || l.contains("fenced"))
        })
        .collect::<Vec<_>>()
        .join("; ")
}
#[derive(Default)]
struct Progress {
    last: AtomicU64,
    ready: AtomicU64,
    moved: AtomicU64,
    reconnects: AtomicU64,
    gaps: AtomicU64,
}
async fn client(
    addr: SocketAddr,
    room: &'static str,
    id: String,
    p: Arc<Progress>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let mut last = 0;
    let mut attempts = 0u32;
    loop {
        let connection = tokio::select! { _=&mut stop=>return, c=tokio_tungstenite::connect_async(format!("ws://{addr}/r6/join?room={room}&c={id}&n={last}"))=>c };
        if let Ok((mut ws, _)) = connection {
            let mut recovering = true;
            loop {
                let f = tokio::select! { _=&mut stop=>{let _=ws.close(None).await;return;}, f=ws.next()=>f };
                match f {
                    Some(Ok(Message::Text(s))) => {
                        if s.contains("\"t\":\"ready\"") {
                            history(addr, room, &mut last).await;
                            p.gaps.fetch_add(1, Ordering::SeqCst);
                            p.ready.store(1, Ordering::SeqCst);
                            attempts = 0;
                        } else if apply(&s, &mut last, recovering) {
                            history(addr, room, &mut last).await;
                            p.gaps.fetch_add(1, Ordering::SeqCst);
                            if serde_json::from_str::<serde_json::Value>(&s).unwrap()["t"] == "m" {
                                assert!(!apply(&s, &mut last, true));
                            }
                            recovering = true;
                        } else {
                            // Buffered repeats after a rejoin are allowed, but a
                            // subsequent fresh live frame ends that allowance.
                            let j: serde_json::Value = serde_json::from_str(&s).unwrap();
                            if j["t"] == "m" && j["n"].as_u64().unwrap() == last {
                                recovering = false;
                            }
                        }
                        p.last.store(last, Ordering::SeqCst);
                    }
                    Some(Ok(Message::Close(Some(c)))) => {
                        assert!(matches!(u16::from(c.code), 4801 | 1012));
                        if u16::from(c.code) == 4801 {
                            p.moved.fetch_add(1, Ordering::SeqCst);
                        }
                        break;
                    }
                    None | Some(Err(_)) | Some(Ok(Message::Close(None))) => break,
                    _ => {}
                }
            }
        }
        // Not ready until this client is connected again: the growth loop
        // waits for every client to hold a socket, not for one it held.
        p.ready.store(0, Ordering::SeqCst);
        p.reconnects.fetch_add(1, Ordering::SeqCst);
        // Deterministic per-client full jitter, bounded to avoid a retry herd.
        attempts = attempts.saturating_add(1);
        let hash = id.bytes().fold(u64::from(attempts), |a, b| {
            a.wrapping_mul(31) + u64::from(b)
        });
        let delay = hash % (500u64 * 2u64.pow(attempts.min(6))).min(30000);
        tokio::select! {_=&mut stop=>return,_=tokio::time::sleep(Duration::from_millis(delay))=>{}}
    }
}
async fn publish(addr: SocketAddr, room: &str, large: bool) -> u64 {
    static ID: AtomicU64 = AtomicU64::new(0);
    let id = ID.fetch_add(1, Ordering::SeqCst);
    for attempt in 0..30 {
        let (status, body) = get(
            addr,
            &format!(
                "/r6/publish?room={room}&id={id}{}",
                if large { "&large=1" } else { "" }
            ),
        )
        .await;
        if status == 200 {
            return body.parse().unwrap();
        }
        assert!(matches!(status, 429 | 503), "publish: {status} {body}");
        tokio::time::sleep(Duration::from_millis(1000 + attempt * 37)).await;
    }
    panic!("publish retry deadline")
}
async fn caught_up(
    clients: &[(
        Arc<Progress>,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    )],
    head: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(40);
    while clients
        .iter()
        .any(|(p, _, _)| p.last.load(Ordering::SeqCst) < head)
    {
        // A client stops on an epoch it did not expect: say whether an
        // event trapped on its CPU budget (a room restart) or not.
        assert!(
            clients.iter().all(|(_, _, t)| !t.is_finished()),
            "client task failed; CPU-budget traps so far: {}; {}",
            wasmkeep_runtime::cpu_budget_exceeded_total(),
            restarts()
        );
        assert!(
            Instant::now() < deadline,
            "delivery deadline at {head}: {:?}",
            clients
                .iter()
                .map(|(p, _, _)| p.last.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn exercise(count: usize) {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = node("a", store.clone(), wasm.clone(), front.clone()).await;
    let mut b = node("b", store.clone(), wasm.clone(), front.clone()).await;
    let c = node("c", store.clone(), wasm, front).await;
    let room = if count == 300 { "big" } else { "small" };
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Placement is outside this ADR: prewarm names on the desired nodes,
    // before the SDK's normal init/split creates their ranges.
    for (addr, edge) in [(a.addr, 1), (b.addr, 3), (c.addr, 5), (c.addr, 7)] {
        assert_eq!(
            get(addr, &format!("/r6/warm?room={room}&edge={edge}"))
                .await
                .0,
            200
        );
    }
    let mut clients = vec![];
    for i in 0..count - 1 {
        let p = Arc::new(Progress::default());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(client(
            a.addr,
            room,
            format!("client{i}"),
            p.clone(),
            stopped,
        ));
        clients.push((p, stop, task));
        tokio::time::sleep(Duration::from_millis(35)).await;
        if i % 10 == 0 {
            publish(a.addr, room, false).await;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(40);
    let map = loop {
        let head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (s, map) = get(a.addr, &format!("/r6/map?room={room}")).await;
        if s == 200
            && map.split(',').count() >= 3
            && clients.iter().all(|(p, _, _)| {
                p.ready.load(Ordering::SeqCst) == 1 && p.last.load(Ordering::SeqCst) >= head
            })
        {
            break map;
        }
        assert!(Instant::now() < deadline, "growth: {s} {map}");
    };
    assert!(
        clients
            .iter()
            .any(|(p, _, _)| p.moved.load(Ordering::SeqCst) > 0),
        "split must close 4801"
    );
    let spread = [&a, &b, &c].map(|n| (n.service.websockets(), n.service.resident()));
    assert!(
        spread.iter().all(|(sockets, _)| *sockets > 0),
        "sockets must span three actual nodes: edges {map}, (sockets, resident) on a, b, c {spread:?}"
    );
    // The last of the 300 clients joins with a constrained receive window,
    // then stops reading. Other clients drain continuously.
    let head = publish(a.addr, room, false).await;
    caught_up(&clients, head).await;
    let slow_start = head;
    let tcp = tokio::net::TcpSocket::new_v4().unwrap();
    tcp.set_recv_buffer_size(1024).unwrap();
    let stream = tcp.connect(a.addr).await.unwrap();
    let (mut slow, _) = tokio_tungstenite::client_async(
        format!("ws://{}/r6/join?room={room}&c=slow2&n={head}", a.addr),
        stream,
    )
    .await
    .unwrap();
    assert!(
        slow.next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .contains("ready")
    );
    let before: Vec<_> = clients
        .iter()
        .map(|(p, _, _)| p.reconnects.load(Ordering::SeqCst))
        .collect();
    b.kill();
    assert!(
        TcpStream::connect(b.addr).await.is_err(),
        "dead node still accepts"
    );
    let head = publish(a.addr, room, false).await;
    caught_up(&clients, head).await;
    assert!(
        clients
            .iter()
            .zip(before)
            .any(|((p, _, _), n)| p.reconnects.load(Ordering::SeqCst) > n),
        "death must reconnect clients"
    );
    let mut head = head;
    for round in 0..450 {
        if round == 350 {
            for _ in 0..260 {
                slow.send(Message::Text(r#"{"t":"ping"}"#.into()))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        head = publish(a.addr, room, true).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    caught_up(&clients, head).await;
    let code = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match slow.next().await {
                Some(Ok(Message::Close(Some(f)))) => break u16::from(f.code),
                Some(Ok(_)) => {}
                other => panic!("slow socket lacks close: {other:?}"),
            }
        }
    })
    .await
    .expect("slow socket did not close");
    assert_eq!(code, 4802);
    drop(slow);
    let (mut resumed, _) = tokio_tungstenite::connect_async(format!(
        "ws://{}/r6/join?room={room}&c=slow2&n={slow_start}",
        a.addr
    ))
    .await
    .unwrap();
    assert!(
        resumed
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .contains("ready")
    );
    let mut last = slow_start;
    history(a.addr, room, &mut last).await;
    assert_eq!(last, head);
    head = publish(a.addr, room, false).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while last < head {
            if let Some(Ok(Message::Text(s))) = resumed.next().await {
                assert!(!apply(&s, &mut last, true));
            }
        }
    })
    .await
    .unwrap();
    caught_up(&clients, head).await;
    resumed.close(None).await.unwrap();
    for (p, stop, task) in clients {
        assert_eq!(p.last.load(Ordering::SeqCst), head);
        let _ = stop.send(());
        task.await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_30_clients() {
    exercise(30).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_300_clients() {
    exercise(300).await;
}

// ADR 0040, section 13: the pusher, in its own class, on a real cluster.

/// Every `(e, n)` one socket was handed, in order.
type Seen = Arc<std::sync::Mutex<Vec<(u64, u64)>>>;
/// One socket that never reconnects: every `(e, n)` it was handed, repeats
/// in the same epoch dropped (the client's rule 1).
fn listener(addr: SocketAddr, room: &'static str, id: &str) -> (Seen, tokio::task::JoinHandle<()>) {
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let url = format!("ws://{addr}/r6/join?room={room}&c={id}&n=0");
    let out = seen.clone();
    let task = tokio::spawn(async move {
        let mut ws = None;
        for _ in 0..100 {
            if let Ok((s, _)) = tokio_tungstenite::connect_async(&url).await {
                ws = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mut ws = ws.expect("join");
        let mut last = (0u64, 0u64);
        while let Some(Ok(Message::Text(s))) = ws.next().await {
            let j: serde_json::Value = serde_json::from_str(&s).unwrap();
            // Before its first message a socket may start behind; after it, no hole.
            assert!(
                j["t"] != "behind" || out.lock().unwrap().is_empty(),
                "a hole: {s}"
            );
            if j["t"] != "m" {
                continue;
            }
            let e = j["e"].as_u64().unwrap();
            let first = j["n"].as_u64().unwrap();
            for n in first..first + j["m"].as_array().unwrap().len() as u64 {
                if e == last.0 && n <= last.1 {
                    continue;
                }
                // The first message is where this socket starts; then no gap.
                let started = out.lock().unwrap().is_empty();
                assert!(
                    started || (e >= last.0 && n == last.1 + 1),
                    "{e}.{n} after {last:?}"
                );
                last = (e, n);
                out.lock().unwrap().push(last);
            }
        }
    });
    (seen, task)
}
async fn heard(seen: &[Seen], head: u64, within: Duration) {
    let deadline = Instant::now() + within;
    while seen
        .iter()
        .any(|s| s.lock().unwrap().last().map_or(0, |p| p.1) < head)
    {
        assert!(
            Instant::now() < deadline,
            "delivery deadline at {head}: {:?}",
            seen.iter()
                .map(|s| s.lock().unwrap().last().copied())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
/// The pusher alone on a node that dies while rounds run: the room keeps
/// numbering, the pusher wakes on a live node from its durable state (its
/// children are the directory's), and every client catches up in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_pusher_node_dies_mid_round() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = node("a", store.clone(), wasm.clone(), front.clone()).await;
    let mut b = node("b", store.clone(), wasm.clone(), front.clone()).await;
    let c = node("c", store.clone(), wasm, front).await;
    let room = "small";
    tokio::time::sleep(Duration::from_millis(500)).await;
    // The directory and the room on a, the edges on a and c, the pusher on b.
    get(a.addr, "/r6/map?room=small").await;
    for (addr, edge) in [(a.addr, 1), (c.addr, 3)] {
        assert_eq!(
            get(addr, &format!("/r6/warm?room={room}&edge={edge}"))
                .await
                .0,
            200
        );
    }
    assert_eq!(get(a.addr, "/r6/history?room=small&n=0").await.0, 200);
    assert_eq!(get(b.addr, "/r6/warm-pusher?room=small").await.0, 200);
    let mut clients = vec![];
    for i in 0..16 {
        let p = Arc::new(Progress::default());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(client(
            a.addr,
            room,
            format!("client{i}"),
            p.clone(),
            stopped,
        ));
        clients.push((p, stop, task));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let head = publish(a.addr, room, false).await;
    caught_up(&clients, head).await;
    // A stream of publishes keeps rounds going while the pusher's node dies.
    let addr = a.addr;
    let stream = tokio::spawn(async move {
        let mut head = 0;
        for _ in 0..60 {
            head = publish(addr, room, false).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        head
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    b.kill();
    let streamed = stream.await.unwrap();
    let head = publish(a.addr, room, false).await;
    assert!(head > streamed);
    caught_up(&clients, head).await;
    // The pusher woke cold on a live node: its durable children are the map's.
    let (status, children) = get(a.addr, "/r6/children?room=small").await;
    assert_eq!(status, 200, "{children}");
    let (_, map) = get(a.addr, "/r6/map?room=small").await;
    let mut map: Vec<_> = map.split(',').collect();
    map.sort_unstable();
    assert_eq!(children, map.join(","));
    for (p, stop, task) in clients {
        assert_eq!(p.last.load(Ordering::SeqCst), head);
        let _ = stop.send(());
        task.await.unwrap();
    }
    // No event trapped and every room event (pulls, publishes, history)
    // stayed under the fuel the 50 ms budget is calibrated for.
    fuel::assert_ceiling(500_000_000);
}
/// A room event that traps discards the instance: the next operation opens
/// an epoch, the pusher's pulls carry it, and the clients go on in order
/// with nothing void (the trap lost no committed write).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_room_trap_opens_an_epoch() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = node("a", store, wasm, front).await;
    let room = "small";
    assert_eq!(
        get(a.addr, &format!("/r6/warm?room={room}&edge=1")).await.0,
        200
    );
    let mut seen = vec![];
    let mut tasks = vec![];
    for i in 0..6 {
        let (s, t) = listener(a.addr, room, &format!("trap{i}"));
        seen.push(s);
        tasks.push(t);
    }
    // Every socket is connected once the first publish reaches them all.
    let mut head = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen.iter().any(|s| s.lock().unwrap().is_empty()) {
        assert!(Instant::now() < deadline, "joins");
        head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    heard(&seen, head, Duration::from_secs(10)).await;
    let (status, _) = get(a.addr, "/r6/trap?room=small").await;
    assert_ne!(status, 200, "the room trapped");
    for _ in 0..3 {
        head = publish(a.addr, room, false).await;
    }
    heard(&seen, head, Duration::from_secs(10)).await;
    for s in &seen {
        let s = s.lock().unwrap();
        assert_eq!(s.last().unwrap().0, 2, "the trap opened epoch 2: {s:?}");
        assert!(s.iter().any(|p| p.0 == 1));
    }
    for t in tasks {
        t.abort();
    }
}
/// Stage 1 still blocks a round on its slowest child, but only the pusher
/// waits: publishes are the room's and are acknowledged at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_publish_acknowledged_while_a_round_is_held_by_a_slow_edge() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = node("a", store, wasm, front).await;
    let room = "small";
    assert_eq!(
        get(a.addr, &format!("/r6/warm?room={room}&edge=1")).await.0,
        200
    );
    let (seen, task) = listener(a.addr, room, "patient");
    let mut head = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "join");
        head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    heard(std::slice::from_ref(&seen), head, Duration::from_secs(10)).await;
    // Every push to the edge now takes 3 s: each round is held that long.
    let (status, body) = get(a.addr, "/r6/slow?room=small&edge=1&ms=3000").await;
    assert_eq!(status, 200, "{body}");
    let held = Instant::now();
    head = publish(a.addr, room, false).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut slowest = Duration::ZERO;
    for _ in 0..5 {
        let t = Instant::now();
        head = publish(a.addr, room, false).await;
        slowest = slowest.max(t.elapsed());
    }
    assert!(
        slowest < Duration::from_millis(1000),
        "a publish waited {slowest:?} behind a held round"
    );
    heard(std::slice::from_ref(&seen), head, Duration::from_secs(30)).await;
    assert!(
        held.elapsed() > Duration::from_millis(2500),
        "the round was not held: the test proves nothing"
    );
    task.abort();
}
/// A quiet room loses its pusher's node; one publish comes during the
/// takeover and nothing after it. Its poke fails, yet the message is
/// delivered: an idle pusher whose group holds sockets keeps a durable
/// alarm (3 s for this room, 60 s by default), which the sweep delivers on
/// a live node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_quiet_room_delivers_a_publish_made_during_the_pusher_takeover() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = node("a", store.clone(), wasm.clone(), front.clone()).await;
    let mut b = node("b", store.clone(), wasm.clone(), front.clone()).await;
    let _c = node("c", store.clone(), wasm, front).await;
    let room = "quiet";
    tokio::time::sleep(Duration::from_millis(500)).await;
    get(a.addr, "/r6/map?room=quiet").await;
    assert_eq!(get(a.addr, "/r6/warm?room=quiet&edge=1").await.0, 200);
    assert_eq!(get(a.addr, "/r6/history?room=quiet&n=0").await.0, 200);
    assert_eq!(get(b.addr, "/r6/warm-pusher?room=quiet").await.0, 200);
    let mut clients = vec![];
    for i in 0..2 {
        let p = Arc::new(Progress::default());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(client(
            a.addr,
            room,
            format!("quiet{i}"),
            p.clone(),
            stopped,
        ));
        clients.push((p, stop, task));
    }
    let mut head = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while clients.iter().any(|(p, _, _)| {
        p.ready.load(Ordering::SeqCst) == 0 || p.last.load(Ordering::SeqCst) < head.max(1)
    }) {
        assert!(Instant::now() < deadline, "joins");
        head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    caught_up(&clients, head).await;
    // Quiet past the safety round: only the idle alarm stands.
    tokio::time::sleep(Duration::from_millis(6500)).await;
    b.kill();
    // One publish during the takeover, then nothing.
    let head = publish(a.addr, room, false).await;
    caught_up(&clients, head).await;
    for (p, stop, task) in clients {
        assert_eq!(p.last.load(Ordering::SeqCst), head);
        let _ = stop.send(());
        task.await.unwrap();
    }
}

/// A bucket that, while `hold` is set, never answers the pusher's segment
/// uploads (`actors/<tenant>/pusher/.../wal/`): they stay in flight, and
/// the pusher's alarm moves stay unacknowledged. `held` counts them.
#[derive(Debug)]
struct HoldPusherSegments {
    inner: Arc<dyn ObjectStore>,
    hold: Arc<std::sync::atomic::AtomicBool>,
    held: Arc<AtomicU64>,
}
impl std::fmt::Display for HoldPusherSegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HoldPusherSegments")
    }
}
#[async_trait::async_trait]
impl ObjectStore for HoldPusherSegments {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let key = location.as_ref();
        if self.hold.load(Ordering::SeqCst) && key.contains("/pusher/") && key.contains("/wal/") {
            self.held.fetch_add(1, Ordering::SeqCst);
            return std::future::pending().await;
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// ADR 0040, section 13: the pusher is deployed batched. Its node dies
/// with the pusher's last alarm moves unacknowledged (its segments held in
/// flight) and a scale operation under way (a split whose `range` step the
/// edge refuses). A publish whose poke is lost waits undelivered; with no
/// publish after it, the clients get it within 60 s (the last acknowledged
/// alarm, which the sweep delivers on a live node), and the operation is
/// the same one, which then completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_cluster_batched_pusher_crash_keeps_its_operation_and_delivers() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let held = Arc::new(AtomicU64::new(0));
    let b_store: Arc<dyn ObjectStore> = Arc::new(HoldPusherSegments {
        inner: shared.clone(),
        hold: hold.clone(),
        held: held.clone(),
    });
    let a = node_with("a", shared.clone(), wasm.clone(), front.clone(), true).await;
    let mut b = node_with("b", b_store, wasm.clone(), front.clone(), true).await;
    let _c = node_with("c", shared.clone(), wasm, front, true).await;
    let room = "quiet";
    tokio::time::sleep(Duration::from_millis(500)).await;
    get(a.addr, "/r6/map?room=quiet").await;
    assert_eq!(get(a.addr, "/r6/warm?room=quiet&edge=1").await.0, 200);
    assert_eq!(get(a.addr, "/r6/history?room=quiet&n=0").await.0, 200);
    assert_eq!(get(b.addr, "/r6/warm-pusher?room=quiet").await.0, 200);
    // The split six sockets call for (capacity 5) sticks at its `range` step.
    let (status, body) = get(a.addr, "/r6/refuse-range?room=quiet&edge=1&on=1").await;
    assert_eq!(status, 200, "{body}");
    let mut clients = vec![];
    for i in 0..6 {
        let p = Arc::new(Progress::default());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(client(
            a.addr,
            room,
            format!("batched{i}"),
            p.clone(),
            stopped,
        ));
        clients.push((p, stop, task));
    }
    let mut head;
    let deadline = Instant::now() + Duration::from_secs(30);
    let op = loop {
        assert!(Instant::now() < deadline, "no scale operation began");
        head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (status, op) = get(a.addr, "/r6/op?room=quiet").await;
        assert_eq!(status, 200, "{op}");
        if !op.is_empty() {
            break op;
        }
    };
    caught_up(&clients, head).await;
    // From now on the pusher's segments stay in flight: a publish's round
    // still delivers, then its unsent writes age and its events stop.
    hold.store(true, Ordering::SeqCst);
    head = publish(a.addr, room, false).await;
    caught_up(&clients, head).await;
    tokio::time::sleep(Duration::from_millis(3000)).await;
    assert!(
        held.load(Ordering::SeqCst) > 0,
        "no pusher segment was held"
    );
    let (status, n) = get(a.addr, "/r6/publish-unpoked?room=quiet").await;
    assert_eq!(status, 200, "{n}");
    let head: u64 = n.parse().unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        clients
            .iter()
            .all(|(p, _, _)| p.last.load(Ordering::SeqCst) < head),
        "delivered before the crash: the test proves nothing"
    );
    b.kill();
    // No publish from here on.
    let deadline = Instant::now() + Duration::from_secs(60);
    while clients
        .iter()
        .any(|(p, _, _)| p.last.load(Ordering::SeqCst) < head)
    {
        assert!(
            Instant::now() < deadline,
            "not delivered within 60 s: {:?}",
            clients
                .iter()
                .map(|(p, _, _)| p.last.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!(
        "delivered {:?} after the kill, {} pusher segments held",
        Duration::from_secs(60) - deadline.saturating_duration_since(Instant::now()),
        held.load(Ordering::SeqCst)
    );
    // The operation survived: the same one, still pending, then done.
    let (status, after) = get(a.addr, "/r6/op?room=quiet").await;
    assert_eq!((status, after.as_str()), (200, op.as_str()));
    let (status, body) = get(a.addr, "/r6/refuse-range?room=quiet&edge=1&on=0").await;
    assert_eq!(status, 200, "{body}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !get(a.addr, "/r6/op?room=quiet").await.1.is_empty() {
        assert!(Instant::now() < deadline, "the operation did not complete");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (_, children) = get(a.addr, "/r6/children?room=quiet").await;
    let (_, map) = get(a.addr, "/r6/map?room=quiet").await;
    let mut map: Vec<_> = map.split(',').collect();
    map.sort_unstable();
    assert_eq!(children, map.join(","));
    assert_eq!(children.split(',').count(), 2, "{children}");
    for (_, stop, task) in clients {
        let _ = stop.send(());
        task.await.unwrap();
    }
}
