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
            let runtime = Runtime::new(&RuntimeConfig {
                max_instances: 16,
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
            for class in ["room", "edge", "directory", "relay"] {
                config.access.set(class, wasmkeep_server::Access::Internal);
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
                ["room", "edge", "directory", "relay"]
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
        assert!(
            clients.iter().all(|(_, _, t)| !t.is_finished()),
            "client task failed"
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
    loop {
        let head = publish(a.addr, room, false).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (s, map) = get(a.addr, &format!("/r6/map?room={room}")).await;
        if s == 200
            && map.split(',').count() >= 3
            && clients.iter().all(|(p, _, _)| {
                p.ready.load(Ordering::SeqCst) == 1 && p.last.load(Ordering::SeqCst) >= head
            })
        {
            break;
        }
        assert!(Instant::now() < deadline, "growth: {s} {map}");
    }
    assert!(
        clients
            .iter()
            .any(|(p, _, _)| p.moved.load(Ordering::SeqCst) > 0),
        "split must close 4801"
    );
    assert!(
        a.service.websockets() > 0 && b.service.websockets() > 0 && c.service.websockets() > 0,
        "sockets must span three actual nodes"
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
