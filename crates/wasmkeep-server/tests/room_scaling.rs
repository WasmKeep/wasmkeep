//! R4: four real sockets, split/rejoin by number, minute hysteresis and drain.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use futures_util::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, HttpComponent, ServeLimits};
#[path = "support/guests.rs"]
mod guests;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).await.unwrap();
    let (h, b) = raw.split_once("\r\n\r\n").unwrap();
    let status = h.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = String::new();
    if h.to_lowercase().contains("transfer-encoding: chunked") {
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
}
async fn frame(ws: &mut Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn join(addr: SocketAddr, id: &str, e: u64, n: u64) -> Socket {
    for _ in 0..50 {
        match tokio_tungstenite::connect_async(format!("ws://{addr}/join?c={id}&e={e}&n={n}")).await
        {
            Ok((mut ws, _)) => {
                let r = frame(&mut ws).await;
                assert!(r.to_text().unwrap().contains("ready"));
                return ws;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(40)).await,
        }
    }
    panic!("join failed")
}
fn point(key: &str) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in key.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^ (h >> 33)
}
fn consume(frame: Message, last: &mut u64) -> bool {
    match frame {
        Message::Close(Some(c)) => {
            assert_eq!(u16::from(c.code), 4801);
            true
        }
        Message::Text(s) => {
            let j: serde_json::Value = serde_json::from_str(&s).unwrap();
            if j["t"] == "m" {
                let first = j["n"].as_u64().unwrap();
                for n in first..first + j["m"].as_array().unwrap().len() as u64 {
                    if n > *last {
                        assert_eq!(n, *last + 1);
                        *last = n;
                    }
                }
            }
            false
        }
        _ => false,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_scaling_split_reconnect_and_merge() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front_wasm = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let rt = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&rt, &wasm);
    let front = HttpComponent::new(rt.clone(), &guests::compile(&rt, &front_wasm)).unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut cfg = ActorConfig::new(data.path());
    // Keep the epoch stable while testing scaling; crash/hibernate is covered separately.
    cfg.mailbox.idle_timeout = Duration::from_secs(120);
    for c in ["room", "pusher", "edge", "directory", "relay"] {
        cfg.access.set(c, wasmkeep_server::Access::Internal);
    }
    let svc = ActorService::new(
        rt,
        cfg,
        ["room", "pusher", "edge", "directory", "relay"]
            .into_iter()
            .map(|c| (c.into(), component.clone(), wasm.clone()))
            .collect(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve_with(
        listener,
        App {
            front: Some(front),
            actors: Some(svc),
        },
        async {
            let _ = stopped.await;
        },
        ServeLimits::default(),
    ));
    // The fixture merges after one second of hysteresis. Hold the merge for
    // as long as the test needs instead, and let it go when the test asks:
    // no assertion below races that second (the hysteresis rule itself is
    // `r4_hysteresis_and_pair_thresholds` in the SDK).
    assert_eq!(get(addr, "/hysteresis?ms=3600000").await.0, 200);
    let mut low = vec![];
    let mut high = vec![];
    for i in 0..100 {
        let id = format!("client{i}");
        if point(&id) < 1 << 63 {
            if low.len() < 2 {
                low.push(id);
            }
        } else if high.len() < 2 {
            high.push(id);
        }
        if low.len() == 2 && high.len() == 2 {
            break;
        }
    }
    let ids = [
        low[0].clone(),
        high[0].clone(),
        low[1].clone(),
        high[1].clone(),
    ];
    let mut sockets = vec![];
    for id in &ids[..3] {
        sockets.push(join(addr, id, 1, 0).await);
    }
    assert_eq!(get(addr, "/publish").await.0, 200);
    let mut last = [0u64; 4];
    for (i, ws) in sockets.iter_mut().enumerate() {
        while last[i] < 1 {
            assert!(!consume(frame(ws).await, &mut last[i]));
        }
    }
    sockets.push(join(addr, &ids[3], 1, 1).await);
    last[3] = 1;
    assert_eq!(get(addr, "/map").await.1, "1");
    assert_eq!(get(addr, "/publish").await.0, 200);
    for i in [0, 2] {
        while last[i] < 2 {
            assert!(!consume(frame(&mut sockets[i]).await, &mut last[i]));
        }
    }
    for i in [1, 3] {
        while !consume(frame(&mut sockets[i]).await, &mut last[i]) {}
    }
    assert_eq!(get(addr, "/map").await.1, "2");
    // Publish during the disconnection. Rejoin exclusively through the front and directory.
    assert_eq!(get(addr, "/publish").await.0, 200);
    for i in [1, 3] {
        sockets[i] = join(addr, &ids[i], 1, last[i]).await;
        let (status, h) = get(addr, &format!("/history?n={}", last[i])).await;
        assert_eq!(status, 200);
        for n in h
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|n| n.parse::<u64>().unwrap())
        {
            assert_eq!(n, last[i] + 1);
            last[i] = n;
        }
        assert_eq!(last[i], 3);
    }
    for i in [0, 2] {
        while last[i] < 3 {
            assert!(!consume(frame(&mut sockets[i]).await, &mut last[i]));
        }
    }
    assert_eq!(get(addr, "/publish").await.0, 200);
    for (i, ws) in sockets.iter_mut().enumerate() {
        while last[i] < 4 {
            assert!(!consume(frame(ws).await, &mut last[i]));
        }
    }
    // Leave one lower-half client: 1+2 sockets fits the 60% pair rule.
    sockets[2].close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(get(addr, "/publish").await.0, 200);
    assert_eq!(get(addr, "/map").await.1, "2");
    for i in [0, 1, 3] {
        while last[i] < 5 {
            assert!(!consume(frame(&mut sockets[i]).await, &mut last[i]));
        }
    }
    // The merge is held: still two edges after a publish, and the sockets are served.
    for i in [0, 1, 3] {
        sockets[i]
            .send(Message::Text(r#"{"t":"ping"}"#.into()))
            .await
            .unwrap();
        loop {
            let f = frame(&mut sockets[i]).await;
            let pong = f.to_text().is_ok_and(|s| s.contains("pong"));
            assert!(!consume(f, &mut last[i]));
            if pong {
                break;
            }
        }
    }
    // Let the merge go. Its round comes with the next publish, and the split's
    // own steps are done by then or finished first: the test waits below for
    // the map and the state, not for a time.
    assert_eq!(get(addr, "/hysteresis?ms=0").await.0, 200);
    assert_eq!(get(addr, "/publish").await.0, 200);
    while !consume(frame(&mut sockets[0]).await, &mut last[0]) {}
    for _ in 0..500 {
        if get(addr, "/map").await.1 == "1" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(get(addr, "/map").await.1, "1");
    for _ in 0..500 {
        if get(addr, "/state").await.1 == "1,0" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        get(addr, "/state").await.1,
        "1,0",
        "drained child must end and leave the push list"
    );
    sockets[0] = join(addr, &ids[0], 1, last[0]).await;
    let (status, h) = get(addr, &format!("/history?n={}", last[0])).await;
    assert_eq!(status, 200);
    for n in h
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|n| n.parse::<u64>().unwrap())
    {
        assert_eq!(n, last[0] + 1);
        last[0] = n;
    }
    assert_eq!(last[0], 6);
    for i in [1, 3] {
        while last[i] < 6 {
            assert!(!consume(frame(&mut sockets[i]).await, &mut last[i]));
        }
    }
    assert_eq!(get(addr, "/publish").await.0, 200);
    for i in [0, 1, 3] {
        while last[i] < 7 {
            assert!(!consume(frame(&mut sockets[i]).await, &mut last[i]));
        }
        sockets[i].close(None).await.unwrap();
    }
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}

/// Each task is a paced real browser-like socket with HTTP gap recovery.
async fn relay_client(
    addr: SocketAddr,
    id: String,
    last: std::sync::Arc<std::sync::atomic::AtomicU64>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    use std::sync::atomic::Ordering;
    loop {
        let n = last.load(Ordering::SeqCst);
        let connection = tokio::select! {
            _=&mut stop=>return,
            c=tokio_tungstenite::connect_async(format!("ws://{addr}/r5/join?c={id}&e=1&n={n}"))=>c,
        };
        if let Ok((mut ws, _)) = connection {
            let ready =
                tokio::select! {_=&mut stop=>{let _=ws.close(None).await;return;},f=ws.next()=>f};
            if matches!(ready,Some(Ok(Message::Text(ref t))) if t.contains("ready")) {
                let (status, h) = get(addr, &format!("/r5/history?n={n}")).await;
                assert_eq!(status, 200, "history: {h}");
                let mut cursor = n;
                for n in h
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|n| n.parse::<u64>().unwrap())
                {
                    assert_eq!(n, cursor + 1);
                    cursor = n;
                }
                last.store(cursor, Ordering::SeqCst);
                loop {
                    let f = tokio::select! {_=&mut stop=>{let _=ws.close(None).await;return;},f=ws.next()=>f};
                    match f {
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                        Some(Ok(f)) => {
                            let gap = if let Message::Text(ref text) = f {
                                let j: serde_json::Value = serde_json::from_str(text).unwrap();
                                j["t"] == "behind"
                                    || (j["t"] == "m" && j["n"].as_u64().unwrap() > cursor + 1)
                            } else {
                                false
                            };
                            if gap {
                                let (status, h) =
                                    get(addr, &format!("/r5/history?n={cursor}")).await;
                                assert_eq!(status, 200, "history: {h}");
                                for n in h
                                    .split(',')
                                    .filter(|s| !s.is_empty())
                                    .map(|n| n.parse::<u64>().unwrap())
                                {
                                    assert_eq!(n, cursor + 1);
                                    cursor = n;
                                }
                            }
                            assert!(!consume(f, &mut cursor));
                            last.store(cursor, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
        tokio::select! {_=&mut stop=>return,_=tokio::time::sleep(Duration::from_millis(100))=>{}}
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_relays_grow_deliver_and_retire() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front_wasm = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let rt = Runtime::new(&RuntimeConfig {
        max_instances: 64,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&rt, &wasm);
    let front = HttpComponent::new(rt.clone(), &guests::compile(&rt, &front_wasm)).unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut cfg = ActorConfig::new(data.path());
    // Keep the epoch stable while testing scaling; crash/hibernate is covered separately.
    cfg.mailbox.idle_timeout = Duration::from_secs(120);
    for c in ["room", "pusher", "edge", "directory", "relay"] {
        cfg.access.set(c, wasmkeep_server::Access::Internal);
    }
    let svc = ActorService::new(
        rt,
        cfg,
        ["room", "pusher", "edge", "directory", "relay"]
            .into_iter()
            .map(|c| (c.into(), component.clone(), wasm.clone()))
            .collect(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve_with(
        listener,
        App {
            front: Some(front),
            actors: Some(svc),
        },
        async {
            let _ = stopped.await;
        },
        ServeLimits::default(),
    ));

    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    // Thirty-four distinct coarse buckets; at capacity two every occupied pair splits.
    let mut ids = vec![None; 34];
    for i in 0..10000 {
        let id = format!("relay-client{i}");
        let bin = (point(&id) >> 58) as usize;
        if bin < 34 && ids[bin].is_none() {
            ids[bin] = Some(id);
        }
        if ids.iter().all(Option::is_some) {
            break;
        }
    }
    let mut clients = vec![];
    let mut head: u64;
    for id in ids {
        let last = Arc::new(AtomicU64::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(relay_client(addr, id.unwrap(), last.clone(), stopped));
        clients.push((last, stop, task));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (status, _) = get(addr, "/r5/publish").await;
        assert_eq!(status, 200);
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < deadline,
            "growth deadline: {}",
            get(addr, "/r5/state").await.1
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, n) = get(addr, "/r5/publish").await;
        head = n.parse().unwrap();
        let (_, state) = get(addr, "/r5/state").await;
        let counts: Vec<usize> = state.split(',').map(|s| s.parse().unwrap()).collect();
        if counts[0] > 16
            && counts[1] > 0
            && counts[2] == 0
            && clients
                .iter()
                .all(|(n, _, _)| n.load(Ordering::SeqCst) >= head.saturating_sub(1))
        {
            break;
        }
    }
    // Prove contiguous live delivery to every client after the layer is active.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, n) = get(addr, "/r5/publish").await;
        head = n.parse().unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while clients
        .iter()
        .any(|(n, _, _)| n.load(Ordering::SeqCst) < head)
    {
        assert!(
            Instant::now() < deadline,
            "every client must catch up through relays"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while clients.len() > 1 {
        let (_, stop, task) = clients.pop().unwrap();
        let _ = stop.send(());
        task.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < deadline,
            "retirement deadline: {}",
            get(addr, "/r5/state").await.1
        );
        let (_, n) = get(addr, "/r5/publish").await;
        head = n.parse().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, state) = get(addr, "/r5/state").await;
        let counts: Vec<usize> = state.split(',').map(|s| s.parse().unwrap()).collect();
        if counts[0] <= 12 && counts[1] == 0 && counts[2] == 0 {
            break;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while clients[0].0.load(Ordering::SeqCst) < head {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (_, done, task) = clients.pop().unwrap();
    let _ = done.send(());
    task.await.unwrap();
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
/// ADR 0040, section 13: a group as a 0.6.0 room left it (the room the
/// controller: its scale rows, the map's owner and the edges' room) moves
/// to its pusher, and its sockets receive the next publish from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_scaling_a_0_6_0_room_moves_to_its_pusher() {
    let wasm = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    let front_wasm = guests::build_guest("tests/guests/room-scaling-front", "room_scaling_front");
    let rt = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&rt, &wasm);
    let front = HttpComponent::new(rt.clone(), &guests::compile(&rt, &front_wasm)).unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut cfg = ActorConfig::new(data.path());
    cfg.mailbox.idle_timeout = Duration::from_secs(120);
    for c in ["room", "pusher", "edge", "directory", "relay"] {
        cfg.access.set(c, wasmkeep_server::Access::Internal);
    }
    let svc = ActorService::new(
        rt,
        cfg,
        ["room", "pusher", "edge", "directory", "relay"]
            .into_iter()
            .map(|c| (c.into(), component.clone(), wasm.clone()))
            .collect(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve_with(
        listener,
        App {
            front: Some(front),
            actors: Some(svc),
        },
        async {
            let _ = stopped.await;
        },
        ServeLimits::default(),
    ));
    let (status, body) = get(addr, "/r6/legacy-boot?room=legacy").await;
    assert_eq!(status, 200, "{body}");
    let (_, owner) = get(addr, "/r6/owner?room=legacy").await;
    assert!(
        owner.ends_with("/room/legacy"),
        "the 0.6.0 room owns the map: {owner}"
    );
    let (_, rows) = get(addr, "/r6/legacy-rows?room=legacy").await;
    assert!(rows.contains("wasmkeep_room_scale"), "{rows}");
    // A client joins through the old map; its poke wakes the pusher.
    let mut ws = None;
    for _ in 0..50 {
        if let Ok((mut s, _)) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/r6/join?room=legacy&c=old&n=0"))
                .await
        {
            assert!(frame(&mut s).await.to_text().unwrap().contains("ready"));
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut ws = ws.expect("join");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, owner) = get(addr, "/r6/owner?room=legacy").await;
        let (_, rows) = get(addr, "/r6/legacy-rows?room=legacy").await;
        if owner.ends_with("/pusher/legacy") && rows.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the move: owner {owner}, rows {rows}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The edge now takes the pusher's pushes (its rewritten configuration parses).
    let (status, n) = get(addr, "/r6/publish?room=legacy&id=after-move").await;
    assert_eq!(status, 200, "{n}");
    let n: u64 = n.parse().unwrap();
    let mut last = n - 1;
    while last < n {
        assert!(!consume(frame(&mut ws).await, &mut last));
    }
    let (_, children) = get(addr, "/r6/children?room=legacy").await;
    let (_, map) = get(addr, "/r6/map?room=legacy").await;
    assert_eq!(children, map, "the adopted children are the map's");
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
