//! ADR 0040 R3: one room, one edge, three clients; no load generator.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::tungstenite::Message;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};
#[path = "support/fuel.rs"]
mod fuel;
#[path = "support/guests.rs"]
mod guests;
async fn request(addr: SocketAddr, path: &str) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_edge_slow_socket_and_restart() {
    // Warnings name the cause of a red (an actor's trap, a retried alarm).
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep_server=warn,wasmkeep_runtime=warn")
        .with_test_writer()
        .try_init();
    let wasm = guests::build_guest("../wasmkeep-sdk/tests/room-edge-guest", "room_edge_guest");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, &wasm);
    let dir = tempfile::tempdir().unwrap();
    // One node on `dir` whose actors hibernate after `idle`.
    let node = |idle: Duration| {
        let mut config = ActorConfig::new(dir.path());
        config.mailbox.idle_timeout = idle;
        let svc = ActorService::new(
            runtime.clone(),
            config,
            vec![
                ("t".into(), component.clone(), wasm.clone()),
                ("pusher".into(), component.clone(), wasm.clone()),
                ("edge".into(), component.clone(), wasm.clone()),
            ],
        )
        .unwrap();
        async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (stop, stopped) = oneshot::channel::<()>();
            let server = tokio::spawn(wasmkeep_server::serve(listener, svc.clone(), async {
                let _ = stopped.await;
            }));
            (svc, addr, stop, server)
        }
    };
    // A short idle timeout, for the cold wake below.
    let (svc, addr, stop, server) = node(Duration::from_millis(300)).await;
    let warm = request(addr, "/actor/t/driver/r3-start").await;
    assert!(warm.starts_with("HTTP/1.1 200"), "{warm}");
    let (mut ws, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/edge/e/?c=one&p=0000000000000000"
    ))
    .await
    .unwrap();
    let ready = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(ready.to_text().unwrap().contains("ready"));
    // A real cold wake, proved by resident count, while the socket survives.
    tokio::time::timeout(Duration::from_secs(5), async {
        while svc.resident() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actors did not hibernate");
    ws.close(None).await.unwrap();
    drop(ws);
    // The rest restarts the node on the same data, with an idle timeout no
    // stall of a loaded runner reaches: an actor that hibernated mid-stream
    // would forget its cursors, and the edge would rightly skip what its
    // sockets missed (ADR 0040, "After a hibernation"). Every skip, repeat or
    // reorder below is then a delivery fault.
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    drop(svc);
    let (_svc, addr, stop, server) = node(Duration::from_secs(60)).await;
    let warm = request(addr, "/actor/t/driver/r3-start").await;
    assert!(warm.starts_with("HTTP/1.1 200"), "{warm}");
    let (mut one, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/edge/e/?c=one&p=0000000000000000&e=1&n=0"
    ))
    .await
    .unwrap();
    assert!(
        one.next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .contains("ready")
    );
    let (mut two, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/edge/e/?c=two&p=0000000000000001"
    ))
    .await
    .unwrap();
    assert!(
        two.next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .contains("ready")
    );
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(64 * 1024).unwrap();
    let stream = socket.connect(addr).await.unwrap();
    let (mut slow, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/actor/edge/e/?c=slow&p=0000000000000002"),
        stream,
    )
    .await
    .unwrap();
    let slow_ready = slow.next().await.unwrap().unwrap();
    let slow_ready: serde_json::Value =
        serde_json::from_str(slow_ready.to_text().unwrap()).unwrap();
    assert_eq!(slow_ready["t"], "ready");
    assert_eq!(slow_ready["n"], 0);
    let slow_epoch = slow_ready["e"].as_u64().unwrap();
    let reader = |mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<TcpStream>,
    >| {
        tokio::spawn(async move {
            let mut last = 0;
            while last < 400 {
                let m = tokio::time::timeout(Duration::from_secs(20), ws.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if let Message::Text(s) = m {
                    let frame: serde_json::Value = serde_json::from_str(&s).unwrap();
                    if frame["t"] == "m" {
                        let n = frame["n"].as_u64().unwrap();
                        assert_eq!(n, last + 1);
                        last = n + frame["m"].as_array().unwrap().len() as u64 - 1;
                    } else {
                        assert_ne!(frame["t"], "behind");
                    }
                }
            }
            (ws, last)
        })
    };
    let first = reader(one);
    let second = reader(two);
    // Paced traffic to fill one stalled TCP connection's bounded host queue.
    // The other two clients continuously drain their frames.
    for round in 0..400 {
        if round == 300 {
            // Fill the remaining frame permits with small pongs as well as
            // the byte budget: a tiny behind can otherwise still fit.
            for _ in 0..260 {
                slow.send(Message::Text(r#"{"t":"ping"}"#.into()))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        let result = request(addr, "/actor/t/driver/r3-publish").await;
        assert!(result.starts_with("HTTP/1.1 200"), "{result}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (mut one, n) = first.await.unwrap();
    assert_eq!(n, 400);
    let (mut two, n) = second.await.unwrap();
    assert_eq!(n, 400);
    let code = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match slow.next().await {
                Some(Ok(Message::Close(Some(frame)))) => break u16::from(frame.code),
                Some(Ok(_)) => {}
                other => panic!("slow socket ended without its 4802 frame: {other:?}"),
            }
        }
    })
    .await
    .expect("slow client was not closed");
    assert_eq!(code, 4802);
    // Rejoin with the last consumed position (ready at zero), fetch the
    // retained window through the front in bounded pages, then resume live.
    drop(slow);
    assert!(
        request(addr, "/actor/t/driver/r3-start")
            .await
            .starts_with("HTTP/1.1 200")
    );
    let (mut resumed, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/edge/e/?c=slow&p=0000000000000002&e={slow_epoch}&n=0"
    ))
    .await
    .unwrap();
    let ready = resumed.next().await.unwrap().unwrap();
    let ready: serde_json::Value = serde_json::from_str(ready.to_text().unwrap()).unwrap();
    assert_eq!(ready["t"], "ready");
    assert!(ready["n"].is_null());
    let mut last = 0;
    let mut epoch = slow_epoch;
    loop {
        resumed
            .send(Message::Text(r#"{"t":"ping"}"#.into()))
            .await
            .unwrap();
        let pong = tokio::time::timeout(Duration::from_secs(5), resumed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let pong: serde_json::Value = serde_json::from_str(pong.to_text().unwrap()).unwrap();
        assert_eq!(pong["t"], "pong");
        let response = request(
            addr,
            &format!("/actor/t/driver/r3-history?after={epoch}.{last}"),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        let body = if headers
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            let mut rest = body;
            let mut decoded = String::new();
            loop {
                let (size, tail) = rest.split_once("\r\n").unwrap();
                let size = usize::from_str_radix(size, 16).unwrap();
                if size == 0 {
                    break;
                }
                decoded.push_str(&tail[..size]);
                rest = &tail[size + 2..];
            }
            decoded
        } else {
            body.into()
        };
        let h: serde_json::Value = serde_json::from_str(&body).unwrap();
        let numbers = h["numbers"].as_array().unwrap();
        assert!(!numbers.is_empty());
        if last == 0 {
            assert_eq!(h["reset"], true);
            last = numbers[0].as_u64().unwrap() - 1;
        }
        for n in numbers {
            assert_eq!(n.as_u64().unwrap(), last + 1);
            last += 1;
        }
        epoch = h["e"].as_u64().unwrap();
        if h["more"] == false {
            break;
        }
    }
    assert_eq!(last, 400);
    assert!(
        request(addr, "/actor/t/driver/r3-publish")
            .await
            .starts_with("HTTP/1.1 200")
    );
    let live = tokio::time::timeout(Duration::from_secs(5), resumed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let live: serde_json::Value = serde_json::from_str(live.to_text().unwrap()).unwrap();
    assert_eq!(live["t"], "m");
    assert_eq!(live["n"], 401);
    resumed.close(None).await.unwrap();
    one.close(None).await.unwrap();
    two.close(None).await.unwrap();
    let _ = stop.send(());
    let _ = server.await;
}

// A full 512-item retained buffer, 500 caught-up loopback sockets, 2 pushes/s.
// Keep the default 50 ms event CPU budget; bulk replay is a separate workload.
async fn fanout_budget(engine: &str) {
    // nextest runs engines in separate processes; keep loopback sockets <=500.
    let lock = std::fs::File::create(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/fanout-test.lock"),
    )
    .unwrap();
    lock.lock().unwrap();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep_runtime=debug,wasmkeep_server=warn")
        .with_test_writer()
        .try_init();
    let wasm = if engine == "rust" {
        guests::build_guest("../wasmkeep-sdk/tests/room-edge-guest", "room_edge_guest")
    } else {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let tmp = tempfile::tempdir().unwrap();
        let output = tmp.path().join("edge.wasm");
        assert!(
            std::process::Command::new("node")
                .current_dir(root)
                .args([
                    "sdks/typescript/bin/wasmkeep-ts.mjs",
                    "build",
                    "crates/wasmkeep-server/tests/guests/room-fanout/edge.ts",
                    "--engine",
                    engine,
                    "-o"
                ])
                .arg(&output)
                .status()
                .unwrap()
                .success()
        );
        std::fs::read(output).unwrap()
    };
    let runtime_config = RuntimeConfig {
        measure_fuel: true,
        ..RuntimeConfig::default()
    };
    assert_eq!(runtime_config.cpu_budget, Duration::from_millis(50));
    let runtime = Runtime::new(&runtime_config).unwrap();
    let component = guests::compile(&runtime, &wasm);
    let data = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.idle_timeout = Duration::from_secs(60);
    let service =
        ActorService::new(runtime, config, vec![("fanout".into(), component, wasm)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    let mut sockets = Vec::new();
    for _ in 0..500 {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/fanout/e/"))
            .await
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(ready.to_text().unwrap().contains("ready"));
        sockets.push(ws);
    }
    for n in 513..=516 {
        let response = request(addr, "/actor/fanout/e/push").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{engine}: {response}");
        for ws in &mut sockets {
            let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            assert_eq!(frame["n"], n);
            assert_eq!(frame["m"].as_array().unwrap().len(), 1);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    drop(sockets);
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    fuel::assert_ceiling(match engine {
        "rust" => 10_000_000,
        "quickjs" => 250_000_000,
        _ => 500_000_000,
    });
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load-sensitive: the in-test node build is sensitive to CPU contention; scripts/check.sh --full runs it"]
async fn fanout_500_quickjs_default_budget() {
    fanout_budget("quickjs").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load-sensitive: the in-test node build is sensitive to CPU contention; scripts/check.sh --full runs it"]
async fn fanout_500_starlingmonkey_default_budget() {
    fanout_budget("starlingmonkey").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_500_rust_default_budget() {
    fanout_budget("rust").await;
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
async fn json_frame(ws: &mut Ws, what: &str) -> serde_json::Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .unwrap_or_else(|_| panic!("no frame: {what}"))
            .unwrap()
            .unwrap();
        if let Message::Text(s) = m {
            return serde_json::from_str(&s).unwrap();
        }
    }
}
fn body_of(response: &str) -> String {
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    if !headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return body.into();
    }
    let (mut rest, mut decoded) = (body, String::new());
    loop {
        let (size, tail) = rest.split_once("\r\n").unwrap();
        let size = usize::from_str_radix(size, 16).unwrap();
        if size == 0 {
            return decoded;
        }
        decoded.push_str(&tail[..size]);
        rest = &tail[size + 2..];
    }
}
async fn hibernated(svc: &ActorService) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while svc.resident() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actors did not hibernate");
}
// ADR 0040, section 5: an edge woken from hibernation holds its sockets but
// no cursors. A ping right after the wake gets a pong; in a quiet room, one
// publish after the edge hibernated reaches the open socket with no second
// publish, live or through the position the edge announces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_edge_quiet_wake_delivers_one_publish_and_answers_ping() {
    let wasm = guests::build_guest("../wasmkeep-sdk/tests/room-edge-guest", "room_edge_guest");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, &wasm);
    let dir = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.mailbox.idle_timeout = Duration::from_millis(300);
    let svc = ActorService::new(
        runtime,
        config,
        vec![
            ("t".into(), component.clone(), wasm.clone()),
            ("pusher".into(), component.clone(), wasm.clone()),
            ("edge".into(), component, wasm),
        ],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(listener, svc.clone(), async {
        let _ = stopped.await;
    }));
    let warm = request(addr, "/actor/t/driver/r3-start").await;
    assert!(warm.starts_with("HTTP/1.1 200"), "{warm}");
    let (mut ws, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/actor/edge/e/?c=one&p=0000000000000000"
    ))
    .await
    .unwrap();
    let ready = json_frame(&mut ws, "ready").await;
    assert_eq!(ready["t"], "ready");
    // The client's position: the stream starts after the head it joined at.
    let mut last = (ready["e"].as_u64().unwrap(), ready["n"].as_u64().unwrap());
    hibernated(&svc).await;
    // A ping wakes a headless edge: it answers with no head.
    ws.send(Message::Text(r#"{"t":"ping"}"#.into()))
        .await
        .unwrap();
    let pong = json_frame(&mut ws, "pong after a wake").await;
    assert_eq!(pong["t"], "pong");
    hibernated(&svc).await;
    // One publish into the quiet room; no second one follows.
    let published = request(addr, "/actor/t/driver/r3-publish").await;
    assert!(published.starts_with("HTTP/1.1 200"), "{published}");
    let at = body_of(&published);
    let (e, n) = at.trim().split_once('.').unwrap();
    let target: (u64, u64) = (e.parse().unwrap(), n.parse().unwrap());
    let mut fetched = false;
    while last < target {
        let f = json_frame(&mut ws, "the published message").await;
        let e = f["e"].as_u64().unwrap();
        match f["t"].as_str().unwrap() {
            "m" => {
                let first = f["n"].as_u64().unwrap();
                assert!(
                    e == last.0 && first <= last.1 + 1 || f["b"].as_u64() <= Some(last.1),
                    "{f} after {last:?}"
                );
                last = (e, first + f["m"].as_array().unwrap().len() as u64 - 1);
            }
            "ready" => {
                // Rules 2 and 3: the next epoch's base, then fetch only if
                // the announced position is past the client's own.
                let (b, n) = (f["b"].as_u64().unwrap(), f["n"].as_u64().unwrap());
                if e == last.0 + 1 && b >= last.1 {
                    last.0 = e;
                }
                if e != last.0 || n > last.1 {
                    let h = request(
                        addr,
                        &format!("/actor/t/driver/r3-history?after={}.{}", last.0, last.1),
                    )
                    .await;
                    assert!(h.starts_with("HTTP/1.1 200"), "{h}");
                    let h: serde_json::Value = serde_json::from_str(&body_of(&h)).unwrap();
                    for m in h["numbers"].as_array().unwrap() {
                        last = (h["e"].as_u64().unwrap(), m.as_u64().unwrap());
                    }
                    fetched = true;
                }
            }
            other => panic!("unexpected {other}: {f}"),
        }
    }
    assert_eq!(last, target, "fetched over HTTP: {fetched}");
    ws.close(None).await.unwrap();
    let _ = stop.send(());
    let _ = server.await;
}
// ADR 0040, sections 5 and 13: a quiet room whose room, pusher and edge all
// hibernated. The pusher kept where its edge stood when it went idle, so
// the one publish that wakes everything reaches every socket live, in an
// m frame of the new epoch: no ready, no history fetch by any client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn room_edge_full_hibernation_pushes_one_publish_with_no_history_fetch() {
    let wasm = guests::build_guest("../wasmkeep-sdk/tests/room-edge-guest", "room_edge_guest");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = guests::compile(&runtime, &wasm);
    let dir = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.mailbox.idle_timeout = Duration::from_millis(300);
    let svc = ActorService::new(
        runtime,
        config,
        vec![
            ("t".into(), component.clone(), wasm.clone()),
            ("pusher".into(), component.clone(), wasm.clone()),
            ("edge".into(), component, wasm),
        ],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(listener, svc.clone(), async {
        let _ = stopped.await;
    }));
    let warm = request(addr, "/actor/t/driver/r3-start").await;
    assert!(warm.starts_with("HTTP/1.1 200"), "{warm}");
    let mut clients = vec![];
    for c in ["one", "two", "three"] {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/actor/edge/e/?c={c}&p=0000000000000000"
        ))
        .await
        .unwrap();
        let ready = json_frame(&mut ws, "ready").await;
        assert_eq!(ready["t"], "ready");
        let last = (ready["e"].as_u64().unwrap(), ready["n"].as_u64().unwrap());
        clients.push((ws, last));
    }
    // Room, pusher and edge hibernate; the sockets stay open.
    hibernated(&svc).await;
    let published = request(addr, "/actor/t/driver/r3-publish").await;
    assert!(published.starts_with("HTTP/1.1 200"), "{published}");
    let at = body_of(&published);
    let (e, n) = at.trim().split_once('.').unwrap();
    let target: (u64, u64) = (e.parse().unwrap(), n.parse().unwrap());
    assert!(target.0 > clients[0].1.0, "the wake opened an epoch");
    for (ws, last) in &mut clients {
        let f = json_frame(ws, "the published message").await;
        // The first frame after the wake is the message, live: an m frame
        // of the new epoch whose base is the client's own position (rule 2:
        // nothing void), right after it (rule 3: no gap).
        assert_eq!(f["t"], "m", "{f} after {last:?}");
        assert_eq!(f["e"].as_u64(), Some(target.0), "{f}");
        assert_eq!(f["b"].as_u64(), Some(last.1), "{f}");
        assert_eq!(f["n"].as_u64(), Some(last.1 + 1), "{f}");
        assert_eq!(f["m"].as_array().unwrap().len(), 1, "{f}");
        *last = target;
    }
    for (mut ws, _) in clients {
        ws.close(None).await.unwrap();
    }
    let _ = stop.send(());
    let _ = server.await;
}

/// The guest for an engine: the Rust room-edge guest, or a TypeScript one.
fn engine_guest(engine: &str, ts: &str) -> Vec<u8> {
    if engine == "rust" {
        return guests::build_guest("../wasmkeep-sdk/tests/room-edge-guest", "room_edge_guest");
    }
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let tmp = tempfile::tempdir().unwrap();
    let output = tmp.path().join("edge.wasm");
    assert!(
        std::process::Command::new("node")
            .current_dir(root)
            .args([
                "sdks/typescript/bin/wasmkeep-ts.mjs",
                "build",
                ts,
                "--engine",
                engine,
                "-o"
            ])
            .arg(&output)
            .status()
            .unwrap()
            .success()
    );
    std::fs::read(output).unwrap()
}
// ADR 0040, section 5: one edge woken from hibernation by a push, holding
// 500 sockets and no cursors. Each wake, header-only (500 ready frames)
// and carrying a message (500 m frames), stays within the default 50 ms
// event CPU budget: no trap, and every event under the fanout ceilings.
async fn wake_budget(engine: &str) {
    // nextest runs engines in separate processes; keep loopback sockets <=500.
    let lock = std::fs::File::create(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/fanout-test.lock"),
    )
    .unwrap();
    lock.lock().unwrap();
    let wasm = engine_guest(
        engine,
        "crates/wasmkeep-server/tests/guests/room-fanout/wake.ts",
    );
    let runtime_config = RuntimeConfig {
        measure_fuel: true,
        ..RuntimeConfig::default()
    };
    assert_eq!(runtime_config.cpu_budget, Duration::from_millis(50));
    let runtime = Runtime::new(&runtime_config).unwrap();
    let component = guests::compile(&runtime, &wasm);
    let data = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.idle_timeout = Duration::from_millis(500);
    let service =
        ActorService::new(runtime, config, vec![("wake".into(), component, wasm)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    let mut sockets = Vec::new();
    for _ in 0..500 {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/wake/e/"))
            .await
            .unwrap();
        let ready = json_frame(&mut ws, "ready").await;
        assert_eq!(ready["t"], "ready");
        sockets.push(ws);
    }
    for kind in ["header", "messages"] {
        // Every cursor is gone; the 500 sockets stay.
        hibernated(&service).await;
        let response = request(addr, &format!("/actor/wake/e/{kind}")).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{engine}: {response}");
        let ack: serde_json::Value = serde_json::from_str(&body_of(&response)).unwrap();
        assert_eq!(ack["sockets"], 500, "{engine} {kind}: {ack}");
        for ws in &mut sockets {
            let f = json_frame(ws, kind).await;
            if kind == "header" {
                assert_eq!(
                    (&f["t"], &f["b"], &f["n"]),
                    (&"ready".into(), &0.into(), &512.into()),
                    "{f}"
                );
            } else {
                assert_eq!((&f["t"], &f["n"]), (&"m".into(), &513.into()), "{f}");
            }
        }
    }
    drop(sockets);
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    fuel::assert_ceiling(match engine {
        "rust" => 10_000_000,
        "quickjs" => 250_000_000,
        _ => 500_000_000,
    });
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_500_rust_default_budget() {
    wake_budget("rust").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_500_quickjs_default_budget() {
    wake_budget("quickjs").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "speed-sensitive: a slow CI runner's CPU can trap on the real 50 ms budget; scripts/check.sh --full runs it"]
async fn wake_500_starlingmonkey_default_budget() {
    wake_budget("starlingmonkey").await;
}
