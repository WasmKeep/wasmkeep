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
#[ignore = "speed-sensitive: the default 50 ms budget per event on a slow CI runner measures the runner; scripts/check.sh --full runs it"]
async fn fanout_500_quickjs_default_budget() {
    fanout_budget("quickjs").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "speed-sensitive: the default 50 ms budget per event on a slow CI runner measures the runner; scripts/check.sh --full runs it"]
async fn fanout_500_starlingmonkey_default_budget() {
    fanout_budget("starlingmonkey").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_500_rust_default_budget() {
    fanout_budget("rust").await;
}
