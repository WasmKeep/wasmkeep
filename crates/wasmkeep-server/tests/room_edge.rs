//! ADR 0040 R3: one room, one edge, three clients; no load generator.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use futures_util::StreamExt;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::tungstenite::Message;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};
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
    socket.set_recv_buffer_size(1024).unwrap();
    let stream = socket.connect(addr).await.unwrap();
    let (mut slow, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/actor/edge/e/?c=slow&p=0000000000000002"),
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
    for _ in 0..400 {
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
    one.close(None).await.unwrap();
    two.close(None).await.unwrap();
    drop(slow);
    let _ = stop.send(());
    let _ = server.await;
}
