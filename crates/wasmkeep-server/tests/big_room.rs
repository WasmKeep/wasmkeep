//! The actual TypeScript example, on both engines built by the languages job.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use futures_util::StreamExt;
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, HttpComponent};
#[path = "support/guests.rs"]
#[allow(dead_code)]
mod guests;
async fn example(suffix: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep=debug")
        .with_test_writer()
        .try_init();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/ts-big-room");
    let paths: Vec<_> = ["front", "room", "edge", "relay", "directory"]
        .iter()
        .map(|c| root.join(format!("{c}{suffix}.wasm")))
        .collect();
    if paths.iter().any(|p| !p.exists()) {
        assert!(
            std::env::var("WASMKEEP_REQUIRE_LANGS").is_err(),
            "build examples/ts-big-room first"
        );
        eprintln!("skipped: build examples/ts-big-room ({suffix})");
        return;
    }
    let rt = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let front = HttpComponent::new(
        rt.clone(),
        &guests::compile(&rt, &std::fs::read(&paths[0]).unwrap()),
    )
    .unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut cfg = ActorConfig::new(data.path());
    let mut classes = vec![];
    for (class, path) in ["room", "edge", "relay", "directory"]
        .iter()
        .zip(&paths[1..])
    {
        cfg.access.set(class, wasmkeep_server::Access::Internal);
        let wasm = std::fs::read(path).unwrap();
        classes.push(((*class).into(), guests::compile(&rt, &wasm), wasm));
    }
    let service = ActorService::new(rt, cfg, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(wasmkeep_server::serve(
        listener,
        App {
            front: Some(front),
            actors: Some(service),
        },
        async {
            let _ = stopped.await;
        },
    ));
    let request = |method: &str, path: &str, body: &str| {
        let raw = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(raw.as_bytes()).await.unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).await.unwrap();
            out
        }
    };
    let page = request("GET", "/", "").await;
    assert!(page.starts_with("HTTP/1.1 200") && page.contains("Drop connection"));
    let script = request("GET", "/client.js", "").await;
    assert!(script.contains("RoomClient") && !script.contains("wasmkeep:actor"));
    assert!(
        request("GET", "/join?name=bad%20name", "")
            .await
            .starts_with("HTTP/1.1 400")
    );
    assert!(
        request("GET", "/actor/room/lobby", "")
            .await
            .starts_with("HTTP/1.1 403")
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut ws = loop {
        if let Ok((ws, _)) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/join?name=alice")).await
        {
            break ws;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "join unavailable: {}",
            request("GET", "/stats?name=alice", "").await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let ready = ws.next().await.unwrap().unwrap();
    assert!(ready.to_text().unwrap().contains("ready"));
    let published = request(
        "POST",
        "/publish?name=alice",
        r#"{"text":"hello <world>","id":"retry-1"}"#,
    )
    .await;
    assert!(published.starts_with("HTTP/1.1 200"), "{published}");
    let live = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let live: serde_json::Value = serde_json::from_str(live.to_text().unwrap()).unwrap();
    assert_eq!(live["n"], 1);
    assert_eq!(live["m"][0]["d"], "alice: hello <world>");
    let repeated = request(
        "POST",
        "/publish?name=alice",
        r#"{"text":"hello <world>","id":"retry-1"}"#,
    )
    .await;
    assert!(repeated.contains("\"n\":1"), "{repeated}");
    let history = request("GET", "/history?name=alice&after=1.0", "").await;
    assert!(
        history.contains("alice: hello <world>") && history.contains("\"more\":false"),
        "{history}"
    );
    let stats = request("GET", "/stats?name=alice", "").await;
    assert!(stats.contains("\"edges\":1"), "{stats}");
    ws.close(None).await.unwrap();
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room() {
    example("").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room_starlingmonkey() {
    example("-starlingmonkey").await;
}
