//! Who may reach a class from outside (ADR 0010), with tests/guests/test-actor
//! as three classes: `t` (existing), `u` (internal), `v` (open).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{Access, AccessRules, ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

async fn start(data: &Path) -> SocketAddr {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(data);
    let mut access = AccessRules::default();
    access.set("t", Access::Existing);
    access.set("u", Access::Internal);
    config.access = access;
    let classes = ["t", "u", "v"]
        .iter()
        .map(|c| ((*c).to_owned(), component.clone(), wasm.clone()))
        .collect();
    let service = ActorService::new(runtime, config, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    addr
}

/// (status, de-chunked body).
async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("")
        }
        _ => body,
    };
    (status, body.trim_end().to_owned())
}

/// Databases on disk for `class`.
fn databases(data: &Path, class: &str) -> Vec<String> {
    let dir = data.join("state/default/default/actors").join(class);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".db"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// The status a WebSocket upgrade was refused with, or 101.
async fn upgrade_status(addr: SocketAddr, path: &str) -> u16 {
    match tokio_tungstenite::connect_async(format!("ws://{addr}{path}")).await {
        Ok(_) => 101,
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => r.status().as_u16(),
        Err(e) => panic!("{path}: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_actors_only_and_nothing_created_for_unknown_names() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path()).await;

    // Unknown names: 404, and nothing on disk, however many are tried.
    for name in ["ghost", "ghost-2", "x%20y"] {
        let (s, body) = send(addr, "GET", &format!("/actor/t/{name}/durable")).await;
        assert_eq!((s, body.as_str()), (404, "no such actor"), "{name}");
        let (s, _) = send(addr, "POST", &format!("/actor/t/{name}/durable")).await;
        assert_eq!(s, 404, "{name}: a write creates nothing either");
    }
    assert_eq!(upgrade_status(addr, "/actor/t/ghost").await, 404);
    assert!(
        databases(dir.path(), "t").is_empty(),
        "{:?}",
        databases(dir.path(), "t")
    );

    // Another actor creates one; from then on it is reachable from outside.
    let (s, body) = send(
        addr,
        "POST",
        "/actor/v/maker/call?to=t/made&path=%2Fdurable",
    )
    .await;
    assert_eq!((s, body.as_str()), (200, "200:1"));
    assert_eq!(
        send(addr, "GET", "/actor/t/made/durable").await,
        (200, "1".into())
    );
    assert_eq!(
        send(addr, "POST", "/actor/t/made/durable").await,
        (200, "2".into())
    );
    assert_eq!(databases(dir.path(), "t"), vec!["made.db".to_owned()]);

    // Deleted, it is unknown again.
    assert_eq!(send(addr, "POST", "/actor/t/made/delete").await.0, 200);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(send(addr, "GET", "/actor/t/made/durable").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_actor_s_sockets_go_with_it_and_cannot_bring_it_back() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path()).await;
    let (s, _) = send(
        addr,
        "POST",
        "/actor/v/maker/call?to=t/made&path=%2Fdurable",
    )
    .await;
    assert_eq!(s, 200);
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/t/made"))
        .await
        .unwrap();
    ws.send(Message::text("ping")).await.unwrap();
    let echo = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(echo, Message::text("ping"));

    // Deleted from outside (it exists, so that is allowed): the socket is
    // closed, and its close event recreates nothing.
    assert_eq!(send(addr, "POST", "/actor/t/made/delete").await.0, 200);
    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => return frame,
                Some(Ok(_)) => {}
                other => panic!("expected a close frame, got {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(closed.map(|f| u16::from(f.code)), Some(1000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        databases(dir.path(), "t").is_empty(),
        "{:?}",
        databases(dir.path(), "t")
    );
    assert_eq!(send(addr, "GET", "/actor/t/made/durable").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn internal_classes_answer_actors_only() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path()).await;
    let (s, body) = send(addr, "GET", "/actor/u/x/durable").await;
    assert_eq!((s, body.as_str()), (403, "not reachable from outside"));
    assert_eq!(upgrade_status(addr, "/actor/u/x").await, 403);
    assert!(databases(dir.path(), "u").is_empty());
    // From an actor it works as usual.
    let (s, body) = send(addr, "POST", "/actor/v/a/call?to=u/x&path=%2Fdurable").await;
    assert_eq!((s, body.as_str()), (200, "200:1"));
    // Open classes are unchanged.
    assert_eq!(
        send(addr, "GET", "/actor/v/anyone/durable").await,
        (200, "0".into())
    );
}
