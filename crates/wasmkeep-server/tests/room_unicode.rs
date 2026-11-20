//! Real TypeScript front against both room implementations; U+FEFF is data.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::{net::SocketAddr, path::PathBuf, process::Command};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, HttpComponent};
#[path = "support/guests.rs"]
mod guests;
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = vec![];
    s.read_to_end(&mut raw).await.unwrap();
    let at = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let h = String::from_utf8(raw[..at].to_vec()).unwrap();
    let status = h.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[at + 4..].to_vec();
    // Joined before decoding: a chunk may end inside a character.
    if h.to_lowercase().contains("transfer-encoding: chunked") {
        let (mut joined, mut rest) = (vec![], &body[..]);
        loop {
            let end = rest.windows(2).position(|w| w == b"\r\n").unwrap();
            let n = usize::from_str_radix(std::str::from_utf8(&rest[..end]).unwrap(), 16).unwrap();
            if n == 0 {
                break;
            }
            joined.extend_from_slice(&rest[end + 2..end + 2 + n]);
            rest = &rest[end + 4 + n..];
        }
        body = joined;
    }
    (status, String::from_utf8(body).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicode_through_typescript_front_and_both_rooms() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let tmp = tempfile::tempdir().unwrap();
    for name in ["front", "room"] {
        let mut command = Command::new("node");
        command
            .current_dir(&root)
            .args(["sdks/typescript/bin/wasmkeep-ts.mjs", "build"])
            .arg(format!(
                "crates/wasmkeep-server/tests/guests/room-unicode/{name}.ts"
            ))
            .arg("-o")
            .arg(tmp.path().join(format!("{name}.wasm")));
        if name == "front" {
            command.args(["--world", "front"]);
        }
        assert!(command.status().unwrap().success());
    }
    let rust = guests::build_guest("tests/guests/room-scaling", "room_scaling_guest");
    for wasm in [rust, std::fs::read(tmp.path().join("room.wasm")).unwrap()] {
        let rt = Runtime::new(&RuntimeConfig::default()).unwrap();
        let front = HttpComponent::new(
            rt.clone(),
            &guests::compile(&rt, &std::fs::read(tmp.path().join("front.wasm")).unwrap()),
        )
        .unwrap();
        let data = tempfile::tempdir().unwrap();
        let mut cfg = ActorConfig::new(data.path());
        cfg.access.set("room", wasmkeep_server::Access::Internal);
        let component = guests::compile(&rt, &wasm);
        let service = ActorService::new(rt, cfg, vec![("room".into(), component, wasm)]).unwrap();
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
        let (status, body) = get(addr, "/publish").await;
        assert_eq!(status, 200, "{body}");
        let published: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(published, serde_json::json!([{"e":1,"n":1},{"e":1,"n":2}]));
        let (status, body) = get(addr, "/history").await;
        assert_eq!(status, 200, "{body}");
        let history: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(history["messages"][0]["id"], "x");
        assert_eq!(history["messages"][1]["id"], "\u{feff}x");
        assert_eq!(history["messages"][1]["body"], "\u{feff}body");
        // The room's own JSON is what the front made of the binary page,
        // byte for byte, whatever the text holds.
        let (_, converted) = get(addr, "/history-old").await;
        assert_eq!(body, converted);
        let (status, out) = get(addr, "/publish-tricky").await;
        assert_eq!(status, 200, "{out}");
        let (status, body) = get(addr, "/history").await;
        assert_eq!(status, 200, "{body}");
        let (_, converted) = get(addr, "/history-old").await;
        assert_eq!(body, converted);
        assert!(body.contains(r#""body":"\u0000\u0001"#), "{body}");
        assert!(body.contains("\u{2028}\u{2029}") && body.contains("\u{1f600}"));
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    }
}
