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

/// One node serving the example's front and classes on `data`, at the
/// default CPU budget (50 ms per event).
async fn node(
    suffix: &str,
    data: &std::path::Path,
) -> Option<(
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), wasmkeep_server::Error>>,
)> {
    node_with(suffix, data, None).await
}

/// [`node`], with another component as the edge class.
async fn node_with(
    suffix: &str,
    data: &std::path::Path,
    edge: Option<PathBuf>,
) -> Option<(
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), wasmkeep_server::Error>>,
)> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/ts-big-room");
    let mut paths: Vec<_> = ["front", "room", "edge", "relay", "directory"]
        .iter()
        .map(|c| root.join(format!("{c}{suffix}.wasm")))
        .collect();
    if let Some(edge) = edge {
        paths[2] = edge;
    }
    if paths.iter().any(|p| !p.exists()) {
        assert!(
            std::env::var("WASMKEEP_REQUIRE_LANGS").is_err(),
            "build examples/ts-big-room and tests/guests/ts-fetch first"
        );
        eprintln!("skipped: build examples/ts-big-room and tests/guests/ts-fetch ({suffix})");
        return None;
    }
    let config = RuntimeConfig {
        max_instances: 512,
        ..RuntimeConfig::default()
    };
    assert_eq!(config.cpu_budget, Duration::from_millis(50));
    let rt = Runtime::new(&config).unwrap();
    let front = HttpComponent::new(
        rt.clone(),
        &guests::compile(&rt, &std::fs::read(&paths[0]).unwrap()),
    )
    .unwrap();
    let mut cfg = ActorConfig::new(data);
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
    Some((addr, stop, server))
}

async fn http(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> String {
    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(raw.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

/// `(e, n)` of a publish answer, or the whole answer if it is not a 200.
/// A 429 or 503 is retried under the same id, as clients do (ADR 0040,
/// section 6), for at most 10 s.
async fn publish(addr: std::net::SocketAddr, id: &str, text: &str) -> Result<(u64, u64), String> {
    let body = serde_json::json!({ "text": text, "id": id }).to_string();
    let until = std::time::Instant::now() + Duration::from_secs(10);
    let out = loop {
        let out = http(addr, "POST", "/publish?name=alice", &body).await;
        let retry = out.starts_with("HTTP/1.1 503") || out.starts_with("HTTP/1.1 429");
        if !retry || std::time::Instant::now() > until {
            break out;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    if !out.starts_with("HTTP/1.1 200") {
        return Err(out);
    }
    // Plain or chunked: the JSON array is the body's only bracketed span.
    let json = &out[out.find('[').ok_or(out.clone())?..=out.rfind(']').ok_or(out.clone())?];
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| format!("{e}: {out}"))?;
    Ok((v[0]["e"].as_u64().unwrap(), v[0]["n"].as_u64().unwrap()))
}

/// The room's database under `data`: the one file with a scale row.
fn room_db(data: &std::path::Path) -> PathBuf {
    let mut todo = vec![data.to_path_buf()];
    while let Some(dir) = todo.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                todo.push(path);
            } else if let Ok(db) = rusqlite::Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            ) && db
                .query_row("SELECT count(*) FROM wasmkeep_room_scale", [], |r| {
                    r.get::<_, i64>(0)
                })
                .is_ok_and(|n| n == 1)
            {
                return path;
            }
        }
    }
    panic!("no room database under {}", data.display());
}

/// Rewrites the stopped room's scale row: its edge, then 199 more, and
/// relays of 16 edges each.
fn seed_topology(data: &std::path::Path) {
    seed_edges(data, 200, "f");
}

/// [`seed_topology`] with `count` edges named `edge/lobby.<tag><i>`.
fn seed_edges(data: &std::path::Path, count: u64, tag: &str) {
    let db = rusqlite::Connection::open(room_db(data)).unwrap();
    let raw: String = db
        .query_row("SELECT data FROM wasmkeep_room_scale WHERE id=1", [], |r| {
            r.get(0)
        })
        .unwrap();
    let mut state: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let first = state["children"][0]["name"].as_str().unwrap().to_owned();
    let step = u64::MAX / count;
    let children: Vec<_> = (0..count)
        .map(|i| {
            let name = if i == 0 && tag == "f" {
                first.clone()
            } else {
                format!("edge/lobby.{tag}{i}")
            };
            let mut c =
                serde_json::json!({ "name": name, "lo": { "$u64": (i * step).to_string() } });
            if i + 1 < count {
                c["hi"] = serde_json::json!({ "$u64": ((i + 1) * step).to_string() });
            }
            c
        })
        .collect();
    let relays: Vec<_> = children
        .chunks(16)
        .enumerate()
        .map(|(r, c)| serde_json::json!({ "name": format!("relay/lobby.r{r}"), "edges": c.iter().map(|c| c["name"].clone()).collect::<Vec<_>>() }))
        .collect();
    state["children"] = serde_json::Value::Array(children);
    state["relays"] = serde_json::Value::Array(relays);
    db.execute(
        "UPDATE wasmkeep_room_scale SET data=?1 WHERE id=1",
        [state.to_string()],
    )
    .unwrap();
}

/// A room whose window is full (2,000 messages),
/// whose history pages are full and whose callers leave mid-answer must
/// keep serving at the default 50 ms budget, before and after a restart.
/// A room event that traps discards the instance, and the next publish
/// then opens a new epoch: one epoch throughout proves no room event
/// trapped.
async fn heavy(suffix: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep=info")
        .with_test_writer()
        .try_init();
    let data = tempfile::tempdir().unwrap();
    let Some((addr, stop, server)) = node(suffix, data.path()).await else {
        return;
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut ws = loop {
        if let Ok((ws, _)) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/join?name=alice")).await
        {
            break ws;
        }
        assert!(std::time::Instant::now() < deadline, "join unavailable");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // The socket's frames: the highest number delivered.
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reader = {
        let seen = seen.clone();
        tokio::spawn(async move {
            while let Some(Ok(frame)) = ws.next().await {
                let Ok(text) = frame.to_text() else { continue };
                let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
                    continue;
                };
                let last = match v["t"].as_str() {
                    Some("m") => {
                        v["n"].as_u64().unwrap() + v["m"].as_array().unwrap().len() as u64 - 1
                    }
                    Some("behind") | Some("ready") => v["n"].as_u64().unwrap_or(0),
                    _ => continue,
                };
                seen.fetch_max(last, std::sync::atomic::Ordering::Relaxed);
            }
        })
    };
    let text = "y".repeat(1900);
    let total = 2100u64;
    let mut numbers = Vec::new();
    for batch in 0..total / 6 {
        let mut tasks = Vec::new();
        for i in 0..6 {
            let text = text.clone();
            tasks.push(tokio::spawn(async move {
                publish(addr, &format!("m{}", batch * 6 + i), &text).await
            }));
        }
        for t in tasks {
            let (e, n) = t
                .await
                .unwrap()
                .unwrap_or_else(|out| panic!("publish failed: {out}"));
            assert_eq!(e, 1, "a room event trapped (new epoch) by message {n}");
            numbers.push(n);
        }
    }
    numbers.sort_unstable();
    assert_eq!(numbers, (1..=total).collect::<Vec<_>>());
    // A retry under an id still in the window answers its number.
    assert_eq!(publish(addr, "m2000", &text).await.unwrap().0, 1);
    // Full history pages, and callers that leave after the headers.
    for _ in 0..20 {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /history?name=alice&after=1.0 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut head = [0u8; 64];
        let _ = s.read(&mut head).await.unwrap();
        drop(s);
    }
    let page = http(addr, "GET", "/history?name=alice&after=1.0", "").await;
    assert!(
        page.starts_with("HTTP/1.1 200") && page.contains("\"reset\""),
        "{}",
        &page[..page.len().min(300)]
    );
    assert_eq!(
        publish(addr, "after-pages", "ok").await.unwrap(),
        (1, total + 1)
    );
    let caught = std::time::Instant::now() + Duration::from_secs(30);
    while seen.load(std::sync::atomic::Ordering::Relaxed) < total + 1 {
        assert!(
            std::time::Instant::now() < caught,
            "the socket reached {} of {}",
            seen.load(std::sync::atomic::Ordering::Relaxed),
            total + 1
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    reader.abort();
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    // A topology as a heavy run leaves it: 200 edges behind 13 relays (the
    // others never answer, so they turn suspect), on top of the full window.
    seed_topology(data.path());
    // Cold, on the same state: the first events load nothing whole.
    let (addr, stop, server) = node(suffix, data.path()).await.unwrap();
    assert_eq!(
        publish(addr, "cold", "after restart").await.unwrap(),
        (2, total + 2)
    );
    let page = http(addr, "GET", "/history?name=alice&after=1.0", "").await;
    assert!(
        page.starts_with("HTTP/1.1 200"),
        "{}",
        &page[..page.len().min(300)]
    );
    assert_eq!(
        publish(addr, "cold-2", "still").await.unwrap(),
        (2, total + 3)
    );
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room_full_window_at_default_budget() {
    heavy("").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room_full_window_at_default_budget_starlingmonkey() {
    heavy("-starlingmonkey").await;
}

/// A round's reads are charged per row, not per body byte: 2,000 messages
/// with one-character texts and 256 children that never move, each at its
/// own position (so every round meets 256 distinct slices), at the default
/// budget. A trapped room event shows as a new epoch.
async fn distinct_positions(suffix: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep=info")
        .with_test_writer()
        .try_init();
    let stub = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/guests/ts-fetch/stub_edge{suffix}.wasm"));
    let data = tempfile::tempdir().unwrap();
    let Some((addr, stop, server)) = node_with(suffix, data.path(), Some(stub.clone())).await
    else {
        return;
    };
    for batch in 0..2000 / 8 {
        let mut tasks = Vec::new();
        for i in 0..8 {
            tasks.push(tokio::spawn(async move {
                publish(addr, &format!("{:036}", batch * 8 + i), "x").await
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap().0, 1);
        }
    }
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    seed_edges(data.path(), 256, "s");
    let (addr, stop, server) = node_with(suffix, data.path(), Some(stub)).await.unwrap();
    assert_eq!(publish(addr, "cold", "x").await.unwrap(), (2, 2001));
    // About a hundred rounds over 256 lagging children.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let stats = http(addr, "GET", "/stats?name=alice", "").await;
    assert!(stats.contains("\"edges\":256"), "{stats}");
    assert_eq!(publish(addr, "after", "x").await.unwrap(), (2, 2002));
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room_distinct_positions_at_default_budget() {
    distinct_positions("").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_big_room_distinct_positions_at_default_budget_starlingmonkey() {
    distinct_positions("-starlingmonkey").await;
}
