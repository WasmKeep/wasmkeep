//! examples/agent end to end against a mock chat-completions server on
//! 127.0.0.1 (reached through the egress allow list): a round trip and the
//! history across hibernation, the reminder tool on an alarm reaching a
//! WebSocket, the API key kept out of the history, the database and the
//! logs, messages to one conversation handled in order, and the hourly cap.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Value as Json, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService};

#[path = "support/guests.rs"]
mod guests;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const KEY: &str = "sk-test-0123456789-SECRET";

fn agent_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("../../examples/agent", "agent"))
}

/// The bodies of the requests the mock model received, in arrival order.
type Seen = Arc<Mutex<Vec<String>>>;

/// A mock OpenAI-compatible chat-completions endpoint. It answers 401
/// without `Authorization: Bearer KEY`, and otherwise by the last message:
/// a tool result gets "Done.", "remind" gets a `remind_me` tool call,
/// "slow" waits 1.5 s first, "echo the key" gets an error that repeats the
/// key (as some APIs do with a wrong one), anything else "reply to <text>".
async fn mock_model() -> (SocketAddr, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Seen = Arc::default();
    let bodies = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(answer(stream, Arc::clone(&bodies)));
        }
    });
    (addr, seen)
}

async fn answer(mut stream: TcpStream, seen: Seen) {
    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    let (head, body) = loop {
        let Ok(n) = stream.read(&mut buf).await else {
            return;
        };
        if n == 0 {
            return;
        }
        raw.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&raw).to_string();
        if let Some((head, body)) = text.split_once("\r\n\r\n") {
            let len = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if body.len() >= len {
                break (head.to_owned(), body.to_owned());
            }
        }
    };
    seen.lock().unwrap().push(body.clone());
    let authorized = head.lines().any(|l| {
        l.split_once(':').is_some_and(|(k, v)| {
            k.eq_ignore_ascii_case("authorization") && v.trim() == format!("Bearer {KEY}")
        })
    });
    let request: Json = serde_json::from_str(&body).unwrap_or(Json::Null);
    let last = request["messages"]
        .as_array()
        .and_then(|m| m.last())
        .cloned()
        .unwrap_or(Json::Null);
    let content = last["content"].as_str().unwrap_or_default().to_owned();
    let (status, reply) = if !authorized {
        (401, json!({ "error": { "message": "no key" } }))
    } else if content.contains("echo the key") {
        (
            401,
            json!({ "error": { "message": format!("Incorrect API key provided: {KEY}") } }),
        )
    } else if last["role"] == "tool" {
        (
            200,
            completion(json!({ "role": "assistant", "content": "Done." })),
        )
    } else if content.contains("remind") {
        let arguments = json!({ "minutes": 0.05, "text": "stretch" }).to_string();
        (
            200,
            completion(json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": { "name": "remind_me", "arguments": arguments },
                }],
            })),
        )
    } else {
        if content.contains("slow") {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
        }
        (
            200,
            completion(json!({ "role": "assistant", "content": format!("reply to {content}") })),
        )
    };
    let reply = reply.to_string();
    let _ = stream
        .write_all(
            format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                reply.len()
            )
            .as_bytes(),
        )
        .await;
}

fn completion(message: Json) -> Json {
    json!({ "id": "x", "object": "chat.completion", "choices": [{ "index": 0, "message": message }] })
}

async fn start(
    data: &std::path::Path,
    model: SocketAddr,
    extra: &str,
) -> (SocketAddr, ActorService, oneshot::Sender<()>) {
    // The server's warnings reach the test's output, shown when it fails:
    // this test failed once under load with only a symptom to go on.
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .try_init();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![(
        "agent".to_owned(),
        runtime.compile(agent_wasm()).unwrap(),
        agent_wasm().to_vec(),
    )];
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_millis(300);
    config.alarm_retry_initial = Duration::from_millis(100);
    // The mock listens on 127.0.0.1, which the default policy refuses.
    config.egress = wasmkeep_server::EgressPolicy::allowing(vec!["127.0.0.1".parse().unwrap()]);
    let mut env = ActorEnv::default();
    env.extend(
        "agent",
        wasmkeep_server::env::parse(&format!(
            "MODEL_URL=http://{model}/v1\nMODEL=test-model\nAPI_KEY={KEY}\n{extra}"
        ))
        .unwrap(),
    );
    config.env = env;
    let svc = ActorService::new(runtime, config, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc.clone(), async {
        let _ = stopped.await;
    }));
    (addr, svc, stop)
}

async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    // Chunked bodies: take the chunk contents.
    let mut text = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        match usize::from_str_radix(size.trim(), 16) {
            Ok(0) => break,
            Ok(n) if tail.len() >= n => {
                text.push_str(&tail[..n]);
                rest = tail[n..].trim_start_matches("\r\n");
            }
            _ => {
                text = body.to_owned();
                break;
            }
        }
    }
    (status, text)
}

async fn post(addr: SocketAddr, conversation: &str, text: &str) -> (u16, String) {
    let (s, body) = request(
        addr,
        "POST",
        &format!("/actor/agent/{conversation}/message"),
        text,
    )
    .await;
    (s, body.trim_end().to_owned())
}

async fn history(addr: SocketAddr, conversation: &str) -> Vec<Json> {
    let (s, body) = request(
        addr,
        "GET",
        &format!("/actor/agent/{conversation}/history"),
        "",
    )
    .await;
    assert_eq!(s, 200, "{body}");
    serde_json::from_str::<Json>(&body)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

/// `kind: content` for each entry, tool calls shown by name.
fn summary(entries: &[Json]) -> Vec<String> {
    entries
        .iter()
        .map(|e| {
            let what = e["content"].as_str().map_or_else(
                || format!("calls {}", e["tool_calls"][0]["function"]["name"]),
                str::to_owned,
            );
            format!("{}: {what}", e["kind"].as_str().unwrap())
        })
        .collect()
}

async fn wait_for(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < limit, "{what} within {limit:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn next_entry(ws: &mut Ws, what: &str) -> Json {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .unwrap_or_else(|| panic!("socket ended while waiting for {what}"))
            .unwrap();
        match msg {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Close(f) => panic!("closed: {f:?}"),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conversation_round_trips_and_survives_hibernation() {
    let data = tempfile::tempdir().unwrap();
    let (model, seen) = mock_model().await;
    let (addr, svc, _stop) = start(data.path(), model, "").await;

    assert_eq!(
        post(addr, "c1", "hello").await,
        (200, "reply to hello".into())
    );
    wait_for("hibernation", Duration::from_secs(5), || {
        svc.resident() == 0
    })
    .await;

    let h = history(addr, "c1").await;
    assert_eq!(
        summary(&h),
        ["user: hello", "assistant: reply to hello"],
        "{h:?}"
    );

    // Woken from disk, the actor sends the earlier exchange to the model.
    assert_eq!(
        post(addr, "c1", "again").await,
        (200, "reply to again".into())
    );
    let last: Json = serde_json::from_str(seen.lock().unwrap().last().unwrap()).unwrap();
    let sent: Vec<&str> = last["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["content"].as_str().unwrap())
        .collect();
    assert_eq!(sent, ["hello", "reply to hello", "again"]);
    assert_eq!(last["model"], "test-model");
    assert_eq!(last["tools"][0]["function"]["name"], "remind_me");

    // Conversations are separate actors.
    assert!(history(addr, "c2").await.is_empty());
    let (s, _) = post(addr, "c1", "").await;
    assert_eq!(s, 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reminder_fires_on_an_alarm_while_the_agent_hibernates() {
    let data = tempfile::tempdir().unwrap();
    let (model, _) = mock_model().await;
    let (addr, svc, _stop) = start(data.path(), model, "").await;
    let mut ws = connect_async(format!("ws://{addr}/actor/agent/c1"))
        .await
        .unwrap()
        .0;

    let asked = Instant::now();
    assert_eq!(
        post(addr, "c1", "remind me to stretch in 3 seconds").await,
        (200, "Done.".into())
    );
    // The socket sees each step of the turn.
    let mut steps = Vec::new();
    for _ in 0..4 {
        steps.push(next_entry(&mut ws, "a step of the turn").await);
    }
    assert_eq!(
        summary(&steps),
        [
            "user: remind me to stretch in 3 seconds",
            "assistant: calls \"remind_me\"",
            "tool: reminder set for 0.05 minutes from now",
            "assistant: Done.",
        ]
    );
    assert_eq!(steps[2]["tool_call_id"], "call_1");

    // Waiting costs no memory: the actor hibernates with the socket open.
    wait_for("hibernation", Duration::from_secs(2), || {
        svc.resident() == 0
    })
    .await;
    assert!(
        asked.elapsed() < Duration::from_secs(3),
        "hibernated before the reminder"
    );
    let reminder = next_entry(&mut ws, "the reminder").await;
    assert!(asked.elapsed() >= Duration::from_millis(2_900));
    assert_eq!(reminder["kind"], "reminder");
    assert_eq!(reminder["content"], "Reminder: stretch");

    let h = history(addr, "c1").await;
    assert_eq!(summary(&h).last().unwrap(), "reminder: Reminder: stretch");
    // Delivered once: no reminder left, no alarm pending.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(history(addr, "c1").await.len(), 5);
    let storage = wasmkeep_storage::Storage::new(
        wasmkeep_storage::StorageConfig::new(data.path()),
        wasmkeep_server::state_dir(&wasmkeep_server::StateRoot::new("", "default")),
    );
    let mut db = storage.open("agent", "c1").unwrap();
    let pending = db
        .query("SELECT count(*) FROM reminders WHERE delivered = 0", &[])
        .unwrap();
    assert_eq!(format!("{:?}", pending.values[0][0]), "Integer(0)");
}

/// A `MakeWriter` collecting every log line in memory.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Prints the collected warnings if the test fails: it once closed a
/// socket with "unavailable" under full-suite load, and the server's
/// reason went only to this buffer.
struct WarningsOnFailure(Logs);

impl Drop for WarningsOnFailure {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let logs = self.0.0.lock().unwrap_or_else(|p| p.into_inner());
            for line in String::from_utf8_lossy(&logs).lines() {
                if line.contains("WARN") || line.contains("ERROR") {
                    eprintln!("server: {line}");
                }
            }
        }
    }
}

fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn the_api_key_never_reaches_the_history_the_database_or_the_logs() {
    let logs = Logs::default();
    let writer = logs.clone();
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .init();
    let _warnings = WarningsOnFailure(logs.clone());

    let data = tempfile::tempdir().unwrap();
    let (model, seen) = mock_model().await;
    let (addr, _svc, _stop) = start(data.path(), model, "").await;
    let mut ws = connect_async(format!("ws://{addr}/actor/agent/c1"))
        .await
        .unwrap()
        .0;

    // The key reaches the model (the mock refuses requests without it).
    assert_eq!(post(addr, "c1", "hi").await, (200, "reply to hi".into()));
    assert_eq!(
        post(addr, "c1", "remind me soon").await,
        (200, "Done.".into())
    );
    // An endpoint repeating the key in an error: the actor redacts it.
    let (s, error) = post(addr, "c1", "echo the key").await;
    assert_eq!(s, 502, "{error}");
    assert!(error.contains("[API_KEY]"), "{error}");
    assert!(!error.contains(KEY), "{error}");

    let mut socket = String::new();
    for _ in 0..7 {
        socket.push_str(&next_entry(&mut ws, "an entry").await.to_string());
    }
    let reminder = next_entry(&mut ws, "the reminder").await;
    assert_eq!(reminder["kind"], "reminder");
    socket.push_str(&reminder.to_string());
    assert!(!socket.contains(KEY));

    let h = serde_json::to_string(&history(addr, "c1").await).unwrap();
    assert!(h.contains("echo the key"), "{h}");
    assert!(!h.contains(KEY));
    // Nor in the request bodies sent to the model (it is a header).
    assert!(seen.lock().unwrap().iter().all(|b| !b.contains(KEY)));
    // Nor anywhere on disk: databases, WAL, alarm index.
    for file in files_under(data.path()) {
        let bytes = std::fs::read(&file).unwrap();
        assert!(
            !bytes.windows(KEY.len()).any(|w| w == KEY.as_bytes()),
            "{} holds the key",
            file.display()
        );
    }
    let logged = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(!logged.is_empty(), "the capture works");
    assert!(!logged.contains(KEY), "{logged}");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_messages_to_one_conversation_are_handled_in_order() {
    let data = tempfile::tempdir().unwrap();
    let (model, seen) = mock_model().await;
    let (addr, _svc, _stop) = start(data.path(), model, "").await;

    // The first is slow at the model; the second arrives meanwhile and waits.
    let first = tokio::spawn(async move { post(addr, "c1", "slow one").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = tokio::spawn(async move { post(addr, "c1", "two").await });
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!(first, (200, "reply to slow one".into()));
    assert_eq!(second, (200, "reply to two".into()));

    // The model saw the second message after the whole first exchange.
    let bodies = seen.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    let second_request: Json = serde_json::from_str(&bodies[1]).unwrap();
    let sent: Vec<&str> = second_request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["content"].as_str().unwrap())
        .collect();
    assert_eq!(sent, ["slow one", "reply to slow one", "two"]);
    assert_eq!(
        summary(&history(addr, "c1").await),
        [
            "user: slow one",
            "assistant: reply to slow one",
            "user: two",
            "assistant: reply to two"
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn model_calls_are_capped_per_conversation_and_hour() {
    let data = tempfile::tempdir().unwrap();
    let (model, seen) = mock_model().await;
    let (addr, _svc, _stop) = start(data.path(), model, "MAX_CALLS_PER_HOUR=2").await;

    assert_eq!(post(addr, "c1", "one").await.0, 200);
    assert_eq!(post(addr, "c1", "two").await.0, 200);
    let (s, body) = post(addr, "c1", "three").await;
    assert_eq!(s, 429, "{body}");
    // Refused before it joined the history or reached the model.
    assert_eq!(history(addr, "c1").await.len(), 4);
    assert_eq!(seen.lock().unwrap().len(), 2);
    // Another conversation has its own count.
    assert_eq!(post(addr, "c2", "one").await.0, 200);
}
