//! WebSockets end to end with examples/chat-room: broadcast, history,
//! hibernation with open sockets, closes, refusals and room isolation.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The Rust room (examples/chat-room).
fn rust_room() -> Vec<u8> {
    guests::build_guest("../../examples/chat-room", "chat_room")
}

/// A room built outside cargo (`examples/<dir>/chat_room.wasm`), if built;
/// with `WASMKEEP_REQUIRE_LANGS` set (CI) a missing build fails instead.
fn prebuilt_room(dir: &str) -> Option<Vec<u8>> {
    prebuilt_room_file(dir, "chat_room.wasm")
}

fn prebuilt_room_file(dir: &str, file: &str) -> Option<Vec<u8>> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(dir)
        .join(file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!("{} not built; skipping", path.display());
            None
        }
        Err(e) => panic!("{}: {e}", path.display()),
    }
}

fn ts_room() -> Option<Vec<u8>> {
    prebuilt_room("ts-chat-room")
}

/// The TypeScript room on StarlingMonkey (`npm run build:starlingmonkey`);
/// `ts_room` is on QuickJS, the default engine.
fn ts_starlingmonkey_room() -> Option<Vec<u8>> {
    prebuilt_room_file("ts-chat-room", "chat_room-starlingmonkey.wasm")
}

fn py_room() -> Option<Vec<u8>> {
    prebuilt_room("py-chat-room")
}

fn c_room() -> Option<Vec<u8>> {
    prebuilt_room("c-chat-room")
}

fn zig_room() -> Option<Vec<u8>> {
    prebuilt_room("zig-chat-room")
}

/// Runs each room test against every room: they must behave the same.
macro_rules! in_every_language {
    ($($test:ident),* $(,)?) => {
        mod rust_room {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $test() {
                    super::$test(super::rust_room()).await;
                }
            )*
        }
        in_every_language!(@prebuilt typescript_room, ts_room, $($test),*);
        in_every_language!(@prebuilt typescript_starlingmonkey_room, ts_starlingmonkey_room, $($test),*);
        in_every_language!(@prebuilt python_room, py_room, $($test),*);
        in_every_language!(@prebuilt c_room, c_room, $($test),*);
        in_every_language!(@prebuilt zig_room, zig_room, $($test),*);
    };
    (@prebuilt $module:ident, $room:ident, $($test:ident),*) => {
        mod $module {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $test() {
                    if let Some(room) = super::$room() {
                        super::$test(room).await;
                    }
                }
            )*
        }
    };
}

in_every_language!(
    messages_are_broadcast_to_everyone_in_the_room,
    sockets_stay_open_while_the_room_hibernates,
    rooms_are_isolated,
    a_room_knows_its_own_name,
    the_operator_s_motd_greets_each_newcomer,
);

async fn start(
    chat: Vec<u8>,
    data: &std::path::Path,
    idle: Duration,
) -> (SocketAddr, ActorService, oneshot::Sender<()>) {
    start_with_env(chat, data, idle, wasmkeep_server::ActorEnv::default()).await
}

async fn start_with_env(
    chat: Vec<u8>,
    data: &std::path::Path,
    idle: Duration,
    env: wasmkeep_server::ActorEnv,
) -> (SocketAddr, ActorService, oneshot::Sender<()>) {
    // The server's warnings reach the test's output, shown when it fails:
    // this test failed once under load with only a symptom to go on.
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .try_init();
    let counter = guests::build_guest("../../examples/counter", "counter");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![
        ("chat".to_owned(), guests::compile(&runtime, &chat), chat),
        (
            "counter".to_owned(),
            runtime.compile(&counter).unwrap(),
            counter,
        ),
    ];
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = idle;
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

async fn connect(addr: SocketAddr, room: &str) -> Ws {
    connect_async(format!("ws://{addr}/actor/chat/{room}"))
        .await
        .unwrap()
        .0
}

/// Next text message, within 5 s.
async fn next_text(ws: &mut Ws) -> String {
    next_text_for(ws, "a message").await
}

/// Like `next_text`, naming what is awaited so a timeout says which step's
/// message never arrived (this test failed once under load, unreproduced).
async fn next_text_for(ws: &mut Ws, what: &str) -> String {
    let started = std::time::Instant::now();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap_or_else(|_| {
                panic!("timed out after {:?} waiting for {what}", started.elapsed())
            })
            .unwrap_or_else(|| panic!("socket ended while waiting for {what}"))
            .unwrap();
        match msg {
            Message::Text(t) => return t.to_string(),
            Message::Close(f) => panic!("closed: {f:?}"),
            _ => {}
        }
    }
}

async fn say(ws: &mut Ws, text: &str) {
    ws.send(Message::text(text)).await.unwrap();
}

async fn http_post(addr: SocketAddr, path: &str, body: &str) -> u16 {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).await.unwrap();
    raw.split_whitespace().nth(1).unwrap().parse().unwrap()
}

async fn messages_are_broadcast_to_everyone_in_the_room(chat: Vec<u8>) {
    let data = tempfile::tempdir().unwrap();
    let (addr, svc, _stop) = start(chat, data.path(), Duration::from_secs(30)).await;

    let mut alice = connect(addr, "lobby").await;
    assert_eq!(
        next_text(&mut alice).await,
        "* someone joined #lobby (1 online)"
    );
    let mut bob = connect(addr, "lobby").await;
    assert_eq!(
        next_text(&mut alice).await,
        "* someone joined #lobby (2 online)"
    );
    assert_eq!(
        next_text(&mut bob).await,
        "* someone joined #lobby (2 online)"
    );
    assert_eq!(svc.websockets(), 2);

    say(&mut alice, "alice: hi").await;
    assert_eq!(next_text(&mut alice).await, "alice: hi");
    assert_eq!(next_text(&mut bob).await, "alice: hi");

    // A newcomer gets the history first.
    let mut carol = connect(addr, "lobby").await;
    assert_eq!(next_text(&mut carol).await, "alice: hi");
    assert_eq!(
        next_text(&mut carol).await,
        "* someone joined #lobby (3 online)"
    );

    // Leaving is announced to those who stay.
    bob.close(None).await.unwrap();
    assert_eq!(
        next_text(&mut alice).await,
        "* someone joined #lobby (3 online)"
    );
    assert_eq!(
        next_text(&mut alice).await,
        "* someone left #lobby (2 online)"
    );
}

async fn sockets_stay_open_while_the_room_hibernates(chat: Vec<u8>) {
    let data = tempfile::tempdir().unwrap();
    let (addr, svc, _stop) = start(chat, data.path(), Duration::from_millis(100)).await;
    let mut alice = connect(addr, "quiet").await;
    assert_eq!(
        next_text_for(&mut alice, "the join notice").await,
        "* someone joined #quiet (1 online)"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(svc.resident(), 0, "room should have hibernated");
    assert_eq!(svc.websockets(), 1, "socket should still be open");

    // A plain HTTP post wakes the room and reaches the parked socket.
    assert_eq!(
        http_post(addr, "/actor/chat/quiet", "bob: anyone?").await,
        200
    );
    assert_eq!(
        next_text_for(&mut alice, "the HTTP post's broadcast after hibernation").await,
        "bob: anyone?"
    );

    // So does a message on the socket itself, after another hibernation.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(svc.resident(), 0, "room should have hibernated again");
    say(&mut alice, "alice: here").await;
    assert_eq!(
        next_text_for(&mut alice, "the socket message's echo after hibernation").await,
        "alice: here"
    );
}

async fn rooms_are_isolated(chat: Vec<u8>) {
    let data = tempfile::tempdir().unwrap();
    let (addr, _svc, _stop) = start(chat, data.path(), Duration::from_secs(30)).await;
    let mut a = connect(addr, "one").await;
    let mut b = connect(addr, "two").await;
    assert_eq!(next_text(&mut a).await, "* someone joined #one (1 online)");
    assert_eq!(next_text(&mut b).await, "* someone joined #two (1 online)");
    say(&mut a, "in one").await;
    assert_eq!(next_text(&mut a).await, "in one");
    say(&mut b, "in two").await;
    // b sees only its own room's message.
    assert_eq!(next_text(&mut b).await, "in two");
}

/// The room announces itself by its name, which it reads from the host
/// (`self-address`): decoded, exactly as the URL named it.
async fn a_room_knows_its_own_name(chat: Vec<u8>) {
    let data = tempfile::tempdir().unwrap();
    let (addr, _svc, _stop) = start(chat, data.path(), Duration::from_secs(30)).await;
    let mut ws = connect(addr, "caf%C3%A9%20au%20lait%3F").await;
    assert_eq!(
        next_text(&mut ws).await,
        "* someone joined #café au lait? (1 online)"
    );
}

/// `MOTD` from the operator (`env`), sent to each new connection first.
/// Twice, so a guest's cached lookup is exercised too.
async fn the_operator_s_motd_greets_each_newcomer(chat: Vec<u8>) {
    let data = tempfile::tempdir().unwrap();
    let mut env = wasmkeep_server::ActorEnv::default();
    env.extend(
        "chat",
        wasmkeep_server::env::parse("MOTD=\"be kind, café\"").unwrap(),
    );
    let (addr, _svc, _stop) = start_with_env(chat, data.path(), Duration::from_secs(30), env).await;
    let mut open = Vec::new(); // stay open: the second sees 2 online
    for n in 1..=2 {
        let mut ws = connect(addr, "lobby").await;
        assert_eq!(next_text(&mut ws).await, "* be kind, café");
        let joined = format!("* someone joined #lobby ({n} online)");
        assert_eq!(next_text_for(&mut ws, "the join notice").await, joined);
        open.push(ws);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_without_websocket_support_refuses_the_connection() {
    let data = tempfile::tempdir().unwrap();
    let (addr, svc, _stop) = start(rust_room(), data.path(), Duration::from_secs(30)).await;
    let (mut ws, _) = connect_async(format!("ws://{addr}/actor/counter/x"))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match msg {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected a 1008 close, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(svc.websockets(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutting_down_closes_sockets_with_1012() {
    // Clients learn the node is going away (1012: reconnect), instead of
    // seeing the connection drop.
    let data = tempfile::tempdir().unwrap();
    let (addr, _svc, stop) = start(rust_room(), data.path(), Duration::from_secs(30)).await;
    let mut ws = connect(addr, "lobby").await;
    assert!(next_text(&mut ws).await.contains("joined"));

    drop(stop);
    let started = std::time::Instant::now();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("no close frame within 5 s of shutdown");
        match msg {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(u16::from(frame.code), 1012, "{frame:?}");
                break;
            }
            Some(Ok(Message::Text(_))) => {}
            other => panic!(
                "expected a 1012 close after {:?}, got {other:?}",
                started.elapsed()
            ),
        }
    }
}
