//! WebSocket upgrades reach the owner whichever node the client connects to.

use super::*;

/// The WebSocket upgrade's ownership probe acquires and restores an actor
/// with no instance started. A client that hangs up right after sending
/// its upgrade leaves it so, and the committed alarm must still be armed
/// here: it used to wait for the sweep (review). Node a sets the alarm and
/// dies without handing it over; once a is fenced, an upgrade to b that is
/// abandoned at once takes the actor, and the alarm fires on b on time,
/// with no other request to the actor.
#[tokio::test(flavor = "multi_thread")]
async fn an_abandoned_upgrade_that_takes_an_actor_arms_its_alarm() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let wall = Duration::from_secs(30);
    // A slow sweep: only arming on the takeover can deliver it on time.
    let slow_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().alarm_sweep = Duration::from_secs(60);
    };
    let wasm = test_actor_wasm();
    let a = node_configured(
        "a",
        da.path(),
        &seen_by_a,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    let b = node_configured(
        "b",
        db.path(),
        &shared,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        slow_sweep,
    )
    .await;
    settle().await;
    let due = now_ms() + 6_000;
    let r = send(a.addr, "POST", &format!("/actor/t/al/alarm?at={due}"), "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    settle().await;
    // Killed: nothing more reaches the bucket, then it serves nothing.
    down.store(true, Ordering::SeqCst);
    stop_listening(a).await;
    // b fences a once a's lease is stale (about 2 s with these timings).
    let started = std::time::Instant::now();
    loop {
        let body = shared
            .get(&object_store::path::Path::from("nodes/a"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        if String::from_utf8_lossy(&body)
            .lines()
            .nth(1)
            .is_some_and(|l| l.starts_with("fenced"))
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a was never fenced"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // ttl plus grace after a's last write: the record is free for b.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(now_ms() + 1_500 < due, "too slow to test before the alarm");
    // Upgrades, each abandoned as soon as it is sent, until one's probe
    // took the actor on b. A connection closed that early may also be
    // dropped before its request reaches the probe (it failed so once,
    // under load): then nothing happened, and the next one tries again.
    // None opens a socket, so no event runs the actor on b.
    let started = std::time::Instant::now();
    while lease_holder(&shared, "al").await.as_deref() != Some("b") {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "b never took al"
        );
        let mut stream = TcpStream::connect(b.addr).await.unwrap();
        stream
            .write_all(
                b"GET /actor/t/al HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();
        drop(stream);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Taken before the alarm's time, or on-time arming is not what this
    // shows (under load the loop could run past it).
    assert!(
        now_ms() + 200 < due,
        "too slow: b took al after the alarm's time"
    );
    // Not asked before its time: a request would start the instance on b
    // and arm the alarm whatever the probe did.
    tokio::time::sleep(Duration::from_millis((due + 800).saturating_sub(now_ms()))).await;
    let r = send(b.addr, "GET", "/actor/t/al/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref(), r.body.trim()),
        (200, Some("b"), "1"),
        "the alarm did not fire within 800 ms of its time"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn websockets_reach_the_room_whichever_node_they_connect_to() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "chat", chat_wasm()).await;
    let b = node_with("b", db.path(), &store, idle, "chat", chat_wasm()).await;
    settle().await;

    async fn next(
        ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    ) -> String {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => return t.to_string(),
                Ok(Some(Ok(Message::Close(f)))) => panic!("closed: {f:?}"),
                Ok(Some(Ok(_))) => {}
                other => panic!("no message: {other:?}"),
            }
        }
    }

    // alice connects to a, which takes the room; bob connects to b, which
    // relays his socket to a. Both are in the same room.
    let (mut alice, _) = connect_async(format!("ws://{}/actor/chat/lobby", a.addr))
        .await
        .unwrap();
    assert_eq!(next(&mut alice).await, "* someone joined #lobby (1 online)");
    let (mut bob, _) = connect_async(format!("ws://{}/actor/chat/lobby", b.addr))
        .await
        .unwrap();
    assert_eq!(next(&mut alice).await, "* someone joined #lobby (2 online)");
    assert_eq!(next(&mut bob).await, "* someone joined #lobby (2 online)");

    bob.send(Message::text("bob: hi from b")).await.unwrap();
    assert_eq!(next(&mut alice).await, "bob: hi from b");
    assert_eq!(next(&mut bob).await, "bob: hi from b");
    alice.send(Message::text("alice: hi from a")).await.unwrap();
    assert_eq!(next(&mut bob).await, "alice: hi from a");
}

#[tokio::test(flavor = "multi_thread")]
async fn awkward_names_reach_their_room_from_any_node() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &store, idle, "chat", chat_wasm()).await;
    let b = node_with("b", db.path(), &store, idle, "chat", chat_wasm()).await;
    settle().await;
    for name in awkward_names() {
        let enc = url_encode(&name);
        let (mut on_a, _) = connect_async(format!("ws://{}/actor/chat/{enc}", a.addr))
            .await
            .unwrap_or_else(|e| panic!("{name:?}: {e}"));
        let (mut on_b, _) = connect_async(format!("ws://{}/actor/chat/{enc}", b.addr))
            .await
            .unwrap_or_else(|e| panic!("{name:?}: {e}"));
        on_b.send(Message::text("hi")).await.unwrap();
        // a's client sees joins, then the message relayed from b.
        let mut seen = Vec::new();
        while !seen.iter().any(|t: &String| t == "hi") {
            match tokio::time::timeout(Duration::from_secs(5), on_a.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => seen.push(t.to_string()),
                other => panic!("{name:?}: {other:?} after {seen:?}"),
            }
        }
    }
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A chat client of room `lobby` on the node at `addr`, past its join
/// message.
async fn join_lobby(addr: SocketAddr) -> Client {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/actor/chat/lobby"))
        .await
        .unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) if t.contains("joined") => return ws,
            Ok(Some(Ok(_))) => {}
            other => panic!("no join message: {other:?}"),
        }
    }
}

/// The close code and reason `ws` gets within `within`, reading past any
/// message; panics if it gets none. Reads on until the connection ends, so
/// the client's answer to the close goes out, as a browser's does.
async fn closed_within(ws: &mut Client, within: Duration) -> (u16, String) {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let deadline = tokio::time::Instant::now() + within;
    let closed = loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(f))))) => {
                break (u16::from(f.code), f.reason.to_string());
            }
            Ok(Some(Ok(Message::Close(None)))) => break (1005, String::new()),
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("the socket ended without a close frame: {other:?}"),
            Err(_) => panic!("not closed within {within:?}"),
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
    closed
}

/// Whether `ws` stays open, with nothing but messages, for `for_`.
async fn stays_open(ws: &mut Client, for_: Duration) {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let deadline = tokio::time::Instant::now() + for_;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) => return,
            Ok(Some(Ok(Message::Text(_) | Message::Ping(_) | Message::Pong(_)))) => {}
            other => panic!("closed: {other:?}"),
        }
    }
}

/// The node `owners/default/chat/<name>` names, if any.
async fn chat_holder(store: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(format!("owners/default/chat/{name}"));
    let body = match store.get(&key).await {
        Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
        Err(object_store::Error::NotFound { .. }) => return None,
        Err(e) => panic!("{e}"),
    };
    let node = body.lines().nth(1)?.split(' ').next()?.to_owned();
    (node != "-").then_some(node)
}

/// Room `lobby` hibernated on `a` with its sockets open: its record
/// released, no instance left.
async fn lobby_parked(store: &Arc<dyn ObjectStore>, a: &Node) {
    eventually("the lobby hibernated on a", async || {
        a.service.resident() == 0 && chat_holder(store, "lobby").await.is_none()
    })
    .await;
}

/// b takes the lobby with a plain request, and holds it.
async fn b_takes_lobby(store: &Arc<dyn ObjectStore>, b: &Node) {
    let r = send_reachable(b.addr, "GET", "/actor/chat/lobby/", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    assert_eq!(chat_holder(store, "lobby").await.as_deref(), Some("b"));
}

/// A room that hibernated with sockets open on a, then taken by b, has
/// sockets on the wrong node. The first one to send finds a no longer
/// owns the room, and every socket of the room on a closes with 1012, not
/// only that one: a client that only listens would otherwise stay
/// connected to a room that no longer broadcasts to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_room_taken_by_another_node_closes_all_its_sockets_on_the_first_message() {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let wasm = chat_wasm();
    let wall = Duration::from_secs(30);
    // a parks the room soon; b keeps what it takes. The sweep never runs
    // here, so only the first message can close the listener.
    let no_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().socket_sweep = Duration::from_secs(600);
    };
    let idle = Duration::from_millis(300);
    let a = node_configured(
        "a",
        da.path(),
        &store,
        idle,
        &["chat"],
        wasm,
        wall,
        "",
        no_sweep,
    )
    .await;
    let long = Duration::from_secs(30);
    let b = node_configured(
        "b",
        db.path(),
        &store,
        long,
        &["chat"],
        wasm,
        wall,
        "",
        no_sweep,
    )
    .await;
    settle().await;
    let mut talker = join_lobby(a.addr).await;
    let mut listener = join_lobby(a.addr).await;
    lobby_parked(&store, &a).await;
    b_takes_lobby(&store, &b).await;
    talker.send(Message::text("anyone?")).await.unwrap();
    assert_eq!(
        closed_within(&mut talker, Duration::from_secs(5)).await.0,
        1012
    );
    assert_eq!(
        closed_within(&mut listener, Duration::from_secs(5)).await.0,
        1012,
        "the listening client stayed on a"
    );
}

/// A client that only listens never sends a frame, so no event finds its
/// room gone: the owner sweep reads the record of each room with sockets
/// and no hold here, and closes them with 1012 once another node holds
/// it. A parked room that nobody took keeps its sockets.
#[tokio::test(flavor = "multi_thread")]
async fn the_owner_sweep_closes_a_listening_client_of_a_room_taken_elsewhere() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let wasm = chat_wasm();
    let wall = Duration::from_secs(30);
    let quick_sweep = |c: &mut ActorConfig| {
        c.cluster.as_mut().unwrap().socket_sweep = Duration::from_millis(200);
    };
    let idle = Duration::from_millis(300);
    let a = node_configured(
        "a",
        da.path(),
        &store,
        idle,
        &["chat"],
        wasm,
        wall,
        "",
        quick_sweep,
    )
    .await;
    let long = Duration::from_secs(30);
    let b = node_with("b", db.path(), &store, long, "chat", wasm).await;
    settle().await;
    let mut listener = join_lobby(a.addr).await;
    lobby_parked(&store, &a).await;
    // Several sweeps over a parked room nobody took: it stays open.
    stays_open(&mut listener, Duration::from_secs(1)).await;
    b_takes_lobby(&store, &b).await;
    let (code, reason) = closed_within(&mut listener, Duration::from_secs(5)).await;
    assert_eq!(code, 1012, "{reason}");
    eventually("a forgot the socket", async || a.service.websockets() == 0).await;
}

/// A node whose lease runs out (the bucket out of reach for longer than
/// ttl minus safety) may no longer own anything, and a peer may claim its
/// actors once ttl plus grace passed: its sockets close with 1012 when
/// the validity ends, before any peer can claim, without waiting for the
/// bucket to answer again.
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_node_lease_closes_its_sockets() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let da = tempfile::tempdir().unwrap();
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "chat", chat_wasm()).await;
    settle().await;
    let mut client = join_lobby(a.addr).await;
    let cut = std::time::Instant::now();
    down.store(true, Ordering::SeqCst);
    // Valid for ttl minus safety (800 ms here) after the last write.
    let (code, reason) = closed_within(&mut client, Duration::from_secs(5)).await;
    assert_eq!((code, reason.as_str()), (1012, "node lost its lease"));
    assert!(
        cut.elapsed() < Duration::from_secs(3),
        "closed after {:?}",
        cut.elapsed()
    );
    assert!(
        down.load(Ordering::SeqCst),
        "the bucket is still out of reach"
    );
}
