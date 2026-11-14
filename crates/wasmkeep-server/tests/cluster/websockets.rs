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

/// The node the ownership record of `chat/<name>` names, if any.
async fn chat_holder(store: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("chat/{name}")));
    let body = match store.get(&key).await {
        Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
        Err(object_store::Error::NotFound { .. }) => return None,
        Err(e) => panic!("{e}"),
    };
    let node = body.lines().nth(1)?.split(' ').next()?.to_owned();
    (node != "-").then_some(node)
}

/// Room `lobby` hibernated on `a` with its sockets open: no instance
/// left, its record kept naming `a` (ADR 0024, parked).
async fn lobby_parked(store: &Arc<dyn ObjectStore>, a: &Node) {
    eventually("the lobby hibernated on a, its record kept", async || {
        a.service.resident() == 0 && chat_holder(store, "lobby").await.as_deref() == Some("a")
    })
    .await;
}

/// The lobby's record made to name nobody behind its node's back, at a
/// higher generation: a fault ADR 0024 does not expect (a parked record
/// that no longer matches), so that another node can take the room while
/// its sockets stay on the first.
async fn release_lobby_behind(store: &Arc<dyn ObjectStore>) {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, "chat/lobby"));
    let body = String::from_utf8(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let generation: u64 = body.lines().nth(2).unwrap().parse().unwrap();
    let released = wasmkeep_cluster::node_lease::OwnerRecord {
        holder: None,
        generation: generation + 1,
    };
    store.put(&key, released.encode().into()).await.unwrap();
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
    release_lobby_behind(&store).await;
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

/// Serves this test process's metrics, and returns where.
async fn metrics_page() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    addr
}

/// Ownership records the owner sweeps of this process read so far.
async fn sweep_reads(metrics: SocketAddr) -> u64 {
    let (status, page) = get_raw(metrics, "/metrics").await;
    assert_eq!(status, 200);
    page.lines()
        .find_map(|l| l.strip_prefix("wasmkeep_websocket_sweep_reads_total "))
        .unwrap_or_else(|| panic!("no sweep reads in\n{page}"))
        .trim()
        .parse()
        .unwrap()
}

/// A client that only listens never sends a frame, so no event finds its
/// room gone: the owner sweep reads the record of each room with sockets
/// and no hold here, and closes them with 1012 once another node holds
/// it. A parked room (ADR 0024) is read only on one pass in ten, as a
/// safeguard: here its record is released behind a's back, a fault, and
/// b takes the room. A parked room that nobody took keeps its sockets.
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
    // Several sweeps read the record of a parked room nobody took: it
    // stays open. (Only a sweeps often; b's sweep runs once a minute.)
    let metrics = metrics_page().await;
    let start = sweep_reads(metrics).await;
    eventually("a swept the parked room twice", async || {
        sweep_reads(metrics).await >= start + 2
    })
    .await;
    stays_open(&mut listener, Duration::from_millis(100)).await;
    release_lobby_behind(&store).await;
    b_takes_lobby(&store, &b).await;
    let (code, reason) = closed_within(&mut listener, Duration::from_secs(5)).await;
    assert_eq!(code, 1012, "{reason}");
    eventually("a forgot the socket", async || a.service.websockets() == 0).await;
}

/// A node whose lease runs out (the bucket out of reach for longer than
/// ttl minus safety) may no longer own anything: its sockets close with
/// 1012 when the validity ends, without waiting for the bucket to answer
/// again (it never does here).
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
    down.store(true, Ordering::SeqCst);
    // Valid for ttl minus safety (800 ms here) after the last write.
    let (code, reason) = closed_within(&mut client, Duration::from_secs(5)).await;
    assert_eq!((code, reason.as_str()), (1012, "node lost its lease"));
}

/// `nodes/<id>` as written now (empty before its first write).
async fn heartbeat_body(store: &Arc<dyn ObjectStore>, id: &str) -> String {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(format!("nodes/{id}"));
    match store.get(&key).await {
        Ok(r) => String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap(),
        Err(object_store::Error::NotFound { .. }) => String::new(),
        Err(e) => panic!("{e}"),
    }
}

/// Waits until `nodes/<id>` was written `n` more times, and returns it.
async fn heartbeats(store: &Arc<dyn ObjectStore>, id: &str, n: usize) -> String {
    let mut last = heartbeat_body(store, id).await;
    for _ in 0..n {
        let before = last.clone();
        eventually("another heartbeat", async || {
            last = heartbeat_body(store, id).await;
            last != before
        })
        .await;
    }
    last
}

/// A draining node hands over its resident actors; a room that hibernated
/// with its sockets open is not resident, so its sockets used to stay
/// open, and the node reported `drained` while it still held them. Now
/// the drain closes them with 1012 and counts them until they are gone.
/// A client that never answers the close, and keeps pinging, holds its
/// socket only for the close wait: the node reports `draining` meanwhile,
/// then `drained`.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_closes_the_sockets_of_a_hibernated_room() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let wasm = chat_wasm();
    let close_wait = Duration::from_secs(2);
    let a = node_configured(
        "a",
        da.path(),
        &store,
        Duration::from_millis(300),
        &["chat"],
        wasm,
        Duration::from_secs(30),
        "",
        |c: &mut ActorConfig| c.websocket_close_wait = close_wait,
    )
    .await;
    let _b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "chat",
        wasm,
    )
    .await;
    settle().await;
    let client = join_lobby(a.addr).await;
    lobby_parked(&store, &a).await;
    // The client never reads the close (so never answers it), and pings.
    let (mut tx, mut rx) = client.split();
    let pinger = tokio::spawn(async move {
        while tx.send(Message::Ping(Vec::new().into())).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    drain(&store, "a").await;
    // The parked record is released at once (ADR 0024), before the socket
    // is gone: no record names a draining node for a room it gives up.
    eventually("the drain released the parked lobby", async || {
        chat_holder(&store, "lobby").await.is_none()
    })
    .await;
    // Several drain passes: the socket is closing, still here, counted.
    let body = heartbeats(&store, "a", 3).await;
    assert_eq!(a.service.websockets(), 1, "the socket went early");
    assert_eq!(drain_line(&body), Some("draining"), "{body}");
    // Past the close wait it is dropped, pings or not.
    eventually("a forgot the socket", async || a.service.websockets() == 0).await;
    pinger.abort();
    wait_drain_line(&store, "a", "drained").await;
    let code = loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.next()).await {
            Ok(Some(Ok(Message::Close(Some(f))))) => break u16::from(f.code),
            Ok(Some(Ok(_))) => {}
            other => panic!("no close frame: {other:?}"),
        }
    };
    assert_eq!(code, 1012);
}

/// The next text message `ws` gets within 5 s, past pings.
async fn next_text(ws: &mut Client) -> String {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return t.to_string(),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            other => panic!("no message: {other:?}"),
        }
    }
}

/// ADR 0024: a room that hibernated with its sockets open on a keeps its
/// record, so a request reaching it through b is relayed to a and wakes
/// it there (each time, at a new generation): the socket stays open and
/// the room still answers it. Once the last socket closes, the record is
/// released: any node may take the room.
#[tokio::test(flavor = "multi_thread")]
async fn a_room_with_open_sockets_wakes_on_their_node() {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let wasm = chat_wasm();
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_millis(300),
        "chat",
        wasm,
    )
    .await;
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "chat",
        wasm,
    )
    .await;
    settle().await;
    let mut client = join_lobby(a.addr).await;
    for round in 0..2 {
        lobby_parked(&store, &a).await;
        let parked_at = chat_generation(&store, "lobby").await;
        let r = send_reachable(b.addr, "GET", "/actor/chat/lobby/", "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "round {round}: {}",
            r.body
        );
        assert_eq!(chat_holder(&store, "lobby").await.as_deref(), Some("a"));
        assert!(
            chat_generation(&store, "lobby").await > parked_at,
            "round {round}: woken without a new generation"
        );
        client
            .send(Message::text(format!("still here {round}")))
            .await
            .unwrap();
        assert_eq!(next_text(&mut client).await, format!("still here {round}"));
    }
    assert_eq!(b.service.resident(), 0, "b ran the room");
    // The last socket goes: released, then free for any node.
    client.close(None).await.unwrap();
    eventually("a released the lobby after its last socket", async || {
        a.service.websockets() == 0 && chat_holder(&store, "lobby").await.is_none()
    })
    .await;
}

/// The generation of the ownership record of `chat/<name>`.
async fn chat_generation(store: &Arc<dyn ObjectStore>, name: &str) -> u64 {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("chat/{name}")));
    let body = String::from_utf8(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    body.lines().nth(2).unwrap().parse().unwrap()
}

/// Parked records this process released for `reason` so far.
async fn parked_releases(metrics: SocketAddr, reason: &str) -> u64 {
    let (status, page) = get_raw(metrics, "/metrics").await;
    assert_eq!(status, 200);
    let series = format!("wasmkeep_parked_releases_total{{reason=\"{reason}\"}} ");
    page.lines()
        .find_map(|l| l.strip_prefix(&series))
        .map_or(0, |v| v.trim().parse().unwrap())
}

/// ADR 0024: when the room's last socket goes and its close event cannot
/// run (here the event's wake cannot write the record), the freed slot
/// still releases the parked record: no record is left naming a node for
/// a room with no socket there. The release waits while the bucket
/// refuses it, and lands once it accepts.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_record_is_released_when_its_last_socket_goes() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky_a = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = flaky_a.clone();
    let da = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        da.path(),
        &seen_by_a,
        Duration::from_millis(300),
        &["chat"],
        chat_wasm(),
        Duration::from_secs(30),
        "",
        |c: &mut ActorConfig| c.cluster.as_mut().unwrap().socket_sweep = Duration::from_secs(600),
    )
    .await;
    settle().await;
    let metrics = metrics_page().await;
    let client = join_lobby(a.addr).await;
    lobby_parked(&shared, &a).await;
    let before = parked_releases(metrics, "last_socket").await;
    flaky_a.fail_owner_puts.store(true, Ordering::SeqCst);
    drop(client);
    eventually("a forgot the socket", async || a.service.websockets() == 0).await;
    eventually("the freed slot released the parked record", async || {
        parked_releases(metrics, "last_socket").await > before
    })
    .await;
    assert_eq!(
        chat_holder(&shared, "lobby").await.as_deref(),
        Some("a"),
        "released though the bucket refused it"
    );
    flaky_a.fail_owner_puts.store(false, Ordering::SeqCst);
    eventually("the release landed", async || {
        chat_holder(&shared, "lobby").await.is_none()
    })
    .await;
}
