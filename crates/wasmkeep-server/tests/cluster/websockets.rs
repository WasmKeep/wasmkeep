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
