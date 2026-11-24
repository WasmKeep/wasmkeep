//! A room and its pusher wake on different nodes (ADR 0041, amendment of
//! 2026-11-22): the classes `room` and `pusher` are kept apart by default,
//! so a wake-up of one, of an actor nobody holds, on the node that holds
//! the other goes to a peer, by a request, a call, an alarm or a drain;
//! with no peer it wakes where it lands. Off with placement.

use super::*;

/// A node serving the classes `room` and `pusher` (the test actor for
/// both), placement on as by default; `adjust` changes the rest.
async fn pair_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    adjust: impl FnOnce(&mut ActorConfig),
) -> Node {
    node_configured(
        id,
        data,
        store,
        Duration::from_secs(600),
        &["room", "pusher"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        adjust,
    )
    .await
}

/// The node `class/name`'s ownership record names, if any.
async fn holder(store: &Arc<dyn ObjectStore>, class: &str, name: &str) -> Option<String> {
    use object_store::ObjectStoreExt as _;
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("{class}/{name}")));
    let body = store.get(&key).await.ok()?.bytes().await.ok()?;
    wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)?
        .holder
        .map(|(node, _)| node)
}

/// Wakes `class/name` with a request to `node`; the node that ran it.
async fn wake(node: &Node, class: &str, name: &str) -> String {
    let r = send_reachable(
        node.addr,
        "GET",
        &format!("/actor/{class}/{name}/fired"),
        "",
    )
    .await;
    assert_eq!(r.status, 200, "{class}/{name}: {}", r.body);
    r.node
        .unwrap_or_else(|| panic!("{class}/{name}: no x-wasmkeep-node"))
}

/// A node with room for many residents: placement at wake-up stays on, as
/// by default, and its load rule keeps the few actors of these tests below
/// its floor (a fifth of the process's 16 instances), so only the rule
/// keeping a pair apart moves anything.
fn roomy(config: &mut ActorConfig) {
    config.mailbox.max_resident = 1000;
}

/// Three nodes, default placement. Each wake-up lands on `a`, which holds
/// the companion first: a request from outside, a call from the companion
/// itself (the pusher's pull wakes the room on the pusher's node), and the
/// reverse pair.
#[tokio::test(flavor = "multi_thread")]
async fn a_room_and_its_pusher_wake_on_different_nodes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = pair_node("a", dirs[0].path(), &store, roomy).await;
    let _b = pair_node("b", dirs[1].path(), &store, roomy).await;
    let _c = pair_node("c", dirs[2].path(), &store, roomy).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;

    // A request from outside: the room first, then its pusher, both on a.
    assert_eq!(wake(&a, "room", "lobby").await, "a");
    let pusher = wake(&a, "pusher", "lobby").await;
    assert_ne!(pusher, "a", "the pusher woke beside its room");
    assert_eq!(
        holder(&store, "pusher", "lobby").await.as_deref(),
        Some(pusher.as_str())
    );
    assert_eq!(holder(&store, "room", "lobby").await.as_deref(), Some("a"));

    // The other way round: a pusher on a, then its room.
    assert_eq!(wake(&a, "pusher", "hall").await, "a");
    assert_ne!(wake(&a, "room", "hall").await, "a");

    // A call from the companion: the pusher, on a, calls its room.
    assert_eq!(wake(&a, "pusher", "deck").await, "a");
    let r = send_reachable(a.addr, "POST", "/actor/pusher/deck/call?to=room/deck", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.starts_with("200:"), "{}", r.body);
    let room = holder(&store, "room", "deck").await;
    assert!(room.is_some() && room.as_deref() != Some("a"), "{room:?}");
}

/// The fallback: with one node alive, a room and its pusher wake there,
/// never refused. And nodes with placement off (`--no-placement`) keep
/// them where they land on three nodes.
#[tokio::test(flavor = "multi_thread")]
async fn alone_or_turned_off_the_pair_wakes_where_it_lands() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = pair_node("a", da.path(), &store, |_| {}).await;
    wait_drain_line(&store, "a", "serving").await;
    assert_eq!(wake(&a, "room", "lobby").await, "a");
    assert_eq!(wake(&a, "pusher", "lobby").await, "a");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let off = |config: &mut ActorConfig| {
        roomy(config);
        no_placement(config);
    };
    let a = pair_node("a", dirs[0].path(), &store, off).await;
    let _b = pair_node("b", dirs[1].path(), &store, off).await;
    let _c = pair_node("c", dirs[2].path(), &store, off).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    assert_eq!(wake(&a, "room", "lobby").await, "a");
    assert_eq!(wake(&a, "pusher", "lobby").await, "a");
}

/// `cluster.rs::rendezvous`, for choosing a name whose drain the plain
/// choice would send to the room's node.
fn weight(key: &str, node: &str) -> u64 {
    let mut h = key
        .bytes()
        .chain([0])
        .chain(node.bytes())
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

fn rendezvous<'a>(key: &str, nodes: &[&'a str]) -> &'a str {
    nodes
        .iter()
        .copied()
        .max_by_key(|n| (weight(key, n), *n))
        .unwrap()
}

/// A rolling restart's first step: the pusher's node drains. Its alarm
/// (handed off by the drain) and its next wake-up (sent on by the
/// draining node) go to the third node, not to the room's, though the
/// drain's plain choice for these names is the room's node.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_pushers_node_hands_it_to_the_node_without_its_room() {
    // Names whose pusher wakes apart from a on b (rendezvous over {b, c}),
    // and for which the drain's plain choice from b, over {a, c}, is a.
    let names: Vec<String> = (0..)
        .map(|i| format!("lobby{i}"))
        .filter(|n| {
            let key = format!("default/pusher/{n}");
            rendezvous(&key, &["b", "c"]) == "b" && rendezvous(&key, &["a", "c"]) == "a"
        })
        .take(2)
        .collect();
    let (alarmed, woken) = (&names[0], &names[1]);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = pair_node("a", dirs[0].path(), &store, roomy).await;
    let b = pair_node("b", dirs[1].path(), &store, roomy).await;
    let c = pair_node("c", dirs[2].path(), &store, roomy).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    for name in [alarmed, woken] {
        assert_eq!(wake(&a, "room", name).await, "a");
        assert_eq!(wake(&a, "pusher", name).await, "b");
    }
    // The first pusher's alarm, due after the drain.
    let path = format!("/actor/pusher/{alarmed}/alarm?in=2000");
    let r = send_reachable(a.addr, "POST", &path, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    drain(&store, "b").await;
    wait_drain_line(&store, "b", "drained").await;
    // The alarm was handed to c, which fires it there.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let fired_on = loop {
        if let Some(node) = holder(&store, "pusher", alarmed).await {
            break node;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the alarm never fired"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(fired_on, "c");
    let path = format!("/actor/pusher/{alarmed}/fired");
    let r = send_reachable(c.addr, "GET", &path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.head);
    // The next wake-up reaches the draining node: sent to c.
    assert_eq!(holder(&store, "pusher", woken).await, None);
    let r = send(b.addr, "GET", &format!("/actor/pusher/{woken}/fired"), "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("c")),
        "{}",
        r.body
    );
    for name in [alarmed, woken] {
        assert_eq!(holder(&store, "room", name).await.as_deref(), Some("a"));
    }
}

/// The count of one series of this process's metrics page (0 if absent).
fn metric(series: &str) -> u64 {
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    page.lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse().ok())
        .unwrap_or(0)
}

/// A placed wake-up (the load rule, ADR 0041 section 6) of a pusher to the
/// node that holds its room is refused there, and runs on its sender.
#[tokio::test(flavor = "multi_thread")]
async fn a_placed_hop_to_the_rooms_node_is_refused_and_runs_on_the_sender() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ten = |config: &mut ActorConfig| config.mailbox.max_resident = 10;
    // a alone first: its wake-ups are kept, to six of ten (600).
    let a = pair_node("a", dirs.0.path(), &store, ten).await;
    for i in 0..6 {
        assert_eq!(wake(&a, "room", &format!("fill{i}")).await, "a");
    }
    // b holds the room, one of ten (100).
    let b = pair_node("b", dirs.1.path(), &store, ten).await;
    assert_eq!(wake(&b, "room", "lobby").await, "b");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while wake(&a, "room", "lobby").await != "b" {
        assert!(std::time::Instant::now() < deadline, "a never saw b");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    settle().await;
    // a at 600, b at 100: the load rule sends the pusher to b, which holds
    // its room and refuses; a runs it.
    let apart =
        r#"wasmkeep_placements_received_total{tenant="default",app="default",outcome="apart"}"#;
    let before = metric(apart);
    assert_eq!(wake(&a, "pusher", "lobby").await, "a");
    assert_eq!(metric(apart) - before, 1);
    assert_eq!(
        holder(&store, "pusher", "lobby").await.as_deref(),
        Some("a")
    );
    // That refusal backs b off nothing: a's next placeable wake-up of an
    // actor without a companion there is still placed on b.
    assert_eq!(wake(&a, "room", "after").await, "b");
}

/// A node with the room, a dead node that held its pusher: the pusher's
/// record names the dead node, so routing answers "try here", not "nobody
/// holds it". The rule does not apply: the request waits for the takeover
/// here and is served (sent apart, a peer that cannot acquire it would
/// answer `503`), and no peer is suspected: the next pair wakes apart.
#[tokio::test(flavor = "multi_thread")]
async fn a_pusher_whose_record_names_a_dead_node_is_taken_over_not_sent_apart() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fd = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_d: Arc<dyn ObjectStore> = fd.clone();
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let waits = |config: &mut ActorConfig| {
        roomy(config);
        config.cluster.as_mut().unwrap().failover_wait = Some(Duration::from_secs(5));
    };
    let a = pair_node("a", dirs[0].path(), &shared, waits).await;
    let _b = pair_node("b", dirs[1].path(), &shared, waits).await;
    let _c = pair_node("c", dirs[2].path(), &shared, waits).await;
    let d = pair_node("d", dirs[3].path(), &seen_by_d, waits).await;
    for id in ["a", "b", "c", "d"] {
        wait_drain_line(&shared, id, "serving").await;
    }
    settle().await;
    assert_eq!(wake(&d, "pusher", "x").await, "d");
    assert_eq!(wake(&a, "room", "x").await, "a");
    // d dies: nothing more of it reaches the bucket, then its port closes.
    fd.down.store(true, Ordering::SeqCst);
    stop_listening(d).await;
    // Out of a's view: a POST, never kept, says d is not reachable.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r = send(a.addr, "POST", "/actor/pusher/x/durable", "").await;
        if r.status == 503 && r.body.trim() == "the actor's node is not reachable; retry" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "d never left a's view"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = send(a.addr, "GET", "/actor/pusher/x/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    // No peer was suspected: the next pair wakes apart.
    assert_eq!(wake(&a, "room", "y").await, "a");
    assert_ne!(wake(&a, "pusher", "y").await, "a");
}

/// An alarm due on a node that holds the companion and is not draining:
/// handed to a peer, which fires it there and never sends it on. The pair
/// woke together on a while it was alone and stays resident there, kept
/// busy, while peers join; the pusher sets its alarm and hibernates; the
/// room stays busy on a.
///
/// Kept busy from the start: idle, either would hibernate while the peers
/// start, and the wake-ups that brought them back would race (a pusher
/// woken while the room's wake-up is under way goes to a peer, apart).
#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_due_beside_its_room_is_handed_to_a_peer_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let short = |config: &mut ActorConfig| {
        roomy(config);
        config.mailbox.idle_timeout = Duration::from_millis(500);
    };
    let a = pair_node("a", dirs[0].path(), &store, short).await;
    wait_drain_line(&store, "a", "serving").await;
    assert_eq!(wake(&a, "room", "lobby").await, "a");
    assert_eq!(wake(&a, "pusher", "lobby").await, "a");
    // Each of the pair kept busy on a: an event every 50 ms, far inside
    // the 500 ms idle timeout.
    let busy = |class: &'static str| {
        let addr = a.addr;
        tokio::spawn(async move {
            loop {
                send(addr, "GET", &format!("/actor/{class}/lobby/fired"), "").await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };
    let room_busy = busy("room");
    let pusher_busy = busy("pusher");
    let _b = pair_node("b", dirs[1].path(), &store, roomy).await;
    let _c = pair_node("c", dirs[2].path(), &store, roomy).await;
    for id in ["b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    // The pusher, still resident on a, sets its alarm there; left alone,
    // it hibernates while the room stays busy.
    let r = send_reachable(a.addr, "POST", "/actor/pusher/lobby/alarm?in=3000", "").await;
    let due = std::time::Instant::now() + Duration::from_secs(3);
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    pusher_busy.abort();
    let _ = pusher_busy.await;
    assert_eq!(holder(&store, "room", "lobby").await.as_deref(), Some("a"));
    // Off a before the alarm is due: hibernated (no holder), or already
    // fired on the peer it was handed to.
    let mut now_on = holder(&store, "pusher", "lobby").await;
    while now_on.as_deref() == Some("a") {
        assert!(
            std::time::Instant::now() < due,
            "the pusher was still resident on a when its alarm came due"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        now_on = holder(&store, "pusher", "lobby").await;
    }
    // Due: handed to b or c, fired there.
    let deadline = due + Duration::from_secs(10);
    let fired_on = loop {
        if let Some(node) = now_on {
            break node;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the alarm never fired"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        now_on = holder(&store, "pusher", "lobby").await;
    };
    assert_ne!(fired_on, "a");
    room_busy.abort();
    assert_eq!(holder(&store, "room", "lobby").await.as_deref(), Some("a"));
    let r = send_reachable(a.addr, "GET", "/actor/pusher/lobby/fired", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.head);
}
