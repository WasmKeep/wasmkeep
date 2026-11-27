//! A big room's edges spread over the live nodes (ADR 0040, amendment of
//! 2026-11-22): in a version with the classes `room`, `pusher` and
//! `edge`, a wake-up of an edge nobody holds goes to the rendezvous choice
//! for the edge's address over the live nodes, the waking node included,
//! once; with no peer it wakes where it lands. Off with placement.
//!
//! Since the amendment of 2026-11-26, that choice is the edge's home: a
//! node holding an edge whose home is another node moves it there once
//! the cluster is quiet and the home has served for the settle delay
//! (`--no-rebalance` turns that off), and a socket for an edge nobody
//! holds has its home take it first.

use super::*;

/// A node serving the broadcast SDKs' default class names (the test actor
/// for each), with an instance pool of 128: placement's floor (a fifth of
/// the residents it allows) is then far above the actors of these tests,
/// even with a stub for each edge of a `call-many` being woken, so the
/// load rule moves nothing and only the rules for a big room do. `adjust`
/// changes the rest.
async fn room_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    adjust: impl FnOnce(&mut ActorConfig),
) -> Node {
    start_node_pooled(
        id,
        data,
        store,
        Duration::from_secs(600),
        &["room", "pusher", "edge"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        adjust,
        false,
        128,
    )
    .await
    .0
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

/// `cluster.rs::rendezvous`: where a spread edge is predicted to wake.
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

fn rendezvous(key: &str, nodes: &[&str]) -> String {
    nodes
        .iter()
        .copied()
        .max_by_key(|n| (weight(key, n), *n))
        .unwrap()
        .to_owned()
}

/// Where `edge/<name>` is predicted to wake, over `nodes`.
fn choice(name: &str, nodes: &[&str]) -> String {
    rendezvous(&format!("default/edge/{name}"), nodes)
}

/// The node the pusher `pusher/lobby` wakes on when a, holding its room,
/// wakes it: the rendezvous choice over b and c (ADR 0041, amendment).
fn pusher_node() -> &'static str {
    if rendezvous("default/pusher/lobby", &["b", "c"]) == "b" {
        "b"
    } else {
        "c"
    }
}

/// The first edge name whose choice over the three nodes is `node`,
/// skipping `skip`.
fn edge_on(node: &str, skip: &[&str]) -> String {
    (1..)
        .map(|k| format!("lobby.e{k}"))
        .find(|e| !skip.contains(&e.as_str()) && choice(e, &["a", "b", "c"]) == node)
        .unwrap()
}

/// The count of one series of this process's metrics page (0 if absent).
fn metric(series: &str) -> u64 {
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    page.lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse().ok())
        .unwrap_or(0)
}

const SENT: &str =
    r#"wasmkeep_placement_spread_total{tenant="default",app="default",outcome="sent"}"#;
const PLACED: &str =
    r#"wasmkeep_placements_total{tenant="default",app="default",kind="call",outcome="placed"}"#;

/// The room's edges `lobby.e1` to `lobby.e12`, as the SDK names them.
fn edges() -> Vec<String> {
    (1..=12).map(|k| format!("lobby.e{k}")).collect()
}

/// One `call-many` from the pusher to every edge, sent through `via`: each
/// edge's `durable` count, which must be `count` (the edge ran each call
/// once, on one node).
async fn push(via: &Node, edges: &[String], count: u64) {
    let to: Vec<String> = edges.iter().map(|e| format!("edge/{e}")).collect();
    let path = format!(
        "/actor/pusher/lobby/call-many?to={}&path=%2Fdurable",
        to.join(",")
    );
    let r = send_reachable(via.addr, "POST", &path, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let want = vec![format!("200:{count}"); edges.len()].join(" | ");
    assert_eq!(r.body.trim(), want);
}

/// One call from the pusher to `edge/<name>`, through `via`: its answer.
async fn call(via: &Node, name: &str) -> String {
    let path = format!("/actor/pusher/lobby/call?to=edge/{name}&path=%2Fdurable");
    let r = send_reachable(via.addr, "POST", &path, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    r.body.trim().to_owned()
}

/// Three nodes, `adjust` for each by id, every one serving; the room woken
/// on a and its pusher, kept apart, on [`pusher_node`].
async fn three(
    store: &Arc<dyn ObjectStore>,
    dirs: &[tempfile::TempDir],
    adjust: impl Fn(&str, &mut ActorConfig),
) -> [Node; 3] {
    let mut nodes = Vec::new();
    for (id, dir) in ["a", "b", "c"].into_iter().zip(dirs) {
        nodes.push(room_node(id, dir.path(), store, |config| adjust(id, config)).await);
    }
    for id in ["a", "b", "c"] {
        wait_drain_line(store, id, "serving").await;
    }
    settle().await;
    let a = &nodes[0];
    let r = send_reachable(a.addr, "GET", "/actor/room/lobby/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send_reachable(a.addr, "GET", "/actor/pusher/lobby/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some(pusher_node())),
        "{}",
        r.body
    );
    nodes.try_into().ok().unwrap()
}

/// Three nodes, default placement. The room wakes on a and its pusher on
/// a peer (kept apart); one `call-many` of the pusher wakes twelve edges,
/// each on the rendezvous choice over the three nodes, the pusher's own
/// node included. The edges end on every node, none moves on the next
/// calls from any node, and when a node drains (a rolling restart's step)
/// its edges, released, wake where the pusher's next push sends them: the
/// choice over the two others.
#[tokio::test(flavor = "multi_thread")]
async fn a_rooms_edges_wake_spread_and_stay_put() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let [a, b, c] = three(&store, &dirs, |_, _| {}).await;
    let pusher = pusher_node();

    let edges = edges();
    let predicted: Vec<String> = edges.iter().map(|e| choice(e, &["a", "b", "c"])).collect();
    // The names cover every case: an edge whose choice is the pusher's own
    // node (woken where it is called), the room's node, and the third.
    for node in ["a", "b", "c"] {
        assert!(predicted.iter().any(|p| p == node), "{predicted:?}");
    }
    let (sent, placed) = (metric(SENT), metric(PLACED));
    push(&a, &edges, 1).await;
    let away = predicted.iter().filter(|p| *p != pusher).count() as u64;
    let mut held = Vec::new();
    for e in &edges {
        held.push(holder(&store, "edge", e).await.unwrap());
    }
    assert_eq!(held, predicted, "the pusher on {pusher}");
    assert_eq!(metric(SENT) - sent, away, "one hop per edge woken away");
    assert_eq!(metric(PLACED), placed, "the load rule placed none");

    // None bounces: the pusher's next calls, and requests from every node,
    // reach each edge where it is, and nothing is spread again.
    let sent = metric(SENT);
    push(&a, &edges, 2).await;
    push(&b, &edges, 3).await;
    for node in [&a, &b, &c] {
        for (e, at) in edges.iter().zip(&held) {
            let r = send_reachable(node.addr, "GET", &format!("/actor/edge/{e}/durable"), "").await;
            assert_eq!((r.status, r.body.trim()), (200, "3"), "{e}");
            assert_eq!(r.node.as_deref(), Some(at.as_str()), "{e}");
        }
    }
    assert_eq!(metric(SENT), sent, "a held edge is never spread");
    for (e, at) in edges.iter().zip(&held) {
        assert_eq!(holder(&store, "edge", e).await.as_ref(), Some(at), "{e}");
    }

    // A rolling restart's step: the node that holds neither the room nor
    // the pusher drains. A drain releases each edge's record and hands it
    // to nobody; the first event after decides where it wakes. Here that
    // is the pusher's next push (no client reconnects in this test), so
    // each released edge wakes on the choice over the two other nodes,
    // even on a node that still holds an owner hint naming the drained
    // one (the GETs above, under a second old). The others stay.
    let third = ["b", "c"].into_iter().find(|n| *n != pusher).unwrap();
    drain(&store, third).await;
    wait_drain_line(&store, third, "drained").await;
    for e in &edges {
        // A call reaching the drained node before its release answers
        // "retry", as a round's failed child does.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while call(&a, e).await != "200:4" {
            assert!(std::time::Instant::now() < deadline, "{e}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for (e, before) in edges.iter().zip(&held) {
        let now = holder(&store, "edge", e).await.unwrap();
        let want = if before == third {
            choice(e, &["a", pusher])
        } else {
            before.clone()
        };
        assert_eq!(now, want, "{e}: was on {before}");
    }
    assert_eq!(holder(&store, "room", "lobby").await.as_deref(), Some("a"));
}

/// The receiver of a spread hop still holds an owner hint (one second,
/// `HINT_MS`) naming the node that held the edge until just now. It reads
/// the record again instead of answering "ownership is changing": the
/// edge wakes there, and its sender suspects nothing (the next edge whose
/// choice is that node goes there too).
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_owner_hint_on_the_chosen_node_neither_refuses_nor_suspects_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let pusher = pusher_node();
    let third = if pusher == "b" { "c" } else { "b" };
    // The third node's actors hibernate soon after their last event.
    // Rebalancing off: the edge stays where the outside request woke it.
    let [a, b, c] = three(&store, &dirs, |id, config| {
        no_rebalance(config);
        if id == third {
            config.mailbox.idle_timeout = Duration::from_millis(200);
        }
    })
    .await;
    let on_third = if third == "b" { &b } else { &c };
    let edge = edge_on("a", &[]);
    let next = edge_on("a", &[edge.as_str()]);

    // The edge wakes on the third node (a request from outside is not
    // spread), then a learns from the record that the third node holds it.
    let path = format!("/actor/edge/{edge}/durable");
    let r = send_reachable(on_third.addr, "GET", &path, "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some(third)),
        "{}",
        r.body
    );
    let r = send_reachable(a.addr, "GET", &path, "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some(third)),
        "{}",
        r.body
    );
    let learned = std::time::Instant::now();
    // It hibernates there, its record released, within a's hint.
    while holder(&store, "edge", &edge).await.is_some() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        learned.elapsed() < Duration::from_millis(900),
        "released after a's hint expired: the test proves nothing"
    );
    // The pusher's call: spread to a, which holds the stale hint.
    assert_eq!(call(&a, &edge).await, "200:1");
    assert_eq!(holder(&store, "edge", &edge).await.as_deref(), Some("a"));
    // a was not suspected: the next edge whose choice is a goes there.
    assert_eq!(call(&a, &next).await, "200:1");
    assert_eq!(holder(&store, "edge", &next).await.as_deref(), Some("a"));
}

/// The chosen node does not run the edge's wake-up: it has no resident
/// slot (one, busy). Its host answers `503` before the actor, so the edge
/// runs where it was called, the pusher's node, once.
#[tokio::test(flavor = "multi_thread")]
async fn an_edge_its_chosen_node_does_not_run_runs_where_it_is_called_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let pusher = pusher_node();
    let third = if pusher == "b" { "c" } else { "b" };
    let [a, b, c] = three(&store, &dirs, |id, config| {
        no_rebalance(config);
        if id == third {
            config.mailbox.max_resident = 1;
        }
    })
    .await;
    let on_third = if third == "b" { &b } else { &c };
    // Its one slot, busy for a while.
    let addr = on_third.addr;
    let busy =
        tokio::spawn(async move { send(addr, "POST", "/actor/room/busy/sleep?ms=5000", "").await });
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while holder(&store, "room", "busy").await.is_none() {
        assert!(std::time::Instant::now() < deadline, "never busy");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let edge = edge_on(third, &[]);
    let sent = metric(SENT);
    assert_eq!(call(&a, &edge).await, "200:1", "ran once");
    assert_eq!(metric(SENT) - sent, 1, "sent to its choice first");
    assert_eq!(holder(&store, "edge", &edge).await.as_deref(), Some(pusher));
    let r = busy.await.unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
}

/// The fallback: with one node alive, every edge wakes there, never
/// refused. And nodes with placement off (`--no-placement`) wake each edge
/// where it is called, on three nodes.
#[tokio::test(flavor = "multi_thread")]
async fn alone_or_turned_off_the_edges_wake_where_they_are_called() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = room_node("a", da.path(), &store, |_| {}).await;
    wait_drain_line(&store, "a", "serving").await;
    let edges = edges();
    push(&a, &edges, 1).await;
    for e in &edges {
        assert_eq!(holder(&store, "edge", e).await.as_deref(), Some("a"), "{e}");
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = room_node("a", dirs[0].path(), &store, no_placement).await;
    let _b = room_node("b", dirs[1].path(), &store, no_placement).await;
    let _c = room_node("c", dirs[2].path(), &store, no_placement).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    let sent = metric(SENT);
    push(&a, &edges, 1).await;
    for e in &edges {
        assert_eq!(holder(&store, "edge", e).await.as_deref(), Some("a"), "{e}");
    }
    assert_eq!(metric(SENT), sent);
}

const MOVED: &str =
    r#"wasmkeep_rebalance_moves_total{tenant="default",app="default",outcome="moved"}"#;
const HOME: &str =
    r#"wasmkeep_placement_spread_total{tenant="default",app="default",outcome="home"}"#;

/// Waits until every edge of `edges` is held by its home over `nodes`.
async fn all_home(
    store: &Arc<dyn ObjectStore>,
    edges: &[String],
    nodes: &[&str],
    within: Duration,
) {
    let deadline = std::time::Instant::now() + within;
    loop {
        let mut away = Vec::new();
        for e in edges {
            let at = holder(store, "edge", e).await;
            let home = choice(e, nodes);
            if at.as_deref() != Some(home.as_str()) {
                away.push(format!("{e} on {at:?}, home {home}"));
            }
        }
        if away.is_empty() {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "not home: {away:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A client of `edge/<name>` that connects through `addr` and connects
/// again whenever its socket closes, until told to stop; `connected`
/// counts the clients connected now. Its answer: how many times it was
/// closed with 1012 ("reconnect").
fn reconnecting(
    addr: SocketAddr,
    name: String,
    connected: Arc<AtomicUsize>,
    stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<u32> {
    tokio::spawn(async move {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message;
        let mut stop = stop;
        let mut closes = 0;
        let url = format!("ws://{addr}/actor/edge/{name}");
        loop {
            if *stop.borrow() {
                return closes;
            }
            let Ok((mut ws, _)) = tokio_tungstenite::connect_async(&url).await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            connected.fetch_add(1, Ordering::SeqCst);
            let stopped = loop {
                tokio::select! {
                    _ = stop.changed() => break true,
                    msg = ws.next() => match msg {
                        Some(Ok(Message::Close(frame))) => {
                            if frame.is_some_and(|f| u16::from(f.code) == 1012) {
                                closes += 1;
                            }
                            // Answered, as a client does: read on until the
                            // close handshake ends.
                            while let Some(Ok(_)) = ws.next().await {}
                            break false;
                        }
                        Some(Ok(_)) => {}
                        _ => break false,
                    },
                }
            };
            drop(ws);
            connected.fetch_sub(1, Ordering::SeqCst);
            if stopped {
                return closes;
            }
            // A client's backoff before it reconnects.
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
}

/// A rolling restart's step, with a client on every edge (ADR 0040,
/// section 14, amendment of 2026-11-26): the node that holds neither the
/// room nor the pusher drains, is stopped and starts again. No rebalance
/// move starts while it drains; once it serves again, every edge is back
/// on its home over the three nodes, each client was closed with 1012 at
/// most twice (by the drain, then by the move home), and the edges'
/// counts went on without a gap.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_nodes_edges_return_home_and_nothing_moves_while_it_drains() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let pusher = pusher_node();
    let third = if pusher == "b" { "c" } else { "b" };
    let [a, b, c] = three(&store, &dirs, |_, _| {}).await;
    let (kept, gone) = if third == "b" { (c, b) } else { (b, c) };
    let edges = edges();
    let all = ["a", "b", "c"];
    push(&a, &edges, 1).await;
    all_home(&store, &edges, &all, Duration::from_secs(5)).await;
    assert!(edges.iter().any(|e| choice(e, &all) == third));
    let connected = Arc::new(AtomicUsize::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let clients: Vec<_> = edges
        .iter()
        .map(|e| reconnecting(a.addr, e.clone(), Arc::clone(&connected), stop_rx.clone()))
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while connected.load(Ordering::SeqCst) < edges.len() {
        assert!(
            std::time::Instant::now() < deadline,
            "clients never connected"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let moved = metric(MOVED);
    drain(&store, third).await;
    wait_drain_line(&store, third, "drained").await;
    push(&a, &edges, 2).await;
    let two = ["a", pusher];
    all_home(&store, &edges, &two, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        metric(MOVED),
        moved,
        "no rebalance move while a node drains"
    );

    // Stopped, then back as a new process with the same id.
    let dir = if third == "b" { &dirs[1] } else { &dirs[2] };
    stop_listening(gone).await;
    {
        use object_store::ObjectStoreExt as _;
        store
            .delete(&object_store::path::Path::from(format!(
                "control/drain/{third}"
            )))
            .await
            .unwrap();
    }
    announce_intent(&store).await;
    let back = room_node(third, dir.path(), &store, |_| {}).await;
    wait_drain_line(&store, third, "serving").await;
    all_home(&store, &edges, &all, Duration::from_secs(20)).await;
    assert!(metric(MOVED) > moved, "moved by the rebalance");
    push(&a, &edges, 3).await;
    stop_tx.send(true).unwrap();
    for (e, client) in edges.iter().zip(clients) {
        let closes = client.await.unwrap();
        assert!(closes <= 2, "{e}: closed {closes} times with 1012");
    }
    drop((kept, back));
}

/// A socket for an edge nobody holds, landing on a node that is not its
/// home (ADR 0040, section 14, amendment of 2026-11-26): its home takes the
/// edge first and the socket is relayed there. With rebalancing off it
/// wakes the edge where it lands, as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_socket_for_an_edge_nobody_holds_wakes_it_at_its_home() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    for on in [true, false] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let [a, b, c] = three(&store, &dirs, |_, config| {
            config.mailbox.idle_timeout = Duration::from_millis(200);
            if !on {
                no_rebalance(config);
            }
        })
        .await;
        // The edge exists (a room creates its edges before any socket
        // reaches them), woken on its home, then hibernated: nobody holds
        // it.
        let edge = edge_on("b", &[]);
        let path = format!("/actor/edge/{edge}/durable");
        let r = send_reachable(b.addr, "POST", &path, "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("b")),
            "{}",
            r.body
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while holder(&store, "edge", &edge).await.is_some() {
            assert!(std::time::Instant::now() < deadline, "never released");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let home = metric(HOME);
        let url = format!("ws://{}/actor/edge/{edge}", c.addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        ws.send(Message::text("hi")).await.unwrap();
        let echoed = loop {
            match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => break t.to_string(),
                Ok(Some(Ok(_))) => {}
                other => panic!("no echo: {other:?}"),
            }
        };
        assert_eq!(echoed, "hi");
        let want = if on { "b" } else { "c" };
        assert_eq!(
            holder(&store, "edge", &edge).await.as_deref(),
            Some(want),
            "rebalance {on}"
        );
        assert_eq!(metric(HOME) - home, u64::from(on));
        drop((a, b, c, ws));
    }
}

/// An edge woken away from its home (a request from outside wakes it
/// where it lands) is moved there once its home has been serving for the
/// settle delay, its state with it; with `--no-rebalance`, or with
/// placement off, it stays.
#[tokio::test(flavor = "multi_thread")]
async fn an_edge_away_from_home_moves_there_unless_rebalancing_is_off() {
    type Adjust = fn(&mut ActorConfig);
    let cases: [(&str, Adjust); 3] = [
        ("on", |_| {}),
        ("--no-rebalance", no_rebalance),
        ("--no-placement", no_placement),
    ];
    for (case, adjust) in cases {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let mut nodes = Vec::new();
        for (id, dir) in ["a", "b", "c"].into_iter().zip(&dirs) {
            nodes.push(room_node(id, dir.path(), &store, adjust).await);
        }
        for id in ["a", "b", "c"] {
            wait_drain_line(&store, id, "serving").await;
        }
        settle().await;
        let edge = edge_on("b", &[]);
        let path = format!("/actor/edge/{edge}/durable");
        let r = send_reachable(nodes[2].addr, "POST", &path, "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("c")),
            "{case}: {}",
            r.body
        );
        if case == "on" {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while holder(&store, "edge", &edge).await.as_deref() != Some("b") {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{case}: never moved home"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // Its state came with it.
            let r = send_reachable(nodes[0].addr, "GET", &path, "").await;
            assert_eq!(
                (r.status, r.node.as_deref(), r.body.trim()),
                (200, Some("b"), "1"),
                "{case}"
            );
        } else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(
                holder(&store, "edge", &edge).await.as_deref(),
                Some("c"),
                "{case}"
            );
        }
    }
}

/// An operator's move of an edge off its home (ADR 0017) is undone by the
/// rebalance, but only once the actor has stayed `stay` after its take.
#[tokio::test(flavor = "multi_thread")]
async fn an_operators_move_off_home_is_undone_after_the_stay() {
    let stay = Duration::from_secs(3);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let [a, b, c] = three(&store, &dirs, |_, config| {
        let cluster = config.cluster.as_mut().unwrap();
        cluster.rebalance_settings = Some(wasmkeep_cluster::rebalance::Settings {
            stay,
            ..wasmkeep_cluster::rebalance::Settings::for_period(cluster.heartbeat)
        });
    })
    .await;
    let edge = edge_on("b", &[]);
    let r = send_reachable(a.addr, "POST", &format!("/actor/edge/{edge}/durable"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    all_home(
        &store,
        std::slice::from_ref(&edge),
        &["a", "b", "c"],
        Duration::from_secs(5),
    )
    .await;
    request_move_of(&store, "edge", &edge, "c").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while holder(&store, "edge", &edge).await.as_deref() != Some("c") {
        assert!(
            std::time::Instant::now() < deadline,
            "the operator's move never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let moved = std::time::Instant::now();
    while holder(&store, "edge", &edge).await.as_deref() != Some("b") {
        assert!(
            moved.elapsed() < stay + Duration::from_secs(10),
            "never moved back home"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        moved.elapsed() >= stay - Duration::from_millis(300),
        "moved back before its stay: {:?}",
        moved.elapsed()
    );
    drop((a, b, c));
}

/// A pseudo-random delay in [0, 2 s), as ADR 0040's reconnecting client
/// draws its first one (rule 5), from `seed` and the attempt.
fn jitter(seed: u64, attempt: u64) -> Duration {
    let mut z = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(attempt.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    z = (z ^ (z >> 30)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    Duration::from_millis(z % 2000)
}

/// How long a client waits before a connection, from its seed and the
/// attempt.
type Delay = fn(u64, u64) -> Duration;

/// A client that reconnects 1 ms after a close or a refused upgrade,
/// with no backoff: the worst a client can do to a move.
fn at_once(_seed: u64, _attempt: u64) -> Duration {
    Duration::from_millis(1)
}

/// A client of `edge/<name>` through `addr` that waits `delay(seed,
/// attempt)` before every connection, the first included, and so after
/// every close or refused upgrade; `connected` counts the clients
/// connected now. Its answer: how many times it was closed with 1012.
fn crowd_client(
    addr: SocketAddr,
    name: String,
    seed: u64,
    delay: Delay,
    connected: Arc<AtomicUsize>,
    stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<u32> {
    tokio::spawn(async move {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message;
        let mut stop = stop;
        let (mut closes, mut attempt) = (0, 0);
        let url = format!("ws://{addr}/actor/edge/{name}");
        loop {
            attempt += 1;
            tokio::time::sleep(delay(seed, attempt)).await;
            if *stop.borrow() {
                return closes;
            }
            let Ok((mut ws, _)) = tokio_tungstenite::connect_async(&url).await else {
                continue;
            };
            connected.fetch_add(1, Ordering::SeqCst);
            let stopped = loop {
                tokio::select! {
                    _ = stop.changed() => break true,
                    msg = ws.next() => match msg {
                        Some(Ok(Message::Close(frame))) => {
                            if frame.is_some_and(|f| u16::from(f.code) == 1012) {
                                closes += 1;
                            }
                            while let Some(Ok(_)) = ws.next().await {}
                            break false;
                        }
                        Some(Ok(_)) => {}
                        _ => break false,
                    },
                }
            };
            drop(ws);
            connected.fetch_sub(1, Ordering::SeqCst);
            if stopped {
                return closes;
            }
        }
    })
}

const BUSY: &str =
    r#"wasmkeep_rebalance_moves_total{tenant="default",app="default",outcome="busy"}"#;

/// An edge away from its home with 30 clients, connected through another
/// node and reconnecting after a random [0, 2 s) wait (ADR 0040, section
/// 7, rule 5), is moved home while they come back: an upgrade that reaches
/// its old node while the move closes its sockets is refused with `503`
/// and tried again, so the move is not held up by sockets reopening behind
/// it. It ends home, every client connected again, each closed with 1012
/// at most twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_crowded_edge_moves_home_while_its_clients_reconnect_at_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // Its home is not a target for 6 s: the clients connect first.
    let [a, b, c] = three(&store, &dirs, |_, config| {
        let cluster = config.cluster.as_mut().unwrap();
        cluster.rebalance_settings = Some(wasmkeep_cluster::rebalance::Settings {
            settle: Duration::from_secs(6),
            ..wasmkeep_cluster::rebalance::Settings::for_period(cluster.heartbeat)
        });
    })
    .await;
    let edge = edge_on("b", &[]);
    let path = format!("/actor/edge/{edge}/durable");
    let r = send_reachable(c.addr, "POST", &path, "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("c")),
        "{}",
        r.body
    );
    let (moved, busy) = (metric(MOVED), metric(BUSY));
    let connected = Arc::new(AtomicUsize::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let clients: Vec<_> = (0..30)
        .map(|k| {
            crowd_client(
                a.addr,
                edge.clone(),
                k,
                jitter,
                Arc::clone(&connected),
                stop_rx.clone(),
            )
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while holder(&store, "edge", &edge).await.as_deref() != Some("b") {
        assert!(std::time::Instant::now() < deadline, "never moved home");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while connected.load(Ordering::SeqCst) < 30 {
        assert!(
            std::time::Instant::now() < deadline,
            "{} of 30 clients connected again",
            connected.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(metric(MOVED) > moved, "moved by the rebalance");
    assert!(
        metric(BUSY) - busy <= 1,
        "the move was held up by reopened sockets {} times",
        metric(BUSY) - busy
    );
    assert_eq!(holder(&store, "edge", &edge).await.as_deref(), Some("b"));
    stop_tx.send(true).unwrap();
    for client in clients {
        let closes = client.await.unwrap();
        assert!(closes <= 2, "closed {closes} times with 1012");
    }
    drop((a, b, c));
}

/// An edge away from its home that hibernated with a socket open keeps its
/// record parked on its node (ADR 0024); the pass moves it as a parked
/// room: the record released, the socket closed with 1012, the home taking
/// it. The client reconnects, through the old node, to the home.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_edge_with_a_socket_moves_home() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // c's residents hibernate soon after their last event, and the home
    // is not a target for 4 s: the edge parks on c first.
    let [a, b, c] = three(&store, &dirs, |id, config| {
        if id == "c" {
            config.mailbox.idle_timeout = Duration::from_millis(200);
        }
        let cluster = config.cluster.as_mut().unwrap();
        cluster.rebalance_settings = Some(wasmkeep_cluster::rebalance::Settings {
            settle: Duration::from_secs(4),
            ..wasmkeep_cluster::rebalance::Settings::for_period(cluster.heartbeat)
        });
    })
    .await;
    let edge = edge_on("b", &[]);
    let path = format!("/actor/edge/{edge}/durable");
    let r = send_reachable(c.addr, "POST", &path, "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("c")),
        "{}",
        r.body
    );
    // A socket on c, where it is held (relayed upgrades included).
    let url = format!("ws://{}/actor/edge/{edge}", c.addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let parked_moves = {
        let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
        page.lines()
            .find_map(|l| {
                l.strip_prefix(r#"wasmkeep_websocket_owner_closes_total{reason="move"}"#)?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0)
    };
    let closed = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Close(frame) = msg {
                return frame.map(|f| u16::from(f.code));
            }
        }
        None
    })
    .await
    .expect("closed by the move in time");
    assert_eq!(closed, Some(1012));
    // The sockets close before the take: it lands a moment later.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while holder(&store, "edge", &edge).await.as_deref() != Some("b") {
        assert!(std::time::Instant::now() < deadline, "never taken home");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let after = {
        let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
        page.lines()
            .find_map(|l| {
                l.strip_prefix(r#"wasmkeep_websocket_owner_closes_total{reason="move"}"#)?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0)
    };
    assert!(after > parked_moves, "closed as a parked room's move");
    // Reconnected through c, it reaches the edge on b.
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(Message::text("back")).await.unwrap();
    let echoed = loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => break t.to_string(),
            Ok(Some(Ok(_))) => {}
            other => panic!("no echo: {other:?}"),
        }
    };
    assert_eq!(echoed, "back");
    assert_eq!(holder(&store, "edge", &edge).await.as_deref(), Some("b"));
    drop((a, b, c, ws));
}

/// The ways the crowds below reconnect: at once, and after ADR 0040's
/// rule 5 delay.
const CROWDS: [(&str, Delay); 2] = [("1 ms", at_once), ("[0, 2 s)", jitter)];

/// 30 clients on each of `edges` through `via`, `delay` before each
/// connection; waits until all are connected.
async fn crowd(
    via: SocketAddr,
    edges: &[String],
    delay: Delay,
    connected: &Arc<AtomicUsize>,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<u32>> {
    let clients: Vec<_> = edges
        .iter()
        .flat_map(|e| std::iter::repeat_n(e, 30))
        .zip(0_u64..)
        .map(|(e, seed)| {
            crowd_client(
                via,
                e.clone(),
                seed,
                delay,
                Arc::clone(connected),
                stop.clone(),
            )
        })
        .collect();
    wait_connected(connected, clients.len(), "at first").await;
    clients
}

/// Waits until `want` clients are connected.
async fn wait_connected(connected: &AtomicUsize, want: usize, when: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while connected.load(Ordering::SeqCst) < want {
        assert!(
            std::time::Instant::now() < deadline,
            "{} of {want} clients connected {when}",
            connected.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Two edges whose home is c, woken there (through c).
async fn two_edges_on_c(c: &Node) -> Vec<String> {
    let first = edge_on("c", &[]);
    let edges = vec![first.clone(), edge_on("c", &[first.as_str()])];
    for e in &edges {
        let r = send_reachable(c.addr, "POST", &format!("/actor/edge/{e}/durable"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("c")),
            "{}",
            r.body
        );
    }
    edges
}

/// A drain's hand-over closes an actor's sockets as a move does, and gets
/// the same closing mark (ADR 0040, section 14): two edges on c, each with
/// 30 clients connected through a that reconnect at once (1 ms) or after
/// rule 5's random [0, 2 s), and c drains. Without the mark, upgrades
/// relayed back to c reopen sockets behind each hand-over, which then
/// finds them and waits for the next pass, again and again. With it, c is
/// `drained` within 15 s, every client is connected again, both edges are
/// then held elsewhere, and each client was closed with 1012 at most twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_hands_over_crowded_edges_whose_clients_reconnect_at_once() {
    use object_store::ObjectStoreExt as _;
    for (case, delay) in CROWDS {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let [a, b, c] = three(&store, &dirs, |_, _| {}).await;
        let edges = two_edges_on_c(&c).await;
        let connected = Arc::new(AtomicUsize::new(0));
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let clients = crowd(a.addr, &edges, delay, &connected, &stop_rx).await;

        let started = std::time::Instant::now();
        drain(&store, "c").await;
        let key = object_store::path::Path::from("nodes/c");
        loop {
            let body = store.get(&key).await.unwrap().bytes().await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if drain_line(&body) == Some("drained") {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "{case}: c not drained after {:?}: {:?}",
                started.elapsed(),
                drain_line(&body)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Released by c; woken elsewhere by the clients coming back.
        wait_connected(&connected, clients.len(), "after the drain").await;
        for e in &edges {
            let at = holder(&store, "edge", e).await;
            assert!(
                at.as_deref().is_some_and(|n| n != "c"),
                "{case}: {e} on {at:?}"
            );
        }
        stop_tx.send(true).unwrap();
        for client in clients {
            let closes = client.await.unwrap();
            assert!(closes <= 2, "{case}: closed {closes} times with 1012");
        }
        drop((a, b, c));
    }
}

/// An operator's move (ADR 0017) closes an actor's sockets as a rebalance
/// move does, and gets the same closing mark: two edges on c, each with
/// 30 clients connected through b that reconnect at once (1 ms) or after
/// rule 5's random [0, 2 s), are moved to b. Without the mark, upgrades
/// relayed back to c reopen sockets behind each move, whose job finds them
/// and gives up until a later pass, again and again. With it, both land on
/// b within 15 s, every client is connected again and each was closed with
/// 1012 at most twice. Rebalancing is off, so b keeps them.
///
/// The clients go through the target: between c's release and b's take,
/// an event landing on another node wakes the actor there (ADR 0017: the
/// move is then moot), and that node moves it on only if it still sees
/// the request.
#[tokio::test(flavor = "multi_thread")]
async fn an_operators_move_of_crowded_edges_whose_clients_reconnect_at_once() {
    for (case, delay) in CROWDS {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let [a, b, c] = three(&store, &dirs, |_, config| no_rebalance(config)).await;
        let edges = two_edges_on_c(&c).await;
        let connected = Arc::new(AtomicUsize::new(0));
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let clients = crowd(b.addr, &edges, delay, &connected, &stop_rx).await;

        let started = std::time::Instant::now();
        for e in &edges {
            request_move_of(&store, "edge", e, "b").await;
        }
        for e in &edges {
            loop {
                let at = holder(&store, "edge", e).await;
                if at.as_deref() == Some("b") {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(15),
                    "{case}: {e} not moved to b after {:?}: on {at:?}",
                    started.elapsed()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        wait_connected(&connected, clients.len(), "after the move").await;
        stop_tx.send(true).unwrap();
        for client in clients {
            let closes = client.await.unwrap();
            assert!(closes <= 2, "{case}: closed {closes} times with 1012");
        }
        for e in &edges {
            assert_eq!(
                holder(&store, "edge", e).await.as_deref(),
                Some("b"),
                "{case}"
            );
        }
        drop((a, b, c));
    }
}
