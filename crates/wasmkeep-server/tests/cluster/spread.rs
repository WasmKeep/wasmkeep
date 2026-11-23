//! A big room's edges spread over the live nodes (ADR 0040, amendment of
//! 2026-11-22): in a version with the classes `room`, `pusher` and
//! `edge`, a wake-up of an edge nobody holds goes to the rendezvous choice
//! for the edge's address over the live nodes, the waking node included,
//! once; with no peer it wakes where it lands. A running edge never moves
//! for it. Off with placement.

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
    let [a, b, c] = three(&store, &dirs, |id, config| {
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
