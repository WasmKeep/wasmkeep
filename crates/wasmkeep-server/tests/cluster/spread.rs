//! A big room's edges spread over the live nodes (ADR 0040, amendment of
//! 2026-11-22): in a version with the classes `room`, `pusher` and
//! `edge`, a wake-up of an edge nobody holds goes to the rendezvous choice
//! for the edge's address over the live nodes, the waking node included,
//! once; with no peer it wakes where it lands. A running edge never moves
//! for it. Off with placement.

use super::*;

/// A node serving the broadcast SDKs' default class names (the test actor
/// for each), with room for many residents so that the load rule keeps
/// these few actors below its floor: only the rules for a big room move
/// anything. `adjust` changes the rest.
async fn room_node(
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
        &["room", "pusher", "edge"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config: &mut ActorConfig| {
            config.mailbox.max_resident = 1000;
            adjust(config);
        },
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

/// Three of the room's edges, named as the SDK names them (`lobby.e<k>`):
/// the first whose rendezvous choice over the three nodes is `a`, `b` and
/// `c` each. One per node keeps each node, with the room or the pusher,
/// below placement's floor (a fifth of the 16 instances of a test
/// process), so the load rule moves none of them.
fn edges() -> Vec<String> {
    let mut on = std::collections::HashMap::new();
    (1..)
        .map(|k| format!("lobby.e{k}"))
        .filter(|e| {
            let n = on
                .entry(rendezvous(&format!("default/edge/{e}"), &["a", "b", "c"]))
                .or_insert(0);
            *n += 1;
            *n <= 1
        })
        .take(3)
        .collect()
}

/// The pusher's first call to each edge, one at a time, as its scale steps
/// create them (one split a step): each wakes the edge, on `count`.
async fn create(via: &Node, edges: &[String]) {
    for e in edges {
        let path = format!("/actor/pusher/lobby/call?to=edge/{e}&path=%2Fdurable");
        let r = send_reachable(via.addr, "POST", &path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, "200:1"), "{e}");
        // The stub left on the pusher's node for an edge woken away
        // closes before the next decision counts its residents.
        settle().await;
    }
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

/// Three nodes, default placement. The room wakes on a and its pusher on
/// a peer (kept apart); the pusher's calls wake its edges, each on the
/// rendezvous choice over the three nodes, the pusher's own node included.
/// The edges end on two nodes or more, none moves on the next calls from
/// any node, and a drain of one node (a rolling restart's step) keeps them
/// on two.
#[tokio::test(flavor = "multi_thread")]
async fn a_rooms_edges_wake_spread_and_stay_put() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = room_node("a", dirs[0].path(), &store, |_| {}).await;
    let b = room_node("b", dirs[1].path(), &store, |_| {}).await;
    let c = room_node("c", dirs[2].path(), &store, |_| {}).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;

    let r = send_reachable(a.addr, "GET", "/actor/room/lobby/fired", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send_reachable(a.addr, "GET", "/actor/pusher/lobby/fired", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let pusher = r.node.unwrap();
    assert_ne!(pusher, "a", "the pusher woke beside its room");

    let edges = edges();
    let predicted: Vec<String> = edges
        .iter()
        .map(|e| rendezvous(&format!("default/edge/{e}"), &["a", "b", "c"]))
        .collect();
    // The names cover every case: an edge whose choice is the pusher's own
    // node (woken where it is called), the room's node, and the third.
    for node in ["a", "b", "c"] {
        assert!(predicted.iter().any(|p| p == node), "{predicted:?}");
    }
    let (sent, placed) = (metric(SENT), metric(PLACED));
    create(&a, &edges).await;
    let away = predicted.iter().filter(|p| **p != pusher).count() as u64;
    let mut held = Vec::new();
    for e in &edges {
        held.push(holder(&store, "edge", e).await.unwrap());
    }
    assert_eq!(held, predicted, "the pusher on {pusher}");
    assert_eq!(metric(SENT) - sent, away, "one hop per edge woken away");
    assert_eq!(metric(PLACED), placed, "the load rule placed none");
    let nodes: std::collections::BTreeSet<_> = held.iter().collect();
    assert!(nodes.len() >= 2, "edges on {nodes:?}");

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
    // the pusher drains. Its edges go to the drain's choice over the other
    // two; the others stay. The edges are still on two nodes.
    let third = ["b", "c"].into_iter().find(|n| *n != pusher).unwrap();
    drain(&store, third).await;
    wait_drain_line(&store, third, "drained").await;
    // The pusher's next call to each edge: one whose hand-over is still
    // under way answers "retry", as a round's failed child does.
    for e in &edges {
        let path = format!("/actor/pusher/lobby/call?to=edge/{e}&path=%2Fdurable");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let r = send_reachable(a.addr, "POST", &path, "").await;
            if r.status == 200 && r.body.starts_with("200:") {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{e}: {}", r.body);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let mut after = std::collections::BTreeSet::new();
    for (e, before) in edges.iter().zip(&held) {
        let now = holder(&store, "edge", e).await.unwrap();
        assert_ne!(now, third, "{e}");
        if before != third {
            assert_eq!(&now, before, "{e} moved off a node that is not draining");
        }
        after.insert(now);
    }
    assert_eq!(after.len(), 2, "edges on {after:?} after the drain");
    assert_eq!(holder(&store, "room", "lobby").await.as_deref(), Some("a"));
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
