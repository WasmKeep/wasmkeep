//! A room and its pusher wake on different nodes (ADR 0041, amendment of
//! 2026-11-22): the classes `room` and `pusher` are kept apart by default,
//! so a wake-up of one on the node that holds the other goes to a peer, by
//! a request, a call or a drain; with no peer it wakes where it lands.

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

/// Three nodes with placement's load rule off (`--no-placement`), so
/// only this rule moves anything: it does not depend on load. Each
/// wake-up lands on `a`, which holds the companion first:
/// a request from outside, a call from the companion itself (the pusher's
/// pull wakes the room on the pusher's node), and the reverse pair.
#[tokio::test(flavor = "multi_thread")]
async fn a_room_and_its_pusher_wake_on_different_nodes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = pair_node("a", dirs[0].path(), &store, no_placement).await;
    let _b = pair_node("b", dirs[1].path(), &store, no_placement).await;
    let _c = pair_node("c", dirs[2].path(), &store, no_placement).await;
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

    // Another name is nobody's companion: it wakes where it lands.
    assert_eq!(wake(&a, "pusher", "alone").await, "a");
}

/// The fallback: with one node alive, a room and its pusher wake there,
/// never refused. And a version that turns the default pair off (`-`)
/// keeps them where they land on three nodes.
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
        no_placement(config);
        config.apart.insert(
            "pusher".into(),
            wasmkeep_cluster::placement::APART_NONE.into(),
        );
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

/// A rolling restart's first step: the pusher's node drains. Its next
/// wake-up goes to the third node, not to the room's, though the drain's
/// plain choice for this name is the room's node.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_pushers_node_hands_it_to_the_node_without_its_room() {
    // The pusher wakes apart from a on rendezvous({b, c}); from there the
    // drain's plain choice among {a, the other} must be a.
    let name = (0..)
        .map(|i| format!("lobby{i}"))
        .find(|n| {
            let key = format!("default/pusher/{n}");
            let first = rendezvous(&key, &["b", "c"]);
            let other = if first == "b" { "c" } else { "b" };
            rendezvous(&key, &["a", other]) == "a"
        })
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = pair_node("a", dirs[0].path(), &store, |_| {}).await;
    let b = pair_node("b", dirs[1].path(), &store, |_| {}).await;
    let c = pair_node("c", dirs[2].path(), &store, |_| {}).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    assert_eq!(wake(&a, "room", &name).await, "a");
    let first = wake(&a, "pusher", &name).await;
    let (draining, other) = match first.as_str() {
        "b" => (&b, "c"),
        "c" => (&c, "b"),
        n => panic!("the pusher woke on {n}"),
    };
    drain(&store, &first).await;
    wait_drain_line(&store, &first, "drained").await;
    assert_eq!(holder(&store, "pusher", &name).await, None);
    // The next wake-up reaches the draining node: sent to the node without
    // the room.
    let r = send(
        draining.addr,
        "GET",
        &format!("/actor/pusher/{name}/fired"),
        "",
    )
    .await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some(other)),
        "{}",
        r.body
    );
    assert_eq!(holder(&store, "room", &name).await.as_deref(), Some("a"));
}
