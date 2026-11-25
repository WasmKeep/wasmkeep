//! A node's start checks for an upgrade of WasmKeep itself (ADR 0043):
//! the skew check (section 5), the floor from below and the born-finalized
//! rule (section 3). Each node here is given the version it stands for, so
//! the tests do not depend on the workspace version.

use object_store::path::Path as StorePath;
use wasmkeep_cluster::app_format::floor::{self, Minor};

use super::*;

fn v(major: u32, minor: u32) -> Minor {
    Minor { major, minor }
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// A node of app `t/default`, standing for `version`.
fn node_at(id: &str, version: &str, store: &Arc<dyn ObjectStore>) -> Arc<Cluster> {
    let mut config = ClusterConfig::new(
        id,
        format!("http://{id}-{}", random_hex(4).unwrap()),
        Arc::clone(store),
    );
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    config.version = Some(version.to_owned());
    Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap()
}

/// The node lease of `id` for `app`, written directly: a node of
/// `version`, in `state`. A live one is dated ahead, so it stays live.
async fn lease_of(store: &Arc<dyn ObjectStore>, id: &str, app: &str, version: &str, state: &str) {
    let at = now_ms() + if state == "live" { 60_000 } else { 0 };
    let body = format!(
        "wasmkeep-node 3\n{state} beef\n{at}\n{id}\nhttp://{id}\n{version}\n{app}\n\n\nserving\n\ntake front\n"
    );
    store
        .put(
            &StorePath::from(format!("nodes/{id}")),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

async fn exists(store: &Arc<dyn ObjectStore>, key: &str) -> bool {
    store.head(&StorePath::from(key)).await.is_ok()
}

async fn floor_of(store: &Arc<dyn ObjectStore>) -> Option<Minor> {
    match store.get(&StorePath::from("formats/t/default/floor")).await {
        Ok(got) => floor::decode(Some(&got.bytes().await.unwrap())).unwrap(),
        Err(_) => None,
    }
}

/// Section 5: a node of N beside a live node of N-2, N-1, N, N+1 and N+2,
/// of another major version, and of a version that does not parse. A
/// refusal names the node and its version, writes no lease, and says so
/// at the readiness endpoint. Another app's node, and a stopped one, are
/// not this node's concern.
#[tokio::test]
async fn a_node_starts_only_beside_the_version_before_and_the_one_after() {
    let cases = [
        ("0.7.4", "t/default", "live", false),
        ("0.8.1", "t/default", "live", true),
        ("0.9.0+0a1b2c3d4e5f", "t/default", "live", true),
        ("0.10.0", "t/default", "live", true),
        ("0.11.0", "t/default", "live", false),
        ("1.9.0", "t/default", "live", false),
        ("nine", "t/default", "live", false),
        ("0.7.4", "t/blog", "live", true),
        ("0.7.4", "t/default", "released", true),
    ];
    let run = async |(version, app, state, starts): (&str, &str, &str, bool)| {
        let store = store();
        // The floor an app upgraded to 0.9 has: the floor's own checks
        // are not what is tested here.
        store
            .put(
                &StorePath::from("formats/t/default/floor"),
                floor::encode(v(0, 9)).into(),
            )
            .await
            .unwrap();
        lease_of(&store, "peer", app, version, state).await;
        let a = node_at("a", "0.9.2", &store);
        let got = a.identity().await;
        assert_eq!(
            got.is_ok(),
            starts,
            "beside {version} ({app}, {state}): {got:?}"
        );
        assert_eq!(exists(&store, "nodes/a").await, starts, "{version}");
        if let Err(why) = got {
            assert!(
                why.contains("node `peer`") && why.contains(&format!("`{version}`")),
                "{why}"
            );
            assert!(why.contains("0.8 to 0.10"), "{why}");
            assert_eq!(
                a.not_ready(),
                Some(
                    "a node of a WasmKeep version this one cannot run beside is live, or the \
                     app's last upgrade was not finalized"
                )
            );
        }
    };
    futures_util::future::join_all(cases.into_iter().map(run)).await;
}

/// Section 5: an object under `nodes/` that does not decode cannot be
/// judged by its fields. Left as it is through the start check, its node
/// stopped: this one starts, a lease's ttl plus grace after it first saw
/// the object. Rewritten meanwhile, or written since, a node this version
/// cannot read is running: the start is refused.
#[tokio::test]
async fn an_unread_node_object_written_during_the_start_refuses_it() {
    let put = async |store: &Arc<dyn ObjectStore>, key: &str, body: &str| {
        store
            .put(&StorePath::from(key), body.as_bytes().to_vec().into())
            .await
            .unwrap();
    };
    // Static: the node starts, no sooner than ttl plus grace (1.2 s).
    let still = store();
    put(&still, "nodes/z", "wasmkeep-node 4\nlive\n1\n").await;
    let started = std::time::Instant::now();
    let a = node_at("a", "0.9.0", &still);
    // Rewritten during the check.
    let moving = store();
    put(&moving, "nodes/z", "wasmkeep-node 4\nlive\n1\n").await;
    let b = node_at("b", "0.9.0", &moving);
    // Written since the first listing.
    let appearing = store();
    let c = node_at("c", "0.9.0", &appearing);
    tokio::time::sleep(Duration::from_millis(400)).await;
    put(&moving, "nodes/z", "wasmkeep-node 4\nlive\n2\n").await;
    put(&appearing, "nodes/y", "wasmkeep-node 4\nlive\n1\n").await;

    a.identity().await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(1_200));
    for (node, key, store) in [(&b, "nodes/z", &moving), (&c, "nodes/y", &appearing)] {
        let why = node.identity().await.unwrap_err();
        assert!(
            why.contains(&format!("`{key}`")) && why.contains("cannot read"),
            "{why}"
        );
        assert!(!exists(store, &format!("nodes/{}", node.node_id)).await);
    }
}

/// Section 3: a node of N refuses an app with registered nodes whose
/// floor is below N-1 (the upgrade before was never finalized). With no
/// record, that stops 0.10 and passes 0.9.
#[tokio::test]
async fn a_floor_below_the_previous_version_stops_the_start() {
    // No record (0.8), a stopped node registered.
    let unfinalized = store();
    lease_of(&unfinalized, "gone", "t/default", "0.9.0", "released").await;
    let ten = node_at("a", "0.10.0", &unfinalized);
    let nine = node_at("b", "0.9.0", &unfinalized);
    // Finalized at 0.9.
    let finalized = store();
    lease_of(&finalized, "gone", "t/default", "0.9.0", "released").await;
    finalized
        .put(
            &StorePath::from("formats/t/default/floor"),
            floor::encode(v(0, 9)).into(),
        )
        .await
        .unwrap();
    let after = node_at("a", "0.10.0", &finalized);

    let why = ten.identity().await.unwrap_err();
    assert!(
        why.contains("upgrade floor is 0.8")
            && why.contains("never finalized")
            && why.contains("this binary is 0.10"),
        "{why}"
    );
    assert!(!exists(&unfinalized, "nodes/a").await, "no lease written");
    nine.identity().await.unwrap();
    after.identity().await.unwrap();
    assert_eq!(
        floor_of(&unfinalized).await,
        None,
        "a start raises no floor"
    );
    assert_eq!(floor_of(&finalized).await, Some(v(0, 9)));
}

/// Section 3: a fresh app is born finalized at its first node's version,
/// and a node of the version before then stops at the floor. An app that
/// only looks new is not: with state or with a registered node, its floor
/// is left to `finalize` ("Not settled" 8, as decided: the app whose node
/// leases were all removed with `cluster forget`). A deploy alone does not
/// make it one that ran: with `serve --app` a node always starts after
/// one, and the deploy may have written no floor.
#[tokio::test]
async fn a_fresh_app_is_born_finalized_and_a_forgotten_one_is_not() {
    let fresh = store();
    let forgotten_state = store();
    let deployed = store();
    let registered = store();
    let put = async |store: &Arc<dyn ObjectStore>, key: &str| {
        store
            .put(&StorePath::from(key), b"x".to_vec().into())
            .await
            .unwrap();
    };
    put(&forgotten_state, "state/t/default/owners/room/lobby").await;
    put(&deployed, "apps/t/default/current").await;
    lease_of(&registered, "gone", "t/default", "0.8.1", "released").await;
    // Another app's state, deploy and node change nothing for this one.
    put(&fresh, "state/t/blog/owners/room/lobby").await;
    put(&fresh, "apps/t/blog/current").await;
    lease_of(&fresh, "other", "t/blog", "0.8.1", "released").await;

    let nodes = [
        node_at("a", "0.9.0", &fresh),
        node_at("a", "0.9.0", &forgotten_state),
        node_at("a", "0.9.0", &deployed),
        node_at("a", "0.9.0", &registered),
    ];
    for node in &nodes {
        node.identity().await.unwrap();
    }
    assert_eq!(floor_of(&fresh).await, Some(v(0, 9)));
    assert_eq!(
        store_body(&fresh, "formats/t/default/floor").await,
        b"wasmkeep-floor 1\n0.9\n"
    );
    assert_eq!(floor_of(&deployed).await, Some(v(0, 9)), "a deploy alone");
    for (what, store) in [
        ("state", &forgotten_state),
        ("a registered node", &registered),
    ] {
        assert_eq!(floor_of(store).await, None, "an app with {what}");
    }

    // 0.8.1 beside the born-finalized app: stopped at its first read.
    let old = node_at("b", "0.8.1", &fresh);
    let why = old.identity().await.unwrap_err();
    assert!(
        why.contains("finalized at 0.9") && why.contains("this binary is 0.8"),
        "{why}"
    );
    assert!(!exists(&fresh, "nodes/b").await, "no lease written");
    // And beside the forgotten app, it still starts: its way back is open.
    let back = node_at("b", "0.8.1", &forgotten_state);
    back.identity().await.unwrap();
    // A second node of the fresh app finds the floor and leaves it.
    let second = node_at("c", "0.10.0", &fresh);
    second.identity().await.unwrap();
    assert_eq!(floor_of(&fresh).await, Some(v(0, 9)));
}

/// Section 3: a single node restarted in place on a newer binary is not
/// the first node of a new app. Its own earlier lease names the version
/// it ran: from 0.8.1 to 0.9 it creates no floor (the way back stays open
/// until `finalize`), and from 0.8.1 to 0.10 it is refused, the upgrade
/// to 0.9 never having been finalized. Restarted on its own version, its
/// earlier lease is only its own.
#[tokio::test]
async fn a_node_restarted_in_place_on_a_newer_binary_creates_no_floor() {
    let one_step = store();
    lease_of(&one_step, "a", "t/default", "0.8.1", "released").await;
    let two_steps = store();
    lease_of(&two_steps, "a", "t/default", "0.8.1", "released").await;
    let same = store();
    lease_of(&same, "a", "t/default", "0.9.0", "released").await;
    let nine = node_at("a", "0.9.2", &one_step);
    let ten = node_at("a", "0.10.0", &two_steps);
    let again = node_at("a", "0.9.2", &same);

    nine.identity().await.unwrap();
    assert_eq!(floor_of(&one_step).await, None);
    let why = ten.identity().await.unwrap_err();
    assert!(
        why.contains("never finalized") && why.contains("this binary is 0.10"),
        "{why}"
    );
    assert_eq!(floor_of(&two_steps).await, None);
    again.identity().await.unwrap();
    assert_eq!(floor_of(&same).await, Some(v(0, 9)));
}

/// Section 5, "A skipped minor version": an app with no floor whose
/// nodes are all stopped is started on N only when a lease left names
/// N-1 or later. Stopped on 0.7, a 0.9 node writes no lease and no floor;
/// stopped on 0.8 it starts, and so it does beside a retired node's 0.7
/// lease, in a fresh app, and in one whose leases were all removed with
/// `cluster forget` (nothing says what ran it). A heartbeat from before
/// node leases is left to the start guard, as it was.
#[tokio::test]
async fn a_stopped_cluster_is_not_started_over_a_minor_version() {
    let skipped = store();
    for id in ["a", "b", "c"] {
        lease_of(&skipped, id, "t/default", "0.7.4", "released").await;
    }
    // Another app's nodes ran 0.8: they say nothing of this one.
    lease_of(&skipped, "other", "t/blog", "0.8.1", "released").await;
    let one_step = store();
    for id in ["a", "b", "c"] {
        lease_of(&one_step, id, "t/default", "0.8.0", "released").await;
    }
    let retired = store();
    lease_of(&retired, "gone", "t/default", "0.7.4", "fenced").await;
    lease_of(&retired, "b", "t/default", "0.8.1", "released").await;
    let fresh = store();
    let forgotten = store();
    forgotten
        .put(
            &StorePath::from("state/t/default/owners/room/lobby"),
            b"x".to_vec().into(),
        )
        .await
        .unwrap();
    let before_leases = store();
    before_leases
        .put(
            &StorePath::from("nodes/old"),
            b"wasmkeep-node 2\nold\nhttp://old\n0.3.0\n5\nt/default\n"
                .to_vec()
                .into(),
        )
        .await
        .unwrap();

    let earlier = store_body(&skipped, "nodes/a").await;
    for id in ["a", "d"] {
        let why = node_at(id, "0.9.0", &skipped).identity().await.unwrap_err();
        assert!(
            why.contains("last ran `0.7.4`")
                && why.contains("this binary is 0.9")
                && why.contains("install the last release of 0.8"),
            "{why}"
        );
    }
    assert!(!exists(&skipped, "nodes/d").await, "no lease written");
    assert_eq!(store_body(&skipped, "nodes/a").await, earlier);
    assert_eq!(floor_of(&skipped).await, None, "no floor written");
    // The release between, started over the same bucket; then 0.9 runs
    // beside it.
    node_at("a", "0.8.1", &skipped).identity().await.unwrap();
    node_at("d", "0.9.0", &skipped).identity().await.unwrap();

    node_at("a", "0.9.0", &one_step).identity().await.unwrap();
    assert_eq!(floor_of(&one_step).await, None);
    node_at("a", "0.9.0", &retired).identity().await.unwrap();
    // A replacement under the id of the node that crashed on 0.7 (its
    // lease is fenced, and no command removes it) starts too.
    node_at("gone", "0.9.0", &retired).identity().await.unwrap();
    // A node stopped cleanly on 0.7 and restarted in place is refused,
    // with its own ways out.
    let in_place = store();
    lease_of(&in_place, "c", "t/default", "0.7.4", "released").await;
    lease_of(&in_place, "b", "t/default", "0.9.0", "released").await;
    let why = node_at("c", "0.9.0", &in_place)
        .identity()
        .await
        .unwrap_err();
    assert!(
        why.contains("this node did not run 0.8")
            && why.contains("on this node and start it once")
            && why.contains("under a new node id")
            && why.contains("`wasmkeep cluster forget c`"),
        "{why}"
    );

    // An app deleted while it ran 0.7 and created again (`deploy
    // --recreate` leaves `recreated`): the leases of the incarnation
    // before, which `app delete` fenced and no command removes, say
    // nothing of this one. Every node id starts. A lease written since on
    // 0.7 is this incarnation's, and refuses.
    let again = store();
    lease_of(&again, "a", "t/default", "0.7.4", "fenced").await;
    lease_of(&again, "b", "t/default", "0.7.4", "released").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    again
        .put(
            &StorePath::from("apps/t/default/recreated"),
            b"wasmkeep-app-deleted 1\nby op\nat 5\nid x\n"
                .to_vec()
                .into(),
        )
        .await
        .unwrap();
    let since = store();
    lease_of(&since, "a", "t/default", "0.7.4", "fenced").await;
    since
        .put(
            &StorePath::from("apps/t/default/recreated"),
            b"wasmkeep-app-deleted 1\nby op\nat 5\nid x\n"
                .to_vec()
                .into(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    lease_of(&since, "c", "t/default", "0.7.4", "released").await;
    for id in ["a", "b", "d"] {
        node_at(id, "0.9.0", &again).identity().await.unwrap();
    }
    let why = node_at("d", "0.9.0", &since).identity().await.unwrap_err();
    assert!(why.contains("node `c` last ran `0.7.4`"), "{why}");
    node_at("a", "0.9.0", &fresh).identity().await.unwrap();
    assert_eq!(floor_of(&fresh).await, Some(v(0, 9)));
    node_at("a", "0.9.0", &forgotten).identity().await.unwrap();
    assert_eq!(floor_of(&forgotten).await, None);
    node_at("a", "0.9.0", &before_leases)
        .identity()
        .await
        .unwrap();
}

/// A node whose version has no minor version to compare is never judged
/// by a guess: it refuses to start, before any lease write.
#[tokio::test]
async fn a_version_with_no_minor_version_refuses_to_start() {
    let store = store();
    let a = node_at("a", "nine", &store);
    let why = a.identity().await.unwrap_err();
    assert!(why.contains("has no minor version"), "{why}");
    assert!(!exists(&store, "nodes/a").await, "no lease written");
    assert_eq!(floor_of(&store).await, None);
}

async fn store_body(store: &Arc<dyn ObjectStore>, key: &str) -> Vec<u8> {
    store
        .get(&StorePath::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}
