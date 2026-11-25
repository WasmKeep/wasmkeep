//! The per-class placement keys `apart-from` and `spread` (manifest format
//! 8), the first format behind the upgrade floor (ADR 0043, section 2): a
//! deploy that says one is refused until the app's upgrade is finalized,
//! and while a live node is older or does not announce the word `apart`;
//! once every node has it, it is accepted, and the nodes that follow the
//! app keep the classes it names apart and spread, as they do the
//! broadcast SDKs' `room`, `pusher` and `edge`. A deploy that says neither
//! keeps the format a node of 0.8 reads.
//!
//! The nodes and the `wasmkeep` that deploys stand for 0.9, the version
//! that introduced the format, whatever this build's version.

use object_store::ObjectStoreExt as _;
use wasmkeep_cluster::app_format::floor::{self, Minor};
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::upgrade::{self, Finalize};
use wasmkeep_server::apps::{Changes, DeployError};

use super::upgrade::{horizon, lease_of};
use super::*;

const NINE: Minor = Minor { major: 0, minor: 9 };
const EIGHT: Minor = Minor { major: 0, minor: 8 };
/// What a node of 0.9 announces.
const NEW: &str = "0.9.0";
/// Every word a node of 0.8.1 announces that a manifest needs.
const WORDS: &str = "take front handoff ends flush rate_limits assets blobs";

/// A node of 0.9 serving whatever is deployed for `default/default`, as
/// `serve --app` starts: the version's classes, and what it asks of
/// placement. It follows the app, and has an instance pool of 128, so
/// placement's floor (a fifth of it) is far above the actors of these
/// tests and its load rule moves nothing.
async fn following(id: &str, data: &Path, store: &Arc<dyn ObjectStore>) -> (Node, ActorService) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 128,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let app = AppRef::new(Arc::clone(store), "", "default", "default");
    let current = app.current().await.unwrap().expect("something is deployed");
    let version = app.version(&current.version).await.unwrap();
    let classes = version
        .classes
        .into_iter()
        .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
        .collect();
    let mut config = ActorConfig::new(data);
    config.access = version.access;
    config.placing = version.placing;
    config.mailbox.idle_timeout = Duration::from_secs(600);
    config.mailbox.max_resident = 1000;
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    cluster.version = Some(NEW.to_owned());
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(runtime, config, classes).unwrap();
    service.set_deploy_seq(current.seq);
    tokio::spawn(app.follow(service.clone(), Duration::from_millis(100)));
    let (stop, stopped) = oneshot::channel::<()>();
    spawn_serve(listener, cluster_port, service.clone(), async {
        let _ = stopped.await;
    });
    service.node_id_checked().await.unwrap();
    (
        Node {
            addr,
            service: service.clone(),
            _stop: stop,
        },
        service,
    )
}

/// The node `class/name`'s ownership record names, if any.
async fn holder(store: &Arc<dyn ObjectStore>, class: &str, name: &str) -> Option<String> {
    let key = object_store::path::Path::from(test_key(Family::Owners, &format!("{class}/{name}")));
    let body = store.get(&key).await.ok()?.bytes().await.ok()?;
    wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)?
        .holder
        .map(|(node, _)| node)
}

/// Wakes `class/name` with a request to `node`; the node that ran it.
async fn wake(node: &Node, class: &str, name: &str) -> String {
    let path = format!("/actor/{class}/{name}/fired");
    let r = send_reachable(node.addr, "GET", &path, "").await;
    assert_eq!(r.status, 200, "{class}/{name}: {}", r.body);
    r.node
        .unwrap_or_else(|| panic!("{class}/{name}: no x-wasmkeep-node"))
}

/// One call from `feed/<feed>`, through `via`, to `leaf/<name>`: a wake-up
/// of the leaf by a call, as a pusher wakes its edges.
async fn call_leaf(via: &Node, feed: &str, name: &str) {
    let path = format!("/actor/feed/{feed}/call?to=leaf/{name}&path=%2Fdurable");
    let r = send_reachable(via.addr, "POST", &path, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.starts_with("200:"), "leaf/{name}: {}", r.body);
}

/// `cluster.rs::rendezvous`: where a spread actor is predicted to wake.
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

/// The first name `<prefix><k>` whose `leaf` actor's choice over the three
/// nodes is `node`.
fn leaf_on(prefix: &str, node: &str) -> String {
    (1..)
        .map(|k| format!("{prefix}{k}"))
        .find(|name| {
            let key = format!("default/leaf/{name}");
            ["a", "b", "c"]
                .into_iter()
                .max_by_key(|n| (weight(&key, n), *n))
                == Some(node)
        })
        .unwrap()
}

/// The version's manifest as the bucket holds it.
async fn manifest_text(store: &Arc<dyn ObjectStore>, version: &str) -> String {
    let key = format!("apps/default/default/{version}/manifest");
    let got = store.get(&key.as_str().into()).await.unwrap();
    String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()
}

/// The app's classes, under its own names: a `feed` and its `fan` (a room
/// and its pusher, renamed) and a `leaf` (an edge, renamed).
fn classes() -> Changes {
    Changes {
        classes: Some(
            ["fan", "feed", "leaf"]
                .map(|c| (c.to_owned(), test_actor_wasm().to_vec()))
                .to_vec(),
        ),
        ..Changes::default()
    }
}

/// The same classes kept, with the keys: `fan` apart from `feed`, `leaf`
/// spread.
fn keys() -> Changes {
    Changes {
        apart: vec![("fan".to_owned(), Some("feed".to_owned()))],
        spread: vec![("leaf".to_owned(), Some(true))],
        ..Changes::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_with_apart_from_waits_for_the_floor_and_the_word_then_places() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // The app as the upgrade to 0.8 left it, its nodes now all on 0.9 and
    // the upgrade not yet finalized.
    store
        .put(
            &"formats/default/default/floor".into(),
            floor::encode(EIGHT).into(),
        )
        .await
        .unwrap();
    let bucket = ObjectStoreBucket::new(Arc::clone(&store), "");
    let app = AppRef::new(Arc::clone(&store), "", "default", "default").as_version(NINE);
    let rt = deploy_runtime();

    // Without the keys the version is written in the format a node of 0.8
    // reads, whatever the floor.
    let first = app.deploy_changes(&rt, classes()).await.unwrap();
    let text = manifest_text(&store, &first.current.version).await;
    assert!(text.starts_with("wasmkeep-manifest 1\n"), "{text}");

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (a, sa) = following("a", dirs[0].path(), &store).await;
    let (_b, sb) = following("b", dirs[1].path(), &store).await;
    let (_c, sc) = following("c", dirs[2].path(), &store).await;
    for id in ["a", "b", "c"] {
        wait_drain_line(&store, id, "serving").await;
    }
    settle().await;
    // The built-in rules are for the classes named room, pusher and edge:
    // these wake where they land.
    assert_eq!(wake(&a, "feed", "plain").await, "a");
    assert_eq!(wake(&a, "fan", "plain").await, "a");
    let unspread = leaf_on("plain", "b");
    call_leaf(&a, "plain", &unspread).await;
    assert_eq!(
        holder(&store, "leaf", &unspread).await.as_deref(),
        Some("a")
    );

    // The keys make manifest format 8. Refused while the upgrade is not
    // finalized, every node on 0.9 or not.
    let refused = async || {
        let e = app.deploy_changes(&rt, keys()).await.unwrap_err();
        let current = app.current().await.unwrap().unwrap();
        assert_eq!(current, first.current, "nothing deployed: {e}");
        e
    };
    let e = refused().await.to_string();
    assert!(
        e.contains("wasmkeep-manifest 8 needs every node at 0.9")
            && e.contains("upgrade floor is 0.8")
            && e.contains("cluster upgrade finalize"),
        "{e}"
    );
    assert_eq!(
        upgrade::finalize_write(&bucket, "default", "default", NINE, now_ms(), horizon()).await,
        Ok(Finalize::Raise(Some(EIGHT), NINE))
    );

    // Beside a live node of 0.8.1, one that started while the floor was
    // written: refused, naming it and its version.
    lease_of(&store, "old", "0.8.1", "live", WORDS).await;
    let e = refused().await.to_string();
    assert!(e.contains("old (0.8.1)"), "{e}");
    // Beside a node of 0.9 that does not announce `apart`: refused,
    // naming it.
    lease_of(&store, "old", NEW, "live", WORDS).await;
    let e = refused().await;
    assert!(
        matches!(&e, DeployError::NodesWithoutApart { nodes, .. } if nodes == "old"),
        "{e}"
    );
    // A deploy without the keys is still accepted beside it, in the older
    // format.
    let plain = app
        .deploy_changes(
            &rt,
            Changes {
                expire: vec![("leaf".to_owned(), Some(86_400))],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let text = manifest_text(&store, &plain.current.version).await;
    assert!(text.starts_with("wasmkeep-manifest 4\n"), "{text}");

    // Every live node announces the word: accepted, in format 8.
    lease_of(&store, "old", NEW, "live", &format!("{WORDS} apart")).await;
    let done = app.deploy_changes(&rt, keys()).await.unwrap();
    let text = manifest_text(&store, &done.current.version).await;
    assert!(text.starts_with("wasmkeep-manifest 8\n"), "{text}");
    assert!(text.ends_with("apart fan feed\nspread leaf on\n"), "{text}");
    assert_eq!(
        done.diff.placement,
        [
            "fan: apart from default -> feed",
            "leaf: spread default -> on"
        ]
    );

    // The written lease goes; the three nodes that follow the app run the
    // version and apply its keys.
    store.delete(&"nodes/old".into()).await.unwrap();
    for service in [&sa, &sb, &sc] {
        runs_deploy(service, done.current.seq).await;
    }
    settle().await;
    // `fan` apart from `feed`, both ways: the second of a pair to wake on
    // the node that holds the first goes to a peer.
    assert_eq!(wake(&a, "feed", "lobby").await, "a");
    let fan = wake(&a, "fan", "lobby").await;
    assert_ne!(fan, "a", "the fan woke beside its feed");
    assert_eq!(holder(&store, "fan", "lobby").await, Some(fan));
    assert_eq!(holder(&store, "feed", "lobby").await.as_deref(), Some("a"));
    assert_eq!(wake(&a, "fan", "hall").await, "a");
    assert_ne!(wake(&a, "feed", "hall").await, "a");
    // `leaf` spread: called by the feed on a, each wakes on the choice for
    // its own name over the live nodes.
    for node in ["a", "b", "c"] {
        let name = leaf_on("lobby.e", node);
        call_leaf(&a, "lobby", &name).await;
        assert_eq!(
            holder(&store, "leaf", &name).await.as_deref(),
            Some(node),
            "{name}"
        );
    }
}

/// A fresh app is born finalized (ADR 0043, section 3), so its first
/// deploy may carry the keys: it is checked on the floor it would create,
/// creates it once accepted, and writes format 8. An app that only looks
/// new, or one whose live neighbours under the prefix are older, gets no
/// floor and is refused before anything of it is written.
#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_apps_first_deploy_may_carry_the_keys() {
    let rt = deploy_runtime();
    let new_store = || -> Arc<dyn ObjectStore> { Arc::new(InMemory::new()) };
    let floor_of = async |store: &Arc<dyn ObjectStore>| match store
        .get(&"formats/default/default/floor".into())
        .await
    {
        Ok(got) => Some(got.bytes().await.unwrap().to_vec()),
        Err(_) => None,
    };
    let app_of = |store: &Arc<dyn ObjectStore>| {
        AppRef::new(Arc::clone(store), "", "default", "default").as_version(NINE)
    };
    let deploy = async |store: &Arc<dyn ObjectStore>, accept: bool| {
        let both = Changes {
            classes: classes().classes,
            ..keys()
        };
        app_of(store)
            .deploy_changes_with(&rt, both, |_| accept)
            .await
    };

    let fresh = new_store();
    assert!(matches!(
        deploy(&fresh, false).await,
        Err(DeployError::Stopped)
    ));
    assert_eq!(floor_of(&fresh).await, None, "cancelled: nothing written");
    let done = deploy(&fresh, true).await.unwrap();
    assert_eq!(floor_of(&fresh).await, Some(floor::encode(NINE)));
    let text = manifest_text(&fresh, &done.current.version).await;
    assert!(text.starts_with("wasmkeep-manifest 8\n"), "{text}");

    // State without a floor: not fresh. Let through on the floor it would
    // create, refused on the floor it has, and nothing of it is written.
    let with_state = new_store();
    with_state
        .put(&"state/default/default/owners/c/x".into(), "x".into())
        .await
        .unwrap();
    let e = deploy(&with_state, true).await.unwrap_err().to_string();
    assert!(e.contains("cluster upgrade finalize"), "{e}");
    assert_eq!(floor_of(&with_state).await, None);
    assert!(app_of(&with_state).current().await.unwrap().is_none());

    // A live node of 0.8.1 of another app under the prefix: the new app's
    // floor would not be above it, so the keys wait.
    let beside = new_store();
    let body = format!(
        "wasmkeep-node 3\nlive beef\n{}\nn\nhttp://127.0.0.1:9\n0.8.1\ndefault/other\n\n\nserving\n\n{WORDS}\n",
        now_ms() + 60_000
    );
    beside
        .put(&"nodes/n".into(), body.into_bytes().into())
        .await
        .unwrap();
    let e = deploy(&beside, true).await.unwrap_err().to_string();
    assert!(e.contains("upgrade floor is 0.8"), "{e}");
    assert_eq!(floor_of(&beside).await, None);
    assert!(app_of(&beside).current().await.unwrap().is_none());
}
