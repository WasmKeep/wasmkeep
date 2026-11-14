//! The front in deploys (ADR 0023): it travels with the version, every
//! node switches to it with the classes, a rollback restores the one
//! before, `--no-front` removes it, its values come from the same three
//! layers as a class's, and nodes that cannot read it hold the deploy back.

use std::collections::BTreeSet;

use wasmkeep_server::apps::{Changes, DeployError, FrontChange};
use wasmkeep_server::secret_store::SecretStore;
use wasmkeep_server::secrets::SecretsKey;

use super::*;

fn front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-front", "test_front"))
}

/// A second front: answers `ok` to anything (a plain `wasi:http/proxy`
/// component).
fn other_front_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/misbehave", "misbehave"))
}

fn names(n: &[&str]) -> BTreeSet<String> {
    n.iter().map(|s| (*s).to_owned()).collect()
}

fn vars(text: &str) -> Option<wasmkeep_server::env::Vars> {
    Some(wasmkeep_server::env::parse(text).unwrap())
}

/// A node following `default/shop` as `serve --app` does: the version's
/// classes, rules, values and front, the front served beside the actors,
/// and, with `secrets`, the app's secrets followed every 100 ms.
async fn front_app_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    secrets: Option<&SecretsKey>,
) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let app = AppRef::new(Arc::clone(store), "", "default", "shop");
    let current = app.current().await.unwrap().expect("something is deployed");
    let version = app.version(&current.version).await.unwrap();
    let classes = version
        .classes
        .into_iter()
        .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
        .collect();
    let mut config = ActorConfig::new(data);
    config.app = "shop".into();
    config.access = version.access;
    config.config = version.config;
    config.requires = version.requires;
    config.front = version.front.map(|f| wasmkeep_server::DeployedFront {
        component: runtime.compile(&f.wasm).unwrap(),
        config: f.config,
        requires: f.requires,
    });
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    cluster.app = "default/shop".into();
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(runtime, config, classes).unwrap();
    service.set_deploy_seq(current.seq);
    tokio::spawn(app.follow(service.clone(), Duration::from_millis(100)));
    if let Some(key) = secrets {
        let store = SecretStore::new(Arc::clone(store), "", "default", "shop");
        tokio::spawn(store.follow(
            service.clone(),
            vec![key.clone()],
            None,
            Duration::from_millis(100),
            wasmkeep_server::secret_store::STALE_AFTER,
        ));
    }
    let front = wasmkeep_server::HttpComponent::deployed(&service);
    let app = wasmkeep_server::App {
        front: Some(front),
        actors: Some(service.clone()),
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = async {
        let _ = stopped.await;
    };
    match cluster_port {
        Some(cluster) => tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
            listener,
            cluster,
            app,
            stopped,
            wasmkeep_server::ServeLimits::default(),
        )),
        None => tokio::spawn(wasmkeep_server::serve(listener, app, stopped)),
    };
    let _ = service.node_id_checked().await;
    Node {
        addr,
        service,
        _stop: stop,
    }
}

/// Waits until `path` on `addr` answers `status` and `body`.
async fn serves(addr: SocketAddr, path: &str, status: u16, body: &str) {
    let started = std::time::Instant::now();
    loop {
        let r = send(addr, "GET", path, "").await;
        if r.status == status && r.body.trim() == body {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{path} stayed {} {:?}, wanted {status} {body:?}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A heartbeat of a node of the version before ADR 0023 serving
/// `default/shop`: live, reading secrets, taking actors, without `front`.
async fn older_node_heartbeat(store: &Arc<dyn ObjectStore>, id: &str, app: &str) {
    use object_store::ObjectStoreExt as _;
    let body = format!(
        "wasmkeep-node 2\n{id}\nhttp://10.0.0.9:8080\n0.3.9\n{}\n{app}\n\n\nserving\nsecrets\ntake\n",
        now_ms()
    );
    store
        .put(
            &format!("nodes/{id}").as_str().into(),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn deploys_carry_the_front_and_rollbacks_restore_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let front_of = |v: &str| {
        let (app, v) = (app.clone(), v.to_owned());
        async move { app.version(&v).await.unwrap().front }
    };

    // An actor given as a front is refused, before anything is written.
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Set(test_actor_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::Front(_)), "{e}");
    assert!(bucket_keys(&store).await.is_empty());
    // Front values without a front.
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("c".into(), counter_wasm().to_vec())]),
                front_requires: Some(names(&["KEY"])),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::Front(_)), "{e}");

    // A first deploy may give a front and no class.
    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Set(front_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(v1.diff.lines(), vec!["front: added".to_owned()]);
    let manifest = app.manifest(&v1.current.version).await.unwrap();
    assert_eq!(manifest.format_number(), 3);
    assert!(manifest.classes.is_empty());
    assert_eq!(
        front_of(&v1.current.version).await.unwrap().wasm,
        front_wasm()
    );

    // Classes, the front's values and required names; the front kept,
    // copied inside the bucket.
    let v2 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("c".into(), test_actor_wasm().to_vec())]),
                access: vec![("c".into(), "existing".into())],
                front_config: Some(vars("GREETING=hi")),
                front_requires: Some(names(&["SESSION_KEY"])),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let lines = v2.diff.lines();
    for want in [
        "front: unchanged",
        "class c: added",
        "config _front: GREETING added",
        "requires _front: SESSION_KEY now required",
    ] {
        assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
    }
    // `existing` is not open: nothing reachable past the front.
    assert!(
        !lines.iter().any(|l| l.contains("past the front")),
        "{lines:?}"
    );
    let f2 = front_of(&v2.current.version).await.unwrap();
    assert_eq!(f2.wasm, front_wasm());
    assert_eq!(f2.config.get("GREETING").map(String::as_str), Some("hi"));
    assert_eq!(f2.requires, names(&["SESSION_KEY"]));
    // The front's values are not a class's.
    let v = app.version(&v2.current.version).await.unwrap();
    assert!(v.config.is_empty() && v.requires.is_empty(), "{v:?}");

    // New classes only: the front and its values kept; an open class is
    // listed as reachable past it.
    let v3 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![
                    ("c".into(), test_actor_wasm().to_vec()),
                    ("d".into(), counter_wasm().to_vec()),
                ]),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(
        v3.diff
            .lines()
            .contains(&"class d: open, reachable at /actor/d/<name>, past the front".to_owned()),
        "{:?}",
        v3.diff.lines()
    );
    let f3 = front_of(&v3.current.version).await.unwrap();
    assert_eq!(
        (f3.wasm.as_slice(), f3.config.len(), &f3.requires),
        (front_wasm(), 1, &names(&["SESSION_KEY"]))
    );

    // Another front: its values stay.
    let v4 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Set(other_front_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(v4.diff.front, Some("changed"));
    let f4 = front_of(&v4.current.version).await.unwrap();
    assert_eq!(f4.wasm, other_front_wasm());
    assert_eq!(f4.requires, names(&["SESSION_KEY"]));

    // A rollback restores the front before, with its values.
    let back = app.rollback(false).await.unwrap();
    assert_eq!(back.current.version, v3.current.version);
    assert_eq!(back.diff.front, Some("changed"));
    let f = front_of(&back.current.version).await.unwrap();
    assert_eq!(f.wasm, front_wasm());

    // `--no-front`: the front goes with its values and required names,
    // and the version is written as before ADR 0023.
    let v5 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Remove,
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let lines = v5.diff.lines();
    for want in [
        "front: removed",
        "config _front: GREETING removed",
        "requires _front: SESSION_KEY no longer required",
    ] {
        assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
    }
    let m5 = app.manifest(&v5.current.version).await.unwrap();
    assert!(m5.front.is_none() && m5.config.is_empty() && m5.requires.is_empty());
    assert_eq!(m5.format_number(), 2, "class c keeps its rule");
    assert!(front_of(&v5.current.version).await.is_none());
    // A deploy that only adds front values now has no front to give them.
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                front_config: Some(vars("GREETING=again")),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::Front(_)), "{e}");

    // Collection removes a version's front with it. v4 left the lineage
    // with the rollback: collection never touches it (ADR 0012).
    let fronts = |keys: Vec<String>| {
        keys.into_iter()
            .filter(|k| k.contains("/_front.") && !k.contains(&v4.current.version))
            .collect::<Vec<_>>()
    };
    assert!(!fronts(bucket_keys(&store).await).is_empty());
    app.collect_versions(0, Duration::ZERO).await.unwrap();
    assert_eq!(fronts(bucket_keys(&store).await), Vec::<String>::new());
}

/// Two nodes follow the app: both serve the deployed front within the
/// switch, a rollback brings the previous front back on both, a version
/// whose front does not link leaves a node on the one before (not ready),
/// and `--no-front` gives 404 for the front's paths, actors still served.
#[tokio::test(flavor = "multi_thread")]
async fn every_node_switches_fronts_with_the_version() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("t".into(), test_actor_wasm().to_vec())]),
                front: FrontChange::Set(front_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_app_node("a", da.path(), &store, None).await;
    let b = front_app_node("b", db.path(), &store, None).await;
    settle().await;
    for node in [a.addr, b.addr] {
        serves(node, "/self", 200, "default/_front/main").await;
    }
    let r = send_reachable(
        a.addr,
        "GET",
        "/call?to=t/x&method=POST&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:1"), "{}", r.body);

    // Another front: both switch to it.
    let v2 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Set(other_front_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    for node in [&a, &b] {
        runs_deploy(&node.service, v2.current.seq).await;
        serves(node.addr, "/self", 200, "ok").await;
    }

    // Rolled back: the first front again, on both.
    let back = app.rollback(false).await.unwrap().current;
    assert_eq!(back.version, v1.current.version);
    for node in [&a, &b] {
        runs_deploy(&node.service, back.seq).await;
        serves(node.addr, "/self", 200, "default/_front/main").await;
    }

    // A version whose front does not link (an actor, written past the
    // deploy's checks): handled like a class that does not link. The
    // nodes keep the version before, whole, and say they are behind.
    let manifest = app.manifest(&back.version).await.unwrap();
    let actor_digest = sha256_hex(test_actor_wasm());
    let bad = "badf00dbadf00d00";
    let put = |key: String, body: Vec<u8>| {
        let store = Arc::clone(&store);
        async move {
            store.put(&key.as_str().into(), body.into()).await.unwrap();
        }
    };
    put(
        format!("apps/default/shop/{bad}/t.wasm"),
        test_actor_wasm().to_vec(),
    )
    .await;
    put(
        format!("apps/default/shop/{bad}/_front.wasm"),
        test_actor_wasm().to_vec(),
    )
    .await;
    put(
        format!("apps/default/shop/{bad}/manifest"),
        format!(
            "wasmkeep-manifest 3\nprevious {}\nclass t {}\nfront main {actor_digest}\n",
            back.version, manifest.classes["t"]
        )
        .into_bytes(),
    )
    .await;
    put(
        "apps/default/shop/current".into(),
        format!("{bad} {} 3\n", back.seq + 1).into_bytes(),
    )
    .await;
    for node in [&a, &b] {
        let started = std::time::Instant::now();
        while node.service.not_ready() != Some("cannot apply the deployed version") {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{:?}",
                node.service.not_ready()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            node.service.deploy_seq(),
            back.seq,
            "the version before, whole"
        );
        serves(node.addr, "/self", 200, "default/_front/main").await;
    }

    // `--no-front`: 404 on both, the actors still served.
    let v5 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Remove,
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    for node in [&a, &b] {
        runs_deploy(&node.service, v5.current.seq).await;
        serves(node.addr, "/self", 404, "not found").await;
        assert_eq!(node.service.not_ready(), None);
    }
    let r = send_reachable(b.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}

/// The front's secrets (scope `_front`) reach its `env.get` over its
/// deployed configuration; while a required name is not set, or the app's
/// secrets cannot be opened, it answers a generic 503 that names nothing.
#[tokio::test(flavor = "multi_thread")]
async fn front_secrets_reach_the_front_and_required_names_hold_it_back() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    app.deploy_changes(
        &rt,
        Changes {
            classes: Some(vec![("t".into(), test_actor_wasm().to_vec())]),
            front: FrontChange::Set(front_wasm().to_vec()),
            front_config: Some(vars("GREETING=hi\nSESSION_HINT=plain")),
            front_requires: Some(names(&["SESSION_KEY"])),
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    let key = SecretsKey::generate().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let a = front_app_node("a", dir.path(), &store, Some(&key)).await;
    settle().await;

    // Required and not set: a generic 503, the name in the log only.
    for _ in 0..3 {
        let r = send(a.addr, "GET", "/env?name=GREETING", "").await;
        assert_eq!(
            (r.status, r.retry_after.as_deref(), r.body.trim()),
            (503, Some("1"), "this node cannot serve the app now; retry")
        );
        assert!(!r.body.contains("SESSION"), "{}", r.body);
    }
    // The actors are not held back by the front's names.
    let r = send_reachable(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);

    // Set as the front's secret: the front runs, and reads it.
    let secrets = SecretStore::new(Arc::clone(&store), "", "default", "shop");
    secrets
        .change(std::slice::from_ref(&key), &key, |set| {
            set.set("_front", "SESSION_KEY", "s3cret")?;
            set.set("_front", "SESSION_HINT", "sealed")?;
            set.set("t", "SESSION_KEY", "the actor's")
        })
        .await
        .unwrap()
        .unwrap();
    serves(a.addr, "/env?name=SESSION_KEY", 200, "s3cret").await;
    serves(a.addr, "/env?name=GREETING", 200, "hi").await;
    // A secret wins over the deployed configuration.
    serves(a.addr, "/env?name=SESSION_HINT", 200, "sealed").await;

    // While the app's secrets cannot be opened: the same 503.
    a.service.set_secrets_blocked(true);
    let r = send(a.addr, "GET", "/env?name=GREETING", "").await;
    assert_eq!((r.status, r.retry_after.as_deref()), (503, Some("1")));
    assert_eq!(r.body.trim(), "this node cannot serve the app now; retry");
    a.service.set_secrets_blocked(false);
    serves(a.addr, "/env?name=GREETING", 200, "hi").await;
    // Counted as `unconfigured`, and nothing names the missing value.
    let page = wasmkeep_server::metrics::render_for(
        wasmkeep_server::metrics::Gauges::default(),
        a.service.metrics_identity(),
    );
    assert!(
        page.contains("wasmkeep_front_requests_total{tenant=\"default\",app=\"shop\",outcome=\"unconfigured\"} "),
        "{page}"
    );
    assert!(!page.contains("SESSION_KEY"), "{page}");

    // Removed again: held back again.
    secrets
        .change(std::slice::from_ref(&key), &key, |set| {
            set.unset("_front", "SESSION_KEY");
            Ok::<(), std::convert::Infallible>(())
        })
        .await
        .unwrap()
        .unwrap();
    serves(
        a.addr,
        "/env?name=GREETING",
        503,
        "this node cannot serve the app now; retry",
    )
    .await;
}

/// ADR 0023: while a live node of the app does not announce `front`, no
/// deploy writes format 3 and no rollback returns to it; deploys without a
/// front go on, and other apps' nodes or stopped ones do not count.
#[tokio::test(flavor = "multi_thread")]
async fn nodes_without_the_front_word_hold_front_deploys_back() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let with_front = || Changes {
        classes: Some(vec![("c".into(), counter_wasm().to_vec())]),
        front: FrontChange::Set(front_wasm().to_vec()),
        ..Changes::default()
    };
    let v1 = app.deploy_changes(&rt, with_front()).await.unwrap();
    let v2 = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Remove,
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        app.nodes_without_front().await.unwrap(),
        Vec::<String>::new()
    );

    // Another app's older node, and a stopped one of this app: no matter.
    older_node_heartbeat(&store, "elsewhere", "default/blog").await;
    store
        .put(
            &"nodes/gone".into(),
            "wasmkeep-node 2\ngone\nhttp://10.0.0.8:8080\n0.3.9\n1\ndefault/shop\n\n\nserving\nsecrets\ntake\n"
                .as_bytes()
                .to_vec()
                .into(),
        )
        .await
        .unwrap();
    assert_eq!(
        app.nodes_without_front().await.unwrap(),
        Vec::<String>::new()
    );

    older_node_heartbeat(&store, "old", "default/shop").await;
    assert_eq!(
        app.nodes_without_front().await.unwrap(),
        vec!["old".to_owned()]
    );
    let before = bucket_keys(&store).await;
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                front: FrontChange::Set(other_front_wasm().to_vec()),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFront { nodes, .. } if nodes == "old"),
        "{e}"
    );
    assert!(e.to_string().contains("no --force"), "{e}");
    assert_eq!(bucket_keys(&store).await, before, "nothing written");
    let e = app.rollback(false).await.unwrap_err();
    assert!(matches!(e, DeployError::NodesWithoutFront { .. }), "{e}");
    assert_eq!(
        app.current().await.unwrap().unwrap().version,
        v2.current.version
    );
    // Without a front, deploys go on.
    let v3 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("c".into(), test_actor_wasm().to_vec())]),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        app.manifest(&v3.current.version)
            .await
            .unwrap()
            .format_number(),
        1
    );
    // Upgraded (it now says `front`): the deploy goes through.
    store
        .put(
            &"nodes/old".into(),
            format!(
                "wasmkeep-node 2\nold\nhttp://10.0.0.9:8080\n0.4.0\n{}\ndefault/shop\n\n\nserving\nsecrets\ntake front\n",
                now_ms()
            )
            .into_bytes()
            .into(),
        )
        .await
        .unwrap();
    let v4 = app.deploy_changes(&rt, with_front()).await.unwrap();
    assert_ne!(v4.current.version, v1.current.version, "another lineage");
}
