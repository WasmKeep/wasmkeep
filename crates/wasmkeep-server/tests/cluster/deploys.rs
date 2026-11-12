//! Deploys, versions, access rules and apps as every node sees them.

use super::*;
use object_store::ObjectStoreExt as _;

#[tokio::test(flavor = "multi_thread")]
async fn existing_access_checks_the_bucket_and_leaves_nothing_for_unknown_names() {
    // Class t is `existing`, u is `internal`; v (open) creates and calls
    // them. Both nodes serve all three, with the same rules, under a bucket
    // prefix and with names the bucket keys must escape.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let restrict = |config: &mut ActorConfig| {
        let mut rules = wasmkeep_server::AccessRules::default();
        rules.set("t", wasmkeep_server::Access::Existing);
        rules.set("u", wasmkeep_server::Access::Internal);
        config.access = rules;
    };
    let (idle, wall) = (Duration::from_millis(300), Duration::from_secs(30));
    let (wasm, classes, prefix) = (test_actor_wasm(), &["t", "u", "v"][..], "team~1/prod");
    let a = node_configured(
        "a",
        dirs.0.path(),
        &store,
        idle,
        classes,
        wasm,
        wall,
        prefix,
        restrict,
    )
    .await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &store,
        idle,
        classes,
        wasm,
        wall,
        prefix,
        restrict,
    )
    .await;
    settle().await;

    // Unknown: 404 from either node, over HTTP or WebSocket, and nothing in
    // the bucket for it, not even a lease (leases are never deleted).
    for node in [a.addr, b.addr] {
        let r = send(node, "GET", "/actor/t/Made%20It/durable", "").await;
        assert_eq!(r.status, 404, "{}", r.body);
        assert_eq!(upgrade_status(node, "/actor/t/Made%20It").await, 404);
    }
    let keys = bucket_keys(&store).await;
    assert!(
        !keys.iter().any(|k| k.contains("/t/") || k.contains("/u/")),
        "the bucket holds something for a name nobody created: {keys:?}"
    );

    // An actor on a creates t/"Made It".
    let r = send(
        a.addr,
        "POST",
        "/actor/v/maker/call?to=t/Made%20It&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:1"));
    // b has never run it, but the bucket knows it: reachable through b,
    // over HTTP and WebSocket.
    let r = send(b.addr, "GET", "/actor/t/Made%20It/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    assert_eq!(upgrade_status(b.addr, "/actor/t/Made%20It").await, 101);
    // After it hibernates on a, b may take it over: still allowed, same data.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let r = send(b.addr, "POST", "/actor/t/Made%20It/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));

    // Internal: an actor on a creates u/x; an actor on b reaches it on a
    // (a call between nodes, proven by the cluster secret).
    let r = send(
        a.addr,
        "POST",
        "/actor/v/m1/call?to=u/x&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:1"));
    let r = send(
        b.addr,
        "POST",
        "/actor/v/m2/call?to=u/x&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:2"));
    // From outside, through either node, even posing as a node or an actor.
    for node in [a.addr, b.addr] {
        let forged = "x-wasmkeep-internal: guess\r\nx-wasmkeep-caller: default/v/m1\r\nx-wasmkeep-forwarded: 1\r\n";
        for extra in ["", forged] {
            let r = send(node, "GET", "/actor/u/x/durable", extra).await;
            assert_eq!(r.status, 403, "{extra:?}: {}", r.body);
        }
        assert_eq!(upgrade_status(node, "/actor/u/x").await, 403);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_apps_of_one_tenant_on_one_prefix_are_detected() {
    // acme/shop and acme/admin on the same bucket prefix would share actor
    // keys; `serve` refuses to start the second when this reports it.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let service = |id: &str, app: &str, dir: &Path| {
        let runtime = Runtime::new(&RuntimeConfig {
            max_instances: 4,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let component = runtime.compile(counter_wasm()).unwrap();
        let mut config = ActorConfig::new(dir);
        config.tenant = "acme".into();
        let mut cluster = ClusterConfig::new(id, format!("http://{id}"), Arc::clone(&store));
        cluster.heartbeat = Duration::from_millis(100);
        cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        cluster.failover_wait = Some(Duration::ZERO);
        cluster.app = app.into();
        config.cluster = Some(cluster);
        ActorService::new(
            runtime,
            config,
            vec![("c".into(), component, counter_wasm().to_vec())],
        )
        .unwrap()
    };
    let shop = service("shop-1", "acme/shop", dirs.0.path());
    // Its start check, then its first heartbeat.
    shop.node_id_checked().await.unwrap();
    // Another node of the same app: fine.
    let shop2 = service("shop-2", "acme/shop", dirs.1.path());
    assert_eq!(shop2.app_conflicts().await.unwrap(), Vec::<String>::new());
    drop(shop2);
    let admin = service("admin-1", "acme/admin", dirs.1.path());
    // shop-2's heartbeat is still fresh: it may be listed too.
    let mixed = admin.app_conflicts().await.unwrap();
    assert!(
        mixed.contains(&"shop-1 (acme/shop)".to_owned()),
        "{mixed:?}"
    );
    assert!(mixed.iter().all(|m| m.contains("acme/shop")), "{mixed:?}");
    drop((shop, admin));
}

#[tokio::test(flavor = "multi_thread")]
async fn deploys_roll_out_and_roll_back() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    assert!(
        app.rollback(false).await.is_err(),
        "nothing to roll back yet"
    );

    let v1 = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    assert_eq!(v1.seq, 1);
    let again = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    assert_eq!(again, v1, "redeploying the same code changes nothing");

    let dir = tempfile::tempdir().unwrap();
    let (a, service) = node_app(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/c/x", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "v1 is the counter");

    let v2 = app
        .deploy(
            &rt,
            vec![
                ("c".into(), test_actor_wasm().to_vec()),
                ("d".into(), counter_wasm().to_vec()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(v2.seq, 2);
    runs_deploy(&service, 2).await;
    let r = send(a.addr, "POST", "/actor/d/y", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "v2 added class d");
    let r = send(a.addr, "POST", "/actor/c/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "v2 is the test actor, same actor"
    );

    let back = app.rollback(false).await.unwrap().current;
    assert_eq!((back.version.as_str(), back.seq), (v1.version.as_str(), 3));
    runs_deploy(&service, 3).await;
    let r = send(a.addr, "POST", "/actor/d/y", "").await;
    assert_eq!(r.status, 404, "a class the current version lacks is gone");
    let r = send(a.addr, "POST", "/actor/c/x", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "the counter again, data kept"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn old_deploy_versions_are_collected_while_a_node_follows() {
    // ADR 0012 (deploy artifacts): eight versions in a line; the current
    // one and the five before it are kept, the two oldest go (no
    // retention window here). The node following the app keeps serving,
    // the next deploy lands, and a rollback still finds the version before.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let code = |i: usize| {
        if i.is_multiple_of(2) {
            counter_wasm().to_vec()
        } else {
            test_actor_wasm().to_vec()
        }
    };
    let first = app.deploy(&rt, vec![("c".into(), code(0))]).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (a, service) = node_app(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await;
    settle().await;
    let mut lineage = vec![first.version.clone()];
    for i in 1..8 {
        let v = app.deploy(&rt, vec![("c".into(), code(i))]).await.unwrap();
        assert_eq!(v.seq, u64::try_from(i + 1).unwrap());
        lineage.push(v.version);
    }
    runs_deploy(&service, 8).await;

    let files_of = |version: String| {
        let store = Arc::clone(&store);
        async move {
            use futures_util::TryStreamExt as _;
            store
                .list(Some(&object_store::path::Path::from(format!(
                    "apps/default/shop/{version}"
                ))))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len()
        }
    };
    assert_eq!(
        app.collect_versions(wasmkeep_server::apps::KEEP_PREVIOUS, Duration::ZERO)
            .await
            .unwrap(),
        2
    );
    for (i, version) in lineage.iter().enumerate() {
        let n = files_of(version.clone()).await;
        if i < 2 {
            assert_eq!(n, 0, "version {i} was collected");
        } else {
            assert!(n >= 2, "version {i} (manifest and component) kept: {n}");
        }
    }

    // The node serves the current version (v8 is the test actor), takes
    // the next deploy, and a rollback goes back to the one before.
    let r = send(a.addr, "POST", "/actor/c/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    let ninth = app.deploy(&rt, vec![("c".into(), code(8))]).await.unwrap();
    runs_deploy(&service, 9).await;
    let back = app.rollback(false).await.unwrap().current;
    assert_eq!(back.version, lineage[7]);
    assert_ne!(back.version, ninth.version);
    runs_deploy(&service, 10).await;
    let r = send(a.addr, "POST", "/actor/c/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "the test actor again, data kept"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_component_is_refused_before_anything_is_uploaded() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let v1 = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    let bad = app
        .deploy(&rt, vec![("c".into(), b"not wasm".to_vec())])
        .await;
    assert!(
        matches!(bad, Err(wasmkeep_server::apps::DeployError::Invalid(..))),
        "{bad:?}"
    );
    assert_eq!(app.current().await.unwrap(), Some(v1));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_runs_on_the_class_it_was_admitted_with() {
    // ADR 0013: B is admitted (outside, class c open), then waits for the
    // actor's lease; a deploy changing c's code and making it `internal`
    // lands meanwhile. B must run the code it was admitted under (the
    // counter answers any GET with its value), never the new code from
    // outside (the test actor answers /inits with its count).
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(shared, Arc::new(AtomicBool::new(false))));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dir = tempfile::tempdir().unwrap();
    let wall = Duration::from_secs(30);
    let a = node_configured(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        &["c"],
        counter_wasm(),
        wall,
        "",
        |_| {},
    )
    .await;
    settle().await;
    // The second lease read from now: the front door's owner lookup is the
    // first, the event's lease acquisition (after admission) the second.
    let seen = flaky.lease_gets.load(Ordering::SeqCst);
    flaky.hold_lease_get_at.store(seen + 2, Ordering::SeqCst);
    let b = tokio::spawn(async move { send(a.addr, "GET", "/actor/c/fresh/inits", "").await });
    let started = std::time::Instant::now();
    while !flaky.lease_held.load(Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "B never asked for the lease"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("c", wasmkeep_server::Access::Internal);
    let runtime = a.service.runtime().clone();
    let v2 = test_actor_wasm().to_vec();
    let component = runtime.compile(&v2).unwrap();
    a.service
        .apply_version(
            2,
            &[("c".into(), component, v2)],
            &rules,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
    let r = b.await.unwrap();
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "0"),
        "B ran other code than it was admitted with"
    );
    let r = send(a.addr, "GET", "/actor/c/fresh/inits", "").await;
    assert_eq!(r.status, 403, "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_cannot_apply_the_deployed_version_is_not_ready() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let v1 = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (_a, service) = node_app(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    settle().await;
    let wait_for = |want: Option<&'static str>| {
        let service = service.clone();
        async move {
            let started = std::time::Instant::now();
            while service.not_ready() != want {
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "readiness stayed {:?}",
                    service.not_ready()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    wait_for(None).await;
    // A version whose component is missing: the node reads it and cannot
    // apply it.
    let bad = "badbadbadbadbad0";
    let put = |key: String, body: String| {
        let store = Arc::clone(&store);
        async move {
            store
                .put(&key.as_str().into(), body.into_bytes().into())
                .await
                .unwrap();
        }
    };
    put(
        format!("apps/default/shop/{bad}/manifest"),
        format!(
            "wasmkeep-manifest 1\nprevious {}\nclass c {}\n",
            v1.version,
            "0".repeat(64)
        ),
    )
    .await;
    put("apps/default/shop/current".into(), format!("{bad} 2\n")).await;
    wait_for(Some("cannot apply the deployed version")).await;
    // A version it can apply: ready again.
    let v3 = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    runs_deploy(&service, v3.seq).await;
    wait_for(None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_version_carries_its_access_rules_and_configuration() {
    use object_store::ObjectStoreExt as _;
    use std::collections::BTreeMap;
    use wasmkeep_server::apps::DeployError;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let vars = |text: &str| wasmkeep_server::env::parse(text).unwrap();
    let classes = || {
        vec![
            ("c".to_owned(), counter_wasm().to_vec()),
            ("d".to_owned(), counter_wasm().to_vec()),
        ]
    };
    let read = |key: String| {
        let store = Arc::clone(&store);
        async move {
            let bytes = store.get(&key.as_str().into()).await.unwrap();
            bytes.bytes().await.unwrap().to_vec()
        }
    };
    let plain = app.deploy(&rt, classes()).await.unwrap();
    assert!(!plain.marked, "nothing new: written as before ADR 0013");
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.apply("c", "existing:main,room 1").unwrap();
    let config = BTreeMap::from([(
        "d".to_owned(),
        vars("# TOKEN=commented-out\nURL=https://example.test\n"),
    )]);
    let v2 = app
        .deploy_version(&rt, classes(), &rules, config.clone())
        .await
        .unwrap();
    assert_eq!(v2.seq, 2);
    assert!(v2.marked);
    assert_ne!(v2.version, plain.version, "rules and config make a version");
    let again = app
        .deploy_version(&rt, classes(), &rules, config.clone())
        .await
        .unwrap();
    assert_eq!(again, v2, "the same version again changes nothing");

    let version = app.version(&v2.version).await.unwrap();
    assert_eq!(
        version.access.spec("c").as_deref(),
        Some("existing:main,room 1")
    );
    assert_eq!(version.access.spec("d"), None);
    assert_eq!(
        version
            .config
            .get("d")
            .and_then(|v| v.get("URL"))
            .map(String::as_str),
        Some("https://example.test")
    );
    assert!(!version.config.contains_key("c"));
    let manifest =
        String::from_utf8(read(format!("apps/default/shop/{}/manifest", v2.version)).await)
            .unwrap();
    assert!(manifest.starts_with("wasmkeep-manifest 2\n"), "{manifest}");
    assert!(
        !manifest.contains("example.test"),
        "a value in the manifest"
    );
    let stored = read(format!("apps/default/shop/{}/d.config", v2.version)).await;
    assert_eq!(
        &stored[..],
        b"URL='https://example.test'\n",
        "stored as parsed"
    );

    // Rules and configuration must name classes of the version; nothing
    // changes otherwise.
    let mut typo = wasmkeep_server::AccessRules::default();
    typo.apply("monitr", "existing").unwrap();
    let bad = app
        .deploy_version(&rt, classes(), &typo, BTreeMap::new())
        .await;
    assert!(matches!(bad, Err(DeployError::Invalid(..))), "{bad:?}");
    let elsewhere = BTreeMap::from([("x".to_owned(), vars("A=1"))]);
    let bad = app
        .deploy_version(
            &rt,
            classes(),
            &wasmkeep_server::AccessRules::default(),
            elsewhere,
        )
        .await;
    assert!(matches!(bad, Err(DeployError::Invalid(..))), "{bad:?}");
    assert_eq!(app.current().await.unwrap(), Some(v2.clone()));

    // A configuration object that does not match its digest is refused
    // whole, without showing what it holds.
    store
        .put(
            &format!("apps/default/shop/{}/d.config", v2.version).into(),
            b"URL=https://attacker.test secret\n".to_vec().into(),
        )
        .await
        .unwrap();
    let e = app.version(&v2.version).await.unwrap_err().to_string();
    assert!(e.contains("d.config (digest)"), "{e}");
    assert!(!e.contains("attacker"), "{e}");

    // The marker stays once set: a later version without rules, and a
    // rollback to it, keep it, so tools from before ADR 0013 cannot write
    // over them.
    let v3 = app
        .deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    assert!(v3.marked);
    let raw = String::from_utf8(read("apps/default/shop/current".into()).await).unwrap();
    assert_eq!(raw, format!("{} {} 2\n", v3.version, v3.seq));
}

/// ADR 0016: the names a class requires travel with its version, are kept
/// by a deploy that does not name them, removed with `CLASS=`, and listed
/// in what a deploy changed.
#[tokio::test(flavor = "multi_thread")]
async fn required_names_travel_with_deploys() {
    use std::collections::BTreeSet;
    use wasmkeep_server::apps::Changes;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let names = |n: &[&str]| {
        n.iter()
            .map(|s| (*s).to_owned())
            .collect::<BTreeSet<String>>()
    };
    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("c".into(), counter_wasm().to_vec())]),
                requires: vec![("c".into(), names(&["TOKEN"]))],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(
        v1.diff
            .lines()
            .contains(&"requires c: TOKEN now required".to_owned()),
        "{:?}",
        v1.diff.lines()
    );
    // Required names alone do not mark the app (ADR 0013): nodes still on
    // `--access` keep their rules.
    assert!(!v1.newly_marked && !v1.current.marked);
    let read = |v: &str| {
        let (app, v) = (app.clone(), v.to_owned());
        async move { app.version(&v).await.unwrap().requires }
    };
    assert_eq!(
        read(&v1.current.version).await.get("c"),
        Some(&names(&["TOKEN"]))
    );
    // New code only: kept.
    let v2 = app
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
        read(&v2.current.version).await.get("c"),
        Some(&names(&["TOKEN"]))
    );
    // `c=` removes them.
    let v3 = app
        .deploy_changes(
            &rt,
            Changes {
                requires: vec![("c".into(), BTreeSet::new())],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(read(&v3.current.version).await.is_empty());
    assert!(
        v3.diff
            .lines()
            .contains(&"requires c: TOKEN no longer required".to_owned())
    );
    // A class the version does not have.
    let refused = app
        .deploy_changes(
            &rt,
            Changes {
                requires: vec![("nope".into(), names(&["X"]))],
                ..Changes::default()
            },
        )
        .await;
    assert!(refused.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_changes_what_it_names_and_keeps_the_rest() {
    use object_store::ObjectStoreExt as _;
    use wasmkeep_server::apps::{Changes, DeployError};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let vars = |t: &str| Some(wasmkeep_server::env::parse(t).unwrap());
    let nothing = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "existing".into())],
                ..Changes::default()
            },
        )
        .await;
    assert!(
        matches!(nothing, Err(DeployError::Nothing(_))),
        "{nothing:?}"
    );

    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![
                    ("c".into(), counter_wasm().to_vec()),
                    ("d".into(), counter_wasm().to_vec()),
                ]),
                access: vec![("c".into(), "existing".into())],
                config: vec![("d".into(), vars("URL=one"))],
                requires: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!(v1.newly_marked);
    // New code only, as a CI job would deploy it: rules and values kept.
    let v2 = app
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
    assert!(!v2.newly_marked);
    assert_eq!(v2.diff.lines(), vec!["class c: new code".to_owned()]);
    let read = app.version(&v2.current.version).await.unwrap();
    assert_eq!(read.access.spec("c").as_deref(), Some("existing"));
    assert_eq!(read.config["d"]["URL"], "one");

    // Rules and values only: the components are copied inside the bucket.
    let v3 = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "open".into()), ("d".into(), "internal".into())],
                config: vec![("d".into(), vars("URL=two\nMODE=x"))],
                requires: Vec::new(),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        v3.diff.lines(),
        vec![
            "access c: existing -> open".to_owned(),
            "access d: open -> internal".to_owned(),
            "config d: MODE added".to_owned(),
            "config d: URL changed".to_owned(),
        ]
    );
    let read = app.version(&v3.current.version).await.unwrap();
    assert_eq!(read.access.spec("c"), None);
    assert_eq!(read.classes[0].1, test_actor_wasm(), "copied as it was");
    let copied = store
        .get(&format!("apps/default/shop/{}/c.wasm", v3.current.version).into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&copied[..], test_actor_wasm());
    // Removing a class's configuration; a rule for a class not there.
    let v4 = app
        .deploy_changes(
            &rt,
            Changes {
                config: vec![("d".into(), None)],
                requires: Vec::new(),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(
        app.version(&v4.current.version)
            .await
            .unwrap()
            .config
            .is_empty()
    );
    let typo = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("monitr".into(), "existing".into())],
                ..Changes::default()
            },
        )
        .await;
    assert!(matches!(typo, Err(DeployError::Invalid(..))), "{typo:?}");
    // Removing a class takes its rule with it.
    let v5 = app
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
        v5.diff.removed,
        vec![("d".to_owned(), Some("internal".to_owned()))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_that_reopens_a_class_needs_allow_reopen() {
    use wasmkeep_server::apps::{Changes, DeployError};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    // Before rules existed (format 1), then the migration's rules deploy.
    app.deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
        .await
        .unwrap();
    let ruled = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "existing:main".into())],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let refused = app.rollback(false).await;
    let Err(DeployError::Reopens(what)) = &refused else {
        panic!("{refused:?}");
    };
    assert_eq!(what, "c (existing:main -> open)");
    assert_eq!(app.current().await.unwrap(), Some(ruled.current.clone()));
    let back = app.rollback(true).await.unwrap();
    assert_eq!(
        back.diff.lines(),
        vec!["access c: existing:main -> open".to_owned()]
    );
    assert!(back.current.marked, "the marker stays");

    // Hardening, then rolling it back: refused too; tightening is not.
    let hard = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "internal".into())],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        app.rollback(false).await,
        Err(DeployError::Reopens(_))
    ));
    app.deploy_changes(
        &rt,
        Changes {
            access: vec![("c".into(), "existing:a,b".into())],
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    // existing:a,b -> internal is stricter.
    let back = app.rollback(false).await.unwrap();
    assert_eq!(back.current.version, hard.current.version);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_deploys_never_lose_a_rule() {
    use wasmkeep_server::apps::{Changes, DeployError};
    for round in 0..10 {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
        let rt = deploy_runtime();
        app.deploy(&rt, vec![("c".into(), counter_wasm().to_vec())])
            .await
            .unwrap();
        let rules = app.deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "existing".into())],
                ..Changes::default()
            },
        );
        let code = app.deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![("c".into(), test_actor_wasm().to_vec())]),
                ..Changes::default()
            },
        );
        let (rules, code) = tokio::join!(rules, code);
        let ok = |r: &Result<_, DeployError>| match r {
            Ok(_) => true,
            Err(DeployError::Conflict) => false,
            Err(e) => panic!("round {round}: {e}"),
        };
        let current = app.current().await.unwrap().unwrap();
        let version = app.version(&current.version).await.unwrap();
        if ok(&rules) {
            assert_eq!(
                version.access.spec("c").as_deref(),
                Some("existing"),
                "round {round}: the rule was lost (code deployed: {})",
                ok(&code)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_version_deployed_again_later_records_what_it_replaced() {
    // ADR 0013: X, then Y, then X again. The third is a new directory
    // whose `previous` is Y, so a rollback goes to Y, then to the first X.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let x = || vec![("c".to_owned(), counter_wasm().to_vec())];
    let y = || vec![("c".to_owned(), test_actor_wasm().to_vec())];
    let first = app.deploy(&rt, x()).await.unwrap();
    let mid = app.deploy(&rt, y()).await.unwrap();
    let again = app.deploy(&rt, x()).await.unwrap();
    assert_ne!(again.version, first.version, "the directory was reused");
    assert_eq!(
        app.manifest(&again.version).await.unwrap().previous,
        Some(mid.version.clone())
    );
    assert_eq!(
        app.deploy(&rt, x()).await.unwrap(),
        again,
        "current: a no-op"
    );
    assert_eq!(
        app.rollback(false).await.unwrap().current.version,
        mid.version
    );
    assert_eq!(
        app.rollback(false).await.unwrap().current.version,
        first.version
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deployed_rules_and_configuration_reach_nodes_started_without_any() {
    // ADR 0013: neither node is given rules or values. Class m is
    // deployed `existing` with a URL; v (open) creates m's actors.
    use std::collections::BTreeMap;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let classes = || {
        vec![
            ("m".to_owned(), test_actor_wasm().to_vec()),
            ("v".to_owned(), test_actor_wasm().to_vec()),
        ]
    };
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("m", wasmkeep_server::Access::Existing);
    let config = BTreeMap::from([(
        "m".to_owned(),
        wasmkeep_server::env::parse("URL=https://one.test").unwrap(),
    )]);
    app.deploy_version(&rt, classes(), &rules, config)
        .await
        .unwrap();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_millis(300);
    let follow = Duration::from_millis(100);
    let (a, a_service) = node_app("a", da.path(), &store, idle, follow).await;
    let (b, b_service) = node_app("b", db.path(), &store, idle, follow).await;
    settle().await;

    for node in [a.addr, b.addr] {
        let r = send_reachable(node, "GET", "/actor/m/nobody/inits", "").await;
        assert_eq!(r.status, 404, "{}", r.body);
        assert_eq!(upgrade_status_reachable(node, "/actor/m/nobody").await, 404);
    }
    let keys = bucket_keys(&store).await;
    assert!(
        !keys.iter().any(|k| k.contains("/m/")),
        "the bucket holds something for a name nobody created: {keys:?}"
    );

    let r = send_reachable(
        a.addr,
        "POST",
        "/actor/v/maker/call?to=m/made&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "200:1"));
    for node in [a.addr, b.addr] {
        let r = send_reachable(node, "GET", "/actor/m/made/env?name=URL", "").await;
        assert_eq!(
            (r.status, r.body.trim()),
            (200, "https://one.test wasi=unset")
        );
    }

    // A rules-and-values deploy (same code) reaches both nodes.
    let mut rules = wasmkeep_server::AccessRules::default();
    rules.set("m", wasmkeep_server::Access::Internal);
    let config = BTreeMap::from([(
        "m".to_owned(),
        wasmkeep_server::env::parse("URL=https://two.test").unwrap(),
    )]);
    let v2 = app
        .deploy_version(&rt, classes(), &rules, config)
        .await
        .unwrap();
    runs_deploy(&a_service, v2.seq).await;
    runs_deploy(&b_service, v2.seq).await;
    for node in [a.addr, b.addr] {
        let r = send_reachable(node, "GET", "/actor/m/made/env?name=URL", "").await;
        assert_eq!(r.status, 403, "{}", r.body);
    }
    let r = send_reachable(
        a.addr,
        "POST",
        "/actor/v/maker/call?to=m/made&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "200:2"),
        "calls still reach it"
    );
    let r = send_reachable(
        b.addr,
        "POST",
        "/actor/v/other/call?to=m/made&path=%2Fdurable",
        "",
    )
    .await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "200:3"),
        "from either node"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_never_runs_older_code_than_it_already_ran() {
    never_runs_older_code(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_sequence_that_failed_to_record_is_recorded_on_the_next_event() {
    // Recording the deploy sequence fails once: the next event must record
    // it, or a node on the older deploy later runs old code on this data.
    never_runs_older_code(true).await;
}

/// Copies every object of app `from` to app `to` (both `tenant/app`) in
/// the same bucket: an app deployed before a check refused it.
async fn copy_app(store: &Arc<dyn ObjectStore>, from: &str, to: &str) {
    for key in bucket_keys(store).await {
        if let Some(rest) = key.strip_prefix(&format!("apps/{from}/")) {
            object_store::ObjectStoreExt::copy(
                store.as_ref(),
                &object_store::path::Path::from(key.as_str()),
                &object_store::path::Path::from(format!("apps/{to}/{rest}")),
            )
            .await
            .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn new_apps_need_tenant_and_app_names_of_the_class_grammar() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let code = || vec![("c".into(), counter_wasm().to_vec())];
    for (tenant, name) in [
        ("Acme", "shop"),
        ("acme", "Shop"),
        ("acme", "shop_1"),
        ("1acme", "shop"),
        ("acme", "-shop"),
        ("acme", &"s".repeat(65)),
    ] {
        let app = AppRef::new(Arc::clone(&store), "", tenant, name);
        let e = app.deploy(&rt, code()).await.unwrap_err().to_string();
        assert!(e.contains("[a-z][a-z0-9-]{0,63}"), "{tenant}/{name}: {e}");
        assert_eq!(app.current().await.unwrap(), None, "{tenant}/{name}");
    }
    assert!(bucket_keys(&store).await.is_empty(), "nothing was written");

    // An app that already has such a name keeps deploying and rolling back.
    AppRef::new(Arc::clone(&store), "", "default", "seed")
        .deploy(&rt, code())
        .await
        .unwrap();
    copy_app(&store, "default/seed", "Old_Tenant/My~App").await;
    let old = AppRef::new(Arc::clone(&store), "", "Old_Tenant", "My~App");
    let v2 = old
        .deploy_changes(&rt, changes(vec![("c".into(), test_actor_wasm().to_vec())]))
        .await
        .unwrap();
    assert_eq!(v2.current.seq, 2);
    // Its own folder, however the bucket spells it, is not another app.
    assert_eq!(v2.others, Vec::<String>::new());
    assert_eq!(old.rollback(false).await.unwrap().current.seq, 3);

    // The longest valid names are fine.
    let long = "a".repeat(64);
    AppRef::new(Arc::clone(&store), "", &long, &long)
        .deploy(&rt, code())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_app_of_a_tenant_on_a_prefix_is_refused_and_an_existing_pair_keeps_working() {
    // Actors are keyed tenant/class/name: two apps of one tenant on one
    // prefix would share them.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let code = || vec![("c".into(), counter_wasm().to_vec())];
    let app = |prefix: &str, tenant: &str, name: &str| {
        AppRef::new(Arc::clone(&store), prefix, tenant, name)
    };
    let first = app("", "acme", "shop")
        .deploy_changes(&rt, changes(code()))
        .await;
    assert_eq!(first.unwrap().others, Vec::<String>::new());

    // A second app of acme: refused, naming the first, what to do, and
    // the keys to delete if it is abandoned. Nothing written.
    let e = app("", "acme", "admin")
        .deploy(&rt, code())
        .await
        .unwrap_err()
        .to_string();
    for part in [
        "acme/shop",
        "another tenant",
        "a bucket prefix other than",
        "apps/acme/shop/",
        "actors/acme/",
    ] {
        assert!(e.contains(part), "{part:?} missing from: {e}");
    }
    assert_eq!(app("", "acme", "admin").current().await.unwrap(), None);
    assert!(
        !bucket_keys(&store)
            .await
            .iter()
            .any(|k| k.starts_with("apps/acme/admin/")),
        "nothing written for the refused app"
    );

    // Another tenant (even one whose name starts the same), and another
    // prefix, are fine. The app itself redeploys.
    app("", "acme-2", "admin")
        .deploy(&rt, code())
        .await
        .unwrap();
    app("other", "acme", "admin")
        .deploy(&rt, code())
        .await
        .unwrap();
    let again = app("", "acme", "shop")
        .deploy_changes(&rt, changes(vec![("c".into(), test_actor_wasm().to_vec())]))
        .await
        .unwrap();
    assert_eq!((again.current.seq, again.others), (2, Vec::new()));

    // Secrets set for an app never deployed do not make it an app.
    store
        .put(
            &object_store::path::Path::from("apps/acme/draft/secrets/current"),
            object_store::PutPayload::from_static(b"0000000000000001\n"),
        )
        .await
        .unwrap();
    assert_eq!(
        app("", "acme", "shop").other_apps().await.unwrap(),
        Vec::<String>::new()
    );

    // Two apps of acme deployed before this check: both keep deploying
    // and rolling back, each told about the other.
    copy_app(&store, "acme/shop", "acme/legacy").await;
    let shop = app("", "acme", "shop").rollback(false).await.unwrap();
    assert_eq!(shop.others, ["acme/legacy"]);
    let legacy = app("", "acme", "legacy")
        .deploy_changes(&rt, changes(code()))
        .await
        .unwrap();
    assert_eq!(legacy.others, ["acme/shop"]);
    // A third is refused, naming both.
    let e = app("", "acme", "blog")
        .deploy(&rt, code())
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("acme/legacy") && e.contains("acme/shop"), "{e}");

    // An abandoned app removed as the error says: the name is free.
    for key in bucket_keys(&store).await {
        if key.starts_with("apps/acme/legacy/") || key.starts_with("apps/acme/shop/") {
            store
                .delete(&object_store::path::Path::from(key.as_str()))
                .await
                .unwrap();
        }
    }
    app("", "acme", "blog").deploy(&rt, code()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_second_app_check_names_the_keys_under_the_bucket_prefix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let code = || vec![("c".into(), counter_wasm().to_vec())];
    AppRef::new(Arc::clone(&store), "team/prod", "acme", "shop")
        .deploy(&rt, code())
        .await
        .unwrap();
    let e = AppRef::new(Arc::clone(&store), "team/prod/", "acme", "admin")
        .deploy(&rt, code())
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("team/prod/apps/acme/shop/"), "{e}");
    assert!(e.contains("team/prod/actors/acme/"), "{e}");
}

fn changes(classes: Vec<(String, Vec<u8>)>) -> wasmkeep_server::apps::Changes {
    wasmkeep_server::apps::Changes {
        classes: Some(classes),
        ..Default::default()
    }
}
