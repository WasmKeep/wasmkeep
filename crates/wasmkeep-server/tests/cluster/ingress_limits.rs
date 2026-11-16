//! Host ingress policies on deployed apps, including rolling-reader refusal.
use super::*;
use object_store::ObjectStoreExt as _;
use wasmkeep_server::apps::limits::{Policies, Policy};
use wasmkeep_server::apps::{Changes, FrontChange};

fn rate_runtime() -> Runtime {
    Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn policy(path: &str, key: &str) -> Policies {
    [(
        "ingress".into(),
        Policy::parse(
            path,
            key,
            (key == "header").then_some("x-client-id"),
            "1/h",
            1,
        )
        .unwrap(),
    )]
    .into()
}
async fn deploy(store: &Arc<dyn ObjectStore>, limits: Policies) -> AppRef {
    let app = AppRef::new(store.clone(), "", "default", "shop");
    app.deploy_changes(
        &rate_runtime(),
        Changes {
            front: FrontChange::Set(super::front_deploys::front_wasm().to_vec()),
            classes: Some(vec![("t".into(), test_actor_wasm().to_vec())]),
            limits: Some(limits),
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    app
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_reject_before_front_body_or_direct_actor_work() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = deploy(&store, policy("/*", "ip")).await;
    let data = tempfile::tempdir().unwrap();
    let node = super::front_deploys::front_app_node("limited", data.path(), &store, None).await;
    assert_eq!(
        send(node.addr, "GET", "/unmatched-front-path", "")
            .await
            .status,
        200
    );
    for path in ["/actor/t/never/durable", "/anything", "/actor/t/socket"] {
        let r=send(node.addr,"GET",path,"x-forwarded-for: 192.0.2.1\r\nx-wasmkeep-internal: forged\r\nx-wasmkeep-forwarded: 1\r\n").await;
        assert_eq!(r.status, 429, "{}", r.body);
        assert_eq!(r.retry_after.as_deref(), Some("3600"));
    }
    let request = "POST /actor/t/never/durable HTTP/1.1\r\nHost: t\r\nContent-Length: 100\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n";
    let r = tokio::time::timeout(Duration::from_secs(2), exchange(node.addr, request))
        .await
        .unwrap();
    assert_eq!(r.status, 429);
    assert!(
        !data
            .path()
            .join("state/default/shop/actors/t/never.db")
            .exists()
    );
    // An unrelated partial deploy preserves quotas and policy bytes.
    let changed = app
        .deploy_changes(
            &rate_runtime(),
            Changes {
                config: vec![(
                    "t".into(),
                    Some(wasmkeep_server::env::parse("X=one").unwrap()),
                )],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    runs_deploy(&node.service, changed.current.seq).await;
    assert_eq!(send(node.addr, "GET", "/anything", "").await.status, 429);
    let rollback = app.rollback(false).await.unwrap();
    runs_deploy(&node.service, rollback.current.seq).await;
    assert_eq!(send(node.addr, "GET", "/anything", "").await.status, 429);
    node.service.set_deploy_behind(true);
    assert_eq!(
        send(node.addr, "GET", "/actor/t/never/durable", "")
            .await
            .status,
        503
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_header_keys_and_internal_forwarding() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/actor/*", "header")).await;
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let a = super::front_deploys::front_app_node("limit-a", da.path(), &store, None).await;
    let b = super::front_deploys::front_app_node("limit-b", db.path(), &store, None).await;
    for extra in [
        "",
        "x-client-id: a\r\nx-client-id: b\r\n",
        "x-client-id: \r\n",
    ] {
        assert_eq!(
            send(a.addr, "GET", "/actor/t/x/durable", extra)
                .await
                .status,
            400
        );
    }
    assert_eq!(
        send(
            a.addr,
            "POST",
            "/actor/t/x/durable",
            "x-client-id: client\r\n"
        )
        .await
        .status,
        200
    );
    // Same key on another node gets its independent allowance. Forwarding to
    // the already depleted owner must not charge that owner a second time.
    let r = send_reachable(
        b.addr,
        "GET",
        "/actor/t/x/durable",
        "x-client-id: client\r\n",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        send(
            b.addr,
            "GET",
            "/actor/t/x/durable",
            "x-client-id: client\r\n"
        )
        .await
        .status,
        429
    );
    assert_eq!(
        send(
            a.addr,
            "GET",
            "/actor/t/y/durable",
            "x-client-id: other\r\n"
        )
        .await
        .status,
        200
    );
}

async fn old_reader(store: &Arc<dyn ObjectStore>) {
    store.put(&"nodes/old-reader".into(),format!("wasmkeep-node 2\nold-reader\nhttp://10.0.0.9:8080\n0.4.2\n{}\ndefault/shop\n\n\nserving\nsecrets\ntake front ends flush\n",now_ms()).into_bytes().into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_deploy_and_rollback_refuse_old_readers() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let rt = rate_runtime();
    app.deploy_changes(
        &rt,
        Changes {
            classes: Some(vec![("t".into(), test_actor_wasm().to_vec())]),
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    let before = app.current().await.unwrap();
    old_reader(&store).await;
    let err = app
        .deploy_changes(
            &rt,
            Changes {
                limits: Some(policy("/*", "ip")),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rate_limits"), "{err}");
    assert_eq!(app.current().await.unwrap(), before);
    store.delete(&"nodes/old-reader".into()).await.unwrap();
    app.deploy_changes(
        &rt,
        Changes {
            limits: Some(policy("/*", "ip")),
            ..Changes::default()
        },
    )
    .await
    .unwrap();
    let removal = app
        .deploy_changes(&rt, Changes::whole(None, vec![]))
        .await
        .unwrap();
    assert_eq!(removal.diff.limits, vec!["ingress: removed"]);
    old_reader(&store).await;
    let err = app.rollback(true).await.unwrap_err();
    assert!(err.to_string().contains("rate_limits"), "{err}");
    assert_eq!(app.current().await.unwrap().unwrap(), removal.current);
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_count_websocket_handshakes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/actor/t/socket", "ip")).await;
    let data = tempfile::tempdir().unwrap();
    let node = super::front_deploys::front_app_node("limit-ws", data.path(), &store, None).await;
    assert_eq!(
        send_runnable(node.addr, "GET", "/actor/t/socket/durable", "")
            .await
            .status,
        200
    );
    assert_eq!(upgrade_status(node.addr, "/actor/t/socket").await, 101);
    assert_eq!(upgrade_status(node.addr, "/actor/t/socket").await, 429);
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_and_front_values_share_a_snapshot_during_deploy_and_rollback() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let policies = |mark: &str| -> Policies {
        [(
            "version".into(),
            Policy::parse(
                "/env",
                "header",
                Some(&format!("x-client-{mark}")),
                "1000000/s",
                1000000,
            )
            .unwrap(),
        )]
        .into()
    };
    let changes = |mark: &str| Changes {
        limits: Some(policies(mark)),
        front_config: Some(Some(
            wasmkeep_server::env::parse(&format!("MARK={mark}")).unwrap(),
        )),
        ..Changes::default()
    };
    let app = deploy(&store, policies("a")).await;
    let rt = rate_runtime();
    app.deploy_changes(&rt, changes("a")).await.unwrap();
    let data = tempfile::tempdir().unwrap();
    let node =
        super::front_deploys::front_app_node("limit-snapshot", data.path(), &store, None).await;
    let done = Arc::new(AtomicBool::new(false));
    let finished = done.clone();
    let reader = tokio::spawn(async move {
        let mut accepted = 0;
        while !finished.load(Ordering::Acquire) {
            for mark in ["a", "b"] {
                let response = send(
                    node.addr,
                    "GET",
                    "/env?name=MARK",
                    &format!("x-client-{mark}: client\r\n"),
                )
                .await;
                match response.status {
                    200 => {
                        assert_eq!(
                            response.body.trim(),
                            mark,
                            "policy and front must be from the same version"
                        );
                        accepted += 1;
                    }
                    400 => {}
                    status => panic!("unexpected {status}: {}", response.body),
                }
            }
        }
        accepted
    });
    for mark in ["b", "a", "b", "a", "b", "a"] {
        let deployed = app.deploy_changes(&rt, changes(mark)).await.unwrap();
        runs_deploy(&node.service, deployed.current.seq).await;
    }
    let back = app.rollback(false).await.unwrap();
    runs_deploy(&node.service, back.current.seq).await;
    done.store(true, Ordering::Release);
    assert!(reader.await.unwrap() > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_limits_fail_closed_when_a_new_manifest_cannot_be_read() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = deploy(&store, Policies::new()).await;
    let data = tempfile::tempdir().unwrap();
    let node =
        super::front_deploys::front_app_node("limit-unknown", data.path(), &store, None).await;
    assert_eq!(send(node.addr, "GET", "/self", "").await.status, 200);
    let current = app.current().await.unwrap().unwrap();
    // An unsupported future manifest could contain policies absent from the
    // running version. Readiness alone must not leave old public intake open.
    store
        .put(
            &"apps/default/shop/unknown/manifest".into(),
            "wasmkeep-manifest 7\n".as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    store
        .put(
            &"apps/default/shop/current".into(),
            format!("unknown {} 3\n", current.seq + 1)
                .into_bytes()
                .into(),
        )
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while node.service.not_ready() != Some("cannot apply the deployed version") {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for path in ["/self", "/actor/t/never/durable"] {
        let r = send(node.addr, "GET", path, "").await;
        assert_eq!(r.status, 503);
        assert_eq!(r.retry_after.as_deref(), Some("1"));
    }
}
