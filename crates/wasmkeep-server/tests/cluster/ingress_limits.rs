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
    deploy(&store, policy("/*", "header")).await;
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
    // Forwarding charges the same client on the owner.
    let r = send_reachable(
        b.addr,
        "GET",
        "/actor/t/x/durable",
        "x-client-id: client\r\n",
    )
    .await;
    assert_eq!(r.status, 429, "{}", r.body);
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
    // The forwarder's local quota is untouched by a successfully forwarded request.
    let header = "x-client-id: forward-only\r\n";
    assert_eq!(
        send_reachable(b.addr, "GET", "/actor/t/x/durable", header)
            .await
            .status,
        200
    );
    assert_eq!(send(b.addr, "GET", "/self", header).await.status, 200);
    assert_eq!(send(b.addr, "GET", "/self", header).await.status, 429);
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
                    // The bounded front runtime may be busy while old
                    // snapshots finish during repeated publication. This is
                    // retryable overload, not a policy/front mismatch.
                    503 if response.body == "this node is busy; retry\n" => {
                        tokio::task::yield_now().await;
                    }
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

#[tokio::test(flavor = "multi_thread")]
async fn fix2_forwarder_without_policy_charges_owner_quota() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/actor/*", "ip")).await;
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let a = super::front_deploys::front_app_node("review-a", da.path(), &store, None).await;
    let b = super::front_deploys::front_app_node("review-b", db.path(), &store, None).await;
    let path = "/actor/t/skew/durable";
    assert_eq!(send_runnable(a.addr, "GET", path, "").await.status, 200);
    // Simulate a forwarder still on a version without policies. Its follow
    // loop sees no newer sequence and leaves this local snapshot alone.
    b.service
        .apply_deployed(
            b.service.deploy_seq(),
            &[(
                "t".into(),
                b.service.runtime().compile(test_actor_wasm()).unwrap(),
                test_actor_wasm().to_vec(),
            )],
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &Default::default(),
            None,
            &Policies::new(),
        )
        .unwrap();
    for _ in 0..2 {
        let r = send_reachable(b.addr, "GET", path, "").await;
        assert_eq!(r.status, 429, "{}", r.body);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn review_used_internal_signature_is_refused_before_body_read() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/actor/t/replay/durable", "ip")).await;
    let dir = tempfile::tempdir().unwrap();
    let a = super::front_deploys::front_app_node("review-replay", dir.path(), &store, None).await;
    assert_eq!(
        send_runnable(a.addr, "GET", "/actor/t/replay/fired", "")
            .await
            .status,
        200
    );
    let secret = store
        .get(&"cluster/secret".into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let secret = std::str::from_utf8(&secret).unwrap().trim();
    let ts = now_ms();
    let path = "/actor/t/replay/durable";
    let hash = sha256_hex(b"");
    let msg = format!(
        "wasmkeep-call v4\n{ts}\nabc\nreview-replay\nPOST\n{path}\n{hash}\nx-wasmkeep-app:default/shop\n"
    );
    let mac = hmac_hex(secret.as_bytes(), msg.as_bytes());
    let headers = format!(
        "x-wasmkeep-internal: v4 {ts} abc {hash} {mac}\r\nx-wasmkeep-app: default/shop\r\n"
    );
    let first = send(a.addr, "POST", path, &headers).await;
    // In the TLS suite, signatures on the public port do not grant bypass.
    // First arrival still burns this signature, so a replay cannot bypass.
    assert_eq!(first.status, 200, "{}", first.body);
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 100\r\nExpect: 100-continue\r\n{headers}Connection: close\r\n\r\n"
    );
    let replay = tokio::time::timeout(Duration::from_secs(2), exchange(a.addr, &request))
        .await
        .expect("replayed signature waited for a body");
    assert!(
        matches!(replay.status, 403 | 429),
        "{} {}",
        replay.status,
        replay.body
    );
}

#[derive(Debug)]
struct ManifestOutage(Arc<dyn ObjectStore>, &'static str);
impl std::fmt::Display for ManifestOutage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "manifest-outage")
    }
}
#[async_trait::async_trait]
impl ObjectStore for ManifestOutage {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        b: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.0.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if p.as_ref().ends_with(self.1) {
            return Err(object_store::Error::Generic {
                store: "outage",
                source: "temporary read failure".into(),
            });
        }
        self.0.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.0.delete_stream(p)
    }
    fn list(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.0.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &object_store::path::Path,
        b: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(a, b, o).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn review_manifest_outage_does_not_invent_pending_policy() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = deploy(&store, Policies::new()).await;
    let dir = tempfile::tempdir().unwrap();
    let node =
        super::front_deploys::front_app_node("review-outage", dir.path(), &store, None).await;
    // Its real follow loop sees the current sequence as applied; only our
    // fault-injected reader attempts a newer version.
    node.service.set_deploy_seq(0);
    let faulty = AppRef::new(
        Arc::new(ManifestOutage(store.clone(), "/manifest")),
        "",
        "default",
        "shop",
    );
    assert!(matches!(
        faulty.update(&node.service).await,
        Err(wasmkeep_server::apps::DeployError::Bucket(_))
    ));
    node.service
        .set_deploy_seq(app.current().await.unwrap().unwrap().seq);
    node.service.set_deploy_behind(true);
    let r = send(node.addr, "GET", "/self", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn review_decoded_policy_survives_component_outage() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/*", "ip")).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.app = "shop".into();
    let service = ActorService::new(rate_runtime(), config, vec![]).unwrap();
    let faulty = AppRef::new(
        Arc::new(ManifestOutage(store, ".wasm")),
        "",
        "default",
        "shop",
    );
    assert!(matches!(
        faulty.update(&service).await,
        Err(wasmkeep_server::apps::DeployError::Bucket(_))
    ));
    service.set_deploy_behind(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(
        listener,
        wasmkeep_server::App {
            front: None,
            actors: Some(service),
        },
        async {
            let _ = stopped.await;
        },
    ));
    assert_eq!(send(addr, "GET", "/missing", "").await.status, 503);
    drop(stop);
}

#[tokio::test(flavor = "multi_thread")]
async fn fix3_rejection_never_creates_an_owner_record() {
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(CountOwnerWrites(
        Arc::new(InMemory::new()),
        writes.clone(),
        Arc::new(AtomicBool::new(false)),
    ));
    deploy(&store, policy("/*", "header")).await;
    let data = tempfile::tempdir().unwrap();
    let node = super::front_deploys::front_app_node("fix3", data.path(), &store, None).await;
    for attempt in 0..100 {
        let r = send(
            node.addr,
            "GET",
            "/actor/t/warm/durable",
            &format!("x-client-id: warm-{attempt}\r\n"),
        )
        .await;
        if r.status == 200 {
            break;
        }
        assert!(attempt < 99, "warmup failed: {}", r.status);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        send(node.addr, "GET", "/self", "x-client-id: client\r\n")
            .await
            .status,
        200
    );
    let before = writes.load(Ordering::Relaxed);
    let resident = node.service.resident();
    assert_eq!(
        send(
            node.addr,
            "GET",
            "/actor/t/rejected/durable",
            "x-client-id: client\r\n"
        )
        .await
        .status,
        429
    );
    let socket = send(node.addr, "GET", "/actor/t/rejected-socket", "x-client-id: client\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n").await;
    assert_eq!(socket.status, 429);
    assert_eq!(
        node.service.resident(),
        resident,
        "rejection allocated a mailbox"
    );
    assert_eq!(
        writes.load(Ordering::Relaxed),
        before,
        "rejected request wrote ownership"
    );
}
#[derive(Debug)]
struct CountOwnerWrites(
    Arc<dyn ObjectStore>,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<AtomicBool>,
);
impl std::fmt::Display for CountOwnerWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "manifest-outage")
    }
}
#[async_trait::async_trait]
impl ObjectStore for CountOwnerWrites {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        b: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if p.as_ref().contains("/owners/") {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
        self.0.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if p.as_ref().ends_with("/owners/t/race") && self.2.swap(false, Ordering::AcqRel) {
            return Err(object_store::Error::NotFound {
                path: p.to_string(),
                source: "owner became visible after routing".into(),
            });
        }
        self.0.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.0.delete_stream(p)
    }
    fn list(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.0.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &object_store::path::Path,
        b: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(a, b, o).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fix3_charged_before_owner_race_is_not_charged_twice() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store, policy("/*", "header")).await;
    let miss = Arc::new(AtomicBool::new(false));
    let racing: Arc<dyn ObjectStore> = Arc::new(CountOwnerWrites(
        store.clone(),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        miss.clone(),
    ));
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let a = super::front_deploys::front_app_node("race-a", da.path(), &store, None).await;
    let b = super::front_deploys::front_app_node("race-b", db.path(), &racing, None).await;
    for (node, path) in [(&a, "/actor/t/race/durable"), (&b, "/actor/t/warm/durable")] {
        for attempt in 0..100 {
            let r = send(
                node.addr,
                "GET",
                path,
                &format!("x-client-id: warm-{attempt}\r\n"),
            )
            .await;
            if r.status == 200 {
                break;
            }
            assert!(attempt < 99, "warmup failed: {}", r.status);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let header = "x-client-id: racing-client\r\n";
    assert_eq!(send(a.addr, "GET", "/self", header).await.status, 200);
    miss.store(true, Ordering::Release);
    let r = send(b.addr, "GET", "/actor/t/race/durable", header).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(!miss.load(Ordering::Acquire));
    assert_eq!(send(b.addr, "GET", "/self", header).await.status, 429);
}
