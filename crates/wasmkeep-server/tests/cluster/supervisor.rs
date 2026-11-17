//! One app through the supervisor (ADR 0038, slice M2): the process owns
//! the listeners, the shutdown, the listing of `nodes/`, the trust table
//! and the clients to other nodes; it serves exactly one app, which
//! behaves as a node did before. A tombstone or a verdict on its node id
//! still ends the process with the reason (a non-zero exit).

use super::*;
use wasmkeep_server::apps::DeleteApp;
use wasmkeep_server::{Listeners, NodeProcess, Supervisor};

/// A node `id` served through a supervisor of its own, with `classes`
/// (`None`: `default/shop` as `serve --app` serves it, following its
/// deploys). Returns the node and what the supervisor returned.
async fn supervised(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    app: bool,
) -> (
    Node,
    tokio::task::JoinHandle<Result<Option<String>, wasmkeep_server::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let supervisor = Supervisor::new();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_secs(30);
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    cluster.process = Some(supervisor.process());
    let mut follow = None;
    let classes = if app {
        let app = AppRef::new(Arc::clone(store), "", "default", "shop");
        app.prepare_local(&wasmkeep_server::local_app_dir(data, "default", "shop"))
            .await
            .unwrap();
        let current = app.current().await.unwrap().expect("something is deployed");
        let version = app.version(&current.version).await.unwrap();
        config.access = version.access;
        cluster.app = "default/shop".to_owned();
        cluster.follows_app = true;
        follow = Some((app, current.seq));
        version
            .classes
            .into_iter()
            .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
            .collect()
    } else {
        let component = runtime.compile(counter_wasm()).unwrap();
        vec![("counter".to_owned(), component, counter_wasm().to_vec())]
    };
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(runtime, config, classes).unwrap();
    if let Some((app, seq)) = follow {
        service.set_deploy_seq(seq);
        tokio::spawn(app.follow(service.clone(), Duration::from_millis(200)));
    }
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = Arc::new(tokio::sync::Mutex::new(Some(stopped)));
    let app = wasmkeep_server::App {
        front: None,
        actors: Some(service.clone()),
    };
    let served = tokio::spawn(async move {
        supervisor
            .serve(
                Listeners {
                    public: listener,
                    cluster: cluster_port,
                    metrics: None,
                },
                app,
                move || {
                    let stopped = Arc::clone(&stopped);
                    async move {
                        if let Some(rx) = stopped.lock().await.take() {
                            let _ = rx.await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }
                },
                wasmkeep_server::ServeLimits::default(),
            )
            .await
    });
    let _ = service.node_id_checked().await;
    (
        Node {
            addr,
            service,
            _stop: stop,
        },
        served,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_served_through_a_supervisor_answer_as_before() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, _) = supervised("a", da.path(), &store, false).await;
    let (b, _) = supervised("b", db.path(), &store, false).await;
    settle().await;
    let r = send_reachable(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    let owner = r.node.clone().unwrap();
    // The other node forwards to the owner, over the process's clients.
    let r = send_reachable(b.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));
    assert_eq!(r.node.as_deref(), Some(owner.as_str()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_process_serves_exactly_one_app() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let process = NodeProcess::new();
    let build = |id: &str, app: &str| {
        let runtime = Runtime::new(&RuntimeConfig {
            max_instances: 16,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let component = runtime.compile(counter_wasm()).unwrap();
        let mut config = ActorConfig::new(da.path().join(id));
        let mut cluster = ClusterConfig::new(id, "http://127.0.0.1:9", Arc::clone(&store));
        cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        cluster.heartbeat = Duration::from_millis(100);
        cluster.process = Some(Arc::clone(&process));
        config.cluster = Some(cluster);
        config.app = app.to_owned();
        ActorService::new(
            runtime,
            config,
            vec![("counter".to_owned(), component, counter_wasm().to_vec())],
        )
    };
    let shop = build("shop-1", "shop").unwrap();
    assert_eq!(process.serves().as_deref(), Some("shop-1"));
    let e = build("blog-1", "blog").unwrap_err().to_string();
    assert!(e.contains("one app"), "{e}");
    assert!(e.contains("shop-1"), "{e}");
    // The first app is untouched by the refusal.
    shop.node_id_checked().await.unwrap();
    assert_eq!(process.serves().as_deref(), Some("shop-1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stop_ends_the_supervisor_without_a_reason() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let (a, served) = supervised("a", da.path(), &store, false).await;
    let r = send_reachable(a.addr, "POST", "/actor/counter/alice", "").await;
    assert_eq!(r.status, 200);
    drop(a);
    let ended = tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("the supervisor never stopped")
        .unwrap();
    assert_eq!(ended.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_twin_ends_the_supervisor_with_the_reason() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let (a, served) = supervised("a", da.path(), &store, false).await;
    a.service.node_id_checked().await.unwrap();
    settle().await;
    // As in `node_leases.rs`: another process's heartbeat for this id.
    let twin = format!(
        "wasmkeep-node 3\nlive feedface\n{}\na\nhttp://127.0.0.1:1\n0\ndefault/\n\n\nserving\n\ntake\n",
        now_ms()
    );
    store
        .put(
            &object_store::path::Path::from("nodes/a"),
            twin.into_bytes().into(),
        )
        .await
        .unwrap();
    let why = tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("the twin never ended the supervisor")
        .unwrap()
        .unwrap()
        .expect("a reason, for a non-zero exit");
    assert!(
        why.starts_with("another process runs with this node id: "),
        "{why}"
    );
    drop(a);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tombstone_ends_the_supervisor_with_the_reason() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    super::app_delete::deploy(&store).await.unwrap();
    let da = tempfile::tempdir().unwrap();
    let (a, served) = supervised("a", da.path(), &store, true).await;
    settle().await;
    let done = wasmkeep_server::apps::delete_app(
        Arc::clone(&store),
        "",
        "default",
        "shop",
        &DeleteApp {
            resume: None,
            wait: Duration::from_secs(30),
            force: false,
            who: "test".into(),
            late: Duration::from_secs(1),
        },
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    assert!(done.starts_with("Deleted default/shop"), "{done}");
    let why = tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("the tombstone never ended the supervisor")
        .unwrap()
        .unwrap()
        .expect("a reason, for a non-zero exit");
    assert!(why.contains("the app was deleted"), "{why}");
    drop(a);
}
