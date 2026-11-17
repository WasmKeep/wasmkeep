//! Deleting an app (ADR 0028, section 5): the tombstone first; the app's
//! nodes stop serving it (sockets closed with 1001, events refused, their
//! node lease released, their local files removed); once none is live,
//! everything under its two prefixes but the tombstone goes; deploys and
//! starts refuse it from then on; `--recreate` reuses the name, and a node
//! that kept files from before the delete removes them before it serves.

use super::deletes::{read, write};
use super::websockets::{closed_within, join_lobby};
use super::*;
use wasmkeep_server::apps::{DeleteApp, DeployError};

const TENANT: &str = "default";
const APP: &str = "shop";

fn app_ref(store: &Arc<dyn ObjectStore>) -> AppRef {
    AppRef::new(Arc::clone(store), "", TENANT, APP)
}

pub(super) async fn deploy(store: &Arc<dyn ObjectStore>) -> Result<(), DeployError> {
    app_ref(store)
        .deploy(
            &deploy_runtime(),
            vec![
                ("t".to_owned(), test_actor_wasm().to_vec()),
                ("chat".to_owned(), chat_wasm().to_vec()),
            ],
        )
        .await
        .map(drop)
}

/// A node serving `default/shop` as `serve --app` does: its local files
/// prepared against the bucket first (refused for a deleted app), then
/// the deployed version, following the app every 200 ms. With the task
/// that follows it: a node stopped for good has none.
pub(super) async fn app_node_following(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<(Node, tokio::task::JoinHandle<()>), DeployError> {
    let app = app_ref(store);
    app.prepare_local(&wasmkeep_server::local_app_dir(data, TENANT, APP))
        .await?;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let current = app.current().await?.expect("something is deployed");
    let version = app.version(&current.version).await?;
    let classes = version
        .classes
        .into_iter()
        .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
        .collect();
    let mut config = ActorConfig::new(data);
    config.access = version.access;
    config.mailbox.idle_timeout = Duration::from_secs(30);
    let mut cluster = ClusterConfig::new(id, advertised(addr), Arc::clone(store));
    cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(100);
    cluster.failover_wait = Some(Duration::ZERO);
    cluster.app = format!("{TENANT}/{APP}");
    cluster.follows_app = true;
    config.cluster = Some(cluster);
    let cluster_port = cluster_listener(&mut config).await;
    let service = ActorService::new(runtime, config, classes).unwrap();
    service.set_deploy_seq(current.seq);
    let follow = tokio::spawn(app.follow(service.clone(), Duration::from_millis(200)));
    let (stop, stopped) = oneshot::channel::<()>();
    spawn_serve(listener, cluster_port, service.clone(), async {
        let _ = stopped.await;
    });
    let _ = service.node_id_checked().await;
    Ok((
        Node {
            addr,
            service,
            _stop: stop,
        },
        follow,
    ))
}

async fn app_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<Node, DeployError> {
    Ok(app_node_following(id, data, store).await?.0)
}

fn ask() -> DeleteApp {
    DeleteApp {
        resume: None,
        wait: Duration::from_secs(30),
        force: false,
        who: "test".into(),
        late: Duration::from_secs(1),
    }
}

async fn delete(store: &Arc<dyn ObjectStore>) -> Result<String, DeployError> {
    wasmkeep_server::apps::delete_app(
        Arc::clone(store),
        "",
        TENANT,
        APP,
        &ask(),
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(300),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_an_app_stops_its_nodes_removes_its_data_and_refuses_it() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    // c writes an actor, then stops with its files on disk.
    let (c, follows) = app_node_following("c", dirs[2].path(), &store)
        .await
        .unwrap();
    assert_eq!(read(c.addr, "y").await, "0");
    let r = send(c.addr, "POST", "/actor/t/y/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    follows.abort();
    drop(c);
    let a = app_node("a", dirs[0].path(), &store).await.unwrap();
    let b = app_node("b", dirs[1].path(), &store).await.unwrap();
    settle().await;
    assert_eq!(read(a.addr, "x").await, "0");
    write(a.addr, "x", 3).await;
    // A sealed secrets set goes with the app (a stand-in object).
    store
        .put(
            &object_store::path::Path::from("apps/default/shop/secrets/0000000000000001"),
            b"sealed".to_vec().into(),
        )
        .await
        .unwrap();
    let mut client = join_lobby(b.addr).await;

    let done = delete(&store).await.unwrap();
    assert!(done.starts_with("Deleted default/shop"), "{done}");
    // The nodes stopped: the socket closed with 1001, events refused, the
    // service says why.
    let (code, reason) = closed_within(&mut client, Duration::from_secs(10)).await;
    assert_eq!((code, reason.as_str()), (1001, "app deleted"));
    for node in [&a, &b] {
        tokio::time::timeout(Duration::from_secs(5), node.service.app_deleted())
            .await
            .expect("the node never stopped for the deleted app");
        let r = send(node.addr, "GET", "/actor/t/x/durable", "").await;
        assert_eq!(r.status, 503, "{}", r.body);
    }
    // Everything of the app is gone but the tombstone: histories, records,
    // versions, secrets; and the nodes' local files.
    let app = app_ref(&store);
    assert_eq!(app.app_keys().await.unwrap(), Vec::<String>::new());
    assert!(app.tombstone().await.unwrap().is_some());
    for dir in &dirs[..2] {
        assert!(
            !wasmkeep_server::local_app_dir(dir.path(), TENANT, APP).exists(),
            "local files left in {}",
            dir.path().display()
        );
    }
    // Refused from now on: a deploy, a rollback, a node's start (which
    // removes its local files of the app).
    assert!(matches!(deploy(&store).await, Err(DeployError::Deleted(_))));
    assert!(matches!(
        app.rollback(false).await,
        Err(DeployError::Deleted(_))
    ));
    let other = tempfile::tempdir().unwrap();
    let local = wasmkeep_server::local_app_dir(other.path(), TENANT, APP);
    std::fs::create_dir_all(&local).unwrap();
    assert!(matches!(
        app.prepare_local(&local).await,
        Err(DeployError::Deleted(_))
    ));
    assert!(!local.exists());
    // Run again: it finishes (nothing left), the tombstone kept.
    assert!(delete(&store).await.is_ok());
    assert!(app.tombstone().await.unwrap().is_some());

    // --recreate: the tombstone goes once nothing else is left; the name
    // deploys anew. c, whose files are from before the delete, removes them
    // before it serves: y starts empty.
    let too_soon = app.recreate(Duration::from_secs(600)).await;
    assert!(
        matches!(&too_soon, Err(DeployError::Refused(m)) if m.contains("may still land")),
        "{too_soon:?}"
    );
    let late = Duration::from_millis(500);
    assert!(app.recreate(late).await.unwrap());
    assert!(app.tombstone().await.unwrap().is_none());
    assert!(
        !app.recreate(late).await.unwrap(),
        "nothing to recreate twice"
    );
    deploy(&store).await.unwrap();
    assert!(wasmkeep_server::local_app_dir(dirs[2].path(), TENANT, APP).exists());
    let c = app_node("c", dirs[2].path(), &store).await.unwrap();
    assert_eq!(
        read(c.addr, "y").await,
        "0",
        "a deleted actor came back from a disk"
    );
    assert_eq!(read(c.addr, "x").await, "0");
}

#[tokio::test(flavor = "multi_thread")]
async fn recreate_is_refused_while_data_is_left() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let app = app_ref(&store);
    assert!(app.write_tombstone("x").await.unwrap());
    assert!(!app.write_tombstone("y").await.unwrap(), "created twice");
    // A tombstone with the versions still there: not recreated.
    assert!(app.recreate(Duration::ZERO).await.is_err());
    assert!(app.tombstone().await.unwrap().is_some());
    // No such app: nothing written.
    let none = AppRef::new(Arc::clone(&store), "", TENANT, "none");
    let refused = wasmkeep_server::apps::delete_app(
        Arc::clone(&store),
        "",
        TENANT,
        "none",
        &ask(),
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(100),
    )
    .await;
    assert!(
        matches!(&refused, Err(DeployError::Refused(m)) if m.contains("no such app")),
        "{refused:?}"
    );
    assert!(none.tombstone().await.unwrap().is_none());
    // A live node that ignores tombstones (no `ends`): refused before the
    // tombstone.
    let other = AppRef::new(Arc::clone(&store), "", TENANT, "other");
    other
        .deploy(
            &deploy_runtime(),
            vec![("t".to_owned(), test_actor_wasm().to_vec())],
        )
        .await
        .unwrap();
    // A heartbeat in the format before node leases, as a node of 0.3
    // writes it: no capability words.
    let old = format!(
        "wasmkeep-node 2\nn\nhttp://127.0.0.1:9\n0.3.0\n{}\ndefault/other\n",
        now_ms()
    );
    store
        .put(
            &object_store::path::Path::from("nodes/n"),
            old.into_bytes().into(),
        )
        .await
        .unwrap();
    let refused = wasmkeep_server::apps::delete_app(
        Arc::clone(&store),
        "",
        TENANT,
        "other",
        &ask(),
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(100),
    )
    .await;
    assert!(
        matches!(&refused, Err(DeployError::Refused(m)) if m.contains("ignores")),
        "{refused:?}"
    );
    assert!(other.tombstone().await.unwrap().is_none());
}

/// A node cut off from the bucket while its app is deleted (paused, say),
/// past the horizon at which it no longer counts as live: the command
/// fences its node lease before it removes anything, so once the node is
/// back it can never renew, and it writes nothing into the deleted app.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_cut_off_through_the_delete_never_writes_again() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&shared).await.unwrap();
    let down = Arc::new(AtomicBool::new(false));
    let flaky: Arc<dyn ObjectStore> = Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let dir = tempfile::tempdir().unwrap();
    let a = app_node("a", dir.path(), &flaky).await.unwrap();
    assert_eq!(read(a.addr, "x").await, "0");
    write(a.addr, "x", 2).await;
    down.store(true, Ordering::SeqCst);
    let mut said = Vec::new();
    let done = wasmkeep_server::apps::delete_app(
        Arc::clone(&shared),
        "",
        TENANT,
        APP,
        &ask(),
        &mut |l| said.push(l),
        Duration::from_millis(200),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    assert!(done.starts_with("Deleted"), "{done}");
    assert!(said.iter().any(|l| l.contains("fenced node a")), "{said:?}");
    // Back: its lease is fenced, so it never acts again; nothing of the app
    // comes back, whatever it is sent.
    down.store(false, Ordering::SeqCst);
    for _ in 0..10 {
        let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
        assert_ne!(r.status, 200, "a fenced node ran an event: {}", r.body);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(
        app_ref(&shared).app_keys().await.unwrap(),
        Vec::<String>::new()
    );
}

/// ADR 0035, sections 5 and 6: an app's ownership-format setting lives
/// outside its two prefixes, so neither a delete nor `--recreate` removes
/// or rewrites it: a recreated app keeps the format its name had.
#[tokio::test]
async fn delete_and_recreate_keep_the_app_format_setting() {
    use object_store::ObjectStoreExt as _;
    use wasmkeep_cluster::app_format;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let setting = object_store::path::Path::from(app_format::key("", TENANT, APP));
    store
        .put(&setting, app_format::ENABLED_BODY.to_vec().into())
        .await
        .unwrap();
    let app = app_ref(&store);
    assert!(
        !app.app_keys()
            .await
            .unwrap()
            .iter()
            .any(|k| k.contains("formats/")),
        "not one of the app's keys"
    );
    delete(&store).await.unwrap();
    assert!(app.recreate(Duration::from_millis(500)).await.unwrap());
    let body = store.get(&setting).await.unwrap().bytes().await.unwrap();
    assert_eq!(&body[..], app_format::ENABLED_BODY);
}
