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
                ("blob".to_owned(), super::blobs::blob_actor().to_vec()),
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
    config.blob_links.base = Some(format!("http://{addr}"));
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

/// A seen tombstone refuses every node link (ADR 0030, section 4), and
/// the command says when to run the listings that remove late uploads.
#[tokio::test(flavor = "multi_thread")]
async fn a_tombstone_refuses_every_node_link() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (a, _follows) = app_node_following("a", dir.path(), &store).await.unwrap();
    settle().await;
    let r = send_runnable(a.addr, "PUT", "/actor/blob/x/b/k", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = send_runnable(a.addr, "GET", "/actor/blob/x/link/k", "").await;
    let url = r.body.trim().to_owned();
    let path = url
        .strip_prefix(&format!("http://{}", a.addr))
        .unwrap()
        .to_owned();
    assert_eq!(send(a.addr, "GET", &path, "").await.status, 200);
    let refused = async {
        eventually(
            "the link is refused once the tombstone is seen",
            async || match try_send(a.addr, "GET", &path, "").await {
                Ok(r) => r.status != 200,
                Err(_) => true,
            },
        )
        .await;
        if let Ok(r) = try_send(a.addr, "GET", &path, "").await {
            assert!(matches!(r.status, 403 | 503), "{} {}", r.status, r.body);
        }
    };
    let (done, ()) = tokio::join!(delete(&store), refused);
    let done = done.unwrap();
    assert!(done.contains("--resume "), "{done}");
    assert!(done.contains("in 60 min"), "{done}");
}

/// Actor names that `encode_segment` escapes (`:`, a space, `%`, `/`,
/// uppercase, `~`), as the directory and broadcast-group actors of big
/// rooms have: written as `bg%3Alobby`, a node's key goes through
/// `Path::from`, which escapes the `%` again, so the bucket holds
/// `bg%253Alobby`. Hand-made keys use that same spelling; two actors are
/// written by a node to show it is the one nodes use.
async fn escaped_actors(store: &Arc<dyn ObjectStore>) {
    use object_store::ObjectStoreExt as _;
    for (class, name) in [
        ("directory", "bg:lobby"),
        ("t", "directory/bg:lobby"),
        ("t", "room 1"),
        ("t", "a%b"),
    ] {
        let encoded = wasmkeep_storage::encode_segment(name);
        for key in [
            format!(
                "state/{TENANT}/{APP}/actors/{class}/{encoded}/snapshot/0000000000000001-0000000000000001.ltx"
            ),
            format!("state/{TENANT}/{APP}/owners/{class}/{encoded}"),
        ] {
            store
                .put(&object_store::path::Path::from(key), b"x".to_vec().into())
                .await
                .unwrap();
        }
    }
    assert_listed(
        store,
        &[
            "/actors/directory/bg%253Alobby/",
            "/owners/directory/bg%253Alobby",
            "/actors/t/directory%252Fbg%253Alobby/",
            "/actors/t/room%25201/",
            "/actors/t/a%2525b/",
        ],
    )
    .await;
}

/// Asserts the app's listing has a key containing each of `spelled`.
async fn assert_listed(store: &Arc<dyn ObjectStore>, spelled: &[&str]) {
    let listed = app_ref(store).app_keys().await.unwrap();
    for spelled in spelled {
        assert!(
            listed.iter().any(|k| k.contains(spelled)),
            "no key with {spelled} in {listed:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_an_app_removes_actors_whose_names_are_escaped() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let a = app_node("a", dir.path(), &store).await.unwrap();
    settle().await;
    for name in ["bg:lobby", "Room~1"] {
        assert_eq!(read(a.addr, name).await, "0");
        write(a.addr, name, 2).await;
    }
    escaped_actors(&store).await;
    assert_listed(
        &store,
        &["/actors/t/bg%253Alobby/", "/actors/t/%2552oom%257E1/"],
    )
    .await;

    let done = delete(&store).await.unwrap();
    assert!(done.starts_with("Deleted default/shop"), "{done}");
    let app = app_ref(&store);
    assert_eq!(app.app_keys().await.unwrap(), Vec::<String>::new());
    assert!(app.tombstone().await.unwrap().is_some());
}

/// A bucket a delete gave up on (its tombstone written, the escaped keys
/// left): `--resume` removes them, with no migration of the keys.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_an_app_delete_removes_escaped_keys_left_behind() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    escaped_actors(&store).await;
    let app = app_ref(&store);
    let id = wasmkeep_cluster::control::new_request_id();
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert!(
        app.write_tombstone(&format!(
            "wasmkeep-app-deleted 1\nby test\nat {at}\nid {id}\n"
        ))
        .await
        .unwrap()
    );

    let done = wasmkeep_server::apps::delete_app(
        Arc::clone(&store),
        "",
        TENANT,
        APP,
        &DeleteApp {
            resume: Some(id.clone()),
            ..ask()
        },
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    assert!(done.starts_with("Deleted default/shop"), "{done}");
    assert_eq!(app.app_keys().await.unwrap(), Vec::<String>::new());
}

/// Counts the listed keys read before the tombstone is written.
#[derive(Debug)]
struct ListedBeforeTombstone {
    inner: Arc<dyn ObjectStore>,
    listed: Arc<AtomicUsize>,
    at_tombstone: std::sync::Mutex<Option<usize>>,
}

impl std::fmt::Display for ListedBeforeTombstone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "listed-before-tombstone")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ListedBeforeTombstone {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        b: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if p.as_ref().ends_with("/deleted") {
            self.at_tombstone
                .lock()
                .unwrap()
                .get_or_insert(self.listed.load(Ordering::SeqCst));
        }
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(p)
    }
    fn list(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let listed = Arc::clone(&self.listed);
        self.inner
            .list(p)
            .inspect(move |_| {
                listed.fetch_add(1, Ordering::SeqCst);
            })
            .boxed()
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &object_store::path::Path,
        b: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

/// The tombstone is written before the app's keys are listed, not after:
/// an app with a long history has hundreds of thousands of keys, and
/// listing them all first held the tombstone back for minutes on a real
/// bucket, its nodes serving the app meanwhile. Its `at` is when it was
/// written.
#[tokio::test(flavor = "multi_thread")]
async fn the_tombstone_is_written_before_the_apps_keys_are_listed() {
    use object_store::ObjectStoreExt as _;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&inner).await.unwrap();
    // A long history: 3,000 objects under the app's state prefix.
    for i in 0..3_000 {
        inner
            .put(
                &format!("state/{TENANT}/{APP}/actors/t/x/wal/{i:016}.ltx").into(),
                b"x".to_vec().into(),
            )
            .await
            .unwrap();
    }
    let store = Arc::new(ListedBeforeTombstone {
        inner: Arc::clone(&inner),
        listed: Arc::new(AtomicUsize::new(0)),
        at_tombstone: std::sync::Mutex::new(None),
    });
    let before = now_ms();
    let done = wasmkeep_server::apps::delete_app(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        "",
        TENANT,
        APP,
        &ask(),
        &mut |_| {},
        Duration::from_millis(100),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    assert!(done.starts_with("Deleted"), "{done}");
    let listed = store
        .at_tombstone
        .lock()
        .unwrap()
        .expect("no tombstone written");
    assert!(
        listed <= 10,
        "{listed} keys listed before the tombstone was written"
    );
    let body = app_ref(&inner).tombstone().await.unwrap().unwrap();
    let at: u64 = body
        .lines()
        .find_map(|l| l.strip_prefix("at "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        at >= before,
        "the tombstone's `at` is before the command ran"
    );
}

/// The store nodes read through: the manifest of the version
/// [`break_deploys`] names never answers, as in a bucket outage of that
/// key alone.
pub(super) fn outage_of_the_next_manifest(store: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(super::ingress_limits::ManifestOutage(
        Arc::clone(store),
        "/future/manifest",
    ))
}

/// Puts a `current` naming a version whose manifest a node reading through
/// [`outage_of_the_next_manifest`] cannot read: every deploy check of that
/// node fails from then with a bucket error (it stays ready, serving the
/// version it has), and its follower backs off, doubling its wait.
pub(super) async fn break_deploys(store: &Arc<dyn ObjectStore>, tenant: &str, app: &str) {
    use object_store::ObjectStoreExt as _;
    let app_ref = AppRef::new(Arc::clone(store), "", tenant, app);
    let current = app_ref.current().await.unwrap().unwrap();
    store
        .put(
            &format!("apps/{tenant}/{app}/current").into(),
            format!("future {} 3\n", current.seq + 1)
                .into_bytes()
                .into(),
        )
        .await
        .unwrap();
}

/// How long a broken deploy's backoff is let grow, following every 200 ms:
/// failed checks near 0.2, 0.4, 0.8, 1.6, 3.2 and 6.4 s, the next one due
/// near 12.8 s.
pub(super) const BACKOFF_GROWN: Duration = Duration::from_secs(7);

/// What "a few periods" allows, following every 200 ms.
pub(super) const A_FEW_PERIODS: Duration = Duration::from_secs(2);

/// ADR 0028, section 5: the tombstone is checked every period even while
/// the node backs off from a deploy it cannot apply (a backoff that grows
/// to 300 s): the node stops serving the app within a few periods, not at
/// its next full deploy check.
#[tokio::test(flavor = "multi_thread")]
async fn a_tombstone_is_seen_within_a_few_periods_while_deploys_fail() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (a, _follows) = app_node_following("a", dir.path(), &outage_of_the_next_manifest(&store))
        .await
        .unwrap();
    settle().await;
    assert_eq!(read(a.addr, "x").await, "0");
    break_deploys(&store, TENANT, APP).await;
    tokio::time::sleep(BACKOFF_GROWN).await;
    // It keeps serving the version it has.
    assert_eq!(read(a.addr, "x").await, "0");
    assert!(app_ref(&store).write_tombstone("test").await.unwrap());
    tokio::time::timeout(A_FEW_PERIODS, a.service.app_deleted())
        .await
        .expect("the node did not stop within a few periods of the tombstone");
    let r = send(a.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
}
