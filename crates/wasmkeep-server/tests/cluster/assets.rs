//! A4/A5: real HTTP requests against an app following format 7 deploys.
use super::*;
use object_store::ObjectStoreExt;
use std::collections::BTreeMap;
use wasmkeep_server::apps::Changes;
use wasmkeep_server::assets::{Bundle, Change, Content, File, Index, digest};

pub(super) fn changes(body: &[u8]) -> Changes {
    let hash = digest(body);
    Changes {
        classes: Some(vec![]),
        assets: Change::Set(Bundle {
            index: Index {
                files: BTreeMap::from([(
                    "index.html".into(),
                    File {
                        content_type: "text/html;charset=utf-8".into(),
                        identity: Content {
                            digest: hash.clone(),
                            size: body.len() as u64,
                        },
                        br: None,
                        gzip: None,
                    },
                )]),
                ..Index::default()
            },
            contents: BTreeMap::from([(hash, body.to_vec())]),
        }),
        ..Changes::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_http_switch_missing_index_repair_and_rollback() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app_ref = AppRef::new(store.clone(), "", "default", "default");
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let first = app_ref
        .deploy_changes(&runtime, changes(b"first"))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    app_ref
        .prepare_local(&wasmkeep_server::local_app_dir(
            dir.path(),
            "default",
            "default",
        ))
        .await
        .unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.assets = app_ref
        .version(&first.current.version)
        .await
        .unwrap()
        .assets;
    config.deploy_seq = first.current.seq;
    let service = ActorService::new(runtime.clone(), config, vec![]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let app = wasmkeep_server::App {
        front: Some(wasmkeep_server::HttpComponent::deployed(&service)),
        actors: Some(service.clone()),
    };
    let server = tokio::spawn(wasmkeep_server::serve(listener, app, async {
        let _ = stopped.await;
    }));
    assert_eq!(send(addr, "GET", "/", "").await.body, "first");
    let head = send(addr, "HEAD", "/index.html", "").await;
    assert_eq!(head.status, 200);
    assert_eq!(head.header("content-length").as_deref(), Some("5"));
    assert!(head.body.is_empty());
    for (path, status) in [
        ("/%61ctor/c/x", 400),
        ("/%5fwasmkeep/ready", 400),
        ("/actor/c/x", 404),
    ] {
        assert_eq!(send(addr, "GET", path, "").await.status, status);
    }
    let tag = format!("\"{}\"", &digest(b"first")[..32]);
    assert_eq!(
        send(addr, "GET", "/", &format!("If-None-Match: W/{tag}\r\n"))
            .await
            .status,
        304
    );
    let partial = send(addr, "GET", "/", "Range: bytes=1-3\r\n").await;
    assert_eq!((partial.status, partial.body.as_str()), (206, "irs"));
    assert_eq!(
        send(addr, "GET", "/", "Range: bytes=99-\r\n").await.status,
        416
    );

    let second = app_ref
        .deploy_changes(&runtime, changes(b"second"))
        .await
        .unwrap();
    let manifest = app_ref.manifest(&second.current.version).await.unwrap();
    let key = format!(
        "apps/default/default/_assets/index/{}",
        manifest.assets.unwrap()
    );
    store.delete(&key.into()).await.unwrap();
    let follower = tokio::spawn(
        app_ref
            .clone()
            .follow(service.clone(), Duration::from_millis(20)),
    );
    wait_status(addr, 503).await;
    assert_eq!(
        service.deploy_seq(),
        first.current.seq,
        "missing index must not publish partial version"
    );
    let repaired = app_ref
        .deploy_changes(&runtime, changes(b"second"))
        .await
        .unwrap();
    assert!(repaired.unchanged);
    assert_eq!(repaired.repaired, 1);
    wait_status(addr, 200).await;
    assert_eq!(send(addr, "GET", "/", "").await.body, "second");
    app_ref.rollback(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while send(addr, "GET", "/", "").await.body != "first" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    follower.abort();
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
async fn wait_status(addr: SocketAddr, status: u16) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while send(addr, "GET", "/_wasmkeep/ready", "").await.status != status {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

mod concurrent {
    use super::*;
    use futures_util::{StreamExt, stream::BoxStream};
    use object_store::{path::Path as StorePath, *};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    // The bucket clock is two hours behind the workstation. Per-key ages let
    // tests cross retention boundaries without sleeping or changing the host clock.
    #[derive(Debug, Default, Clone)]
    struct Bucket {
        inner: Arc<InMemory>,
        ages: Arc<Mutex<BTreeMap<StorePath, Duration>>>,
        pause: Arc<AtomicBool>,
        listed: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }
    impl std::fmt::Display for Bucket {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test bucket")
        }
    }
    impl Bucket {
        fn meta(&self, mut m: ObjectMeta) -> ObjectMeta {
            m.last_modified -= Duration::from_secs(7200)
                + self
                    .ages
                    .lock()
                    .unwrap()
                    .get(&m.location)
                    .copied()
                    .unwrap_or_default();
            m
        }
        fn age(&self, key: StorePath, seconds: u64) {
            self.ages
                .lock()
                .unwrap()
                .insert(key, Duration::from_secs(seconds));
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for Bucket {
        async fn put_opts(&self, p: &StorePath, b: PutPayload, o: PutOptions) -> Result<PutResult> {
            self.ages.lock().unwrap().remove(p);
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &StorePath,
            o: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        async fn get_opts(&self, p: &StorePath, o: GetOptions) -> Result<GetResult> {
            let mut r = self.inner.get_opts(p, o).await?;
            r.meta = self.meta(r.meta);
            Ok(r)
        }
        fn delete_stream(
            &self,
            p: BoxStream<'static, Result<StorePath>>,
        ) -> BoxStream<'static, Result<StorePath>> {
            self.inner.delete_stream(p)
        }
        fn list(&self, p: Option<&StorePath>) -> BoxStream<'static, Result<ObjectMeta>> {
            let this = self.clone();
            let pause = p.is_some_and(|p| p.as_ref().ends_with("_assets/content"))
                && self.pause.swap(false, Ordering::SeqCst);
            let stream = self.inner.list(p).map(move |r| r.map(|m| this.meta(m)));
            let this = self.clone();
            stream
                .chain(
                    futures_util::stream::once(async move {
                        if pause {
                            this.listed.notify_one();
                            this.resume.notified().await;
                        }
                        None
                    })
                    .filter_map(std::future::ready),
                )
                .boxed()
        }
        async fn list_with_delimiter(&self, p: Option<&StorePath>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, a: &StorePath, b: &StorePath, o: CopyOptions) -> Result<()> {
            self.inner.copy_opts(a, b, o).await
        }
    }

    async fn start_asset_node(
        id: &str,
        store: &Arc<dyn ObjectStore>,
        data: &Path,
    ) -> (Node, tokio::task::JoinHandle<()>) {
        let app_ref = AppRef::new(store.clone(), "", "default", "shop");
        let current = app_ref.current().await.unwrap().unwrap();
        let version = app_ref.version(&current.version).await.unwrap();
        app_ref
            .prepare_local(&wasmkeep_server::local_app_dir(data, "default", "shop"))
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut config = ActorConfig::new(data);
        config.app = "shop".into();
        config.assets = version.assets;
        config.assets_cache = wasmkeep_server::assets::Cache::new(0);
        config.deploy_seq = current.seq;
        let mut cluster = ClusterConfig::new(id, advertised(addr), store.clone());
        cluster.app = "default/shop".into();
        cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        cluster.heartbeat = Duration::from_millis(100);
        cluster.failover_wait = Some(Duration::ZERO);
        config.cluster = Some(cluster);
        let port = cluster_listener(&mut config).await;
        let service = ActorService::new(
            Runtime::new(&RuntimeConfig {
                max_instances: 16,
                ..RuntimeConfig::default()
            })
            .unwrap(),
            config,
            vec![],
        )
        .unwrap();
        let follower = tokio::spawn(app_ref.follow(service.clone(), Duration::from_millis(20)));
        let (stop, stopped) = oneshot::channel();
        spawn_serve(listener, port, service.clone(), async {
            let _ = stopped.await;
        });
        service.node_id_checked().await.unwrap();
        (
            Node {
                addr,
                service,
                _stop: stop,
            },
            follower,
        )
    }
    fn site(n: usize) -> Changes {
        let mut c = changes(format!("site {n}").as_bytes());
        let Change::Set(bundle) = &mut c.assets else {
            unreachable!()
        };
        bundle.index.files.insert(
            format!("file{n}.txt"),
            bundle.index.files["index.html"].clone(),
        );
        c
    }
    async fn served(nodes: &[Node], seq: u64, n: usize) {
        for node in nodes {
            tokio::time::timeout(Duration::from_secs(5), async {
                while node.service.deploy_seq() != seq {
                    // Every currently named version must remain available during the switch.
                    assert_eq!(send(node.addr, "GET", "/", "").await.status, 200);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            for path in ["/".to_owned(), format!("/file{n}.txt")] {
                let r = send(node.addr, "GET", &path, "").await;
                assert_eq!((r.status, r.body), (200, format!("site {n}")), "{path}");
            }
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assets_seven_deploys_concurrent_collection_both_orders_and_rollback() {
        for collector_first in [true, false] {
            let bucket = Arc::new(Bucket::default());
            let store: Arc<dyn ObjectStore> = bucket.clone();
            let app = AppRef::new(store.clone(), "", "default", "shop");
            let first = app
                .deploy_changes(&deploy_runtime(), site(0))
                .await
                .unwrap();
            let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
            let mut nodes = Vec::new();
            let mut followers = Vec::new();
            for (i, dir) in dirs.iter().enumerate() {
                let (node, follower) =
                    start_asset_node(&format!("assets-{i}"), &store, dir.path()).await;
                nodes.push(node);
                followers.push(follower);
            }
            served(&nodes, first.current.seq, 0).await;
            // A stale orphan with the next deploy's bytes must be removed, then uploaded again.
            let orphan = StorePath::from(format!(
                "apps/default/shop/_assets/content/{}",
                digest(b"site 1")
            ));
            store.put(&orphan, b"site 1".to_vec().into()).await.unwrap();
            bucket.age(orphan.clone(), 7200);
            bucket.pause.store(true, Ordering::SeqCst);
            let deploy = if collector_first {
                let app = app.clone();
                let collecting = tokio::spawn(async move { app.collect(5, Duration::ZERO).await });
                tokio::time::timeout(Duration::from_secs(5), bucket.listed.notified())
                    .await
                    .unwrap();
                let deploying = {
                    let app = AppRef::new(store.clone(), "", "default", "shop");
                    tokio::spawn(async move {
                        let result = app.deploy_changes(&deploy_runtime(), site(1)).await;
                        assert!(result.is_ok(), "{result:?}");
                        result
                    })
                };
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if store
                            .list(Some(&StorePath::from(
                                "apps/default/shop/_assets/deploying",
                            )))
                            .next()
                            .await
                            .is_some()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                assert!(!deploying.is_finished());
                served(&nodes, first.current.seq, 0).await;
                bucket.resume.notify_one();
                assert_eq!(collecting.await.unwrap().unwrap().1, 1);
                deploying
            } else {
                let deploying = {
                    let app = app.clone();
                    tokio::spawn(async move {
                        let result = app.deploy_changes(&deploy_runtime(), site(1)).await;
                        assert!(result.is_ok(), "{result:?}");
                        result
                    })
                };
                tokio::time::timeout(Duration::from_secs(5), bucket.listed.notified())
                    .await
                    .unwrap();
                assert_eq!(app.collect(5, Duration::ZERO).await.unwrap(), (0, 0));
                served(&nodes, first.current.seq, 0).await;
                bucket.resume.notify_one();
                deploying
            };
            let second = deploy.await.unwrap().unwrap();
            served(&nodes, second.current.seq, 1).await;
            assert!(store.head(&orphan).await.is_ok());
            for n in 2..7 {
                let d = app
                    .deploy_changes(&deploy_runtime(), site(n))
                    .await
                    .unwrap();
                served(&nodes, d.current.seq, n).await;
                app.collect(5, Duration::ZERO).await.unwrap();
                served(&nodes, d.current.seq, n).await;
            }
            assert!(
                store
                    .head(&StorePath::from(format!(
                        "apps/default/shop/{}/manifest",
                        first.current.version
                    )))
                    .await
                    .is_err()
            );
            assert!(
                store
                    .head(&StorePath::from(format!(
                        "apps/default/shop/_assets/content/{}",
                        digest(b"site 0")
                    )))
                    .await
                    .is_err()
            );
            // Each surviving version is served cold, from every node, after every collection.
            for n in (1..6).rev() {
                app.rollback(false).await.unwrap();
                let current = app.current().await.unwrap().unwrap();
                app.collect_assets(Duration::ZERO).await.unwrap();
                served(&nodes, current.seq, n).await;
            }
            for follower in followers {
                follower.abort();
            }
        }
    }
}
