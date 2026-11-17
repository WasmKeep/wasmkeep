//! Several apps in one process (ADR 0038): the public port routes by
//! hostname, each app keeps its own node id, lease, state and restarts,
//! and two apps with the same class and actor names stay separate.
//!
//! Several apps talk to other nodes on the cluster port only: these run in
//! the TLS copy of the suite, and return at once in the plain one.

use super::*;
use std::collections::HashMap;
use wasmkeep_server::Runtime as AppRuntime;
use wasmkeep_server::apps::DeleteApp;
use wasmkeep_server::{AppSpec, Listeners, Supervisor};

/// One app of a test process.
#[derive(Clone)]
struct Def {
    node: &'static str,
    tenant: &'static str,
    app: &'static str,
    hosts: &'static [&'static str],
    /// Serve the app's deployed version (and follow its tombstone), rather
    /// than the counter class.
    deployed: bool,
    /// Its floor in the process's resident pool, when it has one.
    min_resident: Option<usize>,
    /// Serve the blob test actor as class `blob` beside the counter.
    blob: bool,
    /// Its stored-bytes limit, counted every 100 ms.
    quota: Option<u64>,
}

const fn def(
    node: &'static str,
    tenant: &'static str,
    app: &'static str,
    hosts: &'static [&'static str],
) -> Def {
    Def {
        node,
        tenant,
        app,
        hosts,
        deployed: false,
        min_resident: None,
        blob: false,
        quota: None,
    }
}

/// A process serving `defs` through one supervisor, until dropped.
struct Proc {
    public: SocketAddr,
    services: Arc<std::sync::Mutex<HashMap<String, ActorService>>>,
    _stop: oneshot::Sender<()>,
}

impl Proc {
    fn service(&self, node: &str) -> Option<ActorService> {
        self.services.lock().unwrap().get(node).cloned()
    }
}

async fn process(store: &Arc<dyn ObjectStore>, data: &Path, defs: &[Def]) -> Proc {
    process_pooled(store, data, defs, None).await
}

/// [`process`], its apps' residents drawn from a pool of `pool` residents.
async fn process_pooled(
    store: &Arc<dyn ObjectStore>,
    data: &Path,
    defs: &[Def],
    pool: Option<usize>,
) -> Proc {
    process_inner(store, data, defs, pool, true).await
}

async fn process_inner(
    store: &Arc<dyn ObjectStore>,
    data: &Path,
    defs: &[Def],
    pool: Option<usize>,
    wait: bool,
) -> Proc {
    let pool = pool.map(wasmkeep_runtime::ResidentPool::new);
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_addr = public.local_addr().unwrap();
    let cluster_advertise = format!("https://{}", cluster.local_addr().unwrap());
    let supervisor = Supervisor::named("p");
    let services: Arc<std::sync::Mutex<HashMap<String, ActorService>>> = Arc::default();
    let mut specs = Vec::new();
    for d in defs.iter().cloned() {
        let (store, data, process, services, pool) = (
            Arc::clone(store),
            data.join(d.node),
            supervisor.process(),
            Arc::clone(&services),
            pool.clone(),
        );
        let advertise = cluster_advertise.clone();
        let (name, hostnames) = (
            format!("{}/{}", d.tenant, d.app),
            d.hosts.iter().map(|h| (*h).to_owned()).collect(),
        );
        let build = move || {
            let (store, data, process, services, advertise, d, pool) = (
                Arc::clone(&store),
                data.clone(),
                Arc::clone(&process),
                Arc::clone(&services),
                advertise.clone(),
                d.clone(),
                pool.clone(),
            );
            Box::pin(async move {
                let runtime = Runtime::new(&RuntimeConfig {
                    max_instances: 16,
                    ..RuntimeConfig::default()
                })
                .map_err(|e| e.to_string())?;
                let mut config = ActorConfig::new(&data);
                config.tenant = d.tenant.into();
                config.app = d.app.into();
                // As the CLI does: node links under the app's first hostname.
                config.blob_links.base = d.hosts.first().map(|h| format!("https://{h}"));
                config.mailbox.idle_timeout = Duration::from_secs(30);
                if let Some(pool) = pool {
                    config.mailbox.max_resident = pool.limit();
                    config.resident_pool = Some(pool);
                    config.min_resident = d.min_resident;
                }
                let mut cluster =
                    ClusterConfig::new(d.node, "http://127.0.0.1:9", Arc::clone(&store));
                cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
                cluster.heartbeat = Duration::from_millis(100);
                cluster.failover_wait = Some(Duration::ZERO);
                cluster.cluster_tls = Some(ClusterTlsConfig::new(advertise));
                cluster.cluster_plaintext = wasmkeep_server::ClusterPlaintext::Refuse;
                cluster.app = format!("{}/{}", d.tenant, d.app);
                cluster.process = Some(process);
                config.max_app_blob_bytes = d.quota;
                if d.quota.is_some() {
                    cluster.blob_sweep_every = Duration::from_millis(100);
                    cluster.blob_epochs_every = Duration::from_millis(100);
                }
                let mut follow = None;
                let classes = if d.deployed {
                    let app = AppRef::new(Arc::clone(&store), "", d.tenant, d.app);
                    app.prepare_local(&wasmkeep_server::local_app_dir(&data, d.tenant, d.app))
                        .await
                        .map_err(|e| e.to_string())?;
                    let current = app
                        .current()
                        .await
                        .map_err(|e| e.to_string())?
                        .ok_or("nothing deployed")?;
                    let version = app
                        .version(&current.version)
                        .await
                        .map_err(|e| e.to_string())?;
                    config.assets = version.assets;
                    config.access = version.access;
                    cluster.follows_app = true;
                    follow = Some((app, current.seq));
                    version
                        .classes
                        .into_iter()
                        .map(|(name, bytes)| (name, runtime.compile(&bytes).unwrap(), bytes))
                        .collect()
                } else {
                    let c = runtime.compile(counter_wasm()).unwrap();
                    let mut classes = vec![("counter".to_owned(), c, counter_wasm().to_vec())];
                    if d.blob {
                        let wasm = super::blobs::blob_actor();
                        let c = guests::compile(&runtime, wasm);
                        classes.push(("blob".to_owned(), c, wasm.to_vec()));
                    }
                    classes
                };
                config.cluster = Some(cluster);
                let service =
                    ActorService::new(runtime, config, classes).map_err(|e| e.to_string())?;
                let mut tasks = Vec::new();
                if let Some((app, seq)) = follow {
                    service.set_deploy_seq(seq);
                    tasks.push(tokio::spawn(
                        app.follow(service.clone(), Duration::from_millis(200)),
                    ));
                }
                // The start check, as a new process runs it.
                service.node_id_checked().await?;
                services
                    .lock()
                    .unwrap()
                    .insert(d.node.to_owned(), service.clone());
                Ok(AppRuntime {
                    app: wasmkeep_server::App {
                        front: None,
                        actors: Some(service),
                    },
                    tasks,
                })
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
        };
        specs.push(AppSpec {
            name,
            hostnames,
            build: Arc::new(build),
        });
    }
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = Arc::new(tokio::sync::Mutex::new(Some(stopped)));
    tokio::spawn(async move {
        let _ = supervisor
            .serve_apps(
                Listeners {
                    public,
                    cluster: Some(cluster),
                    metrics: None,
                },
                specs,
                move || {
                    let stopped = Arc::clone(&stopped);
                    async move {
                        match stopped.lock().await.take() {
                            Some(rx) => {
                                let _ = rx.await;
                            }
                            None => std::future::pending::<()>().await,
                        }
                    }
                },
                wasmkeep_server::ServeLimits::default(),
            )
            .await;
    });
    let proc = Proc {
        public: public_addr,
        services,
        _stop: stop,
    };
    if wait {
        eventually("every app started", async || {
            defs.iter().all(|d| proc.service(d.node).is_some())
        })
        .await;
    }
    proc
}

/// [`process`], not waiting for every app to start.
async fn process_pooled_quiet(store: &Arc<dyn ObjectStore>, data: &Path, defs: &[Def]) -> Proc {
    process_inner(store, data, defs, None, false).await
}

/// `method path` to `addr` with `Host: host` (none if empty) and `extra`
/// header lines.
async fn to(addr: SocketAddr, host: &str, method: &str, path: &str, extra: &str) -> Reply {
    let host = if host.is_empty() {
        String::new()
    } else {
        format!("Host: {host}\r\n")
    };
    exchange(
        addr,
        &format!(
            "{method} {path} HTTP/1.1\r\n{host}Content-Length: 0\r\n{extra}Connection: close\r\n\r\n"
        ),
    )
    .await
}

/// [`to`], again while the actor's node is not in this node's view yet.
async fn to_reachable(addr: SocketAddr, host: &str, method: &str, path: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = to(addr, host, method, path, "").await;
        let retry = not_in_view(&r) || (r.status == 503 && tokio::time::Instant::now() < deadline);
        if !retry || tokio::time::Instant::now() >= deadline {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const SHOP_BLOG: [Def; 2] = [
    def("shop-1", "acme", "shop", &["shop.test"]),
    def("blog-1", "acme", "blog", &["blog.test"]),
];

/// The exit criterion (ADR 0038): two apps with the same class and actor
/// names, served by the same processes, stay separate.
#[tokio::test(flavor = "multi_thread")]
async fn two_apps_with_the_same_names_served_by_the_same_processes_stay_separate() {
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let p1 = process(&store, d1.path(), &SHOP_BLOG).await;
    let p2 = process(
        &store,
        d2.path(),
        &[
            def("shop-2", "acme", "shop", &["shop.test"]),
            def("blog-2", "acme", "blog", &["blog.test"]),
        ],
    )
    .await;
    settle().await;
    for (host, first, second) in [("shop.test", &p1, &p2), ("blog.test", &p2, &p1)] {
        let r = to_reachable(first.public, host, "POST", "/actor/counter/alice").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"), "{host}: {}", r.body);
        let r = to_reachable(second.public, host, "POST", "/actor/counter/alice").await;
        assert_eq!((r.status, r.body.trim()), (200, "2"), "{host}: {}", r.body);
    }
    // Each app's records are under its own root, naming its own node ids.
    let keys = bucket_keys(&store).await;
    for app in ["shop", "blog"] {
        assert!(
            keys.iter()
                .any(|k| k.contains(&format!("acme/{app}/")) && k.contains("owner")),
            "no record of acme/{app}: {keys:?}"
        );
    }
    let shop_owner = keys
        .iter()
        .find(|k| k.contains("acme/shop/") && k.contains("owner"))
        .unwrap()
        .clone();
    let body = store
        .get(&object_store::path::Path::from(shop_owner))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let holder = String::from_utf8_lossy(&body);
    assert!(holder.contains("shop-"), "{holder}");
    assert!(!holder.contains("blog-"), "{holder}");
}

#[tokio::test(flavor = "multi_thread")]
async fn apps_of_different_tenants_with_the_same_names_stay_separate_in_one_process() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(
        &store,
        d.path(),
        &[
            def("a-1", "acme", "shop", &["a.test"]),
            def("b-1", "beta", "shop", &["b.test"]),
        ],
    )
    .await;
    for host in ["a.test", "b.test"] {
        let r = to_reachable(p.public, host, "POST", "/actor/counter/alice").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"), "{host}");
    }
    let r = to_reachable(p.public, "a.test", "POST", "/actor/counter/alice").await;
    assert_eq!(r.body.trim(), "2");
}

/// ADR 0038, section 6: the authority, taken once and strictly; an unknown
/// host never falls into a default app.
#[tokio::test(flavor = "multi_thread")]
async fn the_public_port_routes_by_hostname_only() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(&store, d.path(), &SHOP_BLOG).await;
    let path = "/actor/counter/alice";
    let r = to_reachable(p.public, "SHOP.test.:8080", "POST", path).await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "normalized: {}",
        r.body
    );
    // Missing, repeated, or another in the absolute form: 400.
    assert_eq!(to(p.public, "", "POST", path, "").await.status, 400);
    assert_eq!(
        to(p.public, "shop.test", "POST", path, "Host: blog.test\r\n")
            .await
            .status,
        400
    );
    let absolute = format!(
        "POST http://blog.test{path} HTTP/1.1\r\nHost: shop.test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    assert_eq!(exchange(p.public, &absolute).await.status, 400);
    // HTTP/1.0 without Host, and an unknown host: 421.
    let old = format!("POST {path} HTTP/1.0\r\nContent-Length: 0\r\n\r\n");
    assert_eq!(exchange(p.public, &old).await.status, 421);
    assert_eq!(
        to(p.public, "other.test", "POST", path, "").await.status,
        421
    );
    // Forwarding headers are not a route.
    let r = to(
        p.public,
        "shop.test",
        "POST",
        path,
        "X-Forwarded-Host: blog.test\r\nForwarded: host=blog.test\r\n",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));
    // The process's health needs no host.
    assert_eq!(
        to(p.public, "", "GET", "/_wasmkeep/health", "")
            .await
            .status,
        200
    );
    // A connection belongs to its first request's app.
    let both = format!(
        "POST {path} HTTP/1.1\r\nHost: shop.test\r\nContent-Length: 0\r\n\r\nPOST {path} HTTP/1.1\r\nHost: blog.test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let mut stream = TcpStream::connect(p.public).await.unwrap();
    stream.write_all(both.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let statuses: Vec<&str> = raw
        .match_indices("HTTP/1.1 ")
        .map(|(i, _)| &raw[i + 9..i + 12])
        .collect();
    assert_eq!(statuses, ["200", "421"], "{raw}");
}

/// ADR 0038, section 5: the public port of a process serving several apps
/// treats every request as a client's, whatever node headers it forges.
#[tokio::test(flavor = "multi_thread")]
async fn forged_node_headers_on_the_public_port_make_a_clients_request() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(&store, d.path(), &SHOP_BLOG).await;
    let forged = "x-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: acme/blog\r\nx-wasmkeep-internal: v4 1 a b c\r\nx-wasmkeep-sender: blog-1 00\r\nx-wasmkeep-caller: acme/counter/bob\r\n";
    let r = to(
        p.public,
        "shop.test",
        "POST",
        "/actor/counter/alice",
        forged,
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let r = to(p.public, "blog.test", "POST", "/actor/counter/alice", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "blog's alice is its own"
    );
}

/// ADR 0038, section 3: a second process writing one app's node id stops
/// that app's runtime alone (its hosts answer 503 with Retry-After while a
/// restart is pending); the start check refuses while the twin writes,
/// and the runtime comes back in place under a new token once it stops.
#[tokio::test(flavor = "multi_thread")]
async fn a_twin_stops_one_apps_runtime_and_it_comes_back_in_place() {
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(&store, d.path(), &SHOP_BLOG).await;
    let r = to_reachable(p.public, "shop.test", "POST", "/actor/counter/alice").await;
    assert_eq!(r.status, 200);
    let old = p.service("shop-1").unwrap();
    let token = |body: &[u8]| {
        String::from_utf8_lossy(body)
            .lines()
            .nth(1)
            .map(str::to_owned)
    };
    let before = store
        .get(&object_store::path::Path::from("nodes/shop-1"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let twin = format!(
        "wasmkeep-node 3\nlive feedface\n{}\nshop-1\nhttp://127.0.0.1:1\n0\nacme/shop\n\n\nserving\n\ntake multi-app\n",
        now_ms()
    );
    store
        .put(
            &object_store::path::Path::from("nodes/shop-1"),
            twin.into_bytes().into(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), old.node_id_lost())
        .await
        .expect("the twin was never noticed");
    eventually("shop's hosts answer 503 with Retry-After", async || {
        let r = to(p.public, "shop.test", "POST", "/actor/counter/alice", "").await;
        r.status == 503 && r.retry_after.is_some()
    })
    .await;
    // Blog serves on.
    let r = to_reachable(p.public, "blog.test", "POST", "/actor/counter/alice").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    // The twin wrote once: once its heartbeat is stale, the restart passes
    // the start check under a new token.
    eventually("shop restarted in place", async || {
        let r = to(p.public, "shop.test", "POST", "/actor/counter/alice", "").await;
        r.status == 200
    })
    .await;
    // The stopped runtime's actors all ended before the new one started
    // over the same data directory.
    assert_eq!(old.resident(), 0);
    let after = store
        .get(&object_store::path::Path::from("nodes/shop-1"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_ne!(token(&before), token(&after));
    assert_ne!(token(&after).as_deref(), Some("live feedface"));
}

/// ADR 0038, section 3: one app's tombstone stops that runtime for good
/// (503 without Retry-After), while a sibling app of the same tenant keeps
/// serving its actors of the same names.
#[tokio::test(flavor = "multi_thread")]
async fn one_apps_tombstone_stops_that_runtime_only() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for app in ["shop", "blog"] {
        AppRef::new(Arc::clone(&store), "", "default", app)
            .deploy(
                &deploy_runtime(),
                vec![("t".to_owned(), test_actor_wasm().to_vec())],
            )
            .await
            .unwrap();
    }
    let d = tempfile::tempdir().unwrap();
    let p = process(
        &store,
        d.path(),
        &[
            Def {
                deployed: true,
                ..def("shop-1", "default", "shop", &["shop.test"])
            },
            Def {
                deployed: true,
                ..def("blog-1", "default", "blog", &["blog.test"])
            },
        ],
    )
    .await;
    for host in ["shop.test", "blog.test"] {
        let r = to_reachable(p.public, host, "POST", "/actor/t/x/durable").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"), "{host}: {}", r.body);
    }
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
    eventually("shop's hosts answer 503 without Retry-After", async || {
        let r = to(p.public, "shop.test", "GET", "/actor/t/x/durable", "").await;
        r.status == 503 && r.retry_after.is_none()
    })
    .await;
    let r = to_reachable(p.public, "blog.test", "POST", "/actor/t/x/durable").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}

/// ADR 0038, section 10: a live node without `multi-app` (an older binary
/// started after the second app was enabled) makes every runtime of a
/// process serving several apps not ready; actors keep being served.
#[tokio::test(flavor = "multi_thread")]
async fn an_older_node_joining_makes_several_apps_not_ready_and_actors_keep_serving() {
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(&store, d.path(), &SHOP_BLOG).await;
    eventually("ready", async || {
        to(p.public, "shop.test", "GET", "/_wasmkeep/ready", "")
            .await
            .status
            == 200
    })
    .await;
    let r = to_reachable(p.public, "shop.test", "POST", "/actor/counter/alice").await;
    assert_eq!(r.status, 200);
    // An older node, writing its heartbeat as one does.
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                let old = format!(
                    "wasmkeep-node 3\nlive beef\n{}\nold\nhttp://127.0.0.1:1\n0\nacme/shop\n\n\nserving\n\ntake\n",
                    now_ms()
                );
                let _ = store
                    .put(
                        &object_store::path::Path::from("nodes/old"),
                        old.into_bytes().into(),
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    eventually("not ready, naming the reason", async || {
        to(p.public, "blog.test", "GET", "/_wasmkeep/ready", "")
            .await
            .status
            == 503
    })
    .await;
    let svc = p.service("blog-1").unwrap();
    assert_eq!(
        svc.not_ready(),
        Some("a live node of an older version (no multi-app) runs in this cluster")
    );
    let r = to(p.public, "shop.test", "POST", "/actor/counter/alice", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));
    writer.abort();
}

/// ADR 0038, section 10: what a process listing a second app checks before
/// it starts: the live nodes without `multi-app`.
#[tokio::test(flavor = "multi_thread")]
async fn the_start_refusal_finds_the_live_nodes_without_multi_app() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (id, words, at) in [
        ("old", "take", now_ms()),
        ("new", "take multi-app", now_ms()),
        ("gone", "take", now_ms() - 600_000),
    ] {
        let body = format!(
            "wasmkeep-node 3\nlive beef\n{at}\n{id}\nhttp://127.0.0.1:1\n0\nacme/shop\n\n\nserving\n\n{words}\n"
        );
        store
            .put(
                &object_store::path::Path::from(format!("nodes/{id}")),
                body.into_bytes().into(),
            )
            .await
            .unwrap();
    }
    let older = wasmkeep_server::nodes_without_multi_app(Arc::clone(&store), "")
        .await
        .unwrap();
    assert_eq!(older, ["old"]);
}

/// ADR 0038, section 8: the residents of a process's apps count against
/// one pool; with the pool full of app A's idle actors, app B's wake-up
/// evicts one of them; with A at its floor, B's waits and is refused.
#[tokio::test(flavor = "multi_thread")]
async fn one_apps_idle_residents_make_room_for_anothers_wake_up() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process_pooled(&store, d.path(), &SHOP_BLOG, Some(2)).await;
    for name in ["a", "b"] {
        let r = to_reachable(
            p.public,
            "shop.test",
            "POST",
            &format!("/actor/counter/{name}"),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    assert_eq!(p.service("shop-1").unwrap().resident(), 2);
    let r = to_reachable(p.public, "blog.test", "POST", "/actor/counter/x").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    eventually("shop gave a resident up", async || {
        p.service("shop-1").unwrap().resident() == 1
    })
    .await;

    // Shop at its floor (the whole pool): blog's wake-up is refused.
    let d = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let floor = [
        Def {
            min_resident: Some(2),
            ..def("shop-2", "acme", "shop", &["shop.test"])
        },
        def("blog-2", "acme", "blog", &["blog.test"]),
    ];
    let p = process_pooled(&store, d.path(), &floor, Some(2)).await;
    for name in ["a", "b"] {
        let r = to_reachable(
            p.public,
            "shop.test",
            "POST",
            &format!("/actor/counter/{name}"),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = to(p.public, "blog.test", "POST", "/actor/counter/x", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(
        p.service("shop-2").unwrap().resident(),
        2,
        "shop kept its floor"
    );
}

/// The instance token `nodes/<id>` holds now (its line 2's second word).
async fn token_of(store: &Arc<dyn ObjectStore>, id: &str) -> String {
    use object_store::ObjectStoreExt as _;
    let body = store
        .get(&object_store::path::Path::from(format!("nodes/{id}")))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body).into_owned();
    text.lines()
        .nth(1)
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_owned()
}

/// ADR 0038, section 3: a fence over one app's node id stops that runtime
/// alone; it comes back in place under a new token, and the sibling app
/// serves throughout.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_node_id_stops_its_runtime_alone_and_it_restarts_in_place() {
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let p = process(&store, d.path(), &SHOP_BLOG).await;
    let r = to_reachable(p.public, "shop.test", "POST", "/actor/counter/alice").await;
    assert_eq!(r.status, 200);
    let old = p.service("shop-1").unwrap();
    let before = token_of(&store, "shop-1").await;
    let fence = wasmkeep_cluster::node_lease::NodeLeaseObject {
        state: wasmkeep_cluster::node_lease::NodeState::Fenced,
        instance: Some(before.clone()),
        written_at_ms: now_ms(),
    };
    store
        .put(
            &object_store::path::Path::from("nodes/shop-1"),
            fence.encode().into(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), old.node_id_lost())
        .await
        .expect("the fence was never noticed");
    let r = to_reachable(p.public, "blog.test", "POST", "/actor/counter/alice").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "blog serves on");
    eventually("shop restarted in place", async || {
        to(p.public, "shop.test", "POST", "/actor/counter/alice", "")
            .await
            .status
            == 200
    })
    .await;
    assert_eq!(old.resident(), 0);
    assert_ne!(token_of(&store, "shop-1").await, before, "a new token");
}

/// ADR 0038, tests: one app's broken deploy (its version unreadable) keeps
/// that app's runtime from starting (503 with Retry-After) and leaves the
/// other app serving.
#[tokio::test(flavor = "multi_thread")]
async fn one_apps_broken_deploy_leaves_the_other_serving() {
    use futures_util::StreamExt as _;
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for app in ["shop", "blog"] {
        AppRef::new(Arc::clone(&store), "", "default", app)
            .deploy(
                &deploy_runtime(),
                vec![("t".to_owned(), test_actor_wasm().to_vec())],
            )
            .await
            .unwrap();
    }
    // Shop's version objects are gone: its deploy cannot be applied.
    let versions: Vec<_> = store
        .list(Some(&object_store::path::Path::from("apps/default/shop")))
        .collect()
        .await;
    let manifests: Vec<_> = versions
        .into_iter()
        .map(|v| v.unwrap().location)
        .filter(|k| k.as_ref().ends_with("/manifest"))
        .collect();
    assert!(!manifests.is_empty());
    for m in manifests {
        store.delete(&m).await.unwrap();
    }
    let d = tempfile::tempdir().unwrap();
    let defs = [
        Def {
            deployed: true,
            ..def("shop-1", "default", "shop", &["shop.test"])
        },
        Def {
            deployed: true,
            ..def("blog-1", "default", "blog", &["blog.test"])
        },
    ];
    let public = start_without_waiting(&store, d.path(), &defs).await;
    eventually("blog serves", async || {
        to(public.public, "blog.test", "POST", "/actor/t/x/durable", "")
            .await
            .status
            == 200
    })
    .await;
    let r = to(public.public, "shop.test", "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert!(r.retry_after.is_some(), "a start is still pending");
}

/// [`process`], not waiting for every app to start.
async fn start_without_waiting(store: &Arc<dyn ObjectStore>, data: &Path, defs: &[Def]) -> Proc {
    process_pooled_quiet(store, data, defs).await
}

/// ADR 0038, tests: a process started again with one app removed from its
/// node file: that app's actors fail over to its other node, the other
/// app's resume on this one.
#[tokio::test(flavor = "multi_thread")]
async fn an_app_removed_from_the_file_fails_over_and_the_other_resumes() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let p1 = process(&store, d1.path(), &SHOP_BLOG).await;
    for host in ["shop.test", "blog.test"] {
        let r = to_reachable(p1.public, host, "POST", "/actor/counter/alice").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"), "{host}");
    }
    let p2 = process(
        &store,
        d2.path(),
        &[
            def("shop-2", "acme", "shop", &["shop.test"]),
            def("blog-2", "acme", "blog", &["blog.test"]),
        ],
    )
    .await;
    // p1 stops (a clean stop releases its records), and starts again
    // without blog.
    let shop1 = p1.service("shop-1").unwrap();
    drop(p1);
    eventually("p1 stopped", async || shop1.not_ready().is_some()).await;
    drop(shop1);
    let p1 = process(&store, d1.path(), &SHOP_BLOG[..1]).await;
    let r = to_reachable(p1.public, "shop.test", "POST", "/actor/counter/alice").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    assert_eq!(
        to(p1.public, "blog.test", "POST", "/actor/counter/alice", "")
            .await
            .status,
        421
    );
    let r = to_reachable(p2.public, "blog.test", "POST", "/actor/counter/alice").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}

/// ADR 0038, rolling upgrade: single-app nodes (an older one among them,
/// without `multi-app`) first; a process of several apps refuses to start
/// while the older node is live, starts once it is upgraded, and reports
/// not ready while an older node joins again; rolled back, the single-app
/// nodes serve the same actors on.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolling_upgrade_from_single_app_nodes_and_back() {
    use object_store::ObjectStoreExt as _;
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let single = |id: &'static str, app: &'static str| {
        let store = Arc::clone(&store);
        async move {
            let d = tempfile::tempdir().unwrap();
            let node = node_configured(
                id,
                d.path(),
                &store,
                Duration::from_secs(30),
                &["counter"],
                counter_wasm(),
                Duration::from_secs(30),
                "",
                |c| {
                    c.tenant = "acme".into();
                    c.app = app.into();
                    if let Some(cluster) = c.cluster.as_mut() {
                        cluster.app = format!("acme/{app}");
                    }
                },
            )
            .await;
            (node, d)
        }
    };
    let (shop_a, _da) = single("shop-a", "shop").await;
    let (blog_a, _db) = single("blog-a", "blog").await;
    settle().await;
    for addr in [shop_a.addr, blog_a.addr] {
        let r = send_reachable(addr, "POST", "/actor/counter/alice", "").await;
        assert_eq!((r.status, r.body.trim()), (200, "1"));
    }
    // A 0.5.x node of shop, without the word: a second app is refused.
    let older = |live: bool| {
        let store = Arc::clone(&store);
        async move {
            let state = if live { "live" } else { "released" };
            let body = format!(
                "wasmkeep-node 3\n{state} beef\n{}\nold\nhttp://127.0.0.1:1\n0\nacme/shop\n\n\nserving\n\ntake\n",
                now_ms()
            );
            store
                .put(
                    &object_store::path::Path::from("nodes/old"),
                    body.into_bytes().into(),
                )
                .await
                .unwrap();
        }
    };
    older(true).await;
    let refused = wasmkeep_server::nodes_without_multi_app(Arc::clone(&store), "")
        .await
        .unwrap();
    assert_eq!(refused, ["old"], "the start refusal names it");
    // It is upgraded (it leaves): the second app is enabled.
    older(false).await;
    assert!(
        wasmkeep_server::nodes_without_multi_app(Arc::clone(&store), "")
            .await
            .unwrap()
            .is_empty()
    );
    let dm = tempfile::tempdir().unwrap();
    let p = process(
        &store,
        dm.path(),
        &[
            def("shop-m", "acme", "shop", &["shop.test"]),
            def("blog-m", "acme", "blog", &["blog.test"]),
        ],
    )
    .await;
    for host in ["shop.test", "blog.test"] {
        let r = to_reachable(p.public, host, "POST", "/actor/counter/alice").await;
        assert_eq!((r.status, r.body.trim()), (200, "2"), "{host}: {}", r.body);
    }
    eventually("ready", async || {
        to(p.public, "shop.test", "GET", "/_wasmkeep/ready", "")
            .await
            .status
            == 200
    })
    .await;
    // An older node joins again: not ready, actors still served.
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                let body = format!(
                    "wasmkeep-node 3\nlive beef\n{}\nold\nhttp://127.0.0.1:1\n0\nacme/shop\n\n\nserving\n\ntake\n",
                    now_ms()
                );
                let _ = store
                    .put(
                        &object_store::path::Path::from("nodes/old"),
                        body.into_bytes().into(),
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    eventually("not ready in the mix", async || {
        to(p.public, "blog.test", "GET", "/_wasmkeep/ready", "")
            .await
            .status
            == 503
    })
    .await;
    let r = to_reachable(p.public, "shop.test", "POST", "/actor/counter/alice").await;
    assert_eq!((r.status, r.body.trim()), (200, "3"));
    writer.abort();
    older(false).await;
    // Rolled back: the process of several apps stops; the single-app
    // nodes serve the same actors on.
    drop(p);
    for addr in [shop_a.addr, blog_a.addr] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let r = send(addr, "POST", "/actor/counter/alice", "").await;
            if r.status == 200 {
                assert!(["3", "4"].contains(&r.body.trim()), "{}", r.body);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "{}", r.body);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// A6: identical URL names belong to the hostname-selected app alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_are_isolated_by_hostname_and_cache_directory() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (name, body) in [("shop", b"shop"), ("blog", b"blog")] {
        let mut change = super::assets::changes(body);
        if let wasmkeep_server::assets::Change::Set(bundle) = &mut change.assets {
            bundle.index.files.insert(
                format!("{name}.txt"),
                bundle.index.files["index.html"].clone(),
            );
            if name == "shop" {
                let wasmkeep_server::assets::Change::Set(other) =
                    super::assets::changes(b"blog").assets
                else {
                    unreachable!()
                };
                bundle.index.files.insert(
                    "borrowed.txt".into(),
                    other.index.files["index.html"].clone(),
                );
                bundle.contents.extend(other.contents);
            }
        }
        AppRef::new(store.clone(), "", "acme", name)
            .deploy_changes(&deploy_runtime(), change)
            .await
            .unwrap();
    }
    // Only blog owns these bytes; shop's otherwise valid index names them too.
    use object_store::ObjectStoreExt as _;
    store
        .delete(
            &format!(
                "apps/acme/shop/_assets/content/{}",
                wasmkeep_server::assets::digest(b"blog")
            )
            .into(),
        )
        .await
        .unwrap();
    let d = tempfile::tempdir().unwrap();
    let defs = [
        Def {
            deployed: true,
            ..def(
                "shop-assets",
                "acme",
                "shop",
                &["shop.test", "www.shop.test"],
            )
        },
        Def {
            deployed: true,
            ..def("blog-assets", "acme", "blog", &["blog.test"])
        },
    ];
    let p = process(&store, d.path(), &defs).await;
    for (host, body, node) in [
        ("shop.test", "shop", "shop-assets"),
        ("www.shop.test", "shop", "shop-assets"),
        ("blog.test", "blog", "blog-assets"),
    ] {
        let r = to(
            p.public,
            host,
            "GET",
            "/index.html",
            "X-Forwarded-Host: blog.test\r\nForwarded: host=blog.test\r\n",
        )
        .await;
        assert_eq!((r.status, r.body.as_str()), (200, body));
        let cache = d.path().join(node).join("assets/acme").join(body);
        assert!(
            cache
                .join(wasmkeep_server::assets::digest(body.as_bytes()))
                .exists(),
            "{}",
            cache.display()
        );
        assert!(
            !cache
                .join(wasmkeep_server::assets::digest(if body == "shop" {
                    b"blog"
                } else {
                    b"shop"
                }))
                .exists()
        );
    }
    assert_eq!(
        to(p.public, "unknown.test", "GET", "/index.html", "")
            .await
            .status,
        421
    );
    let metrics = wasmkeep_server::metrics::render(Default::default());
    for name in ["shop", "blog"] {
        for metric in [
            "wasmkeep_asset_requests_total",
            "wasmkeep_asset_bytes_total",
            "wasmkeep_asset_cache_bytes",
            "wasmkeep_asset_fetches_total",
        ] {
            assert!(
                metrics.lines().any(|line| line.starts_with(metric)
                    && line.contains("tenant=\"acme\"")
                    && line.contains(&format!("app=\"{name}\""))),
                "{metric}: {metrics}"
            );
        }
    }
    assert_eq!(
        to(p.public, "shop.test", "GET", "/borrowed.txt", "")
            .await
            .status,
        503
    );
    assert_eq!(
        to(p.public, "shop.test", "GET", "/blog.txt", "")
            .await
            .status,
        404
    );
    assert_eq!(
        to(p.public, "blog.test", "GET", "/shop.txt", "")
            .await
            .status,
        404
    );
}

/// Files of two apps in one process (ADR 0030, section 8): the same class,
/// name and key read their own bytes, each under its own app's root.
#[tokio::test(flavor = "multi_thread")]
async fn two_apps_with_the_same_class_name_and_key_keep_their_own_files() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let defs = [
        Def {
            blob: true,
            ..def("shop-1", "acme", "shop", &["shop.test"])
        },
        Def {
            blob: true,
            ..def("blog-1", "acme", "blog", &["blog.test"])
        },
    ];
    let p = process(&store, d.path(), &defs).await;
    settle().await;
    for host in ["shop.test", "blog.test"] {
        let body = host.as_bytes();
        let r = exchange(
            p.public,
            &format!(
                "PUT /actor/blob/alice/b/k HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{host}",
                body.len()
            ),
        )
        .await;
        assert_eq!(r.status, 200, "{host}: {}", r.body);
    }
    for host in ["shop.test", "blog.test"] {
        let r = to_reachable(p.public, host, "GET", "/actor/blob/alice/b/k").await;
        assert_eq!((r.status, r.body.as_str()), (200, host));
        let r = to_reachable(p.public, host, "GET", "/actor/blob/alice/list").await;
        assert_eq!(r.body, "k 9 application/octet-stream\nnext=-", "{host}");
    }
    let keys = bucket_keys(&store).await;
    let files: Vec<&String> = keys.iter().filter(|k| k.contains("/blobs/")).collect();
    assert_eq!(files.len(), 2, "{files:?}");
    for app in ["shop", "blog"] {
        assert!(
            files
                .iter()
                .any(|k| k.starts_with(&format!("state/acme/{app}/blobs/"))),
            "{files:?}"
        );
    }
}

/// A node link is checked by the app runtime its hostname picks, under
/// that app's key: another app's hostname refuses it, in this process or
/// another; its own app's takes it in either (ADR 0030, section 8).
#[tokio::test(flavor = "multi_thread")]
async fn a_node_link_works_only_under_its_own_apps_hostnames() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let blob = |node, app, host| Def {
        blob: true,
        ..def(node, "acme", app, host)
    };
    let p1 = process(
        &store,
        d1.path(),
        &[
            blob("shop-1", "shop", &["shop.test"]),
            blob("blog-1", "blog", &["blog.test"]),
        ],
    )
    .await;
    let p2 = process(
        &store,
        d2.path(),
        &[
            blob("shop-2", "shop", &["shop.test"]),
            blob("blog-2", "blog", &["blog.test"]),
        ],
    )
    .await;
    settle().await;
    for host in ["shop.test", "blog.test"] {
        let r = exchange(
            p1.public,
            &format!("PUT /actor/blob/alice/b/k HTTP/1.1\r\nHost: {host}\r\nContent-Length: 4\r\nConnection: close\r\n\r\n{}", &host[..4]),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = to_reachable(p1.public, "shop.test", "GET", "/actor/blob/alice/link/k").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let path = r
        .body
        .trim()
        .strip_prefix("https://shop.test")
        .unwrap()
        .to_owned();
    for (p, host, want) in [
        (&p1, "shop.test", 200),
        (&p2, "shop.test", 200),
        (&p1, "blog.test", 403),
        (&p2, "blog.test", 403),
    ] {
        let r = to(p.public, host, "GET", &path, "").await;
        assert_eq!(r.status, want, "{host}: {}", r.body);
        if want == 200 {
            assert_eq!(r.body, "shop");
        }
    }
}

/// One app over its limit is refused; the other, in the same process, is
/// not; each app's file series carry its own labels (ADR 0030, section 7).
#[tokio::test(flavor = "multi_thread")]
async fn one_app_over_its_file_limit_does_not_refuse_the_other() {
    if !over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let d = tempfile::tempdir().unwrap();
    let defs = [
        Def {
            blob: true,
            quota: Some(5),
            ..def("shop-1", "acme", "shop", &["shop.test"])
        },
        Def {
            blob: true,
            quota: Some(1_000),
            ..def("blog-1", "acme", "blog", &["blog.test"])
        },
    ];
    let p = process(&store, d.path(), &defs).await;
    settle().await;
    let put = |host: &'static str, key: &'static str| {
        let public = p.public;
        async move {
            exchange(
                public,
                &format!(
                    "PUT /actor/blob/alice/b/{key} HTTP/1.1\r\nHost: {host}\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789"
                ),
            )
            .await
        }
    };
    for host in ["shop.test", "blog.test"] {
        let r = put(host, "k").await;
        assert_eq!(r.status, 200, "{host}: {}", r.body);
    }
    let started = std::time::Instant::now();
    while put("shop.test", "more").await.status != 507 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "shop never refused"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = put("blog.test", "more").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let started = std::time::Instant::now();
    loop {
        let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
        let stored = |app: &str| {
            page.lines().any(|l| {
                l.starts_with(&format!(
                    "wasmkeep_blob_stored_bytes{{tenant=\"acme\",app=\"{app}\"}} "
                ))
            })
        };
        if stored("shop") && stored("blog") {
            assert!(
                page.lines().any(|l| l.starts_with(
                    "wasmkeep_blob_ops_total{tenant=\"acme\",app=\"shop\",op=\"put\",outcome=\"quota\"}"
                )),
                "{page}"
            );
            assert!(
                !page.lines().any(|l| l.starts_with(
                    "wasmkeep_blob_ops_total{tenant=\"acme\",app=\"blog\",op=\"put\",outcome=\"quota\"}"
                )),
                "{page}"
            );
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "{page}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
