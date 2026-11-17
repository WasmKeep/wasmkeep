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
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_addr = public.local_addr().unwrap();
    let cluster_advertise = format!("https://{}", cluster.local_addr().unwrap());
    let supervisor = Supervisor::named("p");
    let services: Arc<std::sync::Mutex<HashMap<String, ActorService>>> = Arc::default();
    let mut specs = Vec::new();
    for d in defs.iter().cloned() {
        let (store, data, process, services) = (
            Arc::clone(store),
            data.join(d.node),
            supervisor.process(),
            Arc::clone(&services),
        );
        let advertise = cluster_advertise.clone();
        let (name, hostnames) = (
            format!("{}/{}", d.tenant, d.app),
            d.hosts.iter().map(|h| (*h).to_owned()).collect(),
        );
        let build = move || {
            let (store, data, process, services, advertise, d) = (
                Arc::clone(&store),
                data.clone(),
                Arc::clone(&process),
                Arc::clone(&services),
                advertise.clone(),
                d.clone(),
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
                config.mailbox.idle_timeout = Duration::from_secs(30);
                let mut cluster =
                    ClusterConfig::new(d.node, "http://127.0.0.1:9", Arc::clone(&store));
                cluster.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
                cluster.heartbeat = Duration::from_millis(100);
                cluster.failover_wait = Some(Duration::ZERO);
                cluster.cluster_tls = Some(ClusterTlsConfig::new(advertise));
                cluster.cluster_plaintext = wasmkeep_server::ClusterPlaintext::Refuse;
                cluster.app = format!("{}/{}", d.tenant, d.app);
                cluster.process = Some(process);
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
                    vec![("counter".to_owned(), c, counter_wasm().to_vec())]
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
    eventually("every app started", async || {
        defs.iter().all(|d| proc.service(d.node).is_some())
    })
    .await;
    proc
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
