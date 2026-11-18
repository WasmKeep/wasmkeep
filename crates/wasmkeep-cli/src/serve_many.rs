//! `serve --node-file` with several apps, or with a hostname table (ADR
//! 0038): one process, three ports, one app runtime per `[[app]]`, each
//! started (and started again after a verdict) as `serve --app` starts its
//! node. The single-app path (`serve` in `main.rs`) is unchanged.

use std::sync::Arc;

use anyhow::Context as _;
use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmkeep_server::apps::AppRef;
use wasmkeep_server::{ActorConfig, ActorService, App, ClusterConfig, HttpComponent};

use crate::node_file::NodeFile;
use crate::{
    ClusterPlaintext, DEPLOY_CHECK_EVERY, ServeArgs, check_layout, egress_policy, front_requests,
    load_env, load_front_env, open_bucket, require_rooted, serve_app_rules, serve_batched,
    stop_signal,
};

/// What the app runtimes of one process share.
struct Shared {
    assets_cache: Arc<wasmkeep_server::assets::Cache>,
    runtime: Runtime,
    cache: ComponentCache,
    guest_log: wasmkeep_server::GuestLog,
    socket_budget: Arc<wasmkeep_server::SocketBudget>,
    requested_sockets: usize,
    /// The process's resident pool (ADR 0038, section 8).
    residents: Arc<wasmkeep_runtime::ResidentPool>,
    front_requests: usize,
    max_connections: usize,
    process: Arc<wasmkeep_server::NodeProcess>,
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    bucket: String,
    /// Presigns downloads when the start proof passed, once per process
    /// (ADR 0030, section 4): every app runtime, a restarted one included.
    presigner: Option<Arc<dyn object_store::signer::Signer>>,
}

/// Serves the apps `file` lists, `base` holding the process's flags.
pub(crate) fn serve_many(file: &NodeFile, base: &ServeArgs) -> anyhow::Result<()> {
    let mut entries = Vec::new();
    for i in 0..file.apps.len() {
        let mut args = base.clone();
        file.apply_entry(i, &mut args)?;
        anyhow::ensure!(
            args.node_id.is_some() && args.cluster_listen.is_some(),
            "{}: an app served beside others or by hostname is a cluster node: give it a \
             node-id, and the file [listen] cluster and cluster-advertise",
            file.apps[i].at
        );
        entries.push((args, file.apps[i].hostnames.clone()));
    }
    let bucket = base.bucket.clone().context("--node-file needs --bucket")?;
    let (store, prefix) = open_bucket(&bucket)?;
    let several = entries.len() > 1;
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: base.max_instances,
        cpu_budget: std::time::Duration::from_millis(base.cpu_budget_ms),
        ..RuntimeConfig::default()
    })?;
    let pool = runtime.max_instances() as usize;
    let front = front_requests(base.max_front_requests, pool, true, true)?;
    let wanted_resident = wasmkeep_server::fds::resident_limit(pool, front);
    file.check_resident_floors(wanted_resident)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let requested_sockets = file.limits.max_sockets_per_node.unwrap_or(50_000);
    let requested = wasmkeep_server::fds::Budget {
        public_connections: base.max_connections,
        cluster_connections: wasmkeep_server::fds::CLUSTER_CONNECTIONS,
        resident_actors: wanted_resident,
        socket_sessions: requested_sockets,
    };
    let effective = match wasmkeep_server::fds::raise_open_files_limit(1 << 20) {
        Some(limit) => wasmkeep_server::fds::budget_with_socket_priority(
            limit,
            requested,
            file.limits.max_sockets_per_node.is_some(),
        ),
        None => requested,
    };
    if effective != requested {
        tracing::warn!(
            ?requested,
            ?effective,
            "descriptor admission estimate reduced requested limits; raise LimitNOFILE to admit more"
        );
    }
    let socket_budget =
        wasmkeep_server::SocketBudget::new(requested_sockets, effective.socket_sessions);
    let cache = ComponentCache::new(runtime.clone(), &base.cache_dir)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _entered = rt.enter();
    // Before a second app is enabled anywhere, every live node must run a
    // version that knows it (ADR 0038, section 10).
    if several {
        let older = rt
            .block_on(wasmkeep_server::nodes_without_multi_app(
                Arc::clone(&store),
                &prefix,
            ))
            .map_err(|e| anyhow::anyhow!("reading the cluster's heartbeats: {e}"))?;
        anyhow::ensure!(
            older.is_empty(),
            "nodes of an older version are live in this cluster ({}): upgrade every node, \
             offline ones included, before serving several apps in one process",
            older.join(", ")
        );
    }
    let supervisor = match file.process.id.as_deref() {
        Some(name) => wasmkeep_server::Supervisor::named(name),
        None => wasmkeep_server::Supervisor::new(),
    };
    let presigner = rt.block_on(wasmkeep_cli::blob_links::presigner(
        base.blob_links,
        &bucket,
        base.blob_link_endpoint.as_deref(),
        &store,
        &prefix,
    ))?;
    let shared = Arc::new(Shared {
        presigner,
        assets_cache: wasmkeep_server::assets::Cache::new(base.assets_cache_bytes),
        runtime,
        cache,
        guest_log: wasmkeep_server::GuestLog::new(base.guest_log_rate),
        socket_budget,
        requested_sockets,
        residents: wasmkeep_runtime::ResidentPool::new(effective.resident_actors.max(1)),
        front_requests: front,
        max_connections: effective.public_connections,
        process: supervisor.process(),
        store,
        prefix,
        bucket: bucket.clone(),
    });
    let mut specs = Vec::new();
    for (i, (mut args, hostnames)) in entries.into_iter().enumerate() {
        args.blob_link_base = crate::link_base(&args, &hostnames);
        let floors = (file.apps[i].min_resident, file.apps[i].max_resident);
        let (tenant, name) = args.app.clone().context("an [[app]] names its app")?;
        // Before anything reads or writes the app's state (ADR 0022).
        let migrated = check_layout(&bucket, &shared.store, &shared.prefix, &tenant)?;
        let (shared, args) = (Arc::clone(&shared), Arc::new(args));
        let build: wasmkeep_server::BuildRuntime = Arc::new(move || {
            let (shared, args) = (Arc::clone(&shared), Arc::clone(&args));
            Box::pin(async move {
                start_runtime(&shared, &args, migrated, floors)
                    .await
                    .map_err(|e| format!("{e:#}"))
            })
        });
        specs.push(wasmkeep_server::AppSpec {
            name: format!("{tenant}/{name}"),
            hostnames,
            build,
        });
    }
    let limits = wasmkeep_server::ServeLimits {
        max_connections: shared.max_connections,
        cluster_connections: effective.cluster_connections,
        front_reservations: Some(shared.front_requests),
        ..wasmkeep_server::ServeLimits::default()
    };
    let public = file.listen.public.unwrap_or(base.listen);
    let cluster = file.listen.cluster.context("a cluster listener")?;
    rt.block_on(async move {
        let public_listener = tokio::net::TcpListener::bind(public)
            .await
            .with_context(|| format!("binding {public}"))?;
        tracing::info!("listening on http://{}", public_listener.local_addr()?);
        let metrics = match file.listen.metrics {
            Some(addr) => {
                let l = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding {addr} for metrics"))?;
                tracing::info!("metrics on http://{}/metrics", l.local_addr()?);
                Some(l)
            }
            None => None,
        };
        let cluster_listener = tokio::net::TcpListener::bind(cluster)
            .await
            .with_context(|| format!("binding {cluster} for the cluster listener"))?;
        tracing::info!(
            "cluster listener on https://{} (mutual TLS, other nodes only)",
            cluster_listener.local_addr()?
        );
        supervisor
            .serve_apps(
                wasmkeep_server::Listeners {
                    public: public_listener,
                    cluster: Some(cluster_listener),
                    metrics,
                },
                specs,
                stop_signal,
                limits,
            )
            .await?;
        anyhow::Ok(())
    })
}

/// One start of the app runtime `args` describes: what `serve --app` runs
/// for its node, the start check included, without waiting for a first
/// deploy (an app not deployed yet is tried again, after the backoff).
async fn start_runtime(
    shared: &Shared,
    args: &ServeArgs,
    migrated: bool,
    (min_resident, max_resident): (Option<usize>, Option<usize>),
) -> anyhow::Result<wasmkeep_server::Runtime> {
    let (tenant, name) = args.app.clone().context("no app")?;
    let node_id = args.node_id.clone().context("no node id")?;
    let advertise = args.advertise.clone().context("no public-advertise")?;
    let egress = egress_policy(
        args.allow_outbound.clone(),
        args.allow_outbound_port.clone(),
        &args.outbound_ca,
    )?;
    let front_env = load_front_env(&args.front_env)?;
    let app_ref = AppRef::new(Arc::clone(&shared.store), &shared.prefix, &tenant, &name);
    // A deleted app is never served, and this node's files of it go
    // (ADR 0028).
    let local = wasmkeep_server::local_app_dir(&args.data_dir, &tenant, &name);
    let blob_epoch = app_ref
        .prepare_local(&local)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let current = app_ref
        .current()
        .await?
        .with_context(|| format!("{tenant}/{name} is not deployed yet"))?;
    let deployed = app_ref.version(&current.version).await?;
    require_rooted(&current, &tenant, &name, &shared.bucket)?;
    let mut config = ActorConfig::new(&args.data_dir);
    config.tenant = tenant.clone();
    config.app = name.clone();
    config.blob_epoch = blob_epoch;
    config.max_blob_bytes = args.max_blob_bytes;
    config.max_app_blob_bytes = args.max_app_blob_bytes;
    config.blob_links = wasmkeep_server::blobs::links::LinkSettings {
        presigner: shared.presigner.clone(),
        base: args.blob_link_base.clone(),
    };
    config.egress = egress.clone();
    config.guest_log = shared.guest_log.clone();
    config.storage.max_sql_cpu_per_event = std::time::Duration::from_millis(args.sql_cpu_budget_ms);
    config.config = deployed.config;
    config.requires = deployed.requires;
    config.batched = deployed.batched;
    config.limits = deployed.limits;
    config.assets = deployed.assets;
    config.assets_cache = shared.assets_cache.clone();
    config.expire_after_idle = deployed
        .expire
        .iter()
        .map(|(c, s)| (c.clone(), std::time::Duration::from_secs(*s)))
        .collect();
    if let Some(front) = deployed.front {
        let cache = shared.cache.clone();
        let wasm = front.wasm;
        let component = tokio::task::spawn_blocking(move || cache.get_or_compile(&wasm))
            .await
            .context("compiling the deployed front")?
            .context("compiling the deployed front")?;
        config.front = Some(wasmkeep_server::DeployedFront {
            component,
            config: front.config,
            requires: front.requires,
        });
    }
    let mut follow_secrets = None;
    if !args.no_secrets {
        let keys = args
            .secrets_keys
            .iter()
            .map(|p| wasmkeep_server::secrets::SecretsKey::load(p))
            .collect::<Result<Vec<_>, _>>()?;
        let secrets = wasmkeep_server::secret_store::SecretStore::new(
            Arc::clone(&shared.store),
            &shared.prefix,
            &tenant,
            &name,
        );
        let opened = match secrets.current().await? {
            None => None,
            Some(_) if keys.is_empty() => anyhow::bail!(
                "{tenant}/{name} has secrets in the bucket: give secrets-key, or no-secrets"
            ),
            Some((seq, _)) => {
                let set = secrets
                    .read(&keys, seq)
                    .await
                    .with_context(|| format!("opening {tenant}/{name}'s secrets"))?;
                Some((seq, set))
            }
        };
        if let Some((_, set)) = &opened {
            config.secrets = set.clone();
        }
        follow_secrets = Some((secrets, keys, opened.map(|(seq, _)| seq)));
    }
    // Compiled on a blocking thread: the other apps of this process serve
    // on this runtime meanwhile, and a component embedding a JavaScript
    // engine takes seconds to compile on a cache miss.
    let cache = shared.cache.clone();
    let classes = tokio::task::spawn_blocking(move || {
        let mut classes = Vec::new();
        for (class, wasm) in deployed.classes {
            let component = cache
                .get_or_compile(&wasm)
                .with_context(|| format!("compiling class {class}"))?;
            classes.push((class, component, wasm));
        }
        anyhow::Ok(classes)
    })
    .await
    .context("compiling the deployed classes")??;
    let served: Vec<&str> = classes.iter().map(|(c, _, _)| c.as_str()).collect();
    config.env = load_env(&args.env, &served, false)?;
    serve_batched(&args.writes, &served, true)?;
    config.access = serve_app_rules(
        &args.access,
        &served,
        current.marked,
        deployed.access,
        &tenant,
        &name,
    )?;
    if !current.marked && !args.access.is_empty() {
        config.flag_access = Some(config.access.clone());
    }
    config.mailbox.idle_timeout = std::time::Duration::from_secs(args.idle_timeout_secs);
    // Residents from the process's pool, this app's floor kept; its own
    // cap at most the pool's limit.
    config.mailbox.max_resident = max_resident
        .unwrap_or(usize::MAX)
        .min(shared.residents.limit());
    config.resident_pool = Some(Arc::clone(&shared.residents));
    config.min_resident = min_resident;
    config.socket_limits.per_actor = args.max_sockets_per_actor;
    config.socket_limits.per_app = args.max_sockets_per_app.unwrap_or(shared.requested_sockets);
    config.socket_budget = Arc::clone(&shared.socket_budget);
    // Its own max-front-requests, within the process's front reservations.
    let front_requests = args
        .max_front_requests
        .unwrap_or(shared.front_requests)
        .min(shared.front_requests);
    config.max_front_requests = Some(front_requests);
    config.no_alarms = args.no_alarms;
    config.max_unsent_age = std::time::Duration::from_millis(args.max_unsent_age_ms);
    config.front_env = front_env;
    let mut cluster = ClusterConfig::new(&node_id, &advertise, Arc::clone(&shared.store));
    cluster.prefix = shared.prefix.clone();
    cluster.history_retention = std::time::Duration::from_secs(args.history_retention_secs);
    cluster.failover_wait = args.failover_wait_secs.map(std::time::Duration::from_secs);
    cluster.failover_wait_max = shared.max_connections / 4;
    cluster.follows_app = true;
    cluster.reads_secrets = !args.no_secrets;
    cluster.placement = !args.no_placement;
    cluster.app = format!("{tenant}/{name}");
    cluster.cluster_tls = args
        .cluster_advertise
        .clone()
        .map(wasmkeep_server::ClusterTlsConfig::new);
    cluster.cluster_plaintext = match args.cluster_plaintext {
        ClusterPlaintext::Accept => wasmkeep_server::ClusterPlaintext::Accept,
        ClusterPlaintext::Refuse => wasmkeep_server::ClusterPlaintext::Refuse,
    };
    cluster.process = Some(Arc::clone(&shared.process));
    cluster.migrated = migrated;
    config.cluster = Some(cluster);
    config.deploy_seq = current.seq;
    let service = ActorService::new(shared.runtime.clone(), config, classes)?;
    // Leases name their holder by node id (ADR 0020).
    service
        .node_id_checked()
        .await
        .map_err(|why| anyhow::anyhow!("{why}"))?;
    let older = service
        .older_nodes()
        .await
        .map_err(|e| anyhow::anyhow!("reading the cluster's heartbeats: {e}"))?;
    anyhow::ensure!(
        older.is_empty(),
        "{}.",
        wasmkeep_server::older_nodes_advice(&older)
    );
    let mut tasks = vec![tokio::spawn(
        app_ref.follow(service.clone(), DEPLOY_CHECK_EVERY),
    )];
    if let Some((secrets, keys, opened)) = follow_secrets {
        tasks.push(tokio::spawn(secrets.follow(
            service.clone(),
            keys,
            opened,
            std::time::Duration::from_secs(3),
            wasmkeep_server::secret_store::STALE_AFTER,
        )));
    }
    let front = HttpComponent::deployed(&service)
        .with_egress(egress)
        .with_guest_log(shared.guest_log.clone())
        .with_cpu_budget(std::time::Duration::from_millis(args.front_cpu_budget_ms))
        .with_max_requests(front_requests);
    if let Err(e) = front.check_instantiates(Some(&service)).await {
        tracing::error!(app = %format!("{tenant}/{name}"), error = %format!("{:#}", anyhow::Error::from(e)), "the deployed front does not instantiate: its requests will fail until a deploy replaces it");
    }
    tracing::info!(app = %format!("{tenant}/{name}"), node = %node_id, version = %current.version, "serving the deployed version");
    Ok(wasmkeep_server::Runtime {
        app: App {
            front: Some(front),
            actors: Some(service),
        },
        tasks,
    })
}
