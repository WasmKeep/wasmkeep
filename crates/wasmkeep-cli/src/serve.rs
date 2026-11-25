//! `wasmkeep serve`: its arguments and the command that runs a node.

use super::*;

#[derive(Debug, Clone, Args)]
pub(super) struct ServeArgs {
    /// Process-wide static asset disk cache ceiling in bytes.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub(super) assets_cache_bytes: u64,

    /// The front: a stateless component (.wasm exporting
    /// wasi:http/incoming-handler) for every path outside /actor/ and
    /// /_wasmkeep/. It may call the --actor classes with actors.call, and
    /// read its values (--front-env) with env.get. Optional when --actor is
    /// given; refused with --app, whose front comes with its deploy.
    component: Option<PathBuf>,

    /// Actor class served at /actor/<CLASS>/<name>/..., as CLASS=PATH to a
    /// component implementing the wasmkeep:actor world. Repeatable.
    #[arg(long = "actor", value_name = "CLASS=PATH", value_parser = parse_actor)]
    actors: Vec<(String, PathBuf)>,

    /// Where actor databases live.
    #[arg(long, default_value = ".wasmkeep/data")]
    pub(super) data_dir: PathBuf,

    /// Seconds an idle actor stays in memory before hibernating.
    #[arg(long, default_value_t = 30)]
    pub(super) idle_timeout_secs: u64,

    /// On Linux/glibc, sample actors + sockets every N seconds; trim at half
    /// the peak or zero, with a 60s minimum cooldown. 0 disables checks.
    #[arg(long, default_value_t = 60)]
    pub(super) idle_memory_trim_secs: u64,

    /// Address to listen on. Defaults to localhost; put a reverse proxy in
    /// front for public traffic.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub(super) listen: SocketAddr,

    /// Join a cluster as this node (`[a-z0-9-]`, unique per node). Each
    /// actor then runs on one node at a time, chosen by leases in --bucket;
    /// requests reaching another node are forwarded.
    #[arg(long, requires_all = ["bucket", "advertise"])]
    pub(super) node_id: Option<String>,

    /// Serve the classes deployed for this app (TENANT/APP) with `wasmkeep
    /// deploy`, following new deploys. Before the app's first deploy, the
    /// node waits for it (not listening yet). Needs --bucket; replaces
    /// --actor.
    #[arg(long, value_parser = parse_app, requires = "bucket", conflicts_with = "actors")]
    pub(super) app: Option<(String, String)>,

    /// Serve the apps a node file lists (`wasmkeep-node.toml`, ADR 0038):
    /// the listeners, the node id and every per-app setting come from the
    /// file, so their flags are refused beside it. Relative paths in it are
    /// read from its directory. Needs --bucket; one or more `[[app]]` entries.
    #[arg(long, value_name = "FILE", requires = "bucket", conflicts_with_all = [
        "app", "actors", "component", "node_id", "advertise", "listen", "cluster_listen",
        "cluster_advertise", "metrics_listen", "metrics_hosts", "max_sockets_per_node", "max_sockets_per_app",
        "sqlite_heap_bytes",
        "data_dir", "secrets_keys", "no_secrets", "env", "front_env", "access",
        "allow_outbound", "allow_outbound_port", "outbound_ca", "max_front_requests",
        "front_cpu_budget_ms", "no_alarms", "blob_link_base", "max_app_blob_bytes",
    ])]
    pub(super) node_file: Option<PathBuf>,

    /// URL the other nodes reach this one at, e.g. http://10.0.0.11:8080.
    #[arg(long, requires = "node_id")]
    pub(super) advertise: Option<String>,

    /// Serve the cluster listener on this address (e.g. `0.0.0.0:7443`):
    /// the port other nodes use to reach this one, with mutual TLS 1.3
    /// (each node's certificate is made in memory at start and pinned in
    /// its heartbeat; nothing to provision). Needs --cluster-advertise and
    /// the cluster flags. Only other nodes need to reach it. A cluster node
    /// needs it unless --cluster-plaintext accept.
    #[arg(long, value_name = "ADDR", requires_all = ["cluster_advertise", "node_id"])]
    pub(super) cluster_listen: Option<SocketAddr>,

    /// URL the other nodes reach the cluster listener at, e.g.
    /// https://10.0.0.11:7443 (needs --cluster-listen).
    #[arg(long, value_name = "URL", requires = "cluster_listen", value_parser = parse_cluster_advertise)]
    pub(super) cluster_advertise: Option<String>,

    /// Plain HTTP between nodes. `refuse` (the default): node calls count
    /// only on the cluster port (a signed call on --listen is treated as
    /// coming from outside), and peers without a cluster listener are not
    /// used (their actors get 503 here); a cluster node then needs
    /// --cluster-listen. `accept`: peers without a cluster listener are
    /// reached over plain HTTP, and other nodes' calls are honoured on
    /// --listen too; only on a network nobody else can reach, or while
    /// moving a cluster to TLS one node at a time.
    #[arg(long, value_enum, value_name = "MODE", default_value = "refuse")]
    pub(super) cluster_plaintext: ClusterPlaintext,

    /// Seconds an actor's old history stays in the bucket before garbage
    /// collection may delete it (ADR 0012); 0 deletes it as soon as the
    /// rule allows. A deleted actor's history goes at once whatever this
    /// is. Restoring after a delete reset was collected requires at least
    /// 3003 seconds with default clock skew (50-minute request lifetime
    /// plus clock and timestamp margins).
    #[arg(long, value_name = "SECS", default_value_t = 3600)]
    pub(super) history_retention_secs: u64,

    /// Never compact an idle actor's history (ADR 0042): no snapshot at
    /// the head when an actor hibernates, and no idle pass a retention
    /// window later. On by default;
    /// `[process] idle-compaction = false` in a node file says the same.
    #[arg(long)]
    pub(super) no_idle_compaction: bool,

    /// Seconds a request for an actor whose node died or is stopping is
    /// kept until another node can take the actor over, instead of a 503
    /// at once: a GET or HEAD without a body, or a WebSocket upgrade,
    /// arriving from outside. By default what a takeover can take, from
    /// the lease timing (25 s); 0 answers at once, as before. A load
    /// balancer's server timeout must be longer (see docs/cluster.md).
    #[arg(long, value_name = "SECS")]
    pub(super) failover_wait_secs: Option<u64>,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX]. Credentials,
    /// region and endpoint come from the AWS_* environment variables
    /// (AWS_ENDPOINT for MinIO or R2, AWS_ALLOW_HTTP=true for plain http).
    #[arg(long)]
    pub(super) bucket: Option<String>,

    /// Directory for pre-compiled components.
    #[arg(long, default_value = ".wasmkeep/cache")]
    pub(super) cache_dir: PathBuf,

    /// Maximum concurrently live instances (pooling allocator slots).
    #[arg(long, default_value_t = RuntimeConfig::default().max_instances)]
    pub(super) max_instances: u32,

    /// Guest CPU time allowed per actor event, in milliseconds. Time spent
    /// waiting on host I/O does not count. The front has its own,
    /// --front-cpu-budget-ms.
    #[arg(long, default_value_t = 50)]
    pub(super) cpu_budget_ms: u64,

    /// CPU time one event's SQL statements may use together, in
    /// milliseconds (SQLite runs in the host, outside --cpu-budget-ms).
    #[arg(long, default_value_t = 2000)]
    pub(super) sql_cpu_budget_ms: u64,

    /// SQLite's heap for the whole process, in bytes (default 4 GiB, at
    /// least 64 MiB), temporary data such as sorts included: past it, the
    /// statement that needs more fails with `quota-exceeded`. In a node
    /// file, `[limits]` `sqlite-heap-bytes`.
    #[arg(long, value_name = "BYTES", value_parser = clap::value_parser!(u64).range(wasmkeep_storage::MIN_SQLITE_HEAP_BYTES..))]
    pub(super) sqlite_heap_bytes: Option<u64>,

    /// Send an anonymous usage report (version, OS, cluster or not, and
    /// bucketed counts of classes and resident actors) a minute after start
    /// and daily. Off by default; see docs/telemetry.md for every field.
    #[arg(long)]
    usage_ping: bool,

    /// Export traces as OTLP/HTTP protobuf to this collector base URL. Off by default.
    #[arg(long)]
    pub(super) otlp_endpoint: Option<String>,

    /// Fraction of outside requests to trace (0 to 1), when an endpoint is set.
    #[arg(long, default_value_t = 0.01, value_parser = trace_ratio)]
    pub(super) trace_sample: f64,

    /// Honour both values of an incoming traceparent sampled flag.
    #[arg(long)]
    pub(super) trace_trust_incoming: bool,

    /// Serve Prometheus metrics at `http://ADDR/metrics` (e.g.
    /// `127.0.0.1:9100`). Off by default. The page names classes and
    /// counts: bind it to a private address.
    #[arg(long, value_name = "ADDR")]
    pub(super) metrics_listen: Option<SocketAddr>,

    /// A name the metrics page answers to, for a scraper that names this
    /// node by a dotted host name (`node1.internal`). Repeatable. Without
    /// it, the page answers IP addresses and `localhost`, and, off
    /// loopback, single-label names; any other `Host` gets 421.
    #[arg(long = "metrics-host", value_name = "NAME", value_parser = parse_metrics_host)]
    pub(super) metrics_hosts: Vec<String>,

    /// Let actors' outbound HTTP reach this non-public range (`10.0.0.0/8`,
    /// `127.0.0.1`, `fd00::/8`). Repeatable. By default only public
    /// addresses are reachable: never loopback, private networks or cloud
    /// metadata endpoints.
    #[arg(long = "allow-outbound", value_name = "CIDR")]
    pub(super) allow_outbound: Vec<wasmkeep_server::IpRange>,

    /// Let actors' outbound HTTP use this port. By default the ports
    /// browsers block (the Fetch standard's list: mail such as 25, 465 and
    /// 587, FTP, SSH, IRC and other non-HTTP services) are refused, since a
    /// crafted HTTP request can drive some of those servers. Repeatable;
    /// e.g. 587 for a mail relay the actors call over HTTP.
    #[arg(long = "allow-outbound-port", value_name = "PORT")]
    pub(super) allow_outbound_port: Vec<u16>,

    /// Lines a second each actor instance (and each front request) may
    /// print to the node's log, with bursts of twice that; past it, lines
    /// are dropped and counted. What guests write to stdout and stderr is
    /// logged as target `wasmkeep::guest` (filter it with RUST_LOG). 0
    /// discards it.
    #[arg(long, value_name = "LINES", default_value_t = wasmkeep_server::guest_log::DEFAULT_RATE)]
    pub(super) guest_log_rate: u32,

    /// Transparent huge pages for this process (Linux): `off` (the
    /// default) or `system` (whatever the kernel is set to). With the
    /// kernel's `always`, each resident actor's memory is backed by 2 MiB
    /// pages it mostly does not use: more than twice the memory per actor,
    /// for no measured gain in CPU or latency (docs/benchmarks.md).
    #[arg(long, value_enum, value_name = "MODE", default_value = "off")]
    transparent_huge_pages: HugePages,

    /// Also trust this certificate authority (PEM) for actors' outbound
    /// HTTPS, besides the public ones: for an internal API behind a private
    /// CA. Repeatable.
    #[arg(long = "outbound-ca", value_name = "FILE")]
    pub(super) outbound_ca: Vec<PathBuf>,

    /// Client connections open at once; more wait until one closes.
    #[arg(long, default_value_t = wasmkeep_server::ServeLimits::default().max_connections)]
    pub(super) max_connections: usize,
    /// Socket sessions per actor owner; zero refuses all WebSockets at this scope.
    #[arg(long, default_value_t = 1_000)]
    pub(super) max_sockets_per_actor: usize,
    /// Process socket sessions, including relays; default 50,000, clamped to
    /// the FD budget. Zero refuses all WebSockets. Explicit values share
    /// descriptor capacity with actors; the default uses the remainder.
    #[arg(long)]
    pub(super) max_sockets_per_node: Option<usize>,
    /// App socket sessions; defaults to the node cap. Set below the node
    /// cap for per-app isolation. Zero refuses all WebSockets for the app.
    #[arg(long)]
    pub(super) max_sockets_per_app: Option<usize>,

    /// Configuration and secrets for a class, as CLASS=FILE: `NAME=value`
    /// lines its actors read with `env.get` (see docs/concepts.md). Keep the
    /// file readable only by the server's user. Repeatable; later files win.
    #[arg(long = "env", value_name = "CLASS=FILE", value_parser = parse_actor)]
    pub(super) env: Vec<(String, PathBuf)>,

    /// The front's configuration and secrets on this node: `NAME=value`
    /// lines it reads with `env.get`, as --env gives a class's. Repeatable;
    /// later files win.
    #[arg(long = "front-env", value_name = "FILE")]
    pub(super) front_env: Vec<PathBuf>,

    /// Front requests running at once on this node; past it, 503 with
    /// `Retry-After: 1`. Their instances are kept from the pool: resident
    /// actors get the rest of --max-instances. By default a tenth of
    /// --max-instances, rounded up, beside actors (--actor or --app), and
    /// all of it on a node that serves only a component.
    #[arg(long, value_name = "N")]
    pub(super) max_front_requests: Option<usize>,

    /// One file's largest size, in bytes, for a streamed write of an
    /// actor's or the front's files (ADR 0030): at most 5 GiB, one
    /// presigned PUT's ceiling on S3. The process's, shared by every app.
    #[arg(long, value_name = "BYTES", default_value_t = wasmkeep_server::blobs::DEFAULT_MAX_FILE,
          value_parser = clap::value_parser!(u64).range(1..=wasmkeep_server::blobs::MAX_FILE_CEILING))]
    pub(super) max_blob_bytes: u64,

    /// The app's stored-bytes limit (ADR 0030, section 7): once the
    /// hourly count of its files is over it, writes and upload links are
    /// refused with `quota-exceeded` until a count is under it again. It
    /// lags by up to an hour of writes plus the upload links outstanding.
    /// By default none. In a node file, the `[[app]]` field
    /// `max-app-blob-bytes`.
    #[arg(long, value_name = "BYTES", value_parser = clap::value_parser!(u64).range(1..))]
    pub(super) max_app_blob_bytes: Option<u64>,

    /// Links to actors' and the front's files (ADR 0030): `auto` presigns
    /// downloads on a bucket whose start proof passed and uses node links
    /// otherwise; `presigned` refuses to start when the proof fails;
    /// `node`: node links only. Uploads are always node links.
    #[arg(long, value_enum, default_value_t = wasmkeep_cli::blob_links::Mode::Auto)]
    pub(super) blob_links: wasmkeep_cli::blob_links::Mode,

    /// The bucket's endpoint as browsers reach it, for presigned links
    /// (default: the one this node uses, `AWS_ENDPOINT` or the provider's).
    /// Must not be a host under the app's own site.
    #[arg(long, value_name = "URL")]
    pub(super) blob_link_endpoint: Option<String>,

    /// Where node links point, `https://HOST[:PORT]` (no path): by default
    /// `https://` and the app's first hostname, else --advertise. Never
    /// taken from a request.
    #[arg(long, value_name = "URL", value_parser = wasmkeep_server::blobs::links::check_base)]
    pub(super) blob_link_base: Option<String>,

    /// Never fire an actor's alarms on this node: no delivery, no sweep of
    /// the cluster's alarm index. Requests are still served, and alarms
    /// stay recorded and pending, so a node started without this flag fires
    /// them later. For a node on a restored copy (see the backup drill),
    /// which must not run the app's side effects twice.
    #[arg(long)]
    pub(super) no_alarms: bool,

    /// Never send a wake-up of an actor nobody holds to a lighter node,
    /// nor receive one (ADR 0041): this process's node ids keep every
    /// actor they wake and do not announce `place`. Placement is on by
    /// default; `[process] placement = false` in a node file says the same.
    #[arg(long)]
    pub(super) no_placement: bool,

    /// How a class given with --actor writes, as CLASS=MODE: `strict` (the
    /// default) or `batched` (ADR 0034: plain writes are uploaded
    /// behind the events). Not with --app: a deployed version names its classes' modes.
    /// Repeatable.
    #[arg(long = "writes", value_name = "CLASS=MODE", value_parser = parse_writes)]
    pub(super) writes: Vec<(String, wasmkeep_server::apps::Writes)>,

    /// How long a committed write of a batched class may wait unsent behind
    /// the segment in flight, in milliseconds, before its actor stops
    /// starting events (ADR 0034, section 3): 100 to 5000.
    #[arg(long = "max-unsent-age", value_name = "MS", default_value_t = 500,
          value_parser = clap::value_parser!(u64).range(100..=5000))]
    pub(super) max_unsent_age_ms: u64,

    /// The front's CPU time per request, in milliseconds (apart from
    /// --cpu-budget-ms, the actors'). Past it, the request answers 500.
    #[arg(long, value_name = "MS", default_value_t = 50,
          value_parser = clap::value_parser!(u64).range(1..))]
    pub(super) front_cpu_budget_ms: u64,

    /// The app's secrets key (`wasmkeep secrets new-key`), with `--app`:
    /// secrets set with `wasmkeep secrets` reach `env.get`, over the deployed
    /// configuration and under `--env` (ADR 0016). Given twice during a key
    /// change. Readable by its owner only.
    #[arg(long = "secrets-key", value_name = "FILE", requires = "app")]
    pub(super) secrets_keys: Vec<PathBuf>,

    /// With `--app`, run without the app's secrets from the bucket (`--env`
    /// only). Without it and without `--secrets-key`, a node refuses to
    /// start once the app has secrets.
    #[arg(long, requires = "app", conflicts_with = "secrets_keys")]
    pub(super) no_secrets: bool,

    /// Who may reach a class from outside, as CLASS=MODE: `open` (the
    /// default: any request, and a new name creates the actor), `existing`
    /// (only actors that exist; other actors create them with
    /// actors.call), `existing:NAME,...` (also lets outside requests create
    /// these names: a class's well-known actors), or `internal` (actors
    /// and alarms only). Repeatable. With --app, rules come from the deploy
    /// (`wasmkeep deploy --access`); this is refused once rules are deployed.
    #[arg(long = "access", value_name = "CLASS=MODE", value_parser = parse_access)]
    pub(super) access: Vec<(String, String)>,
}

/// Checks `--metrics-host` is a host name (no port, no scheme), lowercase
/// with no trailing dot.
pub(super) fn parse_metrics_host(s: &str) -> Result<String, String> {
    node_file::hostname(s).ok_or_else(|| {
        format!(
            "{s:?} is not a hostname (letters, digits and '-' in dot-separated labels; no \
             port, no scheme)"
        )
    })
}

/// Calls between nodes travel in plain HTTP (signed, not encrypted): warn
/// when the advertised address is a public IP, where others may see them.
pub(super) fn warn_if_public(advertise: &str) {
    let host = advertise
        .split_once("://")
        .map_or(advertise, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default();
    let host = host
        .rsplit_once(':')
        .filter(|(h, port)| !h.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
        .map_or(host, |(h, _)| h)
        .trim_start_matches('[')
        .trim_end_matches(']');
    if let Ok(ip) = host.parse::<std::net::IpAddr>()
        && wasmkeep_server::EgressPolicy::default().permits(ip)
    {
        tracing::warn!(
            %advertise,
            "--advertise is a public address: calls between nodes are signed but not \
             encrypted; keep node-to-node traffic on a private network"
        );
    }
}

pub(super) fn serve(mut args: ServeArgs) -> anyhow::Result<()> {
    huge_pages(args.transparent_huge_pages);
    let mut trace_guard = None;
    // A node file (ADR 0038) says what its flags would: read and checked
    // before anything else, then served as those flags are. `--app` is a
    // one-entry file with the same meaning, never written to disk.
    let node_file = match args.node_file.take() {
        Some(path) => {
            let file = node_file::read(&path)?;
            file.check_plaintext(args.cluster_plaintext == ClusterPlaintext::Accept)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            for w in file.warnings() {
                tracing::warn!("{w}");
            }
            // Several apps, or a hostname table: one runtime per app behind
            // the process's ports (ADR 0038).
            file.apply_traces(&mut args);
            trace_guard = wasmkeep_server::traces::start(
                args.otlp_endpoint.as_deref(),
                args.trace_sample,
                args.trace_trust_incoming,
                file.process
                    .id
                    .as_deref()
                    .or_else(|| file.apps.first().and_then(|app| app.node_id.as_deref()))
                    .or(args.node_id.as_deref())
                    .unwrap_or("standalone"),
            )
            .map_err(anyhow::Error::msg)?;
            if file.several() {
                tracing::info!(file = %path.display(), apps = file.apps.len(), "node file read");
                return serve_many::serve_many(&file, &args);
            }
            file.apply_to(&mut args)?;
            tracing::info!(file = %path.display(), "node file read");
            Some(file)
        }
        None => node_file::NodeFile::from_serve(&args),
    };
    let _trace_guard = match trace_guard {
        Some(guard) => Some(guard),
        None => wasmkeep_server::traces::start(
            args.otlp_endpoint.as_deref(),
            args.trace_sample,
            args.trace_trust_incoming,
            args.node_id.as_deref().unwrap_or("standalone"),
        )
        .map_err(anyhow::Error::msg)?,
    };
    // With --app the front comes from the deploy only (ADR 0023), whatever
    // is deployed: refused before anything is read or compiled.
    if let (Some((tenant, name)), Some(path)) = (&args.app, &args.component) {
        anyhow::bail!(
            "{}: with --app, the front comes with the app's deploy, not the command line: \
             wasmkeep deploy --app {tenant}/{name} --front PATH",
            path.display()
        );
    }
    anyhow::ensure!(
        args.env.iter().all(|(class, _)| class != "_front"),
        "--env _front=...: --env takes classes; give the front's values with --front-env FILE"
    );
    anyhow::ensure!(
        args.front_env.is_empty() || args.component.is_some() || args.app.is_some(),
        "--front-env: no front is served (give a component, or --app)"
    );
    anyhow::ensure!(
        args.component.is_some() || !args.actors.is_empty() || args.app.is_some(),
        "nothing to serve: give a component, --actor CLASS=PATH, --app, or a mix"
    );
    anyhow::ensure!(
        args.bucket.is_none() || args.node_id.is_some() || args.app.is_some(),
        "--bucket needs --node-id (to join a cluster) or --app (to serve deploys)"
    );
    anyhow::ensure!(
        args.cluster_listen.is_none() || !args.actors.is_empty() || args.app.is_some(),
        "--cluster-listen needs actors (--actor or --app): the cluster port serves actors only"
    );
    // A cluster node talks to the others over mutual TLS unless told not to
    // (ADR 0011).
    anyhow::ensure!(
        args.node_id.is_none()
            || args.cluster_listen.is_some()
            || args.cluster_plaintext == ClusterPlaintext::Accept,
        "nodes talk over mutual TLS by default: give this node --cluster-listen and \
         --cluster-advertise (a port only the other nodes reach, e.g. --cluster-listen \
         0.0.0.0:7443 --cluster-advertise https://10.0.0.11:7443), or allow plain HTTP \
         between nodes with --cluster-plaintext accept (a private network only; \
         see docs/cluster.md)"
    );
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: args.max_instances,
        cpu_budget: std::time::Duration::from_millis(args.cpu_budget_ms),
        ..RuntimeConfig::default()
    })?;
    let pool = runtime.max_instances() as usize;
    let serves_actors = !args.actors.is_empty() || args.app.is_some();
    let serves_front = args.component.is_some() || args.app.is_some();
    anyhow::ensure!(
        args.max_front_requests.is_none() || serves_front,
        "--max-front-requests: no front is served (give a component, or --app)"
    );
    let front_requests =
        front_requests(args.max_front_requests, pool, serves_front, serves_actors)?;
    // Every resident actor holds three open files: size the caps to the
    // descriptors this process may open (raising the soft limit first).
    let wanted_resident = wasmkeep_server::fds::resident_limit(pool, front_requests);
    if let Some(file) = &node_file {
        file.check_resident_floors(wanted_resident)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    let requested_sockets = args.max_sockets_per_node.unwrap_or(50_000);
    let requested = wasmkeep_server::fds::Budget {
        public_connections: args.max_connections,
        cluster_connections: if args.cluster_listen.is_some() {
            wasmkeep_server::fds::CLUSTER_CONNECTIONS
        } else {
            0
        },
        resident_actors: if serves_actors { wanted_resident } else { 0 },
        socket_sessions: if serves_actors { requested_sockets } else { 0 },
    };
    let effective = match wasmkeep_server::fds::raise_open_files_limit(1 << 20) {
        Some(limit) => {
            let effective = wasmkeep_server::fds::budget_with_socket_priority(
                limit,
                requested,
                args.max_sockets_per_node.is_some(),
            );
            if limit <= wasmkeep_server::fds::RESERVED + 8 {
                tracing::warn!(
                    open_files = limit,
                    ?effective,
                    "degraded startup: minimum service slots retained without reserved descriptor headroom; raise RLIMIT_NOFILE (ulimit -n or LimitNOFILE)"
                );
            }
            // A default socket request cut by the limit alone is the
            // INFO below, not a warning about the node's setup.
            if (wasmkeep_server::fds::Budget {
                socket_sessions: requested.socket_sessions,
                ..effective
            }) != requested
                || (args.max_sockets_per_node.is_some()
                    && effective.socket_sessions < requested.socket_sessions)
            {
                tracing::warn!(
                    open_files = limit,
                    ?requested,
                    ?effective,
                    "descriptor admission estimate reduced requested limits; raise LimitNOFILE to admit more"
                );
            }
            match wasmkeep_server::fds::socket_clamp(
                requested,
                effective,
                args.max_sockets_per_node.is_some(),
            ) {
                wasmkeep_server::fds::SocketClamp::None => {}
                wasmkeep_server::fds::SocketClamp::Default => {
                    tracing::info!(
                        effective_sockets = effective.socket_sessions,
                        default_sockets = requested.socket_sessions,
                        open_files = limit,
                        "socket session capacity limited by the open-files limit: this node admits {} WebSockets (default request {}); raise RLIMIT_NOFILE (ulimit -n or LimitNOFILE) to admit more",
                        effective.socket_sessions,
                        requested.socket_sessions
                    );
                }
                wasmkeep_server::fds::SocketClamp::Requested => {
                    tracing::warn!(
                        requested_sockets = requested.socket_sessions,
                        effective_sockets = effective.socket_sessions,
                        open_files = limit,
                        "socket session capacity reduced by descriptor budget; raise RLIMIT_NOFILE (ulimit -n or LimitNOFILE) to admit more WebSockets"
                    );
                }
            }
            effective
        }
        None => {
            tracing::warn!(
                "open-files limit unavailable; descriptor admission estimate not applied"
            );
            requested
        }
    };
    let max_connections = effective.public_connections;
    let resident_cap = effective.resident_actors;
    let cluster_connections = effective.cluster_connections;
    let socket_budget =
        wasmkeep_server::SocketBudget::new(requested_sockets, effective.socket_sessions);
    let cache = ComponentCache::new(runtime.clone(), &args.cache_dir)?;
    let load = |path: &PathBuf| -> anyhow::Result<(wasmtime::component::Component, Vec<u8>)> {
        let wasm = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let started = std::time::Instant::now();
        let component = cache
            .get_or_compile(&wasm)
            .with_context(|| format!("compiling {}", path.display()))?;
        tracing::info!(
            component = %path.display(),
            elapsed_ms = started.elapsed().as_millis(),
            "component ready"
        );
        Ok((component, wasm))
    };

    // 8 MiB thread stacks: SQLite runs on them (`serving_runtime`).
    let rt = wasmkeep_cli::serving_runtime()?;
    // ActorService starts its alarm timer on this runtime.
    let _entered = rt.enter();

    let egress = egress_policy(
        args.allow_outbound.clone(),
        args.allow_outbound_port.clone(),
        &args.outbound_ca,
    )?;
    if !egress.allow.is_empty() {
        let ranges: Vec<String> = egress.allow.iter().map(ToString::to_string).collect();
        tracing::info!(allowed = %ranges.join(", "), "outbound HTTP may also reach these ranges");
    }
    let mut app = App::default();
    // The process's listeners, shutdown and transport to other nodes
    // (ADR 0038): one app.
    let supervisor = match node_file.as_ref().and_then(|f| f.process.id.as_deref()) {
        Some(name) => wasmkeep_server::Supervisor::named(name),
        None => wasmkeep_server::Supervisor::new(),
    };
    // One for the node: each app's guests share that app's line budget.
    let guest_log = wasmkeep_server::GuestLog::new(args.guest_log_rate);
    let front_env = load_front_env(&args.front_env)?;
    if let Some(path) = &args.component {
        let (component, _) = load(path)?;
        app.front = Some(
            HttpComponent::new(runtime.clone(), &component)?
                .with_egress(egress.clone())
                .with_guest_log(guest_log.clone())
                .with_cpu_budget(std::time::Duration::from_millis(args.front_cpu_budget_ms))
                .with_max_requests(front_requests)
                .with_env(front_env.clone()),
        );
    }
    let mut follow = None;
    let mut follow_secrets = None;
    if !args.actors.is_empty() || args.app.is_some() {
        let mut classes = Vec::new();
        for (class, path) in &args.actors {
            let (component, wasm) = load(path)?;
            classes.push((class.clone(), component, wasm));
        }
        wasmkeep_cli::prepare_data_root(&args.data_dir)?;
        let mut config = ActorConfig::new(&args.data_dir);
        config.max_blob_bytes = args.max_blob_bytes;
        config.max_app_blob_bytes = args.max_app_blob_bytes;
        config.blob_links.base = link_base(&args, &[]);
        // With --app: whether rules were ever deployed (ADR 0013), and the
        // version's own.
        let mut deployed_rules = None;
        config.egress = egress.clone();
        config.guest_log = guest_log.clone();
        config.storage.max_sql_cpu_per_event =
            std::time::Duration::from_millis(args.sql_cpu_budget_ms);
        config.storage.sqlite_heap_bytes = args
            .sqlite_heap_bytes
            .unwrap_or(wasmkeep_storage::DEFAULT_SQLITE_HEAP_BYTES);
        if let (Some((tenant, name)), Some(bucket)) = (&args.app, &args.bucket) {
            let (store, prefix) = open_bucket(bucket)?;
            // Before anything reads or writes the app's state (ADR 0022).
            check_layout(bucket, &store, &prefix, tenant)?;
            let app_store = std::sync::Arc::clone(&store);
            let app_ref = AppRef::new(store, &prefix, tenant, name);
            // A deleted app is never served, and this node's files of it
            // go; after a `--recreate`, files from before the delete go
            // too (ADR 0028).
            let local = wasmkeep_server::local_app_dir(&args.data_dir, tenant, name);
            let blob_epoch = rt
                .block_on(app_ref.prepare_local(&local))
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            config.blob_epoch = blob_epoch;
            // Once per process (ADR 0030, section 4).
            config.blob_links.presigner = rt.block_on(wasmkeep_cli::blob_links::presigner(
                args.blob_links,
                bucket,
                args.blob_link_endpoint.as_deref(),
                &app_store,
                &prefix,
            ))?;
            // Before the first deploy: wait for one, as a running node
            // waits for the next (nothing below can start without it).
            let Some((current, deployed)) = rt.block_on(async {
                let Some(current) = first_deploy(
                    &app_ref,
                    &format!("{tenant}/{name}"),
                    bucket,
                    DEPLOY_CHECK_EVERY,
                    stop_signal(),
                )
                .await?
                else {
                    return anyhow::Ok(None);
                };
                let deployed = app_ref.version(&current.version).await?;
                anyhow::Ok(Some((current, deployed)))
            })?
            else {
                tracing::info!("shutting down");
                return Ok(());
            };
            // A `current` from before 0.4.0 (no token 3): its rules may still
            // be `--access` ones, which a deploy of this version would drop
            // at run time. Refuse it at start instead (ADR 0022).
            require_rooted(&current, tenant, name, bucket)?;
            deployed_rules = Some((current.marked, deployed.access));
            config.config = deployed.config;
            config.requires = deployed.requires;
            config.batched = deployed.batched;
            config.placing = deployed.placing;
            config.limits = deployed.limits;
            config.assets = deployed.assets;
            config.assets_cache = wasmkeep_server::assets::Cache::new(args.assets_cache_bytes);
            config.expire_after_idle = deployed
                .expire
                .iter()
                .map(|(c, s)| (c.clone(), std::time::Duration::from_secs(*s)))
                .collect();
            // The version's front (ADR 0023), compiled like its classes.
            if let Some(front) = deployed.front {
                let component = cache
                    .get_or_compile(&front.wasm)
                    .context("compiling the deployed front")?;
                config.front = Some(wasmkeep_server::DeployedFront {
                    component,
                    config: front.config,
                    requires: front.requires,
                });
            }
            // The app's secrets, opened before the service starts so no
            // event runs without them (ADR 0016).
            if !args.no_secrets {
                let keys = args
                    .secrets_keys
                    .iter()
                    .map(|p| wasmkeep_server::secrets::SecretsKey::load(p))
                    .collect::<Result<Vec<_>, _>>()?;
                let secrets = wasmkeep_server::secret_store::SecretStore::new(
                    std::sync::Arc::clone(&app_store),
                    &prefix,
                    tenant,
                    name,
                );
                let opened = rt.block_on(async {
                    match secrets.current().await? {
                        None => anyhow::Ok(None),
                        Some(_) if keys.is_empty() => anyhow::bail!(
                            "{tenant}/{name} has secrets in the bucket: give --secrets-key FILE, \
                             or --no-secrets to run with --env only"
                        ),
                        Some((seq, _)) => {
                            let set = secrets
                                .read(&keys, seq)
                                .await
                                .with_context(|| format!("opening {tenant}/{name}'s secrets"))?;
                            Ok(Some((seq, set)))
                        }
                    }
                })?;
                if let Some((seq, set)) = &opened {
                    tracing::info!(seq, "secrets opened");
                    config.secrets = set.clone();
                }
                follow_secrets = Some((secrets, keys, opened.map(|(seq, _)| seq)));
            }
            for (class, wasm) in deployed.classes {
                let component = cache
                    .get_or_compile(&wasm)
                    .with_context(|| format!("compiling class {class}"))?;
                classes.push((class, component, wasm));
            }
            config.tenant = tenant.clone();
            config.app = name.clone();
            tracing::info!(app = %format!("{tenant}/{name}"), version = %current.version, seq = current.seq, "serving the deployed version");
            follow = Some((app_ref, current.seq));
        }
        // After the deployed classes are known: --env and --access may name
        // those too (classes a later deploy adds cannot be named yet).
        let served: Vec<&str> = classes.iter().map(|(c, _, _)| c.as_str()).collect();
        // With --app, a class a deploy removed must not keep the node from
        // starting; its values reach it if a deploy brings it back.
        config.env = load_env(&args.env, &served, deployed_rules.is_none())?;
        match deployed_rules {
            None => {
                config.access = access_rules(&args.access, &served)?;
                config.batched = serve_batched(&args.writes, &served, false)?;
            }
            Some((marked, deployed)) => {
                serve_batched(&args.writes, &served, true)?;
                let (tenant, name) = args
                    .app
                    .as_ref()
                    .map_or(("", ""), |(t, a)| (t.as_str(), a.as_str()));
                config.access =
                    serve_app_rules(&args.access, &served, marked, deployed, tenant, name)?;
                if !marked && !args.access.is_empty() {
                    config.flag_access = Some(config.access.clone());
                }
            }
        }
        config.mailbox.idle_timeout = std::time::Duration::from_secs(args.idle_timeout_secs);
        config.idle_memory_trim = std::time::Duration::from_secs(args.idle_memory_trim_secs);
        config.mailbox.max_resident = resident_cap;
        config.socket_limits.per_actor = args.max_sockets_per_actor;
        config.socket_limits.per_app = args.max_sockets_per_app.unwrap_or(requested_sockets);
        config.socket_budget = std::sync::Arc::clone(&socket_budget);
        config.max_front_requests = Some(front_requests);
        config.no_alarms = args.no_alarms;
        config.max_unsent_age = std::time::Duration::from_millis(args.max_unsent_age_ms);
        config.front_env = front_env.clone();
        if let (Some(node_id), Some(advertise), Some(bucket)) =
            (&args.node_id, &args.advertise, &args.bucket)
        {
            let (store, prefix) = open_bucket(bucket)?;
            let mut cluster = ClusterConfig::new(node_id, advertise, store);
            cluster.prefix = prefix;
            cluster.history_retention = std::time::Duration::from_secs(args.history_retention_secs);
            cluster.idle_compaction = !args.no_idle_compaction;
            cluster.failover_wait = args.failover_wait_secs.map(std::time::Duration::from_secs);
            // Three quarters of the connections stay for everything else.
            cluster.failover_wait_max = max_connections / 4;
            // Follows its app: no actor before a check for its tombstone
            // (ADR 0028).
            cluster.follows_app = args.app.is_some();
            cluster.reads_secrets = !args.no_secrets;
            cluster.placement = !args.no_placement;
            if let Some((tenant, name)) = &args.app {
                cluster.app = format!("{tenant}/{name}");
            }
            if let Some(cluster_advertise) = &args.cluster_advertise {
                cluster.cluster_tls = Some(wasmkeep_server::ClusterTlsConfig::new(
                    cluster_advertise.clone(),
                ));
            }
            cluster.process = Some(supervisor.process());
            cluster.cluster_plaintext = match args.cluster_plaintext {
                ClusterPlaintext::Accept => wasmkeep_server::ClusterPlaintext::Accept,
                ClusterPlaintext::Refuse => wasmkeep_server::ClusterPlaintext::Refuse,
            };
            config.cluster = Some(cluster);
            tracing::info!(
                node = %node_id,
                %advertise,
                cluster_advertise = args.cluster_advertise.as_deref().unwrap_or("none"),
                cluster_plaintext = ?args.cluster_plaintext,
                %bucket,
                "cluster enabled"
            );
            anyhow::ensure!(
                advertise.starts_with("http://") && advertise.len() > "http://".len(),
                "--advertise must be an http:// URL other nodes reach this one at \
                 (http://10.0.0.11:8080), got {advertise:?}"
            );
            // With a cluster listener, nodes that have one talk over TLS.
            if args.cluster_listen.is_none() {
                warn_if_public(advertise);
            }
        }
        if let Some((_, seq)) = &follow {
            config.deploy_seq = *seq;
        }
        // A cluster node checks the bucket's layout before it writes
        // anything there, even its heartbeat (ADR 0022).
        if let (Some(bucket), Some(cluster)) = (&args.bucket, config.cluster.as_mut()) {
            let (store, prefix) = open_bucket(bucket)?;
            cluster.migrated = check_layout(bucket, &store, &prefix, &config.tenant)?;
        }
        let service = ActorService::new(runtime.clone(), config, classes)?;
        // Leases name their holder by node id: a second process with this
        // id could run the same actors as the first.
        rt.block_on(service.node_id_checked())
            .map_err(|why| anyhow::anyhow!("{why}"))?;
        // Nodes of the version before keep actor state in the old layout:
        // refuse to run beside them (upgrades stop every node). Two apps
        // of one tenant share a prefix safely (ADR 0022).
        let older = rt
            .block_on(service.older_nodes())
            .map_err(|e| anyhow::anyhow!("reading the cluster's heartbeats: {e}"))?;
        anyhow::ensure!(
            older.is_empty(),
            "{}.",
            wasmkeep_server::older_nodes_advice(&older)
        );
        if let Some((app_ref, _)) = follow.take() {
            rt.spawn(app_ref.follow(service.clone(), DEPLOY_CHECK_EVERY));
        }
        if let Some((secrets, keys, opened)) = follow_secrets.take() {
            rt.spawn(secrets.follow(
                service.clone(),
                keys,
                opened,
                std::time::Duration::from_secs(3),
                wasmkeep_server::secret_store::STALE_AFTER,
            ));
        }
        // With --app the front is the deployed version's, swapped with its
        // classes: none until a version brings one (ADR 0023).
        if args.app.is_some() {
            app.front = Some(
                HttpComponent::deployed(&service)
                    .with_egress(egress.clone())
                    .with_guest_log(guest_log.clone())
                    .with_cpu_budget(std::time::Duration::from_millis(args.front_cpu_budget_ms))
                    .with_max_requests(front_requests),
            );
        }
        app.actors = Some(service);
        tracing::info!(data_dir = %args.data_dir.display(), "actors enabled");
    }
    if args.usage_ping {
        usage::start(app.actors.clone(), args.node_id.is_some());
    }

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(args.listen)
            .await
            .with_context(|| format!("binding {}", args.listen))?;
        if let Some(http) = &app.front {
            // Not while its required values are not set (it answers 503
            // without running then): it would run without them.
            match http.check_instantiates(app.actors.as_ref()).await {
                Ok(true) => {}
                Ok(false) if http.has_version() => tracing::info!(
                    "the front is not configured yet (required values not set, or the secrets not open): its test instantiation is skipped"
                ),
                Ok(false) => {}
                // A deployed front that links but does not instantiate:
                // the nodes already running serve it (with 500s), so this
                // one starts too, with its actors, rather than exit (ADR
                // 0023). The next deploy replaces it.
                Err(e) if args.app.is_some() => tracing::error!(
                    error = %format!("{:#}", anyhow::Error::from(e)),
                    "the deployed front does not instantiate: its requests will fail until a deploy replaces it"
                ),
                Err(e) => {
                    return Err(anyhow::Error::from(e)
                        .context("test instantiation of the component failed"));
                }
            }
        }
        tracing::info!("listening on http://{}", listener.local_addr()?);
        let metrics = match args.metrics_listen {
            Some(addr) => {
                let metrics = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding {addr} for metrics"))?;
                tracing::info!("metrics on http://{}/metrics", metrics.local_addr()?);
                if !metrics.local_addr()?.ip().is_loopback() {
                    tracing::warn!("metrics are served on a non-loopback address: keep it private");
                }
                Some(metrics)
            }
            None => None,
        };
        let limits = wasmkeep_server::ServeLimits {
            max_connections,
            cluster_connections,
            ..wasmkeep_server::ServeLimits::default()
        };
        let cluster = match args.cluster_listen {
            Some(addr) => {
                let cluster_listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding {addr} for the cluster listener"))?;
                tracing::info!(
                    "cluster listener on https://{} (mutual TLS, other nodes only)",
                    cluster_listener.local_addr()?
                );
                Some(cluster_listener)
            }
            None => None,
        };
        let listeners = wasmkeep_server::Listeners {
            public: listener,
            cluster,
            metrics,
            metrics_hosts: args.metrics_hosts.clone(),
        };
        // The app deleted, or its node id lost (another process found
        // running with it): the process exits with the reason, non-zero.
        if let Some(why) = supervisor.serve(listeners, app, stop_signal, limits).await? {
            anyhow::bail!("{why}");
        }
        Ok(())
    })
}

/// `serve --app`: the app's `current`, waiting for its first deploy when
/// there is none yet (read every `every`, as a running node reads it for
/// new versions), so nodes may start before the app is deployed. The node
/// neither listens nor writes a heartbeat meanwhile. `None` when `stop`
/// resolves first. A failed first read fails (the bucket or its
/// credentials are wrong); a failed read while waiting is retried.
pub(super) async fn first_deploy(
    app_ref: &AppRef,
    app: &str,
    bucket: &str,
    every: std::time::Duration,
    stop: impl std::future::Future<Output = ()>,
) -> anyhow::Result<Option<wasmkeep_server::apps::Current>> {
    if let Some(current) = app_ref.current().await? {
        return Ok(Some(current));
    }
    tracing::info!(
        "waiting for the first deploy of {app} (wasmkeep deploy --app {app} --bucket {bucket} ...): this node starts serving when one lands"
    );
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => return Ok(None),
            () = tokio::time::sleep(every) => {}
        }
        match app_ref.current().await {
            Ok(Some(current)) => return Ok(Some(current)),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "reading the app's current version failed; retrying")
            }
        }
    }
}

fn trace_ratio(value: &str) -> Result<f64, String> {
    let ratio: f64 = value
        .parse()
        .map_err(|_| "trace-sample must be between 0 and 1")?;
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err("trace-sample must be between 0 and 1".into())
    }
}
