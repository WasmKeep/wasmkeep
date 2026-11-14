//! The wasmkeep command-line tool: serve, dev, deploy, cluster, actor, secrets.

#![forbid(unsafe_code)]

mod dev;
mod migrate_cmd;
mod secrets_cmd;
mod status;
mod usage;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmkeep_server::apps::AppRef;
use wasmkeep_server::{ActorConfig, ActorService, App, ClusterConfig, HttpComponent};

#[derive(Debug, Parser)]
#[command(
    name = "wasmkeep",
    version,
    about = "Self-hosted actor platform for Wasm components"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve actors (/actor/<class>/<name>/...) and/or a front: a stateless
    /// wasi:http component (one instance per request) for every other path,
    /// which calls the actors. Over HTTP/1.1.
    Serve(Box<ServeArgs>),
    /// Build actor crates, serve them, and rebuild and hot-swap them on every
    /// source change. Each crate is served as the class named after its
    /// package, at /actor/<class>/<name>/...
    Dev(DevArgs),
    /// Upload actor classes to the bucket and make them the version every
    /// node serving the app runs (`serve --app`). Nodes switch within a few
    /// seconds, each actor at its next event; no actor is dropped.
    Deploy(DeployArgs),
    /// Inspect or steer a cluster through its bucket.
    #[command(subcommand)]
    Cluster(ClusterCommand),
    /// Act on one actor of a cluster, through its bucket.
    #[command(subcommand)]
    Actor(ActorCommand),
    /// An app's secrets, sealed in the bucket (ADR 0016). Nodes serving the
    /// app with `--secrets-key` read them within a few seconds.
    #[command(subcommand)]
    Secrets(secrets_cmd::SecretsCommand),
}

#[derive(Debug, Subcommand)]
enum ActorCommand {
    /// Move an actor to NODE (ADR 0017): its owner hands it over, or NODE
    /// takes it if nobody holds it, within a few seconds. NODE must serve
    /// the same app and not be draining. The actor is not pinned there: once
    /// idle, its next event may claim it elsewhere. The request expires
    /// after 5 minutes.
    Move(MoveArgs),
}

#[derive(Debug, Args)]
struct MoveArgs {
    /// The actor: CLASS/NAME with `--app`, or `default/CLASS/NAME` for
    /// the implicit app `default/default` (the name may contain `/`).
    actor: String,

    /// The actor's app, TENANT/APP; `default/default` (classes given on
    /// the command line) when the actor is written `default/CLASS/NAME`.
    #[arg(long, value_parser = parse_app)]
    app: Option<(String, String)>,

    /// The node to move it to.
    #[arg(long)]
    to: String,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Write the request even though no live node has the id (one about to
    /// start, say).
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct DrainArgs {
    /// The node's id.
    node: String,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Write (or withdraw) the request even though no live node has the id
    /// (and, for `undrain`, no request names it).
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct NodeArgs {
    /// The node's id.
    node: String,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    /// Show the cluster under a bucket prefix, read from the bucket alone
    /// (no node needs to answer): each node's heartbeat (version, app,
    /// addresses, TLS pin, age, live or not) and each app's deployed
    /// version. A heartbeat is live for 10 s, the nodes' own rule with the
    /// default settings, measured on this machine's clock. Exits with 1 if
    /// no node is live.
    Status(StatusArgs),
    /// Ask a node to drain: stop taking actors and hand its own to other
    /// nodes of the same app (ADR 0014). The node keeps running; watch
    /// `cluster status` for `drained`.
    /// Refused, without `--force`, for an id no live node has.
    Drain(DrainArgs),
    /// Withdraw a drain request: the node takes actors again.
    Undrain(DrainArgs),
    /// Fence a node (ADR 0020): its process stops at its next heartbeat
    /// write, and the actors it held are free 15 s after its last one (a
    /// lease's ttl plus grace, the defaults). What revoking a node means;
    /// safe on a live node too (it exits, and its service manager restarts
    /// it as a new process). With no heartbeat object (deleted, the
    /// machine gone), writes a fence so the node's actors are freed 15 s
    /// from now.
    Fence(NodeArgs),
    /// Remove a retired node's heartbeat in the format from before node
    /// leases, once it is older than 25 s (its horizon plus a lease's ttl
    /// and grace). Refuses a recent one (that node may still run) and a
    /// node lease (never deleted: fence it instead).
    Forget(NodeArgs),
    /// Move the actor state of a bucket prefix written by a version before
    /// 0.4.0 into each app's own root, once, while every node is stopped
    /// (ADR 0022). Without --apply, a dry run: prints the plan (the app
    /// each class goes to, the nodes it sees, object counts, an estimate)
    /// and writes nothing. --apply copies, verifies, puts barriers in front
    /// of older binaries, proves no node ran, then commits; it can be run
    /// again after a crash. --rollback undoes it until a node starts.
    /// --remove-old (a dry run; deletes with --apply) later removes the
    /// copies left in the old layout, keeping the tombstones.
    /// Exits with 0 when done, 1 when refused or aborted (nothing new
    /// committed), 2 when some tenants were committed and others not.
    Migrate(migrate_cmd::MigrateArgs),
}

#[derive(Debug, Args)]
struct StatusArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables; read access is enough).
    #[arg(long)]
    bucket: String,

    /// Print JSON instead of tables, for scripts.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct DeployArgs {
    /// The app, as TENANT/APP.
    #[arg(long, value_parser = parse_app)]
    app: (String, String),

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Actor class to deploy, as CLASS=PATH. Repeatable; together they are
    /// the new version's classes (others are removed). Without any, the
    /// current classes are kept (copied inside the bucket).
    #[arg(long = "actor", value_name = "CLASS=PATH", value_parser = parse_actor)]
    actors: Vec<(String, PathBuf)>,

    /// Who may reach a class from outside, as CLASS=MODE (`open`,
    /// `existing`, `existing:NAME,...`, `internal`; see `serve --access`).
    /// Travels with the version to every node. Classes not named keep
    /// their rule; CLASS=open removes it. Repeatable.
    #[arg(long = "access", value_name = "CLASS=MODE", value_parser = parse_access)]
    access: Vec<(String, String)>,

    /// Plain configuration for a class, as CLASS=FILE (`NAME=value` lines,
    /// read with `env.get`). Stored in the bucket in the clear: never
    /// secrets (keep those in `serve --env` files). Replaces the class's
    /// configuration; classes not named keep theirs; CLASS= removes it.
    /// Repeatable.
    #[arg(long = "config", value_name = "CLASS=FILE", value_parser = parse_actor)]
    config: Vec<(String, PathBuf)>,

    /// Names a class requires among its values, as CLASS=NAME,NAME... (ADR
    /// 0016): while one is not set (by the configuration, the app's secrets
    /// or a node's --env), the class refuses its events with 503 instead of
    /// running without it. Part of the version; classes not named keep
    /// theirs; CLASS= removes them.
    #[arg(long = "requires", value_name = "CLASS=NAMES", value_parser = parse_requires)]
    requires: Vec<(String, std::collections::BTreeSet<String>)>,

    /// Go back to the version deployed before the current one, with its
    /// access rules and configuration.
    #[arg(long, conflicts_with_all = ["actors", "access", "config", "requires"])]
    rollback: bool,

    /// Let a rollback make a class more open (refused otherwise).
    #[arg(long, requires = "rollback")]
    allow_reopen: bool,

    /// Seconds an old version stays in the bucket before this command may
    /// delete it (ADR 0012). The current version and the five before it are
    /// always kept, for rollbacks; 0 deletes older ones at once.
    #[arg(long, value_name = "SECS", default_value_t = 3600)]
    version_retention_secs: u64,
}

#[derive(Debug, Args)]
struct DevArgs {
    /// Actor crate directories (each with a Cargo.toml using wasmkeep-sdk).
    #[arg(required = true)]
    crates: Vec<PathBuf>,

    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// Where actor databases live while developing.
    #[arg(long, default_value = ".wasmkeep/dev")]
    data_dir: PathBuf,

    /// How often to check the sources for changes, in milliseconds.
    #[arg(long, default_value_t = 500)]
    poll_ms: u64,

    /// Let actors' outbound HTTP reach this non-public range, as with
    /// `serve` (for a local API: `--allow-outbound 127.0.0.1`). Repeatable.
    #[arg(long = "allow-outbound", value_name = "CIDR")]
    allow_outbound: Vec<wasmkeep_server::IpRange>,

    /// Let actors' outbound HTTP use this port, which is otherwise blocked,
    /// as with `serve` (e.g. 587 for a mail relay). Repeatable.
    #[arg(long = "allow-outbound-port", value_name = "PORT")]
    allow_outbound_port: Vec<u16>,

    /// Lines a second each actor instance may print to the node's log, as
    /// with `serve`; 0 discards what actors print.
    #[arg(long, value_name = "LINES", default_value_t = wasmkeep_server::guest_log::DEFAULT_RATE)]
    guest_log_rate: u32,

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
    outbound_ca: Vec<PathBuf>,

    /// Configuration and secrets for a class, as CLASS=FILE: `NAME=value`
    /// lines its actors read with `env.get` (see docs/concepts.md). Keep the
    /// file readable only by the server's user. Repeatable; later files win.
    #[arg(long = "env", value_name = "CLASS=FILE", value_parser = parse_actor)]
    env: Vec<(String, PathBuf)>,

    /// Who may reach a class from outside, as CLASS=MODE: `open` (the
    /// default: any request, and a new name creates the actor), `existing`
    /// (only actors that exist; other actors create them with
    /// actors.call), `existing:NAME,...` (also lets outside requests create
    /// these names: a class's well-known actors), or `internal` (actors
    /// and alarms only). Repeatable.
    #[arg(long = "access", value_name = "CLASS=MODE", value_parser = parse_access)]
    access: Vec<(String, String)>,
}

#[derive(Debug, Args)]
struct ServeArgs {
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
    data_dir: PathBuf,

    /// Seconds an idle actor stays in memory before hibernating.
    #[arg(long, default_value_t = 30)]
    idle_timeout_secs: u64,

    /// Address to listen on. Defaults to localhost; put a reverse proxy in
    /// front for public traffic.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// Join a cluster as this node (`[a-z0-9-]`, unique per node). Each
    /// actor then runs on one node at a time, chosen by leases in --bucket;
    /// requests reaching another node are forwarded.
    #[arg(long, requires_all = ["bucket", "advertise"])]
    node_id: Option<String>,

    /// Serve the classes deployed for this app (TENANT/APP) with `wasmkeep
    /// deploy`, following new deploys. Needs --bucket; replaces --actor.
    #[arg(long, value_parser = parse_app, requires = "bucket", conflicts_with = "actors")]
    app: Option<(String, String)>,

    /// URL the other nodes reach this one at, e.g. http://10.0.0.11:8080.
    #[arg(long, requires = "node_id")]
    advertise: Option<String>,

    /// Serve the cluster listener on this address (e.g. `0.0.0.0:7443`):
    /// the port other nodes use to reach this one, with mutual TLS 1.3
    /// (each node's certificate is made in memory at start and pinned in
    /// its heartbeat; nothing to provision). Needs --cluster-advertise and
    /// the cluster flags. Only other nodes need to reach it. A cluster node
    /// needs it unless --cluster-plaintext accept.
    #[arg(long, value_name = "ADDR", requires_all = ["cluster_advertise", "node_id"])]
    cluster_listen: Option<SocketAddr>,

    /// URL the other nodes reach the cluster listener at, e.g.
    /// https://10.0.0.11:7443 (needs --cluster-listen).
    #[arg(long, value_name = "URL", requires = "cluster_listen", value_parser = parse_cluster_advertise)]
    cluster_advertise: Option<String>,

    /// Plain HTTP between nodes. `refuse` (the default): node calls count
    /// only on the cluster port (a signed call on --listen is treated as
    /// coming from outside), and peers without a cluster listener are not
    /// used (their actors get 503 here); a cluster node then needs
    /// --cluster-listen. `accept`: peers without a cluster listener are
    /// reached over plain HTTP, and other nodes' calls are honoured on
    /// --listen too; only on a network nobody else can reach, or while
    /// moving a cluster to TLS one node at a time.
    #[arg(long, value_enum, value_name = "MODE", default_value = "refuse")]
    cluster_plaintext: ClusterPlaintext,

    /// Seconds an actor's old history stays in the bucket before garbage
    /// collection may delete it (ADR 0012); 0 deletes it as soon as the
    /// rule allows. A deleted actor's history goes at once whatever this
    /// is.
    #[arg(long, value_name = "SECS", default_value_t = 3600)]
    history_retention_secs: u64,

    /// Seconds a request for an actor whose node died or is stopping is
    /// kept until another node can take the actor over, instead of a 503
    /// at once: a GET or HEAD without a body, or a WebSocket upgrade,
    /// arriving from outside. By default what a takeover can take, from
    /// the lease timing (25 s); 0 answers at once, as before. A load
    /// balancer's server timeout must be longer (see docs/cluster.md).
    #[arg(long, value_name = "SECS")]
    failover_wait_secs: Option<u64>,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX]. Credentials,
    /// region and endpoint come from the AWS_* environment variables
    /// (AWS_ENDPOINT for MinIO or R2, AWS_ALLOW_HTTP=true for plain http).
    #[arg(long)]
    bucket: Option<String>,

    /// Directory for pre-compiled components.
    #[arg(long, default_value = ".wasmkeep/cache")]
    cache_dir: PathBuf,

    /// Maximum concurrently live instances (pooling allocator slots).
    #[arg(long, default_value_t = RuntimeConfig::default().max_instances)]
    max_instances: u32,

    /// Guest CPU time allowed per actor event, in milliseconds. Time spent
    /// waiting on host I/O does not count. The front has its own,
    /// --front-cpu-budget-ms.
    #[arg(long, default_value_t = 50)]
    cpu_budget_ms: u64,

    /// CPU time one event's SQL statements may use together, in
    /// milliseconds (SQLite runs in the host, outside --cpu-budget-ms).
    #[arg(long, default_value_t = 2000)]
    sql_cpu_budget_ms: u64,

    /// Send an anonymous usage report (version, OS, cluster or not, and
    /// bucketed counts of classes and resident actors) a minute after start
    /// and daily. Off by default; see docs/telemetry.md for every field.
    #[arg(long)]
    usage_ping: bool,

    /// Serve Prometheus metrics at `http://ADDR/metrics` (e.g.
    /// `127.0.0.1:9100`). Off by default. The page names classes and
    /// counts: bind it to a private address.
    #[arg(long, value_name = "ADDR")]
    metrics_listen: Option<SocketAddr>,

    /// Let actors' outbound HTTP reach this non-public range (`10.0.0.0/8`,
    /// `127.0.0.1`, `fd00::/8`). Repeatable. By default only public
    /// addresses are reachable: never loopback, private networks or cloud
    /// metadata endpoints.
    #[arg(long = "allow-outbound", value_name = "CIDR")]
    allow_outbound: Vec<wasmkeep_server::IpRange>,

    /// Let actors' outbound HTTP use this port. By default the ports
    /// browsers block (the Fetch standard's list: mail such as 25, 465 and
    /// 587, FTP, SSH, IRC and other non-HTTP services) are refused, since a
    /// crafted HTTP request can drive some of those servers. Repeatable;
    /// e.g. 587 for a mail relay the actors call over HTTP.
    #[arg(long = "allow-outbound-port", value_name = "PORT")]
    allow_outbound_port: Vec<u16>,

    /// Lines a second each actor instance (and each front request) may
    /// print to the node's log, with bursts of twice that; past it, lines
    /// are dropped and counted. What guests write to stdout and stderr is
    /// logged as target `wasmkeep::guest` (filter it with RUST_LOG). 0
    /// discards it.
    #[arg(long, value_name = "LINES", default_value_t = wasmkeep_server::guest_log::DEFAULT_RATE)]
    guest_log_rate: u32,

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
    outbound_ca: Vec<PathBuf>,

    /// Client connections open at once; more wait until one closes.
    #[arg(long, default_value_t = wasmkeep_server::ServeLimits::default().max_connections)]
    max_connections: usize,

    /// Configuration and secrets for a class, as CLASS=FILE: `NAME=value`
    /// lines its actors read with `env.get` (see docs/concepts.md). Keep the
    /// file readable only by the server's user. Repeatable; later files win.
    #[arg(long = "env", value_name = "CLASS=FILE", value_parser = parse_actor)]
    env: Vec<(String, PathBuf)>,

    /// The front's configuration and secrets on this node: `NAME=value`
    /// lines it reads with `env.get`, as --env gives a class's. Repeatable;
    /// later files win.
    #[arg(long = "front-env", value_name = "FILE")]
    front_env: Vec<PathBuf>,

    /// Front requests running at once on this node; past it, 503 with
    /// `Retry-After: 1`. Their instances are kept from the pool: resident
    /// actors get the rest of --max-instances. By default a tenth of
    /// --max-instances, rounded up.
    #[arg(long, value_name = "N")]
    max_front_requests: Option<usize>,

    /// The front's CPU time per request, in milliseconds (apart from
    /// --cpu-budget-ms, the actors'). Past it, the request answers 500.
    #[arg(long, value_name = "MS", default_value_t = 50)]
    front_cpu_budget_ms: u64,

    /// The app's secrets key (`wasmkeep secrets new-key`), with `--app`:
    /// secrets set with `wasmkeep secrets` reach `env.get`, over the deployed
    /// configuration and under `--env` (ADR 0016). Given twice during a key
    /// change. Readable by its owner only.
    #[arg(long = "secrets-key", value_name = "FILE", requires = "app")]
    secrets_keys: Vec<PathBuf>,

    /// With `--app`, run without the app's secrets from the bucket (`--env`
    /// only). Without it and without `--secrets-key`, a node refuses to
    /// start once the app has secrets.
    #[arg(long, requires = "app", conflicts_with = "secrets_keys")]
    no_secrets: bool,

    /// Who may reach a class from outside, as CLASS=MODE: `open` (the
    /// default: any request, and a new name creates the actor), `existing`
    /// (only actors that exist; other actors create them with
    /// actors.call), `existing:NAME,...` (also lets outside requests create
    /// these names: a class's well-known actors), or `internal` (actors
    /// and alarms only). Repeatable. With --app, rules come from the deploy
    /// (`wasmkeep deploy --access`); this is refused once rules are deployed.
    #[arg(long = "access", value_name = "CLASS=MODE", value_parser = parse_access)]
    access: Vec<(String, String)>,
}

/// `--transparent-huge-pages`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum HugePages {
    Off,
    System,
}

/// Turns transparent huge pages off for this process (Linux), unless the
/// operator keeps the system's setting. Called before the runtime
/// allocates anything; inherited by the whole process.
fn huge_pages(mode: HugePages) {
    if mode == HugePages::System {
        return;
    }
    #[cfg(target_os = "linux")]
    if let Err(e) = rustix::thread::disable_transparent_huge_pages(true) {
        tracing::warn!(error = %e, "could not turn transparent huge pages off for this process");
    }
}

/// `--cluster-plaintext`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ClusterPlaintext {
    Accept,
    Refuse,
}

/// Checks `--cluster-advertise` is an `https://` address with a host (the
/// rest is checked when the node starts).
fn parse_cluster_advertise(s: &str) -> Result<String, String> {
    let host = s.strip_prefix("https://").unwrap_or_default();
    if host.is_empty() || host.starts_with(['/', ':']) {
        return Err(format!(
            "expected the https:// URL other nodes reach the cluster listener at \
             (https://10.0.0.11:7443), got {s:?}"
        ));
    }
    Ok(s.to_owned())
}

/// Calls between nodes travel in plain HTTP (signed, not encrypted): warn
/// when the advertised address is a public IP, where others may see them.
fn warn_if_public(advertise: &str) {
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

/// The outbound policy: public addresses plus `allow`, public CAs plus the
/// `--outbound-ca` files.
fn egress_policy(
    allow: Vec<wasmkeep_server::IpRange>,
    ports: Vec<u16>,
    cas: &[PathBuf],
) -> anyhow::Result<wasmkeep_server::EgressPolicy> {
    let mut roots = Vec::new();
    for path in cas {
        let certs = wasmkeep_server::egress::read_ca_file(path)
            .map_err(|e| anyhow::anyhow!("--outbound-ca: {e}"))?;
        tracing::info!(file = %path.display(), certificates = certs.len(), "outbound HTTPS also trusts");
        roots.extend(certs);
    }
    let blocked: Vec<u16> = ports
        .iter()
        .copied()
        .filter(|p| !wasmkeep_server::egress::BAD_PORTS.contains(p))
        .collect();
    if !blocked.is_empty() {
        tracing::warn!(ports = ?blocked, "--allow-outbound-port: these ports are not blocked anyway");
    }
    Ok(wasmkeep_server::EgressPolicy::allowing(allow)
        .allowing_ports(ports)
        .trusting(roots))
}

/// Reads the `--env` files, refusing one for a class that is not served (a
/// typo would otherwise leave the class without its secrets), or only
/// warning about it when not `strict` (`--app`: a deploy may have removed
/// the class, and may bring it back).
pub(crate) fn load_env(
    files: &[(String, PathBuf)],
    served: &[&str],
    strict: bool,
) -> anyhow::Result<wasmkeep_server::ActorEnv> {
    let mut env = wasmkeep_server::ActorEnv::default();
    for (class, path) in files {
        anyhow::ensure!(
            class != "_front",
            "--env {class}=...: --env takes classes; give the front's values with --front-env FILE"
        );
        if !served.contains(&class.as_str()) {
            anyhow::ensure!(
                !strict,
                "--env {class}=...: no class {class} is served (classes: {})",
                served.join(", ")
            );
            tracing::warn!(%class, "--env names a class the deployed version does not have");
        }
        let (vars, warnings) =
            wasmkeep_server::env::read_file(path).map_err(|e| anyhow::anyhow!("--env: {e}"))?;
        for w in warnings {
            tracing::warn!("{w}");
        }
        env.extend(class, vars);
    }
    for (class, n) in env.summary() {
        tracing::info!(%class, values = n, "env loaded");
    }
    Ok(env)
}

/// The front's values from `--front-env` files, later files winning.
fn load_front_env(files: &[PathBuf]) -> anyhow::Result<wasmkeep_server::env::Vars> {
    let mut vars = wasmkeep_server::env::Vars::new();
    for path in files {
        let (more, warnings) = wasmkeep_server::env::read_file(path)
            .map_err(|e| anyhow::anyhow!("--front-env: {e}"))?;
        for w in warnings {
            tracing::warn!("{w}");
        }
        vars.extend(more);
    }
    if !files.is_empty() {
        tracing::info!(values = vars.len(), "front env loaded");
    }
    Ok(vars)
}

/// `--max-front-requests`, or its default: a tenth of the pool, rounded
/// up. A value of 0, or one that leaves actors no instance, is refused.
fn front_requests(given: Option<usize>, pool: usize, actors: bool) -> anyhow::Result<usize> {
    let Some(n) = given else {
        return Ok(wasmkeep_server::fds::default_front_requests(pool).max(1));
    };
    anyhow::ensure!(n > 0, "--max-front-requests must be at least 1");
    if actors {
        anyhow::ensure!(
            n < pool,
            "--max-front-requests {n} leaves actors no room in --max-instances {pool}: \
             give fewer front requests or more instances"
        );
    } else {
        anyhow::ensure!(
            n <= pool,
            "--max-front-requests {n} is more than --max-instances {pool}"
        );
    }
    Ok(n)
}

fn parse_access(s: &str) -> Result<(String, String), String> {
    let (class, mode) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=MODE, got {s:?}"))?;
    // Checked here so a bad mode fails at parse time, with clap's message.
    wasmkeep_server::AccessRules::default().apply(class, mode)?;
    Ok((class.to_owned(), mode.to_owned()))
}

/// The `--access` rules, refusing one for a class that is not served (a
/// typo would otherwise leave the class open).
pub(crate) fn access_rules(
    rules: &[(String, String)],
    served: &[&str],
) -> anyhow::Result<wasmkeep_server::AccessRules> {
    let mut out = wasmkeep_server::AccessRules::default();
    for (class, mode) in rules {
        anyhow::ensure!(
            served.contains(&class.as_str()),
            "--access {class}=...: no class {class} is served (classes: {})",
            served.join(", ")
        );
        out.apply(class, mode)
            .map_err(|e| anyhow::anyhow!("--access {class}: {e}"))?;
    }
    for (class, mode) in out.restricted() {
        tracing::info!(%class, access = %mode, "access restricted");
    }
    Ok(out)
}

/// Warnings for configuration that looks like a secret (stored in the
/// clear): names and the variable only, never a value.
pub(crate) fn secret_looking(class: &str, vars: &wasmkeep_server::env::Vars) -> Vec<String> {
    const WORDS: [&str; 8] = [
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "PASS",
        "KEY",
        "CREDENTIAL",
        "AUTH",
        "DSN",
    ];
    let mut out = Vec::new();
    for (name, value) in vars {
        let upper = name.to_ascii_uppercase();
        let by_name = WORDS.iter().any(|w| upper.contains(w));
        // A URL with a user and password: scheme://user:pass@host
        let by_value = value.split_once("://").is_some_and(|(_, rest)| {
            let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
            authority
                .rsplit_once('@')
                .is_some_and(|(userinfo, _)| userinfo.contains(':'))
        });
        if by_name || by_value {
            out.push(format!(
                "--config {class}: {name} looks like a secret; configuration is stored in the \
                 bucket in the clear (keep secrets in `serve --env` files on each node)"
            ));
        }
    }
    out
}

/// Warnings about a deploy's changes: a class removed with a rule, and a
/// class added without one at the same time (a rename leaves the new class
/// open).
pub(crate) fn deploy_warnings(diff: &wasmkeep_server::apps::Diff) -> Vec<String> {
    let mut out = Vec::new();
    // A renamed class loses its required values (ADR 0016) as it loses
    // its rule.
    let dropped: Vec<&str> = diff
        .requires
        .iter()
        .filter(|l| l.ends_with("(class removed)"))
        .filter_map(|l| l.split(':').next())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if !dropped.is_empty() && !diff.added.is_empty() {
        out.push(format!(
            "{} had required values and was removed while {} was added: if it was \
             renamed, give the new class its --requires",
            dropped.join(", "),
            diff.added.join(", ")
        ));
    }
    let restricted: Vec<&(String, Option<String>)> =
        diff.removed.iter().filter(|(_, r)| r.is_some()).collect();
    if restricted.is_empty() {
        return out;
    }
    let ruled: Vec<&str> = diff.rules.iter().map(|(c, _, _)| c.as_str()).collect();
    for added in &diff.added {
        if !ruled.contains(&added.as_str()) {
            out.push(format!(
                "class {added} was added and is open, while {} was removed with a rule: \
                 if it was renamed, give the new class its rule (--access {added}=...)",
                restricted
                    .iter()
                    .map(|(c, _)| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    out
}

/// The rules a node following an app starts with (ADR 0013): the deployed
/// version's, once rules have been deployed (`marked`; `--access` is then
/// refused); before that, `--access` if given (the migration's first step),
/// with a warning either way.
pub(crate) fn serve_app_rules(
    flags: &[(String, String)],
    served: &[&str],
    marked: bool,
    deployed: wasmkeep_server::AccessRules,
    tenant: &str,
    app: &str,
) -> anyhow::Result<wasmkeep_server::AccessRules> {
    if marked {
        if !flags.is_empty() {
            let given: Vec<String> = flags
                .iter()
                .map(|(c, m)| format!("--access {c}={m}"))
                .collect();
            anyhow::bail!(
                "--access cannot be used with --app: this app's access rules come with its \
                 deploys (ADR 0013). If these rules are not deployed yet, deploy them once:\n  \
                 wasmkeep deploy --app {tenant}/{app} --bucket ... {}\n\
                 then start this node without --access.",
                given.join(" ")
            );
        }
        if deployed.restricted().is_empty() {
            tracing::warn!(
                "no class of this app has a deployed access rule: every class is open. \
                 Deploy rules with `wasmkeep deploy --access CLASS=MODE`"
            );
        }
        for (class, mode) in deployed.restricted() {
            tracing::info!(%class, access = %mode, "access restricted (deployed)");
        }
        return Ok(deployed);
    }
    if flags.is_empty() {
        tracing::warn!(
            "this app has never deployed access rules: every class is open. \
             Deploy them with `wasmkeep deploy --access CLASS=MODE`"
        );
        return Ok(deployed);
    }
    tracing::warn!(
        "--access with --app is accepted until access rules are deployed, then ignored: \
         deploy them with `wasmkeep deploy --access`, then remove the flag"
    );
    access_rules(flags, served)
}

fn parse_actor(s: &str) -> Result<(String, PathBuf), String> {
    let (class, path) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=PATH, got {s:?}"))?;
    Ok((class.to_owned(), PathBuf::from(path)))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve(*args),
        Command::Deploy(args) => deploy(args),
        Command::Cluster(ClusterCommand::Status(args)) => cluster_status(args),
        Command::Cluster(ClusterCommand::Drain(args)) => cluster_drain(&args, true),
        Command::Cluster(ClusterCommand::Undrain(args)) => cluster_drain(&args, false),
        Command::Cluster(ClusterCommand::Fence(args)) => cluster_fence(&args),
        Command::Cluster(ClusterCommand::Forget(args)) => cluster_forget(&args),
        Command::Cluster(ClusterCommand::Migrate(args)) => {
            let code = migrate_cmd::run(&args)?;
            if code != 0 {
                use std::io::Write as _;
                let _ = std::io::stdout().flush();
                std::process::exit(code);
            }
            Ok(())
        }
        Command::Actor(ActorCommand::Move(args)) => actor_move(&args),
        Command::Secrets(command) => secrets_cmd::run(command),
        Command::Dev(args) => dev::run(dev::DevOptions {
            crates: args.crates,
            listen: args.listen,
            data_dir: args.data_dir,
            poll: std::time::Duration::from_millis(args.poll_ms),
            egress: egress_policy(
                args.allow_outbound,
                args.allow_outbound_port,
                &args.outbound_ca,
            )?,
            guest_log: wasmkeep_server::GuestLog::new(args.guest_log_rate),
            no_huge_pages: args.transparent_huge_pages == HugePages::Off,
            env: args.env,
            access: args.access,
        }),
    }
}

fn serve(args: ServeArgs) -> anyhow::Result<()> {
    huge_pages(args.transparent_huge_pages);
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
    let front_requests = front_requests(args.max_front_requests, pool, serves_actors)?;
    // Every resident actor holds three open files: size the caps to the
    // descriptors this process may open (raising the soft limit first).
    let wanted_resident = wasmkeep_server::fds::resident_limit(pool, front_requests);
    // The cluster port, when served, takes its share first.
    let mut cluster_connections = wasmkeep_server::fds::CLUSTER_CONNECTIONS;
    let (max_connections, resident_cap) = match wasmkeep_server::fds::raise_open_files_limit(
        1 << 20,
    ) {
        Some(limit) => {
            let mut rest = limit;
            if args.cluster_listen.is_some() {
                (cluster_connections, rest) = wasmkeep_server::fds::reserve_cluster_port(limit);
            }
            let (connections, actors) =
                wasmkeep_server::fds::budget(rest, args.max_connections, wanted_resident);
            if connections < args.max_connections
                || actors < wanted_resident
                || cluster_connections < wasmkeep_server::fds::CLUSTER_CONNECTIONS
            {
                tracing::warn!(
                    open_files = limit,
                    max_connections = connections,
                    resident_actors = actors,
                    cluster_connections = args.cluster_listen.map(|_| cluster_connections),
                    "the open-files limit is low: connections and resident actors are capped to fit; raise it (ulimit -n, LimitNOFILE=) to serve more"
                );
            }
            (connections, actors)
        }
        None => (args.max_connections, usize::MAX),
    };
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

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
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
    // One for the node: guests share its node-wide line budget.
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
        let mut config = ActorConfig::new(&args.data_dir);
        // With --app: whether rules were ever deployed (ADR 0013), and the
        // version's own.
        let mut deployed_rules = None;
        config.egress = egress.clone();
        config.guest_log = guest_log.clone();
        config.storage.max_sql_cpu_per_event =
            std::time::Duration::from_millis(args.sql_cpu_budget_ms);
        if let (Some((tenant, name)), Some(bucket)) = (&args.app, &args.bucket) {
            let (store, prefix) = open_bucket(bucket)?;
            // Before anything reads or writes the app's state (ADR 0022).
            check_layout(bucket, &store, &prefix, tenant)?;
            let app_store = std::sync::Arc::clone(&store);
            let app_ref = AppRef::new(store, &prefix, tenant, name);
            let (current, deployed) = rt.block_on(async {
                let current = app_ref.current().await?.ok_or_else(|| {
                    anyhow::anyhow!("nothing is deployed for {tenant}/{name} yet")
                })?;
                let deployed = app_ref.version(&current.version).await?;
                anyhow::Ok((current, deployed))
            })?;
            // A `current` from before 0.4.0 (no token 3): its rules may still
            // be `--access` ones, which a deploy of this version would drop
            // at run time. Refuse it at start instead (ADR 0022).
            require_rooted(&current, tenant, name, bucket)?;
            deployed_rules = Some((current.marked, deployed.access));
            config.config = deployed.config;
            config.requires = deployed.requires;
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
            None => config.access = access_rules(&args.access, &served)?,
            Some((marked, deployed)) => {
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
        config.mailbox.max_resident = resident_cap;
        config.max_front_requests = Some(front_requests);
        config.front_env = front_env.clone();
        if let (Some(node_id), Some(advertise), Some(bucket)) =
            (&args.node_id, &args.advertise, &args.bucket)
        {
            let (store, prefix) = open_bucket(bucket)?;
            let mut cluster = ClusterConfig::new(node_id, advertise, store);
            cluster.prefix = prefix;
            cluster.history_retention = std::time::Duration::from_secs(args.history_retention_secs);
            cluster.failover_wait = args.failover_wait_secs.map(std::time::Duration::from_secs);
            // Three quarters of the connections stay for everything else.
            cluster.failover_wait_max = max_connections / 4;
            cluster.reads_secrets = !args.no_secrets;
            if let Some((tenant, name)) = &args.app {
                cluster.app = format!("{tenant}/{name}");
            }
            if let Some(cluster_advertise) = &args.cluster_advertise {
                cluster.cluster_tls = Some(wasmkeep_server::ClusterTlsConfig::new(
                    cluster_advertise.clone(),
                ));
            }
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
            rt.spawn(app_ref.follow(service.clone(), std::time::Duration::from_secs(3)));
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
            http.instantiate_once()
                .await
                .context("test instantiation of the component failed")?;
        }
        tracing::info!("listening on http://{}", listener.local_addr()?);
        if let Some(addr) = args.metrics_listen {
            let metrics = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding {addr} for metrics"))?;
            tracing::info!("metrics on http://{}/metrics", metrics.local_addr()?);
            if !metrics.local_addr()?.ip().is_loopback() {
                tracing::warn!("metrics are served on a non-loopback address: keep it private");
            }
            let actors = app.actors.clone();
            tokio::spawn(async move {
                let _ = wasmkeep_server::serve_metrics(metrics, actors, stop_signal()).await;
            });
        }
        let limits = wasmkeep_server::ServeLimits {
            max_connections,
            cluster_connections,
            ..wasmkeep_server::ServeLimits::default()
        };
        // A second process found running with this node id (both started
        // at the same moment) stops this one too: it takes no more leases,
        // and shuts down like a SIGTERM, then exits with the reason.
        let lost: std::sync::Arc<std::sync::Mutex<Option<String>>> = std::sync::Arc::default();
        let actors = app.actors.clone();
        let lost_reason = std::sync::Arc::clone(&lost);
        let shutdown = async move {
            let node_id_lost = async {
                match &actors {
                    Some(a) => a.node_id_lost().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = stop_signal() => tracing::info!("shutting down"),
                why = node_id_lost => {
                    tracing::error!("shutting down: {why}");
                    *lost_reason.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(why);
                }
            }
        };
        match args.cluster_listen {
            Some(addr) => {
                let cluster_listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding {addr} for the cluster listener"))?;
                tracing::info!(
                    "cluster listener on https://{} (mutual TLS, other nodes only)",
                    cluster_listener.local_addr()?
                );
                wasmkeep_server::serve_with_cluster_listener(
                    listener,
                    cluster_listener,
                    app,
                    shutdown,
                    limits,
                )
                .await?;
            }
            None => wasmkeep_server::serve_with(listener, app, shutdown, limits).await?,
        }
        if let Some(why) = lost.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            anyhow::bail!("{why}");
        }
        Ok(())
    })
}

/// Refuses to follow a `current` from before 0.4.0 (no token 3, ADR
/// 0022): its nodes may still take their rules from `--access`, which the
/// next deploy of this version (token 3 marks the app) would drop at run
/// time, opening the classes. Refused at start instead, with what to do.
fn require_rooted(
    current: &wasmkeep_server::apps::Current,
    tenant: &str,
    name: &str,
    bucket: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        current.rooted,
        "{tenant}/{name} was last deployed by a version before 0.4.0: if this bucket prefix \
         was not migrated yet, run `wasmkeep cluster migrate --bucket {bucket}` (with --apply); \
         if it was, deploy {tenant}/{name} once with this version. Then start this node"
    );
    Ok(())
}

/// Resolves on Ctrl-C (SIGINT) or SIGTERM, the signal service managers
/// (systemd, Docker, Kubernetes) send to stop a process.
pub(crate) async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM; only Ctrl-C stops gracefully");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// `wasmkeep deploy`: validate, upload, switch (or roll back).
fn deploy(args: DeployArgs) -> anyhow::Result<()> {
    let (tenant, name) = &args.app;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let app_ref = AppRef::new(store, &prefix, tenant, name);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let named = |e: wasmkeep_server::apps::DeployError| {
        anyhow::anyhow!(e.to_string().replace("<this bucket>", &args.bucket))
    };
    let deployed = if args.rollback {
        rt.block_on(app_ref.rollback(args.allow_reopen))
            .map_err(named)?
    } else {
        anyhow::ensure!(
            !(args.actors.is_empty()
                && args.access.is_empty()
                && args.config.is_empty()
                && args.requires.is_empty()),
            "nothing to deploy: give --actor, --access, --config or --requires (or --rollback)"
        );
        let runtime = Runtime::new(&RuntimeConfig::default())?;
        let mut classes = Vec::new();
        for (class, path) in &args.actors {
            let wasm =
                std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
            classes.push((class.clone(), wasm));
        }
        let mut config = Vec::new();
        for (class, path) in &args.config {
            if path.as_os_str().is_empty() {
                config.push((class.clone(), None));
                continue;
            }
            let (vars, _) = wasmkeep_server::env::read_file(path)
                .map_err(|e| anyhow::anyhow!("--config: {e}"))?;
            for warning in secret_looking(class, &vars) {
                eprintln!("warning: {warning}");
            }
            config.push((class.clone(), Some(vars)));
        }
        let changes = wasmkeep_server::apps::Changes {
            classes: (!classes.is_empty()).then_some(classes),
            access: args.access.clone(),
            config,
            requires: {
                let mut seen = std::collections::BTreeSet::new();
                for (class, _) in &args.requires {
                    anyhow::ensure!(
                        seen.insert(class.clone()),
                        "--requires names {class} twice: give its names once, as {class}=A,B"
                    );
                }
                args.requires.clone()
            },
        };
        rt.block_on(app_ref.deploy_changes(&runtime, changes))
            .map_err(named)?
    };
    for line in deployed.diff.lines() {
        println!("  {line}");
    }
    for warning in deploy_warnings(&deployed.diff) {
        eprintln!("warning: {warning}");
    }
    if deployed.newly_marked {
        println!(
            "Access rules now come with deploys: nodes ignore --access from now on, \
             and refuse it at their next start. Every class not listed is open."
        );
    }
    let current = deployed.current;
    println!(
        "{tenant}/{name}: version {} is current (deploy {})",
        current.version, current.seq
    );
    // Old versions beyond what rollbacks need (ADR 0012). The deploy is
    // done either way: a failure here only leaves them for next time.
    let retention = std::time::Duration::from_secs(args.version_retention_secs);
    match rt.block_on(app_ref.collect_versions(wasmkeep_server::apps::KEEP_PREVIOUS, retention)) {
        Ok(0) => {}
        Ok(n) => println!("removed {n} old version(s) from the bucket"),
        Err(e) => {
            eprintln!("warning: removing old versions failed (the next deploy tries again): {e}")
        }
    }
    Ok(())
}

/// `wasmkeep cluster status`: prints the report; exits with 1 if no node is
/// live.
/// The heartbeat of the live node with `id`, if one has it: live as
/// `cluster status` counts it (the nodes' own horizon, this machine's
/// clock).
async fn live_node(
    keys: &wasmkeep_cluster::object_store::ObjectStoreBucket,
    id: &str,
) -> anyhow::Result<Option<wasmkeep_cluster::membership::NodeInfo>> {
    use wasmkeep_cluster::membership;
    let horizon_ms = wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_server::LeaseConfig::default_safe(),
    );
    let now = status::now_ms();
    Ok(membership::all_heartbeats(keys)
        .await
        .map_err(|e| anyhow::anyhow!("listing the heartbeats: {e}"))?
        .into_iter()
        .find(|n| n.id == id && membership::is_live(n, now, horizon_ms)))
}

/// Writes or deletes `control/drain/<node>` (ADR 0014). Without `--force`,
/// a drain needs a live node with the id (a request for an id no node uses
/// stays in `control/` until an undrain with the same id), and an undrain a
/// live node or a request to withdraw.
fn cluster_drain(args: &DrainArgs, drain: bool) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::{self, Keys as _};
    let key = control::drain_key(&args.node)?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    if !args.force && rt.block_on(live_node(&keys, &args.node))?.is_none() {
        let id = &args.node;
        if drain {
            anyhow::bail!(
                "no live node has the id {id} (see `wasmkeep cluster status`): a drain request \
                 for an id no node uses stays in the bucket until an undrain with the same id. \
                 Pass --force to write it anyway"
            );
        }
        let requested = rt
            .block_on(control::read(&keys, None, status::now_ms()))
            .map_err(|e| anyhow::anyhow!("reading the drain requests: {e}"))?
            .draining
            .contains(id);
        anyhow::ensure!(
            requested,
            "neither a live node nor a drain request has the id {id}: nothing to withdraw \
             (see `wasmkeep cluster status`). Pass --force to delete the request key anyway"
        );
    }
    if drain {
        rt.block_on(keys.put_empty(&key))?;
        println!(
            "Asked {} to drain; it reports `draining` within a few seconds (wasmkeep cluster status).",
            args.node
        );
    } else {
        rt.block_on(keys.delete(&key))?;
        println!("Withdrew the drain request for {}.", args.node);
    }
    Ok(())
}

/// `wasmkeep cluster fence` (ADR 0020, section 2).
fn cluster_fence(args: &NodeArgs) -> anyhow::Result<()> {
    use wasmkeep_cluster::node_lease::{OperatorFence, operator_fence};
    wasmkeep_cluster::membership::validate_node_id(&args.node)
        .map_err(|e| anyhow::anyhow!("invalid node id {:?}: {e}", args.node))?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let lease = wasmkeep_server::LeaseConfig::default_safe();
    let key = format!("nodes/{}", args.node);
    let mut outcome = rt
        .block_on(operator_fence(&bucket, &status::Wall, &key))
        .map_err(|e| anyhow::anyhow!("fencing {}: {e:?}", args.node))?;
    // A fence created here carries this machine's clock, which the free
    // rule trusts within the allowed skew: checked against the bucket's own
    // time for that write (rounded up to its second, plus the skew), and
    // moved later if this clock runs behind it.
    if let OperatorFence::Fenced {
        written_at_ms,
        created: true,
        ..
    } = &mut outcome
    {
        let modified = rt
            .block_on(bucket.modified_ms(&key))
            .map_err(|e| anyhow::anyhow!("reading the fence back: {e:?}"))?
            .ok_or_else(|| anyhow::anyhow!("the fence vanished right after it was written"))?;
        let bucket_ms = modified + 1_000 + lease.max_skew_ms();
        if bucket_ms > *written_at_ms {
            if bucket_ms - *written_at_ms > 1_000 + 2 * lease.max_skew_ms() {
                eprintln!(
                    "warning: this machine's clock is {:.1} s behind the bucket's; the fence uses the bucket's time",
                    (bucket_ms - *written_at_ms) as f64 / 1000.0
                );
            }
            rt.block_on(wasmkeep_cluster::node_lease::raise_created_fence(
                &bucket, &key, bucket_ms,
            ))
            .map_err(|e| anyhow::anyhow!("moving the fence to the bucket's time: {e:?}"))?;
            *written_at_ms = bucket_ms;
        }
    }
    let free_at = |at: u64| {
        let free = at
            .saturating_add(lease.ttl_ms())
            .saturating_add(lease.grace_ms());
        let left = free.saturating_sub(status::now_ms());
        format!("{:.1} s from now", left as f64 / 1000.0)
    };
    match outcome {
        OperatorFence::Fenced {
            written_at_ms,
            created: false,
            ..
        } => println!(
            "Fenced {}: it stops at its next write; its actors are free {} (its last write plus 15 s).",
            args.node,
            free_at(written_at_ms)
        ),
        OperatorFence::Fenced {
            written_at_ms,
            created: true,
            ..
        } => println!(
            "{} had no heartbeat object: wrote a fence; its actors are free {}, and a process still running under that id stops at its next write.",
            args.node,
            free_at(written_at_ms)
        ),
        OperatorFence::NotLive {
            state,
            written_at_ms,
        } => println!(
            "{} is already {}; its actors are free {}.",
            args.node,
            state.as_str(),
            free_at(written_at_ms)
        ),
        OperatorFence::OldFormat => anyhow::bail!(
            "{}'s heartbeat is in the format from before node leases: that node would not read a fence. Stop it; once its heartbeat is stale, `wasmkeep cluster forget {}` removes it",
            args.node,
            args.node
        ),
    }
    Ok(())
}

/// `wasmkeep cluster forget` (ADR 0020, section 2).
fn cluster_forget(args: &NodeArgs) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::Keys as _;
    use wasmkeep_cluster::lease::Bucket as _;
    wasmkeep_cluster::membership::validate_node_id(&args.node)
        .map_err(|e| anyhow::anyhow!("invalid node id {:?}: {e}", args.node))?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let key = format!("nodes/{}", args.node);
    let Some((body, _)) = rt
        .block_on(bucket.get(&key))
        .map_err(|e| anyhow::anyhow!("reading {key}: {e:?}"))?
    else {
        println!("{} has no heartbeat object: nothing to forget.", args.node);
        return Ok(());
    };
    status::may_forget(&body, status::now_ms()).map_err(|why| anyhow::anyhow!("{why}"))?;
    // Read again right before the delete (a store's DELETE takes no
    // condition): a new node's first write over the stale heartbeat in
    // between must not be deleted. The window left is the time between the
    // two requests.
    let again = rt
        .block_on(bucket.get(&key))
        .map_err(|e| anyhow::anyhow!("reading {key}: {e:?}"))?
        .map(|(b, _)| b);
    if again.as_deref() != Some(body.as_slice()) {
        anyhow::bail!("{key} changed while it was checked: left alone; run the command again");
    }
    rt.block_on(bucket.delete(&key))?;
    println!("Removed {}'s retired heartbeat.", args.node);
    Ok(())
}

/// Writes the actor's move request (ADR 0017), under its app's root.
fn actor_move(args: &MoveArgs) -> anyhow::Result<()> {
    actor_move_on(args, || open_bucket(&args.bucket))
}

/// [`actor_move`] on the bucket `open` gives (opened once the request is
/// valid): the start check `serve` runs
/// first (ADR 0022), so that a request for a tenant left in the layout
/// before 0.4.0 is refused with the migrate command rather than written
/// where no node reads it.
fn actor_move_on(
    args: &MoveArgs,
    open: impl FnOnce() -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)>,
) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::{self, Keys as _};
    let ((tenant, app), class, name) = move_target(&args.actor, args.app.as_ref())?;
    if let Some(warning) = move_target_warning(&args.actor, args.app.as_ref()) {
        eprintln!("warning: {warning}");
    }
    let (tenant, class, name) = (&tenant, &class, &name);
    let key = control::checked_move_key(
        &wasmkeep_cluster::state_root::StateRoot::new("", tenant.as_str(), app.as_str()),
        class,
        name,
        &args.to,
    )?;
    let (store, prefix) = open()?;
    check_layout(&args.bucket, &store, &prefix, tenant)?;
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let to = &args.to;
    match rt.block_on(live_node(&keys, to))? {
        None if !args.force => anyhow::bail!(
            "no live node has the id {to} (see `wasmkeep cluster status`). Pass --force to \
             write the request anyway (it expires in 5 minutes)"
        ),
        None => {}
        Some(node) => {
            for warning in move_warnings(&node, &format!("{tenant}/{app}")) {
                eprintln!("warning: {warning}");
            }
        }
    }
    rt.block_on(keys.put_empty(&key))?;
    println!(
        "Asked the owner of {tenant}/{class}/{name} to move it to {} (expires in 5 minutes).",
        args.to
    );
    Ok(())
}

/// The app (`tenant`, `app`), class and name of `actor move`'s actor:
/// `CLASS/NAME` of `app`, or, without `--app`, `default/CLASS/NAME` of the
/// implicit app `default/default` (ADR 0022). Another tenant written in
/// front is refused: its app must be named.
fn move_target(
    actor: &str,
    app: Option<&(String, String)>,
) -> anyhow::Result<((String, String), String, String)> {
    if let Some(app) = app {
        let (class, name) = actor
            .split_once('/')
            .filter(|(c, n)| !c.is_empty() && !n.is_empty())
            .ok_or_else(|| anyhow::anyhow!("expected CLASS/NAME with --app, got {actor:?}"))?;
        return Ok((app.clone(), class.to_owned(), name.to_owned()));
    }
    let (tenant, class, name) = parse_actor_address(actor).map_err(|e| anyhow::anyhow!(e))?;
    anyhow::ensure!(
        tenant == wasmkeep_server::IMPLICIT_APP_NAME,
        "{actor} is of tenant {tenant}: name its app, as `--app {tenant}/APP {class}/{name}` \
         (without --app, the actor is of the implicit app default/default)"
    );
    Ok((
        (tenant, wasmkeep_server::IMPLICIT_APP_NAME.to_owned()),
        class,
        name,
    ))
}

/// A warning for `actor move --app TENANT/APP TENANT/CLASS/NAME`, the
/// syntax before `--app` with the tenant kept in front: it names class
/// `TENANT`, which is allowed (a class may share its tenant's name) but
/// most likely a mistake.
fn move_target_warning(actor: &str, app: Option<&(String, String)>) -> Option<String> {
    let (tenant, _) = app?;
    let rest = actor.strip_prefix(tenant.as_str())?.strip_prefix('/')?;
    rest.contains('/').then(|| {
        format!(
            "{actor} names class {tenant} of the app: with --app, the actor is CLASS/NAME. \
             Did you mean {rest}?"
        )
    })
}

/// Why a live move target may not take the actor of `app` (`tenant/app`):
/// the request is written anyway (the owner keeps the actor, or another
/// node takes it).
fn move_warnings(node: &wasmkeep_cluster::membership::NodeInfo, app: &str) -> Vec<String> {
    use wasmkeep_cluster::membership::DrainState;
    let mut warnings = Vec::new();
    // A node serves one app (`tenant/app`; `default/default` for classes
    // given on the command line); `tenant/` from the version before, empty
    // from nodes that predate both.
    if !node.app.is_empty() && node.app != app {
        warnings.push(format!(
            "{} serves {}, not {app}: it refuses to take the actor",
            node.id, node.app
        ));
    }
    if !node.can_take {
        warnings.push(format!(
            "{} does not take moved actors (an older version): the owner keeps the actor, \
             or another node takes it",
            node.id
        ));
    }
    if node.drain.is_some_and(|d| d != DrainState::Serving) {
        warnings.push(format!(
            "{} is draining: it takes no actors until undrained",
            node.id
        ));
    }
    warnings
}

/// `TENANT/CLASS/NAME`; the name keeps any further `/`.
fn parse_actor_address(s: &str) -> Result<(String, String, String), String> {
    let mut parts = s.splitn(3, '/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(t), Some(c), Some(n)) if !t.is_empty() && !c.is_empty() && !n.is_empty() => {
            Ok((t.to_owned(), c.to_owned(), n.to_owned()))
        }
        _ => Err(format!("expected TENANT/CLASS/NAME, got {s:?}")),
    }
}

fn cluster_status(args: StatusArgs) -> anyhow::Result<()> {
    let (store, prefix) = open_bucket_unchecked(&args.bucket)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let horizon_ms = wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_server::LeaseConfig::default_safe(),
    );
    let report = rt.block_on(status::read(
        store,
        &prefix,
        &args.bucket,
        now_ms,
        horizon_ms,
    ))?;
    if args.json {
        println!("{:#}", report.to_json());
    } else {
        print!("{}", report.to_text());
    }
    if !report.any_live() {
        use std::io::Write as _;
        let _ = std::io::stdout().flush();
        eprintln!("error: no live node under {}", args.bucket);
        std::process::exit(1);
    }
    Ok(())
}

/// `CLASS=NAME,NAME...` (empty after `=` removes the class's names).
fn parse_requires(s: &str) -> Result<(String, std::collections::BTreeSet<String>), String> {
    let (class, names) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=NAME,NAME..., got {s:?}"))?;
    if class.is_empty() {
        return Err(format!("expected CLASS=NAME,NAME..., got {s:?}"));
    }
    let mut set = std::collections::BTreeSet::new();
    for name in names.split(',').filter(|n| !n.is_empty()) {
        if !wasmkeep_server::env::valid_name(name) {
            return Err(format!("invalid name {name:?} in --requires"));
        }
        set.insert(name.to_owned());
    }
    Ok((class.to_owned(), set))
}

/// Parses `TENANT/APP`.
pub(crate) fn parse_app(s: &str) -> Result<(String, String), String> {
    let (tenant, app) = s
        .split_once('/')
        .filter(|(t, a)| !t.is_empty() && !a.is_empty() && !a.contains('/'))
        .ok_or_else(|| format!("expected TENANT/APP, got {s:?}"))?;
    Ok((tenant.to_owned(), app.to_owned()))
}

/// Opens `s3://BUCKET[/PREFIX]`, configured from the AWS_* environment,
/// for a command that writes: refused while `cluster migrate` holds the
/// prefix (ADR 0022). Only reads.
pub(crate) fn open_bucket(
    url: &str,
) -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)> {
    let (store, prefix) = open_bucket_unchecked(url)?;
    refuse_locked(url, &store, &prefix)?;
    Ok((store, prefix))
}

/// [`open_bucket`] without the check: `cluster status` and `cluster
/// migrate`.
pub(crate) fn open_bucket_unchecked(
    url: &str,
) -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)> {
    let (bucket, prefix) = parse_bucket(url)?;
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .with_context(|| format!("opening bucket {url}"))?;
    let store: std::sync::Arc<dyn object_store::ObjectStore> = std::sync::Arc::new(store);
    Ok((store, prefix.to_owned()))
}

/// Runs `check` on a thread of its own (callers may hold a runtime of
/// their own), bounded: an unreachable bucket is the command's to report.
fn on_own_thread<T: Send>(check: impl std::future::Future<Output = T> + Send) -> Option<T> {
    std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            rt.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(10), check)
                    .await
                    .ok()
            })
        })
        .join()
        .ok()
        .flatten()
    })
}

/// Refuses a bucket prefix that `cluster migrate` holds.
fn refuse_locked(
    url: &str,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> anyhow::Result<()> {
    use wasmkeep_cluster::lease::Bucket as _;
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
        std::sync::Arc::clone(store),
        prefix,
    );
    let found = on_own_thread(async { bucket.get(wasmkeep_cluster::migrate::LOCK_KEY).await });
    match found {
        Some(Ok(None)) => Ok(()),
        Some(Ok(Some(_))) => anyhow::bail!(
            "bucket {url}: {}",
            wasmkeep_cluster::migrate::StartRefusal::InProgress
        ),
        // Fail closed, as the layout check does: a migration may hold it.
        Some(Err(e)) => anyhow::bail!("bucket {url}: checking for a migration in progress: {e}"),
        None => anyhow::bail!("bucket {url}: no answer while checking for a migration in progress"),
    }
}

/// What `serve` checks on a bucket before anything writes to it (ADR
/// 0022): no migration in progress, and `tenant`'s actor state not left in
/// the layout before 0.4.0. `true` when the tenant was migrated.
pub(crate) fn check_layout(
    url: &str,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
) -> anyhow::Result<bool> {
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
        std::sync::Arc::clone(store),
        prefix,
    );
    match on_own_thread(wasmkeep_cluster::migrate::start_check(&bucket, tenant)) {
        None => anyhow::bail!("bucket {url}: no answer while checking its layout"),
        Some(Err(e)) => Err(anyhow::anyhow!(
            "bucket {url}: {}",
            e.to_string().replace("<this bucket>", url)
        )),
        Some(Ok(migrated)) => Ok(migrated),
    }
}

/// Splits `s3://BUCKET[/PREFIX]` into the bucket and the key prefix.
fn parse_bucket(url: &str) -> anyhow::Result<(&str, &str)> {
    let rest = url
        .strip_prefix("s3://")
        .with_context(|| format!("--bucket must look like s3://BUCKET[/PREFIX], got {url:?}"))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    anyhow::ensure!(!bucket.is_empty(), "--bucket has no bucket name: {url:?}");
    Ok((bucket, prefix.trim_matches('/')))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::{
        check_layout, load_env, move_target, move_target_warning, move_warnings, parse_bucket,
    };

    /// `serve --app` follows only a rooted `current` (token 3).
    #[test]
    fn a_current_from_before_0_4_is_refused_at_start() {
        use wasmkeep_server::apps::Current;
        let old = Current::parse(b"v 3 2\n").unwrap();
        let e = super::require_rooted(&old, "acme", "shop", "s3://b/p")
            .unwrap_err()
            .to_string();
        assert!(e.contains("cluster migrate --bucket s3://b/p"), "{e}");
        assert!(e.contains("acme/shop"), "{e}");
        let rooted = Current::parse(b"v 4 3\n").unwrap();
        super::require_rooted(&rooted, "acme", "shop", "s3://b/p").unwrap();
    }

    /// A command of this version refuses a prefix being migrated, and a
    /// tenant whose actor state is still in the layout before 0.4.0;
    /// a migrated tenant and a fresh one start (ADR 0022).
    #[test]
    fn the_layout_check_refuses_an_unmigrated_tenant_and_a_running_migration() {
        use object_store::{
            ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
        };
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let put = |k: &str| {
            let store = std::sync::Arc::clone(&store);
            let k = k.to_owned();
            std::thread::spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(store.put(&Path::from(k.as_str()), PutPayload::from("x")))
                    .unwrap();
            })
            .join()
            .unwrap();
        };
        let check = |tenant: &str| check_layout("s3://b/wk", &store, "wk", tenant);
        check("acme").unwrap();
        // Keys of another prefix, or a move, do not count.
        put("other/owners/acme/c/n");
        put("wk/control/move/acme/c/6e/node-b");
        check("acme").unwrap();
        put("wk/owners/acme/c/n");
        let e = check("acme").unwrap_err().to_string();
        assert!(e.contains("cluster migrate --bucket s3://b/wk"), "{e}");
        check("other").unwrap();
        put("wk/cluster/migrated/acme");
        check("acme").unwrap();
        put("wk/cluster/migration");
        let e = check("other").unwrap_err().to_string();
        assert!(e.contains("in progress"), "{e}");
    }

    /// `actor move` runs the start check `serve` runs (ADR 0022): it
    /// refuses a tenant whose state is still in the layout before 0.4.0,
    /// naming the migrate command, and writes nothing.
    #[test]
    fn actor_move_refuses_an_unmigrated_tenant() {
        use clap::Parser as _;
        use object_store::{
            ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
        };
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(&Path::from("wk/owners/acme/room/x"), PutPayload::from("x")))
            .unwrap();
        let super::Cli {
            command: super::Command::Actor(super::ActorCommand::Move(args)),
        } = super::Cli::try_parse_from([
            "wasmkeep",
            "actor",
            "move",
            "--app",
            "acme/shop",
            "room/x",
            "--to",
            "node-b",
            "--force",
            "--bucket",
            "s3://b/wk",
        ])
        .unwrap()
        else {
            unreachable!()
        };
        let e = super::actor_move_on(&args, || Ok((std::sync::Arc::clone(&store), "wk".into())))
            .unwrap_err()
            .to_string();
        assert!(e.contains("cluster migrate --bucket s3://b/wk"), "{e}");
        let listed: Vec<_> = rt.block_on(async {
            use futures_util::TryStreamExt as _;
            store.list(None).try_collect::<Vec<_>>().await.unwrap()
        });
        assert_eq!(listed.len(), 1, "{listed:?}");
    }

    /// `--remove-old` takes its own flags only, and never with
    /// `--rollback` or `--heir`.
    #[test]
    fn remove_old_flags_come_with_it() {
        use clap::Parser as _;
        let parse = |extra: &[&str]| {
            let mut argv = vec!["wasmkeep", "cluster", "migrate", "--bucket", "s3://b"];
            argv.extend_from_slice(extra);
            super::Cli::try_parse_from(argv)
                .map(drop)
                .map_err(|e| e.to_string())
        };
        parse(&["--remove-old"]).unwrap();
        parse(&["--remove-old", "--apply", "--forgo-rollback"]).unwrap();
        parse(&["--remove-old", "--history-retention-secs", "0"]).unwrap();
        assert!(parse(&["--forgo-rollback"]).is_err());
        assert!(parse(&["--history-retention-secs", "0"]).is_err());
        assert!(parse(&["--remove-old", "--rollback"]).is_err());
        assert!(parse(&["--remove-old", "--heir", "a/b"]).is_err());
    }

    /// `actor move` names the actor's app with `--app`, or the implicit
    /// app with `default/` in front (ADR 0022).
    #[test]
    fn a_moved_actor_is_named_within_its_app() {
        let shop = ("acme".to_owned(), "shop".to_owned());
        let owned = |t: &str, a: &str, c: &str, n: &str| {
            ((t.to_owned(), a.to_owned()), c.to_owned(), n.to_owned())
        };
        assert_eq!(
            move_target("room/lobby/1", Some(&shop)).unwrap(),
            owned("acme", "shop", "room", "lobby/1")
        );
        assert_eq!(
            move_target("default/room/x", None).unwrap(),
            owned("default", "default", "room", "x")
        );
        let e = move_target("acme/room/x", None).unwrap_err().to_string();
        assert!(e.contains("--app acme/APP room/x"), "{e}");
        assert!(move_target("room", Some(&shop)).is_err());
        assert!(move_target("room/", Some(&shop)).is_err());
        assert!(move_target("default/room", None).is_err());
    }

    /// The tenant kept in front with `--app` (the syntax before it) parses
    /// as a class named like the tenant: allowed, with a warning.
    #[test]
    fn a_tenant_kept_in_front_with_app_gets_a_warning() {
        let shop = ("acme".to_owned(), "shop".to_owned());
        let w = move_target_warning("acme/room/lobby", Some(&shop)).expect("a warning");
        assert!(w.contains("room/lobby"), "{w}");
        assert!(move_target_warning("room/lobby", Some(&shop)).is_none());
        assert!(move_target_warning("default/room/x", None).is_none());
    }

    /// A move target that cannot take the actor gets a warning saying why.
    #[test]
    fn a_move_target_that_cannot_take_it_gets_a_warning() {
        use wasmkeep_cluster::membership::{DrainState, NodeInfo};
        let node = |can_take, drain| NodeInfo {
            id: "c".into(),
            address: "http://c:8080".into(),
            version: "0.0.0".into(),
            written_at_ms: 0,
            app: "acme/shop".into(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain,
            reads_secrets: true,
            can_take,
            instance: None,
            lease: None,
            load: None,
        };
        assert!(move_warnings(&node(true, Some(DrainState::Serving)), "acme/shop").is_empty());
        assert!(move_warnings(&node(true, None), "acme/shop").is_empty());
        // Another app's node, of the same tenant or not: it refuses the
        // hop (ADR 0022).
        for app in ["globex/shop", "acme/admin"] {
            let other = move_warnings(&node(true, None), app);
            assert!(
                other.len() == 1 && other[0].contains(&format!("not {app}")),
                "{other:?}"
            );
        }
        let old = move_warnings(&node(false, None), "acme/shop");
        assert!(
            old.len() == 1 && old[0].contains("does not take moved actors"),
            "{old:?}"
        );
        for state in [DrainState::Draining, DrainState::Drained] {
            let w = move_warnings(&node(true, Some(state)), "acme/shop");
            assert!(w.len() == 1 && w[0].contains("is draining"), "{w:?}");
        }
    }

    #[test]
    fn bucket_urls_split_into_bucket_and_prefix() {
        assert_eq!(parse_bucket("s3://b").unwrap(), ("b", ""));
        assert_eq!(parse_bucket("s3://b/").unwrap(), ("b", ""));
        assert_eq!(parse_bucket("s3://b/prod/eu/").unwrap(), ("b", "prod/eu"));
        assert!(parse_bucket("b/prod").is_err());
        assert!(parse_bucket("s3:///prod").is_err());
    }

    #[test]
    fn env_files_must_name_a_served_class() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hub.env");
        std::fs::write(&file, "TOKEN=t\n").unwrap();
        let env = load_env(&[("hub".into(), file.clone())], &["hub", "monitor"], true).unwrap();
        assert_eq!(env.for_class("hub").get("TOKEN").unwrap(), "t");
        let e = load_env(&[("hbu".into(), file.clone())], &["hub", "monitor"], true).unwrap_err();
        assert!(e.to_string().contains("no class hbu"), "{e}");
        let e = load_env(
            &[("hub".into(), dir.path().join("missing"))],
            &["hub"],
            true,
        )
        .unwrap_err();
        assert!(e.to_string().contains("missing"), "{e}");
        // With --app (not strict) a class the version lacks is only warned
        // about, and its values are kept for a deploy that brings it.
        let env = load_env(&[("gone".into(), file)], &["hub"], false).unwrap();
        assert_eq!(env.summary().len(), 1);
    }

    #[test]
    fn secret_looking_configuration_is_named_without_its_value() {
        let vars = wasmkeep_server::env::parse(
            "URL=https://api.example.test/v1\n\
             DATABASE_URL=postgres://app:hunter2@db.internal/app\n\
             API_KEY=abc123\n\
             auth_header=xyz\n\
             PLAIN=https://user@example.test/\n\
             MODE=fast\n",
        )
        .unwrap();
        let warnings = super::secret_looking("hub", &vars);
        let named: Vec<&str> = ["API_KEY", "DATABASE_URL", "auth_header"].to_vec();
        assert_eq!(warnings.len(), named.len(), "{warnings:?}");
        for (w, name) in warnings.iter().zip(&named) {
            assert!(w.contains(&format!("hub: {name} ")), "{w}");
        }
        for value in ["hunter2", "abc123", "xyz", "postgres://"] {
            assert!(
                !warnings.concat().contains(value),
                "a value leaked: {value}"
            );
        }
    }

    #[test]
    fn a_rename_that_drops_a_rule_is_warned_about() {
        let diff = wasmkeep_server::apps::Diff {
            added: vec!["monitors".into()],
            removed: vec![("monitor".into(), Some("existing".into()))],
            ..Default::default()
        };
        let w = super::deploy_warnings(&diff);
        assert_eq!(w.len(), 1);
        assert!(w[0].contains("--access monitors="), "{}", w[0]);
        let ruled = wasmkeep_server::apps::Diff {
            rules: vec![("monitors".into(), "open".into(), "existing".into())],
            ..diff.clone()
        };
        assert!(super::deploy_warnings(&ruled).is_empty());
        let open_removed = wasmkeep_server::apps::Diff {
            removed: vec![("old".into(), None)],
            ..diff
        };
        assert!(super::deploy_warnings(&open_removed).is_empty());
    }

    #[test]
    fn requires_flags_parse_and_renames_are_warned_about() {
        let (class, names) = super::parse_requires("hub=TOKEN,URL").unwrap();
        assert_eq!(class, "hub");
        assert_eq!(names.into_iter().collect::<Vec<_>>(), ["TOKEN", "URL"]);
        assert!(
            super::parse_requires("hub=").unwrap().1.is_empty(),
            "removes"
        );
        for bad in ["hub", "=TOKEN", "hub=1BAD", "hub=A B"] {
            assert!(super::parse_requires(bad).is_err(), "{bad}");
        }
        let diff = wasmkeep_server::apps::Diff {
            added: vec!["hub2".into()],
            requires: vec![
                "hub: TOKEN no longer required (class removed)".into(),
                "hub: URL no longer required (class removed)".into(),
            ],
            ..Default::default()
        };
        let w = super::deploy_warnings(&diff);
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].starts_with("hub had required values"), "{}", w[0]);
    }

    #[test]
    fn deploy_flags_parse() {
        use clap::Parser as _;
        let parse = |args: &[&str]| {
            super::Cli::try_parse_from(
                ["wasmkeep", "deploy", "--app", "t/a", "--bucket", "s3://b"]
                    .iter()
                    .chain(args),
            )
        };
        assert!(parse(&["--access", "monitor=existing"]).is_ok());
        assert!(parse(&["--config", "hub=hub.env", "--config", "old="]).is_ok());
        assert!(parse(&["--rollback", "--allow-reopen"]).is_ok());
        assert!(parse(&["--allow-reopen"]).is_err(), "only with --rollback");
        assert!(parse(&["--rollback", "--access", "c=open"]).is_err());
        assert!(parse(&["--rollback", "--config", "c=f"]).is_err());
        assert!(parse(&["--access", "c=closed"]).is_err());
        let retention = |args: &[&str]| match parse(args).unwrap().command {
            super::Command::Deploy(d) => d.version_retention_secs,
            _ => unreachable!(),
        };
        assert_eq!(retention(&["--rollback"]), 3600, "an hour by default");
        assert_eq!(
            retention(&["--rollback", "--version-retention-secs", "0"]),
            0
        );
    }

    #[test]
    fn a_node_following_an_app_takes_its_rules_from_the_deploy() {
        let mut deployed = wasmkeep_server::AccessRules::default();
        deployed.apply("monitor", "existing").unwrap();
        let flags = vec![("monitor".to_owned(), "internal".to_owned())];
        let served = ["hub", "monitor"];
        // Rules deployed (marker): --access refused, with the command to run.
        let e = super::serve_app_rules(&flags, &served, true, deployed.clone(), "t", "a")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("wasmkeep deploy --app t/a --bucket ... --access monitor=internal"),
            "{e}"
        );
        let rules = super::serve_app_rules(&[], &served, true, deployed.clone(), "t", "a").unwrap();
        assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Existing);
        // Never deployed: the flags, during the migration; else nothing.
        let rules =
            super::serve_app_rules(&flags, &served, false, Default::default(), "t", "a").unwrap();
        assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Internal);
        let rules =
            super::serve_app_rules(&[], &served, false, Default::default(), "t", "a").unwrap();
        assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Open);
        let typo = vec![("monitr".to_owned(), "existing".to_owned())];
        assert!(
            super::serve_app_rules(&typo, &served, false, Default::default(), "t", "a").is_err()
        );
    }

    /// `wasmkeep serve` with the cluster flags and `extra`.
    fn parse_serve(extra: &[&str]) -> Result<super::ServeArgs, String> {
        use clap::Parser as _;
        let mut argv = vec![
            "wasmkeep",
            "serve",
            "--actor",
            "counter=counter.wasm",
            "--node-id",
            "a",
            "--advertise",
            "http://10.0.0.11:8080",
            "--bucket",
            "s3://b",
        ];
        argv.extend_from_slice(extra);
        match super::Cli::try_parse_from(argv).map_err(|e| e.to_string())? {
            super::Cli {
                command: super::Command::Serve(args),
            } => Ok(*args),
            _ => unreachable!(),
        }
    }

    #[test]
    fn cluster_listener_flags_come_together_and_need_a_cluster() {
        let args = parse_serve(&[
            "--cluster-listen",
            "0.0.0.0:7443",
            "--cluster-advertise",
            "https://10.0.0.11:7443",
        ])
        .unwrap();
        assert_eq!(args.cluster_listen, Some("0.0.0.0:7443".parse().unwrap()));
        assert_eq!(
            args.cluster_advertise.as_deref(),
            Some("https://10.0.0.11:7443")
        );
        assert!(parse_serve(&[]).unwrap().cluster_listen.is_none());

        let e = parse_serve(&["--cluster-advertise", "https://10.0.0.11:7443"]).unwrap_err();
        assert!(e.contains("--cluster-listen"), "{e}");
        let e = parse_serve(&["--cluster-listen", "0.0.0.0:7443"]).unwrap_err();
        assert!(e.contains("--cluster-advertise"), "{e}");
        for plain in [
            "http://10.0.0.11:7443",
            "10.0.0.11:7443",
            "https://",
            "https://:7443",
        ] {
            let e = parse_serve(&[
                "--cluster-listen",
                "0.0.0.0:7443",
                "--cluster-advertise",
                plain,
            ])
            .unwrap_err();
            assert!(e.contains("https://"), "{plain}: {e}");
        }

        // Without the cluster flags.
        use clap::Parser as _;
        let e = super::Cli::try_parse_from([
            "wasmkeep",
            "serve",
            "--actor",
            "counter=counter.wasm",
            "--cluster-listen",
            "0.0.0.0:7443",
            "--cluster-advertise",
            "https://10.0.0.11:7443",
        ])
        .unwrap_err()
        .to_string();
        assert!(e.contains("--node-id"), "{e}");
    }

    #[test]
    fn plaintext_between_nodes_is_refused_by_default() {
        use super::ClusterPlaintext;
        let listen = [
            "--cluster-listen",
            "0.0.0.0:7443",
            "--cluster-advertise",
            "https://10.0.0.11:7443",
        ];
        assert_eq!(
            parse_serve(&[]).unwrap().cluster_plaintext,
            ClusterPlaintext::Refuse
        );
        assert_eq!(
            parse_serve(&listen).unwrap().cluster_plaintext,
            ClusterPlaintext::Refuse
        );
        let args =
            parse_serve(&[&listen[..], &["--cluster-plaintext", "refuse"]].concat()).unwrap();
        assert_eq!(args.cluster_plaintext, ClusterPlaintext::Refuse);
        let args = parse_serve(&["--cluster-plaintext", "accept"]).unwrap();
        assert_eq!(args.cluster_plaintext, ClusterPlaintext::Accept);

        // Whether a cluster node has a listener is checked when it starts
        // (tests/cluster_tls.rs): a single node has none and needs none.
        let e = parse_serve(&["--cluster-plaintext", "never"]).unwrap_err();
        assert!(e.contains("accept") && e.contains("refuse"), "{e}");
    }

    #[test]
    fn access_rules_must_name_a_served_class() {
        let (class, mode) = super::parse_access("monitor=existing").unwrap();
        let rules = super::access_rules(&[(class, mode)], &["hub", "monitor"]).unwrap();
        assert_eq!(rules.of("monitor"), wasmkeep_server::Access::Existing);
        assert!(super::parse_access("monitor=closed").is_err());
        assert!(super::parse_access("monitor").is_err());
        assert!(super::parse_access("hub=internal:main").is_err());
        let (class, mode) = super::parse_access("hub=existing:main").unwrap();
        let rules = super::access_rules(&[(class, mode)], &["hub"]).unwrap();
        assert_eq!(
            rules.for_actor("hub", "main"),
            wasmkeep_server::Access::Open
        );
        let typo = vec![("monitr".to_owned(), "internal".to_owned())];
        assert!(super::access_rules(&typo, &["monitor"]).is_err());
    }
}
