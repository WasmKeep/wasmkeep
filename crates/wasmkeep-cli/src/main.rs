//! The wasmkeep command-line tool: serve, dev, deploy, cluster, actor, secrets, new.

#![forbid(unsafe_code)]

mod actor_read;
mod actor_restore;
mod actor_where;
mod app_delete;
mod app_format_cmd;
mod inspect;
use wasmkeep_cli::backup;
use wasmkeep_cli::{open_bucket_unchecked, parse_app, parse_bucket};
mod actor_cmd;
mod assets;
mod cluster_cmd;
mod deploy;
mod dev;
mod dev_project;
mod diagnose;
mod new_cmd;
mod node_file;
mod project;
mod secrets_cmd;
mod serve;
mod serve_many;
mod status;
mod upgrade_cmd;
mod usage;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmkeep_server::apps::AppRef;
use wasmkeep_server::{ActorConfig, ActorService, App, ClusterConfig, HttpComponent};

use actor_cmd::*;
use cluster_cmd::*;
use deploy::*;
use serve::*;

#[derive(Debug, Parser)]
#[command(
    name = "wasmkeep",
    version = wasmkeep_server::VERSION,
    about = "Self-hosted actor platform for Wasm components"
)]
struct Cli {
    /// glibc arena cap before threads start; 0 preserves glibc/environment defaults.
    /// Default: available cores clamped to 2..8. No-op outside Linux/glibc.
    #[arg(long, global = true, default_value_t = wasmkeep_runtime::default_malloc_arena_max(), value_parser = clap::value_parser!(i32).range(0..))]
    malloc_arena_max: i32,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Open a local, read-only bucket inspector.
    Inspect(inspect::Args),
    /// Collect unused static assets and expired versions from an app's bucket.
    #[command(subcommand)]
    Assets(AssetsCommand),
    /// Export changed states to local Parquet files in one read-only poll.
    Feed(wasmkeep_cli::feed::CommandArgs),
    /// Create and verify lease-free online archives.
    #[command(subcommand)]
    Backup(backup::Command),
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
    /// Write a new project into an empty directory: a class (a counter),
    /// optionally a front, its wasmkeep.toml, a README, .gitignore and an
    /// .env.local example. It builds and runs with `wasmkeep dev`.
    New(new_cmd::NewArgs),
    /// Act on a whole app, through its bucket.
    #[command(subcommand)]
    App(AppCommand),
}

#[derive(Debug, Subcommand)]
enum AssetsCommand {
    /// Collect old versions and unused contents and indexes (ADR 0030).
    Collect(AssetsCollectArgs),
}

#[derive(Debug, Args)]
struct AssetsCollectArgs {
    /// The app, TENANT/APP.
    #[arg(long, value_parser = parse_app)]
    app: (String, String),
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX].
    #[arg(long)]
    bucket: String,
    /// Minimum retention for old versions and unused assets, in seconds.
    #[arg(long, default_value_t = 3600, value_name = "SECS")]
    version_retention_secs: u64,
}

fn assets_collect(args: AssetsCollectArgs) -> anyhow::Result<()> {
    let (store, prefix) = open_bucket(&args.bucket)?;
    assets_collect_in(args, store, &prefix)
}

fn assets_collect_in(
    args: AssetsCollectArgs,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> anyhow::Result<()> {
    let app = AppRef::new(store, prefix, &args.app.0, &args.app.1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (versions, assets) = rt.block_on(app.collect(
        wasmkeep_server::apps::KEEP_PREVIOUS,
        std::time::Duration::from_secs(args.version_retention_secs),
    ))?;
    println!("removed {versions} old version(s) and {assets} asset object(s) from the bucket");
    Ok(())
}

#[derive(Debug, Subcommand)]
enum AppCommand {
    /// Delete an app (ADR 0028): writes its tombstone first, from which
    /// deploys refuse the app and its nodes stop serving it within a few
    /// seconds (closing its WebSockets with 1001); then, once no live node
    /// of the app is left, removes every key under its two prefixes: every
    /// actor's history, the ownership records, the alarm index, the
    /// requests, every deployed version and the sealed secrets. The
    /// tombstone stays; `deploy --recreate` reuses the name. Run it again
    /// to finish a delete that stopped. Cannot be undone. Keys held
    /// outside the bucket (the secrets key) are yours to destroy.
    Delete(AppDeleteArgs),
    /// Show the app's ownership-format setting (ADR 0035): version 1
    /// records, the recorded-head format enabled, or an unreadable
    /// setting; with --enable, switch the app to the recorded-head format,
    /// one way, once every registered node of it announces
    /// `recorded-head` and none runs it.
    Format(AppFormatArgs),
}

#[derive(Debug, Args)]
struct AppFormatArgs {
    /// The app, TENANT/APP.
    #[arg(long, value_parser = parse_app)]
    app: (String, String),

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Enable the recorded-head format (cannot be undone): refused while
    /// a registered node of the app does not announce `recorded-head`,
    /// while one runs the app, and within the late-write window after
    /// the last stopped.
    #[arg(long)]
    enable: bool,
}

#[derive(Debug, Args)]
struct AppDeleteArgs {
    /// Resume this operation ID without submitting another request.
    #[arg(long)]
    resume: Option<String>,
    /// The app, TENANT/APP.
    #[arg(long, value_parser = parse_app)]
    app: (String, String),

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// How long to wait for the app's live nodes to stop, in seconds;
    /// still live then, the command stops with the tombstone written and
    /// nothing removed (run it again).
    #[arg(long, default_value_t = 60, value_name = "SECS")]
    wait: u64,

    /// Remove the data even though nodes of the app still look live: only
    /// after `wasmkeep cluster fence` of each of them.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct DevArgs {
    /// Actor crate directories (each with a Cargo.toml using wasmkeep-sdk).
    /// Without any, the project file (./wasmkeep.toml, or --project) is
    /// run: its front and classes, as deployed (ADR 0026).
    crates: Vec<PathBuf>,

    /// The project file: a wasmkeep.toml, or a directory holding one.
    #[arg(long, value_name = "PATH", conflicts_with = "no_project")]
    project: Option<PathBuf>,

    /// Ignore ./wasmkeep.toml (with crate directories).
    #[arg(long)]
    no_project: bool,

    /// Address to listen on (default 127.0.0.1:8080, or the project
    /// file's [dev] port).
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Where actor databases live while developing (default
    /// .wasmkeep/dev; with a project file, its [dev] data-dir, or
    /// .wasmkeep/dev beside the file).
    #[arg(long)]
    data_dir: Option<PathBuf>,

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

/// Where an app's node links point (ADR 0030, section 4, decision 7): its
/// `blob-link-base`, else `https://` and its first hostname, else
/// `--advertise`, else, alone, this node's listening address when it is
/// one; never a request's `Host`.
pub(crate) fn link_base(args: &ServeArgs, hostnames: &[String]) -> Option<String> {
    args.blob_link_base
        .clone()
        .or_else(|| hostnames.first().map(|h| format!("https://{h}")))
        .or_else(|| args.advertise.clone())
        .or_else(|| (!args.listen.ip().is_unspecified()).then(|| format!("http://{}", args.listen)))
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
        let vars =
            wasmkeep_server::env::read_file(path).map_err(|e| anyhow::anyhow!("--env: {e}"))?;
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
        let more = wasmkeep_server::env::read_file(path)
            .map_err(|e| anyhow::anyhow!("--front-env: {e}"))?;
        vars.extend(more);
    }
    if !files.is_empty() {
        tracing::info!(values = vars.len(), "front env loaded");
    }
    Ok(vars)
}

/// `--max-front-requests`, or its default: a tenth of the pool, rounded
/// up, on a node that serves actors, and the whole pool on a node that
/// serves only a front (as before the front had its own slots). A value
/// of 0, or one that leaves actors no instance beside a front, is refused;
/// so is a pool too small for the default.
fn front_requests(
    given: Option<usize>,
    pool: usize,
    front: bool,
    actors: bool,
) -> anyhow::Result<usize> {
    let both = front && actors;
    let Some(n) = given else {
        if front && !actors {
            return Ok(pool.max(1));
        }
        let n = wasmkeep_server::fds::default_front_requests(pool).max(1);
        anyhow::ensure!(
            !both || n < pool,
            "--max-instances {pool} leaves no room for both the front and actors: \
             give at least 2"
        );
        return Ok(n);
    };
    anyhow::ensure!(n > 0, "--max-front-requests must be at least 1");
    if both {
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
/// `serve --writes`: the batched classes among those `served` (given with
/// --actor). Refused for a class not served, and with --app (`app`),
/// whose version names its classes' modes.
pub(crate) fn serve_batched(
    writes: &[(String, wasmkeep_server::apps::Writes)],
    served: &[&str],
    app: bool,
) -> anyhow::Result<std::collections::BTreeSet<String>> {
    anyhow::ensure!(
        writes.is_empty() || !app,
        "--writes cannot be used with --app: the deployed version names each class's mode \
         (wasmkeep deploy --writes CLASS=MODE)"
    );
    let mut out = std::collections::BTreeSet::new();
    for (class, mode) in writes {
        anyhow::ensure!(
            served.contains(&class.as_str()),
            "--writes {class}=...: no class {class} is served (classes: {})",
            served.join(", ")
        );
        if *mode == wasmkeep_server::apps::Writes::Batched {
            out.insert(class.clone());
        } else {
            out.remove(class);
        }
    }
    Ok(out)
}

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
    if !rules.is_empty() {
        tracing::info!("access rules (--access): {}", out.describe(served));
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
                 bucket in the clear (keep secrets with `wasmkeep secrets set`)"
            ));
        }
    }
    out
}

/// Warnings about a deploy's changes: a class removed with a rule, or
/// with required names, while a class without them is added (a rename
/// leaves the new class open, or without its names). `file`: a deploy from
/// that project file, told what to set in it rather than flags it refuses.
pub(crate) fn deploy_warnings(
    diff: &wasmkeep_server::apps::Diff,
    file: Option<&str>,
) -> Vec<String> {
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
    let without_names: Vec<&str> = diff
        .added
        .iter()
        .map(String::as_str)
        .filter(|c| {
            !diff
                .requires
                .iter()
                .any(|l| l.starts_with(&format!("{c}: ")) && l.ends_with(" now required"))
        })
        .collect();
    if !dropped.is_empty() && !without_names.is_empty() {
        let fix = match file {
            Some(file) => without_names
                .iter()
                .map(|c| format!("set requires in [classes.{c}] of {file}"))
                .collect::<Vec<_>>()
                .join(", "),
            None => "give the new class its --requires".to_owned(),
        };
        out.push(format!(
            "{} had required values and was removed while {} was added: if it was \
             renamed, {fix}",
            dropped.join(", "),
            without_names.join(", ")
        ));
    }
    let restricted: Vec<&(String, Option<String>)> =
        diff.removed.iter().filter(|(_, r)| r.is_some()).collect();
    if restricted.is_empty() {
        return out;
    }
    for added in &diff.added {
        if diff.added_rule(added) == "open" {
            let fix = match file {
                Some(file) => format!("set access in [classes.{added}] of {file}"),
                None => format!("give the new class its rule (--access {added}=...)"),
            };
            out.push(format!(
                "class {added} was added and is open, while {} was removed with a rule: \
                 if it was renamed, {fix}",
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
        // A fact, once per start (each deploy logs its own): open is the
        // default, and a deploy with `--access CLASS=MODE` changes it.
        tracing::info!("access rules (deployed): {}", deployed.describe(served));
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

/// The front's scope in secrets and messages (ADR 0023).
pub(crate) const FRONT_SCOPE: &str = "_front";

fn parse_actor(s: &str) -> Result<(String, PathBuf), String> {
    let (class, path) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=PATH, got {s:?}"))?;
    Ok((class.to_owned(), PathBuf::from(path)))
}

/// The status a process killed by SIGPIPE reports in a shell (128 + 13).
const BROKEN_PIPE_EXIT: i32 = 141;

/// Whether a panic is the standard library's on a print to a closed pipe
/// (`println!` to `| head` once it exits): "failed printing to stdout:
/// Broken pipe (os error 32)".
fn is_broken_pipe_panic(message: &str) -> bool {
    message.starts_with("failed printing to std")
        && (message.contains("Broken pipe") || message.contains("os error 32"))
}

/// A command whose reader went away (`wasmkeep cluster status --json |
/// head`) stops quietly with [`BROKEN_PIPE_EXIT`], as a command killed by
/// SIGPIPE would, instead of panicking: Rust ignores SIGPIPE, so the print
/// fails instead. Every other panic is reported as before, and so is every
/// error a command returns (an EPIPE on a bucket connection is a real
/// failure). Logs are not affected (the log writer drops what it cannot
/// write), so a node whose log reader goes away keeps serving: `serve`
/// must log through `tracing`, never `println!`, which would end the node
/// here without releasing its lease.
fn quiet_broken_pipes() {
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        if is_broken_pipe_panic(message) {
            std::process::exit(BROKEN_PIPE_EXIT);
        }
        report(info);
    }));
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Configure before logging, runtimes or any worker threads are started.
    let arena_configured = wasmkeep_runtime::configure_malloc_arena_max(cli.malloc_arena_max);
    quiet_broken_pipes();
    // All commands reserve stdout for their output (including JSON).
    // Logs go to stderr, with plain text when it is a file or a pipe.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(log_colours(
            std::io::IsTerminal::is_terminal(&std::io::stderr()),
            std::env::var_os("NO_COLOR"),
        ))
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if cfg!(all(target_os = "linux", target_env = "gnu")) && !arena_configured {
        anyhow::bail!("glibc rejected --malloc-arena-max {}", cli.malloc_arena_max);
    }
    match cli.command {
        Command::Inspect(args) => inspect::run(args),
        Command::Assets(AssetsCommand::Collect(args)) => assets_collect(args),
        Command::Serve(args) => serve(*args),
        Command::Deploy(args) => deploy(args),
        Command::Cluster(ClusterCommand::Status(args)) => cluster_status(args),
        Command::Cluster(ClusterCommand::Drain(args)) => cluster_drain(&args, true),
        Command::Cluster(ClusterCommand::Undrain(args)) => cluster_drain(&args, false),
        Command::Cluster(ClusterCommand::Fence(args)) => cluster_fence(&args),
        Command::Cluster(ClusterCommand::Forget(args)) => cluster_forget(&args),
        Command::Cluster(ClusterCommand::Upgrade(command)) => upgrade_cmd::run(command),
        Command::Cluster(ClusterCommand::Markers(command)) => wasmkeep_cli::markers::run(command),
        Command::Cluster(ClusterCommand::Diagnose(args)) => {
            let code = diagnose::command(&args);
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            std::process::exit(code);
        }
        Command::Cluster(ClusterCommand::Verify(args)) => {
            let code = wasmkeep_cli::verify::command(&args);
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            std::process::exit(code);
        }
        Command::Backup(args) => backup::run(&args),
        Command::Feed(args) => wasmkeep_cli::feed::run(&args),
        Command::Actor(ActorCommand::History(args)) => actor_read::history(&args),
        Command::Actor(ActorCommand::Export(args)) => actor_read::finish(actor_read::export(&args)),
        Command::Actor(ActorCommand::Sql(args)) => actor_read::finish(actor_read::sql(&args)),
        Command::Actor(ActorCommand::Move(args)) => actor_move(&args),
        Command::Actor(ActorCommand::Delete(args)) => actor_delete(&args),
        Command::Actor(ActorCommand::Restore(args)) => actor_restore::run(&args),
        Command::Actor(ActorCommand::Where(args)) => actor_where::run(&args),
        Command::App(AppCommand::Delete(args)) => app_delete::run(&args),
        Command::App(AppCommand::Format(args)) => app_format_cmd::run(&args),
        Command::Secrets(command) => secrets_cmd::run(command),
        Command::Dev(args) => dev(args),
        Command::New(args) => new_cmd::run(&args),
    }
}

/// `wasmkeep dev`: the project file if there is one and no crate is given
/// (ADR 0026), else the crates.
fn dev(args: DevArgs) -> anyhow::Result<()> {
    let project = if args.crates.is_empty() || args.project.is_some() {
        project::discover(args.project.as_deref(), args.no_project, false)?
    } else if !args.no_project && std::path::Path::new(project::FILE_NAME).exists() {
        anyhow::bail!(
            "crate directories are given beside {}: run `wasmkeep dev` alone to run the \
             project file, or pass --no-project to run the crates",
            project::FILE_NAME
        );
    } else {
        None
    };
    let egress = egress_policy(
        args.allow_outbound.clone(),
        args.allow_outbound_port.clone(),
        &args.outbound_ca,
    )?;
    if let Some(p) = project {
        let mut given = Vec::new();
        if !args.crates.is_empty() {
            given.push("crate directories");
        }
        if !args.env.is_empty() {
            given.push("--env");
        }
        if !args.access.is_empty() {
            given.push("--access");
        }
        anyhow::ensure!(
            given.is_empty(),
            "{} given with {}: with a project file, classes, rules and values come from the \
             file ([dev] env for local values); pass --no-project to run crates with flags",
            given.join(", "),
            p.path.display()
        );
        let listen = args
            .listen
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], p.dev.port.unwrap_or(8080))));
        let data_dir = args
            .data_dir
            .or_else(|| p.dev.data_dir.clone())
            .unwrap_or_else(|| p.dir.join(".wasmkeep/dev"));
        return dev_project::run(
            p,
            dev_project::Options {
                listen,
                data_dir,
                poll: std::time::Duration::from_millis(args.poll_ms),
                egress,
                guest_log: wasmkeep_server::GuestLog::new(args.guest_log_rate),
                no_huge_pages: args.transparent_huge_pages == HugePages::Off,
            },
        );
    }
    anyhow::ensure!(
        !args.crates.is_empty(),
        "give at least one actor crate directory, or run from a directory with a {}",
        project::FILE_NAME
    );
    {
        dev::run(dev::DevOptions {
            crates: args.crates,
            listen: args
                .listen
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8080))),
            data_dir: args
                .data_dir
                .unwrap_or_else(|| PathBuf::from(".wasmkeep/dev")),
            poll: std::time::Duration::from_millis(args.poll_ms),
            egress,
            guest_log: wasmkeep_server::GuestLog::new(args.guest_log_rate),
            no_huge_pages: args.transparent_huge_pages == HugePages::Off,
            env: args.env,
            access: args.access,
        })
    }
}

/// How often `serve --app` reads the app's `current`: for its first
/// deploy, then for every new one.
const DEPLOY_CHECK_EVERY: std::time::Duration = std::time::Duration::from_secs(3);

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
         was not migrated yet, stop every node, migrate it with the 0.8.1 binary \
         (`wasmkeep cluster migrate --bucket {bucket}`, then --apply; this version has no \
         `cluster migrate`, removed in 0.9.0), bring the cluster to 0.8.1 as the upgrade \
         pages say, then upgrade to 0.9.0 (docs/deploy/upgrading-to-0.4.md); if it was, \
         deploy {tenant}/{name} once with this version. Then start this node"
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

/// Whether log lines carry colour codes: only on a terminal, and not under
/// `NO_COLOR` (set and not empty, <https://no-color.org>).
fn log_colours(stdout_is_terminal: bool, no_color: Option<std::ffi::OsString>) -> bool {
    stdout_is_terminal && no_color.is_none_or(|v| v.is_empty())
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

/// Tells the nodes an intent changed (`control/changed`, ADR 0014,
/// amendment of 2026-11-14), so they list the intents within a heartbeat
/// period. If that write fails, the intent is still read, by every node's
/// listing a minute at the latest: a warning, not an error.
fn announce_intent(
    rt: &tokio::runtime::Runtime,
    keys: &wasmkeep_cluster::object_store::ObjectStoreBucket,
) {
    rt.block_on(announce_intent_async(keys));
}

/// [`announce_intent`] on the caller's runtime.
pub(crate) async fn announce_intent_async<K: wasmkeep_cluster::control::Keys>(keys: &K) {
    if let Err(e) = announce_with_retry(keys, ANNOUNCE_RETRY).await {
        eprintln!(
            "warning: the request is written, but telling the nodes failed ({e}): they read it \
             within a minute instead of a few seconds"
        );
    }
}

/// The pause before each further announce after a conflict. Some buckets
/// answer 409 or 412 to one of two overwrites of the same key at once
/// (parallel `actor move`s), though neither write has a condition.
pub(crate) const ANNOUNCE_RETRY: [std::time::Duration; 3] = [
    std::time::Duration::from_millis(100),
    std::time::Duration::from_millis(300),
    std::time::Duration::from_millis(900),
];

/// Rewrites `control/changed` with a new token, again after each pause of
/// `retry` while the bucket answers with a conflict. The write has no
/// condition, so a conflict only means that another writer's overwrite
/// was in flight; the body written last differs from what the nodes
/// listed with either way. Any other error is returned at once.
pub(crate) async fn announce_with_retry<K: wasmkeep_cluster::control::Keys>(
    keys: &K,
    retry: impl IntoIterator<Item = std::time::Duration>,
) -> Result<(), wasmkeep_cluster::lease::BucketError> {
    use wasmkeep_cluster::lease::BucketError;
    let mut retry = retry.into_iter();
    loop {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let token = format!("cli {} {nanos}", std::process::id());
        match wasmkeep_cluster::control::announce(keys, &token).await {
            Err(BucketError::Conflict) => {
                let Some(pause) = retry.next() else {
                    return Err(BucketError::Conflict);
                };
                // Up to 100 ms more, so that writers that collided once
                // do not again: from microseconds and the pid, since a
                // clock coarser than 100 ns leaves `nanos % 100` at 0.
                let jitter = u64::try_from((nanos / 1_000 + u128::from(std::process::id())) % 100)
                    .unwrap_or(0);
                tokio::time::sleep(pause + std::time::Duration::from_millis(jitter)).await;
            }
            r => return r,
        }
    }
}

/// Refuses an app deleted with `wasmkeep app delete` (ADR 0028): nothing
/// is written for it.
pub(crate) fn refuse_deleted(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(refuse_deleted_async(store, prefix, tenant, app))
}

/// [`refuse_deleted`] on the caller's runtime. A command that holds a
/// runtime must call this one: a second runtime would find the store's
/// pooled connections tied to the first, which runs nothing while it
/// waits, and each such request would hang until its 30 s timeout.
pub(crate) async fn refuse_deleted_async(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
    app: &str,
) -> anyhow::Result<()> {
    AppRef::new(std::sync::Arc::clone(store), prefix, tenant, app)
        .check_not_deleted()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
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

/// `--expire-after-idle CLASS=DURATION` (ADR 0028): the limit in seconds,
/// or `None` for `CLASS=` (removes it).
/// `--writes CLASS=MODE` (ADR 0034, section 15).
fn parse_writes(s: &str) -> Result<(String, wasmkeep_server::apps::Writes), String> {
    let (class, mode) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=MODE, got {s:?}"))?;
    if class.is_empty() || class == FRONT_SCOPE {
        return Err(format!("expected a class name in {s:?}"));
    }
    Ok((
        class.to_owned(),
        wasmkeep_server::apps::Writes::parse(mode)?,
    ))
}

/// Opens `s3://BUCKET[/PREFIX]`, configured from the AWS_* environment,
/// for a command that writes: refused while a 0.8.x `cluster migrate`
/// holds the prefix (ADR 0022). Only reads.
pub(crate) fn open_bucket(
    url: &str,
) -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)> {
    let (store, prefix) = open_bucket_unchecked(url)?;
    refuse_locked(url, &store, &prefix)?;
    Ok((store, prefix))
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

/// Refuses a bucket prefix that a 0.8.x `cluster migrate` holds.
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
    let found =
        on_own_thread(async { bucket.get(wasmkeep_cluster::legacy_layout::LOCK_KEY).await });
    match found {
        Some(Ok(None)) => Ok(()),
        Some(Ok(Some(_))) => anyhow::bail!(
            "bucket {url}: {}",
            wasmkeep_cluster::legacy_layout::StartRefusal::InProgress
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
    on_own_thread(check_layout_async(url, store, prefix, tenant))
        .unwrap_or_else(|| anyhow::bail!("bucket {url}: no answer while checking its layout"))
}

/// [`check_layout`] on the caller's runtime, bounded the same way (see
/// [`refuse_deleted_async`] for why a command holding a runtime needs it).
/// It covers [`open_bucket`]'s migration check too.
pub(crate) async fn check_layout_async(
    url: &str,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
) -> anyhow::Result<bool> {
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
        std::sync::Arc::clone(store),
        prefix,
    );
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        wasmkeep_cluster::legacy_layout::start_check(&bucket, tenant),
    )
    .await
    {
        Err(_) => anyhow::bail!("bucket {url}: no answer while checking its layout"),
        Ok(Err(e)) => Err(anyhow::anyhow!(
            "bucket {url}: {}",
            e.to_string().replace("<this bucket>", url)
        )),
        Ok(Ok(migrated)) => Ok(migrated),
    }
}

#[cfg(test)]
mod tests;
