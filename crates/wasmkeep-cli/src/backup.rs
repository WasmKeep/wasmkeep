//! Content-addressed, lease-free online archives (ADR 0033).
use crate::actor_groups::actor_groups;
use anyhow::{Context, bail, ensure};
use clap::{Args, Subcommand, ValueEnum};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, PutMode, PutOptions, path::Path as Key};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use wasmkeep_cluster::replication::{Listing, plan_restore};
use wasmkeep_server::history;

const MAX: u64 = 2 << 30;
const META_MAX: u64 = 64 << 20;
// Serialized budgets bound JSON expansion as well as resident indexes. Oversize
// archives fail explicitly; they are never silently truncated.
const ACTOR_MAX: u64 = 8 << 20;
const SPOOL_MAX: u64 = 1 << 30;
const INDEX_MAX: u64 = 64 << 20;
fn charge(used: &mut u64, bytes: u64, limit: u64, label: &str) -> anyhow::Result<()> {
    *used = used.checked_add(bytes).context("backup budget overflow")?;
    ensure!(
        *used <= limit,
        "backup {label} budget exceeded ({limit} bytes)"
    );
    Ok(())
}
type Store = Arc<dyn ObjectStore>;
#[derive(Debug, Subcommand)]
pub enum Command {
    Create(Create),
    List(From),
    Verify(Verify),
    Restore(Restore),
    Prune(Prune),
}
#[derive(Debug, Args)]
pub struct Create {
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    to: String,
    #[arg(long, required_unless_present = "all", conflicts_with = "all", value_parser = crate::parse_app)]
    app: Option<(String, String)>,
    #[arg(long)]
    all: bool,
    #[arg(long, value_enum, default_value = "head")]
    history: History,
    /// Fail if listed retained history could not be archived (default for all).
    #[arg(long, action = clap::ArgAction::Set)]
    require_continuous: Option<bool>,
    /// Local verified content cache; contains plaintext actor data.
    #[arg(long)]
    cache: Option<PathBuf>,
    /// Destination AWS_* settings, without changing the source environment.
    #[arg(long)]
    to_env: Option<PathBuf>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum History {
    Head,
    All,
}
#[derive(Debug, Args)]
pub struct From {
    #[arg(long)]
    from: String,
}
#[derive(Debug, Args)]
pub struct Prune {
    #[arg(long)]
    from: String,
    /// Keep the newest N complete runs, plus latest, recent runs and pins.
    #[arg(long)]
    keep_runs: usize,
}
#[derive(Debug, Args)]
pub struct Verify {
    #[arg(long)]
    from: String,
    #[arg(long)]
    run: Option<String>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    sample: Option<u64>,
}
mod files;
mod prune;
mod restore;
pub(crate) use restore::object_segment;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DeployChoice {
    Backup,
    Keep,
}

#[derive(Debug, Args)]
pub struct Restore {
    /// Resume this operation ID without submitting another request.
    #[arg(long, conflicts_with = "abort")]
    resume: Option<String>,
    #[arg(long)]
    from: String,
    #[arg(long)]
    run: Option<String>,
    #[arg(long, value_parser = crate::parse_app)]
    app: (String, String),
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    in_place: bool,
    /// Cancel an empty-root restore before opening; in-place restores only in prepare.
    #[arg(long, conflicts_with = "run")]
    abort: bool,
    #[arg(long, value_enum)]
    deploy: Option<DeployChoice>,
    /// Allow old nodes and a partial undo backup, with a warning; never live nodes.
    #[arg(long)]
    force: bool,
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: u64,
}
impl std::io::Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.bytes.len() as u64).saturating_add(bytes.len() as u64) > self.limit {
            return Err(std::io::Error::other(
                "backup serialized memory budget exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode_bounded(value: &Value, limit: u64) -> anyhow::Result<Vec<u8>> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}
async fn list_bounded(store: &Store, prefix: &str) -> anyhow::Result<BTreeMap<String, ObjectMeta>> {
    let mut objects = BTreeMap::new();
    let mut used = 0;
    let mut stream = store.list(Some(&Key::parse(prefix.trim_end_matches('/'))?));
    while let Some(meta) = tokio::time::timeout(std::time::Duration::from_secs(30), stream.next())
        .await
        .context("backup listing stalled")?
    {
        let meta = meta?;
        let Some(relative) = meta.location.as_ref().strip_prefix(prefix) else {
            continue;
        };
        charge(
            &mut used,
            relative.len() as u64 + meta.location.as_ref().len() as u64 + 512,
            INDEX_MAX,
            "listing memory",
        )?;
        objects.insert(relative.to_owned(), meta);
    }
    Ok(objects)
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn key(prefix: &str, tail: &str) -> String {
    if prefix.is_empty() {
        tail.into()
    } else {
        format!("{}/{tail}", prefix.trim_end_matches('/'))
    }
}
fn text<'a>(v: &'a Value, field: &str) -> anyhow::Result<&'a str> {
    v[field]
        .as_str()
        .with_context(|| format!("invalid manifest {field}"))
}
fn array<'a>(v: &'a Value, field: &str) -> anyhow::Result<&'a Vec<Value>> {
    v[field]
        .as_array()
        .with_context(|| format!("invalid manifest {field}"))
}
fn number(v: &Value, field: &str) -> anyhow::Result<u64> {
    v[field]
        .as_u64()
        .with_context(|| format!("invalid manifest {field}"))
}
fn safe_id(id: &str) -> anyhow::Result<()> {
    ensure!(
        !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid run id"
    );
    Ok(())
}
async fn read(store: &Store, path: &str, cap: u64) -> anyhow::Result<Vec<u8>> {
    Ok(history::get(store, &Key::from(path), None, cap)
        .await?
        .to_vec())
}
async fn optional(store: &Store, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
    match history::get(store, &Key::from(path), None, META_MAX).await {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(history::Error::Missing(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
async fn put(store: &Store, path: &str, bytes: Vec<u8>, replace: bool) -> anyhow::Result<()> {
    match store
        .put_opts(
            &Key::from(path),
            bytes.into(),
            PutOptions {
                mode: if replace {
                    PutMode::Overwrite
                } else {
                    PutMode::Create
                },
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. })
            if !replace && path.rsplit('/').nth(1) == Some("data") =>
        {
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
async fn json_put(
    store: &Store,
    path: &str,
    value: &Value,
    replace: bool,
) -> anyhow::Result<String> {
    let bytes = encode_bounded(value, META_MAX - 1)?;
    ensure!(
        (bytes.len() as u64) < META_MAX,
        "metadata exceeds loader cap: {path}"
    );
    let hash = digest(&bytes);
    put(store, path, bytes, replace).await?;
    Ok(hash)
}
fn setting(name: &str) -> anyhow::Result<object_store::aws::AmazonS3ConfigKey> {
    let config = name
        .trim()
        .to_ascii_lowercase()
        .parse()
        .map_err(|_| anyhow::anyhow!("unsupported S3 setting {name}"))?;
    ensure!(
        config != object_store::aws::AmazonS3ConfigKey::Bucket,
        "bucket override forbidden: {name}"
    );
    ensure!(
        config != object_store::aws::AmazonS3ConfigKey::VirtualHostedStyleRequest,
        "virtual-hosted addressing is forbidden for online backup: {name}"
    );
    Ok(config)
}
fn configured(
    url: &str,
    file: Option<&std::path::Path>,
    source_bucket: Option<&str>,
) -> anyhow::Result<(Store, String, String)> {
    configured_settled(url, file, source_bucket, None)
}

/// Every HTTP attempt of a store, recorded in a [`wasmkeep_server::settle::Settle`]:
/// the store's own retries can hide an attempt without an answer behind a
/// call that succeeds.
#[derive(Debug)]
struct Observed(Arc<wasmkeep_server::settle::Settle>);
impl object_store::client::HttpConnector for Observed {
    fn connect(
        &self,
        options: &object_store::ClientOptions,
    ) -> object_store::Result<object_store::client::HttpClient> {
        let inner = object_store::client::ReqwestConnector::default().connect(options)?;
        Ok(wasmkeep_server::settle::observe(inner, Arc::clone(&self.0)))
    }
}

/// [`configured`], with every mutating HTTP attempt recorded in `settle`.
fn configured_settled(
    url: &str,
    file: Option<&std::path::Path>,
    source_bucket: Option<&str>,
    settle: Option<Arc<wasmkeep_server::settle::Settle>>,
) -> anyhow::Result<(Store, String, String)> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey as C};
    let (bucket, prefix) = crate::parse_bucket(url)?;
    ensure!(
        Key::from(prefix).as_ref() == prefix,
        "bucket prefix must be canonical"
    );
    let mut builder = AmazonS3Builder::new();
    let settings: Vec<(String, String)> = if let Some(file) = file {
        std::fs::read_to_string(file)?
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(|line| {
                let (k, v) = line
                    .split_once('=')
                    .context("destination env needs NAME=value")?;
                Ok((
                    k.trim().to_owned(),
                    v.trim().trim_matches(['\'', '"']).to_owned(),
                ))
            })
            .collect::<anyhow::Result<_>>()?
    } else {
        std::env::vars()
            .filter(|(k, _)| k.to_ascii_lowercase().parse::<C>().is_ok())
            .collect()
    };
    for (name, value) in settings {
        builder = builder.with_config(setting(&name)?, value);
    }
    let endpoint = builder
        .get_config_value(&C::S3Endpoint)
        .or_else(|| builder.get_config_value(&C::Endpoint))
        .unwrap_or_else(|| "https://s3.amazonaws.com".into());
    let parsed = reqwest::Url::parse(&endpoint).context("invalid S3 endpoint")?;
    let host = parsed
        .host_str()
        .context("S3 endpoint has no host")?
        .trim_end_matches('.');
    let service_host = host
        .strip_suffix(".amazonaws.com.cn")
        .or_else(|| host.strip_suffix(".amazonaws.com"));
    let region = |r: &str| {
        r.split('-').count() >= 3
            && r.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && r.rsplit('-')
                .next()
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    };
    let service = service_host.is_some_and(|h| {
        h == "s3"
            || h.strip_prefix("s3.").is_some_and(region)
            || h.strip_prefix("s3-").is_some_and(region)
            || h.strip_prefix("s3.dualstack.").is_some_and(region)
    });
    ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "invalid S3 endpoint scheme"
    );
    let embeds_bucket = |bucket: &str| {
        host.eq_ignore_ascii_case(bucket)
            || host
                .to_ascii_lowercase()
                .starts_with(&format!("{}.", bucket.to_ascii_lowercase()))
    };
    ensure!(
        (service_host.is_none() || service)
            && !embeds_bucket(bucket)
            && !source_bucket.is_some_and(embeds_bucket),
        "bucket-hosted S3 endpoint forbidden; use a path-style service endpoint"
    );
    ensure!(
        parsed.path() == "/"
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.username().is_empty()
            && parsed.password().is_none(),
        "S3 endpoint must be an origin without path or credentials"
    );
    let endpoint = endpoint_identity(&endpoint)?;
    if let Some(settle) = settle {
        builder = builder.with_http_connector(Observed(settle));
    }
    let store = builder.with_bucket_name(bucket).build()?;
    Ok((Arc::new(store), prefix.to_owned(), endpoint))
}
fn endpoint_identity(endpoint: &str) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(endpoint).context("invalid S3 endpoint")?;
    let host = url.host_str().context("S3 endpoint has no host")?;
    if host.ends_with('.') {
        let host = host.trim_end_matches('.').to_owned();
        url.set_host(Some(&host))
            .context("invalid S3 endpoint host")?;
    }
    let host = url.host_str().context("S3 endpoint has no host")?;
    // AWS bucket names identify the same bucket across regional S3 endpoints.
    if url.scheme() == "https"
        && url.port().is_none()
        && (host == "s3.amazonaws.com" || host.starts_with("s3.") || host.starts_with("s3-"))
        && (host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn"))
    {
        return Ok(if host.ends_with(".cn") {
            "aws-cn"
        } else {
            "aws"
        }
        .into());
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}
#[cfg(test)]
fn destination(args: &Create) -> anyhow::Result<(Store, String)> {
    let (store, prefix, _) = configured(
        &args.to,
        args.to_env.as_deref(),
        Some(crate::parse_bucket(&args.bucket)?.0),
    )?;
    Ok((store, prefix))
}
fn disjoint(source: (&str, &str, &str), dest: (&str, &str, &str)) -> anyhow::Result<()> {
    let source_prefix = Key::from(source.2);
    let dest_prefix = Key::from(dest.2);
    let overlap = |a: &str, b: &str| {
        a.is_empty() || a == b || b.starts_with(&format!("{}/", a.trim_end_matches('/')))
    };
    ensure!(
        (matches!(source.0, "aws" | "aws-cn")
            && matches!(dest.0, "aws" | "aws-cn")
            && source.0 != dest.0)
            || source.1 != dest.1
            || !(overlap(source_prefix.as_ref(), dest_prefix.as_ref())
                || overlap(dest_prefix.as_ref(), source_prefix.as_ref())),
        "source and destination may overlap; choose a different bucket name or a non-overlapping prefix"
    );
    Ok(())
}
pub fn run(command: &Command) -> anyhow::Result<()> {
    crate::run_async(async {
        match command {
            Command::Prune(args) => {
                let (store, prefix, _) = configured(&args.from, None, None)?;
                prune::execute(&store, &prefix, args.keep_runs, now_ms()?).await
            }
            Command::Restore(args) => {
                // The archive's writes too (the undo run, its label): the
                // lock is held while any write of this restore may land.
                let settle = wasmkeep_server::settle::Settle::new();
                let (archive, ap, ae) =
                    configured_settled(&args.from, None, None, Some(Arc::clone(&settle)))?;
                let (target, tp, te) =
                    configured_settled(&args.bucket, None, None, Some(Arc::clone(&settle)))?;
                disjoint(
                    (&ae, crate::parse_bucket(&args.from)?.0, &ap),
                    (&te, crate::parse_bucket(&args.bucket)?.0, &tp),
                )?;
                restore::execute_settled(
                    args,
                    &archive,
                    &ap,
                    &target,
                    &tp,
                    wasmkeep_server::apps::LATE_WRITES,
                    settle,
                )
                .await
            }
            Command::Create(args) => {
                let (source, sp, se) = configured(&args.bucket, None, None)?;
                let (dest, dp, de) = configured(
                    &args.to,
                    args.to_env.as_deref(),
                    Some(crate::parse_bucket(&args.bucket)?.0),
                )?;
                disjoint(
                    (&se, crate::parse_bucket(&args.bucket)?.0, &sp),
                    (&de, crate::parse_bucket(&args.to)?.0, &dp),
                )?;
                create(args, &source, &sp, &dest, &dp).await.map(drop)
            }
            Command::List(args) => {
                let (store, prefix) = crate::open_bucket_unchecked(&args.from)?;
                let objects = list_bounded(&store, &format!("{}/", key(&prefix, "runs"))).await?;
                for path in objects.keys().filter(|p| p.ends_with("/started")) {
                    let id = path.trim_end_matches("/started");
                    safe_id(id)?;
                    match load(&store, &prefix, id).await {
                        Ok(run) => println!(
                            "{id}: complete ({} actors, {} failed)",
                            run.actors.len(),
                            array(&run.complete, "failed")?.len()
                        ),
                        Err(e) => println!("{id}: incomplete or invalid ({e})"),
                    }
                }
                Ok(())
            }
            Command::Verify(args) => {
                let (store, prefix) = crate::open_bucket_unchecked(&args.from)?;
                let id = match &args.run {
                    Some(id) => id.clone(),
                    None => latest(&store, &prefix).await?.context("no latest run")?,
                };
                verify(&store, &prefix, id.trim(), args.sample).await
            }
        }
    })
}
async fn latest(store: &Store, prefix: &str) -> anyhow::Result<Option<String>> {
    let Some(bytes) = optional(store, &key(prefix, "latest")).await? else {
        return Ok(None);
    };
    let reference: Value = serde_json::from_slice(&bytes)?;
    let id = text(&reference, "run")?;
    safe_id(id)?;
    checked_json(
        store,
        &key(prefix, &format!("runs/{id}/complete")),
        text(&reference, "sha256")?,
    )
    .await?;
    Ok(Some(id.to_owned()))
}
struct Run {
    complete: Value,
    actors: Vec<PathBuf>,
    _spool: tempfile::TempDir,
    apps: Vec<Value>,
}
async fn checked_json(store: &Store, path: &str, hash: &str) -> anyhow::Result<Value> {
    let bytes = read(store, path, META_MAX).await?;
    ensure!(digest(&bytes) == hash, "manifest digest mismatch: {path}");
    Ok(serde_json::from_slice(&bytes)?)
}
async fn load(store: &Store, prefix: &str, id: &str) -> anyhow::Result<Run> {
    load_with_budget(store, prefix, id, SPOOL_MAX, INDEX_MAX).await
}
async fn load_with_budget(
    store: &Store,
    prefix: &str,
    id: &str,
    spool_limit: u64,
    index_limit: u64,
) -> anyhow::Result<Run> {
    safe_id(id)?;
    let base = key(prefix, &format!("runs/{id}"));
    let root: Value = serde_json::from_slice(&read(store, &key(&base, "root"), META_MAX).await?)?;
    ensure!(root["run"] == id, "run root mismatch");
    let complete = checked_json(store, &key(&base, "complete"), text(&root, "sha256")?).await?;
    let started = checked_json(
        store,
        &key(&base, "started"),
        text(&complete, "started_sha256")?,
    )
    .await?;
    ensure!(
        started["run"] == id
            && started["incarnations"] == complete["incarnations"]
            && started["source"] == complete["source"],
        "started membership mismatch"
    );
    ensure!(
        complete["format"] == 1 && complete["run"] == id,
        "unknown or inconsistent backup format"
    );
    ensure!(
        encode_bounded(&complete, INDEX_MAX)?.len() as u64 <= index_limit,
        "backup resident index budget exceeded"
    );
    let spool = crate::private_tempdir()?;
    let mut actors = Vec::new();
    let mut pending: Option<std::fs::File> = None;
    let mut actor_bytes = 0;
    let mut disk_bytes = 0;
    let mut index_bytes = 0;
    charge(
        &mut index_bytes,
        encode_bounded(&complete, INDEX_MAX)?.len() as u64,
        index_limit,
        "resident index",
    )?;
    charge(
        &mut index_bytes,
        encode_bounded(&started, INDEX_MAX)?.len() as u64,
        index_limit,
        "resident index",
    )?;
    let mut part = 0;
    let mut apps = Vec::new();
    let mut seen = BTreeSet::new();
    for chunk in array(&complete, "chunks")? {
        let path = text(chunk, "key")?;
        charge(
            &mut index_bytes,
            path.len() as u64 + 128,
            index_limit,
            "resident index",
        )?;
        ensure!(
            path.starts_with(&format!("{base}/actors/")) && seen.insert(path.to_owned()),
            "invalid/duplicate chunk"
        );
        let value = checked_json(store, path, text(chunk, "sha256")?).await?;
        for fragment in value.as_array().context("actor chunk is not an array")? {
            ensure!(
                number(fragment, "part")? == if pending.is_none() { 0 } else { part },
                "actor fragment order mismatch"
            );
            let fragment_bytes = text(fragment, "json")?.as_bytes();
            charge(
                &mut actor_bytes,
                fragment_bytes.len() as u64,
                ACTOR_MAX,
                "actor memory",
            )?;
            charge(
                &mut disk_bytes,
                fragment_bytes.len() as u64,
                spool_limit,
                "spool disk",
            )?;
            let path = spool.path().join(format!("{:08}.json", actors.len()));
            if pending.is_none() {
                pending = Some(std::fs::File::create(&path)?);
            }
            std::io::Write::write_all(pending.as_mut().context("missing spool")?, fragment_bytes)?;
            part += 1;
            if fragment["last"] == true {
                drop(pending.take());
                let actor = disk_actor(&path)?;
                ensure!(
                    actor["run"] == id && actor["started_sha256"] == complete["started_sha256"],
                    "actor run binding mismatch"
                );
                charge(
                    &mut index_bytes,
                    path.as_os_str().len() as u64 + text(&actor, "prefix")?.len() as u64 + 256,
                    index_limit,
                    "resident index",
                )?;
                actors.push(path);
                actor_bytes = 0;
                part = 0;
                ensure!(
                    actor["status"] == "copied" || actor["status"] == "failed",
                    "invalid actor status"
                );
            }
        }
    }
    ensure!(pending.is_none(), "unfinished actor fragments");
    ensure!(
        actors.len() as u64 == number(&complete, "actors")?,
        "actor count mismatch"
    );
    let mut addresses = BTreeSet::new();
    let mut failures = Vec::new();
    for path in &actors {
        let actor = disk_actor(path)?;
        ensure!(
            addresses.insert(text(&actor, "prefix")?.to_owned()),
            "duplicate actor"
        );
        if actor["status"] == "failed" {
            charge(
                &mut index_bytes,
                encode_bounded(&actor["failure"], ACTOR_MAX)?.len() as u64,
                index_limit,
                "resident index",
            )?;
            failures.push(actor["failure"].clone());
        }
    }
    ensure!(
        json!(failures) == complete["failed"],
        "failed membership mismatch"
    );
    for app in array(&complete, "apps")? {
        let path = text(app, "key")?;
        ensure!(
            path.starts_with(&format!("{base}/app/")) && seen.insert(path.to_owned()),
            "invalid/duplicate app record"
        );
        let app = checked_json(store, path, text(app, "sha256")?).await?;
        charge(
            &mut index_bytes,
            serde_json::to_vec(&app)?.len() as u64,
            index_limit,
            "resident index",
        )?;
        ensure!(
            app["run"] == id && app["started_sha256"] == complete["started_sha256"],
            "app run binding mismatch"
        );
        apps.push(app);
    }
    ensure!(!apps.is_empty(), "missing app records");
    let mut membership = BTreeSet::new();
    for app in &apps {
        ensure!(
            membership.insert(text(app, "app")?),
            "duplicate app membership"
        );
    }
    let incarnations = complete["incarnations"]
        .as_object()
        .context("invalid incarnations")?;
    ensure!(
        membership == incarnations.keys().map(String::as_str).collect(),
        "app incarnation membership mismatch"
    );
    array(&complete, "failed")?;
    Ok(Run {
        complete,
        _spool: spool,
        actors,
        apps,
    })
}
fn disk_actor(path: &std::path::Path) -> anyhow::Result<Value> {
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= ACTOR_MAX,
        "backup actor memory budget exceeded"
    );
    Ok(serde_json::from_reader(std::io::BufReader::new(file))?)
}
async fn blob(store: &Store, prefix: &str, object: &Value) -> anyhow::Result<Vec<u8>> {
    let hash = text(object, "sha256")?;
    ensure!(
        hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid object digest"
    );
    let bytes = read(
        store,
        &key(prefix, &format!("data/{hash}")),
        MAX + (MAX / 16) + (1 << 20),
    )
    .await?;
    ensure!(
        digest(&bytes) == hash && bytes.len() as u64 == number(object, "size")?,
        "missing or corrupt backup object {hash}"
    );
    Ok(bytes)
}
async fn check_boundary(
    dest: &Store,
    dp: &str,
    keys: &[String],
    objects: &serde_json::Map<String, Value>,
) -> anyhow::Result<()> {
    let listing = Listing::from_keys(keys.iter().map(String::as_str));
    let plan = plan_restore(&listing)?;
    if let Some(id) = plan.snapshot
        && let Some(segment) = listing.segments.iter().find(|s| s.id == id)
    {
        let snapshot = wasmkeep_cluster::replication::snapshot_key(id);
        let segment = wasmkeep_cluster::replication::segment_key(segment);
        let mut checksums = Vec::new();
        for name in [snapshot, segment] {
            let bytes = blob(
                dest,
                dp,
                objects.get(&name).context("snapshot boundary omitted")?,
            )
            .await?;
            let (_, trailer) =
                wasmkeep_storage::ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
                    .map_err(|e| anyhow::anyhow!("invalid boundary LTX: {e:?}"))?;
            checksums.push(trailer.post_apply_checksum);
        }
        ensure!(
            checksums[0] == checksums[1],
            "snapshot differs from its boundary segment checksum"
        );
    }
    Ok(())
}
async fn replay(dest: &Store, dp: &str, actor: &Value) -> anyhow::Result<history::Export> {
    let tmp = crate::private_tempdir()?;
    let local: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        tmp.path(),
    )?);
    let objects = actor["objects"].as_object().context("missing object map")?;
    let plan = array(actor, "plan")?;
    ensure!(!plan.is_empty(), "empty actor plan");
    let keys = plan
        .iter()
        .map(|v| v.as_str().map(str::to_owned).context("invalid plan key"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    check_boundary(dest, dp, &keys, objects).await?;
    let mut seen = BTreeSet::new();
    for value in plan {
        let path = value.as_str().context("invalid plan key")?;
        ensure!(seen.insert(path), "duplicate plan key");
        let listing = Listing::from_keys([path]);
        ensure!(
            listing.segments.len() + listing.snapshots.len() == 1,
            "invalid history key"
        );
        let bytes = blob(dest, dp, objects.get(path).context("plan object omitted")?).await?;
        put(&local, path, bytes, false).await?;
    }
    let result = history::replay(&local, "", &tmp.path().join("verified.db"), MAX).await?;
    ensure!(
        result.position.generation == number(actor, "generation")?
            && result.position.txid == number(actor, "txid")?
            && result.raw_checksum == number(actor, "checksum")?,
        "replay differs from recorded head/checksum"
    );
    let (alarm, indexed) =
        wasmkeep_storage::readonly::backup_alarm(&tmp.path().join("verified.db"))?;
    ensure!(
        actor["alarm"] == json!(alarm) && actor["alarm_indexed"] == json!(indexed),
        "manifest alarm differs from replayed database"
    );
    ensure!(
        actor["timestamp_ms"] == json!(result.timestamp_ms),
        "manifest capture time differs from replay"
    );
    Ok(result)
}
// Index recoverability once in transaction order. Parents always have a lower
// txid; generation ordering must also satisfy the historical target subset.
fn reachable_positions(listing: &Listing) -> BTreeSet<wasmkeep_cluster::replication::SegmentId> {
    use wasmkeep_cluster::replication::EMPTY;
    let mut reachable: BTreeSet<_> = listing.snapshots.iter().copied().collect();
    let mut segments = listing.segments.clone();
    segments.sort_by_key(|s| (s.id.txid, s.id.generation));
    for segment in segments {
        if segment.parent() == EMPTY
            || (segment.parent() <= segment.id && reachable.contains(&segment.parent()))
        {
            reachable.insert(segment.id);
        }
    }
    reachable
}
fn recoverable_ranges(actor: &Value) -> anyhow::Result<Vec<Value>> {
    let objects = actor["objects"].as_object().context("missing objects")?;
    let listing = Listing::from_keys(objects.keys().map(String::as_str));
    let mut ranges: Vec<Value> = Vec::new();
    for target in reachable_positions(&listing) {
        if let Some(last) = ranges.last_mut()
            && last["generation"] == target.generation
            && last["end"].as_u64().and_then(|n| n.checked_add(1)) == Some(target.txid)
        {
            last["end"] = json!(target.txid);
        } else {
            ranges.push(
                json!({"generation":target.generation,"start":target.txid,"end":target.txid}),
            );
        }
    }
    Ok(ranges)
}

async fn verify_ranges(store: &Store, prefix: &str, actor: &Value) -> anyhow::Result<()> {
    use wasmkeep_cluster::replication::{EMPTY, segment_key, snapshot_key};
    use wasmkeep_storage::{ltx, readonly, wal};
    let ranges = recoverable_ranges(actor)?;
    ensure!(
        actor["ranges"] == json!(ranges),
        "recoverable range mismatch"
    );
    let objects = actor["objects"].as_object().context("missing objects")?;
    let listing = Listing::from_keys(objects.keys().map(String::as_str));
    let reachable = reachable_positions(&listing);
    let segments: BTreeMap<_, _> = listing.segments.iter().map(|s| (s.id, *s)).collect();
    ensure!(
        segments.len() == listing.segments.len(),
        "ambiguous history position"
    );
    let snapshots: BTreeSet<_> = listing.snapshots.iter().copied().collect();
    let mut children: BTreeMap<_, Vec<_>> = BTreeMap::new();
    let mut pending = Vec::new();
    for id in &reachable {
        if let Some(segment) = segments.get(id)
            && segment.parent() <= *id
            && (segment.parent() == EMPTY || reachable.contains(&segment.parent()))
        {
            if segment.parent() == EMPTY {
                pending.push((*id, None));
            } else {
                children.entry(segment.parent()).or_default().push(*id);
            }
        } else {
            pending.push((*id, None));
        }
    }
    let tmp = crate::private_tempdir()?;
    let mut sequence = 0u64;
    let mut checked = 0;
    while let Some((id, parent)) = pending.pop() {
        let path = tmp.path().join(format!("{sequence}.db"));
        sequence += 1;
        let mut chain_checksum = None;
        if parent.is_some() || segments.get(&id).is_some_and(|s| s.parent() == EMPTY) {
            let mut restorer = if let Some((parent_path, checksum)) = parent {
                std::fs::rename(parent_path, &path)?;
                wal::Restorer::resume(&path, MAX, checksum)?
            } else {
                wal::Restorer::create(&path, MAX)?
            };
            let name = segment_key(segments.get(&id).context("range link missing")?);
            let bytes = blob(
                store,
                prefix,
                objects.get(&name).context("range object missing")?,
            )
            .await?;
            check_range_header(&bytes, id, false)?;
            restorer.apply(&bytes)?;
            let checksum = restorer.finish()?.value();
            wal::Restorer::resume(&path, MAX, checksum)?.finish()?;
            readonly::verify(&path)?;
            chain_checksum = Some(checksum);
        }
        if snapshots.contains(&id) {
            let name = snapshot_key(id);
            let bytes = blob(
                store,
                prefix,
                objects.get(&name).context("snapshot missing")?,
            )
            .await?;
            check_range_header(&bytes, id, true)?;
            let snapshot_path = tmp.path().join("snapshot.db");
            let mut restorer = wal::Restorer::create(&snapshot_path, MAX)?;
            restorer.apply(&bytes)?;
            let checksum = restorer.finish()?.value();
            wal::Restorer::resume(&snapshot_path, MAX, checksum)?.finish()?;
            readonly::verify(&snapshot_path)?;
            if let Some(chain) = chain_checksum {
                ensure!(
                    chain == checksum,
                    "snapshot differs from its boundary segment checksum"
                );
                std::fs::remove_file(snapshot_path)?;
            } else {
                // Even an unrecoverable boundary segment must agree with the snapshot.
                if let Some(segment) = segments.get(&id) {
                    let name = segment_key(segment);
                    let bytes = blob(
                        store,
                        prefix,
                        objects.get(&name).context("boundary missing")?,
                    )
                    .await?;
                    check_range_header(&bytes, id, false)?;
                    let (_, trailer) = ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
                        .map_err(|e| anyhow::anyhow!("invalid boundary LTX: {e:?}"))?;
                    ensure!(
                        trailer.post_apply_checksum == checksum,
                        "snapshot differs from its boundary segment checksum"
                    );
                }
                std::fs::rename(snapshot_path, &path)?;
                chain_checksum = Some(checksum);
            }
        }
        let checksum = chain_checksum.context("range position has no base")?;
        checked += 1;
        // Branches get private raw files; linear history reuses the same file.
        let next = children.remove(&id).unwrap_or_default();
        for (index, child) in next.iter().enumerate() {
            let child_path = tmp.path().join(format!("pending-{sequence}-{index}.db"));
            if index + 1 == next.len() {
                std::fs::rename(&path, &child_path)?;
            } else {
                std::fs::copy(&path, &child_path)?;
            }
            pending.push((*child, Some((child_path, checksum))));
        }
        if next.is_empty() {
            std::fs::remove_file(path)?;
        }
    }
    ensure!(checked == reachable.len(), "range positions omitted");
    println!(
        "{} recoverable ranges: {}",
        text(actor, "prefix")?,
        json!(ranges)
    );
    Ok(())
}

fn check_range_header(
    bytes: &[u8],
    id: wasmkeep_cluster::replication::SegmentId,
    snapshot: bool,
) -> anyhow::Result<()> {
    use wasmkeep_storage::ltx;
    let header = ltx::Header::decode(bytes.get(..ltx::HEADER_SIZE).context("short range LTX")?)?;
    ensure!(
        !header.no_checksum()
            && header.max_txid == id.txid
            && header.min_txid == if snapshot { 1 } else { id.txid },
        "range LTX header does not match position"
    );
    Ok(())
}
async fn verify(store: &Store, prefix: &str, id: &str, sample: Option<u64>) -> anyhow::Result<()> {
    verify_mode(store, prefix, id, sample, false, true).await
}

async fn verify_mode(
    store: &Store,
    prefix: &str,
    id: &str,
    sample: Option<u64>,
    partial: bool,
    record: bool,
) -> anyhow::Result<()> {
    let run = load(store, prefix, id).await?;
    ensure!(
        partial || array(&run.complete, "failed")?.is_empty(),
        "partial run: verification refuses failed actors"
    );
    let (_, source_prefix) = crate::parse_bucket(text(&run.complete, "source")?)?;
    let mut covered = BTreeSet::new();
    for app in &run.apps {
        let app_name = text(app, "app")?;
        crate::parse_app(app_name).map_err(anyhow::Error::msg)?;
        let actor_prefix = format!(
            "{}/",
            key(source_prefix, &format!("state/{app_name}/actors"))
        );
        let mut members = BTreeMap::new();
        for path in &run.actors {
            let actor = disk_actor(path)?;
            let address = text(&actor, "prefix")?;
            if address.starts_with(&actor_prefix) {
                ensure!(
                    covered.insert(address.to_owned()),
                    "actor belongs to duplicate app records"
                );
                members.insert(address.to_owned(), actor["status"].clone());
            }
        }
        ensure!(json!(members) == app["members"], "app membership mismatch");
        ensure!(
            members.len() as u64 == number(app, "actors")?,
            "app actor count mismatch"
        );
        verify_app(store, prefix, source_prefix, app).await?;
    }
    ensure!(
        covered.len() == run.actors.len(),
        "manifest actors lack app records"
    );
    // Random sample by fresh entropy, independent of manifest ordering.
    let nonce = crate::private_tempdir()?;
    let mut actors: Vec<_> = run.actors.iter().collect();
    actors.sort_by_key(|a| digest(format!("{}{}", nonce.path().display(), a.display()).as_bytes()));
    let selected = sample.map_or(actors.len(), |n| {
        usize::try_from(n).unwrap_or(usize::MAX).min(actors.len())
    });
    let mut count = 0;
    for actor in actors.into_iter().take(selected) {
        let actor = disk_actor(actor)?;
        if partial && actor["status"] == "failed" {
            continue;
        }
        replay(store, prefix, &actor).await?;
        count += 1;
        verify_ranges(store, prefix, &actor).await?;
        // `all` also promises retained history, not only the head's plan.
        for object in actor["objects"]
            .as_object()
            .context("missing objects")?
            .values()
        {
            let bytes = blob(store, prefix, object).await?;
            wasmkeep_storage::ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
                .map_err(|e| anyhow::anyhow!("invalid archived LTX: {e:?}"))?;
        }
    }
    if record {
        json_put(
            store,
            &key(prefix, &format!("runs/{id}/verified")),
            &json!({"checked":count,"total":run.actors.len(),"sample":sample,"at_ms":now_ms()?}),
            true,
        )
        .await?;
    }
    println!(
        "Verified {count}/{} actors in {id}{}",
        run.actors.len(),
        if sample.is_some() {
            " (sample only)"
        } else {
            ""
        }
    );
    Ok(())
}
fn now_ms() -> anyhow::Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis())
}
async fn save_blob(
    dest: &Store,
    dp: &str,
    source_key: &str,
    bytes: Vec<u8>,
    meta: Option<&ObjectMeta>,
    cache: Option<&std::path::Path>,
) -> anyhow::Result<Value> {
    let hash = digest(&bytes);
    let entry = json!({"key":source_key,"sha256":hash,"size":bytes.len(),"etag":meta.and_then(|m|m.e_tag.as_deref()),"version":meta.and_then(|m|m.version.as_deref())});
    if let Some(dir) = cache {
        std::fs::create_dir_all(dir)?;
        let mut file = tempfile::NamedTempFile::new_in(dir)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        file.as_file().sync_all()?;
        match file.persist_noclobber(dir.join(&hash)) {
            Ok(_) => {}
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.error.into()),
        }
    }
    put(dest, &key(dp, &format!("data/{hash}")), bytes, false).await?;
    Ok(entry)
}
/// How many times one actor is listed, planned and read before the run
/// gives it up (ADR 0033, section 1).
const ATTEMPTS: usize = 8;
/// Whether `key`, absent from `listing`, is a key collection could have
/// deleted: below a listed snapshot whose segment is listed too, by the
/// shared rule (`collectable`, which the idle pass's `idle_collectable`
/// applies as well). The tool sees neither the retention window nor the
/// ownership record: this judges the shape collection leaves, the shape a
/// listing made after the collection would have shown.
fn collected_by_rule(listing: &Listing, key: &str) -> bool {
    use wasmkeep_cluster::replication::collectable;
    let gone = Listing::from_keys([key]);
    if gone.segments.is_empty() && gone.snapshots.is_empty() {
        return false;
    }
    let mut with = listing.clone();
    with.segments.extend(gone.segments);
    with.snapshots.extend(gone.snapshots);
    listing
        .snapshots
        .iter()
        .filter(|s| listing.segments.iter().any(|seg| seg.id == **s))
        .any(|s| {
            collectable(&with, s.generation, *s)
                .iter()
                .any(|k| k == key)
        })
}
fn same(meta: &ObjectMeta, old: &Value) -> bool {
    meta.e_tag.is_some()
        && old["size"].as_u64() == Some(meta.size)
        && old["etag"].as_str() == meta.e_tag.as_deref()
        && old["version"].as_str() == meta.version.as_deref()
}
struct Copier<'a> {
    source: &'a Store,
    dest: &'a Store,
    dp: &'a str,
    cache: Option<&'a std::path::Path>,
    all: bool,
}
impl Copier<'_> {
    async fn actor(
        &self,
        prefix: &str,
        initial: BTreeMap<String, ObjectMeta>,
        previous: Option<&Value>,
    ) -> anyhow::Result<Value> {
        let mut objects = initial;
        let mut reason = String::new();
        let mut expected: BTreeSet<_> = objects.keys().cloned().collect();
        let mut validated = serde_json::Map::new();
        let mut relisted = None;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                objects = match relisted.take() {
                    Some(objects) => objects,
                    None => list_bounded(self.source, prefix).await?,
                };
            }
            validated.retain(|k, entry| objects.get(k).is_none_or(|meta| same(meta, entry)));
            expected.extend(objects.keys().cloned());
            let mut observed_bytes = 0;
            for name in &expected {
                charge(
                    &mut observed_bytes,
                    name.len() as u64 + 256,
                    ACTOR_MAX,
                    "actor observations memory",
                )?;
            }
            let bytes = objects
                .values()
                .map(|m| m.size)
                .fold(0u64, u64::saturating_add);
            let deadline = std::time::Duration::from_secs(
                30u64
                    .saturating_add(bytes.div_ceil(1 << 20))
                    .saturating_add((objects.len() as u64).div_ceil(8)),
            );
            match tokio::time::timeout(
                deadline,
                self.attempt(prefix, &objects, previous, &mut validated),
            )
            .await
            .context("backup actor copy deadline exceeded")?
            {
                Ok(mut actor) => {
                    let copied = actor["objects"].as_object().context("missing objects")?;
                    let mut gaps: Vec<_> = if self.all {
                        expected
                            .iter()
                            .filter(|k| !copied.contains_key(*k))
                            .cloned()
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let mut collected = Vec::new();
                    if !gaps.is_empty() {
                        // Listed, then gone before its read: list again. A
                        // key gone from below a listed snapshot and its
                        // segment, by the collection rule (ADR 0012; ADR
                        // 0042's idle pass applies the same), is what a
                        // collection leaves, as if it had run before the
                        // listing: not a gap. Anything else is one.
                        let now = list_bounded(self.source, prefix).await?;
                        let listing = Listing::from_keys(now.keys().map(String::as_str));
                        (collected, gaps) = gaps
                            .into_iter()
                            .partition(|k| !now.contains_key(k) && collected_by_rule(&listing, k));
                        // A gap left while the actor changed, or a read
                        // that missed a key still listed: list, plan and
                        // read again, within the attempts. Then the gaps
                        // stand.
                        if !gaps.is_empty()
                            && attempt + 1 < ATTEMPTS
                            && (gaps.iter().any(|k| now.contains_key(k))
                                || !now.keys().eq(objects.keys()))
                        {
                            reason = format!("retained history gone while read: {gaps:?}");
                            relisted = Some(now);
                            continue;
                        }
                    }
                    actor["gaps"] = json!(gaps);
                    actor["collected"] = json!(collected);
                    return Ok(actor);
                }
                Err(e)
                    if matches!(
                        e.downcast_ref::<history::Error>(),
                        Some(history::Error::Missing(_) | history::Error::Stale(_))
                    ) =>
                {
                    reason = e.to_string()
                }
                Err(e)
                    if e.downcast_ref::<wasmkeep_cluster::replication::PlanError>()
                        .is_some() =>
                {
                    reason = e.to_string()
                }
                Err(e) => return Err(e),
            }
        }
        bail!(
            "{reason}; collected faster than read: raise --history-retention-secs ({ATTEMPTS} attempts)"
        )
    }
    async fn cached_replay(
        &self,
        prefix: &str,
        previous: Option<&Value>,
        plan: &wasmkeep_cluster::replication::Plan,
        listing: &Listing,
        objects: &serde_json::Map<String, Value>,
        output: &std::path::Path,
    ) -> anyhow::Result<Option<history::Export>> {
        use wasmkeep_storage::{readonly, wal};
        let Some((cache, previous)) = self.cache.zip(previous) else {
            return Ok(None);
        };
        let position = wasmkeep_cluster::replication::SegmentId {
            generation: number(previous, "generation")?,
            txid: number(previous, "txid")?,
        };
        let offset = if plan.snapshot == Some(position) {
            0
        } else if let Some(index) = plan.segments.iter().position(|id| *id == position) {
            index + 1
        } else {
            return Ok(None);
        };
        // A cache cannot prove a newly introduced snapshot behind its head.
        // Reuse only the previously verified base, or compare a snapshot at
        // the cached position directly with that position's checksum.
        if let Some(snapshot) = plan.snapshot {
            let name = wasmkeep_cluster::replication::snapshot_key(snapshot);
            let entry = objects.get(&name).context("snapshot omitted")?;
            if snapshot == position {
                let bytes = blob(self.dest, self.dp, entry).await?;
                let (_, trailer) =
                    wasmkeep_storage::ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
                        .map_err(|e| anyhow::anyhow!("invalid cached-boundary snapshot: {e:?}"))?;
                ensure!(
                    trailer.post_apply_checksum == number(previous, "checksum")?,
                    "new snapshot differs from cached checksum at its position"
                );
            } else if previous["objects"][&name]["sha256"] != entry["sha256"] {
                return Ok(None);
            }
        }
        let cached = cache.join(format!("{}.db", digest(prefix.as_bytes())));
        if !cached.is_file() || std::fs::metadata(&cached)?.len() > MAX {
            return Ok(None);
        }
        std::fs::copy(cached, output)?;
        let mut restorer = match wal::Restorer::resume(output, MAX, number(previous, "checksum")?) {
            Ok(restorer) => restorer,
            Err(_) => {
                std::fs::remove_file(output)?;
                return Ok(None);
            }
        };
        let mut stamp = previous["timestamp_ms"]
            .as_i64()
            .context("invalid cached timestamp")?;
        for id in &plan.segments[offset..] {
            let segment = listing
                .segments
                .iter()
                .find(|s| s.id == *id)
                .context("cached chain missing link")?;
            let name = wasmkeep_cluster::replication::segment_key(segment);
            let entry = objects.get(&name).context("cached chain object omitted")?;
            let bytes = blob(self.dest, self.dp, entry).await?;
            let header = wasmkeep_storage::ltx::Header::decode(
                bytes
                    .get(..wasmkeep_storage::ltx::HEADER_SIZE)
                    .context("short cached segment")?,
            )?;
            ensure!(
                header.min_txid == id.txid && header.max_txid == id.txid && !header.no_checksum(),
                "cached segment position mismatch"
            );
            stamp = header.timestamp_ms;
            restorer.apply(&bytes)?;
        }
        let checksum = restorer.finish()?.value();
        // Independent full-file checksum, just as a fresh replay requires.
        wal::Restorer::resume(output, MAX, checksum)?.finish()?;
        let verification = readonly::verify(output)?;
        Ok(Some(history::Export {
            position: plan.head(),
            listed_head: plan.head(),
            timestamp_ms: stamp,
            owner_generation: None,
            raw_checksum: checksum,
            objects: plan.segments.len() - offset,
            attempts: 1,
            bytes: std::fs::metadata(output)?.len(),
            verification,
            owner_recorded: None,
        }))
    }

    async fn attempt(
        &self,
        prefix: &str,
        listed: &BTreeMap<String, ObjectMeta>,
        previous: Option<&Value>,
        validated: &mut serde_json::Map<String, Value>,
    ) -> anyhow::Result<Value> {
        let listing = Listing::from_keys(listed.keys().map(String::as_str));
        let plan = plan_restore(&listing)?;
        ensure!(plan.head().txid > 0, "actor has no retained history");
        let keys = history::copy_keys(&listing, &plan)?;
        let old = previous.and_then(|p| p["objects"].as_object());
        let unchanged = previous.is_some_and(|p| {
            // A run before files recorded no namespace: read it again.
            p.get("blob_namespace").is_some()
                && p["generation"].as_u64() == Some(plan.head().generation)
                && p["txid"].as_u64() == Some(plan.head().txid)
        }) && keys.iter().all(|k| {
            listed
                .get(k)
                .zip(old.and_then(|o| o.get(k)))
                .is_some_and(|(m, o)| same(m, o))
        });
        if unchanged
            && validated.is_empty()
            && (!self.all
                || listed
                    .iter()
                    .all(|(k, m)| old.and_then(|o| o.get(k)).is_some_and(|o| same(m, o))))
        {
            return Ok(previous.context("previous entry")?.clone());
        }
        let mut copied = if self.all {
            old.cloned().unwrap_or_default()
        } else {
            serde_json::Map::new()
        };
        // A disappeared key keeps its validated reference; a key observed with
        // a new identity must never inherit the earlier bytes, even if GET fails.
        validated.retain(|k, entry| listed.get(k).is_none_or(|meta| same(meta, entry)));
        copied.extend(validated.clone());
        let mut wanted = keys.clone();
        if self.all {
            for k in listed.keys() {
                if !wanted.contains(k) {
                    wanted.push(k.clone());
                }
            }
        }
        for k in &wanted {
            let meta = listed.get(k).context("plan key missing from listing")?;
            if let Some(entry) = validated
                .get(k)
                .or_else(|| old.and_then(|o| o.get(k)))
                .filter(|o| same(meta, o))
            {
                copied.insert(k.clone(), entry.clone());
                continue;
            }
            let bytes = match history::get(
                self.source,
                &meta.location,
                Some(meta),
                MAX + MAX / 16 + (1 << 20),
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(history::Error::Missing(_)) if !keys.contains(k) => continue,
                Err(e) => return Err(e.into()),
            };
            let header = wasmkeep_storage::ltx::Header::decode(
                bytes
                    .get(..wasmkeep_storage::ltx::HEADER_SIZE)
                    .context("short LTX")?,
            )?;
            wasmkeep_storage::ltx::visit(&bytes, |_| Ok::<_, ()>(()), |_, _| Ok(()))
                .map_err(|e| anyhow::anyhow!("invalid LTX: {e:?}"))?;
            let mut entry = save_blob(
                self.dest,
                self.dp,
                &format!("{prefix}{k}"),
                bytes.to_vec(),
                Some(meta),
                self.cache,
            )
            .await?;
            entry["timestamp_ms"] = json!(header.timestamp_ms);
            validated.insert(k.clone(), entry.clone());
            copied.insert(k.clone(), entry);
        }
        check_boundary(self.dest, self.dp, &keys, &copied).await?;
        let tmp = crate::private_tempdir()?;
        let output = tmp.path().join("actor.db");
        let result = if let Some(result) = self
            .cached_replay(prefix, previous, &plan, &listing, &copied, &output)
            .await?
        {
            result
        } else {
            let local: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
                tmp.path(),
            )?);
            for k in &keys {
                let entry = copied.get(k).context("missing copied plan object")?;
                let hash = text(entry, "sha256")?;
                let cached = self
                    .cache
                    .and_then(|dir| std::fs::read(dir.join(hash)).ok())
                    .filter(|b| digest(b) == hash);
                let bytes = match cached {
                    Some(bytes) => bytes,
                    None => blob(self.dest, self.dp, entry).await?,
                };
                put(&local, k, bytes, false).await?;
            }
            history::replay(&local, "", &output, MAX).await?
        };
        ensure!(result.position == plan.head(), "copied plan changed head");
        let (alarm, alarm_indexed) =
            wasmkeep_storage::readonly::backup_alarm(&tmp.path().join("actor.db"))?;
        if let Some(cache) = self.cache {
            let target = cache.join(format!("{}.db", digest(prefix.as_bytes())));
            let mut staged = tempfile::NamedTempFile::new_in(cache)?;
            std::io::copy(
                &mut std::fs::File::open(tmp.path().join("actor.db"))?,
                &mut staged,
            )?;
            staged.as_file().sync_all()?;
            staged.persist(target).map_err(|e| e.error)?;
        }
        let blob_namespace = files::actor_namespace(
            prefix,
            &tmp.path().join("actor.db"),
            result.verification.replaced.as_deref(),
        )?;
        let mut actor = json!({"prefix":prefix,"blob_namespace":blob_namespace,"generation":result.position.generation,"txid":result.position.txid,"checksum":result.raw_checksum,"timestamp_ms":result.timestamp_ms,"replayed_objects":result.objects,"plan":keys,"objects":copied,"replacement":result.verification.replaced,"foreign_key_violations":result.verification.foreign_key_violations,"alarm":alarm,"alarm_indexed":alarm_indexed});
        actor["ranges"] = json!(recoverable_ranges(&actor)?);
        actor["gaps"] = json!([]);
        if self.all {
            verify_ranges(self.dest, self.dp, &actor).await?;
        }
        if let Some(cache) = self.cache {
            let mut sidecar = tempfile::NamedTempFile::new_in(cache)?;
            serde_json::to_writer(&mut sidecar, &actor)?;
            sidecar.as_file().sync_all()?;
            sidecar
                .persist(cache.join(format!("{}.json", digest(prefix.as_bytes()))))
                .map_err(|e| e.error)?;
        }
        Ok(actor)
    }
}
#[cfg(test)]
async fn incarnation(source: &Store, base: &str) -> anyhow::Result<Option<Vec<u8>>> {
    incarnation_for(source, base, None).await
}
async fn incarnation_for(
    source: &Store,
    base: &str,
    undo_fence: Option<&[u8]>,
) -> anyhow::Result<Option<Vec<u8>>> {
    ensure!(
        optional(source, &key(base, "restoring")).await?.as_deref() == undo_fence,
        "app is restoring (or undo fence changed)"
    );
    ensure!(
        optional(source, &key(base, "deleted")).await?.is_none(),
        "app is deleted"
    );
    let recreated = optional(source, &key(base, "recreated")).await?;
    let restored = optional(source, &key(base, "restored")).await?;
    let marker = if recreated.is_none() && restored.is_none() {
        None
    } else {
        Some(encode_bounded(&json!([recreated, restored]), 64 << 10)?)
    };
    ensure!(
        marker.as_ref().is_none_or(|bytes| bytes.len() <= 64 << 10),
        "backup incarnation memory budget exceeded (64 KiB)"
    );
    Ok(marker)
}
fn insert_metadata(
    objects: &mut BTreeMap<String, Value>,
    used: &mut u64,
    name: String,
    value: Value,
) -> anyhow::Result<()> {
    charge(
        used,
        name.len() as u64 + encode_bounded(&value, ACTOR_MAX)?.len() as u64 + 128,
        INDEX_MAX,
        "app metadata memory",
    )?;
    objects.insert(name, value);
    Ok(())
}
async fn app_metadata(
    source: &Store,
    sp: &str,
    dest: &Store,
    dp: &str,
    tenant: &str,
    app: &str,
) -> anyhow::Result<Value> {
    use wasmkeep_server::apps::{Current, Manifest};
    let base = key(sp, &format!("apps/{tenant}/{app}"));
    let mut objects = BTreeMap::new();
    let mut metadata_bytes = 0;
    for family in ["", "secrets/"] {
        let current_key = key(&base, &format!("{family}current"));
        let mut settled = false;
        for _ in 0..8 {
            let current = optional(source, &current_key).await?;
            if let Some(bytes) = &current {
                if family.is_empty() {
                    let c = Current::parse(bytes).context("invalid deploy current")?;
                    ensure!(
                        c.rooted,
                        "legacy app layout must be migrated before online backup"
                    );
                    let mut version = Some(c.version);
                    for _ in 0..6 {
                        let Some(v) = version.take() else {
                            break;
                        };
                        let vp = key(&base, &v);
                        let manifest_key = key(&vp, "manifest");
                        let manifest_bytes = read(source, &manifest_key, META_MAX).await?;
                        let manifest = Manifest::parse(std::str::from_utf8(&manifest_bytes)?)
                            .context("invalid deploy manifest")?;
                        insert_metadata(
                            &mut objects,
                            &mut metadata_bytes,
                            manifest_key.clone(),
                            save_blob(dest, dp, &manifest_key, manifest_bytes, None, None).await?,
                        )?;
                        for (class, hash) in &manifest.classes {
                            let path = key(&vp, &format!("{class}.wasm"));
                            let bytes = read(source, &path, MAX).await?;
                            ensure!(digest(&bytes) == *hash, "component digest mismatch");
                            insert_metadata(
                                &mut objects,
                                &mut metadata_bytes,
                                path.clone(),
                                save_blob(dest, dp, &path, bytes, None, None).await?,
                            )?;
                        }
                        if let Some(hash) = &manifest.front {
                            let path = key(&vp, "_front.wasm");
                            let bytes = read(source, &path, MAX).await?;
                            ensure!(digest(&bytes) == *hash, "front digest mismatch");
                            insert_metadata(
                                &mut objects,
                                &mut metadata_bytes,
                                path.clone(),
                                save_blob(dest, dp, &path, bytes, None, None).await?,
                            )?;
                        }
                        for (class, hash) in &manifest.config {
                            let path = key(&vp, &format!("{class}.config"));
                            let bytes = read(source, &path, META_MAX).await?;
                            ensure!(digest(&bytes) == *hash, "config digest mismatch");
                            insert_metadata(
                                &mut objects,
                                &mut metadata_bytes,
                                path.clone(),
                                save_blob(dest, dp, &path, bytes, None, None).await?,
                            )?;
                        }
                        // The version's assets: its index and every content
                        // it names, never a marker (ADR 0030, section 9).
                        if let Some(hash) = &manifest.assets {
                            let index_key = key(&base, &format!("_assets/index/{hash}"));
                            if !objects.contains_key(&index_key) {
                                let bytes = read(
                                    source,
                                    &index_key,
                                    wasmkeep_server::assets::MAX_INDEX as u64,
                                )
                                .await?;
                                ensure!(digest(&bytes) == *hash, "assets index digest mismatch");
                                let index = wasmkeep_server::assets::Index::parse(&bytes)
                                    .map_err(anyhow::Error::msg)?;
                                insert_metadata(
                                    &mut objects,
                                    &mut metadata_bytes,
                                    index_key.clone(),
                                    save_blob(dest, dp, &index_key, bytes, None, None).await?,
                                )?;
                                for file in index.files.values() {
                                    for content in file.contents() {
                                        let path = key(
                                            &base,
                                            &format!("_assets/content/{}", content.digest),
                                        );
                                        if objects.contains_key(&path) {
                                            continue;
                                        }
                                        let bytes =
                                            read(source, &path, wasmkeep_server::assets::MAX_FILE)
                                                .await?;
                                        ensure!(
                                            digest(&bytes) == content.digest,
                                            "asset digest mismatch"
                                        );
                                        insert_metadata(
                                            &mut objects,
                                            &mut metadata_bytes,
                                            path.clone(),
                                            save_blob(dest, dp, &path, bytes, None, None).await?,
                                        )?;
                                    }
                                }
                            }
                        }
                        version = manifest.previous;
                    }
                } else {
                    let version = std::str::from_utf8(bytes)?.trim();
                    ensure!(
                        version.len() == 16 && version.bytes().all(|b| b.is_ascii_hexdigit()),
                        "invalid secrets current"
                    );
                    let path = key(&base, &format!("secrets/{version}"));
                    let body = read(source, &path, MAX).await?;
                    insert_metadata(
                        &mut objects,
                        &mut metadata_bytes,
                        path.clone(),
                        save_blob(dest, dp, &path, body, None, None).await?,
                    )?;
                }
                insert_metadata(
                    &mut objects,
                    &mut metadata_bytes,
                    current_key.clone(),
                    save_blob(dest, dp, &current_key, bytes.clone(), None, None).await?,
                )?;
            } else if family.is_empty() {
                bail!("app has no deploy current");
            }
            if optional(source, &current_key).await? == current {
                settled = true;
                break;
            }
        }
        ensure!(settled, "deploy/secrets current kept changing");
    }
    Ok(json!({"app":format!("{tenant}/{app}"),"objects":objects.into_values().collect::<Vec<_>>()}))
}
async fn flush_chunk(
    dest: &Store,
    base: &str,
    chunks: &mut Vec<Value>,
    actors: &mut Vec<Value>,
) -> anyhow::Result<()> {
    if actors.is_empty() {
        return Ok(());
    }
    for actor in actors.iter() {
        let encoded = String::from_utf8(encode_bounded(actor, ACTOR_MAX)?)?;
        ensure!(
            encoded.len() as u64 <= ACTOR_MAX,
            "backup actor memory budget exceeded"
        );
        let mut rest = encoded.as_str();
        let mut part = 0;
        while !rest.is_empty() {
            let mut end = rest.len().min(1 << 20);
            while !rest.is_char_boundary(end) {
                end -= 1;
            }
            let (fragment, tail) = rest.split_at(end);
            let record = json!([{"part":part,"last":tail.is_empty(),"json":fragment}]);
            ensure!(
                (serde_json::to_vec(&record)?.len() as u64) < META_MAX,
                "manifest chunk exceeds cap"
            );
            let path = key(base, &format!("actors/{:08}", chunks.len()));
            let hash = json_put(dest, &path, &record, false).await?;
            chunks.push(json!({"key":path,"sha256":hash}));
            rest = tail;
            part += 1;
        }
    }
    actors.clear();
    Ok(())
}

async fn create(
    args: &Create,
    source: &Store,
    sp: &str,
    dest: &Store,
    dp: &str,
) -> anyhow::Result<String> {
    create_undo(args, source, sp, dest, dp, None).await
}

async fn create_undo(
    args: &Create,
    source: &Store,
    sp: &str,
    dest: &Store,
    dp: &str,
    undo_fence: Option<&[u8]>,
) -> anyhow::Result<String> {
    let started_at = now_ms()?;
    let nonce = crate::private_tempdir()?;
    let seconds = i64::try_from(now_ms()? / 1000)?;
    let stamp = chrono::DateTime::from_timestamp(seconds, 0)
        .context("clock out of range")?
        .format("%Y%m%dT%H%M%SZ");
    let id = format!(
        "{stamp}-{}",
        &digest(nonce.path().as_os_str().as_encoded_bytes())[..8]
    );
    let base = key(dp, &format!("runs/{id}"));
    let mut apps = BTreeSet::new();
    if let Some((t, a)) = &args.app {
        apps.insert((t.clone(), a.clone()));
    } else {
        let root = format!("{}/", key(sp, "apps"));
        for path in list_bounded(source, &root).await?.keys() {
            let parts: Vec<_> = path.split('/').collect();
            if parts.len() == 3 && parts[2] == "current" {
                crate::parse_app(&format!("{}/{}", parts[0], parts[1]))
                    .map_err(anyhow::Error::msg)?;
                apps.insert((parts[0].into(), parts[1].into()));
            }
        }
    }
    ensure!(!apps.is_empty(), "no apps found");
    let mut starts = BTreeMap::new();
    let mut starts_bytes = 0;
    for (t, a) in &apps {
        let name = format!("{t}/{a}");
        let marker =
            incarnation_for(source, &key(sp, &format!("apps/{t}/{a}")), undo_fence).await?;
        charge(
            &mut starts_bytes,
            name.len() as u64 + marker.as_ref().map_or(4, |m| m.len() as u64 * 4) + 256,
            INDEX_MAX,
            "incarnations memory",
        )?;
        starts.insert(name, marker);
    }
    let files_epoch = match undo_fence {
        Some(fence) => serde_json::from_slice::<Value>(fence)?["files_epoch"]
            .as_str()
            .map(str::to_owned),
        None => None,
    };
    let prunes = prune::markers(dest, dp).await?;
    let started_hash = json_put(
        dest,
        &key(&base, "started"),
        &json!({"source":args.bucket,"started_ms":now_ms()?,"run":id,"incarnations":starts}),
        false,
    )
    .await?;
    let previous = match optional(dest, &key(dp, "latest")).await? {
        Some(_) => Some(
            load(
                dest,
                dp,
                &latest(dest, dp).await?.context("missing latest")?,
            )
            .await?,
        ),
        None => None,
    };
    let compatible = previous.as_ref().is_some_and(|r| {
        r.complete["source"] == args.bucket && r.complete["incarnations"] == json!(starts)
    });
    let old: BTreeMap<_, _> = if compatible {
        previous
            .as_ref()
            .into_iter()
            .flat_map(|r| &r.actors)
            .map(|a| Ok((text(&disk_actor(a)?, "prefix")?.to_owned(), a)))
            .collect::<anyhow::Result<_>>()?
    } else {
        BTreeMap::new()
    };
    let copier = Copier {
        source,
        dest,
        dp,
        cache: args.cache.as_deref(),
        all: args.history == History::All,
    };
    let mut actors = Vec::new();
    let mut actor_count = 0usize;
    let mut chunks = Vec::new();
    let mut failures = Vec::new();
    let mut app_records = Vec::new();
    let mut run_index_bytes = 0;
    charge(
        &mut run_index_bytes,
        encode_bounded(&json!(starts), META_MAX - 1)?.len() as u64 + 512,
        INDEX_MAX,
        "resident index",
    )?;
    let mut run_disk_bytes = 0;
    for (t, a) in apps {
        let app_start = actor_count;
        let mut members = BTreeMap::new();
        let prefix = format!("{}/", key(sp, &format!("state/{t}/{a}/actors")));
        let mut copying = actor_groups(source, &prefix)
            .map_ok(|(actor_prefix, group)| {
                let copier = &copier;
                let old = &old;
                async move {
                    let previous = old
                        .get(&actor_prefix)
                        .map(|path| disk_actor(path))
                        .transpose()?
                        .filter(|actor| actor["status"] == "copied");
                    let result = copier.actor(&actor_prefix, group, previous.as_ref()).await;
                    Ok::<_, anyhow::Error>((actor_prefix, result))
                }
            })
            .try_buffer_unordered(16);
        while let Some((actor_prefix, result)) = copying.try_next().await? {
            let mut actor = match result {
                Ok(mut actor) => {
                    actor["status"] = json!("copied");
                    if !array(&actor, "gaps")?.is_empty() {
                        println!("{actor_prefix}: retained history gaps: {}", actor["gaps"]);
                    }
                    if args
                        .require_continuous
                        .unwrap_or(args.history == History::All)
                        && !array(&actor, "gaps")?.is_empty()
                    {
                        let failure = json!({"actor":actor_prefix,"error":"retained history gap", "gaps":actor["gaps"]});
                        failures.push(failure.clone());
                        actor["status"] = json!("failed");
                        actor["failure"] = failure;
                    }
                    actor
                }
                Err(e) => {
                    let failure = json!({"actor":actor_prefix,"error":e.to_string()});
                    failures.push(failure.clone());
                    json!({"prefix":actor_prefix,"status":"failed","failure":failure})
                }
            };
            if actor["status"] == "failed" {
                charge(
                    &mut run_index_bytes,
                    encode_bounded(&actor["failure"], ACTOR_MAX)?.len() as u64,
                    INDEX_MAX,
                    "resident index",
                )?;
            }
            actor["run"] = json!(id);
            actor["started_sha256"] = json!(started_hash);
            let encoded = encode_bounded(&actor, ACTOR_MAX)?;
            charge(
                &mut run_disk_bytes,
                encoded.len() as u64,
                SPOOL_MAX,
                "spool disk",
            )?;
            charge(
                &mut run_index_bytes,
                actor_prefix.len() as u64 + 2048 + (encoded.len() as u64 / (1 << 20)) * 512,
                INDEX_MAX,
                "resident index",
            )?;
            members.insert(actor_prefix, actor["status"].clone());
            actor_count += 1;
            actors.push(actor);
            flush_chunk(dest, &base, &mut chunks, &mut actors).await?;
        }
        let mut previous_files = BTreeMap::new();
        if compatible {
            for old_app in previous.iter().flat_map(|r| &r.apps) {
                if old_app["app"] == format!("{t}/{a}") {
                    for entry in files::entries(dest, dp, old_app).await? {
                        previous_files.insert(text(&entry, "source")?.to_owned(), entry);
                    }
                }
            }
        }
        let copied_files = files::copy_app(
            source,
            sp,
            dest,
            dp,
            &base,
            &t,
            &a,
            &previous_files,
            args.cache.as_deref(),
            files_epoch.as_deref(),
        )
        .await?;
        drop(previous_files);
        let mut app = app_metadata(source, sp, dest, dp, &t, &a).await?;
        for (field, value) in copied_files.as_object().context("file fields")? {
            app[field] = value.clone();
        }
        app["run"] = json!(id);
        app["started_sha256"] = json!(started_hash);
        app["actors"] = json!(actor_count - app_start);
        app["members"] = json!(members);
        ensure!(
            incarnation_for(source, &key(sp, &format!("apps/{t}/{a}")), undo_fence).await?
                == starts[&format!("{t}/{a}")],
            "app recreated during backup"
        );
        charge(
            &mut run_index_bytes,
            encode_bounded(&app, META_MAX - 1)?.len() as u64,
            INDEX_MAX,
            "resident index",
        )?;
        let path = key(&base, &format!("app/{t}/{a}"));
        let hash = json_put(dest, &path, &app, false).await?;
        app_records.push(json!({"key":path,"sha256":hash}));
    }
    flush_chunk(dest, &base, &mut chunks, &mut actors).await?;
    // Serialize publication with deletion, including the complete -> latest gap.
    let publication_lock = key(dp, "prune-lock");
    let (lock_version, lock_nonce) = restore::acquire_lock(
        dest,
        &publication_lock,
        &json!({"operation":"create", "run":id}),
    )
    .await?;
    let result: anyhow::Result<String> = async {
    ensure!(
        prunes == prune::markers(dest, dp).await?,
        "prune started during create; refusing completion, retry the run"
    );
    ensure!(
        now_ms()?.saturating_sub(started_at) < 24 * 60 * 60 * 1000,
        "backup exceeded 24 hours; refusing completion"
    );
    // Pin undo runs before publishing them, not after returning to restore.
    if undo_fence.is_some() {
        prune::pin(dest, dp, &id).await?;
    }
    // Recheck every app at the end of the entire run, including apps copied
    // before a later app took a long time.
    for (app, started) in &starts {
        ensure!(
            incarnation_for(source, &key(sp, &format!("apps/{app}")), undo_fence).await?
                == *started,
            "app deleted or recreated before completion"
        );
    }
    let complete = json!({"format":1,"started_sha256":started_hash,"run":id,"source":args.bucket,"incarnations":starts,"actors":actor_count,"chunks":chunks,"apps":app_records,"failed":failures,"history":if args.history==History::All{"all"}else{"head"}});
    charge(
        &mut run_index_bytes,
        encode_bounded(&complete, META_MAX - 1)?.len() as u64,
        INDEX_MAX,
        "resident index",
    )?;
    for chunk in array(&complete, "chunks")? {
        charge(
            &mut run_index_bytes,
            text(chunk, "key")?.len() as u64 + 128,
            INDEX_MAX,
            "resident index",
        )?;
    }
    let hash = json_put(dest, &key(&base, "complete"), &complete, false).await?;
    json_put(
        dest,
        &key(&base, "root"),
        &json!({"run":id,"sha256":hash}),
        false,
    )
    .await?;
    json_put(
        dest,
        &key(dp, "latest"),
        &json!({"run":id,"sha256":hash}),
        true,
    )
    .await?;
    println!(
        "{id}: {} actors, {} failed; no cross-actor consistency",
        actor_count,
        failures.len()
    );
    ensure!(
        failures.is_empty() || undo_fence.is_some(),
        "partial backup; failed actors are recorded, verification will refuse it"
    );
    Ok(id)
    }.await;
    #[cfg(not(test))]
    if result.is_err() {
        tokio::time::sleep(wasmkeep_server::apps::LATE_WRITES).await;
    }
    let unlock = dest
        .put_opts(
            &Key::parse(publication_lock)?,
            format!("unlocked {lock_nonce}").into_bytes().into(),
            PutOptions {
                mode: PutMode::Update(lock_version),
                ..Default::default()
            },
        )
        .await;
    let id = result?;
    unlock?;
    Ok(id)
}

/// Verify the deploy's referenced files, not merely whichever entries a damaged
/// app record happened to retain. Every retained version is checked.
async fn verify_app(store: &Store, dp: &str, sp: &str, app: &Value) -> anyhow::Result<()> {
    use wasmkeep_server::apps::{Current, Manifest};
    let base = key(sp, &format!("apps/{}", text(app, "app")?));
    let mut objects = BTreeMap::new();
    for object in array(app, "objects")? {
        let path = text(object, "key")?;
        ensure!(
            path.starts_with(&format!("{base}/")) && !objects.contains_key(path),
            "invalid/duplicate app object"
        );
        blob(store, dp, object).await?;
        objects.insert(path.to_owned(), object);
    }
    let current = Current::parse(
        &blob(
            store,
            dp,
            objects
                .get(&key(&base, "current"))
                .context("deploy current missing")?,
        )
        .await?,
    )
    .context("invalid deploy current")?;
    let mut version = Some(current.version);
    for _ in 0..6 {
        let Some(v) = version.take() else {
            break;
        };
        let vp = key(&base, &v);
        let manifest = Manifest::parse(std::str::from_utf8(
            &blob(
                store,
                dp,
                objects
                    .get(&key(&vp, "manifest"))
                    .context("deploy manifest missing")?,
            )
            .await?,
        )?)
        .context("invalid deploy manifest")?;
        for (class, hash) in &manifest.classes {
            ensure!(
                digest(
                    &blob(
                        store,
                        dp,
                        objects
                            .get(&key(&vp, &format!("{class}.wasm")))
                            .context("component missing")?
                    )
                    .await?
                ) == *hash,
                "component mismatch"
            );
        }
        if let Some(hash) = &manifest.front {
            ensure!(
                digest(
                    &blob(
                        store,
                        dp,
                        objects
                            .get(&key(&vp, "_front.wasm"))
                            .context("front missing")?
                    )
                    .await?
                ) == *hash,
                "front mismatch"
            );
        }
        for (class, hash) in &manifest.config {
            ensure!(
                digest(
                    &blob(
                        store,
                        dp,
                        objects
                            .get(&key(&vp, &format!("{class}.config")))
                            .context("configuration missing")?
                    )
                    .await?
                ) == *hash,
                "configuration mismatch"
            );
        }
        if let Some(hash) = &manifest.assets {
            let bytes = blob(
                store,
                dp,
                objects
                    .get(&key(&base, &format!("_assets/index/{hash}")))
                    .context("assets index missing")?,
            )
            .await?;
            ensure!(digest(&bytes) == *hash, "assets index mismatch");
            let index =
                wasmkeep_server::assets::Index::parse(&bytes).map_err(anyhow::Error::msg)?;
            for file in index.files.values() {
                for content in file.contents() {
                    ensure!(
                        objects
                            .get(&key(&base, &format!("_assets/content/{}", content.digest)))
                            .is_some_and(|o| o["sha256"] == content.digest),
                        "asset content missing"
                    );
                }
            }
        }
        version = manifest.previous;
    }
    if let Some(bytes) = objects.get(&key(&base, "secrets/current")) {
        let bytes = blob(store, dp, bytes).await?;
        let version = std::str::from_utf8(&bytes)?.trim();
        ensure!(
            objects.contains_key(&key(&base, &format!("secrets/{version}"))),
            "sealed secrets missing"
        );
    }
    files::verify(store, dp, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests;

/// Execute a create or restore against already opened stores. This is the
/// same path as the CLI; callers supply the production late-write deadline
/// (tests may shorten it for an in-memory store).
pub async fn execute_with_stores(
    command: &Command,
    source: &Store,
    sp: &str,
    target: &Store,
    tp: &str,
    late: std::time::Duration,
) -> anyhow::Result<Option<String>> {
    match command {
        Command::Create(args) => create(args, source, sp, target, tp).await.map(Some),
        Command::Restore(args) => restore::execute(args, source, sp, target, tp, late)
            .await
            .map(|()| None),
        _ => bail!("only create and restore accept opened stores"),
    }
}

/// Export one actor from a verified backup run for live actor restoration.
/// Content hashes are checked before the normal history replay verifies LTX.
#[allow(clippy::too_many_arguments)]
pub async fn export_actor(
    from: &str,
    run_id: Option<&str>,
    tenant: &str,
    app: &str,
    class: &str,
    name: &str,
    point: history::Point,
    output: &std::path::Path,
) -> anyhow::Result<(history::Export, String)> {
    let (store, prefix) = crate::open_bucket_unchecked(from)?;
    export_actor_from_store(
        &store, &prefix, run_id, tenant, app, class, name, point, output,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn export_actor_from_store(
    store: &Store,
    prefix: &str,
    run_id: Option<&str>,
    tenant: &str,
    app: &str,
    class: &str,
    name: &str,
    point: history::Point,
    output: &std::path::Path,
) -> anyhow::Result<(history::Export, String)> {
    let id = match run_id {
        Some(id) => id.to_owned(),
        None => latest(store, prefix)
            .await?
            .context("backup has no complete run")?,
    };
    prune::pin(store, prefix, &id).await?;
    let run = load(store, prefix, &id).await?;
    let (_, source_prefix) = crate::parse_bucket(text(&run.complete, "source")?)?;
    let suffix = format!(
        "state/{tenant}/{app}/actors/{class}/{}/",
        wasmkeep_storage::encode_segment(name)
    );
    let expected_prefix = key(source_prefix, &suffix);
    let tmp = crate::private_tempdir()?;
    let local: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        tmp.path(),
    )?);
    let root = wasmkeep_cluster::state_root::StateRoot::new("", tenant, app);
    let address = wasmkeep_runtime::ActorAddress::new(tenant, class, name)?;
    for path in &run.actors {
        let actor = disk_actor(path)?;
        if actor["status"] == "failed" || text(&actor, "prefix")? != expected_prefix {
            continue;
        }
        let objects = actor["objects"].as_object().context("missing object map")?;
        let mut total = 0;
        for (tail, object) in objects {
            let listing = Listing::from_keys([tail.as_str()]);
            ensure!(
                listing.segments.len() + listing.snapshots.len() == 1,
                "invalid history key"
            );
            let bytes = blob(store, prefix, object).await?;
            charge(&mut total, bytes.len() as u64, SPOOL_MAX, "actor restore")?;
            put(&local, &format!("{suffix}{tail}"), bytes, false).await?;
        }
        return history::export_raw(&local, &root, &address, point, output, MAX)
            .await
            .map(|export| (export, id))
            .map_err(Into::into);
    }
    bail!("actor is not in backup run {id}")
}
