//! Content-addressed, lease-free online archives (ADR 0033).
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
type Store = Arc<dyn ObjectStore>;
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Create(Create),
    List(From),
    Verify(Verify),
}
#[derive(Debug, Args)]
pub(crate) struct Create {
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
pub(crate) struct From {
    #[arg(long)]
    from: String,
}
#[derive(Debug, Args)]
pub(crate) struct Verify {
    #[arg(long)]
    from: String,
    #[arg(long)]
    run: Option<String>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    sample: Option<u64>,
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
    let bytes = serde_json::to_vec(value)?;
    let hash = digest(&bytes);
    put(store, path, bytes, replace).await?;
    Ok(hash)
}
fn destination(args: &Create) -> anyhow::Result<(Store, String)> {
    let Some(file) = &args.to_env else {
        return crate::open_bucket_unchecked(&args.to);
    };
    let (bucket, prefix) = crate::parse_bucket(&args.to)?;
    let mut builder = object_store::aws::AmazonS3Builder::new().with_bucket_name(bucket);
    for line in std::fs::read_to_string(file)?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
    {
        let (name, value) = line
            .split_once('=')
            .context("destination env needs NAME=value")?;
        let config = name
            .trim()
            .to_ascii_lowercase()
            .parse()
            .map_err(|_| anyhow::anyhow!("unsupported destination setting {}", name.trim()))?;
        builder = builder.with_config(config, value.trim().trim_matches(['\'', '"']));
    }
    Ok((Arc::new(builder.build()?), prefix.to_owned()))
}
pub(crate) fn run(command: &Command) -> anyhow::Result<()> {
    crate::actor_read::run(async {
        match command {
            Command::Create(args) => {
                let (source, sp) = crate::open_bucket_unchecked(&args.bucket)?;
                let (dest, dp) = destination(args)?;
                ensure!(
                    crate::parse_bucket(&args.bucket)?.0 != crate::parse_bucket(&args.to)?.0,
                    "source and destination must differ"
                );
                create(args, &source, &sp, &dest, &dp).await
            }
            Command::List(args) => {
                let (store, prefix) = crate::open_bucket_unchecked(&args.from)?;
                let objects =
                    history::list_objects(&store, &format!("{}/", key(&prefix, "runs"))).await?;
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
                    None => String::from_utf8(read(&store, &key(&prefix, "latest"), 1024).await?)?,
                };
                verify(&store, &prefix, id.trim(), args.sample).await
            }
        }
    })
}
struct Run {
    complete: Value,
    actors: Vec<Value>,
    apps: Vec<Value>,
}
async fn checked_json(store: &Store, path: &str, hash: &str) -> anyhow::Result<Value> {
    let bytes = read(store, path, META_MAX).await?;
    ensure!(digest(&bytes) == hash, "manifest digest mismatch: {path}");
    Ok(serde_json::from_slice(&bytes)?)
}
async fn load(store: &Store, prefix: &str, id: &str) -> anyhow::Result<Run> {
    safe_id(id)?;
    let base = key(prefix, &format!("runs/{id}"));
    let complete: Value =
        serde_json::from_slice(&read(store, &key(&base, "complete"), META_MAX).await?)?;
    ensure!(
        complete["format"] == 1 && complete["run"] == id,
        "unknown or inconsistent backup format"
    );
    let mut actors = Vec::new();
    let mut apps = Vec::new();
    let mut seen = BTreeSet::new();
    for chunk in array(&complete, "chunks")? {
        let path = text(chunk, "key")?;
        ensure!(
            path.starts_with(&format!("{base}/actors/")) && seen.insert(path.to_owned()),
            "invalid/duplicate chunk"
        );
        let value = checked_json(store, path, text(chunk, "sha256")?).await?;
        actors.extend(
            value
                .as_array()
                .context("actor chunk is not an array")?
                .iter()
                .cloned(),
        );
    }
    ensure!(
        actors.len() as u64 == number(&complete, "actors")?,
        "actor count mismatch"
    );
    let mut addresses = BTreeSet::new();
    for actor in &actors {
        ensure!(addresses.insert(text(actor, "prefix")?), "duplicate actor");
    }
    for app in array(&complete, "apps")? {
        let path = text(app, "key")?;
        ensure!(
            path.starts_with(&format!("{base}/app/")) && seen.insert(path.to_owned()),
            "invalid/duplicate app record"
        );
        apps.push(checked_json(store, path, text(app, "sha256")?).await?);
    }
    ensure!(!apps.is_empty(), "missing app records");
    array(&complete, "failed")?;
    Ok(Run {
        complete,
        actors,
        apps,
    })
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
    let tmp = tempfile::tempdir()?;
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
async fn verify(store: &Store, prefix: &str, id: &str, sample: Option<u64>) -> anyhow::Result<()> {
    let run = load(store, prefix, id).await?;
    ensure!(
        array(&run.complete, "failed")?.is_empty(),
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
        let members: Vec<_> = run
            .actors
            .iter()
            .filter(|actor| {
                actor["prefix"]
                    .as_str()
                    .is_some_and(|p| p.starts_with(&actor_prefix))
            })
            .collect();
        ensure!(
            members.len() as u64 == number(app, "actors")?,
            "app actor count mismatch"
        );
        for member in members {
            ensure!(
                covered.insert(text(member, "prefix")?),
                "actor belongs to duplicate app records"
            );
        }
        verify_app(store, prefix, source_prefix, app).await?;
    }
    ensure!(
        covered.len() == run.actors.len(),
        "manifest actors lack app records"
    );
    // Random sample by fresh entropy, independent of manifest ordering.
    let nonce = tempfile::tempdir()?;
    let mut actors: Vec<_> = run.actors.iter().collect();
    actors.sort_by_key(|a| digest(format!("{}{}", nonce.path().display(), a["prefix"]).as_bytes()));
    let count = sample.map_or(actors.len(), |n| {
        usize::try_from(n).unwrap_or(usize::MAX).min(actors.len())
    });
    for actor in actors.into_iter().take(count) {
        replay(store, prefix, actor).await?;
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
    json_put(
        store,
        &key(prefix, &format!("runs/{id}/verified")),
        &json!({"checked":count,"total":run.actors.len(),"sample":sample,"at_ms":now_ms()?}),
        true,
    )
    .await?;
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
        for attempt in 0..8 {
            if attempt > 0 {
                objects = history::list_objects(self.source, prefix).await?;
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
            match tokio::time::timeout(deadline, self.attempt(prefix, &objects, previous))
                .await
                .context("backup actor copy deadline exceeded")?
            {
                Ok(actor) => return Ok(actor),
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
        bail!("{reason}; collected faster than read: raise --history-retention-secs (8 attempts)")
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
        }))
    }

    async fn attempt(
        &self,
        prefix: &str,
        listed: &BTreeMap<String, ObjectMeta>,
        previous: Option<&Value>,
    ) -> anyhow::Result<Value> {
        let listing = Listing::from_keys(listed.keys().map(String::as_str));
        let plan = plan_restore(&listing)?;
        ensure!(plan.head().txid > 0, "actor has no retained history");
        let keys = history::copy_keys(&listing, &plan)?;
        let old = previous.and_then(|p| p["objects"].as_object());
        let unchanged = previous.is_some_and(|p| {
            p["generation"].as_u64() == Some(plan.head().generation)
                && p["txid"].as_u64() == Some(plan.head().txid)
        }) && keys.iter().all(|k| {
            listed
                .get(k)
                .zip(old.and_then(|o| o.get(k)))
                .is_some_and(|(m, o)| same(m, o))
        });
        if unchanged
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
            if let Some(entry) = old.and_then(|o| o.get(k)).filter(|o| same(meta, o)) {
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
            copied.insert(k.clone(), entry);
        }
        check_boundary(self.dest, self.dp, &keys, &copied).await?;
        let tmp = tempfile::tempdir()?;
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
        let actor = json!({"prefix":prefix,"generation":result.position.generation,"txid":result.position.txid,"checksum":result.raw_checksum,"timestamp_ms":result.timestamp_ms,"replayed_objects":result.objects,"plan":keys,"objects":copied,"replacement":result.verification.replaced,"foreign_key_violations":result.verification.foreign_key_violations,"alarm":alarm,"alarm_indexed":alarm_indexed});
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
async fn incarnation(source: &Store, base: &str) -> anyhow::Result<Option<Vec<u8>>> {
    ensure!(
        optional(source, &key(base, "deleted")).await?.is_none(),
        "app is deleted"
    );
    optional(source, &key(base, "recreated")).await
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
                        let mut required = vec![(manifest_key, manifest_bytes)];
                        for (class, hash) in &manifest.classes {
                            let path = key(&vp, &format!("{class}.wasm"));
                            let bytes = read(source, &path, MAX).await?;
                            ensure!(digest(&bytes) == *hash, "component digest mismatch");
                            required.push((path, bytes));
                        }
                        if let Some(hash) = &manifest.front {
                            let path = key(&vp, "_front.wasm");
                            let bytes = read(source, &path, MAX).await?;
                            ensure!(digest(&bytes) == *hash, "front digest mismatch");
                            required.push((path, bytes));
                        }
                        for (class, hash) in &manifest.config {
                            let path = key(&vp, &format!("{class}.config"));
                            let bytes = read(source, &path, META_MAX).await?;
                            ensure!(digest(&bytes) == *hash, "config digest mismatch");
                            required.push((path, bytes));
                        }
                        for (path, bytes) in required {
                            objects.insert(
                                path.clone(),
                                save_blob(dest, dp, &path, bytes, None, None).await?,
                            );
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
                    objects.insert(
                        path.clone(),
                        save_blob(dest, dp, &path, body, None, None).await?,
                    );
                }
                objects.insert(
                    current_key.clone(),
                    save_blob(dest, dp, &current_key, bytes.clone(), None, None).await?,
                );
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
    let path = key(base, &format!("actors/{:08}", chunks.len()));
    let hash = json_put(dest, &path, &json!(actors), false).await?;
    chunks.push(json!({"key":path,"sha256":hash}));
    actors.clear();
    Ok(())
}

type ActorGroup = (String, BTreeMap<String, ObjectMeta>);
fn actor_groups(
    source: &Store,
    prefix: &str,
) -> futures_util::stream::BoxStream<'static, anyhow::Result<ActorGroup>> {
    let list = source.list(Some(&Key::from(prefix.trim_end_matches('/'))));
    let prefix = prefix.to_owned();
    futures_util::stream::try_unfold(
        (list, None, prefix),
        |(mut list, mut pending, prefix)| async move {
            let mut group = BTreeMap::new();
            let mut actor_prefix = String::new();
            loop {
                let meta = match pending.take() {
                    Some(meta) => meta,
                    None => {
                        match tokio::time::timeout(std::time::Duration::from_secs(30), list.next())
                            .await
                            .context("app listing stalled")?
                        {
                            Some(meta) => meta?,
                            None => break,
                        }
                    }
                };
                let Some(relative) = meta.location.as_ref().strip_prefix(&prefix) else {
                    continue;
                };
                let parts: Vec<_> = relative.splitn(3, '/').collect();
                ensure!(parts.len() == 3, "invalid actor history key");
                let next = format!("{}{}/{}/", prefix, parts[0], parts[1]);
                let tail = parts[2].to_owned();
                if !actor_prefix.is_empty() && next != actor_prefix {
                    ensure!(
                        next > actor_prefix,
                        "source listing is not ordered by actor"
                    );
                    pending = Some(meta);
                    break;
                }
                actor_prefix = next;
                group.insert(tail, meta);
            }
            if group.is_empty() {
                Ok(None)
            } else {
                Ok(Some(((actor_prefix, group), (list, pending, prefix))))
            }
        },
    )
    .boxed()
}

async fn create(
    args: &Create,
    source: &Store,
    sp: &str,
    dest: &Store,
    dp: &str,
) -> anyhow::Result<()> {
    let nonce = tempfile::tempdir()?;
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
        for path in history::list_objects(source, &root).await?.keys() {
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
    for (t, a) in &apps {
        starts.insert(
            format!("{t}/{a}"),
            incarnation(source, &key(sp, &format!("apps/{t}/{a}"))).await?,
        );
    }
    json_put(
        dest,
        &key(&base, "started"),
        &json!({"source":args.bucket,"started_ms":now_ms()?,"incarnations":starts}),
        false,
    )
    .await?;
    let prunes = history::list_objects(dest, &format!("{}/", key(dp, "prunes"))).await?;
    let previous = match optional(dest, &key(dp, "latest")).await? {
        Some(bytes) => Some(load(dest, dp, std::str::from_utf8(&bytes)?.trim()).await?),
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
            .map(|a| Ok((text(a, "prefix")?.to_owned(), a)))
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
    for (t, a) in apps {
        let app_start = actor_count;
        let prefix = format!("{}/", key(sp, &format!("state/{t}/{a}/actors")));
        let mut copying = actor_groups(source, &prefix)
            .map_ok(|(actor_prefix, group)| {
                let copier = &copier;
                let old = &old;
                async move {
                    let result = copier
                        .actor(&actor_prefix, group, old.get(&actor_prefix).copied())
                        .await;
                    Ok::<_, anyhow::Error>((actor_prefix, result))
                }
            })
            .try_buffer_unordered(16);
        while let Some((actor_prefix, result)) = copying.try_next().await? {
            match result {
                Ok(actor) => {
                    actors.push(actor);
                    actor_count += 1;
                    if actors.len() == 1000 {
                        flush_chunk(dest, &base, &mut chunks, &mut actors).await?;
                    }
                }
                Err(e) => failures.push(json!({"actor":actor_prefix,"error":e.to_string()})),
            }
        }
        let mut app = app_metadata(source, sp, dest, dp, &t, &a).await?;
        app["actors"] = json!(actor_count - app_start);
        ensure!(
            incarnation(source, &key(sp, &format!("apps/{t}/{a}"))).await?
                == starts[&format!("{t}/{a}")],
            "app recreated during backup"
        );
        let path = key(&base, &format!("app/{t}/{a}"));
        let hash = json_put(dest, &path, &app, false).await?;
        app_records.push(json!({"key":path,"sha256":hash}));
    }
    flush_chunk(dest, &base, &mut chunks, &mut actors).await?;
    let after = history::list_objects(dest, &format!("{}/", key(dp, "prunes"))).await?;
    ensure!(
        prunes.keys().eq(after.keys()),
        "prune started during create; refusing completion, retry the run"
    );
    // A pre-existing recent prune may still be running. Refuse rather than
    // publishing references it can remove; prune is a later ADR slice.
    ensure!(
        after.is_empty(),
        "destination has prune markers; completion refused until prune coordination is available"
    );
    // Recheck every app at the end of the entire run, including apps copied
    // before a later app took a long time.
    for (app, started) in &starts {
        ensure!(
            incarnation(source, &key(sp, &format!("apps/{app}"))).await? == *started,
            "app deleted or recreated before completion"
        );
    }
    let complete = json!({"format":1,"run":id,"source":args.bucket,"incarnations":starts,"actors":actor_count,"chunks":chunks,"apps":app_records,"failed":failures,"history":if args.history==History::All{"all"}else{"head"}});
    json_put(dest, &key(&base, "complete"), &complete, false).await?;
    put(dest, &key(dp, "latest"), id.as_bytes().to_vec(), true).await?;
    println!(
        "{id}: {} actors, {} failed; no cross-actor consistency",
        actor_count,
        failures.len()
    );
    ensure!(
        failures.is_empty(),
        "partial backup; failed actors are recorded, verification will refuse it"
    );
    Ok(())
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
        objects.insert(path.to_owned(), blob(store, dp, object).await?);
    }
    let current = Current::parse(
        objects
            .get(&key(&base, "current"))
            .context("deploy current missing")?,
    )
    .context("invalid deploy current")?;
    let mut version = Some(current.version);
    for _ in 0..6 {
        let Some(v) = version.take() else {
            break;
        };
        let vp = key(&base, &v);
        let manifest = Manifest::parse(std::str::from_utf8(
            objects
                .get(&key(&vp, "manifest"))
                .context("deploy manifest missing")?,
        )?)
        .context("invalid deploy manifest")?;
        for (class, hash) in &manifest.classes {
            ensure!(
                digest(
                    objects
                        .get(&key(&vp, &format!("{class}.wasm")))
                        .context("component missing")?
                ) == *hash,
                "component mismatch"
            );
        }
        if let Some(hash) = &manifest.front {
            ensure!(
                digest(
                    objects
                        .get(&key(&vp, "_front.wasm"))
                        .context("front missing")?
                ) == *hash,
                "front mismatch"
            );
        }
        for (class, hash) in &manifest.config {
            ensure!(
                digest(
                    objects
                        .get(&key(&vp, &format!("{class}.config")))
                        .context("configuration missing")?
                ) == *hash,
                "configuration mismatch"
            );
        }
        version = manifest.previous;
    }
    if let Some(bytes) = objects.get(&key(&base, "secrets/current")) {
        let version = std::str::from_utf8(bytes)?.trim();
        ensure!(
            objects.contains_key(&key(&base, &format!("secrets/{version}"))),
            "sealed secrets missing"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
