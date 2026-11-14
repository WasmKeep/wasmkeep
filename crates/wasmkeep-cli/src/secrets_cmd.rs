//! `wasmkeep secrets` ([ADR 0016](../../../docs/adr/0016-secrets-in-the-bucket.md)):
//! an app's secrets, sealed in the bucket. Values come from stdin or a
//! file, never from the command line, and are never printed.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Subcommand};
use wasmkeep_server::secret_store::{self, SecretStore};
use wasmkeep_server::secrets::{SecretSet, SecretsKey};

#[derive(Debug, Subcommand)]
pub(crate) enum SecretsCommand {
    /// Write a new secrets key to FILE (64 hex characters, mode 0600; an
    /// existing file is refused). Give it to every node (`serve
    /// --secrets-key`) and keep it with the bucket credentials, not in the
    /// bucket.
    NewKey {
        /// The file to create.
        #[arg(long)]
        out: PathBuf,
    },
    /// Set a secret (value from stdin, one trailing newline removed), or
    /// several from `--file`, as one change.
    Set(SetArgs),
    /// Remove secrets from a class or the front.
    Unset(UnsetArgs),
    /// List classes (and the front's scope, `_front`) and names (never
    /// values) and the current seq.
    List(AppArgs),
    /// Re-seal an app's secrets under a new key (`--all`: every app under
    /// the prefix). Nodes need the new key first (`--secrets-key` twice).
    RotateKey(RotateArgs),
    /// Write a whole set from a file without opening the current one:
    /// recovery after losing the key. Nodes need the new key to open it.
    Reset(ResetArgs),
}

#[derive(Debug, Args)]
pub(crate) struct AppArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,
    /// The app, as TENANT/APP.
    #[arg(long, value_parser = crate::parse_app)]
    app: (String, String),
    /// The secrets key file. Given twice during a key change: either key
    /// opens, the first one seals.
    #[arg(long = "key", required = true)]
    keys: Vec<PathBuf>,
    /// Change the secrets even while a live node of the app does not read
    /// them from the bucket (it would serve them as unset).
    #[arg(long)]
    force: bool,
    /// With two keys, the first seals: say every node already has it when
    /// it is not the key the current set is sealed with (a node without it
    /// answers 503 for the app).
    #[arg(long)]
    nodes_have_new_key: bool,
}

#[derive(Debug, Args)]
pub(crate) struct SetArgs {
    #[command(flatten)]
    app: AppArgs,
    /// The class, with NAME (value from stdin).
    #[arg(long, requires = "name", conflicts_with_all = ["file", "front"])]
    class: Option<String>,
    /// The front's secret NAME (ADR 0023), read with `env.get` by the
    /// app's front (value from stdin).
    #[arg(long, requires = "name", conflicts_with = "file")]
    front: bool,
    /// The secret's name (its value is read from stdin).
    name: Option<String>,
    /// Several secrets at once, in the sealed format
    /// (`<class> <name> <length>\n<value>\n` per secret; `_front` as the
    /// class for the front's).
    #[arg(long)]
    file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct UnsetArgs {
    #[command(flatten)]
    app: AppArgs,
    /// The class.
    #[arg(long, required_unless_present = "front", conflicts_with = "front")]
    class: Option<String>,
    /// Remove the front's secrets (ADR 0023) instead of a class's.
    #[arg(long)]
    front: bool,
    /// The names to remove.
    #[arg(required = true)]
    names: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct RotateArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX].
    #[arg(long)]
    bucket: String,
    /// The app, as TENANT/APP (or `--all`).
    #[arg(long, value_parser = crate::parse_app, required_unless_present = "all", conflicts_with = "all")]
    app: Option<(String, String)>,
    /// Every app under the prefix that has secrets.
    #[arg(long)]
    all: bool,
    /// The key the secrets are sealed with now.
    #[arg(long)]
    key: PathBuf,
    /// The key to seal them with.
    #[arg(long)]
    new_key: PathBuf,
    /// Every node of the app already has the new key (`serve
    /// --secrets-key`, given twice during the change). Required: a node
    /// without it cannot open what this writes, and answers 503 for the app
    /// until it gets the key.
    #[arg(long)]
    nodes_have_new_key: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ResetArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX].
    #[arg(long)]
    bucket: String,
    /// The app, as TENANT/APP.
    #[arg(long, value_parser = crate::parse_app)]
    app: (String, String),
    /// The key to seal with.
    #[arg(long)]
    new_key: PathBuf,
    /// The whole set, in the sealed format.
    #[arg(long)]
    from: PathBuf,
    /// Reset even while a live node of the app does not read secrets.
    #[arg(long)]
    force: bool,
    /// Every node of the app already has the new key (`serve
    /// --secrets-key`, given twice during the change). Required: a node
    /// without it cannot open what this writes, and answers 503 for the app
    /// until it gets the key.
    #[arg(long)]
    nodes_have_new_key: bool,
}

/// Refuses a key change until the operator says every node has the new key
/// (no heartbeat says which keys a node holds, so it cannot be checked).
fn nodes_have_new_key(said: bool, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        said,
        "{what} seals the secrets with the new key: a node without that key cannot open \
         them and answers 503 for the app until it gets it. Give every node the new key \
         first (a second --secrets-key, one node restarted at a time), then pass \
         --nodes-have-new-key"
    );
    Ok(())
}

/// With more than one key, the first seals what `set` and `unset` write:
/// refused without `--nodes-have-new-key` when that key cannot open the
/// current set alone (a new key, which nodes may not have yet).
async fn first_key_is_current(
    store: &SecretStore,
    keys: &[SecretsKey],
    said: bool,
) -> anyhow::Result<()> {
    if keys.len() < 2 || said {
        return Ok(());
    }
    match store.read_current(&keys[..1]).await {
        Ok(_) => Ok(()),
        Err(secret_store::SecretStoreError::Sealed(
            _,
            wasmkeep_server::secrets::SecretsError::UnknownKey(_),
        )) => nodes_have_new_key(false, "the first --key"),
        Err(e) => Err(e.into()),
    }
}

fn load_keys(paths: &[PathBuf]) -> anyhow::Result<Vec<SecretsKey>> {
    paths
        .iter()
        .map(|p| SecretsKey::load(p).map_err(anyhow::Error::from))
        .collect()
}

fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

fn store_for(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<SecretStore> {
    let (store, prefix) = crate::open_bucket(bucket)?;
    Ok(SecretStore::new(store, &prefix, tenant, app))
}

/// Live nodes serving `tenant/app` that do not read secrets from the
/// bucket (older versions, which say nothing on heartbeat line 10).
fn nodes_without_secrets(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<Vec<String>> {
    let (store, prefix) = crate::open_bucket(bucket)?;
    let heartbeats = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let horizon = wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_server::LeaseConfig::default_safe(),
    );
    let now = now_ms();
    let served = format!("{tenant}/{app}");
    let nodes = runtime()?.block_on(wasmkeep_cluster::membership::all_heartbeats(&heartbeats))?;
    Ok(nodes
        .into_iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now, horizon))
        .filter(|n| n.app == served && !n.reads_secrets)
        .map(|n| n.id)
        .collect())
}

/// Live nodes of `tenant/app` that do not announce the capability word
/// `front` (ADR 0023).
fn nodes_without_front(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<Vec<String>> {
    let (store, prefix) = crate::open_bucket(bucket)?;
    Ok(
        runtime()?.block_on(wasmkeep_server::apps::secrets_readers_without_front(
            store, &prefix, tenant, app,
        ))?,
    )
}

/// Refuses a set that gains a `_front` entry (`had`: the set before had
/// one) while `lacking` (live nodes of the app that read its secrets,
/// without the word `front`) is not empty: one entry such a node cannot
/// check makes it fail to open the whole set, refuse every event and drain
/// (ADR 0023). A set that already had one is not refused: such a node
/// already opens nothing, and refusing would only block a fix. Unlike
/// [`check_nodes`], there is no `--force`.
fn refuse_front(
    had: bool,
    set: &SecretSet,
    lacking: &[String],
    tenant: &str,
    app: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        had || !set.has_front() || lacking.is_empty(),
        "live nodes of {tenant}/{app} cannot read the front's secrets ({}): with a `_front` \
         entry in the set they could open none of the app's secrets, and would refuse every \
         event. Upgrade them first (this check has no --force)",
        lacking.join(", ")
    );
    Ok(())
}

/// Refuses a change while a node of the app would not see it, unless
/// `--force`.
fn check_nodes(args: &AppArgs) -> anyhow::Result<()> {
    if args.force {
        return Ok(());
    }
    let (tenant, app) = &args.app;
    let older = nodes_without_secrets(&args.bucket, tenant, app)?;
    anyhow::ensure!(
        older.is_empty(),
        "live nodes of {tenant}/{app} do not read secrets from the bucket ({}): they would \
         serve them as unset. Upgrade them first, or pass --force",
        older.join(", ")
    );
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A value from stdin, less exactly one trailing `\n` or `\r\n`.
pub(crate) fn value_from(mut input: impl std::io::Read) -> anyhow::Result<String> {
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    if bytes.len() > wasmkeep_server::secrets::MAX_PLAINTEXT {
        anyhow::bail!("the value is too large");
    }
    let mut value =
        String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("the value is not UTF-8"))?;
    if value.ends_with("\r\n") {
        value.truncate(value.len() - 2);
    } else if value.ends_with('\n') {
        value.truncate(value.len() - 1);
    }
    Ok(value)
}

fn read_set(path: &PathBuf) -> anyhow::Result<SecretSet> {
    let bytes = std::fs::read(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    SecretSet::decode(&bytes).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
}

/// After a change: delete what no node needs, best effort.
async fn tidy(store: &SecretStore) {
    if let Err(e) = store.delete_old(now_ms(), secret_store::RETENTION).await {
        eprintln!("warning: could not delete old sealed objects: {e}");
    }
}

pub(crate) fn run(command: SecretsCommand) -> anyhow::Result<()> {
    match command {
        SecretsCommand::NewKey { out } => {
            SecretsKey::generate()?.write_new(&out)?;
            let id = SecretsKey::load(&out)?.id().to_owned();
            println!(
                "Wrote a new secrets key to {} (key id {id}).",
                out.display()
            );
            Ok(())
        }
        SecretsCommand::Set(args) => {
            let scope = match (&args.class, args.front) {
                (Some(class), false) => Some(class.as_str()),
                (None, true) => Some(wasmkeep_server::secrets::FRONT_SCOPE),
                _ => None,
            };
            let changes = match (scope, &args.name, &args.file) {
                (Some(scope), Some(name), None) => {
                    let mut one = SecretSet::default();
                    one.set(scope, name, &value_from(std::io::stdin().lock())?)?;
                    one
                }
                (None, None, Some(file)) => read_set(file)?,
                _ => anyhow::bail!(
                    "give --class CLASS NAME or --front NAME (value on stdin), or --file"
                ),
            };
            check_nodes(&args.app)?;
            let keys = load_keys(&args.app.keys)?;
            let (tenant, app) = &args.app.app;
            let lacking = nodes_without_front(&args.app.bucket, tenant, app)?;
            let store = store_for(&args.app.bucket, tenant, app)?;
            let seq = runtime()?.block_on(async {
                first_key_is_current(&store, &keys, args.app.nodes_have_new_key).await?;
                let seq = store
                    .change(&keys, &keys[0], |set| {
                        let had = set.has_front();
                        for (class, names) in changes.names() {
                            let vars = changes.class(&class).cloned().unwrap_or_default();
                            for name in names {
                                set.set(&class, &name, &vars[&name])?;
                            }
                        }
                        refuse_front(had, set, &lacking, tenant, app)
                    })
                    .await??;
                tidy(&store).await;
                anyhow::Ok(seq)
            })?;
            let count: usize = changes.names().iter().map(|(_, n)| n.len()).sum();
            println!("Set {count} secret(s) for {tenant}/{app}: seq {seq}.");
            Ok(())
        }
        SecretsCommand::Unset(args) => {
            check_nodes(&args.app)?;
            let keys = load_keys(&args.app.keys)?;
            let (tenant, app) = &args.app.app;
            let scope = match &args.class {
                Some(class) => class.as_str(),
                None => wasmkeep_server::secrets::FRONT_SCOPE,
            };
            // Removing adds no `_front` entry: no front check (ADR 0023);
            // removing the last one is the way out.
            let store = store_for(&args.app.bucket, tenant, app)?;
            let (seq, removed) = runtime()?.block_on(async {
                first_key_is_current(&store, &keys, args.app.nodes_have_new_key).await?;
                let mut removed = 0;
                let seq = store
                    .change(&keys, &keys[0], |set| {
                        removed = args.names.iter().filter(|n| set.unset(scope, n)).count();
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .await?
                    .unwrap_or_else(|never| match never {});
                tidy(&store).await;
                anyhow::Ok((seq, removed))
            })?;
            println!(
                "Removed {removed} of {} name(s) from {scope}: seq {seq}.",
                args.names.len(),
            );
            Ok(())
        }
        SecretsCommand::List(args) => {
            let keys = load_keys(&args.keys)?;
            let (tenant, app) = &args.app;
            let store = store_for(&args.bucket, tenant, app)?;
            let (seq, set) = runtime()?.block_on(store.read_current(&keys))?;
            match seq {
                None => println!("{tenant}/{app} has no secrets."),
                Some(seq) => {
                    println!("{tenant}/{app}: seq {seq}");
                    for (class, names) in set.names() {
                        let shown = if class == wasmkeep_server::secrets::FRONT_SCOPE {
                            format!("{class} (the front)")
                        } else {
                            class
                        };
                        println!("  {shown}: {}", names.join(", "));
                    }
                }
            }
            Ok(())
        }
        SecretsCommand::RotateKey(args) => {
            nodes_have_new_key(args.nodes_have_new_key, "rotate-key")?;
            let old = SecretsKey::load(&args.key)?;
            let new = SecretsKey::load(&args.new_key)?;
            let (store, prefix) = crate::open_bucket(&args.bucket)?;
            let rt = runtime()?;
            let apps = match &args.app {
                Some(one) => vec![one.clone()],
                None => rt.block_on(secret_store::apps_with_secrets(&store, &prefix))?,
            };
            for (tenant, app) in apps {
                let one = SecretStore::new(Arc::clone(&store), &prefix, &tenant, &app);
                let keys = [old.clone(), new.clone()];
                // Re-sealing adds no entry: no front check (ADR 0023).
                let seq = rt.block_on(async {
                    let seq = one
                        .change(&keys, &new, |_| Ok::<(), std::convert::Infallible>(()))
                        .await?
                        .unwrap_or_else(|never| match never {});
                    tidy(&one).await;
                    anyhow::Ok(seq)
                })?;
                println!("{tenant}/{app}: sealed with key {} at seq {seq}.", new.id());
            }
            Ok(())
        }
        SecretsCommand::Reset(args) => {
            nodes_have_new_key(args.nodes_have_new_key, "reset")?;
            if !args.force {
                let (tenant, app) = &args.app;
                let older = nodes_without_secrets(&args.bucket, tenant, app)?;
                anyhow::ensure!(
                    older.is_empty(),
                    "live nodes of {tenant}/{app} do not read secrets from the bucket ({}); \
                     upgrade them first, or pass --force",
                    older.join(", ")
                );
            }
            let new = SecretsKey::load(&args.new_key)?;
            let set = read_set(&args.from)?;
            let (tenant, app) = &args.app;
            // The current set cannot be opened (its key is lost): judged on
            // the set written alone.
            refuse_front(
                false,
                &set,
                &nodes_without_front(&args.bucket, tenant, app)?,
                tenant,
                app,
            )?;
            let store = store_for(&args.bucket, tenant, app)?;
            let seq = runtime()?.block_on(async {
                let seq = store.reset(&new, &set).await?;
                tidy(&store).await;
                anyhow::Ok(seq)
            })?;
            println!(
                "Reset {tenant}/{app}'s secrets under key {}: seq {seq}.",
                new.id()
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::{refuse_front, value_from};
    use wasmkeep_server::secrets::SecretSet;

    #[test]
    fn a_set_with_the_front_is_refused_while_a_node_cannot_read_it() {
        let mut classes = SecretSet::default();
        classes.set("api", "KEY", "v").unwrap();
        let mut front = classes.clone();
        front.set("_front", "SESSION_KEY", "v").unwrap();
        let old = vec!["old-1".to_owned()];
        // Classes only: an older node reads it, whatever is live.
        assert!(refuse_front(false, &classes, &old, "acme", "shop").is_ok());
        // A `_front` entry: refused while such a node is live, naming it.
        let e = refuse_front(false, &front, &old, "acme", "shop")
            .unwrap_err()
            .to_string();
        assert!(e.contains("old-1") && e.contains("no --force"), "{e}");
        assert!(refuse_front(false, &front, &[], "acme", "shop").is_ok());
        // Already there: such a node opens nothing already; refusing would
        // only block a fix.
        assert!(refuse_front(true, &front, &old, "acme", "shop").is_ok());
    }

    #[test]
    fn secrets_flags_take_the_front() {
        use clap::Parser as _;
        let parse = |args: &[&str]| {
            crate::Cli::try_parse_from(
                ["wasmkeep", "secrets"]
                    .iter()
                    .chain(args)
                    .chain(&["--bucket", "s3://b", "--app", "t/a", "--key", "k"]),
            )
        };
        assert!(parse(&["set", "--front", "SESSION_KEY"]).is_ok());
        assert!(parse(&["set", "--class", "api", "KEY"]).is_ok());
        assert!(parse(&["set", "--front", "--class", "api", "KEY"]).is_err());
        assert!(parse(&["set", "--front", "--file", "f"]).is_err());
        assert!(parse(&["set", "--front"]).is_err(), "a name is needed");
        assert!(parse(&["unset", "--front", "SESSION_KEY"]).is_ok());
        assert!(parse(&["unset", "--class", "api", "KEY"]).is_ok());
        assert!(parse(&["unset", "KEY"]).is_err(), "--class or --front");
        assert!(parse(&["unset", "--front", "--class", "api", "KEY"]).is_err());
    }

    #[test]
    fn one_trailing_newline_is_removed_from_stdin() {
        assert_eq!(value_from(&b"sk_123\n"[..]).unwrap(), "sk_123");
        assert_eq!(value_from(&b"sk_123\r\n"[..]).unwrap(), "sk_123");
        assert_eq!(value_from(&b"sk_123"[..]).unwrap(), "sk_123");
        assert_eq!(value_from(&b"two\n\n"[..]).unwrap(), "two\n");
        assert!(value_from(&b"\xff"[..]).is_err());
    }
}
