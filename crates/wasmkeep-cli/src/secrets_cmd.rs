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
    /// `--finish` then deletes the objects the old key sealed.
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
    #[arg(long, required_unless_present = "finish", conflicts_with = "finish")]
    key: Option<PathBuf>,
    /// The key to seal them with (with `--finish`: the key to keep).
    #[arg(long)]
    new_key: PathBuf,
    /// Every node of the app already has the new key (`serve
    /// --secrets-key`, given twice during the change). Required: a node
    /// without it cannot open what this writes, and answers 503 for the app
    /// until it gets the key.
    #[arg(long)]
    nodes_have_new_key: bool,
    /// The last step, once the old key is removed from every node: delete
    /// every sealed object another key sealed, so the old key opens nothing
    /// left in the bucket. Refused for an app whose current set does not
    /// open with `--new-key`.
    #[arg(long)]
    finish: bool,
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
    // A deleted app's secrets went with it (ADR 0028): none is written.
    crate::refuse_deleted(&store, &prefix, tenant, app)?;
    Ok(SecretStore::new(store, &prefix, tenant, app))
}

/// After a write: the app deleted meanwhile (ADR 0028) leaves what was
/// written in the bucket until `app delete` runs again; say so.
fn deleted_meanwhile(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<()> {
    let (store, prefix) = crate::open_bucket(bucket)?;
    crate::refuse_deleted(&store, &prefix, tenant, app).map_err(|e| {
        anyhow::anyhow!(
            "{e} (it was deleted while this wrote: run `wasmkeep app delete --app \
             {tenant}/{app}` again to remove what was written)"
        )
    })
}

/// Live nodes serving `tenant/app` that do not read secrets from the
/// bucket (older versions, which say nothing on heartbeat line 10).
fn nodes_without_secrets(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<Vec<String>> {
    Ok(readers(bucket, tenant, app)?
        .live()
        .filter(|n| !n.reads_secrets)
        .map(|n| n.id.clone())
        .collect())
}

/// The app's upgrade floor and live nodes, through the helper every
/// writer asks (ADR 0043, section 2).
fn readers(
    bucket: &str,
    tenant: &str,
    app: &str,
) -> anyhow::Result<wasmkeep_server::apps::Readers> {
    let (store, prefix) = crate::open_bucket(bucket)?;
    Ok(runtime()?.block_on(wasmkeep_server::apps::Readers::read(
        store, &prefix, tenant, app,
    ))?)
}

/// Refuses a write to the app's secrets by a `wasmkeep` older than the
/// app's upgrade floor, or beside a live node outside this version's
/// range (ADR 0043, sections 3 and 5).
fn check_writer(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<()> {
    let readers = readers(bucket, tenant, app)?;
    wasmkeep_cluster::upgrade::this_minor()
        .and_then(|this| readers.writer(this))
        .map_err(|why| anyhow::anyhow!("{tenant}/{app}: {why}; nothing written"))
}

/// Live nodes of `tenant/app` that read its secrets from the bucket
/// (heartbeat line 10: a node run with `--no-secrets` never opens the set)
/// and do not announce the capability word `front` (ADR 0023).
fn nodes_without_front(bucket: &str, tenant: &str, app: &str) -> anyhow::Result<Vec<String>> {
    Ok(readers(bucket, tenant, app)?.without_among(&["front"], |n| n.reads_secrets))
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
    let (tenant, app) = &args.app;
    check_writer(&args.bucket, tenant, app)?;
    if args.force {
        return Ok(());
    }
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

/// `rotate-key --finish`: retires every key but `key` in `app`, or in
/// every app with secrets. An app refused or with objects left does not
/// stop the others; the run fails at the end.
async fn finish(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    app: Option<(String, String)>,
    key: &SecretsKey,
    now_ms: u64,
) -> anyhow::Result<()> {
    let apps = match app {
        Some(one) => vec![one],
        None => secret_store::apps_with_secrets(store, prefix).await?,
    };
    let mut unfinished = 0;
    for (tenant, app) in &apps {
        let one = SecretStore::new(Arc::clone(store), prefix, tenant, app);
        let retired = match one
            .retire_other_keys(
                key,
                now_ms,
                secret_store::RETENTION,
                secret_store::STALE_AFTER,
            )
            .await
        {
            Ok(retired) => retired,
            Err(e @ secret_store::SecretStoreError::Sealed(..)) => {
                eprintln!(
                    "error: {tenant}/{app}: {e}. --finish deletes nothing unless the current \
                     set opens with --new-key: run rotate-key without --finish first."
                );
                unfinished += 1;
                continue;
            }
            Err(e) => {
                eprintln!("error: {tenant}/{app}: {e}");
                unfinished += 1;
                continue;
            }
        };
        println!(
            "{tenant}/{app}: deleted {} object(s) sealed with another key.",
            retired.deleted
        );
        if retired.left == 0 {
            println!("{tenant}/{app}: only key {} opens what is left.", key.id());
        } else {
            eprintln!(
                "warning: {tenant}/{app}: left {} object(s): sealed with another key and \
                 still possibly in use (the one before a set made current less than {} s \
                 ago, or one above it written less than {} min ago by a secrets command \
                 that may still be running), unreadable, or too large. Run --finish again \
                 later.",
                retired.left,
                secret_store::STALE_AFTER.as_secs(),
                secret_store::RETENTION.as_secs() / 60,
            );
            unfinished += 1;
        }
    }
    anyhow::ensure!(
        unfinished == 0,
        "{unfinished} of {} app(s) did not finish; see above",
        apps.len()
    );
    Ok(())
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
            deleted_meanwhile(&args.app.bucket, tenant, app)?;
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
            deleted_meanwhile(&args.app.bucket, tenant, app)?;
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
        SecretsCommand::RotateKey(args) if args.finish => {
            let new = SecretsKey::load(&args.new_key)?;
            let (store, prefix) = crate::open_bucket(&args.bucket)?;
            runtime()?.block_on(finish(&store, &prefix, args.app.clone(), &new, now_ms()))
        }
        SecretsCommand::RotateKey(args) => {
            nodes_have_new_key(args.nodes_have_new_key, "rotate-key")?;
            let Some(key) = args.key.as_deref() else {
                anyhow::bail!("--key is required without --finish");
            };
            let old = SecretsKey::load(key)?;
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
            println!(
                "Objects sealed with the old key are still in the bucket. Once the old key \
                 is removed from every node, run rotate-key --finish to delete them."
            );
            Ok(())
        }
        SecretsCommand::Reset(args) => {
            nodes_have_new_key(args.nodes_have_new_key, "reset")?;
            check_writer(&args.bucket, &args.app.0, &args.app.1)?;
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

    mod finish {
        use std::sync::Arc;

        use object_store::memory::InMemory;
        use object_store::{ObjectStore, ObjectStoreExt as _};
        use wasmkeep_server::secret_store::SecretStore;
        use wasmkeep_server::secrets::{SecretSet, SecretsKey};

        use super::super::finish;

        async fn seal(store: &Arc<dyn ObjectStore>, app: &str, keys: &[SecretsKey]) {
            let one = SecretStore::new(Arc::clone(store), "p", "acme", app);
            for with in keys {
                let opens = [keys[0].clone(), with.clone()];
                one.change(&opens, with, |s: &mut SecretSet| s.set("api", "A", "v"))
                    .await
                    .unwrap()
                    .unwrap();
            }
        }

        fn ids(store: &Arc<dyn ObjectStore>, app: &str) -> impl Future<Output = Vec<String>> {
            let one = SecretStore::new(Arc::clone(store), "p", "acme", app);
            let store = Arc::clone(store);
            async move {
                let mut out = Vec::new();
                for l in one.list().await.unwrap() {
                    let path = object_store::path::Path::from(format!(
                        "p/apps/acme/{app}/secrets/{}",
                        wasmkeep_server::secrets::object_name(l.seq)
                    ));
                    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
                    out.push(wasmkeep_server::secrets::sealed_with(&bytes).unwrap());
                }
                out
            }
        }

        const LATER: u64 = 60_000;

        fn now() -> u64 {
            super::super::now_ms() + LATER
        }

        /// `--all`: an app sealed with another key is refused, the others
        /// still finish, and the run fails at the end.
        #[tokio::test]
        async fn all_goes_on_past_a_refused_app_and_fails_at_the_end() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let (old, new, other) = (
                SecretsKey::generate().unwrap(),
                SecretsKey::generate().unwrap(),
                SecretsKey::generate().unwrap(),
            );
            seal(&store, "a-other", std::slice::from_ref(&other)).await;
            seal(&store, "b-rotated", &[old.clone(), new.clone()]).await;
            let e = finish(&store, "p", None, &new, now()).await.unwrap_err();
            assert!(e.to_string().contains("1 of 2"), "{e}");
            assert_eq!(ids(&store, "b-rotated").await, [new.id()]);
            assert_eq!(ids(&store, "a-other").await, [other.id()]);
            let one = Some(("acme".to_owned(), "b-rotated".to_owned()));
            finish(&store, "p", one, &new, now()).await.unwrap();
        }

        /// An object left in place fails the run.
        #[tokio::test]
        async fn objects_left_fail_the_run() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let (old, new) = (
                SecretsKey::generate().unwrap(),
                SecretsKey::generate().unwrap(),
            );
            seal(&store, "shop", &[old.clone(), new.clone()]).await;
            let junk = object_store::path::Path::from(format!(
                "p/apps/acme/shop/secrets/{}",
                wasmkeep_server::secrets::object_name(9)
            ));
            store.put(&junk, b"junk\n".to_vec().into()).await.unwrap();
            let app = Some(("acme".to_owned(), "shop".to_owned()));
            let e = finish(&store, "p", app, &new, now()).await.unwrap_err();
            assert!(e.to_string().contains("1 of 1"), "{e}");
            assert_eq!(ids_count(&store).await, 2);
        }

        async fn ids_count(store: &Arc<dyn ObjectStore>) -> usize {
            SecretStore::new(Arc::clone(store), "p", "acme", "shop")
                .list()
                .await
                .unwrap()
                .len()
        }
    }

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
