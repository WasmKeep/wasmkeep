//! `wasmkeep app delete` ([ADR 0028](../../../docs/adr/0028-actors-that-end.md),
//! section 5): the tombstone first, then, once no live node of the app is
//! left, every key under its two prefixes but the tombstone.

use std::sync::Arc;
use std::time::Duration;

use wasmkeep_server::apps::{self, AppRef};

use crate::AppDeleteArgs;

/// How often the command looks at the app's live nodes while it waits.
const POLL: Duration = Duration::from_secs(1);

/// After a removal round, how long the command waits before it lists
/// again: a write sent before a node stopped may still land.
const SETTLE: Duration = Duration::from_secs(3);

/// Removal rounds at most, each a listing and its deletes.
const ROUNDS: usize = 10;

/// `wasmkeep app delete`.
pub(crate) fn run(args: &AppDeleteArgs) -> anyhow::Result<()> {
    let (store, prefix) = crate::open_bucket(&args.bucket)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let done = rt.block_on(delete(
        store,
        &prefix,
        args,
        &mut |line| println!("{line}"),
        POLL,
        SETTLE,
    ))?;
    println!("{done}");
    Ok(())
}

/// The delete, on `store` under `prefix`; progress lines go to `say`. The
/// last line, on success.
pub(crate) async fn delete(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    args: &AppDeleteArgs,
    say: &mut dyn FnMut(String),
    poll: Duration,
    settle: Duration,
) -> anyhow::Result<String> {
    let (tenant, name) = &args.app;
    anyhow::ensure!(
        !(tenant == "default" && name == "default"),
        "default/default is the implicit app of classes given on the command line: its nodes \
         do not follow deploys and would not see a tombstone. Stop them, then remove its \
         prefixes (state/default/default/) yourself"
    );
    let app = AppRef::new(Arc::clone(&store), prefix, tenant, name);
    let shown = app.name();
    let existed = app.tombstone().await.map_err(bucket)?;
    if existed.is_none() {
        anyhow::ensure!(
            !app.app_keys().await.map_err(bucket)?.is_empty(),
            "no such app: nothing of {shown} is in the bucket"
        );
        // A node of a version before ADR 0028 ignores a tombstone: it would
        // keep serving the app, and writing, while its data is removed.
        let old = apps::nodes_without_ends(Arc::clone(&store), prefix, tenant, name)
            .await
            .map_err(bucket)?;
        anyhow::ensure!(
            old.is_empty() || args.force,
            "live nodes of {shown} run a version that ignores an app's tombstone ({}): upgrade \
             or stop them first (nothing was written)",
            old.join(", ")
        );
        let body = format!(
            "wasmkeep-app-deleted 1\nby {}\nat {}\nid {}\n",
            std::env::var("USER").unwrap_or_else(|_| "-".into()),
            crate::status::now_ms(),
            wasmkeep_cluster::control::new_request_id()
        );
        if app.write_tombstone(&body).await.map_err(bucket)? {
            say(format!(
                "{shown}: tombstone written; deploys now refuse the app and its nodes stop \
                 serving it within a few seconds"
            ));
        }
    } else {
        say(format!(
            "{shown}: already deleted (its tombstone is there); finishing"
        ));
    }
    // Then the data, once no live node of the app is left.
    let started = tokio::time::Instant::now();
    let wait = Duration::from_secs(args.wait);
    loop {
        let live = live_ids(&store, prefix, tenant, name).await?;
        if live.is_empty() {
            break;
        }
        if args.force {
            say(format!(
                "warning: nodes of {shown} still look live ({}); removing its data anyway \
                 (--force)",
                live.join(", ")
            ));
            break;
        }
        if started.elapsed() >= wait {
            anyhow::bail!(
                "{shown}: nodes still live after {}s ({}): the tombstone is written and \
                 nothing was removed. Run the command again once they stopped (or after \
                 `wasmkeep cluster fence`, with --force)",
                args.wait,
                live.join(", ")
            );
        }
        tokio::time::sleep(poll).await;
    }
    let mut removed = 0;
    for _ in 0..ROUNDS {
        let keys = app.app_keys().await.map_err(bucket)?;
        if keys.is_empty() {
            return Ok(format!(
                "Deleted {shown}: {removed} object(s) removed. The tombstone stays; \
                 `wasmkeep deploy --recreate` uses the name again. Destroy the app's \
                 secrets key yourself if it had one."
            ));
        }
        app.delete_keys(&keys).await.map_err(bucket)?;
        removed += keys.len();
        tokio::time::sleep(settle).await;
        // A node started while the command waited: it stops on the
        // tombstone; whatever it wrote goes in the next round.
        if !args.force {
            while !live_ids(&store, prefix, tenant, name).await?.is_empty() {
                tokio::time::sleep(poll).await;
                anyhow::ensure!(
                    started.elapsed() < wait + Duration::from_secs(60),
                    "{shown}: a node of the app started during the delete and is still live: \
                     run the command again"
                );
            }
        }
    }
    anyhow::bail!("{shown}: objects keep appearing under its prefixes; run the command again")
}

async fn live_ids(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    tenant: &str,
    name: &str,
) -> anyhow::Result<Vec<String>> {
    Ok(apps::live_nodes(Arc::clone(store), prefix, tenant, name)
        .await
        .map_err(bucket)?
        .into_iter()
        .map(|n| n.id)
        .collect())
}

fn bucket(e: apps::DeployError) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}
