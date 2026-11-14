//! Deleting an app (`wasmkeep app delete`,
//! [ADR 0028](../../../../docs/adr/0028-actors-that-end.md), section 5):
//! the tombstone first, then, once no live node of the app is left, every
//! key under its two prefixes but the tombstone, until a listing finds
//! none.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use super::DeployError;

/// Removal rounds at most, each a listing and its deletes.
const ROUNDS: usize = 10;

/// What `wasmkeep app delete` was asked.
#[derive(Debug, Clone)]
pub struct DeleteApp {
    /// How long to wait for the app's live nodes to stop.
    pub wait: Duration,
    /// Remove the data even though nodes still look live (after `cluster
    /// fence`), and even beside nodes that ignore a tombstone.
    pub force: bool,
    /// Who asks, written in the tombstone.
    pub who: String,
}

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(DeployError::Refused(format!($($fmt)+)));
        }
    };
}

macro_rules! bail {
    ($($fmt:tt)+) => {
        return Err(DeployError::Refused(format!($($fmt)+)))
    };
}

/// The delete, on `store` under `prefix`; progress lines go to `say`. The
/// last line, on success.
///
/// # Errors
/// Refused (nothing written, or the tombstone written and nothing removed),
/// or the bucket.
#[allow(clippy::too_many_arguments)]
pub async fn delete_app(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    name: &str,
    args: &DeleteApp,
    say: &mut (dyn FnMut(String) + Send),
    poll: Duration,
    settle: Duration,
) -> Result<String, DeployError> {
    ensure!(
        !(tenant == "default" && name == "default"),
        "default/default is the implicit app of classes given on the command line: its nodes \
         do not follow deploys and would not see a tombstone. Stop them, then remove its \
         prefixes (state/default/default/) yourself"
    );
    let app = super::AppRef::new(Arc::clone(&store), prefix, tenant, name);
    let shown = app.name();
    let existed = app.tombstone().await?;
    if existed.is_none() {
        ensure!(
            !app.app_keys().await?.is_empty(),
            "no such app: nothing of {shown} is in the bucket"
        );
        // A node of a version before ADR 0028 ignores a tombstone: it would
        // keep serving the app, and writing, while its data is removed.
        let old = super::nodes_without_ends(Arc::clone(&store), prefix, tenant, name).await?;
        ensure!(
            old.is_empty() || args.force,
            "live nodes of {shown} run a version that ignores an app's tombstone ({}): upgrade \
             or stop them first (nothing was written)",
            old.join(", ")
        );
        let body = format!(
            "wasmkeep-app-deleted 1\nby {}\nat {}\nid {}\n",
            args.who,
            u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0),
            wasmkeep_cluster::control::new_request_id()
        );
        if app.write_tombstone(&body).await? {
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
    let wait = args.wait;
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
            bail!(
                "{shown}: nodes still live after {}s ({}): the tombstone is written and \
                 nothing was removed. Run the command again once they stopped (or after \
                 `wasmkeep cluster fence`, with --force)",
                args.wait.as_secs(),
                live.join(", ")
            );
        }
        tokio::time::sleep(poll).await;
    }
    let mut removed = 0;
    for _ in 0..ROUNDS {
        let keys = app.app_keys().await?;
        if keys.is_empty() {
            return Ok(format!(
                "Deleted {shown}: {removed} object(s) removed. The tombstone stays; \
                 `wasmkeep deploy --recreate` uses the name again. Destroy the app's \
                 secrets key yourself if it had one."
            ));
        }
        app.delete_keys(&keys).await?;
        removed += keys.len();
        tokio::time::sleep(settle).await;
        // A node started while the command waited: it stops on the
        // tombstone; whatever it wrote goes in the next round.
        if !args.force {
            while !live_ids(&store, prefix, tenant, name).await?.is_empty() {
                tokio::time::sleep(poll).await;
                ensure!(
                    started.elapsed() < wait + Duration::from_secs(60),
                    "{shown}: a node of the app started during the delete and is still live: \
                     run the command again"
                );
            }
        }
    }
    bail!("{shown}: objects keep appearing under its prefixes; run the command again")
}

async fn live_ids(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    name: &str,
) -> Result<Vec<String>, DeployError> {
    Ok(super::live_nodes(Arc::clone(store), prefix, tenant, name)
        .await?
        .into_iter()
        .map(|n| n.id)
        .collect())
}
