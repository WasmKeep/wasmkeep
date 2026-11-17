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
    /// Resume the tombstone identified by this ID.
    pub resume: Option<String>,
    /// How long to wait for the app's live nodes to stop.
    pub wait: Duration,
    /// Remove the data even though nodes still look live (after `cluster
    /// fence`), and even beside nodes that ignore a tombstone.
    pub force: bool,
    /// Who asks, written in the tombstone.
    pub who: String,
    /// How long after the app's nodes last wrote (and after the tombstone)
    /// a listing must come back empty before the app is reported deleted:
    /// a write a node sent before it stopped can land that late
    /// ([`LATE_WRITES`]).
    pub late: Duration,
}

/// [`DeleteApp::late`] for `wasmkeep app delete`: as long as a bucket
/// client waits for a write (30 s) plus a lease's ttl with the defaults.
pub const LATE_WRITES: Duration = Duration::from_secs(40);

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
    if let Some(id) = &args.resume {
        let root = wasmkeep_cluster::state_root::StateRoot::new("", tenant, name);
        wasmkeep_cluster::control::checked_delete_key(&root, "app", "app", id)
            .map_err(super::bucket)?;
        say(format!("Operation ID: {id}"));
        let tombstone = app.tombstone().await?;
        ensure!(
            tombstone
                .as_deref()
                .is_some_and(|body| body.lines().any(|line| line == format!("id {id}"))),
            "operation {id} is unknown or its app incarnation changed; nothing removed"
        );
    }
    app.check_not_restoring().await?;
    let shown = app.name();
    let existed = app.tombstone().await?;
    // A node of a version before ADR 0028 ignores a tombstone: it would
    // keep serving the app, and writing, while its data is removed. Asked
    // before the tombstone, and again on a run that resumes.
    let old = super::nodes_without_ends(Arc::clone(&store), prefix, tenant, name).await?;
    ensure!(
        old.is_empty() || args.force,
        "live nodes of {shown} run a version that ignores an app's tombstone ({}): upgrade \
         or stop them first{}",
        old.join(", "),
        if existed.is_none() {
            " (nothing was written)"
        } else {
            ""
        }
    );
    let mut since_ms = now();
    if existed.is_none() {
        ensure!(
            args.resume.is_none(),
            "delete tombstone disappeared; nothing filed"
        );
        ensure!(
            !app.app_keys().await?.is_empty(),
            "no such app: nothing of {shown} is in the bucket"
        );
        let body = format!(
            "wasmkeep-app-deleted 1\nby {}\nat {}\nid {}\n",
            args.who,
            since_ms,
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
    let tombstone = app
        .tombstone()
        .await?
        .ok_or_else(|| DeployError::Refused("delete tombstone disappeared".into()))?;
    if let Some(expected) = existed.as_ref() {
        ensure!(
            &tombstone == expected,
            "app incarnation changed; delete stopped"
        );
    }
    let id = tombstone
        .lines()
        .find_map(|line| line.strip_prefix("id "))
        .ok_or_else(|| DeployError::Refused("delete tombstone has no operation ID".into()))?;
    if let Some(expected) = &args.resume {
        ensure!(expected == id, "delete operation changed; nothing removed");
    }
    say(format!("Operation ID: {id}"));
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
    // Every node lease of the app still marked live (a node paused, or cut
    // off from the bucket, past its horizon) is fenced: it can never renew,
    // so it never acts again on what is removed below.
    since_ms = since_ms.max(fence_app_nodes(&store, prefix, tenant, name, say).await?);
    let mut removed = 0;
    let mut rounds = 0;
    loop {
        ensure!(
            app.tombstone().await?.as_deref() == Some(tombstone.as_str()),
            "app incarnation changed; delete stopped"
        );
        let keys = app.app_keys().await?;
        if keys.is_empty() {
            // A write sent before a node stopped can land until its
            // client gives it up: nothing is declared gone before then.
            let quiet_at =
                since_ms.saturating_add(u64::try_from(args.late.as_millis()).unwrap_or(u64::MAX));
            let now = now();
            if now >= quiet_at {
                let late = late_listings(&store, &app, &shown, id, now).await;
                return Ok(format!(
                    "Deleted {shown}: {removed} object(s) removed. The tombstone stays; \
                     `wasmkeep deploy --recreate` uses the name again. Destroy the app's \
                     secrets key yourself if it had one.\n{late}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(quiet_at - now)).await;
            continue;
        }
        rounds += 1;
        ensure!(
            rounds <= ROUNDS,
            "{shown}: objects keep appearing under its prefixes; run the command again"
        );
        app.delete_keys(&keys).await?;
        removed += keys.len();
        tokio::time::sleep(settle).await;
        // A node started while the command waited: it stops on the
        // tombstone; whatever it wrote goes in the next round.
        if !args.force {
            let old = super::nodes_without_ends(Arc::clone(&store), prefix, tenant, name).await?;
            ensure!(
                old.is_empty(),
                "{shown}: nodes that ignore a tombstone started during the delete ({}): stop \
                 them, then run the command again",
                old.join(", ")
            );
            while !live_ids(&store, prefix, tenant, name).await?.is_empty() {
                tokio::time::sleep(poll).await;
                ensure!(
                    started.elapsed() < wait + Duration::from_secs(60),
                    "{shown}: a node of the app started during the delete and is still live: \
                     run the command again"
                );
            }
            since_ms = since_ms.max(fence_app_nodes(&store, prefix, tenant, name, say).await?);
        }
    }
}

/// When the listings that remove late uploads are due (ADR 0030, section 5,
/// decision 1): one hour and 25 hours after the tombstone, by its
/// last-modified time (the bucket's clock). An upload through a link issued
/// before the delete may land that late. Each run of the command removes
/// what is there; this says which listings are still to come.
async fn late_listings(
    store: &Arc<dyn ObjectStore>,
    app: &super::AppRef,
    shown: &str,
    id: &str,
    now_ms: u64,
) -> String {
    use object_store::ObjectStoreExt as _;
    const HOUR: u64 = 60 * 60 * 1_000;
    let Ok(meta) = store.head(&app.path(super::TOMBSTONE)).await else {
        return String::new();
    };
    let at = u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0);
    let resume = format!("wasmkeep app delete --app {shown} --bucket ... --resume {id}");
    let left = |due: u64| (due.saturating_sub(now_ms)).div_ceil(60_000);
    let (first, last) = (at + HOUR, at + 25 * HOUR);
    if now_ms < first {
        format!(
            "Files: uploads through links issued before the delete may still land. Run              `{resume}` in {} min and again in {} min to remove them.",
            left(first),
            left(last)
        )
    } else if now_ms < last {
        format!(
            "Files: the first late listing ran; run `{resume}` once more in {} min.",
            left(last)
        )
    } else {
        "Files: both late listings ran; nothing is left to wait for.".to_owned()
    }
}

fn now() -> u64 {
    u64::try_from(crate::alarm::now_ms_i64()).unwrap_or(0)
}

/// Fences every node lease of `tenant/app` still written live (ADR 0020,
/// as `cluster fence` does), and returns the latest time any of the app's
/// nodes wrote its heartbeat (ms, its clock), or 0.
/// # Errors
/// Returns an error if listing or fencing a node fails.
pub async fn fence_app_nodes(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    name: &str,
    say: &mut (dyn FnMut(String) + Send),
) -> Result<u64, DeployError> {
    use wasmkeep_cluster::membership::NODES_PREFIX;
    use wasmkeep_cluster::node_lease::{NodeState, OperatorFence, operator_fence};
    struct Wall;
    impl wasmkeep_cluster::lease::Clock for Wall {
        fn now_ms(&self) -> u64 {
            now()
        }
    }
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    };
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(store), prefix);
    let served = format!("{tenant}/{name}");
    let nodes = wasmkeep_cluster::membership::all_heartbeats(&bucket)
        .await
        .map_err(super::bucket)?;
    let mut last = 0;
    for node in nodes.into_iter().filter(|n| n.app == served) {
        last = last.max(node.written_at_ms);
        if node.lease != Some(NodeState::Live) {
            continue;
        }
        let key = format!("{NODES_PREFIX}{}", node.id);
        if let OperatorFence::Fenced { written_at_ms, .. } = operator_fence(&bucket, &Wall, &key)
            .await
            .map_err(super::bucket)?
        {
            last = last.max(written_at_ms);
            say(format!(
                "fenced node {} (its lease was still written live)",
                node.id
            ));
        }
    }
    Ok(last)
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
