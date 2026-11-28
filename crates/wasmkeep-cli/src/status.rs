//! `wasmkeep cluster status`: what the bucket says about a cluster, read
//! without asking any node. Heartbeats (`nodes/<id>`) give the nodes; each
//! app's `apps/<tenant>/<app>/current` gives its deploy. Nothing is written.

use std::fmt::Write as _;
use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
#[cfg(test)]
use object_store::ObjectStoreExt;
use object_store::path::Path as StorePath;
use serde_json::{Value, json};
use wasmkeep_cluster::membership::{self, DrainState, Load, NodeInfo};
use wasmkeep_cluster::node_lease::NodeState;
#[cfg(test)]
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_server::apps::Current;

/// Concurrent reads for CLI status views.
pub(crate) const STATUS_READS_AT_ONCE: usize = 32;

/// This machine's clock, in ms since the epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// This machine's clock, for the lease library.
pub(crate) struct Wall;

impl wasmkeep_cluster::lease::Clock for Wall {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

/// What `wasmkeep cluster forget` may delete, once checked.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Forgettable {
    /// A heartbeat in the format from before node leases.
    OldFormat,
    /// A `released` node lease: deleted only if no ownership record names
    /// its node ([`records_naming`]).
    Released,
}

/// Whether `wasmkeep cluster forget` may delete a heartbeat object with this
/// body at `now_ms`, older than its horizon plus a lease's ttl and grace
/// (the defaults, 25 s): one in the format from before node leases (a
/// retired node's), or a `released` node lease (ADR 0020, amendment of
/// 2026-11-20: its instance never writes again; deleted only if no record
/// names its node). A `live` one is never deleted (fence it to revoke the
/// node), nor a `fenced` one: its process did not stop cleanly, so records
/// likely still name it, and the fence is the evidence that frees them and
/// that ends that instance. A recent one may still run.
pub(crate) fn may_forget(body: &[u8], now_ms: u64) -> Result<Forgettable, String> {
    let Some(info) = NodeInfo::decode(body) else {
        return Err("this heartbeat is in a format this version cannot read: left alone".into());
    };
    let what = match info.lease {
        None => Forgettable::OldFormat,
        Some(NodeState::Released) => Forgettable::Released,
        Some(NodeState::Live) => {
            return Err(format!(
                "{} has a live node lease (ADR 0020), never deleted: its node may run. To revoke it, `wasmkeep cluster fence {}`",
                info.id, info.id
            ));
        }
        Some(NodeState::Fenced) => {
            return Err(format!(
                "{} is fenced (ADR 0020), never deleted: the fence is what frees the actors its process held and ends that process",
                info.id
            ));
        }
    };
    let lease = wasmkeep_cluster::lease::LeaseConfig::default_safe();
    let window = wasmkeep_server::heartbeat_horizon_ms(wasmkeep_server::DEFAULT_HEARTBEAT, &lease)
        .saturating_add(lease.ttl_ms())
        .saturating_add(lease.grace_ms());
    if info.written_at_ms > now_ms || now_ms - info.written_at_ms <= window {
        return Err(format!(
            "{}'s heartbeat is recent (or dated ahead of this clock): its node may still run; stop it and try again once it is {} s old",
            info.id,
            window / 1000
        ));
    }
    Ok(what)
}

/// How many ownership records of the apps under `prefix`
/// (`state/<tenant>/<app>/owners/...`) name `node`, any instance; a record
/// that cannot be read counts, to be safe. One listing per tenant and per
/// app, and one read per record.
pub(crate) async fn records_naming(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    node: &str,
) -> Result<usize, String> {
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStoreExt as _;
    let prefix = prefix.trim_matches('/');
    let base = if prefix.is_empty() {
        StorePath::from("state")
    } else {
        StorePath::from(format!("{prefix}/state"))
    };
    let listed = |p: StorePath| async move {
        store
            .list_with_delimiter(Some(&p))
            .await
            .map(|l| l.common_prefixes)
            .map_err(|e| format!("listing {p}: {e}"))
    };
    let mut named = 0;
    for tenant in listed(base).await? {
        for app in listed(tenant).await? {
            let owners = app.clone().join("owners");
            let records: Vec<_> = store
                .list(Some(&owners))
                .try_collect()
                .await
                .map_err(|e| format!("listing {owners}: {e}"))?;
            for meta in records {
                let body = match store.get(&meta.location).await {
                    Ok(r) => r.bytes().await.map_err(|e| e.to_string())?,
                    Err(object_store::Error::NotFound { .. }) => continue,
                    Err(e) => return Err(format!("reading {}: {e}", meta.location)),
                };
                let names = wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)
                    .is_none_or(|r| r.holder.is_some_and(|(n, _)| n == node));
                if names {
                    named += 1;
                }
            }
        }
    }
    Ok(named)
}

/// `wasmkeep cluster forget <node>` on `store` under `prefix` at `now_ms`:
/// deletes `nodes/<node>` if [`may_forget`] allows it. What it did, or why
/// it refused.
///
/// A store's DELETE takes no condition. For a released lease of an app
/// whose upgrade to 0.9 is finalized (ADR 0043: every node that can start
/// there knows the mark), the lease is first replaced with the mark of
/// this command, by a write conditioned on the version that was checked,
/// and the mark is deleted: a process that started under that id
/// meanwhile makes that write fail, and one that reads the mark waits for
/// it to be gone. Before the finalize, and for a heartbeat from before
/// node leases, the object is read again and deleted, as before 0.9, and
/// the answer says so. A mark an earlier run left is deleted.
pub(crate) async fn forget_on(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    node: &str,
    now_ms: u64,
) -> Result<String, String> {
    use wasmkeep_cluster::control::Keys as _;
    use wasmkeep_cluster::lease::Bucket as _;
    use wasmkeep_cluster::node_lease::{Forgot, forget_finish, forget_released, forgotten};
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(store), prefix);
    let key = format!("nodes/{node}");
    let changed =
        || format!("{key} changed while it was checked: left alone; run the command again");
    let Some((body, version)) = bucket
        .get(&key)
        .await
        .map_err(|e| format!("reading {key}: {e:?}"))?
    else {
        return Ok(format!(
            "{node} has no heartbeat object: nothing to forget."
        ));
    };
    // A delete of the mark that gets no answer is given up, not sent
    // again for as long as the bucket client would: one that landed much
    // later would remove the lease a node created meanwhile.
    let bound = wasmkeep_cluster::node_lease::forget_delete_bound_ms(
        &wasmkeep_cluster::lease::LeaseConfig::default_safe(),
    );
    let give_up = || tokio::time::sleep(std::time::Duration::from_millis(bound));
    let unfinished = || {
        format!(
            "{key} holds this command's mark, and its delete got no answer within {} s: run \
             `wasmkeep cluster forget {node}` again",
            bound / 1000
        )
    };
    // The mark of a run that did not finish: the lease it replaced was
    // checked then, and nothing writes over a mark.
    if forgotten(&body).is_some() {
        return match forget_finish(&bucket, &key, give_up())
            .await
            .map_err(|e| format!("deleting {key}: {e:?}"))?
        {
            Forgot::Unfinished => Err(unfinished()),
            Forgot::Removed => Ok(format!(
                "Removed {node}'s node lease: an earlier `cluster forget` had replaced it with \
                 its mark and not deleted it."
            )),
            Forgot::Changed => Err(changed()),
        };
    }
    let what = may_forget(&body, now_ms)?;
    if what == Forgettable::Released {
        // No new record can name it: only its own process writes records
        // naming it, and that process released. A new process under the
        // id would replace the object first, which the read below sees.
        let named = records_naming(store, prefix, node).await?;
        if named > 0 {
            return Err(format!(
                "{node}'s released lease is kept: {named} ownership record(s) still name it, and it is what frees them. Each is taken over by the next request for its actor; run the command again later"
            ));
        }
    }
    // With the mark, once every node that can start under this id knows
    // it: the write is conditioned on the version checked above.
    let mut unmarked = String::new();
    if what == Forgettable::Released {
        let app = NodeInfo::decode(&body).map(|i| i.app).unwrap_or_default();
        let lease = wasmkeep_cluster::lease::LeaseConfig::default_safe();
        let horizon =
            wasmkeep_server::heartbeat_horizon_ms(wasmkeep_server::DEFAULT_HEARTBEAT, &lease);
        match wasmkeep_cluster::upgrade::forget_gate(&bucket, &app, now_ms, horizon)
            .await
            .map_err(|e| format!("reading the upgrade floor and the nodes of {app}: {e:?}"))?
        {
            Ok(()) => {
                return match forget_released(&bucket, &key, &version, now_ms, give_up())
                    .await
                    .map_err(|e| {
                        format!(
                            "removing {key}: {e:?}. Its mark may have been written: run the \
                             command again to finish"
                        )
                    })? {
                    Forgot::Removed => Ok(format!("Removed {node}'s released node lease.")),
                    Forgot::Changed => Err(changed()),
                    Forgot::Unfinished => Err(unfinished()),
                };
            }
            Err(why) => {
                unmarked = format!(
                    " Removed as before 0.9, by a delete without a condition: a process \
                     starting under that id at the same moment would lose its lease and stop, \
                     to be started again. The removal that keeps such a lease is used once \
                     the app's upgrade to 0.9 is finalized and no older node of it is live \
                     ({why})."
                );
            }
        }
    }
    // Read again right before the delete (a store's DELETE takes no
    // condition): a new node's first write over the stale heartbeat in
    // between must not be deleted. The window left is the time between the
    // two requests.
    let again = bucket
        .get(&key)
        .await
        .map_err(|e| format!("reading {key}: {e:?}"))?
        .map(|(b, _)| b);
    if again.as_deref() != Some(body.as_slice()) {
        return Err(changed());
    }
    bucket
        .delete(&key)
        .await
        .map_err(|e| format!("deleting {key}: {e:?}"))?;
    Ok(match what {
        Forgettable::OldFormat => format!("Removed {node}'s retired heartbeat."),
        Forgettable::Released => format!("Removed {node}'s released node lease.{unmarked}"),
    })
}

/// One heartbeat, as seen at the time of reading.
#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) info: NodeInfo,
    /// How long ago it was written, by this machine's clock (0 if the node's
    /// clock is ahead).
    pub(crate) age_ms: u64,
    /// Within the horizon the nodes themselves use.
    pub(crate) live: bool,
    /// How far its `written_at` is ahead of this machine's clock (0 if not).
    pub(crate) ahead_ms: u64,
}

impl Node {
    /// It has a cluster listener and publishes the pin to trust there.
    fn tls(&self) -> bool {
        self.info.cluster_address.is_some() && !self.info.tls_pins.is_empty()
    }

    /// Its load (payload line 10) while it is live: a stale, fenced or
    /// released heartbeat keeps the last load its node wrote, which no
    /// longer says anything.
    fn load(&self) -> Option<Load> {
        self.info.load.filter(|_| self.live)
    }

    /// Its drain state (line 9), while live: a fenced, released or stale
    /// node serves nothing whatever its last heartbeat said, and LIVE says
    /// why.
    fn state(&self) -> Option<DrainState> {
        self.info.drain.filter(|_| self.live)
    }
}

/// One app's deploy: `Ok(None)` when it has uploads but no `current` yet.
#[derive(Debug)]
pub(crate) struct App {
    pub(crate) name: String,
    pub(crate) current: Result<Option<Current>, String>,
    /// Whether the deployed version has a front (ADR 0023), from its
    /// manifest; `None` without a deployed version or when its manifest
    /// cannot be read (which is no error here: nodes refuse it).
    pub(crate) front: Option<bool>,
    /// The deployed version's manifest format number (5 for a batched
    /// class, 6 for a rate limit); `None` as for `front`.
    pub(crate) manifest_format: Option<u8>,
}

/// Everything `cluster status` prints.
#[derive(Debug)]
pub(crate) struct Report {
    pub(crate) bucket: String,
    pub(crate) read_at_ms: u64,
    pub(crate) horizon_ms: u64,
    pub(crate) nodes: Vec<Node>,
    pub(crate) apps: Vec<App>,
    /// The upgrade of WasmKeep itself (ADR 0043), for each app that has
    /// registered nodes: its floor and one state.
    pub(crate) upgrades: Vec<wasmkeep_cluster::upgrade::AppUpgrade>,
    /// The node ids whose key holds the mark of a `cluster forget` (ADR
    /// 0043) written more than [`FORGET_UNFINISHED_MS`] ago: the command
    /// deletes its mark at once, so one that old was left by a run that
    /// did not finish, and keeps a node from starting under that id.
    pub(crate) unfinished_forgets: Vec<String>,
    /// This `wasmkeep`'s minor version, for the warning of a read-only
    /// command about live nodes outside its range (ADR 0043, section 5);
    /// `None`: not judged (the reader says its version, a test need not).
    pub(crate) wasmkeep: Option<wasmkeep_cluster::app_format::floor::Minor>,
}

pub(crate) use wasmkeep_cli::verify::FORGET_UNFINISHED_MS;

/// Reads the cluster under `prefix` of `store`, at `now_ms`, counting a
/// heartbeat live for `horizon_ms`.
pub(crate) async fn read(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> anyhow::Result<Report> {
    read_with_limit(store, prefix, bucket, now_ms, horizon_ms, None).await
}

/// Inspector-only status read with bounded listings.
pub(crate) async fn read_bounded(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    now_ms: u64,
    horizon_ms: u64,
    max: usize,
) -> anyhow::Result<Report> {
    read_with_limit(store, prefix, bucket, now_ms, horizon_ms, Some(max)).await
}

async fn list_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    max: Option<usize>,
) -> Result<
    std::collections::BTreeMap<String, object_store::ObjectMeta>,
    wasmkeep_server::history::Error,
> {
    match max {
        Some(max) => wasmkeep_server::history::list_objects_bounded(store, prefix, max).await,
        None => wasmkeep_server::history::list_objects(store, prefix).await,
    }
}

async fn read_with_limit(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    now_ms: u64,
    horizon_ms: u64,
    max: Option<usize>,
) -> anyhow::Result<Report> {
    enum NodeBody {
        Forgotten(u64),
        Heartbeat(Box<NodeInfo>),
        Skip,
    }
    let node_prefix = format!("{}/nodes", prefix.trim_end_matches('/'))
        .trim_start_matches('/')
        .to_owned();
    let read_nodes = async {
        let objects = list_objects(&store, &node_prefix, max).await?;
        futures_util::stream::iter(objects.into_keys().map(|key| {
            let location = StorePath::from(format!("{node_prefix}/{key}"));
            let store = &store;
            async move {
                let body = metadata(store, &location).await?;
                let classified = if let Some(at) = wasmkeep_cluster::node_lease::forgotten(&body) {
                    NodeBody::Forgotten(at)
                } else if let Some(info) = NodeInfo::decode(&body) {
                    NodeBody::Heartbeat(Box::new(info))
                } else {
                    NodeBody::Skip
                };
                Ok::<_, anyhow::Error>((key, classified))
            }
        }))
        .buffer_unordered(STATUS_READS_AT_ONCE)
        .try_collect::<Vec<_>>()
        .await
    };
    let (objects, apps) = tokio::try_join!(read_nodes, read_apps(&store, prefix, bucket, max))?;
    let mut heartbeats = Vec::new();
    let mut unfinished_forgets = Vec::new();
    for (key, body) in objects {
        // Not a node: shown only when its command left it there.
        if let NodeBody::Forgotten(at) = body {
            if now_ms.saturating_sub(at) > FORGET_UNFINISHED_MS
                && wasmkeep_cluster::membership::validate_node_id(&key).is_ok()
            {
                unfinished_forgets.push(key);
            }
            continue;
        }
        if let NodeBody::Heartbeat(info) = body
            && key == info.id
            && wasmkeep_cluster::membership::validate_node_id(&info.id).is_ok()
        {
            heartbeats.push(*info);
        }
    }
    heartbeats.sort_by(|a, b| a.id.cmp(&b.id));
    unfinished_forgets.sort();
    // Each app's upgrade floor, one read per app with registered nodes.
    let floors = wasmkeep_cluster::object_store::ObjectStoreBucket::new(
        Arc::clone(&store),
        prefix.to_owned(),
    );
    let mut upgrades: Vec<_> = futures_util::stream::iter(
        wasmkeep_cluster::upgrade::apps_of(&heartbeats)
            .into_iter()
            .map(|app| {
                let floors = &floors;
                let heartbeats = &heartbeats;
                async move {
                    wasmkeep_cluster::upgrade::status_of(
                        floors, &app, heartbeats, now_ms, horizon_ms,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("reading the upgrade floor of {app}: {e}"))
                }
            }),
    )
    .buffer_unordered(STATUS_READS_AT_ONCE)
    .try_collect()
    .await?;
    upgrades.sort_by(|a, b| a.app.cmp(&b.app));
    let nodes = heartbeats
        .into_iter()
        .map(|info| Node {
            age_ms: now_ms.saturating_sub(info.written_at_ms),
            ahead_ms: info.written_at_ms.saturating_sub(now_ms),
            live: membership::is_live(&info, now_ms, horizon_ms),
            info,
        })
        .collect();
    Ok(Report {
        bucket: bucket.to_owned(),
        read_at_ms: now_ms,
        horizon_ms,
        nodes,
        apps,
        upgrades,
        unfinished_forgets,
        wasmkeep: None,
    })
}

/// App names from delimiter listings, independent of deployment history.
/// The inspector retains its bounded recursive listing contract.
/// Component bodies are never downloaded.
async fn read_apps(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    _bucket: &str,
    max: Option<usize>,
) -> anyhow::Result<Vec<App>> {
    let root = if prefix.is_empty() {
        StorePath::from("apps")
    } else {
        StorePath::from(format!("{prefix}/apps"))
    };
    let names: std::collections::BTreeSet<String> = if max.is_some() {
        list_objects(store, root.as_ref(), max)
            .await?
            .keys()
            .filter_map(|key| {
                let mut parts = key.split('/');
                Some(format!("{}/{}", parts.next()?, parts.next()?))
            })
            .collect()
    } else {
        let tenants = wasmkeep_server::history::list_with_delimiter(store, &root)
            .await?
            .common_prefixes;
        let listings: Vec<_> =
            futures_util::stream::iter(tenants.into_iter().map(|tenant| async move {
                wasmkeep_server::history::list_with_delimiter(store, &tenant).await
            }))
            .buffer_unordered(STATUS_READS_AT_ONCE)
            .try_collect()
            .await?;
        let base = format!("{root}/");
        listings
            .into_iter()
            .flat_map(|listing| {
                listing
                    .common_prefixes
                    .into_iter()
                    .chain(listing.objects.into_iter().map(|object| object.location))
            })
            .filter_map(|app| app.as_ref().strip_prefix(&base).map(str::to_owned))
            .collect()
    };
    let mut apps: Vec<_> = futures_util::stream::iter(names.into_iter().map(|name| {
        let app = name
            .split('/')
            .fold(root.clone(), |path, part| path.join(part));
        async move {
            let current = current(store, &app).await.map_err(|e| e.to_string());
            let manifest = match &current {
                Ok(Some(c)) => manifest(store, &app, &c.version).await.ok(),
                _ => None,
            };
            let front = manifest.as_ref().map(|m| m.front.is_some());
            let manifest_format = manifest.as_ref().map(|m| m.format_number());
            App {
                name,
                current,
                front,
                manifest_format,
            }
        }
    }))
    .buffer_unordered(STATUS_READS_AT_ONCE)
    .collect()
    .await;

    apps.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(apps)
}

impl Report {
    fn live(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.live)
    }

    /// Actors resident on the live nodes serving `app`, summed over those
    /// that say (payload line 10); `None` if none does.
    fn app_actors(&self, app: &str) -> Option<u64> {
        self.live()
            .filter(|n| n.info.app == app)
            .filter_map(Node::load)
            .map(|l| l.actors)
            .reduce(u64::saturating_add)
    }

    /// Whether `app`'s spread actors are on their homes (ADR 0040, section
    /// 14), from its live nodes' load lines: `balanced` when every one
    /// publishes `misplaced=0`, `rebalancing` while some do not, or
    /// `paused` with the reason a node's pass waits for (a node draining or
    /// not serving, two minor versions live). `None` without a live node
    /// publishing the count (a version before it). A node with
    /// rebalancing switched off counts as the others do: its heartbeat
    /// does not say so.
    fn app_rebalance(&self, app: &str) -> Option<(&'static str, Option<String>)> {
        let nodes: Vec<&Node> = self.live().filter(|n| n.info.app == app).collect();
        let counts: Vec<Option<u64>> = nodes
            .iter()
            .map(|n| n.load().and_then(|l| l.misplaced))
            .collect();
        if counts.iter().all(Option::is_none) {
            return None;
        }
        if let Some(n) = nodes
            .iter()
            .find(|n| n.state() != Some(DrainState::Serving))
        {
            let why = match n.state() {
                Some(DrainState::Draining | DrainState::Drained | DrainState::Stuck) => {
                    format!("{} drains", n.info.id)
                }
                _ => format!("{} not serving", n.info.id),
            };
            return Some(("paused", Some(why)));
        }
        let minor = |n: &&Node| wasmkeep_cluster::rebalance::minor(&n.info.version);
        if nodes
            .iter()
            .any(|n| minor(n).is_none() || minor(n) != minor(&nodes[0]))
        {
            return Some(("paused", Some("mixed versions".to_owned())));
        }
        if counts.iter().all(|c| *c == Some(0)) {
            Some(("balanced", None))
        } else {
            Some(("rebalancing", None))
        }
    }

    /// Whether any node is live: `cluster status` fails otherwise.
    pub(crate) fn any_live(&self) -> bool {
        self.live().next().is_some()
    }

    /// The WasmKeep versions live nodes run, each with its nodes.
    fn versions(&self) -> Vec<(&str, Vec<&str>)> {
        let mut out: Vec<(&str, Vec<&str>)> = Vec::new();
        for n in self.live() {
            match out.iter_mut().find(|(v, _)| *v == n.info.version) {
                Some((_, ids)) => ids.push(&n.info.id),
                None => out.push((&n.info.version, vec![&n.info.id])),
            }
        }
        out.sort();
        out
    }

    /// Live nodes with and without a cluster listener.
    fn tls_split(&self) -> (Vec<&str>, Vec<&str>) {
        let (tls, plain): (Vec<&Node>, Vec<&Node>) = self.live().partition(|n| n.tls());
        fn ids(v: Vec<&Node>) -> Vec<&str> {
            v.into_iter().map(|n| n.info.id.as_str()).collect()
        }
        (ids(tls), ids(plain))
    }

    /// Nodes whose heartbeat is in a format from before node leases and
    /// recent by the start guard's test (its horizon plus a lease's ttl and
    /// grace, the defaults: one read cannot know a cluster's own timings),
    /// or dated ahead of this clock (one read cannot tell how long it has
    /// been): an old version may still run there (ADR 0020).
    fn old_format_running(&self) -> Vec<&str> {
        let lease = wasmkeep_cluster::lease::LeaseConfig::default_safe();
        let window = self
            .horizon_ms
            .saturating_add(lease.ttl_ms())
            .saturating_add(lease.grace_ms());
        self.nodes
            .iter()
            .filter(|n| n.info.lease.is_none() && n.age_ms < window)
            .map(|n| n.info.id.as_str())
            .collect()
    }

    /// What an operator should look at, one line each.
    pub(crate) fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.any_live() {
            out.push(if self.nodes.is_empty() {
                "no node has a heartbeat under this prefix".to_owned()
            } else {
                "no node is live: every heartbeat is older than the horizon".to_owned()
            });
        }
        let (tls, plain) = self.tls_split();
        if !tls.is_empty() && !plain.is_empty() {
            out.push(format!(
                "mixed plain and TLS nodes: {} still plain while {} use the cluster listener; \
                 give every node a cluster listener, then drop --cluster-plaintext accept (ADR 0011)",
                plain.join(", "),
                tls.join(", ")
            ));
        }
        let stuck: Vec<&str> = self
            .live()
            .filter(|n| n.info.drain == Some(DrainState::Stuck))
            .map(|n| n.info.id.as_str())
            .collect();
        if !stuck.is_empty() {
            out.push(format!(
                "draining but stuck: {} still hold actors and have no node to hand them to; \
                 start or undrain a node that serves the same app (ADR 0014)",
                stuck.join(", ")
            ));
        }
        for id in &self.unfinished_forgets {
            out.push(format!(
                "{id}: forget did not finish; run `wasmkeep cluster forget {id}` (no node \
                 starts under that id until then)"
            ));
        }
        // ADR 0035: an app whose live nodes differ on `recorded-head`
        // cannot enable the format, and if it did, refuses service.
        let mut by_app: std::collections::BTreeMap<&str, (Vec<&str>, Vec<&str>)> =
            std::collections::BTreeMap::new();
        for n in self.live() {
            let e = by_app.entry(n.info.app.as_str()).or_default();
            if n.info.can_recorded_head {
                e.0.push(n.info.id.as_str());
            } else {
                e.1.push(n.info.id.as_str());
            }
        }
        for (app, (with, without)) in by_app {
            if !with.is_empty() && !without.is_empty() {
                out.push(format!(
                    "{app}: live nodes differ on the recorded-head ownership format: {} {} not \
                     support it; upgrade them before `wasmkeep app format --enable` (ADR 0035), \
                     and if it is enabled already, the app is refused service meanwhile",
                    without.join(", "),
                    if without.len() == 1 { "does" } else { "do" }
                ));
            }
        }
        let old = self.old_format_running();
        if !old.is_empty() {
            out.push(format!(
                "an older WasmKeep version runs: {} still write heartbeats in the format from \
                 before node leases; stop them, new nodes refuse to run alongside (ADR 0020)",
                old.join(", ")
            ));
        }
        // Beyond the skew nodes allow each other (the default timings).
        let skew = wasmkeep_server::LeaseConfig::default_safe().max_skew_ms();
        let ahead: Vec<String> = self
            .live()
            .filter(|n| n.info.lease.is_some() && n.ahead_ms > skew)
            .map(|n| format!("{} ({})", n.info.id, rough(n.ahead_ms)))
            .collect();
        if !ahead.is_empty() {
            out.push(format!(
                "node leases dated ahead of this clock: {}. They never go stale, so no node \
                 fences them and their actors stay held until that date; fix the clock that is \
                 wrong (theirs, or this machine's if it is behind). `wasmkeep cluster fence` \
                 stops such a node, but its fence keeps the date: its actors free 15 s after it \
                 (ADR 0020)",
                ahead.join(", ")
            ));
        }
        // Fenced or released with the date still ahead: not live, and its
        // actors still held until that date plus ttl and grace.
        let lease = wasmkeep_server::LeaseConfig::default_safe();
        let hold = lease.ttl_ms().saturating_add(lease.grace_ms());
        let held: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| {
                matches!(
                    n.info.lease,
                    Some(
                        wasmkeep_cluster::node_lease::NodeState::Fenced
                            | wasmkeep_cluster::node_lease::NodeState::Released
                    )
                ) && n.ahead_ms > skew
            })
            .map(|n| {
                format!(
                    "{} (free in {})",
                    n.info.id,
                    rough(n.ahead_ms.saturating_add(hold))
                )
            })
            .collect();
        if !held.is_empty() {
            out.push(format!(
                "fenced or released nodes dated ahead of this clock: {}. Their actors stay held \
                 until that date plus 15 s; fix the clock that is wrong",
                held.join(", ")
            ));
        }
        // ADR 0043, section 5: what the skew check refuses, and a live
        // node below an app's floor.
        for upgrade in &self.upgrades {
            if upgrade.state.exit_code() == 1 {
                out.push(format!("upgrade of {}", upgrade.line()));
            }
        }
        // And what this `wasmkeep` itself does not run beside: it reads
        // and goes on, as `cluster upgrade status` does.
        if let Some(this) = self.wasmkeep {
            out.extend(
                self.upgrades
                    .iter()
                    .filter_map(|u| crate::upgrade_cmd::reader_warning(u, this)),
            );
        }
        let versions = self.versions();
        if versions.len() > 1 {
            let list: Vec<String> = versions
                .iter()
                .map(|(v, ids)| format!("{v} on {}", ids.join(", ")))
                .collect();
            out.push(format!(
                "live nodes run different WasmKeep versions ({}): upgrade every node before you enable or deploy what needs a new capability word",
                list.join("; ")
            ));
        }
        out
    }

    /// The live node ids whose heartbeats name process `process`.
    pub(crate) fn process_nodes(&self, process: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.live && n.info.process.as_deref() == Some(process))
            .map(|n| n.info.id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Each process the live heartbeats name, with its node ids, sorted.
    fn processes(&self) -> Vec<(String, Vec<String>)> {
        let mut names: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.live)
            .filter_map(|n| n.info.process.clone())
            .collect();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|p| {
                let nodes = self.process_nodes(&p);
                (p, nodes)
            })
            .collect()
    }

    pub(crate) fn to_json(&self) -> Value {
        let (tls, plain) = self.tls_split();
        let nodes: Vec<Value> = self
            .nodes
            .iter()
            .map(|n| {
                json!({
                    "id": n.info.id,
                    "ahead_ms": n.ahead_ms,
                    "live": n.live,
                    "age_ms": n.age_ms,
                    "written_at_ms": n.info.written_at_ms,
                    "version": n.info.version,
                    "app": n.info.app,
                    "address": n.info.address,
                    "cluster_address": n.info.cluster_address,
                    "tls_pins": n.info.tls_pins.iter().map(membership::pin_hex).collect::<Vec<_>>(),
                    "tls": n.tls(),
                    // Line 11's capability words, in the order a node
                    // writes them.
                    "words": n.info.capabilities().collect::<Vec<_>>(),
                    // Line 9, while live; null otherwise, or from a
                    // version without it.
                    "state": n.state().map(DrainState::as_str),
                    // ADR 0020: live, released or fenced; null for a
                    // heartbeat in an older format.
                    "lease": n.info.lease.map(NodeState::as_str),
                    // Payload line 10, while live; null otherwise, from a
                    // version without it, or (events) before a minute.
                    "actors": n.load().map(|l| l.actors),
                    "sockets": n.load().map(|l| l.sockets),
                    "events_per_min": n.load().and_then(|l| l.events_per_min),
                    "front_per_min": n.load().and_then(|l| l.front_per_min),
                    // ADR 0041: its app's rooms, and its resident fill in
                    // thousandths; null from a version without them.
                    "actor_room": n.load().and_then(|l| l.actor_room),
                    "socket_room": n.load().and_then(|l| l.socket_room),
                    "fill": resident_fill(n.load()),
                    // ADR 0040, section 14: its spread actors away from
                    // their home; null from a version without it.
                    "misplaced": n.load().and_then(|l| l.misplaced),
                    // ADR 0038: the process the node id runs in.
                    "process": n.info.process,
                })
            })
            .collect();
        let apps: Vec<Value> = self
            .apps
            .iter()
            .map(|a| {
                let actors = self.app_actors(&a.name);
                let rebalance = self.app_rebalance(&a.name);
                let mut app = match &a.current {
                    Ok(Some(c)) => {
                        json!({"app": a.name, "version": c.version, "seq": c.seq, "actors": actors, "front": a.front, "manifest_format": a.manifest_format})
                    }
                    Ok(None) => {
                        json!({"app": a.name, "version": null, "seq": null, "actors": actors, "front": null, "manifest_format": null})
                    }
                    Err(e) => {
                        json!({"app": a.name, "version": null, "seq": null, "actors": actors, "front": null, "manifest_format": null, "error": e})
                    }
                };
                // ADR 0040, section 14: `balanced`, `rebalancing` or
                // `paused` (with why); null without a node publishing it.
                app["rebalance"] = json!(rebalance.as_ref().map(|(w, _)| *w));
                app["rebalance_paused"] = json!(rebalance.and_then(|(_, why)| why));
                app
            })
            .collect();
        let versions: Vec<&str> = self.versions().into_iter().map(|(v, _)| v).collect();
        json!({
            "bucket": self.bucket,
            "read_at_ms": self.read_at_ms,
            "horizon_ms": self.horizon_ms,
            "nodes": nodes,
            "processes": self
                .processes()
                .into_iter()
                .map(|(p, nodes)| json!({"process": p, "nodes": nodes}))
                .collect::<Vec<_>>(),
            "apps": apps,
            "summary": {
                "nodes": self.nodes.len(),
                "live": self.live().count(),
                "live_tls": tls.len(),
                "live_plain": plain.len(),
                "mixed_tls": !tls.is_empty() && !plain.is_empty(),
                "versions": versions,
                "mixed_versions": versions.len() > 1,
                "finalize_pending": self
                    .upgrades
                    .iter()
                    .filter(|u| u.state == wasmkeep_cluster::upgrade::State::FinalizePending)
                    .map(|u| u.app.as_str())
                    .collect::<Vec<_>>(),
            },
            // ADR 0043: each app's upgrade floor and the state of its
            // upgrade, as `cluster upgrade status --json` lists them.
            "upgrade": self
                .upgrades
                .iter()
                .map(crate::upgrade_cmd::app_json)
                .collect::<Vec<_>>(),
            "warnings": self.warnings(),
        })
    }

    pub(crate) fn to_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Cluster {} (live: heartbeat within {})\n",
            self.bucket,
            age(self.horizon_ms)
        );
        if self.nodes.is_empty() {
            out.push_str("No nodes.\n");
        } else {
            let rows: Vec<[String; 16]> = self
                .nodes
                .iter()
                .map(|n| {
                    [
                        n.info.id.clone(),
                        match (n.live, n.info.lease) {
                            // A heartbeat from before node leases (ADR 0020).
                            (true, None) => "yes (old format)".to_owned(),
                            (false, None) => "no (retired, old format)".to_owned(),
                            (true, _) => "yes".to_owned(),
                            // Not live whatever its age (ADR 0020).
                            (false, Some(s @ (NodeState::Fenced | NodeState::Released))) => {
                                format!("no ({})", s.as_str())
                            }
                            (false, Some(NodeState::Live)) => "no".to_owned(),
                        },
                        age(n.age_ms),
                        n.state().map_or("-", DrainState::as_str).to_owned(),
                        count(n.load().map(|l| l.actors)),
                        count(n.load().map(|l| l.sockets)),
                        count(n.load().and_then(|l| l.events_per_min)),
                        count(n.load().and_then(|l| l.front_per_min)),
                        resident_fill(n.load())
                            .map_or_else(|| "-".to_owned(), |f| format!("{}.{}%", f / 10, f % 10)),
                        count(n.load().and_then(|l| l.misplaced)),
                        n.info.version.clone(),
                        or_dash(&n.info.app),
                        n.info.address.clone(),
                        or_dash(n.info.cluster_address.as_deref().unwrap_or_default()),
                        match (n.info.cluster_address.is_some(), n.info.tls_pins.len()) {
                            (false, 0) => "plain".to_owned(),
                            // An address whose pins line was malformed.
                            (true, 0) => "no pin".to_owned(),
                            (true, 1) => "pinned".to_owned(),
                            (true, k) => format!("pinned ({k})"),
                            (false, _) => "no address".to_owned(),
                        },
                        or_dash(&n.info.capabilities().collect::<Vec<_>>().join(" ")),
                    ]
                })
                .collect();
            table(
                &mut out,
                [
                    "NODE",
                    "LIVE",
                    "AGE",
                    "STATE",
                    "ACTORS",
                    "SOCKETS",
                    "EV/MIN",
                    "FRONT/MIN",
                    "FILL",
                    "AWAY",
                    "VERSION",
                    "APP",
                    "ADDRESS",
                    "CLUSTER ADDRESS",
                    "TLS",
                    "WORDS",
                ],
                &rows,
            );
        }
        // Node ids of one process (ADR 0038): grouped by the process their
        // heartbeats name.
        let processes = self.processes();
        if !processes.is_empty() {
            out.push('\n');
            let rows: Vec<[String; 2]> = processes
                .into_iter()
                .map(|(p, nodes)| [p, nodes.join(", ")])
                .collect();
            table(&mut out, ["PROCESS", "LIVE NODE IDS"], &rows);
        }
        out.push('\n');
        if self.apps.is_empty() {
            out.push_str("No deployed apps.\n");
        } else {
            let rows: Vec<[String; 7]> = self
                .apps
                .iter()
                .map(|a| {
                    let [version, seq] = match &a.current {
                        Ok(Some(c)) => [c.version.clone(), c.seq.to_string()],
                        Ok(None) => ["(none)".into(), "-".into()],
                        Err(e) => [format!("({e})"), "-".into()],
                    };
                    [
                        a.name.clone(),
                        version,
                        seq,
                        count(self.app_actors(&a.name)),
                        match a.front {
                            Some(true) => "yes".to_owned(),
                            Some(false) => "no".to_owned(),
                            None => "-".to_owned(),
                        },
                        a.manifest_format
                            .map_or_else(|| "-".to_owned(), |f| f.to_string()),
                        match self.app_rebalance(&a.name) {
                            Some((word, Some(why))) => format!("{word} ({why})"),
                            Some((word, None)) => word.to_owned(),
                            None => "-".to_owned(),
                        },
                    ]
                })
                .collect();
            table(
                &mut out,
                [
                    "APP",
                    "DEPLOYED VERSION",
                    "SEQ",
                    "ACTORS",
                    "FRONT",
                    "FORMAT",
                    "REBALANCE",
                ],
                &rows,
            );
        }
        let (tls, plain) = self.tls_split();
        let versions: Vec<&str> = self.versions().into_iter().map(|(v, _)| v).collect();
        let _ = writeln!(
            out,
            "\n{} of {} nodes live; TLS: {} with a cluster listener, {} plain; version{}: {}",
            self.live().count(),
            self.nodes.len(),
            tls.len(),
            plain.len(),
            if versions.len() == 1 { "" } else { "s" },
            if versions.is_empty() {
                "-".to_owned()
            } else {
                versions.join(", ")
            }
        );
        // One line per app: its upgrade floor and the state of its
        // upgrade (ADR 0043). What needs attention is a warning below.
        for upgrade in self.upgrades.iter().filter(|u| u.state.exit_code() != 1) {
            let _ = writeln!(out, "upgrade of {}", upgrade.line());
        }
        for w in self.warnings() {
            let _ = writeln!(out, "warning: {w}");
        }
        out
    }
}

/// A count, or `-` when unknown.
/// A node's resident fill in thousandths (ADR 0041, section 2), from its
/// load line's rooms; `None` without them.
fn resident_fill(load: Option<membership::Load>) -> Option<u64> {
    let load = load?;
    Some(wasmkeep_cluster::placement::fill(
        load.actors,
        load.actor_room?,
    ))
}

fn count(n: Option<u64>) -> String {
    n.map_or_else(|| "-".to_owned(), |n| n.to_string())
}

fn or_dash(s: &str) -> String {
    if s.is_empty() {
        "-".into()
    } else {
        s.to_owned()
    }
}

/// `850ms`, `4.2s`, `12m`, `3h`, `5d`.
pub(crate) fn age(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms}ms"),
        1_000..60_000 => format!("{:.1}s", ms as f64 / 1_000.0),
        60_000..3_600_000 => format!("{}m", ms / 60_000),
        3_600_000..172_800_000 => format!("{}h", ms / 3_600_000),
        _ => format!("{}d", ms / 86_400_000),
    }
}

/// Left-aligned columns, two spaces apart.
pub(crate) fn table<const N: usize>(out: &mut String, header: [&str; N], rows: &[[String; N]]) {
    let mut width = header.map(str::len);
    for row in rows {
        for (w, cell) in width.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let mut line = |cells: [&str; N]| {
        let mut s = String::new();
        for (i, (cell, w)) in cells.iter().zip(width).enumerate() {
            if i + 1 == N {
                s.push_str(cell);
            } else {
                let _ = write!(s, "{cell:<w$}  ");
            }
        }
        out.push_str(s.trim_end());
        out.push('\n');
    };
    line(header);
    for row in rows {
        line(std::array::from_fn(|i| row[i].as_str()));
    }
}

/// A duration for a warning: whole hours, minutes or seconds.
fn rough(ms: u64) -> String {
    match ms {
        3_600_000.. => format!("{} h", ms / 3_600_000),
        60_000.. => format!("{} min", ms / 60_000),
        _ => format!("{} s", ms / 1_000),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code

    use std::sync::atomic::{AtomicU64, Ordering};

    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use wasmkeep_cluster::lease::Clock;
    use wasmkeep_cluster::membership::Membership;

    use super::*;

    const HORIZON: u64 = 10_000;
    const NOW: u64 = 1_000_000;

    struct At(Arc<AtomicU64>);
    impl Clock for At {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// Writes node `id`'s heartbeat as of `at_ms`, with a cluster listener
    /// when `tls`.
    async fn beat(store: &Arc<dyn ObjectStore>, id: &str, at_ms: u64, tls: bool, app: &str) {
        beat_with(store, id, at_ms, tls, app, None).await;
    }

    /// [`beat`], saying `load` on payload line 10 when given.
    async fn beat_with(
        store: &Arc<dyn ObjectStore>,
        id: &str,
        at_ms: u64,
        tls: bool,
        app: &str,
        load: Option<membership::Load>,
    ) {
        let bucket = ObjectStoreBucket::new(Arc::clone(store), "demo");
        let m = Membership::new(
            bucket,
            At(Arc::new(AtomicU64::new(at_ms))),
            id,
            format!("http://{id}:8080"),
            9_000,
            1_000,
        )
        .unwrap()
        .with_app(app);
        let m = if tls {
            m.with_cluster_tls(format!("https://{id}:7443"), vec![[0xab; 32]])
        } else {
            m
        };
        if let Some(load) = load {
            m.set_load(load);
        }
        // As a node writes it: its node lease, format 3 (ADR 0020).
        let body = wasmkeep_cluster::node_lease::NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(format!("i-{id}")),
            written_at_ms: at_ms,
        }
        .encode_with(&m.lease_payload());
        store
            .put(
                &StorePath::from(format!("demo/nodes/{id}")),
                PutPayload::from(body),
            )
            .await
            .unwrap();
    }

    async fn put(store: &Arc<dyn ObjectStore>, key: &str, body: &str) {
        store
            .put(&StorePath::from(key), PutPayload::from(body.to_owned()))
            .await
            .unwrap();
    }

    fn report(store: Arc<dyn ObjectStore>) -> Report {
        block_on(read(store, "demo", "s3://b/demo", NOW, HORIZON)).unwrap()
    }

    /// ADR 0040, section 14: each node's `misplaced` count, and one word
    /// per app: `balanced` when every live node publishes 0,
    /// `rebalancing` while one does not, `paused` with the node that
    /// drains or the mixed versions; `-` from nodes of a version before it.
    #[test]
    fn the_rebalance_is_shown_per_node_and_per_app() {
        let away = |n| {
            Some(membership::Load {
                misplaced: Some(n),
                ..membership::Load::default()
            })
        };
        let rewrite = |store: Arc<dyn ObjectStore>, id: &'static str, from: String, to: &str| {
            let to = to.to_owned();
            async move {
                let key = StorePath::from(format!("demo/nodes/{id}"));
                let body = store.get(&key).await.unwrap().bytes().await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                assert!(body.contains(&from), "{body}");
                let body = body.replacen(&from, &to, 1);
                store.put(&key, PutPayload::from(body)).await.unwrap();
            }
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            for (id, app, n) in [
                ("a", "acme/one", 0),
                ("b", "acme/one", 0),
                ("c", "acme/two", 0),
                ("d", "acme/two", 3),
                ("e", "acme/three", 0),
                ("f", "acme/three", 0),
                ("g", "acme/four", 0),
                ("h", "acme/four", 1),
            ] {
                beat_with(&store, id, NOW, false, app, away(n)).await;
            }
            beat(&store, "i", NOW, false, "acme/five").await;
            // f drains; h runs another minor version.
            rewrite(
                Arc::clone(&store),
                "f",
                "\nserving\n".into(),
                "\ndraining\n",
            )
            .await;
            rewrite(
                Arc::clone(&store),
                "h",
                format!("\n{}\n", wasmkeep_cluster::VERSION),
                "\n0.1.0\n",
            )
            .await;
            for app in ["one", "two", "three", "four", "five"] {
                put(
                    &store,
                    &format!("demo/apps/acme/{app}/current"),
                    "0123456789abcdef 1\n",
                )
                .await;
            }
        });
        let r = report(store);
        let j = r.to_json();
        let node = |id: &str| {
            j["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == id)
                .unwrap()
                .clone()
        };
        assert_eq!(node("d")["misplaced"], 3);
        assert_eq!(node("a")["misplaced"], 0);
        assert_eq!(node("i")["misplaced"], Value::Null);
        let app = |name: &str| {
            let a = j["apps"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["app"] == name)
                .unwrap()
                .clone();
            (a["rebalance"].clone(), a["rebalance_paused"].clone())
        };
        assert_eq!(app("acme/one"), (json!("balanced"), Value::Null));
        assert_eq!(app("acme/two"), (json!("rebalancing"), Value::Null));
        assert_eq!(app("acme/three"), (json!("paused"), json!("f drains")));
        assert_eq!(app("acme/four"), (json!("paused"), json!("mixed versions")));
        assert_eq!(app("acme/five"), (Value::Null, Value::Null));
        let text = r.to_text();
        assert!(text.contains("AWAY"), "{text}");
        assert!(text.contains("REBALANCE"), "{text}");
        assert!(text.contains("paused (f drains)"), "{text}");
    }

    /// A live node lease dated ahead of this clock by more than the allowed
    /// skew never goes stale: flagged, with how far ahead, in the text and
    /// the JSON. One within the skew is not.
    #[test]
    fn a_node_lease_dated_ahead_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            beat(&store, "b", NOW + 600, false, "acme/shop").await;
            beat(&store, "c", NOW + 3_600_000, false, "acme/shop").await;
            // d fenced, its kept date an hour ahead: not live, actors held
            // (its heartbeat with the state a fence writes).
            beat(&store, "d", NOW + 3_600_000, false, "acme/shop").await;
            let key = StorePath::from("demo/nodes/d");
            let live = store.get(&key).await.unwrap().bytes().await.unwrap();
            let fenced =
                String::from_utf8(live.to_vec())
                    .unwrap()
                    .replacen("\nlive ", "\nfenced ", 1);
            store.put(&key, PutPayload::from(fenced)).await.unwrap();
        });
        let r = report(store);
        let held: Vec<String> = r
            .warnings()
            .into_iter()
            .filter(|w| w.starts_with("fenced or released nodes dated ahead"))
            .collect();
        assert!(
            held.len() == 1 && held[0].contains("d (free in 1 h)"),
            "{held:?}"
        );
        let warnings = r.warnings();
        let ahead: Vec<&String> = warnings
            .iter()
            .filter(|w| w.starts_with("node leases dated ahead"))
            .collect();
        assert_eq!(ahead.len(), 1, "{warnings:?}");
        assert!(
            ahead[0].contains("c (1 h)") && !ahead[0].contains(" b "),
            "{}",
            ahead[0]
        );
        let j = r.to_json();
        let by_id = |id: &str| {
            j["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == id)
                .unwrap()["ahead_ms"]
                .clone()
        };
        assert_eq!(by_id("c"), 3_600_000);
        assert_eq!(by_id("a"), 0);
    }

    /// ADR 0020, section 6: a heartbeat in the format from before node
    /// leases is an old version running while recent (flagged), a retired
    /// node's once stale (shown, not flagged).
    #[test]
    fn an_old_format_heartbeat_is_flagged_while_recent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            // The same version as `a`: only the format is old, so the
            // format is the one warning.
            let version = wasmkeep_cluster::VERSION.to_owned();
            let old = |at: u64| {
                format!("wasmkeep-node 2\nb\nhttp://b:8080\n{version}\n{at}\nacme/shop\n")
            };
            put(&store, "demo/nodes/b", &old(NOW - 1_000)).await;
            put(
                &store,
                "demo/nodes/c",
                &old(NOW - 60_000).replace("\nb\n", "\nc\n"),
            )
            .await;
        });
        let r = report(store);
        let text = r.to_text();
        assert!(text.contains("yes (old format)"), "{text}");
        assert!(text.contains("no (retired, old format)"), "{text}");
        let warnings = r.warnings();
        // The old binary also lacks `recorded-head` beside a (current) a
        // that announces it: both said (ADR 0035).
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(
            warnings[0].starts_with("acme/shop: live nodes differ on the recorded-head")
                && warnings[0].contains(": b does not support it"),
            "{warnings:?}"
        );
        assert!(
            warnings[1].contains("an older WasmKeep version runs: b "),
            "{warnings:?}"
        );
    }

    #[test]
    fn only_a_stale_old_format_heartbeat_may_be_forgotten() {
        let old = |at: u64| format!("wasmkeep-node 2\nb\nhttp://b\n0.0.0\n{at}\nt/\n");
        assert_eq!(
            may_forget(old(NOW - 60_000).as_bytes(), NOW),
            Ok(Forgettable::OldFormat)
        );
        let recent = may_forget(old(NOW - 1_000).as_bytes(), NOW).unwrap_err();
        assert!(recent.contains("may still run"), "{recent}");
        assert!(may_forget(old(NOW + 5_000).as_bytes(), NOW).is_err());
        let lease = format!(
            "wasmkeep-node 3\nfenced beef\n{}\nb\nhttp://b\n0.0.0\nt/\n",
            NOW - 600_000
        );
        let why = may_forget(lease.as_bytes(), NOW).unwrap_err();
        assert!(why.contains("never deleted"), "{why}");
        assert!(may_forget(b"junk", NOW).is_err());
    }

    /// A `released` node lease ([ADR 0020](../../docs/adr/0020-one-lease-per-node.md),
    /// amendment of 2026-11-20): its instance never writes again, so
    /// `cluster forget` deletes it once it is as old as a retired
    /// heartbeat must be, unless an ownership record of any app on the
    /// prefix still names that node (the released object is what frees
    /// such a record; deleted, the record would stay held). A fenced or
    /// live one is never deleted.
    #[test]
    fn a_released_lease_no_record_names_may_be_forgotten() {
        use wasmkeep_cluster::node_lease::OwnerRecord;
        use wasmkeep_cluster::state_root::{Family, StateRoot};
        let lease = |state: &str, at: u64| {
            format!(
                "wasmkeep-node 3\n{state} beef\n{at}\nb\nhttp://b\n0.0.0\nt/\n\n\nserving\n\ntake\n"
            )
        };
        let put = |store: &Arc<dyn ObjectStore>, key: String, body: Vec<u8>| {
            block_on(store.put(&StorePath::from(key), body.into())).unwrap();
        };
        let exists = |store: &Arc<dyn ObjectStore>| {
            block_on(store.head(&StorePath::from("nodes/b"))).is_ok()
        };
        // Released long ago, no record names it: removed.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(
            &store,
            "nodes/b".into(),
            lease("released", NOW - 600_000).into_bytes(),
        );
        let done = block_on(forget_on(&store, "", "b", NOW));
        assert!(done.is_ok(), "{done:?}");
        assert!(!exists(&store));
        // A record of an app on the prefix still names it: kept.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(
            &store,
            "nodes/b".into(),
            lease("released", NOW - 600_000).into_bytes(),
        );
        let root = StateRoot::new("", "acme", "shop");
        let record = OwnerRecord {
            holder: Some(("b".to_owned(), "beef".to_owned())),
            generation: 3,
            recorded: None,
        };
        put(
            &store,
            root.key(Family::Owners, "counter/alice"),
            record.encode(),
        );
        let other = OwnerRecord {
            holder: Some(("c".to_owned(), "cafe".to_owned())),
            generation: 1,
            recorded: None,
        };
        put(
            &store,
            root.key(Family::Owners, "counter/bob"),
            other.encode(),
        );
        let why = block_on(forget_on(&store, "", "b", NOW)).unwrap_err();
        assert!(why.contains("1 ownership record"), "{why}");
        assert!(exists(&store));
        // Recent: its node may have released moments ago; kept.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(
            &store,
            "nodes/b".into(),
            lease("released", NOW - 1_000).into_bytes(),
        );
        assert!(block_on(forget_on(&store, "", "b", NOW)).is_err());
        assert!(exists(&store));
        // Fenced, or live: never.
        for state in ["fenced", "live"] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            put(
                &store,
                "nodes/b".into(),
                lease(state, NOW - 600_000).into_bytes(),
            );
            let why = block_on(forget_on(&store, "", "b", NOW)).unwrap_err();
            assert!(why.contains("never deleted"), "{state}: {why}");
            assert!(exists(&store));
        }
    }

    /// ADR 0043, section 5: `cluster status` is read-only, so beside a
    /// live node outside this `wasmkeep`'s range, or under a floor above
    /// it, it warns as `cluster upgrade status` does, in the text and the
    /// JSON, and goes on. A stopped node outside the range is not one.
    #[test]
    fn a_reader_outside_the_live_nodes_range_warns_and_goes_on() {
        let lease = |id: &str, state: &str, version: &str| {
            format!(
                "wasmkeep-node 3\n{state} beef\n{NOW}\n{id}\nhttp://{id}\n{version}\nacme/shop\n\n\nserving\n\ntake\n"
            )
            .into_bytes()
        };
        let v = |minor: u32| wasmkeep_cluster::app_format::floor::Minor { major: 0, minor };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let put = |key: &str, body: Vec<u8>| {
            block_on(store.put(&StorePath::from(key), body.into())).unwrap();
        };
        put("demo/nodes/a", lease("a", "live", "0.11.0"));
        put("demo/nodes/b", lease("b", "released", "0.7.4"));
        let mut r = report(Arc::clone(&store));
        let before = r.warnings();
        for this in [10, 11, 12] {
            r.wasmkeep = Some(v(this));
            assert_eq!(r.warnings(), before, "0.{this}");
        }
        r.wasmkeep = Some(v(9));
        let warnings = r.warnings();
        let own = "this `wasmkeep` (0.9) runs only beside the version before and the one after, \
                   and acme/shop has live nodes outside that: a (0.11.0); commands that write \
                   are refused";
        assert_eq!(warnings.len(), before.len() + 1, "{warnings:?}");
        assert!(warnings.contains(&own.to_owned()), "{warnings:?}");
        assert!(r.to_text().contains(&format!("warning: {own}")));
        assert!(
            r.to_json()["warnings"]
                .as_array()
                .unwrap()
                .contains(&serde_json::Value::from(own))
        );
        // The same sentence as `cluster upgrade status`.
        assert!(
            crate::upgrade_cmd::status_text("s3://b/demo", &r.upgrades, v(9))
                .contains(&format!("warning: {own}"))
        );
        // A floor above the reader.
        put(
            "demo/formats/acme/shop/floor",
            b"wasmkeep-floor 1\n0.11\n".to_vec(),
        );
        let mut r = report(Arc::clone(&store));
        r.wasmkeep = Some(v(10));
        assert!(
            r.warnings()
                .iter()
                .any(|w| w.contains("this `wasmkeep` (0.10) is older than the cluster")),
            "{:?}",
            r.warnings()
        );
    }

    /// ADR 0043: once the app's upgrade to 0.9 is finalized, `cluster
    /// forget` removes a released lease through its mark (a conditional
    /// write, then the delete), and says nothing more. Before, and while
    /// a live node of the app is older, it deletes as it did before 0.9
    /// and says so, naming what the safe removal waits for.
    #[test]
    fn forget_marks_a_released_lease_only_after_the_finalize() {
        let lease = |id: &str, state: &str, version: &str, at: u64| {
            format!(
                "wasmkeep-node 3\n{state} beef\n{at}\n{id}\nhttp://{id}\n{version}\nacme/shop\n\n\nserving\n\ntake\n"
            )
        };
        let put = |store: &Arc<dyn ObjectStore>, key: &str, body: Vec<u8>| {
            block_on(store.put(&StorePath::from(key), body.into())).unwrap();
        };
        let exists = |store: &Arc<dyn ObjectStore>| {
            block_on(store.head(&StorePath::from("nodes/b"))).is_ok()
        };
        let released = || lease("b", "released", "0.9.0", NOW - 600_000).into_bytes();
        let floor = b"wasmkeep-floor 1\n0.9\n".to_vec();

        // Not finalized (no floor record: 0.8): as before 0.9, and said.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "nodes/b", released());
        let done = block_on(forget_on(&store, "", "b", NOW)).unwrap();
        assert!(
            done.starts_with("Removed b's released node lease. "),
            "{done}"
        );
        assert!(
            done.contains("a delete without a condition")
                && done.contains("upgrade floor is 0.8")
                && done.contains("cluster upgrade finalize"),
            "{done}"
        );
        assert!(!exists(&store));

        // Finalized at 0.9: through the mark, and nothing more to say.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "nodes/b", released());
        put(&store, "formats/acme/shop/floor", floor.clone());
        put(
            &store,
            "nodes/a",
            lease("a", "live", "0.9.0", NOW).into_bytes(),
        );
        assert_eq!(
            block_on(forget_on(&store, "", "b", NOW)).as_deref(),
            Ok("Removed b's released node lease.")
        );
        assert!(!exists(&store));
        // The checks before it are the same: a record that names the
        // node keeps its lease, and a recent release is left alone.
        put(
            &store,
            "nodes/b",
            lease("b", "released", "0.9.0", NOW - 1_000).into_bytes(),
        );
        assert!(block_on(forget_on(&store, "", "b", NOW)).is_err());
        assert!(exists(&store));

        // Finalized, and a live node of the app that is older (it passed
        // its floor read before the finalize): as before 0.9, naming it.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "nodes/b", released());
        put(&store, "formats/acme/shop/floor", floor);
        put(
            &store,
            "nodes/old",
            lease("old", "live", "0.8.1", NOW).into_bytes(),
        );
        let done = block_on(forget_on(&store, "", "b", NOW)).unwrap();
        assert!(
            done.contains("a delete without a condition") && done.contains("old (0.8.1)"),
            "{done}"
        );
        assert!(!exists(&store));
    }

    /// A `cluster forget` that wrote its mark and stopped before its
    /// delete is ended by the next run, whatever the mark's age. Until
    /// then the mark is not a node: `cluster status` shows none for it,
    /// and `cluster fence` does not write over it.
    #[test]
    fn forget_ends_a_removal_an_earlier_run_left_half_way() {
        use wasmkeep_cluster::node_lease::{OperatorFence, encode_forgotten, operator_fence};
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(store.put(&StorePath::from("nodes/b"), encode_forgotten(NOW).into())).unwrap();
        // Just written: its command is about to delete it. Nothing shown.
        let report = block_on(read(Arc::clone(&store), "", "mem", NOW, 10_000)).unwrap();
        assert!(report.nodes.is_empty() && report.upgrades.is_empty());
        assert!(report.unfinished_forgets.is_empty());
        assert!(report.warnings().iter().all(|w| !w.contains("forget")));
        // Still there seconds later: one line, with the command to run.
        let later = NOW + FORGET_UNFINISHED_MS + 1;
        let report = block_on(read(Arc::clone(&store), "", "mem", later, 10_000)).unwrap();
        assert!(report.nodes.is_empty() && report.upgrades.is_empty());
        let lines: Vec<String> = report
            .warnings()
            .into_iter()
            .filter(|w| w.contains("forget"))
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("b: forget did not finish; run `wasmkeep cluster forget b`"),
            "{lines:?}"
        );
        assert!(report.to_text().contains(&format!("warning: {}", lines[0])));
        let bucket = ObjectStoreBucket::new(Arc::clone(&store), "");
        assert_eq!(
            block_on(operator_fence(
                &bucket,
                &Wall,
                "nodes/b",
                wasmkeep_cluster::node_lease::CREATED_FENCE_AHEAD_MS,
                std::future::pending()
            )),
            Ok(OperatorFence::Forgotten)
        );
        let done = block_on(forget_on(&store, "", "b", NOW)).unwrap();
        assert!(done.contains("an earlier `cluster forget`"), "{done}");
        assert!(block_on(store.head(&StorePath::from("nodes/b"))).is_err());
        // Nothing left: nothing to forget.
        let done = block_on(forget_on(&store, "", "b", NOW)).unwrap();
        assert!(done.contains("nothing to forget"), "{done}");
    }

    /// ADR 0038, M6: node ids of one process are grouped by the process
    /// their heartbeats name (`process=<id>`).
    #[test]
    fn node_ids_are_grouped_by_their_process() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            for (id, app, process) in [
                ("shop-1", "acme/shop", " process=edge-1"),
                ("blog-1", "acme/blog", " process=edge-1"),
                ("shop-2", "acme/shop", ""),
            ] {
                let body = format!(
                    "wasmkeep-node 3\nlive beef\n{NOW}\n{id}\nhttp://{id}\n1\n{app}\n\n\nserving\n\ntake multi-app{process}\n"
                );
                put(&store, &format!("demo/nodes/{id}"), &body).await;
            }
        });
        let r = report(store);
        let text = r.to_text();
        assert!(text.contains("PROCESS"), "{text}");
        assert!(
            text.lines()
                .any(|l| l.starts_with("edge-1") && l.contains("blog-1, shop-1")),
            "{text}"
        );
        let j = r.to_json();
        assert_eq!(j["processes"][0]["process"], "edge-1");
        assert_eq!(
            j["processes"][0]["nodes"],
            serde_json::json!(["blog-1", "shop-1"])
        );
        assert_eq!(j["nodes"][0]["process"], "edge-1");
        assert_eq!(j["nodes"][2]["process"], serde_json::Value::Null);
        assert_eq!(r.process_nodes("edge-1"), ["blog-1", "shop-1"]);
    }

    #[test]
    fn a_fenced_or_released_node_lease_is_shown_as_such() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            for (id, state) in [("a", "live"), ("b", "fenced"), ("c", "released")] {
                let body = format!(
                    "wasmkeep-node 3\n{state} beef\n{NOW}\n{id}\nhttp://{id}\n1\nacme/shop\n\n\nserving\n\ntake\n"
                );
                put(&store, &format!("demo/nodes/{id}"), &body).await;
            }
        });
        let r = report(store);
        let text = r.to_text();
        assert!(text.contains("no (fenced)"), "{text}");
        assert!(text.contains("no (released)"), "{text}");
        assert!(text.contains("1 of 3 nodes live"), "{text}");
        let j = r.to_json();
        assert_eq!(j["nodes"][0]["lease"], "live");
        assert_eq!(j["nodes"][1]["lease"], "fenced");
        assert_eq!(j["nodes"][2]["lease"], "released");
    }

    #[test]
    fn a_stale_heartbeat_is_listed_but_not_live() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW - 1_200, false, "acme/shop").await;
            // Exactly at the horizon still counts, as on the nodes.
            beat(&store, "b", NOW - HORIZON, false, "acme/shop").await;
            beat(&store, "c", NOW - HORIZON - 1, false, "acme/shop").await;
            // A node whose clock runs ahead of ours.
            beat(&store, "d", NOW + 500, false, "acme/shop").await;
            // Another cluster's prefix is not read.
            put(
                &store,
                "other/nodes/z",
                "wasmkeep-node 2\nz\nhttp://z\n1\n0\n\n",
            )
            .await;
        });
        let r = report(store);
        let rows: Vec<_> = r
            .nodes
            .iter()
            .map(|n| (n.info.id.as_str(), n.live, n.age_ms))
            .collect();
        assert_eq!(
            rows,
            [
                ("a", true, 1_200),
                ("b", true, HORIZON),
                ("c", false, HORIZON + 1),
                ("d", true, 0)
            ]
        );
        assert!(r.any_live());
        assert!(r.warnings().is_empty(), "{:?}", r.warnings());
        let text = r.to_text();
        assert!(text.contains("3 of 4 nodes live"), "{text}");
        assert!(text.contains("10.0s"), "{text}");
        let j = r.to_json();
        assert_eq!(j["summary"]["live"], 3);
        assert_eq!(j["nodes"][2]["id"], "c");
        assert_eq!(j["nodes"][2]["live"], false);
        assert_eq!(j["nodes"][2]["age_ms"], HORIZON + 1);
    }

    #[test]
    fn a_mixed_plain_and_tls_cluster_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            beat(&store, "b", NOW, false, "acme/shop").await;
            // A stale plain node does not count: it is gone.
            beat(&store, "c", NOW - 60_000, false, "acme/shop").await;
        });
        let r = report(store);
        let w = r.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("b still plain while a use"), "{}", w[0]);
        let j = r.to_json();
        assert_eq!(j["summary"]["mixed_tls"], true);
        assert_eq!(j["summary"]["live_tls"], 1);
        assert_eq!(j["summary"]["live_plain"], 1);
        assert_eq!(j["nodes"][0]["cluster_address"], "https://a:7443");
        assert_eq!(j["nodes"][0]["tls_pins"][0], "ab".repeat(32));
        assert_eq!(j["nodes"][1]["cluster_address"], Value::Null);
        assert_eq!(j["nodes"][1]["tls"], false);
        let text = r.to_text();
        assert!(text.contains("pinned"), "{text}");
        assert!(
            text.contains("warning: mixed plain and TLS nodes"),
            "{text}"
        );

        // Once every live node has a listener, no warning.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            beat(&store, "b", NOW, true, "acme/shop").await;
            beat(&store, "c", NOW - 60_000, false, "acme/shop").await;
        });
        assert!(report(store).warnings().is_empty());
    }

    #[test]
    fn drain_states_are_shown_and_a_stuck_node_is_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            let base = |id: &str| {
                format!(
                    "wasmkeep-node 3\nlive i-{id}\n{NOW}\n{id}\nhttp://{id}:8080\n{}\nacme/shop\n",
                    wasmkeep_cluster::VERSION.to_owned()
                )
            };
            put(&store, "demo/nodes/b", &format!("{}\n\nstuck\n", base("b"))).await;
            put(
                &store,
                "demo/nodes/c",
                &format!("{}\n\ndrained\n", base("c")),
            )
            .await;
            // A node from before ADR 0014: no line 9.
            put(&store, "demo/nodes/d", &base("d")).await;
        });
        let r = report(store);
        let j = r.to_json();
        let states: Vec<&Value> = (0..4).map(|i| &j["nodes"][i]["state"]).collect();
        assert_eq!(
            states,
            [
                &json!("serving"),
                &json!("stuck"),
                &json!("drained"),
                &Value::Null
            ]
        );
        // These hand-written heartbeats announce no capability words, so
        // the recorded-head warning comes first (ADR 0035); the drain one
        // is what this test checks.
        let w: Vec<String> = r
            .warnings()
            .into_iter()
            .filter(|w| !w.contains("recorded-head"))
            .collect();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].starts_with("draining but stuck: b "), "{}", w[0]);
        let text = r.to_text();
        assert!(text.contains("STATE"), "{text}");
        assert!(text.contains("stuck"), "{text}");
    }

    /// Payload line 10: a live node's load per node, summed per app. A node
    /// not live (stale, fenced or released, which keep their last payload)
    /// or of a version without the line shows `-`, never a stale count.
    #[test]
    fn load_is_shown_for_live_nodes_and_summed_per_app() {
        let load = |actors, sockets, events_per_min, front_per_min| {
            Some(membership::Load {
                actors,
                sockets,
                events_per_min,
                front_per_min,
                ..membership::Load::default()
            })
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat_with(
                &store,
                "a",
                NOW,
                false,
                "acme/shop",
                // ADR 0041: with its rooms, so its fill shows.
                load(3, 1, Some(120), Some(45)).map(|l| membership::Load {
                    actor_room: Some(7),
                    socket_room: Some(99),
                    ..l
                }),
            )
            .await;
            beat_with(&store, "b", NOW, false, "acme/shop", load(2, 0, None, None)).await;
            // c: a version without line 10.
            beat(&store, "c", NOW, false, "acme/shop").await;
            // d: stale, its last load kept.
            beat_with(
                &store,
                "d",
                NOW - 60_000,
                false,
                "acme/shop",
                load(40, 4, Some(9), Some(9)),
            )
            .await;
            // e: fenced, its last load kept.
            beat_with(
                &store,
                "e",
                NOW,
                false,
                "beta/new",
                load(50, 5, Some(9), Some(9)),
            )
            .await;
            let key = StorePath::from("demo/nodes/e");
            let live = store.get(&key).await.unwrap().bytes().await.unwrap();
            let fenced =
                String::from_utf8(live.to_vec())
                    .unwrap()
                    .replacen("\nlive ", "\nfenced ", 1);
            store.put(&key, PutPayload::from(fenced)).await.unwrap();
            put(
                &store,
                "demo/apps/acme/shop/current",
                "0123456789abcdef 4\n",
            )
            .await;
            put(&store, "demo/apps/beta/new/current", "fedcba9876543210 1\n").await;
            // acme/shop's version has a front, beta/new's has not.
            put(
                &store,
                "demo/apps/acme/shop/0123456789abcdef/manifest",
                "wasmkeep-manifest 3\nfront main 44\n",
            )
            .await;
            put(
                &store,
                "demo/apps/beta/new/fedcba9876543210/manifest",
                "wasmkeep-manifest 1\nclass chat 11\n",
            )
            .await;
        });
        let r = report(store);
        let j = r.to_json();
        let cells = |i: usize| {
            let n = &j["nodes"][i];
            (
                n["actors"].clone(),
                n["sockets"].clone(),
                n["events_per_min"].clone(),
                n["front_per_min"].clone(),
            )
        };
        assert_eq!(cells(0), (json!(3), json!(1), json!(120), json!(45)));
        assert_eq!(cells(1), (json!(2), json!(0), Value::Null, Value::Null));
        // The rooms and the resident fill (ADR 0041), null without rooms.
        assert_eq!(j["nodes"][0]["actor_room"], 7);
        assert_eq!(j["nodes"][0]["socket_room"], 99);
        assert_eq!(j["nodes"][0]["fill"], 300);
        assert_eq!(j["nodes"][1]["fill"], Value::Null);
        for i in 2..5 {
            assert_eq!(
                cells(i),
                (Value::Null, Value::Null, Value::Null, Value::Null),
                "{i}"
            );
        }
        // Each version's manifest format, and each node's capability words.
        assert_eq!(j["apps"][0]["manifest_format"], 3);
        assert_eq!(j["apps"][1]["manifest_format"], 1);
        let words = j["nodes"][0]["words"].as_array().unwrap();
        for w in [
            "flush",
            "rate_limits",
            "restore",
            "actor-restore",
            "recorded-head",
        ] {
            assert!(words.iter().any(|x| x == w), "{w} in {words:?}");
        }
        let text = r.to_text();
        assert!(text.contains("WORDS") && text.contains("FORMAT"), "{text}");
        assert!(
            text.contains("flush rate_limits restore actor-restore recorded-head"),
            "{text}"
        );
        assert_eq!(j["apps"][0]["actors"], 5);
        assert_eq!(j["apps"][1]["actors"], Value::Null);
        assert_eq!(j["apps"][0]["front"], true);
        assert_eq!(j["apps"][1]["front"], false);

        let text = r.to_text();
        let row = |id: &str| {
            text.lines()
                .find(|l| l.starts_with(&format!("{id} ")))
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>()
        };
        let header: Vec<&str> = text.lines().nth(2).unwrap().split_whitespace().collect();
        assert_eq!(
            header[3..8],
            ["STATE", "ACTORS", "SOCKETS", "EV/MIN", "FRONT/MIN"],
            "{text}"
        );
        assert_eq!(row("a")[4..8], ["3", "1", "120", "45"], "{text}");
        assert_eq!(header[8], "FILL", "{text}");
        assert_eq!(row("a")[8], "30.0%", "{text}");
        assert_eq!(row("b")[8], "-", "{text}");
        assert_eq!(row("b")[4..8], ["2", "0", "-", "-"], "{text}");
        for id in ["c", "d"] {
            assert_eq!(row(id)[4..8], ["-", "-", "-", "-"], "{text}");
        }
        // e's LIVE cell is two words: `no (fenced)`.
        assert_eq!(row("e")[5..9], ["-", "-", "-", "-"], "{text}");
        // STATE too: a node not live serves nothing, whatever its last
        // heartbeat said; LIVE says why.
        assert_eq!(row("a")[3], "serving", "{text}");
        assert_eq!(row("d")[3], "-", "{text}");
        assert_eq!(row("e")[4], "-", "{text}");
        assert_eq!(j["nodes"][0]["state"], json!("serving"));
        for i in [3, 4] {
            assert_eq!(j["nodes"][i]["state"], Value::Null, "{i}");
        }
        assert!(
            text.contains("APP        DEPLOYED VERSION  SEQ  ACTORS  FRONT  FORMAT"),
            "{text}"
        );
        assert_eq!(
            row("acme/shop"),
            ["acme/shop", "0123456789abcdef", "4", "5", "yes", "3", "-"]
        );
        assert_eq!(
            row("beta/new"),
            ["beta/new", "fedcba9876543210", "1", "-", "no", "1", "-"]
        );
    }

    /// ADR 0043: `cluster status` shows each app's upgrade floor and the
    /// state of its upgrade in one line, `finalize pending` among them,
    /// and warns about a live node below the floor.
    #[test]
    fn the_upgrade_floor_and_finalize_pending_are_shown() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node = async |id: &str, version: &str| {
            put(
                &store,
                &format!("demo/nodes/{id}"),
                &format!(
                    "wasmkeep-node 3\nlive i-{id}\n{NOW}\n{id}\nhttp://{id}:8080\n{version}\nacme/shop\n\n\nserving\n\ntake\n"
                ),
            )
            .await;
        };
        block_on(async {
            node("a", "0.9.0").await;
            node("b", "0.9.1").await;
        });
        let r = report(Arc::clone(&store));
        let text = r.to_text();
        assert!(
            text.contains(
                "\nupgrade of acme/shop: floor none (0.8), finalize pending: every live node \
                 runs 0.9 and the floor is 0.8: run `wasmkeep cluster upgrade finalize`"
            ),
            "{text}"
        );
        let j = r.to_json();
        assert_eq!(j["summary"]["finalize_pending"], json!(["acme/shop"]));
        assert_eq!(j["upgrade"][0]["app"], "acme/shop");
        assert_eq!(j["upgrade"][0]["state"], "finalize pending");
        assert_eq!(j["upgrade"][0]["floor"], Value::Null);
        assert_eq!(j["upgrade"][0]["floor_in_force"], "0.8");
        assert!(r.warnings().iter().all(|w| !w.contains("upgrade of")));

        // Finalized: one version, nothing pending.
        block_on(put(
            &store,
            "demo/formats/acme/shop/floor",
            "wasmkeep-floor 1\n0.9\n",
        ));
        let r = report(Arc::clone(&store));
        assert!(
            r.to_text().contains(
                "\nupgrade of acme/shop: floor 0.9, one version: every live node runs 0.9\n"
            ),
            "{}",
            r.to_text()
        );
        assert_eq!(r.to_json()["summary"]["finalize_pending"], json!([]));
        assert_eq!(r.to_json()["upgrade"][0]["floor"], "0.9");

        // A live node below the floor: a warning, naming it.
        block_on(node("late", "0.8.1"));
        let r = report(Arc::clone(&store));
        let w = r.warnings();
        assert!(
            w.iter().any(|w| w.starts_with(
                "upgrade of acme/shop: floor 0.9, node below the floor: late (0.8.1) live below \
                 the floor 0.9"
            )),
            "{w:?}"
        );
        assert_eq!(r.to_json()["upgrade"][0]["blocks"], json!(["late"]));
        // No app, no line.
        let empty = report(Arc::new(InMemory::new()));
        assert!(!empty.to_text().contains("upgrade of"));
        assert_eq!(empty.to_json()["upgrade"], json!([]));
    }

    #[test]
    fn mixed_versions_are_flagged() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, false, "acme/shop").await;
            // A node of another version (format 3, no app).
            put(
                &store,
                "demo/nodes/b",
                &format!("wasmkeep-node 3\nlive i-b\n{NOW}\nb\nhttp://b:8080\n0.0.1\n\n"),
            )
            .await;
        });
        let r = report(store);
        let w = r.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(
            w[0].contains("upgrade every node before you enable or deploy"),
            "{}",
            w[0]
        );
        assert!(w[0].contains("0.0.1 on b"), "{}", w[0]);
        assert_eq!(r.to_json()["summary"]["mixed_versions"], true);
        assert_eq!(r.to_json()["nodes"][1]["app"], "");
    }

    #[test]
    fn deployed_apps_are_read_from_current() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            let load = membership::Load {
                actors: 7,
                sockets: 0,
                events_per_min: None,
                front_per_min: None,
                ..membership::Load::default()
            };
            beat_with(&store, "a", NOW, false, "acme/shop", Some(load)).await;
            put(
                &store,
                "demo/apps/acme/shop/current",
                "0123456789abcdef 4\n",
            )
            .await;
            put(&store, "demo/apps/acme/shop/0123456789abcdef/manifest", "m").await;
            put(
                &store,
                "demo/apps/acme/shop/0123456789abcdef/chat.wasm",
                "w",
            )
            .await;
            put(&store, "demo/apps/acme/admin/current", "garbage").await;
            put(&store, "demo/apps/beta/new/fedcba9876543210/manifest", "m").await;
            // Another prefix's app is not listed.
            put(&store, "other/apps/x/y/current", "v 1").await;
        });
        let r = report(store);
        let j = r.to_json();
        assert_eq!(
            j["apps"],
            json!([
                {"app": "acme/admin", "version": null, "seq": null, "actors": null, "front": null, "manifest_format": null, "error": "unreadable current", "rebalance": null, "rebalance_paused": null},
                // An unreadable manifest: not known, and no error.
                {"app": "acme/shop", "version": "0123456789abcdef", "seq": 4, "actors": 7, "front": null, "manifest_format": null, "rebalance": null, "rebalance_paused": null},
                {"app": "beta/new", "version": null, "seq": null, "actors": null, "front": null, "manifest_format": null, "rebalance": null, "rebalance_paused": null},
            ])
        );
        let text = r.to_text();
        assert!(
            text.contains("acme/shop   0123456789abcdef      4    7       -      -       -\n"),
            "{text}"
        );
    }

    #[test]
    fn no_live_node_is_an_error() {
        let empty = report(Arc::new(InMemory::new()));
        assert!(!empty.any_live());
        assert!(empty.warnings()[0].contains("no node has a heartbeat"));
        assert!(empty.to_text().contains("No nodes."));

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(beat(&store, "a", NOW - HORIZON - 1, false, "acme/shop"));
        let r = report(store);
        assert!(!r.any_live());
        assert!(r.warnings()[0].contains("no node is live"));
        assert_eq!(r.to_json()["summary"]["live"], 0);
    }

    #[test]
    fn a_cluster_address_without_a_valid_pin_is_not_tls() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        block_on(async {
            beat(&store, "a", NOW, true, "acme/shop").await;
            put(
                &store,
                "demo/nodes/b",
                &format!("wasmkeep-node 3\nlive i-b\n{NOW}\nb\nhttp://b:8080\n0.0.0\nacme/shop\nhttps://b:7443\nnot-a-pin\n"),
            )
            .await;
        });
        let r = report(store);
        let j = r.to_json();
        assert_eq!(j["nodes"][1]["cluster_address"], "https://b:7443");
        assert_eq!(j["nodes"][1]["tls"], false);
        assert_eq!(j["summary"]["mixed_tls"], true);
        assert!(r.to_text().contains("no pin"), "{}", r.to_text());
    }

    #[test]
    fn ages_read_at_a_glance() {
        assert_eq!(age(850), "850ms");
        assert_eq!(age(4_200), "4.2s");
        assert_eq!(age(12 * 60_000 + 5), "12m");
        assert_eq!(age(3 * 3_600_000), "3h");
        assert_eq!(age(5 * 86_400_000), "5d");
    }
}

#[cfg(test)]
#[tokio::test]
async fn oversized_current_is_reported() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from("apps/t/a/current"),
            bytes::Bytes::from(vec![b'x'; (4 << 20) + 1]).into(),
        )
        .await
        .unwrap();
    let apps = read_apps(&store, "", "memory", None).await.unwrap();
    assert!(apps[0].current.as_ref().unwrap_err().contains("cap"));
}

/// Bound headers and streamed bodies, including stores that understate length.
pub(crate) async fn metadata(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
) -> anyhow::Result<bytes::Bytes> {
    Ok(wasmkeep_server::history::get(store, key, None, 4 << 20).await?)
}
pub(crate) async fn current(
    store: &Arc<dyn ObjectStore>,
    app: &StorePath,
) -> anyhow::Result<Option<Current>> {
    match wasmkeep_server::history::get(store, &app.clone().join("current"), None, 4 << 20).await {
        Ok(body) => Current::parse(&body)
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("unreadable current")),
        Err(wasmkeep_server::history::Error::Missing(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
pub(crate) async fn manifest(
    store: &Arc<dyn ObjectStore>,
    app: &StorePath,
    version: &str,
) -> anyhow::Result<wasmkeep_server::apps::Manifest> {
    let body = metadata(store, &app.clone().join(version).join("manifest")).await?;
    wasmkeep_server::apps::Manifest::parse(std::str::from_utf8(&body)?)
        .ok_or_else(|| anyhow::anyhow!("unreadable manifest"))
}

#[cfg(test)]
mod hostile_metadata {
    use super::*;
    #[derive(Debug)]
    struct HugeMetadata {
        inner: Arc<dyn ObjectStore>,
    }
    impl std::fmt::Display for HugeMetadata {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("huge metadata")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for HugeMetadata {
        async fn put_opts(
            &self,
            _: &StorePath,
            _: object_store::PutPayload,
            _: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            panic!("source PUT")
        }
        async fn put_multipart_opts(
            &self,
            _: &StorePath,
            _: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            panic!("source multipart")
        }
        async fn get_opts(
            &self,
            path: &StorePath,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let mut result = self.inner.get_opts(path, opts).await?;
            result.meta.size = 8 << 30;
            result.payload =
                object_store::GetResultPayload::Stream(Box::pin(futures_util::stream::poll_fn(
                    |_| panic!("oversized metadata body must never be polled"),
                )));
            Ok(result)
        }

        fn list(
            &self,
            prefix: Option<&StorePath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        fn delete_stream(
            &self,
            _: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
            panic!("source DELETE")
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&StorePath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            _: &StorePath,
            _: &StorePath,
            _: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            panic!("source COPY")
        }
    }

    #[tokio::test]
    async fn huge_current_headers_are_refused_without_polling_body() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        inner
            .put(
                &StorePath::from("apps/t/a/current"),
                bytes::Bytes::new().into(),
            )
            .await
            .unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(HugeMetadata { inner });
        let apps = read_apps(&store, "", "memory", None).await.unwrap();
        assert!(apps[0].current.as_ref().unwrap_err().contains("byte cap"));
    }
}

#[cfg(test)]
#[tokio::test]
async fn large_app_listing_is_unbounded_except_for_inspector() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for i in 0..10_001 {
        store
            .put(
                &StorePath::from(format!("apps/t/a/v/{i}")),
                bytes::Bytes::new().into(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        read(store.clone(), "", "memory", 0, 0)
            .await
            .unwrap()
            .apps
            .len(),
        1
    );
    let error = match read_bounded(store, "", "memory", 0, 0, 10_000).await {
        Err(error) => error,
        Ok(_) => panic!("inspector bound"),
    };
    assert!(
        error
            .to_string()
            .contains("listing exceeds 10000 object limit")
    );
}

#[cfg(test)]
mod discovery_requests {
    use super::*;
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: std::sync::atomic::AtomicUsize,
        lists: std::sync::atomic::AtomicUsize,
        entries: std::sync::atomic::AtomicUsize,
        reverse: bool,
        completed: std::sync::Mutex<Vec<String>>,
        stalls: std::sync::atomic::AtomicUsize,
    }
    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("counting store")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            _: &StorePath,
            _: object_store::PutPayload,
            _: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            panic!("source PUT")
        }
        async fn put_multipart_opts(
            &self,
            _: &StorePath,
            _: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            panic!("source multipart")
        }
        async fn get_opts(
            &self,
            path: &StorePath,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.reverse {
                let rank = path
                    .as_ref()
                    .split('/')
                    .find_map(|part| match part {
                        "a" => Some(3),
                        "b" => Some(2),
                        "c" => Some(1),
                        _ => None,
                    })
                    .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(rank * 10)).await;
                self.completed.lock().unwrap().push(path.to_string());
            }
            self.inner.get_opts(path, opts).await
        }

        fn list(
            &self,
            prefix: Option<&StorePath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            assert!(
                self.reverse,
                "app discovery must not recursively list {prefix:?}"
            );
            self.inner.list(prefix)
        }

        fn delete_stream(
            &self,
            _: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
            panic!("source DELETE")
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&StorePath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.lists
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self
                .stalls
                .try_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |n| n.checked_sub(1),
                )
                .is_ok()
            {
                std::future::pending::<()>().await;
            }
            let listing = self.inner.list_with_delimiter(prefix).await?;
            self.entries.fetch_add(
                listing.common_prefixes.len() + listing.objects.len(),
                std::sync::atomic::Ordering::Relaxed,
            );
            Ok(listing)
        }
        async fn copy_opts(
            &self,
            _: &StorePath,
            _: &StorePath,
            _: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            panic!("source COPY")
        }
    }

    fn wrapped(inner: Arc<dyn ObjectStore>, reverse: bool, stalls: usize) -> Arc<CountingStore> {
        Arc::new(CountingStore {
            inner,
            gets: 0.into(),
            lists: 0.into(),
            entries: 0.into(),
            reverse,
            completed: Default::default(),
            stalls: stalls.into(),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn reverse_reads_keep_all_status_and_directory_outputs_sorted() {
        use wasmkeep_cluster::membership::Directory;
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        for id in ["a", "b", "c"] {
            for (key, body) in [
                (
                    format!("nodes/{id}"),
                    format!(
                        "wasmkeep-node 3\nlive i-{id}\n100\n{id}\nhttp://{id}:8080\n0.9.0\nt/{id}\n"
                    ),
                ),
                (
                    format!("apps/t/{id}/current"),
                    "0123456789abcdef 1\n".into(),
                ),
                (
                    format!("apps/t/{id}/0123456789abcdef/manifest"),
                    "wasmkeep-manifest 1\nclass chat 11\n".into(),
                ),
                (
                    format!("formats/t/{id}/floor"),
                    "wasmkeep-floor 1\n0.9\n".into(),
                ),
            ] {
                inner
                    .put(&StorePath::from(key), bytes::Bytes::from(body).into())
                    .await
                    .unwrap();
            }
        }
        let wrapped = wrapped(inner, true, 0);
        let store: Arc<dyn ObjectStore> = wrapped.clone();
        let report = read(store.clone(), "", "memory", 100, 100).await.unwrap();
        assert_eq!(
            report
                .nodes
                .iter()
                .map(|n| n.info.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(
            report
                .apps
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>(),
            ["t/a", "t/b", "t/c"]
        );
        assert_eq!(
            report
                .upgrades
                .iter()
                .map(|a| a.app.as_str())
                .collect::<Vec<_>>(),
            ["t/a", "t/b", "t/c"]
        );
        for prefix in ["nodes/", "apps/", "formats/"] {
            let completed = wrapped.completed.lock().unwrap();
            let paths: Vec<_> = completed
                .iter()
                .filter(|p| p.starts_with(prefix) && !p.ends_with("manifest"))
                .map(String::as_str)
                .collect();
            let expected = match prefix {
                "nodes/" => vec!["nodes/c", "nodes/b", "nodes/a"],
                "apps/" => vec!["apps/t/c/current", "apps/t/b/current", "apps/t/a/current"],
                _ => vec![
                    "formats/t/c/floor",
                    "formats/t/b/floor",
                    "formats/t/a/floor",
                ],
            };
            assert_eq!(paths, expected);
        }
        let bucket = ObjectStoreBucket::new(store, "");
        let keys: Vec<_> = ["nodes/a", "nodes/b", "nodes/c"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        for bodies in [
            bucket.read(&keys).await.unwrap(),
            bucket.list("nodes").await.unwrap(),
        ] {
            assert_eq!(
                bodies
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>(),
                keys
            );
            for (key, body) in bodies {
                assert_eq!(
                    NodeInfo::decode(&body).unwrap().id,
                    key.strip_prefix("nodes/").unwrap()
                );
            }
        }
    }

    #[tokio::test]
    async fn current_manifest_and_direct_tenant_object_match_bounded_discovery() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        for (key, body) in [
            ("apps/t/a/current", "0123456789abcdef 7\n"),
            (
                "apps/t/a/0123456789abcdef/manifest",
                "wasmkeep-manifest 1\nclass chat 11\n",
            ),
            ("apps/t/x", "file"),
        ] {
            store
                .put(
                    &StorePath::from(key),
                    bytes::Bytes::from_static(body.as_bytes()).into(),
                )
                .await
                .unwrap();
        }
        for max in [None, Some(10)] {
            let apps = read_apps(&store, "", "memory", max).await.unwrap();
            assert_eq!(
                apps.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
                ["t/a", "t/x"]
            );
            assert_eq!(apps[0].current.as_ref().unwrap().as_ref().unwrap().seq, 7);
            assert_eq!(apps[0].manifest_format, Some(1));
            assert_eq!(apps[0].front, Some(false));
            assert!(apps[1].current.as_ref().unwrap().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delimiter_stalls_are_retried_and_eventually_fail() {
        use std::sync::atomic::Ordering;
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let retry = wrapped(inner.clone(), false, 1);
        let store: Arc<dyn ObjectStore> = retry.clone();
        assert!(
            read_apps(&store, "", "memory", None)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(retry.lists.load(Ordering::Relaxed), 2);
        let stalled = wrapped(inner, false, usize::MAX);
        let store: Arc<dyn ObjectStore> = stalled.clone();
        let error = read_apps(&store, "", "memory", None).await.unwrap_err();
        assert!(error.to_string().contains("listing kept stalling"));
        assert_eq!(stalled.lists.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn app_discovery_requests_do_not_grow_with_deployment_history() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for prefix in ["", "demo/nested"] {
            let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let root = StorePath::from(prefix).join("apps");
            for name in ["z/shop", "a/shop", "a/blog"] {
                inner
                    .put(
                        &StorePath::from(format!("{root}/{name}/v0/manifest")),
                        bytes::Bytes::new().into(),
                    )
                    .await
                    .unwrap();
            }
            for history in [0, 2_000] {
                for i in 0..history {
                    inner
                        .put(
                            &StorePath::from(format!("{root}/a/shop/v{i}/component.wasm")),
                            bytes::Bytes::new().into(),
                        )
                        .await
                        .unwrap();
                }
                let counting = Arc::new(CountingStore {
                    inner: Arc::clone(&inner),
                    gets: AtomicUsize::new(0),
                    lists: AtomicUsize::new(0),
                    entries: AtomicUsize::new(0),
                    reverse: false,
                    completed: Default::default(),
                    stalls: AtomicUsize::new(0),
                });
                let store: Arc<dyn ObjectStore> = counting.clone();
                let apps = read_apps(&store, prefix, "memory", None).await.unwrap();
                assert_eq!(
                    apps.iter().map(|app| app.name.as_str()).collect::<Vec<_>>(),
                    ["a/blog", "a/shop", "z/shop"]
                );
                assert_eq!(counting.lists.load(Ordering::Relaxed), 3);
                assert_eq!(counting.entries.load(Ordering::Relaxed), 5);
                assert_eq!(counting.gets.load(Ordering::Relaxed), 3);
            }
        }
    }
}
