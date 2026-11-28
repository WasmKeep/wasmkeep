//! `wasmkeep cluster`: status, drain, undrain, fence and forget (`upgrade`
//! is in `upgrade_cmd.rs`, `verify` in the library's `verify.rs`).

use super::*;

#[derive(Debug, Args)]
pub(super) struct DrainArgs {
    /// The node's id.
    #[arg(required_unless_present = "process", conflicts_with = "process")]
    node: Option<String>,

    /// Every node id this process runs (ADR 0038: the `process=` its
    /// heartbeats name), instead of one node. For a process that names
    /// none (no `[process] id`), `ID` is a node id: each such node is
    /// drained on its own. `undrain --process` needs the nodes live; a
    /// stopped node is undrained by its id.
    #[arg(long, value_name = "ID")]
    process: Option<String>,

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
pub(super) struct NodeArgs {
    /// The node's id.
    node: String,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,
}

#[derive(Debug, Subcommand)]
pub(super) enum ClusterCommand {
    /// Show the cluster under a bucket prefix, read from the bucket alone
    /// (no node needs to answer): each node's heartbeat (version, app,
    /// addresses, TLS pin, age, live or not), each app's deployed
    /// version, and each app's upgrade floor with the state of its
    /// upgrade (`finalize pending` once every live node runs the new
    /// version). A heartbeat is live for 10 s, the nodes' own rule with the
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
    /// leases, or a released node lease no ownership record names, once it
    /// is older than 25 s (its horizon plus a lease's ttl and grace).
    /// Refuses a recent one (that node may still run), a released lease a
    /// record still names (it is what frees that record), and a live or
    /// fenced node lease (never deleted: fence a live one instead). Once
    /// the app's upgrade to 0.9 is finalized, the command never deletes a
    /// lease a node starting under that id wrote meanwhile; before, it
    /// may, and says so (that node stops and is started again).
    Forget(NodeArgs),
    /// Follow and end an upgrade of WasmKeep itself, one node at a time:
    /// `status` shows each node's version, each app's upgrade floor and
    /// what blocks a finalize; `finalize` ends the upgrade.
    #[command(subcommand)]
    Upgrade(crate::upgrade_cmd::UpgradeCommand),
    /// Remove the per-actor lease markers an app still has once its
    /// upgrade to 0.10 is finalized (ADR 0045): `clean`.
    #[command(subcommand)]
    Markers(wasmkeep_cli::markers::MarkersCommand),
    /// Check that a bucket does what WasmKeep needs from it: conditional
    /// creates and replaces, reads and listings that follow writes and
    /// deletes, large objects, last-modified times and the clock. Runs
    /// through the nodes' own bucket code, writing only under a scratch
    /// prefix it requires to be empty and deletes afterwards. One line per
    /// check, then a verdict. Exits with 0 when every check observed the
    /// required behaviour (not a proof of safety), 1 when one did not, 2
    /// when it could not run or finish.
    Diagnose(diagnose::DiagnoseArgs),
    /// Check that a cluster's bucket is whole, reading it and never
    /// writing (it is not `backup verify`, which checks a backup's own
    /// contents, nor `cluster diagnose`, which checks what the bucket's API
    /// does): every actor can be restored from what the bucket holds, and
    /// the records around the actors read. Per app: `current` and its
    /// manifest parse and each component, front and configuration has the
    /// digest its version names; the format setting and the upgrade floor
    /// decode. Per actor: a restore plan exists, from a snapshot through
    /// every segment to the head with no gap; its ownership record
    /// decodes; in an app that records heads (ADR 0035), the recorded head
    /// is reached by that chain, by the rule a restore uses. Also: each
    /// alarm index entry names an actor the bucket knows, the count of
    /// stored files and the sweep records decode, node leases decode, and
    /// the per-actor lease markers left from before node leases are
    /// counted (never deleted). Safe on a running cluster: what looks
    /// wrong is read again before it is reported. Prints what it read
    /// (objects, requests, time; a listing counts one request, and one
    /// more per 1,000 keys) and each problem with its key. Exits with 0
    /// when the bucket is whole, 1 when a problem was found, 2 when there
    /// is no verdict: no problem, but something could not be read, listed
    /// or checked (it is named, and what was found until then is still
    /// printed).
    Verify(wasmkeep_cli::verify::VerifyArgs),
}

#[derive(Debug, Args)]
pub(super) struct StatusArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables; read access is enough).
    #[arg(long)]
    bucket: String,

    /// Print JSON instead of tables, for scripts.
    #[arg(long)]
    json: bool,
}

/// The live node ids among `nodes` whose heartbeats name `process`, sorted.
/// A process that names none (no `process=`: `wasmkeep serve` without a
/// node file, or a node file without `[process] id`) is found by each of
/// its node ids on its own: when no heartbeat, live or stale, names
/// `process`, the live node with that id and no process.
pub(crate) fn process_node_ids(
    nodes: &[wasmkeep_cluster::membership::NodeInfo],
    process: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> Vec<String> {
    let live = |n: &&wasmkeep_cluster::membership::NodeInfo| {
        wasmkeep_cluster::membership::is_live(n, now_ms, horizon_ms)
    };
    let mut ids: Vec<String> = nodes
        .iter()
        .filter(live)
        .filter(|n| n.process.as_deref() == Some(process))
        .map(|n| n.id.clone())
        .collect();
    // Only when no heartbeat at all, stale ones included, names the
    // process: a process that is down must not stand for a node of the
    // same id.
    if !nodes.iter().any(|n| n.process.as_deref() == Some(process)) {
        ids = nodes
            .iter()
            .filter(live)
            .filter(|n| n.process.is_none() && n.id == process)
            .map(|n| n.id.clone())
            .collect();
    }
    ids.sort();
    ids.dedup();
    ids
}

/// Writes or deletes `control/drain/<node>` (ADR 0014). Without `--force`,
/// a drain needs a live node with the id (a request for an id no node uses
/// stays in `control/` until an undrain with the same id), and an undrain a
/// live node or a request to withdraw.
pub(super) fn cluster_drain(args: &DrainArgs, drain: bool) -> anyhow::Result<()> {
    // A bad id is refused before the bucket is touched.
    if let Some(node) = &args.node {
        wasmkeep_cluster::control::drain_key(node)?;
    }
    let (store, prefix) = open_bucket(&args.bucket)?;
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let ids = match (&args.node, &args.process) {
        (Some(node), _) => vec![node.clone()],
        (None, Some(process)) => {
            // ADR 0038: every node id the process runs, as its live
            // heartbeats name it.
            let horizon_ms = wasmkeep_server::heartbeat_horizon_ms(
                wasmkeep_server::DEFAULT_HEARTBEAT,
                &wasmkeep_server::LeaseConfig::default_safe(),
            );
            let all = rt
                .block_on(wasmkeep_cluster::membership::all_heartbeats(&keys))
                .map_err(|e| anyhow::anyhow!("listing the heartbeats: {e}"))?;
            let ids = process_node_ids(&all, process, status::now_ms(), horizon_ms);
            anyhow::ensure!(
                !ids.is_empty(),
                "no live node names the process {process}, nor is a live node that names no \
                 process called {process} (see the PROCESS table of `wasmkeep cluster \
                 status`; a stopped node is undrained by its node id)"
            );
            ids
        }
        (None, None) => anyhow::bail!("give a node id, or --process ID"),
    };
    for id in &ids {
        drain_node(&rt, &keys, id, drain, args.force)?;
    }
    announce_intent(&rt, &keys);
    for id in &ids {
        if drain {
            println!("{}", drain_asked(id));
        } else {
            println!("Withdrew the drain request for {id}.");
        }
    }
    Ok(())
}

/// [`cluster_drain`] for node `id`: the request written or withdrawn.
pub(super) fn drain_node(
    rt: &tokio::runtime::Runtime,
    keys: &wasmkeep_cluster::object_store::ObjectStoreBucket,
    id: &str,
    drain: bool,
    force: bool,
) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::{self, Keys as _};
    let key = control::drain_key(id)?;
    if !force && rt.block_on(live_node(keys, id))?.is_none() {
        if drain {
            anyhow::bail!(
                "no live node has the id {id} (see `wasmkeep cluster status`): a drain request \
                 for an id no node uses stays in the bucket until an undrain with the same id. \
                 Pass --force to write it anyway"
            );
        }
        let requested = rt
            .block_on(control::read(keys, None, status::now_ms()))
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
    } else {
        rt.block_on(keys.delete(&key))?;
    }
    Ok(())
}

/// What `cluster drain` prints once the request is written: the states the
/// node goes through, the shortest included.
pub(super) fn drain_asked(node: &str) -> String {
    format!(
        "Asked {node} to drain; it reports `draining`, then `drained` once its actors are \
         handed over (a node with few actors may go straight to `drained`; see `wasmkeep \
         cluster status`)."
    )
}

/// `wasmkeep cluster fence` (ADR 0020, section 2).
pub(super) fn cluster_fence(args: &NodeArgs) -> anyhow::Result<()> {
    use wasmkeep_cluster::node_lease::{
        CREATED_FENCE_AHEAD_MS, OPERATOR_FENCE_BOUND_MS, OperatorFence, operator_fence,
    };
    wasmkeep_cluster::membership::validate_node_id(&args.node)
        .map_err(|e| anyhow::anyhow!("invalid node id {:?}: {e}", args.node))?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let lease = wasmkeep_server::LeaseConfig::default_safe();
    let key = format!("nodes/{}", args.node);
    // Given up after its bound, so that a create still in flight lands
    // before the date it carries (ADR 0020, amendment of 2026-11-28).
    let mut outcome = rt
        .block_on(async {
            let give_up =
                tokio::time::sleep(std::time::Duration::from_millis(OPERATOR_FENCE_BOUND_MS));
            operator_fence(
                &bucket,
                &status::Wall,
                &key,
                CREATED_FENCE_AHEAD_MS,
                give_up,
            )
            .await
        })
        .map_err(|e| {
            anyhow::anyhow!(
                "fencing {}: {e:?} (a fence may have been written; run this command again)",
                args.node
            )
        })?;
    if let Some(warning) = rt.block_on(date_by_bucket(&bucket, &key, &lease, &mut outcome))? {
        eprintln!("warning: {warning}");
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
        OperatorFence::Forgotten => anyhow::bail!(
            "{}'s node lease is being removed by `wasmkeep cluster forget`, which did not finish: \
             nothing is held under that id (the lease it removes was released). Run `wasmkeep \
             cluster forget {}` again; to fence the id after that, run this command again",
            args.node,
            args.node
        ),
        OperatorFence::OldFormat => anyhow::bail!(
            "{}'s heartbeat is in the format from before node leases: that node would not read a fence. Stop it; once its heartbeat is stale, `wasmkeep cluster forget {}` removes it",
            args.node,
            args.node
        ),
    }
    Ok(())
}

/// Dates a fence an operator created (no instance) by the bucket's time,
/// whichever call created it: one this run created, and one this run found
/// already there (`NotLive`), as after a create whose reply was lost and
/// whose retry found its own fence, or a create that landed late. Such a
/// fence carries the clock of whoever sent the create, read before the
/// create landed, which the free rule trusts within the allowed skew: it
/// is moved to the bucket's time for the object's last write (rounded up
/// to its second, plus the skew) if that is later, and `outcome` follows
/// (ADR 0020, amendment on lease reads). Returns a warning about this
/// machine's clock, if any.
async fn date_by_bucket(
    bucket: &wasmkeep_cluster::object_store::ObjectStoreBucket,
    key: &str,
    lease: &wasmkeep_server::LeaseConfig,
    outcome: &mut wasmkeep_cluster::node_lease::OperatorFence,
) -> anyhow::Result<Option<String>> {
    use wasmkeep_cluster::lease::Bucket as _;
    use wasmkeep_cluster::node_lease::{
        CREATED_FENCE_AHEAD_MS, NodeLeaseObject, NodeState, OperatorFence, created_fence,
    };
    let (written_at_ms, created) = match outcome {
        OperatorFence::Fenced {
            written_at_ms,
            created: true,
            ..
        } => (written_at_ms, true),
        OperatorFence::NotLive {
            state: NodeState::Fenced,
            written_at_ms,
        } => (written_at_ms, false),
        _ => return Ok(None),
    };
    // Only a created fence: a peer's fence keeps its node's own date.
    if !created {
        let body = bucket
            .get(key)
            .await
            .map_err(|e| anyhow::anyhow!("reading the fence back: {e:?}"))?;
        if !body
            .and_then(|(b, _)| NodeLeaseObject::decode(&b))
            .is_some_and(|o| created_fence(&o))
        {
            return Ok(None);
        }
    }
    let modified = bucket
        .modified_ms(key)
        .await
        .map_err(|e| anyhow::anyhow!("reading the fence back: {e:?}"))?
        .ok_or_else(|| anyhow::anyhow!("the fence vanished right after it was read"))?;
    let bucket_ms = modified + 1_000 + lease.max_skew_ms();
    let mut warning = None;
    // This run's create is dated ahead of this machine's clock on purpose
    // (`CREATED_FENCE_AHEAD_MS`): the clock is judged by the time it read.
    if created {
        let read = written_at_ms.saturating_sub(CREATED_FENCE_AHEAD_MS);
        if bucket_ms > read && bucket_ms - read > 1_000 + 2 * lease.max_skew_ms() {
            warning = Some(format!(
                "this machine's clock is {:.1} s behind the bucket's; the fence is dated no \
                 earlier than the bucket's time",
                (bucket_ms - read) as f64 / 1000.0
            ));
        } else if read > bucket_ms {
            // Dated further ahead: the node's next process writes its first
            // lease body only once the fence is twice the skew old on its
            // clock.
            warning = Some(format!(
                "this machine's clock is {:.1} s ahead of the bucket's; the node can start again \
                 only once the fence's date has passed on its clock",
                (read - modified) as f64 / 1000.0
            ));
        }
    }
    // Moved forward only, never back: a reader that read the date may be
    // served it again later.
    if bucket_ms > *written_at_ms
        && wasmkeep_cluster::node_lease::raise_created_fence(bucket, key, bucket_ms)
            .await
            .map_err(|e| anyhow::anyhow!("moving the fence to the bucket's time: {e:?}"))?
    {
        *written_at_ms = bucket_ms;
    }
    Ok(warning)
}

/// `wasmkeep cluster forget` (ADR 0020, section 2).
pub(super) fn cluster_forget(args: &NodeArgs) -> anyhow::Result<()> {
    wasmkeep_cluster::membership::validate_node_id(&args.node)
        .map_err(|e| anyhow::anyhow!("invalid node id {:?}: {e}", args.node))?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let done = rt
        .block_on(status::forget_on(
            &store,
            &prefix,
            &args.node,
            status::now_ms(),
        ))
        .map_err(|why| anyhow::anyhow!("{why}"))?;
    println!("{done}");
    Ok(())
}

pub(super) fn cluster_status(args: StatusArgs) -> anyhow::Result<()> {
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
    let mut report = rt.block_on(status::read(
        store,
        &prefix,
        &args.bucket,
        now_ms,
        horizon_ms,
    ))?;
    report.wasmkeep = wasmkeep_cluster::upgrade::this_minor().ok();
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

#[cfg(test)]
mod fence_tests {
    use super::date_by_bucket;
    use std::sync::Arc;
    use wasmkeep_cluster::lease::Bucket as _;
    use wasmkeep_cluster::node_lease::{NodeLeaseObject, NodeState, OperatorFence};
    use wasmkeep_cluster::object_store::ObjectStoreBucket;

    fn body(instance: Option<&str>, at: u64) -> Vec<u8> {
        NodeLeaseObject {
            state: NodeState::Fenced,
            instance: instance.map(str::to_owned),
            written_at_ms: at,
        }
        .encode()
    }

    /// A created fence this run found already there (its own create whose
    /// reply was lost, or one that landed late) is dated by the bucket's
    /// time, as one it created; a peer's fence keeps its node's date.
    #[tokio::test]
    async fn a_created_fence_found_is_dated_by_the_bucket() {
        let lease = wasmkeep_server::LeaseConfig::default_safe();
        let bucket = ObjectStoreBucket::new(Arc::new(object_store::memory::InMemory::new()), "");
        let date = |b: Option<(Vec<u8>, _)>| NodeLeaseObject::decode(&b.unwrap().0).unwrap();
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        for (key, created) in [("nodes/a", false), ("nodes/c", true)] {
            // Dated 100 s, as by a create sent long before it landed.
            bucket.create(key, body(None, 100_000)).await.unwrap();
            let mut outcome = if created {
                OperatorFence::Fenced {
                    written_at_ms: 100_000,
                    instance: None,
                    created: true,
                }
            } else {
                OperatorFence::NotLive {
                    state: NodeState::Fenced,
                    written_at_ms: 100_000,
                }
            };
            date_by_bucket(&bucket, key, &lease, &mut outcome)
                .await
                .unwrap();
            let at = date(bucket.get(key).await.unwrap()).written_at_ms;
            assert!(at >= now, "{key}: dated {at}, before {now}");
            let reported = match outcome {
                OperatorFence::Fenced { written_at_ms, .. }
                | OperatorFence::NotLive { written_at_ms, .. } => written_at_ms,
                OperatorFence::OldFormat | OperatorFence::Forgotten => unreachable!(),
            };
            assert_eq!(reported, at, "{key}");
        }
        // A peer's fence (an instance): left as it is.
        bucket
            .create("nodes/b", body(Some("i1"), 100_000))
            .await
            .unwrap();
        let mut outcome = OperatorFence::NotLive {
            state: NodeState::Fenced,
            written_at_ms: 100_000,
        };
        date_by_bucket(&bucket, "nodes/b", &lease, &mut outcome)
            .await
            .unwrap();
        assert_eq!(
            date(bucket.get("nodes/b").await.unwrap()).written_at_ms,
            100_000
        );
    }
}
