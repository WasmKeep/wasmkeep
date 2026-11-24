//! `wasmkeep cluster`: status, drain, undrain, fence and forget.

use super::*;

#[derive(Debug, Args)]
pub(super) struct DrainArgs {
    /// The node's id.
    #[arg(required_unless_present = "process", conflicts_with = "process")]
    node: Option<String>,

    /// Every node id this process runs (ADR 0038: the `process=` its
    /// heartbeats name), instead of one node.
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
    /// addresses, TLS pin, age, live or not) and each app's deployed
    /// version. A heartbeat is live for 10 s, the nodes' own rule with the
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
    /// fenced node lease (never deleted: fence a live one instead).
    Forget(NodeArgs),
    /// Move the actor state of a bucket prefix written by a version before
    /// 0.4.0 into each app's own root, once, while every node is stopped
    /// (ADR 0022). Without --apply, a dry run: prints the plan (the app
    /// each class goes to, the nodes it sees, object counts, an estimate)
    /// and writes nothing. --apply copies, verifies, puts barriers in front
    /// of older binaries, proves no node ran, then commits; it can be run
    /// again after a crash. --rollback undoes it until a node starts.
    /// --remove-old (a dry run; deletes with --apply) later removes the
    /// copies left in the old layout, keeping the tombstones.
    /// Exits with 0 when done, 1 when refused or aborted (nothing new
    /// committed), 2 when some tenants were committed and others not.
    Migrate(migrate_cmd::MigrateArgs),
    /// Check that a bucket does what WasmKeep needs from it: conditional
    /// creates and replaces, reads and listings that follow writes and
    /// deletes, large objects, last-modified times and the clock. Runs
    /// through the nodes' own bucket code, writing only under a scratch
    /// prefix it requires to be empty and deletes afterwards. One line per
    /// check, then a verdict. Exits with 0 when every check observed the
    /// required behaviour (not a proof of safety), 1 when one did not, 2
    /// when it could not run or finish.
    Diagnose(diagnose::DiagnoseArgs),
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
pub(crate) fn process_node_ids(
    nodes: &[wasmkeep_cluster::membership::NodeInfo],
    process: &str,
    now_ms: u64,
    horizon_ms: u64,
) -> Vec<String> {
    let mut ids: Vec<String> = nodes
        .iter()
        .filter(|n| {
            n.process.as_deref() == Some(process)
                && wasmkeep_cluster::membership::is_live(n, now_ms, horizon_ms)
        })
        .map(|n| n.id.clone())
        .collect();
    ids.sort();
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
                "no live node id names the process {process} (see `wasmkeep cluster status`)"
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
    use wasmkeep_cluster::node_lease::{OperatorFence, operator_fence};
    wasmkeep_cluster::membership::validate_node_id(&args.node)
        .map_err(|e| anyhow::anyhow!("invalid node id {:?}: {e}", args.node))?;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let lease = wasmkeep_server::LeaseConfig::default_safe();
    let key = format!("nodes/{}", args.node);
    let mut outcome = rt
        .block_on(operator_fence(&bucket, &status::Wall, &key))
        .map_err(|e| anyhow::anyhow!("fencing {}: {e:?}", args.node))?;
    // A fence created here carries this machine's clock, which the free
    // rule trusts within the allowed skew: checked against the bucket's own
    // time for that write (rounded up to its second, plus the skew), and
    // moved later if this clock runs behind it.
    if let OperatorFence::Fenced {
        written_at_ms,
        created: true,
        ..
    } = &mut outcome
    {
        let modified = rt
            .block_on(bucket.modified_ms(&key))
            .map_err(|e| anyhow::anyhow!("reading the fence back: {e:?}"))?
            .ok_or_else(|| anyhow::anyhow!("the fence vanished right after it was written"))?;
        let bucket_ms = modified + 1_000 + lease.max_skew_ms();
        if bucket_ms > *written_at_ms {
            if bucket_ms - *written_at_ms > 1_000 + 2 * lease.max_skew_ms() {
                eprintln!(
                    "warning: this machine's clock is {:.1} s behind the bucket's; the fence uses the bucket's time",
                    (bucket_ms - *written_at_ms) as f64 / 1000.0
                );
            }
            rt.block_on(wasmkeep_cluster::node_lease::raise_created_fence(
                &bucket, &key, bucket_ms,
            ))
            .map_err(|e| anyhow::anyhow!("moving the fence to the bucket's time: {e:?}"))?;
            *written_at_ms = bucket_ms;
        } else if *written_at_ms > bucket_ms {
            // Dated ahead: the node's next process writes its first lease
            // body only once the fence is twice the skew old on its clock
            // (ADR 0020, amendment on lease reads).
            eprintln!(
                "warning: this machine's clock is {:.1} s ahead of the bucket's; node {} can start \
                 again only once the fence's date has passed on its clock",
                (*written_at_ms - modified) as f64 / 1000.0,
                args.node
            );
        }
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
        OperatorFence::OldFormat => anyhow::bail!(
            "{}'s heartbeat is in the format from before node leases: that node would not read a fence. Stop it; once its heartbeat is stale, `wasmkeep cluster forget {}` removes it",
            args.node,
            args.node
        ),
    }
    Ok(())
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
    let report = rt.block_on(status::read(
        store,
        &prefix,
        &args.bucket,
        now_ms,
        horizon_ms,
    ))?;
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
