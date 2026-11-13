//! `wasmkeep cluster migrate`: moves a bucket prefix's actor state into
//! each app's root, once, while every node is stopped (ADR 0022). The
//! protocol is `wasmkeep_cluster::migrate`; this reads the apps' deploys,
//! prints the plan and the outcome, and sets the exit code: 0 done (or
//! nothing to do), 1 refused or aborted with nothing new committed, 2
//! some tenants committed and others not. `--remove-old` (with `--apply`)
//! later deletes the old layout; its exit code is 0 done, 1 refused or
//! stopped (run it again).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use clap::Args;
use wasmkeep_cluster::lease::Clock;
use wasmkeep_cluster::migrate::{
    self, AppInfo, MigrateError, Migrator, Options, Pause, Plan, Removal, RemoveOptions, Store as _,
};
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_server::apps::AppRef;

/// `--history-retention-secs` when not given.
const DEFAULT_HISTORY_RETENTION_SECS: u64 = 3600;

#[derive(Debug, Args)]
pub(crate) struct MigrateArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Migrate, or with --remove-old delete (without it: a dry run, which
    /// writes nothing).
    #[arg(long, conflicts_with = "rollback")]
    apply: bool,

    /// Undo a migration, while no node of any version has started since
    /// (after that, it is one-way).
    #[arg(long)]
    rollback: bool,

    /// Once the app answers on this version: list (with --apply, delete)
    /// the copies a migration left in the layout before 0.4.0, for every
    /// migrated tenant. Keeps the tombstones over the old ownership
    /// records, history younger than --history-retention-secs and move
    /// requests not yet expired; never touches state/, apps/ or nodes/.
    /// Refuses while the rollback still works (no node started since),
    /// unless given --forgo-rollback, and on any old-layout key written
    /// after the migration (an older binary ran). From its first delete,
    /// --rollback refuses. It can be run again after a crash.
    #[arg(long, conflicts_with_all = ["rollback", "heirs"])]
    remove_old: bool,

    /// With --remove-old: delete even though the rollback would still
    /// work (no node has started since the migration), giving it up.
    #[arg(long, requires = "remove_old")]
    forgo_rollback: bool,

    /// With --remove-old: old-layout history younger than this is kept
    /// for a later run, as garbage collection keeps it. Give the nodes'
    /// value.
    #[arg(long, requires = "remove_old", default_value_t = DEFAULT_HISTORY_RETENTION_SECS)]
    history_retention_secs: u64,

    /// The app of a tenant that inherits the classes no single app
    /// deploys (a class two apps deploy, or one no app deploys in a tenant
    /// with several apps), as TENANT/APP. Repeatable, once per tenant.
    #[arg(long = "heir", value_name = "TENANT/APP", value_parser = crate::parse_app)]
    heirs: Vec<(String, String)>,

    /// Bucket requests in flight at once.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        crate::status::now_ms()
    }
}

impl Pause for SystemClock {
    async fn pause_ms(&self, ms: u64) {
        // Tokio's timers run on a monotonic clock.
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// Every app under `apps/` with a `current`, and the classes of its
/// current version. A folder without one (secrets only, a first deploy
/// that failed) deploys nothing, so it inherits nothing.
async fn read_apps(
    bucket: &ObjectStoreBucket,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> anyhow::Result<Vec<AppInfo>> {
    let mut out = Vec::new();
    for tenant in bucket.list_dirs("apps/").await? {
        for app in bucket.list_dirs(&format!("apps/{tenant}/")).await? {
            let app_ref = AppRef::new(std::sync::Arc::clone(store), prefix, &tenant, &app);
            let Some(current) = app_ref.current().await? else {
                continue;
            };
            let classes = app_ref
                .manifest(&current.version)
                .await?
                .classes
                .into_keys()
                .collect();
            out.push(AppInfo {
                tenant: tenant.clone(),
                app,
                classes,
            });
        }
    }
    Ok(out)
}

fn print_plan(plan: &Plan, concurrency: usize) {
    if let Some(mode) = plan.locked {
        println!("A run holds the lock ({}): {mode:?}.", migrate::LOCK_KEY);
    }
    println!("Tenants:");
    for (tenant, t) in &plan.tenants {
        let state = if t.committed {
            "migrated"
        } else if t.pending() {
            "to migrate"
        } else {
            "nothing to do"
        };
        let counts: Vec<String> = t
            .counts
            .iter()
            .map(|(f, n)| format!("{} {n}", f.folder()))
            .collect();
        println!(
            "  {tenant}: {state}; apps: {}; objects: {}",
            if t.apps.is_empty() {
                "none".to_owned()
            } else {
                t.apps.iter().cloned().collect::<Vec<_>>().join(", ")
            },
            if counts.is_empty() {
                "none".to_owned()
            } else {
                counts.join(", ")
            }
        );
        for (class, heir) in &t.heirs {
            println!("    class {class} -> {tenant}/{heir}");
        }
        for (class, apps) in &t.shared {
            println!(
                "    class {class} is deployed by {}: the apps other than its heir start it empty",
                apps.join(" and ")
            );
        }
    }
    println!("Node leases and heartbeats under nodes/:");
    if plan.nodes.is_empty() {
        println!("  none");
    }
    for n in &plan.nodes {
        println!(
            "  {}: {}",
            n.meta.key,
            n.live.as_deref().unwrap_or("stopped")
        );
    }
    let secs = plan.estimate_ms(concurrency).div_ceil(1000);
    println!(
        "About {} objects to copy; an estimate of {secs} s at --concurrency {concurrency} \
         (from this run's listing latency, {} ms a request).",
        plan.objects(),
        plan.request_ms
    );
    if !plan.refusals.is_empty() {
        println!("Refused:");
        for r in &plan.refusals {
            println!("  {r}");
        }
    }
}

/// `verb`: "to delete" for a dry run, "deleted" once done.
fn print_removal(removal: &Removal, verb: &str) {
    if removal.tenants.is_empty() {
        println!("No migrated tenant has keys left in the layout before 0.4.0.");
    }
    for (tenant, t) in &removal.tenants {
        println!(
            "  {tenant}: {} {verb}; kept: {} tombstones, {} history objects younger than the \
             retention, {} move requests not expired{}",
            t.delete.len(),
            t.tombstones,
            t.young_history,
            t.live_moves,
            if t.rollback_open {
                "; the rollback still works"
            } else {
                ""
            }
        );
        for key in t.delete.iter().take(5) {
            println!("    {key}");
        }
        if t.delete.len() > 5 {
            println!("    ... and {} more", t.delete.len() - 5);
        }
    }
    for tenant in &removal.skipped {
        println!(
            "  {tenant}: not migrated (keys in the layout before 0.4.0, no commit): left alone"
        );
    }
}

/// What a dry run of `--remove-old` without refusals says to do next.
fn removal_hint(removal: &Removal, forgo_rollback: bool, history_retention_secs: u64) -> String {
    let kept: usize = removal
        .tenants
        .values()
        .map(|t| t.young_history + t.live_moves)
        .sum();
    if removal.deletes() == 0 {
        let mut hint = "Nothing to delete now".to_owned();
        if kept > 0 {
            let _ = write!(
                hint,
                ": {kept} objects are kept until they are older; run this again later"
            );
        }
        hint.push('.');
        return hint;
    }
    let mut flags = "--remove-old --apply".to_owned();
    if forgo_rollback {
        flags.push_str(" --forgo-rollback");
    }
    if history_retention_secs != DEFAULT_HISTORY_RETENTION_SECS {
        let _ = write!(flags, " --history-retention-secs {history_retention_secs}");
    }
    format!(
        "Dry run: nothing was deleted. Run this again with {flags} to delete {} keys; from \
         then on, --rollback refuses.",
        removal.deletes()
    )
}

/// What a dry run of the migration says to do next.
fn plan_hint(plan: &Plan) -> &'static str {
    if plan.pending().is_empty() {
        "Nothing to migrate: start the nodes of this version (from the first start the \
         migration is one-way)."
    } else {
        "Dry run: nothing was written. Stop every node (the service units), freeze deploys, \
         then run this again with --apply."
    }
}

/// The line `--apply` prints before it waits: the plan, and so the window,
/// is decided inside the run.
fn apply_line(lease_window_ms: u64, old_window_ms: u64) -> String {
    format!(
        "Taking the lock, then waiting {} s (one node-lease window; {} s when nodes/ still \
         holds a heartbeat from before node leases) before copying. Run one --apply or \
         --rollback at a time: another run takes the lock over and this one stops.",
        lease_window_ms.div_ceil(1000),
        old_window_ms.max(lease_window_ms).div_ceil(1000)
    )
}

/// `--remove-old`; gives the exit code.
async fn remove_old<S: migrate::Store, C: Pause>(
    migrator: &Migrator<'_, S, C>,
    args: &MigrateArgs,
) -> i32 {
    let options = RemoveOptions {
        forgo_rollback: args.forgo_rollback,
        history_retention_ms: args.history_retention_secs.saturating_mul(1000),
    };
    if !args.apply {
        return match migrator.removal(options).await {
            Ok(removal) => {
                println!("Old layout of the migrated tenants:");
                print_removal(&removal, "to delete");
                if removal.refusals.is_empty() {
                    println!(
                        "{}",
                        removal_hint(&removal, args.forgo_rollback, args.history_retention_secs)
                    );
                    0
                } else {
                    println!("Refused:");
                    for r in &removal.refusals {
                        println!("  {r}");
                    }
                    1
                }
            }
            Err(e) => {
                eprintln!("error: {e}");
                1
            }
        };
    }
    match migrator.remove_old(options).await {
        Ok(removal) => {
            println!("Old layout of the migrated tenants:");
            print_removal(&removal, "deleted");
            let kept: usize = removal
                .tenants
                .values()
                .map(|t| t.young_history + t.live_moves)
                .sum();
            if kept > 0 {
                println!(
                    "{kept} objects were kept until they are older: run this again later to \
                     delete them."
                );
            }
            0
        }
        Err(e) => {
            eprintln!("error: {e}");
            if !matches!(e, MigrateError::Refused(_)) {
                eprintln!("Run this again: it goes on from where it stopped.");
            }
            1
        }
    }
}

/// Runs the command; gives the exit code.
pub(crate) fn run(args: &MigrateArgs) -> anyhow::Result<i32> {
    let (store, prefix) = crate::open_bucket_unchecked(&args.bucket)?;
    let bucket = ObjectStoreBucket::new(std::sync::Arc::clone(&store), prefix.clone());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let lease = wasmkeep_server::LeaseConfig::default_safe();
    let options = Options {
        heirs: args.heirs.iter().cloned().collect::<BTreeMap<_, _>>(),
        concurrency: args.concurrency.max(1),
        lease_window_ms: lease
            .ttl_ms()
            .saturating_add(lease.grace_ms())
            .saturating_add(lease.max_skew_ms()),
        old_window_ms: wasmkeep_server::heartbeat_horizon_ms(
            wasmkeep_server::DEFAULT_HEARTBEAT,
            &lease,
        )
        .saturating_add(lease.ttl_ms())
        .saturating_add(lease.grace_ms()),
        prefix_bytes: if prefix.is_empty() {
            0
        } else {
            prefix.len() + 1
        },
    };
    let apply_waits = apply_line(options.lease_window_ms, options.old_window_ms);
    anyhow::ensure!(
        options.heirs.len() == args.heirs.len(),
        "--heir names a tenant twice: give one heir per tenant"
    );
    rt.block_on(async {
        let apps = read_apps(&bucket, &store, &prefix).await?;
        let migrator = Migrator::new(&bucket, &SystemClock, apps, options);
        if args.remove_old {
            return Ok(remove_old(&migrator, args).await);
        }
        if args.rollback {
            return match migrator.rollback().await {
                Ok(tenants) if tenants.is_empty() => {
                    println!("Nothing to roll back.");
                    Ok(0)
                }
                Ok(tenants) => {
                    println!(
                        "Rolled back: {}. Actor state is in the layout before 0.4.0 again, \
                         for the version that wrote it.",
                        tenants.join(", ")
                    );
                    Ok(0)
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    Ok(1)
                }
            };
        }
        if !args.apply {
            let plan = migrator.plan().await?;
            print_plan(&plan, args.concurrency.max(1));
            if plan.refusals.is_empty() {
                println!("{}", plan_hint(&plan));
                return Ok(0);
            }
            return Ok(1);
        }
        println!("{apply_waits}");
        match migrator.apply().await {
            Ok(outcome) => {
                println!("{:<24} state", "tenant");
                for t in &outcome.committed {
                    println!("{t:<24} migrated now");
                }
                for t in &outcome.already {
                    println!("{t:<24} migrated before");
                }
                for (t, why) in &outcome.failed {
                    println!("{t:<24} not migrated: {why}");
                }
                if outcome.committed.is_empty()
                    && outcome.already.is_empty()
                    && outcome.failed.is_empty()
                {
                    println!("Nothing to migrate.");
                }
                if outcome.failed.is_empty() {
                    println!(
                        "Done. Start the nodes of this version; from the first start the \
                         migration is one-way."
                    );
                } else {
                    println!("Run this again to migrate the rest (the lock stays until then).");
                }
                Ok(outcome.exit_code())
            }
            Err(e @ (MigrateError::Aborted(_) | MigrateError::Bucket { .. })) => {
                eprintln!("error: {e}");
                eprintln!("Nothing new was committed. Run this again (it resumes), or --rollback.");
                Ok(1)
            }
            Err(e) => {
                eprintln!("error: {e}");
                Ok(1)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmkeep_cluster::migrate::{TenantPlan, TenantRemoval};

    #[test]
    fn a_dry_run_with_every_tenant_migrated_says_to_start_the_nodes() {
        let mut plan = Plan::default();
        plan.tenants.insert(
            "default".into(),
            TenantPlan {
                committed: true,
                ..TenantPlan::default()
            },
        );
        assert!(plan_hint(&plan).starts_with("Nothing to migrate: start the nodes"));
        plan.tenants.insert(
            "acme".into(),
            TenantPlan {
                counts: BTreeMap::from([(wasmkeep_cluster::state_root::Family::History, 1)]),
                ..TenantPlan::default()
            },
        );
        assert!(plan_hint(&plan).contains("then run this again with --apply"));
    }

    #[test]
    fn a_removal_with_nothing_to_delete_offers_no_apply() {
        let mut removal = Removal::default();
        let hint = removal_hint(&removal, true, 0);
        assert!(hint.starts_with("Nothing to delete now"), "{hint}");
        assert!(!hint.contains("--apply"), "{hint}");
        removal.tenants.insert(
            "default".into(),
            TenantRemoval {
                young_history: 2,
                ..TenantRemoval::default()
            },
        );
        assert!(removal_hint(&removal, false, 3600).contains("run this again later"));
        if let Some(t) = removal.tenants.get_mut("default") {
            t.delete.push("actors/default/t/a/snapshot/1".into());
        }
        let hint = removal_hint(&removal, true, 0);
        assert!(
            hint.contains("--remove-old --apply --forgo-rollback --history-retention-secs 0"),
            "{hint}"
        );
        assert!(hint.contains("to delete 1 keys"), "{hint}");
        let hint = removal_hint(&removal, false, 3600);
        assert!(
            hint.contains("again with --remove-old --apply to delete"),
            "{hint}"
        );
    }

    #[test]
    fn the_apply_line_names_both_windows() {
        let line = apply_line(16_000, 25_000);
        assert!(line.contains("16 s"), "{line}");
        assert!(line.contains("25 s"), "{line}");
    }
}
