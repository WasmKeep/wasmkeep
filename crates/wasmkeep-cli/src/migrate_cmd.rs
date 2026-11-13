//! `wasmkeep cluster migrate`: moves a bucket prefix's actor state into
//! each app's root, once, while every node is stopped (ADR 0022). The
//! protocol is `wasmkeep_cluster::migrate`; this reads the apps' deploys,
//! prints the plan and the outcome, and sets the exit code: 0 done (or
//! nothing to do), 1 refused or aborted with nothing new committed, 2
//! some tenants committed and others not.

use std::collections::BTreeMap;

use clap::Args;
use wasmkeep_cluster::lease::Clock;
use wasmkeep_cluster::migrate::{self, AppInfo, MigrateError, Migrator, Options, Plan, Store as _};
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_server::apps::AppRef;

#[derive(Debug, Args)]
pub(crate) struct MigrateArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Migrate (without it: a dry run, which writes nothing).
    #[arg(long, conflicts_with = "rollback")]
    apply: bool,

    /// Undo a migration, while no node of any version has started since
    /// (after that, it is one-way).
    #[arg(long)]
    rollback: bool,

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

/// Every app under `apps/` and the classes of its current version.
async fn read_apps(
    bucket: &ObjectStoreBucket,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> anyhow::Result<Vec<AppInfo>> {
    let mut out = Vec::new();
    for tenant in bucket.list_dirs("apps/").await? {
        for app in bucket.list_dirs(&format!("apps/{tenant}/")).await? {
            let app_ref = AppRef::new(std::sync::Arc::clone(store), prefix, &tenant, &app);
            let classes = match app_ref.current().await? {
                Some(current) => app_ref
                    .manifest(&current.version)
                    .await?
                    .classes
                    .into_keys()
                    .collect(),
                // Uploads, secrets, no `current`: an app with no class.
                None => Default::default(),
            };
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
        } else {
            "to migrate"
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
    };
    anyhow::ensure!(
        options.heirs.len() == args.heirs.len(),
        "--heir names a tenant twice: give one heir per tenant"
    );
    rt.block_on(async {
        let apps = read_apps(&bucket, &store, &prefix).await?;
        let migrator = Migrator::new(&bucket, &SystemClock, apps, options);
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
                println!(
                    "Dry run: nothing was written. Stop every node (the service units), freeze \
                     deploys, then run this again with --apply."
                );
                return Ok(0);
            }
            return Ok(1);
        }
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
