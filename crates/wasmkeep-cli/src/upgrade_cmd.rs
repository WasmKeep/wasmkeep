//! `wasmkeep cluster upgrade status|finalize`: the upgrade of WasmKeep
//! itself, one node at a time ([ADR 0043], section 4).
//!
//! `status` reads each app's upgrade floor and its nodes' versions and
//! names one state. `finalize` is the operator's "the upgrade is done":
//! refused while a live node of the app is older (or, with no node of the
//! app live, unless every lease of the app is stopped and names this
//! version or a later one), it raises the floor to this `wasmkeep`'s
//! minor version, then lists the nodes again after one start check. From
//! then on no node of an older version starts, and the formats the new
//! version introduced may be written.
//!
//! [ADR 0043]: ../../../docs/adr/0043-rolling-upgrades.md

use std::fmt::Write as _;
use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{Value, json};
use wasmkeep_cluster::app_format::floor::Minor;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::upgrade::{self, AppUpgrade, Finalize, State};

#[derive(Debug, Subcommand)]
pub(crate) enum UpgradeCommand {
    /// Show the upgrade of WasmKeep itself, per app, read from the bucket
    /// alone: each registered node with its version, the app's upgrade
    /// floor (the lowest version allowed to serve it), one state (`one
    /// version`, `mixed`, `finalize pending`, `no live node`, `node below
    /// the floor`, `refused`) and the nodes that block a finalize. Exits
    /// with 0 for `one version`, 3 while an upgrade is under way (`mixed`,
    /// `finalize pending`) or no node is live, 1 for a state that needs
    /// attention or an unreadable floor.
    Status(StatusArgs),
    /// End an upgrade: raise each app's upgrade floor to this `wasmkeep`'s
    /// minor version, so that no node of an older version starts again
    /// (there is no way back). Refused, naming them, while a live node of
    /// the app runs an older version. With no node of the app live (every
    /// node stopped for the upgrade), accepted when every registered lease
    /// of the app is released or fenced and names this version or a later
    /// one. After the write it waits one start check (about 25 s) and
    /// lists the nodes again: an older node that is live then is an error
    /// (exit code 1) and must be stopped. From then on no node of an older
    /// version starts, and going back is not supported. Without --app,
    /// every app is raised that can be, each refused app is named with
    /// its reason, and the exit code is 1 if any was refused. Without
    /// --yes it prints what it would do and exits with 2.
    Finalize(FinalizeArgs),
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables; read access is enough).
    #[arg(long)]
    bucket: String,

    /// One app, TENANT/APP, instead of every app with registered nodes.
    #[arg(long, value_parser = crate::parse_app)]
    app: Option<(String, String)>,

    /// Print JSON instead of text, for scripts.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FinalizeArgs {
    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// One app, TENANT/APP, instead of every app with registered nodes.
    #[arg(long, value_parser = crate::parse_app)]
    app: Option<(String, String)>,

    /// Write the floor; without it, print what would be done and exit
    /// with 2.
    #[arg(long)]
    yes: bool,
}

fn horizon_ms() -> u64 {
    wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &wasmkeep_server::LeaseConfig::default_safe(),
    )
}

fn named(app: &Option<(String, String)>) -> Option<String> {
    app.as_ref().map(|(tenant, app)| format!("{tenant}/{app}"))
}

/// `wasmkeep cluster upgrade`: runs the command and exits with its code.
pub(crate) fn run(command: UpgradeCommand) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let this = upgrade::this_minor().map_err(anyhow::Error::msg)?;
    let (out, code) = match command {
        UpgradeCommand::Status(args) => {
            let (store, prefix) = crate::open_bucket_unchecked(&args.bucket)?;
            let bucket = ObjectStoreBucket::new(store, prefix).with_read_concurrency(
                const {
                    std::num::NonZeroUsize::new(crate::status::STATUS_READS_AT_ONCE)
                        .expect("status concurrency is nonzero")
                },
            );
            let apps = rt
                .block_on(upgrade::read_statuses(
                    &bucket,
                    named(&args.app).as_deref(),
                    crate::status::now_ms(),
                    horizon_ms(),
                ))
                .map_err(|e| anyhow::anyhow!("reading the cluster: {e}"))?;
            let code = status_exit(&apps);
            let out = if args.json {
                format!("{:#}\n", status_json(&apps, this))
            } else {
                status_text(&args.bucket, &apps, this)
            };
            (out, code)
        }
        UpgradeCommand::Finalize(args) => {
            let (store, prefix) = crate::open_bucket(&args.bucket)?;
            let bucket = ObjectStoreBucket::new(store, prefix);
            let wait = Duration::from_millis(upgrade::finalize_wait_ms(
                &wasmkeep_server::LeaseConfig::default_safe(),
                horizon_ms(),
            ));
            rt.block_on(finalize(
                &bucket,
                named(&args.app).as_deref(),
                this,
                args.yes,
                wait,
                &mut |line| println!("{line}"),
            ))?
        }
    };
    print!("{out}");
    if code != 0 {
        use std::io::Write as _;
        let _ = std::io::stdout().flush();
        std::process::exit(i32::from(code));
    }
    Ok(())
}

/// The exit code of `upgrade status` over `apps`: the worst of their
/// states (1 over 3 over 0); 3 with no app at all.
pub(crate) fn status_exit(apps: &[AppUpgrade]) -> u8 {
    let codes = || apps.iter().map(|a| a.state.exit_code());
    if codes().any(|c| c == 1) {
        1
    } else if apps.is_empty() || codes().any(|c| c == 3) {
        3
    } else {
        0
    }
}

/// A read-only command warns and goes on when a live node is outside
/// this `wasmkeep`'s range, or the floor is above it (ADR 0043, section
/// 5); a command that writes refuses. `cluster upgrade status`, `cluster
/// status` and `actor where` print it.
pub(crate) fn reader_warning(app: &AppUpgrade, this: Minor) -> Option<String> {
    let outside: Vec<String> = app
        .nodes
        .iter()
        .filter(|n| n.live && n.minor.is_none_or(|v| !upgrade::beside(this, v)))
        .map(|n| format!("{} ({})", n.id, n.version))
        .collect();
    if app.floor.is_ok_and(|f| f.is_some_and(|f| f > this)) {
        return Some(format!(
            "this `wasmkeep` ({this}) is older than the cluster: {}'s floor is {}; commands \
             that write are refused",
            app.app,
            app.floor_text()
        ));
    }
    (!outside.is_empty()).then(|| {
        format!(
            "this `wasmkeep` ({this}) runs only beside the version before and the one after, \
             and {} has live nodes outside that: {}; commands that write are refused",
            app.app,
            outside.join(", ")
        )
    })
}

/// A node's LIVE cell: `yes`, or `no` with what its lease says. A lease
/// that is stale but neither released nor fenced is not proof of a stop.
fn live_text(node: &upgrade::NodeVersion) -> &'static str {
    use wasmkeep_cluster::node_lease::NodeState;
    match (node.live, node.lease) {
        (true, _) => "yes",
        (false, Some(NodeState::Released)) => "no (released)",
        (false, Some(NodeState::Fenced)) => "no (fenced)",
        (false, Some(NodeState::Live)) => "no (stale, not fenced)",
        (false, None) => "no (old format)",
    }
}

pub(crate) fn status_text(bucket: &str, apps: &[AppUpgrade], this: Minor) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Upgrade of {bucket} (this wasmkeep: {this})\n");
    if apps.is_empty() {
        out.push_str("No app has a registered node under this prefix.\n");
    }
    for app in apps {
        let _ = writeln!(out, "{}", app.app);
        let _ = writeln!(out, "  floor: {}", app.floor_text());
        let _ = writeln!(out, "  state: {}", app.state.as_str());
        let _ = writeln!(out, "  {}", app.detail);
        if !app.blocks.is_empty() {
            let _ = writeln!(out, "  blocks a finalize: {}", app.blocks.join(", "));
        }
        if !app.nodes.is_empty() {
            let rows: Vec<[String; 3]> = app
                .nodes
                .iter()
                .map(|n| {
                    [
                        format!("  {}", n.id),
                        n.version.clone(),
                        live_text(n).to_owned(),
                    ]
                })
                .collect();
            crate::status::table(&mut out, ["  NODE", "VERSION", "LIVE"], &rows);
        }
        if let Some(warning) = reader_warning(app, this) {
            let _ = writeln!(out, "  warning: {warning}");
        }
        out.push('\n');
    }
    out
}

/// One app's upgrade as JSON: what `upgrade status --json` lists, and
/// `cluster status --json` under `upgrade`.
pub(crate) fn app_json(app: &AppUpgrade) -> Value {
    json!({
        "app": app.app,
        // The record: null without one.
        "floor": app.floor.ok().flatten().map(|f| f.to_string()),
        // The floor in force (0.8 without a record); null if unreadable.
        "floor_in_force": app.floor_in_force().map(|f| f.to_string()),
        "floor_unreadable_bytes": app.floor.err(),
        "state": app.state.as_str(),
        "finalize_pending": app.state == State::FinalizePending,
        "detail": app.detail,
        "blocks": app.blocks,
        "nodes": app
            .nodes
            .iter()
            .map(|n| json!({
                "id": n.id,
                "version": n.version,
                "minor": n.minor.map(|m| m.to_string()),
                "live": n.live,
                // Its node lease's state; null for a heartbeat from
                // before node leases. Only `released` and `fenced` prove
                // a stop.
                "lease": n.lease.map(wasmkeep_cluster::node_lease::NodeState::as_str),
            }))
            .collect::<Vec<_>>(),
    })
}

pub(crate) fn status_json(apps: &[AppUpgrade], this: Minor) -> Value {
    json!({
        "wasmkeep": this.to_string(),
        "apps": apps.iter().map(app_json).collect::<Vec<_>>(),
        "warnings": apps
            .iter()
            .filter_map(|a| reader_warning(a, this))
            .collect::<Vec<_>>(),
        "exit_code": status_exit(apps),
    })
}

/// `finalize` over every app with registered nodes, or `only`: what it
/// prints once done, and its exit code. `target` is the version that
/// finalizes; `wait` the start check between the write and the second
/// listing; `say` prints a line at once (the wait is long).
///
/// # Errors
/// The bucket did not answer.
pub(crate) async fn finalize(
    bucket: &ObjectStoreBucket,
    only: Option<&str>,
    target: Minor,
    yes: bool,
    wait: Duration,
    say: &mut (dyn FnMut(String) + Send),
) -> anyhow::Result<(String, u8)> {
    let horizon = horizon_ms();
    let now = crate::status::now_ms;
    let apps = upgrade::read_statuses(bucket, only, now(), horizon)
        .await
        .map_err(|e| anyhow::anyhow!("reading the cluster: {e}"))?;
    anyhow::ensure!(
        !apps.is_empty(),
        "no app has a registered node under this prefix: there is nothing to finalize"
    );
    let mut out = String::new();
    if !yes {
        for app in &apps {
            let (tenant, name) = app.app.split_once('/').unwrap_or((&app.app, ""));
            let line =
                match upgrade::finalize_plan(bucket, tenant, name, target, now(), horizon).await {
                    Ok(Finalize::Already(floor)) => {
                        format!("{}: already finalized at {floor}", app.app)
                    }
                    Ok(Finalize::Raise(..)) => format!(
                        "{}: would raise the upgrade floor from {} to {target}; no node older \
                         than {target} could start after it, and going back would not be \
                         supported",
                        app.app,
                        app.floor_text()
                    ),
                    Err(refused) => format!("{}: would be refused: {refused}", app.app),
                };
            let _ = writeln!(out, "{line}");
        }
        out.push_str("Nothing written. Pass --yes to finalize.\n");
        return Ok((out, 2));
    }
    let mut failed = false;
    let mut refused = Vec::new();
    let mut written = Vec::new();
    let mut settled = Vec::new();
    for app in &apps {
        let (tenant, name) = app.app.split_once('/').unwrap_or((&app.app, ""));
        match upgrade::finalize_write(bucket, tenant, name, target, now(), horizon).await {
            Ok(Finalize::Already(floor)) => {
                say(format!("{}: already finalized at {floor}", app.app));
                settled.push((tenant, name, app, false));
            }
            Ok(Finalize::Raise(from, to)) => {
                say(format!(
                    "{}: upgrade floor raised from {} to {to}. No node older than {to} starts \
                     from now, and going back is not supported",
                    app.app,
                    from.map_or_else(|| app.floor_text(), |f| f.to_string())
                ));
                written.push((tenant, name, app, true));
            }
            Err(why) => {
                failed = true;
                refused.push(app.app.as_str());
                say(format!("error: {}: {why}", app.app));
            }
        }
    }
    // The second listing (ADR 0043, section 4, step 5): never skipped.
    if !written.is_empty() {
        say(format!(
            "Waiting {} s (one start check) before listing the nodes again...",
            wait.as_secs()
        ));
        tokio::time::sleep(wait).await;
    }
    for (tenant, name, app, wrote) in written.into_iter().chain(settled) {
        let seen = upgrade::finalize_confirm(bucket, tenant, name, target, now(), horizon)
            .await
            .map_err(|e| anyhow::anyhow!("listing the nodes of {} again: {e}", app.app))?;
        let list = |nodes: &[upgrade::NodeVersion]| {
            nodes
                .iter()
                .map(|n| format!("{} ({})", n.id, n.version))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !seen.late.is_empty() {
            failed = true;
            let _ = writeln!(
                out,
                "error: {}: nodes older than the floor {target} are live, or stale without a \
                 release or a fence: {}. Each started while the floor was written, or keeps \
                 checking a floor it cannot read: stop it, or run `wasmkeep cluster fence` on \
                 it. The floor stays, and formats that need {target} stay refused while it is \
                 live",
                app.app,
                list(&seen.late)
            );
        }
        if !seen.unread.is_empty() {
            failed = true;
            let _ = writeln!(
                out,
                "error: {}: objects under `nodes/` do not decode at the listing after the \
                 write: {}. Each may be a node of this app, of a version this `wasmkeep` cannot \
                 read, which the floor cannot be shown to stop. The floor stays; find what \
                 writes each object, stop it, and delete the object from the bucket by hand",
                app.app,
                seen.unread.join(", ")
            );
        }
        if wrote && seen.late.is_empty() && seen.unread.is_empty() {
            let _ = writeln!(out, "{}: finalized at {target}", app.app);
        }
        if !seen.stopped_older.is_empty() {
            let _ = writeln!(
                out,
                "{}: stopped nodes still name an older version: {}. Such a node stops at its \
                 next start; install {target} there, or remove its lease with `wasmkeep \
                 cluster forget`",
                app.app,
                list(&seen.stopped_older)
            );
        }
    }
    if only.is_none() && !refused.is_empty() && apps.len() > 1 {
        let _ = writeln!(
            out,
            "{} of {} apps refused, nothing written for them: {}. Each `error:` line above says \
             why and what to do; finalize one again with `--app TENANT/APP`",
            refused.len(),
            apps.len(),
            refused.join(", ")
        );
    }
    Ok((out, u8::from(failed)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use wasmkeep_cluster::control::Keys as _;
    use wasmkeep_cluster::lease::Bucket as _;

    fn v(major: u32, minor: u32) -> Minor {
        Minor { major, minor }
    }

    fn bucket() -> ObjectStoreBucket {
        ObjectStoreBucket::new(Arc::new(object_store::memory::InMemory::new()), "")
    }

    async fn put(bucket: &ObjectStoreBucket, app: &str, id: &str, version: &str, state: &str) {
        let body = format!(
            "wasmkeep-node 3\n{state} i-{id}\n{}\n{id}\nhttp://{id}\n{version}\n{app}\n\n\nserving\n\ntake front\n",
            crate::status::now_ms()
        );
        bucket
            .put_body(&format!("nodes/{id}"), body.into_bytes())
            .await
            .unwrap();
    }

    async fn floor(bucket: &ObjectStoreBucket, app: &str) -> Option<Vec<u8>> {
        bucket
            .get(&format!("formats/{app}/floor"))
            .await
            .unwrap()
            .map(|(body, _)| body)
    }

    async fn run(
        bucket: &ObjectStoreBucket,
        only: Option<&str>,
        target: Minor,
        yes: bool,
    ) -> (String, u8) {
        let mut said = String::new();
        let (out, code) = finalize(bucket, only, target, yes, Duration::ZERO, &mut |line| {
            said.push_str(&line);
            said.push('\n');
        })
        .await
        .unwrap();
        (said + &out, code)
    }

    /// ADR 0043, section 4: refused while a live node is older, naming
    /// it, with nothing written; then written, idempotent, and the stopped
    /// older node reported without failing.
    #[tokio::test]
    async fn finalize_is_refused_while_a_live_node_is_older_then_writes_the_floor() {
        let b = bucket();
        put(&b, "acme/shop", "a", "0.9.0", "live").await;
        put(&b, "acme/shop", "old", "0.8.1", "live").await;
        let (out, code) = run(&b, None, v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains("error: acme/shop: live nodes are older than 0.9: old (0.8.1)"),
            "{out}"
        );
        assert_eq!(floor(&b, "acme/shop").await, None);

        put(&b, "acme/shop", "old", "0.8.1", "released").await;
        // Without --yes: what it would do, exit code 2, nothing written.
        let (out, code) = run(&b, None, v(0, 9), false).await;
        assert_eq!(code, 2, "{out}");
        assert!(
            out.contains("acme/shop: would raise the upgrade floor from none (0.8) to 0.9"),
            "{out}"
        );
        assert!(out.ends_with("Nothing written. Pass --yes to finalize.\n"));
        assert_eq!(floor(&b, "acme/shop").await, None);

        let (out, code) = run(&b, None, v(0, 9), true).await;
        assert_eq!(code, 0, "{out}");
        assert!(
            out.contains("acme/shop: upgrade floor raised from none (0.8) to 0.9"),
            "{out}"
        );
        assert!(out.contains("Waiting 0 s (one start check)"), "{out}");
        assert!(out.contains("acme/shop: finalized at 0.9"), "{out}");
        assert!(
            out.contains("stopped nodes still name an older version: old (0.8.1)"),
            "{out}"
        );
        assert_eq!(
            floor(&b, "acme/shop").await.as_deref(),
            Some(&b"wasmkeep-floor 1\n0.9\n"[..])
        );
        // Again: already there.
        let (out, code) = run(&b, Some("acme/shop"), v(0, 9), true).await;
        assert_eq!(code, 0, "{out}");
        assert!(out.contains("acme/shop: already finalized at 0.9"), "{out}");
        // An older wasmkeep is told so, and a newer live node needs its own.
        let (out, code) = run(&b, None, v(0, 8), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(out.contains("older than the cluster"), "{out}");
    }

    /// Each app has its own floor: one refused, the other finalized, and
    /// the command fails as a whole. An older node live at the second
    /// listing is an error, and the floor stays.
    #[tokio::test]
    async fn finalize_covers_every_app_and_reports_a_late_node() {
        let b = bucket();
        put(&b, "acme/shop", "a", "0.9.0", "live").await;
        put(&b, "acme/blog", "b", "0.9.0", "live").await;
        put(&b, "acme/blog", "c", "0.10.0", "live").await;
        let (out, code) = run(&b, None, v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains("error: acme/blog: live nodes run a newer version"),
            "{out}"
        );
        assert!(out.contains("acme/shop: finalized at 0.9"), "{out}");
        assert_eq!(floor(&b, "acme/blog").await, None);
        assert!(floor(&b, "acme/shop").await.is_some());
        // A node of 0.8 that never saw the floor, live at the listing.
        put(&b, "acme/shop", "late", "0.8.1", "live").await;
        let (out, code) = run(&b, Some("acme/shop"), v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains(
                "error: acme/shop: nodes older than the floor 0.9 are live, or stale without a \
                 release or a fence: late (0.8.1)"
            ),
            "{out}"
        );
        assert!(floor(&b, "acme/shop").await.is_some(), "the floor stays");
        // M2: an older lease still `live` but stale is not a stopped node.
        // The finalize is refused and names the fence; once fenced or
        // released, it goes through.
        let c = bucket();
        put(&c, "acme/shop", "a", "0.9.0", "live").await;
        let stale = format!(
            "wasmkeep-node 3\nlive i-cut\n{}\ncut\nhttp://cut\n0.8.1\nacme/shop\n\n\nserving\n\ntake\n",
            crate::status::now_ms() - 3_600_000
        );
        c.put_body("nodes/cut", stale.clone().into_bytes())
            .await
            .unwrap();
        let (out, code) = run(&c, None, v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains(
                "their lease was never released, nor fenced for longer than a lease's ttl plus \
                 grace: cut (0.8.1)"
            ) && out.contains("`wasmkeep cluster fence <id>` for each (cut)"),
            "{out}"
        );
        assert_eq!(floor(&c, "acme/shop").await, None, "nothing written");
        let apps = upgrade::read_statuses(&c, None, crate::status::now_ms(), horizon_ms())
            .await
            .unwrap();
        assert_eq!(apps[0].blocks, ["cut"]);
        assert!(
            status_text("s3://b", &apps, v(0, 9)).contains("no (stale, not fenced)"),
            "{}",
            status_text("s3://b", &apps, v(0, 9))
        );
        c.put_body(
            "nodes/cut",
            stale.replacen("\nlive ", "\nfenced ", 1).into_bytes(),
        )
        .await
        .unwrap();
        let (out, code) = run(&c, None, v(0, 9), true).await;
        assert_eq!(code, 0, "{out}");
        assert!(out.contains("stopped nodes still name an older version: cut (0.8.1)"));
        // No app at all.
        let empty = bucket();
        let mut say = |_: String| {};
        let refused = finalize(&empty, None, v(0, 9), true, Duration::ZERO, &mut say)
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("nothing to finalize"));
    }

    /// The offline finalize (ADR 0043, decided on 2026-11-27): with no
    /// node of an app live, scoped and unscoped, the floor is raised only
    /// where every lease of the app is stopped and names the target or a
    /// later version. Every other app is named with its reason, and the
    /// command fails as a whole.
    #[tokio::test]
    async fn finalize_without_a_live_node_raises_every_app_it_can() {
        let b = bucket();
        // Every node stopped cleanly after the upgrade to 0.9.
        put(&b, "acme/shop", "a", "0.9.0", "released").await;
        put(&b, "acme/shop", "b", "0.9.1", "released").await;
        // One app's leases still name 0.8, one is mixed and live.
        put(&b, "acme/blog", "c", "0.9.0", "released").await;
        put(&b, "acme/blog", "old", "0.8.1", "released").await;
        put(&b, "acme/wiki", "d", "0.9.0", "live").await;
        put(&b, "acme/wiki", "e", "0.8.1", "live").await;
        // Without --yes: the same answers, nothing written.
        let (out, code) = run(&b, None, v(0, 9), false).await;
        assert_eq!(code, 2, "{out}");
        assert!(
            out.contains("acme/shop: would raise the upgrade floor from none (0.8) to 0.9"),
            "{out}"
        );
        assert!(
            out.contains(
                "acme/blog: would be refused: no node of this app is live, and stopped nodes \
                 name a version older than 0.9: old (0.8.1)"
            ),
            "{out}"
        );
        assert!(
            out.contains("acme/wiki: would be refused: live nodes are older than 0.9: e (0.8.1)"),
            "{out}"
        );
        assert_eq!(floor(&b, "acme/shop").await, None);

        let (out, code) = run(&b, None, v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(out.contains("acme/shop: finalized at 0.9"), "{out}");
        assert!(
            out.contains("error: acme/blog: no node of this app is live")
                && out.contains("Released (old): start the app once on 0.9")
                && out.contains("`wasmkeep cluster forget <id>`"),
            "{out}"
        );
        assert!(
            out.contains("error: acme/wiki: live nodes are older than 0.9: e (0.8.1)"),
            "{out}"
        );
        assert!(
            out.contains(
                "2 of 3 apps refused, nothing written for them: acme/blog, acme/wiki. Each \
                 `error:` line above says why and what to do"
            ),
            "{out}"
        );
        assert!(floor(&b, "acme/shop").await.is_some());
        assert_eq!(floor(&b, "acme/blog").await, None);
        assert_eq!(floor(&b, "acme/wiki").await, None);

        // Scoped: refused the same way while the older lease is there,
        // with no summary line; once it is forgotten, raised alone.
        let (out, code) = run(&b, Some("acme/blog"), v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains(
                "error: acme/blog: no node of this app is live, and stopped nodes name a \
                 version older than 0.9: old (0.8.1)"
            ),
            "{out}"
        );
        assert!(!out.contains("apps refused"), "{out}");
        assert_eq!(floor(&b, "acme/blog").await, None);
        b.delete("nodes/old").await.unwrap();
        let (out, code) = run(&b, Some("acme/blog"), v(0, 9), true).await;
        assert_eq!(code, 0, "{out}");
        assert!(out.contains("acme/blog: finalized at 0.9"), "{out}");
        assert!(!out.contains("apps refused"), "{out}");
        assert!(floor(&b, "acme/blog").await.is_some());

        // An object under `nodes/` that does not decode refuses it,
        // scoped and unscoped, and the summary is for several apps only.
        let c = bucket();
        put(&c, "acme/shop", "a", "0.9.0", "released").await;
        c.put_body("nodes/junk", b"junk".to_vec()).await.unwrap();
        for only in [Some("acme/shop"), None] {
            let (out, code) = run(&c, only, v(0, 9), true).await;
            assert_eq!(code, 1, "{out}");
            assert!(
                out.contains(
                    "error: acme/shop: no node of this app is live, and objects under `nodes/` \
                     do not decode: nodes/junk"
                ),
                "{out}"
            );
            assert!(!out.contains("apps refused"), "{out}");
        }
        assert_eq!(floor(&c, "acme/shop").await, None);
        c.delete("nodes/junk").await.unwrap();
        let (out, code) = run(&c, None, v(0, 9), true).await;
        assert_eq!(code, 0, "{out}");
        assert!(out.contains("acme/shop: finalized at 0.9"), "{out}");

        // Beside a live node the write does not look at an object that
        // does not decode; the listing after it does: the floor is written
        // and stays, and the command fails, naming it.
        let d = bucket();
        put(&d, "acme/shop", "a", "0.9.0", "live").await;
        d.put_body("nodes/junk", b"junk".to_vec()).await.unwrap();
        let (out, code) = run(&d, None, v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains(
                "error: acme/shop: objects under `nodes/` do not decode at the listing after \
                 the write: nodes/junk"
            ) && !out.contains("acme/shop: finalized at 0.9"),
            "{out}"
        );
        assert!(floor(&d, "acme/shop").await.is_some(), "the floor stays");

        // An app named that has no lease: nothing proves its version.
        let (out, code) = run(&c, Some("acme/none"), v(0, 9), true).await;
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains("error: acme/none: no node of this app is live or registered"),
            "{out}"
        );
        assert_eq!(floor(&c, "acme/none").await, None);
    }

    /// `upgrade status`: versions, the floor, one state per app, what
    /// blocks a finalize, and the exit code of the worst state.
    #[tokio::test]
    async fn status_shows_versions_the_floor_and_one_state() {
        let b = bucket();
        put(&b, "acme/shop", "a", "0.9.0", "live").await;
        put(&b, "acme/shop", "b", "0.8.1", "live").await;
        put(&b, "acme/blog", "c", "0.9.0", "live").await;
        let read = async |only: Option<&str>| {
            upgrade::read_statuses(&b, only, crate::status::now_ms(), horizon_ms())
                .await
                .unwrap()
        };
        let apps = read(None).await;
        assert_eq!(status_exit(&apps), 3);
        let text = status_text("s3://b", &apps, v(0, 9));
        assert_eq!(
            text,
            "Upgrade of s3://b (this wasmkeep: 0.9)\n\
             \n\
             acme/blog\n\
             \x20 floor: none (0.8)\n\
             \x20 state: finalize pending\n\
             \x20 every live node runs 0.9 and the floor is 0.8: run `wasmkeep cluster upgrade \
             finalize` to end the upgrade (no node of an older version can start after it)\n\
             \x20 NODE  VERSION  LIVE\n\
             \x20 c     0.9.0    yes\n\
             \n\
             acme/shop\n\
             \x20 floor: none (0.8)\n\
             \x20 state: mixed\n\
             \x20 b (0.8.1) on 0.8, a (0.9.0) on 0.9\n\
             \x20 blocks a finalize: b\n\
             \x20 NODE  VERSION  LIVE\n\
             \x20 a     0.9.0    yes\n\
             \x20 b     0.8.1    yes\n\
             \n"
        );
        let json = status_json(&apps, v(0, 9));
        assert_eq!(json["exit_code"], 3);
        assert_eq!(json["apps"][0]["state"], "finalize pending");
        assert_eq!(json["apps"][0]["finalize_pending"], true);
        assert_eq!(json["apps"][0]["floor"], Value::Null);
        assert_eq!(json["apps"][0]["floor_in_force"], "0.8");
        assert_eq!(json["apps"][1]["blocks"], json!(["b"]));
        assert_eq!(json["apps"][1]["nodes"][1]["minor"], "0.8");

        // Finalized: one version, exit 0. A node below the floor: 1.
        put(&b, "acme/shop", "b", "0.9.0", "live").await;
        for app in ["shop", "blog"] {
            upgrade::finalize_write(
                &b,
                "acme",
                app,
                v(0, 9),
                crate::status::now_ms(),
                horizon_ms(),
            )
            .await
            .unwrap();
        }
        let apps = read(None).await;
        assert_eq!(status_exit(&apps), 0);
        assert!(status_text("s3://b", &apps, v(0, 9)).contains("state: one version"));
        put(&b, "acme/shop", "late", "0.8.1", "live").await;
        let apps = read(Some("acme/shop")).await;
        assert_eq!(status_exit(&apps), 1);
        let text = status_text("s3://b", &apps, v(0, 9));
        assert!(text.contains("state: node below the floor"), "{text}");
        assert!(text.contains("keeps checking without serving"), "{text}");
        // A reader older than the cluster warns and goes on.
        let text = status_text("s3://b", &apps, v(0, 8));
        assert!(text.contains("warning: this `wasmkeep` (0.8) is older than the cluster"));
        // Nothing registered.
        assert_eq!(status_exit(&[]), 3);
        // An app named that has no node: no live node, exit 3.
        let apps = read(Some("acme/none")).await;
        assert_eq!(apps[0].state, State::NoLiveNode);
        assert_eq!(status_exit(&apps), 3);
    }
}
