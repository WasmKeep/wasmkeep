//! `wasmkeep actor`: move and delete (the other subcommands live in their own modules).

use super::*;

#[derive(Debug, Subcommand)]
pub(super) enum ActorCommand {
    /// List retained history without taking ownership.
    History(actor_read::HistoryArgs),
    /// Export a verified, portable SQLite database from bucket history. No node or hold.
    Export(actor_read::ExportArgs),
    /// Run one read-only SQL statement on a temporary, verified actor export.
    Sql(actor_read::SqlArgs),
    /// Move an actor to NODE (ADR 0017): its owner hands it over, or NODE
    /// takes it if nobody holds it, within a few seconds. NODE must serve
    /// the same app and not be draining. The actor is not pinned there: once
    /// idle, its next event may claim it elsewhere. The request expires
    /// after 5 minutes.
    Move(MoveArgs),
    /// Delete an actor (ADR 0028): its owner, or any node serving its
    /// class if nobody holds it, closes its WebSockets with 1000, removes
    /// its data and alarm, and records the delete in the bucket. The next
    /// event to the address starts a new, empty actor. Waits until the
    /// delete is recorded (60 s by default); the request stays for an
    /// hour otherwise. Cannot be undone.
    Delete(DeleteArgs),
    /// Restore historical state as a new write by the current owner.
    Restore(actor_restore::RestoreArgs),
    /// Show which node owns each actor, whether that node is live and since
    /// when (read from the actor's ownership record and the heartbeats; an
    /// actor with no owner is "not placed"). Read-only: it claims nothing
    /// and wakes no actor.
    Where(actor_where::WhereArgs),
}

#[derive(Debug, Args)]
pub(super) struct DeleteArgs {
    /// Resume this operation ID without submitting another request.
    #[arg(long)]
    resume: Option<String>,
    /// The actor: CLASS/NAME with `--app`, or `default/CLASS/NAME` for
    /// the implicit app `default/default` (the name may contain `/`).
    actor: String,

    /// The actor's app, TENANT/APP; `default/default` (classes given on
    /// the command line) when the actor is written `default/CLASS/NAME`.
    #[arg(long, value_parser = parse_app)]
    app: Option<(String, String)>,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// How long to wait for the delete to be recorded, in seconds (at most
    /// 3000: the request expires after an hour).
    #[arg(
        long,
        default_value_t = 60,
        conflicts_with = "no_wait",
        value_parser = clap::value_parser!(u64).range(0..=3000)
    )]
    wait: u64,

    /// Write the request and return at once.
    #[arg(long)]
    no_wait: bool,
}

#[derive(Debug, Args)]
pub(super) struct MoveArgs {
    /// The actor: CLASS/NAME with `--app`, or `default/CLASS/NAME` for
    /// the implicit app `default/default` (the name may contain `/`).
    actor: String,

    /// The actor's app, TENANT/APP; `default/default` (classes given on
    /// the command line) when the actor is written `default/CLASS/NAME`.
    #[arg(long, value_parser = parse_app)]
    app: Option<(String, String)>,

    /// The node to move it to.
    #[arg(long)]
    to: String,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Write the request even though no live node has the id (one about to
    /// start, say).
    #[arg(long)]
    force: bool,
}

/// Writes the actor's move request (ADR 0017), under its app's root.
pub(super) fn actor_move(args: &MoveArgs) -> anyhow::Result<()> {
    actor_move_on(args, || open_bucket(&args.bucket))
}

/// [`actor_move`] on the bucket `open` gives (opened once the request is
/// valid): the start check `serve` runs
/// first (ADR 0022), so that a request for a tenant left in the layout
/// before 0.4.0 is refused (migrate with 0.8.x first) rather than written
/// where no node reads it.
pub(super) fn actor_move_on(
    args: &MoveArgs,
    open: impl FnOnce() -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)>,
) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::{self, Keys as _};
    let ((tenant, app), class, name) = move_target(&args.actor, args.app.as_ref())?;
    if let Some(warning) = move_target_warning(&args.actor, args.app.as_ref()) {
        eprintln!("warning: {warning}");
    }
    let (tenant, class, name) = (&tenant, &class, &name);
    let key = control::checked_move_key(
        &wasmkeep_cluster::state_root::StateRoot::new("", tenant.as_str(), app.as_str()),
        class,
        name,
        &args.to,
    )?;
    let (store, prefix) = open()?;
    check_layout(&args.bucket, &store, &prefix, tenant)?;
    refuse_deleted(&store, &prefix, tenant, &app)?;
    let (app_store, app_prefix) = (std::sync::Arc::clone(&store), prefix.clone());
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let to = &args.to;
    match rt.block_on(live_node(&keys, to))? {
        None if !args.force => anyhow::bail!(
            "no live node has the id {to} (see `wasmkeep cluster status`). Pass --force to \
             write the request anyway (it expires in 5 minutes)"
        ),
        None => {}
        Some(node) => {
            for warning in move_warnings(&node, &format!("{tenant}/{app}")) {
                eprintln!("warning: {warning}");
            }
        }
    }
    rt.block_on(keys.put_empty(&key))?;
    // Deleted meanwhile (ADR 0028): the request is taken back.
    if let Err(e) = refuse_deleted(&app_store, &app_prefix, tenant, &app) {
        let _ = rt.block_on(keys.delete(&key));
        return Err(e);
    }
    announce_intent(&rt, &keys);
    println!(
        "Asked the owner of {tenant}/{class}/{name} to move it to {} (expires in 5 minutes).",
        args.to
    );
    Ok(())
}

/// Writes the actor's delete request (ADR 0028), under its app's root, and
/// waits for a node to carry it out.
pub(super) fn actor_delete(args: &DeleteArgs) -> anyhow::Result<()> {
    actor_delete_on(
        args,
        || open_bucket(&args.bucket),
        std::time::Duration::from_millis(500),
    )
}

/// [`actor_delete`] on the bucket `open` gives, looking every `poll`.
/// Refuses an actor with no history in the bucket (nothing is written),
/// and a tenant left in the layout before 0.4.0. Done once the request's
/// key is gone: a node deletes it only after the delete is recorded.
pub(super) fn actor_delete_on(
    args: &DeleteArgs,
    open: impl FnOnce() -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)>,
    poll: std::time::Duration,
) -> anyhow::Result<()> {
    use wasmkeep_cluster::control::{self, Keys as _};
    let ((tenant, app), class, name) = move_target(&args.actor, args.app.as_ref())?;
    if let Some(warning) = move_target_warning(&args.actor, args.app.as_ref()) {
        eprintln!("warning: {warning}");
    }
    let root = wasmkeep_cluster::state_root::StateRoot::new("", tenant.as_str(), app.as_str());
    let id = args.resume.clone().unwrap_or_else(control::new_request_id);
    let key = control::checked_delete_key(&root, &class, &name, &id)?;
    let (store, prefix) = open()?;
    check_layout(&args.bucket, &store, &prefix, &tenant)?;
    if args.resume.is_none() {
        refuse_deleted(&store, &prefix, &tenant, &app)?;
    }
    let (app_store, app_prefix) = (std::sync::Arc::clone(&store), prefix.clone());
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let actor = format!("{class}/{name} of {tenant}/{app}");
    let history = wasmkeep_server::history_folder(&root, &class, &name);
    // Listed by its folder: a listing of a key itself shows only what is
    // under it.
    let folder = format!("{}/", key.rsplit_once('/').map_or("", |(f, _)| f));
    let listed = |keys: &wasmkeep_cluster::object_store::ObjectStoreBucket| {
        rt.block_on(keys.list_keys(&folder))
    };
    if args.resume.is_none() {
        anyhow::ensure!(
            !rt.block_on(keys.list_keys(&history))?.is_empty(),
            "no such actor: {actor} has nothing in the bucket (nothing was written)"
        );
        rt.block_on(keys.put_empty(&key))?;

        // Deleted meanwhile (ADR 0028): the request is taken back.
        if let Err(e) = refuse_deleted(&app_store, &app_prefix, &tenant, &app) {
            let _ = rt.block_on(keys.delete(&key));
            return Err(e);
        }
        // Nodes list the requests when `control/changed` changes (ADR 0014,
        // amendment of 2026-11-14).
        announce_intent(&rt, &keys);
    }
    println!("Operation ID: {id}");
    let written = listed(&keys)?
        .into_iter()
        .find(|(k, _)| *k == key)
        .map(|(_, at)| at);
    let until = written.map_or_else(
        || "an hour from now".to_owned(),
        |at| {
            format!(
                "{} ms since the epoch, the bucket's clock",
                at + control::DELETE_TTL_MS
            )
        },
    );
    if args.no_wait && args.resume.is_none() {
        println!("Asked the cluster to delete {actor} (the request expires at {until}).");
        return Ok(());
    }
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(if args.no_wait { 0 } else { args.wait });
    loop {
        // The node that carried the request out writes its outcome beside
        // it before deleting it: the key going away alone (it expires)
        // says nothing.
        let now = listed(&keys)?;
        let outcome = now
            .iter()
            .find_map(|(k, _)| control::DeleteOutcome::of(k, &key));
        if let Some(outcome) = outcome {
            match outcome {
                control::DeleteOutcome::Done => {
                    println!("Deleted {actor}: the delete is recorded in the bucket.");
                    return Ok(());
                }
                control::DeleteOutcome::Superseded => anyhow::bail!(
                    "delete {id} superseded: target incarnation changed; nothing deleted"
                ),
                control::DeleteOutcome::Absent => anyhow::bail!(
                    "no such actor: the node found nothing for {actor}; nothing was deleted"
                ),
            }
        }
        if !now.iter().any(|(k, _)| *k == key) {
            anyhow::bail!(
                "the request to delete {actor} was dropped without an outcome (expired by a \
                 node's clock, or not an actor address): nothing is known to be deleted"
            );
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "requested; {actor} is not deleted yet after {} s. The cluster keeps trying \
                 until {until}; resume with --resume {id} (a \
                 node of the app must serve class {class} and be able to hold the actor)",
                args.wait
            );
        }
        std::thread::sleep(poll);
    }
}

/// A warning for `actor move --app TENANT/APP TENANT/CLASS/NAME`, the
/// syntax before `--app` with the tenant kept in front: it names class
/// `TENANT`, which is allowed (a class may share its tenant's name) but
/// most likely a mistake.
pub(super) fn move_target_warning(actor: &str, app: Option<&(String, String)>) -> Option<String> {
    let (tenant, _) = app?;
    let rest = actor.strip_prefix(tenant.as_str())?.strip_prefix('/')?;
    rest.contains('/').then(|| {
        format!(
            "{actor} names class {tenant} of the app: with --app, the actor is CLASS/NAME. \
             Did you mean {rest}?"
        )
    })
}

/// Why a live move target may not take the actor of `app` (`tenant/app`):
/// the request is written anyway (the owner keeps the actor, or another
/// node takes it).
pub(super) fn move_warnings(
    node: &wasmkeep_cluster::membership::NodeInfo,
    app: &str,
) -> Vec<String> {
    use wasmkeep_cluster::membership::DrainState;
    let mut warnings = Vec::new();
    // A node serves one app (`tenant/app`; `default/default` for classes
    // given on the command line); `tenant/` from the version before, empty
    // from nodes that predate both.
    if !node.app.is_empty() && node.app != app {
        warnings.push(format!(
            "{} serves {}, not {app}: it refuses to take the actor",
            node.id, node.app
        ));
    }
    if !node.can_take {
        warnings.push(format!(
            "{} does not take moved actors (an older version): the owner keeps the actor, \
             or another node takes it",
            node.id
        ));
    }
    if node.drain.is_some_and(|d| d != DrainState::Serving) {
        warnings.push(format!(
            "{} is draining: it takes no actors until undrained",
            node.id
        ));
    }
    warnings
}
