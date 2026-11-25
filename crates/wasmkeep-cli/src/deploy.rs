//! `wasmkeep deploy`: its arguments and the command that uploads and activates a version.

use super::*;

#[derive(Debug, Args)]
pub(super) struct DeployArgs {
    /// The app, as TENANT/APP. Required without a project file; with one,
    /// it may be given only if it names the file's app.
    #[arg(long, value_parser = parse_app)]
    app: Option<(String, String)>,

    /// The project file (ADR 0026): a wasmkeep.toml, or a directory holding
    /// one. Without it, ./wasmkeep.toml is read if there is one. The deploy
    /// is then exactly the file: its classes (others are removed), every
    /// class's rule (internal by default), configuration and front.
    #[arg(long, value_name = "PATH", conflicts_with = "no_project")]
    project: Option<PathBuf>,

    /// Ignore ./wasmkeep.toml: deploy with flags only.
    #[arg(long)]
    no_project: bool,

    /// With a project file, do not run its build commands (components
    /// built in an earlier step).
    #[arg(long)]
    no_build: bool,

    /// Bucket shared by the cluster: s3://BUCKET[/PREFIX] (credentials from
    /// the AWS_* environment variables).
    #[arg(long)]
    bucket: String,

    /// Actor class to deploy, as CLASS=PATH. Repeatable; together they are
    /// the new version's classes (others are removed). Without any, the
    /// current classes are kept (copied inside the bucket).
    #[arg(long = "actor", value_name = "CLASS=PATH", value_parser = parse_actor)]
    actors: Vec<(String, PathBuf)>,

    /// Who may reach a class from outside, as CLASS=MODE (`open`,
    /// `existing`, `existing:NAME,...`, `internal`; see `serve --access`).
    /// Travels with the version to every node. Classes not named keep
    /// their rule; CLASS=open removes it. Repeatable.
    #[arg(long = "access", value_name = "CLASS=MODE", value_parser = parse_access)]
    access: Vec<(String, String)>,

    /// Plain configuration for a class, as CLASS=FILE (`NAME=value` lines,
    /// read with `env.get`). Stored in the bucket in the clear: never
    /// secrets (keep those with `wasmkeep secrets set`). Replaces the class's
    /// configuration; classes not named keep theirs; CLASS= removes it.
    /// Repeatable.
    #[arg(long = "config", value_name = "CLASS=FILE", value_parser = parse_actor)]
    config: Vec<(String, PathBuf)>,

    /// Names a class requires among its values, as CLASS=NAME,NAME... (ADR
    /// 0016): while one is not set (by the configuration, the app's secrets
    /// or a node's --env), the class refuses its events with 503 instead of
    /// running without it. Part of the version; classes not named keep
    /// theirs; CLASS= removes them.
    #[arg(long = "requires", value_name = "CLASS=NAMES", value_parser = parse_requires)]
    requires: Vec<(String, std::collections::BTreeSet<String>)>,

    /// The app's front (ADR 0023): a stateless `wasi:http` component
    /// (.wasm exporting wasi:http/incoming-handler) that receives every
    /// request outside /actor/ and calls the app's actors. Without --front
    /// or --no-front, the current front is kept.
    #[arg(long, value_name = "PATH", conflicts_with = "no_front")]
    front: Option<PathBuf>,

    /// Remove the front (with its configuration and required names):
    /// requests outside /actor/ then answer 404.
    #[arg(long)]
    no_front: bool,

    /// The front's plain configuration, as --config is a class's (stored in
    /// the clear: never secrets). An empty FILE ('') removes it.
    #[arg(long, value_name = "FILE", value_parser = parse_path)]
    front_config: Option<PathBuf>,

    /// Names the front requires among its values, as --requires is a
    /// class's: while one is not set, the front answers 503. An empty list
    /// ('') removes them.
    #[arg(long, value_name = "NAMES", value_parser = parse_names)]
    pub(super) front_requires: Option<std::collections::BTreeSet<String>>,

    /// Static files to deploy (ADR 0030).
    #[arg(long, conflicts_with = "no_assets")]
    assets: Option<PathBuf>,
    /// Remove static assets from the new version.
    #[arg(long)]
    no_assets: bool,
    #[arg(long, requires = "assets", value_parser = ["front", "spa", "404"])]
    assets_not_found: Option<String>,
    #[arg(long, requires = "assets", value_delimiter = ',')]
    assets_front_first: Vec<String>,
    #[arg(long, requires = "assets", value_delimiter = ',')]
    assets_immutable: Vec<String>,
    /// Download asset objects to detect and repair same-size corruption.
    #[arg(long, conflicts_with = "no_assets")]
    assets_verify: bool,
    /// Confirm a changed deploy with assets without an interactive prompt.
    #[arg(long)]
    yes: bool,

    /// Go back to the version deployed before the current one, with its
    /// access rules, configuration and front.
    #[arg(
        long,
        conflicts_with_all = [
            "actors", "access", "config", "requires",
            "front", "no_front", "front_config", "front_requires",
            "expire_after_idle", "assets", "no_assets", "assets_not_found",
            "assets_front_first", "assets_immutable", "assets_verify",
        ]
    )]
    rollback: bool,

    /// Let a rollback make a class more open (refused otherwise).
    #[arg(long, requires = "rollback")]
    allow_reopen: bool,

    /// How long a class's actors may stay idle before they are deleted, as
    /// CLASS=DURATION (`7d`, `36h`, `90m`; at least 1h) (ADR 0028): in a
    /// cluster, an actor with no event for that long, no pending alarm
    /// and no open WebSocket is deleted. Part of the version; classes not
    /// named keep theirs; CLASS= removes it. Repeatable.
    #[arg(
        long = "expire-after-idle",
        value_name = "CLASS=DURATION",
        value_parser = parse_expire
    )]
    pub(super) expire_after_idle: Vec<(String, Option<u64>)>,

    /// How a class's plain writes reach the bucket, as CLASS=MODE: `strict`
    /// (each event's writes before the next event, the default) or
    /// `batched` (several events to a segment, ADR 0034). Part of the version; classes not named keep
    /// theirs. Repeatable.
    #[arg(long = "writes", value_name = "CLASS=MODE", value_parser = parse_writes)]
    pub(super) writes: Vec<(String, wasmkeep_server::apps::Writes)>,

    /// Deploy to an app deleted with `wasmkeep app delete`: its tombstone
    /// is removed first, only if nothing else of it is left in the bucket.
    #[arg(long, conflicts_with = "rollback")]
    recreate: bool,

    /// Seconds an old version stays in the bucket before this command may
    /// delete it (ADR 0012). The current version and the five before it are
    /// always kept, for rollbacks; 0 deletes older ones at once.
    #[arg(long, value_name = "SECS", default_value_t = 3600)]
    pub(super) version_retention_secs: u64,
}

/// What a deploy prints after it switches: the changes of a flag deploy
/// or rollback (a deploy from the file printed its plan before the
/// switch), and nothing for one that changed nothing (its last line says
/// so).
pub(crate) fn change_lines(
    deployed: &wasmkeep_server::apps::Deployed,
    from_file: bool,
) -> Vec<String> {
    if from_file || deployed.unchanged {
        return Vec::new();
    }
    deployed
        .diff
        .lines()
        .into_iter()
        .map(|l| format!("  {l}"))
        .collect()
}

/// The warning for a deploy or rollback that removes the front while the
/// app has secrets (`has_secrets`: `None` when the bucket did not say):
/// `_front` entries stay in the set, where a node older than the front
/// cannot open it, and a later front would read them (ADR 0023). The set
/// is sealed, so whether it has such entries is only known with the key:
/// the warning gives the commands to list and remove them, complete but
/// for the key file and the names.
pub(super) fn front_secrets_warning(
    diff: &wasmkeep_server::apps::Diff,
    has_secrets: Option<bool>,
    bucket: &str,
    tenant: &str,
    app: &str,
) -> Option<String> {
    (diff.front == Some("removed") && has_secrets != Some(false)).then(|| {
        let at = format!("--bucket {bucket} --app {tenant}/{app} --key KEY_FILE");
        format!(
            "the front was removed, but any secrets it had stay in the app's set. List them \
             with `wasmkeep secrets list {at}` (KEY_FILE: the app's secrets key), then \
             remove the names shown under `{FRONT_SCOPE}` with `wasmkeep secrets unset {at} \
             --front NAME...`. While they stay, nodes older than the front cannot open any of \
             the app's secrets (they refuse every event and drain), and a later front reads them"
        )
    })
}

/// What `wasmkeep deploy --rollback` prints above the line it ends with,
/// on stdout: the version it rolled back to, what changed, and, from a
/// project file, that the file may now describe another version.
pub(super) fn rollback_lines(
    tenant: &str,
    name: &str,
    deployed: &wasmkeep_server::apps::Deployed,
    file: Option<&str>,
) -> Vec<String> {
    let changes = change_lines(deployed, false);
    let mut out = Vec::new();
    if !changes.is_empty() {
        out.push(format!(
            "rolling back {tenant}/{name} to {}:",
            deployed.current.version
        ));
        out.extend(changes);
    }
    if let Some(file) = file {
        out.push(format!(
            "note: {file} may now describe another version than the one rolled back to; the \
             next deploy from it deploys the file again"
        ));
    }
    out
}

/// The line `wasmkeep deploy` ends with: the version now current and how
/// it got there (a deploy, a rollback from the version before, or nothing
/// changed, with the deploy sequence as it was).
pub(super) fn deployed_line(
    tenant: &str,
    name: &str,
    deployed: &wasmkeep_server::apps::Deployed,
    rollback: bool,
) -> String {
    let current = &deployed.current;
    if deployed.unchanged && deployed.diff.assets.is_some() {
        return if deployed.repaired == 0 {
            format!("{tenant}/{name}: unchanged")
        } else {
            format!("{tenant}/{name}: unchanged, {} repaired", deployed.repaired)
        };
    }
    match (&deployed.previous, rollback, deployed.unchanged) {
        (_, _, true) => format!(
            "{tenant}/{name}: nothing changed: version {} is already current (deploy {})",
            current.version, current.seq
        ),
        (Some(from), true, false) => format!(
            "rolled back {tenant}/{name}: {from} -> {} (deploy {})",
            current.version, current.seq
        ),
        _ => format!(
            "{tenant}/{name}: version {} is current (deploy {})",
            current.version, current.seq
        ),
    }
}

/// Refuses the front's scope where a deploy flag takes classes: the
/// front's values have their own flags, which would otherwise win over
/// these without a word (ADR 0023).
pub(super) fn refuse_front_scope(args: &DeployArgs) -> anyhow::Result<()> {
    fn named<T>(flags: &[(String, T)]) -> bool {
        flags.iter().any(|(c, _)| c == FRONT_SCOPE)
    }
    anyhow::ensure!(
        !named(&args.config),
        "--config {FRONT_SCOPE}=...: --config takes classes; give the front's configuration \
         with --front-config FILE"
    );
    anyhow::ensure!(
        !named(&args.requires),
        "--requires {FRONT_SCOPE}=...: --requires takes classes; give the front's required \
         names with --front-requires NAMES"
    );
    anyhow::ensure!(
        !named(&args.access),
        "--access {FRONT_SCOPE}=...: --access takes classes; the front has no access rule \
         (it receives every request outside /actor/)"
    );
    Ok(())
}

/// The flags of `wasmkeep deploy` that say what the version is, as typed,
/// among those given: refused beside a project file (ADR 0026).
pub(super) fn version_flags(args: &DeployArgs) -> Vec<&'static str> {
    [
        ("--assets", args.assets.is_some()),
        ("--no-assets", args.no_assets),
        ("--assets-not-found", args.assets_not_found.is_some()),
        ("--assets-front-first", !args.assets_front_first.is_empty()),
        ("--assets-immutable", !args.assets_immutable.is_empty()),
        ("--actor", !args.actors.is_empty()),
        ("--access", !args.access.is_empty()),
        ("--config", !args.config.is_empty()),
        ("--requires", !args.requires.is_empty()),
        ("--front", args.front.is_some()),
        ("--no-front", args.no_front),
        ("--front-config", args.front_config.is_some()),
        ("--front-requires", args.front_requires.is_some()),
        ("--expire-after-idle", !args.expire_after_idle.is_empty()),
        ("--writes", !args.writes.is_empty()),
    ]
    .into_iter()
    .filter_map(|(flag, given)| given.then_some(flag))
    .collect()
}

pub(super) fn asset_plan(
    diff: &wasmkeep_server::apps::Diff,
    scan: Option<&assets::Scan>,
    yes: bool,
) -> bool {
    if let Some(scan) = scan {
        scan.print();
    }
    if let Some(change) = diff.assets {
        println!("  assets: {change}");
    }
    if diff.assets.is_none() || diff.assets == Some("removed") || yes {
        return true;
    }
    use std::io::Write;
    eprint!("Deploy these assets? [y/N] ");
    let _ = std::io::stderr().flush();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).is_ok() && matches!(answer.trim(), "y" | "Y" | "yes")
}

/// `wasmkeep deploy`: validate, upload, switch (or roll back).
pub(super) fn deploy(args: DeployArgs) -> anyhow::Result<()> {
    let project = project::discover(args.project.as_deref(), args.no_project, args.rollback)?;
    anyhow::ensure!(
        project.is_some() || !args.no_build,
        "--no-build: there is no project file, so nothing to build"
    );
    // A mistyped bucket fails before a build runs.
    parse_bucket(&args.bucket)?;
    // Builds run and files are read before the bucket is opened: a failed
    // build writes nothing. A rollback runs no build (ADR 0026).
    let mut file_changes = None;
    let mut asset_scan = None;
    let app = match &project {
        Some(p) => {
            project::refuse_flags(p, &version_flags(&args))?;
            let app = project::app(p, args.app.as_ref())?;
            if !args.rollback {
                if !args.no_build {
                    project::run_builds(p)?;
                }
                let parts = project::read_parts(p, &mut |w| eprintln!("warning: {w}"))?;
                let mut changes = project::to_changes(p, parts);
                if let Some(assets) = &p.assets {
                    let scan = assets::scan(&assets.dir, assets.rules.clone())?;
                    changes.assets = wasmkeep_server::assets::Change::Set(scan.bundle.clone());
                    asset_scan = Some(scan);
                }
                changes.assets_verify = args.assets_verify;
                file_changes = Some(changes);
            }
            app
        }
        None => args.app.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "no project file ({} in this directory, or --project PATH): give --app TENANT/APP",
                project::FILE_NAME
            )
        })?,
    };
    if let Some(dir) = &args.assets {
        asset_scan = Some(assets::scan(
            dir,
            wasmkeep_server::assets::Index {
                not_found: args
                    .assets_not_found
                    .clone()
                    .unwrap_or_else(|| "front".into()),
                front_first: args.assets_front_first.iter().cloned().collect(),
                immutable: args.assets_immutable.iter().cloned().collect(),
                ..Default::default()
            },
        )?);
    }
    refuse_front_scope(&args)?;
    let (tenant, name) = &app;
    let (store, prefix) = open_bucket(&args.bucket)?;
    let app_ref = AppRef::new(std::sync::Arc::clone(&store), &prefix, tenant, name);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let named = |e: wasmkeep_server::apps::DeployError| {
        anyhow::anyhow!(e.to_string().replace("<this bucket>", &args.bucket))
    };
    let from_file = file_changes.is_some();
    if args.recreate
        && rt
            .block_on(app_ref.recreate(wasmkeep_server::apps::LATE_WRITES))
            .map_err(named)?
    {
        println!("{tenant}/{name}: the deleted app's tombstone is removed; deploying it anew");
    }
    let deployed = if args.rollback {
        rt.block_on(app_ref.rollback(args.allow_reopen))
            .map_err(named)?
    } else if let (Some(changes), Some(p)) = (file_changes, &project) {
        let runtime = Runtime::new(&RuntimeConfig::default())?;
        let shown = p.path.display().to_string();
        rt.block_on(app_ref.deploy_changes_with(&runtime, changes, |diff| {
            project::print_plan(&shown, tenant, name, diff);
            asset_plan(diff, asset_scan.as_ref(), args.yes)
        }))
        .map_err(named)?
    } else {
        anyhow::ensure!(
            !(args.actors.is_empty()
                && args.access.is_empty()
                && args.config.is_empty()
                && args.requires.is_empty()
                && args.front.is_none()
                && !args.no_front
                && args.front_config.is_none()
                && args.front_requires.is_none()
                && args.expire_after_idle.is_empty()
                && args.writes.is_empty()
                && args.assets.is_none()
                && !args.no_assets
                && !args.assets_verify),
            "nothing to deploy: give --actor, --access, --config, --requires, \
             --expire-after-idle, --writes or a front flag (--front, --no-front, --front-config, \
             --front-requires), or --rollback"
        );
        let runtime = Runtime::new(&RuntimeConfig::default())?;
        let mut classes = Vec::new();
        for (class, path) in &args.actors {
            let wasm =
                std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
            classes.push((class.clone(), wasm));
        }
        let mut config = Vec::new();
        for (class, path) in &args.config {
            if path.as_os_str().is_empty() {
                config.push((class.clone(), None));
                continue;
            }
            let vars = wasmkeep_server::env::read_config_file(path)
                .map_err(|e| anyhow::anyhow!("--config: {e}"))?;
            for warning in secret_looking(class, &vars) {
                eprintln!("warning: {warning}");
            }
            config.push((class.clone(), Some(vars)));
        }
        let front = match (&args.front, args.no_front) {
            (Some(path), _) => wasmkeep_server::apps::FrontChange::Set(
                std::fs::read(path).with_context(|| format!("reading {}", path.display()))?,
            ),
            (None, true) => wasmkeep_server::apps::FrontChange::Remove,
            (None, false) => wasmkeep_server::apps::FrontChange::Keep,
        };
        let front_config = match &args.front_config {
            None => None,
            Some(path) if path.as_os_str().is_empty() => Some(None),
            Some(path) => {
                let vars = wasmkeep_server::env::read_config_file(path)
                    .map_err(|e| anyhow::anyhow!("--front-config: {e}"))?;
                for warning in secret_looking(FRONT_SCOPE, &vars) {
                    let warning =
                        warning.replacen(&format!("--config {FRONT_SCOPE}:"), "--front-config:", 1);
                    eprintln!("warning: {warning}");
                }
                Some(Some(vars))
            }
        };
        let changes = wasmkeep_server::apps::Changes {
            // Kept: only the project file sets them (`apart-from`,
            // `spread`).
            apart: Vec::new(),
            spread: Vec::new(),
            assets: match &asset_scan {
                Some(scan) => wasmkeep_server::assets::Change::Set(scan.bundle.clone()),
                None if args.no_assets => wasmkeep_server::assets::Change::Remove,
                None => wasmkeep_server::assets::Change::Keep,
            },
            assets_verify: args.assets_verify,
            limits: None,
            front,
            front_config,
            front_requires: args.front_requires.clone(),
            classes: (!classes.is_empty()).then_some(classes),
            access: args.access.clone(),
            config,
            requires: {
                let mut seen = std::collections::BTreeSet::new();
                for (class, _) in &args.requires {
                    anyhow::ensure!(
                        seen.insert(class.clone()),
                        "--requires names {class} twice: give its names once, as {class}=A,B"
                    );
                }
                args.requires.clone()
            },
            expire: {
                let mut seen = std::collections::BTreeSet::new();
                for (class, _) in &args.expire_after_idle {
                    anyhow::ensure!(
                        seen.insert(class.clone()),
                        "--expire-after-idle names {class} twice"
                    );
                }
                args.expire_after_idle.clone()
            },
            writes: {
                let mut seen = std::collections::BTreeSet::new();
                for (class, _) in &args.writes {
                    anyhow::ensure!(seen.insert(class.clone()), "--writes names {class} twice");
                }
                args.writes.clone()
            },
            every_rule: false,
        };
        rt.block_on(app_ref.deploy_changes_with(&runtime, changes, |diff| {
            asset_plan(diff, asset_scan.as_ref(), args.yes)
        }))
        .map_err(named)?
    };
    // From the file, the plan (and its warnings) was printed before the
    // switch; a deploy that changes nothing says so on its last line.
    let lines = if args.rollback {
        let file = project.as_ref().map(|p| p.path.display().to_string());
        rollback_lines(tenant, name, &deployed, file.as_deref())
    } else {
        change_lines(&deployed, from_file)
    };
    for line in lines {
        println!("{line}");
    }
    if !from_file {
        for warning in deploy_warnings(&deployed.diff, None) {
            eprintln!("warning: {warning}");
        }
    }
    if deployed.diff.front == Some("removed") {
        let secrets = wasmkeep_server::secret_store::SecretStore::new(store, &prefix, tenant, name);
        let has_secrets = rt.block_on(secrets.current()).ok().map(|c| c.is_some());
        if let Some(warning) =
            front_secrets_warning(&deployed.diff, has_secrets, &args.bucket, tenant, name)
        {
            eprintln!("warning: {warning}");
        }
    }
    if deployed.newly_marked {
        println!(
            "Access rules now come with deploys: nodes ignore --access from now on, \
             and refuse it at their next start. Every class not listed is open."
        );
    }
    println!("{}", deployed_line(tenant, name, &deployed, args.rollback));
    // Old versions beyond what rollbacks need (ADR 0012). The deploy is
    // done either way: a failure here only leaves them for next time.
    let retention = std::time::Duration::from_secs(args.version_retention_secs);
    match rt.block_on(app_ref.collect(wasmkeep_server::apps::KEEP_PREVIOUS, retention)) {
        Ok((0, 0)) => {}
        Ok((versions, assets)) => println!(
            "removed {versions} old version(s) and {assets} asset object(s) from the bucket"
        ),
        Err(e) => {
            eprintln!("warning: collection failed (the next deploy tries again): {e}")
        }
    }
    Ok(())
}

/// `CLASS=NAME,NAME...` (empty after `=` removes the class's names).
/// A path that may be empty (`--front-config ''` removes the front's
/// configuration).
pub(super) fn parse_path(s: &str) -> Result<PathBuf, String> {
    Ok(PathBuf::from(s))
}

pub(super) fn parse_expire(s: &str) -> Result<(String, Option<u64>), String> {
    let (class, limit) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=DURATION, got {s:?}"))?;
    if class.is_empty() || class == FRONT_SCOPE {
        return Err(format!("expected a class name in {s:?}"));
    }
    if limit.is_empty() {
        return Ok((class.to_owned(), None));
    }
    let secs = wasmkeep_cluster::expiry::parse_idle(limit)?;
    Ok((class.to_owned(), Some(secs)))
}

/// `--front-requires NAMES`: comma-separated names, empty for none.
pub(super) fn parse_names(s: &str) -> Result<std::collections::BTreeSet<String>, String> {
    let mut names = std::collections::BTreeSet::new();
    for name in s.split(',').filter(|n| !n.is_empty()) {
        if !wasmkeep_server::env::valid_name(name) {
            return Err(format!("invalid name {name:?}"));
        }
        if !names.insert(name.to_owned()) {
            return Err(format!("{name} is named twice"));
        }
    }
    Ok(names)
}

pub(super) fn parse_requires(
    s: &str,
) -> Result<(String, std::collections::BTreeSet<String>), String> {
    let (class, names) = s
        .split_once('=')
        .ok_or_else(|| format!("expected CLASS=NAME,NAME..., got {s:?}"))?;
    if class.is_empty() {
        return Err(format!("expected CLASS=NAME,NAME..., got {s:?}"));
    }
    let mut set = std::collections::BTreeSet::new();
    for name in names.split(',').filter(|n| !n.is_empty()) {
        if !wasmkeep_server::env::valid_name(name) {
            return Err(format!("invalid name {name:?} in --requires"));
        }
        set.insert(name.to_owned());
    }
    Ok((class.to_owned(), set))
}
