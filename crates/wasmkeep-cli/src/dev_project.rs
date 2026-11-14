//! `wasmkeep dev` with a project file (ADR 0026): the app as a deployed
//! version runs on a node (its front, classes, rules, configuration and
//! required names), with local values from `[dev] env`. Builds at start,
//! then rebuilds what `watch` names and reloads the whole app when a
//! component or configuration file changes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, DeployedFront, HttpComponent};

use crate::project::{self, Project};

pub(crate) struct Options {
    pub(crate) listen: std::net::SocketAddr,
    pub(crate) data_dir: PathBuf,
    pub(crate) poll: Duration,
    pub(crate) egress: wasmkeep_server::EgressPolicy,
    pub(crate) guest_log: wasmkeep_server::GuestLog,
    pub(crate) no_huge_pages: bool,
}

/// The app as compiled from disk: what a reload swaps in.
struct Loaded {
    classes: Vec<(String, wasmtime::component::Component, Vec<u8>)>,
    config: BTreeMap<String, wasmkeep_server::env::Vars>,
    front: Option<DeployedFront>,
}

fn load(runtime: &Runtime, project: &Project) -> anyhow::Result<Loaded> {
    let parts = project::read_parts(project, &mut |w| tracing::warn!("{w}"))?;
    let mut config = parts.config;
    let front_config = config.remove(crate::FRONT_SCOPE).unwrap_or_default();
    let mut classes = Vec::new();
    for (name, wasm) in parts.classes {
        let component = runtime
            .compile(&wasm)
            .with_context(|| format!("compiling class {name}"))?;
        classes.push((name, component, wasm));
    }
    let front = match (parts.front, &project.front) {
        (Some(wasm), Some(item)) => Some(DeployedFront {
            component: runtime.compile(&wasm).context("compiling the front")?,
            config: front_config,
            requires: item.requires.clone(),
        }),
        _ => None,
    };
    Ok(Loaded {
        classes,
        config,
        front,
    })
}

/// The local values (`[dev] env`): read if named, or if the default
/// `.env.local` exists. They reach the front and every class.
fn local_values(project: &Project) -> anyhow::Result<wasmkeep_server::env::Vars> {
    if !project.dev.env_named && !project.dev.env.exists() {
        return Ok(wasmkeep_server::env::Vars::new());
    }
    let (vars, warnings) = wasmkeep_server::env::read_file(&project.dev.env)
        .map_err(|e| anyhow::anyhow!("[dev] env: {e}"))?;
    for w in warnings {
        tracing::warn!("{w}");
    }
    tracing::info!(file = %project.dev.env.display(), values = vars.len(), "local values loaded");
    Ok(vars)
}

fn rules(project: &Project) -> anyhow::Result<wasmkeep_server::AccessRules> {
    let mut rules = wasmkeep_server::AccessRules::default();
    for (name, class) in &project.classes {
        rules
            .replace(name, &class.access)
            .map_err(|e| anyhow::anyhow!("class {name}: {e}"))?;
    }
    Ok(rules)
}

fn requires(project: &Project) -> BTreeMap<String, std::collections::BTreeSet<String>> {
    project
        .classes
        .iter()
        .filter(|(_, c)| !c.item.requires.is_empty())
        .map(|(n, c)| (n.clone(), c.item.requires.clone()))
        .collect()
}

pub(crate) fn run(project: Project, options: Options) -> anyhow::Result<()> {
    if options.no_huge_pages {
        crate::huge_pages(crate::HugePages::Off);
    }
    project::run_builds(&project)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _entered = rt.enter();
    let runtime = Runtime::new(&RuntimeConfig::default())?;
    let loaded = load(&runtime, &project)?;
    let local = local_values(&project)?;
    let rules = rules(&project)?;

    let mut config = ActorConfig::new(&options.data_dir);
    config.tenant = project.tenant.clone();
    config.app = project.name.clone();
    config.egress = options.egress.clone();
    config.guest_log = options.guest_log.clone();
    let mut env = wasmkeep_server::ActorEnv::default();
    for name in project.classes.keys() {
        env.extend(name, local.clone());
    }
    config.env = env;
    config.front_env = local;
    config.access = rules.clone();
    config.config = loaded.config.clone();
    config.requires = requires(&project);
    config.front = loaded.front.clone();
    let pool = usize::try_from(runtime.max_instances()).unwrap_or(usize::MAX);
    let front_requests = wasmkeep_server::fds::default_front_requests(pool).max(1);
    config.max_front_requests = Some(front_requests);
    let service = ActorService::new(runtime, config, loaded.classes)?;
    let front = HttpComponent::deployed(&service)
        .with_egress(options.egress)
        .with_guest_log(options.guest_log)
        .with_max_requests(front_requests);
    let app = wasmkeep_server::App {
        front: Some(front),
        actors: Some(service.clone()),
    };

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(options.listen)
            .await
            .with_context(|| format!("binding {}", options.listen))?;
        let addr = listener.local_addr()?;
        tracing::info!(
            app = %format!("{}/{}", project.tenant, project.name),
            file = %project.path.display(),
            "running the project file"
        );
        if project.front.is_some() {
            tracing::info!("serving the front at http://{addr}/");
        }
        for (name, class) in &project.classes {
            tracing::info!(
                "serving {name} at http://{addr}/actor/{name}/<name>/ (access {})",
                class.access
            );
        }
        tracing::info!(
            "watching for changes (Ctrl-C to stop; restart to read {} or {} again)",
            project::FILE_NAME,
            project.dev.env.display()
        );
        let watcher = tokio::spawn(watch(project, service.clone(), rules, options.poll));
        let served = wasmkeep_server::serve_with(
            listener,
            app,
            async {
                crate::stop_signal().await;
                tracing::info!("shutting down");
            },
            wasmkeep_server::ServeLimits::default(),
        )
        .await;
        watcher.abort();
        served.map_err(anyhow::Error::from)
    })
}

/// Latest modification time and file count under `paths` (files or
/// directories, recursively): a cheap change detector.
fn fingerprint<'a>(paths: impl IntoIterator<Item = &'a Path>) -> (Option<SystemTime>, usize) {
    let mut latest = None;
    let mut count = 0;
    let mut stack: Vec<PathBuf> = paths.into_iter().map(Path::to_path_buf).collect();
    while let Some(path) = stack.pop() {
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if meta.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&path) {
                stack.extend(entries.filter_map(Result::ok).map(|e| e.path()));
            }
        } else if let Ok(modified) = meta.modified() {
            latest = latest.max(Some(modified));
            count += 1;
        }
    }
    (latest, count)
}

/// What a reload reads: every component and configuration file. The local
/// values are read at start only.
fn inputs(project: &Project) -> Vec<PathBuf> {
    project::items(project)
        .into_iter()
        .flat_map(|(_, item)| std::iter::once(item.component.clone()).chain(item.config.clone()))
        .collect()
}

/// Polls each item's `watch` paths (a change runs its build) and the
/// inputs (a change reloads the app). A failed build or reload keeps the
/// previous version serving.
async fn watch(
    project: Project,
    service: ActorService,
    rules: wasmkeep_server::AccessRules,
    poll: Duration,
) {
    let watched = |project: &Project| -> Vec<_> {
        project::items(project)
            .into_iter()
            .map(|(_, item)| fingerprint(item.watch.iter().map(PathBuf::as_path)))
            .collect()
    };
    let mut seen_sources = watched(&project);
    let mut seen_inputs = fingerprint(inputs(&project).iter().map(PathBuf::as_path));
    let requires = requires(&project);
    let mut seq = 1;
    loop {
        tokio::time::sleep(poll).await;
        let now = watched(&project);
        let mut build_failed = false;
        for (i, (what, item)) in project::items(&project).into_iter().enumerate() {
            if item.watch.is_empty() || now[i] == seen_sources[i] {
                continue;
            }
            let Some(cmd) = item.build.clone() else {
                continue;
            };
            tracing::info!("change detected in {what}, rebuilding");
            let dir = project.dir.clone();
            let built =
                tokio::task::spawn_blocking(move || project::run_build(&dir, &what, &cmd)).await;
            match built {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    build_failed = true;
                    tracing::warn!(error = %format!("{e:#}"), "build failed; still serving the previous version");
                }
                Err(e) => tracing::warn!(error = %e, "build task failed"),
            }
        }
        // After the builds, so their own writes are not seen as changes.
        seen_sources = watched(&project);
        let inputs_now = fingerprint(inputs(&project).iter().map(PathBuf::as_path));
        if inputs_now == seen_inputs || build_failed {
            continue;
        }
        seen_inputs = inputs_now;
        let runtime = service.runtime().clone();
        let p = project.clone();
        let loaded = tokio::task::spawn_blocking(move || load(&runtime, &p)).await;
        let loaded = match loaded {
            Ok(Ok(l)) => l,
            Ok(Err(e)) => {
                tracing::warn!(error = %format!("{e:#}"), "not reloaded; still serving the previous version");
                continue;
            }
            Err(e) => {
                tracing::warn!(error = %e, "reload task failed");
                continue;
            }
        };
        seq += 1;
        match service.apply_deployed(
            seq,
            &loaded.classes,
            &rules,
            &loaded.config,
            &requires,
            loaded.front.as_ref(),
        ) {
            Ok(()) => tracing::info!("reloaded"),
            Err((name, e)) => {
                tracing::warn!(class = %name, error = %format!("{e:#}"), "rejected; still serving the previous version");
            }
        }
    }
}
