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
        tracing::info!("listening on http://{addr}");
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

/// Latest modification time and file count under some paths.
type Fingerprint = (Option<SystemTime>, usize);

/// Latest modification time and file count under `paths` (files or
/// directories, recursively): a cheap change detector.
fn fingerprint<'a>(paths: impl IntoIterator<Item = &'a Path>) -> Fingerprint {
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

/// Each item's `watch` fingerprint.
fn watched(project: &Project) -> Vec<Fingerprint> {
    project::items(project)
        .into_iter()
        .map(|(_, item)| fingerprint(item.watch.iter().map(PathBuf::as_path)))
        .collect()
}

/// What the watcher keeps between polls: each item's `watch` fingerprint
/// as it was when its build started, and the items whose last build
/// failed.
struct Builds {
    seen: Vec<Fingerprint>,
    failed: std::collections::BTreeSet<usize>,
}

impl Builds {
    fn new(project: &Project) -> Self {
        Self {
            seen: watched(project),
            failed: std::collections::BTreeSet::new(),
        }
    }

    /// Runs `build` for each item whose `watch` paths changed since its
    /// last build started: an edit saved while a build runs is a change
    /// for the next poll, so it is built too (a build must not write under
    /// its own `watch` paths). Returns whether every item's last build
    /// succeeded: a failure holds the reload until that item builds, so a
    /// mix of old and new parts never loads.
    fn poll(
        &mut self,
        project: &Project,
        mut build: impl FnMut(&str, &str) -> anyhow::Result<()>,
    ) -> bool {
        let now = watched(project);
        for (i, (what, item)) in project::items(project).into_iter().enumerate() {
            if item.watch.is_empty() || now[i] == self.seen[i] {
                continue;
            }
            self.seen[i] = now[i];
            let Some(cmd) = &item.build else {
                continue;
            };
            tracing::info!("change detected in {what}, rebuilding");
            match build(&what, cmd) {
                Ok(()) => {
                    self.failed.remove(&i);
                }
                Err(e) => {
                    self.failed.insert(i);
                    tracing::warn!(error = %format!("{e:#}"), "build failed; still serving the previous version");
                }
            }
        }
        self.failed.is_empty()
    }
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
    let mut builds = Builds::new(&project);
    let mut seen_inputs = fingerprint(inputs(&project).iter().map(PathBuf::as_path));
    let requires = requires(&project);
    let mut seq = 1;
    loop {
        tokio::time::sleep(poll).await;
        let built = tokio::task::block_in_place(|| {
            builds.poll(&project, |what, cmd| {
                project::run_build(&project.dir, what, cmd)
            })
        });
        // Not seen while a build has failed: the reload comes once every
        // changed item builds.
        let inputs_now = fingerprint(inputs(&project).iter().map(PathBuf::as_path));
        if inputs_now == seen_inputs || !built {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Two items watching `a/` and `b/`, each with a build.
    fn two_items(dir: &Path) -> Project {
        for part in ["a", "b"] {
            std::fs::create_dir_all(dir.join(part)).unwrap();
            std::fs::write(dir.join(part).join("src"), "1").unwrap();
        }
        let text = "[app]\ntenant='t'\nname='n'\n\
                    [classes.a]\ncomponent='a.wasm'\nbuild='make a'\nwatch=['a']\n\
                    [classes.b]\ncomponent='b.wasm'\nbuild='make b'\nwatch=['b']\n";
        project::parse(text, &dir.join(project::FILE_NAME)).unwrap()
    }

    /// A later modification time than any before (coarse clocks too).
    fn touch(path: &Path, n: u64) {
        std::fs::write(path, n.to_string()).unwrap();
        let t = SystemTime::now() + Duration::from_secs(10 * n);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(t)
            .unwrap();
    }

    #[test]
    fn a_change_under_watch_builds_and_an_edit_during_the_build_builds_again() {
        let dir = tempfile::tempdir().unwrap();
        let p = two_items(dir.path());
        let mut builds = Builds::new(&p);
        let mut ran = Vec::new();
        assert!(builds.poll(&p, |w, _| {
            ran.push(w.to_owned());
            Ok(())
        }));
        assert!(ran.is_empty(), "nothing changed: {ran:?}");

        // An edit, and another saved while its build runs.
        touch(&dir.path().join("a/src"), 1);
        let src = dir.path().join("a/src");
        assert!(builds.poll(&p, |w, _| {
            ran.push(w.to_owned());
            touch(&src, 2);
            Ok(())
        }));
        assert_eq!(ran, ["class a"]);
        assert!(builds.poll(&p, |w, _| {
            ran.push(w.to_owned());
            Ok(())
        }));
        assert_eq!(ran, ["class a", "class a"], "the edit during the build");
        assert!(builds.poll(&p, |w, _| {
            ran.push(w.to_owned());
            Ok(())
        }));
        assert_eq!(ran.len(), 2, "nothing more changed: {ran:?}");
    }

    #[test]
    fn a_failed_build_holds_the_reload_until_it_builds() {
        let dir = tempfile::tempdir().unwrap();
        let p = two_items(dir.path());
        let mut builds = Builds::new(&p);
        touch(&dir.path().join("a/src"), 1);
        touch(&dir.path().join("b/src"), 1);
        let ok = builds.poll(&p, |w, _| {
            if w == "class a" {
                anyhow::bail!("broken")
            }
            Ok(())
        });
        assert!(!ok, "a failed");
        // Nothing else changes: still held (b alone would load a mix).
        assert!(!builds.poll(&p, |_, _| Ok(())));
        // b changes and builds: still held, a has not built.
        touch(&dir.path().join("b/src"), 2);
        assert!(!builds.poll(&p, |_, _| Ok(())));
        // a fixed: the reload may go.
        touch(&dir.path().join("a/src"), 2);
        let mut ran = Vec::new();
        assert!(builds.poll(&p, |w, _| {
            ran.push(w.to_owned());
            Ok(())
        }));
        assert_eq!(ran, ["class a"]);
    }
}
