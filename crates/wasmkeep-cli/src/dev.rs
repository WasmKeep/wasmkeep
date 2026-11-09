//! `wasmkeep dev`: build actor crates, serve them, rebuild and hot-swap on
//! every source change.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context as _;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

/// One actor crate being developed.
struct Project {
    dir: PathBuf,
    class: String,
    artifact: PathBuf,
}

impl Project {
    fn new(dir: PathBuf) -> anyhow::Result<Self> {
        let manifest = dir.join("Cargo.toml");
        let text = std::fs::read_to_string(&manifest)
            .with_context(|| format!("reading {}", manifest.display()))?;
        let package = package_name(&text)
            .with_context(|| format!("no [package] name in {}", manifest.display()))?;
        Ok(Self {
            class: package.replace('_', "-"),
            artifact: dir
                .join("target/wasm32-wasip2/release")
                .join(format!("{}.wasm", package.replace('-', "_"))),
            dir,
        })
    }

    /// `cargo build --release --target wasm32-wasip2`, output to the
    /// terminal; returns the component bytes.
    fn build(&self) -> anyhow::Result<Vec<u8>> {
        let status =
            std::process::Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
                .current_dir(&self.dir)
                .args(["build", "--release", "--target", "wasm32-wasip2"])
                .status()
                .context("running cargo")?;
        anyhow::ensure!(status.success(), "cargo build failed");
        std::fs::read(&self.artifact)
            .with_context(|| format!("reading {}", self.artifact.display()))
    }

    /// Latest modification time and file count of the sources, as a cheap
    /// change detector (polling: portable, no extra dependency).
    fn fingerprint(&self) -> (Option<SystemTime>, usize) {
        let mut latest = None;
        let mut count = 0;
        let mut visit = |path: &Path| {
            if let Ok(modified) = std::fs::metadata(path).and_then(|m| m.modified()) {
                latest = latest.max(Some(modified));
                count += 1;
            }
        };
        for file in ["Cargo.toml", "Cargo.lock"] {
            visit(&self.dir.join(file));
        }
        let mut stack = vec![self.dir.join("src")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    visit(&path);
                }
            }
        }
        (latest, count)
    }
}

/// The `name` in `[package]`, from the manifest text.
fn package_name(manifest: &str) -> Option<String> {
    let mut in_package = false;
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            in_package = line == "[package]";
        } else if in_package
            && let Some(value) = line.strip_prefix("name").map(str::trim_start)
            && let Some(value) = value.strip_prefix('=')
        {
            return Some(value.trim().trim_matches('"').to_owned());
        }
    }
    None
}

pub(crate) struct DevOptions {
    pub(crate) crates: Vec<PathBuf>,
    pub(crate) listen: std::net::SocketAddr,
    pub(crate) data_dir: PathBuf,
    pub(crate) poll: Duration,
    pub(crate) egress: wasmkeep_server::EgressPolicy,
    pub(crate) guest_log: wasmkeep_server::GuestLog,
    /// Turn transparent huge pages off for this process (Linux).
    pub(crate) no_huge_pages: bool,
    pub(crate) env: Vec<(String, PathBuf)>,
    pub(crate) access: Vec<(String, String)>,
}

pub(crate) fn run(options: DevOptions) -> anyhow::Result<()> {
    if options.no_huge_pages {
        crate::huge_pages(crate::HugePages::Off);
    }
    let projects = options
        .crates
        .into_iter()
        .map(Project::new)
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        !projects.is_empty(),
        "give at least one actor crate directory"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _entered = rt.enter();
    let runtime = Runtime::new(&RuntimeConfig::default())?;

    let mut classes = Vec::new();
    for p in &projects {
        tracing::info!(class = %p.class, dir = %p.dir.display(), "building");
        let wasm = p
            .build()
            .with_context(|| format!("building {}", p.dir.display()))?;
        let component = runtime.compile(&wasm)?;
        classes.push((p.class.clone(), component, wasm));
    }
    let mut config = ActorConfig::new(&options.data_dir);
    config.egress = options.egress.clone();
    config.guest_log = options.guest_log.clone();
    let served: Vec<&str> = projects.iter().map(|p| p.class.as_str()).collect();
    config.env = crate::load_env(&options.env, &served, true)?;
    config.access = crate::access_rules(&options.access, &served)?;
    let service = ActorService::new(runtime, config, classes)?;

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(options.listen)
            .await
            .with_context(|| format!("binding {}", options.listen))?;
        let addr = listener.local_addr()?;
        for p in &projects {
            tracing::info!(
                "serving {} at http://{addr}/actor/{}/<name>/",
                p.class,
                p.class
            );
        }
        tracing::info!("watching for changes (Ctrl-C to stop)");

        let watcher = tokio::spawn(watch(projects, service.clone(), options.poll));
        let served = wasmkeep_server::serve(listener, service, async {
            crate::stop_signal().await;
            tracing::info!("shutting down");
        })
        .await;
        watcher.abort();
        served.map_err(anyhow::Error::from)
    })
}

/// Polls each project; on a change, rebuilds and swaps the class. A failed
/// build keeps the previous version serving.
async fn watch(projects: Vec<Project>, service: ActorService, poll: Duration) {
    let mut seen: Vec<_> = projects.iter().map(Project::fingerprint).collect();
    loop {
        tokio::time::sleep(poll).await;
        for (i, project) in projects.iter().enumerate() {
            let now = project.fingerprint();
            if now == seen[i] {
                continue;
            }
            seen[i] = now;
            tracing::info!(class = %project.class, "change detected, rebuilding");
            let started = Instant::now();
            let dir = project.dir.clone();
            let built = tokio::task::spawn_blocking({
                let project = Project::new(dir);
                move || project.and_then(|p| p.build())
            })
            .await;
            let wasm = match built {
                Ok(Ok(wasm)) => wasm,
                Ok(Err(e)) => {
                    tracing::warn!(class = %project.class, error = %format!("{e:#}"), "build failed; still serving the previous version");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "build task failed");
                    continue;
                }
            };
            let runtime = service.runtime().clone();
            let component = match tokio::task::spawn_blocking(move || {
                runtime.compile(&wasm).map(|c| (c, wasm))
            })
            .await
            {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    tracing::warn!(class = %project.class, error = %e, "not a valid component; still serving the previous version");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "compile task failed");
                    continue;
                }
            };
            match service.replace_class(&project.class, &component.0, &component.1) {
                Ok(()) => tracing::info!(
                    class = %project.class,
                    elapsed_ms = started.elapsed().as_millis(),
                    "reloaded"
                ),
                Err(e) => {
                    tracing::warn!(class = %project.class, error = %e, "rejected; still serving the previous version")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::package_name;

    #[test]
    fn reads_the_package_name() {
        let toml = "[package]\nname = \"chat-room\"\nversion = \"0.1.0\"\n\n[dependencies]\nname = \"x\"\n";
        assert_eq!(package_name(toml).as_deref(), Some("chat-room"));
        assert_eq!(package_name("[workspace]\n[lib]\nname = \"x\"\n"), None);
        assert_eq!(
            package_name("[package]\nname=\"a_b\"\n").as_deref(),
            Some("a_b")
        );
    }
}
