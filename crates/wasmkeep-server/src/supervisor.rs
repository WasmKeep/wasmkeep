//! The supervisor of a node process (ADR 0038, section 1): it owns the
//! process's listeners and its shutdown, and the [`NodeProcess`] its apps
//! share the transport through.
//!
//! One app ([`Supervisor::serve`]): its tombstone (ADR 0028) or a verdict
//! on its node id (another process writing it, its lease fenced or
//! released) ends the process as before: `serve` shuts down and returns
//! the reason, and the caller exits non-zero with it.
//!
//! Several apps ([`Supervisor::serve_apps`]): the public port routes by
//! hostname, the cluster port by the signed app; a verdict stops that app's
//! runtime alone, which is started again in place, through the same start
//! check, after a backoff of 1 s doubling to 60 s; a tombstone stops it for
//! good. The process ends only when it is stopped.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::cluster::NodeProcess;
use crate::http::{App, ServeLimits};

/// The process's listeners, bound by the caller.
#[derive(Debug)]
pub struct Listeners {
    /// Client HTTP and WebSockets.
    pub public: TcpListener,
    /// Node-to-node calls over mutual TLS (ADR 0011), if the app has a
    /// cluster listener.
    pub cluster: Option<TcpListener>,
    /// The metrics page, if asked for.
    pub metrics: Option<TcpListener>,
}

/// One start of an app runtime: the app, and the tasks that belong to that
/// start (its deploy and secrets followers), aborted when it stops.
#[allow(missing_debug_implementations)]
pub struct Runtime {
    /// What serves the app.
    pub app: App,
    /// Ended with the runtime.
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts an app runtime: everything a new process would run for it, the
/// start check included (`ActorService::node_id_checked`). Called again for
/// each restart in place.
pub type BuildRuntime =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Runtime, String>> + Send>> + Send + Sync>;

/// One app of a process serving several (ADR 0038, section 4).
#[allow(missing_debug_implementations)]
pub struct AppSpec {
    /// `tenant/app`, as its hops name it (`x-wasmkeep-app`).
    pub name: String,
    /// The hostnames that reach it on the public port, normalized
    /// (lowercase, no port, no trailing dot).
    pub hostnames: Vec<String>,
    /// How to start it.
    pub build: BuildRuntime,
}

/// A node process's supervisor.
#[derive(Debug)]
pub struct Supervisor {
    process: Arc<NodeProcess>,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    /// A supervisor for a process serving no app yet.
    pub fn new() -> Self {
        Self {
            process: NodeProcess::new(),
        }
    }

    /// A supervisor for the process named `name` (`[process] id`): its
    /// certificate is named for it.
    pub fn named(name: &str) -> Self {
        Self {
            process: NodeProcess::named(name),
        }
    }

    /// The process its app's cluster joins ([`crate::ClusterConfig::process`]).
    pub fn process(&self) -> Arc<NodeProcess> {
        Arc::clone(&self.process)
    }

    /// Serves `app` on `listeners` until `stop()` resolves (`Ok(None)`),
    /// or until the app is deleted or its node id lost: then shuts down as
    /// for a stop and returns why (`Ok(Some(why))`). `stop` is called once
    /// for the metrics page and once for the shutdown.
    pub async fn serve<F>(
        &self,
        listeners: Listeners,
        app: App,
        stop: impl Fn() -> F,
        limits: ServeLimits,
    ) -> Result<Option<String>, crate::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Some(metrics) = listeners.metrics {
            let actors = app.actors.clone();
            let stop = stop();
            tokio::spawn(async move {
                let _ = crate::http::serve_metrics(metrics, actors, stop).await;
            });
        }
        // A second process found running with this node id (both started
        // at the same moment) stops this one too: it takes no more leases,
        // and shuts down like a stop, then returns the reason.
        let lost: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
        let actors = app.actors.clone();
        let lost_reason = Arc::clone(&lost);
        let stopped = stop();
        let shutdown = async move {
            let node_id_lost = async {
                match &actors {
                    Some(a) => a.node_id_lost().await,
                    None => std::future::pending().await,
                }
            };
            let app_deleted = async {
                match &actors {
                    Some(a) => a.app_deleted().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = app_deleted => {
                    let why = "the app was deleted (wasmkeep app delete): this node no longer serves it".to_owned();
                    tracing::error!("shutting down: {why}");
                    *lost_reason.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(why);
                }
                () = stopped => tracing::info!("shutting down"),
                why = node_id_lost => {
                    tracing::error!("shutting down: {why}");
                    *lost_reason.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(why);
                }
            }
        };
        match listeners.cluster {
            Some(cluster) => {
                crate::http::serve_with_cluster_listener(
                    listeners.public,
                    cluster,
                    app,
                    shutdown,
                    limits,
                )
                .await?;
            }
            None => crate::http::serve_with(listeners.public, app, shutdown, limits).await?,
        }
        Ok(lost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take())
    }

    /// Serves several apps (`apps`) on `listeners` until `stop()` resolves:
    /// each app's runtime started with its [`AppSpec::build`], restarted in
    /// place after a verdict on its node id, stopped for good by its
    /// tombstone.
    pub async fn serve_apps<F>(
        &self,
        listeners: Listeners,
        apps: Vec<AppSpec>,
        stop: impl Fn() -> F,
        limits: ServeLimits,
    ) -> Result<(), crate::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        use crate::http::apps::{AppTable, SlotState};
        if apps.len() > 1 {
            self.process.serve_several();
        }
        let table = Arc::new(AppTable::new(
            &apps
                .iter()
                .map(|a| (a.name.clone(), a.hostnames.clone()))
                .collect::<Vec<_>>(),
            limits.max_connections,
        ));
        let (stopping_tx, stopping) = tokio::sync::watch::channel(false);
        // The process's front reservations, shared between the apps' fronts
        // (ADR 0038, section 8): sized once the first front is known.
        let fronts: Arc<std::sync::OnceLock<Arc<crate::shares::Shares>>> = Arc::default();
        let front_members: Arc<std::sync::Mutex<Vec<crate::shares::Member>>> = Arc::default();
        // Each app's first start, before the ports open: its start check
        // is the one a new process runs.
        let mut runners = Vec::new();
        let mut started = Vec::new();
        for (spec, slot) in apps.into_iter().zip(table.slots.iter().cloned()) {
            let (first_tx, first_rx) = tokio::sync::oneshot::channel::<()>();
            started.push(first_rx);
            runners.push(tokio::spawn(run_app(
                spec,
                slot,
                limits.front_wall,
                Arc::clone(&fronts),
                Arc::clone(&front_members),
                stopping.clone(),
                first_tx,
            )));
        }
        for first in started {
            let _ = first.await;
        }
        let cluster = match listeners.cluster {
            Some(listener) => {
                let tls = self.process.cluster_tls().ok_or_else(|| {
                    crate::Error::ClusterTls(
                        "no app of this process started with a cluster listener".into(),
                    )
                })?;
                Some(crate::http::ClusterPort {
                    listener,
                    tls: tls.server_config().map_err(crate::Error::ClusterTls)?,
                    pins: Arc::clone(tls.pins()),
                    handshake_timeout: crate::http::CLUSTER_HANDSHAKE_TIMEOUT,
                })
            }
            None => None,
        };
        if let Some(metrics) = listeners.metrics {
            // One page for the process: app series carry their app's labels
            // (ADR 0038, section 9); the process's gauges sum the apps'.
            let metered = crate::http::Metered::Table(Arc::clone(&table));
            let stop = stop();
            tokio::spawn(async move {
                let _ = crate::http::serve_metrics_of(metrics, metered, stop).await;
            });
        }
        let shutdown = stop();
        let stopping_on = async move {
            shutdown.await;
            // No runtime is started again from here on.
            let _ = stopping_tx.send(true);
        };
        let served = crate::http::serve_served(
            listeners.public,
            cluster,
            crate::http::Served::Table(Arc::clone(&table)),
            stopping_on,
            limits,
        )
        .await;
        for r in runners {
            r.abort();
        }
        for slot in &table.slots {
            slot.set(SlotState::Starting);
        }
        served
    }
}

/// The first wait before starting a stopped runtime again, and the most.
const RESTART_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
const RESTART_MOST: std::time::Duration = std::time::Duration::from_secs(60);

/// Runs one app of a process serving several (ADR 0038, section 3):
/// starts it, then waits for a verdict on its node id (it is stopped and
/// started again in place, after a backoff) or its tombstone (stopped for
/// good), until the process stops. `started` is sent after the first try.
async fn run_app(
    spec: AppSpec,
    slot: Arc<crate::http::apps::AppSlot>,
    front_wall: std::time::Duration,
    fronts: Arc<std::sync::OnceLock<Arc<crate::shares::Shares>>>,
    front_members: Arc<std::sync::Mutex<Vec<crate::shares::Member>>>,
    mut stopping: tokio::sync::watch::Receiver<bool>,
    started: tokio::sync::oneshot::Sender<()>,
) {
    use crate::http::apps::SlotState;
    let mut started = Some(started);
    let mut backoff = RESTART_FIRST;
    loop {
        if *stopping.borrow() {
            return;
        }
        let built = (spec.build)().await;
        if let Some(s) = started.take() {
            let _ = s.send(());
        }
        let Runtime { mut app, tasks } = match built {
            Ok(r) => r,
            Err(why) => {
                tracing::error!(app = %slot.name, error = %why, retry_in_ms = backoff.as_millis() as u64, "the app's runtime did not start");
                slot.set(SlotState::Starting);
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = stopping.wait_for(|s| *s) => return,
                }
                backoff = (backoff * 2).min(RESTART_MOST);
                continue;
            }
        };
        backoff = RESTART_FIRST;
        if let Some(front) = app.front.as_mut() {
            front.wall = front_wall;
            // Its app's share of the process's front reservations: the
            // first front's slots size them (every app's are the same).
            let shares = fronts
                .get_or_init(|| Arc::new(crate::shares::Shares::new(front.max_requests() as u64)));
            if !front
                .share
                .as_ref()
                .is_some_and(|(s, _)| Arc::ptr_eq(s, shares))
            {
                let mut members = front_members.lock().unwrap_or_else(|p| p.into_inner());
                if !members.iter().any(|m| m.app() == slot.name) {
                    members.push(shares.join(&slot.name));
                }
                front.share = Some((Arc::clone(shares), Arc::from(slot.name.as_str())));
            }
        }
        let Some(actors) = app.actors.clone() else {
            slot.set(SlotState::Running(app));
            let _ = stopping.wait_for(|s| *s).await;
            return;
        };
        slot.set(SlotState::Running(app));
        tracing::info!(app = %slot.name, "the app's runtime started");
        let deleted = tokio::select! {
            () = actors.app_deleted() => true,
            why = actors.node_id_lost() => {
                tracing::error!(app = %slot.name, "stopping the app's runtime: {why}");
                false
            }
            _ = stopping.wait_for(|s| *s) => {
                for t in tasks {
                    t.abort();
                }
                return;
            }
        };
        // This runtime alone stops; the process's other apps go on.
        slot.set(if deleted {
            SlotState::Deleted
        } else {
            SlotState::Starting
        });
        for t in tasks {
            t.abort();
        }
        actors.begin_shutdown();
        actors.shutdown(crate::http::SHUTDOWN_GRACE).await;
        // Its node id leaves the process now, whoever still holds the
        // stopped service: the next start may take it.
        actors.leave_process();
        drop(actors);
        if deleted {
            tracing::error!(app = %slot.name, "the app was deleted (wasmkeep app delete): this process no longer serves it");
            return;
        }
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            _ = stopping.wait_for(|s| *s) => return,
        }
        backoff = (backoff * 2).min(RESTART_MOST);
    }
}

impl Supervisor {
    /// The process's node ids (one per app runtime running now).
    pub fn node_ids(&self) -> Vec<String> {
        self.process.serving()
    }
}

/// The live nodes on the bucket `prefix` that do not announce `multi-app`
/// (ADR 0038, section 10): a process listing a second app refuses to start
/// while any is found.
pub async fn nodes_without_multi_app(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, prefix.to_owned());
    let all = wasmkeep_cluster::membership::all_heartbeats(&bucket)
        .await
        .map_err(|e| e.to_string())?;
    let horizon = crate::cluster::heartbeat_horizon_ms(
        crate::cluster::DEFAULT_HEARTBEAT,
        &wasmkeep_cluster::lease::LeaseConfig::default_safe(),
    );
    let now = crate::cluster_tls::now_ms();
    Ok(all
        .into_iter()
        .filter(|n| wasmkeep_cluster::membership::is_live(n, now, horizon) && !n.can_multi_app)
        .map(|n| n.id)
        .collect())
}
