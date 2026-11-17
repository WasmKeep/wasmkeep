//! The supervisor of a node process (ADR 0038, section 1): it owns the
//! process's listeners and its shutdown, and the [`NodeProcess`] its app
//! shares the transport through. This version serves exactly one app.
//!
//! The app's tombstone (ADR 0028) or a verdict on its node id (another
//! process writing it, its lease fenced or released) ends the process as
//! before: [`Supervisor::serve`] shuts down and returns the reason, and
//! the caller exits non-zero with it.

use std::future::Future;
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
}
