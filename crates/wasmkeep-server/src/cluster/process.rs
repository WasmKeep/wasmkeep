//! What a node process shares between the apps it serves (ADR 0038,
//! section 2): the cluster's TLS identity and the trust table built from
//! the heartbeats, the clients to other nodes, and the listing of
//! `nodes/`. Owned by the process ([`crate::Supervisor`]); each app's
//! [`Cluster`] holds it.
//!
//! This version serves exactly one app per process: the first app's
//! cluster builds the transport from its configuration (its node id names
//! the certificate, its cluster flags choose TLS), and a second app is
//! refused.

use std::sync::{Arc, Mutex, RwLock};

use wasmkeep_cluster::lease::BucketError;
use wasmkeep_cluster::membership::NodeInfo;
use wasmkeep_cluster::object_store::ObjectStoreBucket;

use super::{Clients, ClusterConfig, ClusterPlaintext};
use crate::cluster_tls::{ClusterConnector, ClusterTls};

/// A node process: what its apps share. Made with [`NodeProcess::new`]
/// and given to the app's [`ClusterConfig::process`]; a cluster made
/// without one has a process of its own.
pub struct NodeProcess {
    /// The process's name ([`NodeProcess::named`]): its certificate's.
    name: Option<String>,
    transport: Mutex<Option<Arc<Transport>>>,
}

impl std::fmt::Debug for NodeProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeProcess")
            .field("serves", &self.serves())
            .finish()
    }
}

impl NodeProcess {
    /// A process serving no app yet.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            name: None,
            transport: Mutex::new(None),
        })
    }

    /// A process named `name` (`[process] id`, ADR 0038): its one
    /// certificate is the process's, named for it, whichever of its apps
    /// made it.
    pub fn named(name: &str) -> Arc<Self> {
        Arc::new(Self {
            name: Some(name.to_owned()),
            transport: Mutex::new(None),
        })
    }

    /// The name of its certificate, once made: the process's, or the
    /// node id of the app that made it.
    #[cfg(test)]
    pub(crate) fn certificate_name(&self) -> Option<String> {
        self.transport()?.tls.as_ref().map(|t| t.name().to_owned())
    }

    /// The node id of the app this process serves, once its cluster was
    /// made.
    pub fn serves(&self) -> Option<String> {
        self.transport
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|t| t.node_id.clone())
    }

    /// The transport, once an app's cluster made it.
    #[cfg(test)]
    pub(crate) fn transport(&self) -> Option<Arc<Transport>> {
        self.transport
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// The transport for the app whose cluster `config` describes, made
    /// from that configuration; `nodes` reads the bucket's `nodes/` (the
    /// cluster's counted store, under its prefix). Refuses a second app.
    pub(crate) fn attach(
        &self,
        config: &ClusterConfig,
        nodes: ObjectStoreBucket,
    ) -> Result<Arc<Transport>, crate::Error> {
        let mut slot = self.transport.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(first) = slot.as_ref() {
            return Err(crate::Error::Config(format!(
                "this process already serves node {} and serves one app per process in this                  version: node {} needs a process of its own",
                first.node_id, config.node_id
            )));
        }
        let refuse_plaintext = config.cluster_plaintext == ClusterPlaintext::Refuse;
        if refuse_plaintext && config.cluster_tls.is_none() {
            return Err(crate::Error::ClusterTls(
                "refusing plain HTTP between nodes needs a cluster listener".into(),
            ));
        }
        let tls = match &config.cluster_tls {
            Some(t) => Some(Arc::new(
                ClusterTls::generate(
                    self.name.as_deref().unwrap_or(&config.node_id),
                    &t.advertise,
                )
                .map_err(crate::Error::ClusterTls)?,
            )),
            None => None,
        };
        let connector = ClusterConnector::new(tls.clone(), refuse_plaintext);
        let transport = Arc::new(Transport {
            node_id: config.node_id.clone(),
            tls,
            clients: RwLock::new(Clients::new(&connector)),
            connector,
            nodes,
        });
        *slot = Some(Arc::clone(&transport));
        Ok(transport)
    }
}

/// The process's transport to other nodes, and its view of `nodes/`.
pub(crate) struct Transport {
    /// The app whose cluster made it.
    node_id: String,
    /// The process's TLS identity and its peers' pins, with a cluster
    /// listener (ADR 0011).
    pub(crate) tls: Option<Arc<ClusterTls>>,
    /// The clients to other nodes; replaced (with empty connection pools)
    /// when a peer's cluster address changes hands, so a connection pooled
    /// to its previous holder is never used again.
    clients: RwLock<Arc<Clients>>,
    pub(crate) connector: ClusterConnector,
    /// The bucket, for `nodes/`.
    nodes: ObjectStoreBucket,
}

impl Transport {
    /// The clients to other nodes, as they are now.
    pub(super) fn clients(&self) -> Arc<Clients> {
        Arc::clone(&self.clients.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Renews the trust table from the live `nodes` (their pins trusted
    /// for `horizon_ms` after their heartbeat was written); when an
    /// address changed hands (or its node left), drops every pooled
    /// connection, so none to a previous holder is used.
    pub(super) fn refresh_trust(&self, nodes: &[NodeInfo], horizon_ms: u64) {
        if let Some(tls) = &self.tls
            && tls.refresh(nodes, horizon_ms)
        {
            *self.clients.write().unwrap_or_else(|p| p.into_inner()) =
                Clients::new(&self.connector);
        }
    }

    /// Every readable heartbeat under `nodes/`, live or not, sorted by id:
    /// one listing.
    pub(super) async fn list_nodes(&self) -> Result<Vec<NodeInfo>, BucketError> {
        wasmkeep_cluster::membership::all_heartbeats(&self.nodes).await
    }

    /// The heartbeats of the nodes `ids` names, read one by one.
    pub(super) async fn read_nodes(&self, ids: &[String]) -> Result<Vec<NodeInfo>, BucketError> {
        wasmkeep_cluster::membership::heartbeats_of(&self.nodes, ids).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;
    use wasmkeep_cluster::state_root::StateRoot;

    use crate::cluster::Cluster;

    fn config(id: &str, store: &Arc<dyn ObjectStore>, process: &Arc<NodeProcess>) -> ClusterConfig {
        let mut config = ClusterConfig::new(id, format!("http://{id}"), Arc::clone(store));
        config.process = Some(Arc::clone(process));
        config
    }

    #[tokio::test]
    async fn the_certificate_is_named_for_the_process() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let tls = |id: &str, process: &Arc<NodeProcess>| {
            let mut c = config(id, &store, process);
            c.cluster_tls = Some(crate::cluster::ClusterTlsConfig::new("https://n.test:7443"));
            c
        };
        let named = NodeProcess::named("edge-1");
        Cluster::new(&tls("shop-1", &named), &StateRoot::new("", "acme", "shop")).unwrap();
        assert_eq!(named.certificate_name().as_deref(), Some("edge-1"));
        // Unnamed (`serve --app`): the app's node id, as before.
        let unnamed = NodeProcess::new();
        Cluster::new(
            &tls("shop-2", &unnamed),
            &StateRoot::new("", "acme", "shop"),
        )
        .unwrap();
        assert_eq!(unnamed.certificate_name().as_deref(), Some("shop-2"));
    }

    #[tokio::test]
    async fn the_cluster_uses_the_process_transport_and_a_second_app_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let process = NodeProcess::new();
        assert!(process.transport().is_none());
        let shop = Cluster::new(
            &config("shop-1", &store, &process),
            &StateRoot::new("", "acme", "shop"),
        )
        .unwrap();
        let transport = process.transport().expect("made by the first app");
        assert!(Arc::ptr_eq(&shop.transport, &transport));
        assert_eq!(process.serves().as_deref(), Some("shop-1"));

        let e = Cluster::new(
            &config("blog-1", &store, &process),
            &StateRoot::new("", "acme", "blog"),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("one app"), "{e}");
        assert!(e.contains("shop-1") && e.contains("blog-1"), "{e}");
        // The first app keeps the transport.
        assert!(Arc::ptr_eq(&process.transport().unwrap(), &transport));

        // A cluster made without a process has one of its own.
        let solo = Cluster::new(
            &ClusterConfig::new("solo", "http://solo", Arc::clone(&store)),
            &StateRoot::new("", "acme", "solo"),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&solo.transport, &transport));
    }
}
