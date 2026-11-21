//! What a node process shares between the apps it serves (ADR 0038,
//! section 2): the cluster's TLS identity and the trust table built from
//! the heartbeats, the clients to other nodes, and the listing of
//! `nodes/`. Owned by the process ([`crate::Supervisor`]); each app's
//! [`Cluster`] holds it.
//!
//! The first app's cluster builds the transport (the certificate named for
//! the process, or that app's node id); every other app of the process
//! shares it, and must publish the same cluster address under the same
//! bucket prefix. One node id joins a process once at a time.

use std::sync::atomic::{AtomicBool, Ordering};
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
    /// The process's name ([`NodeProcess::named`]): its certificate's, and
    /// `process=` in each of its node ids' heartbeats.
    name: Option<String>,
    transport: Mutex<Option<Arc<Transport>>>,
    /// The node ids whose clusters are in the process now, each with the
    /// number of its place ([`Joined`]).
    joined: Mutex<std::collections::BTreeMap<String, u64>>,
    /// Numbers places, so a place left early is never confused with the
    /// node id's next one.
    places: std::sync::atomic::AtomicU64,
    /// Set by a supervisor serving several apps ([`Self::serve_several`]).
    several: AtomicBool,
}

impl std::fmt::Debug for NodeProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeProcess")
            .field("name", &self.name)
            .field("serves", &self.serving())
            .finish()
    }
}

impl NodeProcess {
    /// A process serving no app yet.
    pub fn new() -> Arc<Self> {
        Self::make(None)
    }

    /// A process named `name` (`[process] id`, ADR 0038): its one
    /// certificate is the process's, named for it, whichever of its apps
    /// made it.
    pub fn named(name: &str) -> Arc<Self> {
        Self::make(Some(name.to_owned()))
    }

    fn make(name: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            name,
            transport: Mutex::new(None),
            joined: Mutex::new(std::collections::BTreeMap::new()),
            places: std::sync::atomic::AtomicU64::new(0),
            several: AtomicBool::new(false),
        })
    }

    /// The process's name, if it has one.
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Marks the process as serving several apps: its runtimes then require
    /// the sender and the forward proof on every hop, and the `multi-app`
    /// word of every live node (ADR 0038, sections 5 and 10).
    pub fn serve_several(&self) {
        self.several.store(true, Ordering::Release);
    }

    /// Whether it serves several apps (marked, or several joined now).
    pub(crate) fn several(&self) -> bool {
        self.several.load(Ordering::Acquire)
            || self.joined.lock().unwrap_or_else(|p| p.into_inner()).len() > 1
    }

    /// The name of its certificate, once made: the process's, or the
    /// node id of the app that made it.
    #[cfg(test)]
    pub(crate) fn certificate_name(&self) -> Option<String> {
        self.transport()?.tls.as_ref().map(|t| t.name().to_owned())
    }

    /// The process's cluster TLS identity, once an app's cluster made it.
    pub(crate) fn cluster_tls(&self) -> Option<Arc<ClusterTls>> {
        self.transport
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|t| t.tls.clone())
    }

    /// The node ids whose clusters are in the process now.
    pub fn serving(&self) -> Vec<String> {
        self.joined
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// The node id of the first app this process serves now.
    pub fn serves(&self) -> Option<String> {
        self.serving().into_iter().next()
    }

    /// The transport, once an app's cluster made it.
    #[cfg(test)]
    pub(crate) fn transport(&self) -> Option<Arc<Transport>> {
        self.transport
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// The transport for the app whose cluster `config` describes: made
    /// from that configuration by the first app, shared by the others;
    /// `nodes` reads the bucket's `nodes/` (the cluster's counted store,
    /// under its prefix). Refuses a node id already in the process, and an
    /// app whose cluster address, plaintext mode or prefix differs from
    /// the transport's. The node id leaves when the guard is dropped.
    pub(crate) fn attach(
        self: &Arc<Self>,
        config: &ClusterConfig,
        nodes: ObjectStoreBucket,
    ) -> Result<(Arc<Transport>, Joined), crate::Error> {
        let mut slot = self.transport.lock().unwrap_or_else(|p| p.into_inner());
        let refuse_plaintext = config.cluster_plaintext == ClusterPlaintext::Refuse;
        let advertise = config.cluster_tls.as_ref().map(|t| t.advertise.clone());
        let transport = match slot.as_ref() {
            Some(t) => {
                let same_address = t.tls.as_ref().map(|tls| tls.advertise().to_owned())
                    == advertise
                        .as_ref()
                        .map(|a| a.trim_end_matches('/').to_owned());
                if !same_address
                    || t.refuse_plaintext != refuse_plaintext
                    || t.prefix != config.prefix
                {
                    return Err(crate::Error::Config(format!(
                        "node {} does not share this process's cluster listener, plaintext mode \
                         and bucket prefix: the apps of one process share them",
                        config.node_id
                    )));
                }
                Arc::clone(t)
            }
            None => {
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
                let t = Arc::new(Transport {
                    tls,
                    refuse_plaintext,
                    prefix: config.prefix.clone(),
                    clients: RwLock::new(Clients::new(&connector)),
                    connector,
                    nodes,
                    live: RwLock::default(),
                    heard: Mutex::default(),
                });
                *slot = Some(Arc::clone(&t));
                t
            }
        };
        let mut joined = self.joined.lock().unwrap_or_else(|p| p.into_inner());
        if joined.contains_key(&config.node_id) {
            return Err(crate::Error::Config(format!(
                "node {} already runs in this process",
                config.node_id
            )));
        }
        let place = self.places.fetch_add(1, Ordering::Relaxed);
        joined.insert(config.node_id.clone(), place);
        Ok((
            transport,
            Joined {
                process: Arc::clone(self),
                node: config.node_id.clone(),
                place,
            },
        ))
    }
}

/// A node id's place in its process; it leaves when this is dropped.
#[derive(Debug)]
pub(crate) struct Joined {
    process: Arc<NodeProcess>,
    node: String,
    place: u64,
}

impl Joined {
    /// The process.
    pub(crate) fn process(&self) -> &Arc<NodeProcess> {
        &self.process
    }

    /// Leaves the process now (a stopped runtime, ADR 0038, section 3);
    /// again when dropped, which then does nothing.
    pub(crate) fn leave(&self) {
        let mut joined = self
            .process
            .joined
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if joined.get(&self.node) == Some(&self.place) {
            joined.remove(&self.node);
        }
    }
}

impl Drop for Joined {
    fn drop(&mut self) {
        self.leave();
    }
}

/// The process's transport to other nodes, and its view of `nodes/`.
pub(crate) struct Transport {
    /// Refuse mode (ADR 0011, phase B), as its first app chose it.
    refuse_plaintext: bool,
    /// The bucket prefix its apps share.
    prefix: String,
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
    /// The live nodes as every runtime of the process last listed them:
    /// each one's app, and until when its heartbeat is live (ms). The
    /// sender of a hop is checked against it, as its pin is against the
    /// process's pins (ADR 0038, section 5).
    live: RwLock<std::collections::HashMap<String, (String, u64)>>,
    /// Each node's heartbeat as last read by any app of the process (a
    /// listing or one read of its object; `None`: found gone), with when
    /// that read was issued (ADR 0020, amendment of 2026-11-20). The read
    /// issued last wins, whichever lands last, so a body read earlier never
    /// undoes a fence or a new key read since, by this app or another. A
    /// read that fails changes nothing: its node stays as last read,
    /// trusted until that heartbeat's own horizon and no longer.
    heard: Mutex<Heard>,
}

/// [`Transport::note_heartbeats`]'s state.
#[derive(Debug, Default)]
struct Heard {
    /// Each node's heartbeat as last read, with when that read was issued.
    bodies: std::collections::HashMap<String, (std::time::Instant, Option<NodeInfo>)>,
    /// When the process first read each node's current heartbeat.
    first_seen: wasmkeep_cluster::membership::FirstSeen,
}

/// Heartbeats just read, to record ([`Transport::note_heartbeats`]).
pub(super) enum Heartbeats {
    /// A listing of `nodes/`: every node there is.
    Listed(Vec<NodeInfo>),
    /// Reads of some nodes' objects, each with what it found.
    Read(Vec<(String, HeartbeatRead)>),
}

impl Transport {
    /// Records heartbeats read by a listing or reads issued at `issued`,
    /// then runs `apply` on the result while still holding it, so that two
    /// apps of the process never apply their views out of order (each
    /// applies the newest the process has):
    ///
    /// - a node's entry is replaced only by a read issued later than the
    ///   one it holds, and a listing marks the nodes it leaves out gone;
    /// - never by a `live` body of the instance a held `fenced` or
    ///   `released` body names: that process cannot write again, so such a
    ///   body was served before the fence or the release landed, however
    ///   late its read was issued;
    /// - each heartbeat is judged by its `written_at`, or by when the
    ///   process first read it if that is earlier (a clock ahead,
    ///   [`wasmkeep_cluster::membership::FirstSeen`]): the same for every
    ///   app, and kept while the process runs (a restart of the process
    ///   starts it again).
    ///
    /// `apply` gets every node as last read (by id), the same judged, and
    /// the nodes this call replaced.
    pub(super) fn note_heartbeats<R>(
        &self,
        read: Heartbeats,
        issued: std::time::Instant,
        apply: impl FnOnce(&[NodeInfo], &[NodeInfo], &[String]) -> R,
    ) -> R {
        let now = crate::cluster_tls::now_ms();
        let mut heard = self.heard.lock().unwrap_or_else(|p| p.into_inner());
        let Heard { bodies, first_seen } = &mut *heard;
        let stopped_instance = |held: Option<&(std::time::Instant, Option<NodeInfo>)>| {
            held.and_then(|(_, b)| b.as_ref())
                .filter(|b| {
                    matches!(
                        b.lease,
                        Some(
                            wasmkeep_cluster::node_lease::NodeState::Fenced
                                | wasmkeep_cluster::node_lease::NodeState::Released
                        )
                    )
                })
                .and_then(|b| b.instance.clone())
        };
        let mut put = |id: String, body: Option<NodeInfo>| {
            let held = bodies.get(&id);
            if held.is_some_and(|(at, _)| *at > issued) {
                return false;
            }
            let stale_live = body.as_ref().is_some_and(|b| {
                b.lease == Some(wasmkeep_cluster::node_lease::NodeState::Live)
                    && b.instance.is_some()
                    && stopped_instance(held) == b.instance
            });
            if stale_live {
                return false;
            }
            bodies.insert(id, (issued, body));
            true
        };
        let mut fresh = Vec::new();
        match read {
            Heartbeats::Listed(listed) => {
                let mut listed_ids = std::collections::HashSet::new();
                for n in listed {
                    let id = n.id.clone();
                    listed_ids.insert(id.clone());
                    if put(id.clone(), Some(n)) {
                        fresh.push(id);
                    }
                }
                drop(put);
                let gone: Vec<String> = bodies
                    .keys()
                    .filter(|id| !listed_ids.contains(*id))
                    .cloned()
                    .collect();
                for id in gone {
                    if bodies.get(&id).is_some_and(|(at, _)| *at <= issued) {
                        bodies.insert(id, (issued, None));
                    }
                }
            }
            Heartbeats::Read(reads) => {
                for (id, read) in reads {
                    match read {
                        HeartbeatRead::Found(n) => {
                            if put(id.clone(), Some(*n)) {
                                fresh.push(id);
                            }
                        }
                        HeartbeatRead::Gone => {
                            put(id, None);
                        }
                        HeartbeatRead::Failed(_) => {}
                    }
                }
                drop(put);
            }
        }
        let mut all: Vec<NodeInfo> = bodies.values().filter_map(|(_, b)| b.clone()).collect();
        all.sort_by(|a, b| a.id.cmp(&b.id));
        first_seen.retain(|id| all.iter().any(|n| n.id == id));
        let judged: Vec<NodeInfo> = all
            .iter()
            .map(|n| {
                let mut n = n.clone();
                n.written_at_ms = first_seen.written_at(&n.id, n.written_at_ms, now);
                n
            })
            .collect();
        apply(&all, &judged, &fresh)
    }

    /// Node `id`'s heartbeat as last read.
    #[cfg(test)]
    pub(super) fn heard_body(&self, id: &str) -> Option<NodeInfo> {
        self.heard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .bodies
            .get(id)
            .and_then(|(_, b)| b.clone())
    }

    /// The clients to other nodes, as they are now.
    pub(super) fn clients(&self) -> Arc<Clients> {
        Arc::clone(&self.clients.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Renews the trust table from the live `nodes` (their pins trusted
    /// for `horizon_ms` after their heartbeat was written); when an
    /// address changed hands (or its node left), drops every pooled
    /// connection, so none to a previous holder is used.
    pub(super) fn refresh_trust(&self, nodes: &[NodeInfo], horizon_ms: u64) {
        {
            let now = crate::cluster_tls::now_ms();
            let mut live = self.live.write().unwrap_or_else(|p| p.into_inner());
            for n in nodes {
                let until = n.written_at_ms.saturating_add(horizon_ms);
                let at = live.entry(n.id.clone()).or_insert((n.app.clone(), 0));
                if until >= at.1 {
                    *at = (n.app.clone(), until);
                }
            }
            live.retain(|_, (_, until)| *until >= now);
        }
        if let Some(tls) = &self.tls
            && tls.refresh(nodes, horizon_ms)
        {
            *self.clients.write().unwrap_or_else(|p| p.into_inner()) =
                Clients::new(&self.connector);
        }
    }

    /// The app of node `id`, if a runtime of the process last listed it live.
    pub(super) fn live_app(&self, id: &str) -> Option<String> {
        let now = crate::cluster_tls::now_ms();
        self.live
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .filter(|(_, until)| *until >= now)
            .map(|(app, _)| app.clone())
    }

    /// Every readable heartbeat under `nodes/`, live or not, sorted by id:
    /// one listing.
    pub(super) async fn list_nodes(&self) -> Result<Vec<NodeInfo>, BucketError> {
        wasmkeep_cluster::membership::all_heartbeats(&self.nodes).await
    }

    /// The heartbeats of the nodes `ids` names, all read at once, each
    /// within `attempt` (ADR 0020, amendment of 2026-11-20): one slow or
    /// hung read costs one attempt, not the others' reads. Each with what
    /// its read found, in the order given.
    pub(super) async fn read_each(
        &self,
        ids: &[String],
        attempt: std::time::Duration,
    ) -> Vec<(String, HeartbeatRead)> {
        // Owned, each its own future, collected first: nothing borrowed is
        // held across the joined reads.
        let reads: Vec<_> = ids
            .iter()
            .map(|id| read_one(self.nodes.clone(), id.clone(), attempt))
            .collect();
        futures_util::future::join_all(reads).await
    }
}

/// One read of node `id`'s heartbeat, within `attempt`.
async fn read_one(
    nodes: ObjectStoreBucket,
    id: String,
    attempt: std::time::Duration,
) -> (String, HeartbeatRead) {
    let started = std::time::Instant::now();
    let keys = [id];
    let read = tokio::time::timeout(
        attempt,
        wasmkeep_cluster::membership::heartbeats_of(&nodes, &keys),
    )
    .await;
    crate::metrics::METRICS
        .heartbeat_read_ms
        .observe(started.elapsed());
    let [id] = keys;
    let read = match read {
        Ok(Ok(found)) => match found.into_iter().find(|n| n.id == id) {
            Some(n) => HeartbeatRead::Found(Box::new(n)),
            None => HeartbeatRead::Gone,
        },
        Ok(Err(e)) => HeartbeatRead::Failed(e.to_string()),
        Err(_) => HeartbeatRead::Failed(format!("no answer within {attempt:?}")),
    };
    (id, read)
}

/// What one read of a node's heartbeat object found.
#[derive(Debug, Clone)]
pub(super) enum HeartbeatRead {
    /// Its heartbeat.
    Found(Box<NodeInfo>),
    /// No object (`cluster forget`), or one this version cannot read: no
    /// longer a node, as a listing would leave it out.
    Gone,
    /// No answer in time, or an error: what was last read of it stands.
    Failed(String),
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
    async fn apps_of_a_process_share_its_transport_and_a_node_id_joins_once() {
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
        assert!(!process.several());
        // A second app shares it.
        let blog = Cluster::new(
            &config("blog-1", &store, &process),
            &StateRoot::new("", "acme", "blog"),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&blog.transport, &transport));
        assert_eq!(process.serving(), ["blog-1", "shop-1"]);
        assert!(process.several());
        // One node id once: a second runtime of shop-1 is refused...
        let e = Cluster::new(
            &config("shop-1", &store, &process),
            &StateRoot::new("", "acme", "shop"),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("already runs in this process"), "{e}");
        // ...until the first has left.
        drop(shop);
        assert_eq!(process.serving(), ["blog-1"]);
        Cluster::new(
            &config("shop-1", &store, &process),
            &StateRoot::new("", "acme", "shop"),
        )
        .unwrap();
        // An app with another cluster address is refused.
        let mut other = config("misc-1", &store, &process);
        other.cluster_tls = Some(crate::cluster::ClusterTlsConfig::new("https://o.test:7443"));
        let e = Cluster::new(&other, &StateRoot::new("", "acme", "misc"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("does not share"), "{e}");
        // A cluster made without a process has one of its own.
        let solo = Cluster::new(
            &ClusterConfig::new("solo", "http://solo", Arc::clone(&store)),
            &StateRoot::new("", "acme", "solo"),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&solo.transport, &transport));
    }
}
