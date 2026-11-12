//! Actors over HTTP: `/actor/<class>/<name>/...` reaches one instance per
//! name, through its mailbox, with its own SQLite database.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use wasmkeep_cluster::alarm_index::{self, Entry, SweepClaim};
use wasmkeep_runtime::{
    ActorAddress, BoxError, CpuMeter, MailboxConfig, MailboxError, Mailboxes, Residency, Runtime,
};
use wasmkeep_storage::{ActorDb, Storage, StorageConfig};
use wasmkeep_wit::wasmkeep::actor::actors::{
    CallError, Request as CallRequest, Response as CallResponse,
};
use wasmkeep_wit::wasmkeep::actor::types::{Rows, SqlError, SqlErrorKind, Value};
use wasmkeep_wit::wasmkeep::actor::{actors, alarms, env, storage, types};
use wasmkeep_wit::{ActorCore, ActorCorePre};
use wasmtime::Store;
use wasmtime::component::{Component, HasSelf, InstancePre, Linker, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::bindings::{Proxy, ProxyPre};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

use crate::Error;
use crate::alarm::{self, AlarmHandle, Delivery, now_ms};
use crate::cluster::{
    ALARM_HANDOFF, CALLER, CHAIN, Cluster, ClusterConfig, INTERNAL, LeaseHold, OwnershipError,
    SERVED_BY, decode_address, encode_address,
};
use crate::http::text_response;
use crate::replica::{Replica, ReplicaError};
use crate::websocket::{Outgoing, SendError, Sockets, close_frame};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use wasmkeep_wit::wasmkeep::actor::websockets::{
    self, Message as WsMessageWit, SendError as WsSendError,
};

mod alarm_delivery;
mod classes;
mod collection;
mod deploys;
mod disk;
mod drain;
mod host;
mod moving;
mod requests;
mod routing;
mod sockets;
mod start;
mod state;
mod upgrade;

#[allow(
    clippy::wildcard_imports,
    reason = "the parts of one module, split by topic"
)]
use self::{
    alarm_delivery::*, classes::*, collection::*, disk::*, host::*, routing::*, sockets::*,
    state::*,
};

/// How long a node trusts "node X holds this actor" without asking the
/// bucket again.
pub(crate) const HINT_MS: u64 = 1_000;

/// Let ordinary hibernation finish before refusing a cold activation.
const ADMISSION_WAIT: Duration = Duration::from_millis(250);

/// Meta key holding the component version `on-init` last succeeded for.
const INIT_VERSION_KEY: &str = "init_version";

/// What `publish_alarm` left the actor's alarm index entry as.
struct Published {
    /// The entry the database now names (`None`: no alarm indexed).
    named: Option<Entry>,
    /// The entry it named before, to remove once the event is replicated.
    replaced: Option<Entry>,
}

/// Settings for the actor service.
#[derive(Debug, Clone)]
pub struct ActorConfig {
    /// Tenant all actors belong to (one tenant per process for now; see
    /// ROADMAP.md).
    pub tenant: String,
    /// Where actor databases live.
    pub storage: StorageConfig,
    /// Queue size and hibernation timeout.
    pub mailbox: MailboxConfig,
    /// Wall-clock limit per event, including time awaiting I/O.
    pub wall_per_event: Duration,
    /// First retry delay after a failed `on-alarm`; doubles per failure.
    pub alarm_retry_initial: Duration,
    /// Longest retry delay.
    pub alarm_retry_max: Duration,
    /// Which addresses actors' outbound HTTP may reach: public ones, plus
    /// the ranges this allows.
    pub egress: crate::egress::EgressPolicy,
    /// How much of what actors print reaches the node's log.
    pub guest_log: crate::guest_log::GuestLog,
    /// Each class's values from this node's files (`--env`: configuration
    /// and secrets). They win, by name, over the deployed configuration.
    pub env: crate::env::ActorEnv,
    /// Each class's deployed plain configuration (ADR 0013), under `env`.
    pub config: std::collections::BTreeMap<String, crate::env::Vars>,
    /// The app's secrets from the bucket (ADR 0016), over `config` and
    /// under `env`.
    pub secrets: crate::secrets::SecretSet,
    /// Each class's required names (ADR 0016, `requires`).
    pub requires: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Who may reach each class from outside (ADR 0010): the deployed
    /// version's rules, or `--access` for classes not from a deploy.
    pub access: crate::access::AccessRules,
    /// `--access` on a node following an app that has never deployed rules
    /// (no marker in `current`, ADR 0013): applied to deployed versions
    /// until one with the marker arrives, then ignored with a warning.
    pub flag_access: Option<crate::access::AccessRules>,
    /// The deploy sequence the classes given to [`ActorService::new`] come
    /// from (0: not from `wasmkeep deploy`). Stamped on them before anything
    /// runs, so no event (an alarm at startup, say) runs deployed code
    /// unchecked.
    pub deploy_seq: u64,
    /// Join a cluster: actors run only on the node holding their lease, and
    /// requests reaching another node are forwarded. `None` runs alone.
    pub cluster: Option<ClusterConfig>,
}

impl ActorConfig {
    /// Defaults with the given data directory.
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            tenant: "default".into(),
            storage: StorageConfig::new(data_dir),
            mailbox: MailboxConfig::default(),
            wall_per_event: Duration::from_secs(30),
            alarm_retry_initial: Duration::from_secs(1),
            alarm_retry_max: Duration::from_secs(300),
            cluster: None,
            egress: crate::egress::EgressPolicy::default(),
            guest_log: crate::guest_log::GuestLog::default(),
            env: crate::env::ActorEnv::default(),
            config: std::collections::BTreeMap::new(),
            secrets: crate::secrets::SecretSet::default(),
            requires: std::collections::BTreeMap::new(),
            access: crate::access::AccessRules::default(),
            flag_access: None,
            deploy_seq: 0,
        }
    }
}

/// Routes events to actors. Cheap to clone.
#[derive(Clone)]
pub struct ActorService {
    inner: Arc<ServiceInner>,
}

struct ServiceInner {
    tenant: String,
    wall: Duration,
    sockets: Arc<Sockets>,
    classes: Classes,
    /// Kept so new class versions can be linked (`replace_class`).
    linker: Linker<ActorHost>,
    runtime: Runtime,
    mailboxes: Mailboxes<ActorState>,
    alarms: AlarmHandle,
    cluster: Option<Arc<Cluster>>,
    /// Deploy sequence this node runs (0: classes not from `wasmkeep deploy`).
    deploy_seq: std::sync::atomic::AtomicU64,
    /// Wakes the deploy follower when an actor needs a newer version.
    deploy_wakeup: Arc<tokio::sync::Notify>,
    /// This node's `--env` values, merged over each version's configuration.
    node_env: crate::env::ActorEnv,
    /// The rules given at start, for classes `replace_class` adds.
    start_rules: Arc<crate::access::AccessRules>,
    /// `--access` during the migration of ADR 0013 (`ActorConfig::flag_access`).
    flag_access: Option<crate::access::AccessRules>,
    /// The deployed version is ahead of what this node could apply (it read
    /// one it cannot build or parse): not ready until it catches up.
    deploy_behind: std::sync::atomic::AtomicBool,
    /// Set when the server starts shutting down (readiness).
    shutting_down: std::sync::atomic::AtomicBool,
    /// The deployed version's plain configuration, kept apart so a secrets
    /// change can rebuild values without a deploy (ADR 0016).
    deployed_config: std::sync::RwLock<std::collections::BTreeMap<String, crate::env::Vars>>,
    /// The app's secrets as last opened (ADR 0016).
    secrets: std::sync::RwLock<Arc<crate::secrets::SecretSet>>,
    /// This node cannot open the app's current secrets (ADR 0016):
    /// readiness fails and events are refused (without a cluster; with one,
    /// `Cluster::secrets_blocked` also drains it). Shared with every
    /// actor's state.
    secrets_blocked: Arc<std::sync::atomic::AtomicBool>,
    /// Moves under way (ADR 0017), shared with every actor's state.
    moves: drain::Moves,
    /// Whether the data directory's disk was nearly full at the last
    /// sample, and when that was (ms; 0: not sampled yet). Sampled off the
    /// request path by `sample_disk`.
    disk_low: std::sync::atomic::AtomicBool,
    disk_sampled_ms: std::sync::atomic::AtomicU64,
    /// Other nodes that own actors this node relays to, until when (ms):
    /// learned from the bucket or from an event that found it was not the
    /// owner, so relaying does not read the bucket for every request.
    owner_hints: std::sync::Mutex<HashMap<ActorAddress, (String, u64)>>,
    /// Actors a draining node handed this node an alarm for, until when
    /// (ms): their record may still name that node, its release pending.
    handed: std::sync::Mutex<HashMap<ActorAddress, u64>>,
}

impl ServiceInner {
    /// An alarm found `owner` holding the actor. If that owner is draining
    /// (ADR 0014), it may be about to hand the actor, and this alarm, to
    /// this node, with its lease not yet released: look again shortly
    /// instead of dropping the alarm and leaving it to the sweep. The same
    /// if a node handed this node the actor's alarm lately, whatever this
    /// node's last `control/` listing says: only a draining node hands one
    /// off, and its record names it until its release lands (a release
    /// that failed is retried in the background, ADR 0020).
    fn recheck_if_handing_over(&self, address: &ActorAddress, owner: &str) {
        if self.handed_lately(address)
            || self
                .cluster
                .as_ref()
                .is_some_and(|c| c.is_draining_node(owner))
        {
            self.alarms.set_if_earlier(
                address.clone(),
                now_ms().saturating_add(DRAINING_OWNER_RECHECK_MS),
            );
        }
    }

    /// A draining node handed this node `address`'s alarm.
    fn note_handed(&self, address: &ActorAddress) {
        let now = now_ms();
        let window = self
            .cluster
            .as_ref()
            .map_or(HANDED_RECHECK_MS, |c| c.handed_recheck_ms());
        let mut handed = self.handed.lock().unwrap_or_else(|p| p.into_inner());
        if handed.len() >= MAX_OWNER_HINTS && !handed.contains_key(address) {
            handed.retain(|_, until| now < *until);
            // Still full: the entry closest to its end goes, not all of
            // them (each one is an alarm a sender's release may hold up).
            if handed.len() >= MAX_OWNER_HINTS
                && let Some(oldest) = handed
                    .iter()
                    .min_by_key(|(_, until)| **until)
                    .map(|(a, _)| a.clone())
            {
                handed.remove(&oldest);
            }
        }
        handed.insert(address.clone(), now.saturating_add(window));
    }

    /// Whether a draining node handed this node `address`'s alarm lately.
    fn handed_lately(&self, address: &ActorAddress) -> bool {
        let now = now_ms();
        let mut handed = self.handed.lock().unwrap_or_else(|p| p.into_inner());
        match handed.get(address) {
            Some(until) if now < *until => true,
            Some(_) => {
                handed.remove(address);
                false
            }
            None => false,
        }
    }
}

/// How soon an alarm held by a draining owner is looked at again (ms).
const DRAINING_OWNER_RECHECK_MS: u64 = 1_000;

/// How long after a hand-off its alarm is looked at again while the
/// actor's record names another node, at least (ms): the sender's release,
/// retried in the background, lands well within it, or the sender stops
/// and a peer fences it (ttl plus grace and a watch period, about 18 s
/// with the defaults; [`crate::cluster::Cluster::handed_recheck_ms`] grows
/// it with a longer ttl). Past it, the bucket's sweep delivers the alarm.
/// If another node really owns the actor, the cost is a read of its record
/// a second, and, should that owner give it up meanwhile, this node taking
/// it over to find the alarm already run.
pub(crate) const HANDED_RECHECK_MS: u64 = 30_000;

/// Most owner hints kept; past it, expired ones go, then all of them.
const MAX_OWNER_HINTS: usize = 16_384;

impl std::fmt::Debug for ActorService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorService")
            .field("tenant", &self.inner.tenant)
            .field(
                "classes",
                &read(&self.inner.classes)
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .field("resident", &self.inner.mailboxes.resident())
            .finish()
    }
}

/// Meta key: how many `on-alarm` runs in a row failed for the pending alarm
/// (the retry backoff is computed from it, so it survives hibernation and
/// moves); setting or clearing an alarm resets it.
pub(crate) const ALARM_FAILURES_KEY: &str = "alarm_failures";
/// Meta key: the exact alarm index entry listed for this actor (empty:
/// none; [`alarm_index::to_meta`]), so whichever node owns it next keeps it
/// while the alarm stands and removes exactly it when the alarm changes.
const ALARM_INDEXED_KEY: &str = "alarm_indexed";

/// Meta key a new owner writes, and uploads, before it removes index
/// entries an earlier owner wrote (`fence`): the generation it holds.
const OWNER_FENCE_KEY: &str = "owner_fence";
/// Meta key: the highest deploy sequence that ran an event on this actor.
const DEPLOY_SEQ_KEY: &str = "deploy_seq";

/// `refresh_class`: the actor's class was removed by a deploy.
const CLASS_GONE: &str = "class no longer served";

impl ActorService {
    /// The runtime this service instantiates with. Components passed to
    /// [`replace_class`](Self::replace_class) must be compiled by it (or a
    /// clone): Wasmtime cannot mix engines.
    pub fn runtime(&self) -> &Runtime {
        &self.inner.runtime
    }

    /// The rule the front door sees for an actor, to refuse early: the
    /// event decides again with the class it takes (ADR 0013), so this may
    /// only refuse, never admit.
    fn early_rule(&self, class: &str, name: &str) -> crate::access::Access {
        read(&self.inner.classes)
            .get(class)
            .map_or(crate::access::Access::Open, |c| {
                c.rules.for_actor(class, name)
            })
    }

    /// The classes this service serves.
    pub fn class_names(&self) -> Vec<String> {
        read(&self.inner.classes).keys().cloned().collect()
    }

    /// Registered WebSockets, including slots reserved for pending close delivery.
    pub fn websockets(&self) -> usize {
        self.inner.sockets.len()
    }

    /// Stops this node's actors for a clean shutdown, for good: afterwards
    /// the service refuses every event. WebSockets are closed with 1012
    /// (clients reconnect, possibly to another node), new events are
    /// refused, each actor finishes its queued events, and every actor is
    /// then dropped as in hibernation. In a cluster that releases their
    /// leases, so other nodes take them over at once instead of after the
    /// lease runs out; this waits for those releases to reach the bucket.
    ///
    /// Actors get up to `limit` to finish; releases get a few more seconds
    /// of their own. A release that does not make it is safe: the node
    /// lease's `released` write frees its record, or else a peer's fence.
    pub async fn shutdown(&self, limit: Duration) {
        let started = tokio::time::Instant::now();
        let resident = self.inner.mailboxes.resident();
        self.inner.sockets.close_all(1012, "node shutting down");
        self.inner.mailboxes.close();
        if !self.inner.mailboxes.drained(limit).await {
            tracing::warn!(
                still_running = self.inner.mailboxes.resident(),
                "shutdown: some actors did not finish in time"
            );
        }
        if let Some(cluster) = &self.inner.cluster {
            if !cluster.releases_done(Duration::from_secs(3)).await {
                tracing::warn!(
                    "shutdown: some actors' releases did not land; the node lease's `released` \
                     write frees their records (or, if that fails too, a peer's fence)"
                );
            }
            // Last, the node lease: `released`, so peers need not fence it
            // and a restart under this id waits less (ADR 0020).
            if !cluster
                .stop_node_lease(cluster.node_lease_stop_limit())
                .await
            {
                tracing::warn!(
                    "shutdown: the node lease was not released; it stays live, so peers free \
                     this node's actors only by fencing it, and a restart under this id waits \
                     ttl plus grace before it serves"
                );
            }
        }
        tracing::info!(actors = resident, took = ?started.elapsed(), "actors stopped");
    }

    /// Live nodes on the same bucket prefix serving the same tenant but
    /// another app: their actors would share keys. Empty outside a cluster.
    ///
    /// # Errors
    /// The heartbeats could not be read.
    pub async fn app_conflicts(&self) -> Result<Vec<String>, String> {
        match &self.inner.cluster {
            Some(c) => c.app_conflicts().await,
            None => Ok(Vec::new()),
        }
    }

    /// Waits for the check that no other process runs with this node id
    /// (in a cluster; at once otherwise). Until it passes, the node takes
    /// no lease and writes no heartbeat; a restart soon after a stop waits
    /// two and a half heartbeat periods here.
    ///
    /// # Errors
    /// Another process runs with this node id: why, for the operator. This
    /// node then never takes a lease; the caller should stop it.
    pub async fn node_id_checked(&self) -> Result<(), String> {
        match &self.inner.cluster {
            Some(c) => c.identity().await,
            None => Ok(()),
        }
    }

    /// Resolves, with why, once another process is found running with this
    /// node id, at start or later (in a cluster; never otherwise). This
    /// node then takes no more leases; the caller should shut it down.
    pub async fn node_id_lost(&self) -> String {
        match &self.inner.cluster {
            Some(c) => c.identity_lost().await,
            None => std::future::pending().await,
        }
    }

    /// This node's TLS identity and its peers' pins, when its cluster has a
    /// cluster listener (ADR 0011).
    pub(crate) fn cluster_tls(&self) -> Option<Arc<crate::cluster_tls::ClusterTls>> {
        self.inner.cluster.as_ref()?.tls().cloned()
    }

    /// Number of actors currently resident in memory.
    pub fn resident(&self) -> usize {
        self.inner.mailboxes.resident()
    }

    /// What the metrics page reads at scrape time.
    pub fn gauges(&self) -> crate::metrics::Gauges {
        crate::metrics::Gauges {
            resident: self.resident(),
            websockets: self.websockets(),
            classes: self.class_names().len(),
            not_ready: self.not_ready(),
        }
    }

    /// Marks the node as shutting down: `/_wasmkeep/ready` answers 503 from
    /// now on. Called by the server when its shutdown signal arrives.
    pub fn begin_shutdown(&self) {
        self.inner
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Release);
        // Requests kept for a takeover are answered now: a stopping node
        // takes no actor.
        if let Some(cluster) = &self.inner.cluster {
            cluster.failover.close();
        }
    }

    /// Why this node should not get traffic, if it should not (`/_wasmkeep/ready`):
    /// shutting down; in a cluster, the bucket unreachable or the cluster
    /// secret not loaded; or the data directory's disk nearly full (or not
    /// sampled recently). Reads only flags: cheap for any caller.
    pub fn not_ready(&self) -> Option<&'static str> {
        use std::sync::atomic::Ordering;
        let inner = &self.inner;
        if inner.shutting_down.load(Ordering::Acquire) || inner.mailboxes.is_closed() {
            return Some("shutting down");
        }
        if let Some(reason) = inner.cluster.as_ref().and_then(|c| c.not_ready()) {
            return Some(reason);
        }
        let sampled = inner.disk_sampled_ms.load(Ordering::Acquire);
        if sampled == 0 {
            return Some("disk not checked yet");
        }
        if now_ms().saturating_sub(sampled) > DISK_SAMPLE_STALE.as_millis() as u64 {
            return Some("disk check stalled");
        }
        if inner.disk_low.load(Ordering::Acquire) {
            return Some("disk almost full");
        }
        if inner.deploy_behind.load(Ordering::Acquire) {
            return Some("cannot apply the deployed version");
        }
        if inner.secrets_blocked.load(Ordering::Acquire) {
            return Some("cannot open the app's secrets");
        }
        None
    }
}

/// Admission failed before the event ran. The caller may retry this attempt.
fn capacity_response() -> Response<HyperOutgoingBody> {
    let mut response = text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "actor capacity unavailable\n",
    );
    response.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    response
}

/// Store data for an actor instance.
struct ActorHost {
    /// The deploy sequence this instance last recorded in the database.
    checked_seq: Option<u64>,
    /// The component version this instance knows `on-init` has run for.
    init_version: Option<String>,
    /// The socket whose `on-websocket-open` is running, if any.
    opening: Option<u64>,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    db: Option<ResidentDb>,
    /// Kept across a cancelled guest future; drained before event cleanup.
    pending_db: Option<tokio::task::JoinHandle<ResidentDb>>,
    delete_requested: bool,
    call: Option<CallContext>,
    sockets: Arc<Sockets>,
    /// Replication, so `exec-durable` can wait for the bucket.
    replica: Option<SharedReplica>,
    /// Sends the guest's outbound HTTP under the node's egress policy.
    egress: crate::egress::EgressHooks,
    /// The class's `env` values.
    env: Arc<crate::env::Vars>,
    /// In a cluster: where to list a pending alarm before `exec-durable`
    /// ships it, so no alarm reaches the bucket unindexed.
    alarm_index: Option<(Arc<Cluster>, ActorAddress)>,
}

type ServiceSlot = Arc<std::sync::OnceLock<std::sync::Weak<ServiceInner>>>;
type Chain = Arc<[ActorAddress]>;

/// What an instance needs to make calls during the current event.
#[derive(Clone)]
struct CallContext {
    service: Option<std::sync::Weak<ServiceInner>>,
    me: ActorAddress,
    /// Actors waiting on this event; calling any of them would deadlock.
    chain: Chain,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
