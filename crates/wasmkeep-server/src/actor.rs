//! Actors over HTTP: `/actor/<class>/<name>/...` reaches one instance per
//! name, through its mailbox, with its own SQLite database.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use wasmkeep_cluster::alarm_index::{self, Entry, SweepClaim, SweepFound};
use wasmkeep_runtime::{
    ActorAddress, BoxError, BoxFuture, CpuMeter, DropCause, MailboxConfig, MailboxError, Mailboxes,
    Residency, Runtime, StepOutcome,
};
use wasmkeep_storage::{ActorDb, Storage, StorageConfig};
use wasmkeep_wit::wasmkeep::actor::actors::{
    CallError, Request as CallRequest, Response as CallResponse,
};
use wasmkeep_wit::wasmkeep::actor::types::{Rows, SqlError, SqlErrorKind, Value};
use wasmkeep_wit::wasmkeep::actor::{actors, alarms, calls, env, storage, types};
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
    ALARM_HANDOFF, CALLER, CHAIN, Cluster, ClusterConfig, LeaseHold, OwnershipError, SERVED_BY,
    decode_address, encode_address,
};
use crate::http::text_response;
use crate::replica::{Replica, ReplicaError};
use crate::websocket::{Opener, Outgoing, SendError, Sockets, close_frame};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use wasmkeep_cluster::state_root::StateRoot;
use wasmkeep_wit::wasmkeep::actor::websockets::{
    self, Message as WsMessageWit, SendError as WsSendError,
};

mod alarm_delivery;
mod blob_sweeps;
mod classes;
mod collection;
pub(crate) mod compaction;
mod deletes;
mod deploys;
mod disk;
mod drain;
mod expiry;
mod held;
mod host;
mod load;
mod memory;
mod moving;
mod requests;
mod restores;
mod routing;
mod socket_owners;
mod sockets;
mod start;
mod state;
mod timing;
mod upgrade;
mod uploads;

#[allow(
    clippy::wildcard_imports,
    reason = "the parts of one module, split by topic"
)]
use self::{
    alarm_delivery::*, classes::*, collection::*, disk::*, host::*, routing::*, sockets::*,
    state::*,
};

use self::held::{Barrier, Dropped, Held};

pub(crate) use self::routing::is_wasmkeep_header;
pub(crate) use self::routing::{BodyBudget, refuse_batch};
pub use self::uploads::hold_node_upload_budget;
pub(crate) use self::uploads::{UPLOAD_SHARES, upload_budget_in_use};

/// `--max-unsent-age`'s default and bounds (ADR 0034, section 3).
pub(crate) const DEFAULT_UNSENT_AGE: Duration = Duration::from_millis(500);
/// The shortest `--max-unsent-age`.
pub(crate) const MIN_UNSENT_AGE: Duration = Duration::from_millis(100);
/// The longest `--max-unsent-age`.
pub(crate) const MAX_UNSENT_AGE: Duration = Duration::from_secs(5);

/// How long a node trusts "node X holds this actor" without asking the
/// bucket again.
pub(crate) const HINT_MS: u64 = 1_000;

/// Let ordinary hibernation finish before refusing a cold activation.
const ADMISSION_WAIT: Duration = Duration::from_millis(250);

/// Meta key holding the component version `on-init` last succeeded for.
const INIT_VERSION_KEY: &str = "init_version";

/// After a stop's deadline, how long its steps before a drop that gave up
/// there get to return (to count what they dropped and hand their holds to
/// the pending releases) before the node lease is released (ADR 0034,
/// section 10).
const STOP_STEP_EXTRA: Duration = Duration::from_millis(500);

/// What `publish_alarm` left the actor's alarm index entry as.
struct Published {
    /// The entry the database now names (`None`: no alarm indexed).
    named: Option<Entry>,
    /// The entry it named before, to remove once the event is replicated.
    replaced: Option<Entry>,
}

/// Settings for the actor service. Its `Debug` shows the names of the
/// configuration and secrets it holds, never a value.
#[derive(Clone)]
pub struct ActorConfig {
    /// Owner and application socket limits.
    pub socket_limits: crate::SocketLimits,
    /// Share this allowance across every service in the same process.
    pub socket_budget: Arc<crate::SocketBudget>,
    /// Tenant all actors belong to (one tenant per process for now; see
    /// ROADMAP.md).
    pub tenant: String,
    /// The app, within the tenant, whose actors these are (ADR 0022): its
    /// state lives under `state/<tenant>/<app>/`. The implicit app
    /// `default` for classes given on the command line; a cluster's
    /// [`ClusterConfig::app`], when set, wins.
    pub app: String,
    /// Where actor databases live.
    pub storage: StorageConfig,
    /// Queue size and hibernation timeout.
    pub mailbox: MailboxConfig,
    /// On Linux/glibc, sample residents plus sockets at this interval and
    /// trim on a drop to half the peak since the last trim, or zero. Zero
    /// disables checks. Runs on a blocking worker; peak, serialization and
    /// a minimum 60-second cooldown are shared across the process.
    pub idle_memory_trim: Duration,
    /// Wall-clock limit per event, including time awaiting I/O.
    pub wall_per_event: Duration,
    /// First retry delay after a failed `on-alarm`; doubles per failure.
    pub alarm_retry_initial: Duration,
    /// Longest retry delay.
    pub alarm_retry_max: Duration,
    /// `--no-alarms`: never fire an actor's alarms on this node (no timer
    /// delivery, no sweep of the bucket's alarm index, no re-arming at
    /// start). Alarms stay recorded and pending, so a node without this
    /// setting fires them later. For a node on a restored copy, which must
    /// not run the app's side effects a second time.
    pub no_alarms: bool,
    /// The process's resident pool when it serves several apps (ADR 0038,
    /// section 8): this app's residents count against it, and
    /// `mailbox.max_resident` stays this app's own cap.
    pub resident_pool: Option<Arc<wasmkeep_runtime::ResidentPool>>,
    /// This app's floor in `resident_pool` (`min-resident`); by default the
    /// pool's limit divided by twice its apps.
    pub min_resident: Option<usize>,
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
    /// How long each class's actors may stay idle before they are deleted
    /// (ADR 0028, section 4): the deployed version's limits (in a cluster
    /// only; classes not listed never expire).
    pub expire_after_idle: std::collections::BTreeMap<String, Duration>,
    /// The classes whose writes are batched (ADR 0034, section 1): the
    /// deployed version's; every other class is strict.
    pub batched: std::collections::BTreeSet<String>,
    /// Public ingress rate limits for this app.
    pub limits: crate::apps::limits::Policies,
    /// `--max-unsent-age` (ADR 0034, section 3): how long a committed write
    /// of a batched actor may wait unsent behind the segment in flight
    /// before the actor stops starting events. 500 ms; kept within 100 ms
    /// and 5 s.
    pub max_unsent_age: Duration,
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
    /// Instance slots kept for the front's requests (`--max-front-requests`,
    /// ADR 0023); resident actors get the rest of the pool, at least one.
    /// `None`: a tenth of the pool, rounded up
    /// ([`crate::fds::default_front_requests`]).
    pub max_front_requests: Option<usize>,
    /// The front's values from this node's files (`--front-env`), for the
    /// front a deployed version brings (ADR 0023); they win, by name, over
    /// its deployed configuration and secrets.
    pub front_env: crate::env::Vars,
    /// The front of the deployed version the node starts with, if it has
    /// one (ADR 0023): linked at start, then swapped with each version.
    pub front: Option<crate::front::DeployedFront>,
    /// Version's static assets, with their app-scoped bucket source.
    pub assets: Option<crate::assets::Deployed>,
    /// Live project assets used by the development server only.
    pub local_assets: Option<Arc<crate::assets::Local>>,
    /// Shared process-wide disk cache budget.
    pub assets_cache: Arc<crate::assets::Cache>,
    /// Join a cluster: actors run only on the node holding their lease, and
    /// requests reaching another node are forwarded. `None` runs alone.
    pub cluster: Option<ClusterConfig>,
    /// The app's epoch (ADR 0030, section 2): the segment after `blobs/`
    /// in every file key, from the app's `recreated` and `restored`
    /// markers ([`crate::blobs::epoch`], as
    /// [`crate::apps::AppRef::prepare_local`] returns it). By default the
    /// epoch of an app with neither marker.
    pub blob_epoch: String,
    /// How this app runtime makes links (ADR 0030, section 4): the
    /// process's start proof decides whether downloads are presigned, and
    /// the node file or the flags give node links' base.
    pub blob_links: crate::blobs::links::LinkSettings,
    /// One file's largest size, streamed (`--max-blob-bytes`): by default
    /// [`crate::blobs::DEFAULT_MAX_FILE`], at most
    /// [`crate::blobs::MAX_FILE_CEILING`].
    pub max_blob_bytes: u64,
    /// The app's stored-bytes limit (`max-app-blob-bytes`, ADR 0030,
    /// section 7): writes and upload links are refused with
    /// `quota-exceeded` while the last hourly count is over it. `None`: no
    /// limit.
    pub max_app_blob_bytes: Option<u64>,
    /// How often the app's files are counted on a node without a bucket
    /// (an hour; on a bucket the cluster's `blob_epochs_every`). Tests
    /// shorten it.
    pub blob_count_every: Duration,
    /// How long a WebSocket client may stay silent before the server pings
    /// it: 25 s, under a load balancer's usual 60 s idle timeout. Tests
    /// shorten it.
    pub websocket_ping_after: Duration,
    /// How long a WebSocket the server closed waits for the client's close
    /// frame before it is dropped: 5 s. Until then it delivers nothing and
    /// keeps its slot. Tests shorten it.
    pub websocket_close_wait: Duration,
}

impl std::fmt::Debug for ActorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Every field named, so a new one is a compile error here until
        // someone decides whether its values may be shown.
        let Self {
            socket_limits,
            socket_budget,
            tenant,
            app,
            storage,
            mailbox,
            idle_memory_trim,
            wall_per_event,
            alarm_retry_initial,
            alarm_retry_max,
            no_alarms,
            resident_pool,
            min_resident,
            egress,
            guest_log,
            env,
            config,
            secrets,
            requires,
            expire_after_idle,
            batched,
            limits,
            max_unsent_age,
            access,
            flag_access,
            deploy_seq,
            max_front_requests,
            front_env,
            front,
            assets,
            local_assets,
            assets_cache,
            cluster,
            blob_epoch,
            blob_links,
            max_blob_bytes,
            max_app_blob_bytes,
            blob_count_every,
            websocket_ping_after,
            websocket_close_wait,
        } = self;
        let names = |vars: &crate::env::Vars| vars.keys().cloned().collect::<Vec<_>>();
        let config: std::collections::BTreeMap<_, _> = config
            .iter()
            .map(|(class, vars)| (class, names(vars)))
            .collect();
        f.debug_struct("ActorConfig")
            .field("socket_limits", socket_limits)
            .field("socket_budget", socket_budget)
            .field("tenant", tenant)
            .field("app", app)
            .field("storage", storage)
            .field("mailbox", mailbox)
            .field("idle_memory_trim", idle_memory_trim)
            .field("wall_per_event", wall_per_event)
            .field("alarm_retry_initial", alarm_retry_initial)
            .field("alarm_retry_max", alarm_retry_max)
            .field("no_alarms", no_alarms)
            .field("resident_pool", resident_pool)
            .field("min_resident", min_resident)
            .field("egress", egress)
            .field("guest_log", guest_log)
            // Counts per class only.
            .field("env", env)
            .field("config", &config)
            // Names per class only.
            .field("secrets", secrets)
            .field("requires", requires)
            .field("expire_after_idle", expire_after_idle)
            .field("batched", batched)
            .field("limits", limits)
            .field("max_unsent_age", max_unsent_age)
            .field("access", access)
            .field("flag_access", flag_access)
            .field("deploy_seq", deploy_seq)
            .field("max_front_requests", max_front_requests)
            .field("front_env", &names(front_env))
            // Its configuration's names only.
            .field("front", front)
            .field("assets", assets)
            .field("local_assets", local_assets)
            .field("assets_cache", assets_cache)
            .field("cluster", cluster)
            .field("blob_epoch", blob_epoch)
            .field("blob_links", blob_links)
            .field("max_blob_bytes", max_blob_bytes)
            .field("max_app_blob_bytes", max_app_blob_bytes)
            .field("blob_count_every", blob_count_every)
            .field("websocket_ping_after", websocket_ping_after)
            .field("websocket_close_wait", websocket_close_wait)
            .finish()
    }
}

impl ActorConfig {
    /// Defaults with the given data directory.
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket_limits: crate::SocketLimits::default(),
            socket_budget: crate::SocketBudget::new(50_000, 50_000),
            tenant: "default".into(),
            app: crate::cluster::IMPLICIT_APP_NAME.into(),
            storage: StorageConfig::new(data_dir),
            mailbox: MailboxConfig::default(),
            idle_memory_trim: Duration::from_secs(60),
            wall_per_event: Duration::from_secs(30),
            alarm_retry_initial: Duration::from_secs(1),
            alarm_retry_max: Duration::from_secs(300),
            no_alarms: false,
            resident_pool: None,
            min_resident: None,
            cluster: None,
            egress: crate::egress::EgressPolicy::default(),
            guest_log: crate::guest_log::GuestLog::default(),
            env: crate::env::ActorEnv::default(),
            config: std::collections::BTreeMap::new(),
            secrets: crate::secrets::SecretSet::default(),
            requires: std::collections::BTreeMap::new(),
            expire_after_idle: std::collections::BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
            max_unsent_age: DEFAULT_UNSENT_AGE,
            access: crate::access::AccessRules::default(),
            flag_access: None,
            deploy_seq: 0,
            max_front_requests: None,
            front_env: crate::env::Vars::new(),
            front: None,
            assets: None,
            local_assets: None,
            assets_cache: crate::assets::default_cache(),
            blob_epoch: crate::blobs::epoch(None, None),
            blob_links: crate::blobs::links::LinkSettings::default(),
            max_blob_bytes: crate::blobs::DEFAULT_MAX_FILE,
            max_app_blob_bytes: None,
            blob_count_every: Duration::from_secs(60 * 60),
            websocket_ping_after: crate::websocket::PING_AFTER,
            websocket_close_wait: crate::websocket::CLOSE_WAIT,
        }
    }
}

/// Routes events to actors. Cheap to clone.
#[derive(Clone)]
pub struct ActorService {
    inner: Arc<ServiceInner>,
}

struct ServiceInner {
    /// Its app's metric labels ([`crate::metrics::app_labels`]).
    app_labels: Arc<str>,
    /// Its app, `tenant/app`, for log lines (ADR 0038, section 9).
    app_name: Arc<str>,
    /// Its app's shares of the process's budgets (ADR 0038, section 8),
    /// while it is served.
    _shares: Vec<crate::shares::Member>,
    restoring: std::sync::Mutex<std::collections::HashMap<ActorAddress, usize>>,
    tenant: String,
    wall: Duration,
    sockets: Arc<Sockets>,
    /// [`ActorConfig::websocket_ping_after`] and
    /// [`ActorConfig::websocket_close_wait`].
    ping_after: Duration,
    close_wait: Duration,
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
    /// This node's `--front-env` values, merged over the front's
    /// configuration and secrets (ADR 0023).
    front_env: Arc<crate::env::Vars>,
    /// The deployed version's front, swapped with its classes under the
    /// classes' lock; `None` while it has none.
    front: crate::front::FrontSlot,
    assets: std::sync::RwLock<Arc<crate::assets::State>>,
    limits: std::sync::RwLock<crate::apps::limits::Policies>,
    rate_limiter: crate::rate_limit::Limiter,
    rate_limits_pending: std::sync::atomic::AtomicBool,
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
    /// What the metrics page names this node: its app and node id.
    metrics_identity: crate::metrics::Identity,
    /// Each class's idle limit (ADR 0028, section 4), the deployed
    /// version's.
    expiry: std::sync::RwLock<std::collections::BTreeMap<String, Duration>>,
    /// The app's folder under the data directory, removed once its
    /// tombstone is seen (ADR 0028, section 5).
    local_app_dir: std::path::PathBuf,
    /// Set once the app's tombstone was seen and this node stopped serving
    /// it (ADR 0028, section 5).
    app_deleted: tokio::sync::watch::Sender<bool>,
    /// The app's files (ADR 0030), in the bucket or on this node's disk.
    blobs: Arc<crate::blobs::Blobs>,
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

    /// A node handed this node `address`'s alarm, due at `at`: remembered
    /// until the window after it is due, so the delivery here acquires the
    /// actor and never sends it on again (ADR 0041, amendment of
    /// 2026-11-22).
    fn note_handed(&self, address: &ActorAddress, at: u64) {
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
        handed.insert(address.clone(), now.max(at).saturating_add(window));
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
/// Meta key, present while the running `on-alarm` has set or cleared the
/// alarm (ADR 0009, amendment of 2026-11-20): what it holds then is the
/// handler's own, kept even at the delivered time. A row of the database,
/// not a flag in memory, so it is rolled back with the write that made it
/// (a `set-alarm` inside a transaction the handler rolls back, or leaves
/// open, never happened). Removed after the event, and before the next
/// `on-alarm` (one a crash or a mid-event upload left behind).
pub(crate) const ALARM_TOUCHED_KEY: &str = "alarm_touched";
/// Meta key: the exact alarm index entry listed for this actor (empty:
/// none; [`alarm_index::to_meta`]), so whichever node owns it next keeps it
/// while the alarm stands and removes exactly it when the alarm changes.
const ALARM_INDEXED_KEY: &str = "alarm_indexed";

/// Meta key a new owner writes, and uploads, before it removes index
/// entries an earlier owner wrote (`fence`): the generation it holds.
const OWNER_FENCE_KEY: &str = "owner_fence";
/// Meta key: the highest deploy sequence that ran an event on this actor.
const DEPLOY_SEQ_KEY: &str = "deploy_seq";

/// Meta key holding the last operator delete carried out on this actor:
/// its request's time (ms, the bucket's clock) and id, `<ms> <id>` (ADR
/// 0028). Written into the empty database a delete leaves, and carried
/// over by later deletes, so a request listed again is never carried out
/// twice.
const OPERATOR_DELETE_KEY: &str = "operator_delete";
const OPERATOR_RESTORE_KEY: &str = "operator_restore";
/// The actor's blob incarnation (ADR 0030, section 2): drawn by each
/// delete and written in the empty database its reset carries.
const BLOB_INCARNATION_KEY: &str = "blob_incarnation";

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

    /// The tenant of this node's actors.
    pub(crate) fn tenant(&self) -> &str {
        &self.inner.tenant
    }

    /// How long an `actors.call` may wait at most: the actors' wall per
    /// event.
    pub(crate) fn call_wall(&self) -> Duration {
        self.inner.wall
    }

    /// The actor `address` names, for a call from the front.
    pub(crate) fn front_target(&self, address: &str) -> Result<ActorAddress, CallError> {
        self.inner.call_target(&self.inner.tenant, address)
    }

    /// `actors.call` from the front (ADR 0023): an inside call, with an
    /// empty chain, given up after `limit`.
    /// The response takes its bytes from `budget` (shared by the entries
    /// of a `calls.call-many`, ADR 0025).
    pub(crate) async fn front_call(
        &self,
        target: ActorAddress,
        request: CallRequest,
        limit: Duration,
        budget: &BodyBudget,
    ) -> Result<CallResponse, CallError> {
        let chain: Chain = Arc::from(Vec::new());
        Arc::clone(&self.inner)
            .call(&Caller::Front, &chain, target, request, limit, budget)
            .await
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
        self.stop_actors(limit, true, 1012, "node shutting down")
            .await;
    }

    /// [`Self::shutdown`], its sockets closed with `code` and `reason`.
    /// `flush`: each actor's step before its drop flushes, by the stop's
    /// deadline (a clean stop), rather than dropping (an app deleted);
    /// ADR 0034, sections 6 and 10.
    async fn stop_actors(&self, limit: Duration, flush: bool, code: u16, reason: &str) {
        let started = tokio::time::Instant::now();
        let deadline = started + limit;
        let resident = self.inner.mailboxes.resident();
        // Batched holds' events end at the deadline and their waits stop
        // there, so the steps count what they drop before the releases
        // below (ADR 0034, section 10).
        if let Some(cluster) = &self.inner.cluster {
            cluster.stop_by(deadline);
        }
        self.inner.sockets.close_all(code, reason);
        self.inner.mailboxes.close_by(Some(deadline), flush);
        if !self
            .inner
            .mailboxes
            .drained(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            // Steps that gave up at the deadline count what they dropped
            // and hand their holds to the pending releases: a moment for
            // that, before the releases and the node lease. Only when a
            // step is still running (never for a strict class, whose step
            // ends at once).
            let extra = tokio::time::Instant::now() + STOP_STEP_EXTRA;
            while (self.inner.mailboxes.steps_running() > 0 || self.inner.mailboxes.resident() > 0)
                && tokio::time::Instant::now() < extra
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tracing::warn!(
                still_running = self.inner.mailboxes.resident(),
                "shutdown: some actors did not finish in time"
            );
        }
        if let Some(cluster) = &self.inner.cluster {
            // Records parked by rooms that hibernated with sockets open
            // (ADR 0024): their sockets are closed; released like the rest.
            // Bounded: the node lease's `released` write frees what is left.
            let _ =
                tokio::time::timeout(Duration::from_secs(3), cluster.release_all_parked("stop"))
                    .await;
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

    /// Each class's idle limit from now on (ADR 0028), in seconds: the
    /// deployed version's, swapped in with it.
    pub fn set_expiry(&self, limits: &std::collections::BTreeMap<String, u64>) {
        let limits = limits
            .iter()
            .map(|(c, s)| (c.clone(), Duration::from_secs(*s)))
            .collect();
        *self
            .inner
            .expiry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = limits;
    }

    /// A node following its app's deploys: no actor is acquired until a
    /// check for the app's tombstone made while its lease was valid finds
    /// none ([`Self::app_checked`]; ADR 0028, section 5).
    pub fn expect_app_check(&self) {
        if let Some(c) = &self.inner.cluster {
            c.expect_app_check();
        }
    }

    /// Whether acquisitions wait for a tombstone check (at start, and after
    /// a lapse of the node lease).
    pub fn app_unchecked(&self) -> bool {
        self.inner
            .cluster
            .as_ref()
            .is_some_and(|c| c.app_unchecked())
    }

    /// Answers a node link (`/_wasmkeep/blobs/v1/<token>`, ADR 0030,
    /// section 4) under this app runtime's key.
    pub(crate) async fn blob_link(
        &self,
        req: hyper::Request<hyper::body::Incoming>,
    ) -> Response<HyperOutgoingBody> {
        let deleted = *self.inner.app_deleted.borrow();
        crate::blobs::serve::serve(&self.inner.blobs, deleted, req).await
    }

    /// The app's files (ADR 0030), for the front's `blobs` calls.
    pub(crate) fn blobs(&self) -> Option<Arc<crate::blobs::Blobs>> {
        Some(Arc::clone(&self.inner.blobs))
    }

    /// The app's folder under the data directory.
    pub fn local_app_dir(&self) -> &std::path::Path {
        &self.inner.local_app_dir
    }

    /// A check for the tombstone made while the lease was valid found none.
    pub fn app_checked(&self) {
        if let Some(c) = &self.inner.cluster {
            c.app_checked();
        }
    }

    /// Whether this node would send to the node `node` now: in its view of
    /// the cluster, its heartbeat live here, and over TLS the pins at its
    /// address trusted. False outside a cluster. For tests and diagnostics.
    #[doc(hidden)]
    pub fn reaches_node(&self, node: &str) -> bool {
        self.inner.cluster.as_ref().is_some_and(|c| c.reaches(node))
    }

    /// Whether node `node` is in this node's view of the cluster, live or
    /// not, as its last reads left it; unlike [`Self::reaches_node`], asks
    /// for no read. False outside a cluster. For tests and diagnostics.
    #[doc(hidden)]
    pub fn knows_node(&self, node: &str) -> bool {
        self.inner.cluster.as_ref().is_some_and(|c| c.knows(node))
    }

    /// The idle passes this node remembers (ADR 0042, section 3), due
    /// first: each record key and the generation its release kept. Empty
    /// outside a cluster. For tests and diagnostics.
    #[doc(hidden)]
    pub fn idle_passes_queued(&self) -> Vec<(String, u64)> {
        self.inner.cluster.as_ref().map_or_else(Vec::new, |c| {
            c.idle_passes
                .entries()
                .into_iter()
                .map(|e| (e.key, e.generation))
                .collect()
        })
    }

    /// Tests: stops this node taking idle passes from its queue (`true`),
    /// or lets it again.
    #[doc(hidden)]
    pub fn pause_idle_passes(&self, paused: bool) {
        if let Some(c) = &self.inner.cluster {
            c.idle_passes.pause(paused);
        }
    }

    /// Tests: this node's idle passes delete in batches of `batch` and
    /// stop as at their deadline after `batches` batches (`None`: as
    /// built), so a short history spans several passes.
    #[doc(hidden)]
    pub fn limit_idle_passes(&self, limit: Option<(usize, usize)>) {
        if let Some(c) = &self.inner.cluster {
            c.idle_passes.limit(limit);
        }
    }

    /// Tests: makes every remembered idle pass due now.
    #[doc(hidden)]
    pub fn idle_passes_due_now(&self) {
        if let Some(c) = &self.inner.cluster {
            let now = now_ms();
            for mut e in c.idle_passes.entries() {
                e.due_ms = now;
                c.idle_passes.push(e);
            }
        }
    }

    /// Tests: gives every remembered idle pass the version its record has
    /// now, as an `ETag` that is a hash of the body gives a recreated
    /// app's record at the same generation (ADR 0042, constraint 7).
    #[doc(hidden)]
    pub async fn idle_passes_take_current_versions(&self) {
        use object_store::ObjectStoreExt as _;
        let Some(c) = &self.inner.cluster else {
            return;
        };
        for mut e in c.idle_passes.entries() {
            let path =
                object_store::path::Path::from(format!("{}{}", c.state_root().prefix(), e.key));
            if let Ok(meta) = c.store.head(&path).await {
                e.version = wasmkeep_cluster::lease::Version(format!(
                    "{}\n{}",
                    meta.e_tag.unwrap_or_default(),
                    meta.version.unwrap_or_default()
                ));
                c.idle_passes.push(e);
            }
        }
    }

    /// Whether the node lease is valid now (always, outside a cluster).
    pub fn lease_valid_now(&self) -> bool {
        self.inner
            .cluster
            .as_ref()
            .is_none_or(|c| c.lease_valid_now())
    }

    /// The app's tombstone was seen (ADR 0028, section 5): this node stops
    /// serving it for good. No actor is acquired again; its WebSockets are
    /// closed with 1001, its events and alarms refused, each actor
    /// finishes its queued events and is released; then its node lease is
    /// released (it is no longer live, which `app delete` waits for), and
    /// its local files of the app are removed. [`Self::app_deleted`]
    /// resolves after.
    pub async fn stop_for_deleted_app(&self) {
        if *self.inner.app_deleted.borrow() {
            return;
        }
        tracing::error!("the app was deleted (wasmkeep app delete): this node stops serving it");
        if let Some(c) = &self.inner.cluster {
            c.app_tombstoned();
        }
        self.begin_shutdown();
        self.stop_actors(crate::http::SHUTDOWN_GRACE, false, 1001, "app deleted")
            .await;
        // No new database is opened past the closed mailboxes.
        let dir = self.inner.local_app_dir.clone();
        let removed = tokio::task::spawn_blocking(move || {
            if dir.exists() {
                std::fs::remove_dir_all(&dir)
            } else {
                Ok(())
            }
        })
        .await;
        if !matches!(removed, Ok(Ok(()))) {
            tracing::warn!(dir = %self.inner.local_app_dir.display(), "removing this node's local files of the deleted app failed; remove them before reusing this data directory");
        }
        self.inner.app_deleted.send_replace(true);
    }

    /// Resolves once this node stopped serving its app because it was
    /// deleted ([`Self::stop_for_deleted_app`]); never otherwise. The
    /// server then shuts down.
    pub async fn app_deleted(&self) {
        let mut rx = self.inner.app_deleted.subscribe();
        let _ = rx.wait_for(|d| *d).await;
    }

    /// Live nodes on the same bucket prefix of the version before the
    /// implicit app (ADR 0022), which keep actor state in the layout before
    /// 0.4.0. Empty outside a cluster.
    ///
    /// # Errors
    /// The heartbeats could not be read.
    pub async fn older_nodes(&self) -> Result<Vec<String>, String> {
        match &self.inner.cluster {
            Some(c) => c.older_nodes().await,
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

    /// Judges the signature of `req` once, for every handler after the
    /// router (`Cluster::judge_hop`): why it must answer 503 rather than
    /// run as a client's request, if it must.
    pub(crate) fn judge_hop<B>(&self, req: &mut hyper::Request<B>) -> Option<&'static str> {
        self.inner.cluster.as_ref()?.judge_hop(req)
    }

    /// Whether every actor has ended, its resources closed, within `limit`.
    pub async fn drained(&self, limit: Duration) -> bool {
        self.inner.mailboxes.drained(limit).await
    }

    /// After [`Self::shutdown`]: its node id leaves its process, so the
    /// app's runtime may start again in place (ADR 0038, section 3).
    pub fn leave_process(&self) {
        if let Some(c) = &self.inner.cluster {
            c.leave_process();
        }
    }

    /// Its app's metric labels ([`crate::metrics::app_labels`]).
    pub(crate) fn app_labels(&self) -> Arc<str> {
        Arc::clone(&self.inner.app_labels)
    }

    /// Number of actors currently resident in memory.
    pub fn resident(&self) -> usize {
        self.inner.mailboxes.resident()
    }

    /// Process socket sessions, including relays and upgrades in progress.
    pub fn socket_sessions(&self) -> usize {
        self.inner.sockets.budget.in_use()
    }

    /// What the metrics page reads at scrape time.
    pub fn gauges(&self) -> crate::metrics::Gauges {
        crate::metrics::Gauges {
            socket_sessions: self.socket_sessions(),
            socket_requested: self.inner.sockets.budget.requested(),
            socket_effective: self.inner.sockets.budget.effective(),
            socket_actor_requested: self.inner.sockets.limits.per_actor,
            socket_actor_effective: self
                .inner
                .sockets
                .limits
                .per_actor
                .min(self.inner.sockets.limits.per_app)
                .min(self.inner.sockets.budget.effective()),
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

    /// A peer's hello (ADR 0011, amendment of 2026-11-14): 200 if it is a
    /// node's signed call, 403 otherwise, 404 outside a cluster.
    pub(crate) async fn hello(
        &self,
        req: hyper::Request<hyper::body::Incoming>,
    ) -> hyper::Response<wasmtime_wasi_http::p2::body::HyperOutgoingBody> {
        use http_body_util::BodyExt as _;
        let Some(cluster) = self.inner.cluster.as_ref() else {
            return crate::http::text_response(hyper::StatusCode::NOT_FOUND, "not found\n");
        };
        let on_cluster_port = req
            .extensions()
            .get::<crate::http::ArrivedOnClusterPort>()
            .is_some();
        let (parts, body) = req.into_parts();
        // A hello's body is empty: anything longer is not one.
        let body = match http_body_util::Limited::new(body, 64).collect().await {
            Ok(b) => b.to_bytes(),
            Err(_) => {
                return crate::http::text_response(hyper::StatusCode::FORBIDDEN, "not a hello\n");
            }
        };
        match cluster.hello_received(&parts.method, &parts.headers, &body, on_cluster_port) {
            hyper::StatusCode::OK => crate::http::text_response(hyper::StatusCode::OK, "hello\n"),
            status => crate::http::text_response(status, "not a hello\n"),
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
    /// After a `delete-self-durable` that returned `ok` (ADR 0028): the
    /// actor is deleted, and storage calls are refused. In a cluster, the
    /// empty database its reset carried, for the snapshot at it once the
    /// event ends.
    deleted: Option<Option<wasmkeep_storage::wal::Changes>>,
    /// On one node: a `delete-self-durable` closed the database and could
    /// not remove its files; a retry removes them, storage stays refused.
    delete_retry: bool,
    /// Where the actor's files are, for `delete-self-durable`.
    deleter: Option<(Storage, ActorAddress)>,
    /// While `on-alarm` runs: `set-alarm` and `clear-alarm` record that the
    /// handler touched the alarm (`ALARM_TOUCHED_KEY`), and `get-alarm`
    /// answers none until it has (the delivered alarm is consumed).
    delivering_alarm: bool,
    call: Option<CallContext>,
    sockets: Arc<Sockets>,
    /// Replication, so `exec-durable` can wait for the bucket.
    replica: Option<SharedReplica>,
    replica_stop: Option<tokio::sync::watch::Receiver<Option<crate::replica::Stop>>>,
    /// Sends the guest's outbound HTTP under the node's egress policy.
    egress: crate::egress::EgressHooks,
    /// The class's `env` values.
    env: Arc<crate::env::Vars>,
    /// In a cluster: where to list a pending alarm before `exec-durable`
    /// ships it, so no alarm reaches the bucket unindexed.
    alarm_index: Option<(Arc<Cluster>, ActorAddress)>,
    /// The running event's, when its hold is batched (ADR 0034): where
    /// `flush` stops waiting, and whom its uploads tell when they land.
    batched: Option<BatchedEvent>,
    /// The actor's files (ADR 0030): its namespace, and its hold for writes.
    blob: crate::blobs::host::Caller,
}

crate::blobs::host::impl_blobs_host!(ActorHost);

/// What a batched hold's event gives its `flush` (ADR 0034, section 4).
struct BatchedEvent {
    /// The event's wall less one second.
    flush_until: tokio::time::Instant,
    completion: uploads::Completion,
}

type ServiceSlot = Arc<std::sync::OnceLock<std::sync::Weak<ServiceInner>>>;
type Chain = Arc<[ActorAddress]>;

/// The class part of the front's caller value, `<tenant>/_front/main` (ADR
/// 0023). Never a class: class names start with a lowercase letter.
pub(crate) const FRONT_CLASS: &str = "_front";
/// The name part: the one front an app has.
pub(crate) const FRONT_NAME: &str = "main";

/// Who makes an `actors.call`: an actor, or the app's front (ADR 0023).
/// The front is never in a call chain: it cannot be called back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    Actor(ActorAddress),
    Front,
}

impl Caller {
    /// The `x-wasmkeep-caller` value: the actor's address (its name
    /// percent-encoded), or `<tenant>/_front/main`.
    pub(crate) fn value(&self, tenant: &str) -> String {
        match self {
            Self::Actor(a) => encode_address(a),
            Self::Front => format!("{tenant}/{FRONT_CLASS}/{FRONT_NAME}"),
        }
    }
}

/// What an instance needs to make calls during the current event.
#[derive(Clone)]
struct CallContext {
    service: Option<std::sync::Weak<ServiceInner>>,
    me: ActorAddress,
    /// Actors waiting on this event; calling any of them would deadlock.
    chain: Chain,
    /// When this event's calls give up (ADR 0025): its wall less the
    /// margin it keeps to answer with what they returned.
    deadline: tokio::time::Instant,
}

/// What an actor's event keeps of its wall after its calls give up, so it
/// can still answer: the front's margin (ADR 0023), or a fifth of a wall
/// shorter than five of them.
fn call_margin(wall: Duration) -> Duration {
    crate::front::CALL_MARGIN.min(wall / 5)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod batched_state_tests;
#[cfg(test)]
mod idle_state_tests;
