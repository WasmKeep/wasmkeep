//! Running as one node of a cluster: this node's lease and actors'
//! ownership records on the bucket (ADR 0020), heartbeats, and forwarding
//! requests for actors another node owns.
//!
//! Ownership is decided only by leases (`wasmkeep_cluster::node_lease`): an actor
//! runs an event only while this node holds a valid grant for it. Heartbeats
//! only tell us where to forward.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cluster_tls::{ClusterConnector, ClusterTls};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use wasmkeep_cluster::alarm_index::{self, Entry};
use wasmkeep_cluster::control::{self, Control};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::membership::{DrainState, InvalidNodeId, Membership, NODES_PREFIX, NodeInfo};
use wasmkeep_cluster::node_lease::Owners;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::state_root::{Family, StateRoot};
use wasmkeep_runtime::ActorAddress;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

use crate::http::{ArrivedOnClusterPort, OriginalPath, text_response};

mod failover_wait;
mod forwarding;
mod handoff;
mod hello;
mod identity;
mod lease_hold;
mod node_lease_task;
mod process;
mod signing;
mod view;
pub(crate) use hello::HELLO_PATH;

pub use failover_wait::failover_wait_default;
pub(crate) use failover_wait::{FailoverWait, Kept, Ticket, Waited};
pub(crate) use forwarding::*;
pub(crate) use handoff::*;
pub(crate) use identity::*;
pub(crate) use lease_hold::*;
pub(crate) use node_lease_task::lease_attempt;
use node_lease_task::{lease_every, node_lease_loop};
pub use process::NodeProcess;
pub(crate) use signing::*;
pub use view::older_nodes_advice;
pub(crate) use view::*;

/// On a signed node call: the body is an alarm handed off by a draining
/// node (ADR 0014), not a request for the actor. Stripped from anything a
/// guest or a client sends.
pub(crate) const ALARM_HANDOFF: &str = "x-wasmkeep-alarm-handoff";

/// On a signed node call: take this actor now (ADR 0017), not a request
/// for it. Stripped from anything a guest or a client sends.
pub(crate) const TAKE: &str = "x-wasmkeep-take";

/// On a take's 409: the node that holds the actor.
pub(crate) const HOLDER: &str = "x-wasmkeep-holder";

/// Header marking a request already forwarded once; it is never forwarded
/// again, so nodes that disagree about ownership cannot bounce it forever.
pub(crate) const FORWARDED: &str = "x-wasmkeep-forwarded";
/// Header naming the node that served an actor request.
pub(crate) const SERVED_BY: &str = "x-wasmkeep-node";
/// Header proving a call comes from another node: `v4 <ms> <nonce>
/// <body-sha256> <hmac>`, an HMAC-SHA256 with the cluster secret over the
/// timestamp, a nonce unique to this send, the node it is for, method, path,
/// the body's SHA-256, caller, chain and app; each accepted once (the nonce makes
/// two identical calls in the same millisecond distinct). The receiver checks the body against it before the actor
/// sees it. Only with it are [`CALLER`] and [`CHAIN`] believed; it
/// never reaches a guest. The secret itself never travels.
pub(crate) const INTERNAL: &str = "x-wasmkeep-internal";
/// How far a signature's timestamp may be from this node's clock.
const SIGNATURE_WINDOW_MS: u64 = 60_000;
/// On a request to the front: the host saw a WebSocket upgrade (ADR
/// 0027). On a signed upgrade relayed to the actor's owner: the front
/// handed it over; signed, so no call can be replayed as one.
pub(crate) const UPGRADE_MARK: &str = "x-wasmkeep-upgrade";
/// The calling actor (`actors.call`).
pub(crate) const CALLER: &str = "x-wasmkeep-caller";
/// Actors waiting on a cross-node call, outermost first, one per value.
pub(crate) const CHAIN: &str = "x-wasmkeep-chain";
/// On every node-to-node hop (ADR 0022): the app (`tenant/app`) of the
/// node that sent it, set by that node and never by a client. Signed on
/// signed calls; a node refuses a hop that names another app, or none (a
/// node of an older version). Stripped from anything a guest or a client
/// sends, and never reaches a guest.
pub(crate) const APP: &str = "x-wasmkeep-app";

/// Readiness while a live node lacks `multi-app` and the process serves
/// several apps (ADR 0038, section 10).
pub(crate) const MULTI_APP_MISSING: &str =
    "a live node of an older version (no multi-app) runs in this cluster";

/// What only nodes send each other on a hop, removed before a guest sees
/// the request (a call, or a WebSocket's open): the signature, the
/// sender's proof (ADR 0038), the caller and chain it vouches for, and a
/// hand-off's upgrade mark.
pub(crate) const HOP_PROOFS: [&str; 5] = [INTERNAL, SENDER, CALLER, CHAIN, UPGRADE_MARK];
/// The app name of nodes given classes on the command line (`serve
/// --actor`, `dev`): with their tenant, `default`, the implicit app
/// `default/default` (ADR 0022).
pub const IMPLICIT_APP_NAME: &str = "default";
/// Where the cluster secret lives in the bucket.
const SECRET_KEY: &str = "cluster/secret";
/// Longest wait for the owner's response headers when forwarding.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

/// How this node joins a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's id, `[a-z0-9-]{1,64}`.
    pub node_id: String,
    /// Base URL other nodes forward to, e.g. `http://10.0.0.11:8080`.
    pub advertise: String,
    /// The shared bucket (S3, R2, MinIO, ...).
    pub store: Arc<dyn ObjectStore>,
    /// Key prefix in the bucket, so several clusters can share one.
    pub prefix: String,
    /// Lease timing.
    pub lease: LeaseConfig,
    /// How often this node publishes its heartbeat and refreshes its view.
    pub heartbeat: Duration,
    /// How often this node looks in the bucket for alarms whose owner did
    /// not fire them (it died): those it delivers itself.
    pub alarm_sweep: Duration,
    /// How often this node reads the ownership record of each actor that
    /// has WebSockets here but no hold (it hibernated with them open), and
    /// closes them with 1012 if another node holds it: 60 s. Tests shorten
    /// it.
    pub socket_sweep: Duration,
    /// The deployed app this node serves (`tenant/app`), or empty for
    /// classes given on the command line, which run as the implicit app
    /// `<tenant>/default` (`default/default` for `serve --actor`, ADR 0022).
    /// Every hop between nodes names it, and a node refuses one for
    /// another app. Two apps of one tenant would share actor keys in the
    /// bucket, so nodes refuse to mix them.
    pub app: String,
    /// A cluster listener with mutual TLS (ADR 0011), or `None` (plain HTTP
    /// between nodes only). With it, the node makes a TLS identity in
    /// memory at start, publishes the listener and its pin in its
    /// heartbeat, and reaches peers that do the same over TLS. Serve it
    /// with [`crate::serve_with_cluster_listener`].
    pub cluster_tls: Option<ClusterTlsConfig>,
    /// Whether plain HTTP between nodes is still used (ADR 0011, phase B).
    /// [`ClusterPlaintext::Refuse`] needs [`ClusterConfig::cluster_tls`].
    pub cluster_plaintext: ClusterPlaintext,
    /// Whether the tenant's bucket state was migrated by `cluster migrate`
    /// (`cluster/migrated/<tenant>` exists, as the start check found; ADR
    /// 0022). Only then are the node's local databases of the layout
    /// before 0.4.0 put aside, the bucket being their truth; otherwise
    /// they are the only copy (a node joining with single-node data), and
    /// move into the app's root.
    pub migrated: bool,
    /// How long an actor's history stays in the bucket once garbage
    /// collection could delete it (ADR 0012): objects younger than this
    /// are kept for a later pass. Not applied after a delete.
    pub history_retention: Duration,
    /// The least time between a periodic snapshot whose upload failed and
    /// the next try: 30 s, the longest bounded wait of a caller queued
    /// behind the actor. Tests shorten it.
    pub snapshot_retry_min: Duration,
    /// The time the reads of a restore from the bucket have together,
    /// whatever it restores: 30 s.
    /// A second per megabyte and a second per eight objects of the
    /// history it reads come on top, and what other restores' bytes in
    /// flight add to its reads' limits is not counted. Tests shorten it.
    pub restore_budget: Duration,
    /// This node reads the app's secrets from the bucket (heartbeat line
    /// 10, ADR 0016); `false` with `--no-secrets`.
    pub reads_secrets: bool,
    /// How long a request for a dead node's actor is kept until the actor
    /// can be taken over, instead of a 503 at once (a GET or HEAD without
    /// a body, or a WebSocket upgrade, arriving from outside): `None` for
    /// [`failover_wait_default`], derived from the lease timing; zero
    /// turns it off.
    pub failover_wait: Option<Duration>,
    /// Requests kept that way at once on this node; more get the 503 at
    /// once. `serve` sets a quarter of its connection limit.
    pub failover_wait_max: usize,
    /// How often the collecting node runs an idle-expiry pass (ADR 0028,
    /// section 4): ten minutes; tests shorten it.
    pub expiry_every: Duration,
    /// The node follows its app's deploys (`serve --app`): it acquires no
    /// actor until it has checked for the app's tombstone with its lease
    /// valid, at start and after every lapse (ADR 0028, section 5).
    pub follows_app: bool,
    /// The process this app runs in (ADR 0038): what it shares with the
    /// process's other apps. `None`: a process of its own.
    pub process: Option<Arc<NodeProcess>>,
}

/// Plain HTTP between nodes, for a node with a cluster listener (ADR 0011).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClusterPlaintext {
    /// Phase A (the default): peers without a cluster listener are reached
    /// at their plain address, and a node's signed call is honoured on
    /// either port.
    #[default]
    Accept,
    /// Phase B: only the cluster port can carry a node's call (a signed
    /// call on the public port is treated as coming from outside), and
    /// peers without a cluster listener are not used (503).
    Refuse,
}

/// This node's cluster listener (ADR 0011).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTlsConfig {
    /// Where other nodes reach the cluster listener: `https://HOST:PORT`.
    pub advertise: String,
}

impl ClusterTlsConfig {
    /// A cluster listener other nodes reach at `advertise`
    /// (`https://HOST:PORT`).
    pub fn new(advertise: impl Into<String>) -> Self {
        Self {
            advertise: advertise.into(),
        }
    }
}

impl ClusterConfig {
    /// Hand-off defaults (lease ttl 10 s, grace 5 s; heartbeat 3 s).
    pub fn new(
        node_id: impl Into<String>,
        advertise: impl Into<String>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            advertise: advertise.into(),
            store,
            prefix: String::new(),
            lease: LeaseConfig::default_safe(),
            heartbeat: DEFAULT_HEARTBEAT,
            alarm_sweep: Duration::from_secs(10),
            socket_sweep: Duration::from_secs(60),
            app: String::new(),
            cluster_tls: None,
            cluster_plaintext: ClusterPlaintext::Accept,
            migrated: false,
            history_retention: Duration::from_secs(3600),
            snapshot_retry_min: crate::replica::SNAPSHOT_RETRY_MIN,
            restore_budget: crate::replica::RESTORE_BUDGET,
            reads_secrets: true,
            failover_wait: None,
            failover_wait_max: crate::http::ServeLimits::default().max_connections / 4,
            expiry_every: Duration::from_millis(wasmkeep_cluster::expiry::PASS_EVERY_MS),
            follows_app: false,
            process: None,
        }
    }
}

/// How long a heartbeat counts before the clock-skew allowance: three
/// heartbeat periods (ms).
fn heartbeat_ttl_ms(heartbeat: Duration) -> u64 {
    u64::try_from(heartbeat.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(3)
}

/// How long after it was written a node's heartbeat still makes it live
/// (ms): three heartbeat periods plus the lease's tolerated clock skew, 10 s
/// with the defaults ([`DEFAULT_HEARTBEAT`], [`LeaseConfig::default_safe`]).
pub fn heartbeat_horizon_ms(heartbeat: Duration, lease: &LeaseConfig) -> u64 {
    heartbeat_ttl_ms(heartbeat).saturating_add(lease.max_skew_ms())
}

/// How often a node writes its heartbeat unless configured otherwise.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(3);

/// Wall clock in milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// The live nodes (fresh heartbeats, this one included) as one node saw
/// them, which share the actors' overdue alarms by a hash of the address:
/// a dead node's alarms are recovered by all of them in parallel instead
/// of all racing for the same ones.
pub(crate) struct AlarmShards {
    /// Sorted node ids.
    nodes: Vec<String>,
    /// This node's place in `nodes`; `None` while it drains (its share is
    /// the other nodes').
    me: Option<usize>,
}

impl AlarmShards {
    /// Whether this node should deliver `address`'s overdue alarms.
    pub(crate) fn is_mine(&self, address: &ActorAddress) -> bool {
        // FNV-1a: stable across processes and Rust versions.
        let hash = encode_address(address)
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        let len = u64::try_from(self.nodes.len()).unwrap_or(1).max(1);
        self.me.is_some() && usize::try_from(hash % len).ok() == self.me
    }
}

/// Rendezvous (highest random weight) choice among `candidates` for
/// `key`: every node picks the same one for the same key, and keys spread
/// evenly; when a candidate leaves, only its keys move (ADR 0014).
pub(crate) fn rendezvous<'a>(
    key: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let weight = |node: &str| {
        // FNV-1a (stable across processes and Rust versions), then
        // SplitMix64's finalizer so similar ids still spread.
        let mut h = key
            .bytes()
            .chain([0])
            .chain(node.bytes())
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        h ^ (h >> 31)
    };
    candidates
        .into_iter()
        .max_by_key(|node| (weight(node), *node))
}

/// This node's cluster membership and lease client.
pub(crate) struct Cluster {
    pub(crate) node_id: String,
    /// What this node serves (see `NodeInfo::app`).
    serves: String,
    /// Node signatures already accepted (replay protection).
    seen: SeenSignatures,
    /// The same for take and alarm hand-off calls (ADR 0014, 0017), kept
    /// apart: a flood of other signed calls that fills `seen` cannot
    /// refuse the calls a drain and a move depend on.
    seen_control: SeenSignatures,
    /// Rare charged ingress hand-offs, isolated from actor and control calls.
    seen_ingress: SeenSignatures,
    /// When each reason for refusing a node signature was last logged
    /// (ms): one slot per reason, so junk on the public port cannot hide a
    /// real refusal.
    refusal_logged: Mutex<HashMap<&'static str, u64>>,
    /// Actors' ownership records, as this process (ADR 0020).
    pub(crate) owners: Owners<ObjectStoreBucket, SystemClock>,
    /// Pending releases, unresolved acquires, live holds, locks per actor.
    pub(crate) pending: Pending,
    membership: Membership<ObjectStoreBucket, SystemClock>,
    /// Live nodes by id, with their addresses; refreshed with every
    /// listing of the heartbeats.
    peers: RwLock<HashMap<String, Peer>>,
    /// The process's TLS identity, trust table, clients to other nodes
    /// and listing of `nodes/` (ADR 0038, section 2).
    transport: Arc<process::Transport>,
    /// Its node id's place in the process (ADR 0038): left when dropped.
    joined: process::Joined,
    /// When this node's identity first reached the bucket in a heartbeat
    /// (ms; 0: not yet): for readiness.
    published_ms: std::sync::atomic::AtomicU64,
    /// How long a heartbeat stays live (ms; its ttl plus the clock skew
    /// allowed): pins are trusted no longer than this after their
    /// heartbeat was written.
    heartbeat_horizon_ms: u64,
    /// When the app-conflict error was last logged (ms).
    conflict_logged_ms: std::sync::atomic::AtomicU64,
    /// Refuse mode (ADR 0011, phase B): only the cluster port carries
    /// node calls, and peers without a cluster listener are not used.
    refuse_plaintext: bool,
    /// When the peers skipped for having no cluster listener were last
    /// logged (ms).
    plain_peers_logged_ms: std::sync::atomic::AtomicU64,
    /// Releases that landed while this node's lease was not valid (ADR
    /// 0020, section 1, amendment of 2026-11-14): counted, logged once per
    /// lapse.
    pub(crate) invalid_releases: lease_hold::InvalidReleases,
    /// Shared by all nodes through the bucket; fetched (or created) once.
    secret: tokio::sync::OnceCell<String>,
    pub(crate) ttl: Duration,
    /// The shared bucket, for replication.
    pub(crate) store: Arc<dyn ObjectStore>,
    /// The root of every actor-state key (ADR 0022), and the one copy of
    /// the bucket prefix.
    root: StateRoot,
    /// How long after its due time an alarm counts as abandoned: a lease
    /// period plus grace, by which its owner fired it or lost the actor.
    pub(crate) alarm_overdue: Duration,
    pub(crate) alarm_sweep: Duration,
    /// How often sockets of actors this node does not hold are checked
    /// against their ownership record ([`ClusterConfig::socket_sweep`]).
    pub(crate) socket_sweep: Duration,
    /// ADR 0012's retention window ([`ClusterConfig::history_retention`]).
    pub(crate) history_retention: Duration,
    /// [`ClusterConfig::snapshot_retry_min`].
    pub(crate) snapshot_retry_min: Duration,
    /// [`ClusterConfig::restore_budget`].
    pub(crate) restore_budget: Duration,
    /// Actors' waits after a periodic snapshot that failed: here, so they
    /// outlive an actor's time in memory.
    pub(crate) snapshot_waits: Arc<crate::replica::SnapshotWaits>,
    /// A clean stop's deadline, once one began (ADR 0034, section 10):
    /// batched holds' events are cut there, and their uploads stop waiting.
    stop_by: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    /// When a heartbeat last reached the bucket (ms since the epoch; 0:
    /// never), and how often one is sent: for readiness.
    heartbeat_ok_ms: std::sync::atomic::AtomicU64,
    heartbeat_every: Duration,
    /// Woken each time a heartbeat reaches the bucket: a call or a
    /// WebSocket upgrade the peer never read waits for one before it is
    /// sent again (see [`Cluster::call`]).
    heartbeat_landed: tokio::sync::Notify,
    /// Whether this process may use its node id: checked once at start,
    /// before its first heartbeat and before any lease (see
    /// [`Cluster::check_identity`]).
    identity: tokio::sync::watch::Sender<Identity>,
    /// Random per process, and a counter: every signed call's nonce is new.
    nonce_seed: String,
    nonce_counter: std::sync::atomic::AtomicU64,
    /// The bucket, for reading `control/` (ADR 0014).
    control_keys: ObjectStoreBucket,
    /// The operator's intents as last read; kept when a listing fails, so a
    /// failure never undrains a node (ADR 0014).
    control: RwLock<Control>,
    /// Peers a draining node chose that failed to take an event (down,
    /// unreachable, or draining themselves), with when they may be chosen
    /// again (ms).
    suspects: Mutex<HashMap<String, u64>>,
    /// See [`Cluster::secrets_blocked`].
    secrets_blocked: std::sync::atomic::AtomicBool,
    /// The lease timing (ttl, grace, safety, skew).
    lease_config: LeaseConfig,
    /// How often the node lease is written ([`lease_every`]).
    lease_every: Duration,
    /// How long one node lease write may take ([`lease_attempt`]).
    lease_attempt: Duration,
    /// This node's lease validity, shared with every hold.
    pub(crate) validity: Arc<NodeValidity>,
    /// Run when a lease write finds the lease had run out, before every
    /// hold ends: the service closes its WebSockets there, so none is
    /// still open once the holds' releases are queued.
    lapse_hook: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
    /// Set by a clean stop: the lease task writes `released` and ends.
    lease_stop: tokio::sync::watch::Sender<bool>,
    /// Set once the lease task has ended, whatever the way.
    lease_done: tokio::sync::watch::Sender<bool>,
    /// Old-format heartbeats seen by the listing: `written_at`, and when
    /// that body was first seen (see [`Cluster::note_old_versions`]).
    old_heartbeats: Mutex<HashMap<String, (u64, tokio::time::Instant)>>,
    /// When another node's lease may be fenced (ADR 0020, section 2).
    fence_watch: Mutex<wasmkeep_cluster::node_lease::FenceWatch>,
    /// Nodes the listing found due for a fence, for the fence task
    /// (`fence_loop`): the listing never waits for a fence.
    fences_due: tokio::sync::mpsc::UnboundedSender<Vec<FenceDue>>,
    /// Requests kept until a dead node's actor can be taken over.
    pub(crate) failover: FailoverWait,
    /// How often the collecting node runs an idle-expiry pass (ADR 0028).
    pub(crate) expiry_every: Duration,
    /// The app's tombstone (ADR 0028, section 5): [`APP_OPEN`],
    /// [`APP_UNCHECKED`] or [`APP_DELETED`].
    app_gate: std::sync::atomic::AtomicU8,
    /// [`ClusterConfig::follows_app`].
    follows_app: bool,
    /// The nodes the last refresh of the view found (live or not): read
    /// one by one between listings of `nodes/` (ADR 0020, amendment of
    /// 2026-11-14).
    known_nodes: Mutex<Vec<String>>,
    /// A listing of `nodes/` was asked for (a hello, a failed early one):
    /// the next refresh lists.
    listing_wanted: std::sync::atomic::AtomicBool,
    /// Wakes the heartbeat loop for an early listing.
    listing_asked: Arc<tokio::sync::Notify>,
    /// The nodes as the last refresh saw them (id, instance, lease state,
    /// live), to tell the alarm sweep when they change.
    view_shape: Mutex<Option<view::ViewShape>>,
    /// Woken when [`Self::view_shape`] changes (ADR 0009, amendment of
    /// 2026-11-14).
    pub(crate) view_changed: tokio::sync::Notify,
    /// Move requests this node deleted, kept out of [`Self::moves`] until
    /// they expire (a listing in flight could bring one back).
    deleted_moves: Mutex<Vec<control::MoveIntent>>,
    /// Delete requests this node ended, kept out of [`Self::deletes`] until
    /// they expire, likewise.
    ended_deletes: Mutex<Vec<control::DeleteIntent>>,
    ended_restores: Mutex<Vec<control::RestoreIntent>>,
    /// Live nodes the last listing found without `multi-app`, while the
    /// process serves several apps (ADR 0038, section 10).
    multi_app_missing: Mutex<Vec<String>>,
}

/// One page of the alarm index, as the sweep reads it.
#[derive(Debug)]
pub(crate) struct AlarmPage {
    /// The entries due: address, due time, key.
    pub(crate) due: Vec<(ActorAddress, u64, StorePath)>,
    /// Where the next page starts, if the walk stopped at its scan cap.
    pub(crate) next: Option<StorePath>,
    /// The due time of the first entry not due yet, if the walk met one.
    pub(crate) next_at: Option<u64>,
}

/// No tombstone: actors are acquired.
const APP_OPEN: u8 = 0;
/// A node following its app's deploys, before its first check for a
/// tombstone made while its lease was valid: no actor is acquired yet.
const APP_UNCHECKED: u8 = 1;
/// The tombstone was seen: no actor is acquired again.
const APP_DELETED: u8 = 2;

/// Where a live node is reached, as its heartbeat says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Peer {
    /// Its advertised `http://` base URL, if valid.
    pub(crate) plain: Option<String>,
    /// Its cluster listener (`https://`), if it published one.
    pub(crate) tls: Option<String>,
    /// What it serves (heartbeat line 6).
    pub(crate) app: String,
    /// Its heartbeat line 9; `None` from a node that predates ADR 0014.
    pub(crate) drain: Option<DrainState>,
    /// It answers take calls (heartbeat line 11, ADR 0017).
    pub(crate) can_take: bool,
    /// It opens sockets the front hands over (heartbeat line 11, ADR 0027).
    pub(crate) can_handoff: bool,
    /// It expires idle actors and honours an app's tombstone (heartbeat
    /// line 11, ADR 0028).
    pub(crate) can_end: bool,
    /// The process its heartbeat names: routing trusts a record only if
    /// it names this one (ADR 0020).
    pub(crate) instance: Option<String>,
}

impl Peer {
    /// From a heartbeat; `None` if it gives no usable address.
    fn of(node: &NodeInfo) -> Option<Self> {
        let plain = (node.address.starts_with("http://")
            && !node.address.contains(char::is_whitespace))
        .then(|| node.address.clone());
        let tls = node.cluster_address.clone();
        (plain.is_some() || tls.is_some()).then(|| Self {
            plain,
            tls,
            app: node.app.clone(),
            drain: node.drain,
            can_take: node.can_take,
            can_handoff: node.can_handoff,
            can_end: node.can_end,
            instance: node.instance.clone(),
        })
    }
}

/// The three clients a node uses to reach its peers.
struct Clients {
    /// Forwarded requests.
    request: Client<ClusterConnector, Incoming>,
    /// WebSocket upgrades sent to another node.
    upgrade: Client<ClusterConnector, http_body_util::Empty<bytes::Bytes>>,
    /// `actors.call` to another node.
    call: Client<ClusterConnector, http_body_util::Full<bytes::Bytes>>,
}

/// How long a connection to a peer may wait idle in a client's pool. Below
/// the port's own idle limit ([`crate::http::ServeLimits`]'s
/// `header_read_timeout`, 30 s), so the pool drops a connection before the
/// peer closes it. A connection the peer closed while it waited in the
/// pool costs nothing: the client sees the close before it writes, and
/// sends on another connection. A request written just as the peer closes
/// fails with `SendRequest`, exactly as one the peer read before the
/// connection was lost: the client cannot tell the two apart, so neither
/// is sent again (the second could run an event twice).
pub(crate) const PEER_POOL_IDLE: Duration = Duration::from_secs(20);

/// The builder of every client a node uses to reach its peers.
fn peer_client_builder(idle: Duration) -> hyper_util::client::legacy::Builder {
    let mut builder = Client::builder(TokioExecutor::new());
    builder
        .pool_idle_timeout(idle)
        .pool_timer(hyper_util::rt::TokioTimer::new());
    builder
}

impl Clients {
    fn new(connector: &ClusterConnector) -> Arc<Self> {
        let builder = peer_client_builder(PEER_POOL_IDLE);
        Arc::new(Self {
            request: builder.build(connector.clone()),
            upgrade: builder.build(connector.clone()),
            call: builder.build(connector.clone()),
        })
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl Cluster {
    /// Sets the hook a lapsed lease runs before ending every hold (once;
    /// a later call is ignored).
    pub(crate) fn on_lease_lapse(&self, hook: Box<dyn Fn() + Send + Sync>) {
        let _ = self.lapse_hook.set(hook);
    }

    /// `root`: whose actors, under the bucket prefix of `config` (the
    /// prefix given here is ignored); what this node serves (see
    /// [`NodeInfo::app`]) is the root's `tenant/app`.
    pub(crate) fn new(config: &ClusterConfig, root: &StateRoot) -> Result<Arc<Self>, crate::Error> {
        let serves = format!("{}/{}", root.tenant(), root.app());
        let serves = serves.as_str();
        let invalid_id =
            |e: InvalidNodeId| crate::Error::InvalidNodeId(config.node_id.clone(), e.to_string());
        // Every bucket request this node makes goes through here (leases,
        // heartbeats, alarms, replication), counted for the metrics page.
        let store = crate::counted_store::counted(Arc::clone(&config.store));
        let (fences_due, fences_due_rx) = tokio::sync::mpsc::unbounded_channel();
        let bucket = ObjectStoreBucket::new(Arc::clone(&store), config.prefix.clone());
        // Three tries to renew the node lease before it runs out (ADR 0020).
        if config.heartbeat > Duration::from_millis(config.lease.ttl_ms() / 3) {
            return Err(crate::Error::LeaseTiming(format!(
                "the heartbeat period ({:?}) must be at most a third of the lease ttl ({} ms)",
                config.heartbeat,
                config.lease.ttl_ms()
            )));
        }
        let heartbeat_ttl_ms = heartbeat_ttl_ms(config.heartbeat);
        let control_keys = bucket.clone();
        let membership = Membership::new(
            bucket.clone(),
            SystemClock,
            &config.node_id,
            config.advertise.clone(),
            heartbeat_ttl_ms,
            config.lease.max_skew_ms(),
        )
        .map_err(invalid_id)?
        .with_app(serves)
        .with_reads_secrets(config.reads_secrets);
        // Random per process: in every heartbeat (a second process writing
        // this id shows another one), and the seed of call nonces.
        let instance = random_hex(8).unwrap_or_else(|_| {
            // No /dev/urandom: the process and the start time still make
            // it unique to this run.
            format!("{:08x}{:08x}", std::process::id(), now_ms() & 0xffff_ffff)
        });
        let membership = membership.with_instance(instance.clone());
        let refuse_plaintext = config.cluster_plaintext == ClusterPlaintext::Refuse;
        // The process's transport: made by its first (and only) app.
        let (transport, joined) = config
            .process
            .clone()
            .unwrap_or_else(NodeProcess::new)
            .attach(config, bucket.clone())?;
        // Its heartbeat names the process (ADR 0038, section 2).
        let membership = match joined.process().name() {
            Some(name) => membership.with_process(name),
            None => membership,
        };
        let membership = match &transport.tls {
            Some(t) => membership.with_cluster_tls(t.advertise(), vec![t.pin()]),
            None => membership,
        };
        let cluster = Arc::new(Self {
            serves: serves.to_owned(),
            seen: SeenSignatures::named("calls"),
            seen_control: SeenSignatures::with_cap("control", signing::MAX_SEEN_CONTROL),
            seen_ingress: SeenSignatures::with_cap("ingress", signing::MAX_SEEN_INGRESS),
            refusal_logged: Mutex::new(HashMap::new()),
            node_id: config.node_id.clone(),
            owners: Owners::new(
                bucket,
                SystemClock,
                config.node_id.clone(),
                instance.clone(),
                NODES_PREFIX,
                config.lease,
            ),
            pending: Pending::default(),
            membership,
            peers: RwLock::new(HashMap::new()),
            heartbeat_horizon_ms: heartbeat_horizon_ms(config.heartbeat, &config.lease),
            conflict_logged_ms: std::sync::atomic::AtomicU64::new(0),
            refuse_plaintext,
            plain_peers_logged_ms: std::sync::atomic::AtomicU64::new(0),
            invalid_releases: lease_hold::InvalidReleases::default(),
            transport,
            joined,
            published_ms: std::sync::atomic::AtomicU64::new(0),
            secret: tokio::sync::OnceCell::new(),
            ttl: Duration::from_millis(config.lease.ttl_ms()),
            store,
            root: StateRoot::new(&config.prefix, root.tenant(), root.app()),
            alarm_overdue: Duration::from_millis(
                config
                    .lease
                    .ttl_ms()
                    .saturating_add(config.lease.grace_ms()),
            ),
            alarm_sweep: config.alarm_sweep,
            socket_sweep: config.socket_sweep,
            history_retention: config.history_retention,
            snapshot_retry_min: config.snapshot_retry_min,
            restore_budget: config.restore_budget,
            snapshot_waits: Arc::default(),
            stop_by: tokio::sync::watch::Sender::new(None),
            heartbeat_ok_ms: std::sync::atomic::AtomicU64::new(0),
            heartbeat_every: config.heartbeat,
            heartbeat_landed: tokio::sync::Notify::new(),
            identity: tokio::sync::watch::Sender::new(Identity::Checking),
            nonce_seed: instance,
            nonce_counter: std::sync::atomic::AtomicU64::new(0),
            control_keys,
            control: RwLock::new(Control::default()),
            suspects: Mutex::new(HashMap::new()),
            secrets_blocked: std::sync::atomic::AtomicBool::new(false),
            lease_config: config.lease,
            lease_every: lease_every(config.heartbeat, &config.lease),
            lease_attempt: lease_attempt(&config.lease),
            validity: Arc::new(NodeValidity::new()),
            lapse_hook: std::sync::OnceLock::new(),
            lease_stop: tokio::sync::watch::Sender::new(false),
            lease_done: tokio::sync::watch::Sender::new(false),
            old_heartbeats: Mutex::new(HashMap::new()),
            fence_watch: Mutex::new(wasmkeep_cluster::node_lease::FenceWatch::new(
                &config.lease,
                u64::try_from(lease_attempt(&config.lease).as_millis()).unwrap_or(u64::MAX),
            )),
            fences_due,
            failover: FailoverWait::new(config),
            expiry_every: config.expiry_every,
            app_gate: std::sync::atomic::AtomicU8::new(if config.follows_app {
                APP_UNCHECKED
            } else {
                APP_OPEN
            }),
            follows_app: config.follows_app,
            known_nodes: Mutex::new(Vec::new()),
            listing_wanted: std::sync::atomic::AtomicBool::new(false),
            listing_asked: Arc::new(tokio::sync::Notify::new()),
            view_shape: Mutex::new(None),
            view_changed: tokio::sync::Notify::new(),
            deleted_moves: Mutex::new(Vec::new()),
            ended_deletes: Mutex::new(Vec::new()),
            ended_restores: Mutex::new(Vec::new()),
            multi_app_missing: Mutex::new(Vec::new()),
        });
        if refuse_plaintext {
            tracing::info!(
                "plain HTTP between nodes refused: node calls only on the cluster port, peers without a cluster listener not used"
            );
        }
        tokio::spawn(node_lease_loop(Arc::downgrade(&cluster)));
        tokio::spawn(pending_loop(Arc::downgrade(&cluster)));
        tokio::spawn(heartbeat_loop(Arc::downgrade(&cluster), config.heartbeat));
        tokio::spawn(fence_loop(Arc::downgrade(&cluster), fences_due_rx));
        Ok(cluster)
    }

    /// `alarms/<tenant>/`: the alarm index (ADR 0009) of this node's root.
    fn alarm_base(&self) -> StorePath {
        StorePath::from(self.root.bucket_base(Family::Alarms))
    }

    /// The key of `address`'s index entry `entry`:
    /// `alarms/<tenant>/<at, 15 digits>/<class>.t/<name as hex>/<generation,
    /// 20 digits>-<nonce>` ([`alarm_index`]), or
    /// `alarms/<tenant>/<at>/<class>/<name as hex>` for a legacy entry. Due
    /// time first, zero-padded, so a listing (in key
    /// order, as S3 returns it) yields the earliest alarms first and a sweep
    /// stops at "now"; the name as hex, so nothing in the key is escaped by
    /// the object store.
    fn alarm_key(&self, address: &ActorAddress, entry: &Entry) -> StorePath {
        self.assert_mine(address);
        StorePath::from(self.root.bucket_key(
            Family::Alarms,
            &alarm_index::key_tail(address.class(), address.name(), entry),
        ))
    }

    /// Leaves the process: a stopped runtime's node id may start again.
    pub(crate) fn leave_process(&self) {
        self.joined.leave();
    }

    /// Whether its process serves several apps (ADR 0038): hops then must
    /// name their sender, forwards carry their proof, and every live node
    /// must announce `multi-app`.
    pub(crate) fn several_apps(&self) -> bool {
        self.joined.process().several()
    }

    /// Notes the live nodes that lack the `multi-app` word, for readiness
    /// (ADR 0038, section 10): checked at every listing while the process
    /// serves several apps.
    pub(super) fn note_multi_app(&self, live: &[NodeInfo]) {
        let missing: Vec<String> = if self.several_apps() {
            live.iter()
                .filter(|n| !n.can_multi_app)
                .map(|n| n.id.clone())
                .collect()
        } else {
            Vec::new()
        };
        let mut last = self
            .multi_app_missing
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !missing.is_empty() && *last != missing {
            tracing::error!(
                nodes = %missing.join(", "),
                "nodes of an older version (no multi-app) are live in this cluster: this \
                 process serves several apps and reports not ready until they are upgraded"
            );
        }
        *last = missing;
    }

    /// The live nodes found without `multi-app` at the last listing.
    pub(crate) fn multi_app_missing(&self) -> Vec<String> {
        self.multi_app_missing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Why this runtime is not ready for want of `multi-app`, if it is not.
    #[cfg(test)]
    pub(crate) fn multi_app_reason(&self) -> Option<String> {
        let missing = self.multi_app_missing();
        (!missing.is_empty()).then(|| format!("{MULTI_APP_MISSING}: {}", missing.join(", ")))
    }

    /// What this node serves, `tenant/app`.
    pub(crate) fn serves(&self) -> &str {
        &self.serves
    }

    /// The root of this node's actor state (ADR 0022).
    pub(crate) fn state_root(&self) -> &StateRoot {
        &self.root
    }

    /// Every address whose keys this node builds is of its root's tenant:
    /// addresses are made with the service's tenant, and a node call
    /// naming another is refused before any key is built. Checked in
    /// every build: a key is built from the root's tenant, never the
    /// address's, so an address of another tenant reaching here would
    /// otherwise read or write that tenant's actor under this one's keys.
    pub(crate) fn assert_mine(&self, address: &ActorAddress) {
        assert_eq!(
            address.tenant(),
            self.root.tenant(),
            "{address} is not of this node's tenant"
        );
    }

    /// A clean stop began, to end by `at` (ADR 0034, section 10).
    pub(crate) fn stop_by(&self, at: tokio::time::Instant) {
        self.stop_by.send_replace(Some(at));
    }

    /// The clean stop's deadline, if one began.
    pub(crate) fn stop_deadline(&self) -> Option<tokio::time::Instant> {
        *self.stop_by.borrow()
    }

    /// Resolves at a clean stop's deadline (never without one).
    pub(crate) fn stop_reached(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut by = self.stop_by.subscribe();
        async move {
            let at = match by.wait_for(Option::is_some).await {
                Ok(at) => *at,
                Err(_) => None,
            };
            match at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        }
    }

    /// Lists an alarm of `address` due at `at` in the bucket's index, as
    /// written by the owner holding lease `generation`, under a key never
    /// used before (rule R1: no delete sent earlier, however late it lands,
    /// can remove it), and returns that entry. Tried twice (the only time a
    /// key is written twice): callers give the actor up if it fails.
    pub(crate) async fn put_new_alarm_entry(
        &self,
        address: &ActorAddress,
        at: u64,
        generation: u64,
    ) -> Result<Entry, object_store::Error> {
        let entry = self.name_alarm_entry(at, generation)?;
        self.put_alarm_entry(address, &entry).await.map(|()| entry)
    }

    /// A fresh entry for an alarm due at `at`, as written by the owner
    /// holding lease `generation` (R1), without writing it: a batched hold
    /// names it at the event and lists it with the segment that carries
    /// it (ADR 0034, section 9).
    pub(crate) fn name_alarm_entry(
        &self,
        at: u64,
        generation: u64,
    ) -> Result<Entry, object_store::Error> {
        // `nonce()` is lowercase hex, unique in this process; the generation
        // is this process's alone.
        Entry::owned(at, generation, self.nonce()).ok_or(object_store::Error::Generic {
            store: "alarm index",
            source: "invalid nonce".into(),
        })
    }

    /// Lists `entry` of `address` in the bucket's index. Tried twice (the
    /// only time a key is written twice).
    pub(crate) async fn put_alarm_entry(
        &self,
        address: &ActorAddress,
        entry: &Entry,
    ) -> Result<(), object_store::Error> {
        let key = self.alarm_key(address, entry);
        if self
            .store
            .put(&key, object_store::PutPayload::new())
            .await
            .is_ok()
        {
            return Ok(());
        }
        self.store
            .put(&key, object_store::PutPayload::new())
            .await
            .map(|_| ())
    }

    /// Removes exactly `entry` of `address` from the index (absent is fine).
    /// Callers check the rules first: it is no longer named by the actor's
    /// replicated state (R2), and not from a newer owner (R3).
    pub(crate) async fn delete_alarm_entry(
        &self,
        address: &ActorAddress,
        entry: &Entry,
    ) -> Result<(), object_store::Error> {
        match self.store.delete(&self.alarm_key(address, entry)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The folder of `address`'s tagged entries due at `at`:
    /// `alarms/<tenant>/<at, 15 digits>/<class>.t/<name as hex>`.
    fn alarm_entries_base(&self, address: &ActorAddress, at: u64) -> StorePath {
        self.assert_mine(address);
        StorePath::from(self.root.bucket_key(
            Family::Alarms,
            &alarm_index::tagged_prefix(address.class(), address.name(), at),
        ))
    }

    /// `address`'s tagged entries due at `at`, as listed now (the legacy
    /// entry is elsewhere: [`alarm_index::entries_to_clear`] adds it).
    pub(crate) async fn alarm_entries_at(
        &self,
        address: &ActorAddress,
        at: u64,
    ) -> Result<Vec<Entry>, object_store::Error> {
        use futures_util::TryStreamExt as _;
        let base = self.alarm_entries_base(address, at);
        let strip = format!("{}/", base.as_ref());
        let listed: Vec<_> = self.store.list(Some(&base)).try_collect().await?;
        Ok(listed
            .iter()
            .filter_map(|meta| meta.location.as_ref().strip_prefix(strip.as_str()))
            .filter_map(alarm_index::Tag::parse)
            .map(|tag| Entry { at, tag: Some(tag) })
            .collect())
    }

    /// Reads this node's index entries in key (so due time) order, after
    /// `after` (a key from an earlier page) if given: entries due before
    /// `before_ms`, at most `scan` of them. Returns them with the key to
    /// resume from, or `None` once the listing reached entries not yet due
    /// (or its end), so the next sweep starts over. Legacy and tagged
    /// entries alike; an actor listed under both is returned for each.
    #[cfg(test)]
    pub(crate) async fn alarms_due_before(
        &self,
        before_ms: u64,
        after: Option<&StorePath>,
        scan: usize,
    ) -> Result<(Vec<(ActorAddress, u64, StorePath)>, Option<StorePath>), object_store::Error> {
        self.alarm_page(before_ms, after, scan)
            .await
            .map(|p| (p.due, p.next))
    }

    /// [`Self::alarms_due_before`], with the due time of the first entry
    /// the walk stopped at, not due before `before_ms` (`next_at`).
    pub(crate) async fn alarm_page(
        &self,
        before_ms: u64,
        after: Option<&StorePath>,
        scan: usize,
    ) -> Result<AlarmPage, object_store::Error> {
        use futures_util::TryStreamExt as _;
        let base = self.alarm_base();
        let tenant = self.root.tenant();
        let strip = format!("{}/", base.as_ref());
        // `after` is a listed location, already in the store's encoding:
        // passed through as is (never rebuilt from a string, which would
        // escape it again).
        let mut listing = match after {
            Some(after) => self.store.list_with_offset(Some(&base), after),
            None => self.store.list(Some(&base)),
        };
        let mut due = Vec::new();
        let mut seen = 0;
        while let Some(meta) = listing.try_next().await? {
            let key = meta.location;
            let Some(listed) = key
                .as_ref()
                .strip_prefix(strip.as_str())
                .and_then(alarm_index::parse_listed)
            else {
                continue;
            };
            let at = listed.entry.at;
            if at >= before_ms {
                return Ok(AlarmPage {
                    due,
                    next: None,
                    next_at: Some(at),
                });
            }
            if let Ok(address) = ActorAddress::new(tenant, listed.class, listed.name) {
                due.push((address, at, key.clone()));
            }
            seen += 1;
            if seen >= scan {
                return Ok(AlarmPage {
                    due,
                    next: Some(key),
                    next_at: None,
                });
            }
        }
        Ok(AlarmPage {
            due,
            next: None,
            next_at: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn alarm_shard_is_mine(&self, address: &ActorAddress) -> bool {
        self.alarm_shards().is_mine(address)
    }

    /// The live nodes as seen now, to decide many entries' shards against
    /// one view ([`AlarmShards::is_mine`]). Only nodes that deliver this
    /// app's alarms count: the same app, and not draining (ADR 0014; a draining node hands its own
    /// alarms off). A node of another tenant on the prefix would never
    /// deliver its share, leaving it until it is anyone's (3x overdue).
    pub(crate) fn alarm_shards(&self) -> AlarmShards {
        let draining = self
            .control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .draining
            .clone();
        let delivers = |id: &str, peer: Option<&Peer>| {
            !draining.contains(id)
                && peer.is_none_or(|p| {
                    p.app == self.serves && p.drain.is_none_or(|d| d == DrainState::Serving)
                })
        };
        let mut nodes: Vec<String> = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(id, peer)| **id != self.node_id && delivers(id, Some(peer)))
            .map(|(id, _)| id.clone())
            .collect();
        // This node, unless it drains; and always if no other node is
        // left, so no entry is nobody's shard.
        if (delivers(&self.node_id, None) && !self.secrets_blocked()) || nodes.is_empty() {
            nodes.push(self.node_id.clone());
        }
        nodes.sort_unstable();
        let me = nodes.iter().position(|n| *n == self.node_id);
        AlarmShards { nodes, me }
    }

    /// Sends a WebSocket upgrade to the node that owns the actor and, once
    /// it answers 101, joins the client's connection to it. The owner holds
    /// the socket; this node only relays bytes.
    ///
    /// If the owner provably never read the upgrade
    /// ([`never_read_by_peer`]: it refused this node's certificate at the
    /// TLS handshake, or the HTTP client handed the upgrade back unsent on
    /// a new TLS connection), the upgrade is sent once more as
    /// [`Cluster::call`] sends a call again: after this node's next
    /// heartbeat reached the bucket and two more periods passed, the
    /// client waiting meanwhile; a 503 if that heartbeat does not land
    /// within two periods, or if the second try fails. So a client whose
    /// upgrade the owner keeps refusing waits up to four periods for its
    /// 503 (two when the heartbeat does not land): `docs/cluster.md` says
    /// why that is accepted. After any other failure the upgrade is not
    /// sent again, and the 503 comes at once: no connection could be
    /// opened, or the connection failed after the upgrade was written,
    /// when the owner may have read it.
    pub(crate) async fn forward_upgrade<B: Send + 'static>(
        &self,
        owner: &str,
        req: Request<B>,
        opener: crate::websocket::Opener,
        permit: crate::socket_limits::SessionPermit,
        close_wait: Duration,
    ) -> Response<HyperOutgoingBody> {
        if never_forward(&req) {
            return unavailable("actor ownership is changing; retry\n");
        }
        // The front's hand-off travels signed (ADR 0027), to a node that
        // reads it: an older one would take it for a client's upgrade.
        let front = opener == crate::websocket::Opener::Front;
        if front && !self.can_handoff(owner) {
            return unavailable("the actor's node runs an older version; retry\n");
        }
        // Sent at most twice: the upgrade has no body, so it can be built
        // again, and it is sent again only when the owner never read it.
        let mut first = true;
        let (base, revocation, response) = loop {
            let Some(base) = self.address_of(owner) else {
                return unavailable("the actor's node is not reachable; retry\n");
            };
            let Some(path) = req.extensions().get::<OriginalPath>().map(|p| p.0.clone()) else {
                return unavailable("cannot forward this request\n");
            };
            if first {
                crate::metrics::inc(
                    &crate::metrics::METRICS
                        .forwards
                        .get(&[("kind", "websocket")]),
                );
            }
            let mut out = Request::builder().method(hyper::Method::GET);
            if let Some(headers) = out.headers_mut() {
                *headers = upgrade_headers(req.headers());
            }
            let app = match self.app_header() {
                Ok(app) => app,
                Err(e) => {
                    tracing::error!(error = %e, "forwarding an upgrade refused");
                    return unavailable("this node's app cannot be named to another node\n");
                }
            };
            let out = out
                .uri(format!("{}{path}", base.trim_end_matches('/')))
                .header(FORWARDED, "1")
                .header(APP, app)
                .body(http_body_util::Empty::new());
            let Ok(mut out) = out else {
                return unavailable("bad advertised address\n");
            };
            if let Some(peer) = req.extensions().get::<crate::http::PeerAddress>().copied() {
                out.extensions_mut().insert(peer);
            }
            if req.extensions().get::<IngressCharged>().is_some() {
                out.extensions_mut().insert(IngressCharged);
            }
            if self.sign_admission(owner, &path, &mut out).await.is_err() {
                return unavailable("cannot authenticate ingress admission; retry\n");
            }
            if front && let Err(e) = self.sign_hand_off(owner, &path, &mut out).await {
                tracing::warn!(owner, error = %e, "relaying a hand-off refused");
                return unavailable("cannot relay this socket now; retry\n");
            }
            if let Err(e) = self.check_tls_target(&base) {
                tracing::warn!(owner, error = %e, "forwarding an upgrade refused");
                return unavailable("the actor's node is not reachable; retry\n");
            }
            let revocation = self.revocation(&base, owner);
            let upgrade = self.clients().upgrade.clone();
            match tokio::time::timeout(FORWARD_TIMEOUT, upgrade.request(out)).await {
                Ok(Ok(r)) => break (base, revocation, r),
                // As for `actors.call` (`Cluster::call`): never read by
                // the owner, which refused the handshake, or whose new TLS
                // connection gave the upgrade back unsent. Sent once more
                // when this node's heartbeat is live there again; a 503,
                // without a second try, if that heartbeat does not land
                // within two periods.
                Ok(Err(e)) if first && never_read_by_peer(&base, &e) => {
                    first = false;
                    tracing::info!(
                        owner,
                        "the owner did not read an upgrade (it refused the handshake, or the \
                         new connection closed before the upgrade was written); sending it \
                         again after this node's next heartbeat"
                    );
                    if !self.heartbeat_lands_then_two_periods().await {
                        return unavailable("the actor's node did not answer; retry\n");
                    }
                }
                Ok(Err(e)) => {
                    self.failover.connect_failed(owner, &e);
                    tracing::warn!(owner, error = %e, "forwarding an upgrade failed");
                    return unavailable("the actor's node did not answer; retry\n");
                }
                Err(_) => return unavailable("the actor's node did not answer in time; retry\n"),
            }
        };
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            // The owner refused (or it moved again): pass its answer on,
            // checked like any answer from a peer.
            return self
                .trusted_response(&base, owner, response)
                .unwrap_or_else(|| unavailable("the actor's node is no longer trusted; retry\n"));
        }
        let headers = response.headers().clone();
        // Over TLS, the relay is cut once the owner's pin stops being
        // trusted (its heartbeat expired, or its address changed hands);
        // and nothing of it is passed on if that already happened.
        let pin = response
            .extensions()
            .get::<crate::cluster_tls::PeerPin>()
            .map(|p| p.0);
        let still = revocation.map(|r| r(pin));
        if still.as_ref().is_some_and(|s| !s()) {
            return unavailable("the actor's node is no longer trusted; retry\n");
        }
        let revoked = still.map(|s| crate::cluster_tls::until_untrusted(move || s()));
        tokio::spawn(async move {
            let _permit = permit;
            let (client, owner_side) =
                match tokio::join!(hyper::upgrade::on(req), hyper::upgrade::on(response)) {
                    (Ok(c), Ok(o)) => (c, o),
                    _ => return,
                };
            let mut client = hyper_util::rt::TokioIo::new(client);
            let mut owner_side = crate::cluster_tls::Revocable::new(
                hyper_util::rt::TokioIo::new(owner_side),
                revoked,
            );
            relay_until_eof(&mut client, &mut owner_side, close_wait).await;
        });
        let mut switching = text_response(StatusCode::SWITCHING_PROTOCOLS, "");
        *switching.headers_mut() = headers;
        switching
    }

    /// Signs a relayed hand-off (ADR 0027) as a call is signed: the front's
    /// caller value and the upgrade mark, over the empty body. Signed for
    /// each send, so a second send has its own nonce.
    async fn sign_hand_off<B>(
        &self,
        owner: &str,
        path: &str,
        out: &mut Request<B>,
    ) -> Result<(), String> {
        let secret = self.secret().await?.to_owned();
        let caller = crate::actor::Caller::Front.value(self.root.tenant());
        let headers = out.headers_mut();
        headers.insert(
            CALLER,
            hyper::header::HeaderValue::from_str(&caller).map_err(|e| e.to_string())?,
        );
        headers.insert(
            UPGRADE_MARK,
            hyper::header::HeaderValue::from_static("websocket"),
        );
        let body_hash = body_sha256(b"");
        let ts = now_ms();
        let nonce = self.nonce();
        let mac = signature(
            &secret,
            ts,
            &nonce,
            owner,
            &hyper::Method::GET,
            path,
            &body_hash,
            headers,
        );
        headers.insert(
            INTERNAL,
            hyper::header::HeaderValue::from_str(&format!("v4 {ts} {nonce} {body_hash} {mac}"))
                .map_err(|e| e.to_string())?,
        );
        self.add_sender(headers, &secret, "internal", &mac);
        Ok(())
    }

    /// The cluster secret: created by the first node that needs it
    /// (conditional create), read by the others. Anyone who can read the
    /// bucket controls the cluster anyway, so it adds no new trust.
    async fn secret(&self) -> Result<&str, String> {
        self.secret
            .get_or_try_init(|| async {
                use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
                let location =
                    object_store::path::Path::from(format!("{}{SECRET_KEY}", self.root.prefix()));
                let fresh = random_hex(32).map_err(|e| e.to_string())?;
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                };
                match self
                    .store
                    .put_opts(
                        &location,
                        PutPayload::from(fresh.clone().into_bytes()),
                        opts,
                    )
                    .await
                {
                    Ok(_) => return Ok(fresh),
                    Err(object_store::Error::AlreadyExists { .. }) => {}
                    Err(e) => return Err(e.to_string()),
                }
                let bytes = self
                    .store
                    .get(&location)
                    .await
                    .map_err(|e| e.to_string())?
                    .bytes()
                    .await
                    .map_err(|e| e.to_string())?;
                let secret = String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())?;
                // Fail closed on anything but what `random_hex(32)` writes: an
                // empty or weak secret would make forging trivial.
                if !valid_secret(&secret) {
                    tracing::error!(key = SECRET_KEY, "the cluster secret in the bucket is malformed; calls between nodes are refused");
                    return Err("cluster secret malformed".into());
                }
                Ok(secret)
            })
            .await
            .map(String::as_str)
    }

    /// Loads the secret now (at startup), so that checking an incoming
    /// call never touches the bucket: a forged header must not make this
    /// node fetch or create anything.
    pub(crate) async fn load_secret(&self) {
        if let Err(e) = self.secret().await {
            tracing::warn!(error = %e, "cluster secret not loaded yet; calls from other nodes are refused until it is");
        }
    }

    /// Whether a request comes from another node of this cluster: its
    /// signature matches `method`, `path` (as sent, `/actor/...`), its
    /// caller and chain headers, and a timestamp within a minute. Uses the
    /// secret only if already loaded (never fetches it for a stranger).
    ///
    /// Returns the signature: the request is a node's only if its body
    /// matches [`Signed::body`] and [`Cluster::first_use`] accepts it.
    ///
    /// In refuse mode, only a request that arrived on the cluster port
    /// (`on_cluster_port`) can be a node's: on the public port the request
    /// counts as coming from outside, and a valid signature there is spent
    /// (without the body check), so that copy can never become a node's
    /// call on the cluster port later.
    pub(crate) fn is_internal(
        &self,
        method: &hyper::Method,
        path: &str,
        headers: &hyper::HeaderMap,
        on_cluster_port: bool,
    ) -> Option<Signed> {
        let given = headers.get(INTERNAL).and_then(|v| v.to_str().ok())?;
        let public_port = self.refuse_plaintext && !on_cluster_port;
        let Some(secret) = self.secret.get() else {
            if !public_port {
                self.note_refused("cluster secret not loaded yet");
            }
            return None;
        };
        let now = now_ms();
        match verify(secret, given, now, &self.node_id, method, path, headers) {
            Some(signed) if public_port => {
                // Burnt, never honoured here, in the record a node's call
                // with the same markers would be checked against (both
                // markers are signed).
                let control = headers.contains_key(TAKE) || headers.contains_key(ALARM_HANDOFF);
                self.record(control).first_use(signed.mac, signed.ts, now);
                self.note_refused("signed call on the public port, treated as from outside (--cluster-plaintext refuse)");
                None
            }
            Some(signed) => {
                match self.sender_checked(headers, secret, "internal", given, path, on_cluster_port)
                {
                    Ok(_) => Some(signed),
                    Err(why) => {
                        self.note_refused(why);
                        None
                    }
                }
            }
            // Junk on the public port is an outside request like any other.
            None if public_port => None,
            None => {
                self.note_refused("bad, expired or misaddressed signature (clock skew? another cluster's secret? a node of an older version?)");
                None
            }
        }
    }

    /// Whether the connection a signed hop came on (`peer`, on the cluster
    /// port) is the one its [`SENDER`] names: that node's own live
    /// heartbeat publishes the pin presented. A hop without a sender (a
    /// node of an older version) or over plain HTTP passes here.
    /// Names this node as the sender of a hop it signed (ADR 0038,
    /// section 5), bound to the hop's own signature `mac`.
    fn add_sender(&self, headers: &mut hyper::HeaderMap, secret: &str, kind: &str, mac: &str) {
        if let Ok(v) =
            hyper::header::HeaderValue::from_str(&sender_value(secret, kind, mac, &self.node_id))
        {
            headers.insert(SENDER, v);
        }
    }

    /// The sender a signed hop names, checked (ADR 0038, section 5): bound
    /// to the hop's signature (`given`, its [`INTERNAL`] or [`ADMITTED`]
    /// value), named once, and a live node of this app. `Ok(None)`: no
    /// sender (a node of an older version, rolling upgrade stage 1). A
    /// hello may come from a node not listed yet (it asks for a listing);
    /// over plain HTTP between nodes (`accept`) an unlisted sender is taken
    /// as an unsigned-sender hop was, its app checked when known.
    fn sender_checked(
        &self,
        headers: &hyper::HeaderMap,
        secret: &str,
        kind: &str,
        given: &str,
        path: &str,
        on_cluster_port: bool,
    ) -> Result<Option<String>, &'static str> {
        let mut values = headers.get_all(SENDER).iter();
        let Some(first) = values.next() else {
            // Required once several apps share the process (ADR 0038,
            // section 5): every live node then sends it.
            if self.several_apps() {
                return Err("hop names no sender (several apps per process require it)");
            }
            return Ok(None);
        };
        if values.next().is_some() {
            return Err("hop names its sender twice");
        }
        let primary = given.rsplit(' ').next().unwrap_or_default();
        let sender = first
            .to_str()
            .ok()
            .and_then(|v| verify_sender(secret, kind, primary, v))
            .ok_or("hop's sender is not bound to its signature")?;
        if path == HELLO_PATH {
            return Ok(Some(sender));
        }
        let app = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&sender)
            .map(|p| p.app.clone());
        match app {
            Some(app) if app == self.serves => Ok(Some(sender)),
            Some(_) => Err("hop's sender is a node of another app"),
            None if on_cluster_port || self.refuse_plaintext => {
                self.want_listing();
                Err("hop's sender is not a live node")
            }
            None => Ok(Some(sender)),
        }
    }

    /// Read-only early replay check; body verification still spends the
    /// signature later, so a bad-body copy cannot burn the legitimate call.
    pub(crate) fn admission_replayed(&self, signed: &Signed, control: bool) -> bool {
        self.record(control).used(signed.mac, signed.ts, now_ms())
    }

    /// Records a signature whose body matched; false if it was already used.
    /// Only then is it spent: a copy sent ahead with another body is
    /// refused without using up the real call's signature. `control`: a
    /// take or an alarm hand-off, recorded apart from other calls.
    pub(crate) fn first_use(&self, signed: &Signed, control: bool) -> bool {
        match self.record(control).spend(signed.mac, signed.ts, now_ms()) {
            signing::Spent::Fresh => true,
            signing::Spent::Used => {
                self.note_refused("signature already used");
                false
            }
            // Not a replay: more node calls than the record holds.
            signing::Spent::Full if control => {
                self.note_refused(
                    "record of used signatures full (control: takes and alarm hand-offs)",
                );
                false
            }
            signing::Spent::Full => {
                self.note_refused("record of used signatures full (calls)");
                false
            }
        }
    }

    /// The record of used signatures for control calls, or for the rest.
    fn record(&self, control: bool) -> &SeenSignatures {
        if control {
            &self.seen_control
        } else {
            &self.seen
        }
    }

    /// A nonce for one signed call: this process's random seed and a
    /// counter, never the same twice in a run.
    fn nonce(&self) -> String {
        let n = self
            .nonce_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{}{n:x}", self.nonce_seed)
    }

    /// Why this node should not get traffic yet, if it should not: the
    /// cluster secret is not loaded (calls from other nodes would be
    /// refused), or this node's lease is not valid.
    pub(crate) fn not_ready(&self) -> Option<&'static str> {
        match *self.identity.borrow() {
            Identity::Checking => {
                return Some("checking that no other process runs with this node id");
            }
            Identity::Taken(stopped, _) => return Some(stopped.words()),
            Identity::Refused(_) => return Some("the bucket does not enforce conditional writes"),
            Identity::Unique => {}
        }
        if !self.multi_app_missing().is_empty() {
            // MULTI_APP_MISSING, spelled out for the list of reasons.
            debug_assert_eq!(
                MULTI_APP_MISSING,
                "a live node of an older version (no multi-app) runs in this cluster"
            );
            return Some("a live node of an older version (no multi-app) runs in this cluster");
        }
        if self.secret.get().is_none() {
            return Some("cluster secret not loaded");
        }
        // Ready only while this node's lease is valid (ADR 0020).
        if !self.node_lease_valid() {
            return Some("bucket unreachable (node lease not renewed)");
        }
        // Peers refuse this node's handshakes until they have listed its
        // pin: give them a heartbeat period (ADR 0011), and half of one more
        // for their listing's own latency.
        if self.transport.tls.is_some() {
            let published = self.published_ms.load(std::sync::atomic::Ordering::Relaxed);
            let period = u64::try_from(self.heartbeat_every.as_millis())
                .unwrap_or(u64::MAX)
                .saturating_mul(3)
                / 2;
            if published == 0 || now_ms().saturating_sub(published) < period {
                return Some("cluster identity not published for a heartbeat period yet");
            }
        }
        None
    }

    /// Records a heartbeat that reached the bucket at `now`. Its identity
    /// counts as published from now if this is the first heartbeat, or the
    /// first after an outage long enough to make it not ready (peers may
    /// have dropped its pin): readiness then waits for them to list it again.
    fn heartbeat_reached_bucket(&self, now: u64) {
        let last = self
            .heartbeat_ok_ms
            .swap(now, std::sync::atomic::Ordering::Relaxed);
        if last == 0 || now.saturating_sub(last) > self.heartbeat_window_ms() {
            self.published_ms
                .store(now, std::sync::atomic::Ordering::Relaxed);
        }
        self.heartbeat_landed.notify_waiters();
    }

    /// How long without a heartbeat reaching the bucket the node stays
    /// ready (ms): three periods, at least a second.
    fn heartbeat_window_ms(&self) -> u64 {
        u64::try_from(self.heartbeat_every.saturating_mul(3).as_millis())
            .unwrap_or(u64::MAX)
            .max(1_000)
    }

    /// Logs a refused node signature, at most once a minute: such a call is
    /// then treated as coming from outside.
    fn note_refused(&self, why: &'static str) {
        if self.refusal_log_due(why, now_ms()) {
            tracing::warn!(
                reason = why,
                "a call claiming to come from a node was refused"
            );
        }
    }

    /// [`Cluster::refusal_log_due`] now, for a refusal logged with more
    /// than its reason.
    fn refusal_log_due_now(&self, why: &'static str) -> bool {
        self.refusal_log_due(why, now_ms())
    }

    /// Whether a refusal for `why` should be logged at `now`: at most once
    /// a minute per reason (the reasons are a handful of constants).
    fn refusal_log_due(&self, why: &'static str, now: u64) -> bool {
        let mut logged = self
            .refusal_logged
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let last = logged.entry(why).or_insert(0);
        if *last != 0 && now.saturating_sub(*last) < 60_000 {
            return false;
        }
        *last = now;
        true
    }
}

impl Cluster {
    /// `actors.call` to an actor another node owns: sends the request to
    /// that node with the caller and the chain of waiting actors, so the
    /// callee sees who called and cycles across nodes are still refused.
    ///
    /// If `owner` refuses this node's certificate at the handshake (its
    /// view of this node's heartbeat lapsed), or the HTTP client hands the
    /// call back unsent on a new TLS connection (the refusal can close it
    /// before the call is written), the call is sent once more,
    /// newly signed, once this node's next heartbeat reached the bucket
    /// and two more periods passed (`owner` lists the heartbeats once a
    /// period, plus its listing's own latency). If that heartbeat does not
    /// land within two periods, the call fails without being sent again.
    /// Safe: either way `owner` never read the request
    /// ([`never_read_by_peer`]). Over TLS 1.3 the refusal usually arrives
    /// after the request was written (this side's handshake was already
    /// done), so hyper reports a send error and does not retry.
    pub(crate) async fn call(
        &self,
        owner: &str,
        target: &ActorAddress,
        chain: &[ActorAddress],
        req: Request<http_body_util::Full<bytes::Bytes>>,
    ) -> Result<Response<HyperOutgoingBody>, String> {
        let (parts, body) = req.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(never) => match never {},
        };
        let again = Request::from_parts(parts.clone(), http_body_util::Full::new(body.clone()));
        let req = Request::from_parts(parts, http_body_util::Full::new(body));
        match self.call_once(owner, target, chain, req).await {
            Err(CallFailure::HandshakeRefused) => {
                tracing::info!(
                    owner,
                    "the owner did not read a call (it refused the handshake, or the new \
                     connection closed before the call was written); sending it again after \
                     this node's next heartbeat"
                );
                if !self.heartbeat_lands_then_two_periods().await {
                    return Err(CallFailure::HandshakeRefused.message(owner));
                }
                self.call_once(owner, target, chain, again)
                    .await
                    .map_err(|e| e.message(owner))
            }
            other => other.map_err(|e| e.message(owner)),
        }
    }

    /// Waits for this node's next heartbeat to reach the bucket (at most
    /// two periods), then two more periods: a peer's next listing, and its
    /// latency (readiness allows half a period for it; this, a whole one).
    /// `false` if the heartbeat did not land.
    async fn heartbeat_lands_then_two_periods(&self) -> bool {
        let landed = tokio::time::timeout(
            self.heartbeat_every.saturating_mul(2),
            self.heartbeat_landed.notified(),
        )
        .await
        .is_ok();
        if landed {
            tokio::time::sleep(self.heartbeat_every.saturating_mul(2)).await;
        }
        landed
    }

    /// [`Cluster::call`], sent once.
    async fn call_once(
        &self,
        owner: &str,
        target: &ActorAddress,
        chain: &[ActorAddress],
        req: Request<http_body_util::Full<bytes::Bytes>>,
    ) -> Result<Response<HyperOutgoingBody>, CallFailure> {
        let (parts, body) = req.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(never) => match never {},
        };
        let body_hash = body_sha256(&body);
        let mut req = Request::from_parts(parts, http_body_util::Full::new(body));
        let base = self
            .address_of(owner)
            .ok_or_else(|| format!("node {owner} is not reachable"))?;
        let path = req.uri().path_and_query().map_or("/", |p| p.as_str());
        let uri = format!(
            "{}/actor/{}/{}{path}",
            base.trim_end_matches('/'),
            target.class(),
            percent_encode(target.name())
        );
        let signed_path = format!(
            "/actor/{}/{}{path}",
            target.class(),
            percent_encode(target.name())
        );
        *req.uri_mut() = uri.parse().map_err(|e| format!("bad address: {e}"))?;
        let secret = self.secret().await?.to_owned();
        let method = req.method().clone();
        let headers = req.headers_mut();
        headers.insert(FORWARDED, hyper::header::HeaderValue::from_static("1"));
        // Signed below with the rest (ADR 0022).
        headers.insert(APP, self.app_header()?);
        headers.remove(CHAIN);
        for a in chain {
            if let Ok(v) = hyper::header::HeaderValue::from_str(&encode_address(a)) {
                headers.append(CHAIN, v);
            }
        }
        crate::metrics::inc(&crate::metrics::METRICS.forwards.get(&[("kind", "call")]));
        let ts = now_ms();
        let nonce = self.nonce();
        let mac = signature(
            &secret,
            ts,
            &nonce,
            owner,
            &method,
            &signed_path,
            &body_hash,
            headers,
        );
        headers.insert(
            INTERNAL,
            hyper::header::HeaderValue::from_str(&format!("v4 {ts} {nonce} {body_hash} {mac}"))
                .map_err(|e| e.to_string())?,
        );
        self.add_sender(headers, &secret, "internal", &mac);
        self.check_tls_target(&base)
            .map_err(|e| format!("node {owner}: {e}"))?;
        let call = self.clients().call.clone();
        match tokio::time::timeout(FORWARD_TIMEOUT, call.request(req)).await {
            Ok(Ok(response)) => self
                .trusted_response(&base, owner, response)
                .ok_or_else(|| CallFailure::Other(format!("node {owner} is no longer trusted"))),
            // Never read by the peer, so sent again once this node's
            // heartbeat is live there: a refused handshake, or a TLS
            // connection the refusal closed before the request was written.
            Ok(Err(e)) if never_read_by_peer(&base, &e) => Err(CallFailure::HandshakeRefused),
            Ok(Err(e)) => {
                self.failover.connect_failed(owner, &e);
                Err(CallFailure::Other(format!("node {owner}: {e}")))
            }
            Err(_) => Err(CallFailure::Other(format!(
                "node {owner} did not answer in time"
            ))),
        }
    }

    /// Hands `address`'s alarm, due at `at`, to `peer`, which arms it in
    /// its timer (ADR 0014). A signed node call, so it travels like
    /// `actors.call`: same signature (which covers the marker header),
    /// replay protection and port rule. Bounded to a few seconds: the
    /// alarm path waits for it inside the actor's mailbox.
    pub(crate) async fn hand_off_alarm(
        &self,
        peer: &str,
        address: &ActorAddress,
        at: u64,
    ) -> Result<(), HandOffError> {
        let req = Request::builder()
            .method(hyper::Method::POST)
            .uri("/")
            .header(hyper::header::HOST, "actor.wasmkeep.internal")
            .header(ALARM_HANDOFF, "1")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                alarm_handoff_body(address.tenant(), at),
            )))
            .map_err(|e| HandOffError::Failed(e.to_string()))?;
        let response = match tokio::time::timeout(
            HAND_OFF_TIMEOUT,
            // Once: a refused handshake is not waited out on the alarm path.
            async {
                self.call_once(peer, address, &[], req)
                    .await
                    .map_err(|e| e.message(peer))
            },
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return Err(HandOffError::Failed(e)),
            Err(_) => {
                return Err(HandOffError::Failed(format!(
                    "node {peer} did not answer in time"
                )));
            }
        };
        match response.status() {
            StatusCode::OK => Ok(()),
            // Not this peer's actor (another tenant, or a class it does
            // not serve): not a sign the peer is down.
            status @ (StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) => Err(HandOffError::Refused(
                format!("node {peer} answered {status}"),
            )),
            status => Err(HandOffError::Failed(format!(
                "node {peer} answered {status}"
            ))),
        }
    }

    /// Asks `peer` to take `address` now (ADR 0017): acquire, restore and
    /// arm its alarm. A signed node call (the signature covers the marker),
    /// sent only to nodes that advertise `take` on heartbeat line 11.
    pub(crate) async fn take(&self, peer: &str, address: &ActorAddress) -> TakeAnswer {
        let req = Request::builder()
            .method(hyper::Method::POST)
            .uri("/")
            .header(hyper::header::HOST, "actor.wasmkeep.internal")
            .header(TAKE, "1")
            .body(http_body_util::Full::new(bytes::Bytes::from(take_body(
                address.tenant(),
            ))));
        let req = match req {
            Ok(req) => req,
            Err(e) => return TakeAnswer::Failed(e.to_string()),
        };
        let response = match tokio::time::timeout(TAKE_TIMEOUT, async {
            self.call_once(peer, address, &[], req)
                .await
                .map_err(|e| e.message(peer))
        })
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => return TakeAnswer::Failed(e),
            Err(_) => return TakeAnswer::Failed(format!("node {peer} did not answer in time")),
        };
        match response.status() {
            StatusCode::OK => TakeAnswer::Held,
            StatusCode::CONFLICT => {
                match response.headers().get(HOLDER).and_then(|v| v.to_str().ok()) {
                    Some(holder) => TakeAnswer::HeldBy(holder.to_owned()),
                    None => TakeAnswer::Failed(format!("node {peer} answered 409")),
                }
            }
            StatusCode::NOT_FOUND => TakeAnswer::NoSuchActor,
            status => TakeAnswer::Failed(format!("node {peer} answered {status}")),
        }
    }

    /// Whether `id` opens sockets the front hands over (ADR 0027): a live
    /// peer that advertises `handoff`.
    pub(crate) fn can_handoff(&self, id: &str) -> bool {
        self.peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .is_some_and(|p| p.can_handoff)
    }

    /// Whether `id` may be a move's target (ADR 0017): an eligible peer
    /// that advertises `take`.
    pub(crate) fn can_take(&self, id: &str) -> bool {
        id != self.node_id
            && self.eligible_peers().iter().any(|p| p == id)
            && self
                .peers
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .is_some_and(|p| p.can_take)
    }

    /// The move requests this node last read (ADR 0017).
    /// Those past their 5 minutes are left out, as a listing leaves them
    /// out: the intents are listed only when they change (ADR 0014,
    /// amendment of 2026-11-14), so a request read once can outlive its
    /// expiry here until the next listing.
    pub(crate) fn moves(&self) -> Vec<wasmkeep_cluster::control::MoveIntent> {
        let now = now_ms();
        let live =
            |m: &control::MoveIntent| now.saturating_sub(m.written_at_ms) <= control::MOVE_TTL_MS;
        let deleted = {
            let mut deleted = self.deleted_moves.lock().unwrap_or_else(|p| p.into_inner());
            deleted.retain(live);
            deleted.clone()
        };
        self.control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .moves
            .iter()
            .filter(|m| live(m) && !deleted.contains(m))
            .cloned()
            .collect()
    }

    /// Why no actor may be acquired now, if so (ADR 0028, section 5): the
    /// app's tombstone was seen, or the node follows its app and has not
    /// yet looked for one since its lease was valid.
    pub(crate) fn app_refusal(&self) -> Option<&'static str> {
        match self.app_gate.load(std::sync::atomic::Ordering::SeqCst) {
            APP_OPEN => None,
            APP_UNCHECKED => Some("this node has not yet checked whether its app was deleted"),
            _ => Some("the app was deleted"),
        }
    }

    /// Acquisitions wait for a tombstone check (a node following its app).
    pub(crate) fn expect_app_check(&self) {
        let _ = self.app_gate.compare_exchange(
            APP_OPEN,
            APP_UNCHECKED,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// The lease was not valid (a lapse): a node following its app checks
    /// for the tombstone again before it acquires.
    pub(crate) fn rearm_app_check(&self) {
        if self.follows_app {
            self.expect_app_check();
        }
    }

    /// Whether acquisitions wait for a tombstone check.
    pub(crate) fn app_unchecked(&self) -> bool {
        self.app_gate.load(std::sync::atomic::Ordering::SeqCst) == APP_UNCHECKED
    }

    /// A check for the tombstone found none: acquisitions may proceed
    /// (unless one was seen before).
    pub(crate) fn app_checked(&self) {
        let _ = self.app_gate.compare_exchange(
            APP_UNCHECKED,
            APP_OPEN,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// The tombstone was seen: no actor is acquired again.
    pub(crate) fn app_tombstoned(&self) {
        self.app_gate
            .store(APP_DELETED, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether this node's lease is valid now.
    pub(crate) fn lease_valid_now(&self) -> bool {
        self.validity.valid()
    }

    /// The delete requests this node last read (ADR 0028), but those it
    /// ended itself since: the intents are listed only when they change
    /// (ADR 0014, amendment of 2026-11-14), and a listing in flight while
    /// it ended one may bring it back into the copy.
    pub(crate) fn deletes(&self) -> Vec<wasmkeep_cluster::control::DeleteIntent> {
        let now = now_ms();
        let live = |d: &control::DeleteIntent| {
            now.saturating_sub(d.written_at_ms) <= control::DELETE_TTL_MS
        };
        let ended = {
            let mut ended = self.ended_deletes.lock().unwrap_or_else(|p| p.into_inner());
            ended.retain(live);
            ended.clone()
        };
        self.control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .deletes
            .iter()
            .filter(|d| live(d) && !ended.contains(d))
            .cloned()
            .collect()
    }

    /// Ends a delete request (ADR 0028): writes its outcome beside it for
    /// the command waiting on it, then deletes the request, best effort (a
    /// request left behind is listed again and found done). Nothing is
    /// deleted if the outcome could not be written: the next pass tries
    /// again.
    pub(crate) async fn end_delete_request(
        &self,
        intent: &wasmkeep_cluster::control::DeleteIntent,
        outcome: Option<wasmkeep_cluster::control::DeleteOutcome>,
    ) {
        use wasmkeep_cluster::control::Keys as _;
        let key = intent.key();
        if let Some(outcome) = outcome
            && let Err(e) = self.control_keys.put_empty(&outcome.key(&key)).await
        {
            tracing::debug!(error = %e, "recording a delete request's outcome failed");
            return;
        }
        if let Err(e) = self.control_keys.delete(&key).await {
            tracing::debug!(error = %e, "deleting a delete request failed");
            return;
        }
        // Forgotten here, and the other nodes told (ADR 0014, amendment of
        // 2026-11-14): without it they would act on their copy, a restore
        // each to find it carried out, until their next listing.
        {
            let mut control = self.control.write().unwrap_or_else(|p| p.into_inner());
            control.deletes.retain(|d| d != intent);
        }
        self.ended_deletes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(intent.clone());
        if let Err(e) = control::announce(&self.control_keys, &self.change_token()).await {
            tracing::debug!(error = %e, "announcing an ended delete request failed");
        }
    }

    /// Two clock comparisons (collector and request reader), plus one second
    /// for buckets that expose last-modified at whole-second precision.
    pub(crate) fn restore_delete_retention_bound(&self) -> Duration {
        Duration::from_millis(
            control::RESTORE_TTL_MS
                .saturating_add(self.lease_config.max_skew_ms().saturating_mul(2))
                .saturating_add(1000),
        )
    }

    pub(crate) fn restores(&self) -> Vec<wasmkeep_cluster::control::RestoreIntent> {
        let now = now_ms();
        let live = |d: &control::RestoreIntent| {
            now.saturating_sub(d.written_at_ms) <= control::RESTORE_TTL_MS
        };
        let ended = {
            let mut ended = self
                .ended_restores
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            ended.retain(live);
            ended.clone()
        };
        self.control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .restores
            .iter()
            .filter(|d| live(d) && !ended.contains(d))
            .cloned()
            .collect()
    }

    pub(crate) async fn restore_outcome(
        &self,
        intent: &control::RestoreIntent,
        suffix: &str,
        body: &str,
    ) -> bool {
        use control::Keys as _;
        self.control_keys
            .put_body(
                &format!("{}.{suffix}", intent.key()),
                body.as_bytes().to_vec(),
            )
            .await
            .is_ok()
    }

    /// Finish only after the durable outcome is published. Suppress stale listings.
    pub(crate) async fn end_restore_request(
        &self,
        intent: &control::RestoreIntent,
        suffix: &str,
        body: String,
    ) -> bool {
        use control::Keys as _;
        let key = intent.key();
        if self
            .control_keys
            .put_body(&format!("{key}.{suffix}"), body.into_bytes())
            .await
            .is_err()
        {
            return false;
        }
        // Delete the request before its data: a crash cannot leave an executable
        // request with missing data unless its durable marker already exists.
        if self.control_keys.delete(&key).await.is_ok() {
            let _ = self.control_keys.delete(&format!("{key}.ltx")).await;
        }
        self.control
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .restores
            .retain(|r| r != intent);
        self.ended_restores
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(intent.clone());
        let _ = control::announce(&self.control_keys, &self.change_token()).await;
        true
    }

    /// Fail closed on an app tombstone, whole-app restore, or unreadable gate.
    pub(crate) async fn actor_restore_allowed(&self) -> bool {
        let nodes = tokio::time::timeout(
            Duration::from_secs(10),
            wasmkeep_cluster::membership::all_heartbeats(&self.control_keys),
        )
        .await;
        if !matches!(nodes, Ok(Ok(nodes)) if nodes.iter().all(|node|
            node.app != format!("{}/{}", self.root.tenant(), self.root.app()) || node.can_actor_restore))
        {
            return false;
        }
        for marker in ["deleted", "restoring"] {
            let path = StorePath::from(format!(
                "{}apps/{}/{}/{marker}",
                self.root.prefix(),
                self.root.tenant(),
                self.root.app()
            ));
            if !matches!(
                tokio::time::timeout(Duration::from_secs(10), self.store.head(&path)).await,
                Ok(Err(object_store::Error::NotFound { .. }))
            ) {
                return false;
            }
        }
        self.lease_valid_now() && self.app_refusal().is_none()
    }

    /// Bounded request/staged object read, including the streaming body.
    pub(crate) async fn restore_object(&self, key: &str, cap: u64) -> Result<Vec<u8>, String> {
        use futures_util::StreamExt as _;
        let path = StorePath::from(format!("{}{key}", self.root.prefix()));
        tokio::time::timeout(
            Duration::from_secs(30) + Duration::from_secs(cap / (1024 * 1024)),
            async {
                let got = self.store.get(&path).await.map_err(|e| e.to_string())?;
                if got.meta.size > cap {
                    return Err("restore object exceeds limit".into());
                }
                let mut stream = got.into_stream();
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| e.to_string())?;
                    if (bytes.len() as u64).saturating_add(chunk.len() as u64) > cap {
                        return Err("restore object exceeds limit".into());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            },
        )
        .await
        .map_err(|_| "restore read timed out".to_owned())?
    }

    /// Deletes a move request, best effort, forgets it here, and tells the
    /// other nodes (`control/changed`, ADR 0014, amendment of 2026-11-14),
    /// which would otherwise act on it until their next listing.
    pub(crate) async fn delete_move(&self, intent: &wasmkeep_cluster::control::MoveIntent) {
        use wasmkeep_cluster::control::Keys as _;
        if let Err(e) = self.control_keys.delete(&intent.key()).await {
            tracing::debug!(error = %e, "deleting a move request failed");
            return;
        }
        self.control
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .moves
            .retain(|m| m != intent);
        // A listing that started before the delete may still bring it back
        // into the copy: left out of [`Self::moves`] until it expires.
        self.deleted_moves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(intent.clone());
        if let Err(e) = control::announce(&self.control_keys, &self.change_token()).await {
            tracing::debug!(error = %e, "announcing a deleted move request failed");
        }
    }

    /// A body for `control/changed` no writer wrote before: this process's
    /// random seed, a counter and the time.
    fn change_token(&self) -> String {
        format!("{} {}", self.nonce(), now_ms())
    }

    /// How long a node that accepted an alarm hand-off looks again while
    /// the actor's record names another node (ms): twice the time a record
    /// naming a stopped node can last before a peer fences it (ttl plus
    /// grace), and at least `HANDED_RECHECK_MS`.
    pub(crate) fn handed_recheck_ms(&self) -> u64 {
        let fenced = self
            .lease_config
            .ttl_ms()
            .saturating_add(self.lease_config.grace_ms());
        fenced
            .saturating_mul(2)
            .max(crate::actor::HANDED_RECHECK_MS)
    }

    /// Whether node `id` is asked to drain, as this node last read.
    pub(crate) fn is_draining_node(&self, id: &str) -> bool {
        self.control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .draining
            .contains(id)
    }

    /// Key prefix in the bucket, ending in `/` (or empty).
    pub(crate) fn store_prefix(&self) -> &str {
        self.root.prefix()
    }

    /// An actor's per-actor lease from before ADR 0020, replaced by a
    /// marker at its first acquisition under node leases (its generation
    /// seeds the first ownership record). The name is encoded like the
    /// database file name, so any actor name is a safe key.
    pub(crate) fn lease_key(&self, address: &ActorAddress) -> String {
        self.assert_mine(address);
        self.root.key(
            Family::Leases,
            &format!(
                "{}/{}",
                address.class(),
                wasmkeep_storage::encode_segment(address.name())
            ),
        )
    }

    /// The folder of `address`'s history in the bucket (ADR 0007), with
    /// the prefix and without a trailing `/`.
    pub(crate) fn history_key(&self, address: &ActorAddress) -> String {
        self.assert_mine(address);
        self.root.bucket_key(
            Family::History,
            &format!(
                "{}/{}",
                address.class(),
                wasmkeep_storage::encode_segment(address.name())
            ),
        )
    }

    /// Base URL to reach a node at, if its heartbeat is fresh: its cluster
    /// listener (mutual TLS) if it published one and this node has an
    /// identity, else its plain address (none in refuse mode). Never the
    /// plain address of a node reachable over TLS: if TLS to it fails, the
    /// caller gets a 503.
    ///
    /// A node this node has not seen at all (a record names a node that
    /// started since the last listing, whose hello did not arrive) asks
    /// for a listing of `nodes/` (at most one early listing a period; ADR
    /// 0020, amendment of 2026-11-14), so its actors' requests get a 503
    /// for a period, not until the next scheduled listing.
    pub(crate) fn address_of(&self, node: &str) -> Option<String> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        let Some(peer) = peers.get(node) else {
            drop(peers);
            let known = self
                .known_nodes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .any(|k| k == node);
            if !known && node != self.node_id {
                self.want_listing();
            }
            return None;
        };
        match (&self.transport.tls, &peer.tls) {
            (Some(_), Some(tls)) => Some(tls.clone()),
            _ if self.refuse_plaintext => None,
            _ => peer.plain.clone(),
        }
    }

    /// This node's TLS identity and its peers' pins, with a cluster
    /// listener.
    pub(crate) fn tls(&self) -> Option<&Arc<ClusterTls>> {
        self.transport.tls.as_ref()
    }

    fn clients(&self) -> Arc<Clients> {
        self.transport.clients()
    }

    /// Before sending to `base` over TLS (perhaps on a pooled connection):
    /// the heartbeat naming that address must still be live. The pins are
    /// checked by the handshake; pools are dropped when they change.
    fn check_tls_target(&self, base: &str) -> Result<(), String> {
        if !base.starts_with("https://") {
            return if self.refuse_plaintext {
                Err("plain HTTP between nodes is refused".into())
            } else {
                Ok(())
            };
        }
        let tls = self
            .transport
            .tls
            .as_ref()
            .ok_or("TLS between nodes is not configured")?;
        let uri: hyper::Uri = base.parse().map_err(|e| format!("bad address: {e}"))?;
        tls.expected(&uri).map(|_| ())
    }

    /// For an answer or a connection from `base`, node `node`: given the pin the peer
    /// presented, whether that pin is still trusted there (checked again
    /// for as long as the answer streams or the connection stays open).
    /// `None` for plain HTTP.
    #[allow(clippy::type_complexity)]
    pub(crate) fn revocation(
        &self,
        base: &str,
        node: &str,
    ) -> Option<Box<dyn FnOnce(Option<[u8; 32]>) -> crate::cluster_tls::StillTrusted + Send>> {
        let tls = Arc::clone(self.transport.tls.as_ref()?);
        let uri: hyper::Uri = base.parse().ok().filter(|_| base.starts_with("https://"))?;
        // Trusted while the pin is published at the address and by the
        // destination node id itself (ADR 0038, section 5): a sibling app's
        // heartbeat on the same process never keeps it trusted.
        let node = node.to_owned();
        Some(Box::new(move |pin| {
            Arc::new(move || {
                tls.still_trusted(&uri, pin) && pin.is_some_and(|p| tls.pins().publishes(&p, &node))
            })
        }))
    }

    /// A response from `base` (over TLS): `None` if the peer that answered
    /// is no longer trusted there (its pin lapsed while it worked on the
    /// request), else the response, its body cut if that happens while it
    /// streams.
    fn trusted_response(
        &self,
        base: &str,
        node: &str,
        response: Response<Incoming>,
    ) -> Option<Response<HyperOutgoingBody>> {
        let pin = response
            .extensions()
            .get::<crate::cluster_tls::PeerPin>()
            .map(|p| p.0);
        let still = self.revocation(base, node).map(|r| r(pin));
        if still.as_ref().is_some_and(|s| !s()) {
            return None;
        }
        Some(response.map(|b| {
            let body = b.map_err(Into::into).boxed_unsync();
            match still {
                Some(s) => crate::cluster_tls::revocable_body(body, s),
                None => body,
            }
        }))
    }

    /// Forwards a request for an actor to the node that owns it.
    pub(crate) async fn forward(
        &self,
        owner: &str,
        mut req: Request<Incoming>,
    ) -> Response<HyperOutgoingBody> {
        if never_forward(&req) {
            // Two nodes disagree about who owns the actor (a lease is
            // changing hands). Do not bounce; let the client retry.
            return unavailable("actor ownership is changing; retry\n");
        }
        let Some(base) = self.address_of(owner) else {
            return unavailable("the actor's node is not reachable; retry\n");
        };
        // Routing cut `/actor/<class>/<name>` off the path; send the original.
        let path = match req.extensions().get::<OriginalPath>() {
            Some(original) => original.0.clone(),
            None => return unavailable("cannot forward this request\n"),
        };
        let Ok(uri) = format!("{}{path}", base.trim_end_matches('/')).parse::<hyper::Uri>() else {
            return unavailable("bad advertised address\n");
        };
        *req.uri_mut() = uri;
        // The client's connection headers are for its hop to this node: a
        // `Connection: close` passed on would close the pooled connection to
        // the owner (over TLS, a handshake per request). The body streams
        // through unchanged, so its framing headers stay. HTTP/1.0 would
        // close the connection after each response; the hop to the owner
        // is HTTP/1.1 whatever the client speaks (a 1.0 body is never
        // chunked, so its framing still holds).
        strip_hop_by_hop(req.headers_mut(), Framing::Keep);
        *req.version_mut() = hyper::Version::HTTP_11;
        // This node answers a client's `Expect: 100-continue` itself, as
        // soon as the body starts streaming to the owner, which is at
        // once. Passed on, it would tell the owner that the sender waits
        // before sending, and to leave an unread body unread (see
        // `CappedBody`).
        req.headers_mut().remove(hyper::header::EXPECT);
        req.headers_mut()
            .insert(FORWARDED, hyper::header::HeaderValue::from_static("1"));
        // This node's app, never the client's (ADR 0022).
        match self.app_header() {
            Ok(app) => req.headers_mut().insert(APP, app),
            Err(_) => return unavailable("cannot forward this request\n"),
        };
        if self.sign_admission(owner, &path, &mut req).await.is_err() {
            return unavailable("cannot authenticate ingress admission; retry\n");
        }
        crate::metrics::inc(&crate::metrics::METRICS.forwards.get(&[("kind", "request")]));
        // Bound the wait for the owner's response headers (a hung node or a
        // half-open connection); the body then streams as the owner sends it.
        if let Err(e) = self.check_tls_target(&base) {
            tracing::warn!(owner, error = %e, "forwarding refused");
            return unavailable("the actor's node is not reachable; retry\n");
        }
        let client = self.clients().request.clone();
        match tokio::time::timeout(FORWARD_TIMEOUT, client.request(req)).await {
            Ok(Ok(response)) => self
                .trusted_response(&base, owner, response)
                .unwrap_or_else(|| {
                    tracing::warn!(owner, "forwarding: the node is no longer trusted");
                    unavailable("the actor's node is no longer trusted; retry\n")
                }),
            // Every failure before an answer, none sent again from here:
            // a connection that could not be opened, a request the client
            // handed back unsent on a new connection, and a request
            // written into a connection that then failed, which the owner
            // may have read. (hyper-util itself sends an unsent request
            // again only on a reused pooled connection, and drops one it
            // finds closed when it takes it from the pool.) The body
            // streams from the client, so the first two cannot be sent
            // again here either. Of the three users of the peer clients,
            // `actors.call` (`Cluster::call`) and a WebSocket upgrade
            // (`Cluster::forward_upgrade`, which has no body) send again,
            // once, a request the owner provably never read
            // (`never_read_by_peer`: refused at the TLS handshake, or
            // handed back unsent on a new TLS connection), and nothing
            // else; this path answers 503 for all of them.
            Ok(Err(e)) => {
                // No connection could be opened: requests that arrive
                // later for this node's actors may be kept (`keep`). This
                // one is answered as ever.
                self.failover.connect_failed(owner, &e);
                tracing::warn!(owner, error = %e, "forwarding failed");
                unavailable("the actor's node did not answer; retry\n")
            }
            Err(_) => {
                tracing::warn!(owner, "forwarding timed out");
                text_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "the actor's node did not answer in time\n",
                )
            }
        }
    }
}

/// Whether a request to the peer at `base` failed in a way that proves the
/// peer never read it, so that sending it again cannot run it twice. Two
/// cases:
///
/// - The peer refused the TLS handshake
///   ([`crate::cluster_tls::handshake_refused`]: one of a fixed list of
///   fatal alerts arrived). The request may have been written, TLS 1.3
///   letting this side write before the refusal arrives, but the peer did
///   not read it: a rustls server sends those alerts only while it handles
///   the ClientHello, the client's certificate or its CertificateVerify,
///   states in which it passes no application data on. (It reads
///   application data before its handshake is done only as early data,
///   which the cluster listener leaves off, with resumption.) Once its
///   handshake is done, it sends none of them. Checked in the source of
///   rustls 0.23.46, the server's side of TLS 1.3, the only version
///   between nodes; to check again when that dependency changes.
/// - Over TLS, the HTTP client handed the request back before writing any
///   of it ([`crate::cluster_tls::request_unsent`]) on a new connection
///   (a request it gets back on a connection reused from the pool, it
///   sends on another connection itself). A refusal that closes the
///   connection before the write looks like this; so does any other close
///   of a new connection before the write, which is as safe to send
///   again.
fn never_read_by_peer(base: &str, error: &hyper_util::client::legacy::Error) -> bool {
    crate::cluster_tls::handshake_refused(error)
        || (base.starts_with("https://") && crate::cluster_tls::request_unsent(error))
}

/// Why a call to another node failed.
#[derive(Debug)]
enum CallFailure {
    /// The node never read the request ([`never_read_by_peer`]): it
    /// refused this node's certificate at the handshake, or its new TLS
    /// connection gave the request back unsent, as a refusal can.
    HandshakeRefused,
    Other(String),
}

impl From<String> for CallFailure {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

impl CallFailure {
    fn message(self, node: &str) -> String {
        match self {
            Self::HandshakeRefused => format!(
                "node {node} refused this node's certificate (this node's heartbeat is not live there)"
            ),
            Self::Other(message) => message,
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// `n` random bytes from the OS, as hex.
fn random_hex(n: usize) -> std::io::Result<String> {
    use std::io::Read;
    let mut buf = vec![0u8; n];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod sender_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod state_keys_tests;

/// Propagate each EOF while draining the reverse stream. The first completed
/// copy starts one close-wait deadline covering both shutdowns and the drain.
async fn relay_until_eof<A, B>(a: &mut A, b: &mut B, close_wait: Duration)
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let (ab_done, ab_eof) = tokio::sync::oneshot::channel();
    let (ba_done, ba_eof) = tokio::sync::oneshot::channel();
    let ab = async {
        let _ = tokio::io::copy(&mut ar, &mut bw).await;
        let _ = ab_done.send(());
        let _ = bw.shutdown().await;
    };
    let ba = async {
        let _ = tokio::io::copy(&mut br, &mut aw).await;
        let _ = ba_done.send(());
        let _ = aw.shutdown().await;
    };
    let drain = async {
        tokio::join!(ab, ba);
    };
    tokio::pin!(drain);
    tokio::select! {
        _ = &mut drain => return,
        _ = ab_eof => {},
        _ = ba_eof => {},
    }
    // Keep polling both directions, including shutdown/flush, under the
    // same bound. Never cancel a copy early and lose its buffered bytes.
    let _ = tokio::time::timeout(close_wait, drain).await;
}

#[cfg(test)]
mod relay_tests;
