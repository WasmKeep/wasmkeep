//! Node discovery through the bucket: each node writes a heartbeat object,
//! `nodes/<node-id>`, every few seconds; everyone lists them to know which
//! nodes are alive and where to forward requests.
//!
//! A heartbeat's own fields are advisory: they drive placement and
//! forwarding, never ownership. Ownership is decided only by ownership
//! records and node leases (`crate::node_lease`), so a stale or missing
//! listing can cause a wasted forward, not two owners.
//!
//! Format 3 ([ADR 0020]) makes the heartbeat a node lease: its first three
//! lines are the lease (`crate::node_lease`: state, instance, `written_at`),
//! written conditionally, and the heartbeat's own fields follow from the id
//! line on ([`NodeInfo::payload`]). Every reader decodes it. The server
//! writes format 3 through its node lease (`crate::node_lease::NodeLease`),
//! with this module's [`Membership::lease_payload`] after the lease lines;
//! this module only reads. Formats 1 and 2 are read, never written: the
//! writers are kept for tests only.
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use crate::lease::{BucketError, Clock};
use crate::node_lease::{NodeLeaseObject, NodeState};

/// Listing heartbeats (and, in tests, writing them unconditionally).
#[allow(async_fn_in_trait)]
pub trait Directory {
    /// Writes `key` unconditionally (tests only: nodes write their
    /// heartbeat as a node lease, conditionally).
    #[cfg(test)]
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError>;
    /// Every object under `prefix`, with its body.
    async fn list(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, BucketError>;
}

/// Key prefix of heartbeat objects.
pub const NODES_PREFIX: &str = "nodes/";

/// What a node publishes about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Stable node id, `[a-z0-9-]{1,64}`.
    pub id: String,
    /// Where other nodes reach it over plain HTTP (its advertised base URL).
    pub address: String,
    /// WasmKeep version it runs.
    pub version: String,
    /// When it wrote this heartbeat, on its own clock (ms).
    pub written_at_ms: u64,
    /// What it serves: `tenant/app`, `default/default` for classes given
    /// on the command line (the implicit app, ADR 0022); `tenant/` from
    /// nodes of the version before it, empty from nodes that predate both.
    pub app: String,
    /// Its cluster listener (`https://host:port`), when it has one
    /// ([ADR 0011](../../../docs/adr/0011-node-to-node-mtls.md)).
    pub cluster_address: Option<String>,
    /// SHA-256 fingerprints of the certificates it presents there (one, or
    /// two while it changes); empty without a cluster listener.
    pub tls_pins: Vec<[u8; 32]>,
    /// Whether it takes actors ([ADR 0014](../../../docs/adr/0014-draining-a-node-and-moving-an-actor.md));
    /// `None` from nodes that predate draining, which never receive a
    /// drained node's actors.
    pub drain: Option<DrainState>,
    /// It reads the app's secrets from the bucket (heartbeat line 10, ADR
    /// 0016); `false` from nodes that predate it.
    pub reads_secrets: bool,
    /// It answers take calls, so it can be a move's target (heartbeat line
    /// 11, `take`, ADR 0017); `false` from nodes that predate it.
    pub can_take: bool,
    /// It runs a version's front and reads manifest format 3 and the
    /// `_front` secrets scope (heartbeat line 11, `front`, ADR 0023);
    /// `false` from nodes that predate it, which cannot read such a
    /// version or set.
    pub can_front: bool,
    /// A token random per process (heartbeat line 11, `instance=<hex>`):
    /// two processes writing this id's heartbeat show two tokens, which
    /// each notices in its next listing. `None` from nodes that predate it.
    /// In format 3 it is the node lease's instance (its second line).
    pub instance: Option<String>,
    /// The node lease's state (format 3); `None` in formats 1 and 2, whose
    /// liveness is their age alone. A `released` or `fenced` node is not
    /// live whatever its age.
    pub lease: Option<NodeState>,
    /// What it runs (format 3, payload line 10, `load ...`); `None` from
    /// nodes that predate it, or when that line is missing or malformed.
    /// Only a live node's says anything: a `released` or `fenced` object
    /// keeps the last payload its node wrote.
    pub load: Option<Load>,
}

/// Payload line 10 (format 3): how much a node runs, as of its last
/// heartbeat. Shown, never acted on: no placement or routing reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Load {
    /// Actors resident in memory.
    pub actors: u64,
    /// WebSockets open to its actors.
    pub sockets: u64,
    /// Events run per minute over the last minute or more (every node of
    /// the process counted); `None` until a minute of samples exists.
    pub events_per_min: Option<u64>,
    /// Front requests (ADR 0023) per minute, measured as `events_per_min`
    /// is; `None` until a minute of samples exists, and from a node of a
    /// version before it (which writes no `front_per_min`).
    pub front_per_min: Option<u64>,
}

impl Load {
    /// The line: `load actors=<n> sockets=<n> events_per_min=<n|->
    /// front_per_min=<n|->`.
    fn line(&self) -> String {
        let rate = |r: Option<u64>| r.map_or_else(|| "-".to_owned(), |n| n.to_string());
        format!(
            "load actors={} sockets={} events_per_min={} front_per_min={}",
            self.actors,
            self.sockets,
            rate(self.events_per_min),
            rate(self.front_per_min)
        )
    }

    /// Reads the line: the word `load`, then `key=value` words in any
    /// order. `actors` and `sockets` are required; `events_per_min` and
    /// `front_per_min` may be `-` or absent (unknown). Unknown keys and words without `=` are
    /// skipped, for later versions; a known key repeated or with a value
    /// that is not a count is `None`.
    fn parse(line: &str) -> Option<Self> {
        let mut words = line.split_whitespace();
        if words.next()? != "load" {
            return None;
        }
        let (mut actors, mut sockets, mut events, mut front) = (None, None, None, None);
        for word in words {
            let Some((key, value)) = word.split_once('=') else {
                continue;
            };
            let slot = match key {
                "actors" => &mut actors,
                "sockets" => &mut sockets,
                "events_per_min" => &mut events,
                "front_per_min" => &mut front,
                _ => continue,
            };
            if slot.is_some() {
                return None;
            }
            let rate = key == "events_per_min" || key == "front_per_min";
            *slot = Some(if rate && value == "-" {
                None
            } else {
                Some(number(value)?)
            });
        }
        Some(Self {
            actors: actors??,
            sockets: sockets??,
            events_per_min: events.flatten(),
            front_per_min: front.flatten(),
        })
    }
}

/// A count as heartbeats write it: decimal digits only.
fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Heartbeat line 9: whether a node takes actors (ADR 0014).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DrainState {
    /// Takes actors as usual.
    #[default]
    Serving,
    /// Handing its actors over; claims none.
    Draining,
    /// Draining, and holds no actors any more.
    Drained,
    /// Draining, with actors left and no node to hand them to.
    Stuck,
}

impl DrainState {
    /// The word heartbeats and `cluster status` use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Serving => "serving",
            Self::Draining => "draining",
            Self::Drained => "drained",
            Self::Stuck => "stuck",
        }
    }

    fn parse(line: &str) -> Option<Self> {
        Some(match line {
            "serving" => Self::Serving,
            "draining" => Self::Draining,
            "drained" => Self::Drained,
            "stuck" => Self::Stuck,
            _ => return None,
        })
    }
}

/// Most pins a heartbeat may carry.
pub const MAX_PINS: usize = 2;

/// A node id that cannot be used as an object key segment.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid node id {0:?}: use 1 to 64 of [a-z0-9-]")]
pub struct InvalidNodeId(pub String);

/// Checks a node id: `[a-z0-9-]{1,64}`.
pub fn validate_node_id(id: &str) -> Result<(), InvalidNodeId> {
    let ok = !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(InvalidNodeId(id.to_owned()))
    }
}

impl NodeInfo {
    #[cfg(test)]
    fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "wasmkeep-node 2\n{}\n{}\n{}\n{}\n{}\n",
            self.id, self.address, self.version, self.written_at_ms, self.app
        );
        // Lines 7 and 8 (cluster address, pins) are written empty without
        // TLS, so line 9 is line 9 on every node. Readers drop an empty or
        // malformed line 7 or 8, and older ones read no further than 8: to
        // them the heartbeat reads exactly as before.
        if self.cluster_address.is_some() || !self.tls_pins.is_empty() || self.drain.is_some() {
            out.push_str(self.cluster_address.as_deref().unwrap_or(""));
            out.push('\n');
            let pins: Vec<String> = self.tls_pins.iter().map(hex).collect();
            out.push_str(&pins.join(" "));
            out.push('\n');
        }
        if let Some(state) = self.drain {
            out.push_str(state.as_str());
            out.push('\n');
            // Line 10 is written (empty if need be) when line 11 follows,
            // so line 11 stays line 11.
            if self.reads_secrets || self.capabilities().next().is_some() || self.instance.is_some()
            {
                out.push_str(if self.reads_secrets {
                    "secrets\n"
                } else {
                    "\n"
                });
            }
            // Line 11: capability words, then the process's token.
            let mut words: Vec<String> = self.capabilities().map(str::to_owned).collect();
            if let Some(instance) = &self.instance {
                words.push(format!("instance={instance}"));
            }
            if !words.is_empty() {
                out.push_str(&words.join(" "));
                out.push('\n');
            }
        }
        out.into_bytes()
    }

    /// Format 2 adds the app; format 1 (older nodes) has none. Lines 7 and
    /// 8 (cluster address, pins) are optional; malformed ones are dropped,
    /// never trusted. Line 9 (drain state) is optional too; a node without
    /// a readable one is treated as predating it. Format 3 is the node
    /// lease (ADR 0020): its three lease lines, then these fields.
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-node 1" => 1,
            "wasmkeep-node 2" => 2,
            "wasmkeep-node 3" => return Self::decode_lease(body),
            _ => return None,
        };
        Some(Self {
            id: lines.next()?.to_owned(),
            address: lines.next()?.to_owned(),
            version: lines.next()?.to_owned(),
            written_at_ms: lines.next()?.parse().ok()?,
            app: if format >= 2 {
                lines.next()?.to_owned()
            } else {
                String::new()
            },
            cluster_address: if format >= 2 {
                lines.next().and_then(cluster_address)
            } else {
                None
            },
            tls_pins: if format >= 2 {
                lines.next().map(pins).unwrap_or_default()
            } else {
                Vec::new()
            },
            drain: if format >= 2 {
                lines.next().and_then(DrainState::parse)
            } else {
                None
            },
            reads_secrets: format >= 2 && lines.next() == Some("secrets"),
            ..Self::words(if format >= 2 { lines.next() } else { None })
        })
    }

    /// The heartbeat's fields as format 3 carries them after the node
    /// lease's three lines: id, address, version, app, cluster address,
    /// pins, drain state, `secrets`, capability words, one per line, every
    /// line written (empty if need be), then the load (line 10) once known.
    /// `written_at` and the instance are not in it: the lease lines carry
    /// them. Readers from before line 10 read 9 lines and stop, so it is
    /// invisible to them.
    pub fn payload(&self) -> Vec<u8> {
        let pins: Vec<String> = self.tls_pins.iter().map(hex).collect();
        let mut out = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
            self.id,
            self.address,
            self.version,
            self.app,
            self.cluster_address.as_deref().unwrap_or(""),
            pins.join(" "),
            self.drain.unwrap_or_default().as_str(),
            if self.reads_secrets { "secrets" } else { "" },
            self.capabilities().collect::<Vec<_>>().join(" "),
        );
        if let Some(load) = &self.load {
            out.push_str(&load.line());
            out.push('\n');
        }
        out.into_bytes()
    }

    /// Format 3: the node lease's lines, then [`NodeInfo::payload`]; a
    /// missing or malformed load line reads as no load. A body
    /// with no payload (an operator's fence over an absent object) names no
    /// node and reads as `None`.
    fn decode_lease(body: &[u8]) -> Option<Self> {
        let lease = NodeLeaseObject::decode(body)?;
        let text = std::str::from_utf8(crate::node_lease::tail(body)).ok()?;
        let mut lines = text.lines();
        let id = lines.next()?.to_owned();
        let address = lines.next()?.to_owned();
        let version = lines.next()?.to_owned();
        let app = lines.next()?.to_owned();
        let cluster_address = lines.next().and_then(cluster_address);
        let tls_pins = lines.next().map(pins).unwrap_or_default();
        let drain = lines.next().and_then(DrainState::parse);
        let reads_secrets = lines.next() == Some("secrets");
        let words = Self::words(lines.next());
        let load = lines.next().and_then(Load::parse);
        Some(Self {
            id,
            address,
            version,
            written_at_ms: lease.written_at_ms,
            app,
            cluster_address,
            tls_pins,
            drain,
            reads_secrets,
            can_take: words.can_take,
            can_front: words.can_front,
            instance: lease.instance,
            lease: Some(lease.state),
            load,
        })
    }
}

impl NodeInfo {
    /// The capability words this node announces on line 11, in order
    /// (`take`, `front`). Readers ignore words they do not know.
    fn capabilities(&self) -> impl Iterator<Item = &'static str> {
        [(self.can_take, "take"), (self.can_front, "front")]
            .into_iter()
            .filter_map(|(has, word)| has.then_some(word))
    }

    /// What line 11 says: capability words (`take`, `front`) and the process's
    /// token (`instance=<hex>`, 1 to 64 lowercase hex digits; anything else
    /// is ignored). Every other field is empty.
    fn words(line: Option<&str>) -> Self {
        let words: Vec<&str> = line
            .map(|l| l.split_whitespace().collect())
            .unwrap_or_default();
        Self {
            id: String::new(),
            address: String::new(),
            version: String::new(),
            written_at_ms: 0,
            app: String::new(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: None,
            reads_secrets: false,
            lease: None,
            load: None,
            can_take: words.contains(&"take"),
            can_front: words.contains(&"front"),
            instance: words.iter().find_map(|w| {
                let token = w.strip_prefix("instance=")?;
                let ok = !token.is_empty()
                    && token.len() <= 64
                    && token
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
                ok.then(|| token.to_owned())
            }),
        }
    }
}

/// A cluster address as published: `https://` and no whitespace.
fn cluster_address(line: &str) -> Option<String> {
    let ok = line.len() > "https://".len()
        && line.starts_with("https://")
        && !line.chars().any(char::is_whitespace);
    ok.then(|| line.to_owned())
}

/// Pins as published: up to [`MAX_PINS`] of 64 lowercase hex, separated by
/// one space. Anything else yields none.
fn pins(line: &str) -> Vec<[u8; 32]> {
    if line.is_empty() {
        return Vec::new();
    }
    let parsed: Option<Vec<[u8; 32]>> = line.split(' ').map(unhex).collect();
    match parsed {
        Some(p) if p.len() <= MAX_PINS => p,
        _ => Vec::new(),
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, pair) in b.as_chunks::<2>().0.iter().enumerate() {
        out[i] = digit(pair[0])? << 4 | digit(pair[1])?;
    }
    Some(out)
}

/// Reads this node's load as a heartbeat is built
/// ([`Membership::set_load_source`]).
pub type LoadSource = Box<dyn Fn() -> Load + Send + Sync>;

/// A [`LoadSource`], printable.
struct LoadFn(LoadSource);

impl std::fmt::Debug for LoadFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LoadFn")
    }
}

/// Heartbeats for one node, and the view of the others.
#[derive(Debug)]
pub struct Membership<D, C> {
    directory: D,
    clock: C,
    id: String,
    address: String,
    version: String,
    app: String,
    cluster_address: Option<String>,
    tls_pins: Vec<[u8; 32]>,
    /// Published on line 9 with every heartbeat; set by the drain logic.
    drain: std::sync::Mutex<DrainState>,
    /// Published on line 10 (ADR 0016).
    reads_secrets: bool,
    /// Published on line 11: this process's token.
    instance: Option<String>,
    /// Published on payload line 10 (format 3); set by the server, or read
    /// from `load_source` as each payload is built (the last read kept).
    load: std::sync::Mutex<Option<Load>>,
    load_source: std::sync::OnceLock<LoadFn>,
    /// A heartbeat older than this (plus `max_skew_ms`) means the node is gone.
    ttl_ms: u64,
    max_skew_ms: u64,
}

impl<D: Directory, C: Clock> Membership<D, C> {
    /// Membership for node `id`, reachable at `address`.
    pub fn new(
        directory: D,
        clock: C,
        id: &str,
        address: impl Into<String>,
        ttl_ms: u64,
        max_skew_ms: u64,
    ) -> Result<Self, InvalidNodeId> {
        validate_node_id(id)?;
        Ok(Self {
            directory,
            clock,
            id: id.to_owned(),
            address: address.into(),
            version: crate::VERSION.to_owned(),
            app: String::new(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: std::sync::Mutex::new(DrainState::Serving),
            reads_secrets: true,
            instance: None,
            load: std::sync::Mutex::new(None),
            load_source: std::sync::OnceLock::new(),
            ttl_ms,
            max_skew_ms,
        })
    }

    /// Whether line 10 says this node reads the app's secrets from the
    /// bucket (ADR 0016): not for a node run with `--no-secrets`.
    #[must_use]
    pub fn with_reads_secrets(mut self, reads: bool) -> Self {
        self.reads_secrets = reads;
        self
    }

    /// Publishes `token` (random per process) with every heartbeat, so a
    /// second process writing this id is seen ([`NodeInfo::instance`]).
    #[must_use]
    pub fn with_instance(mut self, token: impl Into<String>) -> Self {
        self.instance = Some(token.into());
        self
    }

    /// This process's token, as published.
    pub fn instance(&self) -> Option<&str> {
        self.instance.as_deref()
    }

    /// Names what this node serves, published with its heartbeat (see
    /// [`NodeInfo::app`]).
    #[must_use]
    pub fn with_app(mut self, app: impl Into<String>) -> Self {
        self.app = app.into();
        self
    }

    /// Publishes a cluster listener and the pins of its certificates with
    /// every heartbeat ([`NodeInfo::cluster_address`]). At most
    /// [`MAX_PINS`] pins are kept.
    #[must_use]
    pub fn with_cluster_tls(mut self, address: impl Into<String>, mut pins: Vec<[u8; 32]>) -> Self {
        pins.truncate(MAX_PINS);
        self.cluster_address = Some(address.into());
        self.tls_pins = pins;
        self
    }

    /// Sets what the next heartbeats say on line 9 (ADR 0014).
    pub fn set_drain_state(&self, state: DrainState) {
        *self.drain.lock().unwrap_or_else(|p| p.into_inner()) = state;
    }

    /// What this node's heartbeats say on line 9.
    pub fn drain_state(&self) -> DrainState {
        *self.drain.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Sets what the next node lease writes say on payload line 10.
    pub fn set_load(&self, load: Load) {
        *self.load.lock().unwrap_or_else(|p| p.into_inner()) = Some(load);
    }

    /// Reads the load from `source` each time a heartbeat is built, instead
    /// of what [`Self::set_load`] last set: each node lease write then
    /// carries the load as of that write (a takeover shows at the next
    /// one, no later), read just after the drain state, so a `drained`
    /// node never shows actors counted before it handed them over. The
    /// first source set stays.
    pub fn set_load_source(&self, source: LoadSource) {
        let _ = self.load_source.set(LoadFn(source));
    }

    /// What this node's next heartbeats say on payload line 10, if set (as
    /// last read, with a source).
    pub fn load(&self) -> Option<Load> {
        *self.load.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The load for a heartbeat built now: read from the source if there
    /// is one (and kept), else as last set.
    fn load_now(&self) -> Option<Load> {
        let Some(source) = self.load_source.get() else {
            return self.load();
        };
        let load = (source.0)();
        *self.load.lock().unwrap_or_else(|p| p.into_inner()) = Some(load);
        Some(load)
    }

    /// Writes this node's heartbeat in format 2 (tests only).
    #[cfg(test)]
    pub(crate) async fn heartbeat(&self) -> Result<(), BucketError> {
        let info = self.info();
        self.directory
            .put(&format!("{NODES_PREFIX}{}", self.id), info.encode())
            .await
    }

    /// What the node lease writes after its three lines (format 3): this
    /// node's fields as of now ([`NodeInfo::payload`]), drain state
    /// included. The lease task sets it before each write.
    pub fn lease_payload(&self) -> Vec<u8> {
        self.info().payload()
    }

    /// This node's heartbeat as of now: the drain state read first, then
    /// the load, so the load is never older than the state.
    fn info(&self) -> NodeInfo {
        let drain = Some(self.drain_state());
        let load = self.load_now();
        NodeInfo {
            id: self.id.clone(),
            address: self.address.clone(),
            version: self.version.clone(),
            written_at_ms: self.clock.now_ms(),
            app: self.app.clone(),
            cluster_address: self.cluster_address.clone(),
            tls_pins: self.tls_pins.clone(),
            drain,
            reads_secrets: self.reads_secrets,
            can_take: true,
            can_front: true,
            instance: self.instance.clone(),
            lease: None,
            load,
        }
    }

    /// This node id's heartbeat object as the bucket holds it (whoever
    /// wrote it): its bytes, and what they say if this version can read
    /// them. `None` if there is no such object.
    pub async fn own_heartbeat(&self) -> Result<Option<(Vec<u8>, Option<NodeInfo>)>, BucketError> {
        let key = format!("{NODES_PREFIX}{}", self.id);
        // Listed under `nodes/`: a bucket lists by path segment, so the key
        // is not a prefix of itself.
        Ok(self
            .directory
            .list(NODES_PREFIX)
            .await?
            .into_iter()
            .find(|(k, _)| *k == key)
            .map(|(_, body)| {
                let info = NodeInfo::decode(&body).filter(|n| n.id == self.id);
                (body, info)
            }))
    }

    /// Nodes with a fresh heartbeat (this one included), sorted by id.
    /// Unreadable heartbeat objects are skipped.
    pub async fn live_nodes(&self) -> Result<Vec<NodeInfo>, BucketError> {
        Ok(self.live(all_heartbeats(&self.directory).await?))
    }

    /// Every readable heartbeat, live or not (one listing), for callers that
    /// also need the stale ones; [`Membership::live`] filters them.
    pub async fn heartbeats(&self) -> Result<Vec<NodeInfo>, BucketError> {
        all_heartbeats(&self.directory).await
    }

    /// The live ones of `nodes` (see [`Membership::live_nodes`]).
    pub fn live(&self, mut nodes: Vec<NodeInfo>) -> Vec<NodeInfo> {
        let now = self.clock.now_ms();
        let horizon = self.ttl_ms.saturating_add(self.max_skew_ms);
        nodes.retain(|n| is_live(n, now, horizon));
        nodes
    }
}

/// Every readable heartbeat under [`NODES_PREFIX`], live or not, sorted by
/// id: what [`Membership::live_nodes`] filters, for tools that show stale
/// nodes too (`wasmkeep cluster status`). Unreadable objects are skipped.
pub async fn all_heartbeats<D: Directory>(directory: &D) -> Result<Vec<NodeInfo>, BucketError> {
    let mut nodes: Vec<NodeInfo> = directory
        .list(NODES_PREFIX)
        .await?
        .into_iter()
        .filter_map(|(key, body)| {
            // A heartbeat speaks for the node its key names, with a valid
            // id: a stray or forged object cannot pose as another node.
            let node = NodeInfo::decode(&body)?;
            // The listing is recursive: `nodes/x/a` must not pass for `a`.
            let named = key == format!("{NODES_PREFIX}{}", node.id);
            (named && validate_node_id(&node.id).is_ok()).then_some(node)
        })
        .collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(nodes)
}

/// Whether `node`'s heartbeat is still live at `now_ms`: written at most
/// `horizon_ms` (heartbeat ttl plus the tolerated clock skew) ago. A
/// heartbeat from a clock ahead of `now_ms` counts as just written.
/// A `released` or `fenced` node lease (format 3) is never live.
pub fn is_live(node: &NodeInfo, now_ms: u64, horizon_ms: u64) -> bool {
    !matches!(node.lease, Some(NodeState::Released | NodeState::Fenced))
        && now_ms.saturating_sub(node.written_at_ms) <= horizon_ms
}

/// Hex of a pin, as heartbeats publish it.
pub fn pin_hex(pin: &[u8; 32]) -> String {
    hex(pin)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use super::*;

    #[derive(Default)]
    struct Mem(RefCell<BTreeMap<String, Vec<u8>>>);

    impl Directory for &Mem {
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
            self.0.borrow_mut().insert(key.into(), body);
            Ok(())
        }
        async fn list(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, BucketError> {
            Ok(self
                .0
                .borrow()
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }
    }

    struct At<'a>(&'a Cell<u64>);
    impl Clock for At<'_> {
        fn now_ms(&self) -> u64 {
            self.0.get()
        }
    }

    fn run<F: Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => v,
            Poll::Pending => unreachable!("in-memory futures never suspend"),
        }
    }

    #[test]
    fn nodes_appear_and_expire() {
        let mem = Mem::default();
        let (ta, tb) = (Cell::new(1_000), Cell::new(1_000));
        let a = Membership::new(&mem, At(&ta), "node-a", "10.0.0.1:7443", 3_000, 500).unwrap();
        let b = Membership::new(&mem, At(&tb), "node-b", "10.0.0.2:7443", 3_000, 500).unwrap();
        run(a.heartbeat()).unwrap();
        run(b.heartbeat()).unwrap();
        let ids = |m: &Membership<&Mem, At<'_>>| {
            run(m.live_nodes())
                .unwrap()
                .into_iter()
                .map(|n| n.id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&a), ["node-a", "node-b"]);
        assert_eq!(run(a.live_nodes()).unwrap()[1].address, "10.0.0.2:7443");

        // b stops; a keeps beating. After ttl + skew, b is gone.
        ta.set(1_000 + 3_500);
        run(a.heartbeat()).unwrap();
        assert_eq!(ids(&a), ["node-a", "node-b"]);
        ta.set(1_000 + 3_501);
        assert_eq!(ids(&a), ["node-a"]);
    }

    #[test]
    fn junk_under_the_prefix_is_ignored() {
        let mem = Mem::default();
        let t = Cell::new(0);
        mem.0
            .borrow_mut()
            .insert("nodes/garbage".into(), b"not a heartbeat".to_vec());
        let a = Membership::new(&mem, At(&t), "a", "x:1", 3_000, 0).unwrap();
        run(a.heartbeat()).unwrap();
        assert_eq!(run(a.live_nodes()).unwrap().len(), 1);
    }

    #[test]
    fn a_heartbeat_under_another_key_cannot_pose_as_a_node() {
        // `nodes/x` claiming to be node `a` (to receive calls meant for a),
        // and one with an invalid id: both ignored.
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "real:1", 3_000, 0).unwrap();
        run(a.heartbeat()).unwrap();
        let forged = NodeInfo {
            id: "a".into(),
            address: "attacker:1".into(),
            version: "0".into(),
            written_at_ms: 0,
            app: String::new(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: None,
            reads_secrets: false,
            can_take: false,
            can_front: false,
            instance: None,
            lease: None,
            load: None,
        };
        mem.0.borrow_mut().insert("nodes/x".into(), forged.encode());
        mem.0
            .borrow_mut()
            .insert("nodes/x/a".into(), forged.encode());
        let bad_id = NodeInfo {
            id: "Not Valid".into(),
            ..forged.clone()
        };
        mem.0
            .borrow_mut()
            .insert("nodes/Not Valid".into(), bad_id.encode());
        let live = run(a.live_nodes()).unwrap();
        assert_eq!(live.len(), 1, "{live:?}");
        assert_eq!(live[0].address, "real:1");
    }

    #[test]
    fn heartbeats_carry_the_app_and_old_ones_still_read() {
        let info = NodeInfo {
            id: "a".into(),
            address: "x:1".into(),
            version: "1".into(),
            written_at_ms: 5,
            app: "acme/shop".into(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: None,
            reads_secrets: false,
            can_take: false,
            can_front: false,
            instance: None,
            lease: None,
            load: None,
        };
        assert_eq!(NodeInfo::decode(&info.encode()), Some(info));
        let old = b"wasmkeep-node 1\na\nx:1\n0.1\n5\n";
        assert_eq!(NodeInfo::decode(old).unwrap().app, "");
    }

    #[test]
    fn heartbeats_carry_the_process_token_next_to_the_capabilities() {
        let mut info = tls_node();
        info.drain = Some(DrainState::Serving);
        info.can_take = true;
        info.instance = Some("0f1e2d3c4b5a6978".into());
        let body = info.encode();
        assert!(String::from_utf8_lossy(&body).ends_with("\ntake instance=0f1e2d3c4b5a6978\n"));
        assert_eq!(NodeInfo::decode(&body), Some(info.clone()));
        // A token alone, without `take`.
        info.can_take = false;
        assert_eq!(NodeInfo::decode(&info.encode()), Some(info.clone()));
        // A line 11 from before the token, and malformed tokens: none.
        for line in [
            "take",
            "take instance=",
            "take instance=XYZ",
            &format!("take instance={}", "a".repeat(65)),
        ] {
            let body =
                format!("wasmkeep-node 2\na\nhttp://x\n1\n5\nacme/shop\n\n\nserving\n\n{line}\n");
            let read = NodeInfo::decode(body.as_bytes()).unwrap();
            assert_eq!((read.can_take, read.instance), (true, None), "{line}");
        }
    }

    /// Today's decoder before the TLS lines, frozen: what an older node runs.
    fn decode_before_tls(body: &[u8]) -> Option<(String, String, String, u64, String)> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-node 1" => 1,
            "wasmkeep-node 2" => 2,
            _ => return None,
        };
        Some((
            lines.next()?.to_owned(),
            lines.next()?.to_owned(),
            lines.next()?.to_owned(),
            lines.next()?.parse().ok()?,
            if format >= 2 {
                lines.next()?.to_owned()
            } else {
                String::new()
            },
        ))
    }

    fn tls_node() -> NodeInfo {
        NodeInfo {
            id: "a".into(),
            address: "http://10.0.0.1:8080".into(),
            version: "1".into(),
            written_at_ms: 5,
            app: "acme/shop".into(),
            cluster_address: Some("https://10.0.0.1:8443".into()),
            tls_pins: vec![[0xab; 32], [0x01; 32]],
            drain: None,
            reads_secrets: false,
            can_take: false,
            can_front: false,
            instance: None,
            lease: None,
            load: None,
        }
    }

    #[test]
    fn heartbeats_carry_the_cluster_listener_and_older_nodes_still_read_them() {
        let info = tls_node();
        let body = info.encode();
        assert_eq!(NodeInfo::decode(&body), Some(info.clone()));
        // An older node reads the same id, address, version and app.
        assert_eq!(
            decode_before_tls(&body),
            Some((
                "a".into(),
                "http://10.0.0.1:8080".into(),
                "1".into(),
                5,
                "acme/shop".into()
            ))
        );
        // Without TLS, the heartbeat is byte for byte what it was.
        let plain = NodeInfo {
            cluster_address: None,
            tls_pins: Vec::new(),
            ..info
        };
        assert_eq!(
            plain.encode(),
            b"wasmkeep-node 2\na\nhttp://10.0.0.1:8080\n1\n5\nacme/shop\n".to_vec()
        );
    }

    /// The decoder before line 9, frozen: what a node from before ADR 0014
    /// runs. It reads at most 8 lines.
    fn decode_before_drain(body: &[u8]) -> Option<(String, Option<String>, usize)> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        let format = match lines.next()? {
            "wasmkeep-node 1" => 1,
            "wasmkeep-node 2" => 2,
            _ => return None,
        };
        let _id = lines.next()?;
        let _address = lines.next()?;
        let _version = lines.next()?;
        let _written: u64 = lines.next()?.parse().ok()?;
        let app = if format >= 2 {
            lines.next()?.to_owned()
        } else {
            String::new()
        };
        let address = if format >= 2 {
            lines.next().and_then(cluster_address)
        } else {
            None
        };
        let pins = if format >= 2 {
            lines.next().map(pins).unwrap_or_default()
        } else {
            Vec::new()
        };
        Some((app, address, pins.len()))
    }

    #[test]
    fn heartbeats_carry_the_drain_state_and_older_nodes_still_read_them() {
        let plain = NodeInfo {
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: Some(DrainState::Draining),
            ..tls_node()
        };
        let body = plain.encode();
        // Lines 7 and 8 empty, so the state is on line 9.
        assert_eq!(
            body,
            b"wasmkeep-node 2\na\nhttp://10.0.0.1:8080\n1\n5\nacme/shop\n\n\ndraining\n".to_vec()
        );
        assert_eq!(NodeInfo::decode(&body), Some(plain));
        // A node from before: same app, still no cluster listener.
        assert_eq!(
            decode_before_drain(&body),
            Some(("acme/shop".into(), None, 0))
        );

        for state in [
            DrainState::Serving,
            DrainState::Draining,
            DrainState::Drained,
            DrainState::Stuck,
        ] {
            let tls = NodeInfo {
                drain: Some(state),
                ..tls_node()
            };
            let body = tls.encode();
            assert_eq!(NodeInfo::decode(&body), Some(tls));
            assert_eq!(
                decode_before_drain(&body),
                Some(("acme/shop".into(), Some("https://10.0.0.1:8443".into()), 2))
            );
        }
    }

    #[test]
    fn line_10_says_the_node_reads_secrets_and_older_readers_ignore_it() {
        let node = NodeInfo {
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: Some(DrainState::Serving),
            reads_secrets: true,
            ..tls_node()
        };
        let body = node.encode();
        assert!(body.ends_with(b"\n\n\nserving\nsecrets\n"), "{body:?}");
        assert_eq!(NodeInfo::decode(&body), Some(node.clone()));
        assert_eq!(
            decode_before_drain(&body),
            Some(("acme/shop".into(), None, 0))
        );
        // Without line 10 (a node from before ADR 0016).
        let older = NodeInfo {
            reads_secrets: false,
            ..node
        };
        assert!(!NodeInfo::decode(&older.encode()).unwrap().reads_secrets);
        // Real heartbeats say it.
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://x:1", 3_000, 0).unwrap();
        run(a.heartbeat()).unwrap();
        assert!(run(a.live_nodes()).unwrap()[0].reads_secrets);
    }

    #[test]
    fn line_11_says_the_node_takes_actors_and_older_readers_ignore_it() {
        let node = NodeInfo {
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: Some(DrainState::Serving),
            reads_secrets: false,
            can_take: true,
            can_front: false,
            instance: None,
            ..tls_node()
        };
        let body = node.encode();
        // Line 10 is written empty so line 11 stays line 11.
        assert!(body.ends_with(b"\n\n\nserving\n\ntake\n"), "{body:?}");
        let read = NodeInfo::decode(&body).unwrap();
        assert!(read.can_take && !read.reads_secrets);
        assert_eq!(
            decode_before_drain(&body),
            Some(("acme/shop".into(), None, 0))
        );
        // A list of words: `take` among others counts.
        let words = String::from_utf8(body)
            .unwrap()
            .replace("\ntake\n", "\nlater take\n");
        assert!(NodeInfo::decode(words.as_bytes()).unwrap().can_take);
        let none = NodeInfo {
            can_take: false,
            can_front: false,
            instance: None,
            ..node
        };
        assert!(!NodeInfo::decode(&none.encode()).unwrap().can_take);
    }

    #[test]
    fn a_heartbeat_without_a_readable_drain_line_predates_draining() {
        let base = "wasmkeep-node 2\na\nhttp://x:1\n1\n5\nt/\n";
        for body in [
            base.to_owned(),
            format!("{base}\n\n"),
            format!("{base}\n\nleaving\n"),
            format!("{base}\n\nDraining\n"),
            "wasmkeep-node 1\na\nx:1\n0.1\n5\n".to_owned(),
        ] {
            let info = NodeInfo::decode(body.as_bytes()).unwrap();
            assert_eq!(info.drain, None, "{body:?}");
        }
    }

    #[test]
    fn a_node_publishes_its_drain_state() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://x:1", 3_000, 0).unwrap();
        run(a.heartbeat()).unwrap();
        assert_eq!(
            run(a.live_nodes()).unwrap()[0].drain,
            Some(DrainState::Serving)
        );
        a.set_drain_state(DrainState::Stuck);
        run(a.heartbeat()).unwrap();
        assert_eq!(
            run(a.live_nodes()).unwrap()[0].drain,
            Some(DrainState::Stuck)
        );
    }

    #[test]
    fn malformed_tls_lines_are_dropped() {
        let base = "wasmkeep-node 2\na\nhttp://x:1\n1\n5\nt/\n";
        let pin = "ab".repeat(32);
        let cases = [
            // (line 7, line 8, address kept, pins kept)
            ("http://x:2", pin.as_str(), false, 1),
            ("https://x:2 y", pin.as_str(), false, 1),
            ("https://", pin.as_str(), false, 1),
            ("https://x:2", &pin[..63], true, 0),
            ("https://x:2", &"AB".repeat(32), true, 0),
            ("https://x:2", &format!("{pin} {pin} {pin}"), true, 0),
            ("https://x:2", &format!("{pin}  {pin}"), true, 0),
            ("https://x:2", &format!("{pin} {pin}"), true, 2),
        ];
        for (address, pins, address_kept, pins_kept) in cases {
            let body = format!("{base}{address}\n{pins}\n");
            let info = NodeInfo::decode(body.as_bytes()).unwrap();
            assert_eq!(info.cluster_address.is_some(), address_kept, "{body:?}");
            assert_eq!(info.tls_pins.len(), pins_kept, "{body:?}");
        }
    }

    #[test]
    fn pins_count_only_under_the_nodes_own_key() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://real:1", 3_000, 0)
            .unwrap()
            .with_cluster_tls("https://real:2", vec![[7; 32]]);
        run(a.heartbeat()).unwrap();
        // Another key claiming to be a, with the attacker's pin.
        let forged = NodeInfo {
            tls_pins: vec![[9; 32]],
            cluster_address: Some("https://attacker:2".into()),
            ..run(a.live_nodes()).unwrap()[0].clone()
        };
        mem.0.borrow_mut().insert("nodes/x".into(), forged.encode());
        let live = run(a.live_nodes()).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].tls_pins, vec![[7; 32]]);
        assert_eq!(live[0].cluster_address.as_deref(), Some("https://real:2"));
    }

    /// A node lease with `node`'s payload, as `NodeLease` writes it.
    fn format3(node: &NodeInfo, state: NodeState, instance: &str) -> Vec<u8> {
        NodeLeaseObject {
            state,
            instance: Some(instance.into()),
            written_at_ms: node.written_at_ms,
        }
        .encode_with(&node.payload())
    }

    #[test]
    fn a_node_lease_is_a_heartbeat_both_decoders_read() {
        let node = NodeInfo {
            drain: Some(DrainState::Draining),
            reads_secrets: true,
            can_take: true,
            can_front: true,
            // Ignored by the payload: the lease line names the process.
            instance: Some("dead".into()),
            ..tls_node()
        };
        let body = format3(&node, NodeState::Live, "beef");
        let read = NodeInfo::decode(&body).unwrap();
        assert_eq!(
            read,
            NodeInfo {
                instance: Some("beef".into()),
                lease: Some(NodeState::Live),
                ..node.clone()
            }
        );
        let o = NodeLeaseObject::decode(&body).unwrap();
        assert_eq!(o.instance.as_deref(), Some("beef"));
        assert_eq!(o.written_at_ms, node.written_at_ms);
        assert_eq!(crate::node_lease::tail(&body), &node.payload()[..]);
        // Formats 1 and 2 carry no lease state.
        assert_eq!(NodeInfo::decode(&tls_node().encode()).unwrap().lease, None);
    }

    #[test]
    fn a_released_or_fenced_node_is_not_live_whatever_its_age() {
        let node = tls_node();
        for (state, live) in [
            (NodeState::Live, true),
            (NodeState::Released, false),
            (NodeState::Fenced, false),
        ] {
            let read = NodeInfo::decode(&format3(&node, state, "beef")).unwrap();
            assert_eq!(
                is_live(&read, node.written_at_ms, 10_000),
                live,
                "{state:?}"
            );
        }
    }

    #[test]
    fn the_listing_reads_node_leases_and_skips_a_bare_fence() {
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let a = Membership::new(&mem, At(&t), "a", "http://a:1", 3_000, 0)
            .unwrap()
            .with_instance("beef");
        let info = NodeInfo {
            written_at_ms: 1_000,
            ..a.info()
        };
        mem.0
            .borrow_mut()
            .insert("nodes/a".into(), format3(&info, NodeState::Live, "beef"));
        // An operator's fence over an absent object has no payload: it
        // frees records (`node_lease::frees`) but names no address.
        mem.0.borrow_mut().insert(
            "nodes/b".into(),
            NodeLeaseObject {
                state: NodeState::Fenced,
                instance: None,
                written_at_ms: 900,
            }
            .encode(),
        );
        let all = run(a.heartbeats()).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "a");
        assert_eq!(all[0].instance.as_deref(), Some("beef"));
        assert_eq!(run(a.live_nodes()).unwrap().len(), 1);
        // What the lease task writes after the lease lines.
        assert_eq!(a.lease_payload(), info.payload());
    }

    /// Format 3's decoder before payload line 10, frozen: what a node of
    /// the version before it runs. It reads 9 payload lines, no more.
    fn decode_before_load(body: &[u8]) -> Option<NodeInfo> {
        let lease = NodeLeaseObject::decode(body)?;
        let text = std::str::from_utf8(crate::node_lease::tail(body)).ok()?;
        let mut lines = text.lines();
        let id = lines.next()?.to_owned();
        let address = lines.next()?.to_owned();
        let version = lines.next()?.to_owned();
        let app = lines.next()?.to_owned();
        let cluster_address = lines.next().and_then(cluster_address);
        let tls_pins = lines.next().map(pins).unwrap_or_default();
        let drain = lines.next().and_then(DrainState::parse);
        let reads_secrets = lines.next() == Some("secrets");
        let words = NodeInfo::words(lines.next());
        Some(NodeInfo {
            id,
            address,
            version,
            written_at_ms: lease.written_at_ms,
            app,
            cluster_address,
            tls_pins,
            drain,
            reads_secrets,
            can_take: words.can_take,
            can_front: false,
            instance: lease.instance,
            lease: Some(lease.state),
            load: None,
        })
    }

    fn loaded_node() -> NodeInfo {
        NodeInfo {
            drain: Some(DrainState::Serving),
            reads_secrets: true,
            can_take: true,
            can_front: false,
            load: Some(Load {
                actors: 12,
                sockets: 3,
                events_per_min: Some(840),
                front_per_min: Some(90),
            }),
            ..tls_node()
        }
    }

    #[test]
    fn payload_line_10_carries_the_load_and_older_readers_ignore_it() {
        let node = loaded_node();
        let payload = String::from_utf8(node.payload()).unwrap();
        let lines: Vec<&str> = payload.lines().collect();
        assert_eq!(lines.len(), 10, "{payload}");
        assert_eq!(lines[8], "take");
        assert_eq!(
            lines[9],
            "load actors=12 sockets=3 events_per_min=840 front_per_min=90"
        );
        let body = format3(&node, NodeState::Live, "beef");
        let read = NodeInfo::decode(&body).unwrap();
        assert_eq!(read.load, node.load);
        // A node of the version before reads every other field as before.
        assert_eq!(
            decode_before_load(&body),
            Some(NodeInfo {
                load: None,
                ..read.clone()
            })
        );
        // Unknown yet: `-`.
        let early = NodeInfo {
            load: Some(Load {
                events_per_min: None,
                front_per_min: None,
                ..node.load.unwrap()
            }),
            ..node.clone()
        };
        assert!(
            String::from_utf8(early.payload())
                .unwrap()
                .ends_with("\ntake\nload actors=12 sockets=3 events_per_min=- front_per_min=-\n")
        );
        let read = NodeInfo::decode(&format3(&early, NodeState::Live, "beef")).unwrap();
        assert_eq!(read.load, early.load);
        // No load yet: the 9 lines as before, byte for byte.
        let none = NodeInfo { load: None, ..node };
        assert_eq!(
            String::from_utf8(none.payload()).unwrap().lines().count(),
            9
        );
        assert!(none.payload().ends_with(b"\nsecrets\ntake\n"));
    }

    #[test]
    fn line_11_says_the_node_runs_fronts_and_older_readers_ignore_it() {
        // ADR 0023: `front` beside `take`, as a list of words.
        let node = NodeInfo {
            can_front: true,
            ..loaded_node()
        };
        let payload = String::from_utf8(node.payload()).unwrap();
        assert_eq!(payload.lines().nth(8), Some("take front"), "{payload}");
        let read = NodeInfo::decode(&format3(&node, NodeState::Live, "beef")).unwrap();
        assert!(read.can_take && read.can_front);
        assert_eq!(read.load, node.load);
        // Format 2 (line 11) carries it too.
        let read = NodeInfo::decode(&node.encode()).unwrap();
        assert!(read.can_take && read.can_front);
        // Without `take`, alone.
        let alone = NodeInfo {
            can_take: false,
            ..node.clone()
        };
        let read = NodeInfo::decode(&format3(&alone, NodeState::Live, "beef")).unwrap();
        assert!(!read.can_take && read.can_front);
        // A node of the version before reads `take` among the words, and
        // ignores `front`; one of this version ignores words it does not
        // know, and a node without the word lacks it.
        assert!(
            decode_before_load(&format3(&node, NodeState::Live, "beef"))
                .is_some_and(|n| n.can_take)
        );
        let body = "wasmkeep-node 3\nlive beef\n5\na\nhttp://a\n1\nacme/shop\n\n\nserving\n\nlater front take\n";
        let read = NodeInfo::decode(body.as_bytes()).unwrap();
        assert!(read.can_take && read.can_front);
        let body =
            "wasmkeep-node 3\nlive beef\n5\na\nhttp://a\n1\nacme/shop\n\n\nserving\n\ntake\n";
        assert!(!NodeInfo::decode(body.as_bytes()).unwrap().can_front);
        // Real heartbeats say it.
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://x:1", 3_000, 0).unwrap();
        let payload = String::from_utf8(a.lease_payload()).unwrap();
        assert_eq!(payload.lines().nth(8), Some("take front"), "{payload}");
    }

    #[test]
    fn a_payload_without_line_10_has_no_load() {
        // A node of the version before: 9 payload lines.
        let body =
            "wasmkeep-node 3\nlive beef\n5\na\nhttp://a\n1\nacme/shop\n\n\nserving\n\ntake\n";
        let read = NodeInfo::decode(body.as_bytes()).unwrap();
        assert!(read.can_take);
        assert_eq!(read.load, None);
        // Formats 1 and 2 never carry it.
        assert_eq!(
            NodeInfo::decode(&loaded_node().encode()).unwrap().load,
            None
        );
    }

    #[test]
    fn a_malformed_load_line_is_ignored() {
        let base =
            "wasmkeep-node 3\nlive beef\n5\na\nhttp://a\n1\nacme/shop\n\n\nserving\n\ntake\n";
        let read = |line: &str| {
            let info = NodeInfo::decode(format!("{base}{line}\n").as_bytes()).unwrap();
            // The other fields are unaffected.
            assert!(
                info.can_take && info.drain == Some(DrainState::Serving),
                "{line}"
            );
            info.load
        };
        for bad in [
            "",
            "load",
            "loads actors=1 sockets=2",
            "load actors=1",
            "load sockets=2",
            "load actors=-1 sockets=2",
            "load actors=+1 sockets=2",
            "load actors=1 sockets=x",
            "load actors=- sockets=2",
            "load actors=1 sockets=2 events_per_min=lots",
            "load actors=1 sockets=2 events_per_min=",
            "load actors=1 actors=2 sockets=2",
            "load actors=99999999999999999999 sockets=2",
            "actors=1 sockets=2",
        ] {
            assert_eq!(read(bad), None, "{bad:?}");
        }
        let ok = |actors, sockets, events_per_min| {
            Some(Load {
                actors,
                sockets,
                events_per_min,
                front_per_min: None,
            })
        };
        assert_eq!(read("load actors=1 sockets=2"), ok(1, 2, None));
        assert_eq!(
            read("load sockets=2 actors=1 events_per_min=7"),
            ok(1, 2, Some(7))
        );
        // Front requests, and a line from before them (no key): unknown.
        assert_eq!(
            read("load actors=1 sockets=2 events_per_min=7 front_per_min=30"),
            Some(Load {
                front_per_min: Some(30),
                ..ok(1, 2, Some(7)).unwrap()
            })
        );
        assert_eq!(
            read("load actors=1 sockets=2 events_per_min=7 front_per_min=-"),
            ok(1, 2, Some(7))
        );
        for bad in [
            "load actors=1 sockets=2 front_per_min=lots",
            "load actors=1 sockets=2 front_per_min=1 front_per_min=2",
        ] {
            assert_eq!(read(bad), None, "{bad:?}");
        }
        // A later version's keys and words are skipped.
        assert_eq!(
            read("load actors=1 sockets=2 events_per_min=- queued=4 busy"),
            ok(1, 2, None)
        );
    }

    #[test]
    fn a_node_publishes_its_load_once_set() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://x:1", 3_000, 0).unwrap();
        assert_eq!(a.load(), None);
        assert_eq!(
            String::from_utf8(a.lease_payload())
                .unwrap()
                .lines()
                .count(),
            9
        );
        let load = Load {
            actors: 2,
            sockets: 1,
            events_per_min: None,
            front_per_min: None,
        };
        a.set_load(load);
        let body = format3(
            &NodeInfo::decode(&format3(&a.info(), NodeState::Live, "beef")).unwrap(),
            NodeState::Live,
            "beef",
        );
        assert_eq!(NodeInfo::decode(&body).unwrap().load, Some(load));
        assert!(
            a.lease_payload()
                .ends_with(b"\nload actors=2 sockets=1 events_per_min=- front_per_min=-\n")
        );
    }

    /// With a source, each payload reads the load as it is built, with the
    /// drain state of the same moment: a takeover shows at the next lease
    /// write, and `drained` never comes with actors sampled before it.
    #[test]
    fn a_load_source_is_read_when_each_payload_is_built() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Membership::new(&mem, At(&t), "a", "http://x:1", 3_000, 0).unwrap();
        let actors = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&actors);
        a.set_load_source(Box::new(move || Load {
            actors: seen.load(Ordering::SeqCst),
            sockets: 0,
            events_per_min: None,
            front_per_min: None,
        }));
        let line = |a: &Membership<&Mem, At<'_>>| {
            String::from_utf8(a.lease_payload())
                .unwrap()
                .lines()
                .last()
                .unwrap()
                .to_owned()
        };
        assert_eq!(
            line(&a),
            "load actors=0 sockets=0 events_per_min=- front_per_min=-"
        );
        // An actor taken over: the very next payload has it.
        actors.store(1, Ordering::SeqCst);
        assert_eq!(
            line(&a),
            "load actors=1 sockets=0 events_per_min=- front_per_min=-"
        );
        // Handed over, then drained: both in the next payload.
        actors.store(0, Ordering::SeqCst);
        a.set_drain_state(DrainState::Drained);
        let info = NodeInfo::decode(&format3(&a.info(), NodeState::Live, "beef")).unwrap();
        assert_eq!(info.drain, Some(DrainState::Drained));
        assert_eq!(info.load.map(|l| l.actors), Some(0));
        assert_eq!(a.load().map(|l| l.actors), Some(0), "the last read kept");
    }

    #[test]
    fn node_ids_are_checked() {
        for ok in ["a", "node-1", &"x".repeat(64)] {
            assert!(validate_node_id(ok).is_ok(), "{ok}");
        }
        for bad in ["", "Node", "a/b", "a b", &"x".repeat(65)] {
            assert!(validate_node_id(bad).is_err(), "{bad}");
        }
    }
}
