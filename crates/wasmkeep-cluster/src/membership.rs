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
    /// A token random per process (heartbeat line 11, `instance=<hex>`):
    /// two processes writing this id's heartbeat show two tokens, which
    /// each notices in its next listing. `None` from nodes that predate it.
    /// In format 3 it is the node lease's instance (its second line).
    pub instance: Option<String>,
    /// The node lease's state (format 3); `None` in formats 1 and 2, whose
    /// liveness is their age alone. A `released` or `fenced` node is not
    /// live whatever its age.
    pub lease: Option<NodeState>,
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
            if self.reads_secrets || self.can_take || self.instance.is_some() {
                out.push_str(if self.reads_secrets {
                    "secrets\n"
                } else {
                    "\n"
                });
            }
            // Line 11: capability words, then the process's token.
            let mut words: Vec<String> = Vec::new();
            if self.can_take {
                words.push("take".to_owned());
            }
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
    /// line written (empty if need be). `written_at` and the instance are
    /// not in it: the lease lines carry them.
    pub fn payload(&self) -> Vec<u8> {
        let pins: Vec<String> = self.tls_pins.iter().map(hex).collect();
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
            self.id,
            self.address,
            self.version,
            self.app,
            self.cluster_address.as_deref().unwrap_or(""),
            pins.join(" "),
            self.drain.unwrap_or_default().as_str(),
            if self.reads_secrets { "secrets" } else { "" },
            if self.can_take { "take" } else { "" },
        )
        .into_bytes()
    }

    /// Format 3: the node lease's lines, then [`NodeInfo::payload`]. A body
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
            instance: lease.instance,
            lease: Some(lease.state),
        })
    }
}

impl NodeInfo {
    /// What line 11 says: capability words (`take`) and the process's
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
            can_take: words.contains(&"take"),
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
            version: env!("CARGO_PKG_VERSION").to_owned(),
            app: String::new(),
            cluster_address: None,
            tls_pins: Vec::new(),
            drain: std::sync::Mutex::new(DrainState::Serving),
            reads_secrets: true,
            instance: None,
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

    /// This node's heartbeat as of now.
    fn info(&self) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            address: self.address.clone(),
            version: self.version.clone(),
            written_at_ms: self.clock.now_ms(),
            app: self.app.clone(),
            cluster_address: self.cluster_address.clone(),
            tls_pins: self.tls_pins.clone(),
            drain: Some(self.drain_state()),
            reads_secrets: self.reads_secrets,
            can_take: true,
            instance: self.instance.clone(),
            lease: None,
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
            instance: None,
            lease: None,
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
            instance: None,
            lease: None,
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
            instance: None,
            lease: None,
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
