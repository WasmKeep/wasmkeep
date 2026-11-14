//! One lease per node, not one per actor ([ADR 0020]): the protocol, generic
//! over [`Bucket`] and [`Clock`] like [`crate::lease`], so the simulation in
//! `sim/` drives exactly this code. The server's ownership mechanism since
//! ADR 0020's slices 4 and 5.
//!
//! A node's **node lease** is one object, `nodes/<id>`, written
//! conditionally: `live` while the node holds it, `released` after a clean
//! stop, `fenced` when another node (or an operator) took it. A node acts as
//! owner of anything only while its last *acknowledged* write is fresh:
//! `written_at + ttl - safety` on its own clock, the same bound as the
//! retired per-actor grant.
//!
//! An actor's **ownership record** names a node and an instance, or nobody,
//! with a generation. It is written only when the actor changes hands, and
//! every acquisition writes generation + 1 (never reusing or lowering one),
//! so every hold has a generation of its own.
//!
//! A record naming node A and instance *i* is **free** on the evidence of a
//! read of A's node lease made after the read of the record: `live` naming
//! another instance (free at once), or `fenced` or `released` naming any
//! instance, free once that object's `written_at + ttl + grace` has passed
//! on the reader's clock. The fenced process's validity ended at
//! `written_at + ttl - safety` on its own clock, and `grace + safety >=
//! 2 * max_skew` puts the reader's moment after it, whoever wrote the fence.
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use crate::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};

/// The node lease's format header.
const NODE_HEADER: &str = "wasmkeep-node 3";
/// The ownership record's format header.
const OWNER_HEADER: &str = "wasmkeep-owner 1";

/// A node lease's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// The node holds it.
    Live,
    /// The node stopped cleanly.
    Released,
    /// Another node or an operator took it.
    Fenced,
}

impl NodeState {
    /// As written on the object's second line.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Released => "released",
            Self::Fenced => "fenced",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "live" => Some(Self::Live),
            "released" => Some(Self::Released),
            "fenced" => Some(Self::Fenced),
            _ => None,
        }
    }
}

/// A node lease object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLeaseObject {
    /// Its state.
    pub state: NodeState,
    /// The process it names; `None` only for a fence an operator created
    /// over an absent object.
    pub instance: Option<String>,
    /// The send time of the last `live` write, on that node's clock (a
    /// fence or a release keeps it).
    pub written_at_ms: u64,
}

impl NodeLeaseObject {
    /// The stored form.
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "{NODE_HEADER}\n{} {}\n{}\n",
            self.state.as_str(),
            self.instance.as_deref().unwrap_or("-"),
            self.written_at_ms
        )
        .into_bytes()
    }

    /// The stored form followed by `payload` (the heartbeat's other lines,
    /// opaque here: see [`tail`]).
    pub fn encode_with(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = self.encode();
        out.extend_from_slice(payload);
        out
    }

    /// Parses a stored node lease (its first three lines; the rest is the
    /// heartbeat's payload); `None` for anything else, including the older
    /// heartbeat formats (which free nothing).
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        if lines.next()? != NODE_HEADER {
            return None;
        }
        let (state, instance) = lines.next()?.split_once(' ')?;
        let written_at_ms = lines.next()?.parse().ok()?;
        Some(Self {
            state: NodeState::parse(state)?,
            instance: (instance != "-").then(|| instance.to_owned()),
            written_at_ms,
        })
    }
}

/// What follows a node lease's three lines in a stored body: the
/// heartbeat's own fields (address, pins, drain state, ...), which the
/// protocol carries unchanged through every write, fences included. Empty
/// if the body has no more than three lines.
pub fn tail(body: &[u8]) -> &[u8] {
    let mut rest = body;
    for _ in 0..3 {
        match rest.iter().position(|&b| b == b'\n') {
            Some(i) => rest = &rest[i + 1..],
            None => return &[],
        }
    }
    rest
}

/// Whether a record naming `instance` of a node is free, given that node's
/// lease object as read after the record (see the module documentation).
/// An absent object, or one in an older format, frees nothing.
pub fn frees(
    instance: &str,
    node: Option<&NodeLeaseObject>,
    now_ms: u64,
    config: &LeaseConfig,
) -> bool {
    match node {
        None => false,
        Some(o) => match o.state {
            NodeState::Live => o.instance.as_deref() != Some(instance),
            NodeState::Released | NodeState::Fenced => {
                now_ms
                    > o.written_at_ms
                        .saturating_add(config.ttl_ms())
                        .saturating_add(config.grace_ms())
            }
        },
    }
}

/// Why a node lease write did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeLeaseError {
    /// Another writer replaced the object (fenced, released, another
    /// instance, or deleted), as the re-read found it: drop every hold and
    /// exit.
    Lost(LostCause),
    /// No answer: the lease stays valid until it runs out; keep writing.
    Unavailable(String),
}

/// What a node found over its lease when a write's condition failed and
/// the object did not show this process live: why its lease was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostCause {
    /// Fenced, by a peer or an operator (`wasmkeep cluster fence`).
    Fenced,
    /// Written `released`, not by this process's own clean stop.
    Released,
    /// Live under another process with this node id: a twin.
    OtherProcess,
    /// The object is gone.
    Deleted,
    /// A heartbeat in a format from before node leases: an older version
    /// runs with this node id.
    OldVersion,
    /// Something this version cannot read.
    Unreadable,
    /// This process holds no lease to write: never started, or released.
    NotHeld,
}

impl LostCause {
    /// From an object read over a node's lease that does not show its
    /// process live (`None`: absent). A `released` object is taken as
    /// another process's: a process's own release ends its writes.
    pub fn of(body: Option<&[u8]>) -> Self {
        let Some(body) = body else {
            return Self::Deleted;
        };
        match NodeLeaseObject::decode(body) {
            Some(o) => match o.state {
                NodeState::Fenced => Self::Fenced,
                NodeState::Released => Self::Released,
                NodeState::Live => Self::OtherProcess,
            },
            None if body.starts_with(b"wasmkeep-node 1\n")
                || body.starts_with(b"wasmkeep-node 2\n") =>
            {
                Self::OldVersion
            }
            None => Self::Unreadable,
        }
    }
}

/// This node's lease.
#[derive(Debug)]
pub struct NodeLease<B, C> {
    bucket: B,
    clock: C,
    key: String,
    instance: String,
    config: LeaseConfig,
    /// The version and `written_at` of the last acknowledged (or adopted)
    /// `live` write.
    last: Option<(Version, u64)>,
    /// Written after the three lease lines (see [`tail`]).
    payload: Vec<u8>,
}

impl<B: Bucket, C: Clock> NodeLease<B, C> {
    /// The lease at `key` (`nodes/<id>`), for the process `instance`.
    pub fn new(
        bucket: B,
        clock: C,
        key: impl Into<String>,
        instance: impl Into<String>,
        config: LeaseConfig,
    ) -> Self {
        Self {
            bucket,
            clock,
            key: key.into(),
            instance: instance.into(),
            config,
            last: None,
            payload: Vec::new(),
        }
    }

    /// What the next writes carry after the three lease lines: the
    /// heartbeat's fields, as `membership` encodes them.
    pub fn set_payload(&mut self, payload: Vec<u8>) {
        self.payload = payload;
    }

    /// This process's instance token.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// The node's clock.
    pub fn clock(&self) -> &C {
        &self.clock
    }

    /// Until when (this node's clock) it may act as owner; `None` before
    /// its first acknowledged write.
    pub fn valid_until_ms(&self) -> Option<u64> {
        self.last.as_ref().map(|(_, at)| {
            at.saturating_add(self.config.ttl_ms())
                .saturating_sub(self.config.safety_ms())
        })
    }

    /// Whether it may act as owner now.
    pub fn is_valid(&self) -> bool {
        self.valid_until_ms()
            .is_some_and(|until| self.clock.now_ms() < until)
    }

    fn live(&self, sent: u64) -> Vec<u8> {
        NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(self.instance.clone()),
            written_at_ms: sent,
        }
        .encode_with(&self.payload)
    }

    /// The first write after the start check: create the object, or
    /// replace the one the check read last (`expected`). A lost reply is
    /// resolved by the re-read rule of [`NodeLease::renew`].
    pub async fn start(&mut self, expected: Option<&Version>) -> Result<(), NodeLeaseError> {
        let sent = self.clock.now_ms();
        let written = match expected {
            None => self.bucket.create(&self.key, self.live(sent)).await,
            Some(v) => self.bucket.replace(&self.key, self.live(sent), v).await,
        };
        self.settle(sent, written).await
    }

    /// The periodic write, `If-Match` on the last acknowledged version.
    pub async fn renew(&mut self) -> Result<(), NodeLeaseError> {
        let Some((version, _)) = self.last.clone() else {
            return Err(NodeLeaseError::Lost(LostCause::NotHeld));
        };
        let sent = self.clock.now_ms();
        let written = self
            .bucket
            .replace(&self.key, self.live(sent), &version)
            .await;
        self.settle(sent, written).await
    }

    /// Records a write's outcome. A conflict is not proof of a fence: a
    /// write whose reply was lost may have landed. Re-read once; the
    /// object `live` and naming this instance is this node's own write:
    /// adopt its version and stored `written_at`.
    async fn settle(
        &mut self,
        sent: u64,
        written: Result<Version, BucketError>,
    ) -> Result<(), NodeLeaseError> {
        match written {
            Ok(version) => {
                self.last = Some((version, sent));
                Ok(())
            }
            Err(BucketError::Unavailable(e)) => Err(NodeLeaseError::Unavailable(e)),
            Err(BucketError::Conflict) => match self.bucket.get(&self.key).await {
                Ok(Some((body, version))) => match NodeLeaseObject::decode(&body) {
                    Some(o)
                        if o.state == NodeState::Live
                            && o.instance.as_deref() == Some(self.instance.as_str()) =>
                    {
                        self.last = Some((version, o.written_at_ms));
                        Ok(())
                    }
                    _ => {
                        self.last = None;
                        Err(NodeLeaseError::Lost(LostCause::of(Some(&body))))
                    }
                },
                Ok(None) => {
                    self.last = None;
                    Err(NodeLeaseError::Lost(LostCause::of(None)))
                }
                Err(BucketError::Unavailable(e)) => Err(NodeLeaseError::Unavailable(e)),
                Err(BucketError::Conflict) => Err(NodeLeaseError::Unavailable("conflict".into())),
            },
        }
    }

    /// A clean stop's last write, after the node made itself locally
    /// invalid: `released`, keeping the last `written_at`. `Ok` once the
    /// object no longer holds this instance live (released here, or fenced).
    pub async fn release(&mut self) -> Result<(), NodeLeaseError> {
        for _ in 0..2 {
            let Some((version, at)) = self.last.clone() else {
                return Ok(());
            };
            let body = NodeLeaseObject {
                state: NodeState::Released,
                instance: Some(self.instance.clone()),
                written_at_ms: at,
            }
            .encode_with(&self.payload);
            match self.bucket.replace(&self.key, body, &version).await {
                Ok(_) => {
                    self.last = None;
                    return Ok(());
                }
                Err(BucketError::Unavailable(e)) => return Err(NodeLeaseError::Unavailable(e)),
                Err(BucketError::Conflict) => {
                    // Our own live write landed after all: adopt and retry.
                    // Fenced, released or replaced already frees the
                    // records; no answer to the re-read proves nothing.
                    let at = self.clock.now_ms();
                    match self.settle(at, Err(BucketError::Conflict)).await {
                        Ok(()) => {}
                        Err(NodeLeaseError::Lost(_)) => {
                            self.last = None;
                            return Ok(());
                        }
                        Err(e @ NodeLeaseError::Unavailable(_)) => return Err(e),
                    }
                }
            }
        }
        Err(NodeLeaseError::Unavailable(
            "released twice in conflict".into(),
        ))
    }
}

/// What a fence attempt found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fence {
    /// The fence landed.
    Fenced,
    /// The node wrote since it was watched, or is not stale yet: alive.
    Alive,
    /// The object is already not live (fenced or released), or absent.
    NotLive,
    /// No answer.
    Unavailable,
}

/// Fences the node at `key` if its object, read once here, still shows the
/// watched `instance` and `written_at` and is stale on this node's clock.
/// The caller holds a valid node lease and has watched that object
/// unchanged for a lease period plus one attempt timeout. The decision and
/// the condition come from the same read.
pub async fn fence<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    config: &LeaseConfig,
    key: &str,
    watched_instance: &str,
    watched_written_at_ms: u64,
) -> Fence {
    let (body, version) = match bucket.get(key).await {
        Ok(Some(found)) => found,
        Ok(None) => return Fence::NotLive,
        Err(_) => return Fence::Unavailable,
    };
    let Some(o) = NodeLeaseObject::decode(&body) else {
        return Fence::NotLive;
    };
    if o.state != NodeState::Live {
        return Fence::NotLive;
    }
    if o.instance.as_deref() != Some(watched_instance) || o.written_at_ms != watched_written_at_ms {
        return Fence::Alive;
    }
    let stale_at = o
        .written_at_ms
        .saturating_add(config.ttl_ms())
        .saturating_add(config.grace_ms());
    if clock.now_ms() <= stale_at {
        return Fence::Alive;
    }
    // Only the state changes: the node's payload stays as it wrote it.
    let fenced = NodeLeaseObject {
        state: NodeState::Fenced,
        ..o
    }
    .encode_with(tail(&body));
    match bucket.replace(key, fenced, &version).await {
        Ok(_) => Fence::Fenced,
        Err(BucketError::Conflict) => Fence::Alive,
        Err(BucketError::Unavailable(_)) => Fence::Unavailable,
    }
}

/// What `wasmkeep cluster fence` did (ADR 0020, section 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorFence {
    /// Fenced: the node's process stops at its next write, and records
    /// naming it are free once `written_at + ttl + grace` has passed on the
    /// reader's clock. `created` if there was no object (none written by
    /// any process: `written_at` is the operator's clock).
    Fenced {
        /// The kept (or, if created, the operator's) `written_at`.
        written_at_ms: u64,
        /// The process the fence names, if any.
        instance: Option<String>,
        /// Whether the object was absent and the fence created.
        created: bool,
    },
    /// Already not live: fenced or released before; nothing written.
    NotLive {
        /// Its state.
        state: NodeState,
        /// Its kept `written_at`.
        written_at_ms: u64,
    },
    /// A heartbeat in a format from before node leases: refused (its node
    /// would not read a fence; `cluster forget` removes a retired one).
    OldFormat,
}

/// An operator's fence of the node lease at `key`, without the watch a peer
/// needs (the time bound alone keeps it safe): over the current object,
/// `If-Match` on the version read, keeping its `written_at` and fields; if
/// the object is absent, created (`If-None-Match: *`) with `written_at` set
/// to this clock and no instance. A failed condition reads again, a few
/// times.
pub async fn operator_fence<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    key: &str,
) -> Result<OperatorFence, BucketError> {
    let mut last = BucketError::Conflict;
    for _ in 0..3 {
        let (written, outcome) = match bucket.get(key).await? {
            None => {
                let at = clock.now_ms();
                let fence = NodeLeaseObject {
                    state: NodeState::Fenced,
                    instance: None,
                    written_at_ms: at,
                };
                (
                    bucket.create(key, fence.encode()).await,
                    OperatorFence::Fenced {
                        written_at_ms: at,
                        instance: None,
                        created: true,
                    },
                )
            }
            Some((body, version)) => {
                let Some(o) = NodeLeaseObject::decode(&body) else {
                    return Ok(OperatorFence::OldFormat);
                };
                if o.state != NodeState::Live {
                    return Ok(OperatorFence::NotLive {
                        state: o.state,
                        written_at_ms: o.written_at_ms,
                    });
                }
                let outcome = OperatorFence::Fenced {
                    written_at_ms: o.written_at_ms,
                    instance: o.instance.clone(),
                    created: false,
                };
                let fenced = NodeLeaseObject {
                    state: NodeState::Fenced,
                    ..o
                }
                .encode_with(tail(&body));
                (bucket.replace(key, fenced, &version).await, outcome)
            }
        };
        match written {
            Ok(_) => return Ok(outcome),
            Err(e @ BucketError::Conflict) => last = e,
            Err(e) => return Err(e),
        }
    }
    Err(last)
}

/// Moves the `written_at` of a fence the operator created (no instance) up
/// to `at_ms` if it is earlier: a later `written_at` only frees the node's
/// records later, so this is always safe, and it guards against an
/// operator's clock running behind (`cluster fence` passes the bucket's own
/// time for the write). `Ok(true)` if it was moved.
pub async fn raise_created_fence<B: Bucket>(
    bucket: &B,
    key: &str,
    at_ms: u64,
) -> Result<bool, BucketError> {
    let Some((body, version)) = bucket.get(key).await? else {
        return Ok(false);
    };
    match NodeLeaseObject::decode(&body) {
        Some(o)
            if o.state == NodeState::Fenced && o.instance.is_none() && o.written_at_ms < at_ms =>
        {
            let raised = NodeLeaseObject {
                written_at_ms: at_ms,
                ..o
            }
            .encode_with(tail(&body));
            bucket.replace(key, raised, &version).await.map(|_| true)
        }
        _ => Ok(false),
    }
}

/// When a node may try to fence another (ADR 0020, section 2): only while
/// its own lease is valid, and only after it has seen the other's object
/// unchanged (the same instance and `written_at`, read from the heartbeat
/// listing's bodies) for a lease period plus one attempt timeout, counted
/// also from its own first valid moment after any lapse, so that after a
/// bucket outage every node renews before it judges the others. The fence
/// itself is decided on one fresh read ([`fence`]); this only says when to
/// make that read. Times are this node's, in ms; only differences matter.
#[derive(Debug, Clone)]
pub struct FenceWatch {
    window_ms: u64,
    /// A lease's ttl plus grace: `fence` refuses an object younger than
    /// that, so the watch never reports one ready before.
    stale_ms: u64,
    /// Since when this node's lease has been valid without a lapse.
    valid_since_ms: Option<u64>,
    /// The lease's validity as last told.
    valid_until_ms: Option<u64>,
    /// Per watched node: (instance, `written_at`, first seen at).
    watching: std::collections::HashMap<String, (String, u64, u64)>,
}

impl FenceWatch {
    /// A watch of `ttl / 3` (the lease period) plus `attempt_timeout_ms`.
    pub fn new(config: &LeaseConfig, attempt_timeout_ms: u64) -> Self {
        Self {
            window_ms: (config.ttl_ms() / 3).saturating_add(attempt_timeout_ms),
            stale_ms: config.ttl_ms().saturating_add(config.grace_ms()),
            valid_since_ms: None,
            valid_until_ms: None,
            watching: std::collections::HashMap::new(),
        }
    }

    /// This node's own lease at `now_ms`: its [`NodeLease::valid_until_ms`],
    /// on the same clock; call it on every tick. A lapse restarts the wait,
    /// including one that began and ended between two calls: a validity
    /// seen only once the previous one had run out counts as regained now
    /// (the acknowledgement may have come a little earlier; counting from
    /// now only waits longer).
    pub fn own_lease(&mut self, valid_until_ms: Option<u64>, now_ms: u64) {
        let valid = valid_until_ms.is_some_and(|until| now_ms < until);
        let lapsed = self.valid_until_ms.is_none_or(|before| now_ms >= before);
        if !valid {
            self.valid_since_ms = None;
        } else if lapsed || self.valid_since_ms.is_none() {
            self.valid_since_ms = Some(now_ms);
        }
        self.valid_until_ms = valid_until_ms;
    }

    /// `node`'s object was seen `live`, naming `instance`, written at
    /// `written_at_ms`. `true` once it may be fenced now (watched for the
    /// window, and older than ttl plus grace on this clock): the caller then
    /// calls [`fence`] with these values, and the watch of that node starts
    /// over whatever the outcome.
    pub fn observe(&mut self, node: &str, instance: &str, written_at_ms: u64, now_ms: u64) -> bool {
        let Some(valid_since) = self.valid_since_ms else {
            return false;
        };
        match self.watching.get(node) {
            Some((i, at, since)) if i == instance && *at == written_at_ms => {
                // Also past its written_at plus ttl and grace, as `fence`
                // checks: before that an attempt only starts the watch over.
                let ready = now_ms >= since.saturating_add(self.window_ms)
                    && now_ms >= valid_since.saturating_add(self.window_ms)
                    && now_ms > written_at_ms.saturating_add(self.stale_ms);
                if ready {
                    self.watching.remove(node);
                }
                ready
            }
            _ => {
                self.watching.insert(
                    node.to_owned(),
                    (instance.to_owned(), written_at_ms, now_ms),
                );
                false
            }
        }
    }

    /// Stops watching `node` (its object is no longer live, or gone).
    pub fn forget(&mut self, node: &str) {
        self.watching.remove(node);
    }

    /// Stops watching every node `listed` says no longer appears.
    pub fn retain(&mut self, listed: impl Fn(&str) -> bool) {
        self.watching.retain(|node, _| listed(node));
    }
}

/// The header of the marker that replaces an actor's per-actor lease
/// (`leases/<tenant>/<class>/<name>`) at its first acquisition under node
/// leases (ADR 0020, section 6). Binaries from before cannot read it and
/// never take it over.
pub const MARKER_HEADER: &str = "wasmkeep-lease 2";

/// A marker carrying `generation`, the seed of the actor's first record.
pub fn encode_marker(generation: u64) -> Vec<u8> {
    format!("{MARKER_HEADER}\n{generation}\n").into_bytes()
}

/// The generation a marker carries; `None` if `body` is not one.
pub fn decode_marker(body: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(body).ok()?;
    let mut lines = text.lines();
    if lines.next()? != MARKER_HEADER {
        return None;
    }
    lines.next()?.parse().ok()
}

/// Before an actor's first ownership record: reads its per-actor lease at
/// `key` and gives the generation to seed the record from (the record goes
/// at that + 1), at least `keys_generation` (the highest its keys carry).
/// A lease still live by the old rules (`expires_at + grace` not passed on
/// this clock), whatever node it names, this one included (an old record
/// has no instance to tell this process from another), refuses: `Held`.
/// Otherwise the lease is replaced by a marker carrying the seed (created
/// if absent, `If-Match` if present), so an old binary never takes the
/// actor; a marker already there (a crash between the two writes) is kept.
pub async fn mark_old_lease<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    config: &LeaseConfig,
    key: &str,
    keys_generation: u64,
) -> Result<u64, AcquireError> {
    let read = bucket
        .get(key)
        .await
        .map_err(|_| AcquireError::Unavailable)?;
    let (seed, written) = match read {
        None => (
            keys_generation,
            bucket.create(key, encode_marker(keys_generation)).await,
        ),
        Some((body, version)) => {
            if let Some(marked) = decode_marker(&body) {
                return Ok(marked.max(keys_generation));
            }
            let old = crate::lease::LeaseRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
            let live = old.expires_at_ms != 0
                && clock.now_ms() <= old.expires_at_ms.saturating_add(config.grace_ms());
            if live {
                return Err(AcquireError::Held {
                    node: old.owner,
                    instance: String::new(),
                });
            }
            let seed = old.generation.max(keys_generation);
            (
                seed,
                bucket.replace(key, encode_marker(seed), &version).await,
            )
        }
    };
    // A conflict: another node marked it, or an old one wrote, meanwhile;
    // the next acquisition reads it again.
    written.map(|_| seed).map_err(|_| AcquireError::Unavailable)
}

/// An ownership record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRecord {
    /// The node id and instance holding it, or nobody.
    pub holder: Option<(String, String)>,
    /// The generation of the last acquisition.
    pub generation: u64,
}

impl OwnerRecord {
    /// The stored form.
    pub fn encode(&self) -> Vec<u8> {
        let (id, instance) = self
            .holder
            .as_ref()
            .map_or(("-", "-"), |(n, i)| (n.as_str(), i.as_str()));
        format!("{OWNER_HEADER}\n{id} {instance}\n{}\n", self.generation).into_bytes()
    }

    /// Parses a stored record.
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        if lines.next()? != OWNER_HEADER {
            return None;
        }
        let (id, instance) = lines.next()?.split_once(' ')?;
        let generation = lines.next()?.parse().ok()?;
        Some(Self {
            holder: (id != "-").then(|| (id.to_owned(), instance.to_owned())),
            generation,
        })
    }
}

/// A hold this node acquired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// The record's key.
    pub key: String,
    /// The hold's own generation.
    pub generation: u64,
    /// The record's version as this hold wrote it; holds are not renewed,
    /// so it stays the record's version while the hold lasts.
    pub version: Version,
    /// The node the record named when this acquisition found it free (a
    /// failover: that node's process was fenced, released, or replaced),
    /// if another node.
    pub previous: Option<String>,
}

/// Why an acquisition did not give a hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireError {
    /// This node's lease is not valid: no acquisition.
    LeaseInvalid,
    /// Another live process holds it.
    Held {
        /// That node's id.
        node: String,
        /// Its instance.
        instance: String,
    },
    /// The write did not succeed and may have landed (a conflict too, as
    /// a client retries internally), and reading the record back did not
    /// settle it: an unresolved acquire at `generation`.
    Unresolved {
        /// The generation tried.
        generation: u64,
    },
    /// The record or a node lease is unreadable.
    Corrupt,
    /// No answer before writing: nothing written.
    Unavailable,
}

/// What a release or a resolution did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Released {
    /// Written: the record names nobody.
    Released,
    /// The record no longer names this instance at that generation:
    /// nothing to do, the entry ends.
    Superseded,
    /// Not known: keep it pending and retry.
    Pending,
}

/// Ownership records for one node.
#[derive(Debug)]
pub struct Owners<B, C> {
    bucket: B,
    clock: C,
    node: String,
    instance: String,
    nodes_prefix: String,
    config: LeaseConfig,
}

impl<B: Bucket, C: Clock> Owners<B, C> {
    /// Records acquired as `node`'s process `instance`; node leases live at
    /// `nodes_prefix` + node id.
    pub fn new(
        bucket: B,
        clock: C,
        node: impl Into<String>,
        instance: impl Into<String>,
        nodes_prefix: impl Into<String>,
        config: LeaseConfig,
    ) -> Self {
        Self {
            bucket,
            clock,
            node: node.into(),
            instance: instance.into(),
            nodes_prefix: nodes_prefix.into(),
            config,
        }
    }

    fn mine(&self, generation: u64) -> Vec<u8> {
        OwnerRecord {
            holder: Some((self.node.clone(), self.instance.clone())),
            generation,
        }
        .encode()
    }

    /// Acquires the record at `key` at generation + 1 if it names nobody,
    /// names this process, or is free; for an absent record, at what
    /// `first_generation` gives (called only then: seeding it may cost a
    /// listing, ADR 0020 section 6). `lease_valid` (this node's lease) is
    /// asked right before the write, after the reads.
    pub async fn acquire(
        &self,
        key: &str,
        lease_valid: impl Fn() -> bool,
        first_generation: impl AsyncFnOnce() -> Result<u64, AcquireError>,
    ) -> Result<Hold, AcquireError> {
        let read = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?;
        let mut previous = None;
        let (generation, before, written) = match read {
            None => {
                let first = first_generation().await?;
                if !lease_valid() {
                    return Err(AcquireError::LeaseInvalid);
                }
                (first, None, self.bucket.create(key, self.mine(first)).await)
            }
            Some((body, version)) => {
                let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
                if let Some((node, instance)) = &record.holder
                    && !(node == &self.node && instance == &self.instance)
                {
                    // Read after the record, never from a view.
                    let obj = self
                        .bucket
                        .get(&format!("{}{node}", self.nodes_prefix))
                        .await
                        .map_err(|_| AcquireError::Unavailable)?
                        .and_then(|(b, _)| NodeLeaseObject::decode(&b));
                    if !frees(instance, obj.as_ref(), self.clock.now_ms(), &self.config) {
                        return Err(AcquireError::Held {
                            node: node.clone(),
                            instance: instance.clone(),
                        });
                    }
                    if node != &self.node {
                        previous = Some(node.clone());
                    }
                }
                let generation = record
                    .generation
                    .checked_add(1)
                    .ok_or(AcquireError::Corrupt)?;
                if !lease_valid() {
                    return Err(AcquireError::LeaseInvalid);
                }
                let written = self
                    .bucket
                    .replace(key, self.mine(generation), &version)
                    .await;
                (generation, Some(version), written)
            }
        };
        let version = match written {
            Ok(version) => version,
            Err(_) => self.read_back(key, generation, before.as_ref()).await?,
        };
        Ok(Hold {
            key: key.to_owned(),
            generation,
            version,
            previous,
        })
    }

    /// After an acquisition's write failed, which may have landed (a reply
    /// lost; a client's retry meeting the landed write, S3's 409 then
    /// 412): the record, read back. Naming this process at `generation`,
    /// it is this write (only this process writes its instance, one
    /// acquisition of a key at a time, and the generation is this
    /// attempt's): its version. Replaced since the read before the write
    /// (`before`: its version, `None` for an absent record) by another
    /// live holder, the race is lost: `Held`. Anything else stays
    /// unresolved, the record unchanged above all: the write may still be
    /// in flight, and land.
    async fn read_back(
        &self,
        key: &str,
        generation: u64,
        before: Option<&Version>,
    ) -> Result<Version, AcquireError> {
        let unresolved = AcquireError::Unresolved { generation };
        let Ok(Some((body, version))) = self.bucket.get(key).await else {
            return Err(unresolved);
        };
        if before == Some(&version) {
            return Err(unresolved);
        }
        match OwnerRecord::decode(&body).and_then(|r| r.holder.map(|h| (h, r.generation))) {
            Some(((node, instance), g)) if node == self.node && instance == self.instance => {
                if g == generation {
                    Ok(version)
                } else {
                    Err(unresolved)
                }
            }
            Some(((node, instance), _)) => {
                let lease = self
                    .bucket
                    .get(&format!("{}{node}", self.nodes_prefix))
                    .await;
                let Ok(obj) = lease.map(|o| o.and_then(|(b, _)| NodeLeaseObject::decode(&b)))
                else {
                    return Err(unresolved);
                };
                if frees(&instance, obj.as_ref(), self.clock.now_ms(), &self.config) {
                    Err(unresolved)
                } else {
                    Err(AcquireError::Held { node, instance })
                }
            }
            None => Err(unresolved),
        }
    }

    /// Where to send a request for the actor at `key` (routing, ADR 0020
    /// section 3): one read of the record, never of a node lease. The node
    /// it names if that is another node and `live(node, instance)` (the
    /// caller's membership view) says that process is live; `None` to try
    /// here: no record, nobody, this node (any instance), or a holder the
    /// view does not show live, which an acquisition then checks with a
    /// fresh read. A hint only: it grants nothing.
    pub async fn other_owner(
        &self,
        key: &str,
        live: impl Fn(&str, &str) -> bool,
    ) -> Result<Option<String>, AcquireError> {
        let Some((body, _)) = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?
        else {
            return Ok(None);
        };
        let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
        Ok(record
            .holder
            .filter(|(node, instance)| node != &self.node && live(node, instance))
            .map(|(node, _)| node))
    }

    /// Releases a hold: the record names nobody, generation kept, `If-Match`
    /// on the hold's version only (never a version read later).
    pub async fn release(&self, hold: &Hold) -> Released {
        let body = OwnerRecord {
            holder: None,
            generation: hold.generation,
        }
        .encode();
        match self.bucket.replace(&hold.key, body, &hold.version).await {
            Ok(_) => Released::Released,
            Err(BucketError::Unavailable(_)) => Released::Pending,
            Err(BucketError::Conflict) => self.still_this_hold(&hold.key, hold.generation).await,
        }
    }

    /// Whether the record at `key` still names this instance at
    /// `generation` (one read): the safeguard re-read of a parked hold
    /// (ADR 0024, section 5). `Ok(false)` once it names anything else, or
    /// no record exists; `Err` if the bucket did not answer.
    pub async fn still_names_this(&self, key: &str, generation: u64) -> Result<bool, AcquireError> {
        let read = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?;
        Ok(read
            .and_then(|(body, _)| OwnerRecord::decode(&body))
            .is_some_and(|r| {
                r.generation == generation
                    && r.holder
                        .as_ref()
                        .is_some_and(|(n, i)| n == &self.node && i == &self.instance)
            }))
    }

    /// After a conflict: `Superseded` unless the record still names this
    /// instance at `generation` (then the release may still be needed).
    async fn still_this_hold(&self, key: &str, generation: u64) -> Released {
        match self.bucket.get(key).await {
            Ok(Some((body, _))) => match OwnerRecord::decode(&body) {
                Some(r)
                    if r.generation == generation
                        && r.holder
                            .as_ref()
                            .is_some_and(|(n, i)| n == &self.node && i == &self.instance) =>
                {
                    Released::Pending
                }
                _ => Released::Superseded,
            },
            Ok(None) => Released::Superseded,
            Err(_) => Released::Pending,
        }
    }

    /// Resolves an unresolved acquire at `generation`, when no hold for the
    /// actor exists here (the caller holds the actor's lock): if the record
    /// names this instance at that generation, it landed: release it.
    /// `Pending` means call it again: it reads the record afresh, which is
    /// equivalent to retrying with the version it read (only this instance
    /// writes this instance at that generation).
    pub async fn resolve(&self, key: &str, generation: u64) -> Released {
        match self.bucket.get(key).await {
            Ok(Some((body, version))) => match OwnerRecord::decode(&body) {
                Some(r)
                    if r.generation == generation
                        && r.holder
                            .as_ref()
                            .is_some_and(|(n, i)| n == &self.node && i == &self.instance) =>
                {
                    let hold = Hold {
                        key: key.to_owned(),
                        generation,
                        version,
                        previous: None,
                    };
                    self.release(&hold).await
                }
                _ => Released::Superseded,
            },
            Ok(None) => Released::Superseded,
            Err(_) => Released::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> LeaseConfig {
        LeaseConfig::default_safe()
    }

    #[test]
    fn node_lease_objects_round_trip() {
        for o in [
            NodeLeaseObject {
                state: NodeState::Live,
                instance: Some("ab12".into()),
                written_at_ms: 42,
            },
            NodeLeaseObject {
                state: NodeState::Fenced,
                instance: None,
                written_at_ms: 7,
            },
        ] {
            assert_eq!(NodeLeaseObject::decode(&o.encode()), Some(o));
        }
        assert_eq!(NodeLeaseObject::decode(b"wasmkeep-node 2\nx\n"), None);
    }

    /// A write whose condition failed over another writer's object says
    /// which writer it found.
    #[test]
    fn a_lost_lease_says_what_replaced_it() {
        use crate::lease::tests::{At, Mem, run};
        let fenced = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: Some("i1".into()),
            written_at_ms: 1,
        }
        .encode();
        let released = NodeLeaseObject {
            state: NodeState::Released,
            instance: Some("i1".into()),
            written_at_ms: 1,
        }
        .encode();
        let twin = NodeLeaseObject {
            state: NodeState::Live,
            instance: Some("i2".into()),
            written_at_ms: 1,
        }
        .encode();
        let cases: [(Option<&[u8]>, LostCause); 6] = [
            (Some(&fenced), LostCause::Fenced),
            (Some(&released), LostCause::Released),
            (Some(&twin), LostCause::OtherProcess),
            (None, LostCause::Deleted),
            (
                Some(b"wasmkeep-node 2\na\nhttp://a\n0.0.0\n1\nt/\n"),
                LostCause::OldVersion,
            ),
            (Some(b"garbage"), LostCause::Unreadable),
        ];
        for (body, cause) in cases {
            let mem = Mem::default();
            let t = std::cell::Cell::new(1_000);
            let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
            run(lease.start(None)).unwrap();
            mem.overwrite("nodes/a", body);
            assert_eq!(run(lease.renew()), Err(NodeLeaseError::Lost(cause)));
        }
        // A start whose condition fails says the same.
        let mem = Mem::default();
        let t = std::cell::Cell::new(1_000);
        mem.overwrite("nodes/a", Some(&twin));
        let (_, stale) = run((&mem).get("nodes/a")).unwrap().unwrap();
        mem.overwrite("nodes/a", Some(&fenced));
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        assert_eq!(
            run(lease.start(Some(&stale))),
            Err(NodeLeaseError::Lost(LostCause::Fenced))
        );
        // And a lease never written has nothing to renew.
        assert_eq!(
            run(lease.renew()),
            Err(NodeLeaseError::Lost(LostCause::NotHeld))
        );
    }

    #[test]
    fn owner_records_round_trip() {
        for r in [
            OwnerRecord {
                holder: Some(("a".into(), "i1".into())),
                generation: 3,
            },
            OwnerRecord {
                holder: None,
                generation: 9,
            },
        ] {
            assert_eq!(OwnerRecord::decode(&r.encode()), Some(r));
        }
    }

    #[test]
    fn the_lease_writes_carry_the_payload_and_a_fence_keeps_it() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let payload = b"a\nhttp://a:1\n0.1.0\n".to_vec();
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        lease.set_payload(payload.clone());
        run(lease.start(None)).unwrap();
        let body = run((&mem).get("nodes/a")).unwrap().unwrap().0;
        assert_eq!(&body[body.len() - payload.len()..], &payload[..]);
        assert_eq!(tail(&body), &payload[..]);
        let o = NodeLeaseObject::decode(&body).unwrap();
        assert_eq!((o.state, o.written_at_ms), (NodeState::Live, 1_000));

        // A fence changes the state line only.
        t.set(1_000 + 15_001);
        let f = run(fence(&&mem, &At(&t), &cfg(), "nodes/a", "i1", 1_000));
        assert_eq!(f, Fence::Fenced);
        let fenced = run((&mem).get("nodes/a")).unwrap().unwrap().0;
        assert_eq!(tail(&fenced), &payload[..]);
        assert_eq!(
            NodeLeaseObject::decode(&fenced).unwrap(),
            NodeLeaseObject {
                state: NodeState::Fenced,
                ..o
            }
        );

        // A release keeps it too.
        let mut b = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        b.set_payload(payload.clone());
        run(b.start(None)).unwrap();
        run(b.release()).unwrap();
        let released = run((&mem).get("nodes/b")).unwrap().unwrap().0;
        assert_eq!(tail(&released), &payload[..]);
        assert_eq!(
            NodeLeaseObject::decode(&released).unwrap().state,
            NodeState::Released
        );
    }

    #[test]
    fn without_a_payload_the_body_is_the_three_lease_lines() {
        let o = NodeLeaseObject {
            state: NodeState::Live,
            instance: Some("i1".into()),
            written_at_ms: 5,
        };
        assert_eq!(o.encode(), b"wasmkeep-node 3\nlive i1\n5\n");
        assert_eq!(tail(&o.encode()), b"");
    }

    #[test]
    fn a_node_is_fenced_only_after_a_full_watch_of_an_unchanged_object() {
        // Window: 10 s / 3 + 2 s = 5.333 s. The watched objects were all
        // written long before `B`, so they are stale by ttl plus grace
        // throughout; the watch alone decides here.
        const B: u64 = 20_000;
        let mut w = FenceWatch::new(&cfg(), 2_000);
        let window = 5_333;
        // Never before this node's own lease is valid.
        assert!(!w.observe("a", "i1", 100, B));
        w.own_lease(Some(B + 1_000_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        assert!(!w.observe("a", "i1", 100, B + (window - 1)));
        assert!(w.observe("a", "i1", 100, B + (window)));
        // It starts over after an attempt.
        assert!(!w.observe("a", "i1", 100, B + (window + 1)));

        // A new write restarts the watch, and so does another instance.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 1_000_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        assert!(!w.observe("a", "i1", 200, B + (window)));
        assert!(!w.observe("a", "i2", 200, B + (2 * window)));
        assert!(w.observe("a", "i2", 200, B + (3 * window)));

        // A lapse of this node's lease: the wait counts from its renewal,
        // however long the other object has been watched.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 1_000_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        w.own_lease(None, B + 10_000);
        assert!(!w.observe("a", "i1", 100, B + 10_000));
        w.own_lease(Some(B + 1_000_000), B + 20_000);
        assert!(!w.observe("a", "i1", 100, B + (20_000 + window - 1)));
        assert!(w.observe("a", "i1", 100, B + (20_000 + window)));

        // A lapse that starts and ends inside one renewal: the lease ran
        // out at 8 s, and the next acknowledgement is seen at 9 s. Seen
        // valid before and after, it is still a lapse: the wait restarts.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 8_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        w.own_lease(Some(B + 17_000), B + 9_000);
        assert!(!w.observe("a", "i1", 100, B + 9_000));
        assert!(!w.observe("a", "i1", 100, B + (9_000 + window - 1)));
        assert!(w.observe("a", "i1", 100, B + (9_000 + window)));
        // A renewal acknowledged in time is not a lapse.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 8_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        w.own_lease(Some(B + 11_000), B + 3_000);
        assert!(w.observe("a", "i1", 100, B + (window)));
        // Past its validity with no new acknowledgement: invalid.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 8_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        w.own_lease(Some(B + 8_000), B + 8_000);
        assert!(!w.observe("a", "i1", 100, B + 8_000));

        // Watches are per node.
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(B + 1_000_000), B);
        assert!(!w.observe("a", "i1", 100, B));
        assert!(!w.observe("b", "i9", 100, B));
        assert!(w.observe("a", "i1", 100, B + (window)));
        w.forget("b");
        assert!(!w.observe("b", "i9", 100, B + (window)));
    }

    /// The watch is never ready before the watched object's `written_at`
    /// plus ttl and grace has passed on this clock: `fence` would refuse
    /// then, and the watch would start over each time, delaying every
    /// fence by a window (review B1). Waiting keeps the watch.
    #[test]
    fn the_watch_is_never_ready_before_the_object_is_stale() {
        let window = 5_333;
        let stale = cfg().ttl_ms() + cfg().grace_ms(); // 15 s
        let mut w = FenceWatch::new(&cfg(), 2_000);
        w.own_lease(Some(1_000_000), 0);
        // Last written at 1 s, first seen at 1 s.
        assert!(!w.observe("a", "i1", 1_000, 1_000));
        // The window has passed, the object is not stale yet: not ready,
        // and still watched.
        assert!(!w.observe("a", "i1", 1_000, 1_000 + window));
        assert!(!w.observe("a", "i1", 1_000, 1_000 + stale));
        // Past its written_at plus ttl and grace: ready at once, with no
        // second window.
        assert!(w.observe("a", "i1", 1_000, 1_000 + stale + 1));
    }

    /// `wasmkeep cluster fence`: over a live object (fields kept), creating
    /// one when absent, nothing over a fence or an old format.
    #[test]
    fn an_operator_fences_a_node_or_its_missing_object() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(5_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        lease.set_payload(b"a\nhttp://a\n".to_vec());
        run(lease.start(None)).unwrap();
        t.set(9_000);
        assert_eq!(
            run(operator_fence(&&mem, &At(&t), "nodes/a")),
            Ok(OperatorFence::Fenced {
                written_at_ms: 5_000,
                instance: Some("i1".into()),
                created: false
            })
        );
        let (body, _) = run((&mem).get("nodes/a")).unwrap().unwrap();
        assert_eq!(tail(&body), b"a\nhttp://a\n");
        // The node's next write fails: lost.
        assert_eq!(
            run(lease.renew()),
            Err(NodeLeaseError::Lost(LostCause::Fenced))
        );
        // Again: already fenced, nothing written.
        assert_eq!(
            run(operator_fence(&&mem, &At(&t), "nodes/a")),
            Ok(OperatorFence::NotLive {
                state: NodeState::Fenced,
                written_at_ms: 5_000
            })
        );
        // Absent: created at the operator's clock, naming no instance.
        assert_eq!(
            run(operator_fence(&&mem, &At(&t), "nodes/b")),
            Ok(OperatorFence::Fenced {
                written_at_ms: 9_000,
                instance: None,
                created: true
            })
        );
        // A clock behind the bucket's: the created fence moves up, never down;
        // a node's own fence is left alone.
        assert_eq!(run(raise_created_fence(&&mem, "nodes/b", 12_000)), Ok(true));
        assert_eq!(
            run(raise_created_fence(&&mem, "nodes/b", 11_000)),
            Ok(false)
        );
        let (body, _) = run((&mem).get("nodes/b")).unwrap().unwrap();
        assert_eq!(
            NodeLeaseObject::decode(&body).unwrap().written_at_ms,
            12_000
        );
        assert_eq!(
            run(raise_created_fence(&&mem, "nodes/a", 99_000)),
            Ok(false)
        );
        // An old format: refused.
        run((&mem).create("nodes/c", b"wasmkeep-node 2\nc\nhttp://c\n".to_vec())).unwrap();
        assert_eq!(
            run(operator_fence(&&mem, &At(&t), "nodes/c")),
            Ok(OperatorFence::OldFormat)
        );
    }

    /// ADR 0020, section 6: the first acquisition under node leases marks
    /// the per-actor lease so an old binary refuses the actor, seeds from
    /// it, and refuses an old lease still live, whatever node it names.
    #[test]
    fn an_old_lease_is_marked_or_refuses() {
        use crate::lease::Leases;
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(10_000);
        let clock = At(&t);
        let read = |key: &str| run((&mem).get(key)).unwrap().unwrap().0;

        // Absent: a marker at the keys' generation.
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/a", 4)),
            Ok(4)
        );
        assert_eq!(decode_marker(&read("leases/a")), Some(4));
        // Marked already: kept, the seed never below the keys.
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/a", 2)),
            Ok(4)
        );
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/a", 6)),
            Ok(6)
        );
        assert_eq!(decode_marker(&read("leases/a")), Some(4));

        // A live old lease, even this node's own id: refused, untouched.
        let old = Leases::new(&mem, At(&t), "n1", cfg());
        run(old.acquire("leases/b")).unwrap();
        let before = read("leases/b");
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/b", 0)),
            Err(AcquireError::Held {
                node: "n1".into(),
                instance: String::new()
            })
        );
        assert_eq!(read("leases/b"), before);
        // Past its expiry plus grace: replaced by a marker above its
        // generation (1) or the keys' (3).
        t.set(10_000 + cfg().ttl_ms() + cfg().grace_ms() + 1);
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/b", 3)),
            Ok(3)
        );
        assert_eq!(decode_marker(&read("leases/b")), Some(3));
        // An old binary reads the marker as corrupt and never takes it.
        assert!(run(old.acquire("leases/b")).is_err());
        assert!(run(old.acquire("leases/a")).is_err());

        // Something else under the key: corrupt, nothing written.
        run((&mem).create("leases/c", b"junk".to_vec())).unwrap();
        assert_eq!(
            run(mark_old_lease(&&mem, &clock, &cfg(), "leases/c", 0)),
            Err(AcquireError::Corrupt)
        );
    }

    /// The first generation is computed only for an absent record (seeding
    /// it may cost a listing); an acquisition from a fenced node names
    /// that node as the previous holder, one from nobody names none.
    #[test]
    fn the_first_generation_is_asked_only_for_a_new_record() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut la = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(la.start(None)).unwrap();
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        run(lb.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        let asked = Cell::new(0);
        let seed = async || {
            asked.set(asked.get() + 1);
            Ok(41)
        };
        let hold = run(a.acquire("owners/x", || true, seed)).unwrap();
        assert_eq!(
            (hold.generation, asked.get(), hold.previous.clone()),
            (41, 1, None)
        );
        assert_eq!(run(a.release(&hold)), Released::Released);
        let again = run(a.acquire("owners/x", || true, async || panic!("asked again"))).unwrap();
        assert_eq!((again.generation, again.previous.clone()), (42, None));
        // A seed that fails writes nothing.
        assert_eq!(
            run(a.acquire("owners/y", || true, async || Err(AcquireError::Unavailable))),
            Err(AcquireError::Unavailable)
        );
        assert_eq!(run(b.other_owner("owners/y", |_, _| true)), Ok(None));
        // a is fenced (its written_at, 1 000, plus ttl and grace passed):
        // b takes x over, and the hold says from whom.
        t.set(1_000 + cfg().ttl_ms() + cfg().grace_ms() + 1);
        let key = "nodes/a";
        assert_eq!(
            run(fence(&&mem, &At(&t), &cfg(), key, "i1", 1_000)),
            Fence::Fenced
        );
        run(lb.renew()).unwrap();
        let taken = run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(
            (taken.generation, taken.previous),
            (43, Some("a".to_owned()))
        );
    }

    /// A write that landed with a failure reported (its reply lost, or a
    /// client retry meeting the landed write: S3's 409 then 412) is read
    /// back: the record names this process at the generation tried, so
    /// the acquisition holds it, with a version that works (it releases).
    #[test]
    fn an_acquire_whose_write_landed_unanswered_holds_it() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(lease.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        // A create (no record yet) reported as a conflict.
        mem.conflict_after_landing.set(true);
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert!(!mem.conflict_after_landing.get(), "the fault fired");
        assert_eq!(hold.generation, 1);
        assert_eq!(run(a.release(&hold)), Released::Released);
        // A replace reported as a conflict, then one whose reply is lost.
        for fault in [&mem.conflict_after_landing, &mem.lose_next_reply] {
            fault.set(true);
            let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
            assert!(!fault.get(), "the fault fired");
            assert_eq!(run(a.release(&hold)), Released::Released);
        }
    }

    /// A write that met another live holder's record lost a race: the
    /// acquisition names that holder (to forward to), not an unresolved
    /// write. The record naming this node at another generation, or
    /// nobody, is not this write: still unresolved.
    #[test]
    fn an_acquire_that_lost_the_race_names_the_winner() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut la = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(la.start(None)).unwrap();
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        run(lb.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let record = |holder: Option<(&str, &str)>, generation| {
            OwnerRecord {
                holder: holder.map(|(n, i)| (n.to_owned(), i.to_owned())),
                generation,
            }
            .encode()
        };
        // On a create, and on a replace.
        for seeded in [false, true] {
            let key = if seeded { "owners/r" } else { "owners/c" };
            if seeded {
                mem.overwrite(key, Some(&record(None, 4)));
            }
            *mem.steal_next.borrow_mut() = Some((key.into(), record(Some(("b", "i2")), 5)));
            assert_eq!(
                run(a.acquire(key, || true, async || Ok(5))),
                Err(AcquireError::Held {
                    node: "b".into(),
                    instance: "i2".into()
                })
            );
        }
        // This node at another generation, or nobody: not this write.
        for other in [record(Some(("a", "i1")), 9), record(None, 9)] {
            mem.overwrite("owners/n", Some(&record(None, 4)));
            *mem.steal_next.borrow_mut() = Some(("owners/n".into(), other));
            assert_eq!(
                run(a.acquire("owners/n", || true, async || Ok(1))),
                Err(AcquireError::Unresolved { generation: 5 })
            );
        }
    }

    /// The read-back's other ways out, each still an unresolved acquire:
    /// the record unchanged since the read before the write (the write
    /// may still be in flight and land), even when its holder's lease
    /// stopped freeing it meanwhile; a rival that its own lease frees; a
    /// failed read of the record, or of the rival's lease.
    #[test]
    fn a_read_back_that_settles_nothing_stays_unresolved() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut la = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(la.start(None)).unwrap();
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        run(lb.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let record = |holder: (&str, &str), generation| {
            OwnerRecord {
                holder: Some((holder.0.to_owned(), holder.1.to_owned())),
                generation,
            }
            .encode()
        };
        let unresolved = Err(AcquireError::Unresolved { generation: 5 });
        // b's older instance i1 holds it (freed by b's live lease as i2);
        // the write does not land, and b's lease is rewritten released
        // meanwhile: the same record no longer reads as free, yet it is
        // the one the write was conditioned on, not a rival's.
        let released = NodeLeaseObject {
            state: NodeState::Released,
            instance: Some("i2".into()),
            written_at_ms: 1_000,
        }
        .encode();
        mem.overwrite("owners/u", Some(&record(("b", "i1"), 4)));
        mem.refuse_next_write.set(true);
        *mem.steal_next.borrow_mut() = Some(("nodes/b".into(), released));
        assert_eq!(
            run(a.acquire("owners/u", || true, async || Ok(1))),
            unresolved
        );
        // A rival its own lease frees (b restarted as i3; the rival is i2).
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i3", cfg());
        mem.overwrite("nodes/b", None);
        run(lb.start(None)).unwrap();
        mem.overwrite(
            "owners/f",
            Some(
                &OwnerRecord {
                    holder: None,
                    generation: 4,
                }
                .encode(),
            ),
        );
        *mem.steal_next.borrow_mut() = Some(("owners/f".into(), record(("b", "i2"), 5)));
        assert_eq!(
            run(a.acquire("owners/f", || true, async || Ok(1))),
            unresolved
        );
        // A failed read of the record (the second read), then of the
        // rival's lease (the third).
        for after in [1, 2] {
            let key = format!("owners/g{after}");
            mem.overwrite(
                &key,
                Some(
                    &OwnerRecord {
                        holder: None,
                        generation: 4,
                    }
                    .encode(),
                ),
            );
            *mem.steal_next.borrow_mut() = Some((key.clone(), record(("b", "i3"), 5)));
            mem.fail_get_after.set(Some(after));
            assert_eq!(
                run(a.acquire(&key, || true, async || Ok(1))),
                unresolved,
                "{after}"
            );
            assert_eq!(mem.fail_get_after.get(), None, "the read failed");
        }
    }

    #[test]
    fn routing_reads_the_record_and_trusts_only_a_live_holder() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(lease.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        let anyone = |_: &str, _: &str| true;
        assert_eq!(run(b.other_owner("owners/x", anyone)), Ok(None));
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        // Another node, live in the view: forward there.
        assert_eq!(
            run(b.other_owner("owners/x", |n, i| n == "a" && i == "i1")),
            Ok(Some("a".into()))
        );
        // Not live in the view (gone, fenced, or restarted as i9): here.
        assert_eq!(run(b.other_owner("owners/x", |_, i| i == "i9")), Ok(None));
        // This node itself: here.
        assert_eq!(run(a.other_owner("owners/x", anyone)), Ok(None));
        // Released: nobody, here.
        assert_eq!(run(a.release(&hold)), Released::Released);
        assert_eq!(run(b.other_owner("owners/x", anyone)), Ok(None));
    }

    #[test]
    fn what_frees_a_record() {
        let c = cfg();
        let live = |i: &str| NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(i.into()),
            written_at_ms: 1_000,
        };
        let fenced = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: Some("i1".into()),
            written_at_ms: 1_000,
        };
        // Absent or old-format: never.
        assert!(!frees("i1", None, u64::MAX, &c));
        // Live, this instance: held; another instance: free at once.
        assert!(!frees("i1", Some(&live("i1")), u64::MAX, &c));
        assert!(frees("i1", Some(&live("i2")), 0, &c));
        // Fenced or released: only after written_at + ttl + grace.
        assert!(!frees("i1", Some(&fenced), 1_000 + 15_000, &c));
        assert!(frees("i1", Some(&fenced), 1_000 + 15_001, &c));
    }
}
