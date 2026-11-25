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
//! That read is never judged when it is older than a body of the same
//! lease the process has already read ([`NewestReads`]): it is read again,
//! and counts as unanswered if it stays older (ADR 0020, amendment on
//! lease reads).
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use crate::hold_cell::HoldHead;
use crate::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use crate::local_heads::Requirement;
use crate::recorded_head::{OWNER_HEADER_V2, RecordedHead};

/// The node lease's format header.
const NODE_HEADER: &str = "wasmkeep-node 3";
/// The ownership record's version 1 format header (version 2:
/// [`crate::recorded_head::OWNER_HEADER_V2`]).
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

/// Whether `body` is provably older than `seen`, two bodies of one node's
/// lease ([ADR 0020], amendment on lease reads).
///
/// - The same instance: a `fenced` or `released` body is newer than any
///   `live` one, whatever their dates (that instance never writes `live`
///   after its fence or release, and its own clock may have stepped back
///   before them); otherwise the earlier date is older.
/// - A fence an operator created over a deleted object (`fenced`, no
///   instance) and a body of a named instance: the body is older if dated
///   less than `window_ms` after the fence, newer otherwise; `window_ms`
///   is twice the skew allowed ([`fence_window_ms`]). The fence is dated
///   by the operator's clock, so a predecessor's bodies may be dated after
///   it, but by less than twice the skew; a successor's start check dates
///   its first body at least that much after the fence on its own clock.
/// - Other different instances: the earlier date is older (a successor's
///   start check dates its bodies after its predecessor's); dated the
///   same, they are not ordered.
///
/// [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
pub fn older_than(body: &NodeLeaseObject, seen: &NodeLeaseObject, window_ms: u64) -> bool {
    match (created_fence(body), created_fence(seen)) {
        (false, true) if body.instance.is_some() => {
            return body.written_at_ms < seen.written_at_ms.saturating_add(window_ms);
        }
        (true, false) if seen.instance.is_some() => {
            return body.written_at_ms.saturating_add(window_ms) <= seen.written_at_ms;
        }
        _ => {}
    }
    if body.instance == seen.instance {
        let ended = |o: &NodeLeaseObject| o.state != NodeState::Live;
        if ended(body) != ended(seen) {
            return ended(seen);
        }
    }
    body.written_at_ms < seen.written_at_ms
}

/// How long after a fence an operator created over a deleted object its
/// node's bodies still count as older than it ([`older_than`]): twice the
/// skew allowed. Every body written before the fence is dated less than
/// that after it, and a start check over the fence writes no body dated
/// earlier than that ([ADR 0020], amendment on lease reads).
///
/// [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
pub fn fence_window_ms(config: &LeaseConfig) -> u64 {
    config.max_skew_ms().saturating_mul(2)
}

/// Whether `o` is a fence an operator created over an absent object
/// ([`operator_fence`]): `fenced`, naming no instance.
pub fn created_fence(o: &NodeLeaseObject) -> bool {
    o.state == NodeState::Fenced && o.instance.is_none()
}

/// How many times a node lease read after a record is made before its
/// caller gives up, when each read returns a body older than one this
/// process has already read ([`NewestReads`]).
pub const BEHIND_READS: usize = 3;

/// What [`NewestReads`] reports, for the server's counters and logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadEvent {
    /// A read made to judge a record returned a body older than the newest
    /// held: it is read again.
    Behind {
        /// The node.
        node: String,
        /// The newest body's date.
        held_ms: u64,
        /// The date of the body read.
        read_ms: u64,
    },
    /// Every read of one judgement was behind: nothing judged.
    Exhausted {
        /// The node.
        node: String,
    },
    /// The newest held was not confirmed by any read for the unpin window
    /// while reads kept returning an older body: that body replaced it.
    Unpinned {
        /// The node.
        node: String,
        /// The date of the body replaced.
        held_ms: u64,
        /// The date of the body that replaced it.
        read_ms: u64,
    },
}

/// Deliberate faults in [`NewestReads`], for a simulation's broken
/// variants only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadFault {
    /// None: the rule.
    #[default]
    None,
    /// Remembers nothing: every body is judged as read (the rule before
    /// the amendment).
    Off,
    /// The read back after a failed acquisition write judges its body as
    /// read, bypassing the table.
    ReadBackUnchecked,
    /// When every read is behind, the last one is judged anyway.
    ExhaustedJudged,
    /// A created fence is ordered against named bodies by date alone (the
    /// rule before it had a window of its own).
    FenceByDate,
    /// A read that finds the object absent leaves the unpin window running
    /// (the rule before), so stale bodies read around it unpin the newest.
    AbsentKeepsWindow,
}

/// What [`NewestReads::with_report`] calls.
type ReportFn = Box<dyn Fn(&ReadEvent) + Send + Sync>;

#[derive(Debug, Clone)]
struct Newest {
    body: NodeLeaseObject,
    /// Since when (the reader's clock, ms) every read made to judge a
    /// record returned an older body, with no read returning this one (or
    /// one not older) since: `None` while it is confirmed.
    behind_since_ms: Option<u64>,
}

/// The newest body of each node's lease that a process has read, from any
/// of its reads (heartbeat rounds and listings, the reads that judge a
/// record free), shared by every [`Owners`] of the process ([ADR 0020],
/// amendment on lease reads). It moves forward by [`older_than`]. A read
/// made to judge a record free that returns a body older than the one held
/// here is a stale read (the bucket or a cache in front of it serving an
/// earlier version): it is read again, never trusted.
///
/// The newest held is never kept for ever on its date alone: once older
/// bodies have been read for the unpin window (ttl plus grace, given by the
/// caller) with no read returning it (or a body not older) meanwhile, the
/// next older body read replaces it. The window counts from the first such
/// read, so an outage with no reads does not shorten it. A clock that ran
/// ahead or stepped back cannot hold a node's records unfreeable much
/// longer than that; at worst, after the window, a body is judged as read,
/// the rule before.
///
/// This closes a read that goes back behind what the process has seen
/// recently, not a first read that is stale: that needs the ownership
/// record to carry its holder's lease date, which is a format change.
///
/// [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
#[derive(Default)]
pub struct NewestReads {
    newest: std::sync::Mutex<std::collections::HashMap<String, Newest>>,
    /// Reads found older than the newest held, since the start.
    behind: std::sync::atomic::AtomicU64,
    fault: ReadFault,
    report: Option<ReportFn>,
}

impl std::fmt::Debug for NewestReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewestReads")
            .field("newest", &self.newest)
            .field("behind", &self.behind)
            .field("fault", &self.fault)
            .finish_non_exhaustive()
    }
}

impl NewestReads {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// A table with a deliberate fault: for a simulation's broken variant
    /// only.
    pub fn with_fault(fault: ReadFault) -> Self {
        Self {
            fault,
            ..Self::default()
        }
    }

    /// A table that remembers nothing, so every read is trusted (the rule
    /// before the amendment): for a simulation's broken variant only.
    pub fn off() -> Self {
        Self::with_fault(ReadFault::Off)
    }

    /// Calls `report` on every [`ReadEvent`] (counters, logs).
    #[must_use]
    pub fn with_report(mut self, report: impl Fn(&ReadEvent) + Send + Sync + 'static) -> Self {
        self.report = Some(Box::new(report));
        self
    }

    /// Its fault.
    pub fn fault(&self) -> ReadFault {
        self.fault
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, Newest>> {
        self.newest.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn report(&self, event: ReadEvent) {
        if let Some(report) = &self.report {
            report(&event);
        }
    }

    /// The window that orders a created fence, as this table applies it.
    fn window_ms(&self, config: &LeaseConfig) -> u64 {
        if self.fault == ReadFault::FenceByDate {
            0
        } else {
            fence_window_ms(config)
        }
    }

    /// Notes `body`, just read for node `node`: it becomes the newest held
    /// if the one held is older than it, and confirms the one held unless it
    /// is older ([`older_than`], under the node's lease `config`).
    pub fn observe(&self, node: &str, body: &NodeLeaseObject, config: &LeaseConfig) {
        if self.fault == ReadFault::Off {
            return;
        }
        let window_ms = self.window_ms(config);
        let mut newest = self.lock();
        match newest.get_mut(node) {
            Some(held) => {
                if older_than(&held.body, body, window_ms) {
                    held.body = body.clone();
                }
                if !older_than(body, &held.body, window_ms) {
                    held.behind_since_ms = None;
                }
            }
            None => {
                newest.insert(
                    node.to_owned(),
                    Newest {
                        body: body.clone(),
                        behind_since_ms: None,
                    },
                );
            }
        }
    }

    /// Whether `body`, just read for node `node` at `now_ms`, is older
    /// than the newest held, and so must not be judged (counted and
    /// reported when it is). If not, it is noted ([`Self::observe`]). An
    /// older body replaces the newest held (and is not behind) once older
    /// bodies have been read for the unpin window (`config`'s ttl plus
    /// grace) with no read confirming it.
    pub fn behind(
        &self,
        node: &str,
        body: &NodeLeaseObject,
        now_ms: u64,
        config: &LeaseConfig,
    ) -> bool {
        if self.fault == ReadFault::Off {
            return false;
        }
        let unpin_ms = config.ttl_ms().saturating_add(config.grace_ms());
        let window_ms = self.window_ms(config);
        let mut newest = self.lock();
        let Some(held) = newest.get_mut(node) else {
            newest.insert(
                node.to_owned(),
                Newest {
                    body: body.clone(),
                    behind_since_ms: None,
                },
            );
            return false;
        };
        if !older_than(body, &held.body, window_ms) {
            if older_than(&held.body, body, window_ms) {
                held.body = body.clone();
            }
            held.behind_since_ms = None;
            return false;
        }
        // The first behind read starts the window; a reader's clock that
        // stepped back before it starts it again now.
        let since = match held.behind_since_ms {
            Some(since) if since <= now_ms => since,
            _ => now_ms,
        };
        held.behind_since_ms = Some(since);
        let (held_ms, read_ms) = (held.body.written_at_ms, body.written_at_ms);
        if now_ms - since >= unpin_ms {
            held.body = body.clone();
            held.behind_since_ms = None;
            drop(newest);
            self.report(ReadEvent::Unpinned {
                node: node.to_owned(),
                held_ms,
                read_ms,
            });
            return false;
        }
        drop(newest);
        self.behind
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.report(ReadEvent::Behind {
            node: node.to_owned(),
            held_ms,
            read_ms,
        });
        true
    }

    /// Notes that a read made to judge a record found node `node`'s lease
    /// absent: a current answer, so the newest held is not merely pinned by
    /// its date, and the unpin window starts again. Stale bodies of a
    /// deleted lease, read between such answers, never replace it.
    pub fn absent(&self, node: &str) {
        if matches!(self.fault, ReadFault::Off | ReadFault::AbsentKeepsWindow) {
            return;
        }
        if let Some(held) = self.lock().get_mut(node) {
            held.behind_since_ms = None;
        }
    }

    /// Reports that every read of one judgement of node `node` was behind.
    fn exhausted(&self, node: &str) {
        self.report(ReadEvent::Exhausted {
            node: node.to_owned(),
        });
    }

    /// The newest body held for node `node`.
    pub fn newest(&self, node: &str) -> Option<NodeLeaseObject> {
        self.lock().get(node).map(|n| n.body.clone())
    }

    /// How many reads were found older than the newest held, since the
    /// start.
    pub fn behind_count(&self) -> u64 {
        self.behind.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// A read of a node's lease made to judge a record, as
/// [`Owners`] makes it.
enum LeaseRead {
    /// The lease as read (`None`: absent, or in a format that frees
    /// nothing), never older than the newest this process read.
    Read(Option<NodeLeaseObject>),
    /// No answer.
    Unknown,
    /// Every read older than one the process read before.
    Stale,
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

/// Where routing sends a request for an actor ([`Owners::route`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Another node, live in the view, holds it: forward there.
    Owner(String),
    /// No record, or a released one: nobody holds it.
    Nobody,
    /// The record names a node, but not one to forward to: try here.
    Named,
}

/// An ownership record, in version 1 or 2 ([ADR 0035]).
///
/// [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRecord {
    /// The node id and instance holding it, or nobody.
    pub holder: Option<(String, String)>,
    /// The generation of the last acquisition.
    pub generation: u64,
    /// The recorded head of a version 2 record; `None`: version 1, which
    /// carries no head evidence.
    pub recorded: Option<RecordedHead>,
}

impl OwnerRecord {
    /// The stored form: version 1 without a recorded head (byte for byte
    /// what earlier releases wrote), version 2 with one.
    pub fn encode(&self) -> Vec<u8> {
        let (id, instance) = self
            .holder
            .as_ref()
            .map_or(("-", "-"), |(n, i)| (n.as_str(), i.as_str()));
        match &self.recorded {
            None => format!("{OWNER_HEADER}\n{id} {instance}\n{}\n", self.generation).into_bytes(),
            Some(recorded) => {
                let mut out = format!("{OWNER_HEADER_V2}\n{id} {instance}\n{}\n", self.generation);
                recorded.encode_lines(&mut out);
                out.into_bytes()
            }
        }
    }

    /// Parses a stored record of either version; `None` for anything else,
    /// a migration tombstone ([`OwnerObject`]) and unknown versions
    /// included. Version 1 is read as it always was (its trailing lines
    /// ignored); version 2 must be exactly its canonical form, with a
    /// valid combination of fields.
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        let header = lines.next()?;
        let v2 = match header {
            OWNER_HEADER => false,
            OWNER_HEADER_V2 => true,
            _ => return None,
        };
        let (id, instance) = lines.next()?.split_once(' ')?;
        let holder = (id != "-").then(|| (id.to_owned(), instance.to_owned()));
        if !v2 {
            let generation = lines.next()?.parse().ok()?;
            return Some(Self {
                holder,
                generation,
                recorded: None,
            });
        }
        if (id == "-") != (instance == "-") || !holder_token(id) || !holder_token(instance) {
            return None;
        }
        let generation = crate::recorded_head::number(lines.next()?)?;
        let recorded = RecordedHead::decode_lines(lines)?;
        if !recorded.valid_for(generation, holder.is_some()) {
            return None;
        }
        let record = Self {
            holder,
            generation,
            recorded: Some(recorded),
        };
        // Strict: no extra, missing or non-canonical bytes.
        (record.encode() == body).then_some(record)
    }
}

/// A version 2 holder token (node id or instance, or `-`): nonempty, with
/// no whitespace or control character. Version 1 keeps its historical,
/// lenient parsing.
fn holder_token(t: &str) -> bool {
    !t.is_empty() && !t.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// The bytes of a record about to be written, only if they decode back
/// to it: a version 2 record whose fields are not a valid combination
/// would refuse the actor everywhere. Refused before anything is sent.
fn checked(record: &OwnerRecord) -> Result<Vec<u8>, AcquireError> {
    let body = record.encode();
    if OwnerRecord::decode(&body).as_ref() == Some(record) {
        Ok(body)
    } else {
        Err(AcquireError::Corrupt)
    }
}

/// What an ownership key holds: a record of either version, or the
/// migration tombstone ([`crate::legacy_layout::TOMBSTONE_HEADER`]), which
/// is dispatched first and is never an unknown ownership version. Buckets
/// a 0.8.x `cluster migrate` migrated keep their tombstones for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerObject {
    /// An ownership record.
    Record(OwnerRecord),
    /// The actor moved to the root `to`; `unowned` when no record existed
    /// at the source. Never acquirable here.
    Tombstone {
        /// Where the actor now lives.
        to: String,
        /// Written where no record existed.
        unowned: bool,
    },
}

impl OwnerObject {
    /// Parses what an ownership key holds; `None` if unreadable.
    pub fn decode(body: &[u8]) -> Option<Self> {
        if let Some(to) = crate::legacy_layout::tombstone_target(body) {
            return Some(Self::Tombstone {
                to: to.to_owned(),
                unowned: crate::legacy_layout::is_unowned(body),
            });
        }
        if body.starts_with(crate::legacy_layout::TOMBSTONE_HEADER.as_bytes()) {
            return None;
        }
        OwnerRecord::decode(body).map(Self::Record)
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
    /// Its acknowledged head (ADR 0035, section 2), kept in the hold cell
    /// and frozen when the hold ends: what its release, park or pending
    /// release carries. Unverified at acquisition.
    pub head: crate::hold_cell::HoldHead,
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
        /// The record the write was conditioned on (`None`: there was
        /// none). While the record is still that one, the write may land.
        before: Option<Box<OwnerRecord>>,
    },
    /// The record or a node lease is unreadable.
    Corrupt,
    /// This process still has a live hold of the actor, ending or not
    /// (ADR 0035, section 2): an acquisition waits for its frozen head.
    /// Nothing written.
    LocalHold,
    /// The record is in version 2 (ADR 0035) and this node does not write
    /// that format for the app: refused, nothing written. Never written
    /// over with version 1.
    FormatMismatch,
    /// No answer before writing: nothing written.
    Unavailable,
    /// Every read of the holder's node lease returned a body older than
    /// one this process had read before (a stale replica or cache, ADR
    /// 0020's amendment on lease reads): nothing judged, nothing written.
    StaleRead {
        /// The holder's node.
        node: String,
    },
    /// An idle expiry's acquisition ([`Owners::acquire_unchanged`]): the
    /// record is not the one the listing showed, or names a holder. Nothing
    /// written.
    Changed,
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
    /// A resolution only, asked to wait: the record is still the one the
    /// unresolved acquire's write was conditioned on, so that write may
    /// still land. Read it again later.
    Unchanged,
}

/// How long the resolver of an unresolved acquire waits for a record that
/// has not changed, by default (ms): `object_store`'s own request timeout
/// (30 s), so a write the client gave up on has as long again to land
/// before the resolver stops reading for it ([ADR 0020], section 3).
///
/// [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
pub const UNRESOLVED_WAIT_MS: u64 = 30_000;

/// Ownership records for one node.
#[derive(Debug)]
pub struct Owners<B, C> {
    bucket: B,
    clock: C,
    node: String,
    instance: String,
    nodes_prefix: String,
    config: LeaseConfig,
    unresolved_wait_ms: u64,
    /// The app has enabled the recorded-head format (ADR 0035, section 5):
    /// acquisitions and releases write version 2.
    recorded_head: std::sync::atomic::AtomicBool,
    /// The newest node lease bodies the process has read.
    reads: std::sync::Arc<NewestReads>,
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
            unresolved_wait_ms: UNRESOLVED_WAIT_MS,
            recorded_head: std::sync::atomic::AtomicBool::new(false),
            reads: std::sync::Arc::default(),
        }
    }

    /// Judges node leases against `reads`, the process's newest bodies
    /// read ([`NewestReads`]; by default a table of this value's own).
    /// Every `Owners` of a process, and its heartbeat reads, should share
    /// one.
    #[must_use]
    pub fn with_reads(mut self, reads: std::sync::Arc<NewestReads>) -> Self {
        self.reads = reads;
        self
    }

    /// The table this value judges node leases against.
    pub fn reads(&self) -> &std::sync::Arc<NewestReads> {
        &self.reads
    }

    /// Node `node`'s lease, read after a record to judge it: a body older
    /// than the newest this process has read of that lease is read again,
    /// up to [`BEHIND_READS`] reads in all, and never trusted
    /// ([`LeaseRead::Stale`] if every one is). A failed read is
    /// [`LeaseRead::Unknown`] at once, as every reader before.
    async fn read_lease(&self, node: &str) -> LeaseRead {
        let key = format!("{}{node}", self.nodes_prefix);
        let mut last = None;
        for _ in 0..BEHIND_READS {
            let Ok(read) = self.bucket.get(&key).await else {
                return LeaseRead::Unknown;
            };
            let Some((body, _)) = read else {
                self.reads.absent(node);
                return LeaseRead::Read(None);
            };
            let obj = NodeLeaseObject::decode(&body);
            match &obj {
                Some(o)
                    if self
                        .reads
                        .behind(node, o, self.clock.now_ms(), &self.config) =>
                {
                    last = obj;
                }
                _ => return LeaseRead::Read(obj),
            }
        }
        self.reads.exhausted(node);
        if self.reads.fault() == ReadFault::ExhaustedJudged {
            return LeaseRead::Read(last);
        }
        LeaseRead::Stale
    }

    /// [`Self::read_lease`], or for the read back with the
    /// [`ReadFault::ReadBackUnchecked`] fault, one read judged as read.
    async fn read_lease_back(&self, node: &str) -> LeaseRead {
        if self.reads.fault() != ReadFault::ReadBackUnchecked {
            return self.read_lease(node).await;
        }
        match self
            .bucket
            .get(&format!("{}{node}", self.nodes_prefix))
            .await
        {
            Ok(read) => LeaseRead::Read(read.and_then(|(b, _)| NodeLeaseObject::decode(&b))),
            Err(_) => LeaseRead::Unknown,
        }
    }

    /// How long an unresolved acquire waits for a record that has not
    /// changed ([`UNRESOLVED_WAIT_MS`] by default): at least as long as a
    /// write the bucket client gave up on may still take to land.
    #[must_use]
    pub fn with_unresolved_wait_ms(mut self, ms: u64) -> Self {
        self.unresolved_wait_ms = ms;
        self
    }

    /// Until when (this node's clock, ms) an unresolved acquire made now
    /// waits for a record that has not changed.
    pub fn unresolved_until_ms(&self) -> u64 {
        self.clock.now_ms().saturating_add(self.unresolved_wait_ms)
    }

    /// This node's id.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// This process's instance token.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// This node's clock (ms).
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Whether this app writes the recorded-head format, as its setting
    /// was last read (ADR 0035, section 5). Only ever set by a reader of
    /// the setting; never inferred from a record seen.
    pub fn set_recorded_head(&self, enabled: bool) {
        self.recorded_head
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether acquisitions and releases write version 2.
    pub fn recorded_head(&self) -> bool {
        self.recorded_head.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The record an acquisition at `generation` writes: version 1, or
    /// version 2 with the requirement it carries.
    fn mine(&self, generation: u64, recorded: Option<RecordedHead>) -> OwnerRecord {
        OwnerRecord {
            holder: Some((self.node.clone(), self.instance.clone())),
            generation,
            recorded,
        }
    }

    /// What a record already there lets an acquisition do in this app's
    /// format: a version 2 record is refused unless the app enabled it;
    /// any record is read under the enabled format.
    fn format_allows(&self, record: &OwnerRecord) -> Result<(), AcquireError> {
        if record.recorded.is_some() && !self.recorded_head() {
            return Err(AcquireError::FormatMismatch);
        }
        Ok(())
    }

    /// The requirement an acquisition carries: this process's local
    /// evidence (with the generation of the hold that froze it), combined
    /// by lineage with the record's own when the format is enabled
    /// ([`crate::local_heads::combine`]).
    fn carried(
        &self,
        local: Option<(Requirement, u64)>,
        record: Option<&OwnerRecord>,
    ) -> Option<Requirement> {
        if !self.recorded_head() {
            return local.map(|(r, _)| r);
        }
        let recorded = record.and_then(|r| Requirement::of_record(r).map(|q| (q, r.generation)));
        crate::local_heads::combine(local, recorded)
    }

    /// The version 2 fields of an acquisition carrying `inherited`, over
    /// `before` (its idle origin carried), if the format is enabled.
    fn acquiring(
        &self,
        inherited: Option<Requirement>,
        before: Option<&OwnerRecord>,
        idle_origin_ms: Option<u64>,
    ) -> Option<RecordedHead> {
        self.recorded_head().then(|| {
            // A carried origin, else the one the caller judged (an expiry's
            // listed last-modified time): never a fresh one from a rewrite.
            // Carried only from a released record: a held one may belong to
            // a holder that activated the actor and crashed before its
            // release could clear the origin.
            let idle = before
                .filter(|b| b.holder.is_none())
                .and_then(|b| b.recorded)
                .and_then(|r| r.idle_since_ms)
                .or(idle_origin_ms);
            Requirement::stored(inherited, true, idle)
        })
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
        self.acquire_prepared(key, lease_valid, first_generation, |_, _| Ok(None))
            .await
    }

    /// [`Self::acquire`] with a preparation hook (ADR 0035, section 2):
    /// `prepare(record, generation)` runs after the authoritative read and
    /// the eligibility checks (`record`: what the read decoded, `None` for
    /// an absent record) and before the conditional write at
    /// `generation`. It gives this process's local evidence
    /// ([`crate::local_heads`]) with the generation of the hold that froze
    /// it; combined with the record's (version 2), it is the new hold's
    /// [`crate::hold_cell::HoldHead::inherited`]. An error refuses the
    /// acquisition with nothing written.
    pub async fn acquire_prepared(
        &self,
        key: &str,
        lease_valid: impl Fn() -> bool,
        first_generation: impl AsyncFnOnce() -> Result<u64, AcquireError>,
        prepare: impl FnOnce(
            Option<&OwnerRecord>,
            u64,
        ) -> Result<Option<(Requirement, u64)>, AcquireError>,
    ) -> Result<Hold, AcquireError> {
        let read = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?;
        let mut previous = None;
        let inherited;
        let attempted;
        let (generation, before, written) = match read {
            None => {
                let first = first_generation().await?;
                inherited = self.carried(prepare(None, first)?, None);
                if !lease_valid() {
                    return Err(AcquireError::LeaseInvalid);
                }
                attempted = self.mine(first, self.acquiring(inherited, None, None));
                let written = self.bucket.create(key, checked(&attempted)?).await;
                (first, None, written)
            }
            Some((body, version)) => {
                let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
                self.format_allows(&record)?;
                if let Some((node, instance)) = &record.holder
                    && !(node == &self.node && instance == &self.instance)
                {
                    // Read after the record, never from a view, and never
                    // behind a body of it this process read before.
                    let obj = match self.read_lease(node).await {
                        LeaseRead::Read(obj) => obj,
                        LeaseRead::Unknown => return Err(AcquireError::Unavailable),
                        LeaseRead::Stale => {
                            return Err(AcquireError::StaleRead { node: node.clone() });
                        }
                    };
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
                inherited = self.carried(prepare(Some(&record), generation)?, Some(&record));
                if !lease_valid() {
                    return Err(AcquireError::LeaseInvalid);
                }
                attempted = self.mine(generation, self.acquiring(inherited, Some(&record), None));
                let written = self
                    .bucket
                    .replace(key, checked(&attempted)?, &version)
                    .await;
                (generation, Some((version, record)), written)
            }
        };
        let version = match written {
            Ok(version) => version,
            Err(_) => self.read_back(key, &attempted, before).await?,
        };
        Ok(Hold {
            key: key.to_owned(),
            generation,
            version,
            previous,
            head: HoldHead {
                acknowledged: None,
                inherited,
                recorded: attempted.recorded.is_some(),
                idle_since_ms: attempted.recorded.and_then(|r| r.idle_since_ms),
            },
        })
    }

    /// Acquires the record at `key` for an idle expiry ([ADR 0028],
    /// section 4): only if it names nobody and `unchanged(version)` says
    /// the version read is the one the listing of `owners/` showed. The
    /// check and the conditional write use the one read, so a wake that
    /// rewrote the record since the listing (its acquire, or its acquire
    /// and its release) makes this fail with [`AcquireError::Changed`],
    /// and a wake landing after the read makes the write fail its
    /// condition. Never takes a record that names a holder, this process
    /// included, nor one that is absent.
    ///
    /// [ADR 0028]: ../../../docs/adr/0028-actors-that-end.md
    pub async fn acquire_unchanged(
        &self,
        key: &str,
        unchanged: impl Fn(&Version) -> bool,
        lease_valid: impl Fn() -> bool,
    ) -> Result<Hold, AcquireError> {
        self.acquire_unchanged_prepared(key, unchanged, None, lease_valid, |_, _| Ok(None))
            .await
    }

    /// [`Self::acquire_unchanged`] with the preparation hook of
    /// [`Self::acquire_prepared`]. `idle_origin_ms` is the record's idle
    /// origin as the expiry judged it (its listed last-modified time, or a
    /// carried `idle-since`): a version 2 record that carries none is
    /// rewritten with it, so a refused cycle does not restart the actor's
    /// idle clock (ADR 0035, section 3).
    pub async fn acquire_unchanged_prepared(
        &self,
        key: &str,
        unchanged: impl Fn(&Version) -> bool,
        idle_origin_ms: Option<u64>,
        lease_valid: impl Fn() -> bool,
        prepare: impl FnOnce(
            Option<&OwnerRecord>,
            u64,
        ) -> Result<Option<(Requirement, u64)>, AcquireError>,
    ) -> Result<Hold, AcquireError> {
        let Some((body, version)) = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?
        else {
            return Err(AcquireError::Changed);
        };
        let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
        self.format_allows(&record)?;
        if record.holder.is_some() || !unchanged(&version) {
            return Err(AcquireError::Changed);
        }
        let generation = record
            .generation
            .checked_add(1)
            .ok_or(AcquireError::Corrupt)?;
        let inherited = self.carried(prepare(Some(&record), generation)?, Some(&record));
        if !lease_valid() {
            return Err(AcquireError::LeaseInvalid);
        }
        let attempted = self.mine(
            generation,
            self.acquiring(inherited, Some(&record), idle_origin_ms),
        );
        let written = self
            .bucket
            .replace(key, checked(&attempted)?, &version)
            .await;
        let version = match written {
            Ok(version) => version,
            Err(_) => {
                self.read_back(key, &attempted, Some((version, record)))
                    .await?
            }
        };
        Ok(Hold {
            key: key.to_owned(),
            generation,
            version,
            previous: None,
            head: HoldHead {
                acknowledged: None,
                inherited,
                recorded: attempted.recorded.is_some(),
                idle_since_ms: attempted.recorded.and_then(|r| r.idle_since_ms),
            },
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
    /// in flight, and land. The unresolved acquire carries the record the
    /// write was conditioned on, for the resolver.
    async fn read_back(
        &self,
        key: &str,
        attempted: &OwnerRecord,
        before: Option<(Version, OwnerRecord)>,
    ) -> Result<Version, AcquireError> {
        let generation = attempted.generation;
        let (before_version, before) = before.unzip();
        let unresolved = AcquireError::Unresolved {
            generation,
            before: before.map(Box::new),
        };
        let Ok(Some((body, version))) = self.bucket.get(key).await else {
            return Err(unresolved);
        };
        if before_version.as_ref() == Some(&version) {
            return Err(unresolved);
        }
        let decoded = OwnerRecord::decode(&body);
        // This attempt's write only if exactly its bytes (holder,
        // generation and, in version 2, the requirement it carried).
        let exact = decoded.as_ref() == Some(attempted);
        match decoded.and_then(|r| r.holder.map(|h| (h, r.generation))) {
            Some(((node, instance), g)) if node == self.node && instance == self.instance => {
                if g == generation && exact {
                    Ok(version)
                } else {
                    Err(unresolved)
                }
            }
            Some(((node, instance), _)) => {
                let LeaseRead::Read(obj) = self.read_lease_back(&node).await else {
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
        Ok(match self.route(key, live).await? {
            Route::Owner(node) => Some(node),
            Route::Nobody | Route::Named => None,
        })
    }

    /// [`Self::other_owner`]'s single read, in three answers (ADR 0041,
    /// section 1): another live node holds it, nobody does (no record, or
    /// a released one), or the record names a node this read does not
    /// send to (this node, any instance, or a holder not live in the
    /// view). Only [`Route::Nobody`] may lead to a placement.
    pub async fn route(
        &self,
        key: &str,
        live: impl Fn(&str, &str) -> bool,
    ) -> Result<Route, AcquireError> {
        let Some((body, _)) = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?
        else {
            return Ok(Route::Nobody);
        };
        let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
        Ok(match record.holder {
            None => Route::Nobody,
            Some((node, instance)) if node != self.node && live(&node, &instance) => {
                Route::Owner(node)
            }
            Some(_) => Route::Named,
        })
    }

    /// Releases a hold: the record names nobody, generation kept, `If-Match`
    /// on the hold's version only (never a version read later).
    pub async fn release(&self, hold: &Hold) -> Released {
        self.release_written(hold).await.0
    }

    /// [`Self::release`], with the version the release wrote when this
    /// write's own answer says it landed (`None` for every other outcome):
    /// what an idle pass (ADR 0042, section 3) remembers, to take the
    /// record later only if it is still that one.
    pub async fn release_written(&self, hold: &Hold) -> (Released, Option<Version>) {
        let body = OwnerRecord {
            holder: None,
            generation: hold.generation,
            // ADR 0035, section 1: a hold acquired in version 2 releases
            // with what it froze (complete if verified, else what it
            // inherited, stamp unchanged) and its carried idle origin.
            recorded: hold.head.recorded.then(|| {
                Requirement::stored(Requirement::frozen(hold), false, hold.head.idle_since_ms)
            }),
        }
        .encode();
        // Never a record no reader could decode (a permanent refusal of
        // the actor): kept pending, nothing written.
        if OwnerRecord::decode(&body).is_none() {
            return (Released::Pending, None);
        }
        match self.bucket.replace(&hold.key, body, &hold.version).await {
            Ok(version) => (Released::Released, Some(version)),
            Err(BucketError::Unavailable(_)) => (Released::Pending, None),
            Err(BucketError::Conflict) => {
                (self.still_this_hold(&hold.key, hold.generation).await, None)
            }
        }
    }

    /// Whether the record at `key` still names this instance at
    /// `generation` (one read): the safeguard re-read of a parked hold
    /// (ADR 0024, section 5). `Ok(false)` once it names anything else, or
    /// no record exists; `Err` if the bucket did not answer
    /// (`Unavailable`) or the record cannot be decoded (`Corrupt`: proof of
    /// nothing, so a caller keeps what it holds).
    pub async fn still_names_this(&self, key: &str, generation: u64) -> Result<bool, AcquireError> {
        let Some((body, _)) = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?
        else {
            return Ok(false);
        };
        let r = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
        Ok(r.generation == generation
            && r.holder
                .as_ref()
                .is_some_and(|(n, i)| n == &self.node && i == &self.instance))
    }

    /// Whether the record at `key` names this instance at any generation
    /// (one read): after a release found the record moved on, whether the
    /// actor is still this node's (a wake of it whose write landed
    /// unanswered, which its resolver releases, ADR 0024 section 3). `Err`
    /// if the bucket did not answer or the record is unreadable.
    pub async fn names_this_instance(&self, key: &str) -> Result<bool, AcquireError> {
        let Some((body, _)) = self
            .bucket
            .get(key)
            .await
            .map_err(|_| AcquireError::Unavailable)?
        else {
            return Ok(false);
        };
        let record = OwnerRecord::decode(&body).ok_or(AcquireError::Corrupt)?;
        Ok(record
            .holder
            .is_some_and(|(n, i)| n == self.node && i == self.instance))
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
    ///
    /// With `wait`, a record that is still `before` (the one the write was
    /// conditioned on; `None`: still absent) gives `Unchanged`: the write
    /// may still land, so the entry stays. Without it (its time is up, and
    /// this read was issued after), that record ends the entry, as does
    /// any other. A record's content never comes back once it changed
    /// (every acquisition raises the generation, a release keeps it and
    /// names nobody), so an equal record is the same state; a version token
    /// is never compared.
    pub async fn resolve(
        &self,
        key: &str,
        generation: u64,
        before: Option<&OwnerRecord>,
        wait: bool,
    ) -> Released {
        match self.bucket.get(key).await {
            Ok(Some((body, version))) => match OwnerRecord::decode(&body) {
                // A version 2 attempt of this process that landed: released
                // with the requirement its bytes carried (ADR 0035, section
                // 2), never reconstructed from an older floor. In an app
                // that has not enabled the format, never released over.
                Some(r)
                    if r.recorded.is_some()
                        && r.generation == generation
                        && r.holder
                            .as_ref()
                            .is_some_and(|(n, i)| n == &self.node && i == &self.instance) =>
                {
                    if !self.recorded_head() {
                        return Released::Pending;
                    }
                    let hold = Hold {
                        key: key.to_owned(),
                        generation,
                        version,
                        previous: None,
                        head: HoldHead {
                            acknowledged: None,
                            inherited: Requirement::of_record(&r),
                            recorded: true,
                            idle_since_ms: r.recorded.and_then(|x| x.idle_since_ms),
                        },
                    };
                    self.release(&hold).await
                }
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
                        head: Default::default(),
                    };
                    self.release(&hold).await
                }
                Some(r) if wait && before == Some(&r) => Released::Unchanged,
                _ => Released::Superseded,
            },
            Ok(None) if wait && before.is_none() => Released::Unchanged,
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
                recorded: None,
            },
            OwnerRecord {
                holder: None,
                generation: 9,
                recorded: None,
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

    /// An idle expiry's acquisition (ADR 0028, section 4) takes the record
    /// only as the listing showed it: released, at that version. A wake
    /// since the listing (an acquire, or an acquire and its release), a
    /// record naming a holder (this process too) or no record at all
    /// writes nothing.
    #[test]
    fn an_expiry_acquires_only_the_record_the_listing_showed() {
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
        let version_of = |key: &str| run((&mem).get(key)).unwrap().unwrap().1;
        // Absent: nothing to expire, nothing created.
        assert_eq!(
            run(b.acquire_unchanged("owners/x", |_| true, || true)),
            Err(AcquireError::Changed)
        );
        assert!(run((&mem).get("owners/x")).unwrap().is_none());
        // Held by a (live): never taken, not even when the version matches.
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let listed = version_of("owners/x");
        assert_eq!(
            run(b.acquire_unchanged("owners/x", |v| *v == listed, || true)),
            Err(AcquireError::Changed)
        );
        assert_eq!(
            run(a.acquire_unchanged("owners/x", |v| *v == listed, || true)),
            Err(AcquireError::Changed),
            "this process's own hold"
        );
        // Released: the listing's version, taken at the next generation.
        assert_eq!(run(a.release(&hold)), Released::Released);
        let listed = version_of("owners/x");
        // A wake after the listing (acquire then release): the record
        // names nobody again, at another version: refused.
        let wake = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(run(a.release(&wake)), Released::Released);
        assert_eq!(
            run(b.acquire_unchanged("owners/x", |v| *v == listed, || true)),
            Err(AcquireError::Changed)
        );
        // Listed again: taken, and the waker's next acquire finds b.
        let listed = version_of("owners/x");
        assert_eq!(
            run(b.acquire_unchanged("owners/x", |v| *v == listed, || false)),
            Err(AcquireError::LeaseInvalid)
        );
        let expired = run(b.acquire_unchanged("owners/x", |v| *v == listed, || true)).unwrap();
        assert_eq!(
            (expired.generation, expired.previous),
            (wake.generation + 1, None)
        );
        assert!(matches!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Held { .. })
        ));
    }

    /// The idle pass's memory (ADR 0042, section 3): a release gives the
    /// version it wrote, which is the record's version until anything
    /// rewrites it, so an acquisition unchanged on it takes the record,
    /// and on a stale one does not. A release that does not land gives
    /// none.
    #[test]
    fn a_release_gives_the_version_it_wrote() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut la = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(la.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let version_of = |key: &str| run((&mem).get(key)).unwrap().unwrap().1;
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let (released, written) = run(a.release_written(&hold));
        assert_eq!(released, Released::Released);
        assert_eq!(written, Some(version_of("owners/x")));
        // Released again: the record moved on, nothing written.
        assert_eq!(run(a.release_written(&hold)), (Released::Superseded, None));
        let written = written.unwrap();
        let pass = run(a.acquire_unchanged("owners/x", |v| *v == written, || true)).unwrap();
        assert_eq!(pass.generation, hold.generation + 1);
        assert_eq!(run(a.release(&pass)), Released::Released);
        assert_eq!(
            run(a.acquire_unchanged("owners/x", |v| *v == written, || true)),
            Err(AcquireError::Changed),
            "the pass's own release changed the record"
        );
    }

    /// A write that landed with a failure reported (its reply lost, or a
    /// client retry meeting the landed write: S3's 409 then 412) is read
    /// back: the record names this process at the generation tried, so
    /// the acquisition holds it, with a version that works (it releases).
    /// Whether a record names this process, at any generation (a move of a
    /// parked room asks after a release found the record moved on), and
    /// the parked re-read: an undecodable record is an error for both,
    /// never an answer.
    #[test]
    fn a_record_names_this_process_at_any_generation() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let record = |holder: Option<(&str, &str)>, generation| {
            OwnerRecord {
                holder: holder.map(|(n, i)| (n.to_owned(), i.to_owned())),
                generation,
                recorded: None,
            }
            .encode()
        };
        assert_eq!(run(a.names_this_instance("owners/x")), Ok(false));
        for (holder, names) in [
            (Some(("a", "i1")), true),
            (Some(("a", "i0")), false),
            (Some(("b", "i2")), false),
            (None, false),
        ] {
            mem.overwrite("owners/x", Some(&record(holder, 7)));
            assert_eq!(
                run(a.names_this_instance("owners/x")),
                Ok(names),
                "{holder:?}"
            );
        }
        // At another generation than the one asked: still this process,
        // but not the parked hold.
        mem.overwrite("owners/x", Some(&record(Some(("a", "i1")), 7)));
        assert_eq!(run(a.still_names_this("owners/x", 7)), Ok(true));
        assert_eq!(run(a.still_names_this("owners/x", 6)), Ok(false));
        assert_eq!(run(a.still_names_this("owners/y", 6)), Ok(false));
        mem.overwrite("owners/x", Some(b"junk"));
        assert_eq!(
            run(a.names_this_instance("owners/x")),
            Err(AcquireError::Corrupt)
        );
        assert_eq!(
            run(a.still_names_this("owners/x", 7)),
            Err(AcquireError::Corrupt)
        );
        assert_eq!(
            run(a.other_owner("owners/x", |_, _| true)),
            Err(AcquireError::Corrupt)
        );
    }

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
                recorded: None,
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
                Err(AcquireError::Unresolved {
                    generation: 5,
                    before: Some(Box::new(OwnerRecord {
                        holder: None,
                        generation: 4,
                        recorded: None,
                    })),
                })
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
                recorded: None,
            }
            .encode()
        };
        // Each carries the record its write was conditioned on.
        let unresolved = |holder: Option<(&str, &str)>| {
            Err(AcquireError::Unresolved {
                generation: 5,
                before: Some(Box::new(OwnerRecord {
                    holder: holder.map(|(n, i)| (n.to_owned(), i.to_owned())),
                    generation: 4,
                    recorded: None,
                })),
            })
        };
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
            unresolved(Some(("b", "i1")))
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
                    recorded: None,
                }
                .encode(),
            ),
        );
        *mem.steal_next.borrow_mut() = Some(("owners/f".into(), record(("b", "i2"), 5)));
        assert_eq!(
            run(a.acquire("owners/f", || true, async || Ok(1))),
            unresolved(None)
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
                        recorded: None,
                    }
                    .encode(),
                ),
            );
            *mem.steal_next.borrow_mut() = Some((key.clone(), record(("b", "i3"), 5)));
            mem.fail_get_after.set(Some(after));
            assert_eq!(
                run(a.acquire(&key, || true, async || Ok(1))),
                unresolved(None),
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

    /// ADR 0041, section 1: the same single read answers who holds the
    /// actor in three ways; only "nobody" (no record, or a released one)
    /// may lead to a placement. A record naming any node, live or not,
    /// this one included, is "named".
    #[test]
    fn routing_tells_nobody_from_a_named_holder() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg());
        run(lease.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        let anyone = |_: &str, _: &str| true;
        assert_eq!(run(b.route("owners/x", anyone)), Ok(Route::Nobody));
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(
            run(b.route("owners/x", |n, i| n == "a" && i == "i1")),
            Ok(Route::Owner("a".into()))
        );
        // Not live in the view: named, so never placed (a takeover).
        assert_eq!(run(b.route("owners/x", |_, i| i == "i9")), Ok(Route::Named));
        // This node's own hold: named.
        assert_eq!(run(a.route("owners/x", anyone)), Ok(Route::Named));
        assert_eq!(run(a.release(&hold)), Released::Released);
        // Released: nobody.
        assert_eq!(run(b.route("owners/x", anyone)), Ok(Route::Nobody));
        // `other_owner` is the same read, two ways.
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

    #[test]
    fn which_body_of_a_lease_is_older() {
        let body = |state, i: Option<&str>, at| NodeLeaseObject {
            state,
            instance: i.map(str::to_owned),
            written_at_ms: at,
        };
        let live = |i, at| body(NodeState::Live, Some(i), at);
        // Twice the default skew allowed.
        let tg = fence_window_ms(&cfg());
        assert_eq!(tg, 2_000);
        let older = |a: &NodeLeaseObject, b: &NodeLeaseObject| older_than(a, b, tg);
        // Dated earlier, across instances or between two live bodies.
        assert!(older(&live("i1", 1_000), &live("i1", 1_001)));
        assert!(older(
            &body(NodeState::Released, Some("i1"), 1_000),
            &live("i2", 1_001)
        ));
        assert!(!older(&live("i1", 1_001), &live("i1", 1_000)));
        for end in [NodeState::Fenced, NodeState::Released] {
            // The same instance: its fence or release is newer than any of
            // its live bodies, whatever the dates (its clock stepped back).
            for at in [999, 1_000, 1_001] {
                assert!(older(&live("i1", at), &body(end, Some("i1"), 1_000)));
                assert!(!older(&body(end, Some("i1"), 1_000), &live("i1", at)));
            }
            // Another instance's, dated the same: not ordered either way.
            assert!(!older(&live("i2", 1_000), &body(end, Some("i1"), 1_000)));
            assert!(!older(&body(end, Some("i1"), 1_000), &live("i2", 1_000)));
        }
        // Two ends of one instance: by date.
        assert!(older(
            &body(NodeState::Released, Some("i1"), 1_000),
            &body(NodeState::Fenced, Some("i1"), 1_001)
        ));
        // The same body is not older than itself.
        assert!(!older(&live("i1", 1_000), &live("i1", 1_000)));
        // A fence an operator created over a deleted object (no instance):
        // newer than every named body, live or ended, dated less than twice
        // the skew after it (a predecessor on a clock ahead), older than one
        // dated that much after it or more (a successor), whatever the
        // instance.
        let created = body(NodeState::Fenced, None, 100_000);
        for state in [NodeState::Live, NodeState::Fenced, NodeState::Released] {
            for at in [99_000, 100_000, 101_000, 100_000 + tg - 1] {
                assert!(older(&body(state, Some("i1"), at), &created));
                assert!(!older(&created, &body(state, Some("i1"), at)));
            }
            for at in [100_000 + tg, 100_000 + tg + 1] {
                assert!(older(&created, &body(state, Some("i2"), at)));
                assert!(!older(&body(state, Some("i2"), at), &created));
            }
        }
        // Two created fences (one raised to a later date): by date.
        assert!(older(&created, &body(NodeState::Fenced, None, 100_001)));
        assert!(!older(&body(NodeState::Fenced, None, 100_001), &created));
    }

    #[test]
    fn the_newest_reads_only_move_forward() {
        let live = |i: &str, at| NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(i.into()),
            written_at_ms: at,
        };
        // ttl plus grace (the unpin window): 15 s.
        let c = cfg();
        let reads = NewestReads::new();
        assert!(!reads.behind("b", &live("i1", 1_000), 0, &c));
        assert!(!reads.behind("b", &live("i2", 2_000), 0, &c));
        // An earlier body is behind, and leaves the newest as it was.
        assert!(reads.behind("b", &live("i1", 1_000), 10, &c));
        reads.observe("b", &live("i1", 1_500), &c);
        assert_eq!(reads.newest("b"), Some(live("i2", 2_000)));
        assert_eq!(reads.behind_count(), 1);
        // Per node.
        assert!(!reads.behind("c", &live("i1", 1_000), 10, &c));
        // Off: nothing held, nothing behind.
        let off = NewestReads::off();
        off.observe("b", &live("i2", 2_000), &c);
        assert!(!off.behind("b", &live("i1", 1_000), 0, &c));
        assert_eq!(off.newest("b"), None);
    }

    /// The newest held never pins a node for ever: a body dated far ahead
    /// (a clock that ran ahead) holds back older bodies only while they
    /// have not persisted for the window with no read confirming it; then
    /// the older body replaces it, reported. The window counts from the
    /// first behind read, so a gap with no reads at all (an outage) does
    /// not shorten it; a confirming read ends it; a clock that steps back
    /// counts it from the step.
    #[test]
    fn a_body_dated_ahead_holds_the_newest_for_a_window_only() {
        let live = |i: &str, at| NodeLeaseObject {
            state: NodeState::Live,
            instance: Some(i.into()),
            written_at_ms: at,
        };
        // ttl plus grace (the unpin window): 15 s.
        let c = cfg();
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = std::sync::Arc::clone(&events);
        let reads = NewestReads::new().with_report(move |e| seen.lock().unwrap().push(e.clone()));
        // k, on a clock an hour ahead, then dead; m, its successor.
        reads.observe("b", &live("k", 3_600_000), &c);
        // A minute with no read, then a behind read: the window starts now.
        assert!(reads.behind("b", &live("m", 61_000), 61_000, &c));
        // A read confirming k ends it.
        reads.observe("b", &live("k", 3_600_000), &c);
        assert!(reads.behind("b", &live("m", 80_000), 80_000, &c));
        assert!(reads.behind("b", &live("m", 94_000), 94_999, &c));
        // Behind for 15 s with no confirming read: m's body replaces it.
        assert!(!reads.behind("b", &live("m", 95_000), 95_000, &c));
        assert_eq!(reads.newest("b"), Some(live("m", 95_000)));
        assert!(events.lock().unwrap().contains(&ReadEvent::Unpinned {
            node: "b".into(),
            held_ms: 3_600_000,
            read_ms: 95_000
        }));
        // The reader's clock steps back: the window counts from the step.
        reads.observe("b", &live("x", 9_000_000), &c);
        assert!(reads.behind("b", &live("m", 99_000), 99_000, &c));
        assert!(reads.behind("b", &live("m", 50_000), 50_000, &c));
        assert!(reads.behind("b", &live("m", 64_000), 64_999, &c));
        assert!(!reads.behind("b", &live("m", 65_000), 65_000, &c));
    }

    /// A deleted lease answers absent between stale reads of its old
    /// bodies: each absent answer is current, so the window starts again
    /// and the old bodies never replace the newest held, however long it
    /// lasts. Without absent answers they do, after the window.
    #[test]
    fn an_absent_lease_keeps_the_newest_pinned() {
        let body = |state, at| NodeLeaseObject {
            state,
            instance: Some("k".into()),
            written_at_ms: at,
        };
        let c = cfg();
        for fault in [ReadFault::None, ReadFault::AbsentKeepsWindow] {
            let reads = NewestReads::with_fault(fault);
            reads.observe("b", &body(NodeState::Fenced, 10_000), &c);
            for at in (20_000..60_000).step_by(1_000) {
                let stale = reads.behind("b", &body(NodeState::Live, 9_000), at, &c);
                reads.absent("b");
                if fault == ReadFault::None {
                    assert!(stale, "{at}: an old body judged");
                }
            }
            let held = reads.newest("b").unwrap().state;
            assert_eq!(
                held == NodeState::Fenced,
                fault == ReadFault::None,
                "{fault:?}"
            );
        }
    }

    /// A stale read of a predecessor's body never frees a record its
    /// current instance holds, once this process has read that instance's
    /// lease: b's i1 stopped cleanly long ago, b runs as i2 and holds the
    /// actor, and the bucket serves i1's `released` body to a's next reads.
    /// Read again, then refused as a stale read while it stays stale; held
    /// by i2 once a read is current.
    #[test]
    fn a_stale_read_of_a_predecessor_frees_nothing() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(100_000);
        let old = NodeLeaseObject {
            state: NodeState::Released,
            instance: Some("i1".into()),
            written_at_ms: 1_000,
        }
        .encode();
        mem.overwrite("nodes/b", Some(&old));
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        let v = run((&mem).get("nodes/b")).unwrap().unwrap().1;
        run(lb.start(Some(&v))).unwrap();
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        let hold = run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(hold.generation, 1);
        let reads = std::sync::Arc::new(NewestReads::new());
        let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
        // a's heartbeat reads saw b live as i2.
        let current =
            NodeLeaseObject::decode(&run((&mem).get("nodes/b")).unwrap().unwrap().0).unwrap();
        reads.observe("b", &current, &cfg());
        // Stale on every read: refused, nothing written.
        *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), old.clone(), BEHIND_READS as u32));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::StaleRead { node: "b".into() })
        );
        assert_eq!(reads.behind_count(), BEHIND_READS as u64);
        // Stale once: read again, and the current body says b holds it.
        *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), old.clone(), 1));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Held {
                node: "b".into(),
                instance: "i2".into()
            })
        );
        // Every read stale, with the exhausted read judged anyway: freed.
        let judging = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(
            std::sync::Arc::new(NewestReads::with_fault(ReadFault::ExhaustedJudged)),
        );
        judging.reads().observe("b", &current, &cfg());
        *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), old.clone(), BEHIND_READS as u32));
        assert_eq!(
            run(judging.acquire("owners/x", || true, async || Ok(1))).map(|h| h.generation),
            Ok(2),
            "judging an exhausted read takes the record b holds"
        );
        // Without the table (the rule before), one stale read frees it.
        let trusting = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg())
            .with_reads(std::sync::Arc::new(NewestReads::off()));
        *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), old.clone(), 1));
        assert_eq!(
            run(trusting.acquire("owners/x", || true, async || Ok(1))).map(|h| h.generation),
            Ok(3),
            "the unfixed rule takes the record a holds"
        );
    }

    /// An operator deleted b's lease while b ran as i1 on a clock ahead,
    /// and `cluster fence` created a fence dated by a clock behind it, so
    /// before i1's last `live` body. b then runs as i2 and holds the actor,
    /// and the bucket serves i1's `live` body to a once more. The created
    /// fence is newer than every body of a named instance dated less than
    /// twice the skew after it, so i1's body is behind and frees nothing
    /// (judged by date alone, the fence was behind and i1's body the
    /// newest, and that stale read took the record i2 held).
    #[test]
    fn a_created_fence_is_newer_than_the_bodies_before_it() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(100_500);
        let i1 = NodeLeaseObject {
            state: NodeState::Live,
            instance: Some("i1".into()),
            written_at_ms: 101_000,
        };
        let fence = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: None,
            written_at_ms: 100_000,
        };
        mem.overwrite("nodes/b", Some(&i1.encode()));
        mem.overwrite(
            "owners/x",
            Some(
                &OwnerRecord {
                    holder: Some(("b".into(), "i1".into())),
                    generation: 1,
                    recorded: None,
                }
                .encode(),
            ),
        );
        let reads = std::sync::Arc::new(NewestReads::new());
        let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
        // a's heartbeat reads saw i1 live.
        reads.observe("b", &i1, &cfg());
        // Deleted, and the fence created: current, so judged (not yet free).
        mem.overwrite("nodes/b", Some(&fence.encode()));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Held {
                node: "b".into(),
                instance: "i1".into()
            })
        );
        assert_eq!(reads.newest("b"), Some(fence.clone()));
        assert_eq!(reads.behind_count(), 0);
        // b starts as i2 at the earliest its start check allows (twice the
        // skew after the fence, on its clock), and takes the actor.
        t.set(102_000);
        let v = run((&mem).get("nodes/b")).unwrap().unwrap().1;
        let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        run(lb.start(Some(&v))).unwrap();
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        let hold = run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(hold.generation, 2);
        // i1's body served once more: behind the fence, read again, and the
        // current body says i2 holds the actor.
        t.set(102_100);
        *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), i1.encode(), 1));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Held {
                node: "b".into(),
                instance: "i2".into()
            })
        );
        assert_eq!(reads.behind_count(), 1);
    }

    /// What the window leaves to a successor that starts soon after a
    /// created fence (review of the window's first form, ttl plus grace):
    /// a pins b's fence (100 s), and b starts as i2 without waiting for
    /// the fence's date, as a node of 0.8.0 may when the fence lands near
    /// the end of its start check (at 101 s, inside the window), or at the
    /// earliest a node of this version starts (102 s). b takes the actor and
    /// renews at 114 s. At 116 s, past the fence's ttl plus grace, a's
    /// current read and then the fence served again both leave the record
    /// to i2, either way. Under a window of ttl plus grace, the renewed body
    /// was behind the fence and the fence then took the record.
    #[test]
    fn a_successor_soon_after_a_created_fence_keeps_its_record() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let fence = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: None,
            written_at_ms: 100_000,
        };
        for start in [101_000, 102_000] {
            let mem = Mem::default();
            let t = Cell::new(100_500);
            mem.overwrite("nodes/b", Some(&fence.encode()));
            let reads = std::sync::Arc::new(NewestReads::new());
            let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
            reads.observe("b", &fence, &cfg());
            t.set(start);
            let v = run((&mem).get("nodes/b")).unwrap().unwrap().1;
            let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
            run(lb.start(Some(&v))).unwrap();
            let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
            let hold = run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
            assert_eq!(hold.generation, 1);
            t.set(114_000);
            run(lb.renew()).unwrap();
            t.set(116_000);
            assert!(lb.is_valid());
            let held = Err(AcquireError::Held {
                node: "b".into(),
                instance: "i2".into(),
            });
            assert_eq!(
                run(a.acquire("owners/x", || true, async || Ok(1))),
                held,
                "start {start}: the current body"
            );
            *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), fence.encode(), 1));
            assert_eq!(
                run(a.acquire("owners/x", || true, async || Ok(1))),
                held,
                "start {start}: the fence served again"
            );
        }
    }

    /// The limit of the window in a cluster with nodes of 0.8.0 (ADR 0020,
    /// amendment on lease reads): a node of 0.8.0 whose start check ends
    /// right after a fence lands over its deleted lease may date its first
    /// body inside the window (101 s for a fence at 100 s). A reader whose
    /// last read of b is that body takes it for a stale read and keeps the
    /// fence; once the fence's ttl plus grace has passed, the fence served
    /// again frees the record b holds. A node of this version dates its
    /// first body past the window (102 s), and the same reads leave the
    /// record to b.
    #[test]
    fn a_start_inside_the_fence_window_is_not_closed() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let fence = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: None,
            written_at_ms: 100_000,
        };
        for (start, freed) in [(101_000, true), (102_000, false)] {
            let mem = Mem::default();
            let t = Cell::new(100_500);
            mem.overwrite("nodes/b", Some(&fence.encode()));
            let reads = std::sync::Arc::new(NewestReads::new());
            let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
            reads.observe("b", &fence, &cfg());
            t.set(start);
            let v = run((&mem).get("nodes/b")).unwrap().unwrap().1;
            let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
            run(lb.start(Some(&v))).unwrap();
            let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
            run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
            // a reads b's first body, and nothing of b after it.
            t.set(start + 100);
            let first = run((&mem).get("nodes/b")).unwrap().unwrap().0;
            let first = NodeLeaseObject::decode(&first).unwrap();
            assert_eq!(reads.behind("b", &first, t.get(), &cfg()), freed);
            // b renews; past the fence's ttl plus grace, the fence again.
            t.set(114_000);
            run(lb.renew()).unwrap();
            t.set(116_000);
            assert!(lb.is_valid());
            *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), fence.encode(), 1));
            let got = run(a.acquire("owners/x", || true, async || Ok(1)));
            if freed {
                assert_eq!(got.map(|h| h.generation), Ok(2), "the limit");
            } else {
                assert_eq!(
                    got,
                    Err(AcquireError::Held {
                        node: "b".into(),
                        instance: "i2".into()
                    })
                );
            }
        }
    }

    /// What a created fence cannot close (ADR 0020, "What it does not
    /// close"): a lease deleted out of band while its node runs. An
    /// operator's create, dated 100 s by `operator_fence` before it is
    /// sent, is still in flight (a bucket retrying it after a 5xx) when b
    /// starts at 108 s over no object and takes the actor. Then b's lease is
    /// deleted, and the create lands: `If-None-Match: *` cannot tell that
    /// absence from the one it was sent for. At 115.5 s the fence is ttl plus
    /// grace old, and a takes the record while b's lease is valid until
    /// 116 s. Dated by the bucket's time once it is seen (as `cluster fence`
    /// does for a created fence it finds), the fence frees nothing then.
    #[test]
    fn a_created_fence_landing_late_is_not_closed() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let fence = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: None,
            written_at_ms: 100_000,
        };
        for raised in [false, true] {
            let mem = Mem::default();
            let t = Cell::new(108_000);
            let mut lb = NodeLease::new(&mem, At(&t), "nodes/b", "ib", cfg());
            run(lb.start(None)).unwrap();
            let b = Owners::new(&mem, At(&t), "b", "ib", "nodes/", cfg());
            let hold = run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
            assert_eq!(hold.generation, 1);
            // Deleted out of band; the create lands at 109 s.
            t.set(109_000);
            mem.overwrite("nodes/b", None);
            mem.overwrite("nodes/b", Some(&fence.encode()));
            if raised {
                // The bucket's time for that write, plus a second and the
                // skew.
                assert_eq!(
                    run(raise_created_fence(&&mem, "nodes/b", 111_000)),
                    Ok(true)
                );
            }
            t.set(115_500);
            assert_eq!(lb.valid_until_ms(), Some(116_000));
            let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg());
            let got = run(a.acquire("owners/x", || true, async || Ok(1)));
            if raised {
                assert_eq!(
                    got,
                    Err(AcquireError::Held {
                        node: "b".into(),
                        instance: "ib".into()
                    })
                );
            } else {
                assert_eq!(got.map(|h| h.generation), Ok(2), "the limit");
            }
        }
    }

    /// b's clock steps back while it runs as i2, then b is fenced: the
    /// fence keeps i2's last date, earlier than a live body a read before
    /// the step. The fence is still newer (the same instance's end), so the
    /// record b held frees once that date is ttl plus grace old, as without
    /// the table.
    #[test]
    fn a_fence_after_a_clock_step_back_frees_after_ttl_and_grace() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(50_000);
        let body = |state, at| NodeLeaseObject {
            state,
            instance: Some("i2".into()),
            written_at_ms: at,
        };
        mem.overwrite(
            "owners/x",
            Some(
                &OwnerRecord {
                    holder: Some(("b".into(), "i2".into())),
                    generation: 3,
                    recorded: None,
                }
                .encode(),
            ),
        );
        let reads = std::sync::Arc::new(NewestReads::new());
        let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
        // a read b live as i2 at 50 s; b's clock then went back 20 s.
        reads.observe("b", &body(NodeState::Live, 50_000), &cfg());
        mem.overwrite("nodes/b", Some(&body(NodeState::Live, 30_000).encode()));
        t.set(52_000);
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::StaleRead { node: "b".into() }),
            "a live body of i2 dated before one read is not judged"
        );
        // Fenced over that body: it keeps 30 s.
        mem.overwrite("nodes/b", Some(&body(NodeState::Fenced, 30_000).encode()));
        t.set(30_000 + 15_000);
        assert!(matches!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Held { .. })
        ));
        t.set(30_000 + 15_001);
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))).map(|h| h.generation),
            Ok(4)
        );
    }

    /// The read back after a failed acquisition write judges the rival's
    /// lease as the acquisition does: a rival b as i3 took the record, a
    /// read b live as i3, and the bucket serves b's earlier `live` body as
    /// i2 once. Read again: the race is lost to i3 (`Held`). Judged as read
    /// (the fault), the stale body frees i3's record and the acquire stays
    /// unresolved instead.
    #[test]
    fn the_read_back_never_judges_a_stale_body() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(100_000);
        let live = |i: &str, at| {
            NodeLeaseObject {
                state: NodeState::Live,
                instance: Some(i.into()),
                written_at_ms: at,
            }
            .encode()
        };
        let record = |holder: Option<(&str, &str)>, generation| {
            OwnerRecord {
                holder: holder.map(|(n, i)| (n.to_owned(), i.to_owned())),
                generation,
                recorded: None,
            }
            .encode()
        };
        mem.overwrite("nodes/b", Some(&live("i3", 99_000)));
        for (fault, want) in [
            (
                ReadFault::None,
                Err(AcquireError::Held {
                    node: "b".into(),
                    instance: "i3".into(),
                }),
            ),
            (
                ReadFault::ReadBackUnchecked,
                Err(AcquireError::Unresolved {
                    generation: 5,
                    before: Some(Box::new(OwnerRecord {
                        holder: None,
                        generation: 4,
                        recorded: None,
                    })),
                }),
            ),
        ] {
            let reads = std::sync::Arc::new(NewestReads::with_fault(fault));
            let a = Owners::new(&mem, At(&t), "a", "a1", "nodes/", cfg()).with_reads(reads.clone());
            reads.observe(
                "b",
                &NodeLeaseObject::decode(&live("i3", 99_000)).unwrap(),
                &cfg(),
            );
            mem.overwrite("owners/r", Some(&record(None, 4)));
            *mem.steal_next.borrow_mut() = Some(("owners/r".into(), record(Some(("b", "i3")), 5)));
            *mem.serve_stale.borrow_mut() = Some(("nodes/b".into(), live("i2", 90_000), 1));
            assert_eq!(
                run(a.acquire("owners/r", || true, async || Ok(1))),
                want,
                "{fault:?}"
            );
        }
    }

    /// A version 2 record (ADR 0035) is read everywhere a version 1 record
    /// is, but a node whose app has not enabled the format never writes
    /// over one: acquisition (either path) refuses with a format mismatch,
    /// and a release or a resolution over it writes nothing.
    #[test]
    fn a_version_2_record_is_read_but_never_written_over() {
        use crate::lease::tests::{At, Mem, run};
        use crate::recorded_head::{Completeness, Position, RecordedHead};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/b", "i2", cfg());
        run(lease.start(None)).unwrap();
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let recorded = RecordedHead {
            head: Position::Known {
                generation: 4,
                txid: 9,
                checksum: 0xfeed,
            },
            set_at: 5,
            completeness: Completeness::Complete,
            idle_since_ms: None,
        };
        let released = OwnerRecord {
            holder: None,
            generation: 5,
            recorded: Some(recorded),
        };
        let read = |k: &str| run((&mem).get(k)).unwrap().unwrap();

        mem.overwrite("owners/x", Some(&released.encode()));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::FormatMismatch)
        );
        let (_, version) = read("owners/x");
        assert_eq!(
            run(a.acquire_unchanged("owners/x", |v| v == &version, || true)),
            Err(AcquireError::FormatMismatch)
        );
        assert_eq!(read("owners/x").0, released.encode());

        // Held by this very process in version 2 (which this binary never
        // writes): routing and the read helpers see it; a release or a
        // resolution never replaces it with version 1 bytes.
        let mine = OwnerRecord {
            holder: Some(("a".into(), "i1".into())),
            generation: 6,
            recorded: Some(RecordedHead {
                completeness: Completeness::LowerBound,
                ..recorded
            }),
        };
        mem.overwrite("owners/x", Some(&mine.encode()));
        let (_, version) = read("owners/x");
        assert_eq!(run(a.still_names_this("owners/x", 6)), Ok(true));
        assert_eq!(run(a.names_this_instance("owners/x")), Ok(true));
        let hold = Hold {
            key: "owners/x".into(),
            generation: 6,
            version,
            previous: None,
            head: Default::default(),
        };
        let _ = hold;
        assert_eq!(
            run(a.resolve("owners/x", 6, None, false)),
            Released::Pending
        );
        assert_eq!(read("owners/x").0, mine.encode());

        // Another live node's version 2 record routes there.
        let theirs = OwnerRecord {
            holder: Some(("b".into(), "i2".into())),
            generation: 7,
            recorded: mine.recorded,
        };
        mem.overwrite("owners/x", Some(&theirs.encode()));
        assert_eq!(
            run(a.other_owner("owners/x", |n, i| n == "b" && i == "i2")),
            Ok(Some("b".into()))
        );
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::FormatMismatch)
        );
        // A tombstone stays unreadable as a record: never acquired.
        mem.overwrite("owners/x", Some(&crate::legacy_layout::tombstone("p2/")));
        assert_eq!(
            run(a.acquire("owners/x", || true, async || Ok(1))),
            Err(AcquireError::Corrupt)
        );
    }

    /// A version 2 record whose fields would not decode (a stamp above its
    /// generation, here) is never written: refused, nothing sent.
    #[test]
    fn an_invalid_version_2_record_is_never_written() {
        use crate::lease::tests::{At, Mem, run};
        use crate::local_heads::Requirement;
        use crate::recorded_head::{Completeness, Position};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        a.set_recorded_head(true);
        let bad = Requirement {
            head: Position::Known {
                generation: 9,
                txid: 3,
                checksum: 1,
            },
            set_at: 99,
            completeness: Completeness::Complete,
        };
        let got = run(a.acquire_prepared(
            "owners/x",
            || true,
            async || Ok(1),
            |_, _| Ok(Some((bad, 1))),
        ));
        assert_eq!(got, Err(AcquireError::Corrupt));
        assert!(
            run((&mem).get("owners/x")).unwrap().is_none(),
            "nothing written"
        );
    }

    /// Review of S3 (M1): an idle origin is carried only from a released
    /// record. A record held by a node that crashed after activating the
    /// actor (verified, writes served) still carries the origin its own
    /// acquisition took; the takeover must not carry it on, or a recently
    /// active actor would be expired as idle.
    #[test]
    fn an_idle_origin_is_carried_only_from_a_released_record() {
        use crate::lease::tests::{At, Mem, run};
        use std::cell::Cell;
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let a = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg());
        let b = Owners::new(&mem, At(&t), "b", "i2", "nodes/", cfg());
        for o in [&a, &b] {
            o.set_recorded_head(true);
        }
        let idle = |k: &str| {
            OwnerRecord::decode(&run((&mem).get(k)).unwrap().unwrap().0)
                .unwrap()
                .recorded
                .and_then(|r| r.idle_since_ms)
        };
        // A refused expiry left the origin in a released record.
        let released = OwnerRecord {
            holder: None,
            generation: 3,
            recorded: Some(RecordedHead {
                idle_since_ms: Some(1_234),
                ..RecordedHead::UNKNOWN
            }),
        };
        mem.overwrite("owners/x", Some(&released.encode()));
        // B takes it (carried: the record was released), activates, and
        // crashes holding it.
        run(b.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(idle("owners/x"), Some(1_234));
        let fenced = NodeLeaseObject {
            state: NodeState::Fenced,
            instance: Some("i2".into()),
            written_at_ms: 0,
        };
        mem.overwrite("nodes/b", Some(&fenced.encode()));
        t.set(100_000);
        // The takeover over B's held record carries no origin.
        let hold = run(a.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(idle("owners/x"), None);
        assert_eq!(hold.head.idle_since_ms, None);
        assert_eq!(run(a.release(&hold)), Released::Released);
        assert_eq!(idle("owners/x"), None, "released without the stale origin");
    }
}
