//! Whether another process runs with this node id, and whether the bucket
//! enforces conditional writes: checked at start.

use wasmkeep_cluster::lease::{Bucket as _, Version};
use wasmkeep_cluster::node_lease::{NodeLeaseObject, NodeState};

use super::*;

/// Whether this process may use its node id (see [`Cluster::check_identity`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Identity {
    /// Not checked yet: no heartbeat written, no lease taken.
    Checking,
    /// No other process wrote this id's heartbeat while it was checked.
    Unique,
    /// This process must not serve: another writer has its id or its
    /// lease. The verdict, and why in full for the operator (starting
    /// with the verdict's words).
    Taken(Stopped, String),
    /// The bucket breaks a guarantee leases rely on; why, for the operator
    /// ([`Cluster::check_bucket`]).
    Refused(String),
    /// The app's upgrade floor is above this binary's minor version, or
    /// unreadable; why, for the operator ([`Cluster::read_floor`]).
    BelowFloor(String),
    /// A live node of the app runs a version this one cannot run beside,
    /// or the app's floor is below the version before this one; why, for
    /// the operator ([`Cluster::version_guard`], ADR 0043, section 5).
    VersionSkew(String),
}

/// What one read of the app's upgrade floor found ([`Cluster::read_floor`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FloorRead {
    /// Absent (`None`), or at most this binary's minor version: the floor
    /// found.
    Allowed(Option<wasmkeep_cluster::app_format::floor::Minor>),
    /// Above this binary's minor version, or unreadable: why.
    Refused(String),
    /// No answer, or an error: proves nothing.
    NoAnswer(String),
}

/// Why this process stops serving (ADR 0020): what it found writing its
/// node id or over its lease. One phrase per verdict, the same in the
/// exit reason, the readiness answer and the logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// Another process writes this node id.
    Twin,
    /// A peer or an operator fenced this node's lease.
    Fenced,
    /// Another process wrote this node's lease `released`.
    Released,
    /// This node's lease object was deleted.
    Deleted,
    /// A heartbeat in the old format under this node id: an older
    /// version writes it.
    OldVersion,
    /// A node of a version from before node leases runs in this cluster
    /// (the start guard).
    OlderNode,
    /// Something this version cannot read under this node id.
    Unreadable,
    /// The mark of a `wasmkeep cluster forget` under this node id, which
    /// that command did not remove in time.
    Forgotten,
    /// This process held no lease to write (not expected while running).
    NotHeld,
}

impl Stopped {
    /// The verdict, in the words every report of it uses.
    pub(crate) fn words(self) -> &'static str {
        match self {
            Self::Twin => "another process runs with this node id",
            Self::Fenced => "this node was fenced",
            Self::Released => "this node's lease was released by another process",
            Self::Deleted => "this node's lease object was deleted",
            Self::OldVersion => "an older version of WasmKeep runs with this node id",
            Self::OlderNode => "a node of an older WasmKeep version runs in this cluster",
            Self::Unreadable => "this node's lease object holds something this version cannot read",
            Self::Forgotten => "this node's lease is being removed by cluster forget",
            Self::NotHeld => "this node holds no lease",
        }
    }

    /// What a write over the node lease found there.
    pub(crate) fn of(cause: wasmkeep_cluster::node_lease::LostCause) -> Self {
        use wasmkeep_cluster::node_lease::LostCause;
        match cause {
            LostCause::Fenced => Self::Fenced,
            LostCause::Released => Self::Released,
            LostCause::OtherProcess => Self::Twin,
            LostCause::Deleted => Self::Deleted,
            LostCause::OldVersion => Self::OldVersion,
            LostCause::Unreadable => Self::Unreadable,
            LostCause::Forgotten => Self::Forgotten,
            LostCause::NotHeld => Self::NotHeld,
        }
    }

    /// The identity state for this verdict, with `detail` after its words.
    pub(crate) fn identity(self, detail: &str) -> Identity {
        Identity::Taken(self, format!("{}: {detail}", self.words()))
    }
}

/// The start check's line, before the wait: what it waits for, and for how
/// long (whole seconds, rounded up).
pub(super) fn checking_message(node: &str, wait: Duration) -> String {
    let secs = wait.as_millis().div_ceil(1000);
    format!("checking that no other process runs node id {node}; ready in about {secs} s")
}

/// The start check's line when an operator's fence over this node id's
/// missing lease holds the first write back (ADR 0020, amendment on lease
/// reads): seen `when`, and for how long more (whole seconds, rounded up).
pub(super) fn fence_wait_message(node: &str, when: &str, wait: Duration) -> String {
    let secs = wait.as_millis().div_ceil(1000);
    format!(
        "an operator's fence of node id {node} {when}: its first lease write waits until the \
         fence is twice the allowed clock skew old on this clock; ready in about {secs} s"
    )
}

/// The start check's line when `wasmkeep cluster forget` is removing this
/// node id's released lease: the first write waits for the key to be
/// absent, for at most `wait` (whole seconds, rounded up).
pub(super) fn forgotten_wait_message(node: &str, wait: Duration) -> String {
    let secs = wait.as_millis().div_ceil(1000);
    format!(
        "`wasmkeep cluster forget` is removing node id {node}'s released lease: waiting up to \
         {secs} s for it to be gone, then checking the node id again"
    )
}

/// The line once the start check passed and the first lease write landed.
pub(super) fn checked_message(node: &str) -> String {
    format!("no other process runs node id {node}: ready to take actors")
}

impl Cluster {
    /// Whether another writer was found with this node id or over its
    /// lease (at start, or since).
    pub(crate) fn identity_taken(&self) -> bool {
        matches!(*self.identity.borrow(), Identity::Taken(..))
    }

    /// Resolves, with why, once another process is found running with
    /// this node id: at start, or while running ([`Cluster::check_twin`]).
    pub(crate) async fn identity_lost(&self) -> String {
        let mut seen = self.identity.subscribe();
        // The reason copied out, so no guard lives across an await.
        let why = match seen.wait_for(|s| matches!(s, Identity::Taken(..))).await {
            Ok(state) => match &*state {
                Identity::Taken(_, why) => Some(why.clone()),
                _ => Some(String::new()),
            },
            Err(_) => None,
        };
        match why {
            Some(why) => why,
            // The sender lives in `self`: not reached.
            None => std::future::pending().await,
        }
    }

    /// The running check: two processes started with one id at the same
    /// moment both pass the start check, then take turns writing this id's
    /// heartbeat. Each heartbeat carries its process's token, so a listing
    /// that shows this id with another token (or none) means a twin: this
    /// process stops writing its node lease, ends every hold at once without
    /// releasing it (`invalidate_node_lease`), takes no more, and
    /// `wasmkeep serve` shuts down. Listings come just before each write, so
    /// at least one twin always sees the other's write; usually exactly one
    /// does, stops, and the other goes on.
    pub(crate) fn check_twin(&self, nodes: &[NodeInfo]) {
        let Some(ours) = self.membership.instance() else {
            return;
        };
        // Until this process's own heartbeat has reached the bucket, the
        // key may still hold a stopped predecessor's (the start check let
        // this one go on): not a twin.
        if self
            .heartbeat_ok_ms
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return;
        }
        let Some(listed) = nodes.iter().find(|n| n.id == self.node_id) else {
            return;
        };
        // Once this process's write has landed, only another writer can put
        // a different heartbeat under this id: one with another token, or
        // one with none (a process of an older version, which writes no
        // token and does not check).
        if listed.instance.as_deref() == Some(ours) || self.identity_taken() {
            return;
        }
        // A runtime restarted in place: the process may still hold its
        // stopped predecessor's heartbeat as last read (a read of a peer
        // alone brings every node as last read here), or a read issued
        // before this runtime's first write may land after it. That
        // instance ran in this process and stopped before this one joined:
        // it writes no more, so its heartbeat is not a twin's. Judged by
        // the writer, not by when the body was read, so no read in flight,
        // of this runtime or a sibling's, can bring it back as one.
        if listed
            .instance
            .as_deref()
            .is_some_and(|i| self.transport.ran_here(&self.node_id, i))
        {
            return;
        }
        // A new process (another instance) whose first write replaced its
        // stopped predecessor's node lease: a read that answers with that
        // body again after the write (a replica behind the acknowledged
        // write, or a read that lands late), or with an earlier one of the
        // same writer, shows the past, not a twin. That writer can put
        // nothing more under this id: every write of a node lease is
        // conditional on its own last version, which this process's first
        // write replaced, and a body of it written since would be dated
        // later on its own clock. Any other instance, or a later body of
        // that one, is judged below as before.
        if listed.lease.is_some()
            && let Some(instance) = listed.instance.as_deref()
            && self
                .replaced
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_some_and(|(was, at)| was == instance && listed.written_at_ms <= *at)
        {
            return;
        }
        // The same verdict the next lease write's re-read would give.
        let stopped = match listed.lease {
            None => Stopped::OldVersion,
            Some(NodeState::Live) => Stopped::Twin,
            Some(NodeState::Released) => Stopped::Released,
            Some(NodeState::Fenced) => Stopped::Fenced,
        };
        let verdict = stopped.identity(&format!(
            "node {} is also advertised at {} (both started at about the same moment): this one \
             stops and the other goes on; give each node an id of its own",
            self.node_id, listed.address
        ));
        if let Identity::Taken(_, why) = &verdict {
            tracing::error!("{why}");
        }
        self.identity.send_replace(verdict);
        // The other process may take this id's actors: stop acting now,
        // without releasing anything.
        self.invalidate_node_lease(false);
    }

    /// Waits for the node id check; `Err` (why) if another process runs
    /// with this id.
    pub(crate) async fn identity(&self) -> Result<(), String> {
        let mut seen = self.identity.subscribe();
        let state = seen
            .wait_for(|s| *s != Identity::Checking)
            .await
            .map(|s| s.clone());
        match state {
            Ok(
                Identity::Taken(_, why)
                | Identity::Refused(why)
                | Identity::BelowFloor(why)
                | Identity::VersionSkew(why),
            ) => Err(why),
            Ok(_) => Ok(()),
            // The sender lives in `self`: not reached.
            Err(_) => Err("the node id check stopped".into()),
        }
    }

    /// Whether the bucket enforces the conditional writes leases,
    /// replication and the alarm index rely on, proven on a throwaway object
    /// under `probe/` before this node writes or takes anything
    /// ([`wasmkeep_cluster::bucket_check`]). `Some(why)` if it breaks one: the
    /// node must not run. A bucket that does not answer is tried again
    /// every half period, as at any start before the bucket exists.
    pub(super) async fn check_bucket(&self) -> Option<String> {
        use wasmkeep_cluster::bucket_check::{CheckError, check_conditional_writes};
        let period = self.heartbeat_every;
        for attempt in 0u64.. {
            // The instance, never the nonce seed: this key is in the bucket.
            let instance = self.membership.instance().unwrap_or_default();
            let key = format!("probe/{}-{instance}-{attempt}", self.node_id);
            let verdict = check_conditional_writes(&self.control_keys, &key).await;
            let path = object_store::path::Path::from(format!("{}{key}", self.store_prefix()));
            let _ = self.store.delete(&path).await;
            match verdict {
                Ok(()) => return None,
                Err(CheckError::Unsafe(why)) => {
                    return Some(format!(
                        "the bucket does not enforce conditional writes: {why}. WasmKeep relies on \
                         them to keep one owner per actor; use a store that implements them \
                         (Cloudflare R2 and OVHcloud Object Storage pass this check; AWS S3 and MinIO document both conditions)"
                    ));
                }
                Err(CheckError::Unavailable(why)) => {
                    tracing::warn!(error = %why, "checking the bucket's conditional writes failed; retrying before starting");
                    tokio::time::sleep(period / 2).await;
                }
            }
        }
        None
    }

    /// One read of the app's upgrade floor
    /// ([`wasmkeep_cluster::app_format::floor`]), within one lease attempt.
    /// Refused only on an answer: the floor above this binary's minor
    /// version (the message names both), or a body this version cannot
    /// read (fail closed, as an unreadable format setting; the message
    /// names the key). Absent or at most this version: allowed.
    pub(super) async fn read_floor(&self) -> FloorRead {
        use wasmkeep_cluster::app_format::floor;
        let name = floor::key_of(&self.root);
        let key = object_store::path::Path::from(name.as_str());
        let this = match &self.own_minor {
            Ok(this) => *this,
            Err(why) => return FloorRead::Refused(why.clone()),
        };
        let read = tokio::time::timeout(self.lease_attempt, async {
            match self.store.get(&key).await {
                Ok(got) => got.bytes().await.map(Some),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
        .await;
        match read {
            Ok(Ok(body)) => match floor::decode(body.as_deref()) {
                Ok(found) => match floor::allows(found, this) {
                    Ok(()) => {
                        self.floor_read(found);
                        FloorRead::Allowed(found)
                    }
                    Err(why) => FloorRead::Refused(why),
                },
                Err(e) => FloorRead::Refused(format!(
                    "the app's upgrade floor `{name}` is unreadable ({} bytes); refusing to start",
                    e.len
                )),
            },
            Ok(Err(e)) => FloorRead::NoAnswer(e.to_string()),
            Err(_) => FloorRead::NoAnswer("no answer".to_owned()),
        }
    }

    /// Notes a floor read: the one this node knows only rises.
    fn floor_read(&self, found: Option<wasmkeep_cluster::app_format::floor::Minor>) {
        let mut known = self.known_floor.lock().unwrap_or_else(|p| p.into_inner());
        if found > *known {
            *known = found;
        }
    }

    /// Whether the app's upgrade floor is at `since` (ADR 0045, section
    /// 2, step 3): from the floor this node knows when that is at it,
    /// otherwise from a read of the floor now, within one lease attempt.
    /// A node whose own version is below `since` never reads: the floor
    /// cannot be above it while it serves, and it writes as before.
    ///
    /// # Errors
    /// `Unavailable` without an answer; `Corrupt` for a floor this node
    /// may not serve under or cannot read, or for this node's own version
    /// when it has no minor version.
    pub(super) async fn floor_at(
        &self,
        since: wasmkeep_cluster::app_format::floor::Minor,
    ) -> Result<bool, wasmkeep_cluster::node_lease::AcquireError> {
        use wasmkeep_cluster::node_lease::AcquireError;
        // A version with no minor version is never judged by a guess.
        let this = self.own_minor.as_ref().map_err(|_| AcquireError::Corrupt)?;
        if *this < since {
            return Ok(false);
        }
        let known = *self.known_floor.lock().unwrap_or_else(|p| p.into_inner());
        if known.is_some_and(|k| k >= since) {
            return Ok(true);
        }
        match self.read_floor().await {
            FloorRead::Allowed(found) => Ok(found.is_some_and(|f| f >= since)),
            FloorRead::Refused(why) => {
                tracing::warn!("{why}");
                Err(AcquireError::Corrupt)
            }
            FloorRead::NoAnswer(_) => Err(AcquireError::Unavailable),
        }
    }

    /// The app's upgrade floor, read before this node writes its first
    /// lease ([`Self::read_floor`]): `Err(why)` if the node must not
    /// start, the floor in force to go on. A read with no answer proves
    /// nothing and is tried again every half period, as the bucket check
    /// is.
    ///
    /// An app with no floor that holds nothing is born finalized (ADR
    /// 0043, section 3): this node creates its floor at its own minor
    /// version ([`wasmkeep_cluster::upgrade::born_finalized`]).
    pub(super) async fn check_floor(
        &self,
    ) -> Result<Option<wasmkeep_cluster::app_format::floor::Minor>, String> {
        // A floor another writer created meanwhile is read again, a few
        // times at most (a read may lag the create).
        let mut again = 3;
        loop {
            match self.read_floor().await {
                FloorRead::Allowed(None) => match self.born_finalized().await {
                    Ok(floor) => return Ok(floor),
                    Err(()) if again > 0 => again -= 1,
                    Err(()) => return Ok(None),
                },
                FloorRead::Allowed(found) => return Ok(found),
                FloorRead::Refused(why) => return Err(why),
                FloorRead::NoAnswer(e) => {
                    tracing::warn!(error = %e, "reading the app's upgrade floor failed; retrying before starting");
                    tokio::time::sleep(self.heartbeat_every / 2).await;
                }
            }
        }
    }

    /// Creates the floor of an app that holds nothing, at this node's
    /// minor version; the floor in force after (`None`: still none). Any
    /// doubt writes nothing and the node starts as before: another
    /// registered node, this node's own earlier lease at an older
    /// version, state, a tombstone, or a bucket that did not answer. A
    /// deploy is not a doubt (see
    /// [`wasmkeep_cluster::upgrade::born_finalized`]). `Err`: the app has
    /// a floor after all (another writer created it), to read.
    ///
    /// The nodes are listed again after the create. An older node of the
    /// app found then started between the listing and the create: the
    /// floor stays, and this node does not start beside it
    /// ([`Self::version_guard`] reports it).
    async fn born_finalized(
        &self,
    ) -> Result<Option<wasmkeep_cluster::app_format::floor::Minor>, ()> {
        use wasmkeep_cluster::upgrade::{Born, FirstWriter, born_finalized};
        let Ok(this) = self.own_minor.clone() else {
            return Ok(None);
        };
        let born = tokio::time::timeout(
            self.lease_attempt.saturating_mul(2),
            born_finalized(
                &self.control_keys,
                self.root.tenant(),
                self.root.app(),
                FirstWriter::Node(&self.node_id),
                this,
                now_ms(),
                self.heartbeat_horizon_ms,
            ),
        )
        .await;
        match born {
            Ok(Ok(Born::Late { floor, nodes })) => {
                let why = format!(
                    "this node created its app's upgrade floor at {floor}, and the listing after \
                     it shows older nodes of the app that started meanwhile: {nodes}. The floor \
                     stays, and such a node cannot come back on its old version: upgrade each of \
                     them (or stop it, or `wasmkeep cluster fence` it), then start this node again"
                );
                *self.born_late.lock().unwrap_or_else(|p| p.into_inner()) = Some(why);
                Ok(Some(floor))
            }
            Ok(Ok(Born::Created(floor))) => {
                tracing::info!(
                    %floor,
                    "this app held nothing: created its upgrade floor at this node's version (ADR 0043)"
                );
                Ok(Some(floor))
            }
            Ok(Ok(Born::HasFloor)) => Err(()),
            Ok(Ok(Born::NotFresh(_))) => Ok(None),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "checking whether this app is new failed; starting without creating its upgrade floor");
                Ok(None)
            }
            Err(_) => {
                tracing::warn!(
                    "checking whether this app is new got no answer; starting without creating its upgrade floor"
                );
                Ok(None)
            }
        }
    }

    /// One listing of the node objects for the start's version check,
    /// tried again every half period until the bucket answers.
    async fn list_for_start(&self) -> wasmkeep_cluster::upgrade::Listed {
        loop {
            match wasmkeep_cluster::upgrade::list_nodes(&self.control_keys).await {
                Ok(listed) => return listed,
                Err(e) => {
                    tracing::warn!(error = %e, "listing the nodes failed; retrying before starting");
                    tokio::time::sleep(self.heartbeat_every / 2).await;
                }
            }
        }
    }

    /// The live nodes of this app in `listed` that this node must not run
    /// beside (ADR 0043, section 5), as one sentence naming each; `None`
    /// if it may. This node's own id is left out, and so is a heartbeat
    /// from before node leases: the start guard waits for it or refuses.
    fn skewed(&self, listed: &wasmkeep_cluster::upgrade::Listed) -> Option<String> {
        let served = format!("{}/{}", self.root.tenant(), self.root.app());
        let now = now_ms();
        let live = listed.nodes.iter().filter(|n| {
            n.id != self.node_id
                && wasmkeep_cluster::membership::serves(n, &served)
                && n.lease.is_some()
                && wasmkeep_cluster::membership::is_live(n, now, self.heartbeat_horizon_ms)
        });
        let outside = match &self.own_minor {
            Ok(this) => wasmkeep_cluster::upgrade::skew(*this, live),
            Err(why) => vec![why.clone()],
        };
        (!outside.is_empty()).then(|| outside.join("; "))
    }

    /// The skew check at start (ADR 0043, section 5), first half, beside
    /// the node id check: `Err(why)` if this node must not start, else
    /// the listing it judged, for [`Self::version_recheck`].
    ///
    /// A node of minor version N refuses to start while a live node of
    /// its app runs a version outside N-1 to N+1, or names one that does
    /// not parse, and names that node. It also refuses when the app has
    /// registered nodes and `floor` (as read before) is below N-1: the
    /// upgrade before this one was never finalized (section 3). And it
    /// refuses when the app has no floor and its registered nodes, all
    /// stopped, last ran a version below N-1
    /// ([`wasmkeep_cluster::upgrade::skipped_minor`]).
    pub(super) async fn version_guard(
        &self,
        floor: Option<wasmkeep_cluster::app_format::floor::Minor>,
    ) -> Result<(wasmkeep_cluster::upgrade::Listed, tokio::time::Instant), String> {
        let this = self.own_minor.clone()?;
        let born_late = self
            .born_late
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(why) = born_late {
            return Err(why);
        }
        let first = self.list_for_start().await;
        if let Some(why) = self.skewed(&first) {
            return Err(why);
        }
        // This node's own earlier lease counts when it names an older
        // version: a node restarted in place on a newer binary.
        let served = format!("{}/{}", self.root.tenant(), self.root.app());
        let registered = first.registered_at_start(&served, &self.node_id, this);
        wasmkeep_cluster::upgrade::floor_from_below(floor, this, registered)?;
        // With no floor record, the leases say what last ran the app: a
        // stopped node is not seen by the skew check above.
        let mut of_app: Vec<&NodeInfo> = first
            .nodes
            .iter()
            .filter(|n| wasmkeep_cluster::membership::serves(n, &served))
            .collect();
        let judge = |nodes: &[&NodeInfo]| {
            wasmkeep_cluster::upgrade::skipped_minor(
                floor,
                this,
                &self.node_id,
                nodes.iter().copied(),
            )
        };
        if let Err(why) = judge(&of_app) {
            // Leases from before the app was deleted and created again
            // are another app's: read only when they would refuse.
            let Some(since) = self.recreated_at().await else {
                return Err(why);
            };
            of_app.retain(|n| n.written_at_ms > since);
            judge(&of_app)?;
        }
        if floor.is_none() {
            let behind = wasmkeep_cluster::upgrade::ran_below_previous(this, of_app);
            if !behind.is_empty() {
                tracing::warn!(
                    nodes = %behind
                        .iter()
                        .map(|n| format!("{} ({})", n.id, n.version))
                        .collect::<Vec<_>>()
                        .join(", "),
                    "node leases of this app name a version more than one minor version below \
                     this one: taken for nodes retired before the app's last upgrade, as others \
                     ran the version between (ADR 0043, section 5)"
                );
            }
        }
        Ok((first, tokio::time::Instant::now()))
    }

    /// When this app was created again after an `app delete` (`deploy
    /// --recreate`, ADR 0028): the last-modified time of its `recreated`
    /// marker, by the bucket's clock; `None` if it never was. Every lease
    /// of the incarnation before was last written earlier: `--recreate` is
    /// refused until well after the app's nodes last wrote. Listed again
    /// every half period until the bucket answers.
    async fn recreated_at(&self) -> Option<u64> {
        use wasmkeep_cluster::control::Keys as _;
        let folder = format!("apps/{}/{}/", self.root.tenant(), self.root.app());
        let marker = format!("{folder}recreated");
        loop {
            match self.control_keys.list_keys(&folder).await {
                Ok(keys) => {
                    return keys
                        .into_iter()
                        .find_map(|(k, at)| (k == marker).then_some(at));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "listing the app's markers failed; retrying before starting");
                    tokio::time::sleep(self.heartbeat_every / 2).await;
                }
            }
        }
    }

    /// The skew check's second half, once the node id check passed and
    /// before the first lease write: `Some(why)` if this node must not
    /// start. The nodes are listed again, for one that became live
    /// meanwhile, and for an object under `nodes/` that does not decode:
    /// it cannot be judged by its fields, but rewritten between the two
    /// listings (or written since the first), a node this version cannot
    /// read is running. Only when the first listing (`first`, made at
    /// `at`) held such an object is the second one held back until a
    /// lease's ttl plus grace after it, the time its writer is given to
    /// show itself; a start with none is not delayed. This node's own key
    /// is the node id check's to judge.
    pub(super) async fn version_recheck(
        &self,
        first: &wasmkeep_cluster::upgrade::Listed,
        at: tokio::time::Instant,
    ) -> Option<String> {
        let own = self.node_key();
        if first.unread.iter().any(|(key, _)| *key != own) {
            tokio::time::sleep_until(at + self.alarm_overdue).await;
        }
        let second = self.list_for_start().await;
        if let Some(why) = self.skewed(&second) {
            return Some(why);
        }
        let rewritten: Vec<&str> = second
            .rewritten(first)
            .into_iter()
            .filter(|key| *key != own)
            .collect();
        (!rewritten.is_empty()).then(|| {
            format!(
                "a node of a WasmKeep version this one cannot read runs in this cluster (`{}` \
                 holds something this version does not read, written while this node started): \
                 this version ({}) runs only beside the one before and the one after",
                rewritten.join("`, `"),
                self.own_minor
                    .as_ref()
                    .map_or_else(String::clone, ToString::to_string)
            )
        })
    }

    /// Waits while any heartbeat in a format from before node leases
    /// (headers 1 and 2) is recent: its node may still hold per-actor
    /// leases, which it renews on a task of its own, so only one older than
    /// its horizon plus a lease's ttl and grace proves it stopped (ADR
    /// 0020, section 6). One whose `written_at` is ahead of this clock lapses
    /// that long plus the allowed skew after this node first read it (its
    /// clock cannot hold the cluster forever), with a warning. One that is
    /// rewritten meanwhile means an old version runs: `Err` with why, and
    /// the node does not start. Listed every half period; a failed listing
    /// proves nothing and is listed again.
    pub(super) async fn start_guard(&self) -> Result<(), String> {
        let period = self.heartbeat_every;
        let wait = self
            .heartbeat_horizon_ms
            .saturating_add(u64::try_from(self.alarm_overdue.as_millis()).unwrap_or(u64::MAX));
        let ahead_wait =
            Duration::from_millis(wait.saturating_add(self.lease_config.max_skew_ms()));
        // Per old heartbeat: its `written_at`, when first seen ahead.
        let mut seen: HashMap<String, (u64, Option<tokio::time::Instant>)> = HashMap::new();
        let mut logged = false;
        loop {
            let nodes = match self.membership.heartbeats().await {
                Ok(nodes) => nodes,
                Err(e) => {
                    tracing::warn!(error = %e, "listing the heartbeats failed; retrying before starting");
                    tokio::time::sleep(period / 2).await;
                    continue;
                }
            };
            let now = now_ms();
            let mut left = Duration::ZERO;
            for n in nodes.iter().filter(|n| n.lease.is_none()) {
                let first = seen.get(&n.id).map(|(at, _)| *at);
                if first.is_some_and(|at| at != n.written_at_ms) {
                    return Err(format!(
                        "node {} runs a WasmKeep version from before node leases (its heartbeat \
                         `nodes/{}` in the old format was rewritten while this node started): \
                         stop every node of the older version before starting this one (ADR 0020)",
                        n.id, n.id
                    ));
                }
                let entry = seen.entry(n.id.clone()).or_insert((n.written_at_ms, None));
                let this_left = if n.written_at_ms > now {
                    let since = *entry.1.get_or_insert_with(|| {
                        tracing::warn!(object = %format!("nodes/{}", n.id), "an old-format heartbeat is dated ahead of this clock; it lapses on this node's own count");
                        tokio::time::Instant::now()
                    });
                    ahead_wait.saturating_sub(since.elapsed())
                } else {
                    Duration::from_millis(n.written_at_ms.saturating_add(wait).saturating_sub(now))
                };
                left = left.max(this_left);
            }
            if left.is_zero() {
                return Ok(());
            }
            if !logged {
                tracing::info!(
                    wait_ms = u64::try_from(left.as_millis()).unwrap_or(u64::MAX),
                    "a heartbeat in the old format is recent: waiting until its node's leases lapsed (ADR 0020)"
                );
                logged = true;
            }
            tokio::time::sleep(period / 2).await;
        }
    }

    /// [`Self::check_identity_once`], begun again whenever a released
    /// lease it was watching is gone, or it meets the mark of a `wasmkeep
    /// cluster forget` under this node id (ADR 0043):
    /// that command replaced this id's released lease with the mark and
    /// deletes it next, without a condition, so a lease written over the
    /// mark would be deleted with it. This node never writes over a mark.
    /// It reads the key every half period until the mark is gone, then
    /// runs the whole check again, its full wait included, and so creates
    /// its lease over no object. A mark still there four times a lease's ttl
    /// plus grace after it was first read (60 s at the defaults; the
    /// command's delete follows its write at once) is one its command did
    /// not remove: the node does not start, and says to run the command
    /// again.
    pub(super) async fn check_identity(
        &self,
    ) -> (Identity, Option<Version>, Option<(String, u64)>) {
        // Whether this start met a removal: a mark under its id, or the
        // released lease it watched gone. It then watches the absent key
        // longer before it creates its lease ([`Cluster::after_removal_wait`]).
        let mut met_removal = false;
        loop {
            match self.forgotten_gone().await {
                Err(verdict) => return (verdict, None, None),
                Ok(waited) => met_removal |= waited,
            }
            match self.check_identity_once(met_removal).await {
                (Identity::Checking, _, _) => met_removal = true,
                checked => return checked,
            }
        }
    }

    /// Waits while this node id's key holds the mark of a `cluster forget`
    /// ([`wasmkeep_cluster::node_lease::forgotten`]), reading it every
    /// half period. `Err`: it is still there after four times a lease's ttl
    /// plus grace, with the verdict that stops the start. A read that
    /// fails proves nothing and is made again. `Ok(true)`: a mark was read.
    async fn forgotten_gone(&self) -> Result<bool, Identity> {
        let period = self.heartbeat_every;
        let key = self.node_key();
        let bound = self.alarm_overdue * 4;
        let mut until = None;
        loop {
            let marked = match self.control_keys.get(&key).await {
                Ok(found) => found.is_some_and(|(body, _)| {
                    wasmkeep_cluster::node_lease::forgotten(&body).is_some()
                }),
                Err(e) => {
                    tracing::warn!(error = %e, "reading this node id's heartbeat failed; retrying before starting");
                    tokio::time::sleep(period / 2).await;
                    continue;
                }
            };
            if !marked {
                return Ok(until.is_some());
            }
            let until = *until.get_or_insert_with(|| {
                tracing::info!("{}", forgotten_wait_message(&self.node_id, bound));
                tokio::time::Instant::now() + bound
            });
            if tokio::time::Instant::now() >= until {
                return Err(Stopped::Forgotten.identity(&format!(
                    "`{key}` holds the mark of a `wasmkeep cluster forget` that did not finish, \
                     and this node never writes over it: run `wasmkeep cluster forget {}` \
                     again, then start this node",
                    self.node_id
                )));
            }
            tokio::time::sleep(period / 2).await;
        }
    }

    /// Whether another process runs with this node id, checked before this
    /// one writes its first node lease; with the version of the object to
    /// condition that write on (`None`: there is none, create it) and, if
    /// that object is a node lease naming an instance, its instance and
    /// `written_at` (the predecessor that write replaces).
    /// [`Identity::Checking`]: a read found the mark of a `cluster forget`,
    /// or the released lease read before is gone, and nothing was judged
    /// ([`Self::check_identity`] begins again).
    ///
    /// The object is read (a versioned GET) every half period for at least
    /// a lease's ttl plus grace from the first read in every case, even
    /// when there is none (ADR 0020, section 1): a process whose object was
    /// deleted while it still ran is given time to show itself. A running
    /// process rewrites it: taken. A `fenced` or `released` object, or such
    /// a write over the process first read, is not another process: that
    /// one lapsed once the object's kept `written_at` is ttl plus grace old
    /// on this clock, and the check goes on then. A heartbeat in a format
    /// from before node leases, or one this version cannot read, keeps the
    /// age rule: watched until it is older than its horizon plus ttl and
    /// grace, since its process may renew per-actor leases on a task of its
    /// own. Unchanged through a successful read at the end: the predecessor
    /// stopped, and this one goes on. Failed reads are retried and never
    /// count as unchanged; nothing is claimed meanwhile.
    async fn check_identity_once(
        &self,
        met_removal: bool,
    ) -> (Identity, Option<Version>, Option<(String, u64)>) {
        let period = self.heartbeat_every;
        let key = self.node_key();
        // The mark of a `cluster forget` is never judged, and never the
        // object a first write is conditioned on.
        let forgotten = |found: &Option<(Vec<u8>, Version)>| {
            found
                .as_ref()
                .is_some_and(|(body, _)| wasmkeep_cluster::node_lease::forgotten(body).is_some())
        };
        let read = async || loop {
            match self.control_keys.get(&key).await {
                Ok(found) => return found,
                Err(e) => {
                    tracing::warn!(error = %e, "reading this node id's heartbeat failed; retrying before starting");
                    tokio::time::sleep(period / 2).await;
                }
            }
        };
        let decode = |body: &[u8]| NodeInfo::decode(body).filter(|i| i.id == self.node_id);
        let ttl_grace = u64::try_from(self.alarm_overdue.as_millis()).unwrap_or(u64::MAX);
        // When a process whose last live write was at `written_at` lapsed
        // for sure, as a moment on this node's clock: `written_at + ttl +
        // grace` (now, if that passed).
        let lapsed_at = |written_at: u64| {
            tokio::time::Instant::now()
                + Duration::from_millis(
                    written_at
                        .saturating_add(ttl_grace)
                        .saturating_sub(now_ms()),
                )
        };
        // A moment on this node's clock, as an instant (now, if passed).
        let at_ms = |ms: u64| {
            tokio::time::Instant::now() + Duration::from_millis(ms.saturating_sub(now_ms()))
        };
        let window = wasmkeep_cluster::node_lease::fence_window_ms(&self.lease_config);
        let mut last = read().await;
        if forgotten(&last) {
            return (Identity::Checking, None, None);
        }
        // The lease's ttl plus grace in every case, also with no object: a
        // process whose object was deleted while it ran is given time to
        // show itself.
        let mut deadline = tokio::time::Instant::now() + self.alarm_overdue;
        // After a removal, over no object: the client's whole request
        // budget more, so that removal's delete, if the network holds it,
        // lands before this lease is created (ADR 0020, amendment of
        // 2026-11-27).
        if met_removal && last.is_none() {
            deadline = tokio::time::Instant::now() + self.after_removal_wait;
        }
        // The process the first read names, if live.
        let mut first_instance = None;
        if let Some((body, _)) = &last {
            // A fence an operator created over a deleted object (no
            // instance, no payload): the first write lands no earlier than
            // its `written_at` plus twice the skew on this clock, so this
            // process's bodies are dated after it as `older_than` orders
            // them (ADR 0020, amendment on lease reads).
            if let Some(o) =
                NodeLeaseObject::decode(body).filter(wasmkeep_cluster::node_lease::created_fence)
            {
                deadline = deadline.max(at_ms(o.written_at_ms.saturating_add(window)));
            }
            match decode(body) {
                // A node lease (ADR 0020): its process acts only until its
                // last write's `written_at + ttl - safety`, and any write of
                // its own changes the object, so watching it unchanged for
                // ttl plus grace from the first read is enough.
                Some(i) if i.lease == Some(NodeState::Live) => {
                    first_instance.clone_from(&i.instance);
                }
                // Fenced or released: no write of that process can land any
                // more; it lapsed once its kept `written_at` is that old.
                Some(i) if i.lease.is_some() => deadline = lapsed_at(i.written_at_ms),
                // An old format: its process may still renew per-actor
                // leases on a task of its own, so only a heartbeat older than
                // its horizon plus ttl and grace proves they lapsed.
                Some(i) => {
                    first_instance.clone_from(&i.instance);
                    let old_enough = i
                        .written_at_ms
                        .saturating_add(self.heartbeat_horizon_ms)
                        .saturating_add(ttl_grace);
                    deadline = deadline.max(
                        tokio::time::Instant::now()
                            + Duration::from_millis(old_enough.saturating_sub(now_ms())),
                    );
                }
                // A format this version cannot read: watched for the whole
                // wait (at least until it would have stopped being live, had
                // it been one).
                None => {
                    deadline = deadline.max(
                        tokio::time::Instant::now()
                            + Duration::from_millis(self.heartbeat_horizon_ms),
                    );
                }
            }
        }
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
        tracing::info!(
            wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            "{}",
            checking_message(&self.node_id, wait)
        );
        loop {
            tokio::time::sleep(period / 2).await;
            let now = read().await;
            if forgotten(&now) {
                return (Identity::Checking, None, None);
            }
            // A released node lease that is gone: `cluster forget` removed
            // it (its mark is there for less than the time between two
            // reads), or an operator deleted it. Its process stopped
            // cleanly and nothing writes under this id, so this is no
            // other writer: the check runs again from its first read,
            // over no object, with its full wait. Only for a released
            // lease: a live or fenced one, or a heartbeat from before
            // node leases, that goes absent keeps the deleted verdict.
            if now.is_none()
                && last
                    .as_ref()
                    .and_then(|(body, _)| NodeLeaseObject::decode(body))
                    .is_some_and(|o| o.state == NodeState::Released)
            {
                tracing::info!(
                    "node id {}'s released lease was removed during the start check; checking again",
                    self.node_id
                );
                return (Identity::Checking, None, None);
            }
            let changed = now.as_ref().map(|(body, _)| body) != last.as_ref().map(|(body, _)| body);
            if changed {
                // A fence or a release over the process first read: that
                // process stopped writing, not another one started.
                // Over no object: an operator's fence (`cluster fence`
                // writes one with no instance) ends nothing more than the
                // absence did, and is not a twin either.
                let ended = now
                    .as_ref()
                    .and_then(|(body, _)| NodeLeaseObject::decode(body))
                    .filter(|o| {
                        o.state != NodeState::Live
                            && ((first_instance.is_some() && o.instance == first_instance)
                                || (last.is_none() && o.instance.is_none()))
                    });
                if let Some(o) = ended {
                    // It lapsed once its kept `written_at` is ttl plus grace
                    // old: the check goes on then, without the rest of its
                    // wait (ADR 0020, section 1). Any later change is
                    // another writer.
                    deadline = deadline.min(lapsed_at(o.written_at_ms));
                    // An operator's fence over no object: the first write
                    // lands no earlier than its `written_at` plus twice the
                    // skew, as for one read first.
                    if o.instance.is_none() {
                        let fenced = at_ms(o.written_at_ms.saturating_add(window));
                        // Logged only when the fence is what holds the
                        // first write back.
                        if fenced > deadline {
                            deadline = fenced;
                            tracing::info!(
                                "{}",
                                fence_wait_message(
                                    &self.node_id,
                                    "was written during the start check",
                                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                                )
                            );
                        }
                    }
                    first_instance = None;
                } else {
                    // Classified as a lease write's re-read classifies it.
                    let body = now.as_ref().map(|(body, _)| body.as_slice());
                    let verdict = Stopped::of(wasmkeep_cluster::node_lease::LostCause::of(body));
                    let by = match body {
                        Some(body) => match decode(body) {
                            Some(i) => format!("advertised at {}", i.address),
                            None => {
                                "in an object this version cannot read as a heartbeat".to_owned()
                            }
                        },
                        None => "the object was removed meanwhile".to_owned(),
                    };
                    return (
                        verdict.identity(&format!(
                            "node {}'s heartbeat was changed by another writer ({by}) while this \
                             one started: stop any other process with the same --node-id, or \
                             give this node an id of its own",
                            self.node_id
                        )),
                        None,
                        None,
                    );
                }
            }
            last = now;
            // Over a created fence, on the clock that dates the first write
            // (the wait above runs on the monotonic one).
            if tokio::time::Instant::now() >= deadline
                && let Some((body, _)) = &last
                && let Some(o) = NodeLeaseObject::decode(body)
                    .filter(wasmkeep_cluster::node_lease::created_fence)
                && now_ms() < o.written_at_ms.saturating_add(window)
            {
                deadline = at_ms(o.written_at_ms.saturating_add(window));
                tracing::info!(
                    "{}",
                    fence_wait_message(
                        &self.node_id,
                        "is dated ahead of this clock",
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    )
                );
                continue;
            }
            if tokio::time::Instant::now() >= deadline {
                // Format 3 only: a process of an older version writes
                // without conditions, so a copy of its body proves nothing.
                let replaced = last
                    .as_ref()
                    .and_then(|(body, _)| decode(body))
                    .filter(|i| i.lease.is_some())
                    .and_then(|i| Some((i.instance?, i.written_at_ms)));
                return (Identity::Unique, last.map(|(_, version)| version), replaced);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One line when the start check begins, with the node id and the
    /// wait in seconds, and one once the node is ready.
    #[test]
    fn the_start_check_says_how_long_and_when_it_is_done() {
        assert_eq!(
            checking_message("a", Duration::from_millis(14_999)),
            "checking that no other process runs node id a; ready in about 15 s"
        );
        assert_eq!(
            checking_message("b", Duration::from_millis(3_200)),
            "checking that no other process runs node id b; ready in about 4 s"
        );
        assert_eq!(
            checking_message("c", Duration::ZERO),
            "checking that no other process runs node id c; ready in about 0 s"
        );
        assert_eq!(
            checked_message("a"),
            "no other process runs node id a: ready to take actors"
        );
    }
}
