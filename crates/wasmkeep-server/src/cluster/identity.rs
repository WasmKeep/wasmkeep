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
            Ok(Identity::Taken(_, why) | Identity::Refused(why)) => Err(why),
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
            let key = format!("probe/{}-{}-{attempt}", self.node_id, self.nonce_seed);
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

    /// Whether another process runs with this node id, checked before this
    /// one writes its first node lease; with the version of the object to
    /// condition that write on (`None`: there is none, create it).
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
    pub(super) async fn check_identity(&self) -> (Identity, Option<Version>) {
        let period = self.heartbeat_every;
        let key = self.node_key();
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
        let mut last = read().await;
        // The lease's ttl plus grace in every case, also with no object: a
        // process whose object was deleted while it ran is given time to
        // show itself.
        let mut deadline = tokio::time::Instant::now() + self.alarm_overdue;
        // The process the first read names, if live.
        let mut first_instance = None;
        if let Some((body, _)) = &last {
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
                    );
                }
            }
            last = now;
            if tokio::time::Instant::now() >= deadline {
                return (Identity::Unique, last.map(|(_, version)| version));
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
