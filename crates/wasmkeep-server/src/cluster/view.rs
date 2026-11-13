//! The heartbeat loop and this node's view of the cluster: live peers,
//! drain state, the operator's intents, and apps sharing a prefix.

use wasmkeep_cluster::node_lease::{Fence, NodeState, fence};

use super::*;

/// A node the listing found due for a fence.
#[derive(Debug, Clone)]
pub(super) struct FenceDue {
    /// Its id.
    node: String,
    /// The instance and `written_at` it was watched with.
    instance: String,
    written_at_ms: u64,
    /// This node's lease epoch when it was found due: a lapse since (the
    /// epoch moved) restarts the watch's wait, and its fence is dropped.
    epoch: u64,
}

/// Fences a fence task tries at once.
const FENCE_CONCURRENCY: usize = 8;

/// Fences the nodes the listing finds due, on a task of its own, so a slow
/// bucket during a fence never holds up the listing (review finding B3).
pub(super) async fn fence_loop(
    cluster: std::sync::Weak<Cluster>,
    mut due: tokio::sync::mpsc::UnboundedReceiver<Vec<FenceDue>>,
) {
    while let Some(first) = due.recv().await {
        // Batches that queued while a fence was slow, merged: one fence per
        // node, as last found due.
        let mut merged: HashMap<String, FenceDue> = HashMap::new();
        let mut take = |batch: Vec<FenceDue>| {
            for d in batch {
                merged.insert(d.node.clone(), d);
            }
        };
        take(first);
        while let Ok(batch) = due.try_recv() {
            take(batch);
        }
        let Some(c) = cluster.upgrade() else {
            return;
        };
        if c.identity_taken() {
            return;
        }
        c.fence_due(merged.into_values().collect()).await;
    }
}

/// Refreshes the peer list, loads the cluster secret and reads `control/`
/// every heartbeat period, once the node lease's first write landed (the
/// node lease itself is written by its own task, `node_lease_loop`). With
/// TLS, an unknown pin (a peer that just started, or one that changed its
/// key) makes it list the heartbeats sooner, at most once a period:
/// strangers knocking on the cluster port can at most double this node's
/// listings.
pub(super) async fn heartbeat_loop(cluster: std::sync::Weak<Cluster>, every: Duration) {
    // Held apart from the cluster, so waiting does not keep it alive.
    let tls = cluster.upgrade().and_then(|c| c.tls.clone());
    // Nothing until the start check passed and the first lease write
    // landed; never if another process runs with this id.
    {
        let Some(c) = cluster.upgrade() else {
            return;
        };
        let mut seen = c.identity.subscribe();
        drop(c);
        let unique = seen
            .wait_for(|s| *s != Identity::Checking)
            .await
            .is_ok_and(|s| *s == Identity::Unique);
        if !unique {
            return;
        }
    }
    loop {
        // From the top: the period does not stretch by the bucket's latency.
        let next = tokio::time::Instant::now() + every;
        let Some(c) = cluster.upgrade() else {
            return;
        };
        // Another process found running as this node: no more listings.
        if c.identity_taken() {
            return;
        }
        // Until loaded, calls from other nodes are refused; retried here.
        if c.secret.get().is_none() {
            c.load_secret().await;
        }
        c.refresh_view().await;
        if c.identity_taken() {
            return;
        }
        c.refresh_control().await;
        drop(c);
        let unknown = async {
            match &tls {
                Some(t) => t.pins().unknown_seen().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(next) => {}
            () = unknown => {
                let Some(c) = cluster.upgrade() else {
                    return;
                };
                c.refresh_view().await;
                drop(c);
                // No other early listing before the next heartbeat.
                tokio::time::sleep_until(next).await;
            }
        }
    }
}

impl Cluster {
    /// Reads the operator's intents (drains under `control/drain/`, this
    /// app's moves under its root) and publishes whether
    /// this node is asked to drain. A failed listing keeps the last intents
    /// read (ADR 0014).
    pub(super) async fn refresh_control(&self) {
        match control::read(&self.control_keys, Some(&self.root), now_ms()).await {
            Ok(read) => {
                let asked = read.draining.contains(&self.node_id);
                let was = std::mem::replace(
                    &mut *self.control.write().unwrap_or_else(|p| p.into_inner()),
                    read,
                )
                .draining
                .contains(&self.node_id);
                if asked != was {
                    tracing::info!(draining = asked, "drain intent changed (ADR 0014)");
                }
                // A node that cannot open the secrets drains too (ADR
                // 0016); `set_secrets_blocked` logs that change.
                let draining = asked || self.secrets_blocked();
                // Draining, drained and stuck are set by the hand-over;
                // here only whether the node is asked to drain at all.
                let state = self.membership.drain_state();
                match (draining, state) {
                    (true, DrainState::Serving) => {
                        self.membership.set_drain_state(DrainState::Draining);
                    }
                    (false, s) if s != DrainState::Serving => {
                        self.membership.set_drain_state(DrainState::Serving);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "reading control/ failed; keeping the last intents")
            }
        }
    }

    /// How often heartbeats are written (and `control/` read).
    pub(crate) fn heartbeat_period(&self) -> Duration {
        self.heartbeat_every
    }

    /// What this node's next heartbeats say on line 9 (ADR 0014), if it
    /// is still asked to drain (an undrain read meanwhile wins).
    pub(crate) fn set_drain_state(&self, state: DrainState) {
        if !self.draining() {
            return;
        }
        if self.membership.drain_state() != state {
            tracing::info!(state = state.as_str(), "drain state");
        }
        self.membership.set_drain_state(state);
    }

    /// Whether the operator asked this node to drain, as last read.
    /// A node that cannot open the app's secrets drains too (ADR 0016):
    /// it hands its actors over and takes none.
    pub(crate) fn draining(&self) -> bool {
        self.secrets_blocked()
            || self
                .control
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .draining
                .contains(&self.node_id)
    }

    /// This node cannot open the app's current secrets (ADR 0016): it
    /// drains, and refuses events rather than claim when no peer can take
    /// them.
    pub(crate) fn secrets_blocked(&self) -> bool {
        self.secrets_blocked
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Sets [`Self::secrets_blocked`].
    pub(crate) fn set_secrets_blocked(&self, blocked: bool) {
        self.secrets_blocked
            .store(blocked, std::sync::atomic::Ordering::Release);
    }

    /// The node to send `address`'s actor to instead of claiming it, while
    /// this node drains (ADR 0014): among the live peers that serve the
    /// same app, report `serving` on heartbeat line 9 (so know draining),
    /// are not asked to drain, and can be reached the way node calls reach
    /// them, the rendezvous choice for the actor. `None`: no eligible peer.
    pub(crate) fn chosen_peer(&self, address: &ActorAddress) -> Option<String> {
        let eligible = self.eligible_peers();
        rendezvous(
            &encode_address(address),
            eligible.iter().map(String::as_str),
        )
        .map(str::to_owned)
    }

    /// Whether any peer could take an actor now (eligibility does not
    /// depend on the actor).
    pub(crate) fn has_eligible_peer(&self) -> bool {
        !self.eligible_peers().is_empty()
    }

    /// The peers that may receive this node's actors (see `chosen_peer`).
    pub(super) fn eligible_peers(&self) -> Vec<String> {
        let draining = self
            .control
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .draining
            .clone();
        let now = now_ms();
        let suspects: Vec<String> = {
            let mut s = self.suspects.lock().unwrap_or_else(|p| p.into_inner());
            s.retain(|_, until| *until > now);
            s.keys().cloned().collect()
        };
        let eligible: Vec<String> = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(id, peer)| {
                **id != self.node_id
                    && peer.app == self.serves
                    && peer.drain == Some(DrainState::Serving)
                    && !draining.contains(*id)
                    && !suspects.contains(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        eligible
            .into_iter()
            .filter(|id| self.address_of(id).is_some())
            .collect()
    }

    /// A chosen peer failed to take an event: skip it for a heartbeat
    /// horizon, by which a dead node has left the peer list and a draining
    /// one shows it on line 9 (ADR 0014). The event that failed got its
    /// error; the next ones go to another peer, or are claimed here.
    pub(crate) fn suspect(&self, peer: &str) {
        let until = now_ms().saturating_add(self.heartbeat_horizon_ms);
        tracing::info!(%peer, "chosen peer did not take an event; not choosing it for a while");
        self.suspects
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(peer.to_owned(), until);
    }

    /// Lists the live nodes: where to reach each (the peer list) and, with
    /// TLS, the pins this node trusts.
    pub(super) async fn refresh_view(&self) {
        // Timed (`wasmkeep_listing_seconds`): past the heartbeat horizon,
        // peers' pins would expire before this renews them.
        let started = std::time::Instant::now();
        self.list_and_refresh().await;
        crate::metrics::METRICS
            .listing_seconds
            .observe(started.elapsed());
    }

    async fn list_and_refresh(&self) {
        match self.membership.heartbeats().await {
            Ok(all) => {
                // Against every heartbeat, live or not: a twin whose clock
                // lags may look stale.
                self.check_twin(&all);
                self.note_old_versions(&all);
                self.note_future_leases(&all);
                let nodes = self.membership.live(all.clone());
                let mixed = older_nodes(&self.node_id, &nodes);
                let now = now_ms();
                let period = u64::try_from(self.heartbeat_every.as_millis()).unwrap_or(u64::MAX);
                let logged = self
                    .conflict_logged_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                if !mixed.is_empty() && now.saturating_sub(logged) >= period {
                    self.conflict_logged_ms
                        .store(now, std::sync::atomic::Ordering::Relaxed);
                    tracing::error!(serves = %self.serves, "{}", older_nodes_advice(&mixed));
                }
                if let Some(tls) = &self.tls
                    && tls.refresh(&nodes, self.heartbeat_horizon_ms)
                {
                    // An address changed hands (or its node left): drop every
                    // pooled connection, so none to a previous holder is used.
                    *self.clients.write().unwrap_or_else(|p| p.into_inner()) =
                        Clients::new(&self.connector);
                }
                if self.refuse_plaintext {
                    self.note_plain_peers(&nodes, now);
                }
                // Plain `http://` bases and `https://` cluster listeners
                // only: calls between nodes carry signatures, and the
                // forwarding clients speak HTTP, over TLS or not.
                let peers = nodes
                    .iter()
                    .filter_map(|n| Some((n.id.clone(), Peer::of(n)?)))
                    .collect();
                *self.peers.write().unwrap_or_else(|p| p.into_inner()) = peers;
                // The same bodies, for the requests kept until a dead
                // node's actors are free: no bucket request.
                self.failover.observe(&all, now);
                // Last, and handed to the fence task: a fence awaits the
                // bucket, and neither this view nor the next listing may
                // wait for it.
                let due = self.fences_due(&all);
                if !due.is_empty() {
                    let _ = self.fences_due.send(due);
                }
            }
            Err(e) => tracing::warn!(error = %e, "listing nodes failed"),
        }
    }
}

impl Cluster {
    /// Watches the other nodes' leases in the listing's bodies and fences
    /// each one that kept the same instance and `written_at` for a lease
    /// period plus an attempt timeout, counted also from this node's own
    /// lease being valid again after a lapse (ADR 0020, section 2). The
    /// fence itself is decided on one fresh read, conditional on it, and
    /// only once that node's `written_at + ttl + grace` has passed here:
    /// a node that is merely slow renews in time and is never fenced.
    /// Only while this node's own lease is valid. Old formats are never
    /// fenced (an old binary would not see it).
    #[cfg(test)]
    pub(super) async fn fence_stale_nodes(&self, all: &[NodeInfo]) {
        let due = self.fences_due(all);
        self.fence_due(due).await;
    }

    /// The listing's side of [`Self::fence_stale_nodes`]: watches every
    /// node's lease in the listing and returns those due for a fence. No
    /// bucket request.
    pub(super) fn fences_due(&self, all: &[NodeInfo]) -> Vec<FenceDue> {
        let now = now_ms();
        // One read: the validity the watch counts from and the epoch each
        // fence is checked against are the same.
        let validity = self.validity.get();
        {
            let mut watch = self.fence_watch.lock().unwrap_or_else(|p| p.into_inner());
            watch.own_lease(validity.until_ms, now);
            watch.retain(|node| all.iter().any(|n| n.id == node));
            let mut due = Vec::new();
            for n in all.iter().filter(|n| n.id != self.node_id) {
                match (&n.lease, &n.instance) {
                    (Some(NodeState::Live), Some(instance)) => {
                        if watch.observe(&n.id, instance, n.written_at_ms, now) {
                            due.push(FenceDue {
                                node: n.id.clone(),
                                instance: instance.clone(),
                                written_at_ms: n.written_at_ms,
                                epoch: validity.epoch,
                            });
                        }
                    }
                    _ => watch.forget(&n.id),
                }
            }
            due
        }
    }

    /// The fence task's side: fences the nodes found due, up to
    /// [`FENCE_CONCURRENCY`] at once. Each is asked right before it starts:
    /// a twin found since, a lapse since it was found due (the epoch moved:
    /// the watch's wait restarts, and the next listings bring it back), or
    /// too little validity left for the whole fence (its read and its
    /// write, bounded by one attempt) to be sent while this node's lease is
    /// valid.
    pub(super) async fn fence_due(&self, due: Vec<FenceDue>) {
        use futures_util::StreamExt as _;
        let attempt = u64::try_from(self.lease_attempt.as_millis()).unwrap_or(u64::MAX);
        futures_util::stream::iter(due)
            .for_each_concurrent(FENCE_CONCURRENCY, |due| async move {
                let now = now_ms();
                let room = self
                    .validity
                    .until()
                    .is_some_and(|u| u.saturating_sub(now) >= attempt);
                if self.identity_taken() || self.validity.epoch() != due.epoch || !room {
                    return;
                }
                self.fence_one(due).await;
            })
            .await;
    }

    /// One fence, decided on a fresh read and conditional on it.
    async fn fence_one(&self, due: FenceDue) {
        let FenceDue {
            node,
            instance,
            written_at_ms: written_at,
            ..
        } = due;
        let key = format!("{NODES_PREFIX}{node}");
        let outcome = tokio::time::timeout(
            self.lease_attempt,
            fence(
                &self.control_keys,
                &SystemClock,
                &self.lease_config,
                &key,
                &instance,
                written_at,
            ),
        )
        .await
        .unwrap_or(Fence::Unavailable);
        match outcome {
            Fence::Fenced => {
                crate::metrics::inc(&crate::metrics::METRICS.node_fences);
                tracing::warn!(
                    node = %node,
                    written_at_ms = written_at,
                    "fenced a node that stopped renewing its lease (ADR 0020)"
                );
                self.failover.nudge(&node);
            }
            // Fenced or released by another: requests kept for its actors
            // need not wait for the next listing to ask.
            Fence::NotLive => self.failover.nudge(&node),
            Fence::Alive => {}
            Fence::Unavailable => {
                tracing::warn!(node = %node, "fencing a stale node failed; watching it again");
            }
        }
    }

    /// Logs, at most once a minute, other nodes' live leases dated ahead
    /// of this clock by more than the allowed skew: they never go stale
    /// here, so no node fences them and the actors they hold stay held
    /// until that date passes. The fix is those nodes' clocks; `cluster
    /// status` shows them too.
    pub(super) fn note_future_leases(&self, all: &[NodeInfo]) {
        let now = now_ms();
        let skew = self.lease_config.max_skew_ms();
        let hold = self
            .lease_config
            .ttl_ms()
            .saturating_add(self.lease_config.grace_ms());
        let others = || all.iter().filter(|n| n.id != self.node_id);
        let ahead: Vec<String> = others()
            .filter(|n| n.lease == Some(NodeState::Live))
            .filter(|n| n.written_at_ms > now.saturating_add(skew))
            .map(|n| format!("{} ({} s)", n.id, (n.written_at_ms - now) / 1_000))
            .collect();
        // Fenced or released, its kept date still ahead: its actors stay
        // held until that date plus ttl and grace, live or not.
        let held: Vec<String> = others()
            .filter(|n| matches!(n.lease, Some(NodeState::Fenced | NodeState::Released)))
            .filter(|n| n.written_at_ms > now.saturating_add(skew))
            .map(|n| {
                let free = n.written_at_ms.saturating_add(hold).saturating_sub(now);
                format!("{} (its actors free in {} s)", n.id, free / 1_000)
            })
            .collect();
        if (ahead.is_empty() && held.is_empty())
            || !self.refusal_log_due("node lease dated ahead", now)
        {
            return;
        }
        tracing::error!(
            live = %ahead.join(", "),
            stopped = %held.join(", "),
            "node leases dated ahead of this node's clock by more than the allowed skew: a live \
             one never goes stale, so it is never fenced, and a fenced or released one keeps its \
             actors until that date; fix the clock that is wrong (theirs, or this node's if it \
             is behind; `wasmkeep cluster status` from another machine tells)"
        );
    }

    /// Logs, at most once a minute, a heartbeat in a format from before
    /// node leases that shows an old version running alongside this one,
    /// which ADR 0020 forbids (it frees nothing, and its per-actor leases
    /// are refused here): rewritten since the previous listing, or recent by
    /// the start guard's test (one dated ahead of this clock lapses on this
    /// node's own count from when it was first seen). A stale one is a
    /// retired node's and is left to `cluster status`.
    pub(super) fn note_old_versions(&self, all: &[NodeInfo]) {
        let now = now_ms();
        let wait = self
            .heartbeat_horizon_ms
            .saturating_add(u64::try_from(self.alarm_overdue.as_millis()).unwrap_or(u64::MAX));
        let ahead_wait =
            Duration::from_millis(wait.saturating_add(self.lease_config.max_skew_ms()));
        let mut seen = self
            .old_heartbeats
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        seen.retain(|id, _| all.iter().any(|n| n.lease.is_none() && n.id == *id));
        let mut running = Vec::new();
        for n in all.iter().filter(|n| n.lease.is_none()) {
            let first = seen
                .entry(n.id.clone())
                .or_insert((n.written_at_ms, tokio::time::Instant::now()));
            let rewritten = first.0 != n.written_at_ms;
            if rewritten {
                *first = (n.written_at_ms, tokio::time::Instant::now());
            }
            let recent = if n.written_at_ms > now {
                first.1.elapsed() < ahead_wait
            } else {
                n.written_at_ms.saturating_add(wait) > now
            };
            if rewritten || recent {
                running.push(n.id.as_str());
            }
        }
        drop(seen);
        if running.is_empty() || !self.refusal_log_due("old version running", now) {
            return;
        }
        tracing::error!(
            nodes = %running.join(", "),
            "a node of a WasmKeep version from before node leases runs on this bucket: stop it (ADR 0020)"
        );
    }

    /// In refuse mode, logs the live peers without a cluster listener (at
    /// most once a minute): they are not used, so requests and calls for
    /// their actors get a 503 here.
    pub(super) fn note_plain_peers(&self, nodes: &[NodeInfo], now: u64) {
        let plain: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id != self.node_id && n.cluster_address.is_none())
            .map(|n| n.id.as_str())
            .collect();
        let logged = self
            .plain_peers_logged_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if !plain.is_empty() && now.saturating_sub(logged) >= 60_000 {
            self.plain_peers_logged_ms
                .store(now, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                nodes = %plain.join(", "),
                "peers without a cluster listener are not used (--cluster-plaintext refuse): their actors are unreachable from this node"
            );
        }
    }
}

/// Live nodes of the version before the implicit app (ADR 0022), which
/// name their tenant alone (`tenant/`): `node (tenant/)` each. They keep
/// actor state in the layout before 0.4.0, so they must not run beside
/// this version: upgrades stop every node. Another app of any tenant is
/// fine (each app's state has its own root), and nodes that predate the
/// app line are left to the start guard of ADR 0020.
pub(crate) fn older_nodes(me: &str, nodes: &[NodeInfo]) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| n.id != me && n.app.ends_with('/'))
        .map(|n| format!("{} ({})", n.id, n.app))
        .collect()
}

/// What an operator reads about `older` ([`older_nodes`]).
pub fn older_nodes_advice(older: &[String]) -> String {
    format!(
        "a node of an earlier WasmKeep version is live on this bucket prefix ({}): stop \
         every node and upgrade them together (docs/cluster.md, Upgrading WasmKeep)",
        older.join(", ")
    )
}

impl Cluster {
    /// [`older_nodes`] from a fresh read of the heartbeats.
    pub(crate) async fn older_nodes(&self) -> Result<Vec<String>, String> {
        let nodes = self
            .membership
            .live_nodes()
            .await
            .map_err(|e| e.to_string())?;
        Ok(older_nodes(&self.node_id, &nodes))
    }
}
