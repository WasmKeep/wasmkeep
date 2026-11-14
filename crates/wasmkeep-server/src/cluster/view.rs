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

/// The nodes as one refresh saw them: id, instance, lease state, live.
pub(super) type ViewShape = Vec<(String, Option<String>, Option<NodeState>, bool)>;

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

/// Refreshes the peer list, loads the cluster secret and reads the
/// operator's intents every heartbeat period, once the node lease's first
/// write landed (the node lease itself is written by its own task,
/// `node_lease_loop`).
///
/// The peer list (ADR 0020 and 0011, amendments of 2026-11-14): a listing
/// of `nodes/` at the first period and every
/// [`membership::FULL_LISTING_EVERY`] periods; in between, one read of
/// each node the last refresh found, which gives the same bodies. A
/// listing is also made at once when a peer says hello (it just started),
/// when a record names a node this node does not know, or, with TLS, when
/// an unknown pin knocks (a peer that just started, or one that changed
/// its key): at most one such early listing a period, none in a period
/// that listed already, and one asked for after that at the next period.
/// So at most two listings a period, as before the amendment over TLS;
/// strangers can ask only by knocking with a pin (TLS), since a hello is
/// signed.
///
/// After its first refresh, the node says hello to the peers it found, so
/// that they list at once and see it within a period; again the next two
/// periods if a peer did not take it.
///
/// The intents: see [`Cluster::refresh_control_if_changed`].
pub(super) async fn heartbeat_loop(cluster: std::sync::Weak<Cluster>, every: Duration) {
    // Held apart from the cluster, so waiting does not keep it alive.
    let tls = cluster.upgrade().and_then(|c| c.tls.clone());
    let Some(asked) = cluster.upgrade().map(|c| Arc::clone(&c.listing_asked)) else {
        return;
    };
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
    // Periods since the last listing of `nodes/` that succeeded, that
    // listing's period counted as the first.
    let mut since_full: Option<u32> = None;
    let mut watch = control::ControlWatch::new();
    // Rounds of hellos left: the first after the first refresh, and again
    // the next periods while a peer did not take it (lost, or refused
    // because its cluster secret was not loaded yet).
    let mut hellos_left = HELLO_ROUNDS;
    let hello_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
        let wanted = c.take_listing_wanted();
        let full = wasmkeep_cluster::membership::full_listing_due(since_full, wanted);
        let ok = c.refresh_view(full).await;
        since_full = match (full, ok) {
            (true, true) => Some(1),
            // A listing asked for and not made: asked again.
            (true, false) => {
                if wanted {
                    c.want_listing();
                }
                since_full
            }
            (false, _) => since_full.map(|n| n.saturating_add(1)),
        };
        if c.identity_taken() {
            return;
        }
        let retry = hellos_left < HELLO_ROUNDS
            && hello_failed.swap(false, std::sync::atomic::Ordering::AcqRel);
        if (hellos_left == HELLO_ROUNDS || retry)
            && hellos_left > 0
            && ok
            && c.secret.get().is_some()
        {
            hellos_left -= 1;
            c.say_hello(Arc::clone(&hello_failed));
        }
        c.refresh_control_if_changed(&mut watch).await;
        drop(c);
        // An early listing, at most one a period, and none in a period
        // that listed already: an unknown pin (TLS) or a peer's hello. One
        // asked for after that is made at the next period. So at most two
        // listings a period, a scheduled or asked-for one and an early one.
        let mut early_left = !(full && ok);
        let mark = async {
            loop {
                listing_asked(&tls, &asked).await;
                let Some(c) = cluster.upgrade() else {
                    return;
                };
                if early_left {
                    early_left = false;
                    c.take_listing_wanted();
                    if c.refresh_view(true).await {
                        since_full = Some(1);
                    } else {
                        c.want_listing();
                    }
                } else {
                    c.listing_wanted
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        };
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(next) => {}
            () = mark => {}
        }
    }
}

/// Rounds of hellos a node sends at most after it starts.
const HELLO_ROUNDS: u32 = 3;

/// Resolves when a listing is asked for: an unknown pin knocked (TLS), or
/// a hello or a failed early listing asked ([`Cluster::want_listing`]).
async fn listing_asked(tls: &Option<Arc<ClusterTls>>, asked: &tokio::sync::Notify) {
    match tls {
        Some(t) => tokio::select! {
            () = t.pins().unknown_seen() => {}
            () = asked.notified() => {}
        },
        None => asked.notified().await,
    }
}

impl Cluster {
    /// Once a period: reads `control/changed` (one GET) and, when
    /// [`control::ControlWatch`] says so, lists the intents
    /// ([`Self::refresh_control`]): when it changed since the last listing,
    /// and every [`control::LISTING_FALLBACK_EVERY`] periods anyway (ADR
    /// 0014, amendment of 2026-11-14).
    pub(super) async fn refresh_control_if_changed(&self, watch: &mut control::ControlWatch) {
        use control::Keys as _;
        let marker = self.control_keys.get_body(control::CHANGED).await;
        if let Err(e) = &marker {
            tracing::debug!(error = %e, "reading control/changed failed");
        }
        let read = marker.as_ref().map(Option::as_deref).map_err(|_| ());
        if watch.should_list(read) && self.refresh_control().await {
            watch.listed(marker.ok());
        }
    }

    /// Reads the operator's intents (drains under `control/drain/`, this
    /// app's moves under its root) and publishes whether
    /// this node is asked to drain. A failed listing keeps the last intents
    /// read (ADR 0014); false then.
    pub(super) async fn refresh_control(&self) -> bool {
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
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "reading control/ failed; keeping the last intents");
                false
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

    /// Where this node's heartbeats read payload line 10, as each is built.
    pub(crate) fn set_load_source(&self, source: wasmkeep_cluster::membership::LoadSource) {
        self.membership.set_load_source(source);
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

    /// Whether this node runs the idle-expiry passes (ADR 0028, section
    /// 4): its lease valid, not draining, and the lowest id among itself
    /// and the live peers of its app that serve (heartbeat line 9), are not
    /// asked to drain, are not suspected, and announce `ends`. Another node
    /// takes over once this one stops heartbeating.
    pub(crate) fn collects_expiry(&self) -> bool {
        if self.draining() || !self.lease_valid_now() {
            return false;
        }
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
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        let others = peers.iter().filter(|(id, peer)| {
            **id != self.node_id
                && peer.app == self.serves
                && peer.drain == Some(DrainState::Serving)
                && peer.can_end
                && !draining.contains(*id)
                && !suspects.contains(*id)
        });
        wasmkeep_cluster::expiry::collects(&self.node_id, others.map(|(id, _)| id.as_str()))
    }

    /// The ownership records of class `class` (ADR 0028): one listing of
    /// `owners/<class>/`, each with its key after that folder, its
    /// last-modified time and its `ETag`.
    pub(crate) async fn list_owner_records(
        &self,
        class: &str,
    ) -> Result<Vec<wasmkeep_cluster::expiry::Listed>, String> {
        use wasmkeep_cluster::migrate::Store as _;
        let folder = format!("{}/", self.root.key(Family::Owners, class));
        let listed = self
            .control_keys
            .list(&folder)
            .await
            .map_err(|e| e.to_string())?;
        Ok(listed
            .into_iter()
            .filter_map(|m| {
                let name = m.key.strip_prefix(&folder)?;
                (!name.is_empty() && !name.contains('/')).then(|| {
                    wasmkeep_cluster::expiry::Listed {
                        name: name.to_owned(),
                        modified_ms: m.modified_ms,
                        version: m.etag.unwrap_or_default(),
                    }
                })
            })
            .collect())
    }

    /// The actors with an entry in this root's alarm index (ADR 0028): one
    /// listing of `alarms/`, as `(class, name encoded as record keys are)`.
    pub(crate) async fn alarmed_actors(
        &self,
    ) -> Result<std::collections::BTreeSet<(String, String)>, String> {
        use wasmkeep_cluster::migrate::Store as _;
        let folder = format!("{}/", self.root.base(Family::Alarms));
        let listed = self
            .control_keys
            .list(&folder)
            .await
            .map_err(|e| e.to_string())?;
        Ok(listed
            .iter()
            .filter_map(|m| {
                let entry =
                    wasmkeep_cluster::alarm_index::parse_listed(m.key.strip_prefix(&folder)?)?;
                Some((
                    entry.class.to_owned(),
                    wasmkeep_storage::encode_segment(&entry.name),
                ))
            })
            .collect())
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

    /// Reads the nodes' heartbeats, by a listing of `nodes/` (`full`) or
    /// one read of each node the last refresh found, and refreshes from
    /// them where to reach each live node (the peer list) and, with TLS,
    /// the pins this node trusts. False if the bucket did not answer (the
    /// view is kept).
    pub(super) async fn refresh_view(&self, full: bool) -> bool {
        // Timed (`wasmkeep_listing_seconds`): past the heartbeat horizon,
        // peers' pins would expire before this renews them.
        let started = std::time::Instant::now();
        let ok = self.list_and_refresh(full).await;
        crate::metrics::METRICS
            .listing_seconds
            .observe(started.elapsed());
        ok
    }

    async fn list_and_refresh(&self, full: bool) -> bool {
        let read = if full {
            self.membership.heartbeats().await
        } else {
            let mut known = self
                .known_nodes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            // This node's own object too: the twin check, and this node's
            // place in the live set (alarm shards) come from it.
            if !known.contains(&self.node_id) {
                known.push(self.node_id.clone());
            }
            self.membership.heartbeats_of(&known).await
        };
        match read {
            Ok(all) => {
                *self.known_nodes.lock().unwrap_or_else(|p| p.into_inner()) =
                    all.iter().map(|n| n.id.clone()).collect();
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
                self.note_view_shape(&nodes, &all);
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "reading the nodes' heartbeats failed");
                false
            }
        }
    }

    /// Wakes the alarm sweep when the nodes it shares the index with
    /// changed: a node joined, left, went stale, was fenced or released,
    /// or restarted (ADR 0009, amendment of 2026-11-14). Not on the first
    /// refresh: the sweep runs anyway then.
    fn note_view_shape(&self, live: &[NodeInfo], all: &[NodeInfo]) {
        let mut shape: ViewShape = all
            .iter()
            .map(|n| {
                (
                    n.id.clone(),
                    n.instance.clone(),
                    n.lease,
                    live.iter().any(|l| l.id == n.id),
                )
            })
            .collect();
        shape.sort_by(|a, b| a.0.cmp(&b.0));
        let mut last = self.view_shape.lock().unwrap_or_else(|p| p.into_inner());
        let changed = last.as_ref().is_some_and(|l| *l != shape);
        *last = Some(shape);
        drop(last);
        if changed {
            self.view_changed.notify_one();
        }
    }

    /// Asks for a listing of `nodes/` at once (at most one early listing a
    /// period; otherwise at the next period).
    pub(crate) fn want_listing(&self) {
        self.listing_wanted
            .store(true, std::sync::atomic::Ordering::Release);
        self.listing_asked.notify_one();
    }

    /// Whether a listing was asked for, and no longer.
    fn take_listing_wanted(&self) -> bool {
        self.listing_wanted
            .swap(false, std::sync::atomic::Ordering::AcqRel)
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
