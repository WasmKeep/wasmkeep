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

use super::process::Heartbeats;
use wasmkeep_cluster::lease::BucketError;

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

/// Refreshes the peer list and loads the cluster secret every heartbeat
/// period, once the node lease's first write landed (the node lease itself
/// is written by its own task, `node_lease_loop`; the operator's intents
/// are read by theirs, [`control_loop`]; listings of `nodes/` by theirs,
/// [`listing_loop`]).
///
/// The peer list (ADR 0020, amendments of 2026-11-14 and 2026-11-20): a
/// listing of `nodes/` at the first period, within two read attempts;
/// then every period one read of each node that may still write (the
/// nodes as last read, less those whose heartbeat is `released` or
/// `fenced`: their instance never writes again), all at once, each within
/// [`heartbeat_read_attempt`]. A peer whose read fails or times out stays
/// as last read. A peer whose trust would end before the next round could
/// land is read again early, once per heartbeat of it
/// ([`wasmkeep_cluster::membership::early_reads`]). Discovery (new node
/// ids, nodes whose object is gone, and the stopped nodes' objects) is the
/// listing task's: a slow LIST never holds up a round.
///
/// Once its rounds find a peer, the node says hello to the peers it found,
/// so that they read it at once and see it within a period; again the
/// next two periods if a peer did not take it.
pub(super) async fn heartbeat_loop(cluster: std::sync::Weak<Cluster>, every: Duration) {
    if !identity_unique(cluster.clone()).await {
        return;
    }
    // Rounds of hellos left: the first once a round found a peer, and
    // again the next periods while a peer did not take it (lost, or
    // refused because its cluster secret was not loaded yet).
    let mut hellos_left = HELLO_ROUNDS;
    let hello_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // The peers read early, each with the `written_at` it was read early
    // for: at most one early read per heartbeat of a peer.
    let mut read_early: HashMap<String, u64> = HashMap::new();
    let mut first = true;
    loop {
        // From the top: the period does not stretch by the bucket's latency.
        let next = tokio::time::Instant::now() + every;
        let Some(c) = cluster.upgrade() else {
            return;
        };
        // Another process found running as this node: no more reads.
        if c.identity_taken() {
            return;
        }
        // Until loaded, calls from other nodes are refused; retried here,
        // within two read attempts so that a slow bucket does not hold up
        // the view.
        if c.secret.get().is_none() {
            let limit = c.heartbeat_read_attempt * 2;
            let loading = Arc::clone(&c);
            let _ = tokio::time::timeout(limit, async move { loading.load_secret().await }).await;
        }
        let ok = if first {
            first = false;
            let held = c.transport.heard_writers();
            if held.iter().any(|id| *id != c.node_id) {
                // The process already read the nodes (another app, or a
                // runtime restarted in place): they are read by key at
                // once, and the listing task lists. A slow LIST then holds
                // up neither this view nor the early reads.
                c.want_listing();
                c.refresh_first(held).await
            } else {
                // The first period lists, to find the peers; one that
                // fails or runs out leaves it to the listing task, asked
                // at once.
                let listed = c.refresh_view(true).await;
                if !listed {
                    c.want_listing();
                }
                listed || c.refresh_view(false).await
            }
        } else {
            c.refresh_view(false).await
        };
        if c.identity_taken() {
            return;
        }
        let found_peers = c.found_peers();
        let retry = hellos_left < HELLO_ROUNDS
            && hello_failed.swap(false, std::sync::atomic::Ordering::AcqRel);
        if ((hellos_left == HELLO_ROUNDS && found_peers) || retry)
            && hellos_left > 0
            && ok
            && c.secret.get().is_some()
        {
            hellos_left -= 1;
            c.say_hello(Arc::clone(&hello_failed));
        }
        c.forget_early_reads(&mut read_early);
        drop(c);
        loop {
            let Some(c) = cluster.upgrade() else {
                return;
            };
            let due = c.early_reads_due(next, &read_early);
            drop(c);
            let early_at = due.as_ref().map_or(next, |(at, _)| *at);
            // The early read itself is awaited outside the `select!`: the
            // period's end must not cancel it halfway, since it is what
            // renews the trust before that period's round could.
            let early = tokio::select! {
                biased;
                () = tokio::time::sleep_until(next) => None,
                () = tokio::time::sleep_until(early_at), if due.is_some() => due,
            };
            let Some((_, ids)) = early else {
                break;
            };
            let Some(c) = cluster.upgrade() else {
                return;
            };
            c.read_early(&ids, &mut read_early).await;
        }
    }
}

/// Lists `nodes/` on a task of its own, for discovery (ADR 0020,
/// amendment of 2026-11-20): new node ids, the nodes whose object is gone,
/// and the objects of stopped nodes, which the rounds do not read. Every
/// [`wasmkeep_cluster::membership::FULL_LISTING_EVERY`] periods after the first period's
/// listing (made by [`heartbeat_loop`]), and when a listing is asked for
/// (a hello, a record naming a node this node does not know, an unknown
/// pin knocking, a first listing that failed): at once, at most one
/// listing a period, so strangers knocking with a pin cannot make it list
/// more. An ask that arrives during a listing is kept for the next one.
/// Each listing within [`Cluster::listing_limit`]; one that fails or runs
/// out is tried again after one period, then two, four, and so on, at most
/// the scheduled interval. Nothing here is cancelled at a period's end.
pub(super) async fn listing_loop(cluster: std::sync::Weak<Cluster>, every: Duration) {
    // Held apart from the cluster, so waiting does not keep it alive.
    let tls = cluster.upgrade().and_then(|c| c.transport.tls.clone());
    if let Some(t) = &tls {
        // An unlisted sender's hop waits for this task's next listing.
        t.pins().listed_every(every);
    }
    let Some(asked) = cluster.upgrade().map(|c| Arc::clone(&c.listing_asked)) else {
        return;
    };
    if !identity_unique(cluster.clone()).await {
        return;
    }
    let scheduled = every.saturating_mul(wasmkeep_cluster::membership::FULL_LISTING_EVERY);
    let mut next = tokio::time::Instant::now() + scheduled;
    let mut last: Option<tokio::time::Instant> = None;
    let mut failures: u32 = 0;
    loop {
        let wanted = {
            let Some(c) = cluster.upgrade() else {
                return;
            };
            c.listing_wanted.load(std::sync::atomic::Ordering::Acquire)
        };
        let at = if wanted {
            last.map_or_else(tokio::time::Instant::now, |l| l + every)
                .min(next)
        } else {
            next
        };
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(at) => {}
            () = listing_asked(&tls, &asked) => {
                if let Some(c) = cluster.upgrade() {
                    c.listing_wanted.store(true, std::sync::atomic::Ordering::Release);
                }
                continue;
            }
        }
        let Some(c) = cluster.upgrade() else {
            return;
        };
        if c.identity_taken() {
            return;
        }
        c.take_listing_wanted();
        let started = tokio::time::Instant::now();
        let listed = c.list_heartbeats(c.listing_limit()).await;
        drop(c);
        // Counted from when it ended: a listing that hung until its limit
        // is not followed by another at once.
        let ended = tokio::time::Instant::now();
        last = Some(ended);
        if listed {
            failures = 0;
            next = started + scheduled;
        } else {
            failures = failures.saturating_add(1);
            let periods = 2u32
                .saturating_pow(failures.saturating_sub(1))
                .min(wasmkeep_cluster::membership::FULL_LISTING_EVERY);
            next = ended + every.saturating_mul(periods);
        }
    }
}

/// Reads by key, at once and on a task of their own, the heartbeats of
/// nodes a signed call named that this node holds no live heartbeat of (a
/// hello, a hop: a node that just joined), so that it is seen without
/// waiting for a listing (ADR 0020, amendment of 2026-11-20). Each id at
/// most once a period.
pub(super) async fn join_read_loop(
    cluster: std::sync::Weak<Cluster>,
    mut wanted: tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    // Held apart from the cluster, so waiting does not keep it alive.
    let tls = cluster.upgrade().and_then(|c| c.transport.tls.clone());
    if !identity_unique(cluster.clone()).await {
        return;
    }
    // When each id was last read for a signed call (a hello, a hop, this
    // node looking for it), and apart, for a certificate's name: anyone
    // can present a certificate naming a node, and such a read must not
    // take the slot a genuine call of that node would use.
    let mut read_at: HashMap<String, tokio::time::Instant> = HashMap::new();
    let mut name_read_at: HashMap<String, tokio::time::Instant> = HashMap::new();
    loop {
        // Over TLS, a node that just joined is refused at the handshake
        // until its pin is listed: the name on its certificate (a few a
        // period, `PinSet::note_unknown_name`) is read by key too, so it
        // need not wait for the listing. Whether or not it is in the view:
        // a node restarted under its id has a new key, which its object
        // names. A process's name is no node id when it serves several
        // apps: that read finds nothing.
        let mut ids = tokio::select! {
            first = wanted.recv() => match first {
                Some(first) => vec![first],
                None => return,
            },
            () = unknown_names(&tls) => Vec::new(),
        };
        while let Ok(id) = wanted.try_recv() {
            ids.push(id);
        }
        let mut names = tls
            .as_ref()
            .map(|t| t.pins().take_unknown_names())
            .unwrap_or_default();
        let Some(c) = cluster.upgrade() else {
            return;
        };
        let now = tokio::time::Instant::now();
        let every = c.heartbeat_every;
        read_at.retain(|_, at| now.saturating_duration_since(*at) < every);
        name_read_at.retain(|_, at| now.saturating_duration_since(*at) < every);
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|id| *id != c.node_id && !read_at.contains_key(id));
        names.retain(|n| *n != c.node_id && !ids.contains(n) && !name_read_at.contains_key(n));
        for id in &ids {
            read_at.insert(id.clone(), now);
        }
        for name in &names {
            name_read_at.insert(name.clone(), now);
        }
        ids.extend(names);
        if ids.is_empty() {
            continue;
        }
        tracing::debug!(nodes = ?ids, "reading the heartbeats of nodes that called before they were seen");
        c.read_joined(&ids).await;
    }
}

/// Resolves when a certificate whose pin was not known left its name to
/// read ([`crate::cluster_tls::pinning::PinSet::note_unknown_name`]); never
/// without TLS.
async fn unknown_names(tls: &Option<Arc<ClusterTls>>) {
    match tls {
        Some(t) => t.pins().unknown_name_seen().await,
        None => std::future::pending().await,
    }
}

/// Resolves once the start check passed and the first lease write landed:
/// true then, false if another process runs with this id (or the cluster
/// is gone).
async fn identity_unique(cluster: std::sync::Weak<Cluster>) -> bool {
    let Some(c) = cluster.upgrade() else {
        return false;
    };
    let mut seen = c.identity.subscribe();
    drop(c);
    seen.wait_for(|s| *s != Identity::Checking)
        .await
        .is_ok_and(|s| *s == Identity::Unique)
}

/// Reads the operator's intents every heartbeat period
/// ([`Cluster::refresh_control_if_changed`]), on a task of its own (ADR
/// 0020, amendment of 2026-11-20): before, it ran after the view's refresh
/// in [`heartbeat_loop`], and a read of `control/` that hung held up the
/// heartbeats' reads, and the trust in every peer lapsed a horizon later.
/// Each bucket operation of a refresh is bounded by one heartbeat read
/// attempt ([`Bounded`]), not the whole refresh: a slow bucket that answers
/// still has the intents applied, late; one operation that does not answer
/// fails the refresh, which keeps the last intents read (ADR 0014).
pub(super) async fn control_loop(cluster: std::sync::Weak<Cluster>, every: Duration) {
    if !identity_unique(cluster.clone()).await {
        return;
    }
    let mut watch = control::ControlWatch::new();
    loop {
        let next = tokio::time::Instant::now() + every;
        let Some(c) = cluster.upgrade() else {
            return;
        };
        if c.identity_taken() {
            return;
        }
        c.refresh_control_if_changed(&mut watch).await;
        drop(c);
        tokio::time::sleep_until(next).await;
    }
}

/// The bucket's `control/` and intents, each operation within `attempt`
/// (one that runs out is a failure, as the bucket not answering).
pub(super) struct Bounded<'a> {
    keys: &'a ObjectStoreBucket,
    attempt: Duration,
}

impl Bounded<'_> {
    async fn within<T>(
        &self,
        op: impl std::future::Future<Output = Result<T, BucketError>>,
    ) -> Result<T, BucketError> {
        tokio::time::timeout(self.attempt, op)
            .await
            .unwrap_or_else(|_| {
                Err(BucketError::Unavailable(format!(
                    "no answer within {:?}",
                    self.attempt
                )))
            })
    }
}

impl control::Keys for Bounded<'_> {
    async fn list_keys(&self, prefix: &str) -> Result<Vec<(String, u64)>, BucketError> {
        self.within(self.keys.list_keys(prefix)).await
    }

    async fn put_empty(&self, key: &str) -> Result<(), BucketError> {
        self.within(self.keys.put_empty(key)).await
    }

    async fn put_body(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
        self.within(self.keys.put_body(key, body)).await
    }

    async fn get_body(&self, key: &str) -> Result<Option<Vec<u8>>, BucketError> {
        self.within(self.keys.get_body(key)).await
    }

    async fn delete(&self, key: &str) -> Result<(), BucketError> {
        self.within(self.keys.delete(key)).await
    }
}

impl Cluster {
    /// `control/` and the intents, each operation within one heartbeat
    /// read attempt.
    pub(super) fn bounded_control(&self) -> Bounded<'_> {
        Bounded {
            keys: &self.control_keys,
            attempt: self.heartbeat_read_attempt,
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
        let marker = self.bounded_control().get_body(control::CHANGED).await;
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
        match control::read(&self.bounded_control(), Some(&self.root), now_ms()).await {
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

    /// Where a wake-up of `address`, kept apart from companions (ADR 0041,
    /// amendment of 2026-11-22), goes instead of here: `companion_here`
    /// says whether this node holds one of them now. Among the peers
    /// [`Self::chosen_peer`] chooses from, by the same rendezvous. `None`:
    /// wake here, as without the rule (no companion here, or no peer).
    pub(crate) fn apart_peer(
        &self,
        address: &ActorAddress,
        companion_here: bool,
    ) -> Option<String> {
        let eligible = self.eligible_peers();
        let eligible: Vec<&str> = eligible.iter().map(String::as_str).collect();
        wasmkeep_cluster::placement::apart(companion_here, &eligible, &encode_address(address))
            .map(str::to_owned)
    }

    /// Where a wake-up of a spread actor (ADR 0040, amendment of 2026-11-22)
    /// goes instead of here: the rendezvous choice for `address` over this
    /// node and the peers [`Self::chosen_peer`] chooses from. `None`: wake
    /// here (this node is the choice, or no peer can take it). A peer in
    /// placement's backoff is left out: a placed hop it refused runs on
    /// the ordinary path, which must not send it straight back.
    pub(crate) fn spread_peer(&self, address: &ActorAddress) -> Option<String> {
        let eligible = self.eligible_peers();
        let backed_off = self.placement_backed_off();
        let eligible: Vec<&str> = eligible
            .iter()
            .filter(|p| !backed_off.contains(p))
            .map(String::as_str)
            .collect();
        wasmkeep_cluster::placement::spread(&self.node_id, &eligible, &encode_address(address))
            .map(str::to_owned)
    }

    /// [`Self::chosen_peer`] for an actor kept apart from companions that
    /// the nodes `avoid` hold: another eligible peer if there is one.
    pub(crate) fn chosen_peer_apart(
        &self,
        address: &ActorAddress,
        avoid: &[String],
    ) -> Option<String> {
        let eligible = self.eligible_peers();
        let eligible: Vec<&str> = eligible.iter().map(String::as_str).collect();
        let avoid: Vec<&str> = avoid.iter().map(String::as_str).collect();
        wasmkeep_cluster::placement::peer_apart(&eligible, &encode_address(address), &avoid)
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
        self.collects(|_| true)
    }

    /// Whether this node runs the sweep of deleted actors' files (ADR
    /// 0030, section 5): the lowest-id live node of the app that announces
    /// `ends` and `blobs`, as idle expiry chooses with `ends`.
    pub(crate) fn collects_blob_sweeps(&self) -> bool {
        self.collects(|peer| peer.can_blobs)
    }

    fn collects(&self, also: impl Fn(&Peer) -> bool) -> bool {
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
                && also(peer)
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
        use wasmkeep_cluster::store::Store as _;
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
                        idle_since_ms: None,
                    }
                })
            })
            .collect())
    }

    /// One actor's ownership record as a listing would show it (version,
    /// last-modified time, carried idle origin), or `None` without one.
    pub(crate) async fn owner_record_listed(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<wasmkeep_cluster::expiry::Listed>, String> {
        use object_store::ObjectStoreExt as _;
        let key = self.owner_key(address);
        let r = match self.store.get(&StorePath::from(key)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        let meta = r.meta.clone();
        let body = r.bytes().await.map_err(|e| e.to_string())?;
        let record = wasmkeep_cluster::node_lease::OwnerRecord::decode(&body);
        Ok(Some(wasmkeep_cluster::expiry::Listed {
            name: String::new(),
            modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0),
            version: meta.e_tag.unwrap_or_default(),
            idle_since_ms: record
                .and_then(|r| r.recorded)
                .and_then(|r| r.idle_since_ms),
        }))
    }

    /// The actors with an entry in this root's alarm index (ADR 0028): one
    /// listing of `alarms/`, as `(class, name encoded as record keys are)`.
    pub(crate) async fn alarmed_actors(
        &self,
    ) -> Result<std::collections::BTreeSet<(String, String)>, String> {
        use wasmkeep_cluster::store::Store as _;
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
    /// the pins this node trusts. False if the bucket did not answer every
    /// read (what did answer is applied; the others stay as last read) or
    /// the listing failed (the view is kept).
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
        if full {
            // In a round (the first period's): two attempts.
            return self.list_heartbeats(self.heartbeat_read_attempt * 2).await;
        }
        let stopped = self
            .stopped_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut ids: Vec<String> = self
            .known_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|id| !stopped.contains(*id))
            .cloned()
            .collect();
        // This node's own object too: the twin check reads it (the alarm
        // shards count this node whatever its object shows).
        if !ids.contains(&self.node_id) {
            ids.push(self.node_id.clone());
        }
        self.read_heartbeats(&ids).await
    }

    /// The first round of a runtime whose process already holds
    /// heartbeats: one read by key of each node in `held` (as
    /// [`super::process::Transport::heard_writers`] gives them) and of this
    /// node's own object, timed as a refresh.
    async fn refresh_first(&self, mut held: Vec<String>) -> bool {
        let started = std::time::Instant::now();
        if !held.contains(&self.node_id) {
            held.push(self.node_id.clone());
        }
        let ok = self.read_heartbeats(&held).await;
        crate::metrics::METRICS
            .listing_seconds
            .observe(started.elapsed());
        ok
    }

    /// How long a listing of the listing task may take: it is off the
    /// read path, so a slow LIST still finds new nodes, late. Two read
    /// attempts, the scheduled interval, or 30 s, whichever is longest.
    pub(super) fn listing_limit(&self) -> Duration {
        (self.heartbeat_read_attempt * 2)
            .max(
                self.heartbeat_every
                    .saturating_mul(wasmkeep_cluster::membership::FULL_LISTING_EVERY),
            )
            .max(Duration::from_secs(30))
    }

    /// Lists `nodes/` and reads what it lists, within `limit`, and applies
    /// it. False if the bucket did not answer in time (nothing applied).
    pub(super) async fn list_heartbeats(&self, limit: Duration) -> bool {
        let issued = std::time::Instant::now();
        let transport = Arc::clone(&self.transport);
        match tokio::time::timeout(limit, async move { transport.list_nodes().await }).await {
            Ok(Ok(all)) => {
                self.apply_heartbeats(Heartbeats::Listed(all), issued);
                true
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "reading the nodes' heartbeats failed");
                false
            }
            Err(_) => {
                tracing::warn!("listing the nodes' heartbeats took longer than {limit:?}");
                false
            }
        }
    }

    /// Reads the heartbeats of `ids`, all at once, each within one
    /// attempt, and applies what came back as it lands (whatever answered
    /// together, each batch stamped with its earliest read's send time): a
    /// read that hangs holds up the renewal of no other node, and a node
    /// whose read failed stays as last read. A round: its slowest read
    /// plans the early reads. True if every read answered.
    pub(super) async fn read_heartbeats(&self, ids: &[String]) -> bool {
        self.read_and_apply(ids, true).await
    }

    /// [`Self::read_heartbeats`] for join reads (a hello, a hop, the name
    /// on an unknown certificate): they do not plan the early reads, since
    /// anyone reaching the cluster port can cause one.
    pub(super) async fn read_joined(&self, ids: &[String]) -> bool {
        self.read_and_apply(ids, false).await
    }

    async fn read_and_apply(&self, ids: &[String], round: bool) -> bool {
        use futures_util::StreamExt as _;
        let mut landed = self
            .transport
            .read_stream(ids, self.heartbeat_read_attempt)
            .ready_chunks(super::process::HEARTBEAT_READ_CONCURRENCY);
        let (mut failed, mut slowest) = (Vec::new(), Duration::ZERO);
        while let Some(batch) = landed.next().await {
            let Some(issued) = batch.iter().map(|l| l.sent).min() else {
                continue;
            };
            let mut reads = Vec::with_capacity(batch.len());
            for l in batch {
                slowest = slowest.max(l.took);
                match l.read {
                    super::process::HeartbeatRead::Failed(e) => {
                        failed.push(format!("{}: {e}", l.id));
                    }
                    read => reads.push((l.id, read)),
                }
            }
            if !reads.is_empty() {
                self.apply_heartbeats(Heartbeats::Read(reads), issued);
            }
        }
        // The reads went out at once: the round took its slowest one (a
        // timed-out read at the attempt).
        if round {
            self.slowest_read_ms.store(
                u64::try_from(slowest.as_millis()).unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if !failed.is_empty() {
            crate::metrics::METRICS.heartbeat_read_failures.fetch_add(
                u64::try_from(failed.len()).unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            tracing::warn!(
                failed = %failed.join("; "),
                "reading some nodes' heartbeats failed; each stays as last read, until its own horizon"
            );
        }
        failed.is_empty()
    }

    /// Reads `ids` before the next round ([`Self::early_reads_due`]),
    /// noting the heartbeat each was read early for.
    pub(super) async fn read_early(&self, ids: &[String], read_early: &mut HashMap<String, u64>) {
        {
            let held = self.trust_ends.lock().unwrap_or_else(|p| p.into_inner());
            for id in ids {
                if let Some((written_at, _)) = held.get(id) {
                    read_early.insert(id.clone(), *written_at);
                }
            }
        }
        crate::metrics::METRICS.heartbeat_early_reads.fetch_add(
            u64::try_from(ids.len()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        tracing::debug!(peers = ?ids, "reading heartbeats early: their trust would end before the next round lands");
        self.read_heartbeats(ids).await;
    }

    /// Drops from `read_early` the peers whose heartbeat changed since
    /// (or that left): they may be read early again.
    pub(super) fn forget_early_reads(&self, read_early: &mut HashMap<String, u64>) {
        let held = self.trust_ends.lock().unwrap_or_else(|p| p.into_inner());
        read_early.retain(|id, w| held.get(id).is_some_and(|(now, _)| now == w));
    }

    /// The early reads due before the round at `next`
    /// ([`wasmkeep_cluster::membership::early_reads`]): when, and which
    /// peers, leaving out those already read early for their current
    /// heartbeat. Planned `lead` ahead: twice the slowest read of the last
    /// round plus 50 ms, at most one attempt.
    pub(super) fn early_reads_due(
        &self,
        next: tokio::time::Instant,
        read_early: &HashMap<String, u64>,
    ) -> Option<(tokio::time::Instant, Vec<String>)> {
        let now = now_ms();
        let at_now = tokio::time::Instant::now();
        let to_ms = |i: tokio::time::Instant| {
            now.saturating_add(
                u64::try_from(i.saturating_duration_since(at_now).as_millis()).unwrap_or(u64::MAX),
            )
        };
        let attempt = u64::try_from(self.heartbeat_read_attempt.as_millis()).unwrap_or(u64::MAX);
        let slowest = self
            .slowest_read_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let lead = slowest.saturating_mul(2).saturating_add(50).min(attempt);
        let held = self.trust_ends.lock().unwrap_or_else(|p| p.into_inner());
        let claims = held
            .iter()
            .filter(|(id, (written_at, _))| read_early.get(*id) != Some(written_at))
            .map(|(id, (_, ends))| (id.as_str(), *ends));
        let (at, ids) = wasmkeep_cluster::membership::early_reads(claims, now, to_ms(next), lead)?;
        let at = at_now + Duration::from_millis(at.saturating_sub(now));
        Some((at, ids.into_iter().map(str::to_owned).collect()))
    }

    /// Applies heartbeats just read by a listing or reads issued at
    /// `issued` ([`super::process::Transport::note_heartbeats`]: a node
    /// found gone is dropped, one whose read failed stays as last read, and
    /// a read issued earlier than the one held, by this app or another of
    /// the process, changes nothing). Then refreshes, from every node as
    /// last read, the view, the trust and the watches.
    fn apply_heartbeats(&self, read: Heartbeats, issued: std::time::Instant) {
        let transport = Arc::clone(&self.transport);
        transport.note_heartbeats(read, issued, &self.lease_config, |all, judged, fresh| {
            self.apply_view(all, judged, fresh);
        });
    }

    /// [`Self::apply_heartbeats`]'s second half, under the process's lock
    /// of the heartbeats as last read: every node as last read (`all`),
    /// the same judged by their first read (`judged`), and those just read
    /// (`fresh`).
    fn apply_view(&self, all: &[NodeInfo], judged: &[NodeInfo], fresh: &[String]) {
        let now = now_ms();
        *self.known_nodes.lock().unwrap_or_else(|p| p.into_inner()) =
            all.iter().map(|n| n.id.clone()).collect();
        for n in all
            .iter()
            .filter(|n| n.id != self.node_id && fresh.contains(&n.id))
        {
            crate::metrics::METRICS
                .heartbeat_age_ms
                .observe_ms(now.saturating_sub(n.written_at_ms));
        }
        // Against every heartbeat, live or not: a twin whose clock lags
        // may look stale.
        self.check_twin(all);
        self.note_old_versions(all);
        self.note_future_leases(all);
        // The view and the trust judge each heartbeat by its `written_at`,
        // or by when the process first read it if that is earlier (a clock
        // ahead): never by when a read finished, and never later than its
        // own date.
        *self.stopped_nodes.lock().unwrap_or_else(|p| p.into_inner()) = all
            .iter()
            .filter(|n| matches!(n.lease, Some(NodeState::Released | NodeState::Fenced)))
            .map(|n| n.id.clone())
            .collect();
        let nodes = self.membership.live(judged.to_vec());
        {
            let raw = |id: &str| all.iter().find(|n| n.id == id).map(|n| n.written_at_ms);
            *self.trust_ends.lock().unwrap_or_else(|p| p.into_inner()) = nodes
                .iter()
                .filter(|n| n.id != self.node_id)
                .filter_map(|n| {
                    let ends = n.written_at_ms.saturating_add(self.heartbeat_horizon_ms);
                    Some((n.id.clone(), (raw(&n.id)?, ends)))
                })
                .collect();
        }
        self.note_multi_app(&nodes);
        let mixed = older_nodes(&self.node_id, &nodes);
        let period = u64::try_from(self.heartbeat_every.as_millis()).unwrap_or(u64::MAX);
        let logged = self
            .conflict_logged_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if !mixed.is_empty() && now.saturating_sub(logged) >= period {
            self.conflict_logged_ms
                .store(now, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(serves = %self.serves, "{}", older_nodes_advice(&mixed));
        }
        // The process's trust table; when an address changed hands (or its
        // node left), every pooled connection is dropped. A node kept as
        // last read has the same pins there: nothing is dropped for it.
        self.transport
            .refresh_trust(&nodes, all, self.heartbeat_horizon_ms);
        if self.refuse_plaintext {
            self.note_plain_peers(&nodes, now);
        }
        // Plain `http://` bases and `https://` cluster listeners only:
        // calls between nodes carry signatures, and the forwarding clients
        // speak HTTP, over TLS or not.
        let mut peers = nodes
            .iter()
            .filter_map(|n| Some((n.id.clone(), Peer::of(n)?)))
            .collect();
        {
            let mut view = self.peers.write().unwrap_or_else(|p| p.into_inner());
            carry_first_seen(&view, &mut peers);
            *view = peers;
        }
        // The same bodies, for the requests kept until a dead node's
        // actors are free: no bucket request. Only those just read and
        // not older than a body of that lease the process already read
        // are news: a body kept after a failed read, or an earlier one a
        // replica served late, leaves the watch's entry as it was. A body
        // another app of the process read since is news here too, unless
        // it is older.
        let kept = |id: &str| {
            use wasmkeep_cluster::failover::Shown;
            let older = all.iter().find(|n| n.id == id).is_some_and(|n| {
                n.lease.is_some_and(|state| {
                    let body = wasmkeep_cluster::node_lease::NodeLeaseObject {
                        state,
                        instance: n.instance.clone(),
                        written_at_ms: n.written_at_ms,
                    };
                    self.transport.lease_reads.newest(id).is_some_and(|newest| {
                        wasmkeep_cluster::node_lease::older_than(
                            &body,
                            &newest,
                            wasmkeep_cluster::node_lease::fence_window_ms(&self.lease_config),
                        )
                    })
                })
            });
            if older {
                Shown::Older
            } else if fresh.iter().any(|f| f == id) {
                Shown::Read
            } else {
                Shown::Kept
            }
        };
        self.failover.observe_read(all, kept, now);
        // Last, and handed to the fence task: a fence awaits the bucket,
        // and neither this view nor the next listing may wait for it. Only
        // the heartbeats just read are watched as seen again: one kept as
        // last read is not evidence that its node stopped writing.
        let due = self.fences_due(all, fresh);
        if !due.is_empty() {
            let _ = self.fences_due.send(due);
        }
        self.note_view_shape(&nodes, all);
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

    /// Reads node `id`'s heartbeat by key soon, on a task of its own
    /// ([`join_read_loop`]), unless it is in this node's view and not
    /// stopped: a signed call named it, or this node looks for it, so it
    /// may have just joined, or restarted under its id after a clean stop
    /// (its heartbeat as last read `released`, which the rounds no longer
    /// read; the process's table of live senders may still hold the
    /// stopped instance's last live heartbeat until its horizon, so it is
    /// not what decides).
    pub(crate) fn read_soon(&self, id: &str) {
        if id == self.node_id {
            return;
        }
        let in_view = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(id);
        let stopped = self
            .stopped_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(id);
        if in_view && !stopped {
            return;
        }
        let _ = self.reads_wanted.send(id.to_owned());
    }

    /// Whether node `id` is among the nodes as last read (no read asked).
    pub(crate) fn knows(&self, id: &str) -> bool {
        self.known_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|n| n == id)
    }

    /// Whether the last refresh found a peer to say hello to: a live node
    /// of this app in the view, as [`Self::say_hello`] chooses them. A
    /// stopped node, or a node of another app, gets no hello, so it must
    /// not use up the first round of hellos, which is then kept for the
    /// first peer that shows up.
    pub(super) fn found_peers(&self) -> bool {
        self.peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|(id, peer)| *id != self.node_id && peer.app == self.serves)
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
        let read: Vec<String> = all.iter().map(|n| n.id.clone()).collect();
        let due = self.fences_due(all, &read);
        self.fence_due(due).await;
    }

    /// The listing's side of [`Self::fence_stale_nodes`]: watches the
    /// lease of every node `all` holds (as last read) that `read` names
    /// (read just now) and returns those due for a fence; a node only kept
    /// as last read is neither seen again nor forgotten. No bucket
    /// request.
    pub(super) fn fences_due(&self, all: &[NodeInfo], read: &[String]) -> Vec<FenceDue> {
        let now = now_ms();
        // One read: the validity the watch counts from and the epoch each
        // fence is checked against are the same.
        let validity = self.validity.get();
        {
            let mut watch = self.fence_watch.lock().unwrap_or_else(|p| p.into_inner());
            watch.own_lease(validity.until_ms, now);
            watch.retain(|node| all.iter().any(|n| n.id == node));
            let mut due = Vec::new();
            for n in all
                .iter()
                .filter(|n| n.id != self.node_id && read.contains(&n.id))
            {
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

/// A live peer of this node's app as the rebalance pass reads it (ADR
/// 0040, section 14): owned, so the view's lock is not held while the
/// pass decides.
#[derive(Debug, Clone)]
pub(crate) struct RebalancePeer {
    pub(crate) node: String,
    pub(crate) version: String,
    pub(crate) drain: Option<DrainState>,
    pub(crate) drain_requested: bool,
    pub(crate) reachable: bool,
    pub(crate) serving_for: Option<Duration>,
    pub(crate) can_take: bool,
    pub(crate) load: Option<wasmkeep_cluster::placement::RoomLoad>,
    pub(crate) suspected: bool,
}

impl Cluster {
    /// Whether spread actors are moved home ([`ClusterConfig::rebalance`],
    /// off with placement).
    pub(crate) fn rebalance_on(&self) -> bool {
        self.rebalance
    }

    /// How long a heartbeat stays live (ms): a failed rebalance target is
    /// left out that long.
    pub(crate) fn heartbeat_horizon(&self) -> u64 {
        self.heartbeat_horizon_ms
    }

    /// The rebalance pass's constants.
    pub(crate) fn rebalance_settings(&self) -> wasmkeep_cluster::rebalance::Settings {
        self.rebalance_settings
    }

    /// The live peers of this node's app, with what the rebalance pass
    /// reads of each.
    pub(crate) fn rebalance_peers(&self) -> Vec<RebalancePeer> {
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
        let period = self.heartbeat_every;
        let peers: Vec<RebalancePeer> = self
            .peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(id, peer)| **id != self.node_id && peer.app == self.serves)
            .map(|(id, peer)| RebalancePeer {
                node: id.clone(),
                version: peer.version.clone(),
                drain: peer.drain,
                drain_requested: draining.contains(id),
                reachable: true,
                serving_for: peer.serving_since.map(|s| s.elapsed()),
                can_take: peer.can_take,
                load: peer
                    .seen
                    .as_ref()
                    .and_then(|s| s.fresh(period))
                    .map(|l| wasmkeep_cluster::placement::RoomLoad::from(*l)),
                suspected: suspects.contains(id),
            })
            .collect();
        peers
            .into_iter()
            .map(|mut p| {
                p.reachable = self.address_of(&p.node).is_some();
                p
            })
            .collect()
    }

    /// A spread actor's home (ADR 0040, section 14): the rendezvous choice
    /// over this node and the peers of its app that serve, are not asked
    /// to drain and can be reached. `None`: this node.
    pub(crate) fn spread_home(&self, address: &ActorAddress) -> Option<String> {
        let peers = self.rebalance_peers();
        let set: Vec<&str> = peers
            .iter()
            .filter(|p| p.drain == Some(DrainState::Serving) && !p.drain_requested && p.reachable)
            .map(|p| p.node.as_str())
            .collect();
        wasmkeep_cluster::rebalance::home(&self.node_id, &set, &encode_address(address))
            .map(str::to_owned)
    }
}
