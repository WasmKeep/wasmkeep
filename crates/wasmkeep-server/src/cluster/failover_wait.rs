//! Keeping a request for a dead node's actor until the actor can be taken
//! over, instead of answering 503 at once.
//!
//! Only the node a client reached keeps a request, before anything of it
//! was sent to another node, and outside the actor's mailbox: a kept
//! request holds a connection and its headers, nothing else. The rule that
//! decides is `wasmkeep_cluster::failover` ([`OwnerWatch`], [`Waiter`]),
//! fed from the heartbeat listing, failed connections to peers and this
//! node's own fences. A kept request asks for the actor again through the
//! ordinary path, so every takeover is still an ordinary acquisition, at
//! the moment one would succeed for a client that retried just then.

use wasmkeep_cluster::failover::{OwnerWatch, Seen, Step, Waiter};

use super::*;

/// Requests kept at once for one actor; more get the answer at once.
const FAILOVER_WAIT_PER_ACTOR: usize = 64;

/// The least time between two tries of one kept request: past the time a
/// node trusts "another node holds this actor" without asking the bucket,
/// so a try is not answered from what an earlier try of the same request
/// left in memory. (`ActorService::hold` also forgets the owner before
/// each try; what another request learnt meanwhile can still answer one.)
const RETRY_MS: u64 = crate::actor::HINT_MS + 100;

/// The first try once an actor looks free waits up to this long, by actor:
/// one fence does not start every takeover in the same instant.
const JITTER_MS: u64 = 1_000;

/// The default wait: what a takeover can take after the owner's last
/// lease write. A lease period plus grace until its actors may be freed,
/// the fence watch (a lease period plus one attempt), one listing for a
/// node that did not write the fence itself, and a second for the spread
/// of the tries. 25 s with the default timing.
pub fn failover_wait_default(lease: &LeaseConfig, heartbeat: Duration) -> Duration {
    Duration::from_millis(
        lease
            .ttl_ms()
            .saturating_add(lease.grace_ms())
            .saturating_add(lease.ttl_ms() / 3),
    )
    .saturating_add(lease_attempt(lease))
    .saturating_add(heartbeat)
    .saturating_add(Duration::from_millis(JITTER_MS))
}

/// What a kept request is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kept {
    Request,
    WebSocket,
}

impl Kept {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::WebSocket => "websocket",
        }
    }
}

/// How a wait ended, for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Waited {
    /// Ask for the actor again.
    Try,
    /// The owner is alive: answer as without the wait.
    Leave,
    /// The deadline passed, this node is stopping or its lease ran out:
    /// the 503, without dialling anybody.
    GiveUp,
}

#[derive(Debug, Default)]
struct Counts {
    total: usize,
    actors: HashMap<ActorAddress, usize>,
}

/// The requests this node keeps, and what it knows about its peers'
/// leases.
#[derive(Debug)]
pub(crate) struct FailoverWait {
    /// How long a request is kept at most; zero: none is.
    wait: Duration,
    /// Requests kept at once.
    max: usize,
    watch: Mutex<OwnerWatch>,
    /// Moves when the evidence does: a listing, a fence.
    tick: tokio::sync::watch::Sender<u64>,
    /// Set when the node begins to stop.
    closed: tokio::sync::watch::Sender<bool>,
    counts: Mutex<Counts>,
}

impl FailoverWait {
    pub(super) fn new(config: &ClusterConfig) -> Self {
        let wait = config
            .failover_wait
            .unwrap_or_else(|| failover_wait_default(&config.lease, config.heartbeat));
        let free = Duration::from_millis(
            config
                .lease
                .ttl_ms()
                .saturating_add(config.lease.grace_ms()),
        );
        if !wait.is_zero() && wait < free {
            tracing::warn!(
                wait_ms = wait.as_millis(),
                free_after_ms = free.as_millis(),
                "the failover wait is shorter than a lease period plus grace: only requests \
                 that arrive late after a node's death can be served by it"
            );
        }
        Self {
            wait,
            max: config.failover_wait_max,
            watch: Mutex::new(OwnerWatch::new(
                config.node_id.clone(),
                &config.lease,
                heartbeat_horizon_ms(config.heartbeat, &config.lease),
            )),
            tick: tokio::sync::watch::Sender::new(0),
            closed: tokio::sync::watch::Sender::new(false),
            counts: Mutex::new(Counts::default()),
        }
    }

    fn enabled(&self) -> bool {
        !self.wait.is_zero()
    }

    /// The deadline (ms) of a request arriving now; `None` with the wait
    /// off.
    pub(crate) fn deadline_ms(&self) -> Option<u64> {
        self.enabled().then(|| {
            now_ms().saturating_add(u64::try_from(self.wait.as_millis()).unwrap_or(u64::MAX))
        })
    }

    fn watch(&self) -> std::sync::MutexGuard<'_, OwnerWatch> {
        self.watch.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn bump(&self) {
        self.tick.send_modify(|t| *t = t.wrapping_add(1));
    }

    /// A listing of the heartbeats: every node lease it showed.
    pub(super) fn observe(&self, all: &[NodeInfo], now: u64) {
        if self.enabled() {
            self.watch().observe(all.iter().map(Seen::from), now);
            self.bump();
        }
    }

    /// This node's fence attempt found `node` fenced or not live.
    pub(super) fn nudge(&self, node: &str) {
        if self.enabled() {
            self.watch().nudge(node);
            self.bump();
        }
    }

    /// A connection to `node` could not be opened, as far as the HTTP
    /// client tells: its `is_connect` also covers a failure on this side
    /// (no socket or port left) and, rarely, a pooled connection that
    /// could not be handed out. A hint for requests that arrive later;
    /// the request that found it out has its answer. A wrong mark on a
    /// live node costs those requests a few seconds, until a listing shows
    /// its next lease write.
    pub(super) fn connect_failed(&self, node: &str, error: &hyper_util::client::legacy::Error) {
        if self.enabled() && error.is_connect() {
            self.watch().connect_failed(node, now_ms());
        }
    }

    /// The node begins to stop: every kept request is answered.
    pub(crate) fn close(&self) {
        self.closed.send_replace(true);
    }

    fn count(kind: Kept, outcome: &'static str) {
        crate::metrics::inc(
            &crate::metrics::METRICS
                .failover_waits
                .get(&[("kind", kind.as_str()), ("outcome", outcome)]),
        );
    }
}

/// Up to [`JITTER_MS`], the same for every request kept for `address` on
/// this node.
fn jitter_ms(address: &ActorAddress, node: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (address, node).hash(&mut hasher);
    hasher.finish() % JITTER_MS
}

impl Cluster {
    /// Keeps a request for `address`, which `owner` (another node) holds
    /// and which arrived with `deadline_ms` ([`FailoverWait::deadline_ms`]),
    /// if waiting can serve it: this node's lease is valid, the owner is
    /// suspected dead, stale or stopping, the deadline reaches the moment
    /// its actors can be free, and a place is left. `None`: answer as
    /// without the wait.
    pub(crate) fn keep(
        &self,
        owner: &str,
        address: &ActorAddress,
        deadline_ms: u64,
        kind: Kept,
    ) -> Option<Ticket<'_>> {
        let failover = &self.failover;
        if !failover.enabled() || *failover.closed.borrow() || !self.validity.valid() {
            return None;
        }
        let now = now_ms();
        let verdict = failover.watch().verdict(owner, now, None);
        let waiter = Waiter::admit(
            verdict,
            now,
            deadline_ms,
            RETRY_MS,
            jitter_ms(address, &self.node_id),
        )?;
        {
            let mut counts = failover.counts.lock().unwrap_or_else(|p| p.into_inner());
            let actor = counts.actors.get(address).copied().unwrap_or(0);
            if counts.total >= failover.max || actor >= FAILOVER_WAIT_PER_ACTOR {
                FailoverWait::count(kind, "over_cap");
                return None;
            }
            counts.total += 1;
            counts.actors.insert(address.clone(), actor + 1);
        }
        tracing::debug!(app = %crate::logging::app(address), actor = %crate::logging::actor(address), owner, ?verdict, "keeping a request for a takeover");
        Some(Ticket {
            cluster: self,
            address: address.clone(),
            owner: owner.to_owned(),
            waiter,
            kind,
            outcome: "left",
        })
    }
}

/// One kept request. Dropping it frees its place and counts how the wait
/// ended; dropped while it waits, the client has left, and the request is
/// never run.
#[derive(Debug)]
pub(crate) struct Ticket<'a> {
    cluster: &'a Cluster,
    address: ActorAddress,
    owner: String,
    waiter: Waiter,
    kind: Kept,
    outcome: &'static str,
}

impl Ticket<'_> {
    /// The node this request waits for.
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    /// A try came back naming the same owner.
    pub(crate) fn refused(&mut self) {
        self.waiter.refused(now_ms());
    }

    /// The wait is over: `outcome` is counted.
    pub(crate) fn end(mut self, outcome: &'static str) {
        self.outcome = outcome;
    }

    /// Waits until the request should ask for its actor again, or stop
    /// waiting. After [`Waited::Try`] the caller asks through the ordinary
    /// path, and comes back here (after [`Ticket::refused`]) if the same
    /// owner is still named.
    pub(crate) async fn wait(&mut self) -> Waited {
        let failover = &self.cluster.failover;
        let mut tick = failover.tick.subscribe();
        let mut closed = failover.closed.subscribe();
        let mut validity = self.cluster.validity.subscribe();
        self.outcome = "left";
        loop {
            let now = now_ms();
            if *closed.borrow_and_update() {
                self.outcome = "shutdown";
                return Waited::GiveUp;
            }
            let valid = *validity.borrow_and_update();
            if !valid.valid(now) {
                self.outcome = "lease_lost";
                return Waited::GiveUp;
            }
            tick.borrow_and_update();
            let verdict =
                failover
                    .watch()
                    .verdict(&self.owner, now, Some(self.waiter.held_since_ms()));
            let until_ms = match self.waiter.step(verdict, now) {
                Step::Try => {
                    // Until the caller says otherwise: the try answered
                    // something else than the actor's response.
                    self.outcome = "answered_other";
                    return Waited::Try;
                }
                Step::Leave => {
                    self.outcome = "owner_alive";
                    return Waited::Leave;
                }
                Step::Expire => {
                    self.outcome = "expired";
                    return Waited::GiveUp;
                }
                Step::Wait { until_ms } => until_ms,
            };
            // This node's lease running out ends the wait too.
            let until_ms = valid.until_ms.map_or(until_ms, |v| v.min(until_ms));
            let sleep = Duration::from_millis(until_ms.saturating_sub(now).max(1));
            tokio::select! {
                () = tokio::time::sleep(sleep) => {}
                _ = tick.changed() => {}
                _ = closed.changed() => {}
                _ = validity.changed() => {}
            }
        }
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let failover = &self.cluster.failover;
        {
            let mut counts = failover.counts.lock().unwrap_or_else(|p| p.into_inner());
            counts.total = counts.total.saturating_sub(1);
            match counts.actors.get_mut(&self.address) {
                Some(n) if *n > 1 => *n -= 1,
                _ => {
                    counts.actors.remove(&self.address);
                }
            }
        }
        if self.outcome == "left" {
            tracing::debug!(app = %crate::logging::app(&self.address), actor = %crate::logging::actor(&self.address), "a kept request's client left; not run");
        }
        FailoverWait::count(self.kind, self.outcome);
    }
}

/// What the watch says and how many requests are kept (tests).
#[cfg(test)]
impl FailoverWait {
    pub(super) fn verdict(&self, node: &str) -> wasmkeep_cluster::failover::Verdict {
        self.watch().verdict(node, now_ms(), None)
    }

    pub(super) fn kept(&self) -> usize {
        self.counts.lock().unwrap_or_else(|p| p.into_inner()).total
    }
}
