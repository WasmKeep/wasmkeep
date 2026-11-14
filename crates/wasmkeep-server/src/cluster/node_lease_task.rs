//! This node's lease (ADR 0020): the heartbeat object `nodes/<id>`, written
//! conditionally on a task of its own, never waiting for the heartbeat
//! listing, the cluster secret or `control/`.

use wasmkeep_cluster::lease::Version;
use wasmkeep_cluster::node_lease::{NodeLease, NodeLeaseError};

use super::*;

/// How long a clean stop goes on trying the `released` write after its
/// first attempt timeout.
const RELEASE_RETRIES: Duration = Duration::from_secs(1);
/// Between two tries of the `released` write.
const RELEASE_RETRY_WAIT: Duration = Duration::from_millis(100);
/// The tries end this long before the stop's limit.
const RELEASE_STOP_MARGIN: Duration = Duration::from_millis(250);
/// The shortest try worth making.
const RELEASE_SHORTEST: Duration = Duration::from_millis(200);

/// Marks the lease task finished when dropped, whatever the way out.
struct Finished(std::sync::Weak<Cluster>);

impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(c) = self.0.upgrade() {
            // Whatever the way out, no hold acts on it any more.
            c.invalidate_node_lease(false);
            c.lease_done.send_replace(true);
        }
    }
}

/// How often the node lease is written: every heartbeat period, which
/// [`Cluster::new`] keeps at most a third of the ttl, so a node has three
/// tries to renew before its lease runs out.
pub(super) fn lease_every(heartbeat: Duration, lease: &LeaseConfig) -> Duration {
    heartbeat.min(Duration::from_millis(lease.ttl_ms() / 3))
}

/// How long one lease write may take: a third of `ttl - safety` (2.7 s with
/// the defaults), so three attempts fit in the lease's validity.
pub(crate) fn lease_attempt(lease: &LeaseConfig) -> Duration {
    Duration::from_millis(lease.ttl_ms().saturating_sub(lease.safety_ms()) / 3)
}

/// Checks the bucket and this node id, writes the node lease's first
/// `live` write, then renews it every [`lease_every`] from the previous
/// send, until it is lost or a clean stop asks for its release.
pub(super) async fn node_lease_loop(cluster: std::sync::Weak<Cluster>) {
    let _finished = Finished(cluster.clone());
    let Some(c) = cluster.upgrade() else {
        return;
    };
    let mut stop = c.lease_stop.subscribe();
    let mut lease = NodeLease::new(
        c.control_keys.clone(),
        SystemClock,
        c.node_key(),
        c.membership.instance().unwrap_or_default().to_owned(),
        c.lease_config,
    );
    // A clean stop asked before the node started writes nothing.
    let started = tokio::select! {
        started = c.start_node_lease(&mut lease) => started,
        _ = stop.wait_for(|s| *s) => false,
    };
    if !started {
        return;
    }
    c.identity.send_replace(Identity::Unique);
    tracing::info!("{}", super::identity::checked_message(&c.node_id));
    let every = c.lease_every;
    drop(c);
    // The first renewal a period after the first write.
    let mut next = tokio::time::Instant::now() + every;
    loop {
        let asked = tokio::select! {
            () = tokio::time::sleep_until(next) => false,
            _ = stop.wait_for(|s| *s) => true,
        };
        let Some(c) = cluster.upgrade() else {
            return;
        };
        if asked {
            c.release_node_lease(&mut lease).await;
            return;
        }
        // Another process found running with this node id by the listing.
        if c.identity_taken() {
            c.invalidate_node_lease(false);
            return;
        }
        lease.set_payload(c.membership.lease_payload());
        // The next send a period after this one, however long this one
        // takes: a write that hangs until its timeout must not push the
        // next past the lease's validity (three tries per lease). One
        // that took longer than a period is followed at once.
        next = tokio::time::Instant::now() + every;
        // A clean stop asked while the write is in flight makes the lease
        // locally invalid at once, lets the write finish (it is not
        // aborted, and gives the version `released` conditions on), and
        // publishes nothing from it (ADR 0020, section 4).
        let mut stopping = false;
        let attempt = {
            let attempt = tokio::time::timeout(c.lease_attempt, lease.renew());
            tokio::pin!(attempt);
            tokio::select! {
                biased;
                done = &mut attempt => done,
                () = async { let _ = stop.wait_for(|s| *s).await; } => {
                    c.invalidate_node_lease(false);
                    stopping = true;
                    attempt.await
                }
            }
        };
        if stopping {
            c.release_node_lease(&mut lease).await;
            return;
        }
        match attempt {
            Ok(Ok(())) => c.node_lease_written(&lease),
            Ok(Err(NodeLeaseError::Lost(cause))) => {
                c.node_lease_lost(cause);
                return;
            }
            Ok(Err(NodeLeaseError::Unavailable(e))) => {
                crate::metrics::inc(&crate::metrics::METRICS.node_lease_failures);
                c.publish_validity(&lease);
                tracing::warn!(error = %e, "writing the node lease failed; retrying");
            }
            // Cancelled in flight: it may still land. The next write's
            // condition then fails, and its re-read adopts it.
            Err(_) => {
                crate::metrics::inc(&crate::metrics::METRICS.node_lease_failures);
                c.publish_validity(&lease);
                tracing::warn!("writing the node lease timed out; retrying");
            }
        }
    }
}

impl Cluster {
    /// The bucket check, the start check, then the first write. A first
    /// write that fails its condition goes back to the start check (ADR
    /// 0020, section 1): a peer may have fenced a stopped predecessor
    /// meanwhile, or a twin started. It keeps this process's instance.
    /// `false` if this node must not run (the verdict is published).
    async fn start_node_lease(
        &self,
        lease: &mut NodeLease<ObjectStoreBucket, SystemClock>,
    ) -> bool {
        if let Some(why) = self.check_bucket().await {
            tracing::error!("{why}");
            self.identity.send_replace(Identity::Refused(why));
            return false;
        }
        loop {
            // The start guard and the start check run together (ADR 0020,
            // section 6): a new node serves about 15 s after it starts when
            // the old nodes stopped at least 10 s before.
            let (guard, checked) = tokio::join!(self.start_guard(), self.check_identity());
            let checked = match guard {
                Ok(()) => checked,
                Err(why) => (Stopped::OlderNode.identity(&why), None),
            };
            let expected = match checked {
                (Identity::Unique, expected) => expected,
                (verdict, _) => {
                    if let Identity::Taken(_, why) = &verdict {
                        tracing::error!("{why}");
                    }
                    self.identity.send_replace(verdict);
                    return false;
                }
            };
            if self.first_write(lease, expected.as_ref()).await.is_ok() {
                return true;
            }
            tracing::warn!(
                "this node's first lease write found the object changed; checking the node id again"
            );
        }
    }

    /// This node's lease object: `nodes/<id>`.
    pub(crate) fn node_key(&self) -> String {
        format!("{NODES_PREFIX}{}", self.node_id)
    }

    /// The first `live` write after the start check, retried while the
    /// bucket does not answer. `Err` if its condition failed and the re-read
    /// did not show this process's own write.
    async fn first_write(
        &self,
        lease: &mut NodeLease<ObjectStoreBucket, SystemClock>,
        expected: Option<&Version>,
    ) -> Result<(), ()> {
        loop {
            lease.set_payload(self.membership.lease_payload());
            match tokio::time::timeout(self.lease_attempt, lease.start(expected)).await {
                Ok(Ok(())) => {
                    self.node_lease_written(lease);
                    return Ok(());
                }
                Ok(Err(NodeLeaseError::Lost(_))) => return Err(()),
                Ok(Err(NodeLeaseError::Unavailable(e))) => {
                    crate::metrics::inc(&crate::metrics::METRICS.node_lease_failures);
                    tracing::warn!(error = %e, "writing the node lease failed; retrying before starting");
                }
                Err(_) => {
                    crate::metrics::inc(&crate::metrics::METRICS.node_lease_failures);
                    tracing::warn!("writing the node lease timed out; retrying before starting");
                }
            }
            tokio::time::sleep(self.lease_every).await;
        }
    }

    /// A lease write reached the bucket.
    fn node_lease_written(&self, lease: &NodeLease<ObjectStoreBucket, SystemClock>) {
        crate::metrics::inc(&crate::metrics::METRICS.node_lease_writes);
        self.publish_validity(lease);
        self.heartbeat_reached_bucket(now_ms());
    }

    /// Publishes until when this node's lease is valid. If it had run out
    /// since the last write (the bucket unreachable for longer than `ttl -
    /// safety`, no conflict seen), every hold ends first, each into the
    /// pending releases, and none is valid again (ADR 0020, section 1):
    /// the actors are acquired afresh, at new generations.
    fn publish_validity(&self, lease: &NodeLease<ObjectStoreBucket, SystemClock>) {
        if self.validity.publish(lease.valid_until_ms()) {
            tracing::warn!("this node's lease ran out: every actor it held is released");
            self.end_holds_after_lapse();
        }
    }

    /// The lease had run out: runs [`Cluster::on_lease_lapse`]'s hook (the
    /// service's WebSockets close), then ends every hold into the pending
    /// releases.
    pub(super) fn end_holds_after_lapse(&self) {
        if let Some(hook) = self.lapse_hook.get() {
            hook();
        }
        self.pending.end_all(true);
    }

    /// Makes this node's lease invalid here at once: every hold ends (as
    /// pending releases if `release`; dropped otherwise, since a lost lease
    /// and a clean stop's `released` free the records), and none is valid
    /// again.
    pub(super) fn invalidate_node_lease(&self, release: bool) {
        self.validity.invalidate();
        self.pending.end_all(release);
    }

    /// Another writer replaced this node's lease object: a twin, a peer's
    /// or an operator's fence, or a deletion. This process stops at once
    /// (ADR 0020, section 1), through the same path as a twin found by the
    /// listing: holds dropped without releasing, `wasmkeep serve` exits.
    fn node_lease_lost(&self, cause: wasmkeep_cluster::node_lease::LostCause) {
        self.invalidate_node_lease(false);
        let stopped = Stopped::of(cause);
        let then = match stopped {
            Stopped::Fenced => {
                "a peer fenced it after it stopped renewing, or an operator ran `wasmkeep cluster \
                 fence`; its actors are free once ttl plus grace passed, and a restart takes \
                 them back"
            }
            Stopped::Deleted => {
                "the object was removed; this process stops, and a restart writes it again"
            }
            Stopped::Released => "another process stopped cleanly under this id",
            Stopped::OldVersion => {
                "a process of a version from before node leases writes this id's heartbeat; this \
                 one stops and the other goes on: stop it, or give this node an id of its own \
                 (ADR 0020)"
            }
            Stopped::OlderNode => {
                "stop every node of the older version before starting this one (ADR 0020)"
            }
            Stopped::NotHeld => "this process held no lease to write",
            Stopped::Twin | Stopped::Unreadable => {
                "this one stops and the other goes on; give each node an id of its own"
            }
        };
        let verdict = stopped.identity(&format!(
            "this node's lease ({}) was replaced by another writer: {then}",
            self.node_key()
        ));
        if let Identity::Taken(_, why) = &verdict {
            tracing::error!("{why}");
        }
        self.identity.send_replace(verdict);
    }

    /// A clean stop's last write: the node lease made locally invalid
    /// first, then written `released` (ADR 0020, section 4), so the next
    /// process with this id, and peers, need not wait for a fence. A write
    /// that fails is tried again, for one attempt timeout and a second
    /// more (within [`Self::node_lease_stop_limit`]).
    async fn release_node_lease(&self, lease: &mut NodeLease<ObjectStoreBucket, SystemClock>) {
        // Locally invalid on the whole node first: uploads still in flight
        // return late, and the lost-lease cut ends any event still running
        // (ADR 0020, section 4). The `released` write frees what is left.
        self.invalidate_node_lease(false);
        if self.identity_taken() {
            return;
        }
        lease.set_payload(self.membership.lease_payload());
        // Ends short of the stop's limit, so the last try's outcome is known
        // before the limit passes (and the stop reports it truly).
        let deadline = tokio::time::Instant::now() + self.lease_attempt + RELEASE_RETRIES
            - RELEASE_STOP_MARGIN;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let error =
                match tokio::time::timeout(self.lease_attempt.min(left), lease.release()).await {
                    Ok(Ok(())) => {
                        tracing::info!("node lease released");
                        return;
                    }
                    Ok(Err(e)) => format!("{e:?}"),
                    Err(_) => "timed out".to_owned(),
                };
            // No try shorter than RELEASE_SHORTEST: it would be cut before
            // its answer, and could still land.
            if tokio::time::Instant::now() + RELEASE_RETRY_WAIT + RELEASE_SHORTEST >= deadline {
                tracing::warn!(
                    error = %error,
                    "releasing the node lease failed: it stays live, so peers free its actors \
                     only by fencing it (ttl plus grace after its last write), and a process \
                     restarted under this id waits ttl plus grace before it serves"
                );
                return;
            }
            tracing::info!(error = %error, "releasing the node lease failed; trying again");
            tokio::time::sleep(RELEASE_RETRY_WAIT).await;
        }
    }

    /// Whether this node's lease is valid now.
    pub(crate) fn node_lease_valid(&self) -> bool {
        self.validity.valid()
    }

    /// How long a clean stop waits for the `released` write: a renewal
    /// already in flight finishes first (it is not aborted, one attempt
    /// timeout), then the release, tried for one attempt timeout and
    /// [`RELEASE_RETRIES`] more.
    pub(crate) fn node_lease_stop_limit(&self) -> Duration {
        self.lease_attempt * 2 + RELEASE_RETRIES
    }

    /// A clean stop, after the actors' releases: stops the lease task and
    /// writes the node lease `released`. Waits up to `limit`.
    pub(crate) async fn stop_node_lease(&self, limit: Duration) -> bool {
        self.lease_stop.send_replace(true);
        let mut done = self.lease_done.subscribe();
        tokio::time::timeout(limit, done.wait_for(|d| *d))
            .await
            .is_ok()
    }
}
