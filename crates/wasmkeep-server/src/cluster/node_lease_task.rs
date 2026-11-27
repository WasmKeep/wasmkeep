//! This node's lease (ADR 0020): the heartbeat object `nodes/<id>`, written
//! conditionally on a task of its own, never waiting for the heartbeat
//! listing, the cluster secret or `control/`.

use wasmkeep_cluster::lease::Version;
use wasmkeep_cluster::node_lease::{NodeLease, NodeLeaseError, Renewals};

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

/// When the node lease is renewed ([`Renewals`]): a spacing of at most a
/// heartbeat period and a quarter of `ttl - safety` (2 s with the
/// defaults) after the last acknowledged write, each renewal bounded by a
/// quarter of `ttl - safety` (2 s), so one that times out leaves room for
/// another whole one before the lease runs out (ADR 0020, section 5,
/// amendment of 2026-11-27).
pub(super) fn renewals(heartbeat: Duration, lease: &LeaseConfig) -> Renewals {
    Renewals::new(
        lease,
        u64::try_from(heartbeat.as_millis()).unwrap_or(u64::MAX),
    )
}

/// The spacing of [`renewals`]: how long after the last acknowledged lease
/// write the next is sent, and between two tries of the first write.
pub(super) fn lease_every(heartbeat: Duration, lease: &LeaseConfig) -> Duration {
    Duration::from_millis(renewals(heartbeat, lease).spacing_ms)
}

/// How long one lease write may take: a third of `ttl - safety` (2.7 s with
/// the defaults), so three attempts fit in the lease's validity.
pub(crate) fn lease_attempt(lease: &LeaseConfig) -> Duration {
    Duration::from_millis(lease.ttl_ms().saturating_sub(lease.safety_ms()) / 3)
}

/// How long one read of a peer's heartbeat may take (ADR 0020, amendment
/// of 2026-11-20): the lease attempt (2.67 s with the defaults), at most a
/// heartbeat period but never under 1 s. Within the horizon of three
/// periods plus the skew, a read issued that long before a peer's trust
/// ends still renews it when that peer's own write took up to the lease
/// attempt: 3 s of period, 2.67 s of write and 2.67 s of read fit in 10 s
/// less the 1 s of skew.
pub(crate) fn heartbeat_read_attempt(heartbeat: Duration, lease: &LeaseConfig) -> Duration {
    lease_attempt(lease).min(heartbeat.max(Duration::from_secs(1)))
}

/// Checks the bucket and this node id, writes the node lease's first
/// `live` write, then renews it as [`renewals`] says, from the last
/// acknowledged write, until it is lost or a clean stop asks for its
/// release.
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
    let renewals = c.renewals;
    drop(c);
    // When (this node's clock, ms) the next renewal is due, made an
    // instant of the runtime's: one already due goes at once.
    let at = |due_ms: u64| {
        tokio::time::Instant::now() + Duration::from_millis(due_ms.saturating_sub(now_ms()))
    };
    // The first renewal a spacing after the first write. One the floor's
    // read pushed past its time is sent at once.
    let mut next = at(renewals.next_ms(lease.written_at_ms(), now_ms(), now_ms(), false));
    // The upgrade floor, read again now that this node is live: a floor
    // written during the start check (when no listing could see this node)
    // stops it before it serves. Refused only on an answer; without one
    // the node stays checking (no acquisition waits less), its lease
    // renewed each period, and the floor is read again after each renewal.
    let mut floor_read = false;
    loop {
        if !floor_read {
            let Some(c) = cluster.upgrade() else {
                return;
            };
            // Never past a renewal that is due: the read is dropped, made
            // again after the renewal.
            let read = tokio::select! {
                read = c.read_floor() => Some(Some(read)),
                _ = stop.wait_for(|s| *s) => None,
                () = tokio::time::sleep_until(next) => Some(None),
            };
            match read {
                // A clean stop meanwhile: release what was written.
                None => {
                    c.release_node_lease(&mut lease).await;
                    return;
                }
                Some(None) => {}
                Some(Some(read)) => match read {
                    super::identity::FloorRead::Allowed(_) => {
                        floor_read = true;
                        // A verdict reached meanwhile (a twin found by the
                        // listing) stands: Unique only over Checking.
                        let unique = c.identity.send_if_modified(|s| {
                            let checking = *s == Identity::Checking;
                            if checking {
                                *s = Identity::Unique;
                            }
                            checking
                        });
                        if !unique || c.identity_taken() {
                            c.invalidate_node_lease(false);
                            return;
                        }
                        tracing::info!("{}", super::identity::checked_message(&c.node_id));
                    }
                    super::identity::FloorRead::Refused(why) => {
                        tracing::error!("{why}");
                        // It held nothing: acquisitions wait for the verdict.
                        c.release_node_lease(&mut lease).await;
                        c.identity.send_replace(Identity::BelowFloor(why));
                        return;
                    }
                    super::identity::FloorRead::NoAnswer(e) => {
                        tracing::warn!(error = %e, "reading the app's upgrade floor after the first lease write failed; renewing the lease and reading it again");
                    }
                },
            }
        }
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
        let sent_ms = now_ms();
        // A clean stop asked while the write is in flight makes the lease
        // locally invalid at once, lets the write finish (it is not
        // aborted, and gives the version `released` conditions on), and
        // publishes nothing from it (ADR 0020, section 4).
        let mut stopping = false;
        let sent = std::time::Instant::now();
        let attempt = {
            let limit =
                Duration::from_millis(renewals.attempt_for(lease.valid_until_ms(), sent_ms));
            let attempt = tokio::time::timeout(limit, lease.renew());
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
        // The writer's side of how late peers see this heartbeat; a write
        // that timed out counts at the attempt.
        crate::metrics::METRICS
            .node_lease_write_ms
            .observe(sent.elapsed());
        // The next one from the last acknowledged write's date: a spacing
        // after it, at once after an adopted older write of this process
        // or a timeout, soon after a quick error.
        next = at(renewals.next_ms(
            lease.written_at_ms(),
            sent_ms,
            now_ms(),
            !matches!(attempt, Ok(Ok(()))),
        ));
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
    /// The bucket check, the upgrade floor, the start check, then the
    /// first write (the floor is read again once it landed, in
    /// [`node_lease_loop`]). A first
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
        // Before any write: a binary below the app's upgrade floor never
        // writes a lease, so it never holds or serves anything.
        let floor = match self.check_floor().await {
            Ok(floor) => floor,
            Err(why) => {
                tracing::error!("{why}");
                self.identity.send_replace(Identity::BelowFloor(why));
                return false;
            }
        };
        loop {
            // The start guard and the start check run together (ADR 0020,
            // section 6): a new node serves about 15 s after it starts when
            // the old nodes stopped at least 10 s before.
            let (guard, versions, checked) = tokio::join!(
                self.start_guard(),
                self.version_guard(floor),
                self.check_identity()
            );
            // A live node this version cannot run beside (ADR 0043,
            // section 5): this node writes no lease.
            let listed = match versions {
                Ok(listed) => listed,
                Err(why) => {
                    tracing::error!("{why}");
                    self.identity.send_replace(Identity::VersionSkew(why));
                    return false;
                }
            };
            let checked = match guard {
                Ok(()) => checked,
                Err(why) => (Stopped::OlderNode.identity(&why), None, None),
            };
            let expected = match checked {
                (Identity::Unique, expected, replaced) => {
                    // The nodes listed again, right before the write.
                    if let Some(why) = self.version_recheck(&listed.0, listed.1).await {
                        tracing::error!("{why}");
                        self.identity.send_replace(Identity::VersionSkew(why));
                        return false;
                    }
                    // Before the write: the twin check reads it only once
                    // a write landed, and that write is this one, made
                    // over this version (a failed one comes back here).
                    *self.replaced.lock().unwrap_or_else(|p| p.into_inner()) = replaced;
                    expected
                }
                (verdict, _, _) => {
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
            // Bounded as a renewal: a first write slower than that leaves
            // the next renewal too little of the lease.
            let attempt = Duration::from_millis(self.renewals.attempt_ms);
            match tokio::time::timeout(attempt, lease.start(expected)).await {
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
        if self.node_lease_valid() {
            self.lease_valid_again();
        }
        self.heartbeat_reached_bucket(now_ms());
    }

    /// Publishes until when this node's lease is valid. If it had run out
    /// since the last write (the bucket unreachable for longer than `ttl -
    /// safety`, no conflict seen), every hold ends first, each into the
    /// pending releases, and none is valid again (ADR 0020, section 1):
    /// the actors are acquired afresh, at new generations.
    fn publish_validity(&self, lease: &NodeLease<ObjectStoreBucket, SystemClock>) {
        // Not valid until now (the first write, or a lapse: a node paused or
        // cut off): a node following its app checks for its tombstone again
        // before it acquires (ADR 0028). Re-armed before the new validity,
        // and its epoch, are published.
        if !self.validity.valid() {
            self.rearm_app_check();
        }
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
        // No armed idle-pass continuation outlives a lapse (ADR 0042): a
        // release retried in the background lands, if at all, under
        // another epoch. One already queued stays; its pass's version
        // check drops it if the record changed.
        self.idle_passes.end_all_armed();
    }

    /// Makes this node's lease invalid here at once: every hold ends (as
    /// pending releases if `release`; dropped otherwise, since a lost lease
    /// and a clean stop's `released` free the records), and none is valid
    /// again.
    pub(super) fn invalidate_node_lease(&self, release: bool) {
        self.validity.invalidate();
        self.pending.end_all(release);
        self.idle_passes.end_all_armed();
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
            Stopped::Forgotten => {
                "`wasmkeep cluster forget` replaced it with its mark; this process stops, and \
                 a restart writes it again once that command is done"
            }
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
