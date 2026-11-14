//! A node on node leases ([ADR 0020]) as the server runs one, for the
//! simulations. As in the server, the lease runs on a task of its own
//! ([`LeaseTask`], spawned by the simulation): its renewals (conditional
//! writes through [`NodeLease`]), the validity its holds act by
//! ([`Validity`], the server's own rule: a lapse ends every hold into the
//! pending releases, a lost lease drops them), fencing stale peers
//! ([`FenceWatch`], [`fence`]), and pending releases and unresolved acquires
//! retried with [`holds::retry`]. The actor workload runs on the node's
//! main task with a [`LeaseNode`]: [`LeaseNode::acquire`],
//! [`LeaseNode::release`], and [`LeaseNode::may_act`] wherever the server
//! asks `LeaseView::is_valid`. A renewal can time out, a lease lapse, a
//! release land and another node take the actor while the workload's own
//! upload is still in flight, as in production. A hold may also be parked
//! ([`LeaseNode::park`], ADR 0024: an actor hibernating with sockets open)
//! in the server's parked set ([`Parked`]), woken by an acquisition and
//! released by [`LeaseNode::release_parked`], a lapse or a clean stop.
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use wasmkeep_cluster::holds::{self, Parked, PendingEntry, Validity};
use wasmkeep_cluster::lease::{Bucket, Clock, LeaseConfig, Version};
use wasmkeep_cluster::node_lease::{
    AcquireError, Fence, FenceWatch, Hold, NodeLease, NodeLeaseError, NodeLeaseObject, NodeState,
    Owners, Released, fence,
};

use crate::driver::Env;

/// What a node is set up with.
#[derive(Debug, Clone)]
pub struct NodeSetup {
    /// Its id (`nodes/<id>`).
    pub id: String,
    /// The other nodes' ids, watched for fencing.
    pub peers: Vec<String>,
    /// The prefix of node lease objects, ending in `/`.
    pub nodes_prefix: String,
    /// The lease timing.
    pub config: LeaseConfig,
    /// One lease write's limit, ms: a third of `ttl - safety`, as the
    /// server's lease task bounds it. The lease and peers clients should
    /// time out after it; the fence watch counts it.
    pub attempt_ms: u64,
    /// Rules deliberately broken, for the simulations' broken variants
    /// (all off: the server's rules).
    pub breaks: Breaks,
}

/// Rules of the node a simulation may break, to show its checks catch it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Breaks {
    /// A due fence overwrites the peer's object whatever is there when it
    /// writes (it reads again right before, after a pause, and replaces
    /// that version): a renewal landing in between is overwritten.
    pub unconditional_fence: bool,
    /// The resolver releases what an unresolved acquire wrote even while a
    /// hold of the actor lives here.
    pub resolve_while_held: bool,
    /// A lapse ends no hold: holds stay through it and act again once the
    /// lease is valid, whatever epoch they were acquired in.
    pub epoch_ignored: bool,
    /// A retry pass's entries are not counted while they are out of the
    /// pending set: nothing reads as pending meanwhile.
    pub pending_uncounted: bool,
    /// A wake of a parked actor takes the parked hold back without
    /// writing the record (keeping ownership across hibernation, ADR
    /// 0020's rejected first draft): the woken hold shares its generation.
    pub wake_reuses_parked: bool,
    /// A lapse forgets the parked holds instead of making them pending
    /// releases: their records keep naming this node with nothing here to
    /// release them.
    pub lapse_forgets_parked: bool,
    /// The resolver counts a parked hold as held here at any generation,
    /// not only at its own: an unresolved wake that landed is skipped.
    pub resolver_ignores_parked_generation: bool,
    /// A wake whose outcome is unknown drops the parked hold.
    pub unknown_wake_drops_parked: bool,
    /// The resolver ends an unresolved acquire on its first read of an
    /// unchanged record (no wait, ADR 0020 before its amendment): a write
    /// landing after that read leaves the record naming this node with
    /// nothing here to release it.
    pub resolver_ends_unchanged: bool,
    /// The resolver waits for an unresolved acquire's unchanged record
    /// without a bound: an entry whose write never lands stays, and a
    /// drain never ends.
    pub resolver_never_ends: bool,
}

/// How long the resolver waits for an unresolved acquire's unchanged record
/// (ms; the server waits [`wasmkeep_cluster::node_lease::UNRESOLVED_WAIT_MS`]):
/// the simulated bucket applies a slow request up to 2.5 s after it
/// arrives (`Faults::slow_one_in`), and the entry is made only after the
/// write's own timeout and a read back; half a second more for margin.
pub const UNRESOLVED_WAIT_MS: u64 = 3_000;

/// A hold an actor workload keeps: what [`LeaseNode::may_act`] checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    /// The record's key.
    pub key: String,
    /// The hold's own generation.
    pub generation: u64,
    /// The epoch it was acquired in.
    pub epoch: u64,
}

/// What the lease task and the workload share.
#[derive(Debug, Default)]
struct Shared {
    validity: Validity,
    /// Live holds by record key.
    holds: HashMap<String, Hold>,
    /// Holds parked by hibernations with sockets open (ADR 0024).
    parked: Parked<()>,
    pending: Vec<PendingEntry>,
    /// When each unresolved acquire (key, generation) was last made, for
    /// [`LeaseNode::overdue_unresolved`]: by the node's own count, not the
    /// entry's deadline, which a broken wait may not bound.
    unresolved_since: HashMap<(String, u64), u64>,
    /// Releases sent and retry passes under way: their entries are in
    /// neither `holds` nor `pending` meanwhile, and the set must not read
    /// as empty (the server's `InFlight`).
    in_flight: u32,
    /// Actors whose record an acquisition or the resolver is working on:
    /// the server's lock per actor. Taken by either side for its whole
    /// read and write; the other side backs off.
    acquiring: HashSet<String>,
    /// The lease was lost: the process exits.
    lost: bool,
    /// A clean stop asked the lease task to write `released`.
    stop: bool,
    /// The lease task has ended.
    stopped: bool,
    fences: u64,
}

thread_local! {
    /// Fences written by any node in this simulation (a simulation runs on
    /// one thread): what [`take_fences`] returns.
    static FENCES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The fences nodes wrote in this simulation so far, and a reset: for
/// checks that takeovers by fence happened, so a pass is not vacuous.
pub fn take_fences() -> u64 {
    FENCES.with(|f| f.replace(0))
}

thread_local! {
    /// Times a hold from before a lapse was allowed to act after it
    /// ([`Breaks::epoch_ignored`]): what [`take_old_epoch_acts`] returns.
    static OLD_EPOCH_ACTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How often in this simulation so far a hold acquired before a lapse was
/// allowed to act after the lease was valid again (the epoch rule broken),
/// and a reset: so a run of that variant shows it reached its case.
pub fn take_old_epoch_acts() -> u64 {
    OLD_EPOCH_ACTS.with(|k| k.replace(0))
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The actor workload's side of a node on node leases.
#[derive(Debug)]
pub struct LeaseNode<B, C> {
    setup: NodeSetup,
    clock: C,
    owners: Owners<B, C>,
    shared: Arc<Mutex<Shared>>,
}

/// The node's lease, on a task of its own: spawn [`LeaseTask::run`].
#[derive(Debug)]
pub struct LeaseTask<B, C> {
    setup: NodeSetup,
    clock: C,
    lease: NodeLease<B, C>,
    owners: Owners<B, C>,
    peers_bucket: B,
    watch: FenceWatch,
    shared: Arc<Mutex<Shared>>,
}

/// The buckets a node uses, one client each: its lease's, the lease
/// task's ownership records (pending retries), the peers' reads and
/// fences, and the workload's ownership records.
#[derive(Debug)]
pub struct NodeBuckets<B> {
    /// The node lease's writes.
    pub lease: B,
    /// The lease task's record writes (pending releases, resolutions).
    pub task_records: B,
    /// Reads of peers' lease objects, and fences.
    pub peers: B,
    /// The workload's record writes (acquire, release).
    pub records: B,
}

impl<B: Bucket, C: Clock + Copy> LeaseNode<B, C> {
    /// Starts a process with `instance`: the start check (the object read
    /// for ttl plus grace, absent or unchanged; a fence or release naming
    /// the instance first read ends the wait once its `written_at` is that
    /// old), then the first write, conditioned on the version read last; a
    /// first write that fails goes back to the check. Returns the workload's
    /// side and the lease task, to spawn.
    pub async fn start(
        setup: NodeSetup,
        instance: &str,
        clock: C,
        buckets: NodeBuckets<B>,
        payload: Vec<u8>,
        env: &impl Env,
    ) -> (Self, LeaseTask<B, C>) {
        let key = format!("{}{}", setup.nodes_prefix, setup.id);
        let cfg = setup.config;
        let mut lease = NodeLease::new(buckets.lease, clock, &key, instance, cfg);
        lease.set_payload(payload);
        loop {
            let expected = start_check(&buckets.peers, &clock, &key, &cfg, env).await;
            if lease.start(expected.as_ref()).await.is_ok() {
                break;
            }
        }
        let wait = if setup.breaks.resolver_ends_unchanged {
            0
        } else if setup.breaks.resolver_never_ends {
            u64::MAX
        } else {
            UNRESOLVED_WAIT_MS
        };
        let owners = |b| {
            Owners::new(b, clock, &setup.id, instance, &setup.nodes_prefix, cfg)
                .with_unresolved_wait_ms(wait)
        };
        let mut shared = Shared::default();
        let _ = shared
            .validity
            .publish(lease.valid_until_ms(), clock.now_ms());
        let shared = Arc::new(Mutex::new(shared));
        let task = LeaseTask {
            setup: setup.clone(),
            clock,
            watch: FenceWatch::new(&cfg, setup.attempt_ms),
            owners: owners(buckets.task_records),
            peers_bucket: buckets.peers,
            lease,
            shared: Arc::clone(&shared),
        };
        let node = Self {
            owners: owners(buckets.records),
            setup,
            clock,
            shared,
        };
        (node, task)
    }

    /// This node's id.
    pub fn id(&self) -> &str {
        &self.setup.id
    }

    /// This node's clock.
    pub fn clock(&self) -> &C {
        &self.clock
    }

    /// Until when (this node's clock) its lease is valid, if it is.
    pub fn valid_until_ms(&self) -> Option<u64> {
        lock(&self.shared).validity.until_ms
    }

    /// Whether the lease was lost: the process exits.
    pub fn lost(&self) -> bool {
        lock(&self.shared).lost
    }

    /// Fences this node wrote over peers.
    pub fn fences(&self) -> u64 {
        lock(&self.shared).fences
    }

    /// Whether the actor workload may act on `held` now: the hold still
    /// lives here, and it may act in its epoch (what the server's
    /// `LeaseView::is_valid` asks).
    pub fn may_act(&self, held: &Held) -> bool {
        let s = lock(&self.shared);
        let now = self.clock.now_ms();
        s.holds
            .get(&held.key)
            .is_some_and(|h| h.generation == held.generation)
            && if self.setup.breaks.epoch_ignored {
                let valid = s.validity.valid(now);
                if valid && held.epoch != s.validity.epoch {
                    OLD_EPOCH_ACTS.with(|k| k.set(k.get() + 1));
                }
                valid
            } else {
                s.validity.valid_in(held.epoch, now)
            }
    }

    /// Whether `held` still lives on this node (not released, not ended by
    /// a lapse, not lost), whether or not it may act now: an actor stays
    /// resident on such a hold and asks [`Self::may_act`] each time.
    pub fn still_held(&self, held: &Held) -> bool {
        lock(&self.shared)
            .holds
            .get(&held.key)
            .is_some_and(|h| h.generation == held.generation)
    }

    /// Whether the node lease alone is valid now, whatever happened to the
    /// holds (for broken variants that ignore a hold's end: never what the
    /// server asks before acting).
    pub fn lease_valid(&self) -> bool {
        lock(&self.shared).validity.valid(self.clock.now_ms())
    }

    /// Acquires the record at `key` (generation + 1, or `first_generation`
    /// for a new record), only while a hold of the current epoch may act,
    /// asked right before the write. An outcome unknown is kept as an
    /// unresolved acquire.
    pub async fn acquire(&self, key: &str, first_generation: u64) -> Result<Held, AcquireError> {
        let owners = &self.owners;
        self.acquire_via(key, async |valid: &dyn Fn() -> bool| {
            owners
                .acquire(key, valid, async || Ok(first_generation))
                .await
        })
        .await
    }

    /// [`Self::acquire`] with the record's read and write done by `write`
    /// (given the question to ask right before writing): for broken
    /// variants of the acquisition; the lock, the epoch and the pending
    /// set as for the server's.
    pub async fn acquire_via(
        &self,
        key: &str,
        write: impl AsyncFnOnce(&dyn Fn() -> bool) -> Result<Hold, AcquireError>,
    ) -> Result<Held, AcquireError> {
        let ignored = self.setup.breaks.epoch_ignored;
        let epoch = {
            let mut s = lock(&self.shared);
            // The resolver holds this actor (the server's lock per actor):
            // back off, as an acquisition waiting for that lock would.
            if !s.acquiring.insert(key.to_owned()) {
                return Err(AcquireError::Unavailable);
            }
            s.validity.epoch
        };
        let shared = &self.shared;
        let clock = self.clock;
        let valid = || {
            let s = lock(shared);
            if ignored {
                s.validity.valid(clock.now_ms())
            } else {
                s.validity.valid_in(epoch, clock.now_ms())
            }
        };
        let got = write(&valid).await;
        let mut s = lock(&self.shared);
        s.acquiring.remove(key);
        // A parked hold of this actor: dropped if the write may have moved
        // the record (the server's rule, `Parked::after_wake`).
        let _ = s.parked.after_wake(key, &got);
        if self.setup.breaks.unknown_wake_drops_parked
            && matches!(got, Err(AcquireError::Unresolved { .. }))
        {
            let _ = s.parked.take(key);
        }
        match got {
            Ok(hold) => {
                let held = Held {
                    key: key.to_owned(),
                    generation: hold.generation,
                    epoch,
                };
                // A lapse during the acquisition: the hold was dead from
                // the start; it is released like the others.
                if s.validity.epoch == epoch || ignored {
                    s.holds.insert(key.to_owned(), hold);
                } else {
                    s.pending.push(PendingEntry::Release(hold));
                }
                Ok(held)
            }
            Err(e) => {
                if let Some(entry) = PendingEntry::unresolved(&self.owners, key, &e) {
                    if let PendingEntry::Unresolved { generation, .. } = &entry {
                        let now = self.clock.now_ms();
                        s.unresolved_since
                            .insert((key.to_owned(), *generation), now);
                    }
                    holds::add(&mut s.pending, entry);
                }
                Err(e)
            }
        }
    }

    /// Wakes the parked actor at `key` as an event would: an acquisition
    /// ([`Self::acquire`]), which writes the record at a new generation.
    /// With [`Breaks::wake_reuses_parked`], a parked hold is taken back
    /// instead, unwritten, in the current epoch.
    pub async fn wake(&self, key: &str, first_generation: u64) -> Result<Held, AcquireError> {
        if self.setup.breaks.wake_reuses_parked {
            let mut s = lock(&self.shared);
            if let Some((hold, ())) = s.parked.take(key) {
                let held = Held {
                    key: key.to_owned(),
                    generation: hold.generation,
                    epoch: s.validity.epoch,
                };
                s.holds.insert(key.to_owned(), hold);
                return Ok(held);
            }
        }
        self.acquire(key, first_generation).await
    }

    /// Parks `held` (ADR 0024): it stops acting at once, and its record
    /// keeps naming this node, unwritten. In one step under the node's
    /// lock, and only if it may still act in its epoch; otherwise it is
    /// released as [`Self::release`] does. `true` if parked.
    pub async fn park(&self, held: &Held) -> bool {
        {
            let mut s = lock(&self.shared);
            let Some(hold) = s.holds.remove(&held.key) else {
                return false;
            };
            if s.validity.valid_in(held.epoch, self.clock.now_ms()) {
                if let Some(earlier) = s.parked.park(hold, ()) {
                    s.pending.push(PendingEntry::Release(earlier));
                }
                return true;
            }
            s.holds.insert(held.key.clone(), hold);
        }
        self.release(held).await;
        false
    }

    /// Whether an unresolved acquire waits in the pending set more than
    /// `slack_ms` past [`UNRESOLVED_WAIT_MS`] after it was made (this
    /// node's clock) while the lease is valid: the resolver should have
    /// ended it on its first pass after its deadline.
    pub fn overdue_unresolved(&self, slack_ms: u64) -> bool {
        let now = self.clock.now_ms();
        let s = lock(&self.shared);
        s.validity.valid(now)
            && s.pending.iter().any(|e| match e {
                PendingEntry::Unresolved {
                    key, generation, ..
                } => s
                    .unresolved_since
                    .get(&(key.clone(), *generation))
                    .is_some_and(|since| since.saturating_add(UNRESOLVED_WAIT_MS + slack_ms) < now),
                PendingEntry::Release(_) => false,
            })
    }

    /// Whether the record at `key` is parked here.
    pub fn is_parked(&self, key: &str) -> bool {
        lock(&self.shared).parked.contains(key)
    }

    /// A probe of this node's state for `key`, for checks run from other
    /// nodes' tasks: (parked here, lease valid now, its epoch).
    pub fn probe(&self, key: &str) -> impl Fn() -> (bool, bool, u64) + Send + Sync + 'static
    where
        C: Send + Sync + 'static,
    {
        let shared = Arc::clone(&self.shared);
        let clock = self.clock;
        let key = key.to_owned();
        move || {
            let s = lock(&shared);
            (
                s.parked.contains(&key),
                s.validity.valid(clock.now_ms()),
                s.validity.epoch,
            )
        }
    }

    /// The validity's epoch now: it moves on every lapse, a lost lease and
    /// a clean stop (the server closes every socket then).
    pub fn epoch(&self) -> u64 {
        lock(&self.shared).validity.epoch
    }

    /// Releases the parked record at `key` (its last socket closed, a
    /// drain): taken out of the parked set under the actor's lock (an
    /// acquisition holding it now: `false`, try later), then released as a
    /// hold is. `true` if nothing is left to do for it.
    pub async fn release_parked(&self, key: &str) -> bool {
        let hold = {
            let mut s = lock(&self.shared);
            if s.acquiring.contains(key) {
                return false;
            }
            let Some((hold, ())) = s.parked.take(key) else {
                return true;
            };
            s.in_flight += 1;
            hold
        };
        let released = self.owners.release(&hold).await;
        let mut s = lock(&self.shared);
        s.in_flight -= 1;
        if released == Released::Pending {
            s.pending.push(PendingEntry::Release(hold));
        }
        true
    }

    /// Gives `held` up: it stops acting at once; the release is sent, and
    /// kept pending if it does not land. `true` if it landed (or was
    /// superseded) now.
    pub async fn release(&self, held: &Held) -> bool {
        let hold = {
            let mut s = lock(&self.shared);
            let Some(hold) = s.holds.remove(&held.key) else {
                return true;
            };
            s.in_flight += 1;
            hold
        };
        let released = self.owners.release(&hold).await;
        let mut s = lock(&self.shared);
        s.in_flight -= 1;
        match released {
            Released::Pending | Released::Unchanged => {
                s.pending.push(PendingEntry::Release(hold));
                false
            }
            Released::Released | Released::Superseded => true,
        }
    }

    /// A clean stop: every hold released, then the lease task makes the
    /// lease invalid here and writes it `released`, which frees whatever
    /// did not land. Waits for the lease task to end.
    pub async fn clean_stop(&self, env: &impl Env) {
        let held: Vec<Hold> = {
            let mut s = lock(&self.shared);
            let mut held: Vec<Hold> = s.holds.drain().map(|(_, h)| h).collect();
            held.extend(s.parked.take_all().into_iter().map(|(h, ())| h));
            held
        };
        for hold in held {
            if self.owners.release(&hold).await == Released::Pending {
                lock(&self.shared).pending.push(PendingEntry::Release(hold));
            }
        }
        lock(&self.shared).stop = true;
        while !lock(&self.shared).stopped {
            env.sleep_ms(50).await;
        }
    }

    /// Another node the record at `key` names, if any (one read, a hint
    /// that grants nothing: `Owners::other_owner`). Every holder counts as
    /// live here; the server asks its membership view.
    pub async fn other_owner(&self, key: &str) -> Result<Option<String>, AcquireError> {
        self.owners.other_owner(key, |_, _| true).await
    }

    /// Whether nothing waits for its outcome and no record is parked (a
    /// drain is drained only then).
    pub fn nothing_pending(&self) -> bool {
        let s = lock(&self.shared);
        s.pending.is_empty() && s.in_flight == 0 && s.parked.is_empty()
    }
}

impl<B: Bucket, C: Clock + Copy> LeaseTask<B, C> {
    /// The lease, until it is lost or a clean stop: renewed every third of
    /// the ttl (from the previous send), its outcome recorded in the
    /// validity (a lapse ends every hold into the pending releases), peers
    /// watched and fenced when due, pending entries tried again.
    pub async fn run(mut self, env: impl Env) {
        let period = self.setup.config.ttl_ms() / 3;
        loop {
            let sent = env.global_ms();
            if lock(&self.shared).stop {
                return self.stop().await;
            }
            if matches!(self.lease.renew().await, Err(NodeLeaseError::Lost(_))) {
                let mut s = lock(&self.shared);
                s.validity.invalidate();
                s.holds.clear();
                let _ = s.parked.take_all();
                s.pending.clear();
                s.lost = true;
                s.stopped = true;
                return;
            }
            {
                let mut s = lock(&self.shared);
                let until = self.lease.valid_until_ms();
                if s.validity.publish(until, self.clock.now_ms())
                    && !self.setup.breaks.epoch_ignored
                {
                    let ended: Vec<Hold> = s.holds.drain().map(|(_, h)| h).collect();
                    s.pending
                        .extend(ended.into_iter().map(PendingEntry::Release));
                    let parked = s.parked.take_all();
                    if !self.setup.breaks.lapse_forgets_parked {
                        s.pending
                            .extend(parked.into_iter().map(|(h, ())| PendingEntry::Release(h)));
                    }
                }
            }
            self.fence_stale_peers(&env).await;
            self.retry_pending().await;
            let next = sent.saturating_add(period);
            env.sleep_ms(next.saturating_sub(env.global_ms())).await;
        }
    }

    async fn stop(mut self) {
        {
            let mut s = lock(&self.shared);
            s.validity.invalidate();
            s.holds.clear();
            let parked = s.parked.take_all();
            s.pending
                .extend(parked.into_iter().map(|(h, ())| PendingEntry::Release(h)));
        }
        self.retry_pending_while_stopping().await;
        let _ = self.lease.release().await;
        lock(&self.shared).stopped = true;
    }

    async fn fence_stale_peers(&mut self, env: &impl Env) {
        let cfg = self.setup.config;
        let until = lock(&self.shared).validity.until_ms;
        self.watch.own_lease(until, self.clock.now_ms());
        for peer in self.setup.peers.clone() {
            let key = format!("{}{peer}", self.setup.nodes_prefix);
            let seen = self
                .peers_bucket
                .get(&key)
                .await
                .ok()
                .flatten()
                .and_then(|(body, _)| NodeLeaseObject::decode(&body));
            match seen.filter(|o| o.state == NodeState::Live) {
                Some(o) => {
                    let Some(instance) = o.instance.clone() else {
                        continue;
                    };
                    let due =
                        self.watch
                            .observe(&peer, &instance, o.written_at_ms, self.clock.now_ms());
                    // Only with room left in this node's own lease for the
                    // fence's read and write (the server's rule).
                    let now = self.clock.now_ms();
                    let room = lock(&self.shared)
                        .validity
                        .until_ms
                        .is_some_and(|u| u.saturating_sub(now) >= self.setup.attempt_ms);
                    let fenced = due
                        && room
                        && if self.setup.breaks.unconditional_fence {
                            self.overwrite_fenced(env, &key, &o).await
                        } else {
                            fence(
                                &self.peers_bucket,
                                &self.clock,
                                &cfg,
                                &key,
                                &instance,
                                o.written_at_ms,
                            )
                            .await
                                == Fence::Fenced
                        };
                    if fenced {
                        lock(&self.shared).fences += 1;
                        FENCES.with(|f| f.set(f.get() + 1));
                    }
                }
                None => self.watch.forget(&peer),
            }
        }
    }

    /// The broken fence: after a pause, `seen` marked fenced over whatever
    /// the object is by then.
    async fn overwrite_fenced(&self, env: &impl Env, key: &str, seen: &NodeLeaseObject) -> bool {
        env.sleep_ms(self.setup.attempt_ms).await;
        let Ok(Some((_, now))) = self.peers_bucket.get(key).await else {
            return false;
        };
        let body = NodeLeaseObject {
            state: NodeState::Fenced,
            ..seen.clone()
        }
        .encode();
        self.peers_bucket.replace(key, body, &now).await.is_ok()
    }

    async fn retry_pending(&mut self) {
        let entries = {
            let mut s = lock(&self.shared);
            if !s.validity.valid(self.clock.now_ms()) || s.pending.is_empty() {
                return;
            }
            // For the whole pass: the entries are out of the set while
            // they are tried.
            if !self.setup.breaks.pending_uncounted {
                s.in_flight += 1;
            }
            std::mem::take(&mut s.pending)
        };
        for entry in entries {
            if !lock(&self.shared).validity.valid(self.clock.now_ms()) {
                lock(&self.shared).pending.push(entry);
                continue;
            }
            // An unresolved acquire is resolved holding its actor's lock,
            // from the read to the release; an acquisition holding it now:
            // later.
            let key = entry.key().to_owned();
            let locked = matches!(entry, PendingEntry::Unresolved { .. });
            let tried = match &entry {
                PendingEntry::Unresolved { generation, .. } => Some(*generation),
                PendingEntry::Release(_) => None,
            };
            if locked && !lock(&self.shared).acquiring.insert(key.clone()) {
                lock(&self.shared).pending.push(entry);
                continue;
            }
            let shared = &self.shared;
            let held_here = !self.setup.breaks.resolve_while_held;
            let any_generation = self.setup.breaks.resolver_ignores_parked_generation;
            let still = holds::retry(&self.owners, entry, |k, generation| {
                let s = lock(shared);
                let parked = if any_generation {
                    s.parked.contains(k)
                } else {
                    s.parked.holds_at(k, generation)
                };
                held_here && (s.holds.contains_key(k) || parked)
            })
            .await
            .still();
            // An unresolved acquire that ended: a hold parked at an
            // earlier generation is forgotten if the record no longer names
            // it (the server's `forget_parked_behind`).
            if let (None, Some(tried)) = (&still, tried) {
                let parked = lock(&self.shared).parked.generation(&key);
                if let Some(g) = parked.filter(|g| *g < tried)
                    && matches!(self.owners.still_names_this(&key, g).await, Ok(false))
                {
                    let _ = lock(&self.shared).parked.take_at(&key, g);
                }
            }
            let mut s = lock(&self.shared);
            if locked {
                s.acquiring.remove(&key);
            }
            if let Some(entry) = still {
                holds::add(&mut s.pending, entry);
            }
        }
        if !self.setup.breaks.pending_uncounted {
            lock(&self.shared).in_flight -= 1;
        }
    }

    /// A clean stop's last tries of the pending releases, without asking
    /// the validity (already invalidated): the `released` write that
    /// follows frees whatever does not land anyway.
    async fn retry_pending_while_stopping(&mut self) {
        let entries = std::mem::take(&mut lock(&self.shared).pending);
        for entry in entries {
            let _ = holds::retry(&self.owners, entry, |_, _| false).await;
        }
    }
}

/// The start check (see [`LeaseNode::start`]): the version to condition the
/// first write on, `None` to create the object.
async fn start_check<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    key: &str,
    cfg: &LeaseConfig,
    env: &impl Env,
) -> Option<Version> {
    let wait = cfg.ttl_ms() + cfg.grace_ms();
    loop {
        let first = match bucket.get(key).await {
            Ok(first) => first,
            Err(_) => {
                env.sleep_ms(100).await;
                continue;
            }
        };
        let first_live = first
            .as_ref()
            .and_then(|(b, _)| NodeLeaseObject::decode(b))
            .filter(|o| o.state == NodeState::Live)
            .and_then(|o| o.instance);
        env.sleep_ms(wait).await;
        match (first, bucket.get(key).await) {
            (None, Ok(None)) => return None,
            (Some((was, _)), Ok(Some((now, version)))) if now == was => return Some(version),
            // A fence or a release over the process first read: it lapsed
            // once its kept written_at is ttl plus grace old.
            (Some(_), Ok(Some((now, version)))) => {
                if let Some(o) = NodeLeaseObject::decode(&now)
                    && o.state != NodeState::Live
                    && first_live.is_some()
                    && o.instance == first_live
                {
                    let stale = o.written_at_ms.saturating_add(wait);
                    let left = stale.saturating_sub(clock.now_ms());
                    env.sleep_ms(left).await;
                    if matches!(bucket.get(key).await, Ok(Some((again, _))) if again == now) {
                        return Some(version);
                    }
                }
            }
            _ => {}
        }
    }
}
