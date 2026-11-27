//! Released node leases removed by the live nodes ([ADR 0020], amendment
//! of 2026-11-27): no operator runs `wasmkeep cluster forget` for them.
//!
//! One node of each app removes: the lowest-id live node that serves it
//! (the server chooses it as idle expiry chooses its collector). Every
//! round ([`round_every_ms`]) it lists the keys of `nodes/` (keys and
//! last-modified times, no bodies) and reads only the keys it has not read
//! at that time yet and that were not written lately, the most recently
//! written first, at most [`READS_PER_ROUND`] per round. A key it read
//! stays judged until the listing shows it written again.
//!
//! - A **mark** dated more than [`stale_mark_ms`] ago on this node's clock
//!   was left by a removal that did not finish (a remover or a `cluster
//!   forget` that stopped between its two requests, or whose delete was
//!   given up): it is deleted, read right before the delete and only if it
//!   is still the very mark read (its version), with a deadline taken
//!   before that read ([`crate::node_lease::forget_finish_mark`]). That age
//!   is longer than any remover sends its own delete for
//!   ([`forget_delete_bound_ms`](crate::node_lease::forget_delete_bound_ms)).
//! - A **released** lease of its app whose `written_at` is older than the
//!   removal period (24 h by default, never less than the window `cluster
//!   forget` itself waits) is removed with the command's steps after the
//!   finalize: the lease read with its version; the gate of the mark
//!   ([`crate::upgrade::forget_gate`]: the app's floor at `0.9` or later,
//!   no live older node), once a round; no ownership record of any app
//!   under the prefix naming the node, one scan for every due lease not
//!   already cleared at its version; then the mark written `If-Match` on
//!   that version and deleted ([`crate::node_lease::forget_released`]).
//!   Two differences from the command: the gate is read before the
//!   records (the command reads the records first), and a migration
//!   tombstone under `owners/` names nobody (the command counts any object
//!   it cannot read as a record as naming the node). When the gate blocks
//!   nothing is written: never the delete without a condition that the
//!   command falls back to.
//!
//! Bounds: [`MAX_PER_ROUND`] removals and ends of marks together, oldest
//! first, within one round period of time, and only while `go_on` says
//! this node is still the remover. A lease a record names, and every
//! lease of a round the gate blocked, is held back for [`HOLD_BACK_MS`]
//! from the end of that read; a failed request stops the round and holds
//! the due leases of the next ones back (a round, then two, then four at
//! most), while the listing and the end of marks go on every round. A
//! lease the scan cleared keeps that until its version changes: a record
//! written since names a process that rewrote the lease first, which the
//! conditional removal then refuses.
//!
//! Two removers at once (two nodes that each take themselves for the
//! lowest, for a heartbeat period) are safe through the conditional write
//! alone: the second one's write fails on the version the first one
//! replaced. Only a mark's delete can be sent twice: a remover whose
//! delete got no answer in time and that ends its own mark a round later,
//! or two removers (two apps on one prefix) ending one mark that stayed.
//! A remover held between its read and its delete sends nothing past its
//! deadline. What is left is a delete already sent that the network holds
//! past the mark's age: a store's DELETE takes no condition here
//! (`object_store` 0.14.2 has no conditional delete).
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::control::Keys;
use crate::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use crate::membership::{Directory, NODES_PREFIX, NodeInfo};
use crate::node_lease::{
    Forgot, NodeState, OwnerObject, forget_finish_mark, forget_released, forgotten,
};
use crate::store::Store;

/// The removal period by default: a released lease is removed once it is
/// a day old.
pub const DEFAULT_AFTER_MS: u64 = 24 * 60 * 60 * 1000;

/// Leases removed and marks ended per round, at most, together: the rest
/// wait for the next round.
pub const MAX_PER_ROUND: usize = 16;

/// Keys of `nodes/` read per round, at most: a first round over many
/// released leases reads them over several rounds.
pub const READS_PER_ROUND: usize = 64;

/// How long a lease kept because a record names its node, or every lease
/// of a round the gate of the mark blocked, waits before it is tried
/// again (ms): ten minutes. Until then it takes no place in a round and
/// costs no read of the records or of the gate.
pub const HOLD_BACK_MS: u64 = 10 * 60 * 1000;

/// How often the remover runs a round: a lease's ttl plus grace (15 s at
/// the defaults). A round with nothing to do is one listing of `nodes/`,
/// keys only.
pub fn round_every_ms(config: &LeaseConfig) -> u64 {
    config.ttl_ms().saturating_add(config.grace_ms())
}

/// How old a mark must be, by its own date on this node's clock, before a
/// node ends it: a lease's ttl plus grace plus the skew allowed (16 s at
/// the defaults). Longer than [`forget_delete_bound_ms`](crate::node_lease::forget_delete_bound_ms), after which the
/// removal that wrote it no longer sends its delete, and the skew covers a
/// writer whose clock was behind this one.
pub fn stale_mark_ms(config: &LeaseConfig) -> u64 {
    config
        .ttl_ms()
        .saturating_add(config.grace_ms())
        .saturating_add(config.max_skew_ms())
}

/// The youngest a released lease is removed at, whatever the period: the
/// window of `cluster forget`, the heartbeat horizon plus a lease's ttl
/// and grace (25 s at the defaults).
pub fn least_age_ms(config: &LeaseConfig, horizon_ms: u64) -> u64 {
    horizon_ms
        .saturating_add(config.ttl_ms())
        .saturating_add(config.grace_ms())
}

/// What a remover is given.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// The removal period (ms): a released lease older than this is
    /// removed.
    pub after_ms: u64,
    /// The heartbeat horizon (ms), as the nodes count a heartbeat live.
    pub horizon_ms: u64,
    /// The lease timing.
    pub config: LeaseConfig,
    /// How long a lease a record names, and the leases of a blocked round,
    /// wait (ms): [`HOLD_BACK_MS`].
    pub hold_back_ms: u64,
}

/// Deliberate faults, for a simulation's broken variants only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Fault {
    /// None: the rule.
    #[default]
    None,
    /// The lease is read again and deleted without a condition, as
    /// `cluster forget` did before the mark (and does before the gate
    /// allows it).
    Unmarked,
    /// No ownership record is read.
    RecordsUnchecked,
    /// A mark that stayed is never ended.
    MarksKept,
    /// A mark's delete is sent whenever the remover gets to it, with no
    /// deadline taken before its first request.
    NoDeadline,
}

/// What a key held when it was read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    /// A released lease of `app`, with its date and version.
    Released {
        app: String,
        written_at_ms: u64,
        version: Version,
    },
    /// The mark of a removal, with its date and version.
    Mark { at_ms: u64, version: Version },
    /// Anything else: a live or fenced lease, a heartbeat from before node
    /// leases, an object no version reads.
    Other,
}

fn judge(body: &[u8], version: Version) -> Seen {
    if let Some(at_ms) = forgotten(body) {
        return Seen::Mark { at_ms, version };
    }
    match NodeInfo::decode(body) {
        Some(info) if info.lease == Some(NodeState::Released) => Seen::Released {
            app: info.app,
            written_at_ms: info.written_at_ms,
            version,
        },
        _ => Seen::Other,
    }
}

/// What one round did, by node id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Round {
    /// Released leases removed.
    pub removed: Vec<String>,
    /// Marks that stayed, deleted.
    pub ended: Vec<String>,
    /// Released leases found written since they were read (a node started
    /// under that id): left alone.
    pub changed: Vec<String>,
    /// Marks written whose delete got no answer in time: a later round
    /// ends them.
    pub unfinished: Vec<String>,
    /// Released leases kept because an ownership record names their node
    /// (or a record could not be read).
    pub named: Vec<String>,
    /// Cleared for removal but left for a later round ([`MAX_PER_ROUND`],
    /// the round's time budget, or the remover told to stop).
    pub left: usize,
    /// What blocks the mark for this app, when it does: nothing written.
    pub blocked: Option<String>,
    /// The request that failed, when one did: the round stopped there.
    pub failed: Option<String>,
}

impl Round {
    /// Whether it wrote or deleted nothing.
    pub fn is_idle(&self) -> bool {
        self.removed.is_empty() && self.ended.is_empty() && self.unfinished.is_empty()
    }
}

/// Which nodes the ownership records name.
#[derive(Debug, PartialEq, Eq)]
enum Named {
    /// These, among the ones asked about.
    These(BTreeSet<String>),
    /// A record could not be read: it may name any.
    Any,
}

impl Named {
    fn names(&self, node: &str) -> bool {
        match self {
            Self::These(named) => named.contains(node),
            Self::Any => true,
        }
    }
}

/// Which of `nodes` the ownership records of every app under the prefix
/// name (`state/<tenant>/<app>/owners/...`), any instance: one listing per
/// tenant and per app, one read per record, as `cluster forget` reads
/// them. A migration tombstone names nobody; a record that cannot be read
/// may name any node.
async fn records_naming<B: Store>(
    bucket: &B,
    nodes: &BTreeSet<String>,
) -> Result<Named, BucketError> {
    let mut named = BTreeSet::new();
    for tenant in bucket.list_dirs("state/").await? {
        for app in bucket.list_dirs(&format!("state/{tenant}/")).await? {
            for meta in bucket
                .list(&format!("state/{tenant}/{app}/owners/"))
                .await?
            {
                let Some((body, _)) = bucket.get(&meta.key).await? else {
                    continue;
                };
                match OwnerObject::decode(&body) {
                    None => return Ok(Named::Any),
                    Some(OwnerObject::Tombstone { .. }) => {}
                    Some(OwnerObject::Record(record)) => {
                        if let Some((node, _)) = record.holder
                            && nodes.contains(&node)
                        {
                            named.insert(node);
                        }
                    }
                }
            }
        }
    }
    Ok(Named::These(named))
}

/// The removal before the mark ([`Fault::Unmarked`]): read again, and
/// deleted if unchanged.
async fn delete_unmarked<B: Bucket + Keys>(
    bucket: &B,
    key: &str,
    version: &Version,
) -> Result<Forgot, BucketError> {
    match bucket.get(key).await? {
        Some((body, v))
            if &v == version && matches!(judge(&body, v.clone()), Seen::Released { .. }) =>
        {
            Keys::delete(bucket, key).await.map(|()| Forgot::Removed)
        }
        _ => Ok(Forgot::Changed),
    }
}

/// One node's remover: what it read of `nodes/`, between rounds.
#[derive(Debug, Default)]
pub struct Remover {
    /// Each key read, with the last-modified time it was read at.
    seen: HashMap<String, (u64, Seen)>,
    /// Keys held back until then (this node's clock, ms): a record named
    /// their node, or the gate blocked ([`HOLD_BACK_MS`]).
    held: HashMap<String, u64>,
    /// Released leases the last scan of the records found named by no
    /// record, at the version read before that scan. A record written
    /// since would name a process that rewrote the lease first, so the
    /// conditional removal fails for it: the scan need not be made again
    /// for that version.
    cleared: HashMap<String, Version>,
    /// No round before then (this node's clock, ms), after a failed one.
    paused_until: u64,
    /// Rounds in a row that failed, for the pause after the next.
    failures: u32,
    fault: Fault,
}

impl Remover {
    /// A remover that has read nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// A remover with a deliberate fault: for a simulation's broken
    /// variant only.
    pub fn with_fault(fault: Fault) -> Self {
        Self {
            fault,
            ..Self::default()
        }
    }

    /// Notes a failed request: the round stops, and the due leases of the
    /// next rounds wait a round, then two, then four. The listing and the
    /// end of marks that stayed go on every round.
    fn failed(&mut self, round: &mut Round, why: String, now_ms: u64, settings: &Settings) {
        // At most four rounds: a mark a removal left keeps a node from
        // starting under that id, and a starting node gives up on it after
        // four times ttl plus grace.
        let every = round_every_ms(&settings.config);
        let pause = every
            .saturating_mul(1 << self.failures.min(2))
            .min(settings.hold_back_ms);
        self.failures = self.failures.saturating_add(1);
        self.paused_until = now_ms.saturating_add(pause);
        round.failed = Some(why);
    }

    /// One round for the app `app` (`tenant/app`), this node being its
    /// remover, on this node's `clock`. `give_up` is the timer of a mark's
    /// delete ([`forget_delete_bound_ms`](crate::node_lease::forget_delete_bound_ms)
    /// of `settings`' lease timing), made anew for each removal before its
    /// first request. `go_on` is asked before each removal or end of a
    /// mark: `false` (this node no longer the remover, draining, its lease
    /// ended) stops the round there, as does its time budget (one round
    /// period).
    ///
    /// # Errors
    /// The listing of `nodes/` did not answer; nothing was written. A
    /// later failure stops the round and is in [`Round::failed`]; the next
    /// rounds then wait, longer after each failure in a row.
    pub async fn round<B, C, F>(
        &mut self,
        bucket: &B,
        clock: &C,
        app: &str,
        settings: &Settings,
        give_up: impl Fn() -> F,
        go_on: impl Fn() -> bool,
    ) -> Result<Round, BucketError>
    where
        B: Bucket + Keys + Directory + Store,
        C: Clock,
        F: std::future::Future<Output = ()>,
    {
        let mut round = Round::default();
        let started = clock.now_ms();
        let listed = match Keys::list_keys(bucket, NODES_PREFIX).await {
            Ok(listed) => listed,
            Err(e) => {
                self.failed(
                    &mut round,
                    format!("listing nodes/: {e:?}"),
                    started,
                    settings,
                );
                return Err(e);
            }
        };
        let listed: Vec<(String, u64)> = listed
            .into_iter()
            .filter(|(key, _)| {
                key.strip_prefix(NODES_PREFIX)
                    .is_some_and(|id| !id.is_empty() && !id.contains('/'))
            })
            .collect();
        let keys: HashSet<&str> = listed.iter().map(|(k, _)| k.as_str()).collect();
        self.seen.retain(|key, _| keys.contains(key.as_str()));
        self.held
            .retain(|key, until| keys.contains(key.as_str()) && *until > started);
        self.cleared.retain(|key, _| keys.contains(key.as_str()));
        let recent = stale_mark_ms(&settings.config);

        // The keys to read: not read at their listed time, and not written
        // lately (by the bucket's clock: a lease being renewed or a young
        // mark, read once older). The most recently written first, so a
        // mark is read before the old released leases of a first round.
        let mut unread: Vec<&(String, u64)> = listed
            .iter()
            .filter(|(key, modified)| {
                !self.seen.get(key).is_some_and(|(m, _)| m == modified)
                    && started.saturating_sub(*modified) >= recent
            })
            .collect();
        unread.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for (key, modified) in unread.into_iter().take(READS_PER_ROUND) {
            self.seen.remove(key);
            match bucket.get(key).await {
                Ok(Some((body, version))) => {
                    self.seen
                        .insert(key.clone(), (*modified, judge(&body, version)));
                }
                Ok(None) => {}
                Err(e) => {
                    let now = clock.now_ms();
                    self.failed(&mut round, format!("reading {key}: {e:?}"), now, settings);
                    return Ok(round);
                }
            }
        }
        let id = |key: &str| key.strip_prefix(NODES_PREFIX).unwrap_or(key).to_owned();
        let budget = round_every_ms(&settings.config);
        let mut work = 0;
        let may_go_on = |work: usize| {
            work < MAX_PER_ROUND && clock.now_ms().saturating_sub(started) < budget && go_on()
        };

        // Marks that stayed: a removal that did not finish. Each the very
        // mark read, by its version.
        if self.fault != Fault::MarksKept {
            let mut stale: Vec<(u64, String, Version)> = self
                .seen
                .iter()
                .filter_map(|(key, (_, seen))| match seen {
                    Seen::Mark { at_ms, version } if started.saturating_sub(*at_ms) > recent => {
                        Some((*at_ms, key.clone(), version.clone()))
                    }
                    _ => None,
                })
                .collect();
            stale.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
            for (_, key, version) in stale {
                if !may_go_on(work) {
                    return Ok(round);
                }
                work += 1;
                // Read again at its next listing, whatever happened.
                self.seen.remove(&key);
                let finished = if self.fault == Fault::NoDeadline {
                    forget_finish_mark(bucket, &key, Some(&version), std::future::pending()).await
                } else {
                    forget_finish_mark(bucket, &key, Some(&version), give_up()).await
                };
                match finished {
                    Ok(Forgot::Removed) => round.ended.push(id(&key)),
                    Ok(Forgot::Changed) => {}
                    Ok(Forgot::Unfinished) => round.unfinished.push(id(&key)),
                    Err(e) => {
                        let why = format!("ending the mark at {key}: {e:?}");
                        self.failed(&mut round, why, clock.now_ms(), settings);
                        return Ok(round);
                    }
                }
            }
        }

        // Released leases of this app, old enough, oldest first.
        let least = settings
            .after_ms
            .max(least_age_ms(&settings.config, settings.horizon_ms));
        let mut due: Vec<(u64, String, Version)> = self
            .seen
            .iter()
            .filter(|(key, _)| !self.held.contains_key(key.as_str()))
            .filter_map(|(key, (_, seen))| match seen {
                Seen::Released {
                    app: of,
                    written_at_ms,
                    version,
                } if of == app && *written_at_ms <= started && started - written_at_ms > least => {
                    Some((*written_at_ms, key.clone(), version.clone()))
                }
                _ => None,
            })
            .collect();
        // After a failure, the due leases wait (the gate and the records
        // are the costly reads); the listing and the marks do not.
        if due.is_empty() || !may_go_on(work) || clock.now_ms() < self.paused_until {
            return Ok(round);
        }
        due.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

        // The gate of the mark, once a round, read after the leases'
        // versions: before the finalize a node that does not know the mark
        // may start under that id.
        if self.fault != Fault::Unmarked {
            match crate::upgrade::forget_gate(bucket, app, clock.now_ms(), settings.horizon_ms)
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(blocked)) => {
                    round.blocked = Some(blocked.to_string());
                    let until = clock.now_ms().saturating_add(settings.hold_back_ms);
                    for (_, key, _) in &due {
                        self.held.insert(key.clone(), until);
                    }
                    self.failures = 0;
                    return Ok(round);
                }
                Err(e) => {
                    let why = format!("reading the gate of the mark: {e:?}");
                    self.failed(&mut round, why, clock.now_ms(), settings);
                    return Ok(round);
                }
            }
        }

        // The records, after the versions, once for every due lease not
        // cleared at its version yet: a record written since names a
        // process that wrote its lease first, which fails the condition.
        let unscanned: BTreeSet<String> = due
            .iter()
            .filter(|(_, key, version)| self.cleared.get(key) != Some(version))
            .map(|(_, key, _)| id(key))
            .collect();
        if !unscanned.is_empty() {
            let named = if self.fault == Fault::RecordsUnchecked {
                Named::These(BTreeSet::new())
            } else {
                match records_naming(bucket, &unscanned).await {
                    Ok(named) => named,
                    Err(e) => {
                        let why = format!("reading the ownership records: {e:?}");
                        self.failed(&mut round, why, clock.now_ms(), settings);
                        return Ok(round);
                    }
                }
            };
            // Held back from the time the scan ended, not from its start.
            let until = clock.now_ms().saturating_add(settings.hold_back_ms);
            for (_, key, version) in &due {
                let node = id(key);
                if !unscanned.contains(&node) {
                    continue;
                }
                if named.names(&node) {
                    round.named.push(node);
                    self.held.insert(key.clone(), until);
                } else {
                    self.cleared.insert(key.clone(), version.clone());
                }
            }
        }
        self.failures = 0;

        let cleared: Vec<(String, Version)> = due
            .into_iter()
            .filter(|(_, key, version)| self.cleared.get(key) == Some(version))
            .map(|(_, key, version)| (key, version))
            .collect();
        for (key, version) in cleared {
            if !may_go_on(work) {
                round.left = round.left.saturating_add(1);
                continue;
            }
            work += 1;
            let node = id(&key);
            // Whatever the outcome, the key is read again at its next
            // listing.
            self.seen.remove(&key);
            self.cleared.remove(&key);
            let removed = if self.fault == Fault::Unmarked {
                delete_unmarked(bucket, &key, &version).await
            } else {
                forget_released(bucket, &key, &version, clock.now_ms(), give_up()).await
            };
            match removed {
                Ok(Forgot::Removed) => round.removed.push(node),
                Ok(Forgot::Changed) => round.changed.push(node),
                Ok(Forgot::Unfinished) => round.unfinished.push(node),
                Err(e) => {
                    let why = format!("removing {key}: {e:?}");
                    self.failed(&mut round, why, clock.now_ms(), settings);
                    return Ok(round);
                }
            }
        }
        Ok(round)
    }
}

#[cfg(test)]
#[cfg(feature = "object-store")]
mod tests;
