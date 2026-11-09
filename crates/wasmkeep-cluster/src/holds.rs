//! When a node's holds may act ([ADR 0020], sections 1 and 3): the rules the
//! server and the simulations share, so the simulations test the server's
//! own predicate rather than a copy of it.
//!
//! A hold (an ownership record this node acquired, [`crate::node_lease::Hold`])
//! may act only while this node's lease is valid *and* no lapse happened
//! since the hold was acquired: a lease that ran out ends every hold, and
//! the node acquires afresh, at new generations, once its lease is valid
//! again. [`Validity`] carries both facts in one value, so one read gives a
//! consistent answer. Releases and acquisitions whose outcome is unknown wait
//! as [`PendingEntry`]s; [`retry`] tries one again.
//!
//! No runtime here: the caller keeps the value where its tasks can share it
//! (the server: a watch channel) and orders an acquisition and the
//! resolution of an unresolved acquire of the same actor (the server: a lock
//! per actor).
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md

use crate::lease::{Bucket, Clock};
use crate::node_lease::{Hold, Owners, Released};

/// This node's lease validity, as its holds see it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Validity {
    /// Until when the lease is valid (this node's clock, ms); `None` before
    /// its first write, once lost, and from a clean stop on.
    pub until_ms: Option<u64>,
    /// Moves each time the lease ran out, was lost or made invalid: a hold
    /// acquired in an earlier epoch never acts again.
    pub epoch: u64,
}

impl Validity {
    /// Whether a hold acquired in `epoch` may act at `now_ms`.
    pub fn valid_in(&self, epoch: u64, now_ms: u64) -> bool {
        self.epoch == epoch && self.valid(now_ms)
    }

    /// Whether the lease itself is valid at `now_ms` (for acquiring, and for
    /// fencing others).
    pub fn valid(&self, now_ms: u64) -> bool {
        self.until_ms.is_some_and(|until| now_ms < until)
    }

    /// Records a lease write's outcome at `now_ms`: the lease is valid until
    /// `until_ms` (the writer's last acknowledged write, `None` if none). If
    /// it had run out since the previous record (the bucket unreachable for
    /// longer than `ttl - safety`), the epoch moves first and this returns
    /// `true`: the caller ends every hold, each into its pending releases. A
    /// validity already past is recorded as none, so a lapse is reported
    /// once, not again on every failed write until the next one lands.
    #[must_use = "a lapse ends every hold"]
    pub fn publish(&mut self, until_ms: Option<u64>, now_ms: u64) -> bool {
        let lapsed = self.until_ms.is_some_and(|until| now_ms >= until);
        if lapsed {
            self.epoch = self.epoch.wrapping_add(1);
        }
        self.until_ms = until_ms.filter(|&until| now_ms < until);
        lapsed
    }

    /// Makes the lease invalid at once and for every hold acquired so far: a
    /// lost lease, a twin found, a clean stop.
    pub fn invalidate(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.until_ms = None;
    }
}

/// A release or an acquire whose outcome is not known yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingEntry {
    /// A release to send again, `If-Match` on the hold's own version only.
    Release(Hold),
    /// An acquire that did not succeed and may have landed, at this
    /// generation.
    Unresolved {
        /// The record's key.
        key: String,
        /// The generation it tried.
        generation: u64,
    },
}

impl PendingEntry {
    /// The record's key.
    pub fn key(&self) -> &str {
        match self {
            Self::Release(hold) => &hold.key,
            Self::Unresolved { key, .. } => key,
        }
    }
}

/// Tries one pending entry again: a release is sent again; an unresolved
/// acquire is read and released if it landed, unless `held_here` says a
/// hold of that actor lives on this node (a later acquisition, possibly at
/// the very generation the unresolved one tried). The caller holds the
/// actor's lock across this call for an unresolved acquire. `Some(entry)`
/// if it is still pending; `None` once it ended (landed, superseded, or
/// behind a live hold).
pub async fn retry<B: Bucket, C: Clock>(
    owners: &Owners<B, C>,
    entry: PendingEntry,
    held_here: impl Fn(&str) -> bool,
) -> Option<PendingEntry> {
    match entry {
        PendingEntry::Release(hold) => match owners.release(&hold).await {
            Released::Pending => Some(PendingEntry::Release(hold)),
            Released::Released | Released::Superseded => None,
        },
        PendingEntry::Unresolved { key, generation } => {
            if held_here(&key) {
                return None;
            }
            match owners.resolve(&key, generation).await {
                Released::Pending => Some(PendingEntry::Unresolved { key, generation }),
                Released::Released | Released::Superseded => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseConfig;
    use crate::lease::tests::{At, Mem, run};
    use crate::node_lease::{NodeLease, OwnerRecord};
    use std::cell::Cell;

    #[test]
    fn a_hold_acts_only_in_its_epoch_while_the_lease_is_valid() {
        let mut v = Validity::default();
        assert!(!v.valid(0), "no write yet");
        assert!(!v.publish(Some(8_000), 0));
        let epoch = v.epoch;
        assert!(v.valid_in(epoch, 7_999));
        assert!(!v.valid_in(epoch, 8_000), "ran out");
        // Renewed in time: the same epoch goes on.
        assert!(!v.publish(Some(11_000), 3_000));
        assert!(v.valid_in(epoch, 10_000));
        // A failed write past the validity: the lapse is reported once,
        // the epoch moves, and the past validity is recorded as none.
        assert!(v.publish(Some(11_000), 12_000));
        assert_eq!(v.until_ms, None);
        assert!(!v.publish(Some(11_000), 13_000), "reported once");
        // Renewed after the lapse: valid again, but not for the old hold.
        assert!(!v.publish(Some(21_000), 13_000));
        assert!(v.valid(14_000));
        assert!(!v.valid_in(epoch, 14_000));
        assert!(v.valid_in(v.epoch, 14_000));
        // Invalidated: nothing acts, whatever comes next for old holds.
        let before = v.epoch;
        v.invalidate();
        assert!(!v.valid(14_000));
        assert!(!v.publish(Some(30_000), 15_000));
        assert!(!v.valid_in(before, 16_000));
    }

    #[test]
    fn a_pending_entry_ends_when_it_lands_or_a_hold_lives_here() {
        let cfg = LeaseConfig::default_safe();
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg);
        run(lease.start(None)).unwrap();
        let owners = Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg);
        let read = |key: &str| {
            let (body, _) = run((&mem).get(key)).unwrap().unwrap();
            OwnerRecord::decode(&body).unwrap()
        };
        // A release goes through: the entry ends, the record names nobody.
        let hold = run(owners.acquire("owners/x", || true, async || Ok(1))).unwrap();
        assert_eq!(
            run(retry(&owners, PendingEntry::Release(hold), |_| false)),
            None
        );
        assert_eq!(read("owners/x").holder, None);
        // An unresolved acquire that landed, with no hold here: released.
        let landed = run(owners.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let entry = PendingEntry::Unresolved {
            key: "owners/x".into(),
            generation: landed.generation,
        };
        assert_eq!(run(retry(&owners, entry, |_| false)), None);
        assert_eq!(read("owners/x").holder, None);
        // With a hold of the actor here: left alone, the entry ends.
        let live = run(owners.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let entry = PendingEntry::Unresolved {
            key: "owners/x".into(),
            generation: live.generation,
        };
        assert_eq!(entry.key(), "owners/x");
        assert_eq!(run(retry(&owners, entry, |k| k == "owners/x")), None);
        assert!(
            read("owners/x").holder.is_some(),
            "a live hold was released"
        );
    }
}
