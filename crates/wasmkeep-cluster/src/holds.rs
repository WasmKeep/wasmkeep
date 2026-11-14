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
//! as [`PendingEntry`]s; [`retry`] tries one again. Holds of actors that
//! hibernated with sockets open wait, unreleased, in [`Parked`]
//! ([ADR 0024]).
//!
//! No runtime here: the caller keeps the value where its tasks can share it
//! (the server: a watch channel) and orders an acquisition and the
//! resolution of an unresolved acquire of the same actor (the server: a lock
//! per actor).
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
//! [ADR 0024]: ../../../docs/adr/0024-socket-home.md

use std::collections::HashMap;

use crate::lease::{Bucket, Clock};
use crate::node_lease::{AcquireError, Hold, OwnerRecord, Owners, Released};

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
        /// The record its write was conditioned on (`None`: there was
        /// none): while the record is still that one, the write may land.
        before: Option<OwnerRecord>,
        /// Until when (this node's clock, ms) the resolver waits for a
        /// record still at `before`; past it, a read that finds it so ends
        /// the entry ([ADR 0020], section 3).
        until_ms: u64,
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

    /// The unresolved acquire of `key` that `error` reports, if it is one,
    /// waiting for an unchanged record from now until `owners`' wait has
    /// passed.
    pub fn unresolved<B: Bucket, C: Clock>(
        owners: &Owners<B, C>,
        key: &str,
        error: &AcquireError,
    ) -> Option<Self> {
        match error {
            AcquireError::Unresolved { generation, before } => Some(Self::Unresolved {
                key: key.to_owned(),
                generation: *generation,
                before: before.clone(),
                until_ms: owners.unresolved_until_ms(),
            }),
            _ => None,
        }
    }
}

/// Adds `entry` to `entries`. An unresolved acquire already there for the
/// same write (key, generation and record conditioned on: a second
/// acquisition that read the same record and also went unanswered) is
/// kept once, waiting until the later of the two deadlines.
pub fn add(entries: &mut Vec<PendingEntry>, entry: PendingEntry) {
    if let PendingEntry::Unresolved {
        key,
        generation,
        before,
        until_ms,
    } = &entry
    {
        for e in entries.iter_mut() {
            if let PendingEntry::Unresolved {
                key: k,
                generation: g,
                before: b,
                until_ms: u,
            } = e
                && k == key
                && g == generation
                && b == before
            {
                *u = (*u).max(*until_ms);
                return;
            }
        }
    }
    entries.push(entry);
}

/// What one more try of a pending entry did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retried {
    /// It ended: landed, superseded, or behind a live hold here.
    Ended,
    /// No answer, or not known yet: still pending.
    Pending(PendingEntry),
    /// An unresolved acquire whose record is still the one its write was
    /// conditioned on: still pending, though the bucket answered (not a
    /// refusal, for a caller that backs off on those).
    Waiting(PendingEntry),
}

impl Retried {
    /// The entry, if it is still pending.
    pub fn still(self) -> Option<PendingEntry> {
        match self {
            Self::Ended => None,
            Self::Pending(entry) | Self::Waiting(entry) => Some(entry),
        }
    }
}

/// Holds parked by hibernations with sockets open ([ADR 0024], section 1):
/// releases deferred, never authority to act. Each keeps the hold (its
/// key, generation and version) and `T`, what the caller needs to find
/// the actor again (the server: its address). No runtime and no lock: the
/// caller keeps it beside its live holds, and moves a hold from one to the
/// other in one step.
///
/// [ADR 0024]: ../../../docs/adr/0024-socket-home.md
#[derive(Debug, Clone)]
pub struct Parked<T> {
    by_key: HashMap<String, (Hold, T)>,
}

impl<T> Default for Parked<T> {
    fn default() -> Self {
        Self {
            by_key: HashMap::new(),
        }
    }
}

impl<T> Parked<T> {
    /// Parks `hold`; a hold parked earlier for the same key is returned,
    /// to release (it cannot be the record's version any more).
    pub fn park(&mut self, hold: Hold, tag: T) -> Option<Hold> {
        let key = hold.key.clone();
        self.by_key.insert(key, (hold, tag)).map(|(h, _)| h)
    }

    /// Whether a hold of `key` is parked.
    pub fn contains(&self, key: &str) -> bool {
        self.by_key.contains_key(key)
    }

    /// Whether a hold of `key` is parked at `generation`: what the resolver
    /// of an unresolved acquire at that generation counts as held here
    /// (ADR 0024, section 3). Never at another generation: an unresolved
    /// wake at a later one is resolved as usual.
    pub fn holds_at(&self, key: &str, generation: u64) -> bool {
        self.by_key
            .get(key)
            .is_some_and(|(h, _)| h.generation == generation)
    }

    /// The generation `key` is parked at, if it is.
    pub fn generation(&self, key: &str) -> Option<u64> {
        self.by_key.get(key).map(|(h, _)| h.generation)
    }

    /// Takes the parked hold of `key` out, to release it.
    pub fn take(&mut self, key: &str) -> Option<(Hold, T)> {
        self.by_key.remove(key)
    }

    /// Takes the parked hold of `key` out if it is still at `generation`.
    pub fn take_at(&mut self, key: &str, generation: u64) -> Option<(Hold, T)> {
        if self.holds_at(key, generation) {
            self.by_key.remove(key)
        } else {
            None
        }
    }

    /// After an acquisition of `key` by this node (a wake) with outcome
    /// `acquired`: the parked hold is dropped, and returned, when the
    /// record is known to have moved (the acquisition succeeded: the new
    /// hold replaces it; or another holder was found). It is kept
    /// otherwise: when nothing was written (the bucket did not answer the
    /// read, the lease is not valid, an unreadable record), and when the
    /// outcome is unknown. Then the write may not have landed, and the
    /// record still names this hold; if it did land, the resolver releases
    /// it (a parked hold counts as held only at its own generation), and
    /// the parked hold's own release then finds a moved version and ends.
    pub fn after_wake(
        &mut self,
        key: &str,
        acquired: &Result<Hold, AcquireError>,
    ) -> Option<(Hold, T)> {
        match acquired {
            Ok(_) | Err(AcquireError::Held { .. }) => self.by_key.remove(key),
            Err(
                AcquireError::Unresolved { .. }
                | AcquireError::Unavailable
                | AcquireError::LeaseInvalid
                | AcquireError::Corrupt,
            ) => None,
        }
    }

    /// Takes every parked hold out: a lapse (each becomes a pending
    /// release), a lost lease (dropped), a clean stop or a drain (each
    /// released).
    pub fn take_all(&mut self) -> Vec<(Hold, T)> {
        self.by_key.drain().map(|(_, v)| v).collect()
    }

    /// The parked holds, by key.
    pub fn iter(&self) -> impl Iterator<Item = (&Hold, &T)> {
        self.by_key.values().map(|(h, t)| (h, t))
    }

    /// How many holds are parked.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether none is parked.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

/// Tries one pending entry again: a release is sent again; an unresolved
/// acquire is read and released if it landed, unless `held_here(key,
/// generation)` says a hold of that actor lives on this node (a later
/// acquisition, possibly at the very generation the unresolved one tried;
/// or a parked hold at that generation, ADR 0024). The caller holds the
/// actor's lock across this call for an unresolved acquire.
///
/// An unresolved acquire whose record is still the one its write was
/// conditioned on stays ([`Retried::Waiting`]) until its `until_ms`: the
/// write may still land, and ending the entry then would leave the record
/// naming this node with nothing here to release it. Past that time, a
/// read issued after it that finds the record unchanged ends the entry.
pub async fn retry<B: Bucket, C: Clock>(
    owners: &Owners<B, C>,
    entry: PendingEntry,
    held_here: impl Fn(&str, u64) -> bool,
) -> Retried {
    match entry {
        PendingEntry::Release(hold) => match owners.release(&hold).await {
            Released::Pending | Released::Unchanged => {
                Retried::Pending(PendingEntry::Release(hold))
            }
            Released::Released | Released::Superseded => Retried::Ended,
        },
        PendingEntry::Unresolved {
            key,
            generation,
            before,
            until_ms,
        } => {
            if held_here(&key, generation) {
                return Retried::Ended;
            }
            // Asked before the read is sent: a read that ends the entry
            // for its time being up was issued after that time.
            let wait = owners.now_ms() < until_ms;
            let resolved = owners
                .resolve(&key, generation, before.as_ref(), wait)
                .await;
            let entry = PendingEntry::Unresolved {
                key,
                generation,
                before,
                until_ms,
            };
            match resolved {
                Released::Pending => Retried::Pending(entry),
                Released::Unchanged => Retried::Waiting(entry),
                Released::Released | Released::Superseded => Retried::Ended,
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

    fn hold(key: &str, generation: u64) -> Hold {
        Hold {
            key: key.into(),
            generation,
            version: crate::lease::Version(format!("v{generation}")),
            previous: None,
        }
    }

    #[test]
    fn a_parked_hold_counts_only_at_its_generation() {
        let mut parked = Parked::default();
        assert!(parked.park(hold("owners/x", 4), "x").is_none());
        assert!(parked.contains("owners/x"));
        assert!(parked.holds_at("owners/x", 4));
        assert_eq!(parked.generation("owners/x"), Some(4));
        // An unresolved wake at the next generation is not held here.
        assert!(!parked.holds_at("owners/x", 5));
        assert!(parked.take_at("owners/x", 5).is_none());
        // Parked again (a later hold): the earlier one comes back.
        assert_eq!(parked.park(hold("owners/x", 6), "x").unwrap().generation, 4);
        assert_eq!(parked.take_at("owners/x", 6).unwrap().0.generation, 6);
        assert!(parked.is_empty());
    }

    #[test]
    fn a_wake_drops_the_parked_hold_only_once_the_record_moved() {
        let mut parked = Parked::default();
        let wrote: [Result<Hold, AcquireError>; 2] = [
            Ok(hold("owners/x", 5)),
            Err(AcquireError::Held {
                node: "b".into(),
                instance: "i".into(),
            }),
        ];
        for outcome in &wrote {
            parked.park(hold("owners/x", 4), ());
            assert!(
                parked.after_wake("owners/x", outcome).is_some(),
                "{outcome:?}"
            );
            assert!(!parked.contains("owners/x"));
        }
        for outcome in [
            AcquireError::Unresolved {
                generation: 5,
                before: None,
            },
            AcquireError::Unavailable,
            AcquireError::LeaseInvalid,
            AcquireError::Corrupt,
        ] {
            parked.park(hold("owners/x", 4), ());
            assert!(
                parked
                    .after_wake("owners/x", &Err(outcome.clone()))
                    .is_none()
            );
            assert!(parked.holds_at("owners/x", 4), "{outcome:?}");
        }
        assert_eq!(parked.take_all().len(), 1);
        assert_eq!(parked.len(), 0);
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
            run(retry(&owners, PendingEntry::Release(hold), |_, _| false)),
            Retried::Ended
        );
        assert_eq!(read("owners/x").holder, None);
        // An unresolved acquire that landed, with no hold here: released.
        let landed = run(owners.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let entry = PendingEntry::Unresolved {
            key: "owners/x".into(),
            generation: landed.generation,
            before: None,
            until_ms: owners.unresolved_until_ms(),
        };
        assert_eq!(run(retry(&owners, entry, |_, _| false)), Retried::Ended);
        assert_eq!(read("owners/x").holder, None);
        // With a hold of the actor here: left alone, the entry ends.
        let live = run(owners.acquire("owners/x", || true, async || Ok(1))).unwrap();
        let entry = PendingEntry::Unresolved {
            key: "owners/x".into(),
            generation: live.generation,
            before: None,
            until_ms: owners.unresolved_until_ms(),
        };
        assert_eq!(entry.key(), "owners/x");
        assert_eq!(
            run(retry(&owners, entry, |k, _| k == "owners/x")),
            Retried::Ended
        );
        assert!(
            read("owners/x").holder.is_some(),
            "a live hold was released"
        );
    }

    /// An unresolved acquire whose record is unchanged waits for its
    /// write until its time is up (ADR 0020, section 3): a write landing
    /// meanwhile is released; past the time, an unchanged record ends the
    /// entry; a record changed otherwise ends it at once.
    #[test]
    fn an_unresolved_acquire_waits_for_an_unchanged_record() {
        let cfg = LeaseConfig::default_safe();
        let mem = Mem::default();
        let t = Cell::new(1_000);
        let mut lease = NodeLease::new(&mem, At(&t), "nodes/a", "i1", cfg);
        run(lease.start(None)).unwrap();
        let owners =
            Owners::new(&mem, At(&t), "a", "i1", "nodes/", cfg).with_unresolved_wait_ms(5_000);
        let nobody = |generation| OwnerRecord {
            holder: None,
            generation,
        };
        let mine = |generation| OwnerRecord {
            holder: Some(("a".into(), "i1".into())),
            generation,
        };
        let read = |key: &str| {
            let (body, _) = run((&mem).get(key)).unwrap().unwrap();
            OwnerRecord::decode(&body).unwrap()
        };
        for absent in [false, true] {
            let key = if absent { "owners/c" } else { "owners/r" };
            if !absent {
                mem.overwrite(key, Some(&nobody(4).encode()));
            }
            let before = (!absent).then(|| nobody(4));
            let generation = if absent { 1 } else { 5 };
            let entry = PendingEntry::Unresolved {
                key: key.into(),
                generation,
                before,
                until_ms: owners.unresolved_until_ms(),
            };
            // Unchanged: it waits, however often it is read.
            let Retried::Waiting(entry) = run(retry(&owners, entry, |_, _| false)) else {
                panic!("an unchanged record ended the entry");
            };
            t.set(t.get() + 4_999);
            let Retried::Waiting(entry) = run(retry(&owners, entry, |_, _| false)) else {
                panic!("ended before its time");
            };
            // The write lands late: released, and the entry ends.
            mem.overwrite(key, Some(&mine(generation).encode()));
            assert_eq!(run(retry(&owners, entry, |_, _| false)), Retried::Ended);
            assert_eq!(read(key), nobody(generation));
            t.set(t.get() - 4_999);
        }
        // Past its time, unchanged: the entry ends.
        mem.overwrite("owners/p", Some(&nobody(4).encode()));
        let entry = PendingEntry::Unresolved {
            key: "owners/p".into(),
            generation: 5,
            before: Some(nobody(4)),
            until_ms: owners.unresolved_until_ms(),
        };
        t.set(t.get() + 5_000);
        assert_eq!(run(retry(&owners, entry, |_, _| false)), Retried::Ended);
        t.set(t.get() - 5_000);
        // Changed otherwise (here released by its holder at the same
        // generation; then another holder at the next): ends at once, and
        // the record is left alone.
        for (before, current) in [
            (
                OwnerRecord {
                    holder: Some(("b".into(), "i2".into())),
                    generation: 4,
                },
                nobody(4),
            ),
            (
                nobody(4),
                OwnerRecord {
                    holder: Some(("b".into(), "i2".into())),
                    generation: 5,
                },
            ),
        ] {
            mem.overwrite("owners/o", Some(&current.encode()));
            let entry = PendingEntry::Unresolved {
                key: "owners/o".into(),
                generation: 5,
                before: Some(before),
                until_ms: owners.unresolved_until_ms(),
            };
            assert_eq!(run(retry(&owners, entry, |_, _| false)), Retried::Ended);
            assert_eq!(read("owners/o"), current);
        }
        // A record removed after a write conditioned on one: ends.
        mem.overwrite("owners/o", None);
        let entry = PendingEntry::Unresolved {
            key: "owners/o".into(),
            generation: 5,
            before: Some(nobody(4)),
            until_ms: owners.unresolved_until_ms(),
        };
        assert_eq!(run(retry(&owners, entry, |_, _| false)), Retried::Ended);
    }

    #[test]
    fn one_unresolved_write_is_kept_once() {
        let entry = |generation, until_ms| PendingEntry::Unresolved {
            key: "owners/x".into(),
            generation,
            before: None,
            until_ms,
        };
        let mut entries = Vec::new();
        add(&mut entries, entry(5, 100));
        add(&mut entries, entry(5, 300));
        add(&mut entries, entry(5, 200));
        add(&mut entries, entry(6, 100));
        add(&mut entries, PendingEntry::Release(hold("owners/x", 4)));
        add(&mut entries, PendingEntry::Release(hold("owners/x", 4)));
        assert_eq!(entries.len(), 4, "{entries:?}");
        assert_eq!(entries[0], entry(5, 300));
    }
}
