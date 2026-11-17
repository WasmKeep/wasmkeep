//! A node's local evidence of each actor's acknowledged head ([ADR 0035],
//! section 2): the head each ended, parked or pending hold froze, kept by
//! actor key from the moment the hold ends until a successor here
//! inherits it, its release lands, or a fresh read of the record proves a
//! remote successor ([`LocalHeads::prepare`]).
//!
//! An acquisition reads it under the actor's lock, after the record's
//! authoritative read and before its conditional write (the preparation
//! hook of [`crate::node_lease::Owners::acquire_prepared`]), so a same-
//! process re-acquire never starts from less than what this process
//! acknowledged, even while its release is still queued, in flight or
//! pending, or the hold is parked. The new hold carries the requirement
//! ([`crate::hold_cell::HoldHead::inherited`]); once it is registered the
//! entry is transferred to it ([`LocalHeads::transferred`]).
//!
//! Process-local: a crash loses it (ADR 0035, section 4). Until the record
//! itself carries the head (`wasmkeep-owner 2` written on release), this
//! is the only place the requirement lives.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

use std::collections::HashMap;

use crate::node_lease::{Hold, OwnerRecord};
use crate::recorded_head::{Completeness, Position};

/// A required position: the head, the ownership generation in which it was
/// established (`set-at`), and whether it covers every acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requirement {
    /// The position.
    pub head: Position,
    /// When it was established (never moved by a mere change of owner).
    pub set_at: u64,
    /// Whether it covers every acknowledgement of its holder.
    pub completeness: Completeness,
}

impl Requirement {
    /// The requirement a version 2 record carries; `None` for an unknown
    /// position (no evidence) or a version 1 record.
    pub fn of_record(record: &OwnerRecord) -> Option<Self> {
        let r = record.recorded?;
        (r.head != Position::Unknown).then_some(Self {
            head: r.head,
            set_at: r.set_at,
            completeness: r.completeness,
        })
    }

    /// As stored in a record at `generation`, released (`held == false`)
    /// or held. A held record stores `lower-bound`: this version writes no
    /// `complete-inherited` phase, so it never needs the pre-upload
    /// demotion (ADR 0035, section 1); a released one keeps `complete`
    /// only from a hold that froze it.
    pub fn stored(
        requirement: Option<Self>,
        held: bool,
        idle_since_ms: Option<u64>,
    ) -> crate::recorded_head::RecordedHead {
        use crate::recorded_head::RecordedHead;
        match requirement {
            None => RecordedHead {
                idle_since_ms,
                ..RecordedHead::UNKNOWN
            },
            Some(r) => RecordedHead {
                head: r.head,
                set_at: r.set_at,
                completeness: match (held, r.completeness) {
                    (false, Completeness::Complete) => Completeness::Complete,
                    _ => Completeness::LowerBound,
                },
                idle_since_ms,
            },
        }
    }

    /// What an ended hold proves. A verified hold froze every
    /// acknowledgement under its cell's lock: its acknowledged head,
    /// complete, established in its own generation (the head itself may
    /// be older). An unverified one proves only what it inherited,
    /// unchanged, stamp included; nothing if it inherited nothing.
    pub fn frozen(hold: &Hold) -> Option<Self> {
        match hold.head.acknowledged {
            Some(head) => Some(Self {
                head,
                set_at: hold.generation,
                completeness: Completeness::Complete,
            }),
            None => hold.head.inherited,
        }
    }
}

/// The requirement an acquisition carries, from this process's local
/// evidence (`local`, with the generation of the hold that froze it) and
/// the record's (`record`, with the record's generation). Not a numeric
/// maximum of positions: whichever was established by the later hold of
/// this lineage covers the other. A local entry at or above the record's
/// generation is this process's own hold, which inherited the record's
/// requirement and verified or kept it; a record above the entry (after
/// retirement, only this process's own attempt, whose bytes carried the
/// entry) covers it.
pub fn combine(
    local: Option<(Requirement, u64)>,
    record: Option<(Requirement, u64)>,
) -> Option<Requirement> {
    match (local, record) {
        (Some((l, gl)), Some((r, gr))) => Some(if gl >= gr { l } else { r }),
        (Some((l, _)), None) => Some(l),
        (None, Some((r, _))) => Some(r),
        (None, None) => None,
    }
}

/// Whether a restore's selected chain covers a required floor (ADR 0035,
/// section 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proof {
    /// Proven: no floor to reach, the exact position with its checksum,
    /// or a validated object on the chain in a generation above the
    /// floor's stamp.
    Proven,
    /// The exact position is on the chain: proven once its checksum, known
    /// only by replaying it, matches.
    NeedsReplay,
    /// Nothing on the chain proves it: the listing may be behind; list
    /// again within the bound, then refuse.
    Missing,
    /// The exact position is on the chain with another checksum: a
    /// conflicting history, not a visibility miss.
    Conflict,
}

/// Proves `floor` against the selected chain `chain`: each object's
/// position, ascending (base snapshot first), with the database checksum
/// after it when known (replayed). A larger transaction id, or a
/// generation above the floor's head generation but not above its stamp,
/// proves nothing; so does an object on another branch (not in `chain`).
/// Every generation above the stamp could upload only after verifying its
/// predecessor's requirement, by induction this floor (capability gating:
/// no writer that skips this check writes after a floor was established).
pub fn prove(
    floor: Option<&Requirement>,
    chain: &[(crate::replication::SegmentId, Option<u64>)],
) -> Proof {
    let Some(floor) = floor else {
        return Proof::Proven;
    };
    let Position::Known {
        generation,
        txid,
        checksum,
    } = floor.head
    else {
        return Proof::Proven;
    };
    if let Some((_, sum)) = chain
        .iter()
        .find(|(id, _)| id.generation == generation && id.txid == txid)
    {
        return match sum {
            Some(s) if *s == checksum => Proof::Proven,
            Some(_) => Proof::Conflict,
            None => Proof::NeedsReplay,
        };
    }
    if chain.iter().any(|(id, _)| id.generation > floor.set_at) {
        return Proof::Proven;
    }
    Proof::Missing
}

/// Proof 1 beyond the planner's snapshot stop (ADR 0035, section 3): the
/// plan starts from snapshot `base`, above an exact floor the chain does
/// not show. Proven if the floor's own position is listed (a segment or a
/// snapshot) and `base` links back to it through listed segments, each
/// naming its parent: the selected chain descends from the floor, though
/// the restore never replays it.
pub fn linked_back(
    floor: Option<&Requirement>,
    base: crate::replication::SegmentId,
    listing: &crate::replication::Listing,
) -> bool {
    use crate::replication::SegmentId;
    let Some(Requirement {
        head: Position::Known {
            generation, txid, ..
        },
        ..
    }) = floor
    else {
        return false;
    };
    let target = SegmentId {
        generation: *generation,
        txid: *txid,
    };
    let listed =
        listing.segments.iter().any(|s| s.id == target) || listing.snapshots.contains(&target);
    if !listed {
        return false;
    }
    let parents: std::collections::BTreeMap<SegmentId, SegmentId> = listing
        .segments
        .iter()
        .map(|s| (s.id, s.parent()))
        .collect();
    let mut at = base;
    // Each step lowers the txid: at most one step per transaction.
    while at.txid > target.txid {
        let Some(&parent) = parents.get(&at) else {
            return false;
        };
        at = parent;
    }
    at == target
}

/// One actor's local evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalHead {
    /// The ownership generation of the hold that froze it.
    pub generation: u64,
    /// What it requires.
    pub requirement: Requirement,
    /// Generations this process tried to acquire the actor at while the
    /// entry lived: a released record at one of them may be this
    /// process's own unresolved acquisition, which inherited nothing it
    /// could publish, so it retires nothing.
    attempts: Vec<u64>,
    /// Acquisitions that read it and have not finished (registered,
    /// failed, or their unresolved write resolved): while any lasts, a
    /// landed release only marks it ([`LocalHeads::released`]).
    reserved: u32,
    /// The release of its hold landed while it was reserved.
    released: bool,
}

/// How many attempted generations an entry remembers.
const ATTEMPTS_KEPT: usize = 64;

/// The local evidence, by ownership record key. No runtime: the server
/// keeps it under a synchronous mutex taken after its live-hold set and
/// never across an await.
#[derive(Debug, Default)]
pub struct LocalHeads {
    by_key: HashMap<String, LocalHead>,
}

impl LocalHeads {
    /// Registers what `hold` froze as it ended (before its cell empties or
    /// its release is sent). An entry of a later generation is kept: it
    /// already covers this one by inheritance.
    pub fn register(&mut self, hold: &Hold) {
        let Some(requirement) = Requirement::frozen(hold) else {
            return;
        };
        match self.by_key.get(&hold.key) {
            Some(e) if e.generation > hold.generation => {}
            _ => {
                self.by_key.insert(
                    hold.key.clone(),
                    LocalHead {
                        generation: hold.generation,
                        requirement,
                        attempts: Vec::new(),
                        reserved: 0,
                        released: false,
                    },
                );
            }
        }
    }

    /// The preparation of an acquisition of `key` at `attempted`, after
    /// the authoritative read found `record` (`None`: absent) and before
    /// the conditional write: retires the entry if the record proves a
    /// remote successor, then returns the requirement the acquisition
    /// carries.
    ///
    /// Proof of a remote successor: the record, freshly read and decoded,
    /// names another holder (not this node's process `node`/`instance`) at
    /// a strictly higher generation than the entry's, or names nobody at a
    /// strictly higher generation that this process never tried. A record
    /// at the same or a lower generation, an absent one and (by the
    /// caller never calling this) an unreadable one or a failed read prove
    /// nothing. A record naming this process at a higher generation is its
    /// own unresolved acquisition: it inherited the entry, and keeps it.
    pub fn prepare(
        &mut self,
        key: &str,
        record: Option<&OwnerRecord>,
        attempted: u64,
        node: &str,
        instance: &str,
    ) -> Option<Requirement> {
        if let (Some(entry), Some(record)) = (self.by_key.get(key), record)
            && record.generation > entry.generation
        {
            let superseded = match &record.holder {
                Some((n, i)) => !(n == node && i == instance),
                None => !entry.attempts.contains(&record.generation),
            };
            if superseded {
                self.by_key.remove(key);
            }
        }
        let entry = self.by_key.get_mut(key)?;
        entry.reserved = entry.reserved.saturating_add(1);
        if !entry.attempts.contains(&attempted) {
            if entry.attempts.len() >= ATTEMPTS_KEPT {
                entry.attempts.remove(0);
            }
            entry.attempts.push(attempted);
        }
        Some(entry.requirement)
    }

    /// An acquisition that [prepared](Self::prepare) with `key`'s entry
    /// failed, or its unresolved write was resolved, without a hold to
    /// inherit the entry: its reservation ends.
    pub fn finished(&mut self, key: &str) {
        if let Some(e) = self.by_key.get_mut(key) {
            e.reserved = e.reserved.saturating_sub(1);
            if e.reserved == 0 && e.released {
                self.by_key.remove(key);
            }
        }
    }

    /// A hold at `generation` inherited the entry and is registered: the
    /// entry is its now, and it registers its own when it ends.
    pub fn transferred(&mut self, key: &str, generation: u64) {
        if self
            .by_key
            .get(key)
            .is_some_and(|e| e.generation < generation)
        {
            self.by_key.remove(key);
        }
    }

    /// The release of the hold at `generation` landed. Its entry ends
    /// (unless a later one replaced it), once no acquisition that read it
    /// is still unfinished: one may yet be registered or unresolved, and
    /// inherit it.
    pub fn released(&mut self, key: &str, generation: u64) {
        match self.by_key.get_mut(key) {
            Some(e) if e.generation == generation && e.reserved > 0 => e.released = true,
            Some(e) if e.generation == generation => {
                self.by_key.remove(key);
            }
            _ => {}
        }
    }

    /// The entry of `key`, if any.
    pub fn get(&self, key: &str) -> Option<&LocalHead> {
        self.by_key.get(key)
    }

    /// How many actors have an entry.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether none has.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hold_cell::HoldHead;
    use crate::lease::Version;

    fn at(generation: u64, txid: u64) -> Position {
        Position::Known {
            generation,
            txid,
            checksum: txid,
        }
    }

    fn hold(
        generation: u64,
        acknowledged: Option<Position>,
        inherited: Option<Requirement>,
    ) -> Hold {
        Hold {
            key: "owners/x".into(),
            generation,
            version: Version("v".into()),
            previous: None,
            head: HoldHead {
                acknowledged,
                inherited,
                ..HoldHead::default()
            },
        }
    }

    fn record(holder: Option<(&str, &str)>, generation: u64) -> OwnerRecord {
        OwnerRecord {
            holder: holder.map(|(n, i)| (n.to_owned(), i.to_owned())),
            generation,
            recorded: None,
        }
    }

    #[test]
    fn a_verified_hold_freezes_a_complete_head_stamped_with_its_generation() {
        let h = hold(5, Some(at(4, 9)), None);
        assert_eq!(
            Requirement::frozen(&h),
            Some(Requirement {
                head: at(4, 9),
                set_at: 5,
                completeness: Completeness::Complete,
            })
        );
        // Unverified: what it inherited, stamp unchanged; or nothing.
        let inherited = Requirement {
            head: at(4, 9),
            set_at: 5,
            completeness: Completeness::Complete,
        };
        assert_eq!(
            Requirement::frozen(&hold(7, None, Some(inherited))),
            Some(inherited)
        );
        assert_eq!(Requirement::frozen(&hold(7, None, None)), None);
    }

    /// The heart of case 2: a re-acquire while the release of H is still
    /// queued, in flight or pending, with the record still at H's
    /// generation, carries H.
    #[test]
    fn a_re_acquire_carries_the_frozen_head() {
        let mut heads = LocalHeads::default();
        heads.register(&hold(5, Some(at(5, 12)), None));
        let carried = heads.prepare(
            "owners/x",
            Some(&record(Some(("a", "i1")), 5)),
            6,
            "a",
            "i1",
        );
        assert_eq!(carried.map(|r| r.head), Some(at(5, 12)));
        // Released already, at the same generation: still carried.
        let carried = heads.prepare("owners/x", Some(&record(None, 5)), 6, "a", "i1");
        assert_eq!(carried.map(|r| r.head), Some(at(5, 12)));
        // Absent record: no proof of anything.
        assert!(heads.prepare("owners/x", None, 6, "a", "i1").is_some());
        // Transferred to the hold of generation 6: the entry is its.
        heads.transferred("owners/x", 6);
        assert!(heads.is_empty());
    }

    /// Case 16: another holder at a higher generation, or a release there
    /// this process never tried, retires the entry; a record naming this
    /// process, or a release at a generation it tried (its own unresolved
    /// acquisition), does not.
    #[test]
    fn only_a_remote_successor_retires_local_evidence() {
        let fresh = || {
            let mut heads = LocalHeads::default();
            heads.register(&hold(5, Some(at(5, 12)), None));
            heads
        };
        let mut heads = fresh();
        assert!(
            heads
                .prepare(
                    "owners/x",
                    Some(&record(Some(("b", "i2")), 6)),
                    7,
                    "a",
                    "i1"
                )
                .is_none()
        );
        let mut heads = fresh();
        assert!(
            heads
                .prepare("owners/x", Some(&record(None, 6)), 7, "a", "i1")
                .is_none()
        );
        // Another instance of this node is another process.
        let mut heads = fresh();
        assert!(
            heads
                .prepare(
                    "owners/x",
                    Some(&record(Some(("a", "i0")), 6)),
                    7,
                    "a",
                    "i1"
                )
                .is_none()
        );
        // This process at a higher generation: kept.
        let mut heads = fresh();
        assert!(
            heads
                .prepare(
                    "owners/x",
                    Some(&record(Some(("a", "i1")), 6)),
                    7,
                    "a",
                    "i1"
                )
                .is_some()
        );
        // Its own attempt at 6, released by the resolver: kept.
        let mut heads = fresh();
        assert!(
            heads
                .prepare("owners/x", Some(&record(None, 5)), 6, "a", "i1")
                .is_some()
        );
        assert!(
            heads
                .prepare("owners/x", Some(&record(None, 6)), 7, "a", "i1")
                .is_some()
        );
        // Same or lower generation, another holder: kept.
        let mut heads = fresh();
        assert!(
            heads
                .prepare(
                    "owners/x",
                    Some(&record(Some(("b", "i2")), 5)),
                    6,
                    "a",
                    "i1"
                )
                .is_some()
        );
    }

    /// A release that lands while an acquisition that read the entry is
    /// unfinished (its write unanswered) keeps the entry until it ends.
    #[test]
    fn a_reserved_entry_outlives_its_release() {
        let mut heads = LocalHeads::default();
        heads.register(&hold(5, Some(at(5, 12)), None));
        let mine = record(Some(("a", "i1")), 5);
        assert!(
            heads
                .prepare("owners/x", Some(&mine), 6, "a", "i1")
                .is_some()
        );
        heads.released("owners/x", 5);
        assert_eq!(heads.len(), 1, "reserved");
        assert!(
            heads
                .prepare("owners/x", Some(&record(None, 5)), 6, "a", "i1")
                .is_some()
        );
        heads.finished("owners/x");
        assert_eq!(heads.len(), 1, "one reservation left");
        heads.finished("owners/x");
        assert!(heads.is_empty());
    }

    /// Case 8: only the exact position with its checksum, or a selected-
    /// chain object in a generation above the stamp, proves the floor.
    #[test]
    fn only_exact_ancestry_or_a_later_generation_proves_a_floor() {
        use crate::replication::SegmentId;
        let id = |generation, txid| SegmentId { generation, txid };
        let floor = Requirement {
            head: at(4, 10),
            set_at: 5,
            completeness: Completeness::Complete,
        };
        let f = Some(&floor);
        assert_eq!(prove(None, &[]), Proof::Proven);
        assert_eq!(prove(f, &[]), Proof::Missing, "an empty listing");
        // Exact: needs the replayed checksum, which must match.
        assert_eq!(prove(f, &[(id(4, 10), None)]), Proof::NeedsReplay);
        assert_eq!(prove(f, &[(id(4, 10), Some(10))]), Proof::Proven);
        assert_eq!(prove(f, &[(id(4, 10), Some(11))]), Proof::Conflict);
        // A larger txid in the floor's own generations proves nothing.
        assert_eq!(
            prove(f, &[(id(4, 9), None), (id(5, 40), None)]),
            Proof::Missing
        );
        // Above the stamp, on the chain: proven even after collection.
        assert_eq!(prove(f, &[(id(6, 70), None)]), Proof::Proven);
        // Unknown or empty floors need nothing.
        let empty = Requirement {
            head: Position::Empty,
            ..floor
        };
        assert_eq!(prove(Some(&empty), &[]), Proof::Proven);
    }

    /// Review M1: a snapshot above an exact floor hides it from the plan;
    /// the listing still shows the floor and the links up to the snapshot.
    #[test]
    fn a_snapshot_above_a_listed_floor_links_back_to_it() {
        use crate::replication::{
            Listing, Segment, SegmentId, plan_restore, segment_key, snapshot_key,
        };
        let id = |generation, txid| SegmentId { generation, txid };
        let seg = |g, t, pg| {
            segment_key(&Segment {
                id: id(g, t),
                parent_generation: pg,
            })
        };
        let floor = Requirement {
            head: at(5, 3),
            set_at: 5,
            completeness: Completeness::Complete,
        };
        // H = (5, 3); a late (5, 4) landed after it froze; the next owner
        // snapshotted at (5, 4) without writing.
        let keys = [
            seg(5, 1, 0),
            seg(5, 2, 5),
            seg(5, 3, 5),
            seg(5, 4, 5),
            snapshot_key(id(5, 4)),
        ];
        let listing = Listing::from_keys(keys.iter().map(String::as_str));
        let plan = plan_restore(&listing).unwrap();
        assert_eq!(plan.snapshot, Some(id(5, 4)));
        let chain: Vec<_> = plan.snapshot.into_iter().map(|i| (i, None)).collect();
        assert_eq!(
            prove(Some(&floor), &chain),
            Proof::Missing,
            "the plan alone"
        );
        assert!(linked_back(Some(&floor), id(5, 4), &listing));
        // Not when the floor is not listed, or a link is missing.
        let no_floor = Listing::from_keys(
            [seg(5, 4, 5), snapshot_key(id(5, 4))]
                .iter()
                .map(String::as_str),
        );
        assert!(!linked_back(Some(&floor), id(5, 4), &no_floor));
        let gap = [seg(5, 3, 5), snapshot_key(id(5, 5))];
        let gap = Listing::from_keys(gap.iter().map(String::as_str));
        assert!(!linked_back(Some(&floor), id(5, 5), &gap));
        // Nor through another branch: (5, 4) built on generation 4's 3.
        let fork = [seg(5, 3, 5), seg(5, 4, 4), snapshot_key(id(5, 4))];
        let fork = Listing::from_keys(fork.iter().map(String::as_str));
        assert!(!linked_back(Some(&floor), id(5, 4), &fork));
        // A floor above the base is never linked back to.
        assert!(!linked_back(Some(&floor), id(5, 2), &listing));
    }

    #[test]
    fn combining_follows_the_later_hold_of_the_lineage() {
        let r = |txid| Requirement {
            head: at(4, txid),
            set_at: 4,
            completeness: Completeness::Complete,
        };
        assert_eq!(combine(Some((r(10), 6)), Some((r(5), 5))), Some(r(10)));
        assert_eq!(combine(Some((r(10), 5)), Some((r(12), 7))), Some(r(12)));
        assert_eq!(combine(Some((r(10), 6)), Some((r(5), 6))), Some(r(10)));
        assert_eq!(combine(None, Some((r(5), 5))), Some(r(5)));
        assert_eq!(combine(None, None), None);
    }

    #[test]
    fn a_landed_release_ends_only_its_own_entry() {
        let mut heads = LocalHeads::default();
        heads.register(&hold(5, Some(at(5, 12)), None));
        heads.released("owners/x", 4);
        assert_eq!(heads.len(), 1);
        heads.released("owners/x", 5);
        assert!(heads.is_empty());
        // An older hold ending late keeps the later entry.
        heads.register(&hold(7, Some(at(7, 20)), None));
        heads.register(&hold(5, Some(at(5, 12)), None));
        assert_eq!(heads.get("owners/x").unwrap().generation, 7);
        // An unverified hold with nothing inherited registers nothing.
        let mut heads = LocalHeads::default();
        heads.register(&hold(5, None, None));
        assert!(heads.is_empty());
    }
}
