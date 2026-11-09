//! Restore planning (ADR 0007): which objects of an actor's bucket prefix to
//! apply, given a listing, so that every acknowledged write is restored and
//! nothing a stale owner uploaded after losing its lease is.
//!
//! Every segment's key names the generation of the segment it was built on,
//! so the objects form a tree of histories; the current one is the chain
//! ending at the newest segment of the highest generation. Pure functions of
//! names only: checksums are verified while applying.

use std::collections::{BTreeMap, BTreeSet};

/// A position in an actor's history: transaction `txid` as written by
/// lease generation `generation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId {
    /// Lease generation of the owner that wrote it.
    pub generation: u64,
    /// Transaction id.
    pub txid: u64,
}

/// The empty database, before transaction 1.
pub const EMPTY: SegmentId = SegmentId {
    generation: 0,
    txid: 0,
};

/// One uploaded transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Segment {
    /// Which transaction, by whom.
    pub id: SegmentId,
    /// Generation of the segment at `txid - 1` it was built on (0 for
    /// transaction 1, built on the empty database).
    pub parent_generation: u64,
}

impl Segment {
    /// The position this segment was applied on top of.
    pub fn parent(&self) -> SegmentId {
        if self.id.txid <= 1 {
            return EMPTY;
        }
        SegmentId {
            generation: self.parent_generation,
            txid: self.id.txid - 1,
        }
    }
}

/// What an actor's prefix holds, parsed from a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    /// `wal/<txid>-<generation>-<parent generation>.ltx`
    pub segments: Vec<Segment>,
    /// `snapshot/<txid>-<generation>.ltx`: the database at that position.
    pub snapshots: Vec<SegmentId>,
}

/// The objects to download and apply, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// The snapshot to start from; `None` to start from the empty database.
    pub snapshot: Option<SegmentId>,
    /// Segments after it, ascending.
    pub segments: Vec<SegmentId>,
}

impl Plan {
    /// Where the database is after applying the plan.
    pub fn head(&self) -> SegmentId {
        self.segments
            .last()
            .copied()
            .or(self.snapshot)
            .unwrap_or(EMPTY)
    }
}

/// Why a prefix cannot be restored without losing data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// The current history refers to a segment that is not there.
    #[error("segment {missing:?} is missing from the current history")]
    Missing {
        /// The missing position.
        missing: SegmentId,
    },
}

/// Plans a restore (ADR 0007):
///
/// 1. The head is the newest segment of the highest generation.
/// 2. Follow parent links back to the empty database, stopping early at the
///    newest snapshot on the way.
/// 3. Apply that snapshot, then the segments after it, in order.
///
/// Uploads that landed after their owner lost the lease are on no chain
/// that a later generation built on, so they are not restored (unless
/// nobody wrote after them, in which case they are simply late commits).
pub fn plan_restore(listing: &Listing) -> Result<Plan, PlanError> {
    let parents: BTreeMap<SegmentId, SegmentId> = listing
        .segments
        .iter()
        .map(|s| (s.id, s.parent()))
        .collect();
    let snapshots: BTreeSet<SegmentId> = listing.snapshots.iter().copied().collect();

    let newest =
        |ids: &mut dyn Iterator<Item = SegmentId>| ids.max_by_key(|id| (id.generation, id.txid));
    let Some(head) =
        newest(&mut parents.keys().copied()).or_else(|| newest(&mut snapshots.iter().copied()))
    else {
        return Ok(Plan::default());
    };

    let mut chain = Vec::new();
    let mut at = head;
    let snapshot = loop {
        if at == EMPTY {
            break None;
        }
        if snapshots.contains(&at) {
            break Some(at);
        }
        let Some(&parent) = parents.get(&at) else {
            return Err(PlanError::Missing { missing: at });
        };
        chain.push(at);
        at = parent;
    };
    chain.reverse();
    Ok(Plan {
        snapshot,
        segments: chain,
    })
}

/// Key of a segment under an actor's prefix. Transaction first, so a
/// listing is in transaction order.
pub fn segment_key(segment: &Segment) -> String {
    format!(
        "wal/{:016x}-{:016x}-{:016x}.ltx",
        segment.id.txid, segment.id.generation, segment.parent_generation
    )
}

/// Key of a snapshot under an actor's prefix.
pub fn snapshot_key(id: SegmentId) -> String {
    format!("snapshot/{:016x}-{:016x}.ltx", id.txid, id.generation)
}

/// The objects of an actor's prefix that garbage collection may delete
/// (ADR 0012), as keys relative to the prefix: after a periodic snapshot
/// `snapshot` at `P = (h, t)`, which the owner of generation
/// `owner_generation` uploaded and had acknowledged, every segment or
/// snapshot below `t`, and every one of a generation below `h`, except the
/// snapshot itself and the segment at `P`.
///
/// The segment at `P` is kept because the planner picks the head from
/// segments first: with only the snapshot left, a segment of an earlier
/// generation landing late would become a head whose parent is gone.
///
/// Nothing is collectable (an empty list) unless the snapshot is the
/// owner's own (its generation is `owner_generation`) and both it and the
/// segment at `P` are in the listing. The caller is responsible for the
/// rest of ADR 0012's conditions: the snapshot's upload was created and
/// acknowledged by the owner, and its lease is valid when deleting.
pub fn collectable(listing: &Listing, owner_generation: u64, snapshot: SegmentId) -> Vec<String> {
    let at_p = listing.segments.iter().any(|s| s.id == snapshot);
    if snapshot.generation != owner_generation || !at_p || !listing.snapshots.contains(&snapshot) {
        return Vec::new();
    }
    let below = |id: SegmentId| id.txid < snapshot.txid || id.generation < snapshot.generation;
    let segments = listing
        .segments
        .iter()
        .filter(|s| s.id != snapshot && below(s.id))
        .map(segment_key);
    let snapshots = listing
        .snapshots
        .iter()
        .filter(|&&id| id != snapshot && below(id))
        .map(|&id| snapshot_key(id));
    segments.chain(snapshots).collect()
}

fn hex16(s: &str) -> Option<u64> {
    (s.len() == 16 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        .then(|| u64::from_str_radix(s, 16).ok())
        .flatten()
}

impl Listing {
    /// The highest generation any segment or snapshot names (as its own or
    /// as its parent), 0 for none: an actor's first ownership record goes
    /// above it, so no hold reuses a generation its keys already carry
    /// (ADR 0020, section 6).
    pub fn highest_generation(&self) -> u64 {
        let segments = self
            .segments
            .iter()
            .flat_map(|s| [s.id.generation, s.parent_generation]);
        let snapshots = self.snapshots.iter().map(|s| s.generation);
        segments.chain(snapshots).max().unwrap_or(0)
    }

    /// Sorts keys relative to an actor's prefix into a listing; anything
    /// else is ignored.
    pub fn from_keys<'a>(keys: impl IntoIterator<Item = &'a str>) -> Self {
        let mut listing = Self::default();
        for key in keys {
            if let Some(name) = key
                .strip_prefix("wal/")
                .and_then(|n| n.strip_suffix(".ltx"))
            {
                let mut parts = name.split('-');
                if let (Some(t), Some(g), Some(p), None) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                    && let (Some(txid), Some(generation), Some(parent_generation)) =
                        (hex16(t), hex16(g), hex16(p))
                    && txid > 0
                {
                    listing.segments.push(Segment {
                        id: SegmentId { generation, txid },
                        parent_generation,
                    });
                }
            } else if let Some(name) = key
                .strip_prefix("snapshot/")
                .and_then(|n| n.strip_suffix(".ltx"))
                && let Some((t, g)) = name.split_once('-')
                && let (Some(txid), Some(generation)) = (hex16(t), hex16(g))
            {
                listing.snapshots.push(SegmentId { generation, txid });
            }
        }
        listing
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    #[test]
    fn the_highest_generation_counts_parents_and_snapshots() {
        assert_eq!(Listing::default().highest_generation(), 0);
        let listing = Listing::from_keys([
            "wal/0000000000000001-0000000000000003-0000000000000002.ltx",
            "wal/0000000000000002-0000000000000004-0000000000000003.ltx",
            "snapshot/0000000000000002-0000000000000007.ltx",
            "wal/garbage.ltx",
        ]);
        assert_eq!(listing.highest_generation(), 7);
        let parent_only = Listing {
            segments: vec![Segment {
                id: SegmentId {
                    generation: 2,
                    txid: 1,
                },
                parent_generation: 9,
            }],
            snapshots: Vec::new(),
        };
        assert_eq!(parent_only.highest_generation(), 9);
    }

    fn id(generation: u64, txid: u64) -> SegmentId {
        SegmentId { generation, txid }
    }

    /// `(generation, txid, parent generation)`
    fn listing(segments: &[(u64, u64, u64)], snapshots: &[(u64, u64)]) -> Listing {
        Listing {
            segments: segments
                .iter()
                .map(|&(g, t, p)| Segment {
                    id: id(g, t),
                    parent_generation: p,
                })
                .collect(),
            snapshots: snapshots.iter().map(|&(g, t)| id(g, t)).collect(),
        }
    }

    #[test]
    fn an_empty_prefix_is_a_new_actor() {
        assert_eq!(plan_restore(&Listing::default()).unwrap(), Plan::default());
        assert_eq!(Plan::default().head(), EMPTY);
    }

    #[test]
    fn one_owner_restores_from_its_newest_snapshot() {
        let l = listing(&[(1, 1, 0), (1, 2, 1), (1, 3, 1), (1, 4, 1)], &[(1, 2)]);
        let plan = plan_restore(&l).unwrap();
        assert_eq!(plan.snapshot, Some(id(1, 2)));
        assert_eq!(plan.segments, vec![id(1, 3), id(1, 4)]);
        assert_eq!(plan.head(), id(1, 4));
    }

    #[test]
    fn late_uploads_of_a_stale_owner_are_not_on_the_chain() {
        // Generation 1 wrote 1..=3; generation 2 restored to 3 and wrote 4
        // and 5; then generation 1's late upload of 4 and a snapshot at 4
        // land.
        let l = listing(
            &[
                (1, 1, 0),
                (1, 2, 1),
                (1, 3, 1),
                (2, 4, 1),
                (2, 5, 2),
                (1, 4, 1),
            ],
            &[(1, 4)],
        );
        let plan = plan_restore(&l).unwrap();
        assert_eq!(plan.snapshot, None, "the stale snapshot is off the chain");
        assert_eq!(
            plan.segments,
            vec![id(1, 1), id(1, 2), id(1, 3), id(2, 4), id(2, 5)]
        );
    }

    #[test]
    fn a_late_upload_nobody_built_on_is_a_late_commit() {
        // Generation 2 restored to 3 and wrote nothing; generation 1's late
        // upload of 4 extends the history it restored, so it is kept.
        let l = listing(&[(1, 1, 0), (1, 2, 1), (1, 3, 1), (1, 4, 1)], &[]);
        assert_eq!(plan_restore(&l).unwrap().head(), id(1, 4));
    }

    #[test]
    fn a_missing_link_is_an_error_not_a_rollback() {
        let l = listing(&[(1, 1, 0), (2, 3, 1)], &[]);
        assert_eq!(
            plan_restore(&l),
            Err(PlanError::Missing { missing: id(1, 2) })
        );
    }

    #[test]
    fn a_snapshot_alone_is_a_head() {
        let l = listing(&[], &[(3, 7)]);
        let plan = plan_restore(&l).unwrap();
        assert_eq!((plan.snapshot, plan.head()), (Some(id(3, 7)), id(3, 7)));
    }

    #[test]
    fn keys_round_trip() {
        let seg = Segment {
            id: id(3, 0x2a),
            parent_generation: 2,
        };
        let keys = [
            segment_key(&seg),
            snapshot_key(id(3, 1)),
            "wal/garbage.ltx".to_owned(),
            "wal/0000000000000000-0000000000000001-0000000000000000.ltx".to_owned(),
            "other".to_owned(),
        ];
        let l = Listing::from_keys(keys.iter().map(String::as_str));
        assert_eq!(l, listing(&[(3, 0x2a, 2)], &[(3, 1)]));
        assert_eq!(
            segment_key(&seg),
            "wal/000000000000002a-0000000000000003-0000000000000002.ltx"
        );
    }

    /// The keys of `listing`, minus what `collectable` returned.
    fn collected(listing: &Listing, owner: u64, snapshot: SegmentId) -> (Listing, Vec<String>) {
        let gone = collectable(listing, owner, snapshot);
        let keys: Vec<String> = listing
            .segments
            .iter()
            .map(segment_key)
            .chain(listing.snapshots.iter().map(|&s| snapshot_key(s)))
            .filter(|k| !gone.contains(k))
            .collect();
        (Listing::from_keys(keys.iter().map(String::as_str)), gone)
    }

    fn chain(generation: u64, from: u64, to: u64, first_parent: u64) -> Vec<(u64, u64, u64)> {
        (from..=to)
            .map(|t| {
                (
                    generation,
                    t,
                    if t == from { first_parent } else { generation },
                )
            })
            .collect()
    }

    #[test]
    fn collection_keeps_the_snapshot_the_segment_under_it_and_what_follows() {
        let segments = chain(1, 1, 10, 0);
        let before = listing(&segments, &[(1, 5), (1, 8)]);
        let (after, gone) = collected(&before, 1, id(1, 8));
        assert_eq!(
            gone.len(),
            7 + 1,
            "segments 1 to 7 and the snapshot at 5: {gone:?}"
        );
        assert_eq!(after.snapshots, vec![id(1, 8)]);
        let kept: Vec<u64> = after.segments.iter().map(|s| s.id.txid).collect();
        assert_eq!(kept, vec![8, 9, 10]);
        let (plan, was) = (
            plan_restore(&after).unwrap(),
            plan_restore(&before).unwrap(),
        );
        assert_eq!(plan.head(), was.head());
        assert_eq!(
            plan,
            Plan {
                snapshot: Some(id(1, 8)),
                segments: vec![id(1, 9), id(1, 10)]
            }
        );
    }

    #[test]
    fn collection_removes_earlier_generations_whatever_their_txid() {
        // Generation 1 wrote 1 to 4; generation 2 restored it and wrote 5 to
        // 9; generation 1's late upload of 5 landed (a stale branch), and an
        // old snapshot of generation 1 at 6 too.
        let mut segments = chain(1, 1, 4, 0);
        segments.extend(chain(2, 5, 9, 1));
        segments.push((1, 5, 1));
        let before = listing(&segments, &[(1, 6), (2, 8)]);
        let (after, _) = collected(&before, 2, id(2, 8));
        assert!(
            after
                .segments
                .iter()
                .all(|s| s.id.generation == 2 && s.id.txid >= 8)
        );
        assert_eq!(after.snapshots, vec![id(2, 8)]);
        assert_eq!(plan_restore(&after).unwrap().head(), id(2, 9));
    }

    #[test]
    fn a_late_segment_of_an_earlier_generation_after_collection_does_not_become_the_head() {
        let mut segments = chain(1, 1, 3, 0);
        segments.extend(chain(2, 4, 6, 1));
        let before = listing(&segments, &[(2, 6)]);
        let (mut after, _) = collected(&before, 2, id(2, 6));
        // Only the snapshot and the segment at its position are left; a
        // late upload of generation 1, built on its own 3, lands now.
        assert_eq!(after.segments.len(), 1);
        after.segments.push(Segment {
            id: id(1, 4),
            parent_generation: 1,
        });
        assert_eq!(
            plan_restore(&after).unwrap(),
            Plan {
                snapshot: Some(id(2, 6)),
                segments: vec![]
            }
        );
        // Without the segment at P, the late segment would be the head, and
        // its parent is gone.
        after.segments.retain(|s| s.id != id(2, 6));
        assert!(matches!(
            plan_restore(&after),
            Err(PlanError::Missing { .. })
        ));
    }

    #[test]
    fn nothing_is_collectable_after_a_snapshot_that_is_not_the_owners() {
        // A snapshot at the previous owner's head (taken right after a
        // restore, before this owner wrote anything).
        let before = listing(&chain(1, 1, 70, 0), &[(1, 70)]);
        assert!(collectable(&before, 2, id(1, 70)).is_empty());
    }

    #[test]
    fn nothing_is_collectable_without_the_snapshot_or_the_segment_at_its_position() {
        let segments = chain(1, 1, 10, 0);
        let without_snapshot = listing(&segments, &[]);
        assert!(collectable(&without_snapshot, 1, id(1, 8)).is_empty());
        let mut short = segments.clone();
        short.retain(|&(_, t, _)| t != 8);
        let without_segment = listing(&short, &[(1, 8)]);
        assert!(collectable(&without_segment, 1, id(1, 8)).is_empty());
    }

    #[test]
    fn a_deleted_actor_collects_down_to_its_reset_and_snapshot() {
        // A delete is a reset segment (here 11, generation 2); the owner then
        // snapshots the empty database at its position.
        let mut segments = chain(1, 1, 6, 0);
        segments.extend(chain(2, 7, 11, 1));
        let before = listing(&segments, &[(1, 5)]);
        let mut with_snapshot = before.clone();
        with_snapshot.snapshots.push(id(2, 11));
        let (after, _) = collected(&with_snapshot, 2, id(2, 11));
        assert_eq!(after.segments.len() + after.snapshots.len(), 2);
        assert_eq!(plan_restore(&after).unwrap().head(), id(2, 11));
    }

    #[test]
    fn collecting_after_a_snapshot_at_the_previous_owners_head_would_break_a_restore() {
        // ADR 0012, condition 2 (the review's interleaving). Generation 1
        // wrote 1 to 6; its upload of 7 is held. Generation 2 restores to 6
        // and uploads 7 on top of it, also held; its lease runs out. 1's 7
        // lands. Generation 3 restores to (1, 7) and, having restored many
        // segments, snapshots there without writing.
        let mut segments = chain(1, 1, 7, 0);
        let before = listing(&segments, &[(1, 7)]);
        // With the guard (the owner is generation 3): nothing to collect.
        assert!(collectable(&before, 3, id(1, 7)).is_empty());
        // Without it, collecting from (1, 7) deletes 1 to 6.
        let (mut after, gone) = collected(&before, 1, id(1, 7));
        assert_eq!(gone.len(), 6);
        // Then generation 2's upload lands: a legal late commit, the head
        // (generation 2 is the highest with a segment), built on (1, 6).
        after.segments.push(Segment {
            id: id(2, 7),
            parent_generation: 1,
        });
        assert!(matches!(
            plan_restore(&after),
            Err(PlanError::Missing { missing }) if missing == id(1, 6)
        ));
        // Without collection, that late commit restores.
        segments.push((2, 7, 1));
        let uncollected = listing(&segments, &[(1, 7)]);
        assert_eq!(plan_restore(&uncollected).unwrap().head(), id(2, 7));
    }
}
