//! The bucket's alarm index (ADR 0009): key and meta formats, and the rules
//! that decide which entries an owner may write and remove.
//!
//! An entry is an empty object
//! `alarms/<tenant>/<at, 15 digits>/<class>.t/<name as hex>/<tag>`, where
//! the tag is `<generation, 20 digits>-<nonce as hex>`: the lease generation
//! of the owner that wrote it and a value it never used before. Entries
//! written before tags existed ("legacy") are
//! `alarms/<tenant>/<at>/<class>/<name as hex>`. The two never nest: no key
//! is both an object and the folder of others, which some stores (MinIO)
//! cannot hold. Class names have no `.`, so `<class>.t` is no class's
//! folder, and nodes without tags skip the 4-segment keys. The actor's database
//! records the exact entry it is indexed under (the `alarm_indexed` meta), so
//! the rules are:
//!
//! - **R1.** Every PUT uses a fresh key ([`Entry::owned`] with a new nonce),
//!   so no write ever recreates a key an earlier request may still delete.
//! - **R2.** An owner deletes only exact keys its durable state no longer
//!   names: the entry it replaced, once the replacement is replicated, or
//!   what [`entries_to_clear`] returns.
//! - **R3.** An owner never deletes an entry with a generation above its own
//!   ([`may_delete`]): a newer owner wrote it.
//!
//! Pure functions of strings and numbers, so the simulation in `sim/` can
//! make the same decisions as the server.

use std::fmt;

/// Most hex digits a nonce may have (a parsed key with more is malformed).
const MAX_NONCE_HEX: usize = 64;

/// Appended to the class in tagged keys (class names have no `.`).
const TAGGED: &str = ".t";

/// Who wrote an entry: the owner's lease generation and a nonce it never
/// used before.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tag {
    /// Lease generation of the owner that wrote the entry.
    pub generation: u64,
    /// Lowercase hex, unique to this write.
    nonce: String,
}

impl Tag {
    /// A tag for `generation`; `None` unless `nonce` is 1 to 64 lowercase
    /// hex digits.
    pub fn new(generation: u64, nonce: impl Into<String>) -> Option<Self> {
        let nonce = nonce.into();
        is_nonce(&nonce).then_some(Self { generation, nonce })
    }

    /// The nonce, as written in the key.
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    /// Parses `<generation, 20 digits>-<nonce>`.
    pub fn parse(s: &str) -> Option<Self> {
        let (generation, nonce) = s.split_once('-')?;
        if generation.len() != 20 || !generation.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Self::new(generation.parse().ok()?, nonce)
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}-{}", self.generation, self.nonce)
    }
}

fn is_nonce(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_NONCE_HEX
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// One index entry of an actor: its due time and, unless it is a legacy
/// entry, its tag.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Entry {
    /// Due time, ms since the Unix epoch.
    pub at: u64,
    /// `None`: a legacy entry, `<at>/<class>/<name>` with nothing after.
    pub tag: Option<Tag>,
}

impl Entry {
    /// A legacy entry (only ever read, or removed).
    pub fn legacy(at: u64) -> Self {
        Self { at, tag: None }
    }

    /// A tagged entry; see [`Tag::new`].
    pub fn owned(at: u64, generation: u64, nonce: impl Into<String>) -> Option<Self> {
        Some(Self {
            at,
            tag: Some(Tag::new(generation, nonce)?),
        })
    }

    /// Whether this is a legacy (untagged) entry.
    pub fn is_legacy(&self) -> bool {
        self.tag.is_none()
    }
}

/// `name` as lowercase hex: nothing in it is escaped by an object store.
pub fn encode_name(name: &str) -> String {
    name.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The inverse of [`encode_name`]; `None` unless it is valid hex of UTF-8.
pub fn decode_name(hex: &str) -> Option<String> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

/// The folder, after `alarms/<tenant>/`, of the tagged entries of an actor
/// due at `at`: `<at, 15 digits>/<class>.t/<name as hex>`. Listing it
/// yields exactly those entries.
pub fn tagged_prefix(class: &str, name: &str, at: u64) -> String {
    format!("{at:015}/{class}{TAGGED}/{}", encode_name(name))
}

/// The key of `entry` after `alarms/<tenant>/`.
pub fn key_tail(class: &str, name: &str, entry: &Entry) -> String {
    match &entry.tag {
        Some(tag) => format!("{}/{tag}", tagged_prefix(class, name, entry.at)),
        None => format!("{:015}/{class}/{}", entry.at, encode_name(name)),
    }
}

/// An entry read from a listing of `alarms/<tenant>/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed<'a> {
    /// The actor's class, as in the key.
    pub class: &'a str,
    /// The actor's name, decoded.
    pub name: String,
    /// Due time and tag.
    pub entry: Entry,
}

/// Parses a listed key after `alarms/<tenant>/`: a legacy entry
/// (`<at>/<class>/<hex>`) or a tagged one (`<at>/<class>.t/<hex>/<tag>`).
/// `None` for anything else, which readers skip.
pub fn parse_listed(rel: &str) -> Option<Listed<'_>> {
    let mut parts = rel.split('/');
    let (Some(at), Some(folder), Some(hex), tag, None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    if at.is_empty() || !at.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (class, tag) = match tag {
        None => (folder, None),
        Some(tag) => (folder.strip_suffix(TAGGED)?, Some(Tag::parse(tag)?)),
    };
    if class.is_empty() || class.contains('.') {
        return None;
    }
    Some(Listed {
        class,
        name: decode_name(hex)?,
        entry: Entry {
            at: at.parse().ok()?,
            tag,
        },
    })
}

/// The `alarm_indexed` meta value naming `entry` (empty: nothing indexed).
/// A tagged entry is `<at>/<tag>`; a legacy one is the bare due time, as
/// nodes without tags wrote it.
pub fn to_meta(entry: Option<&Entry>) -> String {
    match entry {
        None => String::new(),
        Some(Entry { at, tag: None }) => at.to_string(),
        Some(Entry { at, tag: Some(tag) }) => format!("{at}/{tag}"),
    }
}

/// The entry an `alarm_indexed` meta value names: `None` for an empty (or
/// unreadable) value, which means nothing is indexed.
pub fn parse_meta(meta: &str) -> Option<Entry> {
    let (at, tag) = match meta.split_once('/') {
        Some((at, tag)) => (at, Some(Tag::parse(tag)?)),
        None => (meta, None),
    };
    if at.is_empty() || !at.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(Entry {
        at: at.parse().ok()?,
        tag,
    })
}

/// Whether the committed alarm `at` needs a new entry (or, for `None`, the
/// indexed one removed), given the entry the actor is indexed under. An
/// unchanged time keeps its entry, unless it is a legacy one: that is
/// replaced by a tagged entry, which migrates the actor.
pub fn needs_publish(indexed: Option<&Entry>, at: Option<u64>) -> bool {
    match (indexed, at) {
        (None, None) => false,
        (Some(indexed), Some(at)) => indexed.at != at || indexed.is_legacy(),
        _ => true,
    }
}

/// R3: whether an owner at `my_generation` may delete `entry`. Never one a
/// newer owner wrote. Legacy entries carry no generation.
pub fn may_delete(entry: &Entry, my_generation: u64) -> bool {
    entry
        .tag
        .as_ref()
        .is_none_or(|tag| tag.generation <= my_generation)
}

/// The entries due at `hint` an owner at `my_generation`, indexed under
/// `indexed` (replicated), removes when its committed alarm is not at
/// `hint`: the tagged ones `listed` there, plus the legacy entry at `hint`
/// (a delete of an absent key succeeds, so it is not listed first). Never
/// `indexed` (R2), never one written by a newer owner (R3).
pub fn entries_to_clear(
    hint: u64,
    listed: &[Entry],
    indexed: Option<&Entry>,
    my_generation: u64,
) -> Vec<Entry> {
    let mut clear: Vec<Entry> = Vec::new();
    let legacy = Entry::legacy(hint);
    for entry in listed
        .iter()
        .filter(|e| e.at == hint)
        .chain(std::iter::once(&legacy))
    {
        if Some(entry) != indexed && may_delete(entry, my_generation) && !clear.contains(entry) {
            clear.push(entry.clone());
        }
    }
    clear
}

/// How one node's alarm sweep treats a listed, overdue index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepClaim {
    /// Not this node's to deliver: a class it does not serve, or another
    /// live node's shard while the entry is not yet anyone's.
    Skip,
    /// In this node's shard (by its view of the live nodes), any age.
    Mine,
    /// Another node's shard, but overdue long enough to be anyone's (the
    /// nodes may disagree about who is alive, or its shard owner may not
    /// deliver it).
    Anyones,
}

/// One sweep's decisions over a page of listed entries ([`plan_sweep`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepPlan {
    /// Indices into the page of the entries to deliver, at most the cap.
    pub deliver: Vec<usize>,
    /// How many entries of the page the sweep walked: the next sweep
    /// resumes after entry `walked - 1` (after the cursor it started from,
    /// for 0) when this is short of the page, else after the page.
    pub walked: usize,
}

/// Which entries of a page (in key order, so due order) one sweep
/// delivers, at most `cap`, and how far it walks.
///
/// The walk delivers this node's own entries in order and stops at the
/// cap; anyone's entries it walked past take what the cap leaves, in
/// order. Own first, because in a backlog older than the anyone's age the
/// live nodes' cursors sit on the same keys: first come, first served,
/// every node delivered the same entries (all but one finding the actor
/// taken), and the cluster recovered at one node's rate. Own
/// first, each live node delivers a different share.
///
/// The server marks `Skip` what it would deliver for nothing: an actor it
/// already has in flight or retrying. Anyone's entries the walk met still
/// keep a share of the cap, an eighth (at least one), so own entries that
/// never leave the index and keep counting (actors another node holds and
/// never finishes) cannot hold them back for good. Which anyone's entries go is rotated: they are taken in order
/// from position `rotation` (modulo their number) among those the walk
/// met, wrapping around. With a fresh random `rotation` every sweep, no
/// fixed set of entries that never leave (an anyone's actor that cannot
/// start) takes the share on every pass, and nodes left with only
/// anyone's entries mostly deliver different ones. An anyone's entry
/// passed over is met again when the listing starts over. The walk never
/// passes an own entry it does not deliver.
pub fn plan_sweep(claims: &[SweepClaim], cap: usize, rotation: u64) -> SweepPlan {
    let reserve = (cap / 8).max(1);
    let mut own = Vec::new();
    let mut anyones = Vec::new();
    let mut walked = claims.len();
    for (i, claim) in claims.iter().enumerate() {
        if own.len() + anyones.len().min(reserve) >= cap {
            walked = i;
            break;
        }
        match claim {
            SweepClaim::Mine => own.push(i),
            SweepClaim::Anyones => anyones.push(i),
            SweepClaim::Skip => {}
        }
    }
    let spare = cap.saturating_sub(own.len());
    let mut deliver = own;
    if !anyones.is_empty() {
        let len = u64::try_from(anyones.len()).unwrap_or(u64::MAX);
        let start = usize::try_from(rotation % len).unwrap_or(0);
        anyones.rotate_left(start);
        deliver.extend(anyones.into_iter().take(spare));
    }
    SweepPlan { deliver, walked }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    /// A page as two nodes see it: entry `i` is in node `i % 2`'s shard,
    /// and anyone's once `i < anyones`.
    fn two_views(len: usize, anyones: usize) -> [Vec<SweepClaim>; 2] {
        [0, 1].map(|node| {
            (0..len)
                .map(|i| match (i % 2 == node, i < anyones) {
                    (true, _) => SweepClaim::Mine,
                    (false, true) => SweepClaim::Anyones,
                    (false, false) => SweepClaim::Skip,
                })
                .collect()
        })
    }

    #[test]
    fn two_nodes_split_a_page_of_anyones_entries() {
        // A backlog overdue three times over is anyone's. Both
        // nodes' cursors sit on the same key; first come, first served,
        // they delivered the same entries and recovered at one node's rate.
        let [a, b] = two_views(2_048, 2_048);
        let (pa, pb) = (plan_sweep(&a, 256, 0), plan_sweep(&b, 256, 0));
        assert_eq!((pa.deliver.len(), pb.deliver.len()), (256, 256));
        // Each delivers mostly its own share; only the reserve for
        // anyone's entries (an eighth) may overlap the other's.
        let shared = pa.deliver.iter().filter(|i| pb.deliver.contains(i)).count();
        assert!(shared <= 64, "{shared} entries delivered by both nodes");
        for (plan, claims) in [(&pa, &a), (&pb, &b)] {
            let own = plan
                .deliver
                .iter()
                .filter(|&&i| claims[i] == SweepClaim::Mine)
                .count();
            assert_eq!(own, 224);
            assert!((440..460).contains(&plan.walked), "{}", plan.walked);
        }
    }

    #[test]
    fn own_entries_that_never_leave_cannot_hold_anyones_back() {
        // Every page has more own entries than the cap (actors that cannot
        // start keep their entries): anyone's still get their share.
        let claims: Vec<_> = (0..2_048)
            .map(|i| match i % 4 {
                0 => SweepClaim::Anyones,
                _ => SweepClaim::Mine,
            })
            .collect();
        let plan = plan_sweep(&claims, 256, 0);
        let anyones: Vec<_> = plan
            .deliver
            .iter()
            .filter(|&&i| claims[i] == SweepClaim::Anyones)
            .collect();
        assert_eq!(anyones.len(), 32);
        assert_eq!(*anyones[0], 0, "from the rotation's start");
        assert_eq!(plan.deliver.len(), 256);

        // Rotated, the share goes to other anyone's entries of the same
        // walk, so the same ones (actors that cannot start either) do not
        // take it on every pass; the rotation wraps.
        let met = (0..plan.walked)
            .filter(|&i| claims[i] == SweepClaim::Anyones)
            .count() as u64;
        let mut covered: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for rotation in 0..met {
            let rotated = plan_sweep(&claims, 256, rotation);
            assert_eq!(rotated.walked, plan.walked);
            let picked: Vec<_> = rotated
                .deliver
                .iter()
                .filter(|&&i| claims[i] == SweepClaim::Anyones)
                .collect();
            assert_eq!(picked.len(), 32);
            covered.extend(picked.into_iter().copied());
        }
        assert_eq!(
            covered.len() as u64,
            met,
            "every anyone's entry walked is reachable"
        );
        assert_eq!(plan_sweep(&claims, 256, met), plan);
    }

    #[test]
    fn the_walk_never_passes_an_own_entry_it_did_not_deliver() {
        for anyones in [0, 100, 600, 2_048] {
            for claims in two_views(2_048, anyones) {
                let plan = plan_sweep(&claims, 256, 0);
                for (i, claim) in claims[..plan.walked].iter().enumerate() {
                    if *claim == SweepClaim::Mine {
                        assert!(plan.deliver.contains(&i), "own entry {i} passed over");
                    }
                }
                assert!(plan.deliver.iter().all(|&i| i < plan.walked));
            }
        }
    }

    #[test]
    fn anyones_entries_fill_what_the_cap_leaves() {
        // Few own entries: the rest of the cap goes to anyone's, in order,
        // so an entry whose shard owner never delivers it still goes.
        let mut claims = vec![SweepClaim::Anyones; 10];
        claims.push(SweepClaim::Mine);
        claims.extend([SweepClaim::Skip; 5]);
        let plan = plan_sweep(&claims, 4, 0);
        assert_eq!(plan.deliver, [10, 0, 1, 2]);
        assert_eq!(plan.walked, claims.len());
        // Only anyone's (a node whose shard is empty here): first come.
        let plan = plan_sweep(&[SweepClaim::Anyones; 6], 4, 0);
        assert_eq!((plan.deliver, plan.walked), (vec![0, 1, 2, 3], 6));
        // Nothing to walk past with no cap at all.
        assert_eq!(plan_sweep(&claims, 0, 0).walked, 0);
        assert!(plan_sweep(&[], 4, 0).deliver.is_empty());
    }

    fn owned(at: u64, generation: u64, nonce: &str) -> Entry {
        Entry::owned(at, generation, nonce).unwrap()
    }

    #[test]
    fn keys_round_trip_through_a_listing() {
        let tagged = owned(1_700_000_000_000, 7, "a1b2c3");
        let tail = key_tail("chat", "Room 1", &tagged);
        assert_eq!(
            tail,
            "001700000000000/chat.t/526f6f6d2031/00000000000000000007-a1b2c3"
        );
        let listed = parse_listed(&tail).unwrap();
        assert_eq!(
            (listed.class, listed.name.as_str(), &listed.entry),
            ("chat", "Room 1", &tagged)
        );

        let legacy = Entry::legacy(5);
        let tail = key_tail("chat", "é", &legacy);
        assert_eq!(tail, "000000000000005/chat/c3a9");
        // Never a folder of tagged keys: they are under a sibling.
        assert_eq!(tagged_prefix("chat", "é", 5), "000000000000005/chat.t/c3a9");
        assert!(!key_tail("chat", "é", &owned(5, 1, "a")).starts_with(&format!("{tail}/")));
        let listed = parse_listed(&tail).unwrap();
        assert_eq!((listed.name.as_str(), &listed.entry), ("é", &legacy));

        // The largest values still fit their fields and parse back.
        let big = owned(u64::MAX, u64::MAX, &"f".repeat(64));
        assert_eq!(parse_listed(&key_tail("c", "n", &big)).unwrap().entry, big);
    }

    #[test]
    fn malformed_keys_are_skipped() {
        for good in [
            "000000000000005/c/6e",
            "000000000000005/c.t/6e/00000000000000000007-ab",
        ] {
            assert!(parse_listed(good).is_some(), "{good:?}");
        }
        for bad in [
            "",
            "000000000000005",
            "000000000000005/c",
            "x00000000000005/c/6e",
            "/c/6e",
            "000000000000005//6e",
            "000000000000005/c/6",    // odd hex
            "000000000000005/c/zz",   // not hex
            "000000000000005/c/ff",   // not UTF-8
            "000000000000005/c.t/6e", // a tagged folder without a tag
            "000000000000005/c.x/6e", // not a class
            "000000000000005/.t/6e/00000000000000000007-ab",
            "000000000000005/c.t.t/6e/00000000000000000007-ab",
            // Tagged keys under the legacy key (never written by any node).
            "000000000000005/c/6e/00000000000000000007-ab",
            "000000000000005/c.t/6e/",
            "000000000000005/c.t/6e/7-ab",
            "000000000000005/c.t/6e/0000000000000000000x-ab",
            "000000000000005/c.t/6e/00000000000000000007",
            "000000000000005/c.t/6e/00000000000000000007-",
            "000000000000005/c.t/6e/00000000000000000007-AB",
            "000000000000005/c.t/6e/00000000000000000007-ab-cd",
            "000000000000005/c.t/6e/00000000000000000007-ab/x",
            "99999999999999999999999/c/6e", // overflows u64
        ] {
            assert_eq!(parse_listed(bad), None, "{bad:?}");
        }
        let long = format!(
            "000000000000005/c.t/6e/00000000000000000007-{}",
            "a".repeat(65)
        );
        assert_eq!(parse_listed(&long), None);
    }

    #[test]
    fn a_mixed_listing_parses_in_key_order() {
        // Legacy and tagged entries of one actor at one time, another actor,
        // and junk: readers get every valid entry, in due-time order (at one
        // time, tagged keys sort first: `.` comes before `/`).
        let mut keys = [
            "000000000000020/c/61",
            "000000000000010/c.t/61/00000000000000000003-0f",
            "000000000000010/c/61",
            "000000000000010/c.t/62/00000000000000000001-1",
            "000000000000010/c.t/61/00000000000000000002-ff",
            "000000000000015/c.t/61/garbage",
            "000000000000015/c",
        ];
        keys.sort_unstable();
        let parsed: Vec<(String, Entry)> = keys
            .iter()
            .filter_map(|k| parse_listed(k))
            .map(|l| (l.name, l.entry))
            .collect();
        assert_eq!(
            parsed,
            [
                ("a".to_owned(), owned(10, 2, "ff")),
                ("a".to_owned(), owned(10, 3, "0f")),
                ("b".to_owned(), owned(10, 1, "1")),
                ("a".to_owned(), Entry::legacy(10)),
                ("a".to_owned(), Entry::legacy(20)),
            ]
        );
    }

    #[test]
    fn meta_round_trips_and_reads_legacy_values() {
        assert_eq!(to_meta(None), "");
        assert_eq!(parse_meta(""), None);
        let tagged = owned(1_234, 9, "beef");
        assert_eq!(to_meta(Some(&tagged)), "1234/00000000000000000009-beef");
        assert_eq!(parse_meta(&to_meta(Some(&tagged))), Some(tagged));
        // What nodes without tags wrote.
        assert_eq!(parse_meta("1234"), Some(Entry::legacy(1_234)));
        assert_eq!(to_meta(Some(&Entry::legacy(1_234))), "1234");
        for bad in [
            "x",
            "-1",
            "+1",
            "1234/",
            "/00000000000000000009-beef",
            "1234/9-beef",
            "1234/00000000000000000009-",
            "1234/00000000000000000009-beef/1",
        ] {
            assert_eq!(parse_meta(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn only_a_change_or_a_legacy_entry_needs_publishing() {
        let tagged = owned(10, 1, "a");
        assert!(!needs_publish(None, None));
        assert!(needs_publish(None, Some(10)));
        assert!(needs_publish(Some(&tagged), None));
        assert!(needs_publish(Some(&tagged), Some(11)));
        assert!(
            !needs_publish(Some(&tagged), Some(10)),
            "unchanged: no write, whatever the generation now"
        );
        assert!(
            needs_publish(Some(&Entry::legacy(10)), Some(10)),
            "migrated"
        );
        assert!(needs_publish(Some(&Entry::legacy(10)), None));
    }

    #[test]
    fn the_generation_limits_what_may_be_deleted() {
        assert!(may_delete(&owned(1, 4, "a"), 5));
        assert!(may_delete(&owned(1, 5, "a"), 5));
        assert!(!may_delete(&owned(1, 6, "a"), 5));
        assert!(may_delete(&Entry::legacy(1), 0));
    }

    #[test]
    fn clearing_spares_the_indexed_entry_and_newer_owners() {
        let hint = 100;
        let mine = owned(hint, 5, "aa");
        let older = owned(hint, 3, "bb");
        let newer = owned(hint, 6, "cc");
        let elsewhere = owned(hint + 1, 2, "dd");
        let listed = [
            mine.clone(),
            older.clone(),
            newer.clone(),
            elsewhere,
            older.clone(),
        ];

        // Nothing indexed at the hint: everything at the hint from this
        // generation or older goes, the legacy key too, each once.
        assert_eq!(
            entries_to_clear(hint, &listed, None, 5),
            [mine.clone(), older.clone(), Entry::legacy(hint)]
        );
        // The indexed entry stays, whoever wrote it.
        assert_eq!(
            entries_to_clear(hint, &listed, Some(&older), 5),
            [mine.clone(), Entry::legacy(hint)]
        );
        assert_eq!(
            entries_to_clear(hint, &listed, Some(&mine), 5),
            [older.clone(), Entry::legacy(hint)]
        );
        // A legacy indexed entry at the hint stays too.
        assert_eq!(
            entries_to_clear(hint, &listed, Some(&Entry::legacy(hint)), 5),
            [mine.clone(), older.clone()]
        );
        // An owner that is itself stale spares what newer owners wrote.
        assert_eq!(
            entries_to_clear(hint, &listed, None, 4),
            [older, Entry::legacy(hint)]
        );
        // An empty listing still clears the legacy key.
        assert_eq!(entries_to_clear(hint, &[], None, 1), [Entry::legacy(hint)]);
    }

    #[test]
    fn clearing_never_returns_the_indexed_entry_or_a_newer_one() {
        // Exhaustive over small cases: every subset of candidate entries
        // listed, each candidate (or nothing) indexed, each generation.
        let candidates = [
            Entry::legacy(7),
            owned(7, 1, "a"),
            owned(7, 2, "a"),
            owned(7, 2, "b"),
            owned(7, 3, "a"),
            owned(8, 2, "a"),
        ];
        for mask in 0u32..(1 << candidates.len()) {
            let listed: Vec<Entry> = candidates
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, e)| e.clone())
                .collect();
            for indexed in std::iter::once(None).chain(candidates.iter().map(Some)) {
                for generation in 0..4 {
                    let clear = entries_to_clear(7, &listed, indexed, generation);
                    for e in &clear {
                        assert_ne!(Some(e), indexed, "{listed:?} {indexed:?} {generation}");
                        assert!(may_delete(e, generation), "{e:?} at {generation}");
                        assert_eq!(e.at, 7);
                    }
                }
            }
        }
    }

    #[test]
    fn nonces_are_lowercase_hex() {
        assert!(Tag::new(1, "0123456789abcdef").is_some());
        for bad in ["", "A", "g", "-", "a/b", &"a".repeat(65)] {
            assert!(Tag::new(1, bad).is_none(), "{bad:?}");
        }
    }
}
