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

/// How many sweep periods a sweep with nothing left to do may wait at
/// most: with the default 10 s period, a minute (ADR 0009, amendment of
/// 2026-11-14).
pub const IDLE_SWEEP_FACTOR: u32 = 6;

/// What a sweep found, for the wait before the next one
/// ([`next_sweep_wait_ms`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepFound {
    /// The listing failed, stopped at its scan cap, or listed entries due
    /// to the sweep (delivered or not): the next sweep a period later, as
    /// before the amendment.
    Backlog,
    /// Nothing due; the earliest entry not due yet is due at this time
    /// (ms), `None` if the index is empty.
    Idle { next_at: Option<u64> },
}

/// How long the sweep waits before its next listing (ms): `period_ms` (the
/// configured sweep period) after a sweep with a backlog; otherwise until
/// the earliest entry it listed becomes the sweep's (its due time plus
/// `overdue_ms`), at least a period and at most [`IDLE_SWEEP_FACTOR`]
/// periods. A change in the view of the nodes ends the wait early
/// (the server's `sweep_alarms`): that is when entries of a node gone away
/// need the sweep.
pub fn next_sweep_wait_ms(found: SweepFound, now_ms: u64, period_ms: u64, overdue_ms: u64) -> u64 {
    let longest = period_ms.saturating_mul(u64::from(IDLE_SWEEP_FACTOR));
    match found {
        SweepFound::Backlog => period_ms,
        SweepFound::Idle { next_at: None } => longest,
        SweepFound::Idle { next_at: Some(at) } => at
            .saturating_add(overdue_ms)
            .saturating_sub(now_ms)
            .clamp(period_ms, longest.max(period_ms)),
    }
}

#[cfg(test)]
mod tests;
