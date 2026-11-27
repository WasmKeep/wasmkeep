//! The generation floor of an app
//! ([ADR 0045](../../../docs/adr/0045-the-generation-floor-of-a-restored-app.md)):
//! one object per app, `state/<tenant>/<app>/generation-floor`, below
//! which no actor of the app gets a first ownership record. A whole-app
//! restore writes it once the app's upgrade floor is `0.10`, where it
//! wrote one per-actor lease marker before; `wasmkeep cluster markers
//! clean` moves the markers that exist into it and deletes them.
//!
//! - [`GenerationFloor`], [`decode`] and [`raise`]: the object, exact,
//!   written only upward by conditional writes.
//! - [`seed`]: an actor's first generation at its first acquisition
//!   (section 2), in the order that makes the cleanup safe: the marker,
//!   then the upgrade floor, then the generation floor.
//! - [`plan_clean`] and [`clean`]: the cleanup (section 4), which raises
//!   the floor before it deletes a marker that is the only carrier of a
//!   seed.

use crate::app_format::floor::Minor;
use crate::lease::{Bucket, BucketError, Clock, LeaseConfig, LeaseRecord, Version};
use crate::node_lease::{AcquireError, decode_marker, encode_marker};
use crate::state_root::{Family, StateRoot};

/// The object's first line.
pub const HEADER: &str = "wasmkeep-generation-floor 1";

/// The object's name under the app's state root.
pub const NAME: &str = "generation-floor";

/// The minor version whose tree writes the object, and from whose floor
/// the per-actor marker is no longer written (ADR 0045, section 5).
pub const SINCE: Minor = Minor {
    major: 0,
    minor: 10,
};

/// The capability word every node that reads the object announces.
pub const WORD: &str = "genfloor";

/// The object's key under the bucket prefix: `state/<tenant>/<app>/generation-floor`.
pub fn key(root: &StateRoot) -> String {
    format!("{}{NAME}", root.bucket())
}

/// Whether per-actor markers may be left under the app.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Markers {
    /// Some may be: a node reads the actor's marker at a first
    /// acquisition.
    Kept,
    /// Readers may ignore any marker left: none carries a generation its
    /// actor's keys, record or the floor does not, and a node skips the
    /// marker's read. Not a promise that none exists: an acquisition that
    /// read the upgrade floor just before the finalize may land its marker
    /// (its actor's keys alone) after this is written.
    None,
}

/// What the object says (absent: [`GenerationFloor::ABSENT`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationFloor {
    /// No actor of the app gets a first ownership record at or below it.
    pub generation: u64,
    /// Whether markers may be left.
    pub markers: Markers,
}

impl GenerationFloor {
    /// What an absent object reads as: generation 0, markers kept.
    pub const ABSENT: Self = Self {
        generation: 0,
        markers: Markers::Kept,
    };

    /// The object's body, exactly.
    pub fn encode(&self) -> Vec<u8> {
        let markers = match self.markers {
            Markers::Kept => "kept",
            Markers::None => "none",
        };
        format!("{HEADER}\n{}\nmarkers {markers}\n", self.generation).into_bytes()
    }

    /// The higher of both: the generation upward, `kept` to `none` only.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        Self {
            generation: self.generation.max(other.generation),
            markers: self.markers.max(other.markers),
        }
    }
}

/// The object holds something this version cannot read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the app's generation floor is unreadable ({len} bytes)")]
pub struct Unreadable {
    /// Its length.
    pub len: usize,
}

/// What a read of the object gives: only the exact form
/// [`GenerationFloor::encode`] writes is read; absent is
/// [`GenerationFloor::ABSENT`].
///
/// # Errors
/// Anything else.
pub fn decode(body: Option<&[u8]>) -> Result<GenerationFloor, Unreadable> {
    let Some(body) = body else {
        return Ok(GenerationFloor::ABSENT);
    };
    let unreadable = Unreadable { len: body.len() };
    let text = std::str::from_utf8(body).map_err(|_| unreadable.clone())?;
    let rest = text
        .strip_prefix(HEADER)
        .and_then(|t| t.strip_prefix('\n'))
        .ok_or_else(|| unreadable.clone())?;
    let (number, rest) = rest.split_once('\n').ok_or_else(|| unreadable.clone())?;
    let generation = crate::recorded_head::number(number).ok_or_else(|| unreadable.clone())?;
    let markers = match rest {
        "markers kept\n" => Markers::Kept,
        "markers none\n" => Markers::None,
        _ => return Err(unreadable),
    };
    let floor = GenerationFloor {
        generation,
        markers,
    };
    if floor.encode() != body {
        return Err(unreadable);
    }
    Ok(floor)
}

/// Why the object could not be read or written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FloorError {
    /// Its body is not one this version reads: nothing is written over it.
    #[error(
        "the app's generation floor `{key}` is unreadable ({len} bytes); refusing to write. \
         It has no repair command: rewrite it by hand"
    )]
    Unreadable {
        /// The key.
        key: String,
        /// Its length.
        len: usize,
    },
    /// The bucket did not answer, or other writers won every race.
    #[error("the bucket: {0}")]
    Bucket(String),
}

/// Reads the object at `key`.
///
/// # Errors
/// Unreadable (naming the key), or the bucket did not answer.
pub async fn read<B: Bucket>(
    bucket: &B,
    key: &str,
) -> Result<(GenerationFloor, Option<Version>), FloorError> {
    let read = bucket
        .get(key)
        .await
        .map_err(|e| FloorError::Bucket(e.to_string()))?;
    let floor = decode(read.as_ref().map(|(body, _)| body.as_slice())).map_err(|e| {
        FloorError::Unreadable {
            key: key.to_owned(),
            len: e.len,
        }
    })?;
    Ok((floor, read.map(|(_, version)| version)))
}

/// Raises the object at `key` to at least `to` (its generation upward,
/// `kept` to `none`), by conditional writes only: created when absent,
/// replaced only at the version read. One already at or above `to` is
/// kept and nothing is written. A lost race is read again. Gives the
/// object as it is after.
///
/// # Errors
/// Unreadable (nothing written over it), or the bucket did not answer.
pub async fn raise<B: Bucket>(
    bucket: &B,
    key: &str,
    to: GenerationFloor,
) -> Result<GenerationFloor, FloorError> {
    for _ in 0..8 {
        let (found, version) = read(bucket, key).await?;
        let wanted = found.max(to);
        if version.is_some() && wanted == found {
            return Ok(found);
        }
        let written = match &version {
            Some(version) => bucket.replace(key, wanted.encode(), version).await,
            None => bucket.create(key, wanted.encode()).await,
        };
        match written {
            Ok(_) => return Ok(wanted),
            Err(BucketError::Conflict) => {}
            Err(e) => return Err(FloorError::Bucket(e.to_string())),
        }
    }
    Err(FloorError::Bucket(
        "other writers changed it on every try".to_owned(),
    ))
}

/// What an actor's per-actor lease key holds (ADR 0020, section 6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerRead {
    /// Nothing.
    Absent,
    /// A marker (`wasmkeep-lease 2`) and the generation it carries.
    Marker(u64),
    /// A per-actor lease from before node leases (`wasmkeep-lease 1`),
    /// and the version read.
    Old(LeaseRecord, Version),
}

impl MarkerRead {
    /// The generation it carries (0 for nothing).
    pub fn generation(&self) -> u64 {
        match self {
            Self::Absent => 0,
            Self::Marker(n) => *n,
            Self::Old(old, _) => old.generation,
        }
    }

    /// A lease from before node leases that is still live by the old
    /// rules (`expires_at + grace` not passed on `clock`): `Held`, naming
    /// its node.
    ///
    /// # Errors
    /// That lease.
    pub fn refuse_live<C: Clock>(
        &self,
        clock: &C,
        config: &LeaseConfig,
    ) -> Result<(), AcquireError> {
        match self {
            Self::Old(old, _) if old_live(old, clock.now_ms(), config) => Err(AcquireError::Held {
                node: old.owner.clone(),
                instance: String::new(),
            }),
            _ => Ok(()),
        }
    }
}

/// Whether a lease from before node leases is live by the old rules at
/// `now_ms`.
pub fn old_live(old: &LeaseRecord, now_ms: u64, config: &LeaseConfig) -> bool {
    old.expires_at_ms != 0 && now_ms <= old.expires_at_ms.saturating_add(config.grace_ms())
}

/// What `body` at a per-actor lease key is; `None` for neither form.
pub fn decode_lease_key(body: &[u8], version: Version) -> Option<MarkerRead> {
    if let Some(n) = decode_marker(body) {
        return Some(MarkerRead::Marker(n));
    }
    LeaseRecord::decode(body).map(|old| MarkerRead::Old(old, version))
}

/// Reads the per-actor lease key `key`.
///
/// # Errors
/// `Unavailable` without an answer, `Corrupt` for a body of neither form.
pub async fn read_marker<B: Bucket>(bucket: &B, key: &str) -> Result<MarkerRead, AcquireError> {
    match bucket
        .get(key)
        .await
        .map_err(|_| AcquireError::Unavailable)?
    {
        None => Ok(MarkerRead::Absent),
        Some((body, version)) => decode_lease_key(&body, version).ok_or(AcquireError::Corrupt),
    }
}

/// `0.9`'s path from what was read at `key` (ADR 0020, section 6): the
/// seed (the record goes at that + 1), at least `keys_generation`. Absent:
/// a marker carrying the keys' generation is created. A marker: kept, and
/// the higher of the two. An old lease: refused while live, otherwise
/// replaced (`If-Match`) by a marker carrying the higher of its generation
/// and the keys'. Byte for byte what `0.9` writes.
///
/// # Errors
/// `Held` for a live old lease; `Unavailable` for a write that did not
/// land (a conflict: another node marked it, or an old one wrote; the
/// next acquisition reads it again).
pub async fn mark_from_read<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    config: &LeaseConfig,
    key: &str,
    read: MarkerRead,
    keys_generation: u64,
) -> Result<u64, AcquireError> {
    read.refuse_live(clock, config)?;
    let (seed, written) = match read {
        MarkerRead::Absent => (
            keys_generation,
            bucket.create(key, encode_marker(keys_generation)).await,
        ),
        MarkerRead::Marker(marked) => return Ok(marked.max(keys_generation)),
        MarkerRead::Old(old, version) => {
            let seed = old.generation.max(keys_generation);
            (
                seed,
                bucket.replace(key, encode_marker(seed), &version).await,
            )
        }
    };
    written.map(|_| seed).map_err(|_| AcquireError::Unavailable)
}

/// `0.10`'s path (ADR 0045, section 2, step 5): the seed is the highest
/// of the keys, what the marker key held and the generation floor, and
/// nothing is written. A live old lease refuses, as before.
///
/// # Errors
/// `Held` for a live old lease.
pub fn seed_above<C: Clock>(
    clock: &C,
    config: &LeaseConfig,
    read: &MarkerRead,
    keys_generation: u64,
    floor: GenerationFloor,
) -> Result<u64, AcquireError> {
    read.refuse_live(clock, config)?;
    Ok(keys_generation.max(read.generation()).max(floor.generation))
}

/// An actor's first generation, and what the node learned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seeded {
    /// The seed: the first record goes at that + 1.
    pub seed: u64,
    /// The generation floor said `markers none`: the node may skip the
    /// marker's read from now on.
    pub markers_none: bool,
}

/// The seed of an actor's first ownership record (ADR 0045, section 2),
/// `keys_generation` the highest generation its history keys carry:
///
/// 1. unless `markers_none` (this node read `markers none` before): reads
///    the marker key `lease_key`;
/// 2. asks `at_floor` whether the app's upgrade floor is at [`SINCE`]
///    (the caller reads it again while the floor it knows is below);
/// 3. below: `0.9`'s path with what step 1 read ([`mark_from_read`]),
///    and no generation floor is read;
/// 4. at it: reads the generation floor at `floor_key` and returns
///    [`seed_above`], writing nothing.
///
/// The order is what makes the cleanup safe: it runs only at the floor,
/// raises the generation floor, then deletes the marker. A node that read
/// the upgrade floor below read the marker before any cleanup started; one
/// at it that found no marker reads a generation floor that covers it.
///
/// # Errors
/// `Unavailable` for a read without an answer or a write that did not
/// land; `Corrupt` for a marker key or a generation floor this version
/// cannot read; `Held` for a live old lease; what `at_floor` gives.
#[allow(clippy::too_many_arguments)]
pub async fn seed<B: Bucket, C: Clock>(
    bucket: &B,
    clock: &C,
    config: &LeaseConfig,
    lease_key: &str,
    floor_key: &str,
    keys_generation: u64,
    markers_none: bool,
    at_floor: impl AsyncFnOnce() -> Result<bool, AcquireError>,
) -> Result<Seeded, AcquireError> {
    let marker = if markers_none {
        MarkerRead::Absent
    } else {
        read_marker(bucket, lease_key).await?
    };
    if !at_floor().await? {
        let seed =
            mark_from_read(bucket, clock, config, lease_key, marker, keys_generation).await?;
        return Ok(Seeded {
            seed,
            markers_none: false,
        });
    }
    let (floor, _) = read(bucket, floor_key).await.map_err(|e| match e {
        FloorError::Unreadable { .. } => AcquireError::Corrupt,
        FloorError::Bucket(_) => AcquireError::Unavailable,
    })?;
    Ok(Seeded {
        seed: seed_above(clock, config, &marker, keys_generation, floor)?,
        markers_none: floor.markers == Markers::None,
    })
}

/// One key under the app's `leases/` and what the cleanup does with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leftover {
    /// The actor's ownership record is at or above the marker's number:
    /// the record carries it. Deleted.
    Carried {
        /// The key, under the bucket prefix.
        key: String,
        /// The marker's number.
        generation: u64,
    },
    /// The marker (or an old lease that is not live) is the only carrier:
    /// its number is folded into the floor first, then it is deleted.
    Folded {
        /// The key, under the bucket prefix.
        key: String,
        /// The generation it carries.
        generation: u64,
    },
}

impl Leftover {
    /// The key.
    pub fn key(&self) -> &str {
        match self {
            Self::Carried { key, .. } | Self::Folded { key, .. } => key,
        }
    }
}

/// The per-marker rule of the cleanup (ADR 0045, section 4): `record`
/// the actor's ownership record's generation, if it has one.
pub fn judge(key: String, generation: u64, record: Option<u64>) -> Leftover {
    if record.is_some_and(|r| r >= generation) {
        Leftover::Carried { key, generation }
    } else {
        Leftover::Folded { key, generation }
    }
}

/// What the cleanup found, before it writes anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanPlan {
    /// The generation floor's key.
    pub floor_key: String,
    /// The generation floor as read.
    pub floor: GenerationFloor,
    /// Every key under `leases/`, judged.
    pub leftovers: Vec<Leftover>,
}

impl CleanPlan {
    /// The floor the run raises to before any delete: the highest folded
    /// generation, or `None` when the floor already covers every one.
    pub fn raise_to(&self) -> Option<u64> {
        self.leftovers
            .iter()
            .filter_map(|l| match l {
                Leftover::Folded { generation, .. } => Some(*generation),
                Leftover::Carried { .. } => None,
            })
            .max()
            .filter(|g| *g > self.floor.generation)
    }

    /// How many the record carries, and how many are folded.
    pub fn counts(&self) -> (usize, usize) {
        let carried = self
            .leftovers
            .iter()
            .filter(|l| matches!(l, Leftover::Carried { .. }))
            .count();
        (carried, self.leftovers.len() - carried)
    }
}

/// Why the cleanup stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CleanError {
    /// A per-actor lease from before node leases is live by the old rules.
    #[error(
        "`{key}` is a per-actor lease from before node leases, held by `{node}` and live by \
         the old rules: a binary older than 0.4 may run against this bucket. Stop it; nothing \
         was deleted"
    )]
    Live {
        /// The key.
        key: String,
        /// The node it names.
        node: String,
    },
    /// A key under `leases/` or an ownership record this version cannot
    /// read.
    #[error("`{key}` is not a marker, an old lease or an ownership record this version reads")]
    Unknown {
        /// The key.
        key: String,
    },
    /// The generation floor.
    #[error(transparent)]
    Floor(#[from] FloorError),
}

/// Reads what the cleanup of `root`'s app works on (ADR 0045, section 4):
/// every key under its `leases/`, each with the actor's ownership record.
/// Writes nothing.
///
/// # Errors
/// A live old lease, a key it cannot read, an unreadable generation floor,
/// or the bucket did not answer.
pub async fn plan_clean<B: crate::store::Store, C: Clock>(
    bucket: &B,
    root: &StateRoot,
    clock: &C,
    config: &LeaseConfig,
) -> Result<CleanPlan, CleanError> {
    let bucket_err = |e: BucketError| CleanError::Floor(FloorError::Bucket(e.to_string()));
    let floor_key = key(root);
    let (floor, _) = read(bucket, &floor_key).await?;
    let leases = format!("{}/", root.base(Family::Leases));
    let owners = format!("{}/", root.base(Family::Owners));
    let mut leftovers = Vec::new();
    for meta in bucket.list(&leases).await.map_err(bucket_err)? {
        let key = meta.key;
        let Some(tail) = key.strip_prefix(&leases) else {
            continue;
        };
        // Deleted since the listing: nothing to carry.
        let Some((body, version)) = bucket.get(&key).await.map_err(bucket_err)? else {
            continue;
        };
        let read = decode_lease_key(&body, version)
            .ok_or_else(|| CleanError::Unknown { key: key.clone() })?;
        if let MarkerRead::Old(old, _) = &read
            && old_live(old, clock.now_ms(), config)
        {
            return Err(CleanError::Live {
                key,
                node: old.owner.clone(),
            });
        }
        let owner = format!("{owners}{tail}");
        let record = match bucket.get(&owner).await.map_err(bucket_err)? {
            None => None,
            Some((body, _)) => Some(
                crate::node_lease::OwnerRecord::decode(&body)
                    .ok_or(CleanError::Unknown { key: owner })?
                    .generation,
            ),
        };
        leftovers.push(judge(key, read.generation(), record));
    }
    Ok(CleanPlan {
        floor_key,
        floor,
        leftovers,
    })
}

/// What [`clean`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cleaned {
    /// The keys deleted.
    pub deleted: usize,
    /// The generation floor after the run.
    pub floor: GenerationFloor,
    /// Keys under `leases/` the listing after the deletes still showed
    /// (written meanwhile): `markers none` was not written.
    pub left: usize,
}

/// Runs `plan` (ADR 0045, section 4): raises the generation floor to the
/// highest folded number in one conditional write, **then** deletes each
/// key; when a listing after the deletes shows `leases/` empty, writes
/// `markers none`. Idempotent; an interrupt at any point leaves every
/// actor's next generation above what its history carried, since no
/// marker that is the only carrier is deleted before the floor covers it.
///
/// # Errors
/// An unreadable generation floor, or the bucket did not answer.
pub async fn clean<B: crate::store::Store>(
    bucket: &B,
    root: &StateRoot,
    plan: &CleanPlan,
) -> Result<Cleaned, CleanError> {
    let bucket_err = |e: BucketError| CleanError::Floor(FloorError::Bucket(e.to_string()));
    let mut floor = plan.floor;
    if let Some(to) = plan.raise_to() {
        floor = raise(
            bucket,
            &plan.floor_key,
            GenerationFloor {
                generation: to,
                markers: Markers::Kept,
            },
        )
        .await?;
    }
    for leftover in &plan.leftovers {
        bucket.delete(leftover.key()).await.map_err(bucket_err)?;
    }
    let leases = format!("{}/", root.base(Family::Leases));
    let left = bucket.list(&leases).await.map_err(bucket_err)?.len();
    if left == 0 {
        floor = raise(
            bucket,
            &plan.floor_key,
            GenerationFloor {
                generation: 0,
                markers: Markers::None,
            },
        )
        .await?;
    }
    Ok(Cleaned {
        deleted: plan.leftovers.len(),
        floor,
        left,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn floor(generation: u64, markers: Markers) -> GenerationFloor {
        GenerationFloor {
            generation,
            markers,
        }
    }

    #[test]
    fn the_generation_floor_is_absent_exact_or_refused() {
        assert_eq!(decode(None), Ok(GenerationFloor::ABSENT));
        assert_eq!(
            floor(7, Markers::Kept).encode(),
            b"wasmkeep-generation-floor 1\n7\nmarkers kept\n"
        );
        for f in [
            floor(0, Markers::Kept),
            floor(1, Markers::None),
            floor(u64::MAX, Markers::None),
        ] {
            assert_eq!(decode(Some(&f.encode())), Ok(f));
        }
        for bad in [
            &b""[..],
            b"wasmkeep-generation-floor 1\n",
            b"wasmkeep-generation-floor 1\n7\n",
            b"wasmkeep-generation-floor 1\n7\nmarkers kept",
            b"wasmkeep-generation-floor 1\n7\nmarkers kept\n\n",
            b"wasmkeep-generation-floor 1\n7\nmarkers some\n",
            b"wasmkeep-generation-floor 1\n07\nmarkers kept\n",
            b"wasmkeep-generation-floor 1\n+7\nmarkers kept\n",
            b"wasmkeep-generation-floor 1\n-7\nmarkers kept\n",
            b"wasmkeep-generation-floor 1\n7 \nmarkers kept\n",
            b"wasmkeep-generation-floor 1\n18446744073709551616\nmarkers kept\n",
            b"wasmkeep-generation-floor 1\r\n7\nmarkers kept\n",
            b"wasmkeep-generation-floor 2\n7\nmarkers kept\n",
            b" wasmkeep-generation-floor 1\n7\nmarkers kept\n",
        ] {
            assert_eq!(
                decode(Some(bad)),
                Err(Unreadable { len: bad.len() }),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn the_floor_only_goes_up() {
        let a = floor(5, Markers::None);
        let b = floor(9, Markers::Kept);
        assert_eq!(a.max(b), floor(9, Markers::None));
        assert_eq!(b.max(a), floor(9, Markers::None));
        assert_eq!(GenerationFloor::ABSENT.max(a), a);
    }

    #[test]
    fn a_marker_is_deleted_only_when_its_record_carries_it() {
        assert!(matches!(
            judge("k".into(), 5, Some(5)),
            Leftover::Carried { .. }
        ));
        assert!(matches!(
            judge("k".into(), 5, Some(9)),
            Leftover::Carried { .. }
        ));
        assert!(matches!(
            judge("k".into(), 5, Some(4)),
            Leftover::Folded { generation: 5, .. }
        ));
        assert!(matches!(
            judge("k".into(), 5, None),
            Leftover::Folded { .. }
        ));
        let plan = CleanPlan {
            floor_key: String::new(),
            floor: floor(6, Markers::Kept),
            leftovers: vec![judge("a".into(), 5, None), judge("b".into(), 9, Some(9))],
        };
        assert_eq!(plan.raise_to(), None, "the floor covers 5 already");
        assert_eq!(plan.counts(), (1, 1));
        let plan = CleanPlan {
            leftovers: vec![judge("a".into(), 8, None), judge("c".into(), 7, Some(1))],
            ..plan
        };
        assert_eq!(plan.raise_to(), Some(8));
    }

    #[cfg(feature = "object-store")]
    mod bucket {
        use std::sync::Arc;

        use object_store::memory::InMemory;

        use super::*;
        use crate::object_store::ObjectStoreBucket;

        struct At(u64);
        impl Clock for At {
            fn now_ms(&self) -> u64 {
                self.0
            }
        }

        fn config() -> LeaseConfig {
            LeaseConfig::new(10_000, 5_000, 2_000, 1_000).unwrap()
        }

        fn bucket() -> ObjectStoreBucket {
            ObjectStoreBucket::new(Arc::new(InMemory::new()), String::new())
        }

        fn root() -> StateRoot {
            StateRoot::new("", "t", "a")
        }

        async fn body(b: &ObjectStoreBucket, key: &str) -> Option<Vec<u8>> {
            b.get(key).await.unwrap().map(|(body, _)| body)
        }

        #[tokio::test]
        async fn raise_creates_replaces_upward_and_never_over_junk() {
            let b = bucket();
            let k = key(&root());
            assert_eq!(k, "state/t/a/generation-floor");
            let to = floor(4, Markers::Kept);
            assert_eq!(raise(&b, &k, to).await.unwrap(), to);
            assert_eq!(raise(&b, &k, floor(2, Markers::Kept)).await.unwrap(), to);
            assert_eq!(
                raise(&b, &k, floor(0, Markers::None)).await.unwrap(),
                floor(4, Markers::None)
            );
            assert_eq!(
                raise(&b, &k, floor(9, Markers::Kept)).await.unwrap(),
                floor(9, Markers::None),
                "none is never turned back to kept"
            );
            crate::store::Store::put(&b, &k, b"junk".to_vec())
                .await
                .unwrap();
            let err = raise(&b, &k, to).await.unwrap_err();
            assert!(err.to_string().contains(&k), "{err}");
            assert_eq!(body(&b, &k).await.unwrap(), b"junk");
        }

        /// Each row of section 2's table, by the requests it makes: before
        /// the floor, a marker is created as `0.9` does and no generation
        /// floor is read; at it, nothing is written, and the seed is the
        /// highest of the keys, the marker and the floor.
        #[tokio::test]
        async fn the_seed_by_path() {
            let b = bucket();
            let clock = At(1_000);
            let fk = key(&root());
            let lk = "state/t/a/leases/c/x";
            // Below the floor: the marker is created with the keys'.
            let s = seed(&b, &clock, &config(), lk, &fk, 3, false, async || Ok(false))
                .await
                .unwrap();
            assert_eq!(s.seed, 3);
            assert_eq!(body(&b, lk).await.unwrap(), encode_marker(3));
            // A junk floor is not read below the floor.
            crate::store::Store::put(&b, &fk, b"junk".to_vec())
                .await
                .unwrap();
            assert_eq!(
                seed(&b, &clock, &config(), lk, &fk, 1, false, async || Ok(false))
                    .await
                    .unwrap()
                    .seed,
                3
            );
            // At the floor, it is: unreadable fails the acquisition.
            assert_eq!(
                seed(&b, &clock, &config(), lk, &fk, 1, false, async || Ok(true)).await,
                Err(AcquireError::Corrupt)
            );
            crate::store::Store::delete(&b, &fk).await.unwrap();
            // A marker above the floor, a floor above the marker, both
            // below the keys.
            raise(&b, &fk, floor(2, Markers::Kept)).await.unwrap();
            let at = async || Ok(true);
            assert_eq!(
                seed(&b, &clock, &config(), lk, &fk, 1, false, at)
                    .await
                    .unwrap()
                    .seed,
                3
            );
            raise(&b, &fk, floor(8, Markers::Kept)).await.unwrap();
            assert_eq!(
                seed(&b, &clock, &config(), lk, &fk, 1, false, async || Ok(true))
                    .await
                    .unwrap()
                    .seed,
                8
            );
            assert_eq!(
                seed(&b, &clock, &config(), lk, &fk, 20, false, async || Ok(true))
                    .await
                    .unwrap()
                    .seed,
                20
            );
            // Nothing written at the floor: a new actor gets no marker.
            let other = "state/t/a/leases/c/y";
            let s = seed(&b, &clock, &config(), other, &fk, 0, false, async || {
                Ok(true)
            })
            .await
            .unwrap();
            assert_eq!((s.seed, s.markers_none), (8, false));
            assert_eq!(body(&b, other).await, None);
            raise(&b, &fk, floor(0, Markers::None)).await.unwrap();
            let s = seed(&b, &clock, &config(), other, &fk, 0, true, async || {
                Ok(true)
            })
            .await
            .unwrap();
            assert_eq!((s.seed, s.markers_none), (8, true));
        }

        /// The order of section 2, on the code: the marker is read before
        /// the upgrade floor. A cleanup that runs between them (it raises
        /// the floor, then deletes the marker) leaves a seed above the
        /// marker; below the floor, the marker read is used as is, and none
        /// is created from the keys.
        #[tokio::test]
        async fn the_marker_is_read_before_the_floors() {
            let b = bucket();
            let clock = At(1_000);
            let fk = key(&root());
            let lk = "state/t/a/leases/c/x";
            for at in [true, false] {
                crate::store::Store::put(&b, lk, encode_marker(6))
                    .await
                    .unwrap();
                crate::store::Store::delete(&b, &fk).await.unwrap();
                let cleanup = async || {
                    raise(&b, &fk, floor(6, Markers::Kept)).await.unwrap();
                    crate::store::Store::delete(&b, lk).await.unwrap();
                    Ok(at)
                };
                let s = seed(&b, &clock, &config(), lk, &fk, 3, false, cleanup)
                    .await
                    .unwrap();
                assert_eq!(s.seed, 6, "at the floor: {at}");
                assert_eq!(body(&b, lk).await, None, "no marker from the keys");
            }
        }

        /// A bucket whose deletes fail: the cleanup stops after its raise.
        struct NoDeletes(ObjectStoreBucket);
        impl Bucket for NoDeletes {
            async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
                self.0.get(key).await
            }
            async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
                self.0.create(key, body).await
            }
            async fn replace(
                &self,
                key: &str,
                body: Vec<u8>,
                expected: &Version,
            ) -> Result<Version, BucketError> {
                self.0.replace(key, body, expected).await
            }
        }
        impl crate::store::Store for NoDeletes {
            async fn list(
                &self,
                prefix: &str,
            ) -> Result<Vec<crate::store::ObjectMeta>, BucketError> {
                crate::store::Store::list(&self.0, prefix).await
            }
            async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
                self.0.list_dirs(prefix).await
            }
            async fn copy(&self, from: &str, to: &str) -> Result<(), BucketError> {
                self.0.copy(from, to).await
            }
            async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), BucketError> {
                crate::store::Store::put(&self.0, key, body).await
            }
            async fn delete(&self, _key: &str) -> Result<(), BucketError> {
                Err(BucketError::Unavailable("refused".into()))
            }
        }

        /// The cleanup raises before it deletes: interrupted at its first
        /// delete, the floor already covers the only carrier, and every
        /// marker is still there.
        #[tokio::test]
        async fn the_cleanup_raises_before_its_first_delete() {
            use crate::store::Store as _;
            let b = NoDeletes(bucket());
            let r = root();
            b.put("state/t/a/leases/c/only", encode_marker(9))
                .await
                .unwrap();
            let plan = plan_clean(&b, &r, &At(0), &config()).await.unwrap();
            assert!(clean(&b, &r, &plan).await.is_err());
            assert_eq!(read(&b, &key(&r)).await.unwrap().0, floor(9, Markers::Kept));
            assert_eq!(
                body(&b.0, "state/t/a/leases/c/only").await.unwrap(),
                encode_marker(9)
            );
        }

        /// A per-actor lease from before node leases: live refuses on both
        /// paths; not live is replaced by a marker below the floor and
        /// read and left at it.
        #[tokio::test]
        async fn an_old_lease_on_both_paths() {
            let b = bucket();
            let fk = key(&root());
            let lk = "state/t/a/leases/c/x";
            let old = b"wasmkeep-lease 1\nn1\n6\n2000\n".to_vec();
            crate::store::Store::put(&b, lk, old.clone()).await.unwrap();
            for at in [false, true] {
                assert!(matches!(
                    seed(&b, &At(2_000), &config(), lk, &fk, 1, false, async || Ok(
                        at
                    ))
                    .await,
                    Err(AcquireError::Held { .. })
                ));
            }
            let late = At(10_000);
            let s = seed(&b, &late, &config(), lk, &fk, 1, false, async || Ok(true))
                .await
                .unwrap();
            assert_eq!(s.seed, 6);
            assert_eq!(body(&b, lk).await.unwrap(), old, "read and left");
            let s = seed(&b, &late, &config(), lk, &fk, 1, false, async || Ok(false))
                .await
                .unwrap();
            assert_eq!(s.seed, 6);
            assert_eq!(body(&b, lk).await.unwrap(), encode_marker(6));
        }

        /// The cleanup's three cases, an interrupt after the floor's write
        /// (a rerun finishes), a live old lease (nothing deleted), and
        /// `markers none` once `leases/` is empty.
        #[tokio::test]
        async fn the_cleanup_raises_then_deletes() {
            use crate::store::Store as _;
            let b = bucket();
            let r = root();
            let clock = At(100_000);
            b.put("state/t/a/leases/c/carried", encode_marker(4))
                .await
                .unwrap();
            b.put(
                "state/t/a/owners/c/carried",
                b"wasmkeep-owner 1\n- -\n4\n".to_vec(),
            )
            .await
            .unwrap();
            b.put("state/t/a/leases/c/only", encode_marker(9))
                .await
                .unwrap();
            // A name the key escapes (`bg:lobby`), carried by its record.
            b.put("state/t/a/leases/c/bg%3Alobby", encode_marker(2))
                .await
                .unwrap();
            b.put(
                "state/t/a/owners/c/bg%3Alobby",
                b"wasmkeep-owner 1\n- -\n3\n".to_vec(),
            )
            .await
            .unwrap();
            b.put(
                "state/t/a/leases/c/old",
                b"wasmkeep-lease 1\nn1\n12\n2000\n".to_vec(),
            )
            .await
            .unwrap();
            let plan = plan_clean(&b, &r, &clock, &config()).await.unwrap();
            assert_eq!(plan.counts(), (2, 2));
            assert_eq!(plan.raise_to(), Some(12));
            // Interrupted after the floor's write: the markers are still
            // there and the floor covers them.
            raise(&b, &plan.floor_key, floor(12, Markers::Kept))
                .await
                .unwrap();
            let plan = plan_clean(&b, &r, &clock, &config()).await.unwrap();
            assert_eq!(plan.raise_to(), None);
            let done = clean(&b, &r, &plan).await.unwrap();
            assert_eq!(done.deleted, 4);
            assert_eq!(done.left, 0);
            assert_eq!(done.floor, floor(12, Markers::None));
            assert!(b.list("state/t/a/leases/").await.unwrap().is_empty());
            // A rerun changes nothing.
            let plan = plan_clean(&b, &r, &clock, &config()).await.unwrap();
            assert!(plan.leftovers.is_empty());
            assert_eq!(clean(&b, &r, &plan).await.unwrap().floor, done.floor);
            // A live old lease stops it before anything is written.
            b.put(
                "state/t/a/leases/c/live",
                b"wasmkeep-lease 1\nn2\n30\n100000\n".to_vec(),
            )
            .await
            .unwrap();
            let err = plan_clean(&b, &r, &clock, &config()).await.unwrap_err();
            assert!(
                matches!(err, CleanError::Live { ref node, .. } if node == "n2"),
                "{err}"
            );
            // Junk under leases/ stops it too.
            b.delete("state/t/a/leases/c/live").await.unwrap();
            b.put("state/t/a/leases/c/junk", b"x".to_vec())
                .await
                .unwrap();
            assert!(matches!(
                plan_clean(&b, &r, &clock, &config()).await,
                Err(CleanError::Unknown { .. })
            ));
        }
    }
}
