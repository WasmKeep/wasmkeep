//! An app's durable ownership-format setting ([ADR 0035], section 5):
//! `formats/<tenant>/<app>/recorded-head` under the bucket prefix, outside
//! both `state/` and `apps/`, so state replacement, app deletion,
//! `--recreate`, deploy restoration and component rollback never touch it.
//!
//! Absent: the app writes ownership records in version 1. The exact body
//! [`ENABLED_BODY`]: it has enabled the recorded-head format
//! (`wasmkeep-owner 2`), one way. Anything else refuses service.
//!
//! Servers and the CLI enforce it: an enabled app writes version 2
//! records and is served only by nodes that announce `recorded-head`;
//! `wasmkeep app format --enable` writes the setting.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

/// The setting's body once enabled, exactly.
pub const ENABLED_BODY: &[u8] = b"wasmkeep-recorded-head 1\n";

/// The setting's key under the bucket prefix (`prefix` ends in `/` or is
/// empty).
pub fn key(prefix: &str, tenant: &str, app: &str) -> String {
    format!("{prefix}formats/{tenant}/{app}/recorded-head")
}

/// The setting's key for the app `root` names, under its bucket prefix:
/// what servers and the CLI both read.
pub fn key_of(root: &crate::state_root::StateRoot) -> String {
    key(root.prefix(), root.tenant(), root.app())
}

/// An app's ownership format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppFormat {
    /// No setting: version 1 records.
    Version1,
    /// Enabled: version 2 records, with the recorded head.
    RecordedHead,
}

/// The setting holds something this version cannot read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the app's ownership-format setting is unreadable ({len} bytes); refusing service")]
pub struct Unknown {
    /// Its length.
    pub len: usize,
}

/// The format a read of the setting gives (`None`: absent).
pub fn decode(body: Option<&[u8]>) -> Result<AppFormat, Unknown> {
    match body {
        None => Ok(AppFormat::Version1),
        Some(ENABLED_BODY) => Ok(AppFormat::RecordedHead),
        Some(other) => Err(Unknown { len: other.len() }),
    }
}

/// An app's upgrade floor ([ADR 0020], amendment on lease reads):
/// `formats/<tenant>/<app>/floor` under the bucket prefix, beside the
/// recorded-head setting. Its body is exactly `wasmkeep-floor 1\n` then
/// the lowest minor version allowed to serve the app, as
/// `<major>.<minor>\n` (`0.9`), in decimal without leading zeros.
///
/// Read by every node at start since 0.8.1, before its first lease write
/// and again right after it: a floor above the binary's own minor version
/// stops the start, a missing record starts as before, and
/// a body this version cannot read refuses to start (as an unreadable
/// format setting refuses service). Nothing writes it before 0.9.0. From
/// then on [`floor::raise`] is its only writer ([ADR 0043], section 3):
/// `wasmkeep cluster upgrade finalize` calls it once every live node runs
/// the new minor version, so a node of an older version that was offline
/// meanwhile cannot come back, and so does the first writer into an app
/// that holds nothing ([`crate::upgrade::born_finalized`]).
///
/// [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
/// [ADR 0043]: ../../../docs/adr/0043-rolling-upgrades.md
pub mod floor {
    /// The record's first line.
    pub const HEADER: &str = "wasmkeep-floor 1";

    /// A minor version, `major.minor`, ordered.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Minor {
        /// The major version.
        pub major: u32,
        /// The minor version.
        pub minor: u32,
    }

    impl std::fmt::Display for Minor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}.{}", self.major, self.minor)
        }
    }

    impl Minor {
        /// The minor version of a semantic version (`0.8.1`, `0.9.0-rc.1`):
        /// its first two numbers.
        pub fn of_version(version: &str) -> Option<Self> {
            let mut parts = version.split(['.', '-', '+']);
            Some(Self {
                major: number(parts.next()?)?,
                minor: number(parts.next()?)?,
            })
        }

        /// The minor version a node's heartbeat names (ADR 0043, section
        /// 5): `<major>.<minor>.<patch>`, three decimal numbers, then
        /// nothing, or `+` or `-` and anything. Stricter than
        /// [`Self::of_version`], and equal to it on every line this one
        /// accepts.
        pub fn of_node_version(line: &str) -> Option<Self> {
            let core = line.split(['-', '+']).next()?;
            let mut parts = core.split('.');
            let found = Self {
                major: number(parts.next()?)?,
                minor: number(parts.next()?)?,
            };
            number(parts.next()?)?;
            parts.next().is_none().then_some(found)
        }

        /// The minor version before this one in the same major version, if
        /// any (`0.9` gives `0.8`; `1.0` gives none).
        pub fn previous(self) -> Option<Self> {
            Some(Self {
                major: self.major,
                minor: self.minor.checked_sub(1)?,
            })
        }

        /// The next minor version in the same major version.
        pub fn next(self) -> Self {
            Self {
                major: self.major,
                minor: self.minor.saturating_add(1),
            }
        }
    }

    /// What a missing record reads as: `0.8`, the last version without a
    /// floor.
    pub const ABSENT: Minor = Minor { major: 0, minor: 8 };

    /// The floor in force: the record's, or [`ABSENT`] without one.
    pub fn in_force(floor: Option<Minor>) -> Minor {
        floor.unwrap_or(ABSENT)
    }

    /// The record's key relative to the bucket prefix: what a
    /// [`crate::lease::Bucket`] that already carries the prefix is given.
    pub fn relative_key(tenant: &str, app: &str) -> String {
        key("", tenant, app)
    }

    /// What [`raise`] did.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Raised {
        /// There was no record: created at the floor asked.
        Created,
        /// Replaced the record, which named this lower floor.
        Replaced(Minor),
        /// The record already names this floor, at or above the one asked:
        /// left as it is.
        Kept(Minor),
    }

    /// Why [`raise`] wrote nothing.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum RaiseError {
        /// The record holds something this version cannot read.
        #[error("the app's upgrade floor `{key}` is unreadable ({len} bytes); nothing written")]
        Unreadable {
            /// The record's key.
            key: String,
            /// Its length.
            len: usize,
        },
        /// The bucket did not answer, or other writers kept winning.
        #[error("writing the app's upgrade floor: {0}")]
        Bucket(String),
    }

    /// Raises the floor at `key` to `to`: the record's only writer (ADR
    /// 0043, section 3). By conditional writes only (create when absent,
    /// replace only the version read) and only upward: a record at or
    /// above `to` is kept, and nothing lowers or deletes one. A lost race
    /// is read again.
    ///
    /// # Errors
    /// The record is unreadable (fail closed), or the bucket did not
    /// answer.
    pub async fn raise<B: crate::lease::Bucket>(
        bucket: &B,
        key: &str,
        to: Minor,
    ) -> Result<Raised, RaiseError> {
        use crate::lease::BucketError;
        let failed = |e: BucketError| RaiseError::Bucket(e.to_string());
        // Every lost race means another writer landed: a few are plenty.
        for _ in 0..8 {
            let read = bucket.get(key).await.map_err(failed)?;
            let found = decode(read.as_ref().map(|(body, _)| body.as_slice())).map_err(|e| {
                RaiseError::Unreadable {
                    key: key.to_owned(),
                    len: e.len,
                }
            })?;
            let written = match (found, &read) {
                (Some(floor), _) if floor >= to => return Ok(Raised::Kept(floor)),
                (Some(floor), Some((_, version))) => bucket
                    .replace(key, encode(to), version)
                    .await
                    .map(|_| Raised::Replaced(floor)),
                _ => bucket
                    .create(key, encode(to))
                    .await
                    .map(|_| Raised::Created),
            };
            match written {
                Ok(raised) => return Ok(raised),
                Err(BucketError::Conflict) => {}
                Err(e) => return Err(failed(e)),
            }
        }
        Err(RaiseError::Bucket(
            "other writers changed it on every try".to_owned(),
        ))
    }

    /// A decimal number without leading zeros or a sign.
    fn number(s: &str) -> Option<u32> {
        let ok = !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
            && (s == "0" || !s.starts_with('0'));
        if ok { s.parse().ok() } else { None }
    }

    /// The record's key under the bucket prefix (`prefix` ends in `/` or
    /// is empty).
    pub fn key(prefix: &str, tenant: &str, app: &str) -> String {
        format!("{prefix}formats/{tenant}/{app}/floor")
    }

    /// The record's key for the app `root` names, under its bucket prefix.
    pub fn key_of(root: &crate::state_root::StateRoot) -> String {
        key(root.prefix(), root.tenant(), root.app())
    }

    /// The record's body for `floor`.
    pub fn encode(floor: Minor) -> Vec<u8> {
        format!("{HEADER}\n{floor}\n").into_bytes()
    }

    /// The record holds something this version cannot read.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    #[error("the app's upgrade floor record is unreadable ({len} bytes); refusing to start")]
    pub struct Unknown {
        /// Its length.
        pub len: usize,
    }

    /// The floor a read of the record gives (`None`: absent, no floor).
    /// Only the exact form [`encode`] writes is read.
    pub fn decode(body: Option<&[u8]>) -> Result<Option<Minor>, Unknown> {
        let Some(body) = body else {
            return Ok(None);
        };
        let unknown = Unknown { len: body.len() };
        let floor = std::str::from_utf8(body)
            .ok()
            .and_then(|t| {
                t.strip_prefix(HEADER)?
                    .strip_prefix('\n')?
                    .strip_suffix('\n')
            })
            .and_then(|v| {
                let (major, minor) = v.split_once('.')?;
                Some(Minor {
                    major: number(major)?,
                    minor: number(minor)?,
                })
            })
            .ok_or_else(|| unknown.clone())?;
        if encode(floor) != body {
            return Err(unknown);
        }
        Ok(Some(floor))
    }

    /// Whether a binary of minor version `this` may start under `floor`:
    /// `Err` with the reason, naming both versions, if the floor is above.
    pub fn allows(floor: Option<Minor>, this: Minor) -> Result<(), String> {
        match floor {
            Some(floor) if floor > this => Err(format!(
                "this app's upgrade was finalized at {floor}: this binary is {this}, older than \
                 the floor, and must not serve it; install {floor} or later"
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_floor_is_absent_exact_or_refused() {
        use floor::{Minor, decode, encode};
        let m = |major, minor| Minor { major, minor };
        assert_eq!(decode(None), Ok(None));
        assert_eq!(encode(m(0, 9)), b"wasmkeep-floor 1\n0.9\n");
        for floor in [m(0, 9), m(0, 10), m(1, 0), m(12, 345)] {
            assert_eq!(decode(Some(&encode(floor))), Ok(Some(floor)));
        }
        for bad in [
            &b""[..],
            b"wasmkeep-floor 1\n",
            b"wasmkeep-floor 1\n0.9",
            b"wasmkeep-floor 1\n0.9\n\n",
            b"wasmkeep-floor 1\r\n0.9\n",
            b"wasmkeep-floor 2\n0.9\n",
            b"wasmkeep-floor 1\n9\n",
            b"wasmkeep-floor 1\n0.9.0\n",
            b"wasmkeep-floor 1\n0.09\n",
            b"wasmkeep-floor 1\n00.9\n",
            b"wasmkeep-floor 1\n+0.9\n",
            b"wasmkeep-floor 1\n0.9 \n",
            b"wasmkeep-floor 1\n0.99999999999\n",
            b" wasmkeep-floor 1\n0.9\n",
        ] {
            assert_eq!(
                decode(Some(bad)),
                Err(floor::Unknown { len: bad.len() }),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn a_floor_above_the_binary_stops_it() {
        use floor::{Minor, allows};
        let m = |major, minor| Minor { major, minor };
        assert_eq!(Minor::of_version("0.8.1"), Some(m(0, 8)));
        assert_eq!(Minor::of_version("0.10.0-rc.1"), Some(m(0, 10)));
        assert_eq!(Minor::of_version("1.2.3+build"), Some(m(1, 2)));
        assert_eq!(Minor::of_version("x"), None);
        assert!(allows(None, m(0, 8)).is_ok());
        assert!(allows(Some(m(0, 8)), m(0, 8)).is_ok());
        assert!(allows(Some(m(0, 7)), m(0, 8)).is_ok());
        let why = allows(Some(m(0, 9)), m(0, 8)).unwrap_err();
        assert!(why.contains("0.9") && why.contains("0.8"), "{why}");
        assert!(
            allows(Some(m(0, 10)), m(0, 9)).is_err(),
            "ordered as numbers"
        );
        assert!(allows(Some(m(1, 0)), m(0, 12)).is_err());
    }

    /// What `0.8.1` reads, byte for byte: the body its decoder accepts for
    /// a floor of 0.9. A change to the encoder fails this.
    const FLOOR_0_9_AS_0_8_1_READS_IT: &[u8] = b"wasmkeep-floor 1\n0.9\n";

    /// The floor's writer (ADR 0043, section 3): created when absent,
    /// replaced only upward, never lowered, and nothing written over a
    /// body it cannot read.
    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn the_floor_is_only_raised() {
        use crate::lease::Bucket as _;
        use floor::{Minor, RaiseError, Raised, raise};
        let m = |major, minor| Minor { major, minor };
        let bucket = crate::object_store::ObjectStoreBucket::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "wk",
        );
        let key = floor::relative_key("acme", "shop");
        assert_eq!(key, "formats/acme/shop/floor");
        let body = async || bucket.get(&key).await.unwrap().map(|(body, _)| body);
        assert_eq!(raise(&bucket, &key, m(0, 9)).await, Ok(Raised::Created));
        assert_eq!(body().await.as_deref(), Some(FLOOR_0_9_AS_0_8_1_READS_IT));
        // Idempotent, and never downward.
        assert_eq!(
            raise(&bucket, &key, m(0, 9)).await,
            Ok(Raised::Kept(m(0, 9)))
        );
        assert_eq!(
            raise(&bucket, &key, m(0, 8)).await,
            Ok(Raised::Kept(m(0, 9)))
        );
        assert_eq!(body().await.as_deref(), Some(FLOOR_0_9_AS_0_8_1_READS_IT));
        assert_eq!(
            raise(&bucket, &key, m(0, 10)).await,
            Ok(Raised::Replaced(m(0, 9)))
        );
        assert_eq!(body().await, Some(floor::encode(m(0, 10))));
        // Unreadable: fail closed, the body left as it is.
        let (_, version) = bucket.get(&key).await.unwrap().unwrap();
        bucket
            .replace(&key, b"wasmkeep-floor 2\n0.9\n".to_vec(), &version)
            .await
            .unwrap();
        assert_eq!(
            raise(&bucket, &key, m(0, 11)).await,
            Err(RaiseError::Unreadable {
                key: key.clone(),
                len: 21
            })
        );
        assert_eq!(
            body().await.as_deref(),
            Some(&b"wasmkeep-floor 2\n0.9\n"[..])
        );
    }

    /// A lost race is read again: a floor another writer created first is
    /// kept if it is high enough, and raised over if not.
    #[tokio::test]
    async fn a_lost_race_for_the_floor_is_read_again() {
        use crate::lease::{Bucket, BucketError, Version};
        use floor::{Minor, Raised, raise};
        use std::sync::Mutex;
        /// One object; the first read answers "absent" although `rival`
        /// is already there, as a read just before the rival's write.
        #[derive(Debug)]
        struct Racy {
            body: Mutex<(Vec<u8>, u64)>,
            stale_reads: Mutex<u32>,
        }
        impl Bucket for Racy {
            async fn get(&self, _: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
                let mut stale = self.stale_reads.lock().unwrap();
                if *stale > 0 {
                    *stale -= 1;
                    return Ok(None);
                }
                let (body, n) = self.body.lock().unwrap().clone();
                Ok(Some((body, Version(n.to_string()))))
            }
            async fn create(&self, _: &str, _: Vec<u8>) -> Result<Version, BucketError> {
                Err(BucketError::Conflict)
            }
            async fn replace(
                &self,
                _: &str,
                body: Vec<u8>,
                expected: &Version,
            ) -> Result<Version, BucketError> {
                let mut held = self.body.lock().unwrap();
                if expected.0 != held.1.to_string() {
                    return Err(BucketError::Conflict);
                }
                *held = (body, held.1 + 1);
                Ok(Version(held.1.to_string()))
            }
        }
        let m = |major, minor| Minor { major, minor };
        for (rival, want) in [
            (m(0, 9), Raised::Kept(m(0, 9))),
            (m(0, 8), Raised::Replaced(m(0, 8))),
        ] {
            let bucket = Racy {
                body: Mutex::new((floor::encode(rival), 1)),
                stale_reads: Mutex::new(1),
            };
            assert_eq!(raise(&bucket, "k", m(0, 9)).await, Ok(want));
            assert_eq!(bucket.body.lock().unwrap().0, floor::encode(m(0, 9)));
        }
    }

    /// A node's version line (ADR 0043, section 5): three numbers, then
    /// build or pre-release text. It agrees with the parse `0.8.1` applies
    /// to its own version wherever it accepts.
    #[test]
    fn a_nodes_version_line_is_three_numbers() {
        use floor::Minor;
        let m = |major, minor| Minor { major, minor };
        for (line, want) in [
            ("0.8.1", m(0, 8)),
            ("0.9.0+0a1b2c3d4e5f", m(0, 9)),
            ("0.10.0-rc.1", m(0, 10)),
            ("1.2.3+build.7", m(1, 2)),
        ] {
            assert_eq!(Minor::of_node_version(line), Some(want), "{line}");
            assert_eq!(Minor::of_version(line), Some(want), "{line}");
        }
        for line in [
            "", "1", "0.9", "0.9.x", "0.09.1", "0.9.1.2", "v0.9.1", "0.9.+a",
        ] {
            assert_eq!(Minor::of_node_version(line), None, "{line:?}");
        }
        assert_eq!(m(0, 9).previous(), Some(m(0, 8)));
        assert_eq!(m(1, 0).previous(), None);
        assert_eq!(m(0, 9).next(), m(0, 10));
        assert_eq!(floor::in_force(None), m(0, 8));
        assert_eq!(floor::in_force(Some(m(0, 9))), m(0, 9));
    }

    #[test]
    fn the_floor_is_beside_the_format_setting() {
        let root = crate::state_root::StateRoot::new("wk", "acme", "shop");
        assert_eq!(floor::key_of(&root), "wk/formats/acme/shop/floor");
        assert_eq!(floor::key("", "acme", "shop"), "formats/acme/shop/floor");
        let k = floor::key("p/", "acme", "shop");
        let root = crate::state_root::StateRoot::new("p/", "acme", "shop");
        for family in crate::state_root::Family::ALL {
            assert!(!k.starts_with(&root.key(family, "")), "{family:?}");
        }
    }

    #[test]
    fn the_setting_is_absent_exact_or_refused() {
        assert_eq!(decode(None), Ok(AppFormat::Version1));
        assert_eq!(decode(Some(ENABLED_BODY)), Ok(AppFormat::RecordedHead));
        for bad in [
            &b""[..],
            b"wasmkeep-recorded-head 1",
            b"wasmkeep-recorded-head 1\n\n",
            b"wasmkeep-recorded-head 2\n",
            b"wasmkeep-recorded-head 1\r\n",
            b" wasmkeep-recorded-head 1\n",
        ] {
            assert_eq!(
                decode(Some(bad)),
                Err(Unknown { len: bad.len() }),
                "{bad:?}"
            );
        }
    }

    /// A prefix given with or without its slash (the CLI trims it, a
    /// server's configuration may not) names the same key.
    #[test]
    fn a_prefix_names_one_key_however_it_is_written() {
        use crate::state_root::StateRoot;
        let want = "wk/formats/acme/shop/recorded-head";
        for prefix in ["wk", "wk/"] {
            assert_eq!(key_of(&StateRoot::new(prefix, "acme", "shop")), want);
        }
        assert_eq!(
            key_of(&StateRoot::new("", "acme", "shop")),
            "formats/acme/shop/recorded-head"
        );
    }

    #[test]
    fn the_key_is_outside_state_and_apps() {
        let k = key("p/", "acme", "shop");
        assert_eq!(k, "p/formats/acme/shop/recorded-head");
        let root = crate::state_root::StateRoot::new("p/", "acme", "shop");
        for family in crate::state_root::Family::ALL {
            assert!(!k.starts_with(&root.key(family, "")), "{family:?}");
        }
        assert!(!k.starts_with("p/apps/") && !k.starts_with("p/state/"));
    }
}
