//! The recorded head of an ownership record, `wasmkeep-owner 2`
//! ([ADR 0035], section 1): the acknowledged database position a releasing
//! owner froze, the ownership generation in which that requirement was
//! established (`set-at`), whether it is complete, and the idle origin
//! that idle expiry keeps across refused cycles.
//!
//! This module is the codec and its validation only. The record itself
//! ([`crate::node_lease::OwnerRecord`]) carries a [`RecordedHead`] when it
//! was stored in version 2; a version 1 record carries none, which is no
//! evidence of anything. Until an app enables the format (section 5), this
//! binary reads both versions and writes only version 1, and refuses to
//! write over a version 2 record ([`crate::node_lease::AcquireError::FormatMismatch`]).
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

/// The header of a version 2 ownership record.
pub const OWNER_HEADER_V2: &str = "wasmkeep-owner 2";

/// An acknowledged database position, or its absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// No recorded position: no evidence (`set-at 0`, always a lower bound).
    Unknown,
    /// The explicit pre-history position (never an empty listing).
    Empty,
    /// A position in the actor's history.
    Known {
        /// The generation of the history object at that position.
        generation: u64,
        /// Its transaction id (nonzero).
        txid: u64,
        /// The LTX database checksum after it.
        checksum: u64,
    },
}

/// Whether a recorded head covers every acknowledgement of its holder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// A released record whose releasing owner had stopped acknowledging.
    Complete,
    /// A held record preserving a complete predecessor requirement, whose
    /// hold has not been allowed to upload new history.
    CompleteInherited,
    /// The holder may have acknowledged beyond this head.
    LowerBound,
}

impl Completeness {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::CompleteInherited => "complete-inherited",
            Self::LowerBound => "lower-bound",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "complete" => Some(Self::Complete),
            "complete-inherited" => Some(Self::CompleteInherited),
            "lower-bound" => Some(Self::LowerBound),
            _ => None,
        }
    }
}

/// The version 2 fields of an ownership record, after its holder and
/// generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedHead {
    /// The required position.
    pub head: Position,
    /// The ownership generation in which this requirement was established
    /// (not the generation of its history object); acquisition copies it.
    pub set_at: u64,
    /// Whether it covers every acknowledgement.
    pub completeness: Completeness,
    /// An idle origin carried from an earlier release (Unix ms); `None`:
    /// the object's own last-modified time.
    pub idle_since_ms: Option<u64>,
}

impl RecordedHead {
    /// No evidence: what a record converted from version 1 starts with.
    pub const UNKNOWN: Self = Self {
        head: Position::Unknown,
        set_at: 0,
        completeness: Completeness::LowerBound,
        idle_since_ms: None,
    };

    /// Whether these fields are a valid combination for a record at
    /// `generation`, released (`held == false`) or held.
    pub fn valid_for(&self, generation: u64, held: bool) -> bool {
        let position_ok = match self.head {
            Position::Unknown => self.set_at == 0 && self.completeness == Completeness::LowerBound,
            Position::Empty => self.set_at > 0 && self.set_at <= generation,
            Position::Known {
                generation: g,
                txid,
                ..
            } => txid != 0 && g > 0 && g <= self.set_at && self.set_at <= generation,
        };
        let phase_ok = match self.completeness {
            Completeness::Complete => !held,
            Completeness::CompleteInherited => held,
            Completeness::LowerBound => true,
        };
        position_ok && phase_ok
    }

    /// The lines after the generation, each ending in a newline.
    pub(crate) fn encode_lines(&self, out: &mut String) {
        use std::fmt::Write as _;
        match self.head {
            Position::Unknown => out.push_str("unknown\n"),
            Position::Empty => out.push_str("empty\n"),
            Position::Known {
                generation,
                txid,
                checksum,
            } => {
                let _ = writeln!(out, "head {generation} {txid} {checksum:016x}");
            }
        }
        let _ = writeln!(out, "set-at {}", self.set_at);
        out.push_str(self.completeness.as_str());
        out.push('\n');
        match self.idle_since_ms {
            None => out.push_str("idle-since -\n"),
            Some(ms) => {
                let _ = writeln!(out, "idle-since {ms}");
            }
        }
    }

    /// Parses the four lines after the generation. Strictness (no extra or
    /// missing lines, canonical numbers, lowercase checksum) is checked by
    /// the caller comparing the re-encoding with the stored bytes.
    pub(crate) fn decode_lines<'a>(mut lines: impl Iterator<Item = &'a str>) -> Option<Self> {
        let head = match lines.next()? {
            "unknown" => Position::Unknown,
            "empty" => Position::Empty,
            line => {
                let rest = line.strip_prefix("head ")?;
                let mut parts = rest.split(' ');
                let generation = number(parts.next()?)?;
                let txid = number(parts.next()?)?;
                let checksum = parts.next()?;
                if parts.next().is_some()
                    || checksum.len() != 16
                    || !checksum
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return None;
                }
                Position::Known {
                    generation,
                    txid,
                    checksum: u64::from_str_radix(checksum, 16).ok()?,
                }
            }
        };
        let set_at = number(lines.next()?.strip_prefix("set-at ")?)?;
        let completeness = Completeness::parse(lines.next()?)?;
        let idle_since_ms = match lines.next()?.strip_prefix("idle-since ")? {
            "-" => None,
            ms => Some(number(ms)?),
        };
        if lines.next().is_some() {
            return None;
        }
        Some(Self {
            head,
            set_at,
            completeness,
            idle_since_ms,
        })
    }
}

/// A canonical decimal: digits only, no sign, no leading zero, no overflow.
pub(crate) fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return None;
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_lease::{OwnerObject, OwnerRecord};

    /// The ADR's example, byte for byte.
    const EXAMPLE: &[u8] = b"wasmkeep-owner 2\n- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n";

    fn known(generation: u64, txid: u64, checksum: u64) -> Position {
        Position::Known {
            generation,
            txid,
            checksum,
        }
    }

    #[test]
    fn the_adr_example_decodes_and_encodes_byte_for_byte() {
        let r = OwnerRecord::decode(EXAMPLE).unwrap();
        assert_eq!(r.holder, None);
        assert_eq!(r.generation, 42);
        assert_eq!(
            r.recorded,
            Some(RecordedHead {
                head: known(41, 187, 0x09ab_cdef_0123_4567),
                set_at: 42,
                completeness: Completeness::Complete,
                idle_since_ms: None,
            })
        );
        assert_eq!(r.encode(), EXAMPLE);
    }

    #[test]
    fn every_valid_form_round_trips() {
        let held = Some(("n1".to_owned(), "i1".to_owned()));
        let cases = [
            (None, 3, RecordedHead::UNKNOWN),
            (held.clone(), 3, RecordedHead::UNKNOWN),
            (
                None,
                5,
                RecordedHead {
                    head: Position::Empty,
                    set_at: 5,
                    completeness: Completeness::Complete,
                    idle_since_ms: Some(1_700_000_000_000),
                },
            ),
            (
                held.clone(),
                6,
                RecordedHead {
                    head: Position::Empty,
                    set_at: 5,
                    completeness: Completeness::CompleteInherited,
                    idle_since_ms: Some(0),
                },
            ),
            (
                held.clone(),
                9,
                RecordedHead {
                    head: known(4, 1, 0),
                    set_at: 7,
                    completeness: Completeness::LowerBound,
                    idle_since_ms: None,
                },
            ),
            (
                None,
                u64::MAX,
                RecordedHead {
                    head: known(u64::MAX, u64::MAX, u64::MAX),
                    set_at: u64::MAX,
                    completeness: Completeness::LowerBound,
                    idle_since_ms: Some(u64::MAX),
                },
            ),
        ];
        for (holder, generation, recorded) in cases {
            let r = OwnerRecord {
                holder,
                generation,
                recorded: Some(recorded),
            };
            let body = r.encode();
            assert!(body.starts_with(b"wasmkeep-owner 2\n"));
            assert_eq!(OwnerRecord::decode(&body), Some(r), "{body:?}");
        }
    }

    /// A record without version 2 fields encodes exactly as today's
    /// version 1 (the golden trace depends on it).
    #[test]
    fn a_version_1_record_is_unchanged() {
        let r = OwnerRecord {
            holder: Some(("a".into(), "i1".into())),
            generation: 3,
            recorded: None,
        };
        assert_eq!(r.encode(), b"wasmkeep-owner 1\na i1\n3\n");
        let free = OwnerRecord {
            holder: None,
            generation: 9,
            recorded: None,
        };
        assert_eq!(free.encode(), b"wasmkeep-owner 1\n- -\n9\n");
        assert_eq!(OwnerRecord::decode(&free.encode()), Some(free));
    }

    #[test]
    fn malformed_or_invalid_version_2_records_are_rejected() {
        let h = "wasmkeep-owner 2\n";
        let bad: &[&str] = &[
            // Missing lines, extra lines, no final newline.
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\nx\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\nset-at 42\ncomplete\nidle-since -\n",
            // Checksum: uppercase, short, long, not hex.
            "- -\n42\nhead 41 187 09ABCDEF01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 9abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 009abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 g9abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            // Extra field, non-canonical numbers, overflow, signs.
            "- -\n42\nhead 41 187 09abcdef01234567 1\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 041 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at +42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since 18446744073709551616\n",
            "- -\n042\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -1\n",
            "- -\n42\nhead  41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete \nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\r\nset-at 42\ncomplete\nidle-since -\n",
            // Ordering: head generation above set-at, set-at above the
            // ownership generation, zero head generation, zero txid.
            "- -\n42\nhead 43 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 43\ncomplete\nidle-since -\n",
            "- -\n42\nhead 0 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 0 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            // Empty: zero stamp, or above the generation.
            "- -\n42\nempty\nset-at 0\ncomplete\nidle-since -\n",
            "- -\n42\nempty\nset-at 43\ncomplete\nidle-since -\n",
            // Unknown: a stamp, or anything but a lower bound.
            "- -\n42\nunknown\nset-at 1\nlower-bound\nidle-since -\n",
            "- -\n42\nunknown\nset-at 0\ncomplete\nidle-since -\n",
            // Phases: complete only released, complete-inherited only held.
            "n i\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete-inherited\nidle-since -\n",
            // Unknown words, missing idle line label, a bad holder line.
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\nfinished\nidle-since -\n",
            "- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\n-\n",
            "-\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "n i x\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n",
            "n -\n42\nunknown\nset-at 0\nlower-bound\nidle-since -\n",
            "- i\n42\nunknown\nset-at 0\nlower-bound\nidle-since -\n",
            "",
        ];
        for tail in bad {
            let body = format!("{h}{tail}");
            assert_eq!(OwnerRecord::decode(body.as_bytes()), None, "{body:?}");
        }
        // Unknown numeric versions fail closed.
        for header in ["wasmkeep-owner 3", "wasmkeep-owner 0", "wasmkeep-owner 02"] {
            let body = format!("{header}\n- -\n4\n");
            assert_eq!(OwnerRecord::decode(body.as_bytes()), None);
            assert_eq!(OwnerObject::decode(body.as_bytes()), None);
        }
    }

    /// Today's version 1 decoder, frozen as shipped: it reads nothing of a
    /// version 2 record, so an old server treats one as corrupt
    /// (acquisition, routing and expiry alike) rather than as free.
    fn old_decode(body: &[u8]) -> Option<(Option<(String, String)>, u64)> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        if lines.next()? != "wasmkeep-owner 1" {
            return None;
        }
        let (id, instance) = lines.next()?.split_once(' ')?;
        let generation = lines.next()?.parse().ok()?;
        Some((
            (id != "-").then(|| (id.to_owned(), instance.to_owned())),
            generation,
        ))
    }

    #[test]
    fn the_old_decoder_rejects_version_2() {
        assert_eq!(old_decode(EXAMPLE), None);
        let held = OwnerRecord {
            holder: Some(("n".into(), "i".into())),
            generation: 3,
            recorded: Some(RecordedHead::UNKNOWN),
        };
        assert_eq!(old_decode(&held.encode()), None);
        // And reads version 1 as the new decoder does.
        let v1 = OwnerRecord {
            holder: Some(("n".into(), "i".into())),
            generation: 3,
            recorded: None,
        };
        assert_eq!(
            old_decode(&v1.encode()),
            Some((v1.holder.clone(), v1.generation))
        );
    }

    /// The migration tombstone is not an unknown ownership version: it is
    /// dispatched before version 1 and 2, keeps its target and its
    /// `unowned` form, and is never a record.
    #[test]
    fn tombstones_are_dispatched_before_records() {
        let owned = crate::migrate::tombstone("p2/");
        let unowned = crate::migrate::tombstone_unowned("p2/");
        assert_eq!(
            OwnerObject::decode(&owned),
            Some(OwnerObject::Tombstone {
                to: "p2/".into(),
                unowned: false
            })
        );
        assert_eq!(
            OwnerObject::decode(&unowned),
            Some(OwnerObject::Tombstone {
                to: "p2/".into(),
                unowned: true
            })
        );
        assert_eq!(OwnerRecord::decode(&owned), None);
        assert_eq!(OwnerRecord::decode(&unowned), None);
        // A tombstone without a target is nothing this version reads.
        assert_eq!(OwnerObject::decode(b"wasmkeep-owner moved\n\n"), None);
        for body in [EXAMPLE.to_vec(), b"wasmkeep-owner 1\n- -\n4\n".to_vec()] {
            assert_eq!(
                OwnerObject::decode(&body),
                Some(OwnerObject::Record(OwnerRecord::decode(&body).unwrap()))
            );
        }
    }
}
