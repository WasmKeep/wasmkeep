//! The hold cell as the home of a hold's acknowledged head ([ADR 0035],
//! section 2): acknowledgement and ending take the same short synchronous
//! lock, so an ending hold freezes every acknowledgement that won the race
//! and refuses every later one. No await belongs under it.
//!
//! The server keeps each live hold in an `Arc<Mutex<Option<Hold>>>`; the
//! simulations drive these same functions. A hold's head starts
//! unverified ([`HoldHead::acknowledged`] is `None`) and becomes known
//! once its restore is verified ([`verify`]); from then on each durable
//! upload advances it ([`acknowledge`] with a position), while an upload
//! that does not move the database position (a periodic snapshot, the
//! snapshot after a reset) only checks validity ([`acknowledge`] with
//! none). Ending ([`end`]) takes the hold, head included, out of the
//! cell: what a release, a park or a pending release then carries.
//!
//! This slice changes no stored byte: the head travels with the hold but
//! is not yet written to the ownership record.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

use std::sync::Mutex;

use crate::node_lease::Hold;
use crate::recorded_head::Position;

/// What a hold knows of its database position.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HoldHead {
    /// The verified, acknowledged position: `None` until the restore was
    /// verified, then the last position an acknowledged upload reached.
    pub acknowledged: Option<Position>,
    /// The requirement this hold inherited at acquisition (ADR 0035,
    /// section 2: from this process's local evidence, and later the
    /// record's): what its restore must reach, and what it freezes again,
    /// unchanged, if it ends unverified.
    pub inherited: Option<crate::local_heads::Requirement>,
    /// Acquired in the recorded-head format (`wasmkeep-owner 2`): its
    /// release writes its frozen head, and its restore must reach what it
    /// inherited (ADR 0035, sections 1 and 3).
    pub recorded: bool,
    /// An idle origin carried from the record it acquired (Unix ms; ADR
    /// 0035, section 3), kept until a verified activation clears it.
    pub idle_since_ms: Option<u64>,
}

/// An upload completed when its hold could no longer acknowledge it: the
/// hold ended, its lease epoch moved or its validity ran out, or (for an
/// upload that advances the head) the restore was never verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Late;

/// The acknowledgement critical section: under the cell's lock, the hold
/// must still be there and `valid` (its epoch and deadline, checked
/// without awaiting) must hold; then `advance`, if given, becomes the
/// acknowledged head. An advancing upload of an unverified hold is late.
pub fn acknowledge(
    cell: &Mutex<Option<Hold>>,
    valid: impl FnOnce() -> bool,
    advance: Option<Position>,
) -> Result<(), Late> {
    let mut guard = cell
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(hold) = guard.as_mut() else {
        return Err(Late);
    };
    if !valid() {
        return Err(Late);
    }
    if let Some(position) = advance {
        if hold.head.acknowledged.is_none() {
            return Err(Late);
        }
        hold.head.acknowledged = Some(position);
    }
    Ok(())
}

/// After a successful restore: the verified head, set under the same lock
/// and validity check, before activation is allowed.
pub fn verify(
    cell: &Mutex<Option<Hold>>,
    valid: impl FnOnce() -> bool,
    position: Position,
) -> Result<(), Late> {
    let mut guard = cell
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(hold) = guard.as_mut() else {
        return Err(Late);
    };
    if !valid() {
        return Err(Late);
    }
    hold.head.acknowledged = Some(position);
    // A verified activation starts a new idle period at its release.
    hold.head.idle_since_ms = None;
    Ok(())
}

/// Ends the hold: takes it out of the cell, its head frozen. Every
/// acknowledgement that took the lock first is in it; every later one is
/// [`Late`].
pub fn end(cell: &Mutex<Option<Hold>>) -> Option<Hold> {
    cell.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::Version;

    fn cell() -> Mutex<Option<Hold>> {
        Mutex::new(Some(Hold {
            key: "owners/x".into(),
            generation: 3,
            version: Version("v".into()),
            previous: None,
            head: HoldHead::default(),
        }))
    }

    fn at(txid: u64) -> Position {
        Position::Known {
            generation: 3,
            txid,
            checksum: txid * 7,
        }
    }

    #[test]
    fn an_unverified_hold_acknowledges_no_position() {
        let c = cell();
        assert_eq!(acknowledge(&c, || true, Some(at(1))), Err(Late));
        // A check-only acknowledgement is not a position.
        assert_eq!(acknowledge(&c, || true, None), Ok(()));
        assert_eq!(end(&c).unwrap().head.acknowledged, None);
    }

    #[test]
    fn acknowledgements_advance_the_head_until_the_hold_ends() {
        let c = cell();
        verify(&c, || true, Position::Empty).unwrap();
        acknowledge(&c, || true, Some(at(1))).unwrap();
        acknowledge(&c, || true, Some(at(2))).unwrap();
        // A periodic snapshot's check never moves the head.
        acknowledge(&c, || true, None).unwrap();
        // An invalid lease refuses, and changes nothing.
        assert_eq!(acknowledge(&c, || false, Some(at(3))), Err(Late));
        let ended = end(&c).unwrap();
        assert_eq!(ended.head.acknowledged, Some(at(2)));
        // After the end, everything is late: nothing revives the hold.
        assert_eq!(acknowledge(&c, || true, Some(at(3))), Err(Late));
        assert_eq!(acknowledge(&c, || true, None), Err(Late));
        assert_eq!(verify(&c, || true, at(3)), Err(Late));
        assert!(end(&c).is_none());
    }

    /// Whichever takes the lock first wins: an acknowledgement before the
    /// end is in the frozen head; one after it is late. Raced on threads.
    #[test]
    fn the_frozen_head_includes_every_acknowledgement_that_succeeded() {
        for _ in 0..200 {
            let c = std::sync::Arc::new(cell());
            verify(&c, || true, Position::Empty).unwrap();
            let acker = {
                let c = std::sync::Arc::clone(&c);
                std::thread::spawn(move || {
                    let mut last = None;
                    for txid in 1..=50 {
                        if acknowledge(&c, || true, Some(at(txid))).is_ok() {
                            last = Some(at(txid));
                        } else {
                            break;
                        }
                    }
                    last
                })
            };
            let ended = end(&c).unwrap();
            let last_ok = acker.join().unwrap();
            assert_eq!(
                ended.head.acknowledged,
                last_ok.or(Some(Position::Empty)),
                "the frozen head is the last successful acknowledgement"
            );
        }
    }
}
