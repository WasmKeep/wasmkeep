//! App-start decisions shared by the server and deterministic restore model.
//! Store and local-file errors must be handled by callers before deciding.

/// A tombstone or a pending restore prevents starts and continued service.
pub fn start_allowed(deleted: bool, restoring: bool) -> bool {
    !deleted && !restoring
}

/// An incarnation marker invalidates a node prepared before that incarnation.
/// No remote marker preserves the legacy app's local files.
pub fn marker_matches(remote: Option<&[u8]>, local: Option<&[u8]>) -> bool {
    remote.is_none_or(|marker| local == Some(marker))
}

/// The resolution of a bucket's `Last-Modified` (S3 keeps whole seconds): a
/// stamp may be this much earlier than the write it dates.
pub const STAMP_RESOLUTION_MS: u64 = 1_000;

/// How many times a restore waits for the app's node leases to stop
/// changing before it refuses (a node of the app still writes its lease).
pub const QUIET_ROUNDS: usize = 3;

/// How long a whole-app restore still waits before it writes the app's
/// state: its quiet wait (ADR 0033, section 6). Every time is on the
/// bucket's clock, its `Last-Modified` stamps; no node's clock is compared
/// with the operator's.
///
/// - `listing_complete`: every object under `nodes/` decoded, names its
///   app, and every node of the app writes a node lease and honours restore
///   fences. Such a node acquires only after its lease's first live write
///   landed and a check sent after it found no fence (ADR 0020, ADR 0028
///   section 5), so a node that never reads the fence was in the listing,
///   which is made after the fence landed.
/// - `newest_lease_stored_ms`: the newest `Last-Modified` among the node
///   lease objects of the app in that listing, or `None` if it has none.
///   A node acts at most `ttl - safety` after the last of its lease writes
///   that landed (its validity counts from that write's send, on its own
///   clock's rate), whether the object then says `live`, `released` or
///   `fenced`.
/// - `fence_stored_ms`: the restore fence's `Last-Modified`.
/// - `bucket_now_ms`: a lower bound of the bucket's clock now (a stamp read
///   earlier, plus the time elapsed since on the restoring process).
/// - `late_ms`: how long after its lease's last landed write a node's
///   request may still land (`LATE_WRITES`): an assumption, which the
///   bucket client does not enforce for every request (see `LATE_WRITES`).
///
/// A complete listing with no node of the app has nothing to wait for. A
/// complete listing with nodes waits until `late_ms` after the newest of
/// their lease writes. Otherwise (`--force`, a heartbeat this version cannot
/// read, one naming no app, an old node) the fence's stamp also counts, as
/// before 0.7.0: a node that started unseen shows its heartbeat by then, and
/// the check after the wait refuses it.
pub fn quiet_wait_ms(
    listing_complete: bool,
    newest_lease_stored_ms: Option<u64>,
    fence_stored_ms: u64,
    bucket_now_ms: u64,
    late_ms: u64,
) -> u64 {
    let from = match (listing_complete, newest_lease_stored_ms) {
        (true, None) => return 0,
        (true, Some(stored)) => stored,
        (false, stored) => stored.unwrap_or(0).max(fence_stored_ms),
    };
    from.saturating_add(STAMP_RESOLUTION_MS)
        .saturating_add(late_ms)
        .saturating_sub(bucket_now_ms)
}

/// When a restore may release its exclusive lock (`restore-lock`), in ms on
/// one monotonic clock of the restoring process.
///
/// `unsettled_ms`: the latest moment one of its mutating requests ended
/// without a definite answer (no answer, a timeout, a 5xx or 408 or 429
/// status, or a request dropped in flight), or `None` if every one was
/// answered. An answered request has been applied or refused, so it cannot
/// land later; one without an answer may land up to `late_ms` after it
/// ended, and holding the lock until then keeps a late fence deletion from
/// opening the next operation.
pub fn release_at_ms(now_ms: u64, unsettled_ms: Option<u64>, late_ms: u64) -> u64 {
    unsettled_ms.map_or(now_ms, |ended| ended.saturating_add(late_ms).max(now_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_waits_only_for_writes_that_may_still_land() {
        // No node of the app, and the listing proves it: no wait at all.
        assert_eq!(quiet_wait_ms(true, None, 1_000, 5_000, 40_000), 0);
        // A node's last lease write stored at 10 s: until 51 s on the
        // bucket's clock (a whole-second stamp may be a second early).
        assert_eq!(
            quiet_wait_ms(true, Some(10_000), 90_000, 30_000, 40_000),
            21_000
        );
        assert_eq!(quiet_wait_ms(true, Some(10_000), 1_000, 60_000, 40_000), 0);
        // An incomplete listing counts the fence too.
        assert_eq!(quiet_wait_ms(false, None, 1_000, 1_000, 40_000), 41_000);
        assert_eq!(
            quiet_wait_ms(false, Some(10_000), 1_000, 10_000, 40_000),
            41_000
        );
        assert_eq!(
            quiet_wait_ms(false, Some(10_000), 20_000, 20_000, 40_000),
            41_000
        );
    }

    #[test]
    fn release_waits_only_after_an_unanswered_request() {
        assert_eq!(release_at_ms(5_000, None, 40_000), 5_000);
        assert_eq!(release_at_ms(5_000, Some(4_000), 40_000), 44_000);
        assert_eq!(release_at_ms(50_000, Some(4_000), 40_000), 50_000);
    }

    #[test]
    fn closed_fences_and_changed_or_missing_local_markers_refuse() {
        assert!(start_allowed(false, false));
        for (deleted, restoring) in [(true, false), (false, true), (true, true)] {
            assert!(!start_allowed(deleted, restoring));
        }
        assert!(marker_matches(None, Some(b"old")));
        assert!(marker_matches(Some(b"new"), Some(b"new")));
        assert!(!marker_matches(Some(b"new"), Some(b"old")));
        assert!(!marker_matches(Some(b"new"), None));
    }
}
