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

/// When a whole-app restore may start writing the app's state, in ms on the
/// restoring CLI's clock: its quiet wait (ADR 0033, section 6).
///
/// - `fence_ms`: when the restore fence (`apps/<t>/<a>/restoring`) was
///   first written.
/// - `last_node_ms`: the latest `written_at` among the app's node leases,
///   listed after the fence landed (a `released` or `fenced` lease keeps the
///   one of its last live write), or `None` if the app has no node object.
/// - `listing_complete`: every object under `nodes/` decoded, and every
///   node of the app writes a node lease and honours restore fences. Such a
///   node acquires only after its lease's first live write landed and a
///   check sent after it found no fence (ADR 0020, ADR 0028 section 5), so
///   a node that never reads the fence was in that listing.
/// - `late_ms`: how long after a node lease's last live write one of that
///   node's requests may still land: the lease stops the node by
///   `written_at + ttl - safety` on its own clock, and a request it sent
///   then lands within the bucket client's request timeout or never.
///
/// A complete listing with no node of the app has nothing to wait for. A
/// complete listing with nodes waits `late_ms` after the last of their
/// writes. Otherwise (`--force`, an unreadable or old-format heartbeat) the
/// fence's time also counts, as before 0.7.0: a node that started unseen
/// has shown its heartbeat by then, and the check after the wait refuses it.
pub fn quiet_until_ms(
    fence_ms: u64,
    last_node_ms: Option<u64>,
    listing_complete: bool,
    late_ms: u64,
) -> u64 {
    let from = match (listing_complete, last_node_ms) {
        (true, None) => return fence_ms,
        (true, Some(last)) => last,
        (false, last) => last.unwrap_or(0).max(fence_ms),
    };
    from.saturating_add(late_ms)
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
        assert_eq!(quiet_until_ms(1_000, None, true, 40_000), 1_000);
        // A stopped node: its last live write plus the late bound, not the fence.
        assert_eq!(quiet_until_ms(90_000, Some(10_000), true, 40_000), 50_000);
        assert_eq!(quiet_until_ms(1_000, Some(10_000), true, 40_000), 50_000);
        // An incomplete listing counts the fence too.
        assert_eq!(quiet_until_ms(1_000, None, false, 40_000), 41_000);
        assert_eq!(quiet_until_ms(1_000, Some(10_000), false, 40_000), 50_000);
        assert_eq!(quiet_until_ms(20_000, Some(10_000), false, 40_000), 60_000);
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
