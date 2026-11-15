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

#[cfg(test)]
mod tests {
    use super::*;

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
