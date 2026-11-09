//! Invariant checks over what nodes believed.

use crate::driver::Claim;

/// A pair of overlapping claims by different processes: two live owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoubleOwnership {
    /// First claim.
    pub a: Claim,
    /// Overlapping claim by another process.
    pub b: Claim,
}

/// Finds the first moment two different processes (nodes, or instances of
/// one node id) both believed they owned the actor. Claims with an empty
/// window (`until <= from`) are ignored: the node learned of its ownership
/// too late to act on it.
pub fn double_ownership(claims: &[Claim]) -> Option<DoubleOwnership> {
    let live: Vec<&Claim> = claims.iter().filter(|c| c.until_ms > c.from_ms).collect();
    for (i, a) in live.iter().enumerate() {
        for b in &live[i + 1..] {
            let other = (a.node, a.instance) != (b.node, b.instance);
            if other && a.from_ms < b.until_ms && b.from_ms < a.until_ms {
                return Some(DoubleOwnership { a: **a, b: **b });
            }
        }
    }
    None
}

/// Ends `node`'s claims at global time `at`: it stopped acting as owner
/// there (to release the lease).
pub fn stop(claims: &mut [Claim], node: u32, at: u64) {
    for c in claims.iter_mut().filter(|c| c.node == node) {
        c.until_ms = c.until_ms.min(at);
    }
}

/// Ends one process's claims at global time `at` (its twin's stand).
pub fn stop_process(claims: &mut [Claim], node: u32, instance: u64, at: u64) {
    for c in claims
        .iter_mut()
        .filter(|c| c.node == node && c.instance == instance)
    {
        c.until_ms = c.until_ms.min(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(node: u32, from_ms: u64, until_ms: u64) -> Claim {
        Claim {
            node,
            instance: 0,
            generation: 1,
            from_ms,
            until_ms,
        }
    }

    #[test]
    fn overlap_between_nodes_is_found() {
        assert!(double_ownership(&[c(1, 0, 100), c(2, 100, 200)]).is_none());
        assert!(double_ownership(&[c(1, 0, 100), c(1, 50, 150)]).is_none());
        assert!(double_ownership(&[c(1, 0, 100), c(2, 99, 200)]).is_some());
        assert!(double_ownership(&[c(1, 0, 100), c(2, 50, 40)]).is_none());
        // Twins: one node id, two processes.
        let twin = Claim {
            instance: 7,
            ..c(1, 50, 150)
        };
        assert!(double_ownership(&[c(1, 0, 100), twin]).is_some());
        let mut both = [c(1, 0, 100), twin];
        stop_process(&mut both, 1, 7, 60);
        assert_eq!((both[0].until_ms, both[1].until_ms), (100, 60));
    }

    #[test]
    fn stopping_ends_only_that_nodes_claims() {
        let mut claims = [c(1, 0, 100), c(1, 50, 150), c(2, 0, 100)];
        stop(&mut claims, 1, 60);
        assert_eq!(claims[0].until_ms, 60);
        assert_eq!(claims[1].until_ms, 60);
        assert_eq!(claims[2].until_ms, 100);
        // Released at 60: another node may start at 60.
        assert!(double_ownership(&[claims[0], claims[1], c(2, 60, 200)]).is_none());
    }
}
