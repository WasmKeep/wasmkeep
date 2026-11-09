//! File descriptors: every resident actor keeps three open (its SQLite
//! database, write-ahead log and shared-memory index), every connection one.
//! A process starts with a soft limit (often 1,024) far below its hard limit;
//! these helpers raise it and size the caps to what fits.

/// Raises the soft limit on open files to the hard limit (capped at
/// `ceiling`), and returns the soft limit now in force; `None` if it cannot
/// be read (then nothing is sized from it).
pub fn raise_open_files_limit(ceiling: u64) -> Option<u64> {
    #[cfg(unix)]
    {
        use rustix::process::{Resource, getrlimit, setrlimit};
        let limit = getrlimit(Resource::Nofile);
        let soft = limit.current?;
        let target = limit.maximum.unwrap_or(ceiling).min(ceiling);
        if target > soft {
            let raised = rustix::process::Rlimit {
                current: Some(target),
                maximum: limit.maximum,
            };
            if setrlimit(Resource::Nofile, raised).is_ok() {
                return Some(target);
            }
        }
        Some(soft)
    }
    #[cfg(not(unix))]
    {
        let _ = ceiling;
        None
    }
}

/// Descriptors kept back for everything else: the listeners, the bucket and
/// node-to-node clients, outbound HTTP, logs, the compile cache.
pub const RESERVED: u64 = 256;
/// Descriptors one resident actor holds.
pub const PER_ACTOR: u64 = 3;

/// Actor capacity within the instance pool, reserving roughly ten percent
/// for short-lived stateless instances while allowing a one-instance pool.
pub fn resident_pool_limit(pool: usize) -> usize {
    (pool - pool.div_ceil(10)).max(1)
}

/// Connections the cluster port (ADR 0011) holds at most: nodes keep a few
/// each, so this is far more than a cluster needs.
pub const CLUSTER_CONNECTIONS: usize = 1_024;

/// With a cluster listener: its connection cap, and the limit left for
/// [`budget`]. The cap is [`CLUSTER_CONNECTIONS`], or an eighth of what is
/// not reserved when the limit is too low for that, so the public port and
/// the resident actors keep most of it.
pub fn reserve_cluster_port(limit: u64) -> (usize, u64) {
    let usable = limit.saturating_sub(RESERVED);
    let cluster = (CLUSTER_CONNECTIONS as u64).min(usable / 8).max(1);
    (
        usize::try_from(cluster).unwrap_or(CLUSTER_CONNECTIONS),
        limit.saturating_sub(cluster),
    )
}

/// How many connections and resident actors fit in `limit` descriptors:
/// connections get at most a quarter of what is not reserved, actors the
/// rest. Returns `(connections, actors)`, each no more than asked for.
pub fn budget(limit: u64, want_connections: usize, want_actors: usize) -> (usize, usize) {
    let usable = limit.saturating_sub(RESERVED);
    let connections = (want_connections as u64).min(usable / 4).max(1);
    let actors = (usable.saturating_sub(connections) / PER_ACTOR).max(1);
    (
        usize::try_from(connections).unwrap_or(usize::MAX),
        usize::try_from(actors)
            .unwrap_or(usize::MAX)
            .min(want_actors),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_small_limit_shrinks_both_caps_to_fit() {
        // The default soft limit with the default caps (10,000 connections,
        // 900 resident actors): both must shrink.
        let (connections, actors) = budget(1_024, 10_000, 900);
        assert_eq!(connections, 192);
        assert_eq!(actors, 192);
        assert!(256 + connections as u64 + 3 * actors as u64 <= 1_024);
        // A generous limit keeps what was asked.
        assert_eq!(budget(1 << 20, 10_000, 900), (10_000, 900));
    }

    #[test]
    fn the_cluster_port_is_counted_before_the_rest() {
        // The default soft limit: the cluster port gets an eighth of what
        // is not reserved, and everything still fits.
        let (cluster, rest) = reserve_cluster_port(1_024);
        assert_eq!((cluster, rest), (96, 928));
        let (connections, actors) = budget(rest, 10_000, 900);
        assert!(256 + cluster as u64 + connections as u64 + 3 * actors as u64 <= 1_024);
        // A generous limit: the full cap, and the rest as before.
        let (cluster, rest) = reserve_cluster_port(1 << 20);
        assert_eq!(cluster, CLUSTER_CONNECTIONS);
        assert_eq!(budget(rest, 10_000, 900), (10_000, 900));
        // Nothing left: still one connection.
        assert_eq!(reserve_cluster_port(0).0, 1);
    }

    #[test]
    fn small_instance_pools_keep_room_for_actor_calls() {
        for (pool, expected) in [
            (1, 1),
            (2, 1),
            (8, 7),
            (9, 8),
            (10, 9),
            (11, 9),
            (1000, 900),
        ] {
            assert_eq!(resident_pool_limit(pool), expected, "pool={pool}");
        }
        assert_eq!(
            resident_pool_limit(usize::MAX),
            usize::MAX - usize::MAX.div_ceil(10)
        );
    }
}
