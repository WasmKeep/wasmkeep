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

/// Actor capacity within the instance pool with the default front slots
/// ([`default_front_requests`], roughly ten percent), while allowing a
/// one-instance pool.
pub fn resident_pool_limit(pool: usize) -> usize {
    resident_limit(pool, default_front_requests(pool))
}

/// The front requests a node runs at once by default (`--max-front-requests`,
/// ADR 0023): the tenth of the instance pool kept from actors, rounded up.
pub fn default_front_requests(pool: usize) -> usize {
    pool.div_ceil(10)
}

/// Resident actors within an instance pool of `pool` that keeps
/// `front_requests` slots for the front: the rest, at least 1. A larger
/// front takes room from actors, never from itself.
pub fn resident_limit(pool: usize, front_requests: usize) -> usize {
    pool.saturating_sub(front_requests).max(1)
}

/// Connections the cluster port (ADR 0011) holds at most: nodes keep a few
/// each, so this is far more than a cluster needs.
pub const CLUSTER_CONNECTIONS: usize = 1_024;

/// Admission caps derived from the soft descriptor limit. This is an
/// estimate: outbound HTTP and bucket pools are not governed by these caps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Budget {
    pub public_connections: usize,
    pub cluster_connections: usize,
    pub resident_actors: usize,
    pub socket_sessions: usize,
}

/// Allocate cluster connections first, public connections next (at most a
/// quarter), then split the remainder between sockets and actors. Unused
/// actor capacity goes to sockets; unused socket capacity goes to actors.
/// All arithmetic is bounded by `limit`, including at integer extremes.
pub fn budget(limit: u64, requested: Budget) -> Budget {
    let mut left = limit.saturating_sub(RESERVED);
    let cluster = (requested.cluster_connections as u64).min(left / 8);
    left -= cluster;
    let public = (requested.public_connections as u64).min(left / 4);
    left -= public;
    let mut actors = (requested.resident_actors as u64).min(left / 2 / PER_ACTOR);
    let sockets = (requested.socket_sessions as u64).min((left - actors * PER_ACTOR) / 2);
    actors += (requested.resident_actors as u64 - actors)
        .min((left - actors * PER_ACTOR - sockets * 2) / PER_ACTOR);
    Budget {
        public_connections: public as usize,
        cluster_connections: cluster as usize,
        resident_actors: actors as usize,
        socket_sessions: sockets as usize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_budget_never_invents_descriptors() {
        assert_eq!(budget(0, Budget::default()), Budget::default());
        let all = Budget {
            public_connections: 10,
            cluster_connections: 10,
            resident_actors: 10,
            socket_sessions: 10,
        };
        assert_eq!(budget(RESERVED, all), Budget::default());
    }

    #[test]
    fn budget_caps_fit_even_at_integer_extremes() {
        for limit in (0..4096).chain([u64::MAX]) {
            for want in [0, 1, 10, usize::MAX] {
                let requested = Budget {
                    public_connections: want,
                    cluster_connections: want,
                    resident_actors: want,
                    socket_sessions: want,
                };
                let b = budget(limit, requested);
                let used = b.public_connections as u128
                    + b.cluster_connections as u128
                    + 3 * b.resident_actors as u128
                    + 2 * b.socket_sessions as u128;
                assert!(used <= limit.saturating_sub(RESERVED) as u128);
                assert!(
                    b.public_connections <= want
                        && b.cluster_connections <= want
                        && b.resident_actors <= want
                        && b.socket_sessions <= want
                );
            }
        }
    }

    #[test]
    fn unused_shares_are_redistributed() {
        let requested = Budget {
            public_connections: 10_000,
            cluster_connections: 1024,
            resident_actors: 900,
            socket_sessions: 50_000,
        };
        assert_eq!(budget(1 << 20, requested), requested);
        let b = budget(1024, requested);
        assert_eq!(b.cluster_connections, 96);
        assert_eq!(b.public_connections, 168);
        assert_eq!(b.resident_actors, 84);
        assert_eq!(b.socket_sessions, 126);
        let b = budget(
            1024,
            Budget {
                socket_sessions: 0,
                ..requested
            },
        );
        assert_eq!(b.resident_actors, 168);
        let b = budget(
            1024,
            Budget {
                resident_actors: 0,
                ..requested
            },
        );
        assert_eq!(b.socket_sessions, 252);
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
