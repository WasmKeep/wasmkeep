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

/// Default socket demand keeps a minimum share alongside actor storage.
/// Explicit socket demand shares the remaining capacity with actor storage.
pub fn budget(limit: u64, requested: Budget) -> Budget {
    budget_with_socket_priority(limit, requested, false)
}

/// Estimate admission capacity. At very low limits retain one requested
/// public/cluster connection and resident actor for degraded operation; the
/// caller must warn that the reserved descriptor headroom cannot be met.
pub fn budget_with_socket_priority(
    limit: u64,
    requested: Budget,
    explicit_sockets: bool,
) -> Budget {
    let mut left = limit.saturating_sub(RESERVED);
    let cluster = (requested.cluster_connections as u64).min((left / 8).max(1));
    left = left.saturating_sub(cluster);
    let public = (requested.public_connections as u64).min((left / 4).max(1));
    left = left.saturating_sub(public);
    let actor_share = if explicit_sockets {
        left / 2
    } else {
        let socket_floor = (requested.socket_sessions as u64).min(left / 4);
        left - socket_floor * 2
    };
    let mut actors = (requested.resident_actors as u64).min((actor_share / PER_ACTOR).max(1));
    let sockets =
        (requested.socket_sessions as u64).min(left.saturating_sub(actors * PER_ACTOR) / 2);
    actors += (requested.resident_actors as u64 - actors)
        .min(left.saturating_sub(actors * PER_ACTOR + sockets * 2) / PER_ACTOR);
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
    fn default_sockets_keep_a_share_at_1024() {
        for (cluster_connections, actors) in [(0, 96), (1024, 84)] {
            let b = budget(
                1024,
                Budget {
                    public_connections: 10_000,
                    cluster_connections,
                    resident_actors: 900,
                    socket_sessions: 50_000,
                },
            );
            assert_eq!(b.resident_actors, actors);
            assert_eq!(
                b.socket_sessions,
                if cluster_connections == 0 { 144 } else { 126 }
            );
        }
    }

    #[test]
    fn default_sockets_preserve_small_actor_requests() {
        let b = budget(
            1024,
            Budget {
                public_connections: 10_000,
                resident_actors: 20,
                socket_sessions: 50_000,
                ..Default::default()
            },
        );
        assert_eq!(b.resident_actors, 20);
        assert_eq!(b.socket_sessions, 258);
    }

    #[test]
    fn exhausted_budget_keeps_minimum_services_without_sockets() {
        assert_eq!(budget(0, Budget::default()), Budget::default());
        let all = Budget {
            public_connections: 10,
            cluster_connections: 10,
            resident_actors: 10,
            socket_sessions: 10,
        };
        assert_eq!(
            budget(RESERVED, all),
            Budget {
                public_connections: 1,
                cluster_connections: 1,
                resident_actors: 1,
                socket_sessions: 0
            }
        );
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
                assert!(used <= limit.saturating_sub(RESERVED).max(5) as u128);
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
        let b = budget_with_socket_priority(1024, requested, true);
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
