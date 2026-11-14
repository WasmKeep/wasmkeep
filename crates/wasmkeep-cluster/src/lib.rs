//! Node leases and actor ownership records (ADR 0020, [`node_lease`]),
//! heartbeats, discovery and forwarding.
//!
//! Protocol logic is generic over small traits ([`lease::Bucket`],
//! [`lease::Clock`]) and has no network, runtime or object-store dependency,
//! so the deterministic simulation in `sim/` can drive exactly this code
//! (ADR 0006). Production adapters implement the traits elsewhere.

#![forbid(unsafe_code)]

/// The WasmKeep version this build reports, in its heartbeat, its metrics
/// and `--version`: the workspace version for a release (built from its
/// tag), `<version>+<commit>` for any other build from a git checkout
/// (see `build.rs`).
pub const VERSION: &str = env!("WASMKEEP_VERSION");

#[cfg(test)]
mod version_tests {
    #[test]
    fn the_version_is_the_release_or_names_its_commit() {
        let release = env!("CARGO_PKG_VERSION");
        let rest = super::VERSION
            .strip_prefix(release)
            .expect("starts with the workspace version");
        if let Some(commit) = rest.strip_prefix('+') {
            assert!(commit.len() >= 12, "{}", super::VERSION);
            assert!(
                commit.bytes().all(|b| b.is_ascii_hexdigit()),
                "{}",
                super::VERSION
            );
        } else {
            assert_eq!(rest, "", "{}", super::VERSION);
        }
    }
}

pub mod alarm_index;
pub mod bucket_check;
pub mod control;
pub mod failover;
pub mod holds;
pub mod lease;
pub mod membership;
pub mod migrate;
pub mod node_lease;
#[cfg(feature = "object-store")]
pub mod object_store;
pub mod replication;
pub mod state_root;
