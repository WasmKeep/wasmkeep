//! Node leases and actor ownership records (ADR 0020, [`node_lease`]),
//! heartbeats, discovery and forwarding.
//!
//! Protocol logic is generic over small traits ([`lease::Bucket`],
//! [`lease::Clock`]) and has no network, runtime or object-store dependency,
//! so the deterministic simulation in `sim/` can drive exactly this code
//! (ADR 0006). Production adapters implement the traits elsewhere.

#![forbid(unsafe_code)]

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
