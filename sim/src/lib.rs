//! Cluster protocols under deterministic simulation.
//!
//! The protocols themselves live in the production crates
//! (`wasmkeep-cluster`), written against small traits so this crate can drive
//! the real code under madsim. `madsim_node_lease` drives the node lease and
//! ownership records the server uses (`node_lease::Owners`, ADR 0020),
//! checked for "never two live owners" under faults and clock skew.
//! `madsim_replication`, `madsim_alarm_index` and `madsim_drain` run on
//! [`node`], a node on node leases with its lease on a task of its own and
//! its holds acting by the server's own rule (`wasmkeep_cluster::holds`);
//! `madsim_lease` still models the retired per-actor lease itself
//! ([`driver`]), kept after the server moved to node leases (ADR 0020).
//!
//! Simulations run under madsim (ADR 0006) and only build with
//! `RUSTFLAGS="--cfg madsim"`:
//!
//! ```text
//! RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim
//! ```
//!
//! `WASMKEEP_SIM_SEEDS` sets the number of seeds (default 200).

#![forbid(unsafe_code)]

pub mod check;
pub mod driver;
pub mod node;
pub mod rng;
