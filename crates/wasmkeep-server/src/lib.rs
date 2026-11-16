//! HTTP front door and router wiring runtime, storage and cluster together.
//!
//! Requests under `/actor/<class>/<name>/...` go to actors (one instance per
//! name, events serialized through its mailbox, SQLite per actor). Anything
//! else, outside `/_wasmkeep/`, goes to an optional front: a stateless
//! `wasi:http` component, instantiated per request, which calls the actors
//! (ADR 0023).

#![forbid(unsafe_code)]

pub mod access;
mod actor;
mod alarm;
pub mod apps;
mod cluster;
mod cluster_tls;
mod counted_store;
pub mod egress;
pub mod env;
mod error;
pub mod fds;
mod front;
pub mod guest_log;
mod http;
mod local_layout;
mod logging;
pub mod metrics;
mod rate_limit;
mod replica;
pub mod secret_store;
pub mod secrets;
mod websocket;

pub use access::{Access, AccessRules};
#[cfg(feature = "batched-writes")]
#[doc(hidden)]
pub use actor::hold_node_upload_budget;
pub use actor::{ActorConfig, ActorService};
pub use cluster::{
    ClusterConfig, ClusterPlaintext, ClusterTlsConfig, DEFAULT_HEARTBEAT, IMPLICIT_APP_NAME,
    failover_wait_default, heartbeat_horizon_ms, older_nodes_advice,
};
pub use egress::{EgressPolicy, IpRange};
pub use env::ActorEnv;
pub use error::Error;
pub use front::DEFAULT_CPU_BUDGET as DEFAULT_FRONT_CPU_BUDGET;
pub use front::DeployedFront;
pub use guest_log::GuestLog;
pub use http::{
    App, HEALTH_PATH, HttpComponent, MAX_BODY_BYTES, READY_PATH, ServeLimits, serve, serve_metrics,
    serve_with, serve_with_cluster_listener,
};
/// The version this build reports ([`wasmkeep_cluster::VERSION`]).
pub use wasmkeep_cluster::VERSION;
pub use wasmkeep_cluster::lease::LeaseConfig;
pub use wasmkeep_cluster::state_root::StateRoot;

/// Where `root`'s actor databases live under the data directory (ADR 0022):
/// the one place the local layout is spelled.
#[must_use]
pub fn state_dir(root: &StateRoot) -> wasmkeep_storage::StateDir {
    wasmkeep_storage::StateDir {
        dir: root.local(wasmkeep_storage::encode_segment),
        scratch: root.scratch(wasmkeep_storage::encode_segment),
        tenant: root.tenant().to_owned(),
    }
}

/// The folder of actor `class/name`'s history in the bucket, relative to
/// the bucket prefix and ending in `/` (what `Cluster::history_key`
/// lists): for the command line, which checks an actor exists before
/// asking for its delete (ADR 0028).
#[must_use]
pub fn history_folder(root: &StateRoot, class: &str, name: &str) -> String {
    format!(
        "{}/",
        root.key(
            wasmkeep_cluster::state_root::Family::History,
            &format!("{class}/{}", wasmkeep_storage::encode_segment(name)),
        )
    )
}

/// The folder of app `tenant/app`'s local files under the data directory
/// `data_dir` (its databases and scratch space): what a node removes once
/// the app is deleted (ADR 0028).
#[must_use]
pub fn local_app_dir(data_dir: &std::path::Path, tenant: &str, app: &str) -> std::path::PathBuf {
    data_dir.join(StateRoot::new("", tenant, app).local_root(wasmkeep_storage::encode_segment))
}

/// Lease-free verified actor exports (ADR 0033).
pub use replica::history;
