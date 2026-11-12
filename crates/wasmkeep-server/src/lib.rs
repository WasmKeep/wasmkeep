//! HTTP front door and router wiring runtime, storage and cluster together.
//!
//! Requests under `/actor/<class>/<name>/...` go to actors (one instance per
//! name, events serialized through its mailbox, SQLite per actor). Anything
//! else goes to an optional stateless `wasi:http` component, instantiated per
//! request.

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
pub mod guest_log;
mod http;
pub mod metrics;
mod replica;
pub mod secret_store;
pub mod secrets;
mod websocket;

pub use access::{Access, AccessRules};
pub use actor::{ActorConfig, ActorService};
pub use cluster::{
    ClusterConfig, ClusterPlaintext, ClusterTlsConfig, DEFAULT_HEARTBEAT, failover_wait_default,
    heartbeat_horizon_ms,
};
pub use egress::{EgressPolicy, IpRange};
pub use env::ActorEnv;
pub use error::Error;
pub use guest_log::GuestLog;
pub use http::{
    App, HEALTH_PATH, HttpComponent, MAX_BODY_BYTES, READY_PATH, ServeLimits, serve, serve_metrics,
    serve_with, serve_with_cluster_listener,
};
pub use wasmkeep_cluster::lease::LeaseConfig;
