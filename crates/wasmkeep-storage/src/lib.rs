//! SQLite per actor, WAL replication and restore.
//!
//! One SQLite database per actor on local disk, opened in WAL mode and
//! sandboxed with an authorizer. For replication (M2), [`wal`] captures each
//! committed transaction's pages and [`ltx`] encodes them; restoring builds a
//! database back from those files.
//! This crate is synchronous and knows nothing about Wasm, HTTP or clusters;
//! callers run it on blocking threads.

#![forbid(unsafe_code)]

mod db;
mod error;
#[cfg(feature = "fault-injection")]
pub mod faults;
pub mod ltx;
mod path;
mod value;
pub mod wal;

pub use db::{ActorDb, PendingAlarms, StateDir, Storage, StorageConfig};
pub use error::{SqlError, SqlErrorKind, StorageError};
pub use path::encode_segment;
pub use value::{Rows, SqlValue};
