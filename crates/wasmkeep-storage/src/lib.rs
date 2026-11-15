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
mod local;
pub mod ltx;
mod path;
pub mod readonly;
mod value;
pub mod wal;

pub use db::{ActorDb, CaptureHold, PendingAlarms, StateDir, Storage, StorageConfig};
pub use error::{SqlError, SqlErrorKind, StorageError};
pub use local::local_is_at;
pub use path::{decode_segment, encode_segment};
pub use value::{Rows, SqlValue};
