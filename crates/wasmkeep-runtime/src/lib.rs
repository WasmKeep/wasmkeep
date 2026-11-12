//! Wasmtime engine, actor mailbox, hibernation and resource limits.
//!
//! This crate knows nothing about HTTP, sockets or clusters. It owns the
//! [`Runtime`] (a configured [`wasmtime::Engine`]) and the on-disk
//! [`ComponentCache`] of pre-compiled components.

// Unsafe is allowed in this crate only; every block needs a `// SAFETY:` comment.

mod address;
mod cache;
mod config;
mod cpu;
mod error;
mod mailbox;
mod residency;
mod runtime;

pub use address::{ActorAddress, AddressError, MAX_IDENT_LEN, MAX_NAME_LEN, is_identifier};
pub use cache::ComponentCache;
pub use config::RuntimeConfig;
pub use cpu::{CpuBudgetExceeded, CpuMeter, cpu_budget_exceeded_total};
pub use error::Error;
pub use mailbox::{BoxError, BoxFuture, MailboxConfig, MailboxError, Mailboxes};
pub use residency::{Residency, Retained};
pub use runtime::Runtime;
