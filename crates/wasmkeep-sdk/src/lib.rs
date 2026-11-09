//! Rust guest SDK for writing WasmKeep actors.
//!
//! An actor is a type that implements [`Actor`] (lifecycle hooks, all
//! optional) and [`http::Guest`] (the request handler), exported with
//! [`export!`]:
//!
//! ```ignore
//! struct Counter;
//!
//! impl wasmkeep_sdk::Actor for Counter {
//!     fn on_init() -> Result<(), String> {
//!         wasmkeep_sdk::storage::exec("CREATE TABLE IF NOT EXISTS t (v INTEGER)", &[])
//!             .map(drop)
//!             .map_err(|e| e.to_string())
//!     }
//! }
//!
//! impl wasmkeep_sdk::http::Guest for Counter {
//!     fn handle(request: wasmkeep_sdk::http::IncomingRequest, out: wasmkeep_sdk::http::ResponseOutparam) {
//!         wasmkeep_sdk::http::respond(out, 200, "text/plain", b"hi");
//!     }
//! }
//!
//! wasmkeep_sdk::export!(Counter);
//! ```
//!
//! Build with `cargo build --release --target wasm32-wasip2`. The result is a
//! component targeting the `wasmkeep:actor/actor` world.

// wit-bindgen emits `unsafe` glue; it is confined to the `bindings` module.
#![deny(unsafe_code)]

#[doc(hidden)]
#[allow(
    unsafe_code,
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    clippy::all
)]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../wasmkeep-wit/wit",
        world: "actor-core",
        pub_export_macro: true,
        export_macro_name: "export_core",
        default_bindings_module: "::wasmkeep_sdk::bindings",
        additional_derives: [PartialEq, Clone],
    });
}

pub mod http;
pub mod kv;
pub mod queue;
mod value;
mod wire;
pub mod workflow;

/// Calls to other actors of the same tenant.
pub use bindings::wasmkeep::actor::actors;
/// The actor's pending alarm.
pub use bindings::wasmkeep::actor::alarms;
/// Configuration and secrets the operator gives this class
/// (`env::get("API_KEY")`).
pub use bindings::wasmkeep::actor::env;
/// The actor's own SQLite database.
pub use bindings::wasmkeep::actor::storage;
pub use bindings::wasmkeep::actor::types::{Rows, SqlError, SqlErrorKind, Value};
/// WebSocket connections to this actor.
pub use bindings::wasmkeep::actor::websockets;
pub use bindings::wasmkeep::actor::websockets::Message;
/// WASI 0.2 bindings (`wasip2` crate): outbound HTTP, clocks, randomness.
pub use wasip2 as wasi;

/// Milliseconds since the Unix epoch, from the host's wall clock. The unit
/// `alarms::set_alarm` expects.
pub fn now_ms() -> u64 {
    let now = wasip2::clocks::wall_clock::now();
    now.seconds * 1000 + u64::from(now.nanoseconds / 1_000_000)
}

/// Lifecycle hooks, all optional: `on_init` and `on_alarm` default to
/// `Ok(())`, `on_websocket_open` to refusing the connection, and the other
/// WebSocket hooks to ignoring the event.
pub trait Actor {
    /// Runs before the first event this instance handles with this component
    /// version: create or migrate tables here.
    fn on_init() -> Result<(), String> {
        Ok(())
    }

    /// Runs when the pending alarm is due. Returning an error retries it.
    fn on_alarm() -> Result<(), String> {
        Ok(())
    }

    /// A client opened a WebSocket to `path` on this actor. The default
    /// refuses it; return `Ok(())` to accept.
    fn on_websocket_open(conn: u64, path: String) -> Result<(), String> {
        let _ = (conn, path);
        Err("this actor does not accept WebSockets".into())
    }

    /// A message arrived on connection `conn`.
    fn on_websocket_message(conn: u64, msg: Message) {
        let _ = (conn, msg);
    }

    /// Connection `conn` closed.
    fn on_websocket_close(conn: u64, code: u16, reason: String) {
        let _ = (conn, code, reason);
    }
}

/// Exports `$ty` as the actor: its [`Actor`] hooks and its
/// [`http::Guest`] request handler.
#[macro_export]
macro_rules! export {
    ($ty:ident) => {
        const _: () = {
            struct __WasmkeepCore;

            impl $crate::bindings::Guest for __WasmkeepCore {
                fn on_init() -> ::core::result::Result<(), ::std::string::String> {
                    <$ty as $crate::Actor>::on_init()
                }

                fn on_alarm() -> ::core::result::Result<(), ::std::string::String> {
                    <$ty as $crate::Actor>::on_alarm()
                }

                fn on_websocket_open(
                    conn: u64,
                    path: ::std::string::String,
                ) -> ::core::result::Result<(), ::std::string::String> {
                    <$ty as $crate::Actor>::on_websocket_open(conn, path)
                }

                fn on_websocket_message(conn: u64, msg: $crate::Message) {
                    <$ty as $crate::Actor>::on_websocket_message(conn, msg)
                }

                fn on_websocket_close(conn: u64, code: u16, reason: ::std::string::String) {
                    <$ty as $crate::Actor>::on_websocket_close(conn, code, reason)
                }
            }

            $crate::bindings::export_core! { __WasmkeepCore with_types_in $crate::bindings }
            $crate::wasi::http::proxy::export! { $ty with_types_in $crate::wasi }
        };
    };
}

/// Builds a `Vec<Value>` of SQL parameters: `params![42, "name", None::<i64>]`.
#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => {
        ::std::vec![$($crate::Value::from($value)),*]
    };
}
