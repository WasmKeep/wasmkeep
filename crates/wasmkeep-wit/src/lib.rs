//! The `wasmkeep:actor` WIT package and the host bindings generated from it.
//!
//! The WIT files live in `wit/` (with WASI 0.2.12 vendored in `wit/deps/`).
//! Host bindings cover the `actor-core` world: WasmKeep's own imports and the
//! `on-init` / `on-alarm` exports. The `wasi:http/proxy` half of the `actor`
//! world is served by `wasmtime-wasi-http`.

#![forbid(unsafe_code)]

/// Path to the WIT package directory, for tooling and tests.
pub const WIT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/wit");

/// Version of the `wasmkeep:actor` package these bindings were generated from.
pub const PACKAGE_VERSION: &str = "0.2.0";

#[allow(
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    clippy::all
)]
mod generated {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "actor-core",
        imports: { default: async | trappable },
        exports: { default: async },
    });
}

pub use generated::*;
