//! The `wasmkeep:actor` WIT package and the host bindings generated from it.
//!
//! The WIT files live in `wit/` (with WASI 0.2.12 vendored in `wit/deps/`).
//! Host bindings cover the `actor-core` world: WasmKeep's own imports and the
//! `on-init` / `on-alarm` exports, and the `front-core` world (the imports a
//! front gets, [`FrontCore`]), which shares the `actors`, `calls`, `env` and
//! `blobs` types. `blobs` streams are `wasi:io` ones, mapped to
//! `wasmtime-wasi`'s bindings.
//! The `wasi:http/proxy` half of the `actor` and `front` worlds is served by
//! `wasmtime-wasi-http`.

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
        // `blobs` streams are WASI's own (ADR 0030, section 3): the ones
        // `wasmtime-wasi` keeps in the resource table.
        with: {
            "wasi:io": wasmtime_wasi::p2::bindings::io,
        },
    });
}

pub use generated::*;

#[allow(
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    clippy::all
)]
mod front_generated {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "front-core",
        imports: { default: async | trappable },
        with: {
            "wasmkeep:actor/actors": crate::generated::wasmkeep::actor::actors,
            "wasmkeep:actor/calls": crate::generated::wasmkeep::actor::calls,
            "wasmkeep:actor/env": crate::generated::wasmkeep::actor::env,
            "wasmkeep:actor/blobs": crate::generated::wasmkeep::actor::blobs,
            "wasi:io": wasmtime_wasi::p2::bindings::io,
        },
    });
}

/// The `front-core` world's imports (`actors`, `calls`, `env` and `blobs`),
/// for linking a front. Its interfaces are those of [`ActorCore`]: one
/// `Host` impl of each serves both.
pub use front_generated::FrontCore;
