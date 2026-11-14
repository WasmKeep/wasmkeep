//! The `wasmkeep:actor` WIT package and the host bindings generated from it.
//!
//! The WIT files live in `wit/` (with WASI 0.2.12 vendored in `wit/deps/`).
//! Host bindings cover the `actor-core` world: WasmKeep's own imports and the
//! `on-init` / `on-alarm` exports, and the `front-core` world (the imports a
//! front gets, [`FrontCore`]), which shares the `actors`, `calls` and `env`
//! types.
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
        },
    });
}

/// The `front-core` world's imports (`actors`, `calls` and `env`), for
/// linking a front. Its interfaces are those of [`ActorCore`]: one `Host`
/// impl of `actors`, `calls` and `env` serves both.
pub use front_generated::FrontCore;
