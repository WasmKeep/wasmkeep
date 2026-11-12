# wasmkeep-sdk: WasmKeep actors in Rust

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep) actors in Rust. An
actor is a type that implements `Actor` (lifecycle hooks, all optional) and
`http::Guest` (the request handler), exported with `export!`:

```rust
use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::{Actor, params, storage};

struct Visits;

impl Actor for Visits {
    // Runs before the first event of each deployed version: create tables here.
    fn on_init() -> Result<(), String> {
        storage::exec("CREATE TABLE IF NOT EXISTS visits (who TEXT NOT NULL)", &[])
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

impl http::Guest for Visits {
    // One request at a time, in arrival order: no locks needed.
    fn handle(_request: IncomingRequest, out: ResponseOutparam) {
        match storage::exec("INSERT INTO visits (who) VALUES (?1)", &params!["someone"]) {
            Ok(_) => http::respond(out, 200, "text/plain", b"recorded\n"),
            Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
        }
    }
}

wasmkeep_sdk::export!(Visits);
```

## Building an actor

The actor is a library crate built for `wasm32-wasip2` with plain cargo; no
other tool is needed.

```bash
cargo new --lib visits && cd visits
cargo add wasmkeep-sdk
```

In its `Cargo.toml`, the library is a `cdylib`:

```toml
[lib]
crate-type = ["cdylib"]
```

```bash
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2
```

The result, `target/wasm32-wasip2/release/<name>.wasm`, is a component for
the `wasmkeep:actor/actor` world; run it with `wasmkeep serve`. The
[tutorial](https://github.com/WasmKeep/wasmkeep/blob/v0.2.0/docs/tutorials/first-actor.md)
goes from an empty directory to an actor whose state survives a restart.

## What is in the crate

- `storage`: the actor's own SQLite database (`exec`, `query`, `params!`).
- `alarms`: one pending alarm per actor, delivered to `Actor::on_alarm`.
- `actors`: calls to other actors of the same tenant.
- `websockets`: connections to this actor, with the `Actor::on_websocket_*` hooks.
- `env`: configuration and secrets the operator gives the class.
- `http`: the request handler, helpers to answer, and outbound requests.
- `kv`, `queue`, `workflow`: a key-value store, queues and durable workflows,
  each built from actors
  ([KV and queues](https://github.com/WasmKeep/wasmkeep/blob/v0.2.0/docs/kv-and-queues.md),
  [workflows](https://github.com/WasmKeep/wasmkeep/blob/v0.2.0/docs/workflows.md)).
- `wasi`: the WASI 0.2 bindings (the `wasip2` crate).

The [API reference](https://github.com/WasmKeep/wasmkeep/blob/v0.2.0/docs/reference/api.md)
lists every host function with its name in each SDK.

## Versions

The crate has the version of the WasmKeep server it is released with: use
the two at the same version (`cargo add wasmkeep-sdk@0.2.0` for server
0.2.0). It needs Rust 1.97 or later. The interface it binds to, `wasmkeep:actor@0.2.0`, is versioned on its own: the
WIT files are in `wit/`, a copy of the ones the server is built from.

## License

Apache-2.0; see `LICENSE` and `NOTICE`. WasmKeep is pre-alpha.
