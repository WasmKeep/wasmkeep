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
[tutorial](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/tutorials/first-actor.md)
goes from an empty directory to an actor whose state survives a restart.

## What is in the crate

- `storage`: the actor's own SQLite database (`exec`, `query`, `params!`).
- `alarms`: one pending alarm per actor, delivered to `Actor::on_alarm`.
- `actors`: calls to other actors of the same app.
- `calls`: several of those calls at once (`calls::call_many`, at most 16,
  one result per call in input order), from actors and fronts
  ([ADR 0025](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/adr/0025-calling-several-actors-at-once.md)).
- `websockets`: connections to this actor, with the `Actor::on_websocket_*` hooks.
- `env`: configuration and secrets the operator gives the class (or the front).
- `caller`, `Caller`: who sent a request (the app's front, another actor,
  or a client), and the `front` module (below).
- `http`: the request handler, helpers to answer, and outbound requests.
- `kv`, `queue`, `workflow`: a key-value store, queues and durable workflows,
  each built from actors
  ([KV and queues](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/kv-and-queues.md),
  [workflows](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/workflows.md)).
- `wasi`: the WASI 0.2 bindings (the `wasip2` crate).

The [API reference](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/reference/api.md)
lists every host function with its name in each SDK.

## Writing a front

A front is an app's stateless `wasi:http` component
([ADR 0023](https://github.com/WasmKeep/wasmkeep/blob/v0.4.0/docs/adr/0023-the-front.md)):
it receives every request outside `/actor/` and `/_wasmkeep/`, routes,
signs users in, and calls the app's actors with `actors::call` as an
inside caller, so it reaches `internal` classes. It targets the
`wasmkeep:actor/front` world, which has `actors` and `env` and none of
the actor world's storage, alarms or WebSockets, so its crate depends on
the SDK without the default `actor` feature:

```toml
[dependencies]
wasmkeep-sdk = { version = "0.3", default-features = false }
```

With the feature on, the component also imports the actor world and is
not a front. Cargo unifies features of crates built together, so build a
front in its own workspace (or with `-p`), as the examples do.

```rust
use wasmkeep_sdk::front::{self, actors};
use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};

struct Front;

impl http::Guest for Front {
    fn handle(_request: IncomingRequest, out: ResponseOutparam) {
        let call = actors::Request {
            method: "GET".into(),
            path: "/".into(),
            headers: vec![],
            body: vec![],
        };
        match actors::call("todos/alice", &call) {
            Ok(response) => http::respond_with(out, &response),
            Err(e) => http::respond_with(out, &front::unavailable(&e)),
        }
    }
}

wasmkeep_sdk::export_front!(Front);
```

The `front` module has:

- `actors`, `env`: the same modules as in an actor. The callee reads
  `<tenant>/_front/main` as its caller; `env::get` reads the front's own
  values (`serve --front-env FILE`).
- `caller(&request)`: `Caller::Front`, `Caller::Actor(address)` or
  `Caller::Outside`, from the `x-wasmkeep-caller` header the host sets. A
  client cannot set it: the host removes it from what clients send, and
  from what guests send on `actors.call`. Only the exact value
  `<tenant>/_front/main` is the front, and any value the host never sends
  is `Outside`. Actors use it to answer their front only:
  `if wasmkeep_sdk::caller(&request) != Caller::Front { /* 403 */ }`,
  which keeps a sensitive class closed if it is ever deployed `open`.
- `host(&request)`: the request's authority, the client's `Host` (behind a
  proxy, only if the proxy passes it). The client chooses it, unvalidated:
  escape it where it goes (HTML, JSON).
- `scheme(&request)`: `"https"` when the first `X-Forwarded-Proto` value is
  `https`, else `"http"`. A client can send that header too, so it is only
  as good as the proxy in front of the node, which must replace it: use it
  for links and redirects, never to decide what a request may do.
- `unavailable(&err)`: the answer for a failed call: `503` with
  `Retry-After: 1` for `failed` and `timeout`, `500` for the others, with
  a generic body (log the error's detail; do not send it to the client).
- `export_front!(Type)`: exports the request handler, and nothing else.

Validate any name you take from a request before using it in an address:
each new name is a new actor with its own database.
[`examples/todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.4.0/examples/todo-front)
is a front with a page, a JSON API, sign-in by token, and an `internal`
class that checks `caller()`.

Without the `actor` feature the crate has `front`, `actors`, `env`,
`caller`, `http` and `wasi`; `storage`, `alarms`, `websockets`, `Actor`,
`export!`, `params!`, `kv`, `queue` and `workflow` need it.

## Versions

The crate has the version of the WasmKeep server it is released with: use
the two at the same version (`cargo add wasmkeep-sdk@0.4.0` for server
0.4.0). It needs Rust 1.97 or later. The interface it binds to, `wasmkeep:actor@0.2.0`, is versioned on its own: the
WIT files are in `wit/`, a copy of the ones the server is built from.

## License

Apache-2.0; see `LICENSE` and `NOTICE`. WasmKeep is pre-alpha.
