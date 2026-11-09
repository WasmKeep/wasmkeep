# Your first actor in 10 minutes

You will write a small actor in Rust, build it into a WebAssembly component,
serve it with WasmKeep, and see its state survive a restart.

Applies to: WasmKeep `main` (pre-alpha). Commands assume Linux
or macOS.

## 1. Get WasmKeep

You need [rustup](https://rustup.rs). The repository pins its Rust version and
the `wasm32-wasip2` target, so rustup installs both on first use.

```bash
git clone https://github.com/wasmkeep/wasmkeep
cd wasmkeep
cargo build --release -p wasmkeep-cli
```

The CLI is now at `target/release/wasmkeep`.

## 2. Create the actor

An actor is a Rust library compiled to a component. Create a directory for
it next to the examples (not with `cargo new`, which would add it to the
WasmKeep workspace; an actor is built for a different target and is its own
workspace):

```bash
mkdir -p examples/visits/src
```

Create `examples/visits/Cargo.toml`:

```toml
[package]
name = "visits"
version = "0.1.0"
edition = "2024"

# The actor builds for wasm32-wasip2, so it is its own workspace.
[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
wasmkeep-sdk = { path = "../../crates/wasmkeep-sdk" }

# Keeps the component small, as in every example.
[profile.release]
opt-level = "s"
strip = true
lto = true
codegen-units = 1
```

Create `examples/visits/src/lib.rs`:

```rust
use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::{Actor, Value, params, storage};

struct Visits;

impl Actor for Visits {
    // Runs once per actor (and again after you deploy a new version):
    // create your tables here.
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS visits (who TEXT NOT NULL)",
            &[],
        )
        .map(drop)
        .map_err(|e| e.to_string())
    }
}

impl http::Guest for Visits {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        // `/?who=alice` records a visit by alice.
        let path = request.path_with_query().unwrap_or_default();
        let who = path.split_once("who=").map_or("someone", |(_, w)| w);

        let result = storage::exec("INSERT INTO visits (who) VALUES (?1)", &params![who])
            .and_then(|_| storage::query("SELECT count(*) AS n FROM visits", &[]));
        match result {
            Ok(rows) => {
                let n = rows.get(0, "n").and_then(Value::as_i64).unwrap_or(0);
                let body = format!("hello {who}, you are visitor number {n}\n");
                http::respond(out, 200, "text/plain", body.as_bytes());
            }
            Err(e) => http::respond(out, 500, "text/plain", e.to_string().as_bytes()),
        }
    }
}

wasmkeep_sdk::export!(Visits);
```

Two traits, one macro:

- `Actor` holds the lifecycle hooks (`on_init`, `on_alarm`, and
  `on_websocket_open`, `on_websocket_message`, `on_websocket_close`). All
  are optional; by default an actor refuses WebSockets.
- `http::Guest` is the request handler (standard `wasi:http`).
- `export!` wires both into the component.

## 3. Build it

```bash
cd examples/visits
cargo build --release --target wasm32-wasip2
cd ../..
```

The component is `examples/visits/target/wasm32-wasip2/release/visits.wasm`,
close to the 81 KB of the Rust counter, which uses the same release
profile.

## 4. Serve it

```bash
target/release/wasmkeep serve \
  --actor visits=examples/visits/target/wasm32-wasip2/release/visits.wasm
```

Every actor lives at `/actor/<class>/<name>/...`. The class is `visits`; the
name is anything you like, and each name is a separate actor. In another
terminal:

```bash
curl 'localhost:8080/actor/visits/home?who=alice'
# hello alice, you are visitor number 1
curl 'localhost:8080/actor/visits/home?who=bob'
# hello bob, you are visitor number 2
curl 'localhost:8080/actor/visits/about?who=carol'
# hello carol, you are visitor number 1
```

`home` and `about` are two actors, each with its own SQLite database under
`.wasmkeep/data/actors/default/visits/`.

## 5. Restart it

Stop the server with Ctrl-C, start it again with the same command, and:

```bash
curl 'localhost:8080/actor/visits/home?who=dave'
# hello dave, you are visitor number 3
```

The count survived because it lives in the actor's database, not in memory.

## What just happened

- **One event at a time.** Requests to `home` run one after another, in
  arrival order, so `INSERT` then `SELECT` can never interleave with another
  request to `home`. You write no locks. Requests to different actors run in
  parallel.
- **Hibernation.** After 30 s without events an actor is dropped from memory;
  the next request brings it back in about a millisecond. Its database stays
  on disk.
- **Sandboxed.** The component can reach its own database, alarms, calls to
  other actors, WebSockets, outbound HTTP, clocks, randomness and its
  configuration (the `env` interface; the WASI environment stays empty),
  and nothing else: no files, no sockets. What it prints (`println!`) goes
  to the node's log.
- **Limits.** Each event gets 50 ms of CPU (and 30 s of wall time), and each
  of an actor's linear memories (at most two) is capped at 128 MiB, by
  default. A guest that loops forever is
  stopped and gets a 500; the server and other actors carry on.

## Next

- `target/release/wasmkeep dev examples/visits` rebuilds and reloads your actor
  every time you save, keeping its data.

- `examples/rate-limiter` uses **alarms**: code that runs later, at least once,
  even across restarts.
- `wasmkeep_sdk::actors::call` lets one actor call another.
- The design and its guarantees are in [Concepts](../concepts.md), with an
  [overview](../overview.md) of the architecture.
