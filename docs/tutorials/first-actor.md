# Your first actor in 10 minutes

You will write a small actor in Rust, build it into a WebAssembly component,
serve it with WasmKeep, and see its state survive a restart.

Applies to: WasmKeep `0.4.1` (pre-alpha). Commands assume Linux; on macOS,
build the server from source (step 1).

**The fast path**, from the release after `0.4.1`: `wasmkeep new` writes a
project that builds and runs as it is, with its `wasmkeep.toml`, a README,
a `.gitignore` and an `.env.local` example
([starting a project](#starting-a-project-with-wasmkeep-new) below):

```bash
wasmkeep new hits --lang rust && cd hits
rustup target add wasm32-wasip2
wasmkeep dev
```

This page builds an actor by hand instead, to show every part.

## 1. Get WasmKeep

Install the `wasmkeep` binary from a release, as in
[Install](../../README.md#install) (Linux, x86_64 or aarch64), and check it
answers:

```bash
wasmkeep --version
```

On macOS, or to run the server from source: `git clone
https://github.com/wasmkeep/wasmkeep`, then `cargo build --release -p
wasmkeep-cli` in it writes `target/release/wasmkeep`.

To build the actor you need [rustup](https://rustup.rs) with Rust 1.97 or
later, and the WebAssembly target:

```bash
rustup target add wasm32-wasip2
```

## 2. Create the actor

An actor is a Rust library compiled to a component. Create it anywhere
outside a clone of WasmKeep, and add the SDK from crates.io:

```bash
cargo new --lib visits && cd visits
cargo add wasmkeep-sdk
```

`cargo add` takes the newest version: the SDK carries the version of the
server it is released with, so keep the two the same
(`cargo add wasmkeep-sdk@0.4.1` names one). Add to `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

# Keeps the component small, as in every example.
[profile.release]
opt-level = "s"
strip = true
lto = true
codegen-units = 1
```

Replace `src/lib.rs` with:

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
cargo build --release --target wasm32-wasip2
```

The component is `target/wasm32-wasip2/release/visits.wasm`, close to the
81 KB of the Rust counter, which uses the same release profile.

## 4. Serve it

```bash
wasmkeep serve --actor visits=target/wasm32-wasip2/release/visits.wasm
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
`.wasmkeep/data/state/default/default/actors/visits/`, in the directory the server
started in.

## 5. Restart it

Stop the server with Ctrl-C, start it again with the same command, and:

```bash
curl 'localhost:8080/actor/visits/home?who=dave'
# hello dave, you are visitor number 3
```

The count survived because it lives in the actor's database, not in memory.

## Starting a project with `wasmkeep new`

From the release after `0.4.1`:

```bash
wasmkeep new DIR --lang rust|typescript [--front] [--app TENANT/APP]
```

It writes into `DIR` (created, or empty: anything else is refused) a class
`counter`, one durable counter per actor, and with `--front` a front over
it: a page and an API, the class then `internal`. Without a front the
class is `open`, at `/actor/counter/<name>`. `[app]` is `--app`, or the
tenant `my-team` and the directory's name: choose both before the first
deploy. The templates are in the binary, so it needs no network; the
README it writes has the commands (`rustup target add wasm32-wasip2` or
`npm install`, then `wasmkeep dev`, then `wasmkeep deploy`), checked by
`crates/wasmkeep-cli/tests/new_project.rs` for every variant.

The SDK is pinned to the version of the `wasmkeep` that wrote the project
(`wasmkeep-sdk = "0.5.0"`, `"@wasmkeep/sdk": "0.5.0"`, say), the version
every release publishes the SDKs at. A `wasmkeep` built from a clone pins
the clone's version: until that version is on crates.io and npm, build
against the clone's SDK instead (`<clone>` its absolute path), without
editing the project:

```toml
# .cargo/config.toml, in the project or any directory above it
[patch.crates-io]
wasmkeep-sdk = { path = "<clone>/crates/wasmkeep-sdk" }
```

and for TypeScript, `npm install <clone>/sdks/typescript` in
place of `npm install` (run `npm ci` in the clone's `sdks/typescript`
first). Other languages come later ([Languages](../languages.md)).

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

- [Your first app](first-app.md) (**0.4.0 and later**): a front that
  serves a page and an API over an internal class, run with `wasmkeep
  dev`, deployed to a local bucket and rolled back.
- [From one node to three](one-node-to-three.md): run three nodes on
  your machine, kill one and watch its actors carry on.
- `wasmkeep dev .` rebuilds and reloads your actor every time you save,
  keeping its data.
- [`examples/rate-limiter`](../../examples/rate-limiter) uses **alarms**:
  code that runs later, at least once, even across restarts.
- The release carries more to try without building:
  [key-value and queues](../kv-and-queues.md) and
  [workflows](../workflows.md), as classes your actor calls.
- `wasmkeep_sdk::actors::call` lets one actor call another.
- The design and its guarantees are in [Concepts](../concepts.md), with an
  [overview](../overview.md) of the architecture.
