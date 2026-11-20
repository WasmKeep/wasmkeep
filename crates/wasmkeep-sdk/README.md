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
[tutorial](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/tutorials/first-actor.md)
goes from an empty directory to an actor whose state survives a restart.

## What is in the crate

- `storage`: the actor's own SQLite database (`exec`, `query`, `params!`).
- `alarms`: one pending alarm per actor, delivered to `Actor::on_alarm`.
- `actors`: calls to other actors of the same app.
- `calls`: several of those calls at once (`calls::call_many`, at most 16,
  one result per call in input order), from actors and fronts
  ([ADR 0025](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/adr/0025-calling-several-actors-at-once.md)).
- `websockets`: connections to this actor, with the `Actor::on_websocket_*` hooks.
- `env`: configuration and secrets the operator gives the class (or the front).
- `caller`, `Caller`: who sent a request (the app's front, another actor,
  or a client), and the `front` module (below).
- `http`: the request handler, helpers to answer, and outbound requests
  (`http::fetch`, and `http::fetch_all` for several at once).
- `problem`: the structured error envelope an actor may answer a failure with
  (`Problem`, `problem::response`, `problem::classify`), the same bytes the
  TypeScript SDK's typed clients write and read
  (`docs/typed-clients.md` in the repository).
- `client` (feature `client`): typed clients for calls to other actors,
  `contract!` and `record!`; see below.
  It works in a front too.
- `kv`, `queue`, `workflow`: a key-value store, queues and durable workflows,
  each built from actors
  ([KV and queues](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/kv-and-queues.md),
  [workflows](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/workflows.md)).
- `db`: a database kept by one actor, like a Cloudflare D1 database: the
  class's handler and migrations, and the client the app's other actors
  use ([below](#a-database-kept-by-one-actor)).
- `wasi`: the WASI 0.2 bindings (the `wasip2` crate).

The [API reference](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/reference/api.md)
lists every host function with its name in each SDK.

## Typed clients

The `client` feature (no dependency) is the Rust side of the TypeScript
SDK's typed clients: declare an actor class's operations once with
`contract!`, and call them with typed inputs and answers. Every input and
answer is checked, and a call returns a value or a `Failure` of nine kinds
(`transport`, `declared`, `problem`, `invalid-details`,
`unsupported-envelope`, `malformed-envelope`, `http`, `invalid-response`,
`invalid-request`), never a panic.

```rust
use wasmkeep_sdk::client::with_query;
use wasmkeep_sdk::{contract, record};

record! { pub struct AddItem { pub sku: String, pub qty: i64 } }
record! { pub struct Count { pub count: i64 } }
record! { pub struct OutOfStock { pub left: i64 } }

contract! {
    pub mod cart {
        fn add_item(AddItem) -> Count
            = POST |i: &AddItem| with_query("/", &[("sku", &i.sku)]),
            errors AddItemError { OutOfStock("cart.out_of_stock"): OutOfStock };
    }
}

let reply = cart::Client::new("cart/alice").add_item(&AddItem { sku: "A1".into(), qty: 2 });
```

`client.batch()` sends several operations at once over `call_many`. See
`docs/typed-clients.md` in the repository for the rules and how Rust differs
from TypeScript.

## Several outbound requests at once

`http::fetch` waits for its answer, and the handler with it. To call
several services, `http::fetch_all` sends a batch at once and waits for
all of it: one result per request, in order, each what `http::fetch`
would give, so one failure does not fail the others.

```rust
use std::time::Duration;
use wasmkeep_sdk::http::{self, FetchRequest};

let urls = ["https://a.example/price", "https://b.example/price"];
let requests: Vec<_> = urls.iter().map(|u| FetchRequest::get(u)).collect();
for result in http::fetch_all(&requests, Duration::from_secs(10), 64 * 1024) {
    match result {
        Ok(r) => { /* r.status, r.body */ }
        Err(e) => { /* why it failed, as text */ }
    }
}
```

The requests are open together, their responses and bodies read as they
come, so the batch takes about as long as its slowest request.
`FetchRequest::new(method, url)` with `.headers(...)` and `.body(...)`
sends more than a GET; bodies are written one after the other. The host
lets an actor have 16 requests open at once: past 16, the rest start as
earlier ones finish.

Headers are ordered pairs whose names compare without case:
`wasmkeep_sdk::headers::Headers` has `get` (the first value), `get_all`
(every value in order), `append`, `set` (replaces all) and `remove`.
`http::request_headers(&request)` and `Fetched::headers` give one, and
`http::respond_headers` sends one, each `Set-Cookie` as its own header.

## A database kept by one actor

`db` makes one actor the owner of an app's shared tables
(the guide is `docs/database.md` in the repository, the design ADR 0029;
new in `0.4.2`).
The class mounts the handler and lists its schema; deploy it `internal`:

```rust
use wasmkeep_sdk::{db, http};

const MIGRATIONS: &[&str] = &["CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL)"];

struct Db;

impl wasmkeep_sdk::Actor for Db {
    fn on_init() -> Result<(), String> {
        db::migrate(MIGRATIONS).map(drop).map_err(|e| e.to_string())
    }
}

impl http::Guest for Db {
    fn handle(request: http::IncomingRequest, out: http::ResponseOutparam) {
        db::handle(request, out); // 403 for anyone but the app's actors and front
    }
}

wasmkeep_sdk::export!(Db);
```

Any other actor of the app sends it SQL, with every value bound:

```rust
use wasmkeep_sdk::db::{self, Statement};
use wasmkeep_sdk::params;

let main = db::Client::new("db/main");
main.exec("INSERT INTO todos (text) VALUES (?1)", &params!["buy milk"])?;   // rows changed, durable
let rows = main.query("SELECT id, text FROM todos ORDER BY id", &[])?;     // columns and values
main.batch(&[                                                               // all kept, or none
    Statement::exec("DELETE FROM todos WHERE id = ?1", &params![1]),
    Statement::query("SELECT COUNT(*) FROM todos", &[]),
])?;
```

**Never pass SQL text from a request to the client**: write it in code
and bind values as parameters. Each request is one transaction (a read is
always rolled back, a write commits durably), statements that control the
transaction are refused, and requests are capped (`db::Limits`). The
TypeScript SDK has the same handler and client, with the same wire
format: either talks to the other
(`examples/db` in the repository).

## Writing a front

A front is an app's stateless `wasi:http` component
([ADR 0023](https://github.com/WasmKeep/wasmkeep/blob/v0.6.0/docs/adr/0023-the-front.md)):
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
- `is_websocket(&request)` and `hand_off(class, name, path)`: sockets
  through the front (ADR 0027, `docs/adr/0027-sockets-through-the-front.md`,
  from 0.4.1). `is_websocket` is true when the host marked the request as
  a client's WebSocket upgrade (`x-wasmkeep-upgrade`, which a client
  cannot send). Answering `hand_off("room", name, "/ws?user=alice")` to
  it stops the front's instance and has the host open the socket on
  `room/<name>` as an inside caller, so the class can be `internal`; the
  room's `on-websocket-open` receives the path. Check the client's token
  and the name first: an `internal` room trusts what the path says, and
  only an `internal` room knows every socket came through the front. If
  the room refuses, the client gets a 403 with the room's error. The
  host strips the `x-wasmkeep-` range from every outbound HTTP answer
  (from 0.4.1), so an upstream's answer passed on cannot hand a socket
  off.
- `export_front!(Type)`: exports the request handler, and nothing else.

Validate any name you take from a request before using it in an address:
each new name is a new actor with its own database.
[`examples/todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.6.0/examples/todo-front)
is a front with a page, a JSON API, sign-in by token, and an `internal`
class that checks `caller()`.

Without the `actor` feature the crate has `front`, `actors`, `env`,
`caller`, `http` and `wasi`; `storage`, `alarms`, `websockets`, `Actor`,
`export!`, `params!`, `db`, `kv`, `queue` and `workflow` need it.

## Versions

The crate has the version of the WasmKeep server it is released with: use
the two at the same version (`cargo add wasmkeep-sdk@0.6.0` for server
0.5.0). It needs Rust 1.97 or later. The interface it binds to, `wasmkeep:actor@0.2.0`, is versioned on its own: the
WIT files are in `wit/`, a copy of the ones the server is built from.

## License

Apache-2.0; see `LICENSE` and `NOTICE`. WasmKeep is pre-alpha.

## Directory (ADR 0040, R1)

`directory::Directory` is library code exported by an app's own internal
class; custom classes can delegate to `directory::handler::{init, handle}`.
Each actor holds one map. `directory::Client` provides `create`, `lookup`,
`entries`, `change` (`Change::Split` or `Change::Merge`), and `delete`.
`directory::point` hashes key bytes with FNV-1a followed by fmix64.

The creator owns mutations; inside callers may read. Bounds are `u64`,
with `Entry::hi = None` for 2^64. Versions are positive SQLite integers;
conflicts return the current map version. Maps hold at most 4,096 ranges
and metadata is at most 1 KiB per range. Targets and nonempty operation ids
are capped at 1 KiB of UTF-8 each. A change commits once, durably; retry an
unknown outcome with the same op id **before submitting another change**.
Only the latest operation's result is retained. A map lookup is one actor
event and sees either side of a change, never a partial change.

This implements R1 of the proposed ADR only; broadcast groups and rooms
are not provided by this module.

### Room core (ADR 0040, R2)

`broadcast::Room` is an internal room actor; `broadcast::Client` is available
also without the `actor` feature for a stateless HTTP front. The front
handles authorization and maps its publish and `GET /history?after=e.n`
requests to the client's `publish` and `history` methods. The internal
`/wasmkeep-room/v1/` protocol uses the SDK's binary framing; positions retain
all 63 SQLite integer bits. History returns `lost`, `reset`, `more`, the head,
and messages with their individual positions. Continue a paginated read from
the last returned message's position.

Deploy the room with batched writes. The first operation after each cold
start, including history, records one durable epoch boundary so a reconnect
can learn of a loss before another publish. Publishes use plain writes and
set no alarm. The window holds 2,000 messages or 8 MiB of UTF-8 bodies;
history replies include framing within their 256 KiB bound. For
confirmation, delegate initialization to `broadcast::handler::init(true)`:
the room's pull then flushes first, and the pusher retries 50 ms later while
uploading.

Delivery is a fifth class since 0.7.0 (ADR 0040, section 13), deployed
internally as `pusher`: delegate its `on_alarm` to
`broadcast::pusher::handler::scaling(&mut scaling, &mut round)` (or
`::round` for fixed edges) and its `handle` to
`broadcast::pusher::handler::handle` (the poke). It pulls from
`room/<its name>` (`pusher::handler::room_class` changes the class).
`Client::publish` pokes the pusher only when the room's answer asks for it.
A room deployed with 0.6.0 moves its group to its pusher by itself (see the
changelog).
