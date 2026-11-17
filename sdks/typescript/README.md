# @wasmkeep/sdk: WasmKeep actors in TypeScript

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep) actors in TypeScript (or JavaScript). An
actor is a module whose default export is `defineActor({...})`:

```ts
import { defineActor, storage, alarms, sockets, actors, json } from "@wasmkeep/sdk";

export default defineActor({
  // Once per deployed version, before any other event: create tables here.
  init() {
    storage.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)");
  },

  // One HTTP request at a time, in arrival order: no locks needed.
  fetch(req) {
    storage.exec("INSERT INTO hits (at) VALUES (?)", Date.now());
    const [{ n }] = storage.query("SELECT count(*) AS n FROM hits");
    return json({ hits: n, path: req.path });
  },

  // The alarm set with alarms.set(...) fired (at least once).
  alarm() {},

  websocket: {
    open(conn, path) { sockets.send(conn, "welcome"); },
    message(conn, data) { sockets.broadcast(data); },
    close(conn, code, reason) {},
  },
});
```

## Install

```bash
npm install @wasmkeep/sdk
```

You need Node.js 22 or later (CI tests with 22) and npm. The package
carries the WIT files of the `wasmkeep:actor` world and provides the
`wasmkeep-ts` command, which builds the actor above into a component. To
run the result you need the `wasmkeep` server:
[install a release](https://github.com/WasmKeep/wasmkeep#install) of the
same version as the package.

Save the actor above as `src/index.ts`, then, in the project where the
package is installed:

```bash
npx wasmkeep-ts build src/index.ts -o actor.wasm   # QuickJS; --engine starlingmonkey for Web APIs
wasmkeep serve --actor hits=actor.wasm
curl localhost:8080/actor/hits/alice
```

The package has the version of the WasmKeep server it is released with. The
interface it binds to, `wasmkeep:actor@0.2.0`, is versioned on its own.
WasmKeep is pre-alpha.

In a clone of the repository, the
[TypeScript examples](https://github.com/WasmKeep/wasmkeep/tree/v0.5.0/examples/ts-counter)
depend on the SDK by path instead (`"@wasmkeep/sdk": "file:../../sdks/typescript"`,
after `npm ci` in `sdks/typescript`).

## API

| | |
| --- | --- |
| `storage.exec(sql, ...params)` | run a statement, return rows changed |
| `storage.execDurable(sql, ...params)` | same, returns once the write is durable (replicated in a cluster) |
| `storage.query(sql, ...params)` | rows as objects; integers beyond 2^53 come back as `bigint` |
| `storage.deleteSelf()` | delete this actor when the event ends |
| `storage.deleteSelfDurable()` | delete this actor now; returns once the delete is durable (throws `SqlError`) |
| `storage.flush()` | returns once every write committed so far is durable (throws `SqlError`) |
| `alarms.set(at)`, `alarms.get()`, `alarms.clear()` | the actor's alarm (ms since epoch or a `Date`) |
| `env.get(name)` | the class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/reference/api.md#interface-env)), or `undefined` |
| `actors.self()` | this actor's address: `{ tenant, class, name }` |
| `actors.call("class/name", { method, path, headers, body })` | call another actor, wherever it runs |
| `actors.callMany([["class/name", request], ...])` | up to 16 calls at once: one entry per call, in input order, a response or the `CallError` it failed with (returned, not thrown) ([ADR 0025](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/adr/0025-calling-several-actors-at-once.md)) |
| `actorClient()`, `defineOperation`, `codec` | typed operation contracts and a typed client over `actors.call` and `actors.callMany`, with run-time checks and a structured error envelope (`problem(status, code, message)`) ([below](#typed-actor-clients)) |
| `sockets.send(conn, data)`, `sockets.broadcast(data, except?)`, `sockets.close(conn)`, `sockets.connections()` | WebSockets held by the host, open across hibernation |
| `json(value, status?)` | a JSON response |
| `caller(req)` | who sent the request: `{ kind: "front" }`, `{ kind: "actor", address }` or `{ kind: "outside" }` (see [Writing a front](#writing-a-front)) |
| `host(req)`, `scheme(req)`, `unavailable(err)` | the request's authority, `"https"` or `"http"`, and the answer to a failed call (see [Writing a front](#writing-a-front)) |
| `new kv.Client(class, shards)` | key-value over Rust shard actors: `get`, `getFresh`, `put`, `putWith`, `compareAndSet`, `delete`, `deleteIf`, `list`, `withReadCache` |
| `new queue.Client("class/name")` | a Rust queue actor: `send`, `sendDelayed`, `sendBatch`, `lease`, `ack`, `retry`, `settle`, `stats`, `deadLetters` |
| `queue.isDelivery(req)`, `queue.handleDelivery(req, handle)` | answer a queue's push delivery, one outcome per message |
| `http.fetch(url, { method, headers, body, timeoutMs, limit })` | outbound HTTP, synchronously: `{ status, headers, body, text(), json() }`; throws `http.FetchError` when there is no response, or when its body breaks before its end (`incomplete body`; a body longer than `limit`, 16 MiB by default, is cut to it without an error). Works on both engines (QuickJS has no `fetch`, and StarlingMonkey's is asynchronous) |
| `http.fetchAll([{ url, ...options }, ...])` | several outbound requests at once, waited for together: one `{ ok: true, response }` or `{ ok: false, error }` per request, in input order ([below](#several-outbound-requests-at-once)) |
| `db.define({ migrations })`, `new db.Client(address)` | a database kept by one actor: the class's handler and the client (`query`, `exec`, `batch`, `read`) ([below](#a-database-kept-by-one-actor)) |
| `workflow.define(fn, config)`, `new workflow.Client(class)` | durable workflows: `wf.step`, `wf.sleep`, `wf.waitForEvent` ([guide](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/workflows.md#typescript)) |

SQL errors throw `SqlError` (with `kind`), failed calls throw `CallError`.
A `fetch` handler returns a string, bytes, or `{ status, headers, body }`;
an exception becomes a 500. `req.bytes()`, `req.text()` and `req.json()`
read the whole request body (up to the host's 16 MiB). `req.bytes(limit)`
and `req.text(limit)` return `undefined` for a body longer than `limit`
bytes, having read at most `limit + 1` of it: check a size this way, as a
chunked body has no `Content-Length` to check first
(`const body = req.bytes(1000); if (body === undefined) return { status: 413 };`).

## Typed actor clients

Describe an operation once, in a file both sides import, and call it with
types that are checked at run time:

```ts
import { codec, defineOperation, urlPath } from "@wasmkeep/sdk/contracts";
import { actorClient } from "@wasmkeep/sdk";

const addItem = defineOperation({
  name: "cart.addItem",
  method: "POST",
  path: (i) => urlPath`/items/${i.sku}`,
  input: codec.object({ sku: codec.string(), qty: codec.integer() }),
  output: codec.object({ count: codec.integer() }),
  errors: { "cart.out_of_stock": codec.object({ left: codec.integer() }) },
});

const r = actorClient().call("cart/alice", addItem, { sku: "A1", qty: 2 });
if (r.ok) r.value.count;
else if (r.failure.kind === "declared") r.failure.details.left;
```

`@wasmkeep/sdk/contracts` imports no host module, so a contract file loads in
an actor, a front, a test or plain Node. A call returns a result and never
throws for what the callee or the network did; `client.batch` sends several
at once over `callMany` and keeps order and the limit of 16. Actors can answer
failures with `problem(409, "code", "message", details)`, an opt-in envelope
the Rust SDK reads and writes too. The raw `actors.call` stays. See `docs/typed-clients.md` in the
repository (the typed clients guide).

## Several outbound requests at once

A handler is synchronous, so `http.fetch` waits for its answer, and
`Promise.all` over StarlingMonkey's own `fetch` cannot be awaited (the
handler returns before the answers come). `http.fetchAll` sends a batch
at once and waits for all of it, on both engines:

```ts
import { defineActor, http } from "@wasmkeep/sdk";

export default defineActor({
  fetch(req) {
    const results = http.fetchAll([
      { url: "https://a.example/price" },
      { url: "https://b.example/price", timeoutMs: 2000 },
    ]);
    return results.map((r) => (r.ok ? r.response.text() : r.error.message)).join("\n");
  },
});
```

Each result is what `http.fetch` would return or throw, in input order;
one failure does not fail the others. The requests are open together,
their responses and bodies read as they come, so the batch takes about as
long as its slowest request. Request bodies are written one after the
other. The host lets an actor have 16 requests open at once: past 16, the
rest start as earlier ones finish.

## Writing a front

A front is an app's stateless `wasi:http` component
([ADR 0023](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/adr/0023-the-front.md)):
it receives every request outside `/actor/` and `/_wasmkeep/`, routes,
signs users in, and calls the app's actors with `actors.call` as an inside
caller, so it reaches `internal` classes. It targets the
`wasmkeep:actor/front` world (`actors` and `env`, no storage, alarms or
WebSockets): import from `@wasmkeep/sdk/front`, whose default export is a
`defineFront({ fetch })`, and build with `--world front`.

```ts
import { actors, defineFront, unavailable } from "@wasmkeep/sdk/front";

export default defineFront({
  fetch(req) {
    try {
      return actors.call("todos/alice", { path: "/" }).text();
    } catch (e) {
      return unavailable(e);
    }
  },
});
```

```bash
npx wasmkeep-ts build src/index.ts --world front -o front.wasm
wasmkeep serve front.wasm --actor todos=todos.wasm --front-env front.env
```

`@wasmkeep/sdk/front` has `actors`, `env`, `CallError`, `json`, `http`
and the request helpers, which `@wasmkeep/sdk` exports too, for actors:

- `caller(req)`: from the `x-wasmkeep-caller` header the host sets. A
  client cannot set it: the host removes it from what clients send, and
  from what guests send on `actors.call`. Only the exact value
  `<tenant>/_front/main` is the front; any value the host never sends is
  `outside`. An actor answers its front only with
  `if (caller(req).kind !== "front") return { status: 403, body: "forbidden\n" }`,
  which keeps a sensitive class closed if it is ever deployed `open`.
- `host(req)`: the request's authority (`req.authority`), the client's
  `Host`; behind a proxy, only if the proxy passes it. The client chooses
  it, unvalidated: escape it where it goes (HTML, JSON).
- `scheme(req)`: `"https"` when the first `X-Forwarded-Proto` value is
  `https`, else `"http"`. A client can send that header too, so it is only
  as good as the proxy in front of the node, which must replace it: use it
  for links and redirects, never to decide what a request may do.
- `unavailable(err)`: `503` with `Retry-After: 1` for a `CallError` of kind
  `failed` or `timeout`, `500` for anything else, with a generic body (log
  the detail; do not send it to the client).

Sockets through the front (ADR 0027,
`docs/adr/0027-sockets-through-the-front.md`, from 0.4.1), from
`@wasmkeep/sdk/front` only:

- `isWebSocket(req)`: the host marked the request as a client's WebSocket
  upgrade (`x-wasmkeep-upgrade`, which a client cannot send).
- `handOff(cls, name, path)`: the answer that has the host open that
  socket on actor `cls/name` as an inside caller, so the class can be
  `internal`; its `websocket.open` receives `path`. Check the client's
  token and the name first: an `internal` room trusts what the path says.
  If the room refuses, the client gets a 403 with the room's error. The
  host strips the `x-wasmkeep-` range from every outbound `fetch` answer,
  so an upstream's answer passed on cannot hand a socket off.

```ts
if (req.path.startsWith("/rooms/") && isWebSocket(req)) {
  const user = signedIn(req); // your own check
  if (!user) return { status: 401 };
  return handOff("room", roomName, `/ws?user=${encodeURIComponent(user)}`);
}
```

A front's `fetch` that throws answers a generic 500, and the error goes to
the log. Validate any name taken from a request before using it in an
address: each new name is a new actor with its own database.
[`examples/ts-todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.5.0/examples/ts-todo-front)
is a front over the `todos` class of
[`examples/todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.5.0/examples/todo-front).

## Key-value and queues

`kv` and `queue` are the clients of the Rust SDK's key-value and queue
patterns ([guide](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/kv-and-queues.md#typescript),
[ADR 0015](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/adr/0015-kv-and-queues-as-sdk-patterns.md)). Only
the client side is in TypeScript: the shard and queue classes are deployed
as the Rust components from [`examples/jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.5.0/examples/jobs)
(`examples/jobs/kv` as class `kv`, `examples/jobs/queue` as class `queue`,
which a release carries already built as `kv.wasm` and `queue.wasm`, or a
Rust queue class of your own for other settings). The protocol and
the shard hash match the Rust clients byte for byte.

```ts
import { defineActor, kv, queue } from "@wasmkeep/sdk";

const store = new kv.Client("kv", 4).withReadCache(60_000);
const jobs = new queue.Client("queue/jobs");

export default defineActor({
  fetch(req) {
    if (queue.isDelivery(req)) {
      return queue.handleDelivery(req, (_queue, m) => {
        store.put(`job:${m.id}`, m.body);
        return "ack";                      // or "retry", or { retryAfterMs: 500 }
      });
    }
    const version = store.put("greeting", "hello");
    try {
      store.compareAndSet("greeting", version, "hi");
    } catch (e) {
      if (e instanceof kv.KvError && e.kind === "conflict") return { status: 409, body: "moved\n" };
      throw e;
    }
    return `job ${jobs.send(req.bytes())}\n`;
  },
});
```

Versions, ids and times are `number`s; failures throw `kv.KvError` or
`queue.QueueError` with a `kind` (`conflict`, `key-too-long`, `call`,
`refused`, ...). [`examples/ts-jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.5.0/examples/ts-jobs) runs against
the Rust classes; the differences from the Rust API are listed in the
[guide](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/kv-and-queues.md#typescript).

## A database kept by one actor

`db` makes one actor the owner of an app's shared tables, like a
Cloudflare D1 database
(the guide is `docs/database.md` in the repository, the design ADR 0029;
new in `0.4.2`).
The class is one call; deploy it `internal`:

```ts
import { db } from "@wasmkeep/sdk";

export default db.define({
  migrations: ["CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL)"],
});
```

Any other actor of the app sends it SQL, with every value bound:

```ts
import { db } from "@wasmkeep/sdk";

const main = new db.Client("db/main");
main.exec("INSERT INTO todos (text) VALUES (?1)", ["buy milk"]);         // rows changed, durable
const { columns, rows } = main.query("SELECT id, text FROM todos");      // rows: arrays of values
db.objects({ columns, rows });                                           // [{ id, text }, ...]
main.batch([                                                             // all kept, or none
  { sql: "DELETE FROM todos WHERE id = ?1", params: [1] },
  { sql: "SELECT COUNT(*) FROM todos", rows: true },
]);
```

**Never pass SQL text from a request to the client**: write it in code
and bind values as parameters. Each request is one transaction (a read is
always rolled back, a write commits durably), statements that control the
transaction are refused, and requests are capped (`limits`). Failures
throw `db.DbError` with a `kind` (`sql`, `not-allowed`, `too-large`,
`call`, `refused`). A safe integer binds as an integer, any other number
as a real (`{ tag: "real", val: 2 }` for a whole one); integers past 2^53
come back as `bigint`. The wire format is the Rust SDK's byte for byte:
either talks to the other
(`examples/ts-db` in the repository).
A TypeScript database spends more of the event's CPU budget encoding
answers than a Rust one; prefer the Rust class for large answers.

## How it works

`wasmkeep-ts build` bundles your code with esbuild and turns it into a
WebAssembly component (via `jco componentize`) with one of two JavaScript
engines, pre-initialized with your bundle:

- **QuickJS**, the default ([QuickJS-ng](https://github.com/quickjs-ng/quickjs)
  through componentize-qjs, shipped with the pinned jco): components of
  about 1.7 MB and about 0.75 MB of memory per resident actor, but few Web
  APIs: no `URL`, `crypto`, `fetch`, timers or streams. The SDK supplies
  UTF-8 `TextEncoder`/`TextDecoder` and a `console` writing to stdout and
  stderr, as on the other engine; the node logs both
  ([what actors print](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/monitoring.md#what-actors-print)).
- **StarlingMonkey**, with `--engine starlingmonkey`
  ([ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS)):
  those Web APIs, and faster on JavaScript-heavy code, for components of
  about 13 MB and about 4.6× the memory per resident actor. Its engine,
  SpiderMonkey, is MPL-2.0: a component built this way contains it, so
  distributing the component requires making SpiderMonkey's source
  available ([legal](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/legal.md)).

Code that uses a global QuickJS lacks fails with an error that says to
build with `--engine starlingmonkey`: at build time if module code uses
it, otherwise from the handler that does. The first load of a component
on a node compiles it (cached afterwards). See
[TypeScript engines](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/docs/languages.md#typescript-on-quickjs) for the
measurements and the full list.

## Type-checking your actor

The package ships its TypeScript sources and the declarations of the host
interfaces, with no compiled `.d.ts`: `tsc` checks the SDK's sources with
your project's options. The options the SDK is written for are in
[`examples/ts-counter/tsconfig.json`](https://github.com/WasmKeep/wasmkeep/blob/v0.5.0/examples/ts-counter/tsconfig.json):
`"moduleResolution": "Bundler"`, the `ES2022` and `ESNext.Disposable`
libraries, and `"types": []`. Copy that file, add `typescript` to your
devDependencies (the SDK is checked with 5.9.3), and run
`npx tsc --noEmit`. `wasmkeep-ts build` does not type-check.

## Developing the SDK

In a clone of the repository, not from the installed package: `npm run sync` copies the WIT world from `crates/wasmkeep-wit` and regenerates
`types/`; CI fails if they differ from the committed copies. `npm run check`
type-checks the SDK and its tests. `npm test` runs the unit tests (the kv
and queue framing, the shard hash against values from the Rust SDK, and the
clients against fake actors) with node's test runner, bundled by esbuild;
no other dependency.

`scripts/pack-test.sh` checks the package as another project gets it: it
runs `npm pack`, installs the tarball into an empty directory outside the
repository with a copy of `examples/ts-counter`, builds the actor on both
engines and runs `tsc --noEmit`. CI runs it; the script publishes nothing.

The package pins `@bytecodealliance/jco` and `esbuild` to exact versions
and ships no `npm-shrinkwrap.json`: what those two depend on is resolved
when the package is installed, within the ranges they declare.

The build dependencies currently override `@bytecodealliance/jco-std`'s
`minimatch` to 11.0.1 for
[GHSA-g3hv-75wg-m249](https://github.com/advisories/GHSA-g3hv-75wg-m249), while
keeping jco 1.41.0. Remove the override when jco's dependency tree includes a
patched minimatch. Do not apply the suggested jco downgrade from `npm audit fix`.

Run `npm ci` in this SDK directory before building the repository's linked
TypeScript examples. npm applies overrides only from the installation root,
so a project that installs the package does not get this one: the build
works without it, and `npm audit` reports the advisory there. To apply it
in your project until the upstream dependency is fixed, put the same
override in your root `package.json`:

```json
{
  "overrides": {
    "@bytecodealliance/jco-std": {
      "minimatch": "11.0.1"
    }
  }
}
```
