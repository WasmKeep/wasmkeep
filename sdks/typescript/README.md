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
[TypeScript examples](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/ts-counter)
depend on the SDK by path instead (`"@wasmkeep/sdk": "file:../../sdks/typescript"`,
after `npm ci` in `sdks/typescript`).

## API

| | |
| --- | --- |
| `storage.exec(sql, ...params)` | run a statement, return rows changed |
| `storage.execDurable(sql, ...params)` | same, returns once the write is durable (replicated in a cluster) |
| `storage.query(sql, ...params)` | rows as objects; integers beyond 2^53 come back as `bigint` |
| `storage.deleteSelf()` | delete this actor when the event ends |
| `alarms.set(at)`, `alarms.get()`, `alarms.clear()` | the actor's alarm (ms since epoch or a `Date`) |
| `env.get(name)` | the class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/reference/api.md#interface-env)), or `undefined` |
| `actors.self()` | this actor's address: `{ tenant, class, name }` |
| `actors.call("class/name", { method, path, headers, body })` | call another actor, wherever it runs |
| `sockets.send(conn, data)`, `sockets.broadcast(data, except?)`, `sockets.close(conn)`, `sockets.connections()` | WebSockets held by the host, open across hibernation |
| `json(value, status?)` | a JSON response |
| `caller(req)` | who sent the request: `{ kind: "front" }`, `{ kind: "actor", address }` or `{ kind: "outside" }` (see [Writing a front](#writing-a-front)) |
| `host(req)`, `scheme(req)`, `unavailable(err)` | the request's authority, `"https"` or `"http"`, and the answer to a failed call (see [Writing a front](#writing-a-front)) |
| `new kv.Client(class, shards)` | key-value over Rust shard actors: `get`, `getFresh`, `put`, `putWith`, `compareAndSet`, `delete`, `deleteIf`, `list`, `withReadCache` |
| `new queue.Client("class/name")` | a Rust queue actor: `send`, `sendDelayed`, `sendBatch`, `lease`, `ack`, `retry`, `settle`, `stats`, `deadLetters` |
| `queue.isDelivery(req)`, `queue.handleDelivery(req, handle)` | answer a queue's push delivery, one outcome per message |
| `http.fetch(url, { method, headers, body, timeoutMs, limit })` | outbound HTTP, synchronously: `{ status, headers, body, text(), json() }`; throws `http.FetchError` when there is no response, or when its body breaks before its end (`incomplete body`; a body longer than `limit`, 16 MiB by default, is cut to it without an error). Works on both engines (QuickJS has no `fetch`, and StarlingMonkey's is asynchronous) |
| `workflow.define(fn, config)`, `new workflow.Client(class)` | durable workflows: `wf.step`, `wf.sleep`, `wf.waitForEvent` ([guide](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/workflows.md#typescript)) |

SQL errors throw `SqlError` (with `kind`), failed calls throw `CallError`.
A `fetch` handler returns a string, bytes, or `{ status, headers, body }`;
an exception becomes a 500.

## Writing a front

A front is an app's stateless `wasi:http` component
([ADR 0023](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/adr/0023-the-front.md)):
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

A front's `fetch` that throws answers a generic 500, and the error goes to
the log. Validate any name taken from a request before using it in an
address: each new name is a new actor with its own database.
[`examples/ts-todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/ts-todo-front)
is a front over the `todos` class of
[`examples/todo-front`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/todo-front).

## Key-value and queues

`kv` and `queue` are the clients of the Rust SDK's key-value and queue
patterns ([guide](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/kv-and-queues.md#typescript),
[ADR 0015](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/adr/0015-kv-and-queues-as-sdk-patterns.md)). Only
the client side is in TypeScript: the shard and queue classes are deployed
as the Rust components from [`examples/jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/jobs)
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
`refused`, ...). [`examples/ts-jobs`](https://github.com/WasmKeep/wasmkeep/tree/v0.3.0/examples/ts-jobs) runs against
the Rust classes; the differences from the Rust API are listed in the
[guide](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/kv-and-queues.md#typescript).

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
  ([what actors print](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/monitoring.md#what-actors-print)).
- **StarlingMonkey**, with `--engine starlingmonkey`
  ([ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS)):
  those Web APIs, and faster on JavaScript-heavy code, for components of
  about 13 MB and about 4.6× the memory per resident actor. Its engine,
  SpiderMonkey, is MPL-2.0: a component built this way contains it, so
  distributing the component requires making SpiderMonkey's source
  available ([legal](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/legal.md)).

Code that uses a global QuickJS lacks fails with an error that says to
build with `--engine starlingmonkey`: at build time if module code uses
it, otherwise from the handler that does. The first load of a component
on a node compiles it (cached afterwards). See
[TypeScript engines](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/docs/languages.md#typescript-on-quickjs) for the
measurements and the full list.

## Type-checking your actor

The package ships its TypeScript sources and the declarations of the host
interfaces, with no compiled `.d.ts`: `tsc` checks the SDK's sources with
your project's options. The options the SDK is written for are in
[`examples/ts-counter/tsconfig.json`](https://github.com/WasmKeep/wasmkeep/blob/v0.3.0/examples/ts-counter/tsconfig.json):
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
