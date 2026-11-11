# @wasmkeep/sdk: WasmKeep actors in TypeScript

Write [WasmKeep](../../README.md) actors in TypeScript (or JavaScript). An
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

## Using this checkout

The SDK is not published to npm yet: install it from a clone of this
repository. You need Node.js 22 or later (the version CI uses), npm, and a
Rust toolchain for the `wasmkeep` command.

```bash
# Once, in the clone: the SDK's build dependencies, and the wasmkeep binary
(cd sdks/typescript && npm ci)
cargo build --release -p wasmkeep-cli   # target/release/wasmkeep; put it on PATH

# In your actor's project: depend on the SDK by path
npm install /path/to/clone/sdks/typescript
```

The last command adds `"@wasmkeep/sdk": "file:..."` to your `package.json`
and links the directory, as the [TypeScript examples](../../examples/ts-counter)
do; `npm link` works too. The link provides the `wasmkeep-ts` command.

Then build the actor above into a component and serve it:

```bash
npx wasmkeep-ts build src/index.ts -o actor.wasm   # QuickJS; --engine starlingmonkey for Web APIs
wasmkeep serve --actor hits=actor.wasm   # or, from the clone: cargo run -p wasmkeep-cli -- serve ...
curl localhost:8080/actor/hits/alice
```

## API

| | |
| --- | --- |
| `storage.exec(sql, ...params)` | run a statement, return rows changed |
| `storage.execDurable(sql, ...params)` | same, returns once the write is durable (replicated in a cluster) |
| `storage.query(sql, ...params)` | rows as objects; integers beyond 2^53 come back as `bigint` |
| `storage.deleteSelf()` | delete this actor when the event ends |
| `alarms.set(at)`, `alarms.get()`, `alarms.clear()` | the actor's alarm (ms since epoch or a `Date`) |
| `env.get(name)` | the class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](../../docs/reference/api.md#interface-env)), or `undefined` |
| `actors.self()` | this actor's address: `{ tenant, class, name }` |
| `actors.call("class/name", { method, path, headers, body })` | call another actor, wherever it runs |
| `sockets.send(conn, data)`, `sockets.broadcast(data, except?)`, `sockets.close(conn)`, `sockets.connections()` | WebSockets held by the host, open across hibernation |
| `json(value, status?)` | a JSON response |
| `new kv.Client(class, shards)` | key-value over Rust shard actors: `get`, `getFresh`, `put`, `putWith`, `compareAndSet`, `delete`, `deleteIf`, `list`, `withReadCache` |
| `new queue.Client("class/name")` | a Rust queue actor: `send`, `sendDelayed`, `sendBatch`, `lease`, `ack`, `retry`, `settle`, `stats`, `deadLetters` |
| `queue.isDelivery(req)`, `queue.handleDelivery(req, handle)` | answer a queue's push delivery, one outcome per message |
| `http.fetch(url, { method, headers, body, timeoutMs, limit })` | outbound HTTP, synchronously: `{ status, headers, body, text(), json() }`; throws `http.FetchError` when there is no response, or when its body breaks before its end (`incomplete body`; a body longer than `limit`, 16 MiB by default, is cut to it without an error). Works on both engines (QuickJS has no `fetch`, and StarlingMonkey's is asynchronous) |
| `workflow.define(fn, config)`, `new workflow.Client(class)` | durable workflows: `wf.step`, `wf.sleep`, `wf.waitForEvent` ([guide](../../docs/workflows.md#typescript)) |

SQL errors throw `SqlError` (with `kind`), failed calls throw `CallError`.
A `fetch` handler returns a string, bytes, or `{ status, headers, body }`;
an exception becomes a 500.

## Key-value and queues

`kv` and `queue` are the clients of the Rust SDK's key-value and queue
patterns ([guide](../../docs/kv-and-queues.md#typescript),
[ADR 0015](../../docs/adr/0015-kv-and-queues-as-sdk-patterns.md)). Only
the client side is in TypeScript: the shard and queue classes are deployed
as the Rust components from [`examples/jobs`](../../examples/jobs)
(`examples/jobs/kv` as class `kv`, `examples/jobs/queue` as class `queue`,
or a Rust queue class of your own for other settings). The protocol and
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
`refused`, ...). [`examples/ts-jobs`](../../examples/ts-jobs) runs against
the Rust classes; the differences from the Rust API are listed in the
[guide](../../docs/kv-and-queues.md#typescript).

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
  ([what actors print](../../docs/monitoring.md#what-actors-print)).
- **StarlingMonkey**, with `--engine starlingmonkey`
  ([ComponentizeJS](https://github.com/bytecodealliance/ComponentizeJS)):
  those Web APIs, and faster on JavaScript-heavy code, for components of
  about 13 MB and about 4.6× the memory per resident actor. Its engine,
  SpiderMonkey, is MPL-2.0: a component built this way contains it, so
  distributing the component requires making SpiderMonkey's source
  available ([legal](../../docs/legal.md)).

Code that uses a global QuickJS lacks fails with an error that says to
build with `--engine starlingmonkey`: at build time if module code uses
it, otherwise from the handler that does. The first load of a component
on a node compiles it (cached afterwards). See
[TypeScript engines](../../docs/languages.md#typescript-on-quickjs) for the
measurements and the full list.

## Type-checking your actor

The package ships its TypeScript sources and the declarations of the host
interfaces, with no compiled `.d.ts`: `tsc` checks the SDK's sources with
your project's options. The options the SDK is written for are in
[`examples/ts-counter/tsconfig.json`](../../examples/ts-counter/tsconfig.json):
`"moduleResolution": "Bundler"`, the `ES2022` and `ESNext.Disposable`
libraries, and `"types": []`. Copy that file, add `typescript` to your
devDependencies (the SDK is checked with 5.9.3), and run
`npx tsc --noEmit`. `wasmkeep-ts build` does not type-check.

## Developing the SDK

`npm run sync` copies the WIT world from `crates/wasmkeep-wit` and regenerates
`types/`; CI fails if they differ from the committed copies. `npm run check`
type-checks the SDK and its tests. `npm test` runs the unit tests (the kv
and queue framing, the shard hash against values from the Rust SDK, and the
clients against fake actors) with node's test runner, bundled by esbuild;
no other dependency.

`scripts/pack-test.sh` checks the package as another project gets it: it
runs `npm pack`, installs the tarball into an empty directory outside the
repository with a copy of `examples/ts-counter`, builds the actor on both
engines and runs `tsc --noEmit`. CI runs it; nothing is published.

The package pins `@bytecodealliance/jco` and `esbuild` to exact versions
and ships no `npm-shrinkwrap.json`: what those two depend on is resolved
when the package is installed, within the ranges they declare.

The build dependencies currently override `@bytecodealliance/jco-std`'s
`minimatch` to 11.0.1 for
[GHSA-g3hv-75wg-m249](https://github.com/advisories/GHSA-g3hv-75wg-m249), while
keeping jco 1.41.0. Remove the override when jco's dependency tree includes a
patched minimatch. Do not apply the suggested jco downgrade from `npm audit fix`.

Run `npm ci` in this SDK directory before building the repository's linked
TypeScript examples. npm applies overrides only from the installation root:
projects installing a packaged SDK must put the same override in their own
root `package.json` until the upstream dependency is fixed:

```json
{
  "overrides": {
    "@bytecodealliance/jco-std": {
      "minimatch": "11.0.1"
    }
  }
}
```
