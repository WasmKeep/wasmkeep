# What is WasmKeep?

WasmKeep runs **actors**: small, long-lived, addressable programs that each own
their state. You write an actor once (a *class*, compiled to a WebAssembly
component) and WasmKeep runs as many *instances* of it as you have names.

```
/actor/chat/room-42      → the chat-room class, instance "room-42"
/actor/limit/api-key-7   → the rate-limiter class, instance "api-key-7"
```

The first request to a name creates the actor. There is no "create" call, no
registry and no database to provision.

## The guarantees

Each actor:

1. **Handles one event at a time, in arrival order.** HTTP requests, WebSocket
   messages and alarms queue up in the actor's mailbox. Inside an actor you
   never need a lock.
2. **Owns a private SQLite database.** Nothing else can read or write it.
   Committed writes survive restarts.
3. **Hibernates when idle.** Its code leaves memory after a timeout (30 s by
   default) and is brought back by the next event, in about a millisecond.
   Data, the pending alarm and open WebSockets all survive this.
4. **Is sandboxed and limited.** It can only use what the
   [`wasmkeep:actor` interface](../crates/wasmkeep-wit/wit/actor.wit) grants:
   its database, alarms, calls to other actors, WebSockets, outbound HTTP,
   clocks, randomness, and its configuration and secrets (the `env`
   interface). What it prints to stdout or stderr reaches the node's log.
   Each event gets a CPU budget (50 ms by default) and each actor a memory
   cap (128 MiB per linear memory, at most two, by default).

Different actors run in parallel, so a server's throughput comes from having
many actors, not from making one actor busy.

## What it is good for

Anything with *many independent pieces of state*, each touched by a modest
stream of events ([use cases](use-cases.md) compares this with stateless
servers, Postgres and Valkey, and walks through each pattern):

| Use case | One actor per… | Uses |
| --- | --- | --- |
| Chat, presence, collaborative editing | room or document | WebSockets, history in SQLite |
| Multiplayer game lobbies | match | WebSockets, alarms for timeouts |
| Rate limiting and quotas | API key or user | alarms to reset windows |
| Shopping carts, sessions, workflows | user or order | durable state, alarms for reminders |
| Per-user AI agents | user or conversation | durable memory, outbound HTTP |
| Device twins (IoT) | device | latest state, alarms for missed heartbeats |
| Customer plugins in a SaaS | customer or plugin | sandboxing, limits per actor |

## When not to use it

- **One big shared dataset** (analytics, reporting, search across all users):
  use a real database. Actors are islands; querying across all of them is
  slow by design.
- **A single very hot object**: one actor runs on one core at a time. Split
  the state (one actor per room, not one per app).
- **Stateless functions**: plain `wasi:http` components work in WasmKeep, but
  Spin or wasmCloud are built for that.
- **A global edge network**: WasmKeep is self-hosted; it does not route users to
  the nearest of hundreds of locations.
- **Production today**: it is pre-alpha. Clustering works and has been
  soaked against R2 and OVHcloud Object Storage with nodes killed under
  load ([measured](benchmarks.md#three-nodes-against-a-real-bucket)), but
  has not yet carried real users' traffic.

[WasmKeep and Cloudflare Workers](compared-with-workers.md) compares the
two in more detail: where each is the better choice, and why.

Next: [Concepts](concepts.md) for how actors behave, or
[your first actor](tutorials/first-actor.md) to try it.

## How it works, briefly

- **Runtime**: [Wasmtime](https://wasmtime.dev), with a pooling allocator and
  ahead-of-time compiled, cached components.
- **Interface**: a [WIT world](../crates/wasmkeep-wit/wit/actor.wit) on top of
  the standard `wasi:http/proxy`, so the request handler is ordinary WASI
  HTTP, plus WasmKeep's storage, alarms, actor calls and WebSockets.
- **Languages**: anything that builds a WASI 0.2 component. Rust,
  TypeScript, Python, C, Zig and Go (TinyGo) have SDKs and pass the same
  tests; D and MoonBit have experimental SDKs, and standard Go and C#
  each build a counter (see [languages](languages.md)).
- **Storage**: SQLite in WAL mode, one file per actor, sandboxed (no `ATTACH`,
  no extensions, no state-changing pragmas).
- **Clustering**: each actor's changes stream to an S3-compatible bucket;
  several nodes share the bucket and use it to decide, with conditional
  writes, which node owns each actor. If a node dies, another takes its
  actors over. No consensus cluster to run: if the bucket is up, WasmKeep is up.

Design details and the reasoning behind each decision are in
[`docs/adr`](adr/); [Concepts](concepts.md) and [Running a cluster](cluster.md)
describe how it behaves, and
[When a node dies, and how it scales](failover-and-scaling.md) says what
an application sees of it.
