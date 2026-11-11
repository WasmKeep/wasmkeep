# 0002. The `wasmkeep:actor` WIT world, v0.1.0

- Status: accepted; extended to v0.2 by [ADR 0003](0003-websockets-wit-v0.2.md) (0.x may still break).
  Deferred items since: `env` added 2026-11-04 (deploys and secrets
  in ADRs [0013](0013-access-rules-and-config-in-deploys.md) and
  [0016](0016-secrets-in-the-bucket.md)); guest stdout and stderr reach
  the node's log since 2026-11-06, not in M1 as planned (until
  then the host discarded them).
- Date: 2026-11-02
- Milestone: [M1](../../ROADMAP.md#m1-actors-on-one-node)
- Source: [`crates/wasmkeep-wit/wit/actor.wit`](../../crates/wasmkeep-wit/wit/actor.wit)

## Context

The initial design sketched the world (storage, alarms, actors.call, exports
`on-init`, `on-alarm`, `on-websocket-message`, and the `wasi:http` handler)
but left several types undefined (`value`, `row`, `request`, `response`, the
error types) and imported `wasi:logging`, which Wasmtime does not provide.
This ADR records how v0.1.0 fills those gaps. The interface is the product, so
every choice below favours "small now, additive later".

## Decisions

### Shape: two worlds

- `actor-core`: WasmKeep's own imports (`storage`, `alarms`, `actors`) and
  exports (`on-init`, `on-alarm`). Host bindings (`wasmkeep-wit`, wasmtime
  `bindgen!`) and guest bindings (`wasmkeep-sdk`, wit-bindgen) are generated
  from this world only.
- `actor`: `actor-core` + `wasi:http/proxy@0.2.12` + the `wasi:cli` imports
  that Rust's standard library always pulls in (environment, exit,
  terminal-*), served empty by the host. This is the world components are
  validated against (`wasm-tools component targets`, in CI and later in
  `wasmkeep deploy`), so it doubles as the capability list: filesystem and
  sockets are not in it.

Why: generating bindings for `actor` would duplicate the WASI types already
provided by `wasmtime-wasi-http` (host) and the `wasip2` crate (guest). With the
split, each side reuses the standard WASI bindings and adds only WasmKeep's.

### Storage

- `value` is a variant over SQLite's storage classes: `null`, `integer(s64)`,
  `real(f64)`, `text(string)`, `blob(list<u8>)`.
- `query` returns `rows { columns, values }`: column names once, then rows as
  lists of values. Smaller on the wire than a list of records with names.
- One statement per call (`multiple-statements` error otherwise): no
  accidental batches, no stacked-query injection.
- Autocommit per statement; explicit `BEGIN`...`COMMIT` across calls inside
  one event is allowed, and a transaction left open when the event ends is
  rolled back. (No implicit per-event transaction: it would hold the write
  lock across outbound calls and surprise anyone who catches an error.)
- `sql-error` is a record of an `enum` kind plus a message, so guests can
  match on the kind (`constraint`, `quota-exceeded`, ...) without parsing.
- `exec-durable` is in v0.1 even though replication arrives in M2, so guest
  code written now does not change then. On a node without a bucket it means
  "committed and fsynced locally". This settles the open question
  (per-call opt-in vs always durable) as per-call opt-in.

### Alarms

`set-alarm(at)`, `clear-alarm()` as first sketched, plus `get-alarm()`
(Durable Objects users expect it; a rate limiter needs it to avoid
re-scheduling). At-least-once delivery is stated in the interface docs.

### Actor-to-actor calls

`call(address, request) -> result<response, call-error>` with fully buffered
`request` / `response` records (method, path, headers, body). The callee
receives it through its normal `wasi:http/incoming-handler`, so there is no
second export to implement and every language can handle it. `address` is
`class/name` inside the caller's tenant; cross-tenant calls are impossible by
construction. `call-error` includes `reentrant`: with strict one-event-at-a-time
execution, calling yourself or any actor already waiting in the call chain
would deadlock, so the host refuses it up front.

### Lifecycle exports return results

`on-init` and `on-alarm` return `result<_, string>`. A failing migration must
fail the event instead of trapping; a failing alarm is retried.

## Deferred, with reasons

- **WebSockets** (`on-websocket-message`, accept/send/close): without a send
  API an exported message handler is dead weight, and adding an export later is
  a breaking change anyway. They arrive together in a later minor version
  ([ADR 0003](0003-websockets-wit-v0.2.md)).
- **`wasi:logging`**: Wasmtime has no host implementation. Guests write to
  stderr; the host will capture and tag it per actor (M1). Revisit if a
  standard logging interface lands in WASI.
- **`env` (config and secrets)**: no example needs it yet.
- **Actor identity** (`own address`): the request path carries the name; add
  an import when an example needs it.

## Consequences

- Guests implement two traits in Rust: `wasmkeep_sdk::Actor` (hooks, default
  no-ops) and `wasmkeep_sdk::http::Guest`, and call `wasmkeep_sdk::export!`.
- Every example is checked in CI against the `actor` world, so the WIT, the SDK
  and the examples cannot drift apart.
- The host must link only what the `actor` world lists. The server links
  all of WASI 0.2; restricting the linker to the world was planned for M1
  and is still open.
- Any change to `actor.wit` needs a superseding ADR once 0.1.0 is published,
  and an RFC issue once the project takes outside proposals (see
  [CONTRIBUTING.md](../../CONTRIBUTING.md#pull-requests)).
