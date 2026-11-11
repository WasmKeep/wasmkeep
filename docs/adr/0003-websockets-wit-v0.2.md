# 0003. WebSockets in the actor world (`wasmkeep:actor@0.2.0`)

- Status: accepted
- Date: 2026-11-02
- Milestone: [M1](../../ROADMAP.md#m1-actors-on-one-node)
- Supersedes: nothing; extends [ADR 0002](0002-wasmkeep-actor-wit-v0.1.md), which deferred WebSockets

## Context

M1 calls for WebSockets in and out, with connections parked across
hibernation, and the chat-room example needs them. ADR 0002 deferred them
because an exported message handler without a send API is dead weight.

## Decision

Package version 0.2.0 adds, to `actor-core`:

- An imported `websockets` interface: `send(conn, message)`,
  `close(conn, code, reason)`, `connections()`, with `message` =
  `text(string) | binary(list<u8>)` and `send-error` =
  `unknown-connection | backpressure`.
- Three exports: `on-websocket-open(conn, path) -> result<_, string>`,
  `on-websocket-message(conn, message)`, `on-websocket-close(conn, code, reason)`.

Semantics:

- The **host** accepts a GET with `Upgrade: websocket` on the actor's path,
  answers 101, and asks the guest with `on-websocket-open`; an error closes the
  socket with 1008 and the error as reason. The guest never sees a socket, only
  a connection id.
- Every message and the close are **mailbox events**, serialized with the
  actor's requests and alarms, in arrival order per connection. The reader
  waits for each event before reading the next frame: that is the backpressure.
- Sockets are owned by the host, so they **stay open while the actor
  hibernates**; the next message (or any other event) wakes it. This is the
  "park on the host" option from the initial design's open questions; the
  alternative (close and ask clients to reconnect) is kept for actor
  migration in M2.
- A connection id is only valid in the actor it was opened on: `send`, `close`
  and `connections` are scoped to the calling actor.
- `send` never blocks the guest: 256 queued messages per connection, then
  `backpressure` (the message is dropped; the guest decides what to do).

## Why not `wasi:http` upgrades

`wasi:http` 0.2 has no upgrade or bidirectional streaming story, and
handling sockets inside the instance would pin it in memory, which defeats
hibernation.

## Consequences

- Breaking for guests at the binary level: components built for 0.1.0 lack
  the new exports and must be rebuilt. Source compatibility is kept by
  `wasmkeep-sdk`: the new `Actor` hooks have defaults (refuse the upgrade,
  ignore messages and closes), and every example rebuilt unchanged.
- Messages are text or binary; ping/pong is handled by the host. Close codes
  and reasons follow RFC 6455 (reasons truncated to 123 bytes).
- Migration note: rebuild your component against `wasmkeep-sdk` (or regenerate
  bindings against `wasmkeep:actor@0.2.0`) and implement the three hooks to
  accept WebSockets.
