# 0015. Key-value and queues as SDK patterns built on actors

- Status: accepted and implemented in the Rust SDK (2026-11-05;
  `crates/wasmkeep-sdk/src/kv.rs`, `queue.rs`, `examples/jobs`, tests in
  `crates/wasmkeep-server/tests/jobs.rs`). The client side in every other
  SDK (TypeScript, Python, Go, C, Zig) on 2026-11-06, tested against the
  Rust classes by `crates/wasmkeep-server/tests/jobs_langs.rs`.
- Date: 2026-11-05
- Milestone: [M4](../../ROADMAP.md#m4-operating-a-cluster)

## Context

Applications need data shared between actors (settings, a registry, a
cache) and work that must happen later and reliably (webhooks, fan-out).
An earlier plan designed both as platform primitives: a `kv` interface
over bucket objects with a per-node cache, and a native queue class with a
typed batch export. This decision ("SDK first, hybrid later") replaces
that for now: a stable API in the SDKs, backed by ordinary actors, so a primitive can
replace the backend later without changing application code. No new host
function, no WIT change.

Everything needed exists for one actor: its SQLite database
(`exec-durable` for acknowledged writes, ADR 0007), one alarm that survives
failover (ADR 0009), calls to other actors with a caller header the host
vouches for, and one event at a time per actor.

## Decision

### Key-value: sharded actors

Keys are spread over a fixed number of **shard actors** of one class,
`<class>/shard-0` .. `shard-<n-1>`, chosen by FNV-1a (64 bits) of the key
modulo `n`. Not one actor per key: shards hold many keys, so a listing
is `n` calls rather than one per key, and a million small keys are a few databases rather than a million.
(`n` equal to the number of keys would be one actor per key; nothing
stops it, nothing recommends it.)

Each shard has one table:

```sql
CREATE TABLE wasmkeep_kv (key BLOB PRIMARY KEY, value BLOB NOT NULL,
                         version INTEGER NOT NULL, expires_at INTEGER) WITHOUT ROWID;
```

- **get** reads the row; a row past `expires_at` reads as absent.
- **put** writes it with a new **version**, taken from a counter per shard
  (`wasmkeep_kv_meta.seq`), so versions are never reused, not even after a
  delete: a compare-and-set holding an old version always loses (no ABA).
- **compare-and-set** is `put` with an expectation: any, absent, or
  version `v`. The shard reads the current version and writes only if it
  matches, otherwise `409` (`kv::Error::Conflict`). No `UPDATE ... WHERE
  version = ?` race is possible: the shard runs one event at a time.
- **delete** takes the same expectations.
- **list(prefix, after, limit)** asks every shard and merges in byte
  order.
- **Durability**: writes return once durable (`exec-durable`) unless the
  caller asks for a local write (`PutOptions::local()`).
- **Expiry**: lazy on read, and the shard's alarm deletes expired rows in
  batches of 500, then sets itself for the next expiry.
- **Read cache for callers**: optional, in the caller's instance
  (`Client::with_read_cache(max_age_ms)`): reads of other actors' writes are
  stale up to `max_age_ms`; the caller's own writes through the client
  update it. It approximates the planned per-node cache, per actor instead of
  per node, and is lost with the instance (hibernation, a failed event, a
  deploy).

### Queues: one actor per queue

A queue is an actor with a table of messages:

```sql
CREATE TABLE wasmkeep_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, body BLOB NOT NULL,
                            enqueued_at INTEGER NOT NULL, visible_at INTEGER NOT NULL,
                            attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT);
CREATE TABLE wasmkeep_queue_dead (id INTEGER PRIMARY KEY, body BLOB NOT NULL, enqueued_at INTEGER NOT NULL,
                                 attempts INTEGER NOT NULL, failed_at INTEGER NOT NULL, reason TEXT NOT NULL);
```

- **enqueue** (`send`, `send_batch` up to 1,000) is one multi-row `INSERT`
  run with `exec-durable`: an acknowledged send is never lost (ADR 0007),
  and a batch is all or none. `AUTOINCREMENT`, so an id is never reused and
  a late ack cannot settle a newer message.
- **lease(max, visibility)** takes visible messages (`visible_at <= now`,
  oldest first), adds one to `attempts` and hides them until
  `now + visibility`. The pair `(id, attempt)` names the lease.
- **ack** deletes the row if `(id, attempt)` is still the current lease;
  **retry** makes it visible again after a delay (default: backoff from
  1 s doubling to 5 min). A stale lease (it ran out and the message was
  leased again) is ignored and reported as such.
- **Redelivery**: a message neither acked nor retried becomes visible when
  its lease runs out. A visible message that had `max_attempts` leases
  (default 5) moves to the dead-letter table, with the last error.
- **Push delivery, alarm-driven**: a queue configured with a consumer
  (`Config::consumer("class/name")`) sets its alarm to the earliest
  `visible_at`. The alarm leases one batch (default 10), sends it **in the
  body** of one `actors.call` to the consumer's
  `wasmkeep-queue/v1/deliver` path, and applies the outcomes in the reply
  (ack, or retry with a delay). A failed or refused call retries the whole
  batch with backoff; a message the reply does not mention waits out its
  visibility timeout. Then it sets the alarm again: at once if messages are
  ready, so batches follow one another, each its own event, and sends get
  their turn between them. A quiet queue has no alarm and costs nothing.
  The consumer never calls the queue back during a delivery: that call
  would re-enter the queue, which is waiting in the chain, and be refused.

Lease and settle writes use plain `exec`: losing one in a crash means a
message is delivered again, which at-least-once allows.

### Who may call

Shards and queues answer only requests carrying `x-wasmkeep-caller` (set by
the host for `actors.call`, stripped from outside requests): an outside
client gets `403`. `handle_allowing` narrows it to chosen callers. Serving
the classes with `--access CLASS=internal` (ADR 0010) refuses outsiders
before an actor is created; the docs recommend both.

### The stable part

The application-facing API is `kv::Client` (`get`, `get_fresh`, `put`,
`put_with`, `compare_and_set`, `delete`, `delete_if`, `list`) and
`queue::Client` (`send`, `send_delayed`, `send_batch`, `lease`, `ack`,
`retry`, `settle`, `stats`, `dead_letters`) plus `queue::handle_delivery`
on the consumer side. Nothing in those signatures names an actor's
protocol: a client is built from a class name (KV) or an address (queue).

The protocol between client and actor (paths under `wasmkeep-kv/v1` and
`wasmkeep-queue/v1`, a length-prefixed binary framing) is internal to the
SDK and versioned in its path. If a primitive is added later (a native `kv`
interface, native queues), the clients switch to it behind the same calls;
a class name or address keeps naming the data, and a migration copies the
shards' or queues' tables once.

### Limits, stated plainly

- **Throughput per shard and per queue**: one event at a time, so about
  one durable write per bucket round trip (the design targets: under 100 ms
  on local MinIO, under 250 ms on a remote bucket, so roughly 4 to 10
  durable writes per second; much more on one node without a bucket).
  Local KV writes, batched sends and more shards or queues multiply it.
  Not measured yet; no benchmark is claimed.
- **One hop per operation**: every operation is an `actors.call`, which
  holds the caller's event while it runs (and is forwarded when the shard
  lives on another node).
- **No transactions across keys** or shards, and none between a KV write or
  a send and the caller's own database: the caller's event may write its
  own tables, call a shard, and trap after; the shard's write stays. Treat
  them as side effects, like outbound HTTP.
- **A shard or queue cannot use KV or queues of a class already in its call
  chain** (re-entry is refused).
- **The shard count is part of the data layout**: changing it moves keys,
  so it needs a migration (read every shard, write through the new count).
- **Failover**: a shard or queue is one actor, so it is unavailable for a
  failover window when its node dies: 15 to about 21 s after the node's
  last write with the default timings (ADR 0020), then the restore.
  Pending alarms (push deliveries, expiry) fire a lease period plus grace
  after they were due, as for any actor.
- **Ordering**: by visibility time, then id. FIFO only with one consumer,
  batches of one and no retries.
- **Push holds the queue**: while the consumer runs a batch, sends to that
  queue wait (the queue's event is in progress). Keep consumers short, or
  use pull.
- **Lost replies are ambiguous**: a `Call` error on a write or a send
  means it may have applied. Re-read before retrying a compare-and-set;
  a send retried after a lost reply may queue the message twice.

### Not done here

- Shard and queue classes in languages other than Rust. Every SDK has the
  client side (`sdks/typescript`, `sdks/python`, `sdks/go`, `sdks/c`,
  `sdks/zig`, with an `examples/<lang>-jobs` app each, 2026-11-06); the
  shard and queue classes stay the Rust components, which any caller
  deploys as they are.
- A dedupe window on send, and a helper for consumers to record handled
  ids. Applications do it in their own table today.
- No `sim/` case: nothing in the host, leases, replication or deploys
  changes. The behaviour rests on guarantees the simulations already cover
  (durable writes, single ownership, alarms after failover).

## Consequences

- KV and queues ship now, with no WIT, host or wire change between nodes,
  and inherit replication, failover and the access rules from actors.
- Their limits are an actor's limits, and the docs say so. Native key-value and
  queue interfaces stay on the [roadmap](../../ROADMAP.md) as the designs
  for primitives, to start if users hit those limits.
- `exec-durable` cannot end an explicit transaction on a single node (it
  changes `PRAGMA synchronous`, which SQLite refuses inside a transaction).
  The helpers make each durable write one statement instead. (Since lifted:
  `exec-durable("COMMIT")` now ends a transaction durably on both a single
  node and a cluster. The helpers keep their single statements.)
