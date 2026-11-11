# Key-value and queues

WasmKeep has no KV store or queue built into the platform. The Rust SDK
builds both from actors: a **KV shard** and a **queue** are ordinary
actors with an SQLite table, reached with `actors.call`. They get what
every actor gets (replication, failover, alarms, access rules) and have the
same limits. Why, and what a platform primitive would change, is in
[ADR 0015](adr/0015-kv-and-queues-as-sdk-patterns.md). A working app:
[`examples/jobs`](../examples/jobs).

The shard and queue classes are written with the Rust SDK. Callers can
be Rust or any other SDK: the [TypeScript](#typescript),
[Python](#python), [Go](#go), [C](#c) and [Zig](#zig) SDKs have the same
clients and talk to the same Rust classes.

Those callers need no Rust toolchain: a release carries the two classes
of [`examples/jobs`](../examples/jobs) already built, as `kv.wasm` (the
ready-made shard) and `queue.wasm` (its queue class: three attempts, a
short backoff, pull-only except `queue/jobs`, which pushes to
`app/worker`), signed like its other files, with the notices of the
libraries compiled into them in `THIRD_PARTY_NOTICES-components.txt`.
[The example's README](../examples/jobs/README.md#run-it) builds them from
a clone instead.

```bash
# $v: the version of your wasmkeep binary
curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v$v/kv.wasm"
curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v$v/queue.wasm"
wasmkeep serve --actor kv=kv.wasm --actor queue=queue.wasm --actor app=your-app.wasm \
  --access kv=internal --access queue=internal
```

## Key-value

Deploy a shard class: a component that exports the SDK's ready-made shard.

```rust
use wasmkeep_sdk::kv::Shard;
wasmkeep_sdk::export!(Shard);
```

Serve it as class `kv` (`--actor kv=...`, plus `--access kv=internal`), and
use it from any other actor:

```rust
use wasmkeep_sdk::kv::{self, PutOptions};

let kv = kv::Client::new("kv", 8);          // 8 shards: kv/shard-0 .. kv/shard-7
let v1 = kv.put("user:42", b"Ada")?;         // the new version, once durable
let entry = kv.get("user:42")?;             // Some(Entry { value, version, expires_at })
kv.compare_and_set("user:42", Some(v1), b"Ada L.")?;   // Err(Conflict) if it moved
kv.compare_and_set("user:43", None, b"Bob")?;          // only if absent
kv.put_with("session:9", b"...", PutOptions { ttl_ms: Some(60_000), ..PutOptions::default() })?;
kv.delete("user:43")?;
let page = kv.list("user:", None, 100)?;    // keys with that prefix, in byte order
```

- **Keys** are bytes, up to 1 KiB (`&str` works). **Values** up to what one
  call carries (16 MB; keep them small).
- **Shards**: a key lives in shard `fnv1a(key) % n`. Every caller must use
  the same `n`, and changing it needs a migration, so pick it generously
  (shards cost nothing while idle).
- **Versions** change on every write and are never reused within a shard,
  even after a delete.
- **Expiry**: an expired key reads as absent; the shard's alarm deletes it.
- **Read cache**: `Client::with_read_cache(ms)` serves `get` from memory
  in the caller's instance for up to `ms`; the caller's own writes through
  that client update it; `get_fresh` skips it. It is lost with the
  instance (hibernation, a failed event, a deploy).
- For a shard with code of its own (extra checks, other tables), call the
  functions in `kv::shard` from your handler instead of exporting `Shard`.

## Queues

Deploy a queue class. Its settings are code, and may depend on the queue's
name:

```rust
use wasmkeep_sdk::queue::{self, Config};
use wasmkeep_sdk::{actors, http};

struct Queue;

fn config() -> Config {
    match actors::self_address().name.as_str() {
        "emails" => Config::new().consumer("mailer/main"),  // push
        _ => Config::new(),                                 // pull only
    }
}

impl wasmkeep_sdk::Actor for Queue {
    fn on_init() -> Result<(), String> {
        queue::store::init().map_err(|e| e.to_string())
    }
    fn on_alarm() -> Result<(), String> {
        queue::store::on_alarm(&config()).map_err(|e| e.to_string())
    }
}

impl http::Guest for Queue {
    fn handle(request: http::IncomingRequest, out: http::ResponseOutparam) {
        queue::store::handle(&config(), request, out);
    }
}

wasmkeep_sdk::export!(Queue);
```

Producers send to it:

```rust
let emails = wasmkeep_sdk::queue::Client::new("queue/emails");
let id = emails.send(b"welcome ada@example.com")?;     // once durable
emails.send_delayed(b"reminder", 3_600_000)?;
emails.send_batch(&[b"a".as_slice(), b"b"])?;         // one write, all or none
```

**Push** (a queue with a consumer): its alarm delivers batches to the
consumer, which answers one outcome per message:

```rust
use wasmkeep_sdk::queue::{self, Outcome};

// In the consumer's handler:
if queue::is_delivery(&request) {
    queue::handle_delivery(request, out, |_queue, message| {
        match send_email(&message.body) {
            Ok(()) => Outcome::Ack,
            Err(_) => Outcome::Retry(None),   // the queue's backoff
        }
    });
    return;
}
```

**Pull** (any queue): lease, then ack or retry.

```rust
let inbox = wasmkeep_sdk::queue::Client::new("queue/inbox");
for message in inbox.lease(10, 30_000)? {     // up to 10, hidden for 30 s
    handle(&message.body);
    inbox.ack(&message)?;                     // false if the lease ran out
}
```

`Config` sets the batch size (10), the visibility timeout (30 s), the
attempts before a message is dead-lettered (5) and the retry backoff (1 s
doubling to 5 min). `Client::stats` counts ready, delayed, in-flight and
dead messages; `Client::dead_letters` lists the dead ones with the reason.

## TypeScript

The TypeScript SDK (`@wasmkeep/sdk`) has the **client side** only: the
`kv` client, the `queue` client and `queue.handleDelivery` for push
consumers. The shard and queue classes stay Rust: deploy them as the
components from [`examples/jobs`](../examples/jobs), `kv`
([`examples/jobs/kv`](../examples/jobs/kv), the SDK's ready-made shard)
and `queue` ([`examples/jobs/queue`](../examples/jobs/queue), or your own
Rust queue class when you need other settings), built from a clone or
[downloaded from a release](#key-value-and-queues), and call them from a
TypeScript actor. The wire protocol, the shard hash and the status codes
are the same byte for byte in every SDK, so callers in any language share
the same data. [`examples/ts-jobs`](../examples/ts-jobs) is the jobs app in
TypeScript.

```ts
import { defineActor, kv, queue } from "@wasmkeep/sdk";

const store = new kv.Client("kv", 8);             // the same shard count as every other caller
const emails = new queue.Client("queue/emails");
const inbox = new queue.Client("queue/inbox");

export default defineActor({
  fetch(req) {
    // A push consumer: one outcome per message.
    if (queue.isDelivery(req)) {
      return queue.handleDelivery(req, (_queue, message) => "ack"); // or "retry", or { retryAfterMs: 500 }
    }

    // Calls to shards and queues run in an event, never at module level.
    const v1 = store.put("user:42", "Ada");           // the new version, once durable
    const entry = store.get("user:42");               // { value: Uint8Array, version, expiresAt? } or undefined
    store.compareAndSet("user:42", v1, "Ada L.");     // throws KvError (kind "conflict") if it moved
    store.compareAndSet("user:43", undefined, "Bob"); // only if absent
    store.putWith("session:9", "...", { ttlMs: 60_000, durable: true });
    store.deleteIf("user:43", 7);                     // only at that version
    const page = store.list("user:", undefined, 100); // Uint8Array keys, in byte order

    const id = emails.send("welcome ada@example.com");
    emails.sendDelayed("reminder", 3_600_000);
    emails.sendBatch(["a", "b"]);                     // one write, all or none

    for (const message of inbox.lease(10, 30_000)) {
      inbox.ack(message);                             // false if the lease ran out
    }
    return "ok\n";
  },
});
```

The names and semantics follow the Rust ones (`getFresh`,
`withReadCache(ms)`, `stats()`, `deadLetters(limit)`, `settle(items)`),
with these differences:

- **Numbers, not 64-bit integers.** Versions, ids, times and counts are
  `number`s. The wire still carries 64-bit integers; a version or id past
  2^53 - 1 (which a shard or queue does not reach in practice) makes the
  reply malformed (`KvError`/`QueueError` kind `refused`, status 200)
  instead of being rounded.
- **Errors are exceptions**: `KvError` (`kind`: `conflict`,
  `key-too-long`, `storage`, `call`, `refused`, with `status` for
  `refused`) and `QueueError` (`storage`, `call`, `refused`), with the
  Rust messages. A negative or fractional count, delay or limit throws a
  `RangeError` before any call.
- **Keys and values** are strings (sent as UTF-8, so the same key reaches
  the same shard from Rust) or `Uint8Array`s; values come back as
  `Uint8Array`.
- **A throwing delivery handler** makes `fetch` answer 500: the queue
  retries the whole batch with its backoff.

## Python

The Python SDK (`sdks/python`, module `wasmkeep`) has the same **client
side**: `kv`, `queue` and `queue.handle_delivery`, talking to the same Rust
shard and queue classes (deploy them as in [TypeScript](#typescript)) with
the same protocol, hash and statuses. [`examples/py-jobs`](../examples/py-jobs)
is the jobs app in Python.

```python
from wasmkeep import Actor, kv, queue

store = kv.Client("kv", 8)                       # the same shard count as every other caller
emails = queue.Client("queue/emails")
inbox = queue.Client("queue/inbox")


class App(Actor):
    def fetch(self, req):
        # A push consumer: one outcome per message.
        if queue.is_delivery(req):
            return queue.handle_delivery(req, lambda _queue, m: "ack")  # or "retry", or queue.RetryAfter(500)

        # Calls to shards and queues run in an event, never at module level.
        v1 = store.put("user:42", "Ada")                 # the new version, once durable
        entry = store.get("user:42")                     # Entry(value=b"Ada", version, expires_at) or None
        store.compare_and_set("user:42", v1, "Ada L.")   # raises KvError (kind "conflict") if it moved
        store.compare_and_set("user:43", None, "Bob")    # only if absent
        store.put_with("session:9", "...", ttl_ms=60_000, durable=True)  # expect="any" | "absent" | a version
        store.delete_if("user:43", 7)                    # only at that version
        page = store.list("user:", None, 100)            # bytes keys, in byte order

        message_id = emails.send("welcome ada@example.com")
        emails.send_delayed("reminder", 3_600_000)
        emails.send_batch(["a", "b"])                    # one write, all or none

        for message in inbox.lease(10, 30_000):
            inbox.ack(message)                           # False if the lease ran out
        return "ok\n"
```

The names and semantics follow the Rust ones (`get_fresh`,
`with_read_cache(ms)`, `stats()`, `dead_letters(limit)`, `settle(items)`),
with these differences:

- **Integers are exact.** Versions, ids, times and counts are Python
  `int`s, so nothing is rounded. Since an `int` has no bounds, a count,
  delay, limit or version outside what Rust's `u32` or `u64` would hold
  raises `ValueError` before any call.
- **Errors are exceptions**: `KvError` (`kind`: `conflict`,
  `key-too-long`, `storage`, `call`, `refused`, with `status` for
  `refused`) and `QueueError` (`storage`, `call`, `refused`), with the
  Rust messages as `str(e)`.
- **Options are keyword arguments**: `put_with(key, value, expect=,
  ttl_ms=, durable=)`, where `expect` is `"any"`, `"absent"` or a version.
  `list(prefix, after, limit)` defaults to every key (`b""`), from the start,
  100 at a time.
- **Outcomes** are `"ack"`, `"retry"` or `queue.RetryAfter(ms)`; records
  (`Entry`, `Message`, `Settlement`, `Stats`, `DeadLetter`) are frozen
  dataclasses with snake_case fields (`expires_at`, `enqueued_at`,
  `in_flight`, `failed_at`).
- **Keys and values** are `str` (sent as UTF-8, so the same key reaches the
  same shard from Rust) or `bytes`; values come back as `bytes`.
- **A raising delivery handler** makes `fetch` answer 500: the queue
  retries the whole batch with its backoff.

## Go

The [Go SDK](../sdks/go) (for TinyGo) has the **client side**, like
TypeScript: the `kv` client, the `queue` client and
`queue.HandleDelivery` for push consumers, talking to the Rust shard and
queue classes from [`examples/jobs`](../examples/jobs) with the same
protocol, hash and status codes. The clients send through a `call.Caller`:
`wasmkeep.Caller`, the SDK's own call to another actor
([`examples/go-jobs`](../examples/go-jobs) is the jobs app in Go).

```go
import (
	"errors"

	"github.com/wasmkeep/wasmkeep/sdks/go/kv"
	"github.com/wasmkeep/wasmkeep/sdks/go/queue"
	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
)

store := kv.New("kv", 8, wasmkeep.Caller)
v1, err := store.Put("user:42", []byte("Ada"))      // the new version, once durable
entry, err := store.Get("user:42")                  // *kv.Entry, nil if absent
_, err = store.CompareAndSet("user:42", kv.Version(v1), []byte("Ada L."))
if errors.Is(err, kv.ErrConflict) { /* it moved */ }
_, err = store.CompareAndSet("user:43", kv.Absent(), []byte("Bob"))
_, err = store.PutWith("session:9", []byte("..."), kv.PutOptions{TTLMs: 60_000})
keys, err := store.List("user:", 100)               // in byte order; ListAfter pages

emails := queue.New("queue/emails", wasmkeep.Caller)
id, err := emails.Send([]byte("welcome ada@example.com"))
ids, err := emails.SendBatch([][]byte{[]byte("a"), []byte("b")})

inbox := queue.New("queue/inbox", wasmkeep.Caller)
messages, err := inbox.Lease(10, 30_000)
for _, m := range messages {
	acked, err := inbox.Ack(m)                      // false if the lease ran out
}

// A push consumer, in the Fetch hook:
if queue.IsDelivery(req.Method, req.Path) {
	reply := queue.HandleDelivery(queue.CallerFrom(req.Headers), req.Body(),
		func(_ string, m queue.Message) queue.Outcome { return queue.Ack() }) // or queue.Retry(), queue.RetryAfter(500)
	return wasmkeep.Bytes(reply.Status, reply.ContentType, reply.Body)
}
```

The names and semantics follow the Rust ones (`GetFresh`,
`WithReadCache(ms)`, `Retry`, `RetryAfter`, `Settle`, `Stats`,
`DeadLetters(limit)`), with these differences:

- **Errors are values**: `*kv.Error` (`Kind`: `Conflict`, `KeyTooLong`,
  `Storage`, `Call`, `Refused`, with `Status` for `Refused`) and
  `*queue.Error` (`Storage`, `Call`, `Refused`), with the Rust messages.
  `errors.Is(err, kv.ErrConflict)` matches by kind; `errors.As` gives the
  fields.
- **Zero values are the defaults.** `kv.PutOptions{}` is a durable,
  unconditional write without expiry: `Local: true` asks for a local
  write (Rust's `PutOptions::local()`), and `TTLMs: 0` means no expiry (so
  a zero TTL cannot be asked for). The zero `kv.Expect` is `kv.Any()`, the
  zero `queue.Outcome` is `queue.Ack()`.
- **Keys are strings** (any bytes, so the same bytes reach the same shard
  from every SDK); values and bodies are `[]byte`. `List` and `ListAfter`
  replace Rust's `Option` for `after`.
- **The delivery helper does not touch HTTP**: it takes the
  `x-wasmkeep-caller` header (`""` when absent) and the body, and returns
  the status, content type and body to answer. A handler that panics
  traps the event: the queue retries the whole batch with its backoff.
- **For TinyGo**: the clients avoid reflection-heavy packages (no
  `fmt`), so they suit TinyGo's `wasip2` target, and their unit tests run
  with plain `go test`.

## C

The C SDK ([`sdks/c`](../sdks/c), declared in `wasmkeep_kv.h`, which
`wasmkeep.h` includes) has the same client side as TypeScript: the `wk_kv`
client, the `wk_queue` client and `wk_queue_handle_delivery` for push
consumers, speaking the same protocol to the same Rust classes.
[`examples/c-jobs`](../examples/c-jobs) is the jobs app in C. Compile
`wasmkeep_kv.c`, `wasmkeep_wire.c` and `wasmkeep_workflow.c` with
`wasmkeep.c` (the examples' `build.sh` do).

```c
#include "wasmkeep.h"

static const wk_kv store = {.class_name = "kv", .shards = 8};  /* or wk_kv_client("kv", 8) */
static const wk_queue emails = {.address = "queue/emails"};

wk_kv_error err;
uint64_t v1;
wk_kv_put(&store, wk_str("user:42"), wk_str("Ada"), &v1, &err);  /* false on failure: err.kind, err.message */
wk_kv_entry entry;
if (wk_kv_get(&store, wk_str("user:42"), &entry, &err) == 1) {  /* 1 found, 0 absent, -1 failed */
  /* entry.value (wk_bytes), entry.version, entry.expires_at (0: never) */
  wk_kv_entry_free(&entry);
}
wk_kv_compare_and_set(&store, wk_str("user:42"), &v1, wk_str("Ada L."), NULL, &err);  /* WK_KV_CONFLICT if it moved */
wk_kv_compare_and_set(&store, wk_str("user:43"), NULL, wk_str("Bob"), NULL, &err);    /* only if absent */
wk_kv_put_options opts = {.has_ttl = true, .ttl_ms = 60000};  /* zeroed: no condition, durable */
wk_kv_put_with(&store, wk_str("session:9"), wk_str("..."), &opts, NULL, &err);
wk_kv_keys page;
if (wk_kv_list(&store, wk_str("user:"), NULL, 100, &page, &err)) wk_kv_keys_free(&page);

uint64_t id;
wk_queue_error qerr;
wk_queue_send(&emails, wk_str("welcome ada@example.com"), &id, &qerr);

/* A push consumer: one outcome per message; return false to fail the batch. */
static bool work(const char *queue, const wk_queue_message *m, wk_queue_outcome *outcome, void *ctx) {
  *outcome = wk_queue_ack_outcome();  /* or wk_queue_retry_outcome(), wk_queue_retry_after(500) */
  return true;
}

wk_response wk_fetch(wk_request *req) {
  if (wk_queue_is_delivery(req)) return wk_queue_handle_delivery(req, work, NULL);
  return wk_reply(200, "ok\n");
}
```

The names follow the Rust ones (`wk_kv_get_fresh`, `wk_kv_with_read_cache`,
`wk_kv_delete_if`, `wk_queue_send_delayed`, `wk_queue_send_batch`,
`wk_queue_lease`, `wk_queue_ack`, `wk_queue_retry`, `wk_queue_settle`,
`wk_queue_get_stats`, `wk_queue_get_dead_letters`), with these
differences:

- **Errors are values**: each call returns `false` (or `-1`) and fills an
  optional `wk_kv_error` / `wk_queue_error` with a kind
  (`WK_KV_CONFLICT`, `WK_KV_KEY_TOO_LONG`, `WK_KV_STORAGE`, `WK_KV_CALL`,
  `WK_KV_REFUSED` with `status`; `WK_QUEUE_STORAGE`, `WK_QUEUE_CALL`,
  `WK_QUEUE_REFUSED`) and the Rust message. A refusal's message keeps at
  most 400 bytes of the answer, unchanged (not made valid UTF-8).
- **Ownership is explicit**: what comes back with memory (an entry, keys,
  messages, dead letters) is freed with its `_free` function; returned
  bytes are NUL-terminated as well. Clients borrow their class name or
  address. `wk_queue_send_batch` and `wk_queue_settle` write into arrays
  the caller sizes; `settle` marks items the queue's reply leaves out as
  not current.
- **Keys and values are `wk_bytes`** (`wk_str` for a C string), 64-bit
  integers are `uint64_t`, and options are optional pointers (`NULL`: an
  absent key for `compare_and_set`, the queue's backoff for `retry`).
- **A delivery handler returns `false`** to fail the batch (500, retried
  with the queue's backoff), where Rust panics and TypeScript throws.

`wk_response_header(&r, "etag", v)` adds a header to a response (the
jobs app's `etag`).

## Zig

The Zig SDK ([`sdks/zig`](../sdks/zig)) wraps the C clients, with Zig
errors and slices: `wk.kv.Client`, `wk.queue.Client` and
`wk.queue.handleDelivery`. [`examples/zig-jobs`](../examples/zig-jobs) is
the jobs app in Zig.

```zig
const wk = @import("wasmkeep");

const store = wk.kv.Client.init("kv", 8);           // a comptime constant is fine
const emails = wk.queue.Client.init("queue/emails");

fn example() !void {
    const v1 = try store.put("user:42", "Ada");      // the new version, once durable
    if (try store.get("user:42")) |found| {          // ?Entry: value, version, expires_at
        var entry = found;
        defer entry.deinit();
    }
    _ = try store.compareAndSet("user:42", v1, "Ada L."); // error.Conflict if it moved
    _ = try store.putWith("session:9", "...", .{ .ttl_ms = 60_000 });
    var page = try store.list("user:", null, 100);   // page.len(), page.at(i)
    defer page.deinit();
    _ = try emails.send("welcome ada@example.com");
}

fn work(queue: []const u8, message: wk.queue.Message) wk.queue.Outcome {
    _ = queue;
    _ = message;
    return .ack; // or .retry, or .{ .retry_after = 500 }
}

pub fn fetch(req: *wk.Request) !wk.Response {
    if (wk.queue.isDelivery(req)) return wk.queue.handleDelivery(req, work);
    return wk.reply(200, "ok\n", .{});
}
```

The differences from Rust are the C ones, in Zig terms:

- **Errors**: `wk.kv.Error` (`Conflict`, `KeyTooLong`, `Storage`, `Call`,
  `Refused`) and `wk.queue.Error` (`Storage`, `Call`, `Refused`); the
  message and status of the last failure are `wk.kv.lastError()` /
  `lastStatus()` (and the same in `wk.queue`).
- **Ownership**: entries, key pages, leased messages and dead letters have
  `deinit`; `sendBatch` and `settle` take an allocator for the slice they
  return.
- **A delivery handler** returns an `Outcome`, or an error union of one:
  an error makes `handleDelivery` return it, so `fetch` answers 500 and the
  queue retries the batch with its backoff.

## Consistency and failure

- **Per key, strongly consistent.** A shard handles one operation at a
  time, so reads see the latest write and compare-and-set is exact. Only
  the optional read cache serves older values, up to its age.
- **Durable when acknowledged.** `put`, `delete` and `send` return once
  the write is in the bucket (fsynced on a node without one), unless a
  local write is asked for (`PutOptions::local()`).
- **No transactions across keys**, across shards, or between a KV write or
  a send and the caller's own database. If the caller's event fails after
  calling, the call's write stays: treat it as a side effect, like
  outbound HTTP.
- **A failed call is ambiguous.** `Error::Call` on a write or a send means
  it may have applied. Re-read before retrying a compare-and-set; a send
  retried after a lost reply may queue the message twice.
- **At-least-once delivery.** A message may be delivered again after a
  lost ack, a lease that ran out, a failed push call or a failover, with
  the same `id` and a higher `attempt`. Record the ids you handled when a
  duplicate would matter, in the same event as the work.
- **Stale acks are ignored.** An ack or retry names its lease
  (`id`, `attempt`); once the message is leased again, the old one does
  nothing.
- **Ordering**: oldest visible first; FIFO only with one consumer, batches
  of one and no retries.
- **Failover**: a shard or queue is one actor, unavailable while another
  node takes it over: 15 to about 21 s after its node's last heartbeat,
  then a restore ([how](cluster.md#how-it-works); on actors, before ADR
  0020: 15.5 to 17.0 s at the median with nodes next to their bucket,
  and 18.2 s on node leases,
  [measured](benchmarks.md#three-nodes-against-a-real-bucket)). Pending
  push deliveries and expiry are alarms, fired after a failover as for
  any actor ([Alarms](concepts.md#alarms)).

## Limits

- **Throughput** per shard and per queue is about one durable write per
  bucket round trip. A durable write took 54 ms at the median with nodes
  next to an OVHcloud bucket (49 ms in a later run), 81 ms from a desktop
  to the same store and 587 ms from a desktop to R2
  ([measured](benchmarks.md#three-nodes-against-a-real-bucket)): roughly
  18 writes a second next to the bucket, under 2 from a desktop to R2.
  Those figures come from the actor soak; shards and queues were not
  benchmarked on their own. Spread
  hot keys over more shards, split a busy queue into several (by key
  hash), batch sends, or use local writes where losing the last write in a
  node crash is acceptable.
- **Each operation is one call**, which holds the caller's event while it
  runs. A push delivery holds the queue's event while the consumer runs:
  keep consumers short, or pull.
- **No re-entry**: a consumer must not call the queue that is delivering to
  it (the call is refused); a shard must not call itself through KV.
- **Outside clients** get `403` from shards and queues (they answer only
  `actors.call`). Serve their classes with `--access CLASS=internal` as
  well, so outside requests never create an actor.

If these limits get in the way, say so in an issue: a platform primitive
can replace the actors behind the same `Client` API
([ADR 0015](adr/0015-kv-and-queues-as-sdk-patterns.md#the-stable-part)).
