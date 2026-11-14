# API reference

<!-- Generated from crates/wasmkeep-wit/wit/actor.wit by crates/wasmkeep-wit/tests/api_reference.rs. Do not edit: run `WASMKEEP_BLESS=1 cargo nextest run -p wasmkeep-wit --test api_reference`. -->

`wasmkeep:actor@0.2.0`, the interface between WasmKeep and every actor, in any language. Each entry gives the WIT definition, then its name in each SDK ([Rust](../../crates/wasmkeep-sdk), [TypeScript](../../sdks/typescript), [Python](../../sdks/python), [C](../../sdks/c), [Zig](../../sdks/zig)). The SDKs add conveniences (typed rows, JSON, broadcast); see each SDK's README.

The `wasmkeep:actor` package: everything an actor can call on the host, and
everything the host calls on an actor.

An actor is addressed as `tenant/class/name`. One component implements one
class; each distinct name is an independent instance with its own SQLite
database. The host delivers events (requests, alarms) to an instance one at
a time, in arrival order: an actor never runs concurrently with itself.

Versioning: 0.x minor versions may break guests; every change comes with a
migration note (docs/adr). See docs/adr/0002-wasmkeep-actor-wit-v0.1.md and
docs/adr/0003-websockets-wit-v0.2.md.

## What an actor implements

HTTP requests arrive through the standard `wasi:http/incoming-handler` export (`handle`), which the TypeScript, Python, C and Zig SDKs expose as their `fetch` handler and the Rust SDK as `http::Guest::handle`. The world adds:

### `on-init`

```wit
on-init: func() -> result<_, string>;
```

Runs before the first event an instance handles with this component
version: once for a new actor, and again after each deploy. Use it to
create or migrate tables. An error fails the event; it is retried with
the next event.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `Actor::on_init` | `init()` | `Actor.init` | `wk_init` | `pub fn init` |

### `on-alarm`

```wit
on-alarm: func() -> result<_, string>;
```

Runs when the pending alarm is due. An error or trap schedules a retry.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `Actor::on_alarm` | `alarm()` | `Actor.alarm` | `wk_alarm` | `pub fn alarm` |

### `on-websocket-open`

```wit
on-websocket-open: func(conn: u64, path: string) -> result<_, string>;
```

A client upgraded a GET to `path` (the part after the actor's name,
with its query). Return an error to refuse it: the connection is then
closed with code 1008 and the error as reason. It runs before the host
answers the upgrade (101), so the client is connected only once the
actor has accepted it. What the actor sends meanwhile waits in the
socket's queue, with its usual cap (`backpressure` past 4 MiB). A
client that leaves before the 101 (or a node relaying for it, which
gives up after 30 s) still gets this event if it was already queued,
followed by `on-websocket-close` with code 1006.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `Actor::on_websocket_open` | `websocket.open()` | `Actor.websocket_open` | `wk_ws_open` | `pub fn wsOpen` |

### `on-websocket-message`

```wit
on-websocket-message: func(conn: u64, msg: message);
```

A message arrived on one of this actor's connections.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `Actor::on_websocket_message` | `websocket.message()` | `Actor.websocket_message` | `wk_ws_message` | `pub fn wsMessage` |

### `on-websocket-close`

```wit
on-websocket-close: func(conn: u64, code: u16, reason: string);
```

A connection closed (by the client, by `close`, or by the host).

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `Actor::on_websocket_close` | `websocket.close()` | `Actor.websocket_close` | `wk_ws_close` | `pub fn wsClose` |

## Interface `storage`

The actor's own SQLite database. Nothing else can read or write it.

Each call runs one statement. Outside an explicit transaction every
statement commits on its own. `BEGIN` ... `COMMIT` across several calls
within one event is allowed; a transaction still open when the event ends
(or traps) is rolled back. A statement that fails in a way that rolls the
open transaction back (it ran past its deadline or the event's SQL CPU
budget, say) ends that transaction: nothing of it is kept, and every
later storage statement of the event is refused with `not-allowed`
(but a `ROLLBACK`, which is what already happened, succeeds).

### `exec`

```wit
exec: func(sql: string, params: list<value>) -> result<u64, sql-error>;
```

Runs a statement that returns no rows; returns the number of rows
changed. Returns once the write is committed to the local database.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `storage::exec` | `storage.exec` | `storage.exec` | `wk_exec` | `wk.exec` |

### `exec-durable`

```wit
exec-durable: func(sql: string, params: list<value>) -> result<u64, sql-error>;
```

Like `exec`, but returns only once the write is durable: replicated to
the bucket when the node has one, fsynced locally when it does not.
Inside a transaction it takes only `COMMIT` (or `END`), and makes the
whole transaction durable; any other statement there is refused with
`not-allowed` before it runs. Outside one it does not take a
statement that opens or controls a transaction (`BEGIN`, `SAVEPOINT`):
run those with `exec`.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `storage::exec_durable` | `storage.execDurable` | `storage.exec_durable` | `wk_exec_durable` | `wk.execDurable` |

### `query`

```wit
query: func(sql: string, params: list<value>) -> result<rows, sql-error>;
```

Runs a statement and returns its rows.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `storage::query` | `storage.query` | `storage.query` | `wk_query` | `wk.query` |

### `delete-self`

```wit
delete-self: func();
```

Deletes this actor: its database is removed once the current event
ends, and its pending alarm is cleared. The next event to this address
starts a fresh actor.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `storage::delete_self` | `storage.deleteSelf` | `storage.delete_self` | `wk_delete_self` | `wk.deleteSelf` |

## Interface `alarms`

One pending alarm per actor. Alarms survive restarts and fire at least
once: an `on-alarm` that fails or traps is retried, so handlers must be
idempotent.

### `set-alarm`

```wit
set-alarm: func(at: u64);
```

Schedules `on-alarm` at `at` (milliseconds since the Unix epoch),
replacing any pending alarm. A time in the past fires as soon as
possible.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `alarms::set_alarm` | `alarms.set` | `alarms.set` | `wk_alarm_set` | `wk.setAlarm` |

### `get-alarm`

```wit
get-alarm: func() -> option<u64>;
```

The pending alarm time, if any.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `alarms::get_alarm` | `alarms.get` | `alarms.get` | `wk_alarm_get` | `wk.getAlarm` |

### `clear-alarm`

```wit
clear-alarm: func();
```

Cancels the pending alarm, if any.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `alarms::clear_alarm` | `alarms.clear` | `alarms.clear` | `wk_alarm_clear` | `wk.clearAlarm` |

## Interface `actors`

Calls between actors of the same tenant.

### `request` (record)

A fully buffered request, delivered to the callee's
`wasi:http/incoming-handler` like any other request.

| Field | |
| --- | --- |
| `method: string` |  |
| `path: string` | Path and query, starting with `/`. |
| `headers: list<tuple<string, string>>` |  |
| `body: list<u8>` |  |

### `response` (record)

| Field | |
| --- | --- |
| `status: u16` |  |
| `headers: list<tuple<string, string>>` |  |
| `body: list<u8>` |  |

### `call-error` (variant)

| Case | |
| --- | --- |
| `invalid-address(string)` | The address is not `class/name`, or the class does not exist. |
| `reentrant` | The call would re-enter an actor already waiting in this call chain (including the caller itself), which would deadlock. |
| `too-large` | The request or response exceeded the body limit (16 MB). |
| `timeout` | The callee did not answer within the event's wall-clock limit. |
| `failed(string)` | The callee trapped, or the host could not deliver the call. |

### `actor-address` (record)

An actor's address. Within a tenant, `class/name` is what `call`
takes; the name is decoded (any text without `/` or control characters).

| Field | |
| --- | --- |
| `tenant: string` |  |
| `class: string` |  |
| `name: string` |  |

### `call`

```wit
call: func(address: string, request: request) -> result<response, call-error>;
```

Sends `request` to the actor at `address` (`class/name`, within the
caller's tenant) and waits for its response. The caller's own event
stays in progress meanwhile: other events to the caller wait.

The callee sees `x-wasmkeep-caller: <tenant>/<class>/<name>`, the name
percent-encoded (every byte but ASCII letters, digits and `-._~`), so
`room 1` arrives as `room%201`. A call from the app's front arrives
with `x-wasmkeep-caller: <tenant>/_front/main`, which is never an
actor (`_front` is not a class). The host removes that header from
requests coming from outside, so the callee can trust it.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `actors::call` | `actors.call` | `actors.call` | `wk_call` | `wk.call` |

### `self-address`

```wit
self-address: func() -> actor-address;
```

This actor's own address: an actor does not otherwise know its name
or class (requests arrive with `/actor/<class>/<name>` removed, and
alarms carry nothing). For links to itself, for telling other actors
who it is, and for anything keyed by its name.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `actors::self_address` | `actors.self()` | `actors.self_address` | `wk_self_address` | `wk.selfAddress` |

## Interface `calls`

Several calls between actors at once (docs/adr/0025-calling-several-actors-at-once.md).

### `call-many`

```wit
call-many: func(calls: list<tuple<string, request>>) -> list<result<response, call-error>>;
```

Sends each request to its actor (`class/name`, within the caller's
tenant), all at once, and waits until every one has answered or the
batch's deadline passed. Returns one result per entry, in input
order. Each entry is an `actors.call`: the same caller value, chain,
routing and errors, so an entry naming the caller or an actor waiting
in its chain is `reentrant`. The caller's event stays in progress
meanwhile, and each callee still runs its events one at a time: no
order between entries is promised, even to one callee. Entries to the
same actor are allowed; each is its own call.

Limits, for the batch as a whole:
- at most 16 entries: a longer batch starts no call, and every entry
  is `failed("too many calls: ...")`;
- one deadline, fixed when the call starts (an actor's call limit,
  or a front's call deadline): an entry still waiting then is
  `timeout`, and the others keep their answers;
- request bodies together at most 16 MB (else every entry is
  `too-large` and none starts), responses together at most 16 MB
  (an entry whose answer would pass it is `too-large`).

Entries of one batch are not in each other's chain: if two callees
of a batch call each other, both wait until their deadline and end
in `timeout`.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `calls::call_many` | `actors.callMany` | not yet | not yet | not yet |

## Interface `websockets`

WebSocket connections to this actor.

The host accepts upgrades on the actor's path and owns the sockets, so a
connection stays open while the actor hibernates; the next message wakes
it. A connection id is only valid in the actor it was opened on.

### `message` (variant)

A WebSocket message.

| Case | |
| --- | --- |
| `text(string)` |  |
| `binary(list<u8>)` |  |

### `send-error` (variant)

| Case | |
| --- | --- |
| `unknown-connection` | The connection is closed, or belongs to another actor. |
| `backpressure` | The client is not reading fast enough; the message was dropped. |

### `send`

```wit
send: func(conn: u64, msg: message) -> result<_, send-error>;
```

Queues `msg` on the connection.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `websockets::send` | `sockets.send` | `sockets.send` | `wk_send, wk_send_binary` | `wk.send, wk.sendBinary` |

### `close`

```wit
close: func(conn: u64, code: u16, reason: string);
```

Closes the connection with a close code (1000 = normal) and reason.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `websockets::close` | `sockets.close` | `sockets.close` | `wk_close` | `wk.close` |

### `connections`

```wit
connections: func() -> list<u64>;
```

This actor's open connections, oldest first.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `websockets::connections` | `sockets.connections` | `sockets.connections` | `wk_connections` | `wk.connections` |

## Interface `env`

Configuration and secrets the operator gives an actor class: an API
key, a webhook URL, a feature flag. Values come, by name, last wins,
from the deployed version's plain configuration (`wasmkeep deploy
--config CLASS=FILE`), the app's secrets sealed in the bucket (`wasmkeep
secrets set`), and files on the node (`wasmkeep serve --env CLASS=FILE`),
never from the component. They are the same for every actor of the class
and read-only, and change only between events, never during one: when a
deploy or a secrets change lands (the next event runs in a new instance)
or the node restarts.

### `get`

```wit
get: func(name: string) -> option<string>;
```

The value of `name`, or none if it is not set.

| Rust | TypeScript | Python | C | Zig |
| --- | --- | --- | --- | --- |
| `env::get` | `env.get` | `env.get` | `wk_env` | `wk.env` |

## Interface `types`

Types shared by several interfaces.

### `value` (variant)

A SQL value, mapping one to one onto SQLite's storage classes.

| Case | |
| --- | --- |
| `null` |  |
| `integer(s64)` |  |
| `real(f64)` |  |
| `text(string)` |  |
| `blob(list<u8>)` |  |

### `rows` (record)

The result set of a query: column names, then one list of values per
row, in column order.

| Field | |
| --- | --- |
| `columns: list<string>` |  |
| `values: list<list<value>>` |  |

### `sql-error-kind` (enum)

Why a statement failed.

| Case | |
| --- | --- |
| `syntax` | The SQL did not parse, or referred to a missing table or column. |
| `constraint` | A constraint failed (UNIQUE, NOT NULL, CHECK, FOREIGN KEY). |
| `bind` | Parameter count or types do not match the statement. |
| `multiple-statements` | The text held more than one statement; send one at a time. |
| `quota-exceeded` | The database reached its size cap (default 1 GB); reads still work. |
| `not-allowed` | The statement tried something actors may not do (ATTACH, a PRAGMA outside the allowed set, loading extensions). |
| `internal` | The host failed (disk, I/O). The event should fail. |

### `sql-error` (record)

| Field | |
| --- | --- |
| `kind: sql-error-kind` |  |
| `message: string` | Human-readable detail, usually SQLite's own message. |

## Also available

The `actor` world includes the standard `wasi:http/proxy` world: outbound HTTP (`wasi:http/outgoing-handler`), clocks, randomness and stdio (what an actor writes to stdout and stderr goes to the node's log, line by line, within `--guest-log-rate`). The `wasi:cli` interfaces most standard libraries import are present but empty: no arguments, environment or terminal. `wasi:filesystem` and `wasi:sockets`, which many toolchains import whether or not the code uses them, are linked but reach nothing: no directory is opened to the guest, and every socket and name lookup is refused. A component importing an interface the host does not provide is refused at deploy time.
