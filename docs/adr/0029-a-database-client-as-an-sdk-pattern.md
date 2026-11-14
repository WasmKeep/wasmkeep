# 0029. A database client as an SDK pattern

- Status: accepted (2026-11-14). Built in the Rust SDK
  (`crates/wasmkeep-sdk/src/db.rs`) and the TypeScript SDK
  (`sdks/typescript/src/db.ts`): the handler, the client and the
  migrations helper in both, with [`examples/db`](../../examples/db) and
  [`examples/ts-db`](../../examples/ts-db), and every client tested against
  every handler (`crates/wasmkeep-server/tests/db_langs.rs`). The other
  SDKs come later ([languages](../languages.md#a-database-kept-by-one-actor)).
- Date: 2026-11-14
- Milestone: [Now](../../ROADMAP.md#now), files and databases

## Context

Every actor has its own SQLite database, which nothing else reads. Many
apps also want tables that several actors share: users, orders, a
catalogue. Cloudflare D1 answers that with a managed database next to the
code, one writer per database. ADR 0015 set the approach for such needs:
a stable API in the SDKs, backed by ordinary actors, with no host
function and no WIT change, so a primitive could replace it later.

A database kept by one actor already works by hand: one actor owns the
tables, the others call it. What each app would rewrite is the protocol
(how SQL, parameters, rows and errors travel), the transaction around a
batch, the limits, the schema migrations, and the check that only the
app may call it. This decision makes that one SDK pattern.

## Decision

### A database is one actor

A database is an actor of a class the app writes with the SDK: the class
mounts the handler and lists its migrations; each actor of it is one
database (`db/main`, `db/eu`, `db/tenant-42`), with its own SQLite file,
replicated and failed over like any actor. The app's other actors and its
front reach it with a client built from its address:

```rust
let main = db::Client::new("db/main");
main.exec("INSERT INTO todos (text) VALUES (?1)", &params!["buy milk"])?;
let rows = main.query("SELECT id, text FROM todos WHERE done = ?1", &params![0])?;
```

`query` (one read), `exec` (one write), `batch` (writes, all or none) and
`read` (several reads, one snapshot) are the client; `handle` (with
`handle_allowing` and `handle_with` for a caller check and other limits)
and `migrate` are the class's side. TypeScript has the same, with
`db.define({ migrations, limits, allow })` for the whole class.

### The wire format

Both SDKs write the same bytes for the same request and the same answer
(fixtures in both test suites). It is JSON, so a request is readable in a
log and every SDK can speak it with what it already has.

- **Method and paths**: `POST` to the actor's `wasmkeep-db/v1/read` or
  `wasmkeep-db/v1/write` (the request path, from its root), with
  `content-type: application/json`, through `actors.call`. The version is
  in the path.
- **Request**: `{"statements":[{"sql":S,"params":[V,...],"rows":B},...]}`.
  `sql` is one statement; `params` bind its placeholders (`?`, `?N`,
  `:name`) in order; `rows: true` runs it as a query and answers its rows
  (a `SELECT`, or a write with `RETURNING`), `rows: false` as a write
  answering the rows it changed. `params` and `rows` may be left out
  (none, `false`); other keys are refused.
- **Values** `V`: `null`; a JSON string for text; and for the others an
  object with one key whose value is a string:
  `{"integer":"-42"}` (decimal, `-?(0|[1-9][0-9]*)`, 64 bits),
  `{"real":"1.5"}` (as JavaScript's `String` writes a number, so the same
  bytes from Rust; also `-0`, `NaN`, `Infinity`, `-Infinity`), and
  `{"blob":"00ff"}` (hex, written in lowercase; either case is read). Strings because a JSON number cannot
  carry a 64-bit integer to JavaScript, nor tell the real `2.0` from the
  integer `2`. Text is valid Unicode: a lone surrogate is refused.
- **Answer** (200): `{"results":[R,...]}`, one per statement, in order:
  `{"columns":[S,...],"rows":[[V,...],...]}` for a query (rows as arrays,
  so `SELECT a, a` keeps both), `{"changes":N}` for a write.
- **Errors** (any other status):
  `{"error":C,"kind":K,"statement":I,"message":S}`, `kind` and
  `statement` only where they apply:

  | Status | `error` | When |
  | --- | --- | --- |
  | 400 | `bad-request` | The body is not UTF-8 or JSON, or not the shape above |
  | 400 | `not-allowed` | Statement `statement` controls the transaction |
  | 400 (500 for `internal`) | `sql` | Statement `statement` failed, or the commit (no `statement`); `kind` is the host's: `syntax`, `constraint`, `bind`, `multiple-statements`, `quota-exceeded`, `not-allowed`, `internal` |
  | 403 | `forbidden` | The caller is outside, or `allow` refused it |
  | 404, 405 | `not-found`, `method` | Another path, or not `POST` |
  | 413 | `too-large` | The request or its answer is over a limit |
  | 500 | `unknown-outcome` | The commit happened locally but could not be made durable (the bucket upload or the fsync failed): the write may be kept |

  The clients turn these into errors (`db::Error`, `DbError`): `Sql`
  with the kind and statement, `NotAllowed`, `TooLarge`, `Call` (the call
  failed, or `unknown-outcome`: a write may have been committed),
  `Refused` (anything else, `403` included).

### Transactions

Each request is **one transaction**. The handler runs `BEGIN`, then the
statements in order; at the first failure it rolls back and answers that
statement's error, so a batch is kept whole or not at all. A write
request ends with a durable `COMMIT` (`exec-durable`: in the bucket in a
cluster, fsynced on one node), so an answered write survives the loss of
the node; a commit that fails (a deferred foreign key, say) rolls back
too. One failure is different: a commit that SQLite made but whose
upload or fsync then failed. The transaction is no longer open, so the
handler cannot undo it, and answers `unknown-outcome` rather than an
error that would claim nothing was kept. A **read** request ends with `ROLLBACK`: it can never change the
database, whatever its SQL does (a `DELETE ... RETURNING` sent as a read
is undone). That is how read-only is enforced; no guess is made from the
SQL text.

The handler owns the transaction, so caller SQL may not control it: a
statement whose first keyword (after whitespace, `--` and `/* */`
comments and empty `;` statements, ASCII case ignored) is `BEGIN`,
`COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT` or `RELEASE` is refused with
`not-allowed` before anything runs. A `COMMIT` in the middle of a batch
would otherwise keep its first half. The host already refuses more than
one statement per text, `ATTACH`, most `PRAGMA`s and extensions.

### Limits

Per request, set in the class (`db::Limits`, `limits` in TypeScript):
a body of 4 MiB, 100 statements, 100,000 bytes of SQL and 100 parameters
per statement, and an answer of 8 MiB (under the 16 MB a call carries;
blobs travel as hex, two bytes each). Over any of them, `413` and nothing
is kept. The host's limits apply as well and are what really bounds a
request: the event's CPU budget and wall-clock time (one request is one
event), each statement's time and result size, and the database's size
cap. A TypeScript handler spends its CPU budget encoding answers faster
than the Rust one (an interpreter, StarlingMonkey most); a large answer
can run out of it and fail the call.

### Migrations

`migrate(list)`, called from the class's init, applies an ordered list
of statements, each once, each in its own durable transaction with its
record in `wasmkeep_db_migrations (version, sql, applied_at)`. The list
is append-only: a recorded migration whose text changed, or more records
than the list holds (an older component deployed over a newer database),
fails init, and nothing is applied. A migration may not control the
transaction either. The table is an ordinary one, so callers' SQL can
reach it: leave it alone (a deleted record fails the next init).

### Security: SQL from your code, never from a client

**The handler runs whatever SQL its callers send.** That is safe only
because its callers are the app's own code:

- **Inside callers only.** The handler answers only requests carrying
  `x-wasmkeep-caller`, which the host sets on `actors.call` (from an
  actor or the app's front) and strips from every outside request; anyone
  else gets `403`. `allow` sees the caller as the host sends it,
  `tenant/class/name` with the name percent-encoded (`room%201`). Serve
  the class `internal` as well (the project file's
  default; `--access db=internal`), so an outside request never creates
  an actor. `handle_allowing` narrows the callers further (only the
  front, only one class).
- **No raw SQL from clients.** Never pass SQL text from an HTTP request,
  a WebSocket message or any other input to a client. Write the SQL in
  code and **bind every value as a parameter**; never format a value into
  the SQL. A front that forwarded a user's SQL would hand that user the
  whole database.
- **Names are validated by the caller.** A database's name is an actor
  name: an app that picks one from a request (one database per tenant)
  checks it against the names it serves, as the front does for any actor
  address it builds.

### What it is not

- **Not a shared multi-writer database.** One actor runs one request at
  a time: about one durable write per bucket round trip, and every
  request waits for the ones before it. When that is not enough, **split
  the data into more databases** (by tenant, region or shard), or keep
  per-entity data in the entity's own actor. No query spans two
  databases; a transaction never does.
- **Not reachable from outside.** There is no HTTP API for clients; an
  app exposes its own routes and runs its own SQL.
- **Not a transaction across calls.** A request is one transaction, and
  nothing holds a transaction open between requests. There is none
  between a database and the caller's own SQLite either: treat a write
  as a side effect, like outbound HTTP.

### Not done here

- The clients and handlers of the Python, Go, C, Zig, D and MoonBit SDKs.
  The format is JSON so each can be written with what it has; until then
  those SDKs call a database with `actors.call` and this format.
- A helper for queries to several databases at once (`call-many`): an
  app can send them with `calls::call_many` / `actors.callMany` today.
- The client in fronts (Rust's `db` module needs the SDK's `actor`
  feature; a front calls with `actors.call` and this format).
- A local (not durable) write option, like KV's.
- No `sim/` case: nothing in leases, replication or deploys changes; the
  behaviour rests on one actor's guarantees.

## Consequences

- Apps get a shared SQL database with no platform change, inheriting
  replication, failover and the access rules from actors, and a client
  API (`query`, `exec`, `batch`, `read`) that a primitive could serve
  later.
- Its limits are one actor's, and the guide says so: throughput, the
  failover window (15 to about 21 s, ADR 0020), and the host's per-event
  budget on every request.
- The format is JSON with tagged values, a little larger than KV's binary
  framing, in exchange for exact values in every SDK and readable logs.
  Reals are written as ECMAScript's `Number::toString` writes them (the
  closest of the shortest strings that read back), which the Rust SDK
  reproduces (compared with node on two million doubles).
