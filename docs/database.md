# A database kept by one actor

Every actor has its own SQLite database. For tables several actors
share (users, orders, a catalogue), the Rust and TypeScript SDKs have a
**database pattern**: one actor owns the tables, like a Cloudflare D1
database, and the app's other actors and its front send it SQL with a
client over `actors.call`. It gets what every actor gets (replication,
failover, the access rules) and has the same limits. Why, and the exact
wire format, are in [ADR 0029](adr/0029-a-database-client-as-an-sdk-pattern.md).
A working app: [`examples/db`](../examples/db) (Rust) and
[`examples/ts-db`](../examples/ts-db) (TypeScript).

> **SQL comes from your code, never from a client.** The database runs
> whatever SQL its callers send. Only the app's own actors and its front
> can call it (outside requests get `403`), so that is safe as long as
> you never pass SQL text from a request, a message or any other input
> to the client, and bind every value as a parameter (`?1`) instead of
> formatting it into the SQL.

## The database class

A class with the SDK's handler and its schema, deployed `internal` (the
project file's default; `--access db=internal` with `wasmkeep serve`), so
outside requests never even create an actor. Each actor of the class is
one database: `db/main`, or `db/eu` and `db/us`, or one per tenant.

```rust
use wasmkeep_sdk::{db, http};

/// The schema, in order. Append to it; never edit an applied entry.
const MIGRATIONS: &[&str] = &[
    "CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
    "CREATE INDEX todos_done ON todos (done)",
];

struct Db;

impl wasmkeep_sdk::Actor for Db {
    fn on_init() -> Result<(), String> {
        db::migrate(MIGRATIONS).map(drop).map_err(|e| e.to_string())
    }
}

impl http::Guest for Db {
    fn handle(request: http::IncomingRequest, out: http::ResponseOutparam) {
        db::handle(request, out);
    }
}

wasmkeep_sdk::export!(Db);
```

In TypeScript, the whole class is one call:

```ts
import { db } from "@wasmkeep/sdk";

export default db.define({
  migrations: [
    "CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
    "CREATE INDEX todos_done ON todos (done)",
  ],
});
```

- **Migrations** run in `init`, each once, in its own durable
  transaction, recorded in the table `wasmkeep_db_migrations`. Only ever
  append to the list: an edited entry, or a component listing fewer
  migrations than the database has applied, fails init and applies
  nothing. Leave that table alone in your own SQL.
- **Who may call**: `db::handle_allowing(request, out, |caller| ...)`
  (TypeScript: `allow` in `db.define` or `db.handle`) refuses callers
  you do not list; it gets `tenant/class/name` as the host sends it, the
  name percent-encoded (`room%201`), the front being `tenant/_front/main`.
- **Limits**: `db::handle_with(&limits, ...)` (TypeScript: `limits`)
  changes the caps below.

## The client

From any other actor of the app:

```rust
use wasmkeep_sdk::db::{self, Outcome, Statement};
use wasmkeep_sdk::params;

let main = db::Client::new("db/main");

// One write, durable when it returns: the rows it changed.
main.exec("UPDATE todos SET done = 1 WHERE id = ?1", &params![42])?;

// One read: column names, then rows of values.
let rows = main.query("SELECT id, text FROM todos WHERE done = ?1", &params![0])?;
for row in &rows.values {
    let (id, text) = (&row[0], &row[1]);
}

// Several writes in one transaction: all kept, or none.
let outcomes = main.batch(&[
    Statement::query("INSERT INTO todos (text) VALUES (?1) RETURNING id", &params!["buy milk"]),
    Statement::exec("DELETE FROM todos WHERE done = 1", &[]),
])?;
if let Outcome::Rows(r) = &outcomes[0] { /* the new id */ }

// Several reads that see the same data.
let both = main.read(&[
    Statement::query("SELECT COUNT(*) FROM todos", &[]),
    Statement::query("SELECT * FROM todos ORDER BY id DESC LIMIT 10", &[]),
])?;
```

```ts
import { db } from "@wasmkeep/sdk";

const main = new db.Client("db/main");

main.exec("UPDATE todos SET done = 1 WHERE id = ?1", [42]);           // rows changed
const { columns, rows } = main.query("SELECT id, text FROM todos WHERE done = ?1", [0]);
db.objects({ columns, rows });                                         // [{ id, text }, ...]
const [inserted, deleted] = main.batch([
  { sql: "INSERT INTO todos (text) VALUES (?1) RETURNING id", params: ["buy milk"], rows: true },
  { sql: "DELETE FROM todos WHERE done = 1" },
]);                                                                    // { columns, rows }, { changes }
main.read([{ sql: "SELECT COUNT(*) FROM todos", rows: true }]);
```

- **Statements** are one SQL statement each, with `?`, `?N` or `:name`
  placeholders. In a batch, `Statement::query` / `rows: true` answers the
  statement's rows (a `SELECT`, or a write with `RETURNING`);
  `Statement::exec` / the default answers the number of rows it changed.
- **Values** are SQLite's: null, 64-bit integers, reals, text and blobs,
  exact on the way in and out. In TypeScript a safe integer binds as an
  integer and any other number as a real (`{ tag: "real", val: 2 }` binds
  a whole real); integers past 2^53 come back as `bigint`, and booleans
  bind as 1 or 0.
- **Errors**: `db::Error` / `DbError` with a kind: `Sql` / `"sql"`
  (which statement, or the commit, and the host's kind: `constraint`,
  `syntax`, ...), `NotAllowed` (a statement controls the transaction),
  `TooLarge`, `Call` (the call failed, or the commit was kept locally but
  could not be made durable: a write may have been committed, so re-read
  before retrying one that is not idempotent) and `Refused`
  (`403` from a caller the database refuses, or a malformed answer).
- **Names**: a database is named by its actor address. If the name comes
  from a request (one database per tenant), check it against the names
  you serve before building the client.

If you know D1: `db.prepare(sql).bind(...).all()` is `query(sql,
params)`, `.run()` is `exec`, `db.batch([...])` is `batch`, and the rows
come back as columns and arrays (`db.objects` in TypeScript makes objects
of them).

## Consistency and failure

- **One writer.** A database runs one request at a time, so every read
  sees every write answered before it.
- **Each request is one transaction.** A batch is kept whole or not at
  all; the first failing statement rolls everything back, and so does a
  failing commit (a deferred foreign key, say). The exception is a commit
  SQLite made whose upload or fsync then failed: the client gets `Call`
  (outcome unknown), never an error saying nothing was kept. Statements that control
  the transaction (`BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`,
  `RELEASE`) are refused before anything runs.
- **Reads never write.** A read request runs in a transaction that is
  always rolled back, so a write in it is undone.
- **Durable when answered.** A write returns once committed durably: in
  the bucket in a cluster, fsynced on one node.
- **A failed call is ambiguous**, as for any actor call: the write may
  have been committed before the answer was lost.
- **No transaction across databases**, nor with the caller's own SQLite:
  if the caller's event fails after a write, the write stays.
- **Failover**: a database is one actor, unavailable while another node
  takes it over: 15 to about 21 s after its node's last heartbeat
  ([how](cluster.md#how-it-works)).

## Limits

- **Throughput**: one actor, one request at a time, so about one durable
  write per bucket round trip (roughly 18 a second next to the bucket,
  under 2 from a desktop to R2, from the
  [actor soak](benchmarks.md#three-nodes-against-a-real-bucket); the
  database pattern was not benchmarked on its own). Batch writes, and
  **split the data into more databases** (by tenant, region or key) when
  one is not enough. It is not a shared multi-writer database.
- **Per request**, by default: a 4 MiB body, 100 statements, 100,000
  bytes of SQL and 100 parameters per statement, and an 8 MiB answer
  (blobs travel as hex, two bytes each). Over any, the request is refused
  (`TooLarge`) and nothing is kept. A TypeScript database refuses an
  answer whose values' lengths alone pass the cap before encoding any of
  it, so the refusal costs next to nothing of the CPU budget below.
- **The host's limits** bound every request too, since a request is one
  event of the database actor: its CPU budget and wall-clock time, each
  statement's time and result size, and the database's size cap. A
  TypeScript database spends more of the CPU budget encoding answers than
  a Rust one, StarlingMonkey most: prefer the Rust class for large
  answers.
- **Each request is one call**, which holds the caller's event while it
  runs.

If these limits get in the way, say so in an issue: a platform primitive
could serve the same client API later.
