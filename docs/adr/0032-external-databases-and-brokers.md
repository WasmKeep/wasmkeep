# 0032. External databases and brokers

- Status: proposed (2026-11-14). On paper only: nothing here is built.
- Date: 2026-11-14
- Milestone: [Later, on demand](../../ROADMAP.md#later-on-demand) (since
  0.4.2); it is written now to answer whether outbound HTTP is enough
  before anyone builds on the answer.
- Follows: [ADR 0016](0016-secrets-in-the-bucket.md) (secrets),
  [ADR 0021](0021-the-app-model.md) (the app is the unit),
  [ADR 0023](0023-the-front.md) (the front),
  [ADR 0025](0025-calling-several-actors-at-once.md) (deadlines inside an
  event), [ADR 0026](0026-the-project-file.md) (the project file) and
  [ADR 0029](0029-a-database-client-as-an-sdk-pattern.md) (a database kept
  by one actor).
- Would amend: ADR 0016 (a secrets scope for connections), ADR 0026 (a
  `[connections]` table), and the `actor` and `front` worlds (a new
  imported package).

## Context

An actor is a WASI 0.2 component. Its only way out of the sandbox is
outbound HTTP (`wasi:http/outgoing-handler`): the host resolves the name,
refuses private and reserved addresses unless the operator opened a range
with `--allow-outbound`, refuses the ports browsers block, checks TLS
against the name, and allows 16 requests in flight per actor and 256 per
node ([concepts](../concepts.md#what-an-actor-cannot-do),
[security](../security.md)). Toolchains import `wasi:sockets` anyway, so
the host links it inert: every socket and lookup is refused.

The question asked: *if an app needs an external database, is HTTP
enough, or should WasmKeep offer something else?* "External" means a
database or broker that exists outside the cluster and outlives it:
PostgreSQL, MySQL, Redis or Valkey, NATS. It is shared with other systems,
holds more than 1 GB, takes many writers at once, or answers queries
across every entity, which actors do not
([actors or a database](../failover-and-scaling.md#actors-or-a-database)).

[ADR 0029](0029-a-database-client-as-an-sdk-pattern.md) answers a
different need: tables several actors share, kept *inside* the cluster by
one actor, replicated and failed over with it. Both stay:

| Need | Use |
| --- | --- |
| Per-entity state (a user, a room, a cart) | The actor's own SQLite |
| Tables the app's actors share, owned by the app, within one actor's throughput | A database kept by one actor (ADR 0029) |
| A database other systems also use, larger than 1 GB, many concurrent writers, analytics | An external database (this record) |
| Messages to or from other systems | An external broker (this record) |

### What HTTP reaches today

Read on 2026-11-14 from each vendor's documentation:

| Service | Over HTTP | What it does not cover |
| --- | --- | --- |
| Neon ([serverless driver](https://neon.com/docs/serverless/serverless-driver)) | One query per request, or several in one non-interactive transaction (`transaction()`) | Interactive transactions and sessions need its WebSocket mode, which outbound HTTP cannot open. The HTTP endpoint is the JavaScript driver's protocol; no Neon reference for it was found for other languages. Its language-neutral HTTP API is the PostgREST-style [Data API](https://neon.com/docs/data-api/overview), not SQL |
| Supabase ([Data API](https://supabase.com/docs/guides/api)) | PostgREST over the schema, functions through `rpc` | No SQL text; each request is its own transaction ([PostgREST](https://docs.postgrest.org/en/stable/references/transactions.html)), so atomicity across statements means writing a database function |
| PlanetScale, MySQL ([serverless driver](https://planetscale.com/docs/vitess/tutorials/planetscale-serverless-driver)) | SQL over HTTP through `@planetscale/database` | A JavaScript driver; no reference for its HTTP protocol was found for other languages |
| PlanetScale, Postgres ([serverless driver](https://planetscale.com/docs/postgres/connecting/serverless-driver)) | Through Neon's driver, pointed at PlanetScale | The same limits as Neon's HTTP mode |
| Upstash Redis ([REST API](https://upstash.com/docs/redis/features/restapi)) | Any command; a pipeline endpoint (not atomic) and a multi-exec endpoint (atomic) | No blocking commands, no `WATCH`; subscribe is server-sent events |
| Turso / libSQL ([HTTP API](https://docs.turso.tech/sdk/http/reference)) | SQL in pipelines, with a baton to continue a stream, so interactive transactions across requests | A transaction must finish within seconds; this is SQLite's dialect, not Postgres |
| NATS ([Synadia HTTP Gateway](https://docs.synadia.com/platform/http-gateway)) | Key-value and object store; publish, subscribe and request/reply marked experimental (the page's list is dated June 2024) | Synadia's product, not part of `nats-server`; nothing in core NATS speaks HTTP |
| Valkey / Redis ([protocol](https://valkey.io/topics/protocol/)) | Nothing | RESP over TCP; no first-party HTTP interface |
| A self-hosted PostgreSQL or MySQL | Nothing, unless the operator runs a gateway (PostgREST, for example) and opens it with `--allow-outbound` | Everything the gateway does not expose |

So HTTP is enough for managed services built for serverless platforms,
and only for them, and mostly from JavaScript. It reaches no self-hosted
Valkey, MySQL or PostgreSQL without a service in between, no NATS without
Synadia's gateway, and no interactive Postgres transaction at all.

### What the others do

- **Spin** has host interfaces for PostgreSQL, MySQL and Redis
  ([world](https://github.com/spinframework/spin/blob/main/wit/world.wit),
  [relational databases](https://spinframework.dev/v3/rdbms-storage)).
  The guest opens a connection with the address
  ([`postgres.wit`](https://raw.githubusercontent.com/spinframework/spin/main/wit/deps/spin-postgres@4.2.0/postgres.wit):
  `open(address)`), usually from a variable, so the connection string,
  credentials included, passes through guest code. The manifest's
  `allowed_outbound_hosts` lists the hosts a component may reach, by
  scheme (`postgres`, `mysql`, `redis`), host and port
  ([manifest](https://spinframework.dev/v3/manifest-reference)). Its
  Postgres WIT has a rich value type (dates, timestamps, UUID, JSONB,
  decimal, ranges, arrays) and no transaction function. A Redis trigger
  delivers subscribed messages to components
  ([Redis trigger](https://spinframework.dev/v3/redis-trigger)). Whether
  it pools connections across requests is not documented; not confirmed
  here.
- **wasmCloud 2** replaced its out-of-process capability providers with
  host plugins: `wasmcloud:postgres`, `wasmcloud:nats`,
  `wasi:keyvalue` and others
  ([host plugins](https://wasmcloud.com/docs/overview/hosts/plugins)).
  The operator declares each plugin with its configuration and secrets;
  by default a workload may not supply its own connection addresses or
  credentials, and a manifest that tries is refused at deploy. It also
  gives components outbound `wasi:sockets` TCP, checked against allowed
  hosts and an address policy
  ([network access](https://wasmcloud.com/docs/wash/developer-guide/network-access-and-socket-isolation/)).
- **Cloudflare Workers** give code raw TCP (`connect()`,
  [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)),
  opened per request, and put **Hyperdrive** in front of Postgres and
  MySQL: a pool near the database, in transaction mode, so session state
  (`SET`, session advisory locks) does not survive between transactions
  ([how Hyperdrive works](https://developers.cloudflare.com/hyperdrive/concepts/how-hyperdrive-works/)).
  The database's address and credentials are set in the Hyperdrive
  configuration, and the Worker reads a connection string from its
  binding.

### What changed in WASI

WASI 0.3 was released on 2026-06-11, and Wasmtime 46 and later enable it
by default ([WASI 0.3](https://wasi.dev/releases/wasi-p3)); WasmKeep pins
Wasmtime 50 and links only the `p2` interfaces. Tokio 1.51 added
networking on `wasm32-wasip2`, still behind `--cfg tokio_unstable`
([tokio#7933](https://github.com/tokio-rs/tokio/pull/7933)). So "guest
drivers cannot work" is no longer true for Rust. The case against
sockets in each actor below rests on other grounds. Among the WASI
proposals, sockets and HTTP are at phase 3, key-value and messaging at
phase 2, SQL at phase 1
([proposals](https://github.com/WebAssembly/WASI/blob/main/docs/Proposals.md)).

## Decision

### 1. Outbound HTTP stays the general path

Nothing about outbound HTTP changes. An app that uses a database with an
HTTP API (the table above) keeps doing so: the guide gets a page that
lists them, with the same limits as any outbound request. Credentials for
those services are the app's secrets, read with `env.get`, as today.

### 2. Host connectors for the protocols HTTP does not reach

The host gains **connectors**: WIT interfaces whose calls the node runs
over a pool of connections it keeps. In order:

1. **PostgreSQL**: `query`, `exec` and `batch` (several statements, one
   transaction, no code between them).
2. **Redis and Valkey**: one command, a pipeline, an atomic batch.
3. **PostgreSQL transactions across calls** within one event (section 6).
4. **MySQL**: the shape of PostgreSQL's.
5. **NATS publish** and request/reply.

Each comes in its own change, with its SDK functions in Rust and
TypeScript. NATS subscribe, which would wake actors with messages, is a
larger design, left out (Not decided here).

Why a host interface and not an SDK pattern, as ADRs 0015, 0019 and 0029
chose: a pattern can only use outbound HTTP, which is what does not reach
these services; and only the host can keep a connection across events,
across instances and across the many actors of a class.

Why not `wasi:sockets` in each actor (section 14 weighs it in full):

- **Connections do not fit actors.** An actor's instance is discarded
  when it hibernates, traps or moves, so its connection dies with it, and
  each wake pays a TCP and TLS handshake and authentication. A thousand
  resident actors would hold a thousand connections, where Postgres
  serves a few hundred well.
- **Credentials would be in the guest.** A driver needs the password.
  With a connector the node holds it and the guest names a binding.
- **Most SDKs have no driver.** QuickJS and StarlingMonkey (TypeScript)
  have no TCP drivers; componentize-py, TinyGo, C, Zig, D and MoonBit
  would each need one. A WIT interface is one binding per SDK.
- **The policy is per connection either way.** Sockets would need the
  same address checks outbound HTTP has, done at each `connect`.

### 3. Bindings: a name the app declares, a URL only the node reads

A **connection** is a named binding of an app: a kind, the classes that
may use it, and a URL kept as a secret. The guest names the binding,
never a host. In `wasmkeep.toml` (ADR 0026):

```toml
[connections.main]
kind = "postgres"               # postgres | redis | mysql | nats
classes = ["orders", "_front"]  # who may use it; "_front" is the front
pool = 5                        # connections per node, default 5
max-rows = 10000                # optional, at most the default (section 8)
max-bytes = 8388608             # optional, at most the default (section 8)
transaction-timeout-ms = 5000   # optional, at most 10000 (section 6)

[connections.cache]
kind = "redis"
classes = ["sessions"]
```

- `kind` and `classes` are required; the other keys are optional, and
  any other key is refused, as everywhere in the file. A binding name has
  the grammar of class names. A class or `_front` listed that the file
  does not define is refused.

- **The file holds no host and no credential.** ADR 0026 keeps values
  and anything specific to one environment out of the file; staging and
  production reach different databases with the same file. The URL is a
  secret.
- **The URL is a secret in a scope of its own**, `_connection`, named by
  the binding:

  ```bash
  wasmkeep secrets set --bucket s3://b/prod --app acme/shop --key k --connection main < url
  ```

  `env.get` never returns it, in any class or the front, so guest code
  never holds it. The whole URL is the secret
  (user, password, host, port, database and `sslmode`, in one
  `postgres` URL),
  so a password rotation and a move to another host are the same
  operation.
- **What the file's list protects.** `classes` is the allowlist: a class
  not listed gets `not-allowed` on every call to the binding, so a
  compromised or careless class cannot reach a database it was not given.
  It does not decide *where* a binding connects. That is decided by
  whoever can set the app's secrets, within the node's egress policy
  (section 9), which is the operator's ceiling. An allowlist of hosts in
  the file would be environment-specific and would only check what the
  same people already control; it is left out (Not decided here).
- **Nodes without the app's secrets.** `wasmkeep dev`, `serve` without
  `--app`, and a node run with `--no-secrets` read only node files
  (ADR 0016). For them the URL comes from a node file too:
  `--connection main=FILE` (a file holding the URL, readable only by the
  server's user, like `--env` files), and `[dev] connections = ".connections.local"`
  for `wasmkeep dev` (`NAME=URL` lines, kept out of version control like
  `[dev] env`). A node value wins over the sealed one by name, as `--env`
  does over secrets.
- **A binding without a URL** fails its calls with
  `not-allowed("connection main has no URL")`, and is reported once in
  the log, like a missing required name (ADR 0016). `requires` does not
  list connections; a class's events run without one.

### 4. The package: `wasmkeep:connect`

A new package, `wasmkeep:connect@0.1.0`, with one interface per kind,
imported by `actor-core` and `front-core`. A separate package rather
than interfaces in `wasmkeep:actor@0.2.0`: the Postgres value type will
change while connectors are young (arrays, composite types, streamed
rows), and a 0.x package of its own can break without touching the
actor package that every built component imports. Adding the imports to
the worlds changes nothing for a component that does not use them
([versions](../languages.md#versions)). All five WIT copies carry it.

#### PostgreSQL

```wit
package wasmkeep:connect@0.1.0;

/// Errors every connector returns.
interface types {
    variant connect-error {
        /// The binding does not exist, the caller's class is not listed,
        /// it has no URL, or the call is refused as asked (a statement
        /// that controls the transaction outside `transaction`, a
        /// blocking Redis command).
        not-allowed(string),
        /// No connection could be had in time, or the server refused it
        /// (unreachable, TLS, authentication). Nothing was sent: safe to
        /// retry.
        unavailable(string),
        /// The call did not finish before its deadline. A read may be
        /// retried; a write may have happened.
        timeout,
        /// The connection broke after a write or a commit was sent: it
        /// may have been kept.
        unknown-outcome(string),
        /// The request or the answer passed a limit (section 8). A request
        /// over a limit is not sent; an answer over one cancels its
        /// statement.
        too-large,
    }
}

interface postgres {
    use types.{connect-error};

    /// A value, as a parameter or in a row. Types without a case travel
    /// as their text form (`text`), and Postgres casts a text parameter
    /// where the statement says so (`$1::inet`).
    variant value {
        null,
        boolean(bool),
        int16(s16),
        int32(s32),
        int64(s64),
        float32(f32),
        float64(f64),
        text(string),
        bytea(list<u8>),
        /// Exact decimal text: `-12.50`, `NaN`.
        numeric(string),
        /// `xxxxxxxx-xxxx-...`, lowercase.
        uuid(string),
        /// `json` and `jsonb`, as text.
        json(string),
        /// ISO 8601: `2026-11-14`, `13:05:00.25`.
        date(string),
        time(string),
        /// Without time zone, ISO 8601, microseconds.
        timestamp(string),
        /// Microseconds since the Unix epoch, UTC.
        timestamptz(s64),
        /// A column of a type with no case above: its type OID and text.
        other(tuple<u32, string>),
    }

    record column {
        name: string,
        /// The type OID, as Postgres sends it.
        type-oid: u32,
    }

    record rows {
        columns: list<column>,
        values: list<list<value>>,
    }

    record statement {
        sql: string,
        params: list<value>,
        /// Answer rows (a `SELECT`, or `RETURNING`), else the count.
        rows: bool,
    }

    variant outcome {
        rows(rows),
        changed(u64),
    }

    record sql-error {
        /// The SQLSTATE code: `23505` for a unique violation.
        sqlstate: string,
        message: string,
        detail: option<string>,
        /// The failed statement's index, in a batch.
        statement: option<u32>,
    }

    variant error {
        sql(sql-error),
        connect(connect-error),
    }

    record options {
        /// Milliseconds; default 5,000, never past the event's deadline.
        timeout-ms: option<u32>,
    }

    /// One statement that returns rows, on a pooled connection.
    query: func(binding: string, sql: string, params: list<value>, options: options)
        -> result<rows, error>;

    /// One statement that returns no rows; returns the rows changed.
    exec: func(binding: string, sql: string, params: list<value>, options: options)
        -> result<u64, error>;

    /// Several statements in one transaction, in order, with no code
    /// between them: all kept, or none. One outcome per statement.
    batch: func(binding: string, statements: list<statement>, options: options)
        -> result<list<outcome>, error>;

    /// Slice 3 (section 6): a transaction across calls, within this event.
    resource transaction {
        query: func(sql: string, params: list<value>) -> result<rows, error>;
        exec: func(sql: string, params: list<value>) -> result<u64, error>;
        /// Commits and returns the connection to the pool.
        commit: static func(this: transaction) -> result<_, error>;
        /// Dropping a transaction without `commit` rolls it back.
    }

    begin: func(binding: string, options: options) -> result<transaction, error>;
}
```

- **One statement per text**, sent with the extended protocol (parse,
  bind, execute), so parameters are never formatted into the SQL and a
  `;` cannot smuggle a second statement.
- **No statement may control the transaction**: `BEGIN`, `COMMIT`,
  `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`, refused with
  `not-allowed` before anything is sent, by the first keyword as ADR 0029
  checks it. Also refused: `LISTEN`, `UNLISTEN`, `COPY`, `SET` and
  `RESET` outside a transaction (they would outlive the call on a pooled
  connection; `SET LOCAL` inside a `batch` or `transaction` is fine).
  What this check cannot see (`set_config`, session advisory locks) is
  undone by the reset after every call (section 7).
- **A statement is prepared unnamed**, so no prepared statement outlives
  its call; caching prepared statements per connection is an
  optimisation for later.
- `exec` outside a batch commits on its own, as Postgres does.

#### Redis and Valkey

```wit
interface redis {
    use types.{connect-error};

    /// A reply, flattened: RESP nests (an `EXEC`'s answer, a stream
    /// entry), WIT cannot. Nodes are in order, an `array(n)` or `map(n)`
    /// followed by its n children (2n for a map). The SDKs rebuild the tree.
    variant node {
        nil,
        status(string),
        integer(s64),
        double(f64),
        boolean(bool),
        bulk(list<u8>),
        array(u32),
        map(u32),
    }

    record reply {
        nodes: list<node>,
    }

    variant error {
        /// The server answered an error: `WRONGTYPE ...`.
        server(string),
        connect(connect-error),
    }

    record options {
        /// Milliseconds; default 2,000, never past the event's deadline.
        timeout-ms: option<u32>,
    }

    /// One command: `["SET", "k", "v", "NX"]`.
    command: func(binding: string, args: list<list<u8>>, options: options)
        -> result<reply, error>;

    /// Several commands in one round trip, not atomic: one result each.
    pipeline: func(binding: string, commands: list<list<list<u8>>>, options: options)
        -> result<list<result<reply, string>>, connect-error>;

    /// Several commands as `MULTI` ... `EXEC` on one connection: run
    /// together, with no other client's command between them. As in
    /// Redis, a command that fails at run time does not undo the others.
    atomic: func(binding: string, commands: list<list<list<u8>>>, options: options)
        -> result<list<result<reply, string>>, connect-error>;
}
```

Refused with `not-allowed` before anything is sent, because they would
block a pooled connection or change it for the next caller: blocking
commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`,
`BZMPOP`, `XREAD` and `XREADGROUP` with `BLOCK`, `WAIT`, `WAITAOF`), the
subscribe family, `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`,
`SELECT`, `AUTH`, `HELLO`, `CLIENT`, `MONITOR`, `RESET`, `QUIT`, and the
administrative ones (`CONFIG`, `DEBUG`, `SHUTDOWN`, `REPLICAOF`,
`FLUSHALL`, `FLUSHDB`, `SCRIPT KILL`, `FUNCTION KILL`). Scripts
(`EVAL`, `FCALL`) are allowed: they are how Redis does a conditional
write atomically, and they are bounded by the call's timeout like any
command, after which the connection is closed. Cluster mode (`MOVED`,
`ASK`) is not followed in the first slice.

#### MySQL and NATS

MySQL gets the shape of `postgres` (`query`, `exec`, `batch`, later
`transaction`), with its own value type and errors carrying the MySQL
error number and SQLSTATE; its WIT is written when it is built. NATS
gets `publish(binding, subject, headers, payload)`, which returns once
the server has the message (after a flush for core NATS, after the
stream's acknowledgement when the subject belongs to a JetStream stream),
and `request(binding, subject, headers, payload, options)`, which waits
for one reply.

### 5. When a connector can be called

- In any event of an actor (`on-init`, requests, calls, alarms, socket
  events) and in any front request.
- **Each call counts against the event's wall clock, not its CPU
  budget**, like outbound HTTP and `actors.call`. Its timeout is the
  smaller of the one it asks for and the event's remaining wall less the
  margin ADR 0025 keeps for answering. A call started with less than that
  margin left fails with `timeout` at once.
- **Calls run one at a time** within an event, as outbound requests do
  without the SDK's batch helper. `call-many`-style batches across
  bindings are not provided; `pipeline` and `batch` are the batches.
- **An event stopped** (wall clock, CPU, trap) cancels its call: a
  Postgres `CancelRequest` for the running statement, and the connection
  is closed rather than returned to the pool. Its transaction is rolled
  back by the server when the connection closes.
- **An actor's call is refused with `not-allowed` unless the node holds
  the actor** (ADR 0020), checked when the call starts. That is the same
  check the blob writes of the files design make, and the same limit:
  **it is not fencing.** A node whose hold lapses during a call finishes
  it, and the external database has no way to know. Section 10 says what
  that means for writes. A front has no hold and is not checked.

### 6. Transactions across calls (slice 3)

`batch` covers what Neon's HTTP mode, Hyperdrive's transaction mode and
ADR 0029 cover: several statements, one transaction, no code between
them. Most writes that must be atomic fit it, and it holds a connection
only for the time the server takes.

An interactive transaction (`begin`, then reads and writes with code
between them, then `commit`) holds one pooled connection, and the row
locks it took, for as long as the guest takes. Inside an event that can
be up to 30 s, including time spent waiting on `actors.call` or outbound
HTTP. A pool of 5 per node could be held by five slow events. So:

- **It comes after `batch`**, in its own slice, if apps show they need it.
- **One open transaction per event** (across bindings): a second `begin`
  is `not-allowed`, which also rules out an event deadlocking itself on
  two connections of one pool.
- **A transaction lives at most 5 s** from `begin` (a binding setting,
  `transaction-timeout-ms`, at most 10 s). Past it the host rolls back,
  returns the connection, and every later call on it fails with
  `timeout`.
- **Dropped without `commit`**, or still open when the event ends or
  traps, it is rolled back, like an open SQLite transaction.
- **A commit whose answer is lost** returns `unknown-outcome`: the
  transaction may be kept.
- While it is open, the actor may still call other actors. If one of
  them waits on a lock the transaction holds, it waits until the
  transaction ends or the timeout: bounded, and the docs say to keep
  transactions free of calls.

### 7. Pools

- **One pool per node, per app, per binding.** Connections are opened on
  first use, at most `pool` at once (default 5, at most the node's
  `--max-connections-per-binding`, default 32), and closed after 60 s
  idle, so a quiet cluster holds none. The total a database sees is up
  to **nodes × `pool`** per binding; the guide says so, and points at
  PgBouncer or the provider's pooler (Neon's, Supavisor) when that is
  too many.
- **Checkout waits** for a free connection within the call's timeout;
  past it, `unavailable("pool busy")`. A failure to connect is
  `unavailable`, nothing having been sent, and is not retried by the
  host.
- **A connection is reset before reuse**: for Postgres, `DISCARD ALL`
  (which also releases session advisory locks) before it returns to the
  pool, after every call. A check of the SQL text cannot tell which calls
  change session state (`SELECT set_config(...)`,
  `SELECT pg_advisory_lock(...)` look like any query), so the reset is
  unconditional: one more round trip per call, pipelined with the
  call's own messages where the driver allows. Session state never passes
  from one actor to another. For Redis, nothing a caller may send changes
  the session, since `SELECT`, `CLIENT` and `HELLO` are refused. A connection whose call failed in a
  way that leaves it unknown (a timeout, a cancel, a protocol error) is
  closed, never reused.
- **A secrets change** that changes a binding's URL builds a new pool;
  the old one finishes the calls it is running and closes. A deploy that
  changes `pool` or `classes` takes effect at the next event, as access
  rules do.
- **Reads can be stale** to the actor's own state, and to each other
  across connections: a read on one connection does not see a write
  still in flight on another. Replicas and read-your-writes across
  bindings are the database's business.

### 8. Limits

Per call, enforced by the host while it reads the answer:

| Limit | Default | Over it |
| --- | --- | --- |
| Rows in an answer | 10,000 | `too-large`; the statement is cancelled and the connection closed |
| Bytes in an answer | 8 MiB | The same |
| Request (SQL, parameters, commands) | 4 MiB | `too-large`, nothing sent |
| Statements in a `batch`, commands in a `pipeline` or `atomic` | 100 | `too-large`, nothing sent |
| Timeout | 5 s (Postgres, MySQL), 2 s (Redis, NATS) | `timeout` |
| Calls in flight per node, all bindings | 256 | `unavailable("node busy")` |

Bindings may lower the row and byte caps (`max-rows`, `max-bytes` in the
`[connections]` table), not raise them past the defaults, which keep an
answer within what one call to an actor carries (16 MB). Decoding rows is
host work not charged to any CPU budget, as for outbound HTTP; the caps
are what bound it.

### 9. Security

- **The node's egress policy applies to every connection.** The host
  parses the URL, resolves the name itself, keeps only the addresses
  outbound HTTP may reach (public ones, plus the operator's
  `--allow-outbound` ranges), and connects to the checked address, as
  `egress.rs` does today. A database on the private network therefore
  needs the operator to open its range, which is the point: whoever sets
  the app's secrets cannot point a binding at the node's metadata
  endpoint or another node. The blocked-ports list of outbound HTTP does
  not apply (it guards against an HTTP request driving another
  protocol's server; a connector speaks its own protocol), so
  `--allow-outbound-port` is not needed for 5432 or 6379.
- **TLS is verified by default**, against the host name in the URL and
  the built-in roots plus `--outbound-ca`. Whatever the URL asks, a
  weaker mode (`sslmode=require`, `prefer`, `allow`) is treated as
  `verify-full`. Plain TCP (`sslmode=disable`, `redis://`, `nats://`
  without TLS) is accepted only to an address inside an
  `--allow-outbound` range: a private network the operator opened, never
  the internet.
- **Credentials stay in the node.** The URL is opened only by the
  connector; it is not in `env.get`, the logs (which name the binding and
  the host, never the user or password), the metrics or an error message.
  Errors from the server are passed on as they come; an error raised
  while connecting is reduced to its kind (refused, TLS, authentication)
  and the host, so no part of the URL reaches the guest.
- **SQL comes from code, never from a client**, as in ADR 0029: the
  connector runs what the guest sends, with the database user's rights.
  Give each binding a database user with only the rights its classes need.
- **Anyone who can deploy the app** can use its bindings from any listed
  class, and change which classes are listed. Anyone who can set its
  secrets can point a binding anywhere the egress policy allows. Neither
  can read a URL back with `wasmkeep secrets` (values are never printed).

### 10. Failure model

These are the semantics to state plainly in the guide:

- **Outside WasmKeep's durability.** A write to an external database is
  durable when that database says so. It is not part of the actor's
  SQLite transaction, is not undone when the actor's event fails or its
  transaction rolls back, and is not replicated or restored by WasmKeep.
  A restore of the bucket from a backup does not bring an external
  database back to the same moment.
- **No two-phase commit.** Nothing makes an actor's SQLite write and an
  external write happen together. Either can land without the other.
- **At least once.** An event can run again: a client retries a `503`
  that may have run ([failover](../failover-and-scaling.md#what-callers-get-meanwhile)),
  an alarm is retried until it succeeds
  ([alarms](../concepts.md#alarms)), a workflow step is retried
  ([workflows](../workflows.md#two-rules)), and after a node dies its
  actor's event may run again on another node. Its external writes then
  happen again. They need an **idempotency key**: a unique column and
  `INSERT ... ON CONFLICT DO NOTHING` in Postgres, `SET key value NX` in
  Redis, a `Nats-Msg-Id` header within the stream's duplicate window in
  JetStream. A workflow's `step_keyed` already hands out such keys.
- **No fencing.** After a node loses its hold, an event still running
  there can write to the external database while the actor already runs
  on another node (section 5). For writes where that matters, the
  database must decide: a version column compared in the `UPDATE`, or a
  row the actor owns written only with the key of the event.
- **Unknown outcomes are real.** `timeout` on a write and
  `unknown-outcome` mean the write may have been kept. Retry only with an
  idempotency key.
- **Reads can be stale** relative to the actor's own state: the actor's
  SQLite and the external database are two systems, with no snapshot
  across them.

The pattern the guide teaches for a write that must follow the actor's
state: record the intent in the actor's SQLite with `exec-durable`, with
its key, in the event that decides it; send it from `on-alarm`, keyed;
mark it sent. A failure anywhere repeats a keyed send, never loses one.

### 11. Metrics

With `tenant` and `app`, like the other series of an app's work:

- `wasmkeep_connect_calls_total{binding,kind,outcome}`: `ok`, `error`
  (the server answered an error), `not_allowed`, `unavailable`,
  `timeout`, `unknown_outcome`, `too_large`.
- `wasmkeep_connect_call_seconds{binding,kind}`: a histogram, checkout
  included.
- `wasmkeep_connect_pool_connections{binding,state}`: `idle`, `in_use`.
- `wasmkeep_connect_pool_wait_seconds{binding}`: time waiting for a
  connection.
- `wasmkeep_connect_connects_total{binding,outcome}`: `ok`, `refused`
  (egress policy), `tls`, `auth`, `failed`.

### 12. Where the code goes

The WIT in `crates/wasmkeep-wit` first (and its copies). The runtime
declares the host functions and calls a trait for the work, as it does
for outbound HTTP; the connectors live in `wasmkeep-server`
(`connect/`, beside `egress.rs`), since `wasmkeep-runtime` never imports
`tokio::net`. The pool is the node's, shared by every instance of the
app. Driver crates (`tokio-postgres`, a Redis client, a MySQL client, a
NATS client) are new dependencies, each justified in its PR; the egress
policy decides the address before the driver connects, so the driver is
given a connected, checked socket rather than a host name.

### 13. SDKs

Rust and TypeScript first, as for the database client of ADR 0029, with
the same verbs:

```rust
use wasmkeep_sdk::{pg, params};

let rows = pg::query("main", "SELECT id, total FROM orders WHERE customer = $1", &params![customer])?;
pg::batch("main", &[
    pg::exec_stmt("INSERT INTO orders (key, customer, total) VALUES ($1, $2, $3) ON CONFLICT (key) DO NOTHING",
                  &params![key, customer, total]),
    pg::exec_stmt("UPDATE customers SET orders = orders + 1 WHERE id = $1", &params![customer]),
])?;
```

```ts
import { pg, redis } from "@wasmkeep/sdk";

const rows = pg.query("main", "SELECT id, total FROM orders WHERE customer = $1", [customer]);
redis.command("cache", ["SET", `session:${id}`, token, "EX", "3600"]);
```

Rows come back as arrays with named columns, as in ADR 0029; values map
to the language's types (`bigint` for `int64` in TypeScript). The other
SDKs follow; the API reference test lists them as missing until then.

### 14. `wasi:sockets` later, per class

Weighed honestly: wasmCloud 2 and Cloudflare Workers both give guests TCP
behind a policy, and Rust can now drive it. It would reach any protocol
without a connector per protocol, and let unmodified Rust drivers run.
Against it, in WasmKeep: a connection lives in one instance and dies at
hibernation, trap or move; the credentials are in the guest; the
connections multiply with resident actors; and TypeScript, the default
second language, has no driver to use it. It may come later for
protocols with no connector, **per class, behind an explicit grant** in
the project file (`sockets = ["tcp"]` with destinations only within the
node's egress policy, or only to an operator-run proxy), and only with
the same resolve-check-connect the egress policy does. Not before WASI
0.3 is linked: its sockets differ from 0.2's, and the host would carry
both otherwise.

### 15. Compatibility

- **A capability word, `connect`.** Nodes announce it in their
  heartbeat once they understand `[connections]` and the `_connection`
  scope. A secrets write whose resulting set has a `_connection` entry,
  and a deploy whose version has `[connections]` (a new manifest line,
  `connection <binding> <kind> <pool> <class,...>`), are refused while a
  live node of the app does not announce it. There is no `--force`: as
  ADR 0023 found for `_front`, one entry an older node cannot check makes
  it fail to open the whole set, refuse every event and drain.
- **The package is additive.** A component built before imports nothing
  from `wasmkeep:connect` and links as it did; one that imports it is
  refused by an older node at link time, as any unknown import is.

## Consequences

- An app reaches a self-hosted or managed PostgreSQL or Valkey, and
  later MySQL and NATS, from any SDK that binds the interface, with no
  credential in its code and one pool per node instead of one connection
  per actor.
- The operator's egress policy stays the ceiling for every connection:
  opening a private database means opening its range.
- The guide states what WasmKeep does not do for those writes: no
  durability of its own, no transaction with the actor's state, at least
  once, no fencing. Apps write keyed, idempotent external writes, and
  the SDK patterns (alarms, workflows) are how they retry.
- A new secrets scope and a new manifest line are compatibility points,
  gated by the capability word `connect` (section 15).
- Driver dependencies enter `wasmkeep-server`, one per protocol, each
  with its own release cadence and security advisories to follow.
- A database sees up to nodes × `pool` connections per binding, and the
  guide must say it.

## Alternatives

- **HTTP only.** No platform change, and enough for managed services
  built for serverless. It leaves out every self-hosted database and
  broker, interactive Postgres transactions, Valkey and NATS, and SDKs
  other than JavaScript (the vendors' HTTP protocols are documented as
  drivers, not APIs). It also keeps every credential in the guest. Kept
  as the general path (section 1), not as the only one.
- **`wasi:sockets` for every actor.** Section 14: connections that die
  with the instance, one per resident actor, credentials in the guest,
  no driver for TypeScript. Possible later per class.
- **A sidecar proxy run by the operator**: PostgREST in front of
  Postgres, an HTTP gateway for Redis or NATS, reached with outbound HTTP
  and `--allow-outbound`. It works today, with no change, and is what the
  guide recommends until connectors ship. Against it as the answer: one
  more service to run, secure and pool; another hop; a different API per
  gateway (no SQL through PostgREST); and the gateway's token still in
  the guest.
- **Guest-supplied addresses, as in Spin.** Simpler to configure, but
  the guest holds the credentials and an allowlist of hosts is the only
  check. wasmCloud 2, by default, refuses connection addresses and
  credentials supplied by a workload.
- **One connection per actor, kept by the host across events.** It
  solves hibernation but not the count (a thousand actors, a thousand
  connections), and a move to another node still breaks it.
- **Standard interfaces only** (`wasi:keyvalue`, `wasi:messaging`,
  `wasi-sql`). Portable, but SQL is at phase 1, key-value
  covers a fraction of Redis, and messaging is at phase 2. A shim over the connectors is
  left open (Not decided here).
- **The database kept by one actor (ADR 0029) for everything.** It is the
  answer for the app's own shared tables, not for a database other
  systems use, larger than 1 GB, or with many writers at once.

## Not decided here

- **NATS subscribe, and Redis or Postgres notifications.** Messages that
  wake actors need a consumer on the node: which node consumes (one per
  cluster, by lease, or each node a share), how a message maps to an
  actor address, delivery as a new kind of event, acknowledgement after
  the event succeeds (JetStream's ack, at least once), redelivery,
  ordering per actor, backpressure into hibernated actors. A design of
  its own, after publish.
- **An allowlist of hosts in the project file** (`hosts = ["*.neon.tech"]`),
  checked against the URL. Left out because it is environment-specific
  and checked by the people who set the URL; reconsider if someone other
  than the app's owner sets its secrets.
- **Exposing the hold's generation** (ADR 0020) to guests, so an app can
  fence its own external writes with it (`WHERE generation <= $1`).
- **Arrays, composite types, ranges** in the Postgres value type, and
  streamed rows past the caps.
- **A Spin-compatible WIT** (`spin:postgres`, `spin:redis`) as a shim
  over the connectors, for components written for Spin. Its `open` takes
  an address the guest holds; the shim would have to map it to a
  binding.
- **`wasi:keyvalue`** (phase 2) over a Redis binding, for portability of
  simple get, set and compare-and-swap.
- **Kafka, AMQP, MongoDB** and other protocols: by demand, as connectors
  or as sockets (section 14).
- **Limits per app** when several apps share nodes (0.5.0): pools and
  calls in flight then need a share per app.
- Redis Cluster and Sentinel, Postgres read replicas, and failover
  between several URLs of one binding.

## Tests (to build)

- **Local services in CI.** A job with a PostgreSQL and a Valkey service
  container, and later MySQL and NATS. The tests read
  `WASMKEEP_TEST_POSTGRES_URL` and `WASMKEEP_TEST_REDIS_URL`;
  `./scripts/check.sh --connect` starts the containers with Docker when
  it is installed and the variables are not set, and is skipped with a
  note otherwise, like `--s3` with moto.
- **Postgres** (`tests/connect_postgres.rs`): every value type in and
  out, `NULL` in every type, `numeric` and `timestamptz` exact; SQLSTATE
  on a unique violation; a batch rolled back whole at its third
  statement; `BEGIN`, `SET`, `LISTEN` refused; the row and byte caps
  cancel the statement; a timeout cancels it (`pg_stat_activity` shows
  it gone); `SET LOCAL` does not leak to the next caller; a session
  advisory lock taken in a call is gone at the next one.
- **Redis** (`tests/connect_redis.rs`): a command, a pipeline with an
  error in the middle, `atomic`, a nested reply (`XRANGE`) rebuilt by both
  SDKs, every refused command.
- **Pools**: reuse within the cap, `pool busy` past it, idle close,
  a new pool after a secrets change while calls run on the old one.
- **Policy**: a URL to `127.0.0.1` or `169.254.169.254` refused; a
  private address allowed only with `--allow-outbound`; `sslmode=disable`
  to a public address refused; a certificate for another name refused;
  `--outbound-ca` trusted; a class not listed refused; the URL in no log
  line, metric or error (a test greps them for the password).
- **Failures**: a proxy in the test that cuts the connection after the
  commit is sent (`unknown-outcome`), before the statement (`unavailable`),
  and stalls (`timeout`); the database down at the first call.
- **Cluster** (`tests/cluster/connect.rs`): a node killed mid-event, the
  event run again on another node, and the keyed write landing once.
- **SDKs**: Rust and TypeScript against the same fixtures; the examples
  `examples/pg` and `examples/ts-pg` built in CI.
- **No `sim/` case**: nothing in leases, replication or deploys changes.
  The hold check reads the hold as blobs will, and the failure model is
  that of outbound HTTP.

## Plan

| Slice | Size |
| --- | --- |
| This record | S |
| `[connections]`, the `_connection` secrets scope, the capability word | M |
| WIT `wasmkeep:connect`, Postgres `query`, `exec`, `batch`, the pool, the egress check, metrics, Rust and TypeScript SDKs, CI services | L |
| Redis and Valkey | M |
| Postgres transactions across calls | M |
| MySQL | M |
| NATS publish and request | M |
| Adversarial review of the security and failure sections | S |
