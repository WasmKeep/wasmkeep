# Concepts

What an actor is, what happens to it over its life, and what WasmKeep
promises about it. Read [What is WasmKeep?](overview.md) first for the why;
this page is the how. Exact signatures are in the
[API reference](reference/api.md).

## Addresses

An actor's address is `tenant/class/name`:

- **class**: one compiled component, given a name when it is served
  (`--actor chat=chat_room.wasm`) or deployed.
- **name**: 1 to 256 bytes of any text without `/` or control characters,
  other than `.` and `..`. Every distinct name is a separate actor with its
  own database. There is no "create" step: the first event to a name creates it.
- **tenant**: the owner of apps (a team, or a client of the operator; not
  a customer of your app, which is an actor name). A node started with
  `--actor` serves the tenant `default`; `--app acme/shop` serves app
  `shop` of tenant `acme`. What an app is, and what is planned for it, is
  in [ADR 0021](adr/0021-the-app-model.md).

Class and tenant are identifiers: lowercase letters, digits and `-`,
starting with a letter, up to 64 bytes (`[a-z][a-z0-9-]{0,63}`). So is
the app name in `--app TENANT/APP`: the first `wasmkeep deploy` of an app
refuses a tenant or app name outside it. An app deployed before that rule
keeps its name. Only the first deploy checks it: `wasmkeep secrets set`
before that accepts any name, so set secrets after the first deploy, or
check the name yourself.

Over HTTP, an actor lives at `/actor/<class>/<name>`, and anything after the
name is the path it sees:

```text
GET /actor/chat/lobby/history?limit=20
    → class "chat", actor "lobby", which sees GET /history?limit=20
```

Names are URL-decoded (`room%201` is the actor `room 1`). The path an actor
sees has its address removed; it reads its own address with
`actors.self-address` (`actors::self_address()` in Rust, `actors.self()` in
TypeScript), for links to itself or anything keyed by its name.

**Who may create an actor.** By default anyone who can reach the server
can create an actor by requesting a new name. For classes whose actors
another actor manages, serve them with `--access CLASS=existing`: outside
requests then reach only actors that exist, and a new name answers 404
without creating anything (no database, no lease, nothing in the bucket).
Actors create them with `actors.call`. `--access CLASS=existing:main,admin` does the same but lets outside
requests create those names (a class's well-known actors).
`--access CLASS=internal` refuses
outside requests altogether (403): only actors and alarms reach the class.
See [ADR 0010](adr/0010-who-may-create-an-actor.md). An app deployed with
`wasmkeep deploy` carries its rules in the deploy instead
(`wasmkeep deploy --access CLASS=MODE`), so every node applies the same ones
with the code they protect ([Running a cluster](cluster.md#access-rules-and-configuration)). Paths outside
`/actor/` go to the optional front, a stateless component given to `serve`,
except `/_wasmkeep/`, which the server keeps for itself (`/_wasmkeep/health`
and `/_wasmkeep/ready` for load balancers, see [Monitoring](monitoring.md)).
`/actor/` never reaches the front, also on a node that serves no actors
(404).

## The front

The front ([ADR 0023](adr/0023-the-front.md)) is a plain `wasi:http`
component, the `wasmkeep:actor/front` world: a fresh instance per request,
no state. It routes, signs users in and renders pages, and calls the
node's actors with `actors.call`:

- **An inside caller.** Its calls reach `internal` classes and create
  actors of any class, as an actor's do. The callee sees
  `x-wasmkeep-caller: <tenant>/_front/main` (`default/_front/main` for
  `serve` without `--app`). `_front` is never a class, so nothing can call
  the front back, and `actors.call("_front/main")` fails with
  `invalid-address`. Validate names taken from a URL before calling: each
  new name is a new actor and a new database.
- **Its values.** `env.get` reads the files given with `serve --front-env
  FILE` (`NAME=value` lines, as `--env` for a class). `self-address`
  returns `<tenant>/_front/main`.
- **The host's headers are the host's.** Every `x-wasmkeep-` header or
  trailer a client sends is removed before the front sees the request, and
  every one the front sets on its response is removed before the client
  gets it. On
  `actors.call`, the host drops every `x-wasmkeep-` header a guest set
  (actors too), but the four the SDKs' key-value and queue clients send
  (`x-wasmkeep-kv-expect`, `-kv-ttl-ms`, `-kv-durable`,
  `x-wasmkeep-queue-delay-ms`), and sets the actor and caller itself.
- **Limits.** A 30 s wall per request, response included. A call gives
  up with `timeout` while 5 s of that wall remain, or after the actors'
  call limit, whichever comes first, so the front can still answer. At most
  `--max-front-requests` front requests run at once (by default a tenth of
  `--max-instances`; resident actors get the rest of the pool); past it the
  node answers 503 with `Retry-After: 1` without running the front. At most
  a quarter of those may wait on one actor at once; past it the call fails
  with `failed("front busy: ...")`. The front's CPU time per request is
  `--front-cpu-budget-ms` (50 ms), apart from the actors'. A `1xx` status
  from the front becomes a 500. A client that leaves before the end of its
  answer stops the front's instance and frees its slot; one that sends its
  body slowly holds the slot up to the wall, so put a proxy that buffers
  request bodies in front of the node (nginx does by default, see [the
  front door](deploy/front-door.md)).
- **Upgrades.** A WebSocket upgrade outside `/actor/` reaches the front as
  a plain GET; reach sockets at `/actor/<class>/<name>`.
- **Where it runs.** On the node that received the request, on the public
  port only: it is never forwarded, and the cluster port does not serve
  it. Its calls to an actor on another node are signed hops, as an
  actor's.

With `serve --app`, a component on the command line is refused: the front
is to come with the app's deploy, so that a rollback reaches it too
(planned, ADR 0023).

## Events

Everything that reaches an actor is an **event**: an HTTP request, a
WebSocket open, message or close, an alarm, or a call from another actor.

- An actor handles **one event at a time, in arrival order**. Its code never
  runs concurrently with itself, so it needs no locks.
- Different actors run in parallel. Throughput comes from many actors, not
  one busy actor: split hot state (one actor per room, not one per app).
- Each actor has a queue of up to 1,024 waiting events. A request that
  finds it full is answered `429 Too Many Requests`. A request received
  whole (no body, or one that arrived in the first read) whose client
  closes its connection while it waits is not run, nor
  an actor's call whose caller timed out meanwhile (nobody would get the
  answer, and nothing it would write was acknowledged); one that has
  started runs to its end.
- A node that cannot take an actor for now (the bucket does not answer, or
  the actor already ran a newer deploy than this node has, which it is
  fetching) answers `503 Service Unavailable` with `Retry-After: 1`: retry.
  A fault a retry will not fix (an unreadable ownership record, a history
  that cannot be restored) is a `500`.
- An address without a mailbox needs resident capacity, including a new,
  hibernated or failed-over actor. At capacity, admission waits up to 250 ms
  for an idle actor to hibernate and close its resources. If none becomes
  free, HTTP and local `actors.call` receive `503 Service Unavailable` with
  `Retry-After: 1` before their event runs. A request for an actor another
  node owns is relayed without reserving a local slot when this node knows
  the owner (learned in the last second, or read from the actor's ownership
  record); if the record cannot be read in time, the request goes through local
  admission like any other. A due alarm waits for capacity like a request,
  then retries with backoff and jitter. Each request that waits evicts at
  most one idle actor, however often it is woken.
- A WebSocket message waking an evicted actor waits for capacity up to the
  configured event wall limit. Sustained exhaustion closes it with 1013.
  Close callbacks remain pending through capacity or queue pressure; pending
  cleanup continues to count against socket limits until delivered or the
  service shuts down.
- An event may use **50 ms of CPU** (`--cpu-budget-ms`) and **30 s of wall
  time**, including time waiting on storage, calls and outbound HTTP. Time
  in host calls and time the OS spends on other threads do not count
  against the CPU budget. An event over either limit is stopped.
- SQL has its own CPU budget: **2 s per event** for all of the event's
  statements together (`--sql-cpu-budget-ms`). Past it, the running
  statement stops with `quota-exceeded` and later ones are refused until
  the event ends.
- An event that traps or is stopped **fails**: a request gets a `500`, a
  transaction it left open is rolled back, and the actor's instance is
  discarded (the next event gets a fresh one, with the same database).

## Life cycle

```text
first event ─▶ on-init ─▶ resident ──idle 30 s──▶ hibernated
                  ▲          │  ▲                      │
          new version        │  └──── next event ◀─────┘
          deployed           ▼
                        delete-self ─▶ gone (the next event starts afresh)
```

- **`on-init`** runs before the first event an instance handles with a given
  component version: once for a new actor, and again after each deploy. Use
  it to create and migrate tables (`CREATE TABLE IF NOT EXISTS`, then
  `ALTER TABLE` guarded by a version you keep in a table). If it fails, the
  event fails and `on-init` is tried again with the next event.
- **Resident**: the instance stays in memory between events, so in-memory
  variables survive from one event to the next. Treat them as a cache: they
  are lost on hibernation, a crash, a deploy or a move to another node.
- **Hibernation**: after `--idle-timeout-secs` (30 s) without events, the
  instance is dropped. Its database, its pending alarm and its WebSockets
  stay. The next event wakes it in about a millisecond.
- **`delete-self`** removes the actor once the current event ends: its
  database and alarm are gone, and the next event to the address starts a
  new actor.

## Storage

Each actor owns one SQLite database. Nothing else can read or write it.

- `exec` runs a statement and returns the rows changed; `query` returns
  rows. One statement per call, with `?` parameters.
- Outside a transaction each statement commits on its own. `BEGIN` ...
  `COMMIT` across several calls is allowed **within one event**; a
  transaction still open when the event ends is rolled back.
- `exec` returns once the write is committed locally. **`exec-durable`**
  returns only once it is durable: replicated to the bucket in a cluster,
  fsynced on a single node. Use it when you are about to tell someone "done"
  (a payment, an order). It costs about a millisecond more locally, and a
  bucket round trip in a cluster.
- Inside a transaction, `exec-durable` takes only the `COMMIT` (or `END`):
  run the transaction's statements with `exec`, then
  `exec-durable("COMMIT")` makes the whole transaction durable. Any other
  statement there is refused with `not-allowed` before it runs, since
  nothing in a transaction is durable before its commit.
- A database is capped at 1 GB by default; past that, writes fail with
  `quota-exceeded` and reads still work.
- Values (text and blobs) are capped at 16 MB, and so is everything one
  `query` returns; past that, `quota-exceeded`. Page through big results.
- A statement runs at most as long as the event may (30 s by default);
  past that it is stopped with `quota-exceeded`.
- One event may write about 256 MB (counting every rewrite of a page);
  past that, writes fail with `quota-exceeded` until the event ends. Split
  bulk work across events, for example with an alarm.
- Sandboxed: no `ATTACH`, no extensions, no pragmas that change state or
  touch files, no `TEMP` tables, views or triggers (they would escape the
  size cap). Virtual tables: full-text search (`fts3`, `fts4`, `fts5`) and
  `rtree` only.

## Alarms

Each actor has **one** pending alarm: `set-alarm(at)` replaces it. When it is
due, `on-alarm` runs as an event.

- Alarms survive hibernation, restarts and moves between nodes. In a
  cluster, if an actor's node dies, another node fires its alarm a lease
  period plus grace after it was due, plus up to 10 s, plus restoring the
  actor (with the defaults, 24 s late at the median and 36 s at worst
  with nodes next to their bucket, over twelve hours,
  [measured](benchmarks.md#three-nodes-against-a-real-bucket) on
  [one lease per node](adr/0020-one-lease-per-node.md);
  [ADR 0009](adr/0009-alarms-in-a-cluster.md)). An alarm is as
  durable as the rest of the event that set it.
- They fire **at least once**. A failed or trapped `on-alarm` is retried
  after 1 s, doubling up to 5 minutes, so make handlers idempotent (record
  what you did in the database, in the same event). While it is being
  retried, `get-alarm` returns the next attempt's time.
- For several timers, keep them in a table and set the alarm to the
  earliest; reschedule from `on-alarm`.

## Calls between actors

`actors.call("class/name", request)` sends an HTTP-shaped request to another
actor of the same tenant and waits for the answer.

- The caller's event stays open meanwhile: other events to the caller wait.
- The callee sees `x-wasmkeep-caller: tenant/class/name` (the name
  percent-encoded: `room 1` arrives as `room%201`), which outside
  clients cannot forge, so it can trust who is calling. A request
  without it comes from an outside client and should get only what any
  client may do. In a cluster that includes calls from a node still
  talking plain HTTP to one that refuses it (a rollout done in the wrong
  order, see [cluster](cluster.md)): treat them like any client's.
- A call that would come back to an actor already waiting in the chain (A
  calls B calls A) is refused with `reentrant` instead of deadlocking.
- Request bodies are capped at 16 MB (413 when declared larger; a failed
  read past it otherwise). Calls between actors cap both ways. A handler
  may answer without reading the body: the server reads and discards what
  is left (up to the cap, for at most 30 s), so the client can finish
  sending it and read the answer. The exception is a client that sent
  `Expect: 100-continue` straight to the actor's node, when the handler
  read nothing of the body: the answer comes without the 100, and the
  connection closes after it. A client that waited for the 100 has sent
  nothing and loses nothing. One that sends the body anyway, without
  waiting, is not read out: its upload is cut off (a broken pipe or a
  reset), which can cost it the answer, unless the body was small enough
  to have arrived whole already. Through another node there is no such
  case: the node that forwards the request sends the 100 itself, and the
  body is read and discarded like any other.

## WebSockets

A client upgrades a `GET` on the actor's path. The **host** owns the socket,
not the instance, so connections stay open while the actor hibernates; the
next message wakes it.

- `on-websocket-open` may refuse a connection (it is closed with code 1008
  and the guest's reason). Until it accepts, the socket is invisible to the
  actor's other events: no broadcast reaches a socket it may refuse. It
  runs before the client sees the upgrade's 101, so once a client is
  connected, every later event of the actor (a broadcast, say) sees its
  socket.
- `send` queues a message; a client that does not keep up gets
  `backpressure` (at 256 messages or 4 MiB queued) and that message is
  dropped for it.
- `connections` lists the actor's sockets, so broadcasting is a loop.
- Limits: messages from clients up to 1 MiB (larger closes the socket with
  1009); 1,000 sockets per actor and 50,000 per node (more get 503); a
  client silent for 25 s is pinged, and closed if it does not answer within
  30 s.
- In a cluster, a client may connect to any node: the socket is relayed to
  the actor's node. If the actor moves, its sockets close with code 1012 and
  clients reconnect.

## Clusters

Several nodes can share one S3-compatible bucket. Each actor then runs on
exactly one node at a time, recorded in the bucket and kept by that node's
own lease; requests that reach another node are forwarded, and each actor's
writes are replicated to the bucket after every event. A node that stops
cleanly hands its actors over at once; if a node dies, they move to another
node in about 15 to 21 s, with no acknowledged durable write lost. See
[Running a cluster](cluster.md) for how it works,
[When a node dies, and how it scales](failover-and-scaling.md) for what
callers see meanwhile and what adding nodes does, and
[Deploying](deploy/three-nodes.md) for a setup.

## Configuration and secrets

An API key, a webhook URL or a feature flag reaches actors through the `env`
interface, never through the component: the same build runs in staging and
production. The operator writes one file per class, `NAME=value` per line:

```bash
# /etc/wasmkeep/hub.env, readable only by the server's user (chmod 600)
ADMIN_TOKEN=change-me
WEBHOOK_URL="https://hooks.slack.com/services/..."
```

and serves the class with it: `wasmkeep serve --actor hub=hub.wasm --env
hub=/etc/wasmkeep/hub.env` (also `wasmkeep dev`). Actors read it with
`env::get("ADMIN_TOKEN")` (Rust), `env.get(...)` (TypeScript and Python),
`wk_env(...)` (C), `wk.env(...)` (Zig) or `wasmkeep.Env(...)` (Go), and get nothing for a name that is
not set.

- **Per class.** A class sees only its own files; every actor of the class
  sees the same values. `--env` for a class that is not served is an error
  (a typo would otherwise leave a class without its token).
- **On the node.** `--env` values are read once at start (restart to
  change them), are never written to the data directory or the bucket, and
  never appear in logs: a malformed line is reported by file and line
  number only. A file anyone on the machine can read is accepted with a
  warning. In a cluster, give every node the same files, or keep secrets in
  the bucket instead (below).
- **Plain configuration can travel with a deploy.** For an app deployed
  with `wasmkeep deploy`, values that are not secret (URLs, flags) can go
  with the version instead: `wasmkeep deploy --config hub=hub-config.env`.
  They are stored in the bucket in the clear, reach every node with the
  code, and change without a restart (the next event runs in a new
  instance; `on-init` does not rerun) ([Running a cluster](cluster.md#access-rules-and-configuration)).
- **Secrets can live in the bucket, sealed.** `wasmkeep secrets set --app
  TENANT/APP --key FILE --class CLASS NAME` (the value from stdin) seals a
  secret with a key the nodes hold (`wasmkeep serve --secrets-key FILE`).
  It reaches every node within a few seconds, with no deploy or restart,
  and a rollback of code never brings an old secret back. A bucket reader
  sees neither names nor values ([Secrets in the bucket](cluster.md#secrets-in-the-bucket),
  [ADR 0016](adr/0016-secrets-in-the-bucket.md)).
- **Which value wins**, by name: the deployed configuration, then the
  app's secrets, then the node's `--env` (logged, never the value).
- **Required values.** `wasmkeep deploy --requires CLASS=NAME,...` makes a
  class refuse its events (503) while one of those names is not set,
  rather than run without it.
- **The format** is what systemd's `EnvironmentFile=` reads: `#` comments,
  an optional `export `, one pair of quotes around the value removed,
  nothing inside interpreted (no escapes, no `$VAR`), a name set twice keeps
  the later value. Names are letters, digits and `_`; values cannot hold
  control characters other than tab.

The WASI environment (`std::env::var`, `process.env`) stays empty.

## What an actor cannot do

Read files, open sockets, see the node's environment variables, or reach another
tenant's actors. Many toolchains import `wasi:filesystem` and
`wasi:sockets` whether or not the code uses them, so components that do
still load, but those interfaces reach nothing: no directory is opened to
the guest, and every socket and name lookup is refused (tested from inside
a guest). The only way out is outbound HTTP.

Outbound HTTP reaches **public addresses only**. Loopback, private networks
(`10/8`, `172.16/12`, `192.168/16`), IPv6 outside global unicast,
link-local (including cloud metadata at `169.254.169.254`), shared and
reserved ranges are refused, also when a host name resolves to them: the
host resolves the name, checks the address and connects to that address.
A refused address fails with `destination-ip-prohibited`; a name that
resolves only to refused addresses fails like a name that does not resolve
(so refusals cannot map internal names). An operator who wants actors to
reach an internal API allows its range explicitly:
`wasmkeep serve --allow-outbound 10.1.0.0/16` (repeatable).

The ports browsers and Cloudflare Workers block are refused as well,
whatever the address: the
[Fetch standard's list](https://fetch.spec.whatwg.org/#port-blocking) of
ports that belong to other protocols, such as mail (25, 465, 587), FTP
(21), SSH (22) and IRC (6667). A crafted HTTP request can drive some of
those servers, as in sending mail through an SMTP relay. Such a request
fails with `HTTP-request-denied`, before any connection.
`wasmkeep serve --allow-outbound-port 587` opens one (repeatable).

HTTPS certificates are checked against the host name in the URL and the
public certificate authorities (Mozilla's list, built in). An internal API
behind a private CA is trusted with `--outbound-ca ca.pem` (repeatable),
for every actor on the node.

Each actor may have 16 outbound requests open at once (256 per node), and
each step (connect, first byte, between bytes) waits at most 60 s (30 s
unless the guest asks otherwise).
