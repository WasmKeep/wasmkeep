# Changelog

Changes to WasmKeep, newest first. Before 1.0, any minor version (0.x) may
change the `wasmkeep:actor` WIT world, the formats in the bucket or the
command line; its notes say what changed and how to move.

The text under a version's heading is its GitHub release text
(`.github/workflows/release.yml` copies it).

## Unreleased

- **Placement at wake-up** ([ADR 0041](docs/adr/0041-placement-at-wake-up.md)):
  a node about to wake an actor nobody holds sends that one event to a
  clearly lighter node of the app (a resident fill of at least 20%, more
  than 25% above the lighter of two rendezvous candidates, within a
  budget per heartbeat), which acquires it unless its own count, read on
  arrival, says it is not the lighter one; then the event runs where it
  woke. Outside requests with no body or one of 64 KiB or less, and
  `actors.call`, are placed; WebSocket upgrades, alarms and streamed
  bodies never are, and a running actor never moves. The heartbeat's load
  line gains `actor_room` and `socket_room` per app (older readers skip
  them), nodes announce `place`, the hop carries a signed
  `x-wasmkeep-placed` mark, and `wasmkeep cluster status` shows each
  node's fill. On by default; `serve --no-placement` or `[process]
  placement = false` turns it off. New metrics:
  `wasmkeep_placements_total`, `wasmkeep_placements_received_total`,
  `wasmkeep_placement_kept_total`, `wasmkeep_placement_fill`.

- `backup restore` warns when the selected app version imports files but the backup predates file manifests.

- **Actor SQL/export verification:** apply SQLite authorization, schema checks,
  and execution deadlines before verification queries; reject hostile host
  metadata views and run verification off the async executor.

- **Local inspector:** `wasmkeep inspect --app TENANT/APP` serves an embedded,
  authenticated loopback page for bucket status, class and actor listings,
  retained history and constrained read-only SQL. No node endpoint or bucket
  writes. See the [inspector guide](docs/inspector.md).

- **A React quickstart:** `examples/react-todos` is a TypeScript actor
  and a React + Vite page over one typed contract, with Vite proxying to
  `wasmkeep dev` and the built page deployed as static assets
  (`not-found = "spa"`); the tutorial is
  [docs/build/react.md](docs/build/react.md), run as written by
  `scripts/tutorials/react.sh`. `wasmkeep-ts build` now resolves
  `@wasmkeep/sdk/contracts` and `@wasmkeep/sdk/room-client`, as it did
  `@wasmkeep/sdk/front`: before, an actor could not import a contract file
  from the contracts entry.

- **Typed Rust clients:** the `client` feature of `wasmkeep-sdk` adds
  `contract!` (a plain `macro_rules!` macro) and `record!`: declare an actor
  class's operations with typed inputs, answers and declared errors, and get a
  client whose methods return values and the same nine-kind failure as the
  TypeScript clients, with `batch` over `call_many`. Requests, limits and the
  error envelope match TypeScript (the shared problem vectors run through the
  client). No dependency, no WIT or server change. See [typed
  clients](docs/typed-clients.md#typed-clients-in-rust).

- **Static assets (ADR 0030, section 9):** deploy project directories with
  app-scoped indexes, hostname isolation, verified per-app caches, browser
  cache rules, compressed variants and ranges. Deploy/collection markers
  protect retained versions through concurrent collection and rollback;
  unchanged deploys repair missing files. `wasmkeep dev` serves files live
  with the same routing and headers. Format 7 requires the `assets`
  capability. See [static assets](docs/static-assets.md).

- **Big rooms (ADR 0040):** the Rust and TypeScript SDKs gain a
  directory (key ranges to actors, split and merge as durable
  transactions) and a broadcast group: a room actor that numbers messages
  as (epoch, number) and keeps a history window, edge actors of at most
  500 sockets that it feeds by call-many, and a relay layer past 16 edges
  (up to 256). Edges split at 80% full and drain below 30%. Delivery is at
  least once and ordered; the browser client drops repeats, recovers gaps
  over HTTP and reconnects from its last position. Numbers lost with
  batched writes are announced void; `confirm` flushes before each round.
  Example: `examples/ts-big-room`. See the [designing for hot actors](docs/designing-for-hot-actors.md)
  page.

- **The change feed (ADR 0039):** `wasmkeep feed --to parquet --out DIR
  --state DIR` reads an app's actors from the bucket, read-only, and
  writes one append-only Parquet file per class and table for the actors
  that changed since the last poll, with `_wk_states` saying which state
  each row is (`state`, `deleted`, `rebase` after a restore or a recreated
  app). A stopped or crashed feed resumes without gaps; delivery is at
  least once, ordered per actor. Each poll regenerates
  `views.duckdb.sql`, the latest state of every actor per table, with
  columns united across schema changes. `wasmkeep feed compact` keeps
  only each actor's latest state. See [the change
  feed](docs/change-feed.md).

- Actor-requested WebSocket closes preserve their code and reason even when the
  data queue is full; queued frames are discarded and their byte budget released.

- **Several apps per process (ADR 0038):** `wasmkeep serve --node-file
  wasmkeep-node.toml` serves several apps behind one public, one cluster
  and one metrics port. The public port routes by hostname (421 for an
  unknown host, every `x-wasmkeep-*` header removed); each app keeps its
  node id, lease, state and restarts, and a verdict on one app's node id
  restarts that app alone, in place. Residents, connections, front
  reservations, uploads and restore reads are shared between apps with a
  floor per app. Every node now names its sender in each hop it signs and
  announces `multi-app` (and `process=<id>`) in its heartbeat; a process
  of several apps refuses to start while a live node lacks it. `cluster
  status` groups node ids by process, `cluster drain --process ID` drains
  them all. `serve --app` is unchanged. See [the node
  file](docs/reference/node-file.md) and [upgrading to
  0.6.0](docs/deploy/upgrading-to-0.6.md).

## 0.5.1

**Fixes and safer operations on 0.5.0.** Activations run under the same
deadline as events; the guest SDKs keep HTTP headers lossless; destructive
commands print an operation ID and can be resumed with `--resume`; a
refused actor restore no longer disturbs the actor's sockets; the
recorded-head format gets its follow-ups; `serve --no-alarms` runs a node
that never fires alarms. No WIT or manifest format change: a 0.5.0
cluster upgrades node by node, and actors built with the 0.5.0 SDKs keep
running. During the upgrade, use the 0.5.1 CLI, and expect a superseded
actor delete to show as dropped to a 0.5.0 CLI (below). The TypeScript
SDK's `http.fetch(...).headers` changes behaviour (below). Still a
**pre-alpha pre-release**.

- Refused actor restores preserve existing WebSockets and upgrade admission.
  Snapshot validation and restore fences now precede the 1012 close; accepted
  restores retain their checked snapshot while the old sockets drain.
  Conditional restores keep `--if-head` exact through draining and retries:
  intervening writes produce a conflict and survive. Late upgrade refusals
  report "actor restore pending" with `Retry-After: 1`, not capacity exhaustion.

- Actor delete, app delete, actor restore and backup restore print operation IDs
  before waiting and accept `--resume ID`, which never files a new request.
  Actor outcomes are read without consuming them and last for the request TTL;
  after expiry the ID is unknown and nothing is filed. App delete matches the
  tombstone ID; backup restore matches the journal nonce or completed `restored`
  marker. Pending actor deletes superseded by a later restore or a delete at
  or after their bucket time leave the replacement intact. A 0.5.0 CLI reports
  a superseded delete as dropped (fails closed); a 0.5.0 node neither recognises
  nor expires `.superseded`. Bindings, capability negotiation and durable
  receipts move to 0.6.0. See [operation recovery](docs/operations.md).

- **Activation deadlines:** an actor's activation (instantiating
  its component and running its start function) runs under the same wall,
  lease and replication guard as an event; cut, it waits for its pending
  database work, gives up its hold and answers 503 with `Retry-After`. A
  front's validation runs under the front's wall budget. No new setting.

- **Lossless HTTP headers in the guest SDKs:** the TypeScript,
  Python and Rust SDKs keep headers as ordered (name, value) pairs, compare
  names without case, and never join `Set-Cookie`. `get(name)` is the first
  value, `getAll(name)` (`get_all`) every value in order, `append` adds and
  `set` replaces all: `HeaderList` (TypeScript: `req.fields`,
  `actors.call(...).fields`, `http.fetch(...).fields`), `Headers` (Python:
  `req.fields`, `CallResponse.fields`, `req.header_list`) and
  `wasmkeep_sdk::headers::Headers` (Rust). A response sets repeats with
  `headerList` (TypeScript) or `header_list=` (Python); `actors.call` and
  `http.fetch` accept a list value in a record, pairs or the new type;
  `respond_headers` and `request_headers` are new in Rust. No WIT change.
  Behaviour that changes: TypeScript's `http.fetch(...).headers` no longer
  joins `Set-Cookie` values with `, ` (it holds the first; read all through
  `fields`); a TypeScript response whose headers repeat `Content-Type` in
  another case no longer gets a second default one; a TypeScript lookup in a
  request without a header list is now case-insensitive. In Rust, `Fetched`
  gains a public `headers` field (a struct literal of it needs the field).
  The older `headers` dicts and records still hold one value per name.

- **Recorded-head follow-ups ([ADR 0035](docs/adr/0035-a-recorded-head-on-hand-over.md)),
  for apps that enabled the format:** an idle expiry refused for
  visibility keeps the actor's idle origin (`idle-since`), so it still
  expires once its history is visible; a local database that claims the
  recorded head with another checksum is set aside and the history
  downloaded, instead of a 500; a link of the history still missing after
  the bounded re-listing is reported as a corruption (500) instead of a
  503 for ever.

- `wasmkeep serve --no-alarms` starts a node that serves requests but never
  fires an actor's alarms (no delivery, no sweep of the bucket's alarm index);
  alarms stay recorded and pending for a node without the flag. The restore
  drill uses it for its throwaway restored node, so a restored copy does not
  run the app's side effects twice.
- A node at a 65,536 open-file limit with the default socket request (50,000)
  no longer warns that socket capacity was reduced: it admits about 25,800
  sockets and says so in one INFO line stating the number. A warning remains
  when an explicit `--max-sockets-per-node` is cut, or when anything else is.

## 0.5.0

**Batched writes, data access and per-node limits.** A class can batch
its plain writes into one upload (`writes = "batched"`); an operator can
export an actor as SQLite, query it read-only, back up an app while the
cluster runs, prune old backups, restore a whole app or one actor, and
check all of it with a backup and restore drill. A node enforces
declarative rate limits from `wasmkeep.toml`, configurable socket caps
and a file-descriptor budget. An app can opt in to a recorded head on
hand-over, so that the next node serves only the state the last one
acknowledged. TypeScript actor clients are typed. Still a **pre-alpha
pre-release**.

**Upgrade:** [upgrading to 0.5.0](docs/deploy/upgrading-to-0.5.md): the
new capability words, the manifest formats a batched class and a rate
limit write, the one-way recorded-head format, and what a rollback to
0.4.2 needs.

**Soak** of `57b76c2` on three OVH VPS, two hours and 39 kills in each
mode ([strict](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-strict-57b76c2.csv),
[batched](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-batched-57b76c2-batched.csv),
[numbers](docs/benchmarks.md#two-hours-strict-and-batched)). Durable
writes: 386,488 acknowledged in strict and 404,324 in batched, none lost
or handed out twice in either; durable write p50 78.8 ms and 76.8 ms, p99
999 ms and 920 ms; writable again after a kill p50 18.2 s and 16.8 s, p99
22.5 s and 21.9 s, worst 24.9 s and 24.5 s; 312 alarms in each, none
missed. Plain writes acknowledged and lost with their node: 2 of 208,101
in strict and 785 of 1,237,008 in batched (about 20 per kill).

**Rolling upgrade** from 0.4.2 to the release candidate `a391642`, on the
fleet: node by node with writes flowing and actors moved between old and
new nodes; a batched deploy refused while a 0.4.2 node was live;
`app format --enable` refused while nodes ran the app; with strict
current and the recorded head not enabled, one node rolled back to 0.4.2
and forward again, actors written in batched mode included. Not tested:
in-place restore markers, `restores/` and format 6 in a rollback
([upgrading](docs/deploy/upgrading-to-0.5.md#rolling-back-to-042)).

Date: 2026-11-17

- **Per-node rate limits (0.5.0, [ADR 0036](docs/adr/0036-per-node-ingress-rate-limits.md)):**
  `[limits.<name>]` rules in `wasmkeep.toml` by route, method and key (the
  app, the client, or a header). A node checks them before it reads a
  body or does any actor work, and answers 429 with `Retry-After`; a
  refused direct request writes nothing to the bucket. Clients are told by
  their TCP address only (an IPv6 one by its 64-bit prefix), never by a
  forwarded header. A forwarded request is charged once, by the owner,
  from a signed identity. An app with no rule skips all of it. A version
  with a rule is manifest format 6: a deploy is refused while a live node
  lacks `rate_limits`, and a node of 0.4.x reads it as corrupt and limits
  nothing, so upgrade every node first
  ([project file](docs/project-file.md#ingress-limits)).
- **Socket caps and a descriptor budget (0.5.0, [ADR 0037](docs/adr/0037-socket-session-admission.md)):**
  an upgrade holds a permit for its whole life, on the local path and on
  relays, and over a cap it gets 503 with `Retry-After`. Sockets draw from
  a budget derived from the soft open-file limit, with room kept for
  SQLite files, the bucket client and listeners; at a limit of 1,024 that
  is 144 sockets and 96 resident actors (192 before). A very low limit
  starts degraded with a warning. A relay whose one direction ended keeps
  delivering the other until the close wait, so the last frames and the
  Close arrive.
- **Online backup** ([ADR 0033](docs/adr/0033-data-access-export-and-online-backup.md),
  [guide](docs/deploy/backup-and-restore.md)): `wasmkeep backup create`,
  `list` and `verify`, and `actor history`. A backup reads the live bucket
  with LIST and GET only: no lease, no ownership record, no write. Each
  actor is replayed and verified before its record is written; runs are
  content-addressed and end in a checksummed `complete`; `verify` works
  from the destination alone. It detects corruption, not an attacker who
  can rewrite the whole archive.
- **`backup restore`:** restores a whole app from a verified run, into an
  empty root or `--in-place` (a verified backup of the current state is
  taken first and pinned as the undo). A fence is written before any state:
  nodes refuse to serve the app while it stands, and `backup create`,
  `app delete` and `deploy --recreate` refuse. Every phase is journaled, so
  a rerun continues, and `--abort` cancels before the fence opens. Nodes
  announce the word `restore`; `--force` waives old nodes only.
- **`actor restore`:** restores one actor to a time, a transaction or a
  backup while the cluster runs, as a new write at the next position,
  never a rewrite. The owner runs it, closes the actor's sockets (1012),
  and a later delete always wins over an older request. Needs
  `actor-restore` on every node registered for the app, checked by the CLI
  and by the owner. The strict golden trace is re-recorded: every delete
  now labels its snapshot, with no change to a bucket request's key or
  order.
- **`backup prune --keep-runs N`:** keeps the newest N complete runs, the
  latest, every complete run under 24 hours old and pinned runs (an
  in-place restore's undo, a restore in progress), and deletes only what
  no kept run references. It never touches the live bucket; a create
  overlapping a prune fails rather than publishing a run with missing data,
  and an interrupted prune is finished by a rerun.
- **A backup and restore drill, and a `backup create` fix:**
  `scripts/drills/backup-restore.sh` backs up a fixture app under write
  load, verifies the run, restores it into an empty prefix, starts a
  throwaway node there and checks that every acknowledged write is
  present; `check.sh --s3` runs it against moto. It found that `backup
  create` polled an ended actor listing again, which panics on a real
  paginated S3 listing; the listing is fused now.
- **Typed TypeScript clients and a structured error envelope:**
  `defineOperation` and `defineContract` with codecs (a Zod-style
  `{parse}` validator works), a client that returns values and a nine-kind
  failure instead of throwing, `batch()` over `callMany` and `bind`
  ([guide](docs/typed-clients.md)). Application errors may use an opt-in
  envelope, `{"v":1,"code","message","details?"}` as
  `application/vnd.wasmkeep.error+json`, written and read the same way by
  the TypeScript and Rust SDKs (shared test vectors), with a hostile body
  refused rather than trusted. `CallError` gains a closed `CallErrorKind`.
  Additive; no WIT change.

- **Recorded head on hand-over (0.5.0, [ADR 0035](docs/adr/0035-a-recorded-head-on-hand-over.md)):**
  an app enabled with `wasmkeep app format --enable` writes ownership
  records as `wasmkeep-owner 2`, carrying the position each releasing
  node acknowledged. The next node activates only once its restore
  proves it (the exact position, or a later generation on the selected
  chain); a bucket listing that lags answers 503 with `Retry-After`
  after at most ten listings in 15 s, with a per-actor cooldown, instead
  of serving older state. Off by default; one way; every node of the
  app must run 0.5.0 or later (heartbeat word `recorded-head`) and the
  app must be stopped to enable it. All readers accept both record
  versions; binaries before 0.5.0 cannot read version 2. `actor export`
  shows a record's recorded head; `cluster status` warns when an app's
  nodes differ on the capability. See
  [the operator guide](docs/cluster.md#the-recorded-head-ownership-format).

- **Configurable socket admission (0.5.0):** `serve` accepts
  `--max-sockets-per-actor` (1,000 requested) and `--max-sockets-per-node`
  (50,000 requested), plus `--max-sockets-per-app` (defaults to the node cap;
  set lower for isolation). Zero refuses every WebSocket at that scope.
  The node ceiling now includes relays, tightening the previous owner-only accounting. Socket sessions join
  the descriptor admission estimate and hold reservations through transport
  cleanup. Over-capacity upgrades receive 503 with `Retry-After`. Startup
  logs and metrics report requested/effective socket ceilings. See
  [socket admission options](docs/reference/socket-limits.md).

- **Actor export and read-only SQL** ([guide](docs/actor-data-access.md),
  ADR 0033): `actor export TENANT/APP CLASS/NAME -o FILE` and `actor sql`
  replay verified bucket history without contacting nodes or acquiring a
  hold. Current-head, capture-time and transaction selection follow the
  current ancestry; output reports the unacknowledged-tip caveat. Exports
  are portable SQLite files; SQL has an authorizer, bounded results and
  table/CSV/TSV/JSON/JSONL output. Both accept `--tmpdir` with advisory disk
  checks and clean scratch copies on Ctrl-C/SIGTERM; `--require-owner-match`
  refuses unknown/mismatched owner hints (exit 3). Publication falls back to
  private no-clobber copying when links are unavailable. Time selection skips
  undated snapshot-only bases, and short object bodies are retried.

- **SQL result limits now account for retained memory.** The 16 MiB
  budget includes row and cell overhead, vector spare capacity, allocation
  allowances and column names once, as well as text/blob payloads. Queries
  with many empty values or large aliases now fail with `quota-exceeded`
  when they exceed it; names and payloads are checked before copying.
- **`storage.flush`: make the writes so far durable without a
  statement** ([ADR 0034](docs/adr/0034-batched-plain-writes.md),
  section 4). Added to `wasmkeep:actor@0.2.0` (components built before
  link as they did) and to the Rust (`storage::flush`) and TypeScript
  (`storage.flush()`) SDKs. It returns once every write the actor
  committed before the call is durable, plain `exec` included: in a
  cluster this event's writes so far go to the bucket in one upload (an
  alarm set meanwhile listed first, as `exec-durable` does), or nothing is
  sent when nothing is pending; on a single node the WAL is fsynced. It
  is refused with `not-allowed` inside a transaction. An `internal`
  starting `replication stopped` means the writes are not known to be
  durable and the actor is restored from the bucket, as after a failed
  `exec-durable`, and every later `flush` of the event answers it at once
  ([concepts](docs/concepts.md#storage)). `exec-durable` is unchanged.
  TypeScript and Python components built with this SDK import it whether
  or not they call it, so they need a node of this release.
- **`deploy` refuses a component a live node could not link**
  ([cluster](docs/cluster.md#rolling-upgrades-and-new-storage-functions)).
  Nodes now announce the heartbeat word `flush`. `deploy` (and
  `--rollback`) reads the imports of each component of the version it
  would make current and refuses, before writing anything and without
  `--force`, one that imports `storage.flush` while
  a live node of the app lacks `flush`, or one that imports
  `storage.delete-self-durable` while a live node lacks `ends` (a node of
  0.4.1). The second is new for every class: 0.4.2 accepted that deploy
  and the 0.4.1 node stayed not ready on it. The classes a deploy keeps and
  the version a rollback brings back are checked too. The check cannot
  see a node started without `--node-id`, or a node that starts after
  the deploy. Upgrade every node, then deploy.
- **A class's `writes` in `wasmkeep.toml`** (ADR 0034, section 1):
  `"strict"`, the default and every release's behaviour, or `"batched"`,
  which every build of this release deploys and runs (strict stays the
  default). `"strict"` written or not makes the same version, byte for
  byte. A version with a batched class is written as manifest format 5,
  which a deploy holds back while a live node lacks `flush`; each hold
  keeps the mode it was acquired with until its writes are flushed
  ([project file](docs/project-file.md)). `deploy --writes CLASS=MODE`
  and `serve --writes` set it from the command line, and `--max-unsent-age`
  bounds how long a committed write waits unsent. In a batched class, what a node's death can lose is the segment in flight plus the commits not yet captured: with a healthy bucket about two upload round trips plus one event (2r + e), and with a stalled bucket up to about `--max-unsent-age` (500 ms by default) plus one event after the stall began. Durable writes (`exec-durable`, `flush`) are never lost. Measured on three OVH VPS, 2 h and 39 kills per mode ([strict](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-strict-57b76c2.csv) and [batched](bench/results/2026-11-16-57b76c2-soak-ovh-bhs-3vps-2h-batched-57b76c2-batched.csv)): durable writes lost 0 in both; plain writes lost 2 of 208,101 acknowledged in strict and 785 of 1,237,008 in batched (about 20 per kill, at about 172 plain writes a second across the cluster); durable write p50 and p99 78.8 ms and 999 ms in strict, 76.8 ms and 920 ms in batched; failover p99 22.5 s and 21.9 s; no alarm missed. As aggregate counts over all actors and kills, they are evidence, not a proof of the per-actor bound.
- **`cluster diagnose` requires listings that are strongly consistent
  after a write.** A node taking an actor over plans its restore from one
  listing of the actor's history, and after a voluntary hand-over it may
  list at once: a segment that listing omits would make it restore an
  older head and fork the history. 0.4.2 passed a listing that showed a
  write up to a second late, and only warned up to ten seconds. Now the
  first listing that starts after a write was acknowledged must show it
  (`list-new-key`, now five fresh keys, `large-object`, `last-modified`,
  `list-paging`), or the check fails and the bucket is reported not safe;
  a listing that fails or does not answer is still an `error` (no
  verdict). The verdict for exit 0 now reads `required behaviour observed
  by these checks (not a proof of safety)`: the checks sample a bucket,
  they do not prove it safe. The `--json` field `verdict` keeps `safe`
  for exit 0 ([buckets](docs/deploy/buckets.md)).
- **The TypeScript SDK no longer reads a broken request body as a whole
  one.** The incoming body treated any error of its stream as the body's
  end, so a body cut short (a client that went away, a close short of
  its Content-Length, a streamed body over the host's 16 MiB cap) reached
  a handler as a shorter, complete-looking body. Now only the stream's
  `closed` is its end, and only once the trailers say it ended well;
  anything else makes `bytes()`, `text()` and `json()` throw `the request
  body is incomplete: ...`, on QuickJS and StarlingMonkey alike. The
  Rust and Python SDKs already told the two apart, and Go's
  `ReadBody()` does (its `Body()` gives what arrived); the C (and Zig,
  which uses it), D and MoonBit SDKs still read a broken body as a
  shorter one.
- **A failed listing of `nodes/` no longer stops the reads of known
  nodes.** In 0.4.2 a node whose scheduled listing failed tried the
  listing again every heartbeat instead of reading the nodes it knows, so
  a bucket that refuses or throttles LIST while it answers reads would
  freeze its view, and its trust in every peer would lapse one horizon
  later. Now a heartbeat whose listing fails still reads the known nodes,
  and the listing is tried again the next one
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md), amendment of 2026-11-14).

## 0.4.2

**Actors that end, and a database kept by one actor.** An operator
deletes one actor (`wasmkeep actor delete`) or a whole app (`wasmkeep
app delete`); a class can expire actors left idle for a stated time
(`expire-after-idle`); and `storage.delete-self-durable` returns only
once the delete is in the bucket. The Rust and TypeScript SDKs make one
actor the owner of an app's shared tables, which the app's other actors
and its front query with bound SQL. `wasmkeep new` writes a Rust or
TypeScript project that builds and runs the first time, and a toolchain
image, `ghcr.io/wasmkeep/toolchain`, builds any Recommended or Supported
SDK's components with only Podman or Docker installed. The release
carries the counter prebuilt in each of those languages. An idle node
lists the bucket about 13 times less (on R2 and S3 a LIST costs what a
PUT costs). Still a **pre-alpha pre-release**. The interface between a
component and the host stays `wasmkeep:actor@0.2.0`, with one function
added: components built for 0.4.1 run as they are.

**Upgrade: rolling**, from 0.4.1, one node at a time
([upgrading WasmKeep](docs/cluster.md#upgrading-wasmkeep)). 0.4.2 changes
no format 0.4.1 reads in the bucket (layout, ownership records, history,
alarm index, intents), and signs calls between nodes as 0.4.1 does. What
it adds, 0.4.1 never reads: delete requests (`state/<t>/<a>/deletes/`),
`control/changed`, an app's tombstone and its `recreated` marker, and
manifest format 4, written only for a version with an idle limit. Its
heartbeat adds the word `ends`, which 0.4.1 ignores; its hello to peers
gets a 404 from 0.4.1, which lists every period anyway. While both
versions run, `cluster status` warns of mixed versions, and:

- A deploy with `expire-after-idle` and `app delete` are refused while a
  live node of the app runs 0.4.1; `actor delete` requests wait until a
  0.4.2 node holds the actor or finds it free.
- A 0.4.1 node cannot load a component that imports
  `storage.delete-self-durable`, and the deploy does not check for it:
  that node stays not ready on the version. Every TypeScript and Python
  component built with the 0.4.2 SDK imports it, called or not (the
  release's `counter-typescript.wasm` and `counter-python.wasm` too), as
  do Rust, Go, C and Zig components that call it. Deploy them once every
  node runs 0.4.2.
- Upgrade the CLI with the nodes: a drain, an undrain or a move from the
  0.4.1 CLI is read within a minute instead of 3 s, and the 0.4.1 CLI
  knows nothing of idle limits or tombstones.
- A 0.4.1 node that starts is seen by 0.4.2 peers within 30 s (sooner
  over TLS).

Going back to 0.4.1 rolls the same way while no version with an idle
limit is current and no component that needs 0.4.2 is deployed. From
0.4.0, upgrade to 0.4.1 first.

The rolling upgrade was tried on the same three nodes: one node put back
on 0.4.1 beside two on this release, one counter written through each in
turn (1, 2, 3, one owner); the 0.4.2 owner killed (`kill -9`), writable
again through the 0.4.1 node after 17 s with the count kept (4); that
0.4.1 owner killed, writable through a 0.4.2 node after 15 s (5); the
0.4.1 node rolled forward, and 6, 7, 8 through the three nodes.

Soaked on three nodes against a real bucket, a node killed every three
minutes, for two hours on this release's code (commit `563cb94`; later
commits change documentation, these notes and versions only):

39 kills, 378,472 acknowledged durable writes, none lost or applied
twice; writable again after a kill (1,264 takeovers) at 17.4 s at the
median, 22.5 s at p99 and 29.0 s at worst; 312 alarms fired, none
missed; no stale read after a takeover; a restarted node ready within
0.2 s. Of 202,535 plain writes (acknowledged before the bucket holds
them), one was lost to a kill and its number handed out again, as plain
writes allow
([`2026-11-14-563cb94-soak-ovh-bhs-3vps-2h-0.4.2-rc.csv`](bench/results/2026-11-14-563cb94-soak-ovh-bhs-3vps-2h-0.4.2-rc.csv)).

### What changed

- **The counter in every Recommended and Supported language, prebuilt**:
  besides `counter.wasm` (Rust), the release carries
  `counter-typescript.wasm`, `counter-python.wasm`, `counter-go.wasm`
  (TinyGo), `counter-c.wasm` and `counter-zig.wasm`, built with the tool
  versions CI tests, each in `SHA256SUMS` with its own Sigstore bundle
  (36 files in the release, from 26). The license texts of the runtimes
  they embed (QuickJS-ng, CPython and the C libraries it links, the Rust
  crates of the componentize-qjs and componentize-py runtimes, TinyGo and
  the Go standard library, wasi-libc, Zig's library, the WASI adapter) are
  added to `THIRD_PARTY_NOTICES-components.txt`. The Experimental languages
  (standard Go, D, MoonBit, C#) are not released.
  [Verifying a download](docs/deploy/single-node.md#1-install-the-binary).

- **`wasmkeep new DIR --lang rust|typescript [--front] [--app TENANT/APP]`**
  writes a project that builds and runs the first time: a class `counter`,
  optionally a front over it (the class then `internal`), its
  `wasmkeep.toml`, a README with the commands (`wasmkeep dev`, then
  `wasmkeep deploy`), a `.gitignore` and an `.env.local.example`. The
  templates are in the binary (no network), the SDK is pinned to the CLI's
  own version, and a directory that is not empty is refused. Every variant
  is built and served by `wasmkeep dev` in the tests. Other languages
  later. [Starting a project](docs/tutorials/first-actor.md#starting-a-project-with-wasmkeep-new).
- **A database kept by one actor**, in the Rust and TypeScript SDKs
  ([guide](docs/database.md),
  [ADR 0029](docs/adr/0029-a-database-client-as-an-sdk-pattern.md)): a
  class mounts the SDK's handler (`db::handle`, `db.define`) and its
  ordered migrations (`db::migrate`); the app's other actors and its front
  send it SQL with `db::Client` / `db.Client` (`query`, `exec`, `batch`,
  `read`) over `actors.call`. Parameters are bound, each request is one
  transaction (a batch is all or none, a read is always rolled back, a
  write commits durably), statements that control the transaction are
  refused, requests and answers are capped, and only inside callers are
  answered. The SQL comes from the app's code, never from its clients.
  [`examples/db`](examples/db) and [`examples/ts-db`](examples/ts-db);
  every client is tested against every handler. The other SDKs come later.
- **Actors that end** ([ADR 0028](docs/adr/0028-actors-that-end.md)):
  - `wasmkeep actor delete --app TENANT/APP CLASS/NAME --bucket ...`
    deletes an actor: the node that holds it, or any node serving its
    class if nobody does (its node died, say), closes its WebSockets with
    1000, removes its data and alarm and records the delete in the bucket.
    The command waits until the delete is recorded (`--wait`, 60 s;
    `--no-wait`), refuses a name with nothing in the bucket, and a request
    read twice is carried out once. Nodes of an earlier version ignore the
    requests ([cluster](docs/cluster.md#deleting-one-actor)).
  - `storage.delete-self-durable`, added to `wasmkeep:actor@0.2.0` (a
    component built before links as it did): deletes the actor at once and
    returns only once the delete is in the bucket (fsynced on one node),
    or `internal` when it is not known to be. Later storage calls in that
    event are refused with `not-allowed`. Rust
    `storage::delete_self_durable`, TypeScript `storage.deleteSelfDurable`;
    the other SDKs later.
  - Metrics `wasmkeep_actor_deletes_total{how}` and
    `wasmkeep_actor_delete_failures_total{how}` (`self`, `self_durable`,
    `operator`).
  - **Idle expiry**: a class's `expire-after-idle` (`"7d"`, at least an
    hour) in `wasmkeep.toml`, or `deploy --expire-after-idle CLASS=7d`
    (`CLASS=` removes it), part of the version. In a cluster, the lowest
    live node of the app lists the class's ownership records every 10
    minutes and deletes actors idle that long, never one with a pending
    alarm or an open WebSocket; an event arriving meanwhile wins, or
    finds a new actor ([cluster](docs/cluster.md#idle-expiry)). A version
    with a limit is manifest format 4: deploys are refused while a live
    node of the app does not announce the new heartbeat word `ends`.
  - **`wasmkeep app delete --app TENANT/APP --bucket ...`**: writes the
    app's tombstone (`apps/<t>/<a>/deleted`), from which deploys, secrets,
    moves and deletes refuse the app and its nodes stop serving it
    (WebSockets closed with 1001, node lease released, local files
    removed, `serve` exits); once none is live, removes everything under
    `state/<t>/<a>/` and `apps/<t>/<a>/` but the tombstone. Refused while
    a live node of the app lacks `ends`. `deploy --recreate` uses the name
    again ([cluster](docs/cluster.md#deleting-an-app)).
  - Metrics: `how="expired"` on the delete counters,
    `wasmkeep_expiry_passes_total`, `wasmkeep_expiry_candidates_total`,
    `wasmkeep_expiry_kept_total{reason}`.
  - Behaviour changes: a node following an app (`serve --app`) acquires
    no actor until it has checked for the app's tombstone since its lease
    was valid (a moment after it starts); the heartbeat adds the word
    `ends`, which earlier versions ignore.

- **Fewer listings of the bucket.** An idle node lists the bucket about
  0.1 times a second instead of 1.43 (1.1 in 0.4.1; on R2 and S3 a LIST
  costs what a PUT costs): `nodes/` every 30 s, the nodes it knows read one by one every
  heartbeat in between, and a node that starts says hello so its peers list
  at once; the drain, move and delete requests when `control/changed` says
  they changed, or every minute; the alarm index when an entry is due for
  the sweep, at most a minute apart, and at once when a node joins or
  leaves. Fencing, failover timings for known nodes and pin revocation are
  unchanged ([ADR 0009, 0011, 0014, 0017 and 0020](docs/adr/0020-one-lease-per-node.md),
  amendments of 2026-11-14). New metric: `wasmkeep_node_hellos_total`.
  These listings change no format in the bucket. Upgrade the CLI with
  the nodes: `cluster drain`, `undrain` and
  `actor move` of 0.4.1 do not write `control/changed`, so nodes of 0.4.2
  read their requests within a minute instead of 3 s. While both versions
  run, 0.4.1 nodes answer the hello 404 and keep listing every period, and
  0.4.2 nodes see a 0.4.1 node that starts within 30 s (sooner over TLS).
- `wasmkeep_bucket_requests_total` counts the node's read of the app's
  secrets every heartbeat period (`serve --app`), a GET it missed
  before: an idle node's counted GETs go up by one a period, with no new
  request ([three nodes at rest](docs/benchmarks.md)).

- **The toolchain image**, `ghcr.io/wasmkeep/toolchain:<version>` for
  linux/amd64 and linux/arm64: `wasmkeep` and the build tools of the Rust,
  TypeScript, Python, Go (TinyGo), C and Zig SDKs at the versions CI uses
  (`scripts/check-toolchain.py` keeps them equal), so a project builds with
  only Podman or Docker installed. The release workflow builds it, tests the
  pushed image on both platforms (`deploy/toolchain/test.sh`), and signs it
  like the server image; the nightly workflow builds and tests it too. About
  880 MB to download. [The toolchain image](docs/deploy/toolchain.md).
- `wasmkeep_releases_after_lapse_total` counts every release that landed
  while the node's lease was not valid, not only those before its lease
  task saw the lapse: also one sent between the lease task's seeing the
  lapse and its ending the holds (the WebSockets the lapse closes), and
  one of a hold whose acquisition landed after the lapse. Its warning is
  logged once per lapse, for the first such release, with the count
  logged once the lease is valid again, instead of one line per actor.

### Known limits

The limits listed for 0.4.1 still hold. Also:

- The database pattern and `delete-self-durable` are in the Rust and
  TypeScript SDKs only, and `wasmkeep new` writes Rust and TypeScript
  projects only; the other languages come later.
- Idle expiry needs a cluster (a single node keeps no ownership
  records, so nothing expires there), and never expires an actor whose
  encoded name was shortened with a hash.
- A component that imports `delete-self-durable` (every TypeScript and
  Python component built with this SDK) needs a 0.4.2 node: during an
  upgrade from 0.4.1, deploy it once every node runs 0.4.2.
- An `actor delete` or an `app delete` cannot be undone, except from a
  bucket that keeps old versions
  ([backup and restore](docs/deploy/backup-and-restore.md)).
- The toolchain image is about 880 MB to download; the Experimental
  languages (standard Go, D, MoonBit, C#) are neither in it nor
  prebuilt in the release.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release: 36 assets, 17
files and `SHA256SUMS`, each with its Sigstore bundle. The prebuilt
counters `counter-typescript.wasm`, `counter-python.wasm`,
`counter-go.wasm`, `counter-c.wasm` and `counter-zig.wasm` are checked
the same way, for example:

```bash
f=counter-typescript.wasm
for file in "$f" "$f.sigstore.json"; do
  curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v0.4.2/$file"
done
cosign verify-blob --bundle "$f.sigstore.json" \
  --certificate-identity https://github.com/WasmKeep/wasmkeep/.github/workflows/release.yml@refs/tags/v0.4.2 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$f"
wasmkeep serve --actor counter="$f"
```

[Compose](deploy/compose/README.md) checks the server image, and
[the toolchain image](docs/deploy/toolchain.md) its own.

## 0.4.1

**Sockets through the front.** The front hands a client's WebSocket to
an actor of the app, so a room class can be `internal`, with no public
`/actor/` address and its token checked once, in the front. Handlers
send several outbound HTTP requests at once in Rust, TypeScript and Go.
The host strips its reserved `x-wasmkeep-` headers from outbound HTTP
answers, so no upstream can hand a socket off through a front. A room
deleted under its sockets stays deleted: their close events no longer
bring it back. A node whose lease ran out before it noticed can release
an actor early; that was always so and safe by ordering, and is now
documented, counted (`wasmkeep_releases_after_lapse_total`) and pinned
by tests. Still a **pre-alpha pre-release**. The interface between a
component and the host stays `wasmkeep:actor@0.2.0`, unchanged:
components built for 0.4.0 run as they are.

**Upgrade: rolling**, from 0.4.0, one node at a time
([upgrading WasmKeep](docs/cluster.md#upgrading-wasmkeep)). 0.4.1 changes
no format in the bucket (layout, manifests, ownership records, alarm
index) and signs calls between nodes as 0.4.0 does; its heartbeat adds
the word `handoff`, which 0.4.0 ignores, and a 0.4.1 node relays a
hand-off only to a node that announces it. While both versions run,
`cluster status` warns of mixed versions, and a hand-off to a room on a
0.4.0 node gets a 503 with `Retry-After: 1`: deploy an app that hands
sockets off once every node runs 0.4.1. From 0.3.x, upgrade to 0.4.0
first ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md)).

Soaked on three nodes against a real bucket, a node killed every three
minutes, for two hours on this release's code (commit `0bf0547`; later
commits change these notes and versions only): 39 kills, 377,095
acknowledged durable writes, none lost or duplicated; failover p50
17.6 s, p99 23.1 s, max 27.1 s; no alarm missed; no stale read after a
takeover; a restarted node ready again within 0.3 s
(`bench/results/2026-11-14-0bf0547-soak-ovh-bhs-3vps-2h-0.4.1-rc.csv`).
The rolling upgrade was tried on the same three nodes: one node on 0.4.0
beside two on 0.4.1 kept one owner per actor, a node of either version
killed was taken over by a node of the other with every write kept, and
the last node was then rolled forward.

### What changed

- **Sockets through the front**
  ([ADR 0027](docs/adr/0027-sockets-through-the-front.md)). The front
  accepts a WebSocket at its own paths and hands it to an actor of the
  app, so a room class can be `internal` and needs no public `/actor/`
  address. The host marks an upgrade to the front with
  `x-wasmkeep-upgrade: websocket`; the front answers with three
  `x-wasmkeep-handoff-*` headers (class, name, path), built by
  `front::hand_off(class, name, path)` in Rust and `handOff(...)` in
  TypeScript, with `is_websocket` / `isWebSocket` to tell an upgrade. The
  host stops the front's instance, frees its slot, and opens the socket on
  that actor as an inside caller; each socket keeps that mark through
  hibernation. A refused hand-off is an HTTP error (404, 400, 403 with the
  actor's reason, 503), never a socket that opens and closes. Between
  nodes the hand-off is relayed signed, with the front's caller and the
  upgrade mark signed, and only to nodes that announce the new heartbeat
  word `handoff`. `examples/todo-front` gains `/api/live`, a socket
  handed to the signed-in user's `internal` list.
- Behaviour changes: the front sees `x-wasmkeep-upgrade: websocket` on
  WebSocket upgrades; a front's answer to an upgrade with the three
  hand-off headers opens a socket instead of reaching the client; a
  WebSocket upgrade refused because its actor is unavailable (or a value
  it requires is not set) now carries `Retry-After: 1`; outbound HTTP
  answers, to actors and fronts alike, lose the host's `x-wasmkeep-`
  range (headers and trailers), as `actors.call` answers have since
  0.4.0, so no upstream can hand a socket off through a front that passes
  its answer on. Actors and built fronts are unchanged.
- In a cluster, the close events of the sockets a `delete-self` closed
  (under an `existing` or `internal` class) no longer run on a new, empty
  instance of the deleted actor.
- **Several outbound HTTP requests at once** from the Rust, TypeScript and
  Go SDKs: `http::fetch_all`, `http.fetchAll` (QuickJS and StarlingMonkey)
  and `FetchAll` send a batch together and wait for all of it, one result
  per request in input order, at most 16 open at once (the host's limit per
  actor; the rest start as earlier ones finish). `fetch`, `http.fetch` and
  `Fetch` are now a batch of one, with the same answers.
  [Languages](docs/languages.md#several-outbound-requests-at-once) says what
  each SDK does, Python, C, Zig, D and MoonBit included (no outbound HTTP
  helper yet).
- **A node whose lease writes fail while its other writes land** hands an
  actor over as soon as an event reaches it after its lease ran out (8 s
  after the send of its last acknowledged lease write, with the
  defaults; the lapse closes its WebSockets, so every actor with sockets
  open goes then), before
  the node sees the lapse: the release of the hold its actor dropped can
  land, and another node takes the actor at once, without a fence. That
  was already so, and is safe by ordering (the node stops acting before
  it sends the release, and the next owner acquires only after the
  release landed), but ADR 0020 said otherwise and nothing showed it. It
  is now documented ([ADR 0020](docs/adr/0020-one-lease-per-node.md),
  section 1), counted on the releasing node in
  `wasmkeep_releases_after_lapse_total` (a lower bound: a release whose
  reply was lost is not counted) with a warning in its log
  ([monitoring](docs/monitoring.md#failovers)), and pinned by a cluster
  test and a simulation (`madsim_early_release`).

### Known limits

The limits listed for 0.4.0 still hold, except the first: the front now
accepts a WebSocket at its own paths. Also:

- Fronts, `call-many` and the hand-off helpers are in the Rust and
  TypeScript SDKs only; several outbound requests at once are in Rust,
  TypeScript and Go (Python, C, Zig, D and MoonBit have no outbound HTTP
  helper yet).
- A hand-off needs 0.4.1 on the front's node and on the room's: during
  an upgrade from 0.4.0 a hand-off to a room on a 0.4.0 node gets a 503.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.4.0

**An app is one thing.** An app's classes and its front are deployed,
versioned and rolled back together, and the app owns its actors' state:
each app keys its actors under its own root in the bucket, so two apps
never share an actor, and a one-time migration moves a bucket prefix
written by 0.2 or 0.3 there. The front, a stateless component deployed
with the app, receives every request outside `/actor/` and calls the
app's actors, several at once with `call-many`. A project file,
`wasmkeep.toml`, says what the app is, for `deploy` and `dev`, with
classes `internal` unless it says otherwise. Load you can see: actors,
sockets and events per node and per app in `cluster status` and the
metrics. A WebSocket room that hibernates wakes where its sockets are,
so a call through another node keeps its clients connected. `wasmkeep
cluster diagnose` checks that a bucket does what WasmKeep relies on.
Still a **pre-alpha pre-release**: for trying WasmKeep, side projects
and staging, not production. The interface between a component and the
host stays `wasmkeep:actor@0.2.0`, with the worlds `front` and
`front-core` and the interface `calls` added: a component built for an
earlier release runs as it is. Fronts and `call-many` are in the Rust
and TypeScript SDKs first. See the [roadmap](ROADMAP.md) for what comes
next.

**Upgrade: stop-all.** Every node is stopped for a one-time migration of
the bucket layout ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md));
no rolling upgrade from 0.3.x.

Soaked on three nodes against a real bucket, on a bucket prefix migrated
to the new layout, a node killed every three minutes, for two hours on
this release's code (commit `e55c152`; later commits change tests and
these notes only): 39 kills, 443,092 acknowledged durable writes, none
lost or duplicated; failover p50 17.1 s, p99 21.0 s, max 22.7 s; no
alarm missed; no stale read after a takeover; a restarted node ready
again within 0.3 s
(`bench/results/2026-11-14-e55c152-soak-ovh-bhs-3vps-2h-0.4.0-rc.csv`).
An earlier two-hour soak on the code of app-owned state and the
migration (commit `7e03c05`) gave the same picture
(`bench/results/2026-11-14-7e03c05-soak-ovh-bhs-3vps-2h-0.4.csv`).

### What changed

- **`wasmkeep.toml`: the app is what the file says**
  ([ADR 0026](docs/adr/0026-the-project-file.md),
  [reference](docs/project-file.md)). A project file names the app, its
  front, its classes with their rule (`internal` unless it says
  otherwise), configuration file and required names, and a build command
  for each part. `wasmkeep deploy --bucket ...` in its directory (or with
  `--project PATH`) runs the builds, prints every change before it
  switches, and deploys exactly the file: a class or front the file no
  longer has is removed, and configuration or required names it does not
  give are removed. Every class's rule is written in the version, `open`
  included (manifest format 3, now also without a front). Unknown keys
  are refused with their line and column. `--rollback` runs no build.
  `wasmkeep dev` with no crate directory runs the file: the front and the
  classes with their rules, local values from `.env.local`, rebuilt from
  each part's `watch` paths, for any language with a build command.
  **Behaviour change:** in a directory with a `wasmkeep.toml`, `deploy`
  refuses `--actor`, `--access`, `--config`, `--requires` and the front
  flags, and `dev` refuses crate directories, `--access` and `--env`;
  pass `--no-project` to use them. Deploys with flags elsewhere are
  unchanged, their default of `open` included. A build runs without
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`:
  the deploy's bucket credentials are not the build's, and a build that
  needs credentials of its own takes them from a profile or a file.
- **A WebSocket room wakes where its sockets are**
  ([ADR 0024](docs/adr/0024-socket-home.md)). A room that hibernates with
  sockets open keeps its ownership record, so an event reaching it through
  another node (a front's `actors.call`, a request, an upgrade) is relayed
  to the node holding its sockets and wakes it there, at a new generation:
  its clients stay connected. Before, the room woke wherever the event
  landed and its sockets closed with 1012. The record is released when the
  room's last socket goes, and by a drain, a move, a lapse of the node
  lease or a clean stop. Socket classes may now be reached through the
  front on a cluster. A room parked this way is free about 15 to 20 s
  after its node dies, as a resident actor, instead of at once. New
  metrics: `wasmkeep_parked_records`, `wasmkeep_parked_wakes_total` and
  `wasmkeep_parked_releases_total{reason}`; the owner sweep no longer
  reads the record of a parked room each minute (one pass in ten, as a
  safeguard). No format changes: a node of an earlier version beside one
  of this version keeps closing its own sockets as before.
- **`wasmkeep cluster diagnose` checks a bucket.** It runs, through the
  nodes' own bucket code, what WasmKeep relies on: conditional creates
  and replaces (one winner among simultaneous creates, and among
  simultaneous replaces of one version, a stale, deleted or overwritten
  version refused, a read's version accepted), reads right after
  writes, overwrites and deletes, listings that show a new key and drop
  a deleted one (the lag is measured) and return more than one page,
  deletes of absent keys, a 16 MiB round trip, last-modified order and
  precision, and this machine's clock against the bucket's. One line per
  check, then `safe for WasmKeep` (exit 0), `not safe: ...` (1) or no
  verdict (2, also for a safe run whose cleanup failed); `--json` for
  scripts. It writes only under a scratch prefix of its own
  (`run-<time>-<pid>/` in `diagnose/`, or in `--prefix-for-test`) that
  must be empty, and deletes it afterwards even when a check fails. The
  bucket requirements, how to run it and the providers checked so far
  (moto and OVHcloud Object Storage) are in
  [Buckets](docs/deploy/buckets.md).
- **Fronts in Rust and TypeScript.** The Rust SDK has a `front` module
  (from world `front-core`): `actors` and `env` as in an actor,
  `export_front!`, and `http::respond_with`. A front crate depends on
  `wasmkeep-sdk` with `default-features = false`: the new default feature
  `actor` holds the actor world (storage, alarms, WebSockets, `Actor`,
  `export!`, `kv`, `queue`, `workflow`), which a front must not import.
  Actors keep the defaults and change nothing. The TypeScript SDK has
  `@wasmkeep/sdk/front` (`defineFront({ fetch })`) and `wasmkeep-ts build
  --world front`. Both have the helpers `caller()` (`Front`, `Actor`, or
  `Outside`; only the exact `<tenant>/_front/main` is the front), `host()`,
  `scheme()` (`https` when the first `X-Forwarded-Proto` value is) and
  `unavailable(err)` (503 with `Retry-After: 1` for `failed` and
  `timeout`, 500 otherwise, with a generic body), exported for actors too.
  TypeScript requests gain `authority`, `headerList`, and bounded body
  reads: `req.bytes(limit)` and `req.text(limit)` return `undefined` for
  a body longer than `limit` bytes, having read at most `limit + 1` of it,
  chunked or not. Example:
  [`examples/todo-front`](examples/todo-front) (Rust front and `internal`
  class) and [`examples/ts-todo-front`](examples/ts-todo-front). Python,
  Go, C, Zig, D and MoonBit fronts come later
  ([ADR 0023](docs/adr/0023-the-front.md)).
- **Calling several actors at once.** A new interface,
  `wasmkeep:actor/calls`, imported by worlds `actor-core` and
  `front-core`: `call-many(list<(address, request)>)` starts up to 16
  calls together and returns one `result<response, call-error>` per
  entry, in input order, once all have answered or the batch's one
  deadline passed (an actor's call deadline, a front's call deadline). A
  front gathering four slow actors pays about one round trip, not four.
  Each entry is an `actors.call` in every other respect: caller value,
  chain (an entry naming the caller is `reentrant`), routing and signed
  hops between nodes, and from a front, its count against the per-actor
  bound and in `wasmkeep_front_calls_total`. The calls a node's fronts
  have waiting at once are now at most their slots (`front busy: too
  many front calls waiting on this node`), so batches hold no more actor
  instances than single calls could. A batch of more than 16
  starts nothing and fails every entry with `failed("too many calls:
  ...")`; its requests together, and its responses together, are capped
  at 16 MiB (`too-large`). Two callees of one batch that call each other
  wait until the deadline (`timeout`). Rust: `calls::call_many` (also
  `wasmkeep_sdk::front::calls`); TypeScript: `actors.callMany`, which
  returns a `CallError` per failed entry instead of throwing. Other SDKs
  later. The package stays `@0.2.0`: built components do not import the
  interface and run unchanged; one that imports it needs this release.
  The todo-front examples gain `GET /api/team`, every user's count from
  one batch ([ADR 0025](docs/adr/0025-calling-several-actors-at-once.md)).
- **An actor's `actors.call` gives up before its own event is cut.** A
  call from an actor now fails with `timeout` while 5 s of the caller's
  event wall remain (a fifth of a wall shorter than 25 s), as a front's
  call does; before, it waited the whole wall from
  when it started, so the caller's event was always cut first and its
  client got the failed event's answer instead of what the guest made of
  the `timeout`. A batch's entries share that deadline, so the ones that
  answered keep their answers.
- **`wasmkeep cluster status` shows each node's load**: ACTORS, SOCKETS
  and EV/MIN (events per minute over the last minute or more) per live
  node, and ACTORS per app, summed over its live nodes; `actors`,
  `sockets` and `events_per_min` in `--json`, `null` when unknown. Nodes
  say it on a new last line of their heartbeat, which nodes of 0.3.0
  never read; a node that is not live, or of an earlier version, shows
  `-`.
- **Every hop between nodes names its app, and a node refuses another
  app's.** A forwarded request, a relayed WebSocket upgrade and a call
  between nodes carry `x-wasmkeep-app: tenant/app`, the sending node's
  app, covered by the signature on signed calls. A node answers a hop for
  an app it does not serve with 503, `Retry-After: 1`, "not this app's
  node", before reading its body, and the actor does not run. A client's
  header is never believed or passed on: it is removed from what clients
  send to actors and from what guests send, and never reaches an actor. `serve` without `--app` (classes given on
  the command line) runs as the implicit app `default/default`, and says
  so in its heartbeat (it said `default/` before); its keys are unchanged
  ([ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
  Nodes of earlier versions send no app: see Upgrading below.
- **`actor move` names the actor's app**: `wasmkeep actor move --app
  acme/shop room/lobby --to node-c`. Without `--app`, the actor is written
  `default/CLASS/NAME`, of the implicit app; another tenant in front
  (`acme/room/lobby`) is refused with a message asking for `--app`. A
  target serving another app gets a warning
  ([moving one actor](docs/cluster.md#moving-one-actor)).
- **Metrics name the node's app, and `wasmkeep_info` its version.**
  Every series with a `class` label (`wasmkeep_events_total`,
  `wasmkeep_event_duration_seconds`) also carries `tenant` and `app`: the
  app the node serves, or `default/default` without `--app`. This changes
  those series' identity: selectors that match every label exactly, and
  recording rules or dashboards that list all of them, need `tenant` and
  `app` added; `sum by (class)` and selectors on some labels keep
  working. The new gauge `wasmkeep_info{version,tenant,app,node}`, always
  1, shows which version each node runs (`node` only in a cluster); other
  series are unchanged ([metrics](docs/monitoring.md#metrics)).
- **Series from zero, and rules for failover waits and refusals.**
  Counters whose labels take a fixed set of values are on the metrics
  page from the node's start, every series at 0:
  `wasmkeep_failover_waits_total`, `wasmkeep_bucket_requests_total`
  (`status="error"` included), `wasmkeep_event_cuts_total`,
  `wasmkeep_forwards_total`, `wasmkeep_signatures_refused_full_total`,
  `wasmkeep_websocket_owner_closes_total`,
  `wasmkeep_websocket_sweep_unreadable_total`,
  `wasmkeep_parked_releases_total` and the front's two counters, so
  `increase()` counts the first event. `wasmkeep_not_ready` has one series
  per reason, 1 for the reason the node is not ready and 0 for the others,
  where it had a single series only while not ready: count not-ready
  nodes with `count(wasmkeep_not_ready == 1)`, as the dashboard now does.
  `deploy/prometheus/alerts.yml` adds `WasmKeepFailoverWaitsFailed`
  (requests kept for a takeover that got a 503: `expired`, `over_cap`),
  `WasmKeepAdmissionRefused` (5 minutes of `capacity`, `overloaded` or
  `wrong_app` refusals) and `WasmKeepAdmissionUnavailable` (15 minutes of
  `unavailable`, which ticks briefly while nodes catch up with a deploy or
  a secrets change); the Grafana dashboard charts both in a new row
  ([alerts](docs/monitoring.md#alerts-worth-having)).
- **A build names its commit.** A build from a git checkout reports
  the release and its commit, `0.4.0+<commit>` (`wasmkeep --version`, its
  heartbeat, so the VERSION column of `cluster status`, and
  `wasmkeep_info{version}`); a build of a release's tag, or one without
  git (a source tarball, a container image built locally), reports the
  release alone. Before, every build reported the release alone. The usage report still sends the release version alone.
- **Clock skew, one figure.** The pages that gave the lease timings'
  tolerance as "a few seconds" now say what the code allows, as
  `cluster diagnose` does: 1 s between nodes, and between a node and the
  bucket.
- **Clearer command and start messages.**
  - The first `wasmkeep deploy` of a new app no longer prints that access
    rules now come with deploys; only an app deployed before they did is
    told, once.
  - Nodes log their access rules as one INFO line per start and per
    deploy (`access rules (deployed): classes open to outside requests:
    counter; existing actors only: hub`), instead of a WARN that every
    class is open and a line per restricted class. A node following an
    app that never deployed rules still warns.
  - `cluster status` shows `-` in STATE for a node that is not live
    (fenced, released or stale), as it already did for its load; LIVE
    says why. `state` is `null` in `--json` for such a node.
  - A node's load is read as each heartbeat is written, with its drain
    state of the same moment: ACTORS lags by at most one heartbeat (an
    actor just taken over shows at the next one), and a `drained` node no
    longer shows the actors it just handed over.
  - `cluster drain` says the node reports `draining`, then `drained` once
    its actors are handed over, and that a small drain may go straight
    to `drained`.
  - A cluster node logs `checking that no other process runs node id a;
    ready in about 15 s` when its start check begins, and `no other
    process runs node id a: ready to take actors` when it ends.
  - Log lines name an actor as its app does, with the app as a field of
    its own: `app=demo/shop actor=counter/alice`, instead of
    `actor=demo/counter/alice` (tenant, class and name), in the guest log
    and every other line about an actor. Log queries on `actor=` need the
    new form.
  - A command whose output goes to a closed pipe (`wasmkeep cluster
    status --json | head`) exits quietly with status 141 instead of
    panicking with "failed printing to stdout: Broken pipe".
  - `serve --app` started before the app's first deploy waits for it
    (`waiting for the first deploy of demo/shop (wasmkeep deploy --app
    demo/shop --bucket s3://demo ...): this node starts serving when one
    lands`), reading `current` every 3 s as for new versions, instead of
    exiting with "nothing is deployed for demo/shop yet". It neither
    listens nor writes a heartbeat until then; nodes and the first deploy
    may now start in any order.
  - A deploy that changes nothing says `demo/shop: nothing changed:
    version V is already current (deploy N)` (it wrote nothing and `N` is
    unchanged), and a rollback says `rolled back demo/shop: FROM -> TO
    (deploy N)`, instead of the line of a new deploy. From a project
    file, the no-op line replaces `nothing changed: the deployed version
    is the file's`. Scripts that read `version V is current` after a
    rollback need the new line.
  - The warning when a deploy or rollback removes the front of an app
    with secrets gives the two commands to run in full, with the bucket
    and app filled in: `wasmkeep secrets list --bucket B --app T/A --key
    KEY_FILE` and `wasmkeep secrets unset --bucket B --app T/A --key
    KEY_FILE --front NAME...`.
  - `cluster status` shows front requests per minute per node
    (FRONT/MIN; `front_per_min` in `--json`, and a new
    `front_per_min=<n|->` key on the heartbeat's load line, which
    earlier readers skip), and whether each app's deployed version has a
    front (FRONT, `yes` or `no`; `front` in `--json`). EV/MIN still
    counts actor events only.
  - `deploy --rollback` prints `rolling back demo/shop to V:` above the
    changes it lists, and its note that a project file may now describe
    another version goes to stdout with them, before the `rolled back`
    line, instead of to stderr.
  - `wasmkeep serve` and `dev` write their log without colour codes when
    stdout is not a terminal (a file, a pipe, journald), and honour
    `NO_COLOR`; on a terminal they keep their colours.
  - The log line for unset required values names both ways to stop
    requiring them: `requires` in `wasmkeep.toml`, or `--requires
    CLASS=` / `--front-requires` without a project file (deploy refuses
    those flags beside one), and gives `secrets set` with `--class CLASS
    NAME` or `--front NAME`.
- **A move of a room parked with open sockets waits for its record's
  release**: one whose release did not land was reported done, its
  request deleted, while the room stayed. Once it lands and the target
  takes the room, the sockets left on the old node close with 1012 at
  once, not at the next owner sweep. An ownership record that cannot be
  decoded no longer stops every owner sweep pass at that room: it is
  passed over, logged once and counted in the new
  `wasmkeep_websocket_sweep_unreadable_total{read}`. A lapse or a clean stop no
  longer writes a second release for each parked room whose sockets it
  closed ([ADR 0024](docs/adr/0024-socket-home.md)).
- **A drain or a move no longer waits until the node stops for an actor
  whose ownership write went unanswered and landed late**: the node now
  watches such a record for 30 s and releases it if the write lands
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#3-the-ownership-record),
  amended 2026-11-14).

### The front, host side

The stateless component given to `serve` becomes the app's front
([ADR 0023](docs/adr/0023-the-front.md)). It ships with the app's deploy:
see the next section.

- **The front calls actors.** The component is linked with `actors` and
  `env` besides WASI and `wasi:http` (the new WIT worlds `front-core` and
  `front`; the package stays `0.2.0` and world `actor` is unchanged, so
  built actors and built stateless components run as they are). Its
  `actors.call` reaches the `--actor` classes as an inside caller:
  `internal` classes included, and the callee sees `x-wasmkeep-caller:
  default/_front/main`. Without `--actor`, its calls fail with
  `invalid-address`. Calls to an actor on another node are signed hops, as
  an actor's.
- **`serve --front-env FILE`** gives the front its values (`env.get`), as
  `--env CLASS=FILE` does for a class; `--env _front=FILE` is refused.
- **Its own limits.** `--max-front-requests N` front requests at once (by
  default a tenth of `--max-instances`, rounded up, beside actors, and the
  whole pool on a node that serves only a component): past it, 503 with
  `Retry-After: 1`, without running the front; resident actors get the
  rest of the pool. A quarter of those slots may wait on one actor at
  once: past it the call fails with `failed("front busy: ...")`. A call
  from the front gives up with `timeout` while 5 s of its 30 s wall remain.
  The front's CPU budget is its own flag, `--front-cpu-budget-ms` (50 ms by
  default); `--cpu-budget-ms` is the actors' only.
- **Metrics**: `wasmkeep_front_requests_total{tenant,app,outcome}`,
  `wasmkeep_front_request_seconds`, `wasmkeep_front_slots_in_use` and
  `wasmkeep_front_calls_total{tenant,app,outcome}`
  ([metrics](docs/monitoring.md#metrics)); a client that leaves before
  the end of its answer counts as `outcome="client_gone"`.

Behaviour changes:

- With `--app`, a component on the `serve` command line is refused at
  start; such a node answers 404 outside `/actor/` until its deploy brings
  a front.
- Without `--app`, the component gets `actors` and `env`.
- A client's `x-wasmkeep-*` headers and trailers no longer reach the
  stateless component, and its own `x-wasmkeep-*` response headers and
  trailers no longer reach the client. The host removes them silently:
  the component may set them without an error.
- A client's `x-wasmkeep-*` headers no longer reach an actor at
  `/actor/...`, the SDKs' key-value and queue names included (before,
  only the names the host reads were removed); the forwarded mark still
  does.
- An answer to `actors.call` no longer carries any `x-wasmkeep-*` header
  (such as `x-wasmkeep-node` when another node answered), nor the hop's
  framing headers (`transfer-encoding`, `connection` and the like; the
  body is whole, and `content-length` stays), for actors too.
- A client that leaves before the end of its answer stops the stateless
  component's instance and frees its slot; a whole answer (its declared
  length sent, its trailers, or no body: `HEAD`, 204, 304) lets it run
  on, up to its wall.
- No free instance for the stateless component answers 503 with
  `Retry-After: 1`, no longer 500.
- `serve --app --max-instances 1` is refused at start: an `--app` node
  keeps slots for the front its deploy brings, beside an actor.
- The stateless component's CPU budget is `--front-cpu-budget-ms`, no
  longer `--cpu-budget-ms`.
- A guest's own `x-wasmkeep-*` headers on `actors.call` no longer reach
  the callee, for actors too (before, a local call passed a guest-set
  `x-wasmkeep-forwarded` or `x-wasmkeep-node` on unchanged), except the
  four the SDKs' key-value and queue clients send
  (`x-wasmkeep-kv-expect`, `x-wasmkeep-kv-ttl-ms`, `x-wasmkeep-kv-durable`,
  `x-wasmkeep-queue-delay-ms`).
- `/actor/...` no longer reaches a stateless component on a node without
  actors: it answers 404.
- A `1xx` status from the stateless component becomes a 500.
- The guest log names the stateless component `actor=_front` instead of
  `actor=stateless`.

### The front in deploys

An app's front ships in its version, with the classes
([ADR 0023](docs/adr/0023-the-front.md),
[deploying the front](docs/cluster.md#deploying-the-front)).

- **`wasmkeep deploy --front PATH`** sets the front, `--no-front` removes
  it, and without either the current one is kept (copied inside the
  bucket). `--front-config FILE` and `--front-requires NAMES` are its
  `--config` and `--requires`, kept by later deploys, removed with an empty
  value. `--rollback` conflicts with the four, and brings back the
  previous version's front with its classes. A first deploy may give a
  front and no class.
- **Checked before anything is written**: the front must link against the
  front world, export the incoming handler and instantiate once (without
  values, required names or not); one whose start function traps or that
  needs more than the limits, one that imports `storage`, `alarms` or
  `websockets`, or one that exports an actor's `on-*` functions, is
  refused. `--config _front=...`, `--requires _front=...` and `--access
  _front=...` are refused too, naming the front's own flags.
- **The front's secrets stay when it goes**: `--no-front`, or a rollback
  to a version without a front, warns while the app has secrets that its
  `_front` entries remain (`wasmkeep secrets unset --front NAME...`
  removes them): a node older than the front cannot open a set with them,
  and a later front would read them.
- **`deploy` prints** `front: added`, `changed`, `removed` or `unchanged`,
  the front's values by name (`config _front: NAME added`), and each open
  class as `reachable at /actor/<class>/<name>, past the front`.
- **Nodes following the app (`serve --app`)** load the version's
  `_front.wasm` (digest checked), link it before taking any lock, and swap
  it in with the classes; a front that does not link is a version the node
  cannot apply (it keeps the previous one and reports not ready). Without
  a front, paths outside `/actor/` answer 404.
- **Its values** come from the same three layers as a class's: the
  deployed `--front-config`, the app's secrets in the new scope `_front`
  (`wasmkeep secrets set --front NAME`, `unset --front NAME`, `_front
  <name> <length>` entries in `set --file`, shown by `secrets list`), and
  the node's `--front-env`.
- **Required values**: while a name in `--front-requires` is not set, or
  the node cannot open the app's secrets, the front is not run and the
  node answers a generic 503 with `Retry-After: 1`; the missing names go
  to the log once and to the `unconfigured` outcome of
  `wasmkeep_front_requests_total`, never to the client.
- **Bucket**: a version with a front is written as manifest format 3
  (`front main <sha256>`, `config _front`, `requires _front`; it also
  accepts an explicit `access <class> open`), with `_front.wasm` and
  `_front.config` beside the classes, collected with their version.
  Versions without a front keep format 1 or 2, the same bytes and ids.
- **Heartbeats** announce the capability word `front` beside `take`. A
  deploy or rollback making a format 3 version current, and a secrets
  `set` or `reset` that gives the set its first `_front` entry, are
  refused while a live node of the app lacks it (for secrets, one that
  reads them), with no `--force`: such a node could not read the version,
  or any of the app's secrets.

### App-owned state

These change the layout in the bucket and on disk; they supersede the
second-app refusal and the migration guard listed under 0.3.0. See
"Upgrading to 0.4.0" before starting a node of this version
on a bucket that holds actor state.

- **Each app's actor state lives under its own root,
  `state/<tenant>/<app>/`**, beside `apps/<tenant>/<app>/`: history,
  ownership records, old-lease markers, the alarm index and move requests
  (`state/<t>/<a>/{actors,owners,leases,alarms,moves}/`). Each key keeps
  its tail, byte for byte; only its front changes. On disk the databases
  move to `<data_dir>/state/<t>/<a>/actors/`. Nodes given classes on the
  command line (`serve --actor`, `dev`) keep theirs under the implicit app
  `default/default`
  ([ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
- **Two apps of one tenant share a bucket prefix safely.** The same class
  and actor name in two apps are two actors. `deploy` no longer refuses a
  second app of a tenant, `deploy` and `serve` no longer warn about apps
  sharing a prefix, and a node no longer refuses to start next to a node
  of another app. A node still refuses to start next to a live node of
  the version before (its heartbeat names `tenant/`).
- **`wasmkeep cluster migrate`** moves a bucket prefix written by 0.2 or
  0.3 into the new layout, once, while every node is stopped. Without
  `--apply` it is a dry run that writes nothing: it prints the app each
  class goes to, the nodes it sees and whether they may still run, object
  counts and an estimate. A class goes to the one app whose current
  version deploys it; a class in no manifest goes to the tenant's only
  app, or to `default/default` for the tenant `default` with no app (or
  only an app named `default`); a class two apps deploy, a tenant with
  several apps, or the tenant `default` with another app needs `--heir
  TENANT/APP`, which must name an app of the tenant (or
  `default/default`); the other apps start a shared class empty.
  `--apply` waits one node-lease window (16 s with the default timings,
  so that a node whose clock disagrees with the migrator's shows itself),
  copies server-side, verifies, then puts barriers in front of older
  binaries (token `3` in every app's `current`, a tombstone over every old
  ownership record, and one where an actor has history but no record),
  proves that no node ran, and commits each tenant
  (`cluster/migrated/<tenant>`). Move requests are not copied: a copy
  would renew them. It refuses while a node may still run and fences
  nothing; after a crash, run it again. Two runs at once stop each other
  before either commits a state the other undid. `--rollback` undoes it
  while no node of any version has started and no app was deployed
  since; after that, it is one-way. Nothing of the old layout is deleted
  by it. Exit codes: 0 done, 1 refused or aborted, 2
  partly committed ([upgrading to 0.4.0](docs/deploy/upgrading-to-0.4.md)).
- **`wasmkeep cluster migrate --remove-old`** deletes what a migration left
  in the layout before 0.4.0, for every migrated tenant: a dry run that
  lists it, then `--remove-old --apply`. It keeps the tombstones over the
  old ownership records for good, history younger than
  `--history-retention-secs` (3600 by default) and move requests not yet
  expired (5 minutes) for a later run, and never touches `state/`,
  `apps/`, `nodes/` or a tenant with no commit. It refuses while a run of
  migrate is unfinished, while the rollback would still work (no node
  started since; `--forgo-rollback` gives it up), and on any old-layout
  key written after the commit, an old record that is not a tombstone or
  history with no tombstone (a binary before 0.4.0 ran on the prefix),
  naming each key. Before its first delete it writes
  `cluster/removed/<tenant>`, after which `--rollback` refuses for good;
  a tenant with nothing to delete gets none. It stops, deleting nothing,
  if a `--rollback` or `--apply` ran meanwhile. It can be run again after
  a crash.
- **`actor move` runs the start check `serve` runs**: it refuses a tenant
  whose state is still in the layout before 0.4.0, printing the migrate
  command, instead of writing a request no node reads.
- **`wasmkeep_admission_refused_total` counts wrong-app hops** under their
  own reason, `wrong_app`: a hop from another node or a forwarded request
  naming another app (503 "not this app's node"), or no app (a node of an
  older version). The page now has five series.
- **`serve` and `deploy` of this version check the bucket before they
  write there**: they refuse while `cluster/migration` exists, and refuse
  a tenant whose state is still in the layout before 0.4.0, printing the
  command to run. Other commands that write to the bucket (`secrets`,
  `actor move`, `cluster drain`, `fence`, `forget`) refuse while
  `cluster/migration` exists, or when they cannot tell. `cluster status`
  reads on.
- **Token `3` in `current`.** Every deploy and rollback of this version
  writes it, as does `cluster migrate`; it means the app is marked (ADR
  0013: rules come with deploys, nodes refuse `--access` with `--app`)
  and its state is rooted. `serve --app` refuses a `current` without it:
  run `cluster migrate --apply`, or deploy once with this version.
  Versions before 0.4.0 cannot read it: their `serve --app` and `deploy`
  stop.
- **A node moves its local databases once, at start.** In a cluster the
  bucket is the truth: `<data_dir>/actors/<tenant>` goes aside to
  `<data_dir>/actors.pre-0.4/<tenant>`, and each actor restores from its
  new root on its first wake. Alone (`dev`, `serve` without a cluster) the
  local copy is the only one: it becomes the app's, if no app of the
  tenant has local state yet; otherwise the start is refused. One rename,
  never a copy; a file is left at the old path so an older binary fails
  instead of starting empty. Both folders present is refused, never
  merged.
- **Moves** are listed under the node's own root; a move written by an
  older `actor move` (`control/move/...`) is not read, and not migrated:
  request it again after the upgrade.

### Upgrading to 0.4.0

From 0.3.0, on a bucket prefix that holds actor state (a prefix last run
by 0.2.0 has the same layout and takes the same steps; read the notes
under 0.3.0 for the rest of that upgrade):

1. Freeze deploys, secret changes, `actor move` and drains (CI
   included).
2. Stop every node: the service units, not just the processes. Then back
   up the bucket.
3. Install 0.4.0 on every node, without starting it.
4. `wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX`: read the plan;
   give `--heir` where it asks.
5. Run it again with `--apply` (and the same `--heir`).
6. Start every node, and check the app.
7. The same day: `cluster migrate --remove-old`, then `--remove-old
   --apply`.

The whole procedure is in [upgrading to
0.4.0](docs/deploy/upgrading-to-0.4.md). **One-way after the first
start.** Until a node of any version starts, `cluster migrate
--rollback` puts the old layout back for the older binaries. Once one
has started, the migration is one-way; the old keys stay in the bucket
until `--remove-old --apply`. Keep a backup from before the migration
until then. Lifecycle rules, access policies and backup
scripts written for `actors/<tenant>/` must move to `state/<t>/<a>/`.
Nodes that followed an app with `--access` and no deployed rules must
deploy those rules (`deploy --access`) and start without `--access`.

With or without actor state:

- Stop every node of a cluster, upgrade them all, start them all
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)): nodes of this
  version refuse every hop from a node of an earlier one (a forwarded
  request, a relayed upgrade or a call, which carries no app) with a 503
  saying it came from a node of an older version. The other way, a node
  of an earlier version fails the signature of this version's calls:
  calls to `internal` classes, moves and alarm hand-offs fail, and other
  calls run there as requests from outside, with no calling actor.
- A node of this version started while a 0.3 node given classes on the
  command line is still live on the bucket prefix (its heartbeat names
  `default/`) refuses to start, and asks to stop every node and upgrade
  them together; so does a 0.3 node started next to a live node of this
  version, though its message speaks of another app of the tenant.
- `wasmkeep actor move TENANT/CLASS/NAME` becomes `wasmkeep actor move
  --app TENANT/APP CLASS/NAME` for an actor of a deployed app; scripts
  that move actors of `default/default` keep working.
- Components built for 0.1.0 to 0.3.0 need no rebuild. The SDKs are at
  `0.4.0` on their registries, with the server. Actors built with the
  Rust SDK keep its default features; a Rust front depends on
  `wasmkeep-sdk` with `default-features = false`.
- Dashboards, recording rules and exact selectors on the series with a
  `class` label add `tenant` and `app`; log queries on `actor=` use
  `app=TENANT/APP actor=CLASS/NAME` (both under What changed).

### Known limits

The limits listed for 0.3.0 still hold. Also:

- The front cannot yet accept a WebSocket at its own path and hand it to
  an actor: a room is still reached at `/actor/<class>/<name>` (planned
  for 0.4.1, [roadmap](ROADMAP.md#now)).
- Fronts and `call-many` are in the Rust and TypeScript SDKs only; the
  other SDKs build actors as before.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.3.0

**Reliable enough to leave running.** This release fixes the known ways
a cluster stalled or lost an answer when a node or the bucket misbehaved,
keeps a request for a dead node's actor until the takeover instead of
refusing it, and adds the guides that running an application on a cluster
showed missing. Still a **pre-alpha pre-release**: for trying WasmKeep,
side projects and staging, not production. The interface between an actor
and the host is unchanged (`wasmkeep:actor@0.2.0`), and so are the SDKs'
APIs: a component built for 0.1.0 or 0.2.0 runs as it is. See the
[roadmap](ROADMAP.md) for what comes next.

Soaked on three nodes against a real bucket, a node killed every three
minutes: twelve hours on a release candidate (239 kills, failover p99
22.43 s, no acknowledged write lost or duplicated, no alarm missed;
`bench/results/2026-11-12-b849bfe-soak-ovh-bhs-3vps-12h.csv`), then three
hours on this release's code (59 kills, failover p99 20.9 s, the same
result; `bench/results/2026-11-13-648c2d1-soak-ovh-bhs-3vps-3h.csv`).

### What changed

- **A request waits for a takeover.** When an actor's node dies, a GET or
  HEAD without a body, or a WebSocket upgrade, for one of its actors is
  kept by the node that received it until the actor can be taken over (15
  to about 21 s after the dead node's last write, unchanged), then served,
  instead of being answered 503 meanwhile. A request with a body, an
  actor-to-actor call and a forwarded request still get the 503 at once,
  which still means it did not run. A client that leaves while its
  request is kept is dropped, and its request never runs. At most a
  quarter of the node's connection limit, and 64 per actor, are kept;
  beyond that, the 503 at once. `--failover-wait-secs` sets the wait
  (about 25 s by default, from the lease timing); 0 turns it off. Set the
  load balancer's server timeout above it.
  `wasmkeep_failover_waits_total` counts how each wait ended
  ([what a caller sees](docs/cluster.md#when-a-node-dies-what-a-caller-sees),
  [front door](docs/deploy/front-door.md)).
- **A snapshot upload is bounded, tried again after a failure, and
  counted.** The snapshot an actor's owner adds every 64 writes holds the
  actor while it uploads. The upload had no bound of its own, so a bucket
  that left it unanswered held the actor until the bucket client ended the
  request (30 s by default). It now gets one attempt, given up after 5 s
  plus a second per megabyte, 10 s at most. A snapshot that fails costs no
  write and fails no event, and is tried again once the actor has made one
  more write and 30 s have passed, then 8 writes later, then every 64; a
  failure used to put the next snapshot 64 more writes away. The node
  keeps these waits while an idle actor is out of its memory.
  `wasmkeep_snapshot_upload_failures_total` counts the snapshot uploads
  that were not acknowledged
  ([replication](docs/cluster.md#replication),
  [monitoring](docs/monitoring.md)).
- **A restore's reads have one deadline, a refused read is sent again,
  and failed restores are counted.** Restoring an actor from the bucket
  bounded each read but not the reads together, so reads that each stayed
  under their limit added up with the length of the history while every
  caller of the actor waited. The reads of one restore now end within
  30 s, plus a second per megabyte of what it reads and a second per
  eight objects. The clock runs only while the restore waits for the
  bucket to give it the object it applies next: the listing, work on the
  local disk and the wait for a turn among the node's other restores are
  not counted. Neither are the seconds that other restores' reads on the
  node add to the limit of a read (a second per megabyte in flight, 256 at
  most), so restores that read side by side after a takeover are not
  ended for each other's bytes; a restore alone on the node has no more
  than its own time. Running out of time is answered like a bucket that
  does not answer, a 503 with `Retry-After`, and leaves the local file as
  it was. A read the bucket refuses with an error a resend can get past
  (a 5xx or a broken connection, after the bucket client's own retries)
  used to fail the whole restore; it is now sent again after 0.5 s, then
  1 s, three tries per object. `wasmkeep_restore_failures_total` counts
  the restores that did not complete
  ([replication](docs/cluster.md#replication),
  [monitoring](docs/monitoring.md)).
- **A request body the handler left unread no longer costs the upload or
  the answer.** A handler may answer without reading the body. The server
  now reads and discards what is left, up to the body cap and for at most
  30 s, so a large upload is no longer cut off and, when the request came
  through another node, its answer is no longer lost. A body held back
  with `Expect: 100-continue` is not asked for, and a node that forwards a
  request does not pass `Expect` on ([concepts](docs/concepts.md)).
- **Between nodes, a WebSocket upgrade the owner never read is sent once
  more, and a forwarded request that was written is never sent twice.**
  An upgrade relayed to the actor's node that the node provably never read
  (it refused the TLS handshake, or the upgrade came back unsent on a new
  TLS connection) is sent once more while the client waits; it used to
  get a 503 asking the client to retry. Between nodes, the only other
  thing sent a second time is an `actors.call` the peer provably never
  read in the same way, once, as 0.2.0 already did after a refused
  handshake. A forwarded request is never sent again, whether its
  connection could not be opened or it was written into a connection that
  then failed: one that was written may have run on the owner, so the
  client gets a 503, which does not say whether it ran
  ([running a cluster](docs/cluster.md)).
- **A client that only listens no longer misses a room's messages
  without a close.** A room that hibernates keeps its WebSockets open on
  its node and releases its ownership record. When a request then woke it
  on another node, its sockets on the first node closed only one by one,
  each when its client next sent a message: a client that only listens
  stayed connected and silently missed everything the room said from then
  on. Now every socket of the room on that node closes with 1012 as soon
  as one of them sends, and an owner sweep, once a minute, closes the
  sockets of rooms another node took even if none of them sends. The
  sweep reads at most 5,000 records a pass, counted in
  `wasmkeep_websocket_sweep_reads_total`. A node's sockets also close with
  1012 when its lease runs out (8 s without a write reaching the bucket,
  with the defaults), before any other node can take its actors; nothing
  closed them before. A drain closes the sockets of hibernated rooms, and
  reports `drained` only once the node holds no socket. Each such close is
  counted in `wasmkeep_websocket_owner_closes_total`
  ([running a cluster](docs/cluster.md), [monitoring](docs/monitoring.md)).
- **A WebSocket the server closed stops at the close.** Its client used to
  keep it open by not answering the close, and its messages still ran
  events; a node draining waited on it forever. Now nothing it sends after
  the close is delivered, and it is dropped if its client has not answered
  within 5 s.
- **An idle WebSocket is pinged after 25 s** of silence from its client,
  instead of 60 s, so a load balancer's usual idle timeout of 60 s no
  longer cuts it first. The front door needs an idle timeout above 30 s
  ([front door](docs/deploy/front-door.md)).
- **A relayed WebSocket upgrade carries no `x-wasmkeep-*` header from the
  client.** The relay to the actor's node removed a fixed list of node
  headers; it now removes every header in that range, so a client cannot
  pass a marker only nodes may set.
- **Take and alarm hand-off calls between nodes keep their own record of
  used signatures**, so a flood of other node calls that fills the shared
  record (500,000 signatures at once) can no longer refuse the calls a
  drain and a move depend on. The take and hand-off record holds 50,000.
  Refusals because a record is full are counted in
  `wasmkeep_signatures_refused_full_total` and logged as a full record
  rather than as a replay.
- **A database set aside as `<name>.db.replaced` no longer has an older
  WAL beside it.** When a node replaces a local database with the bucket's
  copy and keeps the old file, a replacement without a WAL left the
  previous replacement's WAL next to the newer copy, where SQLite would
  apply it if the copy were opened for recovery. The previous copy's WAL
  and shared memory are now removed first.
- **`wasmkeep_admission_refused_total{reason}`** counts the requests,
  WebSocket upgrades and actor calls a node refused before their event
  ran: `access`, `capacity`, `overloaded` or `unavailable`
  ([monitoring](docs/monitoring.md#metrics)).
- **`deploy` refuses a second app of a tenant on a bucket prefix.**
  Actors are keyed by tenant, class and name, so a second app would share
  the first app's actors without a word; until now only a node refused to
  start, and only while a node of the other app was live. The first deploy
  of an app now lists `apps/<tenant>/` and refuses when another app there
  has a `current`, naming it and saying what to do: another prefix,
  another tenant when another owner runs the app, or, for an abandoned
  app, the keys to delete. Two apps of one tenant already deployed on one
  prefix are left as they were (a node of one still refuses to start while
  a node of the other is live), and `deploy` and `serve --app` warn about
  them by name; a third is refused. This lasts until apps own their state
  (planned for 0.4.0, [deploying](docs/cluster.md#deploying)).
- **A new app's tenant and app names follow the class grammar**,
  `[a-z][a-z0-9-]{0,63}`: the first `wasmkeep deploy` of an app refuses
  others and writes nothing. Actor addresses already required it of the
  tenant; the app name had no rule. An app deployed before keeps its name,
  and every later deploy and rollback of it works as before.
- **A bucket a later version migrated is refused.** Every command that
  opens a bucket first checks for the markers a later version writes while
  it moves actor state to a new layout (`cluster/migration`,
  `cluster/migrated/`), and stops with a message naming the marker. One
  read and one listing per command, bounded to 10 s.
- **The app model, in two decisions.**
  [ADR 0021](docs/adr/0021-the-app-model.md): an app is `tenant/app`, the
  unit of deploy, rollback, secrets and (from 0.4.0) state; the tenant is
  the owner of apps; 0.4.0 plans a front deployed with the app, app-owned
  state and classes `internal` by default with a project file, and later
  shared nodes for trusted tenants.
  [ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md):
  the layout of app-owned state and the one-time migration to it, planned
  for 0.4.0.
- **Guides for running a cluster**: what happens when a node dies and how
  a cluster scales ([failover and scaling](docs/failover-and-scaling.md));
  backing up and restoring a cluster's bucket
  ([backup and restore](docs/deploy/backup-and-restore.md)); the alerts
  [monitoring](docs/monitoring.md) recommends as a Prometheus rules file,
  [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml); and an
  outbound tunnel in front of the nodes
  ([front door](docs/deploy/front-door.md#an-outbound-tunnel)).
- **Releases**: the npm job of the release workflow passed the tarball as
  a bare path, which npm read as a GitHub repository; it now publishes the
  file. See the correction under 0.2.0.

### Upgrading from 0.2.0

- Upgrade every node of a cluster together, as before
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)). The formats in the
  bucket are unchanged.
- Components built for 0.1.0 or 0.2.0 need no rebuild. The SDKs are at
  `0.3.0` on their registries, with the server; their APIs are those of
  0.2.0.
- The front door: a server timeout above the failover wait (about 25 s by
  default) and an idle timeout above 30 s
  ([front door](docs/deploy/front-door.md)). `--failover-wait-secs 0`
  answers 503 at once, as 0.2.0 did.
- The first deploy of a new app refuses a tenant or app name outside
  `[a-z][a-z0-9-]{0,63}`, and a second app of the same tenant on the same
  bucket prefix. Apps already deployed keep deploying and rolling back.

### Known limits

The limits listed for 0.2.0 still hold:

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Nodes of different versions are not supported in one cluster; one
  tenant per process ([security](docs/security.md#known-gaps)); no
  independent third-party security audit yet
  ([security](docs/security.md#audits)).
- Binaries are for Linux; on macOS and Windows, use the image or build
  from source. The Zig SDK is an archive, not yet a package `zig fetch`
  resolves; D and MoonBit are used from the source.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.2.0

**The installable release.** With 0.1.0 the SDKs were used from a clone of
the repository; from this release each comes from where its language
expects it, and trying WasmKeep needs no toolchain. Still a **pre-alpha
pre-release**: for trying WasmKeep, side projects and staging, not
production. The interface between an actor and the host is unchanged
(`wasmkeep:actor@0.2.0`, a version of its own), so a component built for
0.1.0 runs as it is. See the [roadmap](ROADMAP.md) for what comes next.

### What is new

- **The SDKs install from their registries**, each at the server's
  version, `0.2.0`:

  | Language | Install | Then |
  | --- | --- | --- |
  | Rust | `cargo add wasmkeep-sdk` | [crate](crates/wasmkeep-sdk/README.md), [tutorial](docs/tutorials/first-actor.md) |
  | TypeScript | `npm install @wasmkeep/sdk` | `npx wasmkeep-ts build` ([package](sdks/typescript/README.md)) |
  | Python | `pip install wasmkeep` | `python -m wasmkeep build`, new in this release ([package](sdks/python/README.md)) |
  | Go (TinyGo) | `go get github.com/wasmkeep/wasmkeep/sdks/go@v0.2.0` | [module](sdks/go/README.md) |
  | C, Zig | `wasmkeep-c-sdk-0.2.0.tar.gz` and `wasmkeep-zig-sdk-0.2.0.tar.gz`, attached to this release | [C](sdks/c/README.md), [Zig](sdks/zig/README.md) |

  The crate, the npm package and the wheel are published from this
  release's tag once a maintainer approves each, so a registry can be a
  little behind this page. The server and every SDK carry one version;
  use an SDK with the server of the same version
  ([versions](docs/languages.md#versions)).
- **Prebuilt components**: the release carries `counter.wasm`, `kv.wasm`,
  `queue.wasm`, `workflows-orders.wasm` and `workflows-ledger.wasm`, built
  from the examples and signed like its other files. The
  [quickstart](README.md#quickstart) is now a download and one command,
  and the callers of the [key-value, queue](docs/kv-and-queues.md) and
  [workflow](docs/workflows.md#the-workflow-class) classes in other
  languages need no Rust toolchain. The notices of the libraries compiled
  into them are in `THIRD_PARTY_NOTICES-components.txt`. The binary's
  tarball has a short README of its own.
- **The C and Zig SDKs as archives**, signed like the other files: each
  SDK with the C bindings already generated, the WIT and the componentize
  script, and a README whose build command works in the extracted
  directory, without a clone or wit-bindgen. `WASMKEEP_SDK_VERSION` (C)
  and `wk.version` (Zig) carry the version.
- **A durable write on a cluster node is no longer synced to the local
  disk as well as uploaded.** `exec-durable` answers once the bucket holds
  the write, as it was documented to: one disk sync less on every durable
  write. The bucket is the only durable copy of such a write, and a node
  trusts a local file again only after checking it against the bucket. For
  whoever runs a cluster: start a node again on the same bucket and
  prefix, since after a crash or a power loss its local files may lack
  writes the bucket has ([replication](docs/cluster.md#replication)). A
  node without a bucket still syncs to its disk.
- **Languages**: Go through TinyGo moves from experimental to supported,
  with a full [Go SDK](sdks/go) and its counter, chat room and jobs app. D
  and MoonBit are experimental, each with an SDK, a counter and a chat
  room; a C# counter (componentize-dotnet) and a standard-Go counter
  (componentize-go) pass the shared tests.
  [Tiers and numbers](docs/languages.md).
- **Compared with celld**: the same counter on one node and on three, a
  node killed under writes, with a check for lost writes that gives every
  write a token of its own, repeats the takeovers with the killed node's
  disk removed, and includes a run that must lose writes
  ([WasmKeep and celld](docs/compared-with-celld.md),
  [benchmarks](docs/benchmarks.md#three-nodes-wasmkeep-against-celld)).
- **How releases are made**: a tag's release waits for CI to pass on the
  tagged commit (0.1.0 was tagged before its run had finished), and a
  version's image tag is written once. Publishing the release publishes
  the SDKs to crates.io, npm and PyPI from its tag, without stored tokens,
  each after a maintainer's approval ([releasing](docs/releasing.md)).

### Upgrading from 0.1.0

- Upgrade every node of a cluster together, as before
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)). The formats in the
  bucket and the command line are unchanged.
- Components built for 0.1.0 need no rebuild. To move an actor's source
  to a published SDK, replace the path into a clone with the registry's
  package at `0.2.0`.
- The Go module's `v0.1.0` has only the key-value, queue and workflow
  clients; the `wasmkeep` package (hooks, storage, alarms) starts at
  `v0.2.0`.

### Known limits

The limits listed for 0.1.0 still hold:

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Nodes of different versions are not supported in one cluster; one
  tenant per process ([security](docs/security.md#known-gaps)); no
  independent third-party security audit yet
  ([security](docs/security.md#audits)).
- Binaries are for Linux; on macOS and Windows, use the image or build
  from source. The Zig SDK is an archive, not yet a package `zig fetch`
  resolves; D and MoonBit are used from the source.

### Correction

Added after the release; the text above is as it was published.

- The npm package `@wasmkeep/sdk@0.2.0` was published without a
  provenance statement. The text above says the SDKs are published from
  the release's tag by the release workflow; for npm that did not happen:
  the workflow's npm job failed before it reached the registry, and the
  package was published from a maintainer's account instead. Later
  versions will be published by the workflow, with a provenance
  statement.

### Install and verify

[Install](README.md#install) downloads a binary or runs the image.
[Single node](docs/deploy/single-node.md#1-install-the-binary) shows the
cosign check, the same for every file of the release;
[Compose](deploy/compose/README.md) checks the image.

## 0.1.0

The first release, published as a **pre-alpha pre-release**: for trying
WasmKeep, side projects and staging, not production. See the
[roadmap](ROADMAP.md) for what comes next.

### What is in it

- **Actors**: WebAssembly components (`wasmkeep:actor@0.2.0` WIT world),
  one instance per name, events one at a time, a SQLite database each,
  hibernation, alarms that fire at least once, calls between actors,
  WebSockets that stay open while an actor sleeps, outbound HTTP limited to
  public addresses, and hot reload with `wasmkeep dev`.
- **Clusters**: several nodes on one S3-compatible bucket with conditional
  writes, each actor on one node at a time, its database replicated to the
  bucket and restored on another node when its node dies (free 15 to about
  21 s after the node's last write, then the restore). One lease per node,
  checked in deterministic simulation. Mutual TLS between nodes, required
  by default.
- **Operating a cluster**: `wasmkeep deploy` (rolling, with rollbacks,
  access rules and configuration), `wasmkeep cluster status`, `drain`,
  `fence` and `forget`, `wasmkeep actor move`, `wasmkeep secrets` (sealed
  in the bucket), garbage collection of old history in the bucket,
  Prometheus metrics, health and readiness endpoints, and a Grafana
  dashboard.
- **Languages**: Rust (recommended); Zig and C/C++; TypeScript (QuickJS
  by default, StarlingMonkey as an option) and Python; Go through TinyGo
  (experimental). [Tiers and numbers](docs/languages.md).
- **SDK patterns on actors**: sharded key-value store, queues and durable
  workflows ([KV and queues](docs/kv-and-queues.md),
  [workflows](docs/workflows.md)).
- **Packaging**: binaries for x86_64 and aarch64 Linux (glibc 2.28 or
  newer), each with a CycloneDX SBOM and third-party notices; a
  distroless image for linux/amd64 and linux/arm64 at
  `ghcr.io/wasmkeep/wasmkeep:0.1.0`; all signed with keyless cosign. A
  three-node Compose example against MinIO in
  [`deploy/compose/`](deploy/compose/README.md).

### Known limits

- Not production-ready: soaked against Cloudflare R2 and OVHcloud Object
  Storage with nodes killed under load, nothing lost, but not yet carrying
  real users' traffic, and not run against AWS S3
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).
- Upgrade every node of a cluster together: nodes of different versions
  are not supported in one cluster
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)).
- One tenant per process ([security](docs/security.md#known-gaps)).
- No independent third-party security audit yet
  ([security](docs/security.md#audits)).
- The performance targets on dedicated hardware are not measured yet;
  millions of actors per cluster are a goal, not a measurement.

### Install and verify

[Single node](docs/deploy/single-node.md#1-install-the-binary) checks the
binaries with cosign; [Compose](deploy/compose/README.md) checks the image.
