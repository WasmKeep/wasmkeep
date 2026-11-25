# Roadmap

What is being worked on, what comes after, and what is already built.
Dates are not promised; the order is, and features come before speed.
For what each release contained, see the [changelog](CHANGELOG.md); for
the numbers behind every claim here, [benchmarks](docs/benchmarks.md).

WasmKeep is **pre-alpha**. Its releases so far, `0.1.0`, `0.2.0`,
`0.3.0`, `0.4.0`, `0.4.1`, `0.4.2`, `0.5.0`, `0.5.1`, `0.6.0`, `0.7.0`, `0.8.0` and `0.8.1`, are pre-releases for trying it,
side projects and staging.

## Now

`0.9.0`: **operating without the author.** `0.8.1` settled 0.8 (what it
fixed is under [Built](#built) and in the [changelog](CHANGELOG.md)).

1. **Rolling upgrades of WasmKeep** (L): one node at a time from the
   previous minor version, with no stop of the whole cluster and no
   offline migration. Two consecutive versions work in one cluster, and
   a new bucket format is written only once every node reads it. Today
   every node is upgraded together unless a release's page says
   otherwise ([how](docs/cluster.md#upgrading-wasmkeep)).
2. **Verify a bucket** (M/L): a read-only command that checks every
   actor can be restored from what the bucket holds.
3. **Traces** (M/L): one request followed through the front, its actor,
   the calls it makes, the hop between nodes and the bucket write.
4. **Old upgrade paths removed** (M), each with a documented last
   version that carries it.

## Next

After 0.9.0, the items below, not before it.

## After that

Not before the releases above.

- **Operating without the author, continued**: a write acknowledged once
  another node holds it, if measuring shows it is worth its design;
  asynchronous handlers once WebAssembly's next system interface (WASI
  0.3) is ready in Wasmtime and the SDK tools, keeping one event at a
  time per actor by default. Required before a stable `1.0`: within
  `1.x`, every upgrade is rolling, one node at a time.
- **Faster, and compared.** Performance targets on dedicated hardware
  with a stated scale envelope; the comparison with celld on a cloud bucket
  with a node per machine
  ([one machine is measured](docs/compared-with-celld.md)); a run against
  AWS S3.
- **Languages**: an SDK for standard Go; outbound HTTP and the key-value,
  queue and workflow clients for D and MoonBit
  ([tiers](docs/languages.md)). Java, Kotlin and PHP cannot produce an
  actor yet, and languages that need Wasm garbage collection wait on a
  decision of their own.
- Ideas, each needing a design first: encrypting actor state in the
  bucket, composing components
  at deploy time, and loading a component at run time (a user's plugin,
  a tenant's script, generated code) with only the capabilities its
  caller grants; a bucket per app, so that an app's data can stay in its
  owner's account or a chosen country (the cluster itself keeps one
  bucket).

## Later, on demand

These do not block the releases above. Each needs a concrete workload
or an explicit owner decision.

- **The rest of the scaling family**: aggregation trees (M/L), and
  sharding with controlled resharding (L), on the shared directory.
  Automatic resharding follows only if needed.
- **More change-feed sinks** (M): PostgreSQL and further DuckDB
  integration, using the same ordering and resume contract.
- **Automatic moves of running actors** (L), slowly and within
  limits, only if placement at wake-up is not enough.
- **One socket for several actors** (M/L), with independent
  backpressure and reconnect behavior for each channel.
- **External databases and brokers through `wasmkeep:connect`**
  (L for the first connector): PostgreSQL, Redis or Valkey, MySQL
  and NATS, when a user needs them
  ([ADR 0032](docs/adr/0032-external-databases-and-brokers.md)).
- **Google Cloud Storage and Azure Blob Storage** (L) are on hold
  until the owner says otherwise
  ([ADR 0031](docs/adr/0031-google-cloud-storage-and-azure-blob-buckets.md)).
- **Isolation tiers per app** (L each): a sandboxed process per app,
  then a launcher supplied by the operator for a microVM or another
  isolation system. Both reuse the supervisor and the same three
  ports, with local sockets between processes. WasmKeep does not
  manage microVMs or choose a tenant's tier. Each tier documents
  what it protects against.
- **Whole-app point-in-time restore** (L), with an explicit
  cross-actor consistency contract.
- **Multi-region** (L): an actor's home region, a cluster and bucket
  per region. Until then, keep a cluster near its bucket
  ([why](docs/deploy/three-nodes.md#one-region)).
- **Other demand-led work** (S–L): additional client and guest
  languages, native key-value or queue interfaces, vector search,
  cron triggers, built-in TLS and a Helm chart.
- **M5, raw Linux workloads** (L): programs rather than components,
  behind the same routing and storage API.

## Not planned

A hosted cloud, a global anycast network, Cloudflare Workers API
compatibility, or our own JavaScript engine. WasmKeep is meant to be run by
you, for standard Wasm components.

## Built

By milestone, the names the [design decisions](docs/adr) use. In `0.1.0`
unless marked with a later release.

### M0: Hello component

A WebAssembly component served over HTTP with microsecond instantiation
([measured](docs/benchmarks.md#wasmkeep-on-its-own)).

### M1: Actors on one node

One instance per name, events one at a time, SQLite per actor,
hibernation, alarms, calls between actors, WebSockets that survive
hibernation, `on-init` per version, hot reload with `wasmkeep dev`, and a
Rust SDK ([concepts](docs/concepts.md)).

### M2: Cluster and durability

Several nodes on one S3-compatible bucket
([running a cluster](docs/cluster.md)).

- Replication of each actor's changes to the bucket as LTX files
  ([ADR 0005](docs/adr/0005-replication-format-ltx-v3.md)).
- Ownership: one lease per node since
  [ADR 0020](docs/adr/0020-one-lease-per-node.md), which replaced a lease
  per actor. A dead node's actors are free 15 to about 21 s after its last
  write, then restored on another node.
- Forwarding between nodes: HTTP, actor calls and WebSockets.
- `wasmkeep deploy` and rolling upgrades of application code
  ([ADR 0008](docs/adr/0008-deploy-and-rolling-upgrades.md)).
- Every change to these protocols tested in deterministic simulation,
  under thousands of seeded failures
  ([ADR 0006](docs/adr/0006-simulation-tool.md)).
- Soaked on real buckets, R2 and OVHcloud Object Storage: 36 kills under
  load in four runs and twelve hours with 35 more, no acknowledged write
  lost or duplicated
  ([measured](docs/benchmarks.md#three-nodes-against-a-real-bucket)).

### M3: First public release

Languages, documentation, hardening, and `0.1.0`
([changelog](CHANGELOG.md)). What is left of it is under [Now](#now) and
[Next](#next).

- Languages ([tiers and numbers](docs/languages.md)). Recommended: Rust.
  Supported: Zig, C/C++, TypeScript (QuickJS by default,
  [ADR 0018](docs/adr/0018-quickjs-the-default-typescript-engine.md)),
  Python, and in `0.2.0` Go on TinyGo with a full [SDK](sdks/go).
  Experimental, in `0.2.0`: D and MoonBit, each with an SDK, a counter
  and a chat room; a C# counter and a standard-Go counter.
- **Installable, in `0.2.0`**: the Rust, TypeScript and Python SDKs from
  their registries (crates.io, npm, PyPI) and the Go module by its tag,
  published by the release itself ([releasing](docs/releasing.md)); the C
  and Zig SDKs as archives in the release; prebuilt example components,
  so trying WasmKeep needs no toolchain; a getting-started path from a
  release, not a clone ([languages](docs/languages.md)).
- In `0.2.0`: a durable write on a cluster node no longer syncs to the
  local disk as well as the bucket.
- Mutual TLS between nodes by default
  ([ADR 0011](docs/adr/0011-node-to-node-mtls.md)); alarms that survive
  their node ([ADR 0009](docs/adr/0009-alarms-in-a-cluster.md)); a clean
  shutdown hands a node's actors over at once.
- Who may create an actor, per class
  ([ADR 0010](docs/adr/0010-who-may-create-an-actor.md)); access rules and
  configuration carried by `wasmkeep deploy`
  ([ADR 0013](docs/adr/0013-access-rules-and-config-in-deploys.md));
  outbound HTTP limited to public addresses.
- Garbage collection of old history in the bucket
  ([ADR 0012](docs/adr/0012-bucket-garbage-collection.md)).
- `wasmkeep cluster status`, `fence` and `forget`; Prometheus metrics,
  health and readiness endpoints, a
  [Grafana dashboard](deploy/grafana/wasmkeep.json), and what actors print
  in the node's log ([monitoring](docs/monitoring.md)).
- A distroless image, a three-node Compose example
  ([`deploy/compose/`](deploy/compose/README.md)), binaries and image
  built and signed by CI, an SBOM, and an opt-in anonymous usage ping, off
  by default ([what is sent](docs/telemetry.md)).
- A documentation site with a generated API reference and deployment
  guides; an internal security review in ten parts
  ([security](docs/security.md)); the [legal basics](docs/legal.md); CI on
  every pull request, with a benchmark regression check.
- Watch, an uptime monitor built on WasmKeep
  ([what it taught me](docs/dogfood.md)); and in `0.2.0` a comparison
  with celld on one machine
  ([WasmKeep and celld](docs/compared-with-celld.md)).

### M4: Operating a cluster

Started alongside M3; what is left of it is under [Now](#now) and
[Next](#next).

- Draining a node and moving one actor: `wasmkeep cluster drain` and
  `wasmkeep actor move`
  ([ADR 0014](docs/adr/0014-draining-a-node-and-moving-an-actor.md),
  [ADR 0017](docs/adr/0017-moving-one-actor.md)).
- Secrets sealed in the bucket, changed without a deploy or a restart
  ([ADR 0016](docs/adr/0016-secrets-in-the-bucket.md)).
- Key-value and queues as SDK patterns on actors
  ([guide](docs/kv-and-queues.md),
  [ADR 0015](docs/adr/0015-kv-and-queues-as-sdk-patterns.md)), in the
  Rust, TypeScript, Python, Go, C and Zig SDKs.
- Durable workflows as an SDK pattern ([guide](docs/workflows.md),
  [ADR 0019](docs/adr/0019-workflows-as-an-sdk-pattern.md)): runners in
  Rust and TypeScript, clients in the Go, Python, C and Zig SDKs.
- In `0.3.0`, reliable enough to leave running: a request for a dead
  node's actor waits for the takeover instead of being refused; a
  snapshot upload and a restore are bounded, tried again and counted; a
  request body the handler did not read no longer costs the answer;
  WebSockets close when their node loses the actor
  ([what a caller sees](docs/failover-and-scaling.md)).
- In `0.3.0`: guides to [back up and restore](docs/deploy/backup-and-restore.md)
  a cluster's bucket, alert rules as a
  [file](deploy/prometheus/alerts.yml), and the app model
  ([ADR 0021](docs/adr/0021-the-app-model.md),
  [ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
- In `0.4.0`, an app is one thing: a front for each app, deployed,
  versioned and rolled back with its classes
  ([ADR 0023](docs/adr/0023-the-front.md)); each app owns its actors'
  state, two apps never share an actor, with a one-time migration
  ([ADR 0022](docs/adr/0022-app-owned-state-and-the-one-time-migration.md),
  [upgrading](docs/deploy/upgrading-to-0.4.md)); a project file,
  `wasmkeep.toml`, with classes `internal` unless it says otherwise
  ([ADR 0026](docs/adr/0026-the-project-file.md)).
- In `0.4.0`: load per node and per app in the metrics and in `wasmkeep
  cluster status`; WebSocket rooms that wake where their sockets are
  ([ADR 0024](docs/adr/0024-socket-home.md)); calling several actors at
  once, in Rust and TypeScript first
  ([ADR 0025](docs/adr/0025-calling-several-actors-at-once.md)); and
  `wasmkeep cluster diagnose`, which checks a bucket
  ([buckets](docs/deploy/buckets.md)).
- In `0.4.1`, sockets through the front: the front hands a client's
  WebSocket to an actor of the app, so a room class can be `internal`
  ([ADR 0027](docs/adr/0027-sockets-through-the-front.md)); several
  outbound HTTP requests at once in the Rust, TypeScript and Go SDKs
  ([languages](docs/languages.md#several-outbound-requests-at-once)).
- In `0.4.2`, actors that end
  ([ADR 0028](docs/adr/0028-actors-that-end.md)): an operator deletes an
  actor (`wasmkeep actor delete`) or an app (`wasmkeep app delete`), a
  class expires actors left idle (`expire-after-idle`), and
  `storage.delete-self-durable` returns once the delete is in the bucket.
- In `0.4.2`, a database kept by one actor, as an SDK pattern
  ([guide](docs/database.md),
  [ADR 0029](docs/adr/0029-a-database-client-as-an-sdk-pattern.md)): the
  handler, the client and migrations in Rust and TypeScript.
- In `0.4.2`, starting a project: `wasmkeep new` writes a Rust or
  TypeScript app with its `wasmkeep.toml`
  ([ADR 0026](docs/adr/0026-the-project-file.md),
  [starting a project](docs/tutorials/first-actor.md#starting-a-project-with-wasmkeep-new)),
  a toolchain image builds every Recommended and Supported SDK's
  components ([the toolchain image](docs/deploy/toolchain.md)), and the
  release carries the counter prebuilt in each of those languages.
- In `0.4.2`, an idle node lists the bucket about 13 times less
  ([what an idle cluster asks of the bucket](docs/cluster.md#what-an-idle-cluster-asks-of-the-bucket)).

- In `0.5.0`, batched plain writes, opt-in per class
  ([ADR 0034](docs/adr/0034-batched-plain-writes.md)), with
  `storage.flush`, manifest format 5 and strict-versus-batched soaks
  ([measured](docs/benchmarks.md#two-hours-strict-and-batched)).
- In `0.5.0`, data access
  ([ADR 0033](docs/adr/0033-data-access-export-and-online-backup.md),
  [guide](docs/actor-data-access.md)): actor export and read-only SQL,
  online backup, backup prune, whole-app and single-actor restore, and a
  backup and restore drill.
- In `0.5.0`, a big-room baseline
  ([measured](docs/benchmarks.md#one-big-room)), configurable socket
  caps and a file-descriptor budget that counts sockets
  ([ADR 0037](docs/adr/0037-socket-session-admission.md)).
- In `0.5.0`, declarative per-node rate limits in `wasmkeep.toml`
  (manifest format 6,
  [ADR 0036](docs/adr/0036-per-node-ingress-rate-limits.md)).
- In `0.5.0`, a recorded head on hand-over, off by default and one way
  per app ([ADR 0035](docs/adr/0035-a-recorded-head-on-hand-over.md),
  [upgrading](docs/deploy/upgrading-to-0.5.md)).
- In `0.5.0`, typed TypeScript actor clients and structured errors
  ([guide](docs/typed-clients.md)).
- In `0.6.0`, several apps per process behind one node's three ports
  ([ADR 0038](docs/adr/0038-several-apps-per-process.md),
  [the node file](docs/reference/node-file.md)), for one owner's apps
  and trusted tenants.
- In `0.6.0`, files and static assets
  ([ADR 0030](docs/adr/0030-files-and-static-assets.md),
  [files](docs/files.md), [static assets](docs/static-assets.md)).
- In `0.6.0`, the change feed to Parquet
  ([ADR 0039](docs/adr/0039-the-change-feed.md),
  [guide](docs/change-feed.md)).
- In `0.6.0`, a room past one actor: a directory, a broadcast group,
  edges and relays
  ([ADR 0040](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md),
  [measured](docs/benchmarks.md#a-big-room-on-three-nodes-preview)). A
  preview then; no longer one since `0.7.0`
  ([measured](docs/benchmarks.md#a-big-room-on-three-nodes-070)).
- In `0.6.0`, placement at wake-up, on by default
  ([ADR 0041](docs/adr/0041-placement-at-wake-up.md),
  [measured](docs/benchmarks.md#a-node-coming-back-with-and-without-placement)).
- In `0.6.0`, a read-only [inspector](docs/inspector.md), typed Rust
  clients ([guide](docs/typed-clients.md)) and a React quickstart.
- In `0.7.0`, a room's delivery in a pusher of its own, so publishes and
  history no longer wait on delivery rounds
  ([ADR 0040, section 13](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20),
  [upgrading](docs/deploy/upgrading-to-0.7.md)).
- In `0.7.0`, heartbeat reads that keep trusting healthy peers on a slow
  bucket (ADR 0020 and ADR 0011, amendments of 2026-11-20).
- In `0.7.0`, parallel change-feed listing ([guide](docs/change-feed.md)),
  restores that wait on the bucket's clock instead of fixed periods
  ([backup and restore](docs/deploy/backup-and-restore.md)), and
  allocator trimming once activity drops
  ([socket limits](docs/reference/socket-limits.md#room-memory-after-clients-leave)).
- In `0.7.0`, metrics of mailbox wait, event and front work, alarm
  deliveries, segment uploads and bucket requests
  ([monitoring](docs/monitoring.md)), the idle node's GETs modelled
  ([measured](docs/benchmarks.md#bucket-requests-at-rest)), and
  `wasmkeep actor where` ([cluster](docs/cluster.md)).
- In `0.7.0`, a big room measured on three nodes and a real bucket, the
  room and its pusher on different nodes: 4,500 clients at 10 messages/s
  with delivery p99 under 1 s, 6,000 the measured edge
  ([measured](bench/results/2026-11-22-43119bd-room-group-ovh-bhs-3vps-r07-room-apart.json)).
- In `0.8.0`, a big room that needs no hand placement: a room and its
  pusher wake on different nodes by default
  ([ADR 0041, amendment](docs/adr/0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)),
  and a room's edges wake spread over the nodes
  ([ADR 0040, section 14](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#14-edges-spread-over-the-nodes-amended-2026-11-22)).
  Measured on three nodes and a real bucket with default placement:
  4,500 clients at 10 messages/s with delivery p99 under 1 s, the layout
  kept through a rolling restart; 6,000 missed narrowly
  ([measured](docs/benchmarks.md#a-big-room-on-three-nodes-080)).
- In `0.8.0`, an idle actor's history compacted: a snapshot at the head
  when it hibernates, and the history below it collected a retention
  window later
  ([ADR 0042](docs/adr/0042-compacting-an-idle-actors-history.md)).
- In `0.8.0`, a quiet room's first publish after a full hibernation is
  pushed, not fetched by every client
  ([ADR 0040, section 13](docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#13-the-pusher-amended-2026-11-20)).
- In `0.8.0`, hardening from an internal audit of the guest boundary,
  the cluster's signatures, uploads and local files
  ([security](docs/security.md),
  [upgrading](docs/deploy/upgrading-to-0.8.md)).
- In `0.8.1`, 0.8 settled, with no format change: a `LIKE` or `GLOB`
  pattern that could stop a node is fixed, reads of other nodes' leases
  never trust an older copy, an idle pass stopped at its deadline
  continues, and the mixed-version check against the previous release is
  a release gate ([changelog](CHANGELOG.md),
  [upgrading](docs/deploy/upgrading-to-0.8.md)).

## Stretch goal: the benchmark showdown

I would love WasmKeep to be put head to head with the alternatives, measured
by someone other than me, in the style of independent performance
benchmarkers such as [Anton Putra](https://www.youtube.com/@AntonPutra): same
hardware, real load, latency percentiles, throughput, CPU and memory on a
dashboard.

Today `bench/compare` compares one workload, a counter, on WasmKeep
(Rust, Zig, C, Go on TinyGo, TypeScript and Python), `workerd` and celld,
as plain processes on one machine (`bench/run.sh`), and three nodes of
WasmKeep and celld with a node killed under writes (`bench/cluster.sh`);
the results are in [benchmarks](docs/benchmarks.md). The plan is a
comparison suite anyone can run unchanged:

- The same workloads implemented on each platform: a stateless "hello", a
  counter actor (write per request), a chat room (WebSocket fan-out), and
  wake-from-idle for many actors.
- Contenders where the comparison is fair: Cloudflare `workerd` (self-hosted),
  celld, Rivet, and Spin for the stateless case.
- Containers, a load generator, Prometheus and a Grafana dashboard; one command
  to run everything; raw results committed.
- My own numbers published only when anyone can reproduce them.

This is an aspiration and an invitation, not an endorsement by anyone named
here.

---

Want something on this list sooner, or something new on it? Open an issue. The
design decisions behind the roadmap are in [docs/adr](docs/adr).
