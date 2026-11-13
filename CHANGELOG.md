# Changelog

Changes to WasmKeep, newest first. Before 1.0, any minor version (0.x) may
change the `wasmkeep:actor` WIT world, the formats in the bucket or the
command line; its notes say what changed and how to move.

The text under a version's heading is its GitHub release text
(`.github/workflows/release.yml` copies it).

## Unreleased

Since 0.2.0, in no release yet.

### App-owned state (for 0.4.0)

These change the layout in the bucket and on disk; they supersede the
second-app refusal and the migration guard listed under "What changed"
below. See "Upgrading to 0.4.0" before starting a node of this version
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
  app, or to `default/default` for the tenant `default` with no app; a
  class two apps deploy, or a tenant with several apps, needs `--heir
  TENANT/APP` (the other apps start that class empty). `--apply` copies
  server-side, verifies, then puts barriers in front of older binaries
  (token `3` in every app's `current`, a tombstone over every old
  ownership record), proves that no node ran, and commits each tenant
  (`cluster/migrated/<tenant>`). It refuses while a node may still run
  and fences nothing; after a crash, run it again. `--rollback` undoes it
  while no node of any version has started since; after that, it is
  one-way. Nothing of the old layout is deleted yet (the next release
  adds `--remove-old`). Exit codes: 0 done, 1 refused or aborted, 2
  partly committed ([backup and restore](docs/deploy/backup-and-restore.md#migrating-to-040)).
- **A node or command of this version checks the bucket before it writes
  there**: it refuses while `cluster/migration` exists, and refuses a
  tenant whose state is still in the layout before 0.4.0, printing the
  command to run. `cluster status` reads on.
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
  older `actor move` (`control/move/...`) is not read.

### Upgrading to 0.4.0

From 0.2.0 or 0.3.0, on a bucket prefix that holds actor state:

1. Freeze deploys and secret changes (CI included).
2. Stop every node: the service units, not just the processes.
3. `wasmkeep cluster migrate --bucket s3://BUCKET/PREFIX`: read the plan;
   give `--heir` where it asks.
4. Run it again with `--apply` (and the same `--heir`).
5. Upgrade every node, start them all, and check the app.

**One-way after the first start.** Until a node of any version starts,
`cluster migrate --rollback` puts the old layout back for the older
binaries. Once one has started, the migration is one-way; the old keys
stay until `--remove-old` (next release). Keep a backup from before the
migration until then. Lifecycle rules, access policies and backup
scripts written for `actors/<tenant>/` must move to `state/<t>/<a>/`.
Nodes that followed an app with `--access` and no deployed rules must
deploy those rules (`deploy --access`) and start without `--access`.

### What changed

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
- **A bucket a later version migrated is refused.** Every command that
  opens a bucket first checks for the markers a later version writes while
  it moves actor state to a new layout (`cluster/migration`,
  `cluster/migrated/`), and stops with a message naming the marker. One
  read and one listing per command, bounded to 10 s.
- **A request waits for a takeover.** When an actor's node dies, a GET or
  HEAD without a body, or a WebSocket upgrade, for one of its actors is
  kept by the node that received it until the actor can be taken over (15
  to about 21 s after the dead node's last write, unchanged), then served,
  instead of being answered 503 meanwhile. A request with a body, an
  actor-to-actor call and a forwarded request still get the 503 at once,
  which still means it did not run. A client that leaves while its
  request is kept is dropped and its request never runs. At most a
  quarter of the node's connection limit, and 64 per actor, are kept;
  beyond that, the 503 at once. `--failover-wait-secs` sets the wait
  (about 25 s by default, from the lease timing); 0 turns it off. Set the
  load balancer's server timeout above it.
  `wasmkeep_failover_waits_total` counts how each wait ended
  ([what a caller sees](docs/cluster.md#when-a-node-dies-what-a-caller-sees),
  [front door](docs/deploy/front-door.md)).
- **A snapshot upload is bounded, tried again after a failure, and
  counted.** The snapshot an actor's owner adds every 64 writes holds the
  actor while it uploads. It now gets one attempt, given up after 5 s plus
  a second per megabyte and 10 s at most; the upload had no bound of its
  own, so a bucket that left it unanswered held the actor until the bucket
  client ended the request (30 s by default). One that fails costs no
  write and fails no event, and is tried again
  once the actor has made one more write and 30 s have passed, then 8
  writes later, then every 64; a failure used to put the next snapshot 64
  more writes away. The node keeps these waits while an idle actor is out
  of its memory. `wasmkeep_snapshot_upload_failures_total` counts the
  snapshot uploads that were not acknowledged
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
  TLS connection) is sent once more, the client waiting meanwhile; it
  used to get a 503 asking the client to retry. Between nodes, the only
  other thing sent a second time is an `actors.call` the peer provably
  never read in the same way, once, as 0.2.0 already did after a refused
  handshake. A forwarded request is never sent again, whether its
  connection could not be opened or it was written into a connection
  that then failed: one that was written may have run on the owner, so
  the client gets a 503, which does not say whether it ran
  ([running a cluster](docs/cluster.md)).
- **A restore's reads have one deadline, a refused read is sent again,
  and failed restores are counted.** Restoring an actor from the bucket
  bounded each read but not the reads together, so reads that each stayed
  under their limit added up with the length of the history while every
  caller of the actor waited. The reads of one restore now end within
  30 s, plus a second per megabyte of what it reads and a second per
  eight objects; the clock runs only while the restore waits for the
  bucket to give it the object it applies next, so the listing, work on
  the local disk and the wait for a turn among the node's other restores
  are not counted. Neither are the seconds that other restores' reads on
  the node add to the limit of a read (a second per megabyte in flight,
  256 at most), so restores that read side by side after a takeover are
  not ended for each other's bytes; a restore alone on the node has no
  more than its own time. Running out of time is answered like a bucket that
  does not answer, a 503 with `Retry-After`, and leaves the local file as
  it was. A read the bucket refuses with an error a resend can get past
  (a 5xx or a broken connection, after the bucket client's own retries)
  used to fail the whole restore; it is sent again after 0.5 s, then 1 s,
  three tries per object. `wasmkeep_restore_failures_total` counts the
  restores that did not complete
  ([replication](docs/cluster.md#replication),
  [monitoring](docs/monitoring.md)).
- **A client that only listens no longer misses a room's messages
  without a close.** A room that hibernates keeps its WebSockets open on
  its node and releases its ownership record. When a request then woke it
  on another node, its sockets on the first node closed only one by one,
  each when its client next sent a message: a client that only listens
  stayed connected and silently missed everything the room said from then
  on. Now every socket of the room on that node closes with 1012 as soon
  as one of them sends, and an owner sweep, once a minute, closes the
  sockets of rooms another node took even if none of them sends. A
  node's sockets also close with 1012 when its lease runs out (8 s
  without a write reaching the bucket, with the defaults), before any
  other node can take its actors; nothing closed them before. A drain
  closes the sockets of hibernated rooms, and reports `drained` only once
  the node holds no socket. Each such close is counted in
  `wasmkeep_websocket_owner_closes_total`
  ([running a cluster](docs/cluster.md), [monitoring](docs/monitoring.md)).
  The sweep reads at most 5,000 records a pass, counted in
  `wasmkeep_websocket_sweep_reads_total`.
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
  used signatures**, so a flood of other node calls that fills the record
  (500,000 signatures at once) can no longer refuse the calls a drain and
  a move depend on. Refusals because a record is full are counted in
  `wasmkeep_signatures_refused_full_total`, and logged as a full record
  rather than as a replay. The take and hand-off record holds 50,000.
- **`wasmkeep_admission_refused_total{reason}`** counts the requests,
  WebSocket upgrades and actor calls a node refused before their event
  ran: `access`, `capacity`, `overloaded` or `unavailable`
  ([monitoring](docs/monitoring.md#metrics)).
- **`deploy` refuses a second app of a tenant on a bucket prefix.**
  Actors are keyed by tenant, class and name, so a second app would share
  the first app's actors without a word: until now only a node refused to start, and
  only while a node of the other app was live. The first deploy of an app
  now lists `apps/<tenant>/` and refuses when another app there has a
  `current`, naming it and saying what to do: another prefix, or another
  tenant when another owner runs the app, or, for an abandoned app, the
  keys to delete. Two apps of one
  tenant already deployed on one prefix are left as they were (a node of
  one still refuses to start while a node of the other is live), and `deploy` and
  `serve --app` warn about them by name; a third is refused. This lasts
  until apps own their state (planned for 0.4.0,
  [deploying](docs/cluster.md#deploying)).
- **ADR 0021, the app model**: an app is `tenant/app`, the unit of deploy,
  rollback, secrets and (from 0.4.0) state; the tenant is the owner of
  apps; what 0.4.0 plans (a front deployed with the app, app-owned state
  with a one-time migration, classes `internal` by default with a project
  file) and later (shared nodes for trusted tenants)
  ([ADR 0021](docs/adr/0021-the-app-model.md)).
- **A new app's tenant and app names follow the class grammar**,
  `[a-z][a-z0-9-]{0,63}`: the first `wasmkeep deploy` of an app refuses
  others, and writes nothing. Actor addresses already required it of the
  tenant; the app name had no rule. An app deployed before keeps its name, and
  every later deploy and rollback of it works as before.
- **Releases**: the npm job of the release workflow passed the tarball as
  a bare path, which npm read as a GitHub repository; it now publishes the
  file. See the correction under 0.2.0.

### Upgrading from 0.2.0

- Stop every node of a cluster, upgrade them all, start them all
  ([upgrading](docs/cluster.md#upgrading-wasmkeep)): nodes of this
  version refuse every hop from a node of an earlier one (a forwarded
  request, a relayed upgrade or a call, which carries no app) with a 503
  saying it came from a node of an older version. The other way, a node
  of an earlier version fails the signature of this version's calls:
  calls to `internal` classes, moves and alarm hand-offs fail, and other
  calls run there as requests from outside, with no calling actor.
- A node of this version started while a 0.2 node given classes on the
  command line is still live on the bucket prefix (its heartbeat names
  `default/`) refuses to start, and asks to stop every node and upgrade
  them together; so does a 0.2 node started next to a live node of this
  version, though its message speaks of another app of the tenant.
- `wasmkeep actor move TENANT/CLASS/NAME` becomes `wasmkeep actor move
  --app TENANT/APP CLASS/NAME` for an actor of a deployed app; scripts
  that move actors of `default/default` keep working.

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
