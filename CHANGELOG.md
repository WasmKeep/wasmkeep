# Changelog

Changes to WasmKeep, newest first. Before 1.0, any minor version (0.x) may
change the `wasmkeep:actor` WIT world, the formats in the bucket or the
command line; its notes say what changed and how to move.

The text under a version's heading is its GitHub release text
(`.github/workflows/release.yml` copies it).

## Unreleased

Since 0.3.0, in no release yet.

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
  unchanged, their default of `open` included.
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
  `0.3.0+<commit>` (`wasmkeep --version`, its heartbeat, so the VERSION
  column of `cluster status`, and `wasmkeep_info{version}`); a build of a
  release's tag, or one without git (a source tarball, a container image
  built locally), reports the release alone. Before, every build reported
  `0.3.0`. The usage report still sends the release version alone.
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
- **A drain or a move no longer waits until the node stops for an actor
  whose ownership write went unanswered and landed late**: the node now
  watches such a record for 30 s and releases it if the write lands
  ([ADR 0020](docs/adr/0020-one-lease-per-node.md#3-the-ownership-record),
  amended 2026-11-14).

### The front, host side (for 0.4.0)

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

### The front in deploys (for 0.4.0)

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

### App-owned state (for 0.4.0)

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
