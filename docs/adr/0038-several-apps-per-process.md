# 0038. Several apps per process

- Status: proposed (2026-11-17); revised the same day after a design
  review (the sender inside the signed envelope and a pin shared by
  several node ids, the forward's envelope, node hops and the public
  port, restarting one app in place, residents across apps, per-app
  settings, a share of every shared budget). A design, not an available
  capability: until it is accepted and built, a node process serves one
  app.
- Date: 2026-11-17
- Milestone: [Next](../../ROADMAP.md#next), `0.6.0`, first item
- Follows: [ADR 0021](0021-the-app-model.md) (the app is the unit; shared
  nodes are for one owner's apps and trusted tenants),
  [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (each
  app's state under its own root; `nodes/` shared by the prefix),
  [ADR 0011](0011-node-to-node-mtls.md) (the cluster port and its pinned
  certificates), [ADR 0020](0020-one-lease-per-node.md) (one lease per
  node), [ADR 0024](0024-socket-home.md) (socket home and relays),
  [ADR 0028](0028-actors-that-end.md) (deleting an app),
  [ADR 0035](0035-a-recorded-head-on-hand-over.md) (the recorded head),
  [ADR 0036](0036-per-node-ingress-rate-limits.md) (rate limits) and
  [ADR 0037](0037-socket-session-admission.md) (socket and descriptor
  budgets).
- Amends, once accepted: ADR 0020 (several leases per process, one per
  app), ADR 0011 (one certificate shared by several node ids, the sender
  in the envelope), ADR 0036 (its signed client identity becomes the
  mandatory envelope of every forward) and ADR 0037 (how its budgets
  divide between apps).

## Context

`wasmkeep serve --app TENANT/APP` runs one app. Everything the process
owns belongs to that app: the public listener, the cluster listener, the
metrics listener, the cluster's TLS identity, the node id and its lease
`nodes/<id>`, the deploy follower, the class registry, the instance pool
and the descriptor budget. To put two apps on one machine, an operator
runs two processes, each with its own three ports. A network that opens
exactly one public, one cluster and one metrics port between machines
cannot do that.

ADR 0022 already made state app-owned: an app's ownership records, lease
markers, segments and local databases live under its own root
(`state/<tenant>/<app>/`, on the bucket and in the data directory), so two
apps of one tenant may use the same class and actor names without
sharing anything. The simulation `sim/tests/madsim_two_apps.rs` checks
this with two apps whose nodes share a prefix. What stops a single
process from serving both is the process itself, not the data.

Today's code ties the process to one app in several places this record
has to undo:

- `ActorService::new` (`actor/start.rs`) builds one state root, one
  `Cluster`, one metrics identity and one socket registry, and sizes the
  resident limit from the whole instance pool. A second service built
  the same way would claim the whole pool a second time, and its
  mailboxes would evict only its own idle actors.
- `ActorAddress` carries a tenant, a class and a name, and no app. The
  start check refuses an actor of another tenant, not of another app.
  Separation between apps of one tenant rests on each service having
  its own root.
- The log label registry (`logging::register`) maps a tenant to one
  `tenant/app` string: a second app of the same tenant would overwrite
  the first's label.
- Metrics are one process-wide accumulator (`metrics::METRICS`), named
  by one identity when rendered. The heartbeat's load line
  (`actor/load.rs`) reads the process's event totals. Rendering the
  same accumulator once per app would repeat the process's totals under
  every app's name.
- Several budgets are process statics with no notion of an app: the
  pending-upload budget (`actor/uploads.rs`), the restore read slots and
  bytes (`replica.rs`), the public connection cap (`http.rs`) and the
  front's instance reservations.
- The cluster's TLS trust (`cluster_tls.rs`) keys a cluster address by
  the newest heartbeat naming it, and its pin set maps a pin to one node
  id (the last one listed). A peer is judged live by its pin alone. If
  several node ids published one pin, a live app's heartbeat would keep
  the pin trusted after another app's lease lapsed.
- A node call's signature binds the destination, not the sender: the
  receiver cannot tell which of a process's node ids sent it.
- Node-to-node hops carry the app in `x-wasmkeep-app`, and `wrong_app`
  answers 503 to a hop naming another app. Signed node calls and front
  hand-offs put the app inside the HMAC. A plain forward and a WebSocket
  relay carry it as a plain header; the forwarder adds ADR 0036's signed
  client identity (`x-wasmkeep-admitted`), which binds the app, only
  when its cluster secret is already loaded, and the owner requires it
  only when it has rate-limit policies. The cluster secret is shared by
  the prefix, so with several apps behind one port an unsigned app
  header is a choice of app made by the sender alone.
- With `--cluster-plaintext accept`, a node reaches a peer without a
  cluster listener on its public address, and signed calls carry the
  peer's address as `Host`.
- `http.rs` routes by path only: there is no Host table.
- An app's tombstone (ADR 0028), or a verdict on its node id (twin,
  fenced, released), ends the whole process (`serve` in
  `crates/wasmkeep-cli/src/main.rs`), with a non-zero exit that a service
  manager restarts. The twin check and the local heads compare a node id
  with the process token it was started with.
- The one-time move of the layout before 0.4.0 (`local_layout`) moves a
  tenant's databases, `actors/<tenant>/`, into the app the process
  serves.

## Decision

One process serves several apps. A supervisor owns the process's three
ports and routes each request to an app runtime by app. Each app keeps
everything that decides ownership and state; the process shares
everything that is only capacity or transport, and divides every shared
budget between apps.

### 1. Three ports, one supervisor

A node process has exactly three listeners, whatever the number of apps:

- **public**: client HTTP and WebSockets, routed by hostname (section 6);
- **cluster**: node-to-node calls over mutual TLS (ADR 0011), routed by
  the signed app (section 5);
- **metrics**: one page for the process (section 9).

The supervisor binds them, owns shutdown and builds one app runtime per
configured app. Workers in other processes behind Unix sockets are out of
scope (section 11): every app runs inside the supervisor's process.

### 2. What is per app, what is per process

Per app (an **app runtime**):

- the actor service, its class registry and its front;
- its settings from the node file (section 4): secrets keys, class and
  front configuration, access rules before a deploy, egress, alarms,
  front limits;
- its deploy follower, which reads only that app's versions;
- its node lease, its **logical node id** and a process token drawn
  afresh at each start of the runtime (section 3);
- its ownership records, alarms, deletes, restores and recorded heads,
  all under its own root, as today;
- its data directory (by default the process's);
- its rate-limit buckets, its socket ceiling and its shares of the
  shared budgets (sections 7 and 8);
- its signature replay stores: control, actor calls and charged
  ingress proofs, so one app filling a store cannot refuse another's
  calls;
- its readiness.

Per process (the **supervisor**):

- the three listeners and routing;
- one cluster TLS identity and the **process id**, published as
  `process=<id>` on heartbeat line 11 of each of its node ids (readers
  ignore words they do not know);
- the listing of `nodes/` (one per period for the process, not one per
  app), the trust table built from it, and one pool of peer clients
  counted in the descriptor budget;
- one Wasmtime engine and one instance pool, shared by all apps;
- the budgets: descriptors (ADR 0037), rate-limit buckets (ADR 0036),
  pending uploads, restore reads, public connections and front
  reservations, each divided between apps (section 8);
- health (the process is up), and shutdown under one deadline: apps'
  holds are flushed and released concurrently, not one app after
  another.

### 3. One lease and one logical node id per app; restarting one app

Each app keeps its own node lease, `nodes/<id>`, with the logical node id
it has today. No actor data, record or database moves when an app joins
a shared process: an existing single-app node is restarted under the
supervisor with the same node id, data directory and roots, and its
actors' records still name it.

The node leases of one process are renewed independently. Each app
runtime draws a new token (`instance=<hex>`) every time it starts, so
the twin check and the local heads go on comparing a node id with the
token of the runtime that wrote it, exactly as they compare a node id
with a process's token today. Choice: a token per runtime start rather
than one per process, because a runtime that restarts in place must look
to the bucket like a process that restarted, and two processes started
with the same node id must still see two tokens.

**A verdict stops one runtime.** One app's lease lapsing, being fenced
or being released, or another process writing its node id, stops that
app runtime alone: its holds end as ADR 0020 says, its actors stop, and
the other apps keep serving. The supervisor then restarts that runtime in
place, through the same start check a new process runs (the identity
and bucket checks, a fresh token), after a backoff of 1 s doubling to
60 s. While a twin still writes the node id, the check refuses and the
runtime stays stopped. An app's tombstone (ADR 0028) stops the runtime
and removes its local files; it is not restarted, since the app no
longer exists; the node file must change.

While an app runtime is stopped, its hostnames answer 503, with
`Retry-After` while a restart is pending and without it after a
tombstone, and hops for its node id are refused with 503. Its readiness
gives the reason. The process exits only when the operator stops it.

Choice: restarting in place, rather than exiting the process and letting
a service manager restart it, because an exit would stop every healthy
app for one app's verdict. A single-app process keeps today's behaviour
until slice M5: it exits non-zero on a verdict.

A physical failure of the process ends every app it serves at once. To
the cluster this is several node ids lapsing together, each failed over
by its own app's nodes under ADR 0020's timings: nothing waits for the
other apps.

Each node id's heartbeat names its app, its cluster address, its
runtime's token and the process id. The node ids of one process share
the address, the certificate pin and the process id. Peers group node
ids by process only for transport (section 5) and for `status` (slice
M6); ownership is still decided per node id.

### 4. The operator file `wasmkeep-node.toml`

The node is described by an operator-owned file, separate from the app's
deployed `wasmkeep.toml` (ADR 0026): the project file says what an app
is; the node file says which apps this process serves and how.

```toml
[process]
id = "edge-1"                       # the process identity, [a-z0-9-]
data-dir = ".wasmkeep/data"

[listen]
public = "0.0.0.0:8080"
public-advertise = "http://10.0.0.11:8080"
cluster = "0.0.0.0:7443"
cluster-advertise = "https://10.0.0.11:7443"
metrics = "127.0.0.1:9100"

[limits]
max-sockets-per-node = 50000

[[app]]
name = "acme/shop"                  # tenant/app
node-id = "shop-1"                  # the logical node id, kept as today
hostnames = ["shop.example.com"]
secrets-key = ["keys/shop.key"]     # or no-secrets = true
max-sockets = 20000
min-resident = 200
max-resident = 800
max-front-requests = 32
front-cpu-budget-ms = 50
allow-outbound = ["127.0.0.1/32"]
allow-outbound-port = [587]
outbound-ca = ["ca/internal.pem"]
front-env = ["env/shop-front.env"]

[app.env]
cart = ["env/cart.env"]             # CLASS = files, later files win

[[app]]
name = "acme/blog"
node-id = "blog-1"
hostnames = ["blog.example.com", "www.blog.example.com"]
data-dir = "data/blog"              # optional: another data directory
no-alarms = true                    # this node fires none of its alarms
```

Every per-app setting `serve` takes today as a flag is an `[[app]]`
field with the same meaning: `secrets-key` (one or two files during a
key change) or `no-secrets`; `env` by class and `front-env`; `access` by
class, accepted only until access rules are deployed, as `--access` is;
the egress settings `allow-outbound`, `allow-outbound-port` and
`outbound-ca`; `no-alarms` (for a copy that must not fire the app's alarms); the front limits `max-front-requests` and
`front-cpu-budget-ms`; and the ceilings `max-sockets`, `min-resident` and
`max-resident`. Process settings
(listeners, the bucket, cluster flags, `--max-instances`, the node socket
ceiling) stay in `[process]`, `[listen]` and `[limits]` or on the command
line. The bucket and its credentials stay where they are today (flags
and the environment).

The file is read once, at start. Adding, removing or changing an app
means editing the file and restarting the process; there is no reload in
this record.

Start refuses, before binding a listener or writing a heartbeat: an
unknown field; a duplicate app, logical node id or hostname; an invalid
name or hostname; an app ceiling above the node ceiling; floors or shares
whose sum exceeds the process's budget; two apps whose local roots
overlap. It also refuses two apps of one tenant while a data directory
either uses still holds the layout from before 0.4.0
(`actors/<tenant>/`): the one-time move would hand that tenant's
databases to whichever app ran first. The operator runs it with one app
first. Errors name the file, line and field.

`data-dir` is per app and defaults to the process's. An existing node id
moved into a shared process keeps its own data directory by naming it
here: no file moves.

`serve --app TENANT/APP` and its flags stay, and are translated into a
one-entry node file with the same meaning: the existing node id,
listeners and flags, no hostname table, and `--max-sockets-per-app` as
the entry's `max-sockets`. A single-app node behaves as today without
writing a node file.

### 5. Node to node: the sender and the app inside the signed envelope

Every node-to-node hop carries, inside its signature, the app, the
**sender's logical node id** and the destination's. Signed node calls
(actor calls, take calls, alarm hand-offs, the hello) and front hand-offs
gain the sender; plain forwards and WebSocket upgrade relays gain a
mandatory envelope.

**The forward's envelope** is ADR 0036's signed client identity,
`x-wasmkeep-admitted`, extended with the sender's node id, and required
on every forward and relay, whether or not the owner has rate-limit
policies. Choice: that proof, not the node-call signature
`x-wasmkeep-internal`, because a node call is trusted as coming from
inside the app (caller, chain, actor privileges), while a forward is a
client's request and must stay one. A process with several apps loads
the cluster secret before it is ready; a forwarder whose secret is not
loaded answers 503 instead of forwarding unsigned. The receiver refuses
a forward or relay without a valid proof with 503: it fails closed.

The receiver:

- reads `x-wasmkeep-app` only to choose which app runtime verifies the
  hop; until the signature checks, it is untrusted;
- refuses a hop with no app, with the app twice, or whose signed app
  differs from the header (503, with a log line naming the cause);
- never infers an app from an actor path;
- refuses a hop whose signed destination is not this runtime's node id:
  a hop signed for `blog-1` is refused by `shop-1`, even in the same
  process;
- checks that the signed sender is a live node id of the same app and,
  over TLS, that the sender's own heartbeat publishes the pin the
  connection presented;
- checks replays in its own runtime's stores (section 2).

**One pin, several node ids.** The process's certificate pin is published
by each of its node ids. The trust table maps a pin to the **set** of node
ids publishing it, each with its own expiry; a connection's pin is
trusted while any of them is live, but a hop is accepted only for its
signed sender, whose own heartbeat must be live and name that pin. The
same holds for outbound connections: a response or a relayed socket is
trusted only while the destination node id itself is live. A healthy
app's heartbeat therefore never keeps a lapsed app's hops, responses or
relays authorized: when one node id lapses, relays to or from it are
closed while the other apps' relays to the same process stay up.

**Node hops use the cluster port only.** A process with several apps
needs a cluster listener: start refuses a second app without one, and
refuses `--cluster-plaintext accept`. Peers are never reached on their
public address. The public port strips every `x-wasmkeep-*` request
header before routing and treats every request as a client's. Choice:
this, rather than letting a verified signed app route ahead of `Host`
on the public port, because it keeps one routing rule per port and
leaves no forged forwarded mark to judge; `accept` exists to move a
cluster to TLS, which must be finished before a second app is added.
A single-app process keeps today's behaviour in both modes.

### 6. Public routing by an explicit hostname table

With several apps, the public port routes by hostname only:

- The authority is taken once, before admission. On HTTP/1.1, exactly
  one `Host` header is required: missing or repeated, 400. A request
  target in absolute form must name the same authority as `Host`, or it
  is refused with 400 (stricter than HTTP's rule of ignoring `Host`
  then, so that no two parts of the stack can disagree). On HTTP/1.0
  without `Host`, 421. On HTTP/2, `:authority`, and a `Host` beside it
  must match it, or 400.
- The authority is normalized (lowercase, the port and a trailing dot
  removed) and looked up in the node file's table, which maps each
  hostname to one app. An unknown host answers **421 Misdirected
  Request**; it never falls into a default app.
- A connection belongs to the app of its first request, and counts in
  that app's connection share (section 8); a later request on it naming
  another app's host answers 421. Connections that have not sent a
  first request are bounded by the header read timeout.
- `Forwarded` and `X-Forwarded-Host` are ignored for routing. A trusted
  proxy setting may come later (open questions).
- The app is attached before admission (rate limits, sockets, body
  reads), so every later step runs under that app's rules.
- Path routing inside an app is unchanged: `/actor/<class>/<name>`, the
  front, `/_wasmkeep/` (ADR 0023). Reserved internal routes stay
  protected per app.
- `/_wasmkeep/health` is the process's and needs no host; an app's
  readiness is asked with that app's host.
- A stopped app's hostnames answer 503 (section 3).

With one app and no hostname table, no Host is needed and nothing
changes: every request goes to that app, as today.

### 7. Rate limits (ADR 0036)

Buckets stay per app, rule and client, as ADR 0036 already defines them.
The 65,536-bucket process budget is shared between the app services
that have policies, at most 4,096 per service, as ADR 0036 already
describes for several services in one process: no new rule is needed.
The signed client identity binds the app and now the sender, so a
charged proof for one app cannot be spent at another. Its replay store
is the receiving runtime's (section 2); refusals are counted per app.

### 8. Shared budgets: sockets, residents and the rest

**Sockets and descriptors (ADR 0037).** The descriptor budget is computed
once per process, as ADR 0037 does today, and includes the supervisor's
peer clients. The node socket ceiling is the process's. Each app's socket
ceiling comes from the node file (`max-sockets`), and defaults to the
node ceiling: by default the apps share the node's sessions first come,
first served, which is ADR 0037's single-app behavior. An app ceiling
never exceeds the node ceiling, and a session is admitted only when the
actor's, the app's and the process's allowances all have room. A relay
counts against the ingress app's ceiling and the process's, then against
the owner's app and process, as ADR 0037 counts it today. When more than
one app is configured and none sets `max-sockets`, the process logs a
warning at start: one app can then take every socket session. Startup
logs and metrics report each app's requested and effective ceiling.

**Residents.** The resident limit is computed once for the process from
the instance pool, as `fds::resident_limit` does today, and all apps'
residents count against it. Each app has a floor, `min-resident`
(default: the limit divided by twice the number of apps, so half the
pool is reserved evenly and half is shared), and an optional cap,
`max-resident`. When the limit is full, a wake-up evicts the least
recently active **idle** resident of any app that is above its floor; an
app below its floor may always evict an idle resident of an app above
its own. A wake-up waits, as today, only when no such idle resident
exists. Choice: eviction across apps with floors, rather than fixed
per-app pools, because fixed pools leave capacity idle in one app while
another hibernates actors it needs; the floor keeps one busy app from
holding every slot.

**Every other shared budget** (pending uploads, restore read slots and
bytes, public connections, front instance reservations) is divided the
same way. Each app has an equal part of the budget, and half of it is a
floor never lent. An app may use any capacity outside the other apps'
unused floors, but takes nothing above its equal part while an app below
its own waits. Shares are not settable in 0.6.0. Units held briefly (uploads, restore reads, front
reservations) return within their own deadlines; connections are held
long, so a connection beyond the share is refused rather than borrowed
when other apps' reservations need the room. Disk has no budget in
WasmKeep; a per-app `data-dir` lets an operator put an app on its own
filesystem or quota.

### 9. Metrics per app

Each event is recorded with its app when it happens, not when the page
is rendered. Metrics of app work (events, front requests, sockets,
replication, rate limits, refusals, budget shares) carry `tenant` and
`app` labels with the values from the node file; per-class labels stay
bounded by the deployed classes. Process metrics (descriptors, instance
pool, listener connections, uptime) are reported once, without an app
label. The sum over apps of an app metric equals the work done, never a
multiple of the number of apps.

The heartbeat's load line (`actor/load.rs`) becomes per app: each node
id publishes its own app's residents, sockets and rates. The process's
totals appear only in `status`.

Log lines name the actor's app from its runtime, so two apps of one
tenant keep their own labels.

### 10. The `multi-app` capability word

Every process of this version announces `multi-app` on heartbeat line 11
of each of its node ids, whatever the number of apps it serves. A process
whose node file lists a second app refuses to start while a live node
of any listed app lacks the word, naming that node.

The check also runs at every listing of `nodes/`. When a live node of a
listed app lacks the word (an older binary started, or came back from
being offline), every app runtime of a process serving several apps
reports not ready, with the reason naming that node, logs it and counts
it in a metric. The runtimes keep serving the actors they hold. Choice:
readiness rather than stopping, because stopping would turn an
operator's mistake into an outage, while readiness takes the process out
of a balancer and makes the problem visible.

The word cannot make an older binary refuse: it reads a shared
certificate and several node ids at one address as it always has, and
cannot verify the sender in the envelope. Before adding a second app
anywhere, upgrade every node of every app listed, including offline
ones.

### 11. Out of scope

- **Cluster-wide app suspension.** There is no control record that
  stops an app on every node. An app leaves a node by removing it from
  the node file and restarting the process; it leaves the cluster by
  doing so on every node, or by `wasmkeep app delete`.
- **Worker processes.** Apps do not run in separate processes behind
  Unix sockets or in microVMs. All apps share one process, one engine and
  one address space.
- **Reloading the node file** while running.
- **Isolation from a hostile tenant.** Apps sharing a process share its
  crashes, its engine and every allocation not bounded above. Per-app
  ceilings and shares protect against a noisy neighbour, as ADR 0021
  says; they are not isolation.

## Compatibility and a rolling upgrade

The rollout has two stages:

1. **Upgrade with one app per process.** Every node is upgraded to a
   binary that knows the supervisor, still serving one app with `serve
   --app` or a one-entry node file. Behavior, ports, routes, exit codes
   and metrics are unchanged (slice M2). The upgrade is rolling, as for
   any release: one node at a time.
2. **Enable a second app.** Once every node of the apps concerned
   announces `multi-app` and uses the cluster port, an operator adds a
   second `[[app]]` to one node's file and restarts it. That node id
   keeps its app; the second app's node id joins its own app as a new
   node, or as an existing node id moved from another process after
   draining it there (`wasmkeep cluster drain`, ADR 0014), with its data
   directory named in the entry.

Rolling back: drain the shared process's other apps, restart with one
app, then downgrade. A binary older than this record must never run while
a shared process is live in its cluster.

Wire formats: the heartbeat already carries the cluster address and a
token; several node ids now share the address and the pin, and gain the
`process=` and `multi-app` words, which older readers ignore. The
sender's node id in the envelope and the mandatory forward proof are sent
by every node of this version from stage 1 on. A single-app receiver
still accepts the older forms during stage 1; a receiver serving several
apps requires the new ones, and the `multi-app` gate ensures that by then
every live node of its apps sends them.

No manifest format changes. No WIT change. Ownership records, leases and
replication are unchanged.

## Consequences

- One machine can host several trusted apps behind three ports, each
  failing over, deploying and restarting independently.
- Two apps with the same class and actor names stay separate, by their
  roots on the bucket and on disk, and now also in routing, signatures,
  metrics, logs and budgets.
- One process failure is several node failures at once. The failover of
  each is independent, so this costs bucket requests, not correctness.
- Each node id writes its own heartbeat: a process with N apps writes N
  heartbeats per period, but lists `nodes/` once. That cost is accepted
  (open questions).
- Plain forwards and relays get a proof they did not always have, and a
  per-hop HMAC; a forwarder without the cluster secret refuses instead of
  forwarding.
- A shared process cannot run `--cluster-plaintext accept`.

## Tests (to build)

**Exit criterion:** two apps with the same class names and the same actor
names, served by the same processes, stay separate: each app's state,
alarms, sockets, metrics and logs are its own, and no request of one app
reaches an actor of the other.

Unit and server tests:

- node file parsing, every per-app field mapped as its flag is, and every
  start refusal of section 4 before any listener binds or heartbeat is
  written; `serve --app` mapped to a one-entry file with the same
  configuration;
- the authority rules of section 6: missing and repeated `Host`, absolute
  form against `Host`, HTTP/2 `:authority` against `Host`, ignored
  forwarding headers, unknown host 421, a second host on one connection
  421, a stopped app's host 503;
- metrics: the sum over apps equals the process's work; process metrics
  appear once;
- the warning when several apps set no socket ceiling; one app at its
  socket ceiling leaves the other's sessions admissible; one app's
  borrowing never refuses another app within its share; every permit
  returned after cancellation;
- the log label of two apps of one tenant;
- the trust table: a pin published by two node ids, one lapsing;
- repeated build, stop and in-place restart of app runtimes leaks no
  task and draws a new token each time.

Cluster tests, a new `crates/wasmkeep-server/tests/cluster/two_apps.rs`:

- the exit criterion, with three processes each serving two apps;
- **different tenants** with the same class and actor names, in one
  process;
- one app's broken deploy leaves the other serving;
- one app's tombstone stops that app runtime only, while a sibling app
  **of the same tenant** keeps serving its actors of the same names;
- one app's node lease lapsing or being fenced: its actors fail over,
  the other app keeps its actors on the same process; then the
  **in-place restart** brings the lapsed app back under a new token;
- **one node id configured in two shared processes**: the second runtime
  is refused as a twin, and the other apps of both processes keep
  serving;
- a hop signed for the wrong app, for the wrong destination or with a
  sender of another app, or with a duplicate or missing app header,
  refused before it runs;
- an **inbound hop and an inbound relay from a lapsed app's process**,
  over the pin that process's healthy app still publishes, refused;
- a hop replayed from one app to the other, refused;
- a WebSocket relay closed when its app's node id lapses, while the other
  app's relay to the same process stays up;
- on the public port, a request with a **forged forwarded mark and app
  header**, in a process that was configured with `accept` before (now
  refused at start with several apps) and in one with a cluster
  listener: treated as a client's request;
- **app A's idle residents and app B's wake-up**: with the limit full of
  A's idle actors, B's wake-up evicts one; with A at its floor, B waits
  only if nothing else is idle;
- the `multi-app` start refusal while a node lacks the word, and **an
  older node joining after the second app is enabled**: readiness falls
  with the reason, actors keep being served;
- a rolling upgrade from single-app nodes, then enabling a second app.

Existing suites: the single-app server, deploy, deletion, drain,
shutdown, rate-limit and socket suites pass unchanged after slice M2, and
the strict-write golden trace stays unchanged throughout.

## Simulation and cluster cases

- Extend `sim/tests/madsim_two_apps.rs` so that both apps' node ids run in
  the same simulated process: a process kill ends both node ids at once;
  each app's records still name only its own node ids; neither app's
  generations move with the other's holds; each takes over from the dead
  process independently.
- A case where one app's lease lapses on a process whose other app keeps
  renewing: the lapsed app's records are freed under ADR 0020's rule, the
  healthy app's heartbeat never keeps the lapsed app's holds valid, and
  the lapsed runtime's in-place restart acquires only at a new
  generation.
- The recorded head (ADR 0035) across a shared process: an app's
  same-process re-acquire never carries another app's frozen head, and a
  runtime restarted in place carries none from its previous token.
- A cluster test where the process restarts with one app removed from its
  node file: that app's actors fail over, the other's resume locally.

## Docs to change when accepted

`docs/cluster.md` (a node, a process, a node file), `docs/concepts.md`,
`docs/security.md` (the trust model of a shared process),
`docs/monitoring.md` (app labels, budget shares, the readiness reason),
`docs/operations.md`, a node file reference page, an upgrade page, and the
ADRs this record amends.

## Open questions

- Does the `multi-app` check cover every live node on the prefix, or only
  the nodes of the apps the file lists? Membership and certificate trust
  span the prefix, which argues for every node.
- A trusted proxy setting for forwarding headers: which addresses, and
  whether it is per process or per app.
- Whether N heartbeats per period per process should later become one
  object listing N node ids, and what that would change in ADR 0020's
  fencing.
- Whether a verdict that concerns the binary rather than one node id (an
  older node in the cluster before node leases, an unreadable lease
  object) stops the app that saw it or every app of the process.
- Whether the default resident floor (half the pool split evenly) suits
  apps of very different sizes, or should be weighted.

## Plan

Each slice is one change; each says what it proves.

- **M1. This record and the node file.** Parser, validation, every
  per-app field and the `serve --app` translation, with no runtime
  change. A file with a second `[[app]]` is refused until M5. Proves the
  file's rules and that single-app configuration maps without changing
  meaning.
- **M2. One app through the supervisor.** Listeners, shutdown, the
  listing of `nodes/`, the trust table and peer clients move to the
  supervisor; exactly one app is allowed; a tombstone or a node id
  verdict still ends the process with today's non-zero exit. Proves no
  behaviour change: every existing single-app suite passes unchanged.
- **M3. Metrics and budgets by app.** Events recorded with their app,
  process metrics single, the per-app load line, replay stores per
  runtime, socket ceilings, resident floors and cross-app eviction, and
  the shares of every shared budget. Proves that totals equal the work
  done and that one saturated app leaves the other's share available.
- **M4. Shared transport identity and signed routing.** One certificate
  per process published by each node id, the pin-to-node-ids table, the
  sender, app and destination in every envelope, the mandatory forward
  proof, the public port stripping node headers. Proves the wrong-app,
  wrong-sender, lapsed-sibling, replay and relay revocation cases.
- **M5. Hostname routing, several runtimes.** The hostname table and the
  authority rules, several app runtimes and per-app followers, in-place
  restart after a verdict, the `multi-app` word and its running check; a
  second `[[app]]` is accepted from here on. Proves the exit criterion
  and the two-apps cluster tests. **One adversarial review after M5**
  covers M4 and M5 together, before M6.
- **M6. Process-wide operations.** `cluster status` grouped by process,
  `cluster drain` of a process (every node id it runs), the upgrade page.
  Proves the rolling upgrade and the rollback path.
