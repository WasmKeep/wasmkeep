# Running a cluster

> **Pre-alpha.** Ownership, forwarding, replication, failover and deploys
> work and are tested (simulations in `sim/` and multi-node tests), and have run
> against Cloudflare R2 and OVHcloud Object Storage, with nodes killed
> under load and no acknowledged durable writes lost
> ([measured](benchmarks.md#three-nodes-against-a-real-bucket)). Plain writes
> may be lost. Not yet under production load or against AWS S3. Do not
> trust a cluster with data you care about yet (see the [roadmap](../ROADMAP.md)).

Nodes share one S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) that
supports conditional writes (`If-None-Match` / `If-Match`). There is nothing
else to run: no consensus cluster, no coordinator.

For the short version of what callers see when a node stops or dies, and
of what adding nodes does, read
[When a node dies, and how it scales](failover-and-scaling.md).

This page says how a cluster works, how its nodes replicate, and which
flags a node takes. Three more pages cover the rest:
[Deploying to a cluster](cluster-deploying.md) (versions, the front,
access rules, secrets),
[Checking and operating a cluster](cluster-operations.md) (`cluster
status`, `cluster verify`, draining, moving and deleting) and
[Upgrading WasmKeep](cluster-upgrades.md) (the binary itself).

## How it works

- Each node writes a **heartbeat** to `nodes/<id>` with its address, every
  heartbeat period (at most a third of the lease ttl). It is the node's
  **lease** ([ADR 0020](adr/0020-one-lease-per-node.md)): written only if
  the object is still this process's last write, so a node finds out at
  its next write when another writer replaced it, and stops. A node runs
  actors' events and is ready only while that lease is valid (10 s after
  its last write, minus a 2 s safety margin). The lease timings tolerate
  up to 1 s of clock skew between nodes (see `wasmkeep_cluster::node_lease`, proven in
  simulation).
- Each actor has an **ownership record** in the bucket at
  `state/<tenant>/<app>/owners/<class>/<name>`, under its app's state root
  ([ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md);
  `default/default` for `serve --actor`), naming the node (and its process)
  that holds it, with a generation of its own for every hold. A node
  writes it when it takes the actor (its first event after waking) and
  when it gives it up (hibernation, a drain, a move, a clean stop): there
  are no renewals while the actor stays in memory. A request that reaches
  a node not holding the actor is forwarded **once** to the node the
  record names, if that node is live. Responses carry `x-wasmkeep-node:
  <id>`. If no connection to that node can be opened, or the connection
  fails before an answer, the client gets a 503 ("the actor's node did
  not answer; retry") and the request is not sent again: whether or not
  it was written, since a request that was written may have run there,
  and the 503 does not say which it was. Only a connection kept open
  that the node had already closed costs nothing: it is replaced before
  the request is written. (One exception: a WebSocket upgrade that node
  provably never read, because it refused the TLS handshake or the
  upgrade came back unsent on a new TLS connection, is sent once more,
  the client waiting meanwhile; see "A caller its peer briefly stopped
  trusting" below.) A release that fails is retried in the background;
  until it lands, the record keeps naming the node.
- **Placement at wake-up** ([ADR 0041](adr/0041-placement-at-wake-up.md)):
  a node about to wake an actor nobody holds (no record, or a released
  one) keeps it, unless it is clearly fuller than another node of the
  app. Then it sends that one event to the lighter node, once, and that
  node acquires the actor as for any request. "Clearly fuller" is a
  resident fill of at least 20% of the app's room on this node, more than
  25% above the lighter of the two rendezvous candidates, within a
  per-heartbeat budget. The candidates are the live, serving nodes of the
  same app that announce `place`, whose heartbeat was first seen less
  than three periods ago (9 s), and that are not backed off. The
  receiver checks its own count when the hop arrives and refuses
  (`503`, `x-wasmkeep-placed: refused`) unless it stays at 80% of the
  sender's fill or less with the actor; the sender then runs the event
  itself. A placed event is never placed or forwarded again, and two
  nodes never place on each other within three periods. Only an outside
  request with no body or one declared at 64 KiB or less, and
  `actors.call`, are placed: never a WebSocket upgrade, an alarm, a
  streamed body, a take or a drain's hand-over, and never an actor whose
  record names a node. A running actor never moves because of it, and a
  big room's edges are not placed by load: each has a home node, where
  it is moved back after a node rejoins
  ([edges return home](adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#amendment-2026-11-26-edges-return-home)). Turn
  it off per process with `--no-placement` or `[process] placement =
  false` ([node file](reference/node-file.md)); `--no-rebalance` turns
  off only the moves home.
- A node that stops cleanly (`SIGTERM` or Ctrl-C) finishes its in-flight
  events, then releases its actors: the next request for one of them
  starts it on another node at once. Last, it writes its heartbeat
  `released`, which no reader counts as live, and which frees any record
  whose release did not land.
- If a node dies, a peer that saw its heartbeat unchanged for a lease
  period plus a write timeout (a third of the ttl plus 2.7 s, 6 s by
  default), and once 15 s (the ttl plus a 5 s grace period) have passed
  since its last write, writes it **fenced**: no reader counts that node
  live any more, its actors are free, and the next request for one of
  them starts it on another node (15 to about 21 s after the node's
  last write). If its process was only cut off from the bucket, its next
  write fails and it stops (a service manager restarts it). A node whose
  own lease runs out (the bucket unreachable for 8 s) drops every actor
  at once and takes them again, at new generations, once its lease is
  valid again. After a bucket outage, a node renews its own lease and
  waits a lease period plus a write timeout again before it fences
  anyone, so a node that regained the bucket at about the same time has
  renewed by then. One cut off for longer is fenced, and restarts.
  Meanwhile a node that gets a request for one of the dead node's actors
  answers 503, or keeps the request until the actor is free: see "When a
  node dies: what a caller sees" below.
- A deleted heartbeat frees nothing: the actors its node held stay held
  until a process starts with that id (its start check waits, then it
  takes them back) or the node is fenced: `wasmkeep cluster fence <node>`
  writes a fence even with no object, and the actors are free 15 s
  later. Ownership records and markers are never collected on their
  own; `released` node leases are, by the live nodes (below).
  `wasmkeep cluster forget <node>` removes a retired node's
  heartbeat in the format from before node leases, or a `released` node
  lease (a node that stopped cleanly) that no ownership record of any app
  on the prefix still names, once either is older than 25 s; it refuses a
  live or fenced node lease, and a released one a record still names (that
  lease is what frees the record; each such record is taken over by the
  next request for its actor). A store's DELETE takes no condition. Once
  the app's upgrade to 0.9 is finalized ([`wasmkeep cluster upgrade
  finalize`](cluster-upgrades.md#versions-the-upgrade-floor-and-finalize)), the command removes a released lease in two steps:
  it replaces the lease with a mark, by a write that lands only over the
  lease it checked, then deletes the mark. A process that started under
  that id meanwhile makes that write fail, and the command leaves its
  lease alone. A process that starts while the mark is there writes
  nothing over it, waits for it to be gone and then starts as on a new
  id, watching the absent key about 45 s (the bucket client's request
  budget plus ttl and grace) before it creates its lease; if the mark is still there 60 s later (the command stopped between
  its two steps), the process does not start, reports `this node's lease
  is being removed by cluster forget`, and running the command again
  removes the mark; `cluster status` warns about a mark older than a
  few seconds, with that command. A process that was already checking
  its node id when the released lease was removed checks it again, over
  no object, and starts. The mark is not a node: no listing counts it.
  Before
  the finalize, and for a heartbeat from before node leases, the command
  reads the object twice and deletes only if it did not change between
  the reads, and says so: a process starting under that id whose first
  write lands in the moment between the second read and the delete loses
  that lease and exits (its next write finds the object gone), and its
  service manager starts it again: availability only, nothing is taken
  from another node.
- **Released node leases are removed by the live nodes**
  ([ADR 0020](adr/0020-one-lease-per-node.md#amendment-of-2026-11-27-released-leases-removed-by-the-live-nodes)),
  so a cluster whose nodes start under new ids does not pile them up.
  The lowest-id live node of each app removes its app's `released`
  leases a day after their release, the way `cluster forget` does after
  the finalize: the mark, then its delete, and never a lease an
  ownership record still names. Nothing is removed until the app's
  upgrade to 0.9 is finalized and while a node of the app older than 0.9
  is live. A mark a removal left behind (its node stopped between the
  two steps) is deleted about 16 s after it was written, so a process
  waiting on it starts. One round every 15 s, a listing of the keys of
  `nodes/`; at most 16 leases per round. `serve --no-lease-removal` (or
  `[process] lease-removal = false`) turns it off: set it alike on every
  node, since a node with it off is still the one chosen if its id is
  the lowest. Counted by `wasmkeep_node_lease_removals_total{outcome}`.
  Each node's table of the newest lease bodies it read drops a node whose
  lease has been gone for 15 s.

### When a node dies: what a caller sees

Between a node's death and the takeover of its actors (15 to about 21 s
after its last lease write, with the defaults), no node can run them. A
surviving node that gets a request for one of them does one of two
things.

**It keeps the request and answers once it took the actor over**, for a
`GET` or `HEAD` without a body and for a WebSocket upgrade that arrive
from outside. The caller sees one slow answer (the actor's own, with
`x-wasmkeep-node` naming the new owner, or the `101`) instead of a 503 to
retry. The takeover is the ordinary one, and the actor is free at the
same moment as without the wait: nothing about leases changes, the wait
only saves the caller the retries. A kept request asks for the actor up
to a second after this node learns it is free (the requests kept for
different actors are spread over that second), which is at its own fence
of the dead node or, when another node wrote the fence, at its next
next read of the heartbeats (up to 3 s later). A request is kept when the node
has a reason to think the owner is gone or going:

- the owner's heartbeat left this node's view (10 s after its last write)
  and has not moved since;
- the owner is still in view, but a connection to it could not be opened
  from this node, and no lease write of it since then proves it alive;
- the owner is fenced (by a node or by `wasmkeep cluster fence`), or
  stopped and released, and its actors are not free yet.

**It answers 503 with `Retry-After: 1` at once**, as before, for
everything else: a request with a body, any other method (`POST`, `PUT`,
`DELETE`, ...), an `actors.call` (the calling actor gets its error), a
request another node forwarded, and any request over the caps below. Such
a 503 still means the request was not run here; nothing with a body is
ever kept, so a write is never run late for a caller that gave up. A
request that was sent to the owner and may have run there is never
answered this way: its 503 or 504 carries `x-wasmkeep-outcome: unknown`
and no `Retry-After` ([what "saved" means](what-saved-means.md)). An
owner that is alive keeps its actors: a node that renews its lease is
never taken from, however slow it is to answer, and a request for one of
its actors is sent to it as before. The one exception is a node that is
alive but to which this node could not open a connection: requests for
its actors are kept for a few seconds, until its next lease write shows
it alive (see "An owner that is alive but refuses connections" below).

A kept request ends in one of these ways:

- served by this node, which took the actor over;
- sent, once, to another node that took it over first, and answered by it;
- sent to the owner as usual, because the owner turned out to be alive (it
  wrote its lease again, or restarted);
- 503 "the actor's node is not reachable; retry" with `Retry-After: 1`
  when the wait runs out, when this node begins to stop, or when its own
  lease runs out. Never a 504 from the wait itself;
- dropped, and never run, when its client closes the connection first.

`--failover-wait-secs` sets how long a request is kept at most. The
default comes from the lease timing: the ttl plus grace (15 s), the watch
before a fence (6 s), one heartbeat period for a node that did not write
the fence itself (3 s) and a second over which the kept requests are
spread, 25 s in all. `--failover-wait-secs 0` turns the wait off: every
request gets the answer it got before, at once. A node keeps at most a
quarter of its connection limit and 64 requests per actor; a request over
either gets the 503 at once. The connection limit is `--max-connections`
(10,000 by default, so 2,500 kept requests) after `serve` lowered it to
fit the open-file limit: a node left at a limit of 1,024 files keeps far
fewer, and one whose quarter comes to zero keeps none (counted as
`over_cap`). Nothing identifies the
caller before a request is kept, so anyone who can reach the node can
fill the places for a dead node's actors; later callers then get the 503
at once, as without the wait. A kept request holds its connection and its
headers, nothing else: no actor is loaded for it, and no bucket request
is made for it while the owner's heartbeat is only stale. While the owner
is suspected after a failed connection, or stopped and released, each
kept request reads the actor's ownership record about once a second, as
a client retrying every second would; once the actor looks free it asks
for it, about once a second until it has it. Each node decides by itself, nothing travels
between nodes or into the bucket, so nodes with different settings, or
of versions with and without the wait, work together.

What the wait does not cover, with the defaults:

- **The requests that find the death out.** A request already sent to the
  node when it died gets the 503 ("did not answer"), or a 504 after 30 s
  if the host went silent instead of refusing; it may have run. On each
  surviving node, a request that arrives while the dead node is still in
  view is sent to it, and gets that 503 when the connection is refused.
  Only requests after it are kept. If the host is silent rather than
  gone (a cut cable, a frozen machine), connections to it are not
  refused but time out after 5 s: every request that arrives before the
  first of them timed out waits those 5 s for its 503, and one written
  into a connection that was already open gets a 504 after 30 s.
- **A request that arrives in the first seconds can still get the 503.**
  The takeover can come up to about 21 s after the node's last write,
  the surviving node may learn of the fence a heartbeat period later, and
  after a bucket outage the watch before a fence starts over: a request
  kept from the very moment of the death can reach its 25 s and be
  answered 503 after waiting that long. One that arrives once the node
  left the view waits about 15 s at most.
- **Another node took the actor first.** The kept request is then
  forwarded to it, and waits for that node's restore of the actor under
  the usual 30 s limit: about 55 s in the worst case, ending in the usual
  504.
- **An owner that is alive but refuses connections.** When this node
  cannot open a connection to a node that is alive (its listener
  restarts, a firewall or a one-way network fault, the 5 s connection
  timeout, or this node running out of sockets), the request that finds
  it out gets the 503 as before, and the `GET`s and upgrades that follow
  for that node's actors are kept instead of sent, even over connections
  to it that still work. They are released when a heartbeat listing shows
  a lease write of that node more than 2 s (twice the allowed clock skew)
  after the failure: about 2 to 7 s later, since a node writes its lease
  every 2 s and this node reads the heartbeats every 3 s. They are then sent to the
  owner as usual, and get its answer or the 503 they would have got at
  once. Nobody takes the actors. The node is then not suspected again for
  10 s; while the fault lasts, the next failed connection after that
  starts the same again.
- **The answer after the wait can still be an error**: the node is full,
  the bucket fails during the takeover, or an access rule refuses the
  request (a class open only to existing actors answers 404 for one that
  does not exist, after the wait; a class not reachable from outside is
  refused 403 at once, before any wait).
- **A node that drains** takes no actor: a request it would hand to a
  peer is handed over as before, not kept.
- **A `GET` that writes.** A kept `GET` runs up to 25 s after it was sent,
  and its client may have retried elsewhere meanwhile. An actor whose
  `GET` handlers change state should not rely on the wait: turn it off,
  or make those handlers safe to repeat.
- **A load balancer that hides the client's departure.** A kept request
  is dropped when its connection closes. A proxy that keeps its
  connection to the node open after its own client gave up hides that,
  and the request then runs for nobody: see [the front
  door](deploy/front-door.md) for the settings that matter.

`wasmkeep_failover_waits_total` counts the kept requests and how each
wait ended ([monitoring](monitoring.md)).

### What an idle cluster asks of the bucket

With nothing to do, each process of a three-process cluster with one deployed
app sends the following requests. This is the current code's model at the
default heartbeat and deploy/secrets check periods (3 s), with no actors,
alarms, operator intents, secrets, restore/recreate markers or optional idle
expiry. Startup, retries, changed deploys and lease lapses are excluded.

| What | Requests | Per process, one app |
| --- | --- | --- |
| Its node lease | 1 PUT a heartbeat period; no steady-state GET on a successful renewal | 0.333 PUT/s |
| All node heartbeats, including its own | 1 GET per logical node a heartbeat period, including periods that LIST `nodes/`; 1 LIST every ten periods (30 s) | 1.000 GET/s, 0.033 LIST/s |
| The operator's intents (drains, moves, actor deletes and restores) | 1 GET of `control/changed` a heartbeat period; four intent LISTs when it changes, or every minute | 0.333 GET/s, 0.067 LIST/s |
| The alarm index | 1 LIST when an entry is due for the sweep, at most every 10 s and at least every minute; no GET with an empty index | 0.017 LIST/s with no alarms |
| Deployed version pointer (`apps/<tenant>/<app>/current`) | 1 GET every deploy check; no manifest/component GET when unchanged | 0.333 GET/s |
| App deletion tombstone (`deleted`) | 1 GET every deploy check | 0.333 GET/s |
| App restore fence (`restoring`) | 1 GET every deploy check | 0.333 GET/s |
| App recreation marker (`recreated`) | 1 GET every deploy check | 0.333 GET/s |
| App restoration marker (`restored`) | 1 GET every deploy check | 0.333 GET/s |
| Secrets pointer (`secrets/current`, unless `--no-secrets`) | 1 GET every secrets check | 0.333 GET/s |
| File usage (`state/<tenant>/<app>/blob-usage`) | 1 GET per minute on every node, even without files | 0.017 GET/s |
| File sweep records (`blob-sweeps/`), on the collecting node only | 1 LIST per minute, even with no records | +0.017 LIST/s on that node |
| Hourly file epoch cleanup and usage count, on the collecting node only | With no files: 4 GETs (two fresh reads each of `recreated` and `restored`), 2 LISTs and 1 PUT | +0.0011 GET/s, +0.0006 LIST/s, +0.0003 PUT/s on that node; code-derived, outside the short measurement |
| Readiness and the node file | Readiness uses local state; the node file is read locally at start | 0 bucket GET/s |
| Idle expiry, on one node, for an app whose classes expire | 2 LISTs (`owners/`, `alarms/`) every 10 minutes | 0.003 LIST/s on that node |

**Between hourly passes: 3.350 GET/s, 0.333 PUT/s and 0.117 LIST/s
per process, plus 0.017 LIST/s on the collecting node.**
The earlier 2.333 GET/s model omitted `restoring`, `recreated` and
`restored`: together 1 GET/s. The intent table also omitted the fourth
LIST, for actor restores, and file maintenance. The deletion, restore and
recreation reads are safety checks (`AppRef::still_ours`), not redundant probes: they stop an
old runtime from serving a deleted or replaced app. `app_check` repeats
them at startup and after a lease lapse; that is outside steady state.
The node's own heartbeat GET detects twins and supplies the live set;
it must not be subtracted from the membership reads.

With **A apps per process and N processes**, every app's membership loop
reads all **N × A logical node objects**, including other apps, each
heartbeat period. Thus at default periods the GET rate per process is
**A × (N × A + 7) / 3 + A / 60**: heartbeats plus control, five deploy/safety reads,
and secrets, plus the minute file-usage reads. For three processes and
two apps this is **8.700 GET/s**, about a 2.6× increase, with 0.667 PUT/s
and about 0.233 LIST/s, plus 0.017 LIST/s per app collected there. Hourly
passes add the small collector-only costs above. The transport is
shared but the membership observations belong to each app's independent
lease/twin/fence checks. Caching these between loops would need a separate
freshness proof; this accounting change does not alter those semantics.
Old node objects left under `nodes/` also cost reads until forgotten.

GET totals sum **all** `status` labels of
`wasmkeep_bucket_requests_total{operation="get"}`. `status="not_found"`
is a subset, not an additional operation. In this fixture it accounts for
2.017 GET/s per app: control, four safety markers, the secrets pointer,
and the minute file-usage read (until the hourly pass publishes usage).
These are bucket-client calls; retries and paginated LISTs can produce more
wire requests. The [loopback measurement](benchmarks.md#bucket-requests-at-rest)
counts both and checks every GET key against this model.

About 0.117 LIST/s per process, down from 1.43 (1.1 in 0.4.1, before the
app's delete requests were listed too), so about 907,000 LISTs a month
for the three processes, plus 43,200 on the collector and 1,440 hourly
maintenance LISTs: about 952,000 in total instead of 11 million. What got
slower: a drain, an undrain or a move requested by a CLI of 0.4.1 or
before is read within a minute instead of 3 s (one from the 0.4.2 CLI
still within 3 s); a node of 0.4.1 that starts is seen by 0.4.2 peers
within 30 s; and an alarm whose owner is alive but never fires it can
wait up to 75 s instead of 25 s.

## Replication

Each actor's database follows it from node to node (ADR 0007):

- After every event, the owner uploads what the event committed as an LTX
  file under `state/<tenant>/<app>/actors/<class>/<name>/wal/`, and a snapshot every 64
  uploads. A node that takes an actor over rebuilds its database from the
  bucket first, unless its local copy is already current.
- An upload the bucket leaves unanswered is given up and sent again: an
  attempt after 5 s plus a second per megabyte (the next one after twice
  that), three attempts at most, the whole upload within 20 s plus that
  second per megabyte. This holds for a write's file, for an actor's first
  snapshot and for the snapshot after a delete. An attempt is one call to
  the bucket client, which sends the request again within it when the
  bucket answers with an error (a 5xx, 429 or 408) or the request could
  not be sent. The client also ends any single request after 30 s by
  default, and does not send an upload again after that or after a broken
  connection: the attempt fails. So a request lasts 30 s at most whatever
  the object's size, and an attempt as long as the bounds above allow: the
  second per megabyte gives a first attempt's request more time up to
  25 MB and no further, and an object the bucket cannot take within 30 s
  in one request is not uploaded.
- A periodic snapshot is an optimisation, and the actor is held while it
  uploads, so it gets one attempt, which the node does not send again
  (the client may, within the attempt, after an error answer): given up
  after 5 s plus a second per megabyte, 10 s at most (6 s under a
  megabyte), a third of
  the 30 s a queued caller waits for an actor (the default wall-clock
  limit, for `actors.call`, and the wait for a forwarded request's
  answer). One that
  fails costs no write and fails no event (the files already uploaded are
  the history). It is tried again once the actor has made one more write
  and 30 s have passed, then, while it keeps failing, 8 writes later, then
  every 64. With a bucket that leaves every snapshot upload hanging, the
  worst case is 10 s of hold per try and three tries in any 64 writes
  (30 s), never two within 30 s, then one try per 64 writes (10 s); an
  actor that only reads does not try again. The node remembers these waits
  while an idle actor is out of its memory, so waking it does not try
  again either; a node that takes the actor over from another, or one
  that restarted, knows of no wait and tries at the actor's first event
  there when 64 writes or more have no snapshot. A
  database the bucket cannot take within those 10 s gets no periodic
  snapshot: each try is one bounded attempt, and a restore replays every
  write since the last snapshot. Uploads not acknowledged are counted by
  `wasmkeep_snapshot_upload_failures_total` ([monitoring](monitoring.md)).
- `storage.exec` commits locally; its upload follows when the event ends.
  If the node dies in between, that write can be lost. In a class with
  batched writes ([ADR 0034](adr/0034-batched-plain-writes.md); strict is
  the default) the upload follows behind the actor's next
  events, one segment in flight at a time, and a node's death loses the
  segment in flight and what was committed behind it: about two upload round trips plus one event with a healthy bucket,
  up to `--max-unsent-age` (500 ms by default) plus two events after a
  bucket stalls; durable writes are never lost
  ([measured](failover-and-scaling.md#what-callers-get-meanwhile)). A drain, a move, a hibernation, parking
  and a clean stop flush first and lose nothing with a healthy bucket. A
  clean stop ends a batched actor's running event at its deadline (10 s,
  for events and flushes together) and gives up what is not uploaded by
  then, counted in `wasmkeep_plain_events_dropped_total` before the node
  lets its actors go; an actor whose database work does not return within
  a further half second is let go uncounted, as on a node's death. `storage.exec-durable`
  returns only once the write is in the bucket, uploaded while the node still
  held the actor's lease: an acknowledged durable write survives any node
  failure. The bucket is its only durable copy: the node does not fsync
  it locally as well, and trusts its local file again only after checking
  it against the bucket. So start a node again on the same bucket and
  prefix: after a crash or a power loss its local files may lack writes
  the bucket has.
- If an upload fails, the node gives the actor up; the next request restores
  it from the bucket, possibly on another node.
- A restore's reads are bounded together. The node lists the actor's history and
  reads its last snapshot and the files after it, eight at a time; every
  request waiting for the actor waits behind that. A read the bucket leaves
  unanswered for 5 s, or that is slower than 5 s plus a second per
  megabyte, is sent again, and so is one the bucket refuses with an error
  a resend can get past (a 5xx or a broken connection, after the bucket
  client's own retries), half a second later, then a second: three tries
  per file. While it pauses, a read gives up its place among the node's
  reads, and waits for one again before it is resent. The reads of one restore have 30 s together, plus a second
  per megabyte of what it reads and a second per eight files: 39 s for 63
  small files and a snapshot under a megabyte, about 17 minutes more for a
  database of 1 GiB. That counts the time the restore waits for the
  bucket to give it the next file. It does not count the listing (given
  up only when the bucket sends no key for 5 s, so a long history lists
  for as long as its pages take), nor work on the local disk (never
  interrupted), nor the time a read waits for its turn among the node's
  other restores (they share 32 reads and 256 MiB at once): while that
  queue lasts, a caller can wait longer than the deadline. Nor does it
  count what other restores add to a read's time. A read's own limit
  counts every megabyte being read on the node at that moment, other
  restores' included, since they share one link: after a node dies, the
  node that takes over 32 actors of 8 MiB each gives every read 261 s
  (5 s plus 256), while each restore, by its own size, has 39 s. So for
  each file, the seconds that other restores' megabytes added to the
  limits of its read's tries are left out of the count, up to the time
  the restore waited for that file. A restore is therefore never ended
  because of what other restores are reading: what is counted for a file
  is at most what its read is allowed with nothing else read on the node.
  A restore alone on the node gets nothing more than its 30 s and its own
  seconds, and slow reads that add up end it. At worst a restore's waits
  for the bucket last its own time plus 768 s per file (three tries, each
  given at most 256 s by the 256 MiB the node reads at once): only while
  other restores keep the node at that limit and each of its reads is
  tried three times to the end of its time, which is what each read's
  own limit allows. A bucket that never answers ends a restore sooner,
  whatever else is read: a read is given up after three silences of 5 s.
  A restore that fails or runs out of time before its
  database is complete changes nothing on the node: the database is built
  beside the local file and put in its place only when complete. If
  putting it in place fails (an error of the local disk), the file that
  was there has already been moved to `<name>.db.replaced`, and the node
  is left with no file for the actor or with the new one unmarked. Either
  way the next restore starts again from a fresh listing and reads the
  bucket: a node uses a local file as it is only after checking it
  against the bucket's history. The request that waited gets a 503 with `Retry-After`, as for a
  bucket that does not answer (a history that cannot be restored without
  losing data stays an error, 500), the node gives the actor up so
  another may try, and
  `wasmkeep_restore_failures_total` counts it
  ([monitoring](monitoring.md)).
- `storage.delete-self` is recorded in the bucket, so a deleted actor stays
  deleted wherever it runs next. Like a plain `exec`, it is replicated when
  the event ends: if replication fails at that moment, the delete can be
  lost. `storage.delete-self-durable` is the durable one: it returns only
  once the bucket holds the delete, or an error (`internal`: not known to
  be durable; the actor is then restored from what the bucket holds), and
  storage calls after it in the same event are refused. Once the delete
  is recorded, the actor's history in the bucket is erased (see
  [History in the bucket](#history-in-the-bucket)). An operator deletes
  an actor with `wasmkeep actor delete` ([Deleting one actor](cluster-operations.md#deleting-one-actor)).
- Inside a transaction, `exec-durable` takes only the `COMMIT`: it returns
  once the whole transaction is in the bucket. Any other statement there is
  refused before it runs, since nothing is durable before `COMMIT`.
- Pending alarms are listed in the bucket (`alarms/`, by due time). Every
  node reads the overdue part of that list every 10 s and fires alarms
  overdue by more than a lease period plus grace, so an alarm whose node
  died still fires ([ADR 0009](adr/0009-alarms-in-a-cluster.md)).
- When a node rebuilds a database from the bucket, the local file it
  replaces is kept as `<name>.db.replaced` rather than deleted. Only
  one generation is kept: a second replacement of the same actor's file
  overwrites the first `.replaced` copy.

**Joining a cluster with existing data:** the first node to touch an actor
decides its history when the bucket is empty. Start the node that holds the
existing databases first, and let it serve each actor once, before adding
nodes. A database replaced by mistake can be recovered from `.replaced`,
until a later replacement of that actor's file overwrites it, so copy it
elsewhere before then.

## Try it locally

Any S3-compatible store with conditional writes works. Each node proves
it at start, before it writes or takes anything: on a throwaway object
under `probe/`, a create over an existing object and a replace with a
stale version must both be refused. A store that accepts them (it
ignores the conditions) would let two nodes own one actor, so the node
logs why, stays not ready and `wasmkeep serve` exits. A bucket that does
not answer yet is tried again, as at any start. Cloudflare R2 and
OVHcloud Object Storage pass it (tested); AWS S3 and MinIO document both
conditions. The bucket's listings must also be strongly consistent
after a write: a node that takes an actor over plans its restore from one
listing of the actor's history, at once after a voluntary hand-over, so a
segment the listing omits would restore an older head and fork the
history. `wasmkeep cluster diagnose --bucket s3://BUCKET` checks the rest of
what a cluster needs from a bucket (listings, deletes, large objects,
last-modified times, the clock) under a scratch prefix it deletes
afterwards, and says whether these checks observed the required
behaviour; it samples the bucket, it does not prove it safe
([buckets](deploy/buckets.md)). For a quick local try,
[moto](https://github.com/getmoto/moto)'s S3 server is enough (it keeps
everything in memory):

```bash
pip install "moto[server]" && moto_server -p 5055 &
curl -X PUT -H "Content-Length: 0" http://127.0.0.1:5055/wasmkeep-demo

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:5055 AWS_ALLOW_HTTP=true
W=examples/counter/target/wasm32-wasip2/release/counter.wasm

wasmkeep serve --actor counter=$W --data-dir .wasmkeep/a --listen 127.0.0.1:8081 \
  --node-id a --advertise http://127.0.0.1:8081 --bucket s3://wasmkeep-demo \
  --cluster-listen 127.0.0.1:7481 --cluster-advertise https://127.0.0.1:7481 &
wasmkeep serve --actor counter=$W --data-dir .wasmkeep/b --listen 127.0.0.1:8082 \
  --node-id b --advertise http://127.0.0.1:8082 --bucket s3://wasmkeep-demo \
  --cluster-listen 127.0.0.1:7482 --cluster-advertise https://127.0.0.1:7482 &
```

```bash
curl -i -X POST localhost:8081/actor/counter/alice   # 1, x-wasmkeep-node: a
curl -i -X POST localhost:8082/actor/counter/alice   # 2, forwarded to a
```

## Flags

| Flag | Meaning |
| --- | --- |
| `--node-id` | This node's id, `[a-z0-9-]`, unique in the cluster (see below) |
| `--advertise` | URL the other nodes use to reach this one |
| `--bucket` | `s3://BUCKET[/PREFIX]`; several clusters can share a bucket under different prefixes |
| `--cluster-listen` | Address of the cluster listener, e.g. `0.0.0.0:7443`: the port other nodes use, with mutual TLS (see below). Needed unless `--cluster-plaintext accept`; give it with `--cluster-advertise` |
| `--cluster-advertise` | `https://HOST:PORT` the other nodes reach the cluster listener at |
| `--cluster-plaintext` | `refuse` (default) or `accept`: whether plain HTTP between nodes is still used (see below). With `refuse`, a node needs `--cluster-listen` |
| `--failover-wait-secs` | How long a `GET` or `HEAD` without a body, or a WebSocket upgrade, for an actor whose node died or is stopping is kept until another node took the actor over, instead of a 503 at once; default 25 (from the lease timing), 0 turns it off (see "When a node dies: what a caller sees") |
| `--history-retention-secs` | How long old actor history stays in the bucket before garbage collection may delete it; default 3600, 0 allowed (see "History in the bucket") |
| `--no-idle-compaction` | Never take a snapshot at the head when an actor hibernates, nor run the idle pass a retention window later ([ADR 0042](adr/0042-compacting-an-idle-actors-history.md), see "History in the bucket"); on by default; `[process] idle-compaction = false` in a node file says the same |
| `--no-lease-removal` | Never remove released node leases (see "How it works"); by default the lowest-id live node of the app removes them a day after their release, once the app is finalized at 0.9; `[process] lease-removal = false` in a node file says the same |
| `--secrets-key FILE` | With `--app`: the key the app's secrets are sealed with (`wasmkeep secrets new-key`); given twice during a key change (see "Secrets in the bucket") |
| `--no-placement` | Never send a wake-up of an actor nobody holds to a lighter node, nor receive one ([ADR 0041](adr/0041-placement-at-wake-up.md)): the node keeps every actor it wakes and does not announce `place`. Placement is on by default; `[process] placement = false` in a node file says the same |
| `--no-alarms` | Never fire an actor's alarms on this node and sweep none from the bucket's index; requests are still served and the alarms stay pending for a node without the flag. For a node on a restored copy (see [backup and restore](deploy/backup-and-restore.md)). The node logs once at start that alarms are off |
| `--no-secrets` | With `--app`: run without the app's secrets from the bucket (`--env` only); without it and without a key, a node refuses to start once the app has secrets |

**Node ids must be unique.** A node may take back its own lease at once
(that is how a restarted node recovers its actors quickly), so two processes
sharing an id would be treated as one node and could both run the same
actor. A node therefore checks its id when it starts, before its first
heartbeat and before any lease. It reads `nodes/<id>` every half period for
the lease's ttl plus grace (15 s by default), the time any other
node waits before taking a dead node's actors, even when there is no such
object (a process whose object was deleted while it ran is given time to
show itself). A process still running rewrites it: the new one then exits
with an error naming the other's address, having taken no actor. A peer's
fence or the old process's own release of it does not count as running:
that process lapsed once the object's last live write is those 15 s old,
and the new one goes on then. A heartbeat in the format from before node
leases ([ADR 0020](adr/0020-one-lease-per-node.md)) keeps the new one
waiting until it is older than its 10 s horizon plus those 15 s, since such
a process renews its per-actor leases on a task of its own. Unchanged until
the end: the previous process is gone, and the new one writes its first
heartbeat, conditioned on the version it read last. So:

- A first start, or a restart while the previous heartbeat is still live,
  waits those 15 s before `wasmkeep serve` listens (it logs "checking that
  no other process runs with this node id"). After a clean stop or a
  fence of the previous process it waits only until that process's last
  live write is 15 s old: about 1.5 s when that has already passed.
- A restart while the old process still runs, or still finishes its
  graceful shutdown, is refused: let the service manager retry it.
- If the bucket cannot be read, the node keeps retrying and does not
  start serving.
- Two processes started within a moment of each other can both pass the
  check, but only one of their first writes can land: each is conditioned
  on the object the check read last. The other process's write fails; if
  it was already running, its next write fails the same way. That one
  drops its leases at once, takes no new lease, stops heartbeating, and
  `wasmkeep serve` shuts down and exits with an error; the other goes on.
  The listing of the heartbeats still catches a heartbeat with another
  token, or none (a node of an older version, which writes without a
  condition). Until then both may run the same actor, so still start a
  node only once the previous process with its id has stopped.
- Deleting a running node's heartbeat makes that node exit at its next
  write, for the same reason.
- `wasmkeep serve` exits with an error when its node id is refused at
  start too. A service manager that
  restarts it at once (`Restart=always`, a Kubernetes rollout bringing a
  new pod up before the old one stops, both with one fixed `--node-id`)
  retries until the other process has stopped.

When replacing a node, stop the old process before starting the new one
with the same id.

**WebSockets.** A client may open a socket on any node: a node that does
not own the actor relays the upgrade to the owner and joins the two
connections, so everyone in a room shares one actor wherever they connected.
If the actor later moves, its sockets close with code 1012; clients
reconnect. A room that hibernates with sockets open keeps them, and keeps
its ownership record naming its node
([ADR 0024](adr/0024-socket-home.md)): a request, a call (from a front,
say) or an upgrade that reaches the room through another node is relayed
to that node and wakes the room there, so its clients stay connected. The
record is released when the room's last socket there closes, so the room's
next event may then wake it anywhere. A room parked like this stays on its
node until then (a move or a drain closes its sockets, below); if its node
dies, the room is free once the node is fenced, about 15 to 20 s with the
defaults, like any actor of that node. If the record changes behind the
node's back anyway (a node of a version before 0.4.0 does not park, or a
fault), the sockets close with 1012 when one of them next sends a message,
or at the node's owner sweep (once a minute), so a client that only
listens is closed too, a minute later at most (ten minutes for a room the
node had parked). Sockets also close with 1012 when their node drains and
another node can take its actors ([Draining a node](cluster-operations.md#draining-a-node)), and when
its lease runs out (no write reached
the bucket for ttl minus safety, 8 s with the defaults): before any other
node can take its actors, and without waiting for the bucket to answer
again. A socket closed because its node lost the actor gets no
`on-websocket-close` there; a room keeping a list of who is in it should
rebuild it from `connections()`. Such a close can reach every client of
a node at once (a lease that runs out closes them all): clients should
reconnect after a random delay on 1012, backing off on each failure, so
they do not all reconnect in the same instant. A client that does not
answer a close within 5 s is dropped. If the owner does not answer the
upgrade, the client gets a
503 and may open the socket again; over mutual TLS between nodes, that 503
can take up to four heartbeat periods (12 s with the defaults) when the
owner refuses the relaying node's certificate (see "A caller its peer
briefly stopped trusting" below).

**Sockets through the front.** A front on any node can hand a client's
WebSocket to a room ([ADR 0027](adr/0027-sockets-through-the-front.md)).
When the room is owned by another node (or parked there with its sockets),
the front's node relays the upgrade to it **signed**, as a call between
nodes is: the front's caller value and the upgrade mark are among the
signed headers, and each relay is accepted once. The owner opens the
socket as the front's, so an `internal` room accepts it, and the socket
lives there, under the socket home rules above. An unsigned upgrade,
whatever headers it carries, is a client's: an `internal` room refuses
it. A node relays a hand-off only to a node that announces the `handoff`
word in its heartbeat (0.4.1 on); to an older one the client gets a 503
and retries.

**Calls between nodes.** `actors.call` reaches an actor wherever it runs.
Nodes prove to each other that a call comes from the cluster with a
signature: HMAC-SHA256, keyed with a random secret the first node stores at
`cluster/secret` in the bucket, over the node the call is for, the method,
path, body, calling actor, chain of waiting actors, the calling node's app,
a timestamp and a nonce
unique to each send (valid for a minute, and accepted once). Only a signed call's
caller (`x-wasmkeep-caller`) and chain are believed, so a client cannot pass
for an actor or reach an `internal` class, and a call cycle across nodes is
refused instead of deadlocking. The secret itself never travels; a malformed
one in the bucket stops calls between nodes rather than weakening them.

**The app on every hop.** A forwarded request, a relayed WebSocket upgrade
and a call between nodes name the sending node's app in
`x-wasmkeep-app: tenant/app` (signed on calls). A node refuses a hop for
an app it does not serve before reading it: 503, `Retry-After: 1`, "not
this app's node"; the actor does not run. Nodes started without `--app`
run as the implicit app `default/default`. A client's header is never
believed or passed on: it is removed from what clients send to actors and
from what actors send, and never reaches an actor
([ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)).

Credentials, region and endpoint come from the standard `AWS_*` environment
variables.

**Encryption between nodes.** Nodes talk to each other over mutual TLS,
on a cluster listener every node needs: a node started with `--node-id`
and without one refuses to start, unless given `--cluster-plaintext
accept`. In that mode, nodes without a listener talk over plain HTTP: a
call between actors is signed, its body included, but nothing is
encrypted and responses are not signed. Keep such nodes on a private
network (a node warns at start when `--advertise` is a public address).
A cluster listener on every node:

```bash
wasmkeep serve ... --node-id a --advertise http://10.0.0.11:8080 \
  --cluster-listen 0.0.0.0:7443 --cluster-advertise https://10.0.0.11:7443
```

Forwarded requests, relayed WebSockets and `actors.call` between two nodes
that both have one then travel over mutual TLS 1.3 on that port, in both
directions. There is nothing to provision: each node makes a key and a
self-signed certificate in memory at start (a restart is a new key) and
publishes the certificate's fingerprint in its heartbeat; a node accepts a
peer only if the peer's certificate matches a live heartbeat in the bucket
([ADR 0011](adr/0011-node-to-node-mtls.md)). So whoever can write the
bucket controls the cluster, as with the call secret.

- Both flags go together, and only with `--node-id`; `--cluster-advertise`
  must be `https://`. The cluster port serves actor paths and the health
  and readiness checks, nothing else, and only to other nodes.
- **Firewall:** only the other nodes need to reach the cluster port; do not
  expose it to clients or put it behind the load balancer. `--listen`
  still serves clients as before.
- A node with a listener reports not ready (`/_wasmkeep/ready`) for a few
  seconds after start, until its fingerprint has been published long
  enough for peers to have seen it.
- Rolling it out on a cluster that ran in `accept` mode: restart nodes
  one at a time with the flags, keeping `--cluster-plaintext accept`.
  Nodes reach peers that have a listener over TLS and the others over
  plain HTTP, and still accept plain signed calls on `--listen`, so a
  mixed cluster keeps working. A peer that publishes a TLS address is
  reached over TLS or not at all (a 503 the caller retries), never in
  plain text after a TLS failure. Until every node refuses plain HTTP
  (below), keep the public port's node traffic private too.
- **Refusing plain HTTP** (`--cluster-plaintext refuse`, the default): a node's call
  then counts only on the cluster port. A signed call arriving on
  `--listen` is treated as a client's (access rules apply, no calling
  actor, `internal` classes refused), and peers without a cluster
  listener are not used: requests and WebSockets for actors they own get
  a 503 from this node and `actors.call` to them fails (it logs which
  peers it skips, once a minute). Clients are served on `--listen` as
  before. A refusing node next to plain peers still reports ready
  (`/_wasmkeep/ready`): the load balancer will not take it out, so do not
  get into that state (below).
- **Moving a cluster from accept to refuse**: first give every node a
  cluster listener (restart them one at a time with the two flags and
  `--cluster-plaintext accept`, and check with
  [`wasmkeep cluster status`](cluster-operations.md#checking-a-cluster) that every live node
  shows a cluster address and a pin, and no warning about plain nodes: no
  node may still be plain). Then restart them one at a time again,
  without `--cluster-plaintext accept`. Doing it the other way round
  breaks the cluster while it is mixed: a refusing node cannot reach a
  plain node's actors (503), and a plain node's calls to it count as a
  client's (`internal` classes refuse them, callees see no caller). Going
  back is the reverse: add `accept` everywhere before removing a
  listener.
- The cluster port has its own connection cap (1,024, less when the
  open-files limit is low; its share is counted before the client
  connections and resident actors are sized).
- **What it costs:** on a pooled connection, TLS adds about 9 to 12 µs to a
  forwarded request and 15 to 21 µs to an `actors.call` between nodes, on top
  of a hop of about 40 µs over loopback
  ([measured](benchmarks.md#between-nodes-plain-http-against-mutual-tls)).
  Connections between nodes are kept alive, so full handshakes are rare.
- **How fast a removed node loses access:** revoke a node's process with
  `wasmkeep cluster fence <node> --bucket ...` (ADR 0020): it stops at its
  next heartbeat write, and its actors are free 15 s after its last write.
  If its heartbeat was deleted, the fence is created and dated by the
  bucket's time: a process of that node id starting over it writes its
  first heartbeat only once the fence is twice the allowed clock skew old
  on its clock (2 s with the defaults), and logs that wait.
  A service manager that restarts it brings it back as a new process; to
  remove a machine, stop and disable its service too, and rotate its
  bucket credentials ([security](security.md#layers), "Revocation"). A
  node stops trusting a peer whose heartbeat is fenced, released, deleted
  or names a new key at its next read of
  the heartbeats (one per heartbeat, 3 s by default), and one whose
  heartbeat stops at its expiry: three heartbeat periods plus the allowed
  clock skew, 10 s with the defaults, also when the node cannot list the
  bucket. Relayed WebSockets are cut within 500 ms after that
  ([security](security.md#layers), "Revocation"; timed in
  `crates/wasmkeep-server/tests/revocation.rs`).
- **A caller its peer briefly stopped trusting:** if a peer refuses a
  node's certificate at the handshake (it saw that node's heartbeat
  expire, as a slow bucket write can make it), an `actors.call` is sent
  once more after the caller's next heartbeat reached the bucket and two
  more periods passed (12 s at most with the defaults; it is not sent
  again if that heartbeat does not land within two periods); the peer
  never read the first one. Then it fails with "refused this node's certificate". A
  relayed WebSocket upgrade, which has no body, is sent once more the
  same way, the client waiting meanwhile; if it is refused again, or the
  heartbeat does not land, the client gets a 503 asking it to retry. A
  forwarded request gets that 503 at once: its body is not kept. A call
  or an upgrade that came back unsent on a new TLS connection to the
  peer is treated as one refused at the handshake (a refusal can close
  the connection before the request is written; whatever closed it, the
  peer read nothing). Nothing else is sent again: not a request whose
  connection could not be opened, and not one written into a connection
  that then failed, which the peer may have read.

  The price is the client's wait when the peer keeps refusing. The
  upgrade's 503 then comes after up to four heartbeat periods if the
  heartbeat lands and the second try is refused too (12 s at most with
  the default 3 s period, 9 s or less when the next heartbeat lands on
  time: up to one period for it, then two), and after two periods (6 s)
  if the heartbeat does not land; a call waits as long for its failure.
  Answering at once would be faster in that case only: a peer that
  refuses a live node does so for a moment, and then the wait ends in
  the open socket or the call's answer, where a 503 at once would leave
  the client to open the socket again and fail the call. A client that
  gives up on a WebSocket handshake sooner than that sees its own
  timeout instead.

An upgrade of WasmKeep itself is another matter:
[Upgrading WasmKeep](cluster-upgrades.md#upgrading-wasmkeep). One that
changes the format of signed calls upgrades every node together.

## Deploying, operating and upgrading

The rest of this guide is on three pages:
[Deploying to a cluster](cluster-deploying.md),
[Checking and operating a cluster](cluster-operations.md) and
[Upgrading WasmKeep](cluster-upgrades.md). The sections that were on this
page keep their anchors here:

| Section | Now on |
| --- | --- |
| <a id="deploying"></a>Deploying | [Deploying to a cluster](cluster-deploying.md#deploying) |
| <a id="deploying-static-assets"></a>Deploying static assets | [Deploying to a cluster](cluster-deploying.md#deploying-static-assets) |
| <a id="deploying-the-front"></a>Deploying the front | [Deploying to a cluster](cluster-deploying.md#deploying-the-front) |
| <a id="access-rules-and-configuration"></a>Access rules and configuration | [Deploying to a cluster](cluster-deploying.md#access-rules-and-configuration) |
| <a id="secrets-in-the-bucket"></a>Secrets in the bucket | [Deploying to a cluster](cluster-deploying.md#secrets-in-the-bucket) |
| <a id="checking-a-cluster"></a>Checking a cluster | [Checking and operating a cluster](cluster-operations.md#checking-a-cluster) |
| <a id="verifying-a-bucket"></a>Verifying a bucket | [Checking and operating a cluster](cluster-operations.md#verifying-a-bucket) |
| <a id="draining-a-node"></a>Draining a node | [Checking and operating a cluster](cluster-operations.md#draining-a-node) |
| <a id="moving-one-actor"></a>Moving one actor | [Checking and operating a cluster](cluster-operations.md#moving-one-actor) |
| <a id="seeing-where-an-actor-runs"></a>Seeing where an actor runs | [Checking and operating a cluster](cluster-operations.md#seeing-where-an-actor-runs) |
| <a id="deleting-one-actor"></a>Deleting one actor | [Checking and operating a cluster](cluster-operations.md#deleting-one-actor) |
| <a id="idle-expiry"></a>Idle expiry | [Checking and operating a cluster](cluster-operations.md#idle-expiry) |
| <a id="deleting-an-app"></a>Deleting an app | [Checking and operating a cluster](cluster-operations.md#deleting-an-app) |
| <a id="versions-the-upgrade-floor-and-finalize"></a>Versions, the upgrade floor and `finalize` | [Upgrading WasmKeep](cluster-upgrades.md#versions-the-upgrade-floor-and-finalize) |
| <a id="rolling-upgrades-and-new-storage-functions"></a>Rolling upgrades and new storage functions | [Upgrading WasmKeep](cluster-upgrades.md#rolling-upgrades-and-new-storage-functions) |
| <a id="the-recorded-head-ownership-format"></a>The recorded-head ownership format | [Upgrading WasmKeep](cluster-upgrades.md#the-recorded-head-ownership-format) |

## Upgrading WasmKeep

Upgrading the `wasmkeep` binary itself is on its own page,
[Upgrading WasmKeep](cluster-upgrades.md#upgrading-wasmkeep). In short:
from 0.8.1 to 0.9.0 the recommended way is to stop every node and start
them all on 0.9.0, then finalize; one node at a time is supported and
safe for data, but under load it is not yet proven to meet the latency
goal and leaves a big room's edges unbalanced until 0.10.0
([upgrading to 0.9.0](deploy/upgrading-to-0.9.md#measured)); 0.10.0 moves them
back ([upgrading to 0.10.0](deploy/upgrading-to-0.10.md)); between
any other pair of versions, stop every node, upgrade them all and start
them all, unless that release's upgrading page says otherwise.

## Several apps in one process

From 0.6.0 one process can serve several apps behind one public, one
cluster and one metrics port: the [node file](reference/node-file.md)
lists them. Each app keeps its own node id, lease, state and restarts; the
process shares its listeners, its certificate (named for `[process] id`),
its engine and its budgets. `wasmkeep cluster status` groups node ids by
process, and `wasmkeep cluster drain --process ID` drains them all. See
[upgrading to 0.6.0](deploy/upgrading-to-0.6.md) for the rollout.

## History in the bucket

Every write an actor commits becomes an object in the bucket, and every 64
writes its owner adds a snapshot (tried again later when its upload fails,
see [Replication](#replication)). After such a snapshot, the owner deletes
the history it makes unnecessary: older writes and snapshots, and uploads
of earlier owners that no restore would use
([ADR 0012](adr/0012-bucket-garbage-collection.md)). It keeps what a
restore needs and anything younger than `--history-retention-secs`
(default 3600, one hour; 0 deletes as soon as the rule allows), so a bug
can be noticed before history is gone. The window compares the bucket's
timestamps with the node's clock: a node whose clock runs ahead shortens
it by that much (safety does not depend on it). Deleting an actor erases
its history right away, whatever the window: two small objects remain, so
it is still known to exist. If the bucket refuses, the erasure is retried
a few times while the node holds the actor; after that the history stays
until the actor writes again, counted by
`wasmkeep_history_collect_failures_total`. Collection runs in the
background, never delays a write, and stops if the node loses the actor's
lease; deleted objects are counted by `wasmkeep_history_collected_total`
([monitoring](monitoring.md)). An actor that stops writing is compacted
([ADR 0042](adr/0042-compacting-an-idle-actors-history.md)), in two steps:

- **At hibernation.** When an actor hibernates or is evicted, the node
  that wrote its last write first uploads a snapshot there (after 8 writes
  since the last snapshot, or one for a database of 256 KiB or less, never
  above 64 MiB), so the next restore reads one object, and collects below
  it as after a periodic snapshot, honouring the retention window. An
  actor that writes a little at each wake therefore keeps its last
  snapshot, the write under it and what the window keeps.
- **One window later, the idle pass.** The node remembers each actor it
  released after writing, in memory (at most 100,000; a full queue
  refuses new actors, counted by `wasmkeep_idle_passes_dropped_total`).
  Once the window and the clocks' skew bound have passed, it takes the
  actor's record again, only if it is still the one its release wrote,
  without running the actor's code or restoring it, lists its history
  once, deletes what the window kept below the snapshot at the head, and
  releases it. The actor then holds two objects: that snapshot and the
  write at the head. Up to 4 passes run at once per node and app, a new
  one starting as soon as one ends; a pass deletes for at most 5 s, and
  stops as soon as a request for the actor waits behind it or the node
  begins draining. A pass stopped at its 5 s leaves the rest to another
  pass 5 s later, on the record its own release wrote, and so on until
  the history is compact (up to 1,000 passes, given up after 3 in a row
  that delete nothing): a large history takes one of the 4 at a time,
  behind every other pass due. A wake, a move or an expiry in between changed the
  record, and the pass is dropped; so is one on a draining node, of an
  app deleted, restoring or recreated since the node started, or of a
  class with `expire-after-idle` (the pass's rewrites would restart its
  idle clock). A node at its resident limit, behind the deployed version
  or not hearing from the bucket tries again a few seconds later, a few
  times. Counted by `wasmkeep_idle_compactions_total{outcome}`, its
  deletes by `wasmkeep_history_collected_total`.

What stays uncompacted: actors released by a node that then restarted
within the window (the memory is lost), actors woken within the window
without writing, actors whose node crashed, actors whose release's answer
was lost, what a pass stopped by a request, a drain or the end of its
hold did not reach, and what a chain of passes gave up on; each is compacted after
it next writes and rests for a window. `--no-idle-compaction` turns both steps off. Late uploads of
earlier owners that land after a pass stay until the next pass. Old
deploy versions are removed by `wasmkeep deploy` (see
[Deploying](cluster-deploying.md#deploying)).
