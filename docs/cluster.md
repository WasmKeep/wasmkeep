# Running a cluster

> **Pre-alpha.** Ownership, forwarding, replication, failover and deploys
> work and are tested (simulations in `sim/` and multi-node tests), and have run
> against Cloudflare R2 and OVHcloud Object Storage, with nodes killed
> under load and nothing lost
> ([measured](benchmarks.md#three-nodes-against-a-real-bucket)), but not
> yet under production load or against AWS S3. Do not trust a cluster with
> data you care about yet (see the [roadmap](../ROADMAP.md)).

Nodes share one S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) that
supports conditional writes (`If-None-Match` / `If-Match`). There is nothing
else to run: no consensus cluster, no coordinator.

For the short version of what callers see when a node stops or dies, and
of what adding nodes does, read
[When a node dies, and how it scales](failover-and-scaling.md).

## How it works

- Each node writes a **heartbeat** to `nodes/<id>` with its address, every
  heartbeat period (at most a third of the lease ttl). It is the node's
  **lease** ([ADR 0020](adr/0020-one-lease-per-node.md)): written only if
  the object is still this process's last write, so a node finds out at
  its next write when another writer replaced it, and stops. A node runs
  actors' events and is ready only while that lease is valid (10 s after
  its last write, minus a 2 s safety margin). The lease timings tolerate
  clock skew between nodes (see `wasmkeep_cluster::node_lease`, proven in
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
  later. Node leases, ownership records and markers are never collected;
  `wasmkeep cluster forget <node>` removes only a retired node's heartbeat
  in the format from before node leases, once it is older than 25 s.

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
heartbeat listing (up to 3 s later). A request is kept when the node
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
ever kept, so a write is never run late for a caller that gave up. An
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
  after the failure: about 2 to 8 s later, since a node writes its lease
  every 3 s and this node lists every 3 s. They are then sent to the
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
  If the node dies in between, that write can be lost. `storage.exec-durable`
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
  lost. Once the delete is recorded, the actor's history in the bucket is
  erased (see [History in the bucket](#history-in-the-bucket)).
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
conditions. For a quick local try,
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
| `--secrets-key FILE` | With `--app`: the key the app's secrets are sealed with (`wasmkeep secrets new-key`); given twice during a key change (see "Secrets in the bucket") |
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
reconnect. A room that hibernates keeps its sockets open on its node but
releases its ownership record, so a request that reaches it through
another node wakes it there; its sockets on the first node then close with
1012 when one of them next sends a message, or at the node's owner sweep
(once a minute), so a client that only listens is closed too, a minute
later at most. Sockets also close with 1012 when their node drains and
another node can take its actors (see "Draining a node" below), and when
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
  [`wasmkeep cluster status`](#checking-a-cluster) that every live node
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
  A service manager that restarts it brings it back as a new process; to
  remove a machine, stop and disable its service too, and rotate its
  bucket credentials ([security](security.md#layers), "Revocation"). A
  node stops trusting a peer whose heartbeat is fenced, released, deleted
  or names a new key at its next listing of
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

An upgrade of WasmKeep itself, including one that changes the format of
signed calls, upgrades every node together ([below](#upgrading-wasmkeep)).

## Deploying

Instead of giving every node `--actor CLASS=PATH`, deploy once to the bucket
and let nodes follow:

```bash
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo \
  --actor counter=counter.wasm --actor chat=chat_room.wasm
wasmkeep serve --app acme/shop --bucket s3://wasmkeep-demo --node-id a ...
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo --rollback
```

- **Each app owns its actors** ([ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)):
  their state lives under `state/<tenant>/<app>/`, beside
  `apps/<tenant>/<app>/`, so several apps of one tenant can share a bucket
  prefix, and the same class and actor name in two apps are two actors.
  An app is those two prefixes, for deleting, exporting and access
  policies alike. Nodes started with `--actor` run as the implicit app
  `default/default`; `deploy --app default/default` moves such a cluster
  to deploys and keeps its actors. There is no `app delete` yet: to remove
  an app nobody runs, stop its nodes and delete both prefixes:

  ```bash
  aws s3 rm --recursive s3://wasmkeep-demo/apps/acme/legacy/
  aws s3 rm --recursive s3://wasmkeep-demo/state/acme/legacy/
  ```

- A bucket prefix that holds actor state written by a version before
  0.4.0 is migrated once with `wasmkeep cluster migrate` while every node
  is stopped ([Upgrade a cluster to 0.4.0](deploy/upgrading-to-0.4.md)).
  Until then, nodes and `deploy` of this version refuse that tenant and
  print the command to run.
- The tenant and app names of a new app match `[a-z][a-z0-9-]{0,63}`; an
  app deployed earlier keeps its name.
- `deploy` compiles and checks every component before uploading anything;
  a broken one is refused and the current version keeps running.
- Nodes check for a new version every 3 s and switch each actor at its next
  event. No actor is dropped; `on-init` runs once per version.
- An actor never runs older code than it already ran: a node that has not
  caught up yet refuses it (the client retries) and updates at once.
- Rollback makes the previous version current again (as a new deploy),
  with its access rules and configuration.
- Old versions are removed by `wasmkeep deploy` itself, after every deploy
  and rollback ([ADR 0012](adr/0012-bucket-garbage-collection.md)): it
  keeps the current version and the five before it (as many rollbacks
  as you can make in a row), and deletes older ones once they stopped
  being current more than `--version-retention-secs` ago (default 3600;
  0 at once; measured with the bucket's timestamps against the clock of
  the machine running the command). Only versions in the current one's
  line of `previous` links are candidates, oldest first, and the pass
  stops if `current` changes while it runs. Never collected: a version a
  concurrent deploy is still writing, one a failed deploy left, the
  versions a rollback stepped back from, and a version whose manifest
  this WasmKeep cannot read. Those stay in the bucket (they are small).

### Updating the stateless component

A deploy carries actor classes, with their access rules, configuration
and required names. The stateless component, the front
([ADR 0023](adr/0023-the-front.md)) that answers every path outside
`/actor/` and `/_wasmkeep/`, is the positional argument of `wasmkeep
serve`, a file on each node read once when the node starts, for nodes that
serve classes given with `--actor`:

```bash
wasmkeep serve site.wasm --actor cart=cart.wasm --bucket s3://wasmkeep-demo --node-id a ...
```

With `--app`, a component on the command line is refused at start: an
app's front is to ship with its deploy, so that a deploy and a rollback
move it with the classes (planned, ADR 0023). Until then a node serving
`--app` has no front and answers 404 outside `/actor/`.

Nothing in the bucket names the file, so `wasmkeep deploy` does not change
it, `--rollback` does not bring an older one back, and `cluster status`
does not show which one a node runs. To update it:

1. Copy the new file to every node, at the path its `serve` command names.
2. Restart the nodes one at a time (`systemctl restart wasmkeep`), waiting
   for each to be listed live by `wasmkeep cluster status` and to answer
   `/_wasmkeep/ready` before the next.

What happens meanwhile:

- **The restarting node** makes a clean stop: requests under way get up
  to 10 s to finish, its actors are given up and start on the other nodes
  at their next request, and its WebSockets close with 1012. The load
  balancer's check of `/_wasmkeep/ready` takes it out. At its start it
  checks its node id first, which takes what is left of 15 s since it
  stopped ([Flags](#flags)).
- **The other nodes** keep answering, each with the file it was started
  with. Until the last node has restarted, two versions of the component
  answer side by side, and a client may get either one from one request to
  the next. Keep two consecutive versions compatible with each other and
  with the deployed actors.
- **A file that does not compile or instantiate** makes `serve` exit at
  start. Restarting one node first shows it while the others still run
  the previous file.
- **Going back** is the same procedure with the previous file.

The front calls the node's `--actor` classes with `actors.call`, as an
inside caller: the callee sees `x-wasmkeep-caller: default/_front/main`,
and a call to an actor on another node is a signed hop, as an actor's
([the front](concepts.md#the-front)). It runs on the node that received
the request and is never forwarded. Its outbound HTTP follows the same
rules as actors' (public addresses only, unless the node allows a range
with `--allow-outbound`).

### Access rules and configuration

Access rules ([ADR 0010](adr/0010-who-may-create-an-actor.md)) and plain
configuration are part of the deployed version
([ADR 0013](adr/0013-access-rules-and-config-in-deploys.md)): every node
applies the same ones, at the same moment as the code they go with, and
a node started without flags no longer serves a restricted class open.

```bash
wasmkeep deploy --app acme/shop --bucket s3://wasmkeep-demo \
  --access monitor=existing --access hub=existing:main \
  --config hub=hub-config.env
```

- **A deploy changes what it names and keeps the rest.** Without
  `--actor`, the current classes are kept (copied inside the bucket, not
  uploaded again). Classes not named keep their rule and configuration, so
  a CI job deploying new code does not drop rules it never knew about.
  `--access CLASS=open` removes a rule, `--config CLASS=` a class's
  configuration. `deploy` prints what changed (class code, rules, the
  names of configuration values) and warns when a class removed with a
  rule leaves an added one open (a rename).
- **`--config` is stored in the clear**, as parsed (no comments). Keep
  secrets in `serve --env` files on each node: they win over the
  deployed configuration by name (each overlap is logged by name, never
  value). `deploy` warns about names that look like secrets (`TOKEN`,
  `KEY`, `PASSWORD`...) and URLs with a password, without printing them.
- **Rollback** brings back the previous version's rules too. One that
  would make a class more open (`existing` to `open`, say) is refused
  unless `--allow-reopen` is given.
- New values drop the class's resident instances at their next event (the
  database is kept, `on-init` does not rerun); rules alone do not. A class
  made `internal` closes its open sockets (1008) at their next message.
- A node that reads a version it cannot apply keeps the previous one and
  reports not ready (`/_wasmkeep/ready`) until it can.
- **`--access` with `--app`** is refused: rules come with deploys. Every
  deploy of 0.4.0 marks the app (its `current` carries token `3`, [ADR
  0022](adr/0022-app-owned-state-and-the-one-time-migration.md)), and a
  node refuses to follow a `current` from an earlier version. A cluster
  that still passes `--access` to its nodes deploys those rules once
  (`wasmkeep deploy --app acme/shop --bucket ... --access
  monitor=existing`), then removes `--access` from their command lines;
  the start-up error gives the `deploy` command. A node following an app
  that never deployed rules logs that every class is open.

### Secrets in the bucket

Secrets can live in the bucket, sealed with a key the nodes hold, and
change without a deploy or a restart
([ADR 0016](adr/0016-secrets-in-the-bucket.md)):

```bash
wasmkeep secrets new-key --out secrets.key        # mode 0600; keep it with the bucket credentials
wasmkeep serve ... --app acme/shop --secrets-key secrets.key   # every node
wasmkeep secrets set --bucket s3://acme-wasmkeep/prod --app acme/shop --key secrets.key \
  --class hub ADMIN_TOKEN < token.txt            # one trailing newline removed
wasmkeep secrets list --bucket s3://acme-wasmkeep/prod --app acme/shop --key secrets.key   # names only
```

- **Reaching the nodes.** Each node reads the app's secrets at start and
  checks for a change every 3 s. A class whose values changed drops its
  resident instances at their next event (the database is kept, `on-init`
  does not rerun); an event under way keeps the values it started with.
- **Which value wins**, by name: the deployed configuration, then the
  secrets, then the node's `--env` (logged at warn, never the value).
  `--env` stays a node-local break-glass.
- **Several at once:** `set --file FILE` (entries
  `<class> <name> <length>\n<value>\n`, any value, newlines included) and
  `unset --class CLASS NAME...` make one change each, so nodes never serve
  half of it.
- **A node that cannot open the current secrets** refuses the app's
  events with 503 and `Retry-After`, hands its actors to a node that can,
  takes none, and reports not ready. It blocks at once on a set it cannot
  open (no key, another key, an altered object), and after 30 s on one it
  cannot reach (the object missing or unreadable, `current` unreadable).
  Past those windows (one poll, 3 s, or the 30 s) it never runs an actor
  with an older or empty set. `cluster status` then shows it draining or
  drained without a drain marker; `undrain` does not apply. When no node
  can open them, every event for the app is 503 until one can. Socket close
  events on a blocked node do not run. Without `--secrets-key` (and
  without `--no-secrets`), a node refuses to start once the app has
  secrets.
- **Restoring the bucket from a backup** can put `current` below what the
  running nodes opened; they refuse it (and block after 30 s) until
  restarted: restart the nodes after a restore
  ([Back up and restore a cluster's bucket](deploy/backup-and-restore.md)
  has the whole procedure). Deleting `current` by hand
  leaves running nodes with the set they have and restarted ones with
  none.
- **Older nodes.** `set`, `unset` and `reset` refuse, without `--force`,
  while a live node of the app does not read secrets (an older version, or
  one run with `--no-secrets`): it would serve them as unset. Nodes say so
  on heartbeat line 10; the check counts a heartbeat as live with the
  default timings, as `cluster status` does.
- **Changing the key.** Give every node the new key as well (a second
  `--secrets-key`, one node restarted at a time), run `wasmkeep secrets
  rotate-key --key old.key --new-key new.key --app acme/shop
  --nodes-have-new-key` (or `--all`), then remove the old key from the
  nodes. The flag is required: a node without the new key cannot open the
  re-sealed set and answers 503 for the app until it gets the key, and no
  heartbeat says which keys a node holds. `set` and `unset` given two
  keys seal with the first, and take the same flag when that one is not
  the key the current set is sealed with. After a key compromise, change
  the values too: the old key opens the objects it sealed.
- **Losing the key.** Give every node the new key, then `wasmkeep secrets
  reset --new-key new.key --from FILE --nodes-have-new-key` writes a whole
  set without opening the current one.
- **Moving from `--env`.** Upgrade every node, give them the key, `set`
  each name, then remove the names from the `--env` files and restart the
  nodes one at a time.
- **Required values.** `wasmkeep deploy --requires hub=ADMIN_TOKEN,WEBHOOK_URL`
  makes those names part of the version: while one is not set (by the
  configuration, the secrets or a node's `--env`), the class refuses its
  requests with 503 (an alarm waits, a socket closes with 1011) instead of
  running without it, and the node logs the class and the missing names
  once (again if they go missing again), never a value. A value set to the
  empty string counts as set. Once the value is set, requests run at once;
  a waiting alarm runs within the alarm retry ceiling (5 minutes by
  default); an idle socket closes at its next message. Kept by later
  deploys; `--requires hub=` removes them, and a rollback brings back the
  previous version's. Required names alone do not end ADR 0013's
  migration (only rules or configuration do).

## Checking a cluster

`wasmkeep cluster status` reads the bucket alone, so it works from any
machine with read access to the bucket, whether or not any node answers:

```bash
wasmkeep cluster status --bucket s3://wasmkeep-demo
wasmkeep cluster status --bucket s3://wasmkeep-demo --json   # for scripts
```

```text
Cluster s3://wasmkeep-demo (live: heartbeat within 10.0s)

NODE  LIVE  AGE    STATE    ACTORS  SOCKETS  EV/MIN  VERSION  APP        ADDRESS        CLUSTER ADDRESS  TLS
a     yes   979ms  serving  12      3        840     0.4.0    acme/shop  http://a:8080  https://a:7443   pinned
b     yes   2.2s   serving  4       0        95      0.4.0    acme/shop  http://b:8080  -                plain
c     no    14m    serving  -       -        -       0.4.0    acme/shop  http://c:8080  https://c:7443   pinned

APP        DEPLOYED VERSION  SEQ  ACTORS
acme/shop  9cf4de9152d8dd29  1    16

2 of 3 nodes live; TLS: 1 with a cluster listener, 1 plain; version: 0.4.0
warning: mixed plain and TLS nodes: b still plain while a use the cluster listener; give every node a cluster listener, then drop --cluster-plaintext accept (ADR 0011)
```

- **Nodes** come from their heartbeats (`nodes/<id>`): the WasmKeep version
  each runs, the app it serves, its advertised and cluster addresses, and
  whether it publishes a TLS pin. **State** is whether it takes actors:
  `serving`, or, while it is drained ([ADR 0014](adr/0014-draining-a-node-and-moving-an-actor.md)),
  `draining`, `drained` (no actors or WebSockets left) or `stuck` (actors
  or WebSockets left and no node of the same app to hand them to); `-` from a node that
  predates draining. A node is **live** if its heartbeat was
  written within 10 s (three heartbeats plus the allowed clock skew), the
  rule the nodes themselves use with the default settings. Ages are
  measured on the clock of the machine running the command, so keep it in
  sync too. A heartbeat written `fenced` (a peer took its lease) or
  `released` (a clean stop) is shown as `no (fenced)` or `no (released)`
  whatever its age, and as `lease` in `--json`. A stopped or crashed node
  stays listed, not live, until its heartbeat object is removed from the
  bucket.
- **Load** is what each live node says with every heartbeat: **actors**
  resident in memory, open **WebSockets** (`SOCKETS`), and **events per
  minute** (`EV/MIN`) over the last minute or more, `-` for its first
  minute. It is as of the node's last heartbeat (up to two heartbeat
  periods old; a node that crashed keeps showing its last load until its
  heartbeat is older than the horizon), and nothing places actors by it.
  A node not live shows `-` (a fenced or released heartbeat keeps the
  last load it wrote, which no longer holds), and so does a node of a
  version before 0.4.0. In
  `--json`: `actors`, `sockets` and `events_per_min`, `null` when unknown.
  Events are counted per process, so nodes sharing one process (tests)
  share one count.
- **Apps** come from `apps/<tenant>/<app>/current`: the deployed version and
  its `seq`. **Actors** is the sum over the app's live nodes that say
  their load (`-` if none does; `actors` in `--json`): while some of its
  live nodes do not say it (an older version, or a node just started),
  the sum leaves them out. Which version each node has picked up is not
  in the bucket (nodes follow within a few seconds).
- **Warnings**: no live node; live nodes still plain while others use the
  cluster listener (the state [ADR 0011](adr/0011-node-to-node-mtls.md)
  says not to stay in, and not to refuse plain HTTP from); live nodes on
  different WasmKeep versions (see [Upgrading WasmKeep](#upgrading-wasmkeep));
  a draining node that is stuck; a node lease dated ahead of this clock by
  more than the allowed skew (`ahead_ms` in `--json`). Such a lease never
  goes stale, so no node fences it and the actors it holds stay held until
  that date: fix the clock that is wrong (that node's, or this machine's
  if it is behind). `wasmkeep cluster fence` stops the node, but the fence
  keeps the date, so its actors free 15 s after it; a fenced or released
  node whose date is still ahead is warned about too, until then. The
  nodes log the same, at most once a minute.
- The exit code is 1 when no node is live (an empty prefix included), 0
  otherwise, warnings or not; a bucket it cannot read is an error.

## Draining a node

To take a node out of service without stopping it first (to replace the
machine, resize it, or upgrade it), drain it:

```bash
wasmkeep cluster drain node-b --bucket s3://wasmkeep-demo
wasmkeep cluster status --bucket s3://wasmkeep-demo   # wait for STATE drained
```

While it drains, the node ([ADR 0014](adr/0014-draining-a-node-and-moving-an-actor.md)):

- **claims no actor.** A request, a call or a WebSocket upgrade for an
  actor it does not hold goes to another live node serving the same app,
  which takes it;
- **hands over the actors it holds,** up to 16 at a time:
  - their WebSockets close with 1012 (clients reconnect and land on the
    new owner);
  - their queued events finish and are uploaded;
  - the lease is released, and each pending alarm moves to the new
    owner's timer, so it fires on time even after this node stops;
- **closes the WebSockets of rooms that hibernated with sockets open**
  with 1012. Their clients reconnect through another node; their
  `on-websocket-close` events do not run, since the draining node does not
  take the room back to run them;
- **reports `drained`** once it holds nothing, no WebSocket included, and
  keeps running and forwarding until you stop it. You can then stop it
  with nothing left to hand over. A WebSocket this node relays to another
  node is not counted, and is cut when the node stops.

A node with no other node to hand to (alone, or every other node draining
or older than this feature) keeps serving its actors and reports `stuck`.
`cluster status` warns about it. Start another node, or undrain one.

```bash
wasmkeep cluster undrain node-b --bucket s3://wasmkeep-demo   # take actors again
```

Both refuse an id that no live node has (as `cluster status` counts it): a
drain request for a mistyped id would stay in the bucket until an undrain
with the same typo. `undrain` still withdraws a request no node answers to.
`--force` writes or deletes the request anyway (for a node about to start).

During each actor's hand-over its requests may get a 503 with
`Retry-After: 1` for a moment. A peer that fails to take an event is
skipped for about 10 s. A request that hits a dead peer before that gets
its one error.

## Moving one actor

```bash
wasmkeep actor move --app acme/shop room/lobby --to node-c --bucket s3://acme-wasmkeep/prod
```

The actor is `CLASS/NAME` of the app `--app` names. Without `--app`, it is
written `default/CLASS/NAME`, of the implicit app `default/default` (nodes
started without `--app`); another tenant in front is refused, asking for
`--app`.

The target must be a live node: an id no live node has is refused without
`--force`, and a target that is draining, or too old to take moved actors,
gets a warning (the request is written; the owner keeps the actor), as
does one that serves another app.

Within a few seconds the actor's owner hands it to `node-c`
([ADR 0017](adr/0017-moving-one-actor.md)):

1. its WebSockets close with 1012 and their close events run;
2. its queued events finish and are uploaded;
3. the lease is released, and from that moment every event for it on the
   old owner (requests, calls, alarms, reconnecting sockets) goes to
   `node-c`;
4. `node-c` takes it: it acquires the lease, restores the actor and arms
   its alarm.

If no node holds the actor, `node-c` takes it itself. A name that was
never an actor is not created.

- **The target** must be live, serve the same app, not be draining or
  blocked, and run a version that takes actors (heartbeat line 11, `take`).
  Otherwise the request waits, tried on every heartbeat, until it expires.
- **Not a pin.** Once idle, the actor hibernates on `node-c`, and its next
  event claims it wherever that event lands. A draining node does not act
  on move requests; its drain moves everything.
- **Retries.** A move that cannot finish (a socket that will not close, a
  target that does not answer) is tried again on every heartbeat until it
  works or the request expires (5 minutes).

## Upgrading WasmKeep

Upgrading the `wasmkeep` binary itself (not your application: that is
[Deploying](#deploying), which rolls from node to node safely): **stop
every node, upgrade them all, start them all.** WasmKeep does not yet
support nodes of different versions in one cluster, even
for the length of a rolling restart. The cluster serves nothing between
the last stop and the first start, so plan a short window.

1. `wasmkeep cluster status --bucket ...`: note the nodes it lists as live.
2. Stop every node cleanly (`systemctl stop wasmkeep`, `SIGTERM`). Each
   finishes its requests and releases its actors; a clean stop takes up to
   about 29 s.
3. Wait until `cluster status` exits with 1 (no live node), about 10 s after
   the last stop. A node still listed as live is still running the old
   binary: stop it first. This check does not depend on version strings.
4. Install the same new binary on every node.
5. Start every node, then run `cluster status` again: every node live, one
   version in the summary, no warning.

Going back to the previous binary is the same procedure, except across
the 0.4.0 layout change: `cluster migrate --rollback` works only until
the first node starts after the migration, and never after
`--remove-old --apply` ([upgrading to 0.4.0](deploy/upgrading-to-0.4.md)). Builds made from
source report the version of the release they follow (`0.3.0` for any
build since it), so the version column cannot tell two of them apart:
step 3 is what proves no old node is left.

**What goes wrong if old and new nodes run together** (each is a reason
for the rule; there may be others nobody has listed):

- **Every hop, from 0.3 to a later version**: nodes after 0.3.0 name their
  app on every hop to another node, and refuse a hop that names none: a
  request forwarded, an upgrade relayed or a call made by a 0.3 node gets
  503, "refused: sent by a node of an older version". The other way, the
  0.3 node fails the newer node's signatures: calls to `internal` classes,
  moves and alarm hand-offs fail, and other calls run there as requests
  from outside, with no calling actor.
- **Starting next to the other version**: a node started without `--app`
  names its app `default/default` after 0.3.0 and `default/`
  before. A newer node refuses to start while a 0.3 one is live on the bucket prefix,
  asking to stop every node and upgrade them together; a 0.3 node started
  next to a newer one refuses too, saying another app of the tenant runs
  there.
- **The layout of actor state** (0.4.0, [ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)):
  0.4.0 keys every app's actors under `state/<tenant>/<app>/`, and a
  bucket written by an earlier version is migrated once, with every node
  stopped, by `wasmkeep cluster migrate`. A 0.3 binary refuses a bucket
  being migrated or migrated; 0.2's `serve --app` and `deploy` cannot read
  the `current` token `3` and stop; a 0.2 `serve --actor` node finds a
  tombstone over each migrated actor's old record and answers 503 for it
  (an actor it creates under the old keys is not seen by 0.4.0: running
  0.2 on a migrated bucket is unsupported). Going back after the first
  0.4.0 start is not supported ([Upgrade a cluster to 0.4.0](deploy/upgrading-to-0.4.md)).
- **Calls between nodes**: the format of signed calls can change between
  versions.
  Old and new nodes refuse each other's calls (logged as "bad, expired or
  misaddressed signature (... a node of an older version?)"), so calls to
  `internal` classes on the other kind fail, and a call cycle across them
  ends in a timeout instead of `reentrant`.
- **Formats in the bucket**: a version can change how heartbeats,
  ownership records or alarm index entries are written. A node of another
  version may skip what it cannot read, so it may not see its peers'
  ownership or alarms: actors refused, alarms late or missed.
- **No other guard**: the deploy `seq`
  ([ADR 0008](adr/0008-deploy-and-rolling-upgrades.md)) keeps an actor
  from running older *application* code, not from an older WasmKeep.
  Nothing compares WasmKeep versions between nodes; the version in each
  heartbeat is only reported, by `cluster status`.

Rolling upgrades of WasmKeep across mixed versions are planned
([ROADMAP](../ROADMAP.md)).

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
([monitoring](monitoring.md)). An actor that stops writing keeps fewer
than 64 writes (and anything that was younger than the window at its last
pass) until it writes again. Late uploads of earlier owners that land
after a pass stay until the next pass. Old deploy versions are removed
by `wasmkeep deploy` (see [Deploying](#deploying)).
