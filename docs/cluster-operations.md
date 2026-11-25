# Checking and operating a cluster

The commands an operator runs against a running cluster: checking it,
verifying its bucket, draining a node, moving or deleting an actor,
deleting an app. Part of [Running a cluster](cluster.md). For a
walk-through, see
[An upgrade, a verify and a trace](deploy/operating.md).

## Checking a cluster

`wasmkeep cluster status` reads the bucket alone, so it works from any
machine with read access to the bucket, whether or not any node answers:

```bash
wasmkeep cluster status --bucket s3://wasmkeep-demo
wasmkeep cluster status --bucket s3://wasmkeep-demo --json   # for scripts
```

```text
Cluster s3://wasmkeep-demo (live: heartbeat within 10.0s)

NODE  LIVE  AGE    STATE    ACTORS  SOCKETS  EV/MIN  FRONT/MIN  FILL   VERSION  APP        ADDRESS        CLUSTER ADDRESS  TLS     WORDS
a     yes   979ms  serving  12      3        840     310        1.2%   0.8.1    acme/shop  http://a:8080  https://a:7443   pinned  take front handoff ends flush rate_limits restore actor-restore recorded-head place
b     yes   2.2s   serving  4       0        95      288        -      0.8.1    acme/shop  http://b:8080  -                plain   take front handoff ends flush rate_limits restore actor-restore recorded-head
c     no    14m    -        -       -        -       -          -      0.8.1    acme/shop  http://c:8080  https://c:7443   pinned  take front handoff ends flush rate_limits restore actor-restore recorded-head

APP        DEPLOYED VERSION  SEQ  ACTORS  FRONT  FORMAT
acme/shop  9cf4de9152d8dd29  1    16      yes    3

2 of 3 nodes live; TLS: 1 with a cluster listener, 1 plain; version: 0.8.1
warning: mixed plain and TLS nodes: b still plain while a use the cluster listener; give every node a cluster listener, then drop --cluster-plaintext accept (ADR 0011)
```

- **Nodes** come from their heartbeats (`nodes/<id>`): the WasmKeep version
  each runs, the capability words it announces (`WORDS`, `words` in
  `--json`: `flush`, `rate_limits`, `restore`, `actor-restore`,
  `recorded-head` and the older ones), the app it serves, its advertised and cluster addresses, and
  whether it publishes a TLS pin. **State** is whether it takes actors:
  `serving`, or, while it is drained ([ADR 0014](adr/0014-draining-a-node-and-moving-an-actor.md)),
  `draining`, `drained` (no actors or WebSockets left) or `stuck` (actors
  or WebSockets left and no node of the same app to hand them to); `-` from a node that
  predates draining, and for a node not live (fenced, released or stale:
  it serves nothing, whatever its last heartbeat said; `state` is `null`
  in `--json`). A node is **live** if its heartbeat was
  written within 10 s (three heartbeats plus the allowed clock skew), the
  rule the nodes themselves use with the default settings. Ages are
  measured on the clock of the machine running the command, so keep it in
  sync too. A heartbeat written `fenced` (a peer took its lease) or
  `released` (a clean stop) is shown as `no (fenced)` or `no (released)`
  whatever its age, and as `lease` in `--json`. A stopped or crashed node
  stays listed, not live, until its heartbeat object is removed from the
  bucket.
- **Load** is what each live node says with every heartbeat: **actors**
  resident in memory, open **WebSockets** (`SOCKETS`), **events per
  minute** (`EV/MIN`: actor events) and **front requests per minute**
  (`FRONT/MIN`, [ADR 0023](adr/0023-the-front.md)) over the last minute
  or more, each `-` for its first minute. A node that only answers
  through the front shows its traffic under `FRONT/MIN`, and `EV/MIN`
  counts only what reaches its actors. It is read as each heartbeat is written, with the state of the
  same moment, so it lags what the node runs by at most one heartbeat
  period (3 s by default): an actor a node just took over shows at its
  next heartbeat. A node that crashed keeps showing its last load until
  its heartbeat is older than the horizon. **FILL** is its app's
  resident fill: actors over actors plus `actor_room`, how many more
  residents the app could have there now (its `max-resident`, its
  `min-resident` floor and the process's limit counted); `-` from a
  version before placement. Placement at wake-up (above) reads the rooms
  of a heartbeat first seen less than three periods ago, to choose a
  candidate, never to decide who holds an actor.
  A node not live shows `-` (a fenced or released heartbeat keeps the
  last load it wrote, which no longer holds), and so does a node of a
  version before 0.4.0 (`FRONT/MIN` also for a node of a version that
  did not publish it yet). In `--json`: `actors`, `sockets`,
  `events_per_min`, `front_per_min`, `actor_room`, `socket_room` and
  `fill` (thousandths), `null` when unknown. Events and
  front requests are counted per process, so nodes sharing one process
  (tests) share one count.
- **Apps** come from `apps/<tenant>/<app>/current`: the deployed version and
  its `seq`, and **front** (`yes` or `no`) from that version's manifest:
  whether the app has a front (`-` when the manifest cannot be read;
  `front`, `true`, `false` or `null`, in `--json`). **Actors** is the sum over the app's live nodes that say
  their load (`-` if none does; `actors` in `--json`): while some of its
  live nodes do not say it (an older version, or a node just started),
  the sum leaves them out. Which version each node has picked up is not
  in the bucket (nodes follow within a few seconds).
- **Warnings**: no live node; live nodes still plain while others use the
  cluster listener (the state [ADR 0011](adr/0011-node-to-node-mtls.md)
  says not to stay in, and not to refuse plain HTTP from); live nodes on
  different WasmKeep versions (see [Upgrading WasmKeep](cluster-upgrades.md#upgrading-wasmkeep));
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

To check the bucket itself rather than the nodes, `wasmkeep cluster
diagnose --bucket s3://wasmkeep-demo` runs WasmKeep's bucket conformance
checks under a scratch prefix and prints a verdict: required behaviour
observed by these checks (exit 0), not safe (1), or no verdict (2). See [Buckets](deploy/buckets.md).

### Verifying a bucket

`wasmkeep cluster verify` checks that what the bucket holds is whole:
every actor can be restored from it, and the records around the actors
read. It only reads. No write is ever sent, so read access is enough,
and it is safe on a running cluster.

```bash
wasmkeep cluster verify --bucket s3://wasmkeep-demo
wasmkeep cluster verify --bucket s3://wasmkeep-demo --deep           # read every history object too
wasmkeep cluster verify --bucket s3://wasmkeep-demo --app acme/shop  # one app
wasmkeep cluster verify --bucket s3://wasmkeep-demo --app acme/shop --actor room/lobby --deep
wasmkeep cluster verify --bucket s3://wasmkeep-demo --sample 100 --deep
wasmkeep cluster verify --bucket s3://wasmkeep-demo --json           # for scripts
```

With `--app acme/shop --deep`, on the small bucket of the test suite
(in memory, hence the time):

```text
Verifying s3://wasmkeep-demo, bodies read
app acme/shop: deployed 7d551a82e742945e (deploy 2), recorded-head format, floor 0.8
  actors: 4 with history, 4 restorable (34 objects, 17 KiB); 4 ownership records, 4 read; 4 read whole
  alarms: 2 entries; lease markers left: 4
nodes: 3 leases
Read 18 objects (400 KiB) and listed 47 keys in 38 requests, 0.0 s.
The bucket is whole.
```

It is a third command beside two that sound alike:

| Command | What it checks | Writes |
| --- | --- | --- |
| `cluster verify` | What the cluster's bucket holds now: deploys, histories, records | Never |
| `cluster diagnose` | What the bucket's API does (conditional writes, listings, clocks), on scratch objects | Under `diagnose/`, deleted afterwards |
| `backup verify` | A backup's own contents, in the backup's bucket ([online archives](deploy/backup-and-restore.md#online-archives)) | A verification mark in the backup |

What it checks:

- **Per app.** `current` parses, the current version's manifest parses,
  and each component, front and configuration it names has the SHA-256
  it names. A static assets index has its digest and every content it
  names is listed with its size. The recorded-head setting and the
  upgrade floor decode. An app with state and no deploy (served with
  `--actor`) is `not deployed`, which is not a problem. A deleted app is
  not checked. An app under `backup restore` (running, or left
  unfinished) is not checked either, and that is reported as not read.
- **Per actor.** A restore plan exists: from the listing, the chain
  from the head back to a snapshot (or to transaction 1) has every
  segment, as a node's restore plans it. Each object on it is at least
  as long as a history object's header and trailer. The ownership record
  decodes. In an app that records heads
  ([the recorded-head format](cluster-upgrades.md#the-recorded-head-ownership-format)), the
  recorded head is reached by that chain, by the rule a restore uses: it
  is on the chain, or the chain's snapshot links back to it, or the
  chain holds an object of a later generation. A version 2 record in an
  app that has not enabled the format is a problem. A key under
  `actors/` that is no actor's history object (a directory marker from a
  sync tool, a stray file) is a problem too: `backup create` and `feed`
  stop at one.
- **Alarms.** Each entry of the alarm index names an actor that has
  history or an ownership record.
- **Files.** The count of stored files and each sweep record of a deleted
  actor's files decode, and a sweep record names the actor its key names.
  Files are listed and counted by epoch. The count the cluster publishes
  lags by up to an hour, so a difference is shown and is not a problem.
- **Nodes.** Each node lease decodes. A heartbeat in a format from before
  node leases is counted, not a problem.
- **Lease markers.** The per-actor markers left under `leases/` from
  before node leases are counted. They are never read or deleted.

What it does not check:

- Secrets (`apps/<tenant>/<app>/secrets/`), the cluster secret, and the
  requests under `moves/`, `deletes/`, `restores/` and `control/`.
- The bodies of files (`blobs/`). They are listed and counted, never
  read, with or without `--deep`: nothing in the bucket says what a
  file should hold.
- Anything below the snapshot a restore starts from. A gap in older
  history, which only `actor export --txid` or `--at` and `backup create
  --history all` read, is not reported.
- A history cut at its end. With the newest segments gone, what is left
  is still a whole chain to an older head, and it passes. It is found
  only in an app that records heads, when the actor's ownership record
  names a head the chain no longer reaches.

`--deep` also reads every object a restore would read (the snapshot and
each segment after it). It checks each file's checksum, that it is the
transaction its key names, and that each follows the one before it by
its database checksum. In an app that records heads it compares the
checksum at the recorded head. It also reads the previous versions a
deploy keeps for rollbacks and every static asset. It does not replay
the pages into a database: `wasmkeep actor export` and `backup verify`
do that. **Without `--deep`, damage inside a history object is not
found.** A missing or misnamed object, or one cut to almost nothing, is.

`--sample N` reads the ownership record (and with `--deep` the history)
of N actors of each app, chosen at random. The listing is still whole,
so every actor's chain is still checked by its key names. `--actor`
checks one actor's history and record, nothing else.

On a running cluster, nodes collect old history and replace records
while the command reads. What looks wrong is listed and read again, up
to three times, before it is reported. Before an app's problems are
reported, its `deleted` and `restoring` marks are read again: an app
deleted or put under restore meanwhile is not checked. A version 2
record is a problem only if the format setting, read again, is still
absent.

A read the bucket refuses (an error, a timeout, access denied) is not a
problem with what the bucket holds. It is sent again, three times with
a pause, and then listed apart, as `not read:` lines and `unread` in
`--json`. So is an actor with more history keys than the listing budget
holds (32 MiB of key names and ETags), which is not checked, and an app
under restore.

A `note:` line (`notes` in `--json`) is neither: it changes neither the
verdict nor the exit code. Today there is one, for the mark a `wasmkeep
cluster forget` left at `nodes/<id>` more than 5 s ago: the command did
not finish, and no node starts under that id until it is run again. The
mark itself is not a node and not a problem.

Each problem is one line with the key it is about, and `check` and
`detail` in `--json`. For a missing segment the key is the one it would
have, with `*` for the one part its neighbours do not tell:

```text
problem: state/acme/shop/actors/room/lobby/wal/0000000000000005-0000000000000001-*.ltx: the segment of transaction 5 (generation 1) is missing from the chain to the head, and no snapshot stands in for it: state/acme/shop/actors/room/lobby/wal/0000000000000006-0000000000000001-0000000000000001.ltx is built on it (history)
```

The last lines say what the run cost: objects read and their bytes, keys
listed, requests and time. A GET is one request; a listing is one, and
one more for every 1,000 keys it returns. Without `--deep` it is about
one small GET per actor plus the listings and the current components.
`--deep` reads the restorable history of every actor once.

The exit code:

- **0**: the bucket is whole, and everything was read.
- **1**: a problem was found, whether or not the run was complete.
- **2**: no problem was found, but there is no verdict, because
  something was not read or not checked: an object the bucket refused
  to give, an actor over the listing budget, an app under restore, a
  listing the bucket refused or that stalled for 30 s, `--app` naming
  an app the prefix holds nothing of, `--actor` in a deleted app, or a
  prefix that holds no app and no node at all (a mistyped prefix is not
  a whole bucket). Also when the command could not start (a bucket URL
  it cannot read).

A run that stops early still prints what it found until then, in text
and in `--json` (`stopped`, and `stopped` in the app whose listing
failed; the other apps and the nodes are still checked). `--json` also
has `whole`, `complete`, `exit_code` and `writes_refused`, which is
always 0. At most 1,000 problems and 1,000 unread objects are printed;
all are counted.

`--deep` reads every history object on a restore's path. On a bucket
with access-based tiering (S3 Intelligent-Tiering and the like), a read
moves an object back to the frequent tier, so a deep run undoes the
tiering of that history. Objects in an archive class that needs a
restore before a GET are reported as not read.

What it keeps in memory:

- The listing of an app's history is one stream, grouped by actor, 16
  actors at once. One actor's group is at most 32 MiB of key names and
  ETags; an actor over that is not checked.
- An actor that looked wrong is listed again alone, and that listing
  has no such limit. Neither has the listing of an app's static assets
  (the contents of every version it keeps).
- With `--deep`, at most 256 MiB of history objects at once, unless one
  object alone is larger. Components are read one at a time, whole.
- 16 bytes per actor with history, up to 4 million actors per app
  (about 100 MiB). Past that, each further ownership record and alarm
  entry costs one listing request instead.

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
  with 1012, and releases their ownership records at once. Their clients
  reconnect through another node; their `on-websocket-close` events do not
  run, since the draining node does not take the room back to run them;
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

Nodes read a drain or an undrain within a heartbeat period (3 s): the
command rewrites `control/changed` after its request, and nodes list the
requests when that object changes. A request written by a CLI of 0.4.1
or before, which does not, is read within a minute
([ADR 0014](adr/0014-draining-a-node-and-moving-an-actor.md), amendment
of 2026-11-14).

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

Within a few seconds the actor's owner hands it to `node-c` (within a
minute for a request written by a CLI of 0.4.1 or before, which does not
rewrite `control/changed`;
[ADR 0017](adr/0017-moving-one-actor.md)):

1. its WebSockets close with 1012 and their close events run;
2. its queued events finish and are uploaded;
3. the lease is released, and from that moment every event for it on the
   old owner (requests, calls, alarms, reconnecting sockets) goes to
   `node-c`;
4. `node-c` takes it: it acquires the lease, restores the actor and arms
   its alarm.

If no node holds the actor, `node-c` takes it itself. A room that
hibernated with sockets open on a node is that node's to move: it records
the move, releases the room's record, closes its sockets with 1012 (their
close events do not run) and asks `node-c` to take it. A name that was
never an actor is not created.

- **The target** must be live, serve the same app, not be draining or
  blocked, and run a version that takes actors (heartbeat line 11, `take`).
  Otherwise the request waits, tried on every heartbeat, until it expires.
- **Not a pin.** Once idle, the actor hibernates on `node-c`, and its next
  event claims it wherever that event lands. A draining node does not act
  on move requests; its drain moves everything.
- **Big rooms.** For a large room, move its pusher so it does not share a
  node with the room; placement does not yet keep them apart. See
  [Place a large room's pusher on another node](designing-for-hot-actors.md#place-a-large-rooms-pusher-on-another-node).
- **Retries.** A move that cannot finish (a socket that will not close, a
  target that does not answer) is tried again on every heartbeat until it
  works or the request expires (5 minutes).

## Seeing where an actor runs

```bash
wasmkeep actor where --app acme/shop room/lobby cart/u-1842 --bucket s3://acme-wasmkeep/prod
```

```text
ACTOR         OWNER       LIVE  SINCE
room/lobby    node-b      yes   14m ago
cart/u-1842   not placed  -     -
```

The actors are named as for `actor move` (one or more `CLASS/NAME`). For
each, the command reads its ownership record and the heartbeats, and prints
the node that owns it, whether that node is live (as `cluster status`
counts it), and how long ago the record was written, which is when that
node took the actor. An actor with no record, or one released by its
owner (idle), is `not placed`: its next event claims it wherever it lands.
Use it to check that two actors, a busy room and the pusher that feeds it,
run on different nodes before and after an `actor move`. `--json` prints
the same for scripts (`actor`, `app`, `placed`, `owner`, `live`,
`since_ms`).

It is read-only: it writes nothing, claims nothing and wakes no actor, and
read access to the bucket is enough. The answer is the bucket's at that
moment; an actor may move right after. The `x-wasmkeep-node` response
header names the node that served one request.

## Deleting one actor

```bash
wasmkeep actor delete --app acme/shop cart/u-1842 --bucket s3://acme-wasmkeep/prod
```

The actor is named as for `actor move`. The command refuses an actor
with nothing in the bucket (a mistyped name: nothing is written), writes
a request under the app's root (`state/<tenant>/<app>/deletes/`), and
waits until the delete is recorded, 60 s by default (`--wait SECS`;
`--no-wait` returns at once). It prints "Deleted" only once a node has
recorded the delete in the bucket; otherwise it exits non-zero and the
cluster keeps trying until the request is an hour old. Running it again is
safe ([ADR 0028](adr/0028-actors-that-end.md)).

Every node of the app reads the requests once per heartbeat (3 s):

- **The node that holds the actor** (resident, or a room that hibernated
  with sockets open on it) closes its WebSockets with **1000 "actor
  deleted"** (not 1012: clients reconnecting at once would recreate an
  open class's actor), lets their close events run, then deletes it after
  the events already queued.
- **If nobody holds it** (hibernated, or its node died), any node serving
  the class acquires it, restores it and deletes it, running none of its
  code. The actor of a node that died is free once that node's lease has
  run out (about ttl plus grace, as for any takeover); `wasmkeep cluster
  fence` makes that sooner.
- **The delete** is the one `delete-self` writes: the database and the
  alarm go, the bucket records an empty database at the head of the
  actor's history, and its older history is erased. A request read twice
  (its key could not be deleted, or two nodes acted) is carried out once:
  the empty database records the request.

The next event to the address starts a new, empty actor; under `existing`
it still counts as existing ([ADR 0010](adr/0010-who-may-create-an-actor.md)).
A node from before this release ignores the requests: during a rolling
upgrade, a delete waits for an upgraded node to hold the actor or find it
free. A delete cannot be undone, except from a bucket that keeps old
versions ([backup and restore](deploy/backup-and-restore.md)).

## Idle expiry

A class may say how long its actors may stay idle before they are
deleted ([ADR 0028](adr/0028-actors-that-end.md), section 4):

Idle means no event: a [file](files.md) downloaded through a link every
day runs no code and does not keep its actor alive. The actor's files
are deleted with it.

```toml
[classes.carts]
component = "carts.wasm"
expire-after-idle = "7d"
```

or, with flags, `wasmkeep deploy --app acme/shop --expire-after-idle
carts=7d` (per class, kept by later deploys that do not name it;
`carts=` removes it). The limit is a whole number and one unit, `s`,
`m`, `h` or `d`, at least one hour. It is part of the version, so a
rollback restores the limit the version had. A version with a limit uses
manifest format 4: the deploy is refused while a live node of the app
runs a version from before (its heartbeat lacks the word `ends`).

- **Idle** means no event ran the actor's code: no request, call, alarm,
  WebSocket open, message or close. An actor with a **pending alarm** is
  never expired (the alarm is activity to come), nor one with an **open
  WebSocket** (its node holds it, or keeps its record parked).
- **Who collects.** One node of the app: the live node with the lowest id
  that serves and announces `ends`, once every 10 minutes. Per class with
  a limit, it lists `owners/<class>/` once (an actor's ownership record is
  written when it wakes and when it hibernates, so its last-modified time
  says how long it has been idle), and the app's alarm index once. A pass
  deletes at most 100 actors, four at a time, oldest first; another node
  takes over once that one stops heartbeating.
- **The delete** runs none of the actor's code. The node acquires the
  actor only if its record still names nobody and is exactly the version
  listed, restores it, checks its database for an alarm, then deletes it
  as `delete-self` does (the database and its history in the bucket, an
  empty database recorded at the head).
- **An event arriving meanwhile** either wakes the actor before the
  expiry acquires it (the record changes, nothing is deleted) or is
  forwarded to the expiring node and queued after it, and finds a new,
  empty actor, as any event after the expiry would.
- **One node, no cluster**: nothing expires (there are no ownership
  records to list). A name longer than about 200 bytes once encoded (its
  record key holds a hash) is never expired.

`wasmkeep_actor_deletes_total{how="expired"}`,
`wasmkeep_expiry_candidates_total` and `wasmkeep_expiry_kept_total{reason}`
show what the passes do ([monitoring](monitoring.md)).

## Deleting an app

```bash
wasmkeep app delete --app acme/legacy --bucket s3://acme-wasmkeep/prod
```

([ADR 0028](adr/0028-actors-that-end.md), section 5.)

1. **The tombstone first**: `apps/<tenant>/<app>/deleted`, written once,
   holding who and when, as soon as the command has checked that the app
   exists and no restore holds it (a few reads, whatever the app's size). From then `deploy` (and `--rollback`), `secrets
   set`, `actor move` and `actor delete` refuse the app, and a node
   refuses to start serving it. A node serving it reads the tombstone's
   key every 3 s, also while it backs off from a deploy it could not
   apply, and so sees it within about 3 s of the bucket answering it: it
   stops taking events, closes the app's
   WebSockets with **1001 "app deleted"**, stops its alarms, releases its
   actors and its node lease, removes its local files of the app, and
   `serve` exits with an error. No node acquires an actor of an app with a
   tombstone, nor before it has checked for one since its lease was valid.
2. **Then the data**, once no live node of the app is left: the command
   waits up to `--wait SECS` (60) for them, then removes every key under
   `state/<tenant>/<app>/` and `apps/<tenant>/<app>/` but the tombstone:
   every actor's history, the ownership records, the alarm index, the
   requests, every deployed version and the sealed secrets. First it
   fences any node lease of the app still written live (a node paused or
   cut off past its horizon), so that node can never act again. It lists
   again until a listing finds nothing at least 40 s after the app's
   nodes last wrote (a write sent before a node stopped may land late).
   The secrets key, kept outside the bucket, is yours to destroy.
3. **The tombstone stays.** `wasmkeep deploy --app acme/legacy --recreate
   ...` uses the name again: it removes the tombstone only once nothing
   else is left under the two prefixes, and not within 40 s of the
   delete. A node whose data directory still
   holds files of the app from before the delete (it was down throughout)
   removes them before it serves the new app.

The command refuses before writing anything if a live node of the app
runs a version that ignores a tombstone (its heartbeat lacks `ends`), and
the implicit app `default/default` (its nodes do not follow deploys).
Nodes still live after the wait: it stops, the tombstone written and
nothing removed; run it again (it goes on where it stopped). `--force`
removes the data anyway: only after `wasmkeep cluster fence` of each node
still listed. A delete cannot be undone, except from a bucket that keeps
old versions ([backup and restore](deploy/backup-and-restore.md)).
