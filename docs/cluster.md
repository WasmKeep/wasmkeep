# Running a cluster

> **Pre-alpha.** Ownership, forwarding, replication, failover and deploys
> work and are tested (simulations in `sim/` and multi-node tests), and have run
> against Cloudflare R2 and OVHcloud Object Storage, with nodes killed
> under load and nothing lost
> ([measured](benchmarks.md#three-nodes-against-a-real-bucket)), but not
> yet under production load or against AWS S3. Do not trust a cluster with
> data you care about before the first release (see the [roadmap](../ROADMAP.md)).

Nodes share one S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) that
supports conditional writes (`If-None-Match` / `If-Match`). There is nothing
else to run: no consensus cluster, no coordinator.

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
  `owners/<tenant>/<class>/<name>`, naming the node (and its process)
  that holds it, with a generation of its own for every hold. A node
  writes it when it takes the actor (its first event after waking) and
  when it gives it up (hibernation, a drain, a move, a clean stop): there
  are no renewals while the actor stays in memory. A request that reaches
  a node not holding the actor is forwarded **once** to the node the
  record names, if that node is live. Responses carry `x-wasmkeep-node:
  <id>`. A release that fails is retried in the background; until it
  lands, the record keeps naming the node.
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
- A deleted heartbeat frees nothing: the actors its node held stay held
  until a process starts with that id (its start check waits, then it
  takes them back) or the node is fenced: `wasmkeep cluster fence <node>`
  writes a fence even with no object, and the actors are free 15 s
  later. Node leases, ownership records and markers are never collected;
  `wasmkeep cluster forget <node>` removes only a retired node's heartbeat
  in the format from before node leases, once it is older than 25 s.

## Replication

Each actor's database follows it from node to node (ADR 0007):

- After every event, the owner uploads what the event committed as an LTX
  file under `actors/<tenant>/<class>/<name>/wal/`, and a snapshot every 64
  uploads. A node that takes an actor over rebuilds its database from the
  bucket first, unless its local copy is already current.
- `storage.exec` commits locally; its upload follows when the event ends.
  If the node dies in between, that write can be lost. `storage.exec-durable`
  returns only once the write is in the bucket, uploaded while the node still
  held the actor's lease: an acknowledged durable write survives any node
  failure.
- If an upload fails, the node gives the actor up; the next request restores
  it from the bucket, possibly on another node.
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
  replaces is kept as `<name>.db.replaced` (one generation), never deleted.

**Joining a cluster with existing data:** the first node to touch an actor
decides its history when the bucket is empty. Start the node that holds the
existing databases first, and let it serve each actor once, before adding
nodes. A database replaced by mistake can be recovered from `.replaced`.

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
| `--history-retention-secs` | How long old actor history stays in the bucket before garbage collection may delete it; default 3600, 0 allowed (see "History in the bucket") |
| `--secrets-key FILE` | With `--app`: the key the app's secrets are sealed with (`wasmkeep secrets new-key`); given twice during a key change (see "Secrets in the bucket") |
| `--no-secrets` | With `--app`: run without the app's secrets from the bucket (`--env` only); without it and without a key, a node refuses to start once the app has secrets |

**Node ids must be unique.** A node may take back its own lease at once
(that is how a restarted node recovers its actors quickly), so two processes
sharing an id would be treated as one node and could both run the same
actor. A node therefore checks its id when it starts, before its first
heartbeat and before any lease. It reads `nodes/<id>` every half period for
at least the lease's ttl plus grace (15 s by default), the time any other
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

- Every start waits at least those 15 s before `wasmkeep serve` listens (it
  logs "checking that no other process runs with this node id"), less
  after a clean stop or a fence of the previous process.
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
reconnect.

**Calls between nodes.** `actors.call` reaches an actor wherever it runs.
Nodes prove to each other that a call comes from the cluster with a
signature: HMAC-SHA256, keyed with a random secret the first node stores at
`cluster/secret` in the bucket, over the node the call is for, the method,
path, body, calling actor, chain of waiting actors, a timestamp and a nonce
unique to each send (valid for a minute, and accepted once). Only a signed call's
caller (`x-wasmkeep-caller`) and chain are believed, so a client cannot pass
for an actor or reach an `internal` class, and a call cycle across nodes is
refused instead of deadlocking. The secret itself never travels; a malformed
one in the bucket stops calls between nodes rather than weakening them.

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
  forwarded request or WebSocket upgrade gets a 503 asking the client to
  retry.

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

- **One app per bucket prefix.** Actors are keyed by tenant, class and
  name, so two apps of one tenant on the same prefix would share actors.
  A node refuses to start when a live node on its prefix serves another
  app of its tenant: give each app its own prefix (`s3://bucket/shop`,
  `s3://bucket/admin`).
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
- **`--access` with `--app`** is refused once rules have been deployed
  (the start-up error gives the `wasmkeep deploy` command). Before that it
  still works, so a cluster started with `--access` moves in three steps:
  1. upgrade every node, flags unchanged;
  2. deploy the rules once:
     `wasmkeep deploy --app acme/shop --bucket ... --access monitor=existing`
     (nodes switch to them and log that `--access` is now ignored);
  3. remove `--access` from the nodes' command lines.

  A node following an app that never deployed rules, started without
  `--access`, logs that every class is open.

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
  restarted: restart the nodes after a restore. Deleting `current` by hand
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

NODE  LIVE  AGE    STATE    VERSION  APP        ADDRESS        CLUSTER ADDRESS  TLS
a     yes   979ms  serving  0.0.0    acme/shop  http://a:8080  https://a:7443   pinned
b     yes   2.2s   serving  0.0.0    acme/shop  http://b:8080  -                plain
c     no    14m    serving  0.0.0    acme/shop  http://c:8080  https://c:7443   pinned

APP        DEPLOYED VERSION  SEQ
acme/shop  9cf4de9152d8dd29  1

2 of 3 nodes live; TLS: 1 with a cluster listener, 1 plain; version: 0.0.0
warning: mixed plain and TLS nodes: b still plain while a use the cluster listener; give every node a cluster listener, then drop --cluster-plaintext accept (ADR 0011)
```

- **Nodes** come from their heartbeats (`nodes/<id>`): the WasmKeep version
  each runs, the app it serves, its advertised and cluster addresses, and
  whether it publishes a TLS pin. **State** is whether it takes actors:
  `serving`, or, while it is drained ([ADR 0014](adr/0014-draining-a-node-and-moving-an-actor.md)),
  `draining`, `drained` (no actors left) or `stuck` (actors
  left and no node of the same app to hand them to); `-` from a node that
  predates draining. A node is **live** if its heartbeat was
  written within 10 s (three heartbeats plus the allowed clock skew), the
  rule the nodes themselves use with the default settings. Ages are
  measured on the clock of the machine running the command, so keep it in
  sync too. A heartbeat written `fenced` (a peer took its lease) or
  `released` (a clean stop) is shown as `no (fenced)` or `no (released)`
  whatever its age, and as `lease` in `--json`. A stopped or crashed node
  stays listed, not live, until its heartbeat object is removed from the
  bucket.
- **Apps** come from `apps/<tenant>/<app>/current`: the deployed version and
  its `seq`. Which version each node has picked up is not in the bucket
  (nodes follow within a few seconds).
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
- **reports `drained`** once it holds nothing, and keeps running and
  forwarding until you stop it. You can then stop it with nothing left to
  hand over.

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
wasmkeep actor move acme/room/lobby --to node-c --bucket s3://acme-wasmkeep/prod
```

The target must be a live node: an id no live node has is refused without
`--force`, and a target that is draining, or too old to take moved actors,
gets a warning (the request is written; the owner keeps the actor), as
does one that serves another tenant.

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
every node, upgrade them all, start them all.** Until the first release,
WasmKeep does not support nodes of different versions in one cluster, even
for the length of a rolling restart. The cluster serves nothing between
the last stop and the first start, so plan a short window.

1. `wasmkeep cluster status --bucket ...`: note the nodes it lists as live.
2. Stop every node cleanly (`systemctl stop wasmkeep`, `SIGTERM`). Each
   finishes its requests and releases its actors; a clean stop takes up to
   about 25 s.
3. Wait until `cluster status` exits with 1 (no live node), about 10 s after
   the last stop. A node still listed as live is still running the old
   binary: stop it first. This check does not depend on version strings.
4. Install the same new binary on every node.
5. Start every node, then run `cluster status` again: every node live, one
   version in the summary, no warning.

Going back to the previous binary is the same procedure. Builds made from
source before the first release all report version `0.0.0`, so the version
column cannot tell two of them apart: step 3 is what proves no old node is
left.

**What goes wrong if old and new nodes run together** (each is a reason
for the rule; there may be others nobody has listed):

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

Rolling upgrades of WasmKeep across mixed versions are planned after the
first release ([ROADMAP](../ROADMAP.md)).

## History in the bucket

Every write an actor commits becomes an object in the bucket, and every 64
writes its owner adds a snapshot. After such a snapshot, the owner deletes
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
