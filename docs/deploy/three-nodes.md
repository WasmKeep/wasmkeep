# Three nodes

A WasmKeep cluster: three servers sharing one S3-compatible bucket, behind a
load balancer. Any node can lose its machine; its actors continue on the
others 15 to about 21 s after its last heartbeat, then a restore
([how](../cluster.md#how-it-works)), with no acknowledged durable write
lost ([measured](../benchmarks.md#three-nodes-against-a-real-bucket):
twelve hours and 35 kills on
[one lease per node](../adr/0020-one-lease-per-node.md), writable again
18.2 s after the kill at the median).

Read [Running a cluster](../cluster.md) for how ownership, forwarding and
replication work. This page is the setup. Each node is installed as in
[One server](single-node.md) (binary, user, systemd unit, directories);
only the flags differ.

> **Pre-alpha.** The cluster is tested in simulation, with real processes
> against an S3 API, and against Cloudflare R2 and OVHcloud Object Storage
> with nodes killed under load
> ([measured](../benchmarks.md#three-nodes-against-a-real-bucket)), but not
> yet under production load.

To try a cluster on one machine first, [`deploy/compose/`](../../deploy/compose/README.md)
runs the three nodes below in containers against MinIO, with the same
flags.

```text
                 ┌──────────── load balancer (TLS) ────────────┐
                 ▼                       ▼                     ▼
          node a 10.0.0.11        node b 10.0.0.12      node c 10.0.0.13
                 └─ private network: mutual TLS on port 7443 ──┘
                                         │
                            S3-compatible bucket (shared)
```

## 1. The bucket

Any S3-compatible store that supports conditional writes (`If-None-Match`
and `If-Match` on PUT): AWS S3, Cloudflare R2, OVHcloud Object Storage,
MinIO and others. Each node checks this at start and refuses to run on a
store that ignores them ([Running a cluster](../cluster.md)). Create one
bucket (or a prefix in one) for the cluster, and credentials that can read,
write, list and delete in it.

### Connection settings by provider

Every setting is an `AWS_*` variable in the nodes' environment (the
`EnvironmentFile=` below); the bucket name and an optional prefix go in
`--bucket s3://BUCKET/PREFIX`. Keep the bucket in the same region as the
nodes: a durable write waits for one PUT to it, and a failover for many
reads (54 ms per durable write next to the bucket, 587 ms from a distant
desktop to R2: [measured](../benchmarks.md#three-nodes-against-a-real-bucket)).

| Provider | Settings | Credentials |
| --- | --- | --- |
| **Cloudflare R2** (tested) | `AWS_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com`, `AWS_REGION=auto` | An R2 API token with *Object Read & Write* on this bucket only; its S3 access key id and secret |
| **OVHcloud Object Storage** (tested) | `AWS_ENDPOINT=https://s3.<region>.io.cloud.ovh.net` (for example `s3.bhs.io.cloud.ovh.net`), `AWS_REGION=<region>` (`bhs`) | An S3 user of the Public Cloud project with read and write on this bucket |
| **AWS S3** | `AWS_REGION=<the bucket's region>`; no endpoint | An IAM user or role allowed `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on this bucket |
| **MinIO** (in [`deploy/compose/`](../../deploy/compose/README.md)) | `AWS_ENDPOINT=http://<host>:9000`, `AWS_REGION=us-east-1`, `AWS_ALLOW_HTTP=true` for a plain-HTTP endpoint on a private network | A MinIO user with read and write on this bucket |

"Tested" means the storage, failover and cluster suites and a 30-minute
soak passed against it. Every node also checks conditional writes at
start, whatever the provider. Requests cost money on R2 and S3 (before
[ADR 0020](../adr/0020-one-lease-per-node.md), per-actor lease renewals
were most of them: see the
[benchmarks](../benchmarks.md#three-nodes-against-a-real-bucket); node
leases removed them), none on OVHcloud.

The bucket is the cluster's source of truth and its root of trust: whoever
can write to it can run code on the cluster (deployed components live
there) and impersonate nodes (the inter-node secret lives there). Give its
credentials to the nodes and to whoever deploys, no one else.

## 2. The nodes

Every node needs:

- a **unique node id** (`[a-z0-9-]`): `a`, `b`, `c`. A node checks its id
  at start: it watches `nodes/<id>` in the bucket for up to about 15 s
  (less after a clean stop or a fence) before taking any actor, and exits with an error if another process still
  writes it. Two processes started at the same moment can both pass that
  check; one then finds the other at its next write and exits, but until
  then both may run the same actor. When replacing a machine, stop the old
  process before starting the new one under the same id (see
  [Running a cluster](../cluster.md#flags)).
- an **advertise URL** the other nodes can reach it at, on the private
  network.
- a **cluster listener** (`--cluster-listen` and `--cluster-advertise`):
  a second port, 7443 here, where the nodes reach each other over mutual
  TLS. Nothing to provision: each node makes its own key at start and the
  others learn its fingerprint from the bucket (see
  [Running a cluster](../cluster.md#flags)). Without it a node refuses
  to start, unless given `--cluster-plaintext accept`, which makes nodes
  talk to each other in plain HTTP (a private network only).
- the same `--bucket`, and either the same `--actor` flags on every node or,
  better, `--app` to follow deploys.

On node `a` (`/etc/wasmkeep/env`, mode 600, then `EnvironmentFile=` in the
unit):

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
# AWS_ENDPOINT=...   # R2, OVHcloud, MinIO: see "Connection settings by provider"
RUST_LOG=info
```

```ini
ExecStart=/usr/local/bin/wasmkeep serve \
  --app acme/shop \
  --bucket s3://acme-wasmkeep/prod \
  --node-id a \
  --advertise http://10.0.0.11:8080 \
  --listen 10.0.0.11:8080 \
  --cluster-listen 10.0.0.11:7443 \
  --cluster-advertise https://10.0.0.11:7443 \
  --data-dir /var/lib/wasmkeep/data \
  --cache-dir /var/lib/wasmkeep/cache
```

Nodes `b` and `c` are the same with their own id and address. Listening on
the private address (not `0.0.0.0`) keeps the nodes off the public
internet: only the load balancer should reach port 8080, and only the other
nodes port 7443 (firewall it to 10.0.0.11 to 10.0.0.13; never put it behind
the load balancer). Node traffic is encrypted, and 8080 carries no node
calls at all: plain HTTP between nodes is refused by default
(`--cluster-plaintext refuse`), so a signed call arriving there is
treated as a client's. A cluster that ran in `accept` mode (without
listeners) moves to this in two rounds (see Operating it below); while
any node runs with `--cluster-plaintext accept`, keep 8080 private too.

Clocks should be synchronised (NTP, as on any cloud VM). The lease timings
tolerate a few seconds of skew between nodes.

## 3. Deploy the application

`--app` nodes serve whatever was last deployed for the app, so deploy
before starting them (from any machine with the bucket credentials). This
page uses the counter from `examples/counter` as the app's one actor
class:

```bash
cd examples/counter && cargo build --release --target wasm32-wasip2
wasmkeep deploy --app acme/shop --bucket s3://acme-wasmkeep/prod \
  --actor counter=target/wasm32-wasip2/release/counter.wasm
```

`deploy` compiles and checks every component before uploading anything.
Later deploys work the same way: nodes pick the new version up within a few
seconds and switch each actor at its next event; `--rollback` returns to the
previous version. See [Deploying](../cluster.md#deploying).

## 4. The load balancer

Send traffic to all three nodes, health-checking `GET /_wasmkeep/ready`
(`200 ready`; `503` while the node shuts down, cannot reach the bucket or
runs out of disk, see [Monitoring](../monitoring.md)). A request can go to **any** node: if another node owns the actor, it is
forwarded there (one hop), and responses carry `x-wasmkeep-node` so you can
see where an actor runs. WebSockets work the same way.

No stickiness is needed. Routing by actor path (hashing
`/actor/<class>/<name>`) saves the forwarding hop, and is an optimisation,
not a requirement: see [the front door](front-door.md) for HAProxy and nginx
configurations, health checks, WebSocket timeouts, client addresses and WAF
placement.

## 5. Check it

```bash
curl -i -X POST https://actors.example.com/actor/counter/alice
#   x-wasmkeep-node: b        (whichever node took the actor)
#   1
```

Then stop that node (`systemctl stop wasmkeep` on `b`) and repeat the
request: another node answers at once with `2`, the value preserved (a clean
stop hands the node's actors over). Kill it instead (`kill -9`) to see a
crash: the actor comes back 15 to about 21 s after `b`'s last heartbeat
(10 s lease plus 5 s grace, then a peer fences it), plus a restore from
the bucket. Restart `b`: it rejoins and takes on new actors.

From any machine with the bucket credentials, `wasmkeep cluster status
--bucket s3://acme-wasmkeep/prod` lists the nodes from their heartbeats
(live or not, version, addresses, TLS) and the deployed version of the app;
it exits with 1 if no node is live. See
[Checking a cluster](../cluster.md#checking-a-cluster).

## Operating it

- **Turning on the cluster listener** in a cluster that runs with
  `--cluster-plaintext accept` and no listeners: add the two flags (keep
  `accept` for now) and restart one node at a time; nodes with and
  without it keep talking to each other meanwhile. A restarted node
  reports not ready for a few seconds while its new fingerprint reaches
  the others.
- **Refusing plain HTTP between nodes**, once every node has a cluster
  listener (`wasmkeep cluster status` shows a cluster address and a pin for
  every live node, and no warning about plain nodes): remove
  `--cluster-plaintext accept` and restart one node at a time; each logs
  that it refuses plain HTTP. Never do it while any node lacks a
  listener: a refusing node does not reach that node's actors (503), and
  that node's calls to it are treated as a client's. To roll back, add
  `accept` to every node before removing any listener.
- **Upgrading WasmKeep** (the binary, not your app): upgrade every node
  together, not one at a time. `systemctl stop wasmkeep` on all three, wait
  until `wasmkeep cluster status --bucket s3://acme-wasmkeep/prod` exits
  with 1 (no live node, about 10 s after the last stop), install the new
  binary on all three, start them, and check `cluster status` again: three
  live nodes, one version, no warning. Clients get errors between the last
  stop and the first start. Old and new nodes refuse each other's calls
  and handle alarms differently, so a rolling restart across versions is
  not supported yet: see [Upgrading WasmKeep](../cluster.md#upgrading-wasmkeep).
- **Restarting a node** on the same binary (a configuration change, a host
  reboot): one node at a time is fine. A stopping node finishes its
  in-flight requests, then hands its actors over: the other nodes serve
  them on their next request. WebSockets to that node close and clients
  reconnect.
- **Updating the stateless component** (the file given to `serve` for
  paths outside `/actor/` and `/_wasmkeep/`): it is not part of a deploy.
  Copy the new file to every node and restart them one at a time (see
  [Updating the stateless component](../cluster.md#updating-the-stateless-component)).
- **Adding a node**: start it with a new id and add it to the load
  balancer. An actor in memory on another node stays there, and the new
  node forwards its requests; once it hibernates, its node gives it up,
  and its next request starts it on whichever node that request reaches,
  the new one included (see
  [How it scales](../failover-and-scaling.md#how-it-scales)).
- **Secrets**: keep them in the bucket rather than in each node's `--env`
  files. Create a key once (`wasmkeep secrets new-key --out secrets.key`),
  give it to every node (`--secrets-key /etc/wasmkeep/secrets.key`, mode
  0600), and set values with `wasmkeep secrets set`; nodes pick them up
  within a few seconds (see [Secrets in the bucket](../cluster.md#secrets-in-the-bucket)).
- **Removing or replacing a node**: drain it first, then stop it once it
  is drained:

  ```bash
  wasmkeep cluster drain b --bucket s3://acme-wasmkeep/prod
  wasmkeep cluster status --bucket s3://acme-wasmkeep/prod   # STATE drained
  sudo systemctl stop wasmkeep                               # on node b
  ```

  A drain moves its actors a few at a time while the node keeps
  forwarding, and moves their pending alarms with them, so they fire on
  time (see [Draining a node](../cluster.md#draining-a-node)). Stopping
  without draining also works (`SIGTERM`, as `systemctl stop` sends): its
  actors move at once, and alarms of idle actors wait for the sweep. A
  node that is killed or crashes cannot hand over: its actors move after
  a peer fences its heartbeat (15 to about 21 s after its last write) and
  a restore, and their alarms arrive
  later still ([measured](../benchmarks.md#three-nodes-against-a-real-bucket)).
- **Stop timeouts**: a clean stop can take up to about 29 s (10 s for
  in-flight requests, 10 s for actors to finish their queued events, 3 s
  for the handover to reach the bucket, about 6 s for the node to release
  its own lease). Allow 30 s: `TimeoutStopSec=30` in systemd,
  `--stop-timeout 30` for Docker (its default is 10 s),
  `terminationGracePeriodSeconds: 30` in Kubernetes. A node killed earlier
  is treated as a crash, which is safe, only slower.
- **Losing a node**: nothing to do. Its actors move on their next request;
  writes acknowledged by `exec-durable` are in the bucket, a plain `exec`'s
  last event may be lost (see [Running a cluster](../cluster.md#replication)).
- **Logs**: each node logs to the journal; `RUST_LOG=wasmkeep_server=debug`
  shows leases, forwarding and replication decisions.
- **Backups**: the bucket is the only durable copy. Copy it somewhere the
  nodes cannot write, and restore that copy once before you need it (see
  [Back up and restore a cluster's bucket](backup-and-restore.md)).
- **Bucket growth**: each actor's history older than an hour
  (`--history-retention-secs`) is collected after its periodic snapshot,
  a deleted actor's at once, and `wasmkeep deploy` keeps the current
  version and the five before it (see
  [History in the bucket](../cluster.md#history-in-the-bucket)).

## One region

All nodes should be in one region, close to the bucket: every durable write
and every ownership change is a round trip to it. Multi-region placement is
on the [roadmap](../../ROADMAP.md), not in this release.
