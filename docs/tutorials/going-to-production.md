# Going to production

You have a three-node cluster that works. This page is the checklist that
takes it to production: the bucket, the machines, encryption between
nodes and for clients, secrets, deploys and rollbacks, monitoring,
backups, upgrades and maintenance, and what to watch the first week. Do
each step and check it before the next. Each step is short and links to
the page that covers it in depth.

This follows [From one node to three](one-node-to-three.md). Applies to:
WasmKeep built from `main` (the release after `0.3.0`). Steps or flags
that `0.3.0` does not have are marked **main only**. A build from a git
checkout of `main` reports the release it follows and its commit, such as
`0.3.0+4f1c2a9e7b3d` (`wasmkeep --version`, and the VERSION column of
`cluster status`); the release reports `0.3.0` alone.

> **Pre-alpha.** The cluster is tested in simulation, with real processes
> against an S3 API, and against real buckets with nodes killed under load
> ([measured](../benchmarks.md#three-nodes-against-a-real-bucket)), but
> not yet under production load. Read the [security model](../security.md)
> and its known gaps before you put data in it.

## 0. A practice cluster (optional)

Every command on this page that runs against the cluster can be tried on
your machine first, against the local bucket of the previous tutorial.
Do its steps 1 to 3 (the server, the counter and the bucket, with the
`AWS_*` variables set), stopping before the deploy. Then, from the
clone's directory, make a working directory with the counter, a secrets
key and a first deploy:

<!-- test: practice-setup -->
```bash
mkdir -p .wasmkeep/prod
cp examples/counter/target/wasm32-wasip2/release/counter.wasm .wasmkeep/prod/
cd .wasmkeep/prod
export BUCKET=s3://demo APP=demo/shop
wasmkeep secrets new-key --out secrets.key
wasmkeep deploy --app "$APP" --bucket "$BUCKET" --actor counter=counter.wasm
```

Start the three nodes with the two flags a production node also gets: a
metrics page and the secrets key (steps 5 and 7 say why):

<!-- test: practice-nodes -->
```bash
i=0
for node in a b c; do
  i=$((i + 1))
  wasmkeep serve --app "$APP" --bucket "$BUCKET" --node-id $node \
    --data-dir $node --listen 127.0.0.1:808$i --advertise http://127.0.0.1:808$i \
    --cluster-listen 127.0.0.1:748$i --cluster-advertise https://127.0.0.1:748$i \
    --metrics-listen 127.0.0.1:910$((i - 1)) --secrets-key secrets.key \
    > $node.log 2>&1 &
  echo $! > $node.pid
done
for port in 8081 8082 8083; do
  until curl -sf -o /dev/null localhost:$port/_wasmkeep/ready; do sleep 1; done
done
curl -X POST localhost:8081/actor/counter/alice
# 1
```

On the real cluster, set the two variables to its values instead, in the
shell where you run `wasmkeep` with the bucket's credentials:

```bash
export BUCKET=s3://acme-wasmkeep/prod APP=acme/shop
```

## 1. The bucket

Everything the cluster keeps lives in one bucket, so it is the first
thing to get right:

- [ ] **A store that keeps WasmKeep's guarantees**: conditional writes
  above all ([what WasmKeep needs](../deploy/buckets.md#what-wasmkeep-needs-from-a-bucket),
  and the [providers checked](../deploy/buckets.md#providers-checked)
  so far).
- [ ] **In the same region as the nodes**: every durable write and every
  change of owner is a round trip to it
  ([one region](../deploy/three-nodes.md#one-region)).
- [ ] **Credentials for the nodes** that read, write, list and delete
  under the cluster's prefix, and nothing else; set as `AWS_*` variables
  ([by provider](../deploy/three-nodes.md#connection-settings-by-provider)).
- [ ] **It passes `cluster diagnose`** (**main only**), run from one of
  the nodes with their credentials:

<!-- test: diagnose -->
```bash
wasmkeep cluster diagnose --bucket "$BUCKET"
```

It writes only under a scratch prefix, which it empties again, so it is
safe beside a running cluster. The last line is the verdict, and the
exit code says the same:

```text
...
pass   clock-skew            bucket minus this machine between -0.1 s and +0.2 s (nodes tolerate 1.0 s)
pass   list-paging           1001 of 1001 keys listed

cleanup: s3://demo/diagnose/run-1794641388473-1943513/ is empty again
safe for WasmKeep
```

Anything but `safe for WasmKeep` (exit 0) stops here: `not safe` (1) is a
bucket that breaks a guarantee, and no verdict (2) is a bucket that did
not answer. What each check means:
[Running `cluster diagnose`](../deploy/buckets.md#running-cluster-diagnose).
Run it again from another node: the clock check also tests that
machine's clock.

## 2. Where the nodes run

[Where to run it](../deploy/where-to-run.md) compares servers, VMs and
containers, and [Three nodes](../deploy/three-nodes.md#2-the-nodes) has
the systemd unit and the flags. Check each machine against them:

- [ ] A real local disk for `--data-dir` (no overlay or network file
  system), a high open-files limit, no `LimitAS` and no
  `MemoryDenyWriteExecute`
  ([rules that apply everywhere](../deploy/where-to-run.md#rules-that-apply-everywhere)).
- [ ] One `--node-id` per machine, kept across restarts and reinstalls.
- [ ] Clocks synchronised (NTP): the leases are measured on each node's
  clock and tolerate up to 1 s of skew, and `cluster diagnose` reports
  the skew it found.
- [ ] A stop timeout of 30 s (`TimeoutStopSec=30`), so a clean stop can
  hand its actors over ([stop timeouts](../deploy/three-nodes.md#operating-it)).
- [ ] A private network between the nodes: only the load balancer
  reaches the client port (`--listen`), only the other nodes reach the
  cluster port.

## 3. Encryption between nodes

Nodes talk to each other over mutual TLS on their cluster port
(`--cluster-listen` and `--cluster-advertise`), and refuse plain HTTP
between nodes by default. There is nothing to provision: each node makes
its key at start and publishes its certificate's fingerprint in the
bucket ([how](../cluster.md#flags)). Firewall the cluster port to the
other nodes, and never put it behind the load balancer.

- [ ] Every node pinned. From any machine with the bucket's credentials:

<!-- test: status -->
```bash
wasmkeep cluster status --bucket "$BUCKET"
```

```text
Cluster s3://demo (live: heartbeat within 10.0s)

NODE  LIVE  AGE   STATE    ACTORS  SOCKETS  EV/MIN  FRONT/MIN  VERSION             APP        ADDRESS                CLUSTER ADDRESS         TLS
a     yes   2.0s  serving  1       0        -       -          0.3.0+4f1c2a9e7b3d  demo/shop  http://127.0.0.1:8081  https://127.0.0.1:7481  pinned
b     yes   2.0s  serving  0       0        -       -          0.3.0+4f1c2a9e7b3d  demo/shop  http://127.0.0.1:8082  https://127.0.0.1:7482  pinned
c     yes   2.0s  serving  0       0        -       -          0.3.0+4f1c2a9e7b3d  demo/shop  http://127.0.0.1:8083  https://127.0.0.1:7483  pinned

APP        DEPLOYED VERSION  SEQ  ACTORS  FRONT
demo/shop  7973128b9d723db8  1    1       no

3 of 3 nodes live; TLS: 3 with a cluster listener, 0 plain; version: 0.3.0+4f1c2a9e7b3d
```

Each node is live, has a `CLUSTER ADDRESS` and shows `pinned` under TLS;
the summary counts every node with a cluster listener and none plain,
and no `warning:` line follows. A node started with `--cluster-plaintext
accept` and no listener shows `plain`, with a warning: move such a
cluster to TLS in two rounds, as
[Operating it](../deploy/three-nodes.md#operating-it) says. (The ACTORS,
SOCKETS, EV/MIN, FRONT/MIN and FRONT columns are **main only**.)

## 4. The front door

Clients reach the cluster through a load balancer that ends TLS and
sends plain HTTP to any node's `--listen` port.
[The front door](../deploy/front-door.md) has HAProxy, nginx and Caddy
configurations; whichever you use, check:

- [ ] **TLS for clients** at the load balancer, with a certificate it
  renews.
- [ ] **Health checks on `GET /_wasmkeep/ready`**, not `/_wasmkeep/health`
  ([health and readiness](../monitoring.md#health-and-readiness)).
- [ ] **A server timeout above the failover wait** (60 s or more for a
  node's answer) and an idle timeout above 30 s for WebSockets
  ([why](../deploy/front-door.md)).
- [ ] **The client's `Host` and the scheme passed on.** Nodes see the
  load balancer's plain HTTP request: without these headers, an actor or
  a front building a link or checking an origin sees the upstream's name
  and `http`. With nginx, in the `location` of
  [the front door's configuration](../deploy/front-door.md#route-by-actor-recommended):

  ```nginx
  proxy_set_header Host $host;                       # the client's, not the upstream's name
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_set_header X-Forwarded-For $remote_addr;     # replace, never append
  ```

  The SDKs' `host()` and `scheme()` helpers (**main only**) read them:
  `scheme()` is `https` when the first `X-Forwarded-Proto` value is.
  `X-Forwarded-For` is the [client address](../deploy/front-door.md#client-addresses),
  which the load balancer must replace, never append to.
- [ ] **A WAF or rate limits**, if you want them, in front of the load
  balancer ([WAF and rate limits](../deploy/front-door.md#waf-and-rate-limits)).
- [ ] **It answers through the load balancer**, from outside:

  ```bash
  curl -si -X POST https://actors.example.com/actor/counter/alice | grep -iE '^HTTP|^x-wasmkeep-node'
  ```

## 5. Secrets

Keep secrets in the bucket, sealed with a key the nodes hold, rather
than in each node's `--env` file
([Secrets in the bucket](../cluster.md#secrets-in-the-bucket)):

- [ ] **One key**, made once (`wasmkeep secrets new-key --out
  secrets.key`, as in step 0), copied to every node (mode 0600) and kept
  with the bucket's credentials. Losing it means setting every secret
  again ([losing the key](../cluster.md#secrets-in-the-bucket)).
- [ ] **Every node started with `--secrets-key`**. Once the app has
  secrets, a node without the key refuses to start.
- [ ] **Each value set**, from a file or a pipe rather than the command
  line (one trailing newline is removed). For the practice cluster, make
  a value first:

<!-- test: secret-files -->
```bash
echo practice-token > admin-token.txt
echo practice-session-key > session-key.txt
```

<!-- test: secrets-set -->
```bash
wasmkeep secrets set --bucket "$BUCKET" --app "$APP" --key secrets.key \
  --class counter ADMIN_TOKEN < admin-token.txt
wasmkeep secrets set --bucket "$BUCKET" --app "$APP" --key secrets.key \
  --front SESSION_KEY < session-key.txt
wasmkeep secrets list --bucket "$BUCKET" --app "$APP" --key secrets.key
```

```text
Set 1 secret(s) for demo/shop: seq 1.
Set 1 secret(s) for demo/shop: seq 2.
demo/shop: seq 2
  _front (the front): SESSION_KEY
  counter: ADMIN_TOKEN
```

`--class` sets a class's secret, `--front` (**main only**) the front's.
Nodes pick a change up within a few seconds, with no deploy and no
restart. `secrets list` shows names, never values. Then delete the files
the values came from.

## 6. Deploys and rollbacks

Deploy your app with `wasmkeep deploy`, from one place (a CI job or one
operator's machine), never by copying components to the nodes
([Deploying](../cluster.md#deploying)). Each deploy is a new version that
every node follows within a few seconds, switching each actor at its
next event; `deploy` checks every component before it uploads anything.

- [ ] **The values a class needs, required.** With `--requires`, a class
  whose secret is not set refuses its requests with 503 instead of
  running without it:

<!-- test: deploy -->
```bash
wasmkeep deploy --app "$APP" --bucket "$BUCKET" --actor counter=counter.wasm \
  --requires counter=ADMIN_TOKEN
```

```text
  requires counter: ADMIN_TOKEN now required
demo/shop: version 1ce5b445d3fa0b38 is current (deploy 2)
```

- [ ] **A rollback rehearsed**, before you need one. It makes the
  previous version current again, as a new deploy, with its access
  rules, configuration and front:

<!-- test: rollback -->
```bash
wasmkeep deploy --app "$APP" --bucket "$BUCKET" --rollback
```

```text
rolling back demo/shop to 7973128b9d723db8:
  requires counter: ADMIN_TOKEN no longer required
rolled back demo/shop: 1ce5b445d3fa0b38 -> 7973128b9d723db8 (deploy 3)
```

The version rolled back to is the first deploy's again, at a new `seq`
(`cluster status` shows both in its app table). `deploy` keeps the current version
and the five before it, so you can roll back that many times in a row.

## 7. Monitoring

- [ ] **Metrics on a private address**: each node with
  `--metrics-listen` on loopback or the private network, reached only by
  your Prometheus, in a scrape job named `wasmkeep`
  ([Metrics](../monitoring.md#metrics)). On a node:

<!-- test: metrics -->
```bash
curl -s 127.0.0.1:9100/metrics | grep -E '^wasmkeep_(ready|admission_refused_total|bucket_requests_total)'
```

```text
wasmkeep_ready 1
...
wasmkeep_bucket_requests_total{operation="get",status="ok"} 13
wasmkeep_bucket_requests_total{operation="get",status="not_found"} 4
wasmkeep_bucket_requests_total{operation="get",status="precondition"} 0
wasmkeep_bucket_requests_total{operation="get",status="error"} 0
...
wasmkeep_admission_refused_total{reason="access"} 0
wasmkeep_admission_refused_total{reason="capacity"} 0
wasmkeep_admission_refused_total{reason="overloaded"} 0
wasmkeep_admission_refused_total{reason="unavailable"} 0
wasmkeep_admission_refused_total{reason="wrong_app"} 0
```

  Counters with a fixed set of labels are on the page from the start, at
  0 (**main only**; `0.3.0` adds a series at its first count):
  `wasmkeep_bucket_requests_total{status="error"}`,
  `wasmkeep_failover_waits_total{kind,outcome}`, and
  `wasmkeep_not_ready{reason}` with one series per reason, 1 for the
  reason a node is not ready. Only series with a `class` label wait for
  that class's first event.
- [ ] **The alert rules loaded**: [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml),
  with `rule_files:` in `prometheus.yml`, checked with `promtool check
  rules` ([alerts worth having](../monitoring.md#alerts-worth-having)).
- [ ] **The dashboard imported**: [`deploy/grafana/wasmkeep.json`](../../deploy/grafana/wasmkeep.json)
  ([Grafana dashboard](../monitoring.md#grafana-dashboard)).
- [ ] **Someone paged** by the alerts that matter first:

| Watch | What it means | In `alerts.yml` |
| --- | --- | --- |
| Not-ready nodes: `wasmkeep_ready`, `wasmkeep_not_ready{reason}`, `up` | A node out of the load balancer: shutting down, cut off from the bucket, out of disk, or down | `WasmKeepNodeNotReady`, `WasmKeepNodeDown` |
| Bucket errors: `wasmkeep_bucket_requests_total{status="error"}`, `wasmkeep_node_lease_failures_total` | The bucket refusing or not answering; two lease writes failing in a row and a node drops its actors | `WasmKeepBucketErrors`, `WasmKeepNodeLeaseWriteFailing` |
| Failover waits: `wasmkeep_failover_waits_total{outcome}` | Requests kept while a node died; `expired` and `over_cap` are callers who got a 503 ([what each outcome means](../monitoring.md#metrics)) | `WasmKeepFailoverWaitsFailed` (**main only**) |
| Refused admissions: `wasmkeep_admission_refused_total{reason}` | Requests refused before they ran: `capacity` and `overloaded` (the node is full), `unavailable` (a required value not set, secrets the node cannot open), `wrong_app` (**main only**) | `WasmKeepAdmissionRefused` (5 minutes of `capacity`, `overloaded` or `wrong_app`), `WasmKeepAdmissionUnavailable` (15 minutes of `unavailable`, which ticks briefly while nodes catch up with a deploy or a secrets change) (**main only**) |

  Thresholds and durations are starting points: adjust them to your
  traffic in the file. The rules also fire on fences and failovers (`WasmKeepNodeFenced`,
  `WasmKeepFailover`), slow durable writes and failing events. The
  dashboard charts the last two rows in its "Failover waits and
  refusals" row (**main only**).
- [ ] **An alert seen once**: stop one node (`systemctl stop
  wasmkeep`) at a quiet hour, check that `WasmKeepNodeDown` reaches
  whoever is paged after a minute (a stopped node exports nothing, so
  this is the rule that sees it), then start it again.

## 8. Backups

The bucket is the only durable copy: a node's disk is a cache of it.
WasmKeep has no backup command yet; [Back up and restore a cluster's
bucket](../deploy/backup-and-restore.md) is the guide.

- [ ] **A copy made with the cluster stopped**, the only kind established
  as restorable so far
  ([with the cluster stopped](../deploy/backup-and-restore.md#with-the-cluster-stopped-restorable);
  a copy of a running cluster is
  [not established](../deploy/backup-and-restore.md#with-the-cluster-running-not-established)),
  kept in another account or at another provider, with credentials the
  nodes do not have.
- [ ] **The secrets key kept with it**, apart from the bucket: the copy's
  secrets are sealed with it.
- [ ] **A restore drill done**, on a scratch bucket, before you need it
  and after every upgrade
  ([a restore drill](../deploy/backup-and-restore.md#a-restore-drill)).
  Write down how long it took: that is your restore time.

## 9. Upgrades

Two kinds, with different rules:

- **Your app**: `wasmkeep deploy`, step 6. It rolls from node to node
  with no stop.
- **WasmKeep itself** (the `wasmkeep` binary): read the release's notes
  first. Its "Upgrading" section, in the [changelog](../../CHANGELOG.md)
  and the GitHub release, says whether its upgrade is rolling (one node
  at a time) or stops the cluster. So far every upgrade stops it: stop
  every node, wait until `cluster status` exits with 1, install the new
  binary everywhere, start them all
  ([Upgrading WasmKeep](../cluster.md#upgrading-wasmkeep)). `0.4.0` also
  migrates the bucket once, offline:
  [Upgrade a cluster to 0.4.0](../deploy/upgrading-to-0.4.md).

- [ ] **A maintenance window** planned for each upgrade that stops the
  cluster, deploys and secret changes frozen during it, and a backup made
  while it is stopped.
- [ ] **The rollback read** before you start: it is not always possible
  (the 0.4.0 migration is one-way after the first start).

## 10. Maintenance on one node

To work on a machine (a kernel update, a resize, a replacement), drain
its node first: it hands its actors to the others and keeps forwarding
requests ([Draining a node](../cluster.md#draining-a-node)).

<!-- test: drain -->
```bash
wasmkeep cluster drain a --bucket "$BUCKET"
until wasmkeep cluster status --bucket "$BUCKET" | grep -q '^a .* drained '; do sleep 1; done
```

```text
Asked a to drain; it reports `draining`, then `drained` once its actors are handed over (a node with few actors may go straight to `drained`; see `wasmkeep cluster status`).
```

Once it reports `drained`, stop it (`sudo systemctl stop wasmkeep` on
that machine), do the work, start it, and let it take actors again:

<!-- test: undrain -->
```bash
wasmkeep cluster undrain a --bucket "$BUCKET"
```

```text
Withdrew the drain request for a.
```

Within a few seconds `cluster status` shows it `serving`. A drained node
still reports ready, so the load balancer keeps sending it requests,
which it forwards. Drain one node at a time: a node with nowhere to hand
its actors reports `stuck`.

## 11. Clean up the practice cluster

Stop the nodes and the bucket, and remove what the two tutorials wrote:

<!-- test: cleanup -->
```bash
kill $(cat a.pid b.pid c.pid) $(cat ../cluster/moto.pid)
wait
cd ../..
rm -rf .wasmkeep/prod .wasmkeep/cluster
```

## The first week

- **`cluster status` once a day**, or from cron: every node live, one
  version, no warning. It exits with 1 when no node is live.
- **Durable write latency** (`wasmkeep_durable_write_seconds`): your
  bucket's health as actors feel it, and the alert on its p99 says when
  it degrades.
- **Bucket requests** (`wasmkeep_bucket_requests_total` by operation):
  compare them with the bucket's bill after a few days.
- **Bucket growth**: old history is collected after each actor's
  snapshot, and deploys keep six versions
  ([History in the bucket](../cluster.md#history-in-the-bucket));
  steadily rising `wasmkeep_history_collect_failures_total` means the
  bucket refuses deletes.
- **Resident actors, open files and memory** against what each node
  holds ([open files](../monitoring.md#open-files),
  [memory](../monitoring.md#memory)): `capacity` refusals mean a node is
  full.
- **Failed events by class** (`wasmkeep_events_total{outcome="failed"}`)
  after each deploy, and the node logs for what actors print
  ([What actors print](../monitoring.md#what-actors-print)).
- **Any failover** (`wasmkeep_failovers_total`, `wasmkeep_node_fences_total`)
  outside a planned stop: a node that died or lost the bucket. Find out
  why in its log.
- **The restore drill**, if you have not done it yet.

## Next

- [Running a cluster](../cluster.md): every flag, deploys, secrets,
  checking, draining and upgrading.
- [When a node dies, and how it scales](../failover-and-scaling.md): what
  callers get during a failover, and adding nodes.
- [Security model](../security.md): who can do what, and the known gaps.
