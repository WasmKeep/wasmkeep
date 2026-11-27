# The node file

`wasmkeep-node.toml` describes a node process: which apps it serves and
how ([ADR 0038](../adr/0038-several-apps-per-process.md)). It is the
operator's file, separate from an app's [project file](../project-file.md):
the project file says what an app is, the node file says which apps this
process runs and on which ports.

```sh
wasmkeep serve --bucket s3://my-bucket/prod --node-file /etc/wasmkeep/wasmkeep-node.toml
```

`--node-file` replaces the flags it describes (`--app`, `--node-id`,
`--advertise`, `--listen`, the cluster and metrics listeners, the data
directory, the per-app settings below): giving one of them beside it is an
error. Process flags it does not describe (`--bucket`, `--max-instances`,
`--cache-dir`, `--idle-timeout-secs`, `--cluster-plaintext`, ...) stay on
the command line. The file is read once, at start; to add, remove or change
an app, edit it and restart the process.

`serve --app TENANT/APP` and its flags keep working unchanged: they are a
one-entry node file with the same meaning.

## Example

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
metrics-hosts = ["node1.internal"]  # names scrapers use, besides addresses

[limits]
max-sockets-per-node = 50000
sqlite-heap-bytes = 4294967296      # SQLite's heap for the process

[[app]]
name = "acme/shop"
node-id = "shop-1"
hostnames = ["shop.example.com"]
secrets-key = ["keys/shop.key"]
max-sockets = 20000
min-resident = 200
max-resident = 800
max-front-requests = 32
front-cpu-budget-ms = 50
allow-outbound = ["10.0.0.0/8"]
allow-outbound-port = [587]
outbound-ca = ["ca/internal.pem"]
front-env = ["env/shop-front.env"]

[app.env]
cart = ["env/cart.env"]             # CLASS = files, later files win

[app.access]
cart = "existing"

[[app]]
name = "acme/blog"
node-id = "blog-1"
hostnames = ["blog.example.com", "www.blog.example.com"]
data-dir = "data/blog"              # optional: another data directory
no-secrets = true
```

Relative paths are read from the file's directory.

## Fields

`[process]`

| Field | Meaning |
| --- | --- |
| `id` | The process's name: its cluster certificate is named for it, and each of its node ids publishes `process=<id>` in its heartbeat. |
| `data-dir` | Every app's data directory unless its entry names its own (default `.wasmkeep/data`, as `--data-dir`). |
| `placement` | `false` turns placement at wake-up off for every app of the process, as `--no-placement` ([ADR 0041](../adr/0041-placement-at-wake-up.md)): its node ids keep every actor they wake and do not announce `place`. Default `true`. |
| `rebalance` | `false` stops spread actors (a big room's edges) being moved to their home node after a node rejoins, for every app of the process, as `--no-rebalance` ([ADR 0040, section 14](../adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md#amendment-2026-11-26-edges-return-home)). Default `true`; off anyway with `placement = false`. New in 0.10.0: a 0.9 `serve` refuses the key, so remove it before going back. |
| `idle-compaction` | `false` turns idle compaction off for every app of the process, as `--no-idle-compaction` ([ADR 0042](../adr/0042-compacting-an-idle-actors-history.md)): no snapshot at the head when an actor hibernates, and no idle pass a retention window later. Default `true`. |

`[listen]`: `public`, `public-advertise`, `cluster`, `cluster-advertise`,
`metrics` and `metrics-hosts`, as `--listen`, `--advertise`,
`--cluster-listen`, `--cluster-advertise`, `--metrics-listen` and
`--metrics-host` (a list: the names the metrics page answers to besides
IP addresses and `localhost`, see [monitoring](../monitoring.md#metrics)).
`cluster` and `cluster-advertise` go together.

`[limits]`: `max-sockets-per-node`, as `--max-sockets-per-node`, and
`sqlite-heap-bytes`, as `--sqlite-heap-bytes`: SQLite's heap for the whole
process, temporary data included, in bytes (default 4 GiB, at least
64 MiB; past it a statement that needs more fails with `quota-exceeded`,
see [security](../security.md)). Both are the process's, shared by every
app of the file.

`[[app]]`, one per app:

| Field | As the flag |
| --- | --- |
| `name` | `--app TENANT/APP` |
| `node-id` | `--node-id`: the app's logical node id, kept as it is today |
| `hostnames` | (none): the hosts that reach this app on the public port |
| `secrets-key`, `no-secrets` | `--secrets-key` (one or two files), `--no-secrets` |
| `env` (table by class), `front-env` | `--env CLASS=FILE`, `--front-env` |
| `access` (table by class) | `--access CLASS=MODE`, until access rules are deployed |
| `allow-outbound`, `allow-outbound-port`, `outbound-ca` | the egress flags |
| `no-alarms` | `--no-alarms` |
| `max-front-requests`, `front-cpu-budget-ms` | the front limits |
| `max-sockets` | `--max-sockets-per-app` |
| `min-resident`, `max-resident` | (none): the app's floor and cap in the process's resident pool |
| `data-dir` | `--data-dir`, for this app alone |
| `blob-link-base` | `--blob-link-base`: where the app's node links to [files](../files.md) point, `https://HOST[:PORT]` (default `https://` and the first hostname, else `public-advertise`) |
| `max-app-blob-bytes` | `--max-app-blob-bytes`: the app's stored-bytes limit for files, at least 1, none by default; enforced with an hour's lag ([files](../files.md#limits)) |

`--max-blob-bytes`, `--blob-links` and `--blob-link-endpoint` stay on
the command line: they are the process's, shared by every app.

## Several apps in one process

A file with more than one `[[app]]`, or with `hostnames`, is served as
several apps behind the process's three ports:

- **The public port routes by hostname only.** Exactly one `Host` is
  required (400 on HTTP/1.1 otherwise; 421 on HTTP/1.0 without Host;
  400 when an absolute-form target names another authority); an unknown
  host answers **421**, and so does a second host on
  one connection; `Forwarded` and `X-Forwarded-Host` are ignored. Every
  `x-wasmkeep-*` request header is removed: on this port every request is a
  client's. `/_wasmkeep/health` is the process's; an app's readiness is
  asked with one of its hostnames.
- **The cluster port routes by the app a hop names,** which that app's
  runtime checks against the hop's signature, its destination and its
  sender. A hop without a node's proof answers 503.
- **Each app keeps its node id, its lease, its state and its restarts.** A
  verdict on one app's node id (another process writing it, a fence, a
  release) stops that app's runtime alone; its hostnames answer 503 with
  `Retry-After`, and it starts again in place, through the start check, 1 s
  later, doubling to 60 s while it keeps failing. A deleted app's hostnames
  answer 503 without `Retry-After`.
- **Budgets are shared by a floor rule.** Residents (a pool sized from
  `--max-instances`), public connections, front instance reservations,
  batched uploads and restore reads: each app has an equal part, half of
  it a floor never lent; an app may use any capacity outside the other
  apps' unused floors, but takes nothing above its part while an app below
  its own waits. `min-resident` sets an app's resident floor.

Health checks on the public port must send one of the app's configured
hostnames, for example `Host: shop.example.com`, with
`GET /_wasmkeep/ready`. This checks that app, so probe each app separately
if the balancer tracks their readiness independently. An unknown host
returns 421; a missing Host returns 421 on HTTP/1.0 or 400 on HTTP/1.1.
The metrics port (`[listen].metrics` / `--metrics-listen`) serves
`/metrics` only: `/_wasmkeep/ready` there returns 404, not readiness.
The public port's `/_wasmkeep/health` works without an app hostname but
checks process liveness only; it is not a readiness substitute.

Start refuses, before binding a listener or writing a heartbeat: an
unknown field; a duplicate app, node id or hostname; an invalid name or
hostname; an app ceiling above the node's; resident floors above the
process's limit; two apps whose local roots overlap; two apps of one tenant
over a data directory still in the layout from before 0.4.0; a second app
without a cluster listener; several apps with `--cluster-plaintext
accept`; and, with several apps, any live node of the cluster that does
not announce `multi-app` (an older version). Errors name the file, line
and field.

Several apps share one process, one engine and one address space: the
shares protect apps from a noisy neighbour, they do not isolate them from
a hostile one. See the [security model](../security.md#several-apps-in-one-process).
