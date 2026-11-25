# Troubleshooting

What a status code, a readiness reason or a start-up refusal means, how to see which it was, and
what to do. Each entry is short and links to the page that explains the mechanism. For every metric
named here, see the [metrics reference](reference/metrics.md); for what to alert on, see
[Monitoring](monitoring.md).

## Where to look

| What you have | Where it tells you more |
| --- | --- |
| A response | The status, the short text body (`not ready`, `actor unavailable`, ...), `Retry-After`, and `x-wasmkeep-outcome: unknown` when the request may have run. |
| The node's log | Warnings with the reason (`event not run`, `a signed hop refused`, `checking for a new deploy failed`); more at `RUST_LOG=debug`. What guests print is logged as target `wasmkeep::guest`. |
| The metrics page | `wasmkeep_not_ready{reason}`, `wasmkeep_admission_refused_total{reason}`, `wasmkeep_failover_waits_total`, and the series named below. It is on `--metrics-listen`, a private port. |
| `wasmkeep cluster status` and `wasmkeep cluster diagnose` | The nodes, their leases and what each is doing, read through the bucket ([Checking a cluster](cluster.md#checking-a-cluster)). |

The public readiness answer gives no reason (the port faces clients); the reason is on the metrics
page.

## Which status

| Status | Usually | Entry |
| --- | --- | --- |
| 503 | Not ready, over capacity, an actor that cannot run now, a node that did not answer | [503](#503-service-unavailable) |
| 504 | A forwarded request written to a node that did not answer in time | [504](#504-gateway-timeout) |
| 421 | A `Host` the node does not serve | [421](#421-misdirected-request) |
| 403 | An access rule, or a node call that is not valid | [403](#403-forbidden) |
| 431 | A request head over the limit | [431](#431-request-header-fields-too-large) |
| 408 | A client that stopped sending | [408](#408-request-timeout) |
| 409 | A link already in use, or an actor held elsewhere | [409](#409-conflict) |
| 429 | An actor's queue is full, or an app's rate limit | [429](#429-too-many-requests) |
| Others | 400, 404, 412, 413, 507 | [Other statuses](#other-statuses) |

## 503 Service Unavailable

A 503 from WasmKeep says the node did not run the request here. Most carry `Retry-After`. A 503
that carries `x-wasmkeep-outcome: unknown` and no `Retry-After` is different: the request was
written to another node and may have run, so retry it only if it is safe to repeat
([What saved means](what-saved-means.md#locate-the-failure)). A 503 from your own guest is its own.

The body text says which it is.

| Body | Cause | Look at | Do |
| --- | --- | --- | --- |
| `not ready` (on `/_wasmkeep/ready`) | The node is not ready | `wasmkeep_not_ready{reason}` | [Node not ready](#node-not-ready) |
| `actor capacity unavailable` (`Retry-After: 1`) | No resident capacity: `--max-instances` instances are live | `wasmkeep_admission_refused_total{reason="capacity"}`, `wasmkeep_actors_resident` | Raise `--max-instances` if the memory is there, or add a node. Clients retry with a bound. |
| `actor unavailable` (`Retry-After: 1`) | The node cannot run the actor now: a required value is not set (`--requires`), the deployed version or the app's secrets are not applied, or the bucket is not answering for the actor's ownership | `wasmkeep_admission_refused_total{reason="unavailable"}`; the warning `event not run` in the log | Set the missing value or secret, see [Deploy behind](#deploy-behind) and [Node not ready](#node-not-ready), check the bucket. |
| `too many connections` (`Retry-After: 5`) | A WebSocket upgrade past `--max-sockets-per-actor`, `--max-sockets-per-app` or `--max-sockets-per-node` | `wasmkeep_socket_sessions` against `wasmkeep_socket_limit_effective`, `wasmkeep_admission_refused_total{reason="capacity"}` | [Socket admission options](reference/socket-limits.md). Split a larger room ([designing for hot actors](designing-for-hot-actors.md)). |
| `actor restore pending` (`Retry-After: 1`) | The actor is being restored from the bucket | `wasmkeep_restore_read_seconds`, `wasmkeep_restore_failures_total` | Retry. If it lasts, the bucket is slow or refusing: [Exporting and querying an actor](actor-data-access.md). |
| `this app has all the connections it may hold now; retry` (connection closed) | On a node serving several apps, this app holds its share of `--max-connections` | `wasmkeep_budget_share_in_use` | Raise `--max-connections`, or see [the node file](reference/node-file.md). |
| `this app was deleted` (no `Retry-After`) | The app has a tombstone: this is for good | The node's exit line | Nothing to retry. See [Deleting an app](cluster.md#deleting-an-app). |
| `the actor's node did not answer; retry`, `the actor's node is not reachable; retry` (`Retry-After: 1`) | The node that owns the actor died, is stopping or cannot be reached, and the wait for a takeover is over or was not allowed | `wasmkeep_failover_waits_total{outcome}` (`expired`, `over_cap`), `wasmkeep_forwards_total`, `wasmkeep_failovers_total` | Wait out the takeover (about 15 to 25 s with the defaults) and retry: [When a node dies](failover-and-scaling.md#what-callers-get-meanwhile). |
| `this hop is refused; retry` | A signed call from another node was refused | `a signed hop refused` in the log, with the reason | [Signature refusals](#signature-refusals). |
| `refused: sent by a node of an older version ...` | A mixed-version cluster during an upgrade | The node ids in `wasmkeep cluster status` | Stop every node and upgrade them together: [Upgrading WasmKeep](cluster.md#upgrading-wasmkeep). |
| The front answers 503 with `Retry-After: 1` | No front slot (`--max-front-requests`), no instance, or a value the front requires is not set (`--front-requires`) | `wasmkeep_front_requests_total{outcome="busy"}` and `unconfigured`, `wasmkeep_front_slots_in_use` | Raise `--max-front-requests`, or set the value. See [The front](concepts.md#the-front). |
| `asset unavailable; retry` (`Retry-After: 1`) | A static asset's content is missing or corrupt in the bucket | `wasmkeep_asset_fetches_total{outcome="unavailable"}` and `digest_mismatch` | Repair the deploy's objects, or deploy again: [Static assets](static-assets.md). |
| `the file cannot be read now; retry`, `the file cannot be written now; retry`, `links are not served yet; retry` | The files layer could not reach the bucket, or an upload is past its spool budget | `wasmkeep_blob_ops_total{outcome}` | Retry; give uploaders a retry. See [Files](files.md). |

An app that restarts in place after a verdict answers 503 with `Retry-After` on its hostnames until
it is back ([the node file](reference/node-file.md)).

## 504 Gateway Timeout

`the actor's node did not answer in time`, no `Retry-After`, and `x-wasmkeep-outcome: unknown`. A
request was written to the node that owns the actor, on a connection already open, and no answer
came in 30 s. It may have run. The usual cause is a node that died after the request was written.
Look at `wasmkeep_forwards_total` and the heartbeats (`wasmkeep_heartbeat_age_milliseconds`).

A load balancer in front of the nodes can answer its own 504 first: its server timeout must be
longer than `--failover-wait-secs` ([Load balancer, WAF and routing](deploy/front-door.md)).

## 421 Misdirected Request

The node does not serve that `Host`. It is never a guest's answer.

| Where | Cause | Do |
| --- | --- | --- |
| The public port of a node serving apps | The `Host` is not one of any app's hostnames (`unknown host`), a second `Host` was sent, or the `Host` does not match the request target; a missing `Host` is 421 on HTTP/1.0 and 400 on HTTP/1.1 | Send the app's hostname; check what the load balancer forwards. See [the node file](reference/node-file.md) and [Load balancer, WAF and routing](deploy/front-door.md). |
| A connection that belongs to another app's host | A client reused a connection for a host of another app | The client must open a connection per host. |
| The metrics listener | The `Host` is not an IP address, `localhost` or a name under it, a name given with `--metrics-host`, or (off loopback) a single-label name | Scrape by IP address, or name the host with `--metrics-host` (repeatable). |

## 403 Forbidden

| Cause | Look at | Do |
| --- | --- | --- |
| The class's access rule: an `internal` class reached from outside | `wasmkeep_admission_refused_total{reason="access"}` | Reach it through the front or another actor, or change the rule (`wasmkeep deploy --access`). See [Concepts](concepts.md) and [the project file](project-file.md). |
| Outside requests to a database, a key-value shard, a queue or a workflow runner, which answer only to actors | The same series | Call them from an actor: [A database kept by one actor](database.md), [Key-value and queues](kv-and-queues.md), [Workflows](workflows.md). |
| A node call whose signature was already used, or a hello that is not one | `wasmkeep_signatures_*` series | [Signature refusals](#signature-refusals). |
| A WebSocket the front handed to an actor, which the actor refuses | The guest's own reason in the body | The guest's access check ([API reference](reference/api.md)). |

## 431 Request Header Fields Too Large

The client's part of the request head is over 64 KiB on the wire (request line and headers), or has
more than 100 header fields. Node headers (`x-wasmkeep-*`) are not counted: a hop between nodes
gets 24 KiB and 32 fields more. The connection is closed after the answer.

The usual cause is a proxy or a browser sending large cookies or many headers. Look at what the
client sends (`curl -v`), and trim cookies at the proxy. The limits are in the
[security model](security.md#limits-at-a-glance).

## 408 Request Timeout

On a file upload link: the body stopped arriving (nothing for 60 s), or averaged under 16 KiB/s once
its first 30 s were over, or ran past the link's time. Every upload, its commit included, must end
by its link's expiry plus one hour. Make the uploader keep a steady rate, and ask for a new link to
retry ([Files](files.md#limits)). Look at `wasmkeep_blob_ops_total`.

A client that sends no complete request head within 30 s (`header_read_timeout`) has its connection
closed by the node; it may see a closed connection rather than a status.

## 409 Conflict

| Where | Cause | Do |
| --- | --- | --- |
| An upload link | The same link was used again while its first body is still arriving on this node | Wait, or ask for a new link. |
| A call to take or move an actor, with an `x-wasmkeep-holder` header | The actor is held by another node | The answer names the holder. See [Moving one actor](cluster.md#moving-one-actor). |
| A node that is draining | `this node is draining`: it takes no actor and answers a hop for one with 409 | Use another node, or `wasmkeep cluster undrain`: [Draining a node](cluster.md#draining-a-node). |
| A workflow event to a finished instance | `finished` | [Workflows](workflows.md). |

A guest may also answer 409 itself ([typed clients](typed-clients.md)).

## 429 Too Many Requests

- `actor overloaded`: the actor's queue is full (1,024 waiting events). Look at
  `wasmkeep_admission_refused_total{reason="overloaded"}` and `wasmkeep_mailbox_wait_seconds`. Bound
  admission before the queue fills ([Designing for hot actors](designing-for-hot-actors.md)).
- `rate limit exceeded`, with `Retry-After`: an app's configured limit ([the project
  file](project-file.md)). Look at `wasmkeep_rate_limit_requests_total{outcome}` (`quota`,
  `capacity`). Pass `Retry-After` through to clients ([Load balancer, WAF and
  routing](deploy/front-door.md)).

## Other statuses

| Status | Body | Cause |
| --- | --- | --- |
| 400 | `bad actor path`, `a request needs exactly one Host, matching its target` | A malformed actor address ([Concepts](concepts.md#addresses)), or a malformed `Host`. |
| 404 | `not found`; or an `existing` class's new name | No such path under `/_wasmkeep/`; an actor name that does not exist in a class with `existing` access. |
| 412 | `the file exists` | A file write that must not replace one ([Files](files.md)). |
| 413 | `request body too large` | A body declared larger than 16 MiB; a larger one fails the guest's read. |
| 507 | `the app is over its storage limit` | On a node link, the app is over `--max-app-blob-bytes`. A streamed write fails with `quota-exceeded`. |

## Retry-After

`Retry-After` is an integer in seconds. WasmKeep sets it on a refusal that ran nothing and that a
retry can fix: 1 on most (capacity, restore pending, the front, assets, a failed forward), 5 on a
WebSocket upgrade over a socket limit, and the seconds to wait on a rate limit. It is absent when a
retry would not help (a deleted app) and when the request may have run (`x-wasmkeep-outcome:
unknown`). Retry with jitter and a bound, and never retry a non-idempotent write that carries
`x-wasmkeep-outcome: unknown` without an identity of its own ([Give retries an
identity](what-saved-means.md#give-retries-an-identity)).

## Node not ready

`GET /_wasmkeep/ready` answers 503 `not ready`, and `wasmkeep_ready` is 0. The reason is the series
of `wasmkeep_not_ready` at 1. A few reasons are for good: the node stops and `serve` exits with the
same words.

| Reason | Cause | Do |
| --- | --- | --- |
| `shutting down` | A clean stop has begun | Nothing: the node leaves the balancer. |
| `checking that no other process runs with this node id` | Start-up, a check against the bucket | Wait a heartbeat period or two. If it does not end, check the bucket. |
| `another process runs with this node id` (for good) | Two processes share a node id | Give each process its own `--node-id`; stop the twin. |
| `this node was fenced` (for good) | A peer fenced its lease after it stopped renewing, or an operator ran `wasmkeep cluster fence` | Find why it stopped renewing (the bucket, a stalled host), then start it again. |
| `this node's lease was released by another process` (for good) | Another process stopped cleanly under this id | As for a twin. |
| `this node's lease object was deleted` (for good) | `nodes/<id>` was removed | Do not delete it by hand; restart the node. |
| `an older version of WasmKeep runs with this node id`, `a node of an older WasmKeep version runs in this cluster`, `a live node of an older version (no multi-app) runs in this cluster` | A mixed-version cluster | Finish the upgrade, in the order of [Upgrading WasmKeep](cluster.md#upgrading-wasmkeep). |
| `this node's lease object holds something this version cannot read` | An unreadable object under this id | Check that the bucket prefix is the cluster's own; see the version notes under [Upgrade](cluster.md#upgrading-wasmkeep). |
| `this node's lease is being removed by cluster forget` (for good) | A `wasmkeep cluster forget` of this node id replaced its released lease with a mark and did not delete it | Run `wasmkeep cluster forget <node>` again, then start the node. |
| `this node holds no lease` | The node has not written a lease yet | Wait; if it lasts, the bucket refuses writes. |
| `the bucket does not enforce conditional writes` (for good) | The start check failed | Use a bucket that enforces them ([Buckets](deploy/buckets.md)). |
| `the app's upgrade floor is above this node's version, or unreadable` (for good) | The app's upgrade was finalized at a newer minor version than this binary, or its floor record is corrupt | Install that version or later; for a corrupt record, the log names the key ([ADR 0020](adr/0020-one-lease-per-node.md#the-upgrade-floor-record)). |
| `bucket unreachable (node lease not renewed)` | The bucket refused or did not answer the node's lease writes for the lease's ttl less its margin (8 s by default) | Check the bucket and the network. `wasmkeep_node_lease_failures_total`, `wasmkeep_bucket_requests_total{status="error"}`. |
| `cluster secret not loaded` | The node has not read the cluster secret from the bucket | Check bucket access; the node retries. |
| `cluster identity not published for a heartbeat period yet` | A start-up step, one heartbeat period | Wait. |
| `this node has not yet checked whether its app was deleted`, `the app was deleted` | A node following its app checks for a tombstone before it takes actors | Wait for the check; a deleted app is for good ([Deleting an app](cluster.md#deleting-an-app)). |
| `disk not checked yet`, `disk check stalled` | The data directory's free space was not sampled (every 5 s) | Wait; if it stalls, the file system holding `--data-dir` is hung. |
| `disk almost full` | Less than 1 GiB free (or 5% of a smaller disk) where `--data-dir` is | Free space or grow the disk. The node is ready again at the next sample. |
| `cannot apply the deployed version` | See [Deploy behind](#deploy-behind) | |
| `cannot open the app's secrets` | The node holds no key that opens the app's current secrets | Give `--secrets-key` the right key ([Secrets in the bucket](cluster.md#secrets-in-the-bucket)). It refuses the app's events and hands its actors over meanwhile. |

On a process serving several apps, readiness is asked with an app's hostname and reports that
app's runtime ([Monitoring](monitoring.md#a-process-serving-several-apps)).

## Deploy behind

A node serving `--app` follows the app's deploys. It is "behind" when it read a new deploy and
cannot apply it: the deploy is invalid for this node, its front or classes are corrupt, or the
node's own files (the component cache, a disk) fail three checks in a row. It then reports
`cannot apply the deployed version` and is not ready, so it stops deciding requests under rules the
app no longer has. A bucket outage does not make a node behind: every node keeps serving.

- **See it:** `wasmkeep_not_ready{reason="cannot apply the deployed version"}` at 1, and the log
  line `checking for a new deploy failed` with the error and `retry_in_s` (the wait doubles up to a
  ceiling).
- **Do:** read the error. A corrupt version needs a new deploy; a deploy this node cannot apply may
  need the nodes upgraded first ([Upgrading WasmKeep](cluster.md#upgrading-wasmkeep)). For a local failure, check the node's disk and cache
  directory. The node is ready again at the next good check; there is nothing to restart.
- **Roll back:** `wasmkeep deploy --rollback` ([Deploying](cluster.md#deploying)).

## A node that refuses to start

`wasmkeep serve` checks these before it listens, and exits with the reason on its last log line.

| Refusal | Cause | Do |
| --- | --- | --- |
| An `--env` or `--front-env` file is refused: `other users can open it`, `its group can write it`, or `its group can read it ... and it is the server's own file` | The file holds secrets and its mode is too open. It must be `0600` or `0400` and the server user's, or `0640` or `0440` with root as owner and the server's group. Symlinks are followed: the target's mode counts (a mounted secret that is `0644` is refused). A fifo is refused, as is a file over the size limit | `chmod 600 FILE` (or `chmod o= FILE`, `chmod g-w FILE`). `wasmkeep dev` only warns on a file system that keeps no modes. |
| An `--env` file has a malformed line: `<line>: expected NAME=value`, a bad name, or control characters in the value | The format is `NAME=value` per line, as systemd's `EnvironmentFile=`; no escapes and no `$VAR` | Fix the line named. The message never repeats the value. |
| `component cache I/O error at <path>` | `--cache-dir` (default `.wasmkeep/cache`) cannot be created or opened. It is created `0700`; a directory others can write to is logged and not used | Point `--cache-dir` at a directory the server user owns and can write. A relative path is taken from the working directory, which differs under systemd. |
| `<file> is readable by its group or others; it holds the secrets key (chmod 600)` | `--secrets-key` file mode | `chmod 600` the key file. A mounted secret at `0644` is refused. |
| `<file>: not a secrets key (64 hex characters expected)` | A wrong or truncated key file | Use the file `wasmkeep secrets new-key` wrote. |
| `<tenant>/<app> has secrets in the bucket: give --secrets-key FILE, or --no-secrets to run with --env only` | The app has sealed secrets and the node has no key | Give `--secrets-key`, or `--no-secrets` on purpose. |
| `sealed with key <id>, which this node does not hold` | The key is not the one that sealed the set | Give the right key, or both during a key change ([Secrets in the bucket](cluster.md#secrets-in-the-bucket)). |
| `--access cannot be used with --app` | The app's access rules come with its deploys | Deploy the rules, then start without `--access`. |
| `... with --app, the front comes with the app's deploy` | A front path was given with `--app` | Deploy the front: `wasmkeep deploy --front PATH`. |
| `bucket <url>: ...` migration in progress, or no answer while checking its layout | The bucket is being migrated, or did not answer | Wait for the migration to end; check the bucket. See [Upgrading WasmKeep](cluster.md#upgrading-wasmkeep). |
| A cluster node without `--cluster-listen` | The default `--cluster-plaintext refuse` needs a cluster listener | Add `--cluster-listen` and `--cluster-advertise`, or accept plaintext on a private network on purpose ([Running a cluster](cluster.md#flags)). |
| A newer node started while a node of an older version is live on the prefix | A mixed-version cluster | Stop the older nodes first. |
| `--blob-links presigned` and the bucket's start proof failed | The bucket cannot presign downloads as asked | Use `auto`, or fix the bucket ([Files](files.md)). |

Every flag and its default is in the [CLI reference](reference/cli.md).

## Signature refusals

Calls between nodes (forwards, takes, alarm hand-offs, the ingress proofs) carry an HMAC signature
that is valid for a minute and accepted once. A refused one answers 503, so the sender signs again
(or 403 for one already spent in plaintext mode), and the client sees a retryable 503. The log says
`a signed hop refused`, with the reason.

| Series | What it means | Do |
| --- | --- | --- |
| `wasmkeep_signatures_before_start_total` | A call was signed before this node (or its runtime) started, plus the allowed clock skew. The record of used signatures does not survive a restart, so a replay cannot be ruled out. Expected for a few seconds after a restart | Nothing if it stops. If it keeps rising, a sender's clock is behind: check NTP on every node. |
| `wasmkeep_signatures_refused_full_total{record}` | The record of used signatures is full: `calls` (500,000), `control` (50,000) or `ingress` (4,096 charged proofs) | A call flood. Look at `wasmkeep_forwards_total`; rate-limit at the edge. |
| `wasmkeep_signatures_public_dropped_total` | Signed calls on the public port, refused anyway (the default `--cluster-plaintext refuse`), could not be recorded because the record was full | Node-signed calls reached the public port: check that every node has `--cluster-listen` and `--cluster-advertise`. |
| `wasmkeep_node_hellos_total` | Peers that just started and asked for a listing | Informational. |

Other causes of a refused hop: the sender is not a live node of this app (a node left, or is
fenced), a node of an older version (see [Node not ready](#node-not-ready)), or a clock skew beyond
the window. Check the clocks first, then `wasmkeep cluster status`. The mechanism is in the
[security model](security.md) and [Running a cluster](cluster.md).
