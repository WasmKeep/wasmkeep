# Monitoring

What a node tells you about itself: two health endpoints on the main port,
and Prometheus metrics on a port of their own.

## Health and readiness

| Path | Answers |
| --- | --- |
| `GET /_wasmkeep/health` | `200 ok` while the process accepts requests. |
| `GET /_wasmkeep/ready` | `200 ready` when the node should get traffic; `503 not ready` otherwise. |

A node is not ready once it starts shutting down; in a cluster, until it
has checked that the bucket enforces conditional writes and that no other
process runs with its node id (for good if either fails: `serve` exits),
while it has not loaded the cluster secret or its node lease (the heartbeat,
[ADR 0020](adr/0020-one-lease-per-node.md)) is not valid, which takes the
bucket refusing or not answering its writes for the lease's ttl minus its
safety margin (8 s by default; calls and durable writes would fail); and when the
disk holding `--data-dir` has less than 1 GiB free (or 5% of a smaller
disk), sampled every 5 s off the request path. The public answer gives no
reason (the port faces clients); `wasmkeep_not_ready{reason}` on the metrics
page does.

A node that stops for good says why in the same words in that reason and
at the start of `serve`'s exit line, followed there by the details:

| Reason | What it found |
| --- | --- |
| `another process runs with this node id` | another process writes this node id |
| `this node was fenced` | a peer fenced its lease after it stopped renewing, or an operator ran `wasmkeep cluster fence` |
| `this node's lease was released by another process` | another process stopped cleanly under this id |
| `this node's lease object was deleted` | its `nodes/<id>` object was removed |
| `an older version of WasmKeep runs with this node id` | a heartbeat in the format from before node leases under this id |
| `a node of an older WasmKeep version runs in this cluster` | another node of a version from before node leases (stop every older node first) |
| `this node's lease object holds something this version cannot read` | an unreadable object under this id |
| `the bucket does not enforce conditional writes` | the bucket check at start failed |

Point load balancers at `/_wasmkeep/ready`, and use `/_wasmkeep/health` for
"restart it if it stops answering".

## Open files

Every resident actor holds three open files (its database, write-ahead log
and shared-memory index), every connection one. At start, `wasmkeep serve`
raises its soft open-files limit to the hard limit, then caps connections
and resident actors to what fits, logging a warning when that lowers them.
With a cluster listener (`--cluster-listen`), its connections (up to
1,024) are counted first.
Give the process a generous limit (`LimitNOFILE=65536` in the systemd unit,
or `ulimit -n`) to serve many actors at once.

The resident cap includes actors still closing their databases and blocking
database work left by a cancelled event. An address without a mailbox
(including a hibernated or failed-over actor) waits up to 250 ms for an idle
actor's resources to close. If capacity remains full, its request receives
`503 Service Unavailable` with `Retry-After: 1`. Registered mailboxes can
still receive events, subject to their own queue limit (`429`). Relaying
to another owner does not take a local actor slot when the owner is known
(learned in the last second, or read from the actor's ownership record within the
admission wait); otherwise the request goes through local admission.
Clients should honor the retry delay and bound their retries.

Alarms refused by admission use exponential backoff with jitter: a delay
between half and all of a ceiling that starts at 100 ms and reaches 5 s.
The retry counter below makes sustained admission pressure observable.
WebSocket messages retain their frame while waiting up to the event wall
limit; close callbacks remain pending through capacity or queue pressure.
Closing sockets keep counting against socket limits until cleanup ends.

This admission rule uses the startup budget; it is not a live count of all
process descriptors. Unrelated files and outbound connections still need
the budget's reserved headroom. The resident metric below counts registered
mailboxes, so it can briefly fall before retiring resources release capacity.

## Memory

Each resident actor costs about 250 KB for Rust and 740 KB for TypeScript
on QuickJS ([benchmarks](benchmarks.md)); `wasmkeep_actors_resident` times
that, plus the compiled classes, is roughly the process's resident memory.

On Linux, `wasmkeep serve` and `wasmkeep dev` turn transparent huge pages
off for their own process. Where the kernel sets them to `always` (some
desktop and tuned distributions do; most servers use `madvise`), each
actor's memory would otherwise sit on 2 MiB pages it mostly does not
use: on one such machine, a Rust actor measured 577 KB instead of 250 KB,
with no gain in CPU or latency. To check a running node:

```bash
cat /sys/kernel/mm/transparent_hugepage/enabled   # the kernel's setting
pid=$(pidof wasmkeep)
grep THP_enabled /proc/$pid/status                 # 0: off for WasmKeep
```

`--transparent-huge-pages system` keeps the kernel's setting instead. If
the process cannot turn them off, it logs a warning and carries on.

## Metrics

```bash
wasmkeep serve ... --metrics-listen 127.0.0.1:9100
curl 127.0.0.1:9100/metrics
```

Off by default. The page names classes and counts events, so bind it to
loopback or a private address that only your Prometheus reaches. Labels are
class names, event kinds and fixed reasons, never actor names.

| Metric | Type | What it tells you |
| --- | --- | --- |
| `wasmkeep_actors_resident` | gauge | Actors in memory now: capacity planning. |
| `wasmkeep_websockets_open` | gauge | Registered WebSockets, including pending close cleanup. |
| `wasmkeep_classes` | gauge | Classes served. |
| `wasmkeep_ready`, `wasmkeep_not_ready{reason}` | gauges | 1 when `/_wasmkeep/ready` answers 200; otherwise one `wasmkeep_not_ready` series saying why. |
| `wasmkeep_events_total{class,kind,outcome}` | counter | Events run; `kind` is `request`, `alarm`, `websocket` or `init`, `outcome` is `ok` or `failed` (trapped, cut, or a handler that returned an error). |
| `wasmkeep_event_duration_seconds{class}` | histogram | Slow handlers and hot classes. |
| `wasmkeep_event_cuts_total{reason}` | counter | Events the host stopped: `wall` (past the wall-clock limit) or `lease` (this node lost the actor mid-event). |
| `wasmkeep_cpu_budget_exceeded_total` | counter | Guest calls stopped for using their CPU budget. |
| `wasmkeep_instantiate_seconds` | histogram | Creating an actor's instance: the wake cost. |
| `wasmkeep_wakes_total`, `wasmkeep_hibernations_total` | counters | Actor instances created (first request or wake) and dropped (hibernated, evicted, discarded after a trap, replaced by a new version, shut down): is the idle timeout right? |
| `wasmkeep_forwards_total{kind}` | counter | Requests (`request`), sockets (`websocket`) and calls (`call`) sent to the node owning the actor: placement efficiency. |
| `wasmkeep_durable_write_seconds` | histogram | `exec-durable` until durable: the bucket's health as actors feel it. |
| `wasmkeep_requests_abandoned_total` | counter | Requests and actor calls not run because their sender had gone while they waited in the actor's queue (behind a long event, or a takeover's restore): a client that closed its connection, or a caller whose call timed out. Nobody would get the answer, and nothing they would have written was acknowledged. A request that has started runs to its end. Seen only for a request received whole (no body, or one that arrived in the server's first read, about 8 KiB): one whose body is still streaming (larger, chunked, or waiting for `100 Continue`) runs as before. |
| `wasmkeep_restore_read_seconds` | histogram | One object (a segment or a snapshot) a restore read from the bucket, from its request to its last byte, resends included. A restore keeps up to 8 reads, and 32 MiB by their listed sizes, under way while it applies the objects in order (always at least one, however large); a node's restores together read at most 32 objects and 256 MiB at once, one read counting for at most 32 MiB. The time starts once a read has its share, so waiting for it is not counted, and reads overlap, so their sum can exceed the restore's time. A read is resent when the bucket leaves it unanswered for 5 s, or when it takes longer than 5 s plus a second per megabyte read alongside it on the node (its own included); three tries at most. |
| `wasmkeep_listing_seconds` | histogram | One listing of the cluster's heartbeats, with the view and the peers' pins it refreshes (every heartbeat period, and early when an unknown pin knocks). Near the heartbeat horizon (10 s by default), peers' pins expire before the next listing renews them and handshakes between nodes are refused; fences run on a task of their own and are not counted. |
| `wasmkeep_guest_log_lines_total`, `wasmkeep_guest_log_dropped_total` | counters | Lines actors printed that reached the log, and that were dropped over the rate ([what actors print](#what-actors-print)). |
| `wasmkeep_alarm_admission_retries_total` | counter | Alarm deliveries rescheduled because resident capacity or an actor queue was full; no actor labels. |
| `wasmkeep_node_lease_writes_total`, `wasmkeep_node_lease_failures_total` | counters | Writes of this node's lease (its heartbeat, [ADR 0020](adr/0020-one-lease-per-node.md)) that reached the bucket, and that failed or timed out. With the defaults (a write every 3 s, valid for 8 s), two failing in a row and the lease runs out: the node reports not ready. A write that hangs is counted when it times out (2.7 s), so alert on the first increment. |
| `wasmkeep_node_fences_total` | counter | Fences this node wrote over a node that stopped renewing its lease (crashed, or cut off from the bucket). |
| `wasmkeep_bucket_requests_total{operation,status}` | counter | Requests this node made to the bucket: cost and errors. See below. |
| `wasmkeep_failovers_total` | counter | Actors this node took over whose ownership record still named another node, freed because that node was fenced, released or replaced by a new process. See below. |
| `wasmkeep_history_collected_total` | counter | Objects of actors' history deleted from the bucket by garbage collection ([ADR 0012](adr/0012-bucket-garbage-collection.md)). |
| `wasmkeep_history_collect_failures_total` | counter | Garbage-collection passes that failed (a bucket error); the next snapshot tries again. Steadily rising: the bucket refuses deletes. |
| `wasmkeep_snapshot_upload_failures_total` | counter | Snapshot uploads that were not acknowledged: the node gave the upload up, which does not say the bucket lacks it (an attempt given up can still land later, and is then an ordinary snapshot). A periodic snapshot has one attempt (one call to the bucket client, which may send the request again within it after an error answer), given up after 5 s plus a second per megabyte of the database, 10 s at most. One not acknowledged loses nothing and fails no event: the writes already uploaded are the history, and it is tried again once the actor has made one more write and 30 s have passed, then 8 writes later, then every 64 while it keeps failing (the node keeps these waits while an idle actor is out of memory; a node that takes the actor over, or one that restarted, tries at the actor's first event there). Until one succeeds, a restore of that actor replays more writes; a database the bucket cannot take within 10 s counts here at every try. An actor's first snapshot and the snapshot after a delete are sent again as a write is (an attempt is given up after 5 s plus a second per megabyte, the next one after twice that, three at most, the whole upload within 20 s plus that second per megabyte; the bucket client ends any single request after 30 s by default and does not send it again after that): the snapshot after a delete counts here and in `wasmkeep_history_collect_failures_total`, and does not delay the periodic one; an actor's first snapshot counts here, and its event fails. Steadily rising: the bucket refuses or stalls large uploads. |

Not there yet: per-tenant labels (one tenant per process until
multi-tenancy), traps by reason (`wasmkeep_events_total{outcome="failed"}`
and `wasmkeep_cpu_budget_exceeded_total` cover them for now) and parked
WebSockets (sockets are not parked yet; `wasmkeep_websockets_open` counts
them).

### Bucket requests

Every bucket request a node makes is counted: leases, heartbeats, the alarm
index, replication, restores and following a deployed app. `operation` is
`get`, `head`, `put` (conditional writes included), `delete`, `list`,
`copy`, `rename` or `multipart`. `status` is:

| Status | Means |
| --- | --- |
| `ok` | The request succeeded. |
| `not_found` | The object did not exist. Routine: a lease or an app not created yet. |
| `precondition` | A conditional write or read lost its condition (the object exists, or changed since it was read). Routine for leases: another node won the race. |
| `error` | Anything else: unreachable bucket, refused credentials, throttling, a server error. |

Watch `status="error"`; the other two are part of normal operation. A
listing counts once per call, however many pages it took: S3 and R2 return
1,000 keys a page, so a restore of an actor with 10,000 objects in the
bucket counts 2 `list` requests here but bills about 22.

### Failovers

`wasmkeep_failovers_total` counts, on the node that took them, actors
taken from another node whose node lease was fenced (or released with the
actor still recorded as its own, ttl plus grace earlier): that node died,
was cut off from the bucket, or stopped without releasing the actor ([ADR
0020](adr/0020-one-lease-per-node.md)). An actor that moves because its
owner hibernated it, shut down, or gave it up is a hand-off (the owner
released it) and is not counted, nor is a node taking back its own actors
after a restart. Sum
it over nodes for the cluster; outside rolling restarts of crashed nodes it
should stay near zero.

### Grafana dashboard

[`deploy/grafana/wasmkeep.json`](../deploy/grafana/wasmkeep.json) shows
every metric above except `wasmkeep_classes`, the guest log counters
(`wasmkeep_guest_log_lines_total`, `wasmkeep_guest_log_dropped_total`),
the history collection counters (`wasmkeep_history_collected_total`,
`wasmkeep_history_collect_failures_total`),
`wasmkeep_snapshot_upload_failures_total`, the listing and restore-read
histograms (`wasmkeep_listing_seconds`, `wasmkeep_restore_read_seconds`),
the node lease write and fence counters (`wasmkeep_node_lease_writes_total`,
`wasmkeep_node_fences_total`) and `wasmkeep_requests_abandoned_total`. To
see it on a running cluster, the
[Compose example](../deploy/compose/README.md#with-prometheus-and-grafana)
starts three nodes with Prometheus and Grafana, the dashboard loaded.
Otherwise import it in Grafana (Dashboards, New, Import) and pick
your Prometheus data source; it needs Grafana 10 or later. It assumes
Prometheus adds an `instance` label to each node's series, as it does by
default, and filters by node and class.

| Row | Panels |
| --- | --- |
| Overview | Nodes ready and not ready (with the reason), resident actors, open WebSockets, failovers over the time range. |
| Events | Events per second by class and outcome, the failed share by class, duration p50 and p99, events stopped by the host (wall clock, lost lease, CPU budget). |
| Actors and placement | Wakes and hibernations, instantiation p50 and p99, forwards by kind, alarm admission retries. |
| Bucket and leases | Bucket requests by operation and by status, durable write p50 and p99, bucket errors and node lease failures per node, failovers per node. |

A test (`the_grafana_dashboard_uses_only_metrics_that_exist`) fails if the
dashboard queries a metric the page does not have.

### Alerts worth having

These are in [`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml)
as Prometheus rules, with one more for a node Prometheus cannot scrape
(`up == 0` for a minute): a node that is down exports nothing, so none of
the rules below would say so. Load the file with `rule_files:` in
`prometheus.yml` and check it with `promtool check rules` after editing.
It assumes a scrape job named `wasmkeep`, and the rules on durable write
latency and on failed events wait 5 minutes before they fire. Two of the
rules below hold
"when no node was lost": a rule cannot know that, so they fire on every
fence and failover, a deliberate kill included, and their text says so.
The [Compose example](../deploy/compose/README.md#with-prometheus-and-grafana)
loads the file. A test (`the_alert_rules_use_only_metrics_that_exist`)
fails if a rule names a metric the page does not have.

- `wasmkeep_node_lease_failures_total` increasing on any node: two failures
  in a row and the node drops every actor it holds.
- `increase(wasmkeep_node_fences_total[15m])` above 0 when no node was lost.
- `histogram_quantile(0.99, rate(wasmkeep_durable_write_seconds_bucket[5m]))`
  above 1 s.
- `rate(wasmkeep_events_total{outcome="failed"}[5m])` above 1% of all events
  for a class.
- `/_wasmkeep/ready` failing on any node for 2 minutes.
- `rate(wasmkeep_bucket_requests_total{status="error"}[5m])` above 0 for
  5 minutes.
- `increase(wasmkeep_failovers_total[15m])` above 0 when no node was lost.

## What actors print

What an actor writes to stdout or stderr (Rust's `println!` and
`eprintln!`, `console.log` in TypeScript, `print` in Python, `printf` in C)
reaches the node's log, one entry per line, as target `wasmkeep::guest` with
the actor's address and the stream; a stateless component's says
`actor=stateless`:

```text
INFO wasmkeep::guest: order 42 shipped actor=acme/orders/42 stream="stdout"
```

- **Limits:** `--guest-log-rate` lines a second per actor instance (50 by
  default, with bursts of twice that) and 1,000 a second for the whole
  node. Past them, lines are dropped and counted
  (`wasmkeep_guest_log_dropped_total`), and the actor's next logged line is
  preceded by a warning saying how many were. `--guest-log-rate 0`
  discards everything.
- **Lines:** a line is logged when its newline arrives; one left without
  it is logged when the instance ends (hibernation, a trap). Lines are cut
  at 4 KB, and control characters are escaped (`\u{1b}`), so a guest cannot
  forge entries or colour a terminal. Writes never block or fail the guest.
- **Filtering:** the usual `RUST_LOG`, e.g. `RUST_LOG=info,wasmkeep::guest=warn`
  to keep only the drop warnings, or `wasmkeep::guest=off`.
- Actors log what they choose to: whatever they print (a secret included)
  lands in the node's log, and wherever that is shipped.
