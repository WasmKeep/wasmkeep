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
the node's tenant and app, class names, event kinds and fixed reasons, never
actor names. Every series with a `class` label, and the front's counters
and histogram, also have `tenant` and `app`:
the app the node serves (`serve --app tenant/app`), or the implicit app
`default/default` for classes given on the command line.

| Metric | Type | What it tells you |
| --- | --- | --- |
| `wasmkeep_info{version,tenant,app,node}` | gauge | Always 1: the running version and the node's app, with `node` its node id in a cluster (no `node` label otherwise). Join on it to see which version each node runs, or to add the app to the series that do not carry it: `wasmkeep_actors_resident * on (instance) group_left (tenant, app, version) wasmkeep_info`. |
| `wasmkeep_actors_resident` | gauge | Actors in memory now: capacity planning. |
| `wasmkeep_websockets_open` | gauge | Registered WebSockets, including pending close cleanup. |
| `wasmkeep_classes` | gauge | Classes served. |
| `wasmkeep_ready`, `wasmkeep_not_ready{reason}` | gauges | 1 when `/_wasmkeep/ready` answers 200; otherwise one `wasmkeep_not_ready` series saying why. |
| `wasmkeep_events_total{tenant,app,class,kind,outcome}` | counter | Events run; `kind` is `request`, `alarm`, `websocket` or `init`, `outcome` is `ok` or `failed` (trapped, cut, or a handler that returned an error). |
| `wasmkeep_event_duration_seconds{tenant,app,class}` | histogram | Slow handlers and hot classes. |
| `wasmkeep_event_cuts_total{reason}` | counter | Events the host stopped: `wall` (past the wall-clock limit) or `lease` (this node lost the actor mid-event). |
| `wasmkeep_cpu_budget_exceeded_total` | counter | Guest calls stopped for using their CPU budget. |
| `wasmkeep_instantiate_seconds` | histogram | Creating an actor's instance: the wake cost. |
| `wasmkeep_wakes_total`, `wasmkeep_hibernations_total` | counters | Actor instances created (first request or wake) and dropped (hibernated, evicted, discarded after a trap, replaced by a new version, shut down): is the idle timeout right? |
| `wasmkeep_forwards_total{kind}` | counter | Requests (`request`), sockets (`websocket`) and calls (`call`) sent to the node owning the actor: placement efficiency. |
| `wasmkeep_failover_waits_total{kind,outcome}` | counter | Requests (`request`) and WebSocket upgrades (`websocket`) for an actor whose node was dead or stopping that this node kept until the actor could be taken over, instead of answering 503 at once ([what a caller sees when a node dies](cluster.md#when-a-node-dies-what-a-caller-sees)), by how the wait ended: `served` (this node took the actor and answered), `forwarded` (another node took it first; sent there once), `owner_alive` (the owner wrote its lease again or restarted; sent to it as usual), `expired` (the wait ran out: 503), `shutdown` and `lease_lost` (this node began to stop, or its own lease ran out: 503), `left` (the client closed its connection: never run), `answered_other` (asked for the actor here and answered otherwise: the node full, the takeover failed, an access rule; or the client left during that try). `over_cap` counts requests that would have been kept and got the 503 at once because the node already kept a quarter of its connection limit, or 64 for that actor. `expired` rising when a node dies: the wait is shorter than the takeover (`--failover-wait-secs`), or no node could fence the dead one. |
| `wasmkeep_durable_write_seconds` | histogram | `exec-durable` until durable: the bucket's health as actors feel it. |
| `wasmkeep_requests_abandoned_total` | counter | Requests and actor calls not run because their sender had gone while they waited in the actor's queue (behind a long event, or a takeover's restore): a client that closed its connection, or a caller whose call timed out. Nobody would get the answer, and nothing they would have written was acknowledged. A request that has started runs to its end. Seen only for a request received whole (no body, or one that arrived in the server's first read, about 8 KiB): one whose body is still streaming (larger, chunked, or waiting for `100 Continue`) runs as before. |
| `wasmkeep_restore_read_seconds` | histogram | One object (a segment or a snapshot) a restore read from the bucket, from its request to its last byte, resends included. A restore keeps up to 8 reads, and 32 MiB by their listed sizes, under way while it applies the objects in order (always at least one, however large); a node's restores together read at most 32 objects and 256 MiB at once, one read counting for at most 32 MiB. The time starts once a read has its share, so waiting for it is not counted, and reads overlap, so their sum can exceed the restore's time. A read is resent when the bucket leaves it unanswered for 5 s, when it takes longer than 5 s plus a second per megabyte read alongside it on the node (its own included), or when the bucket refuses it with an error a resend can get past (after a pause of half a second, then a second, without its share, then the wait for a share again: both are in this time); three tries at most. |
| `wasmkeep_restore_failures_total` | counter | Restores of an actor from the bucket that did not complete, whatever the reason: a read that failed three times, the bucket refusing, the deadline of its reads (30 s, plus a second per megabyte and a second per eight objects of what it reads, counting the time it waits for the bucket to give it the next object: not the listing, local disk work, waiting for a turn among the node's other restores, or the seconds those restores' bytes in flight add to a read's limit), a history that cannot be restored, or a local file error. A restore that fails before its database is complete leaves the node's files as they were; one that fails putting the complete database in place (a local file error) has set the previous file aside, and the next restore reads the bucket again either way. For the bucket's reasons the request that woke the actor got a 503 with `Retry-After` and the next one restores again from a fresh listing; the log line `restore failed` names the actor and the reason. A few during a bucket incident are expected; steadily rising with a healthy bucket means actors that cannot be restored in their time (see `wasmkeep_restore_read_seconds`) or a damaged history. |
| `wasmkeep_listing_seconds` | histogram | One listing of the cluster's heartbeats, with the view and the peers' pins it refreshes (every heartbeat period, and early when an unknown pin knocks). Near the heartbeat horizon (10 s by default), peers' pins expire before the next listing renews them and handshakes between nodes are refused; fences run on a task of their own and are not counted. |
| `wasmkeep_guest_log_lines_total`, `wasmkeep_guest_log_dropped_total` | counters | Lines actors printed that reached the log, and that were dropped over the rate ([what actors print](#what-actors-print)). |
| `wasmkeep_admission_refused_total{reason}` | counter | Requests, WebSocket upgrades and actor calls this node refused before their event ran, by `reason`: `access` (the class's access rule: an internal class reached from outside, 403, or a name an `existing` class does not let an outside request create, 404), `capacity` (no resident capacity, 503 with `Retry-After`, or a WebSocket over the socket limits), `overloaded` (the actor's queue was full: 429 for requests and calls, 503 for a WebSocket upgrade), `unavailable` (the node cannot run the actor now, 503: a required value not set, the deployed version or the app's secrets not applied, the bucket not answering for the actor's ownership; on a WebSocket upgrade, an ownership record that cannot be read also answers 503 and is counted here, where a request gets a 500 and is not counted) and `wrong_app` (a hop from another node, or a forwarded request, that names another app than this node's, 503 "not this app's node", or no app at all, as a node of an older version sends; [ADR 0022](adr/0022-app-owned-state-and-the-one-time-migration.md)). A 503 while an actor's ownership changes hands between nodes is not a refusal and is not counted. All five series are always on the page. Each refusal is counted once, by the node that answered it: one forwarded to the actor's node is counted there. Alarms are not counted here (`wasmkeep_alarm_admission_retries_total`). `capacity` rising: more actors awake at once than the node holds (see [open files](#open-files) and [memory](#memory)); `access` rising: clients or links aimed at actors they may not reach; `wrong_app` rising: nodes of two apps (or of two versions) on one cluster port, or a front door sending one app's traffic to another app's nodes. |
| `wasmkeep_front_requests_total{tenant,app,outcome}` | counter | Requests the front ([ADR 0023](adr/0023-the-front.md)) received, by `outcome`: `ok` (it answered), `busy` (every front slot was taken, or, briefly, the instance pool had none free: 503 with `Retry-After: 1`, and the front did not run), `unconfigured` (a value the front requires is not set, from the deploy's `--front-requires`, or the node cannot open the app's secrets: 503 with `Retry-After: 1`, the front did not run, and the missing names are in the node's log only), `cpu_budget` (past `--front-cpu-budget-ms`, 500), `wall` (past its 30 s wall, 500) `guest_error` (it trapped, answered nothing, or answered with a `1xx` status, all 500) and `client_gone` (the client left before the end of its answer, which stopped the instance). A request is counted when its instance ends: one stopped while its response was streaming counts as `cpu_budget`, `wall` or `guest_error`, not `ok`, though the client already had a 200. A client with its whole answer (its declared length, its trailers, or an answer without a body: `HEAD`, 204, 304) is not gone: the instance runs on, up to its wall, and counts when it ends. `busy` rising: raise `--max-front-requests`, or find what keeps front requests waiting (their calls). |
| `wasmkeep_front_request_seconds{tenant,app}` | histogram | How long front requests took, to the response's last byte. |
| `wasmkeep_front_slots_in_use` | gauge | Front requests running now, out of `--max-front-requests`. |
| `wasmkeep_front_calls_total{tenant,app,outcome}` | counter | `actors.call` from the front, by `outcome`: `ok` (the callee answered, whatever its status), `busy` (a quarter of the front's slots already waited on that actor: `failed("front busy: ...")`), `timeout` (5 s of the front's wall were left, or the actors' call limit passed) and `failed` (every other error: an invalid address, a callee that failed or was overloaded, a body too large). A call to another node is also counted in `wasmkeep_forwards_total{kind="call"}`. |
| `wasmkeep_alarm_admission_retries_total` | counter | Alarm deliveries rescheduled because resident capacity or an actor queue was full; no actor labels. |
| `wasmkeep_node_lease_writes_total`, `wasmkeep_node_lease_failures_total` | counters | Writes of this node's lease (its heartbeat, [ADR 0020](adr/0020-one-lease-per-node.md)) that reached the bucket, and that failed or timed out. With the defaults (a write every 3 s, valid for 8 s), two failing in a row and the lease runs out: the node reports not ready. A write that hangs is counted when it times out (2.7 s), so alert on the first increment. |
| `wasmkeep_node_fences_total` | counter | Fences this node wrote over a node that stopped renewing its lease (crashed, or cut off from the bucket). |
| `wasmkeep_bucket_requests_total{operation,status}` | counter | Requests this node made to the bucket: cost and errors. See below. |
| `wasmkeep_failovers_total` | counter | Actors this node took over whose ownership record still named another node, freed because that node was fenced, released or replaced by a new process. See below. |
| `wasmkeep_history_collected_total` | counter | Objects of actors' history deleted from the bucket by garbage collection ([ADR 0012](adr/0012-bucket-garbage-collection.md)). |
| `wasmkeep_history_collect_failures_total` | counter | Garbage-collection passes that failed (a bucket error); the next snapshot tries again. Steadily rising: the bucket refuses deletes. |
| `wasmkeep_snapshot_upload_failures_total` | counter | Snapshot uploads that were not acknowledged: the node gave the upload up, which does not say the bucket lacks it (an attempt given up can still land later, and is then an ordinary snapshot). A periodic snapshot has one attempt (one call to the bucket client, which may send the request again within it after an error answer), given up after 5 s plus a second per megabyte of the database, 10 s at most. One not acknowledged loses nothing and fails no event: the writes already uploaded are the history, and it is tried again once the actor has made one more write and 30 s have passed, then 8 writes later, then every 64 while it keeps failing (the node keeps these waits while an idle actor is out of memory; a node that takes the actor over, or one that restarted, tries at the actor's first event there). Until one succeeds, a restore of that actor replays more writes; a database the bucket cannot take within 10 s counts here at every try. An actor's first snapshot and the snapshot after a delete are sent again as a write is (an attempt is given up after 5 s plus a second per megabyte, the next one after twice that, three at most, the whole upload within 20 s plus that second per megabyte; the bucket client ends any single request after 30 s by default and does not send it again after that): the snapshot after a delete counts here and in `wasmkeep_history_collect_failures_total`, and does not delay the periodic one; an actor's first snapshot counts here, and its event fails. Steadily rising: the bucket refuses or stalls large uploads. |
| `wasmkeep_signatures_refused_full_total{record}` | counter | Calls from other nodes refused because this node's record of used signatures (replay protection: 500,000 at once for `calls`, ordinary node calls; 50,000 for `control`, take and alarm hand-off calls, kept apart so a flood of other calls cannot block a drain or a move). The node logs such a refusal as a full record, apart from replays. Any rise: far more node calls a minute than expected. |
| `wasmkeep_websocket_owner_closes_total{reason}` | counter | WebSockets closed with 1012 because this node no longer owned their actor, by how it found out: `event` (a message or close on one of them found another node owning the actor, or this draining node sending it to a peer), `sweep` (the owner sweep found another node holding a room that hibernated with sockets open here), `lease_lapse` (this node's lease ran out), `drain` (a draining node closing the sockets of hibernated rooms) and `move` (a room parked here being moved). Clients reconnect. A room that hibernates with sockets open keeps its record and wakes here ([ADR 0024](adr/0024-socket-home.md)), so `event` and `sweep` stay near zero; a steady rate means rooms are woken on another node than the one holding their sockets (a node of a version before 0.4.0, or a record changed behind this node's back). |
| `wasmkeep_websocket_sweep_reads_total` | counter | Ownership records the owner sweep read: once a minute, one per room with WebSockets open here that this node neither holds nor keeps parked, 5,000 a pass at most (the next pass goes on with the rest), and on one pass in ten one per parked record too (a safeguard). A pass stops at its first failed read. Each read is a bucket GET. |
| `wasmkeep_parked_records` | gauge | Ownership records this node keeps for rooms that hibernated with WebSockets open here ([ADR 0024](adr/0024-socket-home.md)): each such room wakes here, wherever its next event lands. Bounded by the sockets a node accepts (50,000). They cost no request while they last; a node dying with many of them makes those rooms wait for the failover (about 15 to 20 s), as resident actors do. |
| `wasmkeep_parked_wakes_total` | counter | Wakes of a parked room on this node, each an acquisition at a new generation (the record's read and write). |
| `wasmkeep_parked_releases_total{reason}` | counter | Parked records released, by `reason`: `last_socket` (the room's last socket here went), `drain`, `move`, `lapse` (this node's lease ran out: the release is pending, retried) and `stop` (a clean stop). |

Not there yet: `tenant` and `app` on the series without a class (one app
per process for now: join on `wasmkeep_info`), traps by reason
(`wasmkeep_events_total{outcome="failed"}` and
`wasmkeep_cpu_budget_exceeded_total` cover them for now) and parked
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
`wasmkeep_snapshot_upload_failures_total`,
`wasmkeep_signatures_refused_full_total`,
`wasmkeep_websocket_owner_closes_total`,
`wasmkeep_websocket_sweep_reads_total`, the parked record metrics
(`wasmkeep_parked_records`, `wasmkeep_parked_wakes_total`,
`wasmkeep_parked_releases_total`),
`wasmkeep_restore_failures_total`, the listing and restore-read
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
the actor's address and the stream; the front's (the stateless component)
says `actor=_front`:

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
