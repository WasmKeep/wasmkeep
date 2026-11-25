# Metrics reference

Every series a node serves on its metrics page, with its type, labels and meaning. For what to
alert on and how to read the page, see [Monitoring](../monitoring.md); this page is the
catalogue behind it. For what to do when a status code or a readiness reason shows up, see
[Troubleshooting](../troubleshooting.md).

The list follows `crates/wasmkeep-server/src/metrics.rs` and `metrics/render.rs`. A script,
`scripts/check-metrics-reference.py`, fails when a metric name is in the code and not on this page,
or the other way round. It checks names only; types, labels and meanings are kept by hand.

## Reading the tables

- **Type** is the Prometheus type. A histogram `x` is served as `x_bucket`, `x_sum` and `x_count`.
- **app** in the Labels column stands for the two labels `tenant` and `app` of the app the series
  is about. A node that serves several apps ([ADR 0038](../adr/0038-several-apps-per-process.md))
  has one series per app.
- A family whose label values are a fixed set is on the page at 0 from the first scrape, so
  `increase()` sees the first event. Families keyed by class or app appear at their first event.
- Counters count from the start of the process; use `rate()` or `increase()`.

## Node

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_info` | gauge | `version`, app, `node` (in a cluster) | Always 1: the running version and the node's identity, for joining with other series. |
| `wasmkeep_ready` | gauge | none | 1 when `/_wasmkeep/ready` answers 200. |
| `wasmkeep_not_ready` | gauge | `reason` | One series per reason a node can be not ready: 1 for the reason it is not ready, 0 for the others. See [Troubleshooting](../troubleshooting.md#node-not-ready). |
| `wasmkeep_actors_resident` | gauge | none | Actors resident in memory. |
| `wasmkeep_classes` | gauge | none | Actor classes served. |
| `wasmkeep_websockets_open` | gauge | none | Open WebSockets. |
| `wasmkeep_runtime_schedule_lag_seconds` | histogram | none | How much later than asked a task sleeping 100 ms woke: the async runtime's scheduling delay. |
| `wasmkeep_allocator_trims_total` | counter | `released` | Actual `malloc_trim` calls, by whether they released memory. |
| `wasmkeep_allocator_trim_seconds` | histogram | none | Time inside `malloc_trim`. |
| `wasmkeep_cpu_budget_exceeded_total` | counter | none | Guest calls stopped for using their CPU budget. |
| `wasmkeep_guest_log_lines_total` | counter | none | Lines guests printed that reached the node's log. |
| `wasmkeep_guest_log_dropped_total` | counter | none | Lines guests printed that were dropped over the line rate. |

## Traces

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_trace_spans_dropped_total` | counter | none | Completed spans dropped when the bounded export queue cannot accept them. |
| `wasmkeep_trace_exports_total` | counter | `outcome` (`ok`, `error`) | Export batches that succeeded or failed, including timeouts. |

## Sockets and admission

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_socket_sessions` | gauge | none | Process socket sessions, including relays and upgrades still opening. |
| `wasmkeep_socket_limit_requested` | gauge | none | Requested process socket sessions. |
| `wasmkeep_socket_limit_effective` | gauge | none | Process socket sessions after descriptor budgeting. |
| `wasmkeep_socket_actor_limit_requested` | gauge | none | Requested sockets per actor. |
| `wasmkeep_socket_actor_limit_effective` | gauge | none | Sockets per actor, bounded by the app and process ceilings. |
| `wasmkeep_admission_refused_total` | counter | `reason` (`access`, `capacity`, `overloaded`, `unavailable`, `wrong_app`) | Requests, WebSocket upgrades and actor calls refused before their event ran. |
| `wasmkeep_rate_limit_requests_total` | counter | `outcome` (`admitted`, `quota`, `capacity`, `invalid_key`) | Ingress admission decisions. |
| `wasmkeep_rate_limit_active_keys` | gauge | none | Ingress client buckets retained on this process. |

## Events and actors

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_events_total` | counter | app, `class`, `kind`, `outcome` | Events run. |
| `wasmkeep_event_duration_seconds` | histogram | app, `class`, `kind` (`fetch`, `alarm`, `websocket`, `other`) | How long guest events ran. |
| `wasmkeep_event_fuel` | histogram | app, `class` | Wasmtime fuel units consumed per guest event; excludes host work. |
| `wasmkeep_event_cuts_total` | counter | `reason` (`wall`, `lease`, `replica`, `stop`) | Events stopped by the host. |
| `wasmkeep_instantiate_seconds` | histogram | none | Instantiating an actor's component. |
| `wasmkeep_wakes_total` | counter | none | Actors brought into memory (created or woken). |
| `wasmkeep_hibernations_total` | counter | none | Actors that left memory (idle, evicted, deleted or shut down). |
| `wasmkeep_requests_abandoned_total` | counter | none | Requests and actor calls not run because their sender had gone while they waited in the actor's queue. |
| `wasmkeep_mailbox_wait_seconds` | histogram | app, `class` | Time from an accepted enqueue to the start of the mailbox job. |
| `wasmkeep_mailbox_host_work_seconds` | histogram | app, `class`, `work` | Exclusive mailbox host work time. |
| `wasmkeep_durable_write_seconds` | histogram | none | `exec-durable`, from the call until the write is durable. |
| `wasmkeep_alarm_deliveries_total` | counter | app, `class`, `outcome` (`ran`, `stale`, `duplicate`, `deferred`) | Alarm deliveries run in actor mailboxes. |
| `wasmkeep_alarm_stale_hints_total` | counter | app, `class` | Stale alarm hint clear attempts. |
| `wasmkeep_alarm_admission_retries_total` | counter | none | Alarm deliveries rescheduled after a mailbox admission refusal. |
| `wasmkeep_alarm_dropped_deliveries_total` | counter | none | Alarm deliveries the mailbox dropped before they answered, rescheduled with backoff. |
| `wasmkeep_actor_deletes_total` | counter | `how` (`self`, `self_durable`, `operator`, `expired`) | Actor deletes recorded. |
| `wasmkeep_actor_delete_failures_total` | counter | `how` (same values) | Actor deletes that failed to be recorded (the data stays). |
| `wasmkeep_expiry_passes_total` | counter | none | Idle-expiry passes this node ran as the collecting node. |
| `wasmkeep_expiry_candidates_total` | counter | none | Idle actors the expiry passes found and acted on. |
| `wasmkeep_expiry_kept_total` | counter | `reason` (`changed`, `alarm`, `socket`, `active`, `failed`) | Idle-expiry candidates kept. |

## Cluster, leases and failover

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_forwards_total` | counter | `kind` (`request`, `websocket`, `call`) | Requests and calls sent to the node that owns the actor. |
| `wasmkeep_failover_waits_total` | counter | `kind` (`request`, `websocket`), `outcome` | Requests kept for an actor whose node was dead or stopping, instead of a 503 at once, by how the wait ended. |
| `wasmkeep_failovers_total` | counter | none | Actor leases taken from another node whose lease expired without being released. |
| `wasmkeep_node_lease_writes_total` | counter | none | Node lease (heartbeat) writes that reached the bucket. |
| `wasmkeep_node_lease_failures_total` | counter | none | Node lease writes that failed or timed out. |
| `wasmkeep_lease_reads_behind_total` | counter | none | Node lease reads, made to judge an ownership record, older than a body this process read before: read again. |
| `wasmkeep_lease_reads_stale_total` | counter | none | Ownership judgements refused because every node lease read was older than a body read before. |
| `wasmkeep_lease_reads_unpinned_total` | counter | none | Newest node lease bodies replaced by an older one after older bodies were read for ttl plus grace with none confirming them. |
| `wasmkeep_node_lease_write_milliseconds` | histogram | none | One node lease write, from its send to its answer. |
| `wasmkeep_node_fences_total` | counter | none | Fences this node wrote over a node that stopped renewing its lease. |
| `wasmkeep_node_hellos_total` | counter | none | Signed hellos from peers that just started, each asking for a listing of the nodes. |
| `wasmkeep_releases_after_lapse_total` | counter | none | Actors this node released after its lease ran out, before it saw the lapse. |
| `wasmkeep_listing_seconds` | histogram | none | One listing of the cluster's heartbeats, with the view and pins it refreshes. |
| `wasmkeep_heartbeat_read_milliseconds` | histogram | none | One read of a peer's heartbeat; timed-out ones at the read attempt. |
| `wasmkeep_heartbeat_age_milliseconds` | histogram | none | How old each peer's heartbeat was when this node applied it, by this node's clock. |
| `wasmkeep_heartbeat_read_failures_total` | counter | none | Reads of a peer's heartbeat that failed or timed out. |
| `wasmkeep_heartbeat_early_reads_total` | counter | none | Peers' heartbeats read before the next round because the trust in them would have ended before it. |
| `wasmkeep_signatures_refused_full_total` | counter | `record` (`calls`, `control`, `ingress`) | Node calls refused because the record of used signatures was full. |
| `wasmkeep_signatures_public_dropped_total` | counter | none | Signed calls on the public port, refused anyway, not recorded because their record was full. |
| `wasmkeep_signatures_before_start_total` | counter | none | Node calls refused because they were signed before this node started. |
| `wasmkeep_signatures_bad_total` | counter | none | Node calls refused because their signature did not hold (bad, expired or misaddressed). |
| `wasmkeep_websocket_owner_closes_total` | counter | `reason` (`event`, `sweep`, `lease_lapse`, `drain`, `move`) | WebSockets closed with 1012 because this node no longer owned their actor. |
| `wasmkeep_websocket_sweep_reads_total` | counter | none | Ownership records the WebSocket owner sweep read. |
| `wasmkeep_websocket_sweep_unreadable_total` | counter | `read` (`owner`, `parked`) | Ownership records the sweep could not decode and passed over. |
| `wasmkeep_parked_records` | gauge | none | Ownership records kept for actors that hibernated with sockets open on this node. |
| `wasmkeep_parked_wakes_total` | counter | none | Wakes of an actor whose record was parked on this node. |
| `wasmkeep_parked_releases_total` | counter | `reason` (`last_socket`, `drain`, `move`, `lapse`, `stop`) | Parked ownership records released. |

## Placement

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_placements_total` | counter | app, `kind`, `outcome` | Wake-ups this node sent to another node ([ADR 0041](../adr/0041-placement-at-wake-up.md)). |
| `wasmkeep_placements_received_total` | counter | app, `outcome` | Placed wake-ups received. |
| `wasmkeep_placement_kept_total` | counter | app, `reason` | Placeable wake-ups kept on this node. |
| `wasmkeep_placement_apart_total` | counter | app, `outcome` | Wake-ups of an actor whose companion this node held (apart). |
| `wasmkeep_placement_spread_total` | counter | app, `outcome` | Wake-ups of a big room's edge spread over the live nodes (spread). |
| `wasmkeep_placement_fill` | gauge | app, `kind` (`residents`, `sockets`) | This node id's fill, in thousandths. |

## Bucket, restore and history

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_bucket_requests_total` | counter | `operation` (`get`, `head`, `put`, `delete`, `list`, `copy`, `rename`, `multipart`), `status` (`ok`, `not_found`, `precondition`, `error`) | Requests to the bucket. |
| `wasmkeep_bucket_request_seconds` | histogram | `operation`, `status` | How long a bucket request took; a listing counts to its end or drop. |
| `wasmkeep_restore_read_seconds` | histogram | none | One object a restore read from the bucket, resends included. |
| `wasmkeep_restore_failures_total` | counter | none | Restores that failed or ran out of time; the next event restores again. |
| `wasmkeep_snapshot_upload_failures_total` | counter | none | Snapshot uploads not acknowledged; a periodic one is tried again. |
| `wasmkeep_idle_snapshots_total` | counter | `outcome` (`created`, `existed`, `failed`, `refused`, `busy`) | Snapshots at the head taken when an actor hibernates. |
| `wasmkeep_idle_compactions_total` | counter | `outcome` (`done`, `continued`, `stopped`, `changed`, `nothing`, `refused`, `busy`) | Idle passes a retention window after an actor's release, and the passes continuing one stopped at its deadline, by outcome. |
| `wasmkeep_idle_passes_dropped_total` | counter | none | Idle passes not remembered because the node's queue was full. |
| `wasmkeep_history_collected_total` | counter | none | Objects of actors' history deleted from the bucket by garbage collection. |
| `wasmkeep_history_collect_failures_total` | counter | none | Garbage-collection passes that failed; the next snapshot tries again. |

## Batched writes

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_batched_waits_total` | counter | `reason` (`age`, `bytes`, `budget`) | Times a batched actor's next event waited at the end of the one before. |
| `wasmkeep_event_start_rule_wait_seconds` | histogram | app, `class` | Batched boundary time gating the next event (capture, budget and upload wait). |
| `wasmkeep_unsent_age_seconds` | histogram | none | The oldest uncaptured commit's age at each batched capture. |
| `wasmkeep_segment_events` | histogram | none | Events per batched segment. |
| `wasmkeep_segment_bytes` | histogram | none | Encoded bytes per batched segment. |
| `wasmkeep_segment_upload_seconds` | histogram | none | A segment's upload, from its first PUT to the acknowledgement; failed uploads are not observed. |
| `wasmkeep_segment_upload_retries_total` | counter | none | PUTs of a segment sent again after an error, a stall or a race. |
| `wasmkeep_segment_upload_failures_total` | counter | none | Segment uploads given up without an acknowledgement. |
| `wasmkeep_flush_seconds` | histogram | none | How long a batched flush took. |
| `wasmkeep_upload_budget_bytes_in_use` | gauge | none | Bytes of the node's budget of segments in flight, in use now. |
| `wasmkeep_budget_share_part` | gauge | `budget` (`uploads`, `restore_reads`, `restore_megabytes`), app | Each app's equal part of a shared budget; half of it is a floor never lent. |
| `wasmkeep_budget_share_in_use` | gauge | `budget`, app | What each app holds now of a shared budget. |
| `wasmkeep_plain_events_dropped_total` | counter | `reason` (`stop`, `failure`, `lapse`, `lost_lease`, `panic`, `app_deleted`, `unbarriered`) | Events whose writes were committed but not acknowledged when a batched hold ended without a flush. |

## Front and static assets

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_front_requests_total` | counter | app, `outcome` (`ok`, `handoff`, `busy`, `unconfigured`, `cpu_budget`, `wall`, `trap`, `no_response`, `client_gone`) | Front requests. |
| `wasmkeep_front_request_seconds` | histogram | app | How long front requests took, to the response's last byte. |
| `wasmkeep_front_cpu_seconds` | histogram | app | Guest CPU time each front request was charged against its budget. |
| `wasmkeep_front_fuel` | histogram | app | Wasmtime fuel units each front request consumed; excludes host work. |
| `wasmkeep_front_slots_in_use` | gauge | none | Front requests running now. |
| `wasmkeep_front_calls_total` | counter | app, `outcome` (`ok`, `busy`, `timeout`, `failed`) | `actors.call` from the front. |
| `wasmkeep_asset_requests_total` | counter | app, `outcome` | Static asset requests. |
| `wasmkeep_asset_fetches_total` | counter | app, `outcome` | Asset fetches from the bucket. |
| `wasmkeep_asset_bytes_total` | counter | app, `encoding` | Asset bytes served. |
| `wasmkeep_asset_cache_bytes` | gauge | app | Bytes of the asset cache. |

## Files

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wasmkeep_blob_ops_total` | counter | app, `op`, `outcome` | File calls. |
| `wasmkeep_blob_bytes_total` | counter | app, `direction` (`in`, `out`) | Bytes of files through the host, node links included. |
| `wasmkeep_blob_links_total` | counter | app, `kind` (`download`, `upload`) | Links to files made. |
| `wasmkeep_blob_links_mode` | gauge | none | 1 when this process presigns downloads, 0 for node links only. |
| `wasmkeep_blob_node_link_requests_total` | counter | app, `outcome` (`ok`, `expired`, `bad_token`, `wrong_app`, `exists`, `wrong_size`) | Node-link requests answered. |
| `wasmkeep_blob_sweeps_total` | counter | app, `outcome` (`swept`, `not_deleted`, `final`, `aged_out`, `failed`, `old_epoch`) | Sweep records of deleted actors' files this node acted on. |
| `wasmkeep_blob_stored_bytes` | gauge | app | Bytes of an app's files at its last hourly count. |
| `wasmkeep_blob_stored_files` | gauge | app | Files of an app at its last hourly count. |
