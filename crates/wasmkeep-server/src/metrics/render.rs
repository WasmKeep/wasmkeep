//! The metrics page: every family, in the order the page lists them.

use super::*;

/// [`render_for`], from `m`, for the nodes `identities` name (at least the
/// implicit app's).
pub(super) fn render_with(m: &Metrics, gauges: Gauges, identities: &[Identity]) -> String {
    let implicit = [Identity::default()];
    let identities = if identities.is_empty() {
        &implicit[..]
    } else {
        identities
    };
    let apps: Vec<String> = identities.iter().map(Identity::app_labels).collect();
    let mut out = String::new();
    ALLOCATOR.render(&mut out);
    let head = |out: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    };
    let counter = |out: &mut String, name: &str, help: &str, v: u64| {
        head(out, name, "counter", help);
        let _ = writeln!(out, "{name} {v}");
    };
    let gauge = |out: &mut String, name: &str, help: &str, v: usize| {
        head(out, name, "gauge", help);
        let _ = writeln!(out, "{name} {v}");
    };
    let family = |out: &mut String, name: &str, help: &str, f: &Family<AtomicU64>| {
        head(out, name, "counter", help);
        f.each(|labels, v| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", v.load(Ordering::Relaxed));
        });
    };

    head(
        &mut out,
        "wasmkeep_info",
        "gauge",
        "Always 1: the running version, and the tenant, app and node id (in a cluster) of the node, for joining with other series.",
    );
    for identity in identities {
        let mut info = vec![
            ("version", wasmkeep_cluster::VERSION),
            ("tenant", identity.tenant.as_str()),
            ("app", identity.app.as_str()),
        ];
        if !identity.node.is_empty() {
            info.push(("node", identity.node.as_str()));
        }
        let _ = writeln!(out, "wasmkeep_info{{{}}} 1", render_labels(&info));
    }
    gauge(
        &mut out,
        "wasmkeep_actors_resident",
        "Actors resident in memory.",
        gauges.resident,
    );
    for (name, help, value) in [
        (
            "wasmkeep_socket_sessions",
            "Process socket sessions including relays and opening upgrades.",
            gauges.socket_sessions,
        ),
        (
            "wasmkeep_socket_limit_requested",
            "Requested process socket sessions.",
            gauges.socket_requested,
        ),
        (
            "wasmkeep_socket_limit_effective",
            "Effective process socket sessions after descriptor budgeting.",
            gauges.socket_effective,
        ),
        (
            "wasmkeep_socket_actor_limit_requested",
            "Requested sockets per actor.",
            gauges.socket_actor_requested,
        ),
        (
            "wasmkeep_socket_actor_limit_effective",
            "Sockets per actor bounded by app and process ceilings.",
            gauges.socket_actor_effective,
        ),
    ] {
        gauge(&mut out, name, help, value);
    }
    gauge(
        &mut out,
        "wasmkeep_websockets_open",
        "Open WebSockets.",
        gauges.websockets,
    );
    gauge(
        &mut out,
        "wasmkeep_ready",
        "1 when /_wasmkeep/ready answers 200.",
        usize::from(gauges.not_ready.is_none()),
    );
    head(
        &mut out,
        "wasmkeep_not_ready",
        "gauge",
        "Why the node is not ready: one series per reason, 1 for the reason it is not ready, 0 for the others.",
    );
    let not_ready = |out: &mut String, reason: &str, v: u8| {
        let _ = writeln!(
            out,
            "wasmkeep_not_ready{{{}}} {v}",
            render_labels(&[("reason", reason)])
        );
    };
    for reason in NOT_READY_REASONS {
        not_ready(&mut out, reason, u8::from(gauges.not_ready == Some(reason)));
    }
    if let Some(reason) = gauges.not_ready
        && !NOT_READY_REASONS.contains(&reason)
    {
        not_ready(&mut out, reason, 1);
    }
    gauge(
        &mut out,
        "wasmkeep_classes",
        "Actor classes served.",
        gauges.classes,
    );
    head(
        &mut out,
        "wasmkeep_events_total",
        "counter",
        "Events run, by tenant, app, class, kind and outcome.",
    );
    // Recorded with their app (ADR 0038, section 9).
    m.events.each(|labels, v| {
        let _ = writeln!(
            out,
            "wasmkeep_events_total{{{labels}}} {}",
            v.load(Ordering::Relaxed)
        );
    });
    head(
        &mut out,
        "wasmkeep_event_duration_seconds",
        "histogram",
        "How long guest events ran, by tenant, app, class and kind (fetch, alarm, websocket, other).",
    );
    head(
        &mut out,
        "wasmkeep_event_fuel",
        "histogram",
        "Wasmtime fuel units consumed per guest event; excludes host work.",
    );
    m.event_fuel
        .each(|labels, h| h.0.render_labels(&mut out, "wasmkeep_event_fuel", labels));
    m.event_seconds.each(|labels, h| {
        h.render(&mut out, "wasmkeep_event_duration_seconds", labels);
    });
    family(
        &mut out,
        "wasmkeep_alarm_stale_hints_total",
        "Stale alarm hint clear attempts, by tenant, app and class.",
        &m.alarm_stale_hints,
    );
    family(
        &mut out,
        "wasmkeep_alarm_deliveries_total",
        "Alarm deliveries run in actor mailboxes, by tenant, app, class and outcome (ran, stale, duplicate, deferred).",
        &m.alarm_deliveries,
    );
    for (name, help, durations) in [
        (
            "wasmkeep_mailbox_host_work_seconds",
            "Exclusive mailbox host work time, by tenant, app, class and work.",
            &m.mailbox_host_work_seconds,
        ),
        (
            "wasmkeep_mailbox_wait_seconds",
            "Time from accepted enqueue attempt to mailbox job start, by tenant, app and class.",
            &m.mailbox_wait_seconds,
        ),
        (
            "wasmkeep_event_start_rule_wait_seconds",
            "Batched boundary time gating the next event (capture, budget and upload wait), by tenant, app and class.",
            &m.event_start_rule_wait_seconds,
        ),
    ] {
        head(&mut out, name, "histogram", help);
        durations.each(|labels, h| h.render(&mut out, name, labels));
    }
    family(
        &mut out,
        "wasmkeep_event_cuts_total",
        "Events stopped by the host, by reason (wall clock, lost lease, failed replica).",
        &m.cuts,
    );
    counter(
        &mut out,
        "wasmkeep_cpu_budget_exceeded_total",
        "Guest calls stopped for using their CPU budget.",
        wasmkeep_runtime::cpu_budget_exceeded_total(),
    );
    head(
        &mut out,
        "wasmkeep_instantiate_seconds",
        "histogram",
        "Instantiating an actor's component.",
    );
    m.instantiate_seconds
        .render(&mut out, "wasmkeep_instantiate_seconds", "");
    counter(
        &mut out,
        "wasmkeep_wakes_total",
        "Actors brought into memory (created or woken).",
        m.activations.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_requests_abandoned_total",
        "Requests and actor calls not run because their sender had gone while they waited in the actor's queue.",
        m.requests_abandoned.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_hibernations_total",
        "Actors that left memory (idle, evicted, deleted or shut down).",
        m.hibernations.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_forwards_total",
        "Requests and calls sent to the node owning the actor, by kind.",
        &m.forwards,
    );
    family(
        &mut out,
        "wasmkeep_failover_waits_total",
        "Requests kept for an actor whose node was dead or stopping instead of a 503 at once, by kind and how the wait ended.",
        &m.failover_waits,
    );
    head(
        &mut out,
        "wasmkeep_durable_write_seconds",
        "histogram",
        "exec-durable, from the call until the write is durable.",
    );
    m.durable_write_seconds
        .render(&mut out, "wasmkeep_durable_write_seconds", "");
    head(
        &mut out,
        "wasmkeep_listing_seconds",
        "histogram",
        "One listing of the cluster's heartbeats, with the view and pins it refreshes.",
    );
    m.listing_seconds
        .render(&mut out, "wasmkeep_listing_seconds", "");
    head(
        &mut out,
        "wasmkeep_heartbeat_read_milliseconds",
        "histogram",
        "One read of a peer's heartbeat, timed out ones at the read attempt.",
    );
    m.heartbeat_read_ms
        .0
        .render(&mut out, "wasmkeep_heartbeat_read_milliseconds");
    head(
        &mut out,
        "wasmkeep_heartbeat_age_milliseconds",
        "histogram",
        "How old each peer's heartbeat was when this node applied it, by this node's clock.",
    );
    m.heartbeat_age_ms
        .0
        .render(&mut out, "wasmkeep_heartbeat_age_milliseconds");
    head(
        &mut out,
        "wasmkeep_node_lease_write_milliseconds",
        "histogram",
        "One node lease (heartbeat) write of this node, from its send to its answer.",
    );
    m.node_lease_write_ms
        .0
        .render(&mut out, "wasmkeep_node_lease_write_milliseconds");
    counter(
        &mut out,
        "wasmkeep_heartbeat_read_failures_total",
        "Reads of a peer's heartbeat that failed or timed out; the peer is kept as last read until its own horizon.",
        m.heartbeat_read_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_heartbeat_early_reads_total",
        "Peers' heartbeats read before the next round, the trust in them ending before it could land.",
        m.heartbeat_early_reads.load(Ordering::Relaxed),
    );
    head(
        &mut out,
        "wasmkeep_restore_read_seconds",
        "histogram",
        "One object a restore read from the bucket, from its request to its last byte, resends included.",
    );
    m.restore_read_seconds
        .render(&mut out, "wasmkeep_restore_read_seconds", "");
    counter(
        &mut out,
        "wasmkeep_restore_failures_total",
        "Restores from the bucket that failed or ran out of time; the next event restores again.",
        m.restore_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_guest_log_lines_total",
        "Lines guests printed that reached the node's log.",
        m.guest_log_lines.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_guest_log_dropped_total",
        "Lines guests printed that were dropped over the line rate.",
        m.guest_log_dropped.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_lease_writes_total",
        "Node lease (heartbeat) writes that reached the bucket.",
        m.node_lease_writes.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_lease_failures_total",
        "Node lease (heartbeat) writes that failed or timed out.",
        m.node_lease_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_lease_reads_behind_total",
        "Node lease reads, made to judge an ownership record, older than a body this process read before: read again.",
        m.lease_reads_behind.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_lease_reads_stale_total",
        "Ownership judgements refused because every node lease read was older than a body read before.",
        m.lease_reads_stale.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_lease_reads_unpinned_total",
        "Newest node lease bodies replaced by an older one after older bodies were read for ttl plus grace with none confirming them.",
        m.lease_reads_unpinned.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_fences_total",
        "Fences this node wrote over a node that stopped renewing its lease.",
        m.node_fences.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_hellos_total",
        "Signed hellos from peers that just started, each asking for a listing of the nodes.",
        m.hellos_received.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_releases_after_lapse_total",
        "Actors this node released after its lease ran out, before it saw the lapse.",
        m.releases_after_lapse.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_alarm_admission_retries_total",
        "Alarm deliveries rescheduled after mailbox admission refusal.",
        m.alarm_admission_retries.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_alarm_dropped_deliveries_total",
        "Alarm deliveries the mailbox dropped before they answered, rescheduled with backoff.",
        m.alarm_dropped_deliveries.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_trace_spans_dropped_total",
        "Spans dropped by the bounded export queue.",
        m.trace_spans_dropped.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_trace_exports_total",
        "Trace export batches by outcome.",
        &m.trace_exports,
    );
    family(
        &mut out,
        "wasmkeep_bucket_requests_total",
        "Requests to the bucket, by operation and status.",
        &m.bucket_requests,
    );
    head(
        &mut out,
        "wasmkeep_bucket_request_seconds",
        "histogram",
        "How long a bucket request took, by operation and status; a listing counts to its end or drop.",
    );
    m.bucket_request_seconds.each(|labels, h| {
        h.render(&mut out, "wasmkeep_bucket_request_seconds", labels);
    });
    head(
        &mut out,
        "wasmkeep_runtime_schedule_lag_seconds",
        "histogram",
        "How much later than asked a task sleeping 100 ms woke: the async runtime's scheduling delay.",
    );
    m.runtime_schedule_lag_seconds
        .render(&mut out, "wasmkeep_runtime_schedule_lag_seconds", "");
    counter(
        &mut out,
        "wasmkeep_failovers_total",
        "Actor leases taken from another node whose lease expired without being released.",
        m.failovers.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_history_collected_total",
        "Objects of actors' history deleted from the bucket by garbage collection.",
        m.history_collected.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_history_collect_failures_total",
        "Garbage-collection passes that failed; the next snapshot tries again.",
        m.history_collect_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_snapshot_upload_failures_total",
        "Snapshot uploads not acknowledged; a periodic one is tried again.",
        m.snapshot_upload_failures.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_idle_snapshots_total",
        "Snapshots at the head taken when an actor hibernates, by outcome.",
        &m.idle_snapshots,
    );
    family(
        &mut out,
        "wasmkeep_idle_compactions_total",
        "Idle passes a retention window after an actor's release, by outcome.",
        &m.idle_compactions,
    );
    family(
        &mut out,
        "wasmkeep_node_lease_removals_total",
        "Released node leases removed by this node, and the rest of its removal rounds, by outcome.",
        &m.lease_removals,
    );
    counter(
        &mut out,
        "wasmkeep_idle_passes_dropped_total",
        "Idle passes not remembered because the node's queue was full.",
        m.idle_passes_dropped.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_signatures_refused_full_total",
        "Node calls refused because the record of used signatures was full, by record.",
        &m.signatures_refused_full,
    );
    counter(
        &mut out,
        "wasmkeep_signatures_public_dropped_total",
        "Signed calls on the public port, refused anyway, not recorded because their record was full.",
        m.signatures_public_dropped.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_signatures_before_start_total",
        "Node calls refused because they were signed before this node started.",
        m.signatures_before_start.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_signatures_bad_total",
        "Node calls refused because their signature did not hold (bad, expired or misaddressed).",
        m.signatures_bad.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_websocket_owner_closes_total",
        "WebSockets closed with 1012 because this node no longer owned their actor, by how it found out.",
        &m.websocket_owner_closes,
    );
    family(
        &mut out,
        "wasmkeep_actor_deletes_total",
        "Actor deletes recorded, by how they were asked for.",
        &m.actor_deletes,
    );
    family(
        &mut out,
        "wasmkeep_actor_delete_failures_total",
        "Actor deletes that failed to be recorded (the data stays), by how they were asked for.",
        &m.actor_delete_failures,
    );
    counter(
        &mut out,
        "wasmkeep_expiry_passes_total",
        "Idle-expiry passes this node ran as the collecting node.",
        m.expiry_passes.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_expiry_candidates_total",
        "Idle actors the expiry passes found and acted on.",
        m.expiry_candidates.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_expiry_kept_total",
        "Idle-expiry candidates kept, by reason (the record changed since the listing, an alarm, a socket, active here, or failed).",
        &m.expiry_kept,
    );
    counter(
        &mut out,
        "wasmkeep_websocket_sweep_reads_total",
        "Ownership records the WebSocket owner sweep read.",
        m.websocket_sweep_reads.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_websocket_sweep_unreadable_total",
        "Ownership records the WebSocket owner sweep could not decode and passed over, by read.",
        &m.websocket_sweep_unreadable,
    );
    head(
        &mut out,
        "wasmkeep_parked_records",
        "gauge",
        "Ownership records kept for actors that hibernated with sockets open on this node.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_parked_records {}",
        m.parked_records.load(Ordering::Relaxed)
    );
    counter(
        &mut out,
        "wasmkeep_parked_wakes_total",
        "Wakes of an actor whose record was parked here, on this node.",
        m.parked_wakes.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_parked_releases_total",
        "Parked ownership records released, by reason.",
        &m.parked_releases,
    );
    family(
        &mut out,
        "wasmkeep_plain_events_dropped_total",
        "Events whose writes were committed but not acknowledged when a batched hold ended without a flush, by reason.",
        &m.plain_events_dropped,
    );
    head(
        &mut out,
        "wasmkeep_unsent_age_seconds",
        "histogram",
        "The oldest uncaptured commit's age at each batched capture.",
    );
    m.unsent_age_seconds
        .render(&mut out, "wasmkeep_unsent_age_seconds", "");
    head(
        &mut out,
        "wasmkeep_segment_events",
        "histogram",
        "Events per batched segment.",
    );
    m.segment_events.render(&mut out, "wasmkeep_segment_events");
    head(
        &mut out,
        "wasmkeep_segment_bytes",
        "histogram",
        "Encoded bytes per batched segment.",
    );
    m.segment_bytes.render(&mut out, "wasmkeep_segment_bytes");
    head(
        &mut out,
        "wasmkeep_segment_upload_seconds",
        "histogram",
        "A segment's upload, from its first PUT to the bucket's acknowledgement, resends included; failed uploads are not observed.",
    );
    m.segment_upload_seconds
        .render(&mut out, "wasmkeep_segment_upload_seconds", "");
    counter(
        &mut out,
        "wasmkeep_segment_upload_retries_total",
        "PUTs of a segment sent again after an error, a stall or a race with an earlier attempt.",
        m.segment_upload_retries.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_segment_upload_failures_total",
        "Segment uploads given up without an acknowledgement.",
        m.segment_upload_failures.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_batched_waits_total",
        "Times a batched actor's next event waited at the end of the one before, by reason (age, bytes, budget).",
        &m.batched_waits,
    );
    head(
        &mut out,
        "wasmkeep_flush_seconds",
        "histogram",
        "How long a batched flush took.",
    );
    m.flush_seconds
        .render(&mut out, "wasmkeep_flush_seconds", "");
    gauge(
        &mut out,
        "wasmkeep_upload_budget_bytes_in_use",
        "Bytes of the node's budget of batched segments in flight in use now.",
        crate::actor::upload_budget_in_use(),
    );
    // Each app's part of the shared budgets and its use (ADR 0038,
    // section 8): none on a node that joined no cluster.
    let shares: [(&str, &crate::shares::Shares); 3] = [
        ("uploads", &crate::actor::UPLOAD_SHARES),
        ("restore_reads", &crate::replica::RESTORE_SLOT_SHARES),
        ("restore_megabytes", &crate::replica::RESTORE_BYTE_SHARES),
    ];
    for (name, help, part) in [
        (
            "wasmkeep_budget_share_part",
            "Each app's equal part of a shared budget (uploads in bytes, restore reads, restore megabytes); half of it is a floor never lent.",
            true,
        ),
        (
            "wasmkeep_budget_share_in_use",
            "What each app holds now of a shared budget.",
            false,
        ),
    ] {
        head(&mut out, name, "gauge", help);
        for (budget, s) in shares {
            s.each(|app, app_part, used| {
                let (tenant, app) = app.split_once('/').unwrap_or((app, ""));
                let labels = render_labels(&[("budget", budget), ("tenant", tenant), ("app", app)]);
                let v = if part { app_part } else { used };
                let _ = writeln!(out, "{name}{{{labels}}} {v}");
            });
        }
    }
    // Recorded with their app (ADR 0038, section 9); the page's own app
    // has every outcome from the first scrape, at 0 until counted.
    let app_family =
        |out: &mut String, name: &str, help: &str, f: &Family<AtomicU64>, outcomes: &[&str]| {
            head(out, name, "counter", help);
            let mut rows = BTreeMap::new();
            for o in outcomes {
                for app in &apps {
                    rows.insert(join_labels(app, &render_labels(&[("outcome", o)])), 0);
                }
            }
            f.each(|labels, v| {
                rows.insert(labels.to_owned(), v.load(Ordering::Relaxed));
            });
            for (labels, v) in rows {
                let _ = writeln!(out, "{name}{{{labels}}} {v}");
            }
        };
    head(
        &mut out,
        "wasmkeep_rate_limit_requests_total",
        "counter",
        "Ingress admission decisions, by outcome.",
    );
    for (label, count) in ["admitted", "quota", "capacity", "invalid_key"]
        .iter()
        .zip(&INGRESS)
    {
        let _ = writeln!(
            out,
            "wasmkeep_rate_limit_requests_total{{outcome=\"{label}\"}} {}",
            count.load(Ordering::Relaxed)
        );
    }
    head(
        &mut out,
        "wasmkeep_rate_limit_active_keys",
        "gauge",
        "Retained ingress client buckets on this process.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_rate_limit_active_keys {}",
        crate::rate_limit::active_keys()
    );
    for (name, kind, family) in [
        (
            "wasmkeep_asset_requests_total",
            "counter",
            &m.asset_requests,
        ),
        ("wasmkeep_asset_fetches_total", "counter", &m.asset_fetches),
        ("wasmkeep_asset_bytes_total", "counter", &m.asset_bytes),
        ("wasmkeep_asset_cache_bytes", "gauge", &m.asset_cache),
    ] {
        head(&mut out, name, kind, "Static assets, by tenant and app.");
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    for (name, kind, help, family) in [
        (
            "wasmkeep_placements_total",
            "counter",
            "Wake-ups this node sent to another node (ADR 0041), by tenant, app, kind and outcome.",
            &m.placements,
        ),
        (
            "wasmkeep_placements_received_total",
            "counter",
            "Placed wake-ups received, by tenant, app and outcome.",
            &m.placements_received,
        ),
        (
            "wasmkeep_placement_kept_total",
            "counter",
            "Placeable wake-ups kept on this node, by tenant, app and reason.",
            &m.placement_kept,
        ),
        (
            "wasmkeep_placement_apart_total",
            "counter",
            "Wake-ups of an actor whose companion this node held (ADR 0041), by tenant, app and outcome.",
            &m.placement_apart,
        ),
        (
            "wasmkeep_placement_spread_total",
            "counter",
            "Wake-ups of a big room's edge spread over the live nodes (ADR 0040), by tenant, app and outcome.",
            &m.placement_spread,
        ),
        (
            "wasmkeep_placement_fill",
            "gauge",
            "This node id's fill in thousandths, by tenant, app and kind.",
            &m.placement_fill,
        ),
        (
            "wasmkeep_rebalance_misplaced",
            "gauge",
            "Spread actors this node holds whose home is another node (ADR 0040), by tenant and app.",
            &m.rebalance_misplaced,
        ),
        (
            "wasmkeep_rebalance_moves_total",
            "counter",
            "Rebalance moves of spread actors to their home, by tenant, app and outcome.",
            &m.rebalance_moves,
        ),
        (
            "wasmkeep_rebalance_sockets_closed_total",
            "counter",
            "Sockets closed with 1012 by rebalance moves, by tenant and app.",
            &m.rebalance_sockets_closed,
        ),
        (
            "wasmkeep_rebalance_paused",
            "gauge",
            "1 for the reason the rebalance pass is paused, by tenant, app and reason.",
            &m.rebalance_paused,
        ),
    ] {
        head(&mut out, name, kind, help);
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    app_family(
        &mut out,
        "wasmkeep_blob_sweeps_total",
        "Sweep records of deleted actors' files this node acted on, by tenant, app and outcome.",
        &m.blob_sweeps,
        &BLOB_SWEEP_OUTCOMES,
    );
    app_family(
        &mut out,
        "wasmkeep_blob_node_link_requests_total",
        "Node-link requests answered, by tenant, app and outcome.",
        &m.blob_node_links,
        &BLOB_NODE_LINK_OUTCOMES,
    );
    for (name, kind, help, family) in [
        (
            "wasmkeep_blob_ops_total",
            "counter",
            "File calls, by tenant, app, op and outcome.",
            &m.blob_ops,
        ),
        (
            "wasmkeep_blob_bytes_total",
            "counter",
            "Bytes of files through the host, node links included, by tenant, app and direction.",
            &m.blob_bytes,
        ),
        (
            "wasmkeep_blob_links_total",
            "counter",
            "Links to files made, by tenant, app and kind.",
            &m.blob_links,
        ),
        (
            "wasmkeep_blob_stored_bytes",
            "gauge",
            "Bytes of an app's files at its last hourly count.",
            &m.blob_stored_bytes,
        ),
        (
            "wasmkeep_blob_stored_files",
            "gauge",
            "Files of an app at its last hourly count.",
            &m.blob_stored_files,
        ),
    ] {
        head(&mut out, name, kind, help);
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    head(
        &mut out,
        "wasmkeep_blob_links_mode",
        "gauge",
        "1 when this process presigns downloads (the start proof passed), 0 for node links only.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_blob_links_mode {}",
        m.blob_links_mode.load(Ordering::Relaxed)
    );
    app_family(
        &mut out,
        "wasmkeep_front_requests_total",
        "Front requests, by tenant, app and outcome.",
        &m.front_requests,
        &FRONT_REQUEST_OUTCOMES,
    );
    head(
        &mut out,
        "wasmkeep_front_request_seconds",
        "histogram",
        "How long front requests took, to the response's last byte, by tenant and app.",
    );
    m.front_request_seconds.each(|labels, h| {
        if h.count.load(Ordering::Relaxed) > 0 {
            h.render(&mut out, "wasmkeep_front_request_seconds", labels);
        }
    });
    head(
        &mut out,
        "wasmkeep_front_cpu_seconds",
        "histogram",
        "Guest CPU time each front request was charged against its budget, instantiation included, by tenant and app.",
    );
    m.front_cpu_seconds.each(|labels, h| {
        if h.count.load(Ordering::Relaxed) > 0 {
            h.render(&mut out, "wasmkeep_front_cpu_seconds", labels);
        }
    });
    head(
        &mut out,
        "wasmkeep_front_fuel",
        "histogram",
        "Wasmtime fuel units each front request consumed, instantiation included; excludes host work.",
    );
    m.front_fuel
        .each(|labels, h| h.0.render_labels(&mut out, "wasmkeep_front_fuel", labels));
    head(
        &mut out,
        "wasmkeep_front_slots_in_use",
        "gauge",
        "Front requests running now.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_front_slots_in_use {}",
        m.front_slots_in_use.load(Ordering::Relaxed)
    );
    app_family(
        &mut out,
        "wasmkeep_front_calls_total",
        "actors.call from the front, by tenant, app and outcome.",
        &m.front_calls,
        &FRONT_CALL_OUTCOMES,
    );
    head(
        &mut out,
        "wasmkeep_admission_refused_total",
        "counter",
        "Requests, WebSocket upgrades and actor calls refused before their event ran, by reason.",
    );
    for (reason, n) in Refusal::ALL.iter().zip(&m.admission_refused) {
        let _ = writeln!(
            out,
            "wasmkeep_admission_refused_total{{{}}} {}",
            render_labels(&[("reason", reason.label())]),
            n.load(Ordering::Relaxed)
        );
    }
    out
}
