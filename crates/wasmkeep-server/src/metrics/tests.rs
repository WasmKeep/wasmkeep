use super::*;

/// Each app's part of the process's shared budgets and what it uses,
/// once per budget and app.
#[test]
fn allocator_metrics_count_actual_results_and_duration() {
    let metrics = AllocatorMetrics::default();
    for released in [false, true, true] {
        metrics.record(wasmkeep_runtime::AllocatorTrim {
            released,
            duration: Duration::from_millis(10),
        });
    }
    let mut out = String::new();
    metrics.render(&mut out);
    assert!(out.contains("wasmkeep_allocator_trims_total{released=\"false\"} 1\n"));
    assert!(out.contains("wasmkeep_allocator_trims_total{released=\"true\"} 2\n"));
    assert!(out.contains("wasmkeep_allocator_trim_seconds_count 3\n"));
    assert!(out.contains("wasmkeep_allocator_trim_seconds_sum 0.03\n"));
    assert!(out.contains("wasmkeep_allocator_trim_seconds_bucket{le=\"0.01\"} 3\n"));
}

#[test]
fn budget_shares_are_on_the_page_by_app() {
    let _shares = [
        crate::actor::UPLOAD_SHARES.join("acme/shop"),
        crate::actor::UPLOAD_SHARES.join("acme/blog"),
    ];
    let _held = crate::actor::UPLOAD_SHARES
        .try_acquire("acme/shop", 1000)
        .unwrap();
    let page = render(Gauges::default());
    let half = crate::replica::NODE_UPLOAD_BYTES / 2;
    for line in [
        format!(
            "\nwasmkeep_budget_share_part{{budget=\"uploads\",tenant=\"acme\",app=\"shop\"}} {half}\n"
        ),
        format!(
            "\nwasmkeep_budget_share_part{{budget=\"uploads\",tenant=\"acme\",app=\"blog\"}} {half}\n"
        ),
        "\nwasmkeep_budget_share_in_use{budget=\"uploads\",tenant=\"acme\",app=\"shop\"} 1000\n"
            .to_owned(),
        "\nwasmkeep_budget_share_in_use{budget=\"uploads\",tenant=\"acme\",app=\"blog\"} 0\n"
            .to_owned(),
    ] {
        assert!(page.contains(&line), "no {line} in\n{page}");
    }
    assert_eq!(
        page.matches("# TYPE wasmkeep_budget_share_part ").count(),
        1
    );
}

#[test]
fn a_process_of_several_apps_has_an_info_line_per_app() {
    let id = |app: &str, node: &str| Identity {
        tenant: "acme".into(),
        app: app.into(),
        node: node.into(),
    };
    let page = render_for_apps(
        Gauges::default(),
        &[id("shop", "shop-1"), id("blog", "blog-1")],
    );
    let v = wasmkeep_cluster::VERSION;
    for line in [
        format!(
            "\nwasmkeep_info{{version=\"{v}\",tenant=\"acme\",app=\"shop\",node=\"shop-1\"}} 1\n"
        ),
        format!(
            "\nwasmkeep_info{{version=\"{v}\",tenant=\"acme\",app=\"blog\",node=\"blog-1\"}} 1\n"
        ),
        "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"blog\",outcome=\"ok\"} ".to_owned(),
    ] {
        assert!(page.contains(&line), "no {line} in\n{page}");
    }
    assert_eq!(page.matches("# TYPE wasmkeep_info ").count(), 1);
}

#[test]
fn a_process_of_several_apps_sums_their_gauges() {
    let g = |resident, websockets, not_ready| Gauges {
        socket_sessions: 7,
        socket_requested: 100,
        socket_effective: 90,
        socket_actor_requested: 10,
        socket_actor_effective: 10,
        resident,
        websockets,
        classes: 2,
        not_ready,
    };
    let p = Gauges::of_process(&[g(3, 1, None), g(4, 2, Some("disk almost full"))]);
    assert_eq!((p.resident, p.websockets, p.classes), (7, 3, 4));
    assert_eq!((p.socket_sessions, p.socket_effective), (7, 90));
    assert_eq!(p.not_ready, Some("disk almost full"));
    assert_eq!(Gauges::of_process(&[]).resident, 0);
}

/// Two apps' fronts in one process: each request and call is counted
/// under its own app, and the sum is the work done.
#[test]
fn front_work_is_counted_under_its_own_app() {
    let (shop, blog) = (app_labels("acme", "shop"), app_labels("acme", "blog"));
    front_request(&shop, "ok");
    front_request(&shop, "ok");
    front_request(&blog, "busy");
    front_request_took(&shop, Duration::from_millis(2));
    front_request_took(&blog, Duration::from_millis(2));
    front_call(&blog, "timeout");
    assert_eq!(METRICS.front_requests.total_in(&shop), 2);
    assert_eq!(METRICS.front_requests.total_in(&blog), 1);
    let page = render_for(Gauges::default(), &Identity::default());
    for line in [
        "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"shop\",outcome=\"ok\"} 2\n",
        "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"blog\",outcome=\"busy\"} 1\n",
        "\nwasmkeep_front_request_seconds_count{tenant=\"acme\",app=\"shop\"} 1\n",
        "\nwasmkeep_front_request_seconds_count{tenant=\"acme\",app=\"blog\"} 1\n",
        "\nwasmkeep_front_calls_total{tenant=\"acme\",app=\"blog\",outcome=\"timeout\"} 1\n",
    ] {
        assert!(page.contains(line), "no {line} in\n{page}");
    }
}

/// The front's series are on an app's page at 0 from the first scrape,
/// under that app only.
#[test]
fn an_apps_page_has_its_front_series_at_zero() {
    let page = render_for(
        Gauges::default(),
        &Identity {
            tenant: "acme".into(),
            app: "shop".into(),
            node: String::new(),
        },
    );
    for o in FRONT_REQUEST_OUTCOMES {
        let line = format!(
            "\nwasmkeep_front_requests_total{{tenant=\"acme\",app=\"shop\",outcome=\"{o}\"}} 0\n"
        );
        assert!(page.contains(&line), "no {line} in\n{page}");
    }
    assert!(
        !page.contains("wasmkeep_front_requests_total{tenant=\"default\""),
        "{page}"
    );
}

/// Two apps of one process: each event is counted once, under its own
/// app, whatever identity the page is rendered for; the sum over apps
/// is the work done, and the process's series appear once.
#[test]
fn events_are_counted_under_their_own_app() {
    let (shop, blog) = (app_labels("acme", "shop"), app_labels("acme", "blog"));
    let ms = Duration::from_millis(3);
    for _ in 0..2 {
        record_event(&shop, "c", "request", "ok", ms);
    }
    for _ in 0..3 {
        record_event(&blog, "c", "request", "ok", ms);
    }
    assert_eq!(METRICS.events.total_in(&shop), 2);
    assert_eq!(METRICS.events.total_in(&blog), 3);
    assert_eq!(METRICS.events.total(), 5);
    for identity in [
        Identity {
            tenant: "acme".into(),
            app: "shop".into(),
            node: "n1".into(),
        },
        Identity::default(),
    ] {
        let page = render_for(Gauges::default(), &identity);
        for line in [
            "\nwasmkeep_events_total{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"request\",outcome=\"ok\"} 2\n",
            "\nwasmkeep_events_total{tenant=\"acme\",app=\"blog\",class=\"c\",kind=\"request\",outcome=\"ok\"} 3\n",
            "\nwasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} 2\n",
            "\nwasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"blog\",class=\"c\",kind=\"fetch\"} 3\n",
        ] {
            assert!(page.contains(line), "no {line} in\n{page}");
        }
        let events: u64 = page
            .lines()
            .filter(|l| l.starts_with("wasmkeep_events_total{"))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
            .sum();
        assert_eq!(events, 5, "{page}");
        assert_eq!(page.matches("\nwasmkeep_info{").count(), 1);
        assert_eq!(page.matches("\nwasmkeep_actors_resident ").count(), 1);
    }
}

#[test]
fn a_family_total_sums_every_label_set() {
    let f: Family<AtomicU64> = Family::default();
    assert_eq!(f.total(), 0);
    inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
    inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
    inc(&f.get(&[("class", "b"), ("outcome", "failed")]));
    assert_eq!(f.total(), 3);
}

#[test]
fn histograms_are_cumulative_and_labels_escaped() {
    let h = Histogram::<Standard>::default();
    h.observe(Duration::from_micros(50));
    h.observe(Duration::from_millis(3));
    h.observe(Duration::from_secs(60));
    let mut out = String::new();
    h.render(&mut out, "t", "class=\"a\"");
    assert!(
        out.contains("t_bucket{class=\"a\",le=\"0.0001\"} 1\n"),
        "{out}"
    );
    assert!(
        out.contains("t_bucket{class=\"a\",le=\"0.005\"} 2\n"),
        "{out}"
    );
    assert!(out.contains("t_bucket{class=\"a\",le=\"30\"} 2\n"), "{out}");
    assert!(
        out.contains("t_bucket{class=\"a\",le=\"+Inf\"} 3\n"),
        "{out}"
    );
    assert!(out.contains("t_count{class=\"a\"} 3\n"), "{out}");
    assert_eq!(
        render_labels(&[("class", "a\"b\\c\nd")]),
        "class=\"a\\\"b\\\\c\\nd\""
    );
}

#[test]
fn readiness_and_its_reason_are_on_the_page() {
    let page = render(Gauges::default());
    assert!(page.contains("\nwasmkeep_ready 1\n"), "{page}");
    assert!(
        page.lines()
            .filter(|l| l.starts_with("wasmkeep_not_ready{"))
            .all(|l| l.ends_with("} 0")),
        "{page}"
    );
    let page = render(Gauges {
        not_ready: Some("bucket unreachable (node lease not renewed)"),
        ..Gauges::default()
    });
    assert!(page.contains("\nwasmkeep_ready 0\n"), "{page}");
    assert!(
        page.contains(
            "wasmkeep_not_ready{reason=\"bucket unreachable (node lease not renewed)\"} 1\n"
        ),
        "{page}"
    );
}

#[test]
fn alarm_admission_retries_are_exposed_without_actor_labels() {
    let page = render(Gauges::default());
    assert!(page.contains("# TYPE wasmkeep_alarm_admission_retries_total counter\n"));
    assert!(page.contains("\nwasmkeep_alarm_admission_retries_total "));
    assert!(!page.contains("wasmkeep_alarm_admission_retries_total{"));
}

#[test]
fn a_family_keeps_one_value_per_label_set() {
    let f: Family<AtomicU64> = Family::default();
    inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
    inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
    inc(&f.get(&[("class", "b"), ("outcome", "ok")]));
    let mut seen = Vec::new();
    f.each(|labels, v| seen.push((labels.to_owned(), v.load(Ordering::Relaxed))));
    assert_eq!(
        seen,
        [
            ("class=\"a\",outcome=\"ok\"".to_owned(), 2),
            ("class=\"b\",outcome=\"ok\"".to_owned(), 1)
        ]
    );
}

/// The node's tenant and app for [`render_for`]: `acme/shop` on `n1`.
fn shop() -> Identity {
    Identity {
        tenant: "acme".into(),
        app: "shop".into(),
        node: "n1".into(),
    }
}

#[test]
fn series_with_a_class_label_carry_the_tenant_and_app() {
    record_event(
        &app_labels("acme", "shop"),
        "c",
        "request",
        "ok",
        Duration::from_millis(3),
    );
    record_event(
        &app_labels("default", "default"),
        "c",
        "request",
        "ok",
        Duration::from_millis(3),
    );
    inc(&METRICS
        .bucket_requests
        .get(&[("operation", "get"), ("status", "ok")]));
    let page = render_for(Gauges::default(), &shop());
    for line in [
        "wasmkeep_events_total{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"request\",outcome=\"ok\"} 1\n",
        "wasmkeep_event_duration_seconds_bucket{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\",le=\"0.005\"} 1\n",
        "wasmkeep_event_duration_seconds_bucket{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\",le=\"+Inf\"} 1\n",
        "wasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} 1\n",
    ] {
        assert!(page.contains(line), "no {line} in\n{page}");
    }
    assert!(
        page.contains(
            "\nwasmkeep_event_duration_seconds_sum{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} "
        ),
        "{page}"
    );
    // Without a node's identity (no actors served): the implicit app.
    let page = render(Gauges::default());
    assert!(
        page.contains("wasmkeep_events_total{tenant=\"default\",app=\"default\",class=\"c\",kind=\"request\",outcome=\"ok\"} 1\n"),
        "{page}"
    );
    // Every series with a class has them (its own app's, recorded with
    // the event: ADR 0038, section 9), and so do the front's counters
    // and histogram (ADR 0023); the others are as they were, and
    // gauges of the process stay unlabelled.
    for l in page.lines().filter(|l| !l.starts_with('#')) {
        if l.contains("class=\"") {
            assert!(
                l.contains("{tenant=\"default\",app=\"default\",")
                    || l.contains("{tenant=\"acme\",app=\"shop\","),
                "{l}"
            );
        } else if (l.starts_with("wasmkeep_front_") && !l.starts_with("wasmkeep_front_slots"))
            || (l.starts_with("wasmkeep_blob_") && !l.starts_with("wasmkeep_blob_links_mode"))
        {
            assert!(l.contains("{tenant=\"default\",app=\"default\","), "{l}");
        } else if !l.starts_with("wasmkeep_info{") {
            assert!(!l.contains("tenant=") && !l.contains("app="), "{l}");
        }
    }
    for line in [
        "\nwasmkeep_bucket_requests_total{operation=\"get\",status=\"ok\"} 1\n",
        "\nwasmkeep_admission_refused_total{reason=\"access\"} 0\n",
        "\nwasmkeep_actors_resident 0\n",
        "\nwasmkeep_websockets_open 0\n",
    ] {
        assert!(page.contains(line), "no {line} in\n{page}");
    }
}

#[test]
fn the_info_series_names_the_version_tenant_app_and_node() {
    let version = wasmkeep_cluster::VERSION;
    let page = render_for(Gauges::default(), &shop());
    assert!(page.contains("# TYPE wasmkeep_info gauge\n"), "{page}");
    assert!(
        page.contains(&format!(
            "\nwasmkeep_info{{version=\"{version}\",tenant=\"acme\",app=\"shop\",node=\"n1\"}} 1\n"
        )),
        "{page}"
    );
    // Not clustered: no node label.
    let page = render(Gauges::default());
    assert!(
        page.contains(&format!(
            "\nwasmkeep_info{{version=\"{version}\",tenant=\"default\",app=\"default\"}} 1\n"
        )),
        "{page}"
    );
    assert_eq!(page.matches("\nwasmkeep_info{").count(), 1, "{page}");
}

/// `docs/monitoring.md` lists every metric on the page.
#[test]
fn the_monitoring_docs_list_every_metric() {
    let docs = include_str!("../../../../docs/monitoring.md");
    let page = render(Gauges::default());
    let names: Vec<&str> = page
        .lines()
        .filter_map(|l| l.strip_prefix("# TYPE ")?.split(' ').next())
        .collect();
    assert!(names.len() > 20, "{names:?}");
    for name in names {
        assert!(
            docs.contains(&format!("`{name}`")) || docs.contains(&format!("`{name}{{")),
            "docs/monitoring.md does not list {name}"
        );
    }
}

/// The alert rules in `deploy/prometheus` name only metrics this page
/// has, with labels it gives them.
#[test]
fn the_alert_rules_use_only_metrics_that_exist() {
    let rules = include_str!("../../../../deploy/prometheus/alerts.yml");
    // Without the comments: the header is prose about the file.
    let exprs: String = rules
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(exprs.matches("- alert: ").count(), 11, "{exprs}");
    // A fresh node's page: the rules' series are there from the start,
    // except those keyed by class, which a class's first event adds.
    let m = Metrics::seeded();
    inc(&m
        .events
        .get(&[("class", "c"), ("kind", "request"), ("outcome", "failed")]));
    let page = render_with(&m, Gauges::default(), &[Identity::default()]);
    let mut names = std::collections::BTreeSet::new();
    let mut rest = exprs.as_str();
    while let Some(at) = rest.find("wasmkeep_") {
        let name: String = rest[at..]
            .chars()
            .take_while(|c| c.is_ascii_lowercase() || *c == '_')
            .collect();
        rest = &rest[at + name.len()..];
        names.insert(name);
    }
    assert!(names.len() >= 7, "{names:?}");
    for name in &names {
        let base = name
            .strip_suffix("_bucket")
            .filter(|b| page.contains(&format!("# TYPE {b} histogram\n")))
            .unwrap_or(name);
        assert!(
            page.contains(&format!("# TYPE {base} ")),
            "an alert rule names {name}, which the metrics page does not have"
        );
    }
    for (metric, label) in [
        ("wasmkeep_events_total", "outcome=\"failed\""),
        ("wasmkeep_events_total", "class=\""),
        ("wasmkeep_bucket_requests_total", "status=\"error\""),
        ("wasmkeep_failover_waits_total", "outcome=\"expired\""),
        ("wasmkeep_failover_waits_total", "outcome=\"over_cap\""),
        ("wasmkeep_admission_refused_total", "reason=\"capacity\""),
        ("wasmkeep_admission_refused_total", "reason=\"overloaded\""),
        ("wasmkeep_admission_refused_total", "reason=\"wrong_app\""),
        ("wasmkeep_admission_refused_total", "reason=\"unavailable\""),
        ("wasmkeep_admission_refused_total", "reason=\"moving\""),
    ] {
        assert!(
            page.lines()
                .any(|l| l.starts_with(&format!("{metric}{{")) && l.contains(label)),
            "the metrics page has no {metric} series with {label}"
        );
    }
}

/// The Grafana dashboard in `deploy/grafana` queries only metrics this
/// page has.
#[test]
fn the_grafana_dashboard_uses_only_metrics_that_exist() {
    fn exprs(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, v) in m {
                    match (k.as_str(), v) {
                        ("expr" | "definition", serde_json::Value::String(s)) => {
                            out.push(s.clone());
                        }
                        _ => exprs(v, out),
                    }
                }
            }
            serde_json::Value::Array(a) => a.iter().for_each(|v| exprs(v, out)),
            _ => {}
        }
    }
    let dashboard: serde_json::Value =
        serde_json::from_str(include_str!("../../../../deploy/grafana/wasmkeep.json"))
            .expect("the dashboard is JSON");
    let mut found = Vec::new();
    exprs(&dashboard, &mut found);
    assert!(found.len() > 10, "{found:?}");
    let page = render(Gauges::default());
    let mut names = std::collections::BTreeSet::new();
    for expr in &found {
        let mut rest = expr.as_str();
        while let Some(at) = rest.find("wasmkeep_") {
            let name: String = rest[at..]
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            rest = &rest[at + name.len()..];
            names.insert(name);
        }
    }
    for name in &names {
        let base = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|s| name.strip_suffix(s))
            .filter(|b| page.contains(&format!("# TYPE {b} histogram\n")))
            .unwrap_or(name);
        assert!(
            page.contains(&format!("# TYPE {base} ")),
            "the dashboard queries {name}, which the metrics page does not have"
        );
    }
}

/// A page rendered from a fresh, seeded [`Metrics`]: what a node
/// serves before anything happened.
fn fresh_page() -> String {
    render_with(
        &Metrics::seeded(),
        Gauges::default(),
        &[Identity::default()],
    )
}

/// Families whose label values are a fixed set have every series on
/// the page from the start, at 0: their names can be checked and
/// `increase()` sees the first event.
#[test]
fn bounded_families_are_on_the_page_at_zero_from_the_start() {
    let page = fresh_page();
    let mut expected = Vec::new();
    for kind in FAILOVER_KINDS {
        for outcome in FAILOVER_OUTCOMES {
            expected.push(format!(
                "wasmkeep_failover_waits_total{{kind=\"{kind}\",outcome=\"{outcome}\"}} 0"
            ));
        }
    }
    for operation in BUCKET_OPERATIONS {
        for status in BUCKET_STATUSES {
            expected.push(format!(
                "wasmkeep_bucket_requests_total{{operation=\"{operation}\",status=\"{status}\"}} 0"
            ));
        }
    }
    for (name, label, values) in [
        ("wasmkeep_event_cuts_total", "reason", &CUT_REASONS[..]),
        ("wasmkeep_forwards_total", "kind", &FORWARD_KINDS[..]),
        (
            "wasmkeep_signatures_refused_full_total",
            "record",
            &SIGNATURE_RECORDS[..],
        ),
        (
            "wasmkeep_websocket_owner_closes_total",
            "reason",
            &OWNER_CLOSE_REASONS[..],
        ),
        (
            "wasmkeep_websocket_sweep_unreadable_total",
            "read",
            &SWEEP_UNREADABLE_READS[..],
        ),
        ("wasmkeep_actor_deletes_total", "how", &DELETE_HOWS[..]),
        (
            "wasmkeep_expiry_kept_total",
            "reason",
            &EXPIRY_KEPT_REASONS[..],
        ),
        (
            "wasmkeep_actor_delete_failures_total",
            "how",
            &DELETE_HOWS[..],
        ),
        (
            "wasmkeep_parked_releases_total",
            "reason",
            &PARKED_RELEASE_REASONS[..],
        ),
        (
            "wasmkeep_plain_events_dropped_total",
            "reason",
            &PLAIN_DROP_REASONS[..],
        ),
    ] {
        for v in values {
            expected.push(format!("{name}{{{label}=\"{v}\"}} 0"));
        }
    }
    for o in FRONT_REQUEST_OUTCOMES {
        expected.push(format!(
            "wasmkeep_front_requests_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
        ));
    }
    for o in BLOB_SWEEP_OUTCOMES {
        expected.push(format!(
            "wasmkeep_blob_sweeps_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
        ));
    }
    for o in FRONT_CALL_OUTCOMES {
        expected.push(format!(
            "wasmkeep_front_calls_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
        ));
    }
    for line in &expected {
        assert!(page.lines().any(|l| l == line), "no {line} in\n{page}");
    }
    // Families keyed by class get a series at a class's first event
    // only: no names beyond the classes that ran.
    assert!(!page.contains("wasmkeep_events_total{"), "{page}");
    assert!(
        !page.contains("wasmkeep_event_duration_seconds_bucket{"),
        "{page}"
    );
}

/// Every not-ready reason is a series from the start: 0, and 1 for the
/// reason the node is not ready, if it is not.
#[test]
fn every_not_ready_reason_is_a_series() {
    let reasons = |page: &str| -> Vec<(String, String)> {
        page.lines()
            .filter_map(|l| l.strip_prefix("wasmkeep_not_ready{reason=\""))
            .map(|l| {
                let (reason, v) = l.split_once("\"} ").unwrap();
                (reason.to_owned(), v.to_owned())
            })
            .collect()
    };
    let ready = reasons(&fresh_page());
    assert_eq!(ready.len(), NOT_READY_REASONS.len(), "{ready:?}");
    assert!(ready.iter().all(|(_, v)| v == "0"), "{ready:?}");
    let why = "bucket unreachable (node lease not renewed)";
    let not_ready = reasons(&render_with(
        &Metrics::seeded(),
        Gauges {
            not_ready: Some(why),
            ..Gauges::default()
        },
        &[Identity::default()],
    ));
    assert_eq!(not_ready.len(), NOT_READY_REASONS.len(), "{not_ready:?}");
    let ones: Vec<_> = not_ready.iter().filter(|(_, v)| v == "1").collect();
    assert_eq!(ones, [&(why.to_owned(), "1".to_owned())], "{not_ready:?}");
    // A reason missing from the list is still on the page.
    let page = render(Gauges {
        not_ready: Some("something new"),
        ..Gauges::default()
    });
    assert!(
        page.contains("\nwasmkeep_not_ready{reason=\"something new\"} 1\n"),
        "{page}"
    );
}

/// The quoted literal after each `needle` in `src`, whitespace ignored:
/// the first `"..."` before the next `)` or `;`.
fn literals(src: &str, needle: &str) -> Vec<String> {
    let compact: String = src.chars().filter(|c| !c.is_whitespace()).collect();
    let needle: String = needle.chars().filter(|c| !c.is_whitespace()).collect();
    let mut out = Vec::new();
    let mut rest = compact.as_str();
    while let Some(at) = rest.find(&needle) {
        rest = &rest[at + needle.len()..];
        let end = rest.find([')', ';']).unwrap_or(rest.len());
        let call = &rest[..end];
        if let Some(open) = call.find('"') {
            let value = &call[open + 1..];
            if let Some(close) = value.find('"') {
                out.push(value[..close].to_owned());
            }
        }
    }
    out
}

/// ADR 0034's section 16: the batched uploader's series are on the
/// page from the start, the waits' reasons seeded at 0, and what is
/// observed shows in them.
#[test]
fn the_batched_series_are_on_the_page() {
    let m = Metrics::seeded();
    m.unsent_age_seconds.observe(Duration::from_millis(30));
    m.segment_events.observe(3);
    m.segment_bytes.observe(5_000);
    crate::metrics::inc(&m.batched_waits.get(&[("reason", "age")]));
    m.flush_seconds.observe(Duration::from_millis(60));
    m.segment_upload_seconds.observe(Duration::from_millis(40));
    m.segment_upload_retries.store(2, Ordering::Relaxed);
    m.segment_upload_failures.store(1, Ordering::Relaxed);
    let page = render_with(&m, Gauges::default(), &[Identity::default()]);
    for want in [
        "wasmkeep_unsent_age_seconds_count 1",
        "wasmkeep_segment_events_bucket{le=\"5\"} 1",
        "wasmkeep_segment_events_count 1",
        "wasmkeep_segment_bytes_bucket{le=\"16384\"} 1",
        "wasmkeep_segment_bytes_sum 5000",
        "wasmkeep_batched_waits_total{reason=\"age\"} 1",
        "wasmkeep_batched_waits_total{reason=\"bytes\"} 0",
        "wasmkeep_batched_waits_total{reason=\"budget\"} 0",
        "wasmkeep_flush_seconds_count 1",
        "wasmkeep_segment_upload_seconds_bucket{le=\"0.05\"} 1",
        "wasmkeep_segment_upload_seconds_count 1",
        "wasmkeep_segment_upload_retries_total 2",
        "wasmkeep_segment_upload_failures_total 1",
        "# TYPE wasmkeep_upload_budget_bytes_in_use gauge",
    ] {
        assert!(page.contains(want), "{want}:\n{page}");
    }
}

/// The wide histograms have 0.5, 2 and 2.5 s bounds; the standard one
/// does not.
#[test]
fn wide_histograms_have_the_half_two_and_two_and_a_half_second_bounds() {
    let wide = Histogram::<Wide>::default();
    wide.observe(Duration::from_millis(700));
    wide.observe(Duration::from_millis(2_200));
    let mut out = String::new();
    wide.render(&mut out, "w", "");
    for want in [
        "w_bucket{le=\"0.5\"} 0\n",
        "w_bucket{le=\"1\"} 1\n",
        "w_bucket{le=\"2\"} 1\n",
        "w_bucket{le=\"2.5\"} 2\n",
        "w_bucket{le=\"+Inf\"} 2\n",
    ] {
        assert!(out.contains(want), "{want} in\n{out}");
    }
    let standard = Histogram::<Standard>::default();
    let mut out = String::new();
    standard.render(&mut out, "s", "");
    for gone in ["le=\"0.5\"", "le=\"2\"", "le=\"2.5\""] {
        assert!(!out.contains(gone), "{gone} in\n{out}");
    }
}

/// A segment upload recorded by `segment_upload` (what the replica
/// calls when an upload ends) is on the page: its time when
/// acknowledged, its resends, and a failure.
#[test]
fn a_recorded_segment_upload_shows_on_the_page() {
    let m = &METRICS;
    let (seen, resent, failed) = (
        m.segment_upload_seconds.count(),
        m.segment_upload_retries.load(Ordering::Relaxed),
        m.segment_upload_failures.load(Ordering::Relaxed),
    );
    segment_upload(Duration::from_millis(30), 1, true);
    segment_upload(Duration::from_secs(2), 2, false);
    assert_eq!(m.segment_upload_seconds.count(), seen + 1);
    assert_eq!(m.segment_upload_retries.load(Ordering::Relaxed), resent + 3);
    assert_eq!(
        m.segment_upload_failures.load(Ordering::Relaxed),
        failed + 1
    );
    let page = render_with(m, Gauges::default(), &[Identity::default()]);
    for want in [
        "# TYPE wasmkeep_segment_upload_seconds histogram",
        "wasmkeep_segment_upload_seconds_bucket{le=\"+Inf\"}",
        "# TYPE wasmkeep_segment_upload_retries_total counter",
        "# TYPE wasmkeep_segment_upload_failures_total counter",
    ] {
        assert!(page.contains(want), "{want}:\n{page}");
    }
}

/// The scheduling probe's wake-ups reach the page.
#[tokio::test(start_paused = true)]
async fn the_schedule_lag_probe_reaches_the_page() {
    let probe = LagProbe::start();
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(probe);
    assert!(METRICS.runtime_schedule_lag_seconds.count() >= 3);
    let page = render(Gauges::default());
    assert!(
        page.contains("# TYPE wasmkeep_runtime_schedule_lag_seconds histogram"),
        "{page}"
    );
    assert!(
        !page.contains("wasmkeep_runtime_schedule_lag_seconds_count 0\n"),
        "{page}"
    );
}

/// The label values seeded at 0 are the ones the code counts: a value
/// counted somewhere is in its list.
#[test]
fn the_seeded_label_values_are_the_ones_counted() {
    let failover = include_str!("../cluster/failover_wait.rs");
    let check = |what: &str, found: Vec<String>, list: &[&str]| {
        assert!(!found.is_empty(), "{what}: no call site found");
        for v in &found {
            assert!(
                list.contains(&v.as_str()),
                "{what}: {v} is counted but not seeded"
            );
        }
    };
    check(
        "failover outcomes",
        [
            literals(failover, "self.outcome = "),
            literals(failover, "outcome: "),
            literals(failover, "FailoverWait::count(kind, "),
            literals(include_str!("../actor/requests.rs"), ".end("),
            literals(include_str!("../actor/upgrade.rs"), ".end("),
        ]
        .concat(),
        &FAILOVER_OUTCOMES,
    );
    check(
        "bucket operations",
        literals(include_str!("../counted_store.rs"), "record("),
        &BUCKET_OPERATIONS,
    );
    check(
        "event cuts",
        literals(include_str!("../actor/host.rs"), "cut("),
        &CUT_REASONS,
    );
    check(
        "forwards",
        literals(include_str!("../cluster.rs"), ".forwards.get(&[(\"kind\", "),
        &FORWARD_KINDS,
    );
    check(
        "signature records",
        [
            literals(include_str!("../cluster.rs"), "SeenSignatures::named("),
            literals(include_str!("../cluster.rs"), "SeenSignatures::with_cap("),
        ]
        .concat(),
        &SIGNATURE_RECORDS,
    );
    check(
        "owner closes",
        [
            include_str!("../actor/drain.rs"),
            include_str!("../actor/moving.rs"),
            include_str!("../actor/socket_owners.rs"),
            include_str!("../actor/sockets.rs"),
        ]
        .iter()
        .flat_map(|src| literals(src, "owner_closes("))
        .collect(),
        &OWNER_CLOSE_REASONS,
    );
    check(
        "actor deletes",
        [
            include_str!("../actor/host.rs"),
            include_str!("../actor/state.rs"),
            include_str!("../actor/deletes.rs"),
            include_str!("../actor/expiry.rs"),
        ]
        .iter()
        .flat_map(|src| {
            [
                literals(src, "metrics::deleted("),
                literals(src, "metrics::delete_failed("),
            ]
            .concat()
        })
        .collect(),
        &DELETE_HOWS,
    );
    check(
        "parked releases",
        [
            include_str!("../actor.rs"),
            include_str!("../actor/drain.rs"),
            include_str!("../actor/moving.rs"),
            include_str!("../actor/socket_owners.rs"),
            include_str!("../actor/state.rs"),
            include_str!("../cluster/lease_hold.rs"),
        ]
        .iter()
        .flat_map(|src| {
            [
                literals(src, "release_parked("),
                literals(src, "release_all_parked("),
                literals(src, "parked_released("),
            ]
            .concat()
        })
        .collect(),
        &PARKED_RELEASE_REASONS,
    );
    check(
        "sweep unreadable",
        literals(
            include_str!("../actor/socket_owners.rs"),
            "unreadable(&logged, ",
        )
        .into_iter()
        .chain(literals(
            include_str!("../actor/socket_owners.rs"),
            "unreadable(logged, ",
        ))
        .collect(),
        &SWEEP_UNREADABLE_READS,
    );
    check(
        "front requests",
        literals(include_str!("../front.rs"), "outcome("),
        &FRONT_REQUEST_OUTCOMES,
    );
}

/// The not-ready reasons listed are every reason a node gives
/// ([`crate::ActorService::not_ready`], the cluster's, and a stopped
/// node's words).
#[test]
fn the_not_ready_reasons_are_every_reason_given() {
    /// The body of the method `fn name(` (to its closing brace at four
    /// spaces), or of a function at none.
    fn body<'a>(src: &'a str, name: &str) -> &'a str {
        let at = src.find(&format!("fn {name}(")).expect(name);
        let rest = &src[at..];
        let end = rest.find("\n    }\n").expect(name);
        &rest[..end]
    }
    fn quoted(src: &str, after: &str) -> Vec<String> {
        src.split(after)
            .skip(1)
            .filter_map(|s| Some(s.split_once('"')?.0.to_owned()))
            .collect()
    }
    let mut given = quoted(body(include_str!("../actor.rs"), "not_ready"), "Some(\"");
    given.extend(quoted(
        body(include_str!("../cluster.rs"), "not_ready"),
        "Some(\"",
    ));
    given.extend(quoted(
        body(include_str!("../cluster/identity.rs"), "words"),
        "=> \"",
    ));
    assert!(given.len() > 10, "{given:?}");
    for reason in &given {
        assert!(
            NOT_READY_REASONS.contains(&reason.as_str()),
            "the not-ready reason {reason:?} is not in NOT_READY_REASONS"
        );
    }
    for reason in NOT_READY_REASONS {
        assert!(
            given.iter().any(|g| g == reason),
            "NOT_READY_REASONS lists {reason:?}, which no code gives"
        );
    }
}

mod fuel_tests {
    use super::*;

    #[test]
    fn fuel_histograms_preserve_app_class_and_kind() {
        let m = Metrics::default();
        for (app, kind, value) in [("one", "request", 100_000), ("two", "alarm", 2_000_000)] {
            let labels = app_labels("tenant", app);
            m.event_fuel
                .get_in(&labels, &[("class", "room"), ("kind", event_kind(kind))])
                .0
                .observe(value);
        }
        let page = render_with(&m, Gauges::default(), &[]);
        assert!(page.contains("# TYPE wasmkeep_event_fuel histogram"));
        assert!(page.contains("wasmkeep_event_fuel_bucket{tenant=\"tenant\",app=\"one\",class=\"room\",kind=\"fetch\",le=\"100000\"} 1"));
        assert!(page.contains("wasmkeep_event_fuel_sum{tenant=\"tenant\",app=\"two\",class=\"room\",kind=\"alarm\"} 2000000"));
        assert!(page.contains("wasmkeep_event_fuel_count{tenant=\"tenant\",app=\"two\",class=\"room\",kind=\"alarm\"} 1"));
    }
}
