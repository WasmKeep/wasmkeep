//! Assert a ceiling for *every* observed event, not an average. Nextest
//! isolates tests by process. The finite bucket must contain all samples.
use wasmkeep_server::metrics::{Gauges, render};

pub(crate) fn assert_ceiling(ceiling: u64) {
    assert_eq!(
        wasmkeep_runtime::cpu_budget_exceeded_total(),
        0,
        "CPU budget trap"
    );
    let page = render(Gauges::default());
    let mut observations = 0;
    for line in page
        .lines()
        .filter(|l| l.starts_with("wasmkeep_event_fuel_count{"))
    {
        let (series, count) = line.rsplit_once(' ').unwrap();
        let count: u64 = count.parse().unwrap();
        let labels = series
            .strip_prefix("wasmkeep_event_fuel_count{")
            .unwrap()
            .strip_suffix('}')
            .unwrap();
        let bucket = format!("wasmkeep_event_fuel_bucket{{{labels},le=\"{ceiling}\"}} {count}");
        let smallest = page
            .lines()
            .find(|l| {
                l.starts_with(&format!("wasmkeep_event_fuel_bucket{{{labels},"))
                    && l.ends_with(&format!(" {count}"))
            })
            .unwrap();
        eprintln!("fuel calibration: {smallest}");
        assert!(
            page.lines().any(|l| l == bucket),
            "event exceeded {ceiling} fuel: {line}; {smallest}"
        );
        observations += count;
    }
    assert!(
        observations > 0,
        "no fuel samples: metering disabled or guest skipped"
    );
}

/// [`assert_ceiling`] for the events of one `class` and `kind` alone (such
/// as the pusher's alarms), at a tighter ceiling, a bucket boundary.
#[allow(dead_code)] // Not every test binary including this file uses it.
pub(crate) fn assert_class_ceiling(class: &str, kind: &str, ceiling: u64) {
    let page = render(Gauges::default());
    let labels = format!(",class=\"{class}\",kind=\"{kind}\"");
    let total = |prefix: &str, suffix: &str| -> u64 {
        page.lines()
            .filter(|l| l.starts_with(prefix) && l.contains(&format!("{labels}{suffix} ")))
            .map(|l| l.rsplit_once(' ').unwrap().1.parse::<u64>().unwrap())
            .sum()
    };
    let count = total("wasmkeep_event_fuel_count{", "}");
    let within = total(
        "wasmkeep_event_fuel_bucket{",
        &format!(",le=\"{ceiling}\"}}"),
    );
    eprintln!("fuel: {within} of {count} {class} {kind} events within {ceiling}");
    assert!(count > 0, "no {class} {kind} fuel samples");
    assert_eq!(
        within,
        count,
        "{} {class} {kind} events exceeded {ceiling} fuel",
        count - within
    );
}
