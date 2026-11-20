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
