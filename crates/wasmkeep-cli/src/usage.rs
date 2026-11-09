//! The opt-in anonymous usage ping (`serve --usage-ping`).
//!
//! Off unless the operator passes `--usage-ping`. When on, the node sends a
//! small JSON report a minute after it starts, then once a day: the
//! WasmKeep version, OS and CPU architecture, whether it runs in a cluster,
//! and coarse buckets of how many classes and resident actors it serves.
//! Nothing that identifies the operator, the node or the actors: no id,
//! no names, no addresses. Every report is logged before it is sent, and a
//! failure to send never affects serving. See docs/telemetry.md.

use std::time::Duration;

use wasmkeep_server::ActorService;

/// Where reports go. `None` until the project runs a collector: enabling the
/// ping then sends nothing (and says so). `WASMKEEP_USAGE_PING_URL` overrides
/// it, for testing or for an operator's own collector.
const ENDPOINT: Option<&str> = None;

/// Report format version; bumped when a field changes meaning.
const SCHEMA: u32 = 1;

/// A count as a coarse bucket: `0`, `1-9`, `10-99`, ... `10000+`.
pub(crate) fn bucket(n: usize) -> &'static str {
    match n {
        0 => "0",
        1..=9 => "1-9",
        10..=99 => "10-99",
        100..=999 => "100-999",
        1000..=9999 => "1000-9999",
        _ => "10000+",
    }
}

/// The report, as sent.
pub(crate) fn report(cluster: bool, classes: usize, resident: usize) -> String {
    format!(
        r#"{{"schema":{SCHEMA},"version":"{}","os":"{}","arch":"{}","cluster":{cluster},"classes":"{}","resident_actors":"{}"}}"#,
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        bucket(classes),
        bucket(resident),
    )
}

fn endpoint() -> Option<String> {
    std::env::var("WASMKEEP_USAGE_PING_URL")
        .ok()
        .filter(|u| !u.is_empty())
        .or_else(|| ENDPOINT.map(str::to_owned))
}

/// Starts the ping on the current runtime. `service` is `None` when the node
/// serves only a stateless component.
pub(crate) fn start(service: Option<ActorService>, cluster: bool) {
    let Some(url) = endpoint() else {
        tracing::info!("usage ping enabled, but this build has no endpoint: nothing is sent");
        return;
    };
    // Test hook: how long after start the first report goes out.
    let first = std::env::var("WASMKEEP_USAGE_PING_FIRST_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs);
    tracing::info!(url = %redact(&url), "usage ping enabled: an anonymous report a minute after start, then daily");
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent(concat!("wasmkeep/", env!("CARGO_PKG_VERSION")))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(error = %e, "usage ping disabled: no HTTP client");
                return;
            }
        };
        let mut wait = first;
        loop {
            tokio::time::sleep(wait).await;
            wait = Duration::from_secs(24 * 60 * 60);
            let (classes, resident) = service
                .as_ref()
                .map_or((0, 0), |s| (s.class_names().len(), s.resident()));
            let body = report(cluster, classes, resident);
            tracing::info!(report = %body, "sending the usage ping");
            let sent = client
                .post(&url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await;
            match sent {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::debug!(status = %r.status(), "usage ping refused"),
                Err(e) => tracing::debug!(error = %e.without_url(), "usage ping failed"),
            }
        }
    });
}

/// `url` without credentials or query (where tokens hide), for logs.
fn redact(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.set_query(None);
            u.set_fragment(None);
            u.to_string()
        }
        Err(_) => "(unparsable URL)".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_bucketed() {
        assert_eq!(bucket(0), "0");
        assert_eq!(bucket(1), "1-9");
        assert_eq!(bucket(9), "1-9");
        assert_eq!(bucket(10), "10-99");
        assert_eq!(bucket(999), "100-999");
        assert_eq!(bucket(5000), "1000-9999");
        assert_eq!(bucket(1_000_000), "10000+");
    }

    #[test]
    fn the_report_holds_only_the_documented_fields() {
        let r = report(true, 3, 250);
        assert!(r.starts_with(r#"{"schema":1,"version":""#), "{r}");
        assert!(
            r.contains(r#""cluster":true,"classes":"1-9","resident_actors":"100-999"}"#),
            "{r}"
        );
        assert!(
            r.contains(&format!(r#""os":"{}""#, std::env::consts::OS)),
            "{r}"
        );
        assert_eq!(r.matches(':').count(), 7, "exactly seven fields: {r}");
    }
}

#[cfg(test)]
mod redact_tests {
    #[test]
    fn credentials_and_tokens_never_reach_the_log() {
        assert_eq!(
            super::redact("https://user:secret@collector.example/v1/ping"),
            "https://collector.example/v1/ping"
        );
        assert_eq!(
            super::redact("https://u:pa%2Fss@host/x?token=t"),
            "https://host/x"
        );
        assert_eq!(
            super::redact("collector.example/ping?token=t"),
            "(unparsable URL)"
        );
    }
}
