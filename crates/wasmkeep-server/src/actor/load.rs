//! This node's load, published with its heartbeat (payload line 10 of the
//! node lease, `load actors=<n> sockets=<n> events_per_min=<n|->`) for
//! `wasmkeep cluster status`. Advisory: nothing decides on it.

use std::collections::VecDeque;

use wasmkeep_cluster::membership::Load;

use super::*;

/// The shortest span events per minute are measured over.
pub(super) const RATE_WINDOW: Duration = Duration::from_secs(60);

/// How far apart the events totals the rate is measured from are kept.
pub(super) const RATE_SAMPLE_EVERY: Duration = Duration::from_secs(15);

/// Events per minute from a running total, measured from the newest kept
/// total at least [`RATE_WINDOW`] old: unknown until one is.
#[derive(Debug, Default)]
pub(super) struct EventRate {
    /// `(ms, total)`, oldest first, [`RATE_SAMPLE_EVERY`] apart or more.
    kept: VecDeque<(u64, u64)>,
}

impl EventRate {
    /// Notes `total` at `at_ms` (a monotonic clock) and returns events per
    /// minute since the base, or `None` while no kept total is old enough.
    pub(super) fn sample(&mut self, at_ms: u64, total: u64) -> Option<u64> {
        let window = millis(RATE_WINDOW);
        let every = millis(RATE_SAMPLE_EVERY);
        if self
            .kept
            .back()
            .is_none_or(|(last, _)| at_ms.saturating_sub(*last) >= every)
        {
            self.kept.push_back((at_ms, total));
        }
        // Drop every total older than the newest one old enough.
        while self
            .kept
            .get(1)
            .is_some_and(|(t, _)| at_ms.saturating_sub(*t) >= window)
        {
            self.kept.pop_front();
        }
        let (base_at, base_total) = *self.kept.front()?;
        let elapsed = at_ms.saturating_sub(base_at);
        if elapsed < window {
            return None;
        }
        let events = u128::from(total.saturating_sub(base_total));
        u64::try_from(events * 60_000 / u128::from(elapsed)).ok()
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Sets this node's load in its membership every `every` (the heartbeat
/// period, so each node lease write carries a fresh count), until the
/// service is gone. Events are the process's (every node it runs).
pub(super) async fn publish_load(service: std::sync::Weak<ServiceInner>, every: Duration) {
    let start = tokio::time::Instant::now();
    let mut rate = EventRate::default();
    loop {
        let Some(inner) = service.upgrade() else {
            return;
        };
        let Some(cluster) = inner.cluster.as_ref() else {
            return;
        };
        let at = millis(start.elapsed());
        let events_per_min = rate.sample(at, crate::metrics::METRICS.events.total());
        cluster.set_load(Load {
            actors: u64::try_from(inner.mailboxes.resident()).unwrap_or(u64::MAX),
            sockets: u64::try_from(inner.sockets.len()).unwrap_or(u64::MAX),
            events_per_min,
        });
        drop(inner);
        tokio::time::sleep(every).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000;

    #[test]
    fn the_rate_is_unknown_until_a_minute_of_totals_exists() {
        let mut r = EventRate::default();
        // A sample every heartbeat (3 s), 10 events a second.
        for t in (0..60).step_by(3) {
            assert_eq!(r.sample(t * S, t * 10), None, "at {t} s");
        }
        assert_eq!(r.sample(60 * S, 600), Some(600));
        assert_eq!(r.sample(63 * S, 630), Some(600));
        // The base moves on: never more than a window plus a sample apart.
        for t in (66..600).step_by(3) {
            assert_eq!(r.sample(t * S, t * 10), Some(600), "at {t} s");
            assert!(r.kept.len() <= 6, "{:?}", r.kept);
            let base = r.kept.front().unwrap().0;
            assert!((60 * S..75 * S).contains(&(t * S - base)), "at {t} s");
        }
    }

    #[test]
    fn an_idle_node_says_zero_and_a_burst_shows_over_the_window() {
        let mut r = EventRate::default();
        for t in (0..=60).step_by(15) {
            r.sample(t * S, 42);
        }
        assert_eq!(r.sample(61 * S, 42), Some(0));
        // 120 events in a burst: 120 over the 61 s since the base.
        assert_eq!(r.sample(61 * S, 162), Some(118));
        // Measured over at least a minute: a minute on, gone again.
        for t in (75..=135).step_by(15) {
            r.sample(t * S, 162);
        }
        assert_eq!(r.sample(136 * S, 162), Some(0));
    }

    #[test]
    fn a_stalled_sampler_averages_over_the_gap() {
        let mut r = EventRate::default();
        assert_eq!(r.sample(0, 0), None);
        // Nothing sampled for ten minutes, 6000 events meanwhile.
        assert_eq!(r.sample(600 * S, 6_000), Some(600));
        // A counter that went back (never, but not a panic) reads as none.
        assert_eq!(r.sample(601 * S, 0), Some(0));
    }
}
