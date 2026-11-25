//! Exclusive wall-time attribution, shared only by one actor and its live instance.
//! Handles are cached at activation; guards clone Arcs, never allocate.
use crate::metrics::{Histogram, METRICS};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Work {
    StaleAlarmHint,
    CompleteUploads,
    SnapshotCopy,
    FlushToBarrier,
    Hibernate,
    Restore,
    Other,
}
const NAMES: [&str; 7] = [
    "stale_alarm_hint",
    "complete_uploads",
    "snapshot_copy",
    "flush_to_barrier",
    "hibernate",
    "restore",
    "other",
];

/// How an alarm delivery the mailbox ran ended
/// (`wasmkeep_alarm_deliveries_total`).
#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Delivered {
    /// `on-alarm` ran for the committed alarm.
    Ran,
    /// Its hint no longer matched the committed alarm: a stale hint.
    Stale,
    /// A stale hint this hold had already delivered, met again from this
    /// node's timer: not cleared (the move retired its entry).
    Duplicate,
    /// Not run now: another owner, a hand-off, the bucket, a missing
    /// value, an actor not ready, no state, a class no longer served.
    Deferred,
}
const DELIVERED: [&str; 4] = ["ran", "stale", "duplicate", "deferred"];

pub(super) struct Timing {
    pub(super) mailbox_wait_nanos: AtomicU64,
    histograms: [Arc<Histogram>; 7],
    stale_hints: Arc<AtomicU64>,
    deliveries: [Arc<AtomicU64>; 4],
    guest: AtomicU64,
    host: AtomicU64,
    depth: AtomicUsize,
    /// Host work timed apart inside an outer guard ([`Timing::apart`]),
    /// which that guard leaves out: monotonic.
    apart: AtomicU64,
}
impl Timing {
    pub(super) fn new(app: &str, class: &str) -> Arc<Self> {
        Arc::new(Self {
            mailbox_wait_nanos: AtomicU64::new(0),
            histograms: NAMES.map(|work| {
                METRICS
                    .mailbox_host_work_seconds
                    .get_in(app, &[("class", class), ("work", work)])
            }),
            stale_hints: METRICS.alarm_stale_hints.get_in(app, &[("class", class)]),
            deliveries: DELIVERED.map(|outcome| {
                METRICS
                    .alarm_deliveries
                    .get_in(app, &[("class", class), ("outcome", outcome)])
            }),
            guest: AtomicU64::new(0),
            host: AtomicU64::new(0),
            depth: AtomicUsize::new(0),
            apart: AtomicU64::new(0),
        })
    }
    pub(super) fn job(&self, timing: wasmkeep_runtime::JobTiming) {
        match timing {
            wasmkeep_runtime::JobTiming::Started { initialization } => {
                self.guest.store(0, Relaxed);
                self.host.store(0, Relaxed);
                if !initialization.is_zero() {
                    self.record(Work::Restore, initialization);
                }
            }
            wasmkeep_runtime::JobTiming::Finished { elapsed } => {
                let accounted = Duration::from_nanos(
                    self.guest
                        .load(Relaxed)
                        .saturating_add(self.host.load(Relaxed)),
                );
                self.record(Work::Other, elapsed.saturating_sub(accounted));
            }
        }
    }
    pub(super) fn stale_hint(&self) {
        self.stale_hints.fetch_add(1, Relaxed);
    }
    pub(super) fn delivered(&self, outcome: Delivered) {
        self.deliveries[outcome as usize].fetch_add(1, Relaxed);
    }
    pub(super) fn guest(&self, elapsed: Duration) {
        self.guest.fetch_add(nanos(elapsed), Relaxed);
    }
    fn record(&self, work: Work, elapsed: Duration) {
        self.histograms[work as usize].observe(elapsed);
        self.host.fetch_add(nanos(elapsed), Relaxed);
    }
    pub(super) fn work(self: &Arc<Self>, work: Work) -> Guard {
        let outer = self.depth.fetch_add(1, Relaxed) == 0;
        Guard {
            timing: Arc::clone(self),
            work,
            started: outer.then(Instant::now),
            guest: self.guest.load(Relaxed),
            apart: self.apart.load(Relaxed),
        }
    }
    /// Records `elapsed` of `work` that ran inside whatever guard is open
    /// (a periodic snapshot's copy inside a completion job or an event's
    /// step), and takes it out of that guard's time: exclusive still.
    pub(super) fn apart(&self, work: Work, elapsed: Duration) {
        self.record(work, elapsed);
        self.apart.fetch_add(nanos(elapsed), Relaxed);
    }
}
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) struct Guard {
    timing: Arc<Timing>,
    work: Work,
    started: Option<Instant>,
    guest: u64,
    apart: u64,
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.timing.depth.fetch_sub(1, Relaxed);
        if let Some(started) = self.started {
            let guest =
                Duration::from_nanos(self.timing.guest.load(Relaxed).saturating_sub(self.guest));
            let apart =
                Duration::from_nanos(self.timing.apart.load(Relaxed).saturating_sub(self.apart));
            self.timing.record(
                self.work,
                started
                    .elapsed()
                    .saturating_sub(guest)
                    .saturating_sub(apart),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use wasmkeep_runtime::JobTiming;

    fn sum(work: &str) -> f64 {
        let page = crate::metrics::render(Default::default());
        let prefix = format!(
            "wasmkeep_mailbox_host_work_seconds_sum{{tenant=\"timing\",app=\"guards\",class=\"test\",work=\"{work}\"}} "
        );
        page.lines()
            .find_map(|l| l.strip_prefix(&prefix))
            .unwrap()
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn nested_work_and_cancelled_steps_do_not_double_count() {
        let timing = Timing::new(&crate::metrics::app_labels("timing", "guards"), "test");
        // A cancelled pre-drop step is outside a job. Its guard must still
        // observe the time, and must not leak its depth/accounting into a job.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), async {
                let _step = timing.work(Work::Hibernate);
                let _flush = timing.work(Work::FlushToBarrier);
                std::future::pending::<()>().await;
            })
            .await
            .is_err()
        );
        assert!(sum("hibernate") >= 0.015);
        assert_eq!(sum("flush_to_barrier"), 0.0);
        timing.job(JobTiming::Started {
            initialization: Duration::ZERO,
        });
        let start = Instant::now();
        {
            let _restore = timing.work(Work::Restore);
            tokio::time::sleep(Duration::from_millis(10)).await;
            let guest_start = Instant::now();
            tokio::time::sleep(Duration::from_millis(10)).await;
            timing.guest(guest_start.elapsed());
        }
        {
            let _stale = timing.work(Work::StaleAlarmHint);
            let _flush = timing.work(Work::FlushToBarrier);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        {
            // A snapshot's copy inside a completion job: its own label,
            // and out of the job's.
            let _complete = timing.work(Work::CompleteUploads);
            let copy = Instant::now();
            tokio::time::sleep(Duration::from_millis(10)).await;
            timing.apart(Work::SnapshotCopy, copy.elapsed());
        }
        let elapsed = start.elapsed();
        timing.job(JobTiming::Finished { elapsed });
        let guest = Duration::from_nanos(timing.guest.load(Relaxed)).as_secs_f64();
        assert!(sum("snapshot_copy") >= 0.009);
        assert!(
            sum("complete_uploads") < 0.005,
            "{}",
            sum("complete_uploads")
        );
        let host = sum("restore")
            + sum("stale_alarm_hint")
            + sum("complete_uploads")
            + sum("snapshot_copy")
            + sum("other");
        assert!((guest + host - elapsed.as_secs_f64()).abs() < 1e-8);
        assert_eq!(sum("flush_to_barrier"), 0.0);
        assert_eq!(timing.depth.load(Relaxed), 0);
    }
}
