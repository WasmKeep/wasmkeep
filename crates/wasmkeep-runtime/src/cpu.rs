//! Per-store guest CPU accounting and the epoch ticker that enforces it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wasmtime::{CallHook, Engine, Store, UpdateDeadline};

/// The trap raised when a guest runs longer than its CPU budget for one event.
/// Recover it from a call's error with `error.downcast_ref::<CpuBudgetExceeded>()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("guest exceeded its CPU budget of {budget:?} for this event")]
pub struct CpuBudgetExceeded {
    /// The budget that was exceeded.
    pub budget: Duration,
}

/// Guest calls stopped for exceeding their CPU budget, process-wide.
static BUDGET_EXCEEDED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many guest calls this process stopped for exceeding their CPU
/// budget (for metrics).
pub fn cpu_budget_exceeded_total() -> u64 {
    BUDGET_EXCEEDED.load(Ordering::Relaxed)
}

/// CPU time the calling thread has used, from an arbitrary origin. Only
/// readings taken on one thread are compared.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn thread_cpu() -> Duration {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    Duration::new(
        u64::try_from(t.tv_sec).unwrap_or(0),
        u32::try_from(t.tv_nsec).unwrap_or(0),
    )
}

/// Elsewhere, wall time: long computations are then charged the time their
/// thread is preempted too.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn thread_cpu() -> Duration {
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed()
}

/// Guest CPU time of one store, kept per *segment*: a stretch of guest code
/// between two call hooks. Wasmtime brackets every host call and every
/// epoch check (where async stores yield) with hooks, so a segment runs on
/// one thread and time spent in the host, awaiting I/O or waiting to be
/// polled again is never counted.
///
/// The budget is meant as CPU time, not wall time: on a loaded node the OS
/// runs other threads in the middle of a guest's work, and that is not the
/// guest's time. Reading the thread's CPU clock at every hook costs a
/// syscall per host call (about +10% CPU on a small request), so:
///
/// - A segment that follows an epoch check (the guest has been running for
///   a tick or more) is measured with the thread CPU clock: one reading per
///   tick, exact.
/// - Any other segment is measured with the (cheap) wall clock, capped at
///   two ticks. Without an epoch check in between, a guest cannot really
///   run longer than about one tick, so the cap only removes preemption.
#[derive(Debug)]
struct Meter {
    used: Duration,
    running: Option<Segment>,
    /// The next segment follows an epoch check.
    precise_next: bool,
    /// Most a wall-clock segment is charged.
    cap: Duration,
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    wall: Instant,
    /// The thread CPU clock at the start, for a precise segment.
    cpu: Option<Duration>,
}

impl Meter {
    fn new(tick: Duration) -> Self {
        Self {
            used: Duration::ZERO,
            running: None,
            precise_next: false,
            cap: tick.saturating_mul(2),
        }
    }

    fn reset(&mut self) {
        *self = Self::new(self.cap / 2);
    }

    fn enter_guest(&mut self, wall: Instant, cpu: impl FnOnce() -> Duration) {
        let cpu = std::mem::take(&mut self.precise_next).then(cpu);
        self.running = Some(Segment { wall, cpu });
    }

    fn leave_guest(&mut self, wall: Instant, cpu: impl FnOnce() -> Duration) {
        if let Some(segment) = self.running.take() {
            self.used += self.charge(segment, wall, cpu);
        }
    }

    /// An epoch check: the guest is still running, so the next segment is
    /// measured precisely.
    fn checked(&mut self) {
        self.precise_next = true;
    }

    fn charge(&self, segment: Segment, wall: Instant, cpu: impl FnOnce() -> Duration) -> Duration {
        match segment.cpu {
            Some(start) => cpu().saturating_sub(start),
            None => wall.saturating_duration_since(segment.wall).min(self.cap),
        }
    }

    fn total(&self, wall: Instant, cpu: impl FnOnce() -> Duration) -> Duration {
        self.used
            + self
                .running
                .map_or(Duration::ZERO, |segment| self.charge(segment, wall, cpu))
    }
}

/// How the epoch callback hands control back when the budget is not spent.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Execution {
    /// Guest is invoked with sync calls: keep running.
    Sync,
    /// Guest is invoked with `*_async` calls: yield to the executor on each
    /// tick so one busy guest cannot monopolise a worker thread.
    Async,
}

/// Handle to a store's CPU meter, for stores reused across events.
#[derive(Debug, Clone)]
pub struct CpuMeter(Arc<Mutex<Meter>>);

impl CpuMeter {
    /// Starts a new event: the full budget is available again.
    pub fn reset(&self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).reset();
    }

    /// Guest CPU time used since the last reset. Exact while the guest is
    /// not running, or when called from the thread running it.
    pub fn used(&self) -> Duration {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .total(Instant::now(), thread_cpu)
    }
}

/// Installs the CPU meter and budget on a fresh store.
pub(crate) fn install<T: 'static>(
    store: &mut Store<T>,
    budget: Duration,
    tick: Duration,
    execution: Execution,
) -> CpuMeter {
    let meter = Arc::new(Mutex::new(Meter::new(tick)));
    let handle = CpuMeter(Arc::clone(&meter));

    let hook_meter = Arc::clone(&meter);
    store.call_hook(move |_, hook| {
        let now = Instant::now();
        let mut meter = hook_meter.lock().unwrap_or_else(|p| p.into_inner());
        match hook {
            CallHook::CallingWasm | CallHook::ReturningFromHost => {
                meter.enter_guest(now, thread_cpu);
            }
            CallHook::ReturningFromWasm | CallHook::CallingHost => {
                meter.leave_guest(now, thread_cpu);
            }
        }
        Ok(())
    });

    store.epoch_deadline_callback(move |_| {
        // Runs between `CallingHost` and `ReturningFromHost`: the segment
        // that reached this check is already charged.
        let mut meter = meter.lock().unwrap_or_else(|p| p.into_inner());
        if meter.total(Instant::now(), thread_cpu) > budget {
            BUDGET_EXCEEDED.fetch_add(1, Ordering::Relaxed);
            return Err(CpuBudgetExceeded { budget }.into());
        }
        meter.checked();
        drop(meter);
        Ok(match execution {
            Execution::Sync => UpdateDeadline::Continue(1),
            Execution::Async => UpdateDeadline::Yield(1),
        })
    });
    // Check on the next tick; the callback re-arms one tick at a time.
    store.set_epoch_deadline(1);
    handle
}

/// Background thread advancing the engine epoch every `tick`. Stops when
/// dropped (within one tick).
#[derive(Debug)]
pub(crate) struct Ticker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Ticker {
    pub(crate) fn start(engine: Engine, tick: Duration) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("wasmkeep-epoch".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    std::thread::sleep(tick);
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = Duration::from_millis(1);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn meter_counts_only_guest_time() {
        let t0 = Instant::now();
        let mut m = Meter::new(ms(100)); // a cap that does not interfere
        let no_cpu = || unreachable!("short segments use the wall clock");
        m.enter_guest(t0, no_cpu);
        m.leave_guest(t0 + ms(10), no_cpu); // 10 ms guest
        m.enter_guest(t0 + ms(500), no_cpu); // 490 ms in a host call
        assert_eq!(m.total(t0 + ms(505), no_cpu), ms(15));
        m.leave_guest(t0 + ms(520), no_cpu);
        assert_eq!(m.total(t0 + ms(9_000), no_cpu), ms(30));
    }

    #[test]
    fn a_segment_without_an_epoch_check_is_charged_at_most_two_ticks() {
        // Preempted for 60 ms in a short stretch of guest code.
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        m.enter_guest(t0, || unreachable!());
        m.leave_guest(t0 + ms(60), || unreachable!());
        assert_eq!(m.total(t0 + ms(60), || unreachable!()), ms(2));
    }

    #[test]
    fn a_segment_after_an_epoch_check_is_charged_its_cpu_time() {
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        m.enter_guest(t0, || unreachable!());
        m.leave_guest(t0 + ms(1), || unreachable!()); // reached a check
        m.checked();
        // 40 ms of wall time, of which the thread ran 7 ms.
        m.enter_guest(t0 + ms(2), || ms(100));
        m.leave_guest(t0 + ms(42), || ms(107));
        assert_eq!(m.total(t0 + ms(42), || unreachable!()), ms(8));
        // Back to the wall clock for the segment after a plain host call.
        m.enter_guest(t0 + ms(50), || unreachable!());
        assert_eq!(m.total(t0 + ms(51), || unreachable!()), ms(9));
    }

    #[test]
    fn reset_keeps_the_cap() {
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        m.checked();
        m.reset();
        m.enter_guest(t0, || unreachable!("reset forgets the pending check"));
        m.leave_guest(t0 + ms(60), || unreachable!());
        assert_eq!(m.total(t0, || unreachable!()), ms(2));
    }

    #[test]
    fn leaving_without_entering_is_harmless() {
        let mut m = Meter::new(TICK);
        m.leave_guest(Instant::now(), || unreachable!());
        assert_eq!(m.total(Instant::now(), || unreachable!()), Duration::ZERO);
    }
}
