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
/// readings taken on one thread are compared. On Linux in a VM, time the
/// hypervisor steals may count as the thread's: the kernel leaves it out
/// only when built with `CONFIG_PARAVIRT_TIME_ACCOUNTING`.
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
/// guest's time. Reading the thread's CPU clock is a syscall, and reading it
/// at every hook makes a small request markedly slower, so the meter reads
/// it only where the wall clock can be wrong:
///
/// - A segment that follows an epoch check (the guest has been running for
///   a tick or more) is measured with the thread CPU clock: one reading per
///   tick, exact.
/// - Until then, segments are measured with the (cheap) wall clock, capped
///   at two ticks. Without an epoch check in between, a guest cannot really
///   run longer than about one tick, so the cap bounds what preemption adds
///   to one segment.
/// - Once a wall-clock segment lasts longer than a quarter of a tick, the
///   thread was likely preempted (or the guest computes in long stretches,
///   where a reading is cheap next to the work): every later segment of the
///   event reads the thread CPU clock at both ends. That segment often ends
///   at an epoch check, since the epoch advanced while the thread was off its
///   core. An event that makes many host calls on an oversubscribed core is
///   then charged at most one capped segment beyond its CPU time, instead of
///   up to two ticks per segment.
#[derive(Debug)]
struct Meter {
    used: Duration,
    running: Option<Segment>,
    /// The next segment follows an epoch check.
    precise_next: bool,
    /// A wall-clock segment of this event ran long: measure every later one
    /// with the thread CPU clock.
    precise_rest: bool,
    /// Most a wall-clock segment is charged.
    cap: Duration,
    /// A wall-clock segment longer than this sets `precise_rest`.
    long: Duration,
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
            precise_rest: false,
            cap: tick.saturating_mul(2),
            long: tick / 4,
        }
    }

    fn reset(&mut self) {
        *self = Self::new(self.cap / 2);
    }

    fn enter_guest(&mut self, wall: Instant, cpu: impl FnOnce() -> Duration) {
        let precise = std::mem::take(&mut self.precise_next) || self.precise_rest;
        let cpu = precise.then(cpu);
        self.running = Some(Segment { wall, cpu });
    }

    fn leave_guest(&mut self, wall: Instant, cpu: impl FnOnce() -> Duration) {
        if let Some(segment) = self.running.take() {
            if segment.cpu.is_none() && wall.saturating_duration_since(segment.wall) > self.long {
                self.precise_rest = true;
            }
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
pub struct CpuMeter {
    meter: Arc<Mutex<Meter>>,
    fuel_enabled: bool,
}

impl CpuMeter {
    /// Starts a new event: the full budget is available again.
    pub fn reset(&self) {
        self.meter.lock().unwrap_or_else(|p| p.into_inner()).reset();
    }

    /// Starts a new event, resetting CPU accounting and the optional fuel meter.
    pub fn reset_event<T>(&self, store: &mut Store<T>) {
        self.reset();
        if self.fuel_enabled {
            // Enabled on this store: setting fuel cannot fail.
            let _ = store.set_fuel(u64::MAX);
        }
    }

    /// Fuel consumed since reset_event, or None when disabled. Read after
    /// the guest call and post-return. Counts Wasmtime work units, not native
    /// instructions or host work; suitable for per-event test assertions.
    /// Traps can leave unflushed accounting in Wasmtime, so failed events
    /// may be undercounted.
    pub fn fuel_used<T>(&self, store: &Store<T>) -> Option<u64> {
        if !self.fuel_enabled {
            return None;
        }
        store.get_fuel().ok().map(|remaining| u64::MAX - remaining)
    }

    /// Guest CPU time used since the last reset. Exact while the guest is
    /// not running, or when called from the thread running it.
    pub fn used(&self) -> Duration {
        self.meter
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
    let handle = CpuMeter {
        meter: Arc::clone(&meter),
        fuel_enabled: store.get_fuel().is_ok(),
    };

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
        let first = Duration::from_micros(200);
        m.enter_guest(t0, || unreachable!());
        m.leave_guest(t0 + first, || unreachable!()); // reached a check
        m.checked();
        // 40 ms of wall time, of which the thread ran 7 ms.
        m.enter_guest(t0 + ms(2), || ms(100));
        m.leave_guest(t0 + ms(42), || ms(107));
        assert_eq!(m.total(t0 + ms(42), || unreachable!()), first + ms(7));
        // Back to the wall clock for the segment after a plain host call.
        m.enter_guest(t0 + ms(50), || unreachable!());
        let short = Duration::from_micros(200);
        assert_eq!(
            m.total(t0 + ms(50) + short, || unreachable!()),
            first + ms(7) + short
        );
    }

    #[test]
    fn short_segments_never_read_the_cpu_clock() {
        // An unloaded event: many host calls, a little guest code each.
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        let step = Duration::from_micros(100);
        for i in 0..1_000 {
            let at = t0 + step * (2 * i);
            m.enter_guest(at, || unreachable!("short segments use the wall clock"));
            m.leave_guest(at + step, || {
                unreachable!("short segments use the wall clock")
            });
        }
        assert_eq!(m.total(t0, || unreachable!()), ms(100));
    }

    #[test]
    fn a_long_segment_switches_the_rest_of_the_event_to_the_cpu_clock() {
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        m.enter_guest(t0, || unreachable!());
        m.leave_guest(t0 + ms(30), || unreachable!()); // preempted: capped
        // Every later segment, short or not, is read with the CPU clock.
        m.enter_guest(t0 + ms(31), || ms(100));
        m.leave_guest(t0 + ms(32), || ms(100) + Duration::from_micros(300));
        assert_eq!(
            m.total(t0 + ms(32), || unreachable!()),
            ms(2) + Duration::from_micros(300)
        );
    }

    #[test]
    fn reset_keeps_the_cap() {
        let t0 = Instant::now();
        let mut m = Meter::new(TICK);
        m.checked();
        m.enter_guest(t0, || ms(1));
        m.leave_guest(t0 + ms(60), || ms(2)); // ran long
        m.reset();
        m.enter_guest(t0, || unreachable!("reset forgets the pending check"));
        m.leave_guest(t0 + ms(60), || unreachable!());
        assert_eq!(m.total(t0, || unreachable!()), ms(2));
        // ... and, from here, that a segment ran long.
        m.reset();
        m.enter_guest(t0, || unreachable!("reset forgets the long segment"));
        m.leave_guest(t0 + Duration::from_micros(100), || unreachable!());
    }

    /// One thread as the meter sees it: a wall clock and its CPU clock.
    struct Thread {
        wall: Instant,
        cpu: Duration,
    }

    impl Thread {
        fn new() -> Self {
            Self {
                wall: Instant::now(),
                cpu: ms(1_000),
            }
        }

        /// One stretch of guest code between two host calls: `wall` passes,
        /// of which the thread is on a core for `cpu`.
        fn segment(&mut self, m: &mut Meter, wall: Duration, cpu: Duration) {
            let at = self.cpu;
            m.enter_guest(self.wall, || at);
            self.wall += wall;
            self.cpu += cpu;
            let at = self.cpu;
            m.leave_guest(self.wall, || at);
        }

        /// A host call: time passes and the host thread does some work.
        fn host(&mut self, wall: Duration, cpu: Duration) {
            self.wall += wall;
            self.cpu += cpu;
        }
    }

    #[test]
    fn preempted_short_segments_are_charged_their_cpu_time() {
        // An event that makes many host calls with a little guest work in
        // between, on an oversubscribed core: each stretch of guest code is
        // preempted for 30 ms. The guest really runs 50 us per stretch.
        let mut t = Thread::new();
        let mut m = Meter::new(TICK);
        for _ in 0..100 {
            t.segment(&mut m, ms(30), Duration::from_micros(50));
            t.host(Duration::from_micros(20), Duration::from_micros(20));
        }
        let used = m.total(t.wall, || unreachable!("no segment running"));
        // 5 ms of guest CPU, plus at most one capped segment.
        assert!(used <= ms(5) + 2 * TICK, "charged {used:?} for 5 ms");
    }

    #[test]
    fn preempted_segments_that_end_at_an_epoch_check_are_charged_their_cpu_time() {
        // The usual shape of preemption: the epoch advanced while the thread
        // was off its core, so the guest's next check point fires the
        // callback as soon as it runs again. The segment ends at an epoch
        // check, the next one is measured precisely, then a host call.
        let mut t = Thread::new();
        let mut m = Meter::new(TICK);
        for _ in 0..100 {
            t.segment(&mut m, ms(30), Duration::from_micros(30));
            m.checked();
            t.segment(&mut m, Duration::from_micros(20), Duration::from_micros(20));
            t.host(Duration::from_micros(20), Duration::from_micros(20));
        }
        let used = m.total(t.wall, || unreachable!("no segment running"));
        // 5 ms of guest CPU, plus at most one capped segment.
        assert!(used <= ms(5) + 2 * TICK, "charged {used:?} for 5 ms");
    }

    #[test]
    fn many_short_busy_segments_add_up() {
        // A guest that burns its budget in short stretches between host
        // calls, without reaching an epoch check, is still charged in full,
        // loaded or not.
        for preempted in [Duration::ZERO, ms(5)] {
            let mut t = Thread::new();
            let mut m = Meter::new(TICK);
            for i in 0..100 {
                let wall = Duration::from_micros(900)
                    + if i % 2 == 0 {
                        preempted
                    } else {
                        Duration::ZERO
                    };
                t.segment(&mut m, wall, Duration::from_micros(900));
                t.host(Duration::from_micros(10), Duration::from_micros(10));
            }
            let used = m.total(t.wall, || unreachable!("no segment running"));
            assert!(
                used >= ms(90),
                "charged {used:?} for 90 ms (preempted {preempted:?})"
            );
            assert!(
                used <= ms(92),
                "charged {used:?} for 90 ms (preempted {preempted:?})"
            );
        }
    }

    #[test]
    fn leaving_without_entering_is_harmless() {
        let mut m = Meter::new(TICK);
        m.leave_guest(Instant::now(), || unreachable!());
        assert_eq!(m.total(Instant::now(), || unreachable!()), Duration::ZERO);
    }
}
