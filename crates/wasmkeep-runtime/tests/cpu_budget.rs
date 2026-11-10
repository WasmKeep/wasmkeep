//! Per-event CPU budget enforced with epoch interruption.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::time::{Duration, Instant};

use wasmkeep_runtime::{CpuBudgetExceeded, Error, Runtime, RuntimeConfig};
use wasmtime::component::Linker;

const SPIN_WAT: &str = r#"
(component
  (core module $m (func (export "spin") (loop $l (br $l))))
  (core instance $i (instantiate $m))
  (func (export "spin") (canon lift (core func $i "spin"))))
"#;

/// Calls the host `host-sleep` import five times, then runs a short loop:
/// the loop's back-edge is an epoch check, so the budget is evaluated after
/// the host time has elapsed.
const HOST_WAIT_WAT: &str = r#"
(component
  (import "host-sleep" (func $hs))
  (core func $hs_core (canon lower (func $hs)))
  (core module $m
    (import "host" "sleep" (func $sleep))
    (func (export "run") (local $n i32)
      call $sleep call $sleep call $sleep call $sleep call $sleep
      (local.set $n (i32.const 1000))
      (loop $l
        (local.set $n (i32.sub (local.get $n) (i32.const 1)))
        (br_if $l (local.get $n)))))
  (core instance $host (export "sleep" (func $hs_core)))
  (core instance $i (instantiate $m (with "host" (instance $host))))
  (func (export "run") (canon lift (core func $i "run"))))
"#;

/// Counts `n` down to zero: a fixed amount of guest work.
const WORK_WAT: &str = r#"
(component
  (core module $m
    (func (export "work") (param $n i64)
      (loop $l
        (local.set $n (i64.sub (local.get $n) (i64.const 1)))
        (br_if $l (i64.ne (local.get $n) (i64.const 0))))))
  (core instance $i (instantiate $m))
  (func (export "work") (param "n" s64) (canon lift (core func $i "work"))))
"#;

fn runtime(budget: Duration) -> Runtime {
    Runtime::new(&RuntimeConfig {
        max_instances: 8,
        cpu_budget: budget,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

#[test]
fn infinite_loop_is_killed_after_the_budget() {
    let runtime = runtime(Duration::from_millis(50));
    let component = runtime.compile(&wat::parse_str(SPIN_WAT).unwrap()).unwrap();
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let mut store = runtime.new_store(());
    let instance = pre.instantiate(&mut store).unwrap();
    let spin = instance
        .get_typed_func::<(), ()>(&mut store, "spin")
        .unwrap();

    let started = Instant::now();
    let err = spin.call(&mut store, ()).unwrap_err();
    let elapsed = started.elapsed();

    let exceeded = err
        .downcast_ref::<CpuBudgetExceeded>()
        .unwrap_or_else(|| panic!("expected CpuBudgetExceeded, got {err:?}"));
    assert_eq!(exceeded.budget, Duration::from_millis(50));
    assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");

    // The next event gets a fresh store and a full budget.
    let mut store = runtime.new_store(());
    pre.instantiate(&mut store).unwrap();
}

#[test]
fn time_waiting_in_host_calls_is_not_counted() {
    let runtime = runtime(Duration::from_millis(50));
    let component = runtime
        .compile(&wat::parse_str(HOST_WAIT_WAT).unwrap())
        .unwrap();
    let mut linker = Linker::<()>::new(runtime.engine());
    linker
        .root()
        .func_wrap("host-sleep", |_, (): ()| {
            std::thread::sleep(Duration::from_millis(40));
            Ok(())
        })
        .unwrap();
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let mut store = runtime.new_store(());
    let instance = pre.instantiate(&mut store).unwrap();
    let run = instance
        .get_typed_func::<(), ()>(&mut store, "run")
        .unwrap();

    let started = Instant::now();
    run.call(&mut store, ()).unwrap();
    // 200 ms of wall time, far over the 50 ms budget, all of it in the host.
    assert!(started.elapsed() >= Duration::from_millis(200));
}

#[test]
fn async_stores_enforce_the_budget_too() {
    let runtime = runtime(Duration::from_millis(30));
    let component = runtime.compile(&wat::parse_str(SPIN_WAT).unwrap()).unwrap();
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let err = block_on(async {
        let mut store = runtime.new_async_store(());
        let instance = pre.instantiate_async(&mut store).await.unwrap();
        let spin = instance
            .get_typed_func::<(), ()>(&mut store, "spin")
            .unwrap();
        spin.call_async(&mut store, ()).await.unwrap_err()
    });
    assert!(err.downcast_ref::<CpuBudgetExceeded>().is_some(), "{err:?}");
}

#[test]
fn async_stores_yield_to_the_executor_while_spinning() {
    // A busy guest must hand its worker thread back on every epoch tick so
    // other requests are not starved. Count the `Pending`s the call returns.
    let runtime = runtime(Duration::from_millis(30));
    let component = runtime.compile(&wat::parse_str(SPIN_WAT).unwrap()).unwrap();
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let spin_and_count = |mut store: wasmtime::Store<()>| {
        let (result, yields) = block_on_counting(async {
            let instance = pre.instantiate_async(&mut store).await.unwrap();
            let spin = instance
                .get_typed_func::<(), ()>(&mut store, "spin")
                .unwrap();
            spin.call_async(&mut store, ()).await
        });
        assert!(
            result
                .unwrap_err()
                .downcast_ref::<CpuBudgetExceeded>()
                .is_some()
        );
        yields
    };

    let async_yields = spin_and_count(runtime.new_async_store(()));
    let sync_yields = spin_and_count(runtime.new_store(()));
    // About one yield per epoch tick. Ticks are 1 ms on Linux but coarser
    // where the OS coalesces timers (macOS: ~4 ms, so ~7 yields in 30 ms);
    // what matters is that the async store yields repeatedly and the sync
    // store never does.
    assert!(
        async_yields >= 3,
        "async store yielded only {async_yields} times"
    );
    assert_eq!(
        sync_yields, 0,
        "sync store should run to the trap without yielding"
    );
}

#[test]
fn reset_gives_a_reused_store_its_full_budget_again() {
    // An actor's store lives across events; each event starts with a reset.
    let runtime = runtime(Duration::from_millis(50));
    let spin = runtime.compile(&wat::parse_str(SPIN_WAT).unwrap()).unwrap();
    let pre = runtime
        .instantiate_pre(&Linker::<()>::new(runtime.engine()), &spin)
        .unwrap();

    let (mut store, meter) = runtime.new_metered_async_store(());
    let err = block_on(async {
        let instance = pre.instantiate_async(&mut store).await.unwrap();
        let spin = instance
            .get_typed_func::<(), ()>(&mut store, "spin")
            .unwrap();
        spin.call_async(&mut store, ()).await.unwrap_err()
    });
    assert!(err.downcast_ref::<CpuBudgetExceeded>().is_some());
    assert!(
        meter.used() >= Duration::from_millis(50),
        "{:?}",
        meter.used()
    );

    meter.reset();
    assert!(
        meter.used() < Duration::from_millis(5),
        "{:?}",
        meter.used()
    );
}

/// A loop count that takes the work guest at least `target` of CPU.
/// Measured on the thread's CPU clock, not the wall clock: on a loaded
/// machine (the full test suite) wall time includes preemption, which
/// would pick a count with too little work for the tests to mean anything.
fn calibrate(
    pre: &wasmtime::component::InstancePre<()>,
    runtime: &Runtime,
    target: Duration,
) -> i64 {
    let mut n: i64 = 1 << 20;
    loop {
        let (mut store, meter) = runtime.new_metered_async_store(());
        block_on(async {
            let instance = pre.instantiate_async(&mut store).await.unwrap();
            let work = instance
                .get_typed_func::<(i64,), ()>(&mut store, "work")
                .unwrap();
            work.call_async(&mut store, (n,)).await.unwrap();
        });
        if meter.used() >= target {
            return n;
        }
        n *= 2;
    }
}

#[test]
fn time_waiting_to_resume_after_a_yield_is_not_counted() {
    // An async guest yields on every tick. On a busy node the executor takes
    // a while to poll it again; that wait is other tasks' time, not this
    // guest's. Here each resume is 45 ms late, longer than the whole 40 ms
    // budget, so a single yield already puts the wall time past it while
    // the guest's own work stays near 8 ms. (How many yields there are
    // depends on how often the ticker really wakes, which a loaded machine
    // stretches: one loaded macOS run saw 2 for 8 ms of work.)
    let runtime = runtime(Duration::from_secs(10));
    let component = runtime.compile(&wat::parse_str(WORK_WAT).unwrap()).unwrap();
    let pre = runtime
        .instantiate_pre(&Linker::<()>::new(runtime.engine()), &component)
        .unwrap();
    let n = calibrate(&pre, &runtime, Duration::from_millis(8));

    let runtime = self::runtime(Duration::from_millis(40));
    let component = runtime.compile(&wat::parse_str(WORK_WAT).unwrap()).unwrap();
    let pre = runtime
        .instantiate_pre(&Linker::<()>::new(runtime.engine()), &component)
        .unwrap();
    let started = Instant::now();
    let (result, yields) = block_on_slowly(
        async {
            let mut store = runtime.new_async_store(());
            let instance = pre.instantiate_async(&mut store).await.unwrap();
            let work = instance
                .get_typed_func::<(i64,), ()>(&mut store, "work")
                .unwrap();
            work.call_async(&mut store, (n,)).await
        },
        Duration::from_millis(45),
    );
    let elapsed = started.elapsed();
    assert!(yields >= 1, "no yield: the test proves nothing");
    assert!(elapsed > Duration::from_millis(40), "{elapsed:?}");
    result.unwrap_or_else(|e| panic!("trapped after {yields} yields, {elapsed:?}: {e:?}"));
}

#[test]
fn time_the_thread_is_preempted_is_not_counted() {
    // On an overloaded node the OS runs other threads in the middle of a
    // guest's work. Busy threads, eight per CPU, slow about 10 ms of work
    // down to several times that in wall time, against a 40 ms budget.
    let runtime = runtime(Duration::from_secs(10));
    let component = runtime.compile(&wat::parse_str(WORK_WAT).unwrap()).unwrap();
    let pre = runtime
        .instantiate_pre(&Linker::<()>::new(runtime.engine()), &component)
        .unwrap();
    let n = calibrate(&pre, &runtime, Duration::from_millis(10));

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cpus = std::thread::available_parallelism().map_or(4, |n| n.get());
    let hogs: Vec<_> = (0..cpus * 8)
        .map(|_| {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();

    let runtime = self::runtime(Duration::from_millis(40));
    let component = runtime.compile(&wat::parse_str(WORK_WAT).unwrap()).unwrap();
    let pre = runtime
        .instantiate_pre(&Linker::<()>::new(runtime.engine()), &component)
        .unwrap();
    let mut store = runtime.new_store(());
    let instance = pre.instantiate(&mut store).unwrap();
    let work = instance
        .get_typed_func::<(i64,), ()>(&mut store, "work")
        .unwrap();
    let started = Instant::now();
    let result = work.call(&mut store, (n,));
    let elapsed = started.elapsed();

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for hog in hogs {
        hog.join().unwrap();
    }
    if elapsed <= Duration::from_millis(40) {
        eprintln!("not slowed down enough ({elapsed:?}): the test proves nothing here");
    }
    result.unwrap_or_else(|e| panic!("trapped after {elapsed:?}: {e:?}"));
}

#[test]
fn zero_epoch_tick_is_rejected() {
    let err = Runtime::new(&RuntimeConfig {
        epoch_tick: Duration::ZERO,
        ..RuntimeConfig::default()
    })
    .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "{err:?}");
}

/// Minimal single-threaded executor, so this crate's tests need no tokio.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    block_on_counting(fut).0
}

/// Like `block_on`, also returning how many times the future was pending.
fn block_on_counting<F: std::future::Future>(fut: F) -> (F::Output, usize) {
    block_on_slowly(fut, Duration::ZERO)
}

/// Like `block_on_counting`, polling again only `delay` after each wake, as
/// a busy executor would.
fn block_on_slowly<F: std::future::Future>(fut: F, delay: Duration) -> (F::Output, usize) {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    let mut pending = 0;
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return (out, pending),
            Poll::Pending => {
                pending += 1;
                std::thread::park();
                std::thread::sleep(delay);
            }
        }
    }
}
