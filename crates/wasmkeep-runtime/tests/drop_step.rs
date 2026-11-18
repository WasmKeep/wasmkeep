//! The mailbox's step before a drop (ADR 0034, section 6): run with the
//! actor still registered, an event arriving during it wins and keeps the
//! state, only one state ever exists, and a stop closes the queue and
//! gives the step its deadline.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use wasmkeep_runtime::{
    ActorAddress, BoxError, BoxFuture, DropCause, MailboxConfig, MailboxError, Mailboxes,
    StepOutcome,
};

fn addr(name: &str) -> ActorAddress {
    ActorAddress::new("t", "c", name).unwrap()
}

/// A counter; `live` counts the states that exist, `most` the most that
/// ever existed at once.
struct Counter {
    value: u64,
    live: Arc<AtomicUsize>,
}

impl Drop for Counter {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How the fixture's step behaves.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Ready at once (as a strict state).
    Instant,
    /// Waits for a permit from `gate`, then answers `answer`.
    Gated,
    /// Yields a few times, then is ready (a short flush).
    Yielding,
    /// Panics.
    Panic,
}

struct Fixture {
    mailboxes: Mailboxes<Counter>,
    created: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
    /// Each step's cause, as it starts.
    causes: mpsc::UnboundedReceiver<DropCause>,
    /// Steps that ended (any outcome) and steps abandoned unfinished.
    finished: Arc<AtomicUsize>,
    abandoned: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
    answer: Arc<Mutex<StepOutcome>>,
}

/// Counts a step dropped before it finished.
struct Unfinished(Arc<AtomicUsize>, bool);

impl Drop for Unfinished {
    fn drop(&mut self) {
        if !self.1 {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn fixture(config: MailboxConfig, mode: Mode) -> Fixture {
    let created = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let (tx, causes) = mpsc::unbounded_channel();
    let finished = Arc::new(AtomicUsize::new(0));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let answer = Arc::new(Mutex::new(StepOutcome::Ready));
    let (c, l, m) = (Arc::clone(&created), Arc::clone(&live), Arc::clone(&most));
    let factory = move |_address, _residency| {
        let (c, l, m) = (Arc::clone(&c), Arc::clone(&l), Arc::clone(&m));
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            let now = l.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(now, Ordering::SeqCst);
            Ok::<_, BoxError>(Counter { value: 0, live: l })
        }
    };
    let (fin, ab, g, ans) = (
        Arc::clone(&finished),
        Arc::clone(&abandoned),
        Arc::clone(&gate),
        Arc::clone(&answer),
    );
    let step = step_fn(move |_state, cause| {
        tx.send(cause).unwrap();
        let (fin, ab, g, ans) = (
            Arc::clone(&fin),
            Arc::clone(&ab),
            Arc::clone(&g),
            Arc::clone(&ans),
        );
        Box::pin(async move {
            let mut guard = Unfinished(ab, false);
            let outcome = match mode {
                Mode::Instant => StepOutcome::Ready,
                Mode::Gated => {
                    g.acquire().await.unwrap().forget();
                    *ans.lock().unwrap()
                }
                Mode::Yielding => {
                    for _ in 0..3 {
                        tokio::task::yield_now().await;
                    }
                    StepOutcome::Ready
                }
                Mode::Panic => panic!("the step panicked"),
            };
            guard.1 = true;
            fin.fetch_add(1, Ordering::SeqCst);
            outcome
        })
    });
    Fixture {
        mailboxes: Mailboxes::new_with_step(config, factory, step),
        created,
        live,
        most,
        causes,
        finished,
        abandoned,
        gate,
        answer,
    }
}

/// Gives a closure the step's higher-ranked signature.
fn step_fn<F>(f: F) -> F
where
    F: for<'a> Fn(&'a mut Counter, DropCause) -> BoxFuture<'a, StepOutcome>,
{
    f
}

fn increment(state: &mut Counter) -> BoxFuture<'_, u64> {
    Box::pin(async move {
        let seen = state.value;
        tokio::task::yield_now().await;
        state.value = seen + 1;
        state.value
    })
}

fn idle(timeout: Duration) -> MailboxConfig {
    MailboxConfig {
        idle_timeout: timeout,
        ..MailboxConfig::default()
    }
}

/// An event that arrives while the step runs is served by the same state:
/// the hibernation is abandoned, nothing is rebuilt, and the step runs
/// again at the next idle check.
#[tokio::test(start_paused = true)]
async fn a_job_arriving_during_the_step_wins_and_keeps_the_state() {
    let mut f = fixture(idle(Duration::from_secs(30)), Mode::Gated);
    let a = addr("a");
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    // The step is waiting: the actor is still registered.
    assert!(f.mailboxes.contains(&a));
    assert_eq!(f.mailboxes.steps_running(), 1);

    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 2);
    assert_eq!(f.created.load(Ordering::SeqCst), 1, "a state was rebuilt");
    assert_eq!(f.abandoned.load(Ordering::SeqCst), 1);
    assert_eq!(f.mailboxes.steps_running(), 0);

    // The idle wait started over: the next step lets it go.
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    f.gate.add_permits(1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert!(!f.mailboxes.contains(&a));
    assert_eq!(f.finished.load(Ordering::SeqCst), 1);
    assert_eq!(f.live.load(Ordering::SeqCst), 0);
}

/// An eviction abandoned for an event stays pending: the actor goes once
/// the event is done.
#[tokio::test(start_paused = true)]
async fn an_eviction_abandoned_for_a_job_still_happens_after_it() {
    let mut f = fixture(idle(Duration::from_secs(600)), Mode::Gated);
    let a = addr("a");
    f.mailboxes.submit(&a, increment).await.unwrap();
    f.mailboxes.hibernate(&a);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Evicted);
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 2);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Evicted);
    f.gate.add_permits(1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
}

/// Events race hibernation constantly, against an instant and a slow
/// step: every event is served, and no two states ever exist at once.
async fn race(mode: Mode) {
    let f = fixture(idle(Duration::from_millis(1)), mode);
    let a = addr("a");
    let mut served = 0_u64;
    for i in 0..3000_u64 {
        f.mailboxes.submit(&a, increment).await.unwrap();
        served += 1;
        if i % 5 == 0 {
            tokio::time::sleep(Duration::from_micros(700 + (i % 7) * 100)).await;
        }
    }
    assert_eq!(served, 3000);
    assert_eq!(f.most.load(Ordering::SeqCst), 1, "two states at once");
    assert!(
        f.finished.load(Ordering::SeqCst) > 0,
        "no step ran, so the race was not exercised"
    );
    // Each state built was dropped through a finished step, but for the
    // one still resident.
    let created = f.created.load(Ordering::SeqCst);
    let live = f.live.load(Ordering::SeqCst);
    assert!(live <= 1);
    assert!(f.finished.load(Ordering::SeqCst) >= created - live);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_state_at_a_time_when_jobs_race_an_instant_step() {
    race(Mode::Instant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_state_at_a_time_when_jobs_race_a_slow_step() {
    race(Mode::Yielding).await;
}

/// Concurrent submitters against a gated step that is let go at random
/// moments: the most states at once stays one, every event is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_state_at_a_time_with_concurrent_submitters_and_a_gated_step() {
    let f = Arc::new(fixture(idle(Duration::from_millis(1)), Mode::Gated));
    let a = addr("a");
    let stop = Arc::new(AtomicBool::new(false));
    let opener = {
        let (f, stop) = (Arc::clone(&f), Arc::clone(&stop));
        tokio::spawn(async move {
            // A permit only for a step already waiting, so steps do wait
            // (up to a few hundred microseconds) and events meet them.
            while !stop.load(Ordering::SeqCst) {
                if f.mailboxes.steps_running() > 0 && f.gate.available_permits() == 0 {
                    f.gate.add_permits(1);
                }
                tokio::time::sleep(Duration::from_micros(300)).await;
            }
            f.gate.add_permits(1 << 16);
        })
    };
    let mut tasks = Vec::new();
    for t in 0..4_u64 {
        let (f, a) = (Arc::clone(&f), a.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..500_u64 {
                f.mailboxes.submit(&a, increment).await.unwrap();
                if (i + t) % 3 == 0 {
                    tokio::time::sleep(Duration::from_micros(500 + (i % 4) * 300)).await;
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    opener.await.unwrap();
    assert_eq!(f.most.load(Ordering::SeqCst), 1, "two states at once");
    assert!(
        f.created.load(Ordering::SeqCst) > 1,
        "the actor never hibernated, so the race was not exercised"
    );
    assert!(
        f.abandoned.load(Ordering::SeqCst) > 0,
        "no job ever beat a step, so the race was not exercised"
    );
}

/// A stop closes the queue before the step: nothing can join it, the step
/// is told the stop's deadline, and `drained` waits for the step.
#[tokio::test(start_paused = true)]
async fn during_a_stop_the_queue_is_closed_and_the_step_has_the_deadline() {
    let mut f = fixture(idle(Duration::from_secs(600)), Mode::Gated);
    let a = addr("a");
    f.mailboxes.submit(&a, increment).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    f.mailboxes.close_by(Some(deadline), true);
    assert_eq!(
        f.causes.recv().await.unwrap(),
        DropCause::Stop {
            deadline: Some(deadline),
            flush: true
        }
    );
    for name in ["a", "b"] {
        let r = f.mailboxes.submit(&addr(name), increment).await;
        assert!(matches!(r, Err(MailboxError::Closed(_))), "{name}: {r:?}");
        let r = f.mailboxes.submit_if_resident(&addr(name), increment).await;
        assert!(matches!(r, Err(MailboxError::Closed(_))), "{name}: {r:?}");
    }
    assert!(!f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.mailboxes.steps_running(), 1);
    // Keep is not honoured in a stop: the state goes.
    *f.answer.lock().unwrap() = StepOutcome::Keep;
    f.gate.add_permits(1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.live.load(Ordering::SeqCst), 0);
    assert_eq!(f.mailboxes.steps_running(), 0);
}

/// A stop that begins while a hibernation's step runs restarts the step
/// with the stop's cause; an app deleted is told not to flush.
#[tokio::test(start_paused = true)]
async fn a_stop_during_an_idle_step_restarts_it_as_a_stop() {
    let mut f = fixture(idle(Duration::from_secs(30)), Mode::Gated);
    f.mailboxes.submit(&addr("a"), increment).await.unwrap();
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    f.mailboxes.close_by(None, false);
    assert_eq!(
        f.causes.recv().await.unwrap(),
        DropCause::Stop {
            deadline: None,
            flush: false
        }
    );
    assert_eq!(f.abandoned.load(Ordering::SeqCst), 1);
    f.gate.add_permits(1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
}

/// A hibernation's step that keeps the state as a stop begins gives way to
/// the stop's step: the stop still flushes.
#[tokio::test(start_paused = true)]
async fn a_keep_as_a_stop_begins_runs_the_stop_step() {
    let mut f = fixture(idle(Duration::from_secs(30)), Mode::Gated);
    f.mailboxes.submit(&addr("a"), increment).await.unwrap();
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    *f.answer.lock().unwrap() = StepOutcome::Keep;
    // The step answers in the same poll as the close is seen.
    f.gate.add_permits(1);
    f.mailboxes.close_by(None, true);
    assert_eq!(
        f.causes.recv().await.unwrap(),
        DropCause::Stop {
            deadline: None,
            flush: true
        }
    );
    f.gate.add_permits(1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
}

/// A panic while the step is called (before its future exists) is
/// contained as well.
#[tokio::test(start_paused = true)]
async fn a_step_panicking_as_it_is_called_is_contained() {
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    let mailboxes = Mailboxes::new_with_step(
        idle(Duration::from_secs(30)),
        move |_address, _residency| {
            let l = Arc::clone(&l);
            async move {
                l.fetch_add(1, Ordering::SeqCst);
                Ok::<_, BoxError>(Counter { value: 0, live: l })
            }
        },
        step_fn(|_state, _cause| panic!("the step panicked before its future")),
    );
    let a = addr("a");
    mailboxes.submit(&a, increment).await.unwrap();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(mailboxes.submit(&a, increment).await.unwrap(), 1);
}

/// Close with an instant step behaves as without one: queued events
/// finish, every actor is dropped, the first close's terms count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_with_an_instant_step_behaves_as_before() {
    let mut f = fixture(MailboxConfig::default(), Mode::Instant);
    let a = addr("a");
    let queued: Vec<_> = (0..5).map(|_| f.mailboxes.submit(&a, increment)).collect();
    f.mailboxes.submit(&addr("b"), increment).await.unwrap();
    f.mailboxes.close();
    f.mailboxes.close_by(None, false);
    let mut results = Vec::new();
    for q in queued {
        results.push(q.await.unwrap());
    }
    assert_eq!(results, [1, 2, 3, 4, 5]);
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
    assert_eq!(f.live.load(Ordering::SeqCst), 0);
    for _ in 0..2 {
        assert_eq!(
            f.causes.recv().await.unwrap(),
            DropCause::Stop {
                deadline: None,
                flush: true
            }
        );
    }
}

/// `Keep` leaves the actor resident with its state; an eviction refused
/// that way does not leave it marked as leaving, so it is asked again.
#[tokio::test(start_paused = true)]
async fn keep_stays_resident_and_can_be_evicted_later() {
    let mut f = fixture(
        MailboxConfig {
            max_resident: 1,
            idle_timeout: Duration::from_secs(30),
            ..MailboxConfig::default()
        },
        Mode::Gated,
    );
    let a = addr("a");
    f.mailboxes.submit(&a, increment).await.unwrap();
    *f.answer.lock().unwrap() = StepOutcome::Keep;
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    f.gate.add_permits(1);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    assert!(f.mailboxes.contains(&a));
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 2);

    // b needs a's slot: a refuses once, then lets go.
    let b = f
        .mailboxes
        .submit_wait(&addr("b"), Duration::from_secs(60), increment);
    let b = tokio::spawn(b);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Evicted);
    f.gate.add_permits(1);
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Evicted);
    assert!(f.mailboxes.contains(&a));
    *f.answer.lock().unwrap() = StepOutcome::Ready;
    f.gate.add_permits(1);
    assert_eq!(b.await.unwrap().unwrap(), 1);
    assert!(!f.mailboxes.contains(&a));
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
    assert_eq!(f.most.load(Ordering::SeqCst), 1);
}

/// A panic in the step drops the state, as a panicking event does; the
/// actor keeps serving.
#[tokio::test(start_paused = true)]
async fn a_panicking_step_drops_the_state_and_the_actor_recovers() {
    let mut f = fixture(idle(Duration::from_secs(30)), Mode::Panic);
    let a = addr("a");
    f.mailboxes.submit(&a, increment).await.unwrap();
    assert_eq!(f.causes.recv().await.unwrap(), DropCause::Idle);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert_eq!(f.live.load(Ordering::SeqCst), 0);
    assert_eq!(f.mailboxes.steps_running(), 0);
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
}

/// A state whose drop takes time (a store flushing, a database closing),
/// for the window between an actor leaving the registry and its state
/// being gone.
struct SlowDrop {
    live: Arc<AtomicUsize>,
    dropping: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

impl Drop for SlowDrop {
    fn drop(&mut self) {
        self.dropping.store(true, Ordering::SeqCst);
        let (lock, cvar) = &*self.release;
        let released = lock.lock().unwrap();
        let _released = cvar
            .wait_timeout_while(released, Duration::from_secs(5), |r| !*r)
            .unwrap();
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

fn ready(_state: &mut SlowDrop, _cause: DropCause) -> BoxFuture<'_, StepOutcome> {
    Box::pin(async { StepOutcome::Ready })
}

fn noop(_state: &mut SlowDrop) -> BoxFuture<'_, ()> {
    Box::pin(async {})
}

/// An event arriving while a hibernating state is still being dropped
/// must wait for that drop: the actor stays registered until its state is
/// gone, so the event never builds a second state beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_during_the_drop_waits_for_the_state_to_be_gone() {
    let live = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let created = Arc::new(AtomicUsize::new(0));
    let dropping = Arc::new(AtomicBool::new(false));
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let (l, m, c, d, r) = (
        Arc::clone(&live),
        Arc::clone(&most),
        Arc::clone(&created),
        Arc::clone(&dropping),
        Arc::clone(&release),
    );
    let mailboxes = Mailboxes::new_with_step(
        idle(Duration::from_secs(600)),
        move |_address, _residency| {
            let (l, m, c, d, r) = (
                Arc::clone(&l),
                Arc::clone(&m),
                Arc::clone(&c),
                Arc::clone(&d),
                Arc::clone(&r),
            );
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                m.fetch_max(l.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                Ok::<_, BoxError>(SlowDrop {
                    live: l,
                    dropping: d,
                    release: r,
                })
            }
        },
        ready,
    );
    let a = addr("a");
    mailboxes.submit(&a, noop).await.unwrap();
    mailboxes.hibernate(&a);
    while !dropping.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // The old state is mid-drop: queue the next event now.
    let next = tokio::spawn(mailboxes.submit(&a, noop));
    // Give a wrongly admitted second actor time to build its state.
    for _ in 0..200 {
        if created.load(Ordering::SeqCst) > 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    next.await.unwrap().unwrap();
    assert_eq!(most.load(Ordering::SeqCst), 1, "two states at once");
    assert_eq!(created.load(Ordering::SeqCst), 2);
}
