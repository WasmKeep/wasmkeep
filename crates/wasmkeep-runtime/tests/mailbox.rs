//! Actor mailbox semantics: one event at a time per actor, in arrival order;
//! actors in parallel; failures isolated; hibernation without lost events.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use wasmkeep_runtime::{ActorAddress, BoxError, MailboxConfig, MailboxError, Mailboxes, Residency};

fn addr(name: &str) -> ActorAddress {
    ActorAddress::new("t", "c", name).unwrap()
}

/// Per-actor state: a counter, plus a shared count of live states so tests
/// can see hibernation drop them.
struct Counter {
    value: u64,
    live: Arc<AtomicUsize>,
}

impl Drop for Counter {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Fixture {
    mailboxes: Mailboxes<Counter>,
    created: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
}

fn fixture(config: MailboxConfig) -> Fixture {
    let created = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let (c, l) = (Arc::clone(&created), Arc::clone(&live));
    let mailboxes = Mailboxes::new(config, move |_address| {
        let (c, l) = (Arc::clone(&c), Arc::clone(&l));
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            l.fetch_add(1, Ordering::SeqCst);
            Ok::<_, BoxError>(Counter { value: 0, live: l })
        }
    });
    Fixture {
        mailboxes,
        created,
        live,
    }
}

/// Reads, yields several times (inviting interleaving), then writes.
fn increment(
    state: &mut Counter,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = u64> + Send + '_>> {
    Box::pin(async move {
        let seen = state.value;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        state.value = seen + 1;
        state.value
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_run_one_at_a_time_in_arrival_order() {
    let f = fixture(MailboxConfig::default());
    let a = addr("a");
    let pending: Vec<_> = (0..100)
        .map(|_| f.mailboxes.submit(&a, increment))
        .collect();
    let mut results = Vec::new();
    for p in pending {
        results.push(p.await.unwrap());
    }
    // Lost updates would show as repeats; reordering as a non-sorted list.
    assert_eq!(results, (1..=100).collect::<Vec<u64>>());
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submitters_never_interleave() {
    let f = fixture(MailboxConfig::default());
    let a = addr("a");
    let tasks: Vec<_> = (0..200)
        .map(|_| {
            let m = f.mailboxes.clone();
            let a = a.clone();
            tokio::spawn(async move { m.submit(&a, increment).await.unwrap() })
        })
        .collect();
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.unwrap());
    }
    results.sort_unstable();
    assert_eq!(results, (1..=200).collect::<Vec<u64>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_actors_run_in_parallel() {
    // A's event waits for B's event. With a global lock this would deadlock.
    let f = fixture(MailboxConfig::default());
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let a = f.mailboxes.submit(&addr("a"), move |_s| {
        Box::pin(async move {
            g.notified().await;
        })
    });
    let g = Arc::clone(&gate);
    let b = f.mailboxes.submit(&addr("b"), move |_s| {
        Box::pin(async move {
            g.notify_one();
        })
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        b.await.unwrap();
        a.await.unwrap();
    })
    .await
    .expect("actors did not run in parallel");
    assert_eq!(f.mailboxes.resident(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_event_fails_alone_and_the_actor_restarts_fresh() {
    let f = fixture(MailboxConfig::default());
    let a = addr("a");
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);

    let err = f
        .mailboxes
        .submit(&a, |_s: &mut Counter| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async { panic!("host bug") })
        })
        .await
        .unwrap_err();
    assert!(matches!(err, MailboxError::Gone(_)), "{err:?}");

    // Fresh state: the in-memory counter restarted (durable state will live
    // in SQLite, outside this struct).
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_queued_behind_a_panic_still_run() {
    let f = fixture(MailboxConfig::default());
    let a = addr("a");
    for _ in 0..20 {
        // Queue a panicking event and a normal one back to back.
        let bad = f.mailboxes.submit(&a, |_s: &mut Counter| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async { panic!("host bug") })
        });
        let good = f.mailboxes.submit(&a, increment);
        assert!(matches!(bad.await, Err(MailboxError::Gone(_))));
        // State is discarded after a panic, so the counter starts over.
        assert_eq!(good.await.unwrap(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_failure_is_reported_and_retried() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let a2 = Arc::clone(&attempts);
    let mailboxes: Mailboxes<u64> = Mailboxes::new(MailboxConfig::default(), move |_| {
        let n = a2.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                Err::<u64, BoxError>("disk on fire".into())
            } else {
                Ok(7)
            }
        }
    });
    let a = addr("a");
    fn read(s: &mut u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = u64> + Send + '_>> {
        let v = *s;
        Box::pin(async move { v })
    }
    let err = mailboxes.submit(&a, read).await.unwrap_err();
    match err {
        MailboxError::Init { source, .. } => assert_eq!(source.to_string(), "disk on fire"),
        other => panic!("expected Init, got {other:?}"),
    }
    assert_eq!(mailboxes.submit(&a, read).await.unwrap(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_queue_refuses_new_events() {
    let f = fixture(MailboxConfig {
        queue_capacity: 1,
        ..MailboxConfig::default()
    });
    let a = addr("a");
    let (started_tx, started) = oneshot::channel();
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let running = f.mailboxes.submit(&a, move |_s| {
        Box::pin(async move {
            let _ = started_tx.send(());
            g.notified().await;
        })
    });
    started.await.unwrap(); // first event is running, queue empty

    let queued = f.mailboxes.submit(&a, increment); // fills the queue
    let refused = f.mailboxes.submit(&a, increment).await.unwrap_err();
    assert!(matches!(refused, MailboxError::Full(_)), "{refused:?}");

    gate.notify_one();
    running.await.unwrap();
    assert_eq!(queued.await.unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn idle_actors_hibernate_and_wake_on_the_next_event() {
    let f = fixture(MailboxConfig {
        idle_timeout: Duration::from_secs(30),
        ..MailboxConfig::default()
    });
    let a = addr("a");
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);
    assert_eq!(
        (f.mailboxes.resident(), f.live.load(Ordering::SeqCst)),
        (1, 1)
    );

    tokio::time::sleep(Duration::from_secs(29)).await;
    assert_eq!(f.mailboxes.resident(), 1, "hibernated too early");

    tokio::time::sleep(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(f.mailboxes.resident(), 0);
    assert_eq!(f.live.load(Ordering::SeqCst), 0, "state was not dropped");

    // Wakes with fresh in-memory state.
    assert_eq!(f.mailboxes.submit(&a, increment).await.unwrap(), 1);
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_event_is_lost_when_it_races_hibernation() {
    // A 1 ms idle timeout makes actors hibernate constantly while events
    // keep arriving at about that interval: every event must still run.
    let f = fixture(MailboxConfig {
        idle_timeout: Duration::from_millis(1),
        ..MailboxConfig::default()
    });
    let mut ok = 0;
    for i in 0..1500_u64 {
        let a = addr(&format!("a{}", i % 3));
        f.mailboxes.submit(&a, increment).await.unwrap();
        ok += 1;
        if i % 7 == 0 {
            tokio::time::sleep(Duration::from_micros(900 + (i % 5) * 100)).await;
        }
    }
    assert_eq!(ok, 1500);
    assert!(
        f.created.load(Ordering::SeqCst) > 3,
        "actors never hibernated, so the race was not exercised"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_finishes_queued_events_then_drops_every_actor() {
    let f = fixture(MailboxConfig::default()); // idle timeout: 30 s
    // Actor a has a slow event running and two more queued behind it.
    let gate = Arc::new(Notify::new());
    let a = addr("a");
    let g = Arc::clone(&gate);
    let slow = f.mailboxes.submit(&a, move |s: &mut Counter| {
        Box::pin(async move {
            g.notified().await;
            s.value += 1;
            s.value
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = u64> + Send + '_>>
    });
    let queued: Vec<_> = (0..2).map(|_| f.mailboxes.submit(&a, increment)).collect();
    // Actor b is idle and resident.
    assert_eq!(f.mailboxes.submit(&addr("b"), increment).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.mailboxes.resident(), 2);

    f.mailboxes.close();
    // New events are refused, for resident and new actors alike.
    for name in ["a", "b", "c"] {
        let r = f.mailboxes.submit(&addr(name), increment).await;
        assert!(matches!(r, Err(MailboxError::Closed(_))), "{name}: {r:?}");
    }
    // b had nothing queued: dropped at once, long before its idle timeout.
    assert!(
        !f.mailboxes.drained(Duration::from_millis(200)).await,
        "a is still busy"
    );
    assert_eq!(
        f.live.load(Ordering::SeqCst),
        1,
        "b's state was not dropped"
    );

    // a finishes everything queued before it stops.
    gate.notify_one();
    assert_eq!(slow.await.unwrap(), 1);
    let mut results = Vec::new();
    for q in queued {
        results.push(q.await.unwrap());
    }
    assert_eq!(results, [2, 3]);
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
    assert_eq!(
        f.live.load(Ordering::SeqCst),
        0,
        "a's state was not dropped"
    );
}

/// State whose drop is slow (closing a database, releasing a lease starts
/// there) and records when it finished.
struct SlowDrop(Arc<std::sync::atomic::AtomicBool>);

impl Drop for SlowDrop {
    fn drop(&mut self) {
        std::thread::sleep(Duration::from_millis(200));
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drained_waits_until_every_state_is_dropped() {
    // An actor leaves the registry before its state is dropped; a shutdown
    // that stopped waiting there would exit before the drop's work (a lease
    // release) began.
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d = Arc::clone(&dropped);
    let mailboxes = Mailboxes::new(MailboxConfig::default(), move |_address| {
        let d = Arc::clone(&d);
        async move { Ok::<_, BoxError>(SlowDrop(d)) }
    });
    mailboxes
        .submit(&addr("a"), |_s: &mut SlowDrop| Box::pin(async {}))
        .await
        .unwrap();
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(5)).await);
    assert!(
        dropped.load(Ordering::SeqCst),
        "drained before the state was dropped"
    );
}

#[tokio::test]
async fn past_the_resident_cap_the_least_recently_active_idle_actor_makes_room() {
    // Cap 2, idle timeout long: a third actor makes the least recently
    // active of the first two hibernate, keeping its value in no memory
    // (a fresh state on its next event), while the other keeps its own.
    let f = fixture(MailboxConfig {
        max_resident: 2,
        idle_timeout: Duration::from_secs(60),
        ..MailboxConfig::default()
    });
    assert_eq!(f.mailboxes.submit(&addr("a"), increment).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(f.mailboxes.submit(&addr("b"), increment).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_capacity(f.mailboxes.submit(&addr("c"), increment).await);
    let started = std::time::Instant::now();
    while f.live.load(Ordering::SeqCst) > 1 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "nothing made room"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(f.mailboxes.submit(&addr("c"), increment).await.unwrap(), 1);
    // a was the oldest: rebuilding it first asks c to retire; b keeps its state.
    assert_eq!(f.mailboxes.submit(&addr("b"), increment).await.unwrap(), 2);
    assert_capacity(f.mailboxes.submit(&addr("a"), increment).await);
    let started = std::time::Instant::now();
    while f.live.load(Ordering::SeqCst) > 1 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "nothing made room"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(f.mailboxes.submit(&addr("a"), increment).await.unwrap(), 1);
    assert_eq!(f.created.load(Ordering::SeqCst), 4);
}

fn assert_capacity<T: std::fmt::Debug>(result: Result<T, MailboxError>) {
    assert!(
        matches!(result, Err(MailboxError::Capacity(_))),
        "expected a capacity rejection, got {result:?}"
    );
}

#[tokio::test]
async fn a_busy_actor_keeps_its_queue_but_new_names_are_refused_at_capacity() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let (entered, ready) = oneshot::channel();
    let running = f.mailboxes.submit(&addr("a"), move |state: &mut Counter| {
        Box::pin(async move {
            entered.send(()).unwrap();
            g.notified().await;
            state.value += 1;
            state.value
        })
    });
    ready.await.unwrap();
    assert_capacity(f.mailboxes.submit(&addr("b"), increment).await);
    let queued = f.mailboxes.submit(&addr("a"), increment);
    gate.notify_one();
    assert_eq!(running.await.unwrap(), 1);
    assert_eq!(queued.await.unwrap(), 2);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
}

/// Always release a blocking destructor, including when an assertion fails.
struct DropGate(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl DropGate {
    fn new() -> Self {
        Self(Arc::new((
            std::sync::Mutex::new(false),
            std::sync::Condvar::new(),
        )))
    }

    fn release(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

impl Drop for DropGate {
    fn drop(&mut self) {
        self.release();
    }
}

struct HeldDrop {
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    entered: Option<oneshot::Sender<()>>,
}

impl Drop for HeldDrop {
    fn drop(&mut self) {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        let (lock, wake) = &*self.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deregistration_does_not_free_capacity_before_the_state_is_dropped() {
    let gate = DropGate::new();
    let (entered, ready) = oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered)));
    let created = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&created);
    let g = Arc::clone(&gate.0);
    let mailboxes = Mailboxes::new(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |_address| {
            let state = HeldDrop {
                gate: Arc::clone(&g),
                entered: entered.lock().unwrap().take(),
            };
            count.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, BoxError>(state) }
        },
    );
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    assert_capacity(mailboxes.submit(&addr("b"), |_| Box::pin(async {})).await);
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mailboxes.resident(),
        0,
        "actor must leave the registry before Drop finishes"
    );
    assert_capacity(mailboxes.submit(&addr("b"), |_| Box::pin(async {})).await);
    assert_eq!(created.load(Ordering::SeqCst), 1);
    assert!(!mailboxes.drained(Duration::from_millis(20)).await);
    gate.release();
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
    mailboxes
        .submit(&addr("b"), |_| Box::pin(async {}))
        .await
        .unwrap();
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_new_names_reserve_capacity_before_their_factories_run() {
    let f = fixture(MailboxConfig {
        max_resident: 2,
        ..MailboxConfig::default()
    });
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let submitted = Arc::new(AtomicUsize::new(0));
    let pending: Vec<_> = (0..32)
        .map(|i| {
            let mailboxes = f.mailboxes.clone();
            let gate = Arc::clone(&gate);
            let submitted = Arc::clone(&submitted);
            tokio::spawn(async move {
                let event =
                    mailboxes.submit(&addr(&format!("actor-{i}")), move |state: &mut Counter| {
                        Box::pin(async move {
                            gate.acquire().await.unwrap().forget();
                            state.value += 1;
                            state.value
                        })
                    });
                submitted.fetch_add(1, Ordering::SeqCst);
                event.await
            })
        })
        .collect();
    tokio::time::timeout(Duration::from_secs(5), async {
        while submitted.load(Ordering::SeqCst) != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Release before asserting, so a regression cannot strand the admitted jobs.
    gate.add_permits(32);
    let mut admitted = 0;
    for event in pending {
        match event.await.unwrap() {
            Ok(1) => admitted += 1,
            other => assert_capacity(other),
        }
    }
    assert_eq!(admitted, 2);
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn a_zero_resident_cap_never_starts_an_actor() {
    let f = fixture(MailboxConfig {
        max_resident: 0,
        ..MailboxConfig::default()
    });
    assert_capacity(f.mailboxes.submit(&addr("a"), increment).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 0);
    assert_eq!(f.mailboxes.resident(), 0);
    assert!(f.mailboxes.drained(Duration::ZERO).await);
}

#[tokio::test]
async fn closing_at_capacity_finishes_accepted_jobs_without_admitting_retries() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let (entered, ready) = oneshot::channel();
    let running = f.mailboxes.submit(&addr("a"), move |state: &mut Counter| {
        Box::pin(async move {
            entered.send(()).unwrap();
            g.notified().await;
            state.value += 1;
            state.value
        })
    });
    ready.await.unwrap();
    let queued = f.mailboxes.submit(&addr("a"), increment);
    assert_capacity(f.mailboxes.submit(&addr("b"), increment).await);
    f.mailboxes.close();
    for name in ["a", "b"] {
        assert!(matches!(
            f.mailboxes.submit(&addr(name), increment).await,
            Err(MailboxError::Closed(_))
        ));
    }
    gate.notify_one();
    assert_eq!(running.await.unwrap(), 1);
    assert_eq!(queued.await.unwrap(), 2);
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn detached_resources_keep_capacity_reserved_after_the_actor_retires() {
    let worker: Arc<std::sync::Mutex<Option<Residency>>> = Arc::default();
    let detached = Arc::clone(&worker);
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |address: ActorAddress, residency| {
            if address.name() == "a" {
                *detached.lock().unwrap() = Some(residency);
            }
            async { Ok::<_, BoxError>(()) }
        },
    );
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    assert_capacity(mailboxes.submit(&addr("b"), |_| Box::pin(async {})).await);
    tokio::time::timeout(Duration::from_secs(2), async {
        while mailboxes.resident() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The actor is gone, but its detached resource still holds its slot.
    assert_capacity(mailboxes.submit(&addr("b"), |_| Box::pin(async {})).await);
    assert!(!mailboxes.drained(Duration::ZERO).await);
    drop(worker.lock().unwrap().take());
    assert!(mailboxes.drained(Duration::ZERO).await);
    mailboxes
        .submit(&addr("b"), |_| Box::pin(async {}))
        .await
        .unwrap();
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn shutdown_waits_for_a_residency_retained_outside_the_actor() {
    let worker: Arc<std::sync::Mutex<Option<Residency>>> = Arc::default();
    let detached = Arc::clone(&worker);
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |_address, residency| {
            *detached.lock().unwrap() = Some(residency);
            async { Ok::<_, BoxError>(()) }
        },
    );
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    mailboxes.close();
    assert!(!mailboxes.drained(Duration::from_millis(20)).await);
    assert_eq!(mailboxes.resident(), 0);
    assert!(matches!(
        mailboxes.submit(&addr("b"), |_| Box::pin(async {})).await,
        Err(MailboxError::Closed(_))
    ));
    drop(worker.lock().unwrap().take());
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn a_panicking_actor_does_not_reuse_capacity_held_by_its_detached_resource() {
    let worker: Arc<std::sync::Mutex<Option<Residency>>> = Arc::default();
    let detached = Arc::clone(&worker);
    let created = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&created);
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |_address, residency| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                *detached.lock().unwrap() = Some(residency);
            }
            async { Ok::<_, BoxError>(()) }
        },
    );
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    let failed = mailboxes.submit(&addr("a"), |_| Box::pin(async { panic!("host failed") }));
    let queued = mailboxes.submit(&addr("a"), |_| Box::pin(async {}));
    assert!(matches!(failed.await, Err(MailboxError::Gone(_))));
    assert_capacity(queued.await);
    assert_eq!(
        created.load(Ordering::SeqCst),
        1,
        "factory reused a live resource's slot"
    );
    drop(worker.lock().unwrap().take());
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    assert_eq!(created.load(Ordering::SeqCst), 2);
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test(start_paused = true)]
async fn a_failed_factory_can_be_evicted_to_make_room_for_another_name() {
    let mailboxes = Mailboxes::new(
        MailboxConfig {
            max_resident: 1,
            idle_timeout: Duration::from_secs(60),
            ..MailboxConfig::default()
        },
        |address| async move {
            if address.name() == "bad" {
                Err::<(), BoxError>("factory failed".into())
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(
        mailboxes.submit(&addr("bad"), |_| Box::pin(async {})).await,
        Err(MailboxError::Init { .. })
    ));
    assert_capacity(
        mailboxes
            .submit(&addr("good"), |_| Box::pin(async {}))
            .await,
    );
    assert!(
        mailboxes.drained(Duration::from_secs(1)).await,
        "failed factory was left marked busy"
    );
    mailboxes
        .submit(&addr("good"), |_| Box::pin(async {}))
        .await
        .unwrap();
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn a_cold_waiting_submission_reclaims_idle_capacity_without_client_retry() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    f.mailboxes.submit(&addr("a"), increment).await.unwrap();
    assert_eq!(
        f.mailboxes
            .submit_wait(&addr("b"), Duration::from_millis(250), increment)
            .await
            .unwrap(),
        1
    );
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test(start_paused = true)]
async fn a_capacity_wait_expires_without_running_or_replaying_the_job() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let (entered, ready) = oneshot::channel();
    let running = f.mailboxes.submit(&addr("a"), move |_| {
        Box::pin(async move {
            entered.send(()).unwrap();
            g.notified().await;
        })
    });
    ready.await.unwrap();
    let started = tokio::time::Instant::now();
    assert_capacity(
        f.mailboxes
            .submit_wait(&addr("b"), Duration::from_millis(250), increment)
            .await,
    );
    assert_eq!(started.elapsed(), Duration::from_millis(250));
    gate.notify_one();
    running.await.unwrap();
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn waiting_submissions_preserve_immediate_order_for_an_existing_actor() {
    let f = fixture(MailboxConfig::default());
    let first = f
        .mailboxes
        .submit_wait(&addr("a"), Duration::from_millis(250), increment);
    let second = f.mailboxes.submit(&addr("a"), increment);
    assert_eq!(second.await.unwrap(), 2);
    assert_eq!(first.await.unwrap(), 1);
}

#[tokio::test]
async fn a_canceled_capacity_wait_never_runs_its_unaccepted_job() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    f.mailboxes.submit(&addr("a"), increment).await.unwrap();
    let waiting = f
        .mailboxes
        .submit_wait(&addr("b"), Duration::from_millis(250), increment);
    drop(waiting);
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
    f.mailboxes.submit(&addr("b"), increment).await.unwrap();
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn release_before_the_wait_future_is_polled_is_not_lost() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    f.mailboxes.submit(&addr("a"), increment).await.unwrap();
    let waiting = f
        .mailboxes
        .submit_wait(&addr("b"), Duration::from_millis(250), increment);
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
    assert_eq!(waiting.await.unwrap(), 1);
}

#[tokio::test]
async fn a_capacity_wait_wakes_when_a_busy_actor_becomes_idle() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        ..MailboxConfig::default()
    });
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let (entered, ready) = oneshot::channel();
    let running = f.mailboxes.submit(&addr("a"), move |_| {
        Box::pin(async move {
            entered.send(()).unwrap();
            g.notified().await;
        })
    });
    ready.await.unwrap();
    let waiting = f
        .mailboxes
        .submit_wait(&addr("b"), Duration::from_millis(250), increment);
    gate.notify_one();
    running.await.unwrap();
    assert_eq!(waiting.await.unwrap(), 1);
}

#[tokio::test]
async fn shutdown_wakes_an_unaccepted_capacity_wait() {
    let f = fixture(MailboxConfig {
        max_resident: 0,
        ..MailboxConfig::default()
    });
    let waiting = f
        .mailboxes
        .submit_wait(&addr("a"), Duration::from_secs(60), increment);
    f.mailboxes.close();
    assert!(matches!(waiting.await, Err(MailboxError::Closed(_))));
    assert_eq!(f.created.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn requested_hibernation_finishes_queued_events_before_releasing_the_slot() {
    let f = fixture(MailboxConfig {
        max_resident: 1,
        idle_timeout: Duration::from_secs(600),
        ..MailboxConfig::default()
    });
    let gate = Arc::new(Notify::new());
    let g = Arc::clone(&gate);
    let (entered, ready) = oneshot::channel();
    let running = f.mailboxes.submit(&addr("a"), move |_| {
        Box::pin(async move {
            entered.send(()).unwrap();
            g.notified().await;
        })
    });
    ready.await.unwrap();
    let queued = f.mailboxes.submit(&addr("a"), increment);
    f.mailboxes.hibernate(&addr("a"));
    assert!(f.mailboxes.contains(&addr("a")));
    gate.notify_one();
    running.await.unwrap();
    assert_eq!(queued.await.unwrap(), 1);
    assert!(f.mailboxes.drained(Duration::from_secs(1)).await);
    assert!(!f.mailboxes.contains(&addr("a")));
    f.mailboxes.submit(&addr("b"), increment).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn admission_wait_does_not_cancel_an_accepted_slow_event_or_replay_it() {
    let f = fixture(MailboxConfig::default());
    let result = f
        .mailboxes
        .submit_wait(&addr("a"), Duration::from_millis(250), |state| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                state.value += 1;
                state.value
            })
        })
        .await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(f.mailboxes.submit(&addr("a"), increment).await.unwrap(), 2);
}

#[tokio::test]
async fn a_polled_capacity_wait_can_be_canceled_without_running_its_job() {
    let f = fixture(MailboxConfig {
        max_resident: 0,
        ..MailboxConfig::default()
    });
    let called = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&called);
    let mut waiting = Box::pin(f.mailboxes.submit_wait(
        &addr("a"),
        Duration::from_secs(60),
        move |_| Box::pin(async move { calls.fetch_add(1, Ordering::SeqCst) }),
    ));
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(waiting);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::ZERO).await);
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(f.created.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn capacity_wait_is_notified_when_the_last_detached_resource_drops() {
    let worker: Arc<std::sync::Mutex<Option<Residency>>> = Arc::default();
    let detached = Arc::clone(&worker);
    let mailboxes = Mailboxes::new_with_residency(
        MailboxConfig {
            max_resident: 1,
            ..MailboxConfig::default()
        },
        move |address: ActorAddress, residency| {
            if address.name() == "a" {
                *detached.lock().unwrap() = Some(residency);
            }
            async { Ok::<_, BoxError>(()) }
        },
    );
    mailboxes
        .submit(&addr("a"), |_| Box::pin(async {}))
        .await
        .unwrap();
    let mut waiting = Box::pin(
        mailboxes.submit_wait(&addr("b"), Duration::from_secs(5), |_| {
            Box::pin(async { 7 })
        }),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while mailboxes.resident() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(worker.lock().unwrap().take());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap(),
        7
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_capacity_waiters_run_each_job_once_without_exceeding_the_cap() {
    let f = fixture(MailboxConfig {
        max_resident: 2,
        ..MailboxConfig::default()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let mut events = Vec::new();
    for actor in 0..32 {
        let calls = Arc::clone(&calls);
        events.push(tokio::spawn(f.mailboxes.submit_wait(
            &addr(&format!("a{actor}")),
            Duration::from_secs(5),
            move |state| {
                Box::pin(async move {
                    assert!(state.live.load(Ordering::SeqCst) <= 2);
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    assert!(state.live.load(Ordering::SeqCst) <= 2);
                    actor
                })
            },
        )));
    }
    for (actor, event) in events.into_iter().enumerate() {
        assert_eq!(event.await.unwrap().unwrap(), actor);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 32);
    assert_eq!(f.created.load(Ordering::SeqCst), 32);
    f.mailboxes.close();
    assert!(f.mailboxes.drained(Duration::from_secs(2)).await);
}

/// A state that records its drop, and (for one actor) blocks in Drop until
/// released: a slot whose resources take a while to close.
struct Recorded {
    name: String,
    dropped: Arc<std::sync::Mutex<Vec<String>>>,
    gate: Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>,
}

impl Drop for Recorded {
    fn drop(&mut self) {
        self.dropped.lock().unwrap().push(self.name.clone());
        if let Some(gate) = &self.gate {
            let (lock, wake) = &**gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_admission_evicts_one_actor_however_often_it_is_woken() {
    // Four slots: three idle actors and a busy one. A new name waits for a
    // slot; the oldest idle actor is asked to go, but its resources take a
    // while to close. Meanwhile the busy actor finishes event after event,
    // each waking the waiter: none of those wake-ups may evict another
    // actor, since the slot it needs is already on its way.
    let gate = DropGate::new();
    let dropped = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (g, d) = (Arc::clone(&gate.0), Arc::clone(&dropped));
    let mailboxes = Mailboxes::new(
        MailboxConfig {
            max_resident: 4,
            ..MailboxConfig::default()
        },
        move |address: ActorAddress| {
            let state = Recorded {
                name: address.name().to_owned(),
                dropped: Arc::clone(&d),
                gate: (address.name() == "old").then(|| Arc::clone(&g)),
            };
            async move { Ok::<_, BoxError>(state) }
        },
    );
    for name in ["old", "idle-1", "idle-2", "hot"] {
        mailboxes
            .submit(&addr(name), |_| Box::pin(async {}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let waiting = tokio::spawn(
        mailboxes.submit_wait(&addr("new"), Duration::from_secs(5), |_| Box::pin(async {})),
    );
    // `old` was asked to go and is closing (held).
    let started = std::time::Instant::now();
    while !dropped.lock().unwrap().contains(&"old".to_owned()) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "old never evicted"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for _ in 0..20 {
        mailboxes
            .submit(&addr("hot"), |_| Box::pin(async {}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        *dropped.lock().unwrap(),
        ["old"],
        "wake-ups evicted more actors than the one slot needed"
    );
    gate.release();
    waiting.await.unwrap().unwrap();
    assert_eq!(*dropped.lock().unwrap(), ["old"]);
    mailboxes.close();
    assert!(mailboxes.drained(Duration::from_secs(2)).await);
}
