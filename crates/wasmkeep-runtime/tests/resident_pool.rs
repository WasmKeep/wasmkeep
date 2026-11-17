//! A resident limit shared by the mailboxes of several apps (ADR 0038,
//! section 8): residents of every app count against one pool; when it is
//! full, a wake-up evicts the least recently active idle resident of an
//! app above its floor, and waits only when none exists.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use wasmkeep_runtime::{ActorAddress, BoxError, MailboxConfig, Mailboxes, ResidentPool};

fn addr(name: &str) -> ActorAddress {
    ActorAddress::new("t", "c", name).unwrap()
}

struct Live(Arc<AtomicUsize>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An app's mailboxes in `pool` with `floor`; their live states counted.
fn app(pool: &Arc<ResidentPool>, floor: Option<usize>) -> (Mailboxes<Live>, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    let m = Mailboxes::new(
        MailboxConfig {
            idle_timeout: Duration::from_secs(60),
            ..MailboxConfig::default()
        },
        move |_| {
            let l = Arc::clone(&l);
            async move {
                l.fetch_add(1, Ordering::SeqCst);
                Ok::<_, BoxError>(Live(l))
            }
        },
    );
    m.join_pool(pool, floor);
    (m, live)
}

fn touch(_: &mut Live) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
    Box::pin(async {})
}

async fn fill(m: &Mailboxes<Live>, names: &[&str]) {
    for n in names {
        m.submit(&addr(n), touch).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_pool_of_one_apps_idle_residents_makes_room_for_anothers_wake_up() {
    let pool = ResidentPool::new(4);
    let (a, a_live) = app(&pool, None);
    let (b, b_live) = app(&pool, None);
    fill(&a, &["1", "2", "3", "4"]).await;
    assert_eq!((a_live.load(Ordering::SeqCst), pool.used()), (4, 4));
    // B's wake-up evicts A's least recently active idle resident.
    b.submit_wait(&addr("x"), Duration::from_secs(5), touch)
        .await
        .expect("B got a slot");
    assert_eq!(a_live.load(Ordering::SeqCst), 3);
    assert_eq!(b_live.load(Ordering::SeqCst), 1);
    assert_eq!(pool.used(), 4);
    // The one evicted was A's oldest: "1" starts again, the others kept.
    for n in ["2", "3", "4"] {
        a.submit(&addr(n), touch).await.unwrap();
    }
    assert_eq!(a_live.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_at_its_floor_keeps_its_residents_and_the_other_waits_when_nothing_else_is_idle() {
    let pool = ResidentPool::new(4);
    // A's floor is the whole pool: it is never above it.
    let (a, a_live) = app(&pool, Some(4));
    let (b, _b_live) = app(&pool, Some(0));
    fill(&a, &["1", "2", "3", "4"]).await;
    let waited = b
        .submit_wait(&addr("x"), Duration::from_millis(300), touch)
        .await;
    assert!(waited.is_err(), "B waited, then was refused");
    assert_eq!(a_live.load(Ordering::SeqCst), 4, "A kept its floor");
    // A's residents going make room: B gets in.
    a.close();
    assert!(a.drained(Duration::from_secs(5)).await);
    b.submit_wait(&addr("x"), Duration::from_secs(5), touch)
        .await
        .expect("B got the slot A gave back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residents_of_every_app_count_against_one_pool() {
    let pool = ResidentPool::new(3);
    let (a, _) = app(&pool, Some(3));
    let (b, b_live) = app(&pool, Some(3));
    fill(&a, &["1", "2"]).await;
    fill(&b, &["1"]).await;
    assert_eq!(pool.used(), 3);
    // Both at their floor: no one evicts, a new name waits.
    assert!(
        b.submit_wait(&addr("2"), Duration::from_millis(200), touch)
            .await
            .is_err()
    );
    assert_eq!(b_live.load(Ordering::SeqCst), 1);
}
