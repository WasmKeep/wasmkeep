//! Allocator maintenance after actors and socket sessions leave memory.

use std::sync::{Arc, Weak};
use std::time::Duration;

use wasmkeep_runtime::ResidentPool;

use super::ServiceInner;

pub(super) fn spawn(
    service: Weak<ServiceInner>,
    pool: Option<Arc<ResidentPool>>,
    interval: Duration,
) {
    if interval.is_zero() || !cfg!(all(target_os = "linux", target_env = "gnu")) {
        return;
    }
    tokio::spawn(after_drop(
        interval,
        move || {
            let inner = service.upgrade()?;
            if inner
                .shutting_down
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return None;
            }
            // The shared pool and socket budget cover other apps too. This
            // is an activity hint, not a lock against new work: malloc_trim
            // is safe if an allocation races it, but can contend for locks.
            let residents = pool
                .as_ref()
                .map_or_else(|| inner.mailboxes.resident(), |p| p.used());
            Some(residents.saturating_add(inner.sockets.budget.in_use()))
        },
        wasmkeep_runtime::trim_system_allocator,
    ));
}

async fn after_drop<P, R>(interval: Duration, mut probe: P, reclaim: R)
where
    P: FnMut() -> Option<usize> + Send + 'static,
    R: Fn(usize, Duration) -> Option<wasmkeep_runtime::AllocatorTrim> + Send + Sync + 'static,
{
    if interval.is_zero() {
        return;
    }
    let reclaim = Arc::new(reclaim);
    loop {
        // Sleep after the preceding attempt finishes: no queued attempts or
        // catch-up burst, even if the allocator takes longer than expected.
        tokio::time::sleep(interval).await;
        let Some(activity) = probe() else {
            return;
        };
        let reclaim = Arc::clone(&reclaim);
        // Neither a service reference nor an allocator lock crosses the
        // sleep. Arena scans never run on an executor worker.
        if let Ok(Some(result)) = tokio::task::spawn_blocking(move || {
            let result = reclaim(activity, interval);
            if let Some(result) = result {
                // Record even if the awaiting maintenance task is cancelled.
                crate::metrics::allocator_trim(result);
            }
            result
        })
        .await
        {
            tracing::debug!(
                released = result.released,
                "system allocator maintenance after activity drop"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn samples_activity_off_executor_and_does_not_keep_service_alive() {
        let activity = Arc::new(std::sync::atomic::AtomicUsize::new(100));
        let weak = Arc::downgrade(&activity);
        let executor = std::thread::current().id();
        let (sent, received) = std::sync::mpsc::channel();
        let period = Duration::from_secs(60);
        let task = tokio::spawn(after_drop(
            period,
            move || weak.upgrade().map(|s| s.load(Ordering::SeqCst)),
            move |activity, cooldown| {
                sent.send((std::thread::current().id(), activity, cooldown))
                    .unwrap();
                None
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(period).await;
        tokio::task::yield_now().await;
        let (worker, sample, cooldown) = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_ne!(worker, executor);
        assert_eq!(sample, 100);
        assert_eq!(cooldown, period);
        drop(activity);
        task.await.unwrap();
        assert!(received.try_recv().is_err());
    }

    #[tokio::test]
    async fn zero_disables_maintenance_without_polling_or_spinning() {
        after_drop(
            Duration::ZERO,
            || panic!("disabled maintenance probed the service"),
            |_, _| panic!("disabled maintenance called the allocator"),
        )
        .await;
    }
}
