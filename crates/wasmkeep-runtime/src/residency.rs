//! One capacity reservation per actor, shared with work that outlives its task.
//!
//! This module is deliberately independent of Tokio and Wasmtime so the same
//! acquisition and release code can run under deterministic simulation.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Keeps one resident actor's capacity reserved until its final owner drops.
///
/// Clone this into resources or background work that may outlive the actor
/// state. Clones share one reservation; they do not consume additional slots.
/// Drop a resource before the residency that protects it.
#[derive(Debug)]
pub struct Residency {
    reservation: Arc<Reservation>,
}

/// A resource that keeps its actor's slot reserved until after it closes.
#[derive(Debug)]
pub struct Retained<T> {
    // Fields drop in declaration order: close the resource before returning
    // the final reservation to the mailbox's capacity counter.
    resource: T,
    _residency: Residency,
}

impl<T> std::ops::Deref for Retained<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.resource
    }
}

impl<T> std::ops::DerefMut for Retained<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.resource
    }
}

impl Clone for Residency {
    fn clone(&self) -> Self {
        Self {
            reservation: Arc::clone(&self.reservation),
        }
    }
}

impl Residency {
    /// Retains this actor's reservation until after `resource` is dropped.
    pub fn retain<T>(&self, resource: T) -> Retained<T> {
        Retained {
            resource,
            _residency: self.clone(),
        }
    }

    /// Only the mailbox still holds this reservation: resources from a
    /// discarded state have all closed, so its factory may run again.
    pub(crate) fn is_exclusive(&self) -> bool {
        Arc::strong_count(&self.reservation) == 1
    }

    /// Reserves from the mailbox's existing task counter, before a factory
    /// may run. A zero limit refuses all new actors; no arithmetic overflows
    /// when the configured limit is `usize::MAX`.
    pub(crate) fn try_acquire(
        count: &Arc<AtomicUsize>,
        limit: usize,
        released: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Option<Self> {
        let mut current = count.load(Ordering::SeqCst);
        loop {
            if current >= limit {
                return None;
            }
            match count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Some(Self {
            reservation: Arc::new(Reservation {
                count: Arc::clone(count),
                released,
            }),
        })
    }
}

struct Reservation {
    count: Arc<AtomicUsize>,
    released: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        if let Some(released) = &self.released {
            released();
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn clones_keep_one_slot_until_the_final_resource_drops() {
        let count = Arc::new(AtomicUsize::new(0));
        let task = Residency::try_acquire(&count, 1, None).unwrap();
        assert!(task.is_exclusive());
        let worker = task.clone();
        assert!(!task.is_exclusive());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(task);
        assert!(worker.is_exclusive());
        assert!(Residency::try_acquire(&count, 1, None).is_none());
        drop(worker);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(Residency::try_acquire(&count, 1, None).is_some());
    }

    #[test]
    fn zero_and_maximum_limits_do_not_need_special_capacity_allocations() {
        let count = Arc::new(AtomicUsize::new(0));
        assert!(Residency::try_acquire(&count, 0, None).is_none());
        let permit = Residency::try_acquire(&count, usize::MAX, None).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(permit);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_retained_resource_closes_before_the_final_capacity_release() {
        struct Resource(Arc<AtomicUsize>);
        impl Drop for Resource {
            fn drop(&mut self) {
                assert_eq!(
                    self.0.load(Ordering::SeqCst),
                    1,
                    "capacity released before resource close"
                );
            }
        }
        let count = Arc::new(AtomicUsize::new(0));
        let residency = Residency::try_acquire(&count, 1, None).unwrap();
        let resource = residency.retain(Resource(Arc::clone(&count)));
        drop(residency);
        drop(resource);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn release_notification_runs_once_after_the_final_counter_decrement() {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let notifications = Arc::clone(&calls);
        let callback = Arc::new(move || {
            assert_eq!(observed.load(Ordering::SeqCst), 0);
            notifications.fetch_add(1, Ordering::SeqCst);
        });
        let task = Residency::try_acquire(&count, 1, Some(callback)).unwrap();
        let worker = task.clone();
        drop(task);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(worker);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
