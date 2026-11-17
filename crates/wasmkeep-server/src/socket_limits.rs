//! Socket admission shared by all actor services belonging to one process.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Owner-side limits. Zero refuses every WebSocket at that scope; pending and closing sockets count.
#[derive(Debug, Clone, Copy)]
pub struct SocketLimits {
    pub per_actor: usize,
    /// Application ceiling beneath the process ceiling. Includes relays.
    pub per_app: usize,
}

impl Default for SocketLimits {
    fn default() -> Self {
        Self {
            per_actor: 1_000,
            per_app: 50_000,
        }
    }
}

/// A process's socket session allowance. Clone this into every app's config
/// when embedding multiple services in one process. A relay costs one session
/// here and one on its owner; the descriptor budget charges two FDs per session.
#[derive(Debug)]
pub struct SocketBudget {
    requested: usize,
    effective: usize,
    used: AtomicUsize,
}

impl SocketBudget {
    pub fn new(requested: usize, effective: usize) -> Arc<Self> {
        Arc::new(Self {
            requested,
            effective: effective.min(requested),
            used: AtomicUsize::new(0),
        })
    }
    pub fn requested(&self) -> usize {
        self.requested
    }
    pub fn effective(&self) -> usize {
        self.effective
    }
    pub fn in_use(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> Option<SocketPermit> {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.effective).then(|| n + 1)
            })
            .ok()?;
        Some(SocketPermit(Arc::clone(self)))
    }
}

pub(crate) struct SocketPermit(Arc<SocketBudget>);
impl Drop for SocketPermit {
    fn drop(&mut self) {
        self.0.used.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Both reservations follow the transport, including while it is opening.
pub(crate) struct SessionPermit {
    pub(crate) _process: SocketPermit,
    pub(crate) _app: SocketPermit,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_and_drop_and_shared_boundary() {
        assert!(SocketBudget::new(0, 0).reserve().is_none());
        let budget = SocketBudget::new(10, 1);
        let permit = budget.reserve().unwrap();
        assert!(Arc::clone(&budget).reserve().is_none());
        assert_eq!(budget.in_use(), 1);
        drop(permit);
        assert_eq!(budget.in_use(), 0);
        assert!(budget.reserve().is_some());
    }
    #[test]
    fn simultaneous_reservations_cannot_exceed_limit() {
        let budget = SocketBudget::new(1, 1);
        let barrier = Arc::new(std::sync::Barrier::new(16));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let budget = &budget;
                let barrier = &barrier;
                scope.spawn(move || {
                    let permit = budget.reserve();
                    barrier.wait();
                    assert_eq!(budget.in_use(), 1);
                    barrier.wait();
                    drop(permit);
                });
            }
        });
        assert_eq!(budget.in_use(), 0);
    }
}
