//! A resident limit shared by several mailboxes (ADR 0038, section 8): the
//! apps of one process draw their residents from one pool, sized from the
//! instance pool. Each member has a floor (by default the limit divided by
//! twice the number of members); when the pool is full, a wake-up evicts
//! the least recently active idle resident of a member above its floor,
//! whichever member that is, and waits only when none exists.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// What the pool asks of a member mailbox.
pub(crate) trait Member: Send + Sync {
    /// Residents it holds (reservations not yet returned).
    fn resident(&self) -> usize;
    /// Its least recently active idle resident not being evicted: when it
    /// was last active (ms), if any.
    fn oldest_idle(&self) -> Option<u64>;
    /// Asks that resident to hibernate.
    fn evict_oldest(&self);
    /// Slots already on their way back (evicting, or closing).
    fn returning(&self) -> usize;
    /// Its callers waiting for a slot.
    fn waiting(&self) -> usize;
    /// Wakes its waiters: a slot came back to the pool.
    fn wake(&self);
}

/// A member, and its floor if it set one.
type Joined = (Weak<dyn Member>, Option<usize>);

/// A resident limit shared by mailboxes.
pub struct ResidentPool {
    limit: usize,
    pub(crate) used: Arc<AtomicUsize>,
    members: Mutex<Vec<Joined>>,
}

impl std::fmt::Debug for ResidentPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidentPool")
            .field("limit", &self.limit)
            .field("used", &self.used.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ResidentPool {
    /// A pool of `limit` residents.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Arc::new(AtomicUsize::new(0)),
            members: Mutex::new(Vec::new()),
        })
    }

    /// The limit.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Residents of every member now.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    pub(crate) fn join(&self, member: Weak<dyn Member>, floor: Option<usize>) {
        let mut members = self.members.lock().unwrap_or_else(|p| p.into_inner());
        members.retain(|(m, _)| m.strong_count() > 0);
        members.push((member, floor));
    }

    /// The members alive now, each with its floor.
    fn members(&self) -> Vec<(Arc<dyn Member>, usize)> {
        let mut members = self.members.lock().unwrap_or_else(|p| p.into_inner());
        members.retain(|(m, _)| m.strong_count() > 0);
        let default = self.limit / (2 * members.len().max(1));
        members
            .iter()
            .filter_map(|(m, floor)| Some((m.upgrade()?, floor.unwrap_or(default))))
            .collect()
    }

    /// The floor of `member` (by its address), as [`Self::make_room`]
    /// counts it: its own, or the default for the members alive now.
    pub(crate) fn floor_of(&self, member: *const ()) -> usize {
        self.members()
            .into_iter()
            .find(|(m, _)| std::ptr::eq(Arc::as_ptr(m).cast::<()>(), member))
            .map_or(0, |(_, floor)| floor)
    }

    /// Called by a member whose wake-up found the pool full (`extra`: that
    /// caller, if it is not counted among the waiters): evicts the least
    /// recently active idle resident of a member above its floor, unless
    /// enough slots are already on their way back for the waiters. Called
    /// with no member's registry locked.
    pub(crate) fn make_room(&self, extra: usize) {
        let members = self.members();
        let returning: usize = members.iter().map(|(m, _)| m.returning()).sum();
        let demand: usize = members.iter().map(|(m, _)| m.waiting()).sum::<usize>() + extra;
        if returning >= demand {
            return;
        }
        let victim = members
            .iter()
            .filter(|(m, floor)| m.resident() > *floor)
            .filter_map(|(m, _)| Some((m.oldest_idle()?, m)))
            .min_by_key(|(last, _)| *last);
        if let Some((_, m)) = victim {
            m.evict_oldest();
        }
    }

    /// A slot came back: every member's waiters may try again.
    pub(crate) fn released(&self) {
        for (m, _) in self.members() {
            m.wake();
        }
    }
}
