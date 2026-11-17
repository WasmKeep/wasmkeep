//! A process budget divided between the apps it serves (ADR 0038,
//! section 8): each app that joined has an equal part of the budget, and
//! half of it is a floor never lent. An app may use any capacity outside
//! the other apps' unused floors, but takes nothing above its equal part
//! while an app below its own waits. So the budget is never left idle by
//! a partition, one app never takes another's floor, and an app alone
//! has the whole budget, as before.
//!
//! The share sits in front of the budget's own limit (a semaphore, in
//! the order waiters asked), which stays as it is: the share decides
//! whether an app may ask, the budget still hands out the room.
//!
//! Apps are named `tenant/app`: every service of one app in a process
//! (several nodes of one app in a test) shares one share. A service
//! joins when made and leaves when dropped; units held past that are
//! counted until their permit is dropped.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// A budget of `total` units divided between the apps that joined.
#[derive(Debug)]
pub(crate) struct Shares {
    total: u64,
    state: Mutex<BTreeMap<String, App>>,
    changed: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct App {
    /// Services of the app that joined and have not left.
    members: usize,
    /// Units its permits hold.
    used: u64,
    /// Its waits for units under way ([`Shares::acquire`]).
    waiting: usize,
}

impl Shares {
    /// A budget of `total` units (at least one).
    pub(crate) fn new(total: u64) -> Self {
        Self {
            total: total.max(1),
            state: Mutex::new(BTreeMap::new()),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// `app` joins until the guard is dropped.
    pub(crate) fn join(self: &Arc<Self>, app: &str) -> Member {
        self.lock().entry(app.to_owned()).or_default().members += 1;
        self.changed.notify_waiters();
        Member {
            shares: Arc::clone(self),
            app: app.to_owned(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, App>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The equal part of the budget each app that joined has.
    fn part(&self, state: &BTreeMap<String, App>) -> u64 {
        let joined = state.values().filter(|a| a.members > 0).count();
        self.total / u64::try_from(joined.max(1)).unwrap_or(u64::MAX)
    }

    /// Half the equal part: never lent to another app.
    fn floor(&self, state: &BTreeMap<String, App>) -> u64 {
        self.part(state) / 2
    }

    fn reserved_in(&self, state: &BTreeMap<String, App>, app: &str) -> u64 {
        match state.get(app) {
            Some(a) if a.members > 0 => self.part(state),
            _ => 0,
        }
    }

    /// The equal part of `app` now (0 if it did not join).
    #[cfg(test)]
    pub(crate) fn reserved(&self, app: &str) -> u64 {
        let state = self.lock();
        self.reserved_in(&state, app)
    }

    /// Each app that joined, `tenant/app`, with its equal part and the
    /// units it holds now: for the metrics page.
    pub(crate) fn each(&self, mut f: impl FnMut(&str, u64, u64)) {
        let state = self.lock();
        let part = self.part(&state);
        for (app, a) in state.iter().filter(|(_, a)| a.members > 0) {
            f(app, part, a.used);
        }
    }

    /// Units `app`'s permits hold now.
    #[cfg(test)]
    pub(crate) fn used(&self, app: &str) -> u64 {
        self.lock().get(app).map_or(0, |a| a.used)
    }

    /// Units an app is charged for asking `units`: at least one, at most
    /// its floor, so a request never asks for more than an app may always
    /// get.
    fn charge(&self, state: &BTreeMap<String, App>, units: u64) -> u64 {
        units.clamp(1, self.floor(state).max(1))
    }

    /// `units` for `app`, now, if the rule allows them.
    pub(crate) fn try_acquire(self: &Arc<Self>, app: &str, units: u64) -> Option<SharePermit> {
        let mut state = self.lock();
        let permit = |units| SharePermit {
            shares: Arc::clone(self),
            app: app.to_owned(),
            units,
        };
        if self.reserved_in(&state, app) == 0 {
            // Not joined: not limited here.
            return Some(permit(0));
        }
        let units = self.charge(&state, units);
        let (part, floor) = (self.part(&state), self.floor(&state));
        let used: u64 = state.values().map(|a| a.used).sum();
        let others = || {
            state
                .iter()
                .filter(move |(name, a)| name.as_str() != app && a.members > 0)
                .map(|(_, a)| a)
        };
        // Floors are never lent: what every other app has of its floor
        // unused stays free.
        let others_unused_floors: u64 = others().map(|a| floor.saturating_sub(a.used)).sum();
        let free_after = self.total.checked_sub(used)?.checked_sub(units)?;
        if free_after < others_unused_floors {
            return None;
        }
        // Above its part, nothing while an app below its own waits.
        let mine = state.get(app).map_or(0, |a| a.used);
        if mine.saturating_add(units) > part && others().any(|a| a.used < part && a.waiting > 0) {
            return None;
        }
        if let Some(a) = state.get_mut(app) {
            a.used += units;
        }
        drop(state);
        Some(permit(units))
    }

    /// Waits until the rule allows `units` for `app`. Cancel-safe: a
    /// dropped wait takes nothing. An app that did not join is not
    /// limited here (an empty permit).
    pub(crate) async fn acquire(self: &Arc<Self>, app: &str, units: u64) -> SharePermit {
        let _waiting = Waiting::new(self, app);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(permit) = self.try_acquire(app, units) {
                return permit;
            }
            changed.await;
        }
    }

    fn release(&self, app: &str, units: u64) {
        if units == 0 {
            return;
        }
        let mut state = self.lock();
        if let Some(a) = state.get_mut(app) {
            a.used = a.used.saturating_sub(units);
            if a.members == 0 && a.used == 0 && a.waiting == 0 {
                state.remove(app);
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn leave(&self, app: &str) {
        let mut state = self.lock();
        if let Some(a) = state.get_mut(app) {
            a.members = a.members.saturating_sub(1);
            if a.members == 0 && a.used == 0 && a.waiting == 0 {
                state.remove(app);
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

/// A wait of an app under way, counted while it lasts (cancel-safe).
struct Waiting<'a> {
    shares: &'a Shares,
    app: &'a str,
}

impl<'a> Waiting<'a> {
    fn new(shares: &'a Shares, app: &'a str) -> Self {
        if let Some(a) = shares.lock().get_mut(app) {
            a.waiting += 1;
        }
        Self { shares, app }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Some(a) = self.shares.lock().get_mut(self.app) {
            a.waiting = a.waiting.saturating_sub(1);
        }
        // A borrower held back by this wait may go on.
        self.shares.changed.notify_waiters();
    }
}

/// An app's membership in a [`Shares`]; leaving when dropped.
#[derive(Debug)]
pub(crate) struct Member {
    shares: Arc<Shares>,
    app: String,
}

impl Drop for Member {
    fn drop(&mut self) {
        self.shares.leave(&self.app);
    }
}

/// Units of an app's share, given back when dropped.
#[derive(Debug)]
pub(crate) struct SharePermit {
    shares: Arc<Shares>,
    app: String,
    units: u64,
}

impl SharePermit {
    /// The units it holds.
    #[cfg(test)]
    pub(crate) fn units(&self) -> u64 {
        self.units
    }
}

impl Drop for SharePermit {
    fn drop(&mut self) {
        self.shares.release(&self.app, self.units);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(total: u64) -> Arc<Shares> {
        Arc::new(Shares::new(total))
    }

    /// Takes `app`'s units one at a time until one is refused.
    fn fill(s: &Arc<Shares>, app: &str) -> Vec<SharePermit> {
        std::iter::from_fn(|| s.try_acquire(app, 1)).collect()
    }

    #[test]
    fn an_app_alone_has_the_whole_budget() {
        let s = shares(10);
        let _a = s.join("acme/shop");
        assert_eq!(s.reserved("acme/shop"), 10);
        let held = fill(&s, "acme/shop");
        assert_eq!(held.len(), 10, "the whole budget");
        drop(held);
        assert_eq!(s.used("acme/shop"), 0);
    }

    #[test]
    fn a_saturated_app_leaves_the_other_its_floor() {
        let s = shares(10);
        let (_a, _b) = (s.join("acme/shop"), s.join("acme/blog"));
        assert_eq!((s.reserved("acme/shop"), s.reserved("acme/blog")), (5, 5));
        // Blog idle: shop takes everything but blog's floor (half its part).
        let shop = fill(&s, "acme/shop");
        assert_eq!(shop.len(), 8);
        // Blog gets its floor at once, then the budget is full.
        let blog = fill(&s, "acme/blog");
        assert_eq!(blog.len(), 2);
        drop(shop);
        // Shop gone: blog may use any capacity outside shop's floor.
        let more = fill(&s, "acme/blog");
        assert_eq!(more.len(), 6);
        drop((blog, more));
        assert_eq!(s.used("acme/blog"), 0);
    }

    #[test]
    fn floors_are_never_lent() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        // b uses nothing and waits for nothing: its floor (2) stays free.
        let a = fill(&s, "a/a");
        assert_eq!(a.len(), 8);
        assert!(s.try_acquire("a/a", 1).is_none());
        // b using 1 of its floor: a still never gets the other unit.
        drop(a);
        let _b1 = s.try_acquire("b/b", 1).unwrap();
        assert_eq!(fill(&s, "a/a").len(), 8);
    }

    #[tokio::test]
    async fn a_borrower_above_its_part_is_refused_while_another_app_below_its_part_waits() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        let mut a = fill(&s, "a/a");
        let _b = fill(&s, "b/b");
        assert_eq!((a.len(), s.used("b/b")), (8, 2));
        // b waits for one more unit, below its part.
        let s2 = Arc::clone(&s);
        let waiter = tokio::spawn(async move { s2.acquire("b/b", 1).await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        // A unit returns: a, above its part, may not take it back while b
        // waits...
        a.pop();
        assert!(s.try_acquire("a/a", 1).is_none());
        // ...and b gets it.
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("b's waiter woken")
            .unwrap();
        assert_eq!(got.units(), 1);
        // Nobody waiting: a may borrow again what is free.
        drop(got);
        assert!(s.try_acquire("a/a", 1).is_some());
    }

    #[tokio::test]
    async fn a_cancelled_wait_stops_counting_as_waiting() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        let mut a = fill(&s, "a/a");
        let _b = fill(&s, "b/b");
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), s.acquire("b/b", 1)).await;
        assert!(waited.is_err());
        a.pop();
        assert!(s.try_acquire("a/a", 1).is_some(), "b no longer waits");
    }

    #[test]
    fn a_leaving_app_gives_its_floor_back() {
        let s = shares(9);
        let a = s.join("a/a");
        let b = s.join("b/b");
        let c = s.join("c/c");
        assert_eq!(s.reserved("a/a"), 3);
        // Parts of 3, floors of 1: a takes all but b's and c's floors.
        let held = fill(&s, "a/a");
        assert_eq!(held.len(), 7);
        drop(held);
        drop(c);
        // Two apps: parts of 4, floors of 2.
        assert_eq!(s.reserved("a/a"), 4);
        assert_eq!(fill(&s, "a/a").len(), 7);
        drop(b);
        assert_eq!(fill(&s, "a/a").len(), 9);
        drop(a);
        // Nobody joined: no limit here (the budget's own still applies).
        assert_eq!(s.reserved("a/a"), 0);
    }

    #[test]
    fn several_services_of_one_app_share_one_share() {
        let s = shares(10);
        let (one, two) = (s.join("a/a"), s.join("a/a"));
        let _b = s.join("b/b");
        assert_eq!(s.reserved("a/a"), 5);
        drop(one);
        assert_eq!(s.reserved("a/a"), 5, "the app is still served");
        drop(two);
        assert_eq!(s.reserved("b/b"), 10);
    }

    #[test]
    fn a_request_is_charged_at_most_its_apps_floor() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        let p = s.try_acquire("a/a", 8).expect("charged its floor, 2");
        assert_eq!(p.units(), 2);
        assert!(s.try_acquire("b/b", 5).is_some_and(|p| p.units() == 2));
    }

    #[tokio::test]
    async fn a_waiter_gets_its_units_when_they_return_and_a_cancelled_wait_takes_none() {
        let s = shares(4);
        let _a = s.join("a/a");
        let held = fill(&s, "a/a");
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), s.acquire("a/a", 1)).await;
        assert!(waited.is_err());
        assert_eq!(s.used("a/a"), 4);
        let s2 = Arc::clone(&s);
        let waiter = tokio::spawn(async move { s2.acquire("a/a", 1).await.units() });
        tokio::task::yield_now().await;
        drop(held);
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("woken when units returned")
            .unwrap();
        assert_eq!(got, 1);
        assert_eq!(s.used("a/a"), 0, "the waiter's permit was dropped");
    }

    #[test]
    fn an_app_that_did_not_join_is_not_limited() {
        let s = shares(2);
        let _a = s.join("a/a");
        let _a2 = fill(&s, "a/a");
        let p = s.try_acquire("x/x", 5).expect("not limited");
        assert_eq!(p.units(), 0);
    }
}
