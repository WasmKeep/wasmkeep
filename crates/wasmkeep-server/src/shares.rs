//! A process budget divided between the apps it serves (ADR 0038,
//! section 8): each app that joined has a reserved share, an equal part
//! of the budget; an app may use more than its share only while what
//! stays free still covers every other app's unused share. So one app
//! never makes another wait within its share, and an app alone has the
//! whole budget, as before.
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

    fn reserved_in(&self, state: &BTreeMap<String, App>, app: &str) -> u64 {
        match state.get(app) {
            Some(a) if a.members > 0 => self.part(state),
            _ => 0,
        }
    }

    /// The share reserved for `app` now (0 if it did not join).
    #[cfg(test)]
    pub(crate) fn reserved(&self, app: &str) -> u64 {
        let state = self.lock();
        self.reserved_in(&state, app)
    }

    /// Units `app`'s permits hold now.
    #[cfg(test)]
    pub(crate) fn used(&self, app: &str) -> u64 {
        self.lock().get(app).map_or(0, |a| a.used)
    }

    /// Units `app` would be charged for asking `units`: at least one, at
    /// most its reserved share, so a request never asks for more than an
    /// app may always get.
    fn charge(&self, state: &BTreeMap<String, App>, app: &str, units: u64) -> u64 {
        units.clamp(1, self.reserved_in(state, app).max(1))
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
        let units = self.charge(&state, app, units);
        let part = self.part(&state);
        let used: u64 = state.values().map(|a| a.used).sum();
        // What every other app that joined has reserved and not used.
        let others_unused: u64 = state
            .iter()
            .filter(|(name, a)| name.as_str() != app && a.members > 0)
            .map(|(_, a)| part.saturating_sub(a.used))
            .sum();
        let free_after = self.total.checked_sub(used)?.checked_sub(units)?;
        if free_after < others_unused {
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
            if a.members == 0 && a.used == 0 {
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
            if a.members == 0 && a.used == 0 {
                state.remove(app);
            }
        }
        drop(state);
        self.changed.notify_waiters();
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

    #[test]
    fn an_app_alone_has_the_whole_budget() {
        let s = shares(10);
        let _a = s.join("acme/shop");
        assert_eq!(s.reserved("acme/shop"), 10);
        let p = s.try_acquire("acme/shop", 10).expect("the whole budget");
        assert!(s.try_acquire("acme/shop", 1).is_none(), "nothing past it");
        drop(p);
        assert_eq!(s.used("acme/shop"), 0);
        assert!(s.try_acquire("acme/shop", 10).is_some());
    }

    #[test]
    fn a_saturated_app_leaves_the_other_its_share() {
        let s = shares(10);
        let (_a, _b) = (s.join("acme/shop"), s.join("acme/blog"));
        assert_eq!((s.reserved("acme/shop"), s.reserved("acme/blog")), (5, 5));
        // Shop takes all it may while blog uses nothing: its own share.
        let mut held = Vec::new();
        while let Some(p) = s.try_acquire("acme/shop", 1) {
            held.push(p);
        }
        assert_eq!(s.used("acme/shop"), 5);
        // Blog gets every unit of its share, one by one.
        let blog: Vec<_> = (0..5)
            .map(|i| {
                s.try_acquire("acme/blog", 1)
                    .unwrap_or_else(|| panic!("unit {i}"))
            })
            .collect();
        assert!(
            s.try_acquire("acme/blog", 1).is_none(),
            "the budget is full"
        );
        // Blog idle again: its unused share stays free for it, so shop
        // borrows none of it (with equal shares, nothing is unreserved).
        drop(blog);
        assert!(s.try_acquire("acme/shop", 1).is_none());
        drop(held);
        assert_eq!(s.used("acme/shop"), 0);
    }

    #[test]
    fn borrowing_stops_where_another_apps_unused_share_starts() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        // b uses 2 of its 5: a may use 10 - 5 = 5 (its share), not more,
        // since b's 3 unused units must stay free.
        let _b2 = s.try_acquire("b/b", 2).unwrap();
        let _a5 = s.try_acquire("a/a", 5).unwrap();
        assert!(s.try_acquire("a/a", 1).is_none());
        // b at its share: a may then borrow nothing either, nothing is free
        // beyond b's reservation.
        let _b3 = s.try_acquire("b/b", 3).unwrap();
        assert!(s.try_acquire("a/a", 1).is_none());
        // b borrows nothing past its share while a's share is in use.
        assert!(s.try_acquire("b/b", 1).is_none());
    }

    #[test]
    fn an_idle_app_lends_and_a_leaving_app_gives_its_share_back() {
        let s = shares(9);
        let a = s.join("a/a");
        let b = s.join("b/b");
        let c = s.join("c/c");
        assert_eq!(s.reserved("a/a"), 3);
        let _a3 = s.try_acquire("a/a", 3).unwrap();
        // b and c idle: a may not take their reservations.
        assert!(s.try_acquire("a/a", 1).is_none());
        drop(c);
        // Two apps: shares of 4 (and the rest, 1, unreserved).
        assert_eq!(s.reserved("a/a"), 4);
        assert!(s.try_acquire("a/a", 1).is_some_and(|p| p.units() == 1));
        drop(b);
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
    fn a_request_is_charged_at_most_its_apps_share() {
        let s = shares(10);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        let p = s.try_acquire("a/a", 8).expect("charged its share, 5");
        assert_eq!(p.units(), 5);
        assert!(s.try_acquire("b/b", 5).is_some());
    }

    #[tokio::test]
    async fn a_waiter_gets_its_units_when_they_return_and_a_cancelled_wait_takes_none() {
        let s = shares(4);
        let (_a, _b) = (s.join("a/a"), s.join("b/b"));
        let held = s.try_acquire("a/a", 2).unwrap();
        // A cancelled wait: nothing taken.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), s.acquire("a/a", 1)).await;
        assert!(waited.is_err());
        assert_eq!(s.used("a/a"), 2);
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
        let _a2 = s.try_acquire("a/a", 2).unwrap();
        let p = s.try_acquire("x/x", 5).expect("not limited");
        assert_eq!(p.units(), 0);
    }
}
