//! In-memory alarm timer for all actors on this node.
//!
//! The source of truth is each actor's `_wasmkeep_alarm` row. This timer is a
//! cache of "who is due when", updated from the committed row at the end of
//! every event, filled by a scan of the data directory at startup, and drained
//! by delivering due alarms as mailbox events.
//!
//! An event that ends with the time a delivery already handed to the
//! mailbox is for leaves that delivery as it is (ADR 0009, amendment of
//! 2026-11-19): it runs after the event and reads the same alarm.

use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
use std::hash::BuildHasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};

/// How many times in a row a delivery the mailbox dropped is delivered
/// again (with the admission backoff) before its registration is given up:
/// the actor's next event or the sweep delivers the alarm then.
pub(crate) const MAX_DROPPED_REDELIVERIES: u32 = 6;
use wasmkeep_runtime::ActorAddress;

enum Cmd {
    Set(ActorAddress, u64),
    /// From an event's end: like `Set`, unless the delivery in flight is
    /// for the same time (`alarm_index::delivery_covers`).
    Sync(ActorAddress, u64),
    /// Like `Set`, unless `address` is already registered for an earlier
    /// time.
    SetIfEarlier(ActorAddress, u64),
    /// From the bucket's index: like `SetIfEarlier`, but a registration due
    /// within `horizon` ms (a retry this node is backing off) keeps its time.
    Sweep(ActorAddress, u64, u64),
    /// For each (address, at): whether a `Sweep` of it would change
    /// nothing (horizon in ms).
    Pending(Vec<(ActorAddress, u64)>, u64, oneshot::Sender<Vec<bool>>),
    /// Retry a mailbox admission failure (`false`) or a delivery the mailbox
    /// dropped (`true`), retaining its original due-time hint and leaving
    /// any newer registration alone.
    Retry(ActorAddress, Delivery, bool),
    Complete(ActorAddress, Delivery),
    Clear(ActorAddress),
    /// Like `Clear`, only if `address` is registered to wake at this time.
    ClearIf(ActorAddress, u64),
    /// Every registration: (address, its wake time, or its due-time hint
    /// while a delivery is in flight).
    List(oneshot::Sender<Vec<(ActorAddress, u64)>>),
}

/// Sends updates to the timer task. Cheap to clone.
#[derive(Clone, Debug)]
pub(crate) struct AlarmHandle {
    tx: mpsc::UnboundedSender<Cmd>,
    #[cfg(test)]
    admission_retries: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl AlarmHandle {
    /// Fire `address` at `at` (ms since the Unix epoch), replacing any time
    /// already registered for it.
    pub(crate) fn set(&self, address: ActorAddress, at: u64) {
        let _ = self.tx.send(Cmd::Set(address, at));
    }

    /// The committed alarm `at` an event of `address` ended with: like
    /// [`set`](Self::set), unless the delivery this timer handed to the
    /// mailbox and has not heard back from is for `at`. That delivery runs
    /// after the event and reads the same alarm; registering again would
    /// deliver the round twice. If its result comes back with nothing
    /// registered since, it is delivered again ([`complete`](Self::complete)).
    pub(crate) fn sync(&self, address: ActorAddress, at: u64) {
        let _ = self.tx.send(Cmd::Sync(address, at));
    }

    /// Fire `address` at `at`, unless an earlier time is registered for it.
    pub(crate) fn set_if_earlier(&self, address: ActorAddress, at: u64) {
        let _ = self.tx.send(Cmd::SetIfEarlier(address, at));
    }

    /// Fire `address` at `at` (overdue, from the bucket's index), unless
    /// this node already has it registered earlier, or soon: within
    /// `horizon` from now, as a retry with backoff would be.
    pub(crate) fn sweep(&self, address: ActorAddress, at: u64, horizon: Duration) {
        let horizon = u64::try_from(horizon.as_millis()).unwrap_or(u64::MAX);
        let _ = self.tx.send(Cmd::Sweep(address, at, horizon));
    }

    /// For each overdue index entry (address, due time), whether
    /// [`sweep`](Self::sweep) would leave this node's registration as it is
    /// (a delivery in flight, admission backoff, or a time registered
    /// earlier or within `horizon`): delivering it would run nothing. All
    /// `false` if the timer is gone.
    pub(crate) async fn pending(
        &self,
        entries: Vec<(ActorAddress, u64)>,
        horizon: Duration,
    ) -> Vec<bool> {
        let len = entries.len();
        let horizon = u64::try_from(horizon.as_millis()).unwrap_or(u64::MAX);
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Pending(entries, horizon, tx)).is_err() {
            return vec![false; len];
        }
        rx.await.unwrap_or_else(|_| vec![false; len])
    }

    /// Retry a refused delivery with exponential backoff and jitter. The
    /// delay ceiling starts at 100 ms and doubles up to 5 s; each delay is
    /// sampled from the upper half of that range. Newer Set/Clear wins.
    pub(crate) fn retry(&self, address: ActorAddress, delivery: Delivery) {
        let _ = self.tx.send(Cmd::Retry(address, delivery, false));
    }

    /// Deliver again, with the same backoff as [`retry`](Self::retry), a
    /// delivery the mailbox dropped before it answered (the actor failed to
    /// start, or the event stopped): nothing else would arm its alarm
    /// until the actor's next event or a sweep. At most
    /// [`MAX_DROPPED_REDELIVERIES`] times in a row; then the registration
    /// is given up, for the next event or the sweep. Newer Set/Clear wins.
    pub(crate) fn redeliver(&self, address: ActorAddress, delivery: Delivery) {
        let _ = self.tx.send(Cmd::Retry(address, delivery, true));
    }

    /// Release delivery state after the event ran. This cannot clear a
    /// registration made by the actor event; if an event's [`sync`](Self::sync)
    /// was left to this delivery and nothing registered since, the alarm is
    /// delivered again instead.
    pub(crate) fn complete(&self, address: ActorAddress, delivery: Delivery) {
        let _ = self.tx.send(Cmd::Complete(address, delivery));
    }

    /// Per-timer observation for synchronized service tests. Unlike the
    /// process metric, concurrent tests cannot satisfy this observation.
    #[cfg(test)]
    pub(crate) fn admission_retries(&self) -> u64 {
        self.admission_retries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Forget `address`.
    pub(crate) fn clear(&self, address: ActorAddress) {
        let _ = self.tx.send(Cmd::Clear(address));
    }

    /// Every actor this timer holds an alarm for, with its time. Empty if
    /// the timer is gone.
    pub(crate) async fn registered(&self) -> Vec<(ActorAddress, u64)> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::List(tx)).is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// Forget `address` if it is registered to wake at `at`: a newer
    /// registration (another time) stays.
    pub(crate) fn clear_if(&self, address: ActorAddress, at: u64) {
        let _ = self.tx.send(Cmd::ClearIf(address, at));
    }
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// [`now_ms`] as a signed timestamp, for file formats that store one.
pub(crate) fn now_ms_i64() -> i64 {
    i64::try_from(now_ms()).unwrap_or(i64::MAX)
}

/// A due-time hint and the identity of this particular delivery. A late
/// retry/acknowledgment must not revive a cleared or replaced alarm, even
/// when the new committed due-time happens to equal the old one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Delivery {
    pub(crate) hint: u64,
    /// Registered by the sweep, from the bucket's index: the hint is an
    /// index entry's time, which may be named by no state.
    pub(crate) swept: bool,
    generation: u64,
}

#[derive(Clone, Copy)]
struct Registration {
    /// None while the mailbox admission/result is outstanding.
    wake: Option<u64>,
    delivery: Delivery,
    refusals: u32,
    /// An event's `Sync` was left to this delivery while it was in flight.
    covered: bool,
    /// Drops by the mailbox redelivered in a row (`MAX_DROPPED_REDELIVERIES`).
    drops: u32,
}

#[derive(Default)]
struct Timer {
    due: BTreeSet<(u64, ActorAddress)>,
    by_address: HashMap<ActorAddress, Registration>,
    generation: u64,
    jitter: RandomState,
    #[cfg(test)]
    admission_retries: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Timer {
    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    fn clear(&mut self, address: &ActorAddress) {
        if let Some(old) = self.by_address.remove(address)
            && let Some(wake) = old.wake
        {
            self.due.remove(&(wake, address.clone()));
        }
    }

    fn set(&mut self, address: ActorAddress, at: u64) {
        self.register(address, at, false);
    }

    fn register(&mut self, address: ActorAddress, at: u64, swept: bool) {
        self.clear(&address);
        let generation = self.next_generation();
        self.by_address.insert(
            address.clone(),
            Registration {
                wake: Some(at),
                delivery: Delivery {
                    hint: at,
                    swept,
                    generation,
                },
                refusals: 0,
                covered: false,
                drops: 0,
            },
        );
        self.due.insert((at, address));
    }

    /// Whether a sweep of `address` at `at` leaves its registration as is.
    fn sweep_keeps(&self, address: &ActorAddress, at: u64, now: u64, horizon: u64) -> bool {
        self.by_address.get(address).is_some_and(|old| {
            // The index may still contain the same overdue alarm.
            // It must not bypass admission backoff or duplicate an
            // in-flight delivery. An explicit Set still wins.
            old.wake.is_none()
                || (old.refusals > 0 && old.delivery.hint == at)
                || old
                    .wake
                    .is_some_and(|wake| wake <= at || wake <= now.saturating_add(horizon))
        })
    }

    fn apply(&mut self, cmd: Cmd, now: u64) {
        match cmd {
            Cmd::Set(address, at) => self.set(address, at),
            Cmd::Sync(address, at) => {
                let in_flight = self
                    .by_address
                    .get(&address)
                    .and_then(|r| r.wake.is_none().then_some(r.delivery.hint));
                if wasmkeep_cluster::alarm_index::delivery_covers(in_flight, at) {
                    if let Some(r) = self.by_address.get_mut(&address) {
                        r.covered = true;
                    }
                } else {
                    self.set(address, at);
                }
            }
            Cmd::SetIfEarlier(address, at) => {
                if !self
                    .by_address
                    .get(&address)
                    .is_some_and(|old| old.wake.is_some_and(|wake| wake <= at))
                {
                    self.set(address, at);
                }
            }
            Cmd::Sweep(address, at, horizon) => {
                if !self.sweep_keeps(&address, at, now, horizon) {
                    self.register(address, at, true);
                }
            }
            Cmd::Pending(entries, horizon, reply) => {
                let kept = entries
                    .iter()
                    .map(|(address, at)| self.sweep_keeps(address, *at, now, horizon))
                    .collect();
                let _ = reply.send(kept);
            }
            Cmd::Retry(address, delivery, dropped) => {
                let Some(old) = self.by_address.get(&address).copied() else {
                    return;
                };
                if old.delivery != delivery || old.wake.is_some() {
                    return;
                }
                // Bounded: an actor that never starts would otherwise be
                // activated every 5 s at most, forever, with no node-wide
                // cap. Given up, its alarm waits for the actor's next event
                // (the next `Sync` registers anew: nothing is in flight) or
                // the sweep, as before 0.7.0.
                if dropped && old.drops >= MAX_DROPPED_REDELIVERIES {
                    self.clear(&address);
                    return;
                }
                let ceiling = (100_u64 << old.refusals.min(6)).min(5000);
                let floor = ceiling / 2;
                let random = self.jitter.hash_one((&address, delivery.generation));
                let delay = floor + random % (ceiling - floor + 1);
                let wake = now.saturating_add(delay);
                let generation = self.next_generation();
                // The retried delivery is handed over after any event that
                // was left to this one: it reads that event's alarm.
                self.by_address.insert(
                    address.clone(),
                    Registration {
                        wake: Some(wake),
                        delivery: Delivery {
                            generation,
                            ..delivery
                        },
                        refusals: old.refusals.saturating_add(1),
                        covered: false,
                        drops: old.drops.saturating_add(u32::from(dropped)),
                    },
                );
                self.due.insert((wake, address));
                if dropped {
                    crate::metrics::inc(&crate::metrics::METRICS.alarm_dropped_deliveries);
                } else {
                    crate::metrics::inc(&crate::metrics::METRICS.alarm_admission_retries);
                    #[cfg(test)]
                    self.admission_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Cmd::Complete(address, delivery) => {
                let Some(old) = self.by_address.get(&address).copied() else {
                    return;
                };
                if old.delivery != delivery || old.wake.is_some() {
                    return;
                }
                if wasmkeep_cluster::alarm_index::deliver_again(old.covered) {
                    // An event committed this time while the delivery was
                    // out, and its run registered nothing since: the alarm
                    // may still be due (the run ended first, or did not
                    // run `on-alarm`). The new delivery reads it.
                    self.set(address, delivery.hint);
                } else {
                    self.clear(&address);
                }
            }
            Cmd::Clear(address) => self.clear(&address),
            Cmd::List(reply) => {
                let all = self
                    .by_address
                    .iter()
                    .map(|(a, r)| (a.clone(), r.wake.unwrap_or(r.delivery.hint)))
                    .collect();
                let _ = reply.send(all);
            }
            Cmd::ClearIf(address, at) => {
                if self
                    .by_address
                    .get(&address)
                    .is_some_and(|r| r.wake == Some(at))
                {
                    self.clear(&address);
                }
            }
        }
    }

    fn wait(&self, now: u64) -> Duration {
        self.due
            .first()
            .map_or(Duration::from_secs(3600), |(at, _)| {
                Duration::from_millis(at.saturating_sub(now))
            })
    }

    fn pop_due(&mut self, now: u64) -> Option<(ActorAddress, Delivery)> {
        while let Some((at, _)) = self.due.first() {
            if *at > now {
                return None;
            }
            if let Some((_, address)) = self.due.pop_first()
                && let Some(registration) = self.by_address.get_mut(&address)
            {
                registration.wake = None;
                return Some((address, registration.delivery));
            }
        }
        None
    }
}

/// Starts the timer task; `fire` is called for each due actor with its
/// original due-time hint (once per registration: the actor's next event
/// re-registers it if still pending, unless this delivery is for the same
/// time; then its completion does).
pub(crate) fn spawn(fire: impl Fn(ActorAddress, Delivery) + Send + 'static) -> AlarmHandle {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut timer = Timer::default();
    #[cfg(test)]
    let admission_retries = std::sync::Arc::clone(&timer.admission_retries);
    tokio::spawn(async move {
        loop {
            let wait = timer.wait(now_ms());
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    None => return, // every handle dropped
                    Some(cmd) => timer.apply(cmd, now_ms()),
                },
                () = tokio::time::sleep(wait) => {
                    let now = now_ms();
                    while let Some((address, hint)) = timer.pop_due(now) {
                        fire(address, hint);
                    }
                }
            }
        }
    });
    AlarmHandle {
        tx,
        #[cfg(test)]
        admission_retries,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    fn addr() -> ActorAddress {
        ActorAddress::new("t", "c", "x").unwrap()
    }

    #[tokio::test]
    async fn an_earlier_time_replaces_a_later_one_and_not_the_reverse() {
        let (tx, mut fired) = mpsc::unbounded_channel();
        let timer = spawn(move |address, at| {
            let _ = tx.send((address, at));
        });
        // A stale entry a week out must not hold back an overdue alarm.
        timer.set(addr(), now_ms() + 7 * 24 * 3_600_000);
        let now = now_ms();
        timer.set_if_earlier(addr(), now);
        let (_, at) = tokio::time::timeout(Duration::from_secs(1), fired.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at.hint, now);

        // A later time leaves an earlier one alone.
        let soon = now_ms() + 100;
        timer.set(addr(), soon);
        timer.set_if_earlier(addr(), soon + 60_000);
        let (_, at) = tokio::time::timeout(Duration::from_secs(1), fired.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at.hint, soon);
    }

    fn start_delivery(timer: &mut Timer, hint: u64) -> Delivery {
        timer.apply(Cmd::Set(addr(), hint), hint);
        timer.pop_due(hint).unwrap().1
    }

    #[test]
    fn repeated_admission_refusals_back_off_with_jitter_and_preserve_the_hint() {
        let mut timer = Timer::default();
        let mut delivery = start_delivery(&mut timer, 17);
        let mut now = 1000;
        for attempt in 0..100_u32 {
            timer.apply(Cmd::Retry(addr(), delivery, false), now);
            let delay = u64::try_from(timer.wait(now).as_millis()).unwrap();
            let ceiling = (100_u64 << attempt.min(6)).min(5000);
            assert!(
                (ceiling / 2..=ceiling).contains(&delay),
                "attempt {attempt}: {delay}"
            );
            assert!(timer.pop_due(now + delay - 1).is_none());
            now += delay;
            let (address, retried) = timer.pop_due(now).unwrap();
            assert_eq!(address, addr());
            assert_eq!(retried.hint, 17);
            assert_ne!(retried, delivery);
            delivery = retried;
            assert!(timer.pop_due(u64::MAX).is_none());
        }
    }

    #[test]
    fn addresses_do_not_all_retry_at_the_same_time() {
        let mut timer = Timer::default();
        for i in 0..64 {
            let address = ActorAddress::new("t", "c", format!("a{i}")).unwrap();
            timer.apply(Cmd::Set(address.clone(), 17), 0);
            let delivery = timer.pop_due(17).unwrap().1;
            timer.apply(Cmd::Retry(address, delivery, false), 1000);
        }
        let wakes: BTreeSet<_> = timer.due.iter().map(|(wake, _)| *wake).collect();
        assert!(
            wakes.len() > 1,
            "per-address jitter must spread retry times"
        );
    }

    #[test]
    fn duplicate_and_stale_retries_do_not_multiply_or_postpone_delivery() {
        let mut timer = Timer::default();
        let first = start_delivery(&mut timer, 17);
        timer.apply(Cmd::Retry(addr(), first, false), 1000);
        let wake = timer.by_address[&addr()].wake.unwrap();
        for now in 1001..1100 {
            timer.apply(Cmd::Retry(addr(), first, false), now);
        }
        assert_eq!(timer.by_address[&addr()].wake, Some(wake));
        let next = timer.pop_due(wake).unwrap().1;
        timer.apply(Cmd::Retry(addr(), first, false), wake);
        timer.apply(Cmd::Complete(addr(), first), wake);
        assert!(timer.due.is_empty());
        assert_eq!(timer.by_address[&addr()].delivery, next);
        timer.apply(Cmd::Retry(addr(), next, false), wake);
        assert_eq!(timer.due.len(), 1);
    }

    #[test]
    fn a_new_committed_time_supersedes_pending_and_inflight_retries() {
        for pending_retry in [false, true] {
            let mut timer = Timer::default();
            let delivery = start_delivery(&mut timer, 17);
            if pending_retry {
                timer.apply(Cmd::Retry(addr(), delivery, false), 1000);
            }
            // Even the same due-time represents a new registration.
            timer.apply(Cmd::Set(addr(), 17), 1001);
            timer.apply(Cmd::Retry(addr(), delivery, false), 1002);
            timer.apply(Cmd::Complete(addr(), delivery), 1002);
            let replacement = timer.pop_due(1002).unwrap().1;
            assert_eq!(replacement.hint, 17);
            assert_ne!(replacement, delivery);
            assert!(timer.pop_due(u64::MAX).is_none());
        }
    }

    /// `Sync` is what an event's end sends (`Set` keeps the semantics
    /// above). Events that end with the time of the delivery in flight
    /// leave it alone: before 0.7.0 each queued another delivery of the
    /// same round, and each found the alarm moved by the first.
    #[test]
    fn events_ending_with_the_in_flight_time_queue_no_second_delivery() {
        let mut timer = Timer::default();
        let delivery = start_delivery(&mut timer, 17);
        for now in 1000..1010 {
            timer.apply(Cmd::Sync(addr(), 17), now);
            assert!(timer.pop_due(u64::MAX).is_none(), "a second delivery");
        }
        // The delivery's run moves the alarm: registered anew, and its
        // late completion leaves that registration alone.
        timer.apply(Cmd::Sync(addr(), 5000), 1010);
        timer.apply(Cmd::Complete(addr(), delivery), 1011);
        assert!(timer.pop_due(4999).is_none());
        let next = timer.pop_due(5000).unwrap().1;
        assert_eq!(next.hint, 5000);
        assert!(timer.pop_due(u64::MAX).is_none());
    }

    #[test]
    fn a_move_while_a_delivery_is_in_flight_registers_the_new_time() {
        for at in [16, 18, 1_000_000] {
            let mut timer = Timer::default();
            let delivery = start_delivery(&mut timer, 17);
            timer.apply(Cmd::Sync(addr(), at), 1000);
            // Late callbacks of the old delivery change nothing.
            timer.apply(Cmd::Retry(addr(), delivery, false), 1001);
            timer.apply(Cmd::Retry(addr(), delivery, true), 1001);
            timer.apply(Cmd::Complete(addr(), delivery), 1001);
            assert_eq!(timer.by_address[&addr()].wake, Some(at));
            assert_eq!(timer.pop_due(u64::MAX).unwrap().1.hint, at);
        }
    }

    /// The event's sync and the delivery's completion come from different
    /// tasks: in either order, a committed alarm that nothing else
    /// registered is delivered again.
    #[test]
    fn a_covered_delivery_that_registered_nothing_is_delivered_again_in_either_order() {
        for sync_first in [true, false] {
            let mut timer = Timer::default();
            let delivery = start_delivery(&mut timer, 17);
            if sync_first {
                timer.apply(Cmd::Sync(addr(), 17), 1000);
                timer.apply(Cmd::Complete(addr(), delivery), 1001);
            } else {
                timer.apply(Cmd::Complete(addr(), delivery), 1000);
                assert!(timer.by_address.is_empty());
                timer.apply(Cmd::Sync(addr(), 17), 1001);
            }
            let again = timer.pop_due(1001).unwrap().1;
            assert_eq!(again.hint, 17);
            assert_ne!(again, delivery);
            // That one completes with nothing left to do: forgotten.
            timer.apply(Cmd::Complete(addr(), again), 1002);
            assert!(timer.by_address.is_empty(), "sync_first: {sync_first}");
        }
    }

    /// A delivery the mailbox dropped (the actor failed to start, or the
    /// event stopped) is delivered again with backoff, not left to the
    /// next event's sync (which, for the same time, it covers) or to a
    /// sweep.
    #[test]
    fn a_dropped_delivery_is_delivered_again_without_waiting_for_a_sweep() {
        for covered in [false, true] {
            let mut timer = Timer::default();
            let delivery = start_delivery(&mut timer, 17);
            if covered {
                timer.apply(Cmd::Sync(addr(), 17), 1000);
            }
            timer.apply(Cmd::Retry(addr(), delivery, true), 1000);
            let delay = u64::try_from(timer.wait(1000).as_millis()).unwrap();
            assert!((50..=100).contains(&delay), "{delay}");
            let again = timer.pop_due(1000 + delay).unwrap().1;
            assert_eq!(again.hint, 17);
            assert_ne!(again, delivery);
            assert_eq!(
                timer
                    .admission_retries
                    .load(std::sync::atomic::Ordering::Relaxed),
                0,
                "not an admission refusal"
            );
            // A second drop backs off further.
            timer.apply(Cmd::Retry(addr(), again, true), 2000);
            assert!(timer.wait(2000) >= Duration::from_millis(100));
        }
    }

    /// An actor that never starts (a broken deploy, an unreachable bucket)
    /// is not redelivered forever: after `MAX_DROPPED_REDELIVERIES` drops
    /// the registration goes, and the alarm is left to the actor's next
    /// event or the sweep, which deliver it as before.
    #[test]
    fn dropped_redeliveries_stop_after_a_bound_and_the_alarm_is_delivered_later() {
        for later in ["event", "sweep"] {
            let mut timer = Timer::default();
            let mut delivery = start_delivery(&mut timer, 17);
            // A covered sync before the drops: cleared all the same.
            timer.apply(Cmd::Sync(addr(), 17), 1000);
            let mut now = 1000;
            for _ in 0..MAX_DROPPED_REDELIVERIES {
                timer.apply(Cmd::Retry(addr(), delivery, true), now);
                now += 10_000;
                delivery = timer.pop_due(now).unwrap().1;
                assert_eq!(delivery.hint, 17);
            }
            timer.apply(Cmd::Retry(addr(), delivery, true), now);
            assert!(timer.by_address.is_empty(), "given up after the bound");
            assert!(timer.pop_due(u64::MAX).is_none());
            match later {
                "event" => timer.apply(Cmd::Sync(addr(), 17), now),
                _ => timer.apply(Cmd::Sweep(addr(), 17, 0), now),
            }
            let again = timer.pop_due(now).unwrap().1;
            assert_eq!(again.hint, 17, "{later}");
            // A fresh registration starts its drop count afresh.
            timer.apply(Cmd::Retry(addr(), again, true), now);
            assert_eq!(timer.by_address.len(), 1, "{later}");
        }
    }

    #[test]
    fn only_the_sweep_registers_swept_deliveries_and_a_retry_keeps_the_origin() {
        let mut timer = Timer::default();
        timer.apply(Cmd::Sweep(addr(), 17, 0), 1000);
        let swept = timer.pop_due(1000).unwrap().1;
        assert!(swept.swept);
        timer.apply(Cmd::Retry(addr(), swept, false), 1000);
        assert!(timer.pop_due(u64::MAX).unwrap().1.swept);
        let local = start_delivery(&mut timer, 17);
        assert!(!local.swept);
    }

    #[test]
    fn clearing_cancels_pending_retries_and_rejects_late_callbacks() {
        for pending_retry in [false, true] {
            let mut timer = Timer::default();
            let delivery = start_delivery(&mut timer, 17);
            if pending_retry {
                timer.apply(Cmd::Retry(addr(), delivery, false), 1000);
            }
            timer.apply(Cmd::Clear(addr()), 1001);
            timer.apply(Cmd::Retry(addr(), delivery, false), 1002);
            timer.apply(Cmd::Complete(addr(), delivery), 1002);
            assert!(timer.pop_due(u64::MAX).is_none());
            assert!(timer.by_address.is_empty());
        }
    }

    #[test]
    fn completion_releases_backoff_state_and_a_new_alarm_starts_fresh() {
        let mut timer = Timer::default();
        let mut delivery = start_delivery(&mut timer, 17);
        for _ in 0..8 {
            timer.apply(Cmd::Retry(addr(), delivery, false), 1000);
            delivery = timer.pop_due(6000).unwrap().1;
        }
        timer.apply(Cmd::Complete(addr(), delivery), 6000);
        assert!(timer.by_address.is_empty());
        assert!(timer.due.is_empty());
        let delivery = start_delivery(&mut timer, 17);
        timer.apply(Cmd::Retry(addr(), delivery, false), 1000);
        assert!(
            (Duration::from_millis(50)..=Duration::from_millis(100)).contains(&timer.wait(1000))
        );
    }

    #[test]
    fn sweeps_preserve_admission_backoff_and_do_not_duplicate_inflight_delivery() {
        let mut timer = Timer::default();
        let delivery = start_delivery(&mut timer, 17);
        timer.apply(Cmd::Sweep(addr(), 17, 0), 1000);
        assert!(timer.pop_due(1000).is_none());
        timer.apply(Cmd::Retry(addr(), delivery, false), 1000);
        let wake = timer.by_address[&addr()].wake.unwrap();
        // Even a sweep horizon shorter than our delay cannot bypass backoff.
        timer.apply(Cmd::Sweep(addr(), 17, 0), 1001);
        assert_eq!(timer.by_address[&addr()].wake, Some(wake));
        // SetIfEarlier still compares the wake, not the older due-time hint.
        timer.apply(Cmd::SetIfEarlier(addr(), 1040), 1001);
        assert!(timer.pop_due(1039).is_none());
        assert_eq!(timer.pop_due(1040).unwrap().1.hint, 1040);
    }

    /// The rule the WebSocket upgrade's probe arms by: an alarm a failed
    /// delivery moved to a backoff (`retry_alarm`, a plain `Set`, the
    /// committed time left in the past) keeps it under a sweep whose
    /// horizon is the longest backoff; with no horizon the sweep would
    /// put the past time back and deliver it at once.
    #[test]
    fn a_sweep_within_the_retry_horizon_keeps_a_backoff() {
        let backoff = |timer: &mut Timer| {
            timer.apply(Cmd::Set(addr(), 6_000), 1_000);
        };
        let mut timer = Timer::default();
        backoff(&mut timer);
        timer.apply(Cmd::Sweep(addr(), 17, 60_000), 1_000);
        assert_eq!(
            timer.by_address[&addr()].wake,
            Some(6_000),
            "the backoff stands"
        );
        assert!(timer.pop_due(1_000).is_none());
        let mut timer = Timer::default();
        backoff(&mut timer);
        timer.apply(Cmd::Sweep(addr(), 17, 0), 1_000);
        assert_eq!(
            timer.by_address[&addr()].wake,
            Some(17),
            "no horizon: due again at once"
        );
    }

    #[tokio::test]
    async fn pending_tells_which_sweeps_would_change_nothing() {
        // The sweep counts only entries it would deliver: one in
        // flight, backing off or registered soon is not counted again.
        let timer = spawn(|_, _| {});
        let other = ActorAddress::new("t", "c", "other").unwrap();
        let soon = ActorAddress::new("t", "c", "soon").unwrap();
        let now = now_ms();
        timer.set(addr(), now - 10); // fires at once: in flight
        timer.set(soon, now + 60_000);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let asked = vec![
            (addr(), now - 10),
            (other, now - 10),
            (ActorAddress::new("t", "c", "soon").unwrap(), now - 10),
        ];
        assert_eq!(
            timer.pending(asked.clone(), Duration::from_secs(300)).await,
            [true, false, true]
        );
        // Outside the horizon, the later registration gives way.
        assert_eq!(
            timer.pending(asked, Duration::from_secs(1)).await,
            [true, false, false]
        );
    }
}
