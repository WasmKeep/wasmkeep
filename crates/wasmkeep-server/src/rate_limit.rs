//! Bounded, node-local ingress admission (ADR 0036).
use crate::apps::limits::{Policies, Policy};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::sync::{
    Arc, LazyLock, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

const APP_KEYS: usize = 4096;
const PROCESS_KEYS: usize = 65_536;
static KEYS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejection {
    InvalidKey,
    Quota(u64),
    Capacity,
}

#[derive(Debug)]
struct Bucket {
    policy: Policy,
    credit: u128,
    at: Instant,
    used: u64,
}

type Key = (String, [u8; 32]);
#[derive(Debug, Default)]
struct Table {
    buckets: HashMap<Key, Bucket>,
    lru: BTreeSet<(u64, Key)>,
    clock: u64,
    cap: usize,
    requested: usize,
    active: bool,
}
impl Table {
    fn evict(&mut self, now: Instant, protected: &[Key]) {
        let candidate = self.lru.iter().filter(|(_, k)| !protected.contains(k));
        let chosen = candidate
            .clone()
            .find(|(_, k)| {
                let b = &self.buckets[k];
                b.credit.saturating_add(
                    now.saturating_duration_since(b.at)
                        .as_nanos()
                        .saturating_mul(u128::from(b.policy.rate)),
                ) >= u128::from(b.policy.burst) * b.unit()
            })
            .or_else(|| candidate.into_iter().next())
            .cloned();
        if let Some((used, key)) = chosen {
            self.lru.remove(&(used, key.clone()));
            self.buckets.remove(&key);
            KEYS.fetch_sub(1, Ordering::Relaxed);
        }
    }
    fn touch(&mut self, key: &Key) {
        if let Some(b) = self.buckets.get_mut(key) {
            self.lru.remove(&(b.used, key.clone()));
            self.clock += 1;
            b.used = self.clock;
            self.lru.insert((b.used, key.clone()));
        }
    }
}
static TABLES: LazyLock<Mutex<Vec<Weak<Mutex<Table>>>>> = LazyLock::new(Mutex::default);
fn rebalance(tables: &mut Vec<Weak<Mutex<Table>>>) {
    tables.retain(|t| t.strong_count() > 0);
    let count = tables
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|t| t.lock().unwrap_or_else(|p| p.into_inner()).active)
        .count();
    let share = PROCESS_KEYS / count.max(1);
    for table in tables.iter().filter_map(Weak::upgrade) {
        let mut t = table.lock().unwrap_or_else(|p| p.into_inner());
        t.cap = if t.active { t.requested.min(share) } else { 0 };
        while t.buckets.len() > t.cap {
            t.evict(Instant::now(), &[]);
        }
    }
}
#[derive(Debug)]
pub(crate) struct Limiter {
    buckets: Arc<Mutex<Table>>,
}
impl Default for Limiter {
    fn default() -> Self {
        Self::with_cap(APP_KEYS)
    }
}
impl Bucket {
    fn unit(&self) -> u128 {
        u128::from(self.policy.seconds) * 1_000_000_000
    }
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.at).as_nanos();
        self.credit = self
            .credit
            .saturating_add(elapsed.saturating_mul(u128::from(self.policy.rate)))
            .min(u128::from(self.policy.burst) * self.unit());
        self.at = self.at.max(now);
    }
    fn update(&mut self, policy: &Policy, now: Instant) {
        self.refill(now);
        if self.policy != *policy {
            let tokens = (self.credit / self.unit()).min(u128::from(policy.burst));
            self.policy = policy.clone();
            self.credit = tokens * self.unit();
        }
    }
}
impl Drop for Limiter {
    fn drop(&mut self) {
        let mut tables = TABLES.lock().unwrap_or_else(|p| p.into_inner());
        tables.retain(|t| !t.ptr_eq(&Arc::downgrade(&self.buckets)));
        {
            let mut t = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
            KEYS.fetch_sub(t.buckets.len(), Ordering::Relaxed);
            t.buckets.clear();
            t.lru.clear();
        }
        rebalance(&mut tables);
    }
}
pub(crate) fn active_keys() -> usize {
    KEYS.load(Ordering::Relaxed)
}
impl Limiter {
    fn with_cap(cap: usize) -> Self {
        let buckets = Arc::new(Mutex::new(Table {
            requested: cap,
            ..Table::default()
        }));
        let mut tables = TABLES.lock().unwrap_or_else(|p| p.into_inner());
        tables.push(Arc::downgrade(&buckets));
        rebalance(&mut tables);
        Self { buckets }
    }
    fn activate(&self, active: bool) {
        let mut tables = TABLES.lock().unwrap_or_else(|p| p.into_inner());
        let changed = {
            let mut table = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
            let changed = table.active != active;
            table.active = active;
            changed
        };
        if changed {
            rebalance(&mut tables);
        }
    }
    /// Reconcile at publication, even if no requests arrive between edits.
    pub(crate) fn reconcile(&self, policies: &Policies, now: Instant) {
        self.activate(!policies.is_empty());
        let mut buckets = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        let before = buckets.buckets.len();
        buckets.buckets.retain(|(name, _), b| {
            if let Some(policy) = policies.get(name) {
                b.update(policy, now);
                true
            } else {
                false
            }
        });
        KEYS.fetch_sub(before - buckets.buckets.len(), Ordering::Relaxed);
        buckets.lru = buckets
            .buckets
            .iter()
            .map(|(k, b)| (b.used, k.clone()))
            .collect();
    }
    pub(crate) fn admit(
        &self,
        policies: &Policies,
        method: &str,
        path: &str,
        headers: &http::HeaderMap,
        peer: Option<std::net::IpAddr>,
        now: Instant,
    ) -> Result<(), Rejection> {
        self.activate(!policies.is_empty());
        let mut matched = Vec::with_capacity(policies.len());
        for (name, policy) in policies {
            if !policy.matches(method, path) {
                continue;
            }
            let digest: [u8; 32] = if let Some(header) = &policy.header {
                let mut values = headers.get_all(header).iter();
                let value = values.next().ok_or(Rejection::InvalidKey)?.as_bytes();
                if values.next().is_some()
                    || value.is_empty()
                    || value.len() > 256
                    || value.iter().any(|b| !(33..=126).contains(b))
                {
                    return Err(Rejection::InvalidKey);
                }
                Sha256::digest(value).into()
            } else {
                let peer = match peer.ok_or(Rejection::InvalidKey)? {
                    std::net::IpAddr::V6(ip) => {
                        if let Some(v4) = ip.to_ipv4_mapped() {
                            v4.into()
                        } else {
                            std::net::Ipv6Addr::from(u128::from(ip) & (u128::MAX << 64)).into()
                        }
                    }
                    ip => ip,
                };
                Sha256::digest(peer.to_string().as_bytes()).into()
            };
            matched.push(((name.clone(), digest), policy));
        }
        if matched.is_empty() {
            return Ok(());
        }
        let mut buckets = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        let missing = matched
            .iter()
            .filter(|(key, _)| !buckets.buckets.contains_key(key))
            .count();
        if matched.len() > buckets.cap {
            return Err(Rejection::Capacity);
        }
        // Protect this request's existing buckets, and record rejected accesses
        // as uses too. Pressure only pops the LRU index, never refills a table.
        for (key, _) in &matched {
            buckets.touch(key);
        }
        let mut retry = 0;
        for (key, policy) in &matched {
            if let Some(b) = buckets.buckets.get_mut(key) {
                b.update(policy, now);
                if b.credit < b.unit() {
                    let nanos = (b.unit() - b.credit).div_ceil(u128::from(policy.rate));
                    retry = retry.max(nanos.div_ceil(1_000_000_000) as u64);
                }
            }
        }
        if retry > 0 {
            return Err(Rejection::Quota(retry));
        }
        while buckets.buckets.len() + missing > buckets.cap {
            buckets.evict(
                now,
                &matched
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>(),
            );
        }
        KEYS.fetch_add(missing, Ordering::Relaxed);
        for (key, policy) in matched {
            let b = buckets
                .buckets
                .entry(key.clone())
                .or_insert_with(|| Bucket {
                    policy: policy.clone(),
                    credit: u128::from(policy.burst) * u128::from(policy.seconds) * 1_000_000_000,
                    at: now,
                    used: 0,
                });
            b.credit -= b.unit();
            buckets.touch(&key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rules(rate: &str, burst: u32) -> Policies {
        [(
            "login".into(),
            Policy::parse("/api/*", "ip", None, rate, burst).unwrap(),
        )]
        .into()
    }
    fn admit(l: &Limiter, p: &Policies, n: Instant, ip: u8) -> Result<(), Rejection> {
        l.admit(
            p,
            "POST",
            "/api/login",
            &http::HeaderMap::new(),
            Some(std::net::Ipv4Addr::new(127, 0, 0, ip).into()),
            n,
        )
    }
    #[test]
    fn fix2_unconfigured_apps_do_not_enable_identity_cycling() {
        let _idle: Vec<_> = (0..999).map(|_| Limiter::default()).collect();
        let l = Limiter::default();
        let p = rules("1/h", 1);
        let n = Instant::now();
        for round in 0..3 {
            for subnet in 0..66u128 {
                let ip = std::net::Ipv6Addr::from((0x20010db800000000u128 + subnet) << 64);
                let result = l.admit(
                    &p,
                    "POST",
                    "/api/login",
                    &http::HeaderMap::new(),
                    Some(ip.into()),
                    n,
                );
                assert_eq!(
                    result,
                    if round == 0 {
                        Ok(())
                    } else {
                        Err(Rejection::Quota(3600))
                    }
                );
            }
        }
    }
    #[test]
    fn fix2_pressure_prefers_refilled_over_depleted() {
        let l = Limiter::with_cap(2);
        let n = Instant::now();
        let p = rules("1/h", 1);
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &rules("1/s", 1), n, 2), Ok(()));
        assert_eq!(admit(&l, &p, n + Duration::from_secs(2), 3), Ok(()));
        assert_eq!(
            admit(&l, &p, n + Duration::from_secs(2), 1),
            Err(Rejection::Quota(3598))
        );
    }
    #[test]
    fn review_ipv6_subnet_shares_quota() {
        let l = Limiter::default();
        let p = rules("1/h", 1);
        let n = Instant::now();
        for (ip, expected) in [
            ("2001:db8::1", Ok(())),
            ("2001:db8::2", Err(Rejection::Quota(3600))),
        ] {
            assert_eq!(
                l.admit(
                    &p,
                    "POST",
                    "/api/login",
                    &http::HeaderMap::new(),
                    Some(ip.parse().unwrap()),
                    n
                ),
                expected
            );
        }
    }
    #[test]
    fn review_pressure_evicts_lru_without_refilling_survivors() {
        let l = Limiter::with_cap(2);
        let p = rules("1/h", 1);
        let n = Instant::now();
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 2), Ok(()));
        assert_eq!(admit(&l, &p, n + Duration::from_secs(1), 3), Ok(()));
        let buckets = l.buckets.lock().unwrap();
        // Pressure must not refill or visit unrelated surviving buckets.
        let key: [u8; 32] = Sha256::digest(b"127.0.0.2").into();
        assert_eq!(buckets.buckets.get(&("login".into(), key)).unwrap().at, n);
    }
    #[test]
    fn review_capacity_does_not_refill_unrelated_buckets() {
        let l = Limiter::with_cap(2);
        let p = rules("1/h", 1);
        let n = Instant::now();
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 2), Ok(()));
        let _ = admit(&l, &p, n + Duration::from_secs(1), 3);
        let buckets = l.buckets.lock().unwrap();
        let key: [u8; 32] = Sha256::digest(b"127.0.0.2").into();
        assert_eq!(buckets.buckets.get(&("login".into(), key)).unwrap().at, n);
    }
    #[test]
    fn review_apps_have_reserved_capacity() {
        let apps: Vec<_> = (0..17).map(|_| Limiter::default()).collect();
        let p = rules("1/h", 1);
        let n = Instant::now();
        for l in &apps[..16] {
            for ip in 0..4096u32 {
                let _ = l.admit(
                    &p,
                    "POST",
                    "/api/login",
                    &http::HeaderMap::new(),
                    Some(std::net::Ipv4Addr::from(ip).into()),
                    n,
                );
            }
        }
        assert_eq!(admit(&apps[16], &p, n, 1), Ok(()));
        assert!(active_keys() <= PROCESS_KEYS);
    }
    #[test]
    fn fractional_refill_and_rounded_retry() {
        let l = Limiter::default();
        let p = rules("3/m", 1);
        let n = Instant::now();
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(
            admit(&l, &p, n + Duration::from_millis(500), 1),
            Err(Rejection::Quota(20))
        );
        assert_eq!(
            admit(&l, &p, n + Duration::from_secs(19), 1),
            Err(Rejection::Quota(1))
        );
        assert_eq!(admit(&l, &p, n + Duration::from_secs(20), 1), Ok(()));
    }
    #[test]
    fn overlapping_rules_do_not_partially_debit() {
        let l = Limiter::default();
        let n = Instant::now();
        let mut p = rules("1/s", 2);
        p.insert(
            "other".into(),
            Policy::parse("POST /api/login", "ip", None, "1/h", 1).unwrap(),
        );
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 1), Err(Rejection::Quota(3600)));
        p.remove("other");
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 1), Err(Rejection::Quota(1)));
    }
    #[test]
    fn capacity_evicts_least_recently_used_keys() {
        let l = Limiter::with_cap(2);
        let p = rules("1/s", 1);
        let n = Instant::now();
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 2), Ok(()));
        assert_eq!(admit(&l, &p, n, 3), Ok(()));
        assert_eq!(admit(&l, &p, n, 2), Err(Rejection::Quota(1)));
        assert_eq!(admit(&l, &p, n + Duration::from_secs(1), 3), Ok(()));
    }
    #[test]
    fn edits_do_not_grant_a_new_burst() {
        let l = Limiter::default();
        let n = Instant::now();
        assert_eq!(admit(&l, &rules("1/s", 1), n, 1), Ok(()));
        assert_eq!(admit(&l, &rules("2/s", 10), n, 1), Err(Rejection::Quota(1)));
        assert_eq!(
            admit(&l, &rules("2/s", 10), n + Duration::from_millis(500), 1),
            Ok(())
        );
    }
    #[test]
    fn high_cardinality_headers_stay_bounded_and_private() {
        let l = Limiter::with_cap(16);
        let p: Policies = [(
            "api".into(),
            Policy::parse("/*", "header", Some("x-client-id"), "1/h", 1).unwrap(),
        )]
        .into();
        let n = Instant::now();
        for i in 0..1000 {
            let mut h = http::HeaderMap::new();
            h.insert(
                "x-client-id",
                format!("private-client-{i}").parse().unwrap(),
            );
            let result = l.admit(&p, "GET", "/", &h, None, n);
            assert_eq!(result, Ok(()));
        }
        let buckets = l.buckets.lock().unwrap();
        assert_eq!(buckets.buckets.len(), 16);
        assert!(!format!("{buckets:?}").contains("private-client"));
    }

    #[test]
    fn maximum_rates_refill_without_overflow_or_clock_regression() {
        let l = Limiter::default();
        let n = Instant::now();
        let p = rules("1000000/h", 1);
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(
            admit(&l, &p, n - Duration::from_secs(1), 1),
            Err(Rejection::Quota(1))
        );
        assert_eq!(
            admit(&l, &p, n + Duration::from_nanos(3_599_999), 1),
            Err(Rejection::Quota(1))
        );
        assert_eq!(
            admit(&l, &p, n + Duration::from_nanos(3_600_000), 1),
            Ok(())
        );
        assert_eq!(
            admit(&l, &p, n + Duration::from_secs(1_000_000_000), 1),
            Ok(())
        );
    }

    #[test]
    fn concurrent_admission_and_long_idle() {
        let l = std::sync::Arc::new(Limiter::default());
        let n = Instant::now();
        let threads: Vec<_> = (0..32)
            .map(|_| {
                let l = l.clone();
                std::thread::spawn(move || admit(&l, &rules("1/s", 8), n, 1).is_ok())
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|t| t.join().unwrap().then_some(()))
                .count(),
            8
        );
        assert_eq!(
            admit(
                &l,
                &rules("1000000/h", 1000000),
                n + Duration::from_secs(1000000000),
                1
            ),
            Ok(())
        );
    }
}
