//! Bounded, node-local ingress admission (ADR 0036).
use crate::apps::limits::{Policies, Policy};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{
    Mutex,
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
}

#[derive(Debug)]
pub(crate) struct Limiter {
    buckets: Mutex<HashMap<(String, [u8; 32]), Bucket>>,
    cap: usize,
}
impl Default for Limiter {
    fn default() -> Self {
        Self {
            buckets: Mutex::default(),
            cap: APP_KEYS,
        }
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
        let n = self
            .buckets
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        KEYS.fetch_sub(n, Ordering::Relaxed);
    }
}
pub(crate) fn active_keys() -> usize {
    KEYS.load(Ordering::Relaxed)
}
impl Limiter {
    /// Reconcile at publication, even if no requests arrive between edits.
    pub(crate) fn reconcile(&self, policies: &Policies, now: Instant) {
        let mut buckets = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        let before = buckets.len();
        buckets.retain(|(name, _), b| {
            if let Some(policy) = policies.get(name) {
                b.update(policy, now);
                true
            } else {
                false
            }
        });
        KEYS.fetch_sub(before - buckets.len(), Ordering::Relaxed);
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
                Sha256::digest(peer.ok_or(Rejection::InvalidKey)?.to_string().as_bytes()).into()
            };
            matched.push(((name.clone(), digest), policy));
        }
        if matched.is_empty() {
            return Ok(());
        }
        let mut buckets = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        let missing = matched
            .iter()
            .filter(|(key, _)| !buckets.contains_key(key))
            .count();
        if missing > 0
            && (buckets.len() + missing > self.cap
                || KEYS.load(Ordering::Relaxed) + missing > PROCESS_KEYS)
        {
            let before = buckets.len();
            buckets.retain(|key, b| {
                b.refill(now);
                matched.iter().any(|(k, _)| k == key)
                    || b.credit < u128::from(b.policy.burst) * b.unit()
            });
            KEYS.fetch_sub(before - buckets.len(), Ordering::Relaxed);
        }
        if buckets.len() + missing > self.cap {
            return Err(Rejection::Capacity);
        }
        // Reserve atomically across app instances; a failed admission retains no new keys.
        if KEYS
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(missing).filter(|v| *v <= PROCESS_KEYS)
            })
            .is_err()
        {
            return Err(Rejection::Capacity);
        }
        let mut retry = 0;
        for (key, policy) in &matched {
            if let Some(b) = buckets.get_mut(key) {
                b.update(policy, now);
                if b.credit < b.unit() {
                    let nanos = (b.unit() - b.credit).div_ceil(u128::from(policy.rate));
                    retry = retry.max(nanos.div_ceil(1_000_000_000) as u64);
                }
            }
        }
        if retry > 0 {
            KEYS.fetch_sub(missing, Ordering::Relaxed);
            return Err(Rejection::Quota(retry));
        }
        for (key, policy) in matched {
            let b = buckets.entry(key).or_insert_with(|| Bucket {
                policy: policy.clone(),
                credit: u128::from(policy.burst) * u128::from(policy.seconds) * 1_000_000_000,
                at: now,
            });
            b.credit -= b.unit();
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
    fn capacity_never_evicts_depleted_keys() {
        let l = Limiter {
            cap: 2,
            buckets: Mutex::default(),
        };
        let p = rules("1/s", 1);
        let n = Instant::now();
        assert_eq!(admit(&l, &p, n, 1), Ok(()));
        assert_eq!(admit(&l, &p, n, 2), Ok(()));
        assert_eq!(admit(&l, &p, n, 3), Err(Rejection::Capacity));
        assert_eq!(admit(&l, &p, n, 1), Err(Rejection::Quota(1)));
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
        let l = Limiter {
            cap: 16,
            buckets: Mutex::default(),
        };
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
            assert_eq!(
                result,
                if i < 16 {
                    Ok(())
                } else {
                    Err(Rejection::Capacity)
                }
            );
        }
        let buckets = l.buckets.lock().unwrap();
        assert_eq!(buckets.len(), 16);
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
