//! Actor leases on a bucket with conditional writes: exactly one owner.
//!
//! Retired in the server by [ADR 0020] (one lease per node,
//! `crate::node_lease`). This module remains for the shared traits and types
//! ([`Bucket`], [`Clock`], [`LeaseConfig`], [`Version`]), for decoding the
//! old format ([`LeaseRecord`], read by the upgrade marker,
//! `crate::node_lease::mark_old_lease`), and for tests. The per-actor lease
//! operations themselves ([`Leases`], hidden) live in `lease/legacy.rs`, for
//! `madsim_lease`, the simulation of the retired protocol, and their tests.
//!
//! [ADR 0020]: ../../../docs/adr/0020-one-lease-per-node.md
//!
//! The lease for an actor is one object, `leases/<tenant>/<class>/<name>`,
//! holding the owner node, a generation and an expiry (owner's clock, ms).
//! Every write is conditional: create-if-absent, or replace-if-version (S3
//! `If-None-Match: *` / `If-Match`). The generation increments on every
//! acquisition and fences everything the owner writes (it is part of the
//! object keys of replicated segments).
//!
//! # Safety argument
//!
//! Clocks may differ by up to `max_skew`. An owner sends its write at its own
//! time `t` and stores `expires_at = t + ttl`; it only *acts* as owner until
//! `t + ttl - safety` (its clock), which in real time is no later than
//! `t_real + ttl - safety`. Another node takes over only once its own clock
//! passes `expires_at + grace`, which in real time is no earlier than
//! `t_real + ttl + grace - 2 * max_skew`. So the two never overlap when
//! `grace + safety >= 2 * max_skew`; [`LeaseConfig::new`] enforces it.
//! A write that was delayed and applied late cannot extend anything: its
//! version check fails if anyone wrote in between.

use std::fmt;

/// Opaque object version (an S3 ETag), only ever passed back to the bucket
/// in a conditional write. Two tokens for the same object state need not be
/// equal: R2 returns a version id with a write and none with a read, so the
/// token a write returns differs from the one a read of it returns. Never
/// compare them; let the bucket decide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version(pub String);

/// Why a bucket call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BucketError {
    /// The precondition failed: the object exists, or has another version.
    #[error("precondition failed")]
    Conflict,
    /// No answer, or an error. A write may or may not have been applied.
    #[error("bucket unavailable: {0}")]
    Unavailable(String),
}

/// Conditional object operations, as S3, R2 and MinIO provide them.
#[allow(async_fn_in_trait)]
pub trait Bucket {
    /// The object and its version, if present.
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError>;
    /// Creates the object only if absent.
    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError>;
    /// Replaces the object only if its version is `expected`.
    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError>;
}

/// This node's wall clock in milliseconds. Clocks of different nodes may
/// differ by up to [`LeaseConfig::max_skew_ms`].
pub trait Clock {
    /// Milliseconds since the Unix epoch (or since the start of a simulation).
    fn now_ms(&self) -> u64;
}

/// Lease timing. Build with [`LeaseConfig::new`], which checks safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseConfig {
    ttl_ms: u64,
    grace_ms: u64,
    safety_ms: u64,
    max_skew_ms: u64,
}

/// A lease configuration that cannot guarantee a single owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsafe lease config: {0}")]
pub struct ConfigError(&'static str);

impl LeaseConfig {
    /// `ttl`: lease length. `grace`: extra wait before taking over an expired
    /// lease. `safety`: how early an owner stops acting before its expiry.
    /// `max_skew`: the largest clock difference between nodes tolerated.
    pub fn new(
        ttl_ms: u64,
        grace_ms: u64,
        safety_ms: u64,
        max_skew_ms: u64,
    ) -> Result<Self, ConfigError> {
        if safety_ms >= ttl_ms {
            return Err(ConfigError("safety must be shorter than the ttl"));
        }
        if grace_ms.saturating_add(safety_ms) < max_skew_ms.saturating_mul(2) {
            return Err(ConfigError("grace + safety must be at least 2 x max_skew"));
        }
        Ok(Self::new_unchecked(
            ttl_ms,
            grace_ms,
            safety_ms,
            max_skew_ms,
        ))
    }

    /// Skips the safety check. Only for simulations proving the check matters.
    #[doc(hidden)]
    pub fn new_unchecked(ttl_ms: u64, grace_ms: u64, safety_ms: u64, max_skew_ms: u64) -> Self {
        Self {
            ttl_ms,
            grace_ms,
            safety_ms,
            max_skew_ms,
        }
    }

    /// Hand-off defaults: ttl 10 s, grace 5 s, safety 2 s, tolerating 1 s skew.
    pub fn default_safe() -> Self {
        Self::new_unchecked(10_000, 5_000, 2_000, 1_000)
    }

    /// Lease length.
    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// How long after a lease runs out another node may take it.
    pub fn grace_ms(&self) -> u64 {
        self.grace_ms
    }

    /// Largest tolerated clock difference.
    pub fn max_skew_ms(&self) -> u64 {
        self.max_skew_ms
    }

    /// How long before its expiry an owner stops acting as one.
    pub fn safety_ms(&self) -> u64 {
        self.safety_ms
    }
}

/// The stored lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    /// Node that holds it.
    pub owner: String,
    /// Increments on every acquisition.
    pub generation: u64,
    /// Expiry on the owner's clock.
    pub expires_at_ms: u64,
}

impl LeaseRecord {
    fn encode(&self) -> Vec<u8> {
        format!(
            "wasmkeep-lease 1\n{}\n{}\n{}\n",
            self.owner, self.generation, self.expires_at_ms
        )
        .into_bytes()
    }

    /// Parses a stored lease (`wasmkeep-lease 1`); `None` for anything else.
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.lines();
        if lines.next()? != "wasmkeep-lease 1" {
            return None;
        }
        Some(Self {
            owner: lines.next()?.to_owned(),
            generation: lines.next()?.parse().ok()?,
            expires_at_ms: lines.next()?.parse().ok()?,
        })
    }
}

/// The per-actor lease operations, retired (see [`legacy`]).
mod legacy;
#[doc(hidden)]
pub use legacy::{Grant, LeaseError, Leases, Release};

impl fmt::Display for LeaseRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} gen {} until {}",
            self.owner, self.generation, self.expires_at_ms
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use super::*;

    /// In-memory bucket; versions are a counter.
    #[derive(Default)]
    pub(crate) struct Mem {
        objects: RefCell<HashMap<String, (Vec<u8>, u64)>>,
        next: Cell<u64>,
        /// The next replace is applied but reported as unavailable (a reply
        /// lost after the write).
        pub(crate) lose_next_reply: Cell<bool>,
        /// The next create or replace is applied but reported as a
        /// conflict (a client's retry after its first try landed: S3's
        /// 409, then 412).
        pub(crate) conflict_after_landing: Cell<bool>,
        /// Another writer's object, written at its key (the record, or a
        /// node lease) right before the next create or replace.
        pub(crate) steal_next: RefCell<Option<(String, Vec<u8>)>>,
        /// The next create or replace is not applied and reported as
        /// unavailable (no answer, and it did not land).
        pub(crate) refuse_next_write: Cell<bool>,
        /// After this many more successful reads, one read fails.
        pub(crate) fail_get_after: Cell<Option<u32>>,
    }

    impl Mem {
        fn bump(&self) -> Version {
            self.next.set(self.next.get() + 1);
            Version(self.next.get().to_string())
        }

        /// Another writer: `key` overwritten with `body` (`None`: deleted),
        /// whatever its version.
        pub(crate) fn overwrite(&self, key: &str, body: Option<&[u8]>) {
            match body {
                Some(body) => {
                    let v = self.bump();
                    self.objects
                        .borrow_mut()
                        .insert(key.into(), (body.to_vec(), v.0.parse().unwrap_or(0)));
                }
                None => {
                    self.objects.borrow_mut().remove(key);
                }
            }
        }
    }

    impl Bucket for &Mem {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
            match self.fail_get_after.get() {
                Some(0) => {
                    self.fail_get_after.set(None);
                    return Err(BucketError::Unavailable("read failed".into()));
                }
                Some(n) => self.fail_get_after.set(Some(n - 1)),
                None => {}
            }
            Ok(self
                .objects
                .borrow()
                .get(key)
                .map(|(b, v)| (b.clone(), Version(v.to_string()))))
        }
        async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
            if let Some((k, other)) = self.steal_next.take() {
                self.overwrite(&k, Some(&other));
            }
            if self.refuse_next_write.replace(false) {
                return Err(BucketError::Unavailable("no answer".into()));
            }
            if self.objects.borrow().contains_key(key) {
                return Err(BucketError::Conflict);
            }
            let v = self.bump();
            self.objects
                .borrow_mut()
                .insert(key.into(), (body, v.0.parse().unwrap_or(0)));
            if self.conflict_after_landing.replace(false) {
                return Err(BucketError::Conflict);
            }
            Ok(v)
        }
        async fn replace(
            &self,
            key: &str,
            body: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BucketError> {
            if let Some((k, other)) = self.steal_next.take() {
                self.overwrite(&k, Some(&other));
            }
            if self.refuse_next_write.replace(false) {
                return Err(BucketError::Unavailable("no answer".into()));
            }
            let current = self.objects.borrow().get(key).map(|(_, v)| v.to_string());
            if current.as_deref() != Some(expected.0.as_str()) {
                return Err(BucketError::Conflict);
            }
            let v = self.bump();
            self.objects
                .borrow_mut()
                .insert(key.into(), (body, v.0.parse().unwrap_or(0)));
            if self.lose_next_reply.replace(false) {
                return Err(BucketError::Unavailable("reply lost".into()));
            }
            if self.conflict_after_landing.replace(false) {
                return Err(BucketError::Conflict);
            }
            Ok(v)
        }
    }

    pub(crate) struct At<'a>(pub(crate) &'a Cell<u64>);
    impl Clock for At<'_> {
        fn now_ms(&self) -> u64 {
            self.0.get()
        }
    }

    /// The in-memory bucket never suspends, so one poll completes.
    pub(crate) fn run<F: Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => v,
            Poll::Pending => unreachable!("in-memory futures never suspend"),
        }
    }

    fn config() -> LeaseConfig {
        LeaseConfig::new(1_000, 500, 200, 100).unwrap()
    }

    #[test]
    fn routing_hints_are_read_only_and_expire_after_grace() {
        let mem = Mem::default();
        let t = Cell::new(10_000);
        let a = Leases::new(&mem, At(&t), "a", config());
        let b = Leases::new(&mem, At(&t), "b", config());
        assert_eq!(run(b.other_owner("k")).unwrap(), None);
        let grant = run(a.acquire("k")).unwrap();
        let version = mem.next.get();
        assert_eq!(run(a.other_owner("k")).unwrap(), None);
        assert_eq!(run(b.other_owner("k")).unwrap().as_deref(), Some("a"));
        t.set(11_500);
        assert_eq!(run(b.other_owner("k")).unwrap().as_deref(), Some("a"));
        t.set(11_501);
        assert_eq!(run(b.other_owner("k")).unwrap(), None);
        assert_eq!(mem.next.get(), version, "routing must not change the lease");
        let next = run(b.acquire("k")).unwrap();
        assert_eq!(next.generation, grant.generation + 1);
        assert!(matches!(run(a.renew(&grant)), Err(LeaseError::Lost)));
    }

    #[test]
    fn a_renewal_whose_reply_was_lost_does_not_end_ownership() {
        let mem = Mem::default();
        let t = Cell::new(10_000);
        let a = Leases::new(&mem, At(&t), "a", config());
        let g = run(a.acquire("k")).unwrap();
        t.set(10_300);
        mem.lose_next_reply.set(true);
        // The write landed but we did not hear back: our version is stale.
        assert!(matches!(run(a.renew(&g)), Err(LeaseError::Unavailable(_))));
        t.set(10_600);
        let renewed = run(a.renew(&g)).expect("our own write is not someone else's");
        assert_eq!(renewed.generation, g.generation);
        assert_eq!(renewed.valid_until_ms, 10_600 + 1_000 - 200);
    }

    #[test]
    fn a_release_after_a_lost_reply_still_releases() {
        let mem = Mem::default();
        let t = Cell::new(10_000);
        let a = Leases::new(&mem, At(&t), "a", config());
        let b = Leases::new(&mem, At(&t), "b", config());
        let g = run(a.acquire("k")).unwrap();
        mem.lose_next_reply.set(true);
        let _ = run(a.renew(&g));
        run(a.release(&g)).unwrap();
        t.set(10_000 + 500 + 1);
        assert!(
            run(b.acquire("k")).is_ok(),
            "b waited for an unreleased lease"
        );
    }

    #[test]
    fn someone_elses_write_is_still_lost() {
        let mem = Mem::default();
        let t = Cell::new(10_000);
        let a = Leases::new(&mem, At(&t), "a", config());
        let b = Leases::new(&mem, At(&t), "b", config());
        let ga = run(a.acquire("k")).unwrap();
        t.set(10_000 + 1_000 + 500 + 1);
        run(b.acquire("k")).unwrap();
        assert_eq!(run(a.renew(&ga)), Err(LeaseError::Lost));
        // a's own earlier generation: also not a renewal.
        let ga2 = run(a.acquire("k"));
        assert!(matches!(ga2, Err(LeaseError::HeldBy { .. })));
    }

    #[test]
    fn config_rejects_skew_it_cannot_cover() {
        assert!(LeaseConfig::new(1_000, 500, 200, 350).is_ok());
        assert!(LeaseConfig::new(1_000, 500, 200, 351).is_err());
        assert!(LeaseConfig::new(1_000, 500, 1_000, 0).is_err());
        let d = LeaseConfig::default_safe();
        assert_eq!(
            LeaseConfig::new(d.ttl_ms, d.grace_ms, d.safety_ms, d.max_skew_ms),
            Ok(d)
        );
    }

    #[test]
    fn acquire_hold_takeover_and_fencing() {
        let mem = Mem::default();
        let (ta, tb) = (Cell::new(0), Cell::new(0));
        let a = Leases::new(&mem, At(&ta), "a", config());
        let b = Leases::new(&mem, At(&tb), "b", config());

        let ga = run(a.acquire("k")).unwrap();
        assert_eq!((ga.generation, ga.valid_until_ms), (1, 800));
        assert!(matches!(
            run(b.acquire("k")),
            Err(LeaseError::HeldBy { .. })
        ));

        // Renewal extends; b still waits for expiry + grace.
        ta.set(600);
        let ga = run(a.renew(&ga)).unwrap();
        tb.set(1_600 + 500);
        assert!(matches!(
            run(b.acquire("k")),
            Err(LeaseError::HeldBy { .. })
        ));
        tb.set(1_600 + 501);
        let gb = run(b.acquire("k")).unwrap();
        assert_eq!(gb.generation, 2);

        // a's next renewal is refused: it must stop acting as owner.
        ta.set(1_700);
        assert_eq!(run(a.renew(&ga)), Err(LeaseError::Lost));
        assert!(run(b.renew(&gb)).is_ok());
    }

    #[test]
    fn reacquiring_your_own_lease_starts_a_new_generation() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Leases::new(&mem, At(&t), "a", config());
        let g1 = run(a.acquire("k")).unwrap();
        // A restarted process has no grant: it acquires again.
        let g2 = run(a.acquire("k")).unwrap();
        assert_eq!((g1.generation, g2.generation), (1, 2));
        assert_eq!(
            run(a.renew(&g1)),
            Err(LeaseError::Lost),
            "the old incarnation is fenced"
        );
    }

    #[test]
    fn release_lets_others_in_after_the_grace_period() {
        let mem = Mem::default();
        let (ta, tb) = (Cell::new(0), Cell::new(0));
        let a = Leases::new(&mem, At(&ta), "a", config());
        let b = Leases::new(&mem, At(&tb), "b", config());
        let ga = run(a.acquire("k")).unwrap();
        run(a.release(&ga)).unwrap();
        tb.set(400);
        assert!(matches!(
            run(b.acquire("k")),
            Err(LeaseError::HeldBy { .. })
        ));
        tb.set(501);
        assert_eq!(run(b.acquire("k")).unwrap().generation, 2);
    }

    #[test]
    fn a_release_says_whether_it_gave_the_lease_up() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Leases::new(&mem, At(&t), "a", config());
        let b = Leases::new(&mem, At(&t), "b", config());
        let ga = run(a.acquire("k")).unwrap();
        assert_eq!(run(a.release(&ga)).unwrap(), Release::Released);
        assert_eq!(
            run(a.release(&ga)).unwrap(),
            Release::Superseded,
            "already released"
        );
        // Taken over after a's grant ran out: a's late release changes nothing.
        let ga = run(a.acquire("k")).unwrap();
        t.set(10_000);
        let gb = run(b.acquire("k")).unwrap();
        assert_eq!(run(a.release(&ga)).unwrap(), Release::Superseded);
        assert!(run(b.renew(&gb)).is_ok(), "b still holds it");
    }

    #[test]
    fn only_a_lease_taken_after_its_owner_stopped_renewing_is_a_takeover() {
        let mem = Mem::default();
        let t = Cell::new(0);
        let a = Leases::new(&mem, At(&t), "a", config());
        let b = Leases::new(&mem, At(&t), "b", config());
        // New, and our own again: not a takeover.
        let ga = run(a.acquire("k")).unwrap();
        assert_eq!(ga.took_over_from, None);
        let ga = run(a.acquire("k")).unwrap();
        assert_eq!(ga.took_over_from, None);
        // Released, then taken by b: a hand-off.
        run(a.release(&ga)).unwrap();
        t.set(501);
        let gb = run(b.acquire("k")).unwrap();
        assert_eq!(gb.took_over_from, None);
        // b stops renewing; a takes it past expiry and grace: a failover.
        t.set(501 + 1_000 + 501);
        let ga = run(a.acquire("k")).unwrap();
        assert_eq!(ga.took_over_from.as_deref(), Some("b"));
        // Renewals never report one.
        assert_eq!(run(a.renew(&ga)).unwrap().took_over_from, None);
    }

    #[test]
    fn a_corrupt_record_is_reported_not_overwritten() {
        let mem = Mem::default();
        let t = Cell::new(0);
        mem.objects
            .borrow_mut()
            .insert("k".into(), (b"garbage".to_vec(), 1));
        let a = Leases::new(&mem, At(&t), "a", config());
        assert_eq!(run(a.acquire("k")), Err(LeaseError::Corrupt));
        assert_eq!(mem.objects.borrow()["k"].0, b"garbage");
    }

    #[test]
    fn records_round_trip() {
        let r = LeaseRecord {
            owner: "node-a".into(),
            generation: 9,
            expires_at_ms: 123,
        };
        assert_eq!(LeaseRecord::decode(&r.encode()), Some(r));
        assert_eq!(LeaseRecord::decode(b"wasmkeep-lease 2\na\n1\n2\n"), None);
    }
}
