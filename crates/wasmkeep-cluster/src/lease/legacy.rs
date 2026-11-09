//! The per-actor lease operations ([`Leases`]), retired in the server by
//! ADR 0020 (one lease per node, `crate::node_lease`). Kept, hidden from
//! the documentation, for `madsim_lease` (the simulation of the retired
//! protocol, whose safety argument the module above states) and the tests.
//! New code holds actors through `crate::node_lease::Owners`.

use super::{Bucket, BucketError, Clock, LeaseConfig, LeaseRecord, Version};

/// Proof of ownership, held by the owning node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The lease key.
    pub key: String,
    /// Generation to fence writes with.
    pub generation: u64,
    /// Act as owner only while the local clock is before this.
    pub valid_until_ms: u64,
    /// Set by [`Leases::acquire`] when the lease was taken from another
    /// node whose lease expired (past the grace period) instead of being
    /// released: that node died, was cut off, or renewed too slowly. A
    /// failover, as opposed to a hand-off. Informational only.
    pub took_over_from: Option<String>,
    version: Version,
}

impl Grant {
    /// Whether this node may still act as owner at local time `now_ms`.
    pub fn is_valid(&self, now_ms: u64) -> bool {
        now_ms < self.valid_until_ms
    }
}

/// What [`Leases::release`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    /// This node's lease is now released: another node may take it at once.
    Released,
    /// The lease was no longer this grant's (taken over, already released,
    /// or rewritten): nothing to give up.
    Superseded,
}

/// Why a lease operation did not grant ownership.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseError {
    /// Another node holds a live lease (or its grace period is not over).
    #[error("held by {owner} until {expires_at_ms}")]
    HeldBy {
        /// The holder.
        owner: String,
        /// Its expiry, on its clock.
        expires_at_ms: u64,
    },
    /// Someone else wrote the lease since our grant: stop acting as owner now.
    #[error("lease lost")]
    Lost,
    /// Another node took the lease concurrently; try again later.
    #[error("lost a race for the lease")]
    Contended,
    /// The bucket did not answer; a renewal's grant stays valid until its
    /// `valid_until_ms`, then the node must stop.
    #[error("bucket unavailable: {0}")]
    Unavailable(String),
    /// The lease object is not a lease this version understands.
    #[error("unreadable lease record")]
    Corrupt,
}

impl From<BucketError> for LeaseError {
    fn from(e: BucketError) -> Self {
        match e {
            BucketError::Conflict => Self::Contended,
            BucketError::Unavailable(m) => Self::Unavailable(m),
        }
    }
}

/// Lease operations for one node.
#[derive(Debug)]
pub struct Leases<B, C> {
    bucket: B,
    clock: C,
    node: String,
    config: LeaseConfig,
}

impl<B: Bucket, C: Clock> Leases<B, C> {
    /// Leases taken as `node`.
    pub fn new(bucket: B, clock: C, node: impl Into<String>, config: LeaseConfig) -> Self {
        Self {
            bucket,
            clock,
            node: node.into(),
            config,
        }
    }

    /// The node's clock.
    pub fn clock(&self) -> &C {
        &self.clock
    }

    /// The node these leases are taken as.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Another node to try before allocating local actor resources. This
    /// read-only hint grants no authority: the recipient must still check
    /// its lease. Missing, ours, or expired past grace means try locally.
    pub async fn other_owner(&self, key: &str) -> Result<Option<String>, LeaseError> {
        let Some((body, _)) = self.bucket.get(key).await? else {
            return Ok(None);
        };
        let stored = LeaseRecord::decode(&body).ok_or(LeaseError::Corrupt)?;
        Ok((stored.owner != self.node
            && self.clock.now_ms() <= stored.expires_at_ms.saturating_add(self.config.grace_ms))
        .then_some(stored.owner))
    }

    /// Takes the lease if it is absent, ours, or expired past the grace
    /// period. Always starts a new generation.
    pub async fn acquire(&self, key: &str) -> Result<Grant, LeaseError> {
        let current = self.bucket.get(key).await?;
        let sent = self.clock.now_ms();
        let mut took_over_from = None;
        let (generation, written) = match current {
            None => {
                let record = self.record(1, sent);
                (1, self.bucket.create(key, record.encode()).await)
            }
            Some((body, version)) => {
                let stored = LeaseRecord::decode(&body).ok_or(LeaseError::Corrupt)?;
                let takeover_at = stored.expires_at_ms.saturating_add(self.config.grace_ms);
                if stored.owner != self.node && sent <= takeover_at {
                    return Err(LeaseError::HeldBy {
                        owner: stored.owner,
                        expires_at_ms: stored.expires_at_ms,
                    });
                }
                // A record at u64::MAX could only come from a corrupt bucket;
                // wrapping to 0 would reuse generations and their segment keys.
                let generation = stored
                    .generation
                    .checked_add(1)
                    .ok_or(LeaseError::Corrupt)?;
                // A released lease has expiry 0 (see `release`).
                if stored.owner != self.node && stored.expires_at_ms != 0 {
                    took_over_from = Some(stored.owner);
                }
                let record = self.record(generation, sent);
                (
                    generation,
                    self.bucket.replace(key, record.encode(), &version).await,
                )
            }
        };
        let mut grant = self.grant(key, generation, sent, written?);
        grant.took_over_from = took_over_from;
        Ok(grant)
    }

    /// Extends our lease. `Lost` if anyone else wrote it since the grant.
    ///
    /// A conflict can also mean our own earlier write landed but its reply
    /// was lost. Any acquire starts a new generation, so a record still
    /// naming us at our generation was written by us: continue from it.
    pub async fn renew(&self, grant: &Grant) -> Result<Grant, LeaseError> {
        let sent = self.clock.now_ms();
        let record = self.record(grant.generation, sent);
        let mut version = grant.version.clone();
        for attempt in 0..2 {
            match self
                .bucket
                .replace(&grant.key, record.encode(), &version)
                .await
            {
                Ok(v) => return Ok(self.grant(&grant.key, grant.generation, sent, v)),
                Err(BucketError::Unavailable(m)) => return Err(LeaseError::Unavailable(m)),
                Err(BucketError::Conflict) if attempt == 0 => match self.still_ours(grant).await? {
                    Some(v) => version = v,
                    None => return Err(LeaseError::Lost),
                },
                Err(BucketError::Conflict) => return Err(LeaseError::Lost),
            }
        }
        Err(LeaseError::Lost)
    }

    /// Gives the lease up so another node can take it at once. Call only
    /// once this node has stopped acting as the owner.
    pub async fn release(&self, grant: &Grant) -> Result<Release, LeaseError> {
        let record = LeaseRecord {
            owner: self.node.clone(),
            generation: grant.generation,
            expires_at_ms: 0,
        };
        let mut version = grant.version.clone();
        for attempt in 0..2 {
            match self
                .bucket
                .replace(&grant.key, record.encode(), &version)
                .await
            {
                Ok(_) => return Ok(Release::Released),
                Err(BucketError::Unavailable(m)) => return Err(LeaseError::Unavailable(m)),
                // A renewal still in flight may have moved the version on.
                Err(BucketError::Conflict) if attempt == 0 => match self.still_ours(grant).await? {
                    Some(v) => version = v,
                    // Someone else holds it, or it is already released or
                    // expired: nothing to give up.
                    None => return Ok(Release::Superseded),
                },
                Err(BucketError::Conflict) => return Ok(Release::Superseded),
            }
        }
        Ok(Release::Superseded)
    }

    /// The lease object's current version if it is still our live lease at
    /// the grant's generation (so its last write was ours).
    async fn still_ours(&self, grant: &Grant) -> Result<Option<Version>, LeaseError> {
        let Some((body, version)) = self.bucket.get(&grant.key).await? else {
            return Ok(None);
        };
        let stored = LeaseRecord::decode(&body).ok_or(LeaseError::Corrupt)?;
        let ours = stored.owner == self.node
            && stored.generation == grant.generation
            && stored.expires_at_ms != 0;
        Ok(ours.then_some(version))
    }

    fn record(&self, generation: u64, sent: u64) -> LeaseRecord {
        LeaseRecord {
            owner: self.node.clone(),
            generation,
            expires_at_ms: sent.saturating_add(self.config.ttl_ms),
        }
    }

    fn grant(&self, key: &str, generation: u64, sent: u64, version: Version) -> Grant {
        Grant {
            key: key.to_owned(),
            generation,
            valid_until_ms: sent + self.config.ttl_ms - self.config.safety_ms,
            took_over_from: None,
            version,
        }
    }
}
