//! The root of a node's actor state: the one place that spells where each
//! family of per-actor keys and files lives
//! ([ADR 0022](../../../docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
//!
//! Every builder of an actor-state key or path starts from a [`StateRoot`]
//! and never from an address's tenant, so a change of layout is a change
//! here alone. Today the root is the bare layout, with the tenant after
//! the family:
//!
//! ```text
//! actors/<tenant>/<class>/<name>/{wal,snapshot}/...   history (ADR 0007)
//! owners/<tenant>/<class>/<name>                      ownership (ADR 0020)
//! leases/<tenant>/<class>/<name>                      old-lease marker (ADR 0020)
//! alarms/<tenant>/<at>/...                            alarm index (ADR 0009)
//! control/move/<tenant>/<class>/<name hex>/<target>   moves (ADR 0014)
//! <data_dir>/actors/<tenant>/<class>/<name>.db        local databases
//! <data_dir>/.deleting/                               local scratch space
//! ```

use std::path::{Path, PathBuf};

/// A family of actor-state keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    /// LTX segments and snapshots (ADRs 0007, 0012).
    History,
    /// Ownership records (ADR 0020).
    Owners,
    /// Markers left in place of the per-actor leases from before ADR 0020.
    Leases,
    /// The alarm index (ADR 0009).
    Alarms,
    /// Requests to move an actor (ADR 0014).
    Moves,
}

impl Family {
    /// The family's folder in the bare layout.
    const fn bare(self) -> &'static str {
        match self {
            Self::History => "actors",
            Self::Owners => "owners",
            Self::Leases => "leases",
            Self::Alarms => "alarms",
            Self::Moves => "control/move",
        }
    }
}

/// Where a node's actor state lives: in the bucket under `prefix`, and on
/// disk under the data directory. Built once per node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot {
    /// Key prefix in the bucket, ending in `/` (or empty).
    prefix: String,
    /// The tenant whose actors this is.
    tenant: String,
}

impl StateRoot {
    /// The root of `tenant`'s actor state under bucket prefix `prefix` (a
    /// `/` is added unless it is empty or ends in one).
    pub fn new(prefix: &str, tenant: impl Into<String>) -> Self {
        let mut prefix = prefix.to_owned();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self {
            prefix,
            tenant: tenant.into(),
        }
    }

    /// The bucket prefix, ending in `/` (or empty).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The tenant whose actors this is (recorded inside each database and
    /// carried in addresses; never a key input outside this module).
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// `family`'s folder relative to the bucket prefix, without a
    /// trailing `/`.
    pub fn base(&self, family: Family) -> String {
        format!("{}/{}", family.bare(), self.tenant)
    }

    /// The key `tail` of `family`, relative to the bucket prefix (for
    /// stores that add the prefix themselves).
    pub fn key(&self, family: Family, tail: &str) -> String {
        format!("{}/{tail}", self.base(family))
    }

    /// [`Self::base`] with the bucket prefix in front.
    pub fn bucket_base(&self, family: Family) -> String {
        format!("{}{}", self.prefix, self.base(family))
    }

    /// [`Self::key`] with the bucket prefix in front.
    pub fn bucket_key(&self, family: Family, tail: &str) -> String {
        format!("{}{}", self.prefix, self.key(family, tail))
    }

    /// The folder of this root's actor databases, relative to the data
    /// directory, each at `<class>/<name>.db` in it. `encode` turns a name
    /// into one safe file name segment (the storage crate's
    /// `encode_segment`).
    pub fn local(&self, encode: impl Fn(&str) -> String) -> PathBuf {
        Path::new("actors").join(encode(&self.tenant))
    }

    /// Scratch space, relative to the data directory, emptied after each
    /// use.
    pub fn scratch(&self) -> PathBuf {
        PathBuf::from(".deleting")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_root_is_the_bare_layout() {
        for prefix in ["p", "p/"] {
            let root = StateRoot::new(prefix, "acme");
            assert_eq!(root.prefix(), "p/");
            assert_eq!(root.base(Family::Owners), "owners/acme");
            assert_eq!(root.key(Family::Leases, "c/n"), "leases/acme/c/n");
            assert_eq!(
                root.key(Family::Moves, "c/6e/t"),
                "control/move/acme/c/6e/t"
            );
            assert_eq!(root.bucket_base(Family::Alarms), "p/alarms/acme");
            assert_eq!(
                root.bucket_key(Family::History, "c/n/"),
                "p/actors/acme/c/n/"
            );
        }
        let root = StateRoot::new("", "Acme");
        assert_eq!(root.bucket_key(Family::Owners, "c/n"), "owners/Acme/c/n");
        assert_eq!(root.local(|s| s.to_lowercase()), Path::new("actors/acme"));
        assert_eq!(root.scratch(), Path::new(".deleting"));
    }
}
