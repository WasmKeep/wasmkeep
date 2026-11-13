//! The root of a node's actor state: the one place that spells where each
//! family of per-actor keys and files lives
//! ([ADR 0022](../../../docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
//!
//! Every builder of an actor-state key or path starts from a [`StateRoot`]
//! and never from an address's tenant. Each app's state lives under its
//! own root, `state/<tenant>/<app>/`, beside `apps/<tenant>/<app>/`, so
//! two apps never share an actor whatever bucket prefix they use:
//!
//! ```text
//! state/<t>/<a>/actors/<class>/<name>/{wal,snapshot}/...   history (ADR 0007)
//! state/<t>/<a>/owners/<class>/<name>                      ownership (ADR 0020)
//! state/<t>/<a>/leases/<class>/<name>                      old-lease marker (ADR 0020)
//! state/<t>/<a>/alarms/<at>/...                            alarm index (ADR 0009)
//! state/<t>/<a>/moves/<class>/<name hex>/<target>          moves (ADR 0014)
//! <data_dir>/state/<t>/<a>/actors/<class>/<name>.db        local databases
//! <data_dir>/state/<t>/<a>/.deleting/                      local scratch space
//! ```
//!
//! The tenant and app are raw in keys, as under `apps/` (the name grammar
//! keeps them one safe segment), and encoded on disk. Everything after the
//! family is byte-identical to the layout before 0.4.0, the bare one
//! ([`Family::bare`], `<family>/<tenant>/<tail>`), which only
//! `cluster migrate` still reads ([`crate::migrate`]).

use std::path::{Path, PathBuf};

/// A family of actor-state keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// Every family, in the order a migration copies them.
    pub const ALL: [Self; 5] = [
        Self::History,
        Self::Owners,
        Self::Leases,
        Self::Alarms,
        Self::Moves,
    ];

    /// The family's folder under a root.
    pub const fn folder(self) -> &'static str {
        match self {
            Self::History => "actors",
            Self::Owners => "owners",
            Self::Leases => "leases",
            Self::Alarms => "alarms",
            Self::Moves => "moves",
        }
    }

    /// The family's folder in the layout before 0.4.0, the tenant after
    /// it: `<bare>/<tenant>/<tail>`, relative to the bucket prefix.
    pub const fn bare(self) -> &'static str {
        match self {
            Self::History => "actors",
            Self::Owners => "owners",
            Self::Leases => "leases",
            Self::Alarms => "alarms",
            Self::Moves => "control/move",
        }
    }
}

/// The folder of every app's state root, relative to the bucket prefix.
pub const STATE_PREFIX: &str = "state/";

/// Where a node's actor state lives: in the bucket under `prefix`, and on
/// disk under the data directory. Built once per node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot {
    /// Key prefix in the bucket, ending in `/` (or empty).
    prefix: String,
    /// The tenant whose actors this is.
    tenant: String,
    /// The app, within the tenant, whose actors this is.
    app: String,
}

impl StateRoot {
    /// The root of app `tenant/app`'s actor state under bucket prefix
    /// `prefix` (a `/` is added unless it is empty or ends in one).
    pub fn new(prefix: &str, tenant: impl Into<String>, app: impl Into<String>) -> Self {
        let mut prefix = prefix.to_owned();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self {
            prefix,
            tenant: tenant.into(),
            app: app.into(),
        }
    }

    /// The bucket prefix, ending in `/` (or empty).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The tenant whose actors this is (recorded inside each database and
    /// carried in addresses).
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// The app whose actors this is.
    pub fn app(&self) -> &str {
        &self.app
    }

    /// The root itself relative to the bucket prefix, `state/<t>/<a>/`.
    pub fn bucket(&self) -> String {
        format!("{STATE_PREFIX}{}/{}/", self.tenant, self.app)
    }

    /// `family`'s folder relative to the bucket prefix, without a
    /// trailing `/`.
    pub fn base(&self, family: Family) -> String {
        format!("{}{}", self.bucket(), family.folder())
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

    /// The folder of this root's local state, relative to the data
    /// directory: `state/<t>/<a>`, each encoded by `encode` (the storage
    /// crate's `encode_segment`: `..` is a valid tenant or app name).
    pub fn local_root(&self, encode: impl Fn(&str) -> String) -> PathBuf {
        Path::new("state")
            .join(encode(&self.tenant))
            .join(encode(&self.app))
    }

    /// The folder of this root's actor databases, relative to the data
    /// directory, each at `<class>/<name>.db` in it.
    pub fn local(&self, encode: impl Fn(&str) -> String) -> PathBuf {
        self.local_root(encode).join("actors")
    }

    /// Scratch space, relative to the data directory, emptied after each
    /// use.
    pub fn scratch(&self, encode: impl Fn(&str) -> String) -> PathBuf {
        self.local_root(encode).join(".deleting")
    }

    /// Where the tenant's databases were before 0.4.0, relative to the
    /// data directory: `actors/<t>`, shared by every app of the tenant.
    pub fn bare_local(&self, encode: impl Fn(&str) -> String) -> PathBuf {
        Path::new("actors").join(encode(&self.tenant))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_app_has_its_own_root() {
        for prefix in ["p", "p/"] {
            let root = StateRoot::new(prefix, "acme", "blog");
            assert_eq!(root.prefix(), "p/");
            assert_eq!(root.bucket(), "state/acme/blog/");
            assert_eq!(root.base(Family::Owners), "state/acme/blog/owners");
            assert_eq!(
                root.key(Family::Leases, "c/n"),
                "state/acme/blog/leases/c/n"
            );
            assert_eq!(
                root.key(Family::Moves, "c/6e/t"),
                "state/acme/blog/moves/c/6e/t"
            );
            assert_eq!(root.bucket_base(Family::Alarms), "p/state/acme/blog/alarms");
            assert_eq!(
                root.bucket_key(Family::History, "c/n/"),
                "p/state/acme/blog/actors/c/n/"
            );
        }
        let root = StateRoot::new("", "Acme", "Blog");
        assert_eq!(
            root.bucket_key(Family::Owners, "c/n"),
            "state/Acme/Blog/owners/c/n"
        );
        let lower = |s: &str| s.to_lowercase();
        assert_eq!(root.local(lower), Path::new("state/acme/blog/actors"));
        assert_eq!(root.scratch(lower), Path::new("state/acme/blog/.deleting"));
        assert_eq!(root.bare_local(lower), Path::new("actors/acme"));
    }

    #[test]
    fn the_bare_layout_puts_the_tenant_after_the_family() {
        let bare: Vec<&str> = Family::ALL.iter().map(|f| f.bare()).collect();
        assert_eq!(
            bare,
            ["actors", "owners", "leases", "alarms", "control/move"]
        );
    }
}
