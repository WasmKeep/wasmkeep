//! What is left of the layout before 0.4.0
//! ([ADR 0022](../../../docs/adr/0022-app-owned-state-and-the-one-time-migration.md)).
//!
//! `wasmkeep cluster migrate` moved a bucket's actor state from the bare
//! layout (`<family>/<tenant>/<tail>`, [`Family::bare`]) into each app's
//! root. It was removed in 0.9.0: a cluster older than 0.4.0 upgrades
//! through 0.8.x first. This version still reads what a migration left
//! for good, and refuses what it cannot serve:
//!
//! - the tombstones over the bare ownership records of every migrated
//!   tenant ([`tombstone_target`], read by
//!   [`OwnerObject::decode`](crate::node_lease::OwnerObject::decode));
//! - a tenant's commit (`cluster/migrated/<tenant>`) and a 0.8.x run's
//!   lock (`cluster/migration`), which [`start_check`] reads before any
//!   node or command of this version writes;
//! - an app's `current` token `3` ([`ROOTED_TOKEN`]).

use crate::lease::BucketError;
use crate::state_root::Family;
use crate::store::Store;

/// The lock a 0.8.x `cluster migrate` run holds, relative to the bucket
/// prefix.
pub const LOCK_KEY: &str = "cluster/migration";
/// A tenant's commit, `cluster/migrated/<tenant>`: from it on, the roots
/// are the truth for the tenant.
pub const COMMIT_PREFIX: &str = "cluster/migrated/";
/// The third token of an app's `current` once its state is rooted.
pub const ROOTED_TOKEN: &str = "3";
/// The first line of a tombstone over a bare ownership record.
pub const TOMBSTONE_HEADER: &str = "wasmkeep-owner moved";
/// The third line of a tombstone written where no ownership record
/// existed (an actor from before records, ADR 0020 §6).
const UNOWNED: &str = "none";
/// The families whose bare keys mean the tenant was not migrated. Moves
/// are left out: they are transient, and an old `actor move` may still
/// write one.
const BARE_FAMILIES: [Family; 4] = [
    Family::History,
    Family::Owners,
    Family::Leases,
    Family::Alarms,
];

/// The body of a tombstone over a bare record whose actor now lives at
/// `moved_to`, as a migration wrote it.
#[cfg(test)]
pub(crate) fn tombstone(moved_to: &str) -> Vec<u8> {
    format!("{TOMBSTONE_HEADER}\n{moved_to}\n").into_bytes()
}

/// The body of a tombstone where no ownership record existed, as a
/// migration wrote it.
#[cfg(test)]
pub(crate) fn tombstone_unowned(moved_to: &str) -> Vec<u8> {
    format!("{TOMBSTONE_HEADER}\n{moved_to}\n{UNOWNED}\n").into_bytes()
}

/// Whether `body` is a tombstone written where no record existed.
pub(crate) fn is_unowned(body: &[u8]) -> bool {
    tombstone_target(body).is_some()
        && std::str::from_utf8(body).is_ok_and(|t| t.lines().nth(2) == Some(UNOWNED))
}

/// Where a tombstone says its actor went; `None` if `body` is not one.
pub fn tombstone_target(body: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(body).ok()?;
    let mut lines = text.lines();
    (lines.next()? == TOMBSTONE_HEADER).then_some(())?;
    lines.next().filter(|t| !t.is_empty())
}

/// Why a node or command of this version must not start on a bucket.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartRefusal {
    /// A run of a 0.8.x `cluster migrate` holds the lock.
    #[error(
        "a migration of this bucket prefix is in progress ({LOCK_KEY} exists): let it finish, \
         or run `wasmkeep cluster migrate --apply` (or `--rollback`) again with the 0.8.x \
         binary that started it (this version has no `cluster migrate`: it was removed in \
         0.9.0)"
    )]
    InProgress,
    /// The tenant has actor state in the layout before 0.4.0 and no commit.
    #[error(
        "tenant {0} has actor state in the layout of a version before 0.4.0 ({1} exists) and \
         this bucket prefix was not migrated: this version cannot migrate it (`cluster \
         migrate` was removed in 0.9.0). Stop every node, migrate with the 0.8.1 binary \
         (`wasmkeep cluster migrate --bucket <this bucket>`, a dry run, then `--apply`), then \
         start this version (docs/deploy/upgrading-to-0.4.md)"
    )]
    Unmigrated(String, String),
    /// The bucket could not be read.
    #[error("reading the bucket: {0}")]
    Bucket(BucketError),
}

/// What a node or command of this version checks for `tenant` before it
/// writes anything to the bucket: no lock, and either a commit or no bare
/// key (moves aside). The commit is read first: a migrated tenant keeps
/// its tombstones at bare ownership keys for good. `Ok(true)` when the
/// tenant was migrated (its commit exists).
pub async fn start_check<S: Store>(store: &S, tenant: &str) -> Result<bool, StartRefusal> {
    if store
        .get(LOCK_KEY)
        .await
        .map_err(StartRefusal::Bucket)?
        .is_some()
    {
        return Err(StartRefusal::InProgress);
    }
    if store
        .get(&format!("{COMMIT_PREFIX}{tenant}"))
        .await
        .map_err(StartRefusal::Bucket)?
        .is_some()
    {
        return Ok(true);
    }
    for family in BARE_FAMILIES {
        let base = format!("{}/{tenant}/", family.bare());
        let dirs = store.list_dirs(&base).await.map_err(StartRefusal::Bucket)?;
        if let Some(first) = dirs.first() {
            return Err(StartRefusal::Unmigrated(
                tenant.to_owned(),
                format!("{base}{first}/"),
            ));
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstones_read_back() {
        let t = tombstone("state/acme/shop/owners/chat/room");
        assert_eq!(
            tombstone_target(&t),
            Some("state/acme/shop/owners/chat/room")
        );
        assert_eq!(tombstone_target(b"wasmkeep-owner 1\na i\n3\n"), None);
        assert_eq!(tombstone_target(b"wasmkeep-owner moved\n\n"), None);
        let u = tombstone_unowned("state/acme/shop/owners/chat/room");
        assert_eq!(
            tombstone_target(&u),
            Some("state/acme/shop/owners/chat/room")
        );
        assert!(is_unowned(&u) && !is_unowned(&t));
        // Never an ownership record.
        assert!(crate::node_lease::OwnerRecord::decode(&t).is_none());
        assert!(crate::node_lease::OwnerRecord::decode(&u).is_none());
    }

    /// The start check: a fresh tenant starts; bare keys without a commit
    /// refuse it, naming 0.8.1; a commit lets it start over its
    /// tombstones; a 0.8.x run's lock refuses every tenant.
    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn the_start_check_refuses_an_unmigrated_tenant_and_a_running_migration() {
        use std::sync::Arc;

        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        use crate::object_store::ObjectStoreBucket;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b = ObjectStoreBucket::new(Arc::clone(&store), "wk");
        let other = ObjectStoreBucket::new(Arc::clone(&store), "other");
        assert_eq!(start_check(&b, "acme").await, Ok(false));
        // Keys of another prefix, or a move, do not count.
        other.put("owners/acme/c/n", b"x".to_vec()).await.unwrap();
        b.put("control/move/acme/c/6e/node-b", b"x".to_vec())
            .await
            .unwrap();
        assert_eq!(start_check(&b, "acme").await, Ok(false));
        // Each copied family refuses on its own.
        for (key, found) in [
            ("actors/acme/c/n/wal/1.ltx", "actors/acme/c/"),
            ("owners/acme/c/n", "owners/acme/c/"),
            ("leases/acme/c/n", "leases/acme/c/"),
            ("alarms/acme/0001/c.t/6e/1-ab", "alarms/acme/0001/"),
        ] {
            b.put(key, b"x".to_vec()).await.unwrap();
            let e = start_check(&b, "acme").await.unwrap_err();
            assert_eq!(
                e,
                StartRefusal::Unmigrated("acme".into(), found.into()),
                "{key}"
            );
            assert!(e.to_string().contains("0.8.1"), "{e}");
            b.delete(key).await.unwrap();
        }
        // Migrated: the commit, a tombstone at the bare ownership key.
        b.put("owners/acme/c/n", tombstone("state/acme/shop/owners/c/n"))
            .await
            .unwrap();
        assert!(start_check(&b, "acme").await.is_err());
        b.put(&format!("{COMMIT_PREFIX}acme"), b"x".to_vec())
            .await
            .unwrap();
        assert_eq!(start_check(&b, "acme").await, Ok(true));
        assert_eq!(start_check(&b, "other").await, Ok(false));
        b.put(LOCK_KEY, b"wasmkeep-migration 1\nmode apply\n".to_vec())
            .await
            .unwrap();
        assert_eq!(start_check(&b, "acme").await, Err(StartRefusal::InProgress));
        assert_eq!(
            start_check(&b, "other").await,
            Err(StartRefusal::InProgress)
        );
    }
}
