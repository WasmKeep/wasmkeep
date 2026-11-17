//! Garbage collection of old deploy artifacts (ADR 0012; layout
//! from ADR 0013), and marker-protected static assets (ADR 0030).
//!
//! Asset collection marks before listing deploys, uses bucket time for
//! retention, and reads every remaining version's manifest and index before
//! deleting an unused object. Unknown or unreadable references stop the pass.
//!
//! Only versions in the current version's lineage (its `previous` chain)
//! are candidates, never a directory outside it: one a concurrent deploy
//! is still writing, or one a deploy left behind when it failed, is not
//! touched. The current version and the [`KEEP_PREVIOUS`] before it are
//! kept, since rollback walks that chain; older ancestors go once their
//! manifest is older than the retention window. Each is deleted manifest
//! first, so a half-deleted version has no manifest and no node loads it.

use std::collections::HashSet;
use std::time::Duration;

use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;

use super::{AppRef, DeployError, bucket};

/// Versions kept before the current one, for rollbacks.
pub const KEEP_PREVIOUS: usize = 5;

impl AppRef {
    /// Deletes the versions in the current version's lineage beyond the
    /// current one and the `keep_previous` before it, once each stopped
    /// being current more than `retention` ago (when the version after it
    /// was written), then collects unused assets. Returns the version count;
    /// [`Self::collect`] also returns the asset count.
    ///
    /// Run by `wasmkeep deploy` after every deploy and rollback, never by
    /// nodes. The walk follows `previous` while it names a plain version id
    /// and the manifest reads; it stops at a version whose manifest is
    /// missing (a half-deleted one: it is a candidate, its leftovers are
    /// finished off) and before one whose manifest is there but unreadable
    /// (perhaps from a newer WasmKeep: never deleted). Candidates go oldest
    /// first, so a pass cut short leaves only versions no walk reaches.
    /// `current` is read again before each version: if it changed (another
    /// deploy or a rollback), the pass stops.
    pub async fn collect_versions(
        &self,
        keep_previous: usize,
        retention: Duration,
    ) -> Result<usize, DeployError> {
        Ok(self.collect(keep_previous, retention).await?.0)
    }

    async fn collect_versions_at(
        &self,
        keep_previous: usize,
        retention: Duration,
        now_ms: i64,
        start: Option<tokio::time::Instant>,
    ) -> Result<usize, DeployError> {
        let Some(current) = self.current().await? else {
            return Ok(0);
        };
        let mut lineage = Vec::new();
        let mut seen = HashSet::new();
        let mut at = Some(current.version.clone());
        while let Some(version) = at {
            if !is_version_id(&version) || !seen.insert(version.clone()) {
                break; // not a key deploys write, or a cycle: stop
            }
            at = match self.manifest(&version).await {
                Ok(manifest) => manifest.previous,
                Err(DeployError::Corrupt(_)) => {
                    let present = match self.store.head(&self.manifest_path(&version)).await {
                        Ok(_) => true,
                        Err(object_store::Error::NotFound { .. }) => false,
                        Err(e) => return Err(bucket(e)),
                    };
                    if present {
                        break; // unreadable, not missing: keep it
                    }
                    None
                }
                Err(e) => return Err(e),
            };
            lineage.push(version);
        }
        let cutoff =
            now_ms.saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
        let mut deleted = 0;
        // Oldest first; each with the version after it (its successor).
        for index in (keep_previous.saturating_add(1)..lineage.len()).rev() {
            if start.is_some_and(expired) {
                break;
            }
            let (version, successor) = (&lineage[index], &lineage[index - 1]);
            match self.current().await? {
                Some(now) if now.version == current.version && now.seq == current.seq => {}
                _ => break, // another deploy or a rollback: stop here
            }
            if !retention.is_zero() {
                // When this version stopped being current: when its
                // successor's manifest was written.
                match self.store.head(&self.manifest_path(successor)).await {
                    Ok(meta) if meta.last_modified.timestamp_millis() <= cutoff => {}
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => continue,
                    Err(e) => return Err(bucket(e)),
                }
            }
            let manifest = self.manifest_path(version);
            let had_manifest = match self.store.head(&manifest).await {
                Ok(_) => true,
                // A pass that stopped after the manifest: no node can load
                // this version, so finishing its deletion is safe.
                Err(object_store::Error::NotFound { .. }) => false,
                Err(e) => return Err(bucket(e)),
            };
            // The manifest first: from here on no node loads this version.
            if start.is_some_and(expired) {
                break;
            }
            match self.store.delete(&manifest).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(bucket(e)),
            }
            let rest: Vec<_> = self
                .store
                .list(Some(&self.path(version)))
                .try_collect()
                .await
                .map_err(bucket)?;
            if !had_manifest && rest.is_empty() {
                continue; // already gone
            }
            for object in rest {
                if start.is_some_and(expired) {
                    return Ok(deleted);
                }
                match self.store.delete(&object.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(bucket(e)),
                }
            }
            deleted += 1;
        }
        Ok(deleted)
    }

    fn manifest_path(&self, version: &str) -> object_store::path::Path {
        self.path(&format!("{version}/manifest"))
    }
}

/// Whether `id` is a key deploys write: one non-empty segment of
/// lowercase letters and digits (ids are hex). Anything else (`/`, an
/// empty string, a hand-edited value) is never used to build a path to
/// delete under.
fn is_version_id(id: &str) -> bool {
    // `secrets` and `current` live beside the versions (ADR 0016): never
    // a version to collect, whatever a manifest's `previous` says.
    !matches!(id, "secrets" | "current")
        && !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use super::super::{Current, Manifest};
    use super::*;

    /// A bucket with versions `v0` (oldest) to `v{n-1}` (current), each a
    /// manifest naming the one before and a component; plus `orphan`, a
    /// version outside the lineage.
    async fn lineage(n: usize) -> (Arc<dyn ObjectStore>, AppRef) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let app = AppRef::new(Arc::clone(&store), "", "acme", "shop");
        let put = |key: String, body: String| {
            let store = Arc::clone(&store);
            async move {
                store
                    .put(
                        &object_store::path::Path::from(key),
                        PutPayload::from(body.into_bytes()),
                    )
                    .await
                    .unwrap();
            }
        };
        for i in 0..n {
            let manifest = Manifest {
                assets: None,
                previous: (i > 0).then(|| format!("v{}", i - 1)),
                classes: BTreeMap::from([("counter".into(), format!("{i:02}"))]),
                front: None,
                access: BTreeMap::new(),
                config: BTreeMap::new(),
                requires: BTreeMap::new(),
                expire: BTreeMap::new(),
                batched: std::collections::BTreeSet::new(),
                limits: Default::default(),
            };
            put(format!("apps/acme/shop/v{i}/manifest"), manifest.format()).await;
            put(format!("apps/acme/shop/v{i}/counter.wasm"), "wasm".into()).await;
        }
        put(
            "apps/acme/shop/orphan/manifest".into(),
            "wasmkeep-manifest 1\n".into(),
        )
        .await;
        let current = Current {
            version: format!("v{}", n - 1),
            seq: u64::try_from(n).unwrap(),
            marked: false,
            rooted: false,
        };
        put("apps/acme/shop/current".into(), current.format()).await;
        (store, app)
    }

    async fn versions(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        let mut keys: Vec<String> = store
            .list(Some(&object_store::path::Path::from("apps/acme/shop")))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn the_current_version_and_the_ones_before_it_are_kept() {
        let (store, app) = lineage(8).await;
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 2);
        let keys = versions(&store).await;
        // v0 and v1 went, whole; v2 to v7, the orphan and `current` stay.
        assert!(
            !keys
                .iter()
                .any(|k| k.contains("/v0/") || k.contains("/v1/")),
            "{keys:?}"
        );
        for i in 2..8 {
            assert!(
                keys.contains(&format!("apps/acme/shop/v{i}/manifest")),
                "v{i}: {keys:?}"
            );
            assert!(
                keys.contains(&format!("apps/acme/shop/v{i}/counter.wasm")),
                "v{i}: {keys:?}"
            );
        }
        assert!(keys.contains(&"apps/acme/shop/orphan/manifest".to_owned()));
        assert!(keys.contains(&"apps/acme/shop/current".to_owned()));
        // Nothing more to do.
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn versions_younger_than_the_window_are_kept() {
        let (store, app) = lineage(8).await;
        let before = versions(&store).await;
        assert_eq!(
            app.collect_versions(5, Duration::from_secs(3600))
                .await
                .unwrap(),
            0
        );
        assert_eq!(versions(&store).await, before);
    }

    #[tokio::test]
    async fn a_short_lineage_and_an_empty_app_collect_nothing() {
        let (store, app) = lineage(4).await;
        let before = versions(&store).await;
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 0);
        assert_eq!(versions(&store).await, before);
        let empty = AppRef::new(Arc::new(InMemory::new()), "", "acme", "none");
        assert_eq!(empty.collect_versions(5, Duration::ZERO).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn the_walk_stops_at_a_version_without_a_manifest() {
        // v3's manifest is gone (a pass stopped half-way, or a hand edit):
        // v0 to v2, behind it, are not reachable and stay.
        let (store, app) = lineage(8).await;
        store
            .delete(&object_store::path::Path::from(
                "apps/acme/shop/v3/manifest",
            ))
            .await
            .unwrap();
        assert_eq!(app.collect_versions(2, Duration::ZERO).await.unwrap(), 2);
        let keys = versions(&store).await;
        // Lineage v7, v6, v5, v4, v3 (no manifest): v4 and v3 beyond the
        // two kept; v3 had no manifest left, its component goes.
        assert!(
            !keys
                .iter()
                .any(|k| k.contains("/v4/") || k.contains("/v3/")),
            "{keys:?}"
        );
        assert!(
            keys.contains(&"apps/acme/shop/v2/manifest".to_owned()),
            "{keys:?}"
        );
    }

    async fn put(store: &Arc<dyn ObjectStore>, key: &str, body: &str) {
        store
            .put(
                &object_store::path::Path::from(key),
                PutPayload::from(body.as_bytes().to_vec()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_pass_cut_short_leaves_nothing_reachable_behind() {
        // Oldest first: a pass that stopped right after v0's manifest left
        // v0's component, and v1 still links to v0. The next pass finishes
        // v0 and takes v1; nothing older than v2 is stranded.
        let (store, app) = lineage(8).await;
        store
            .delete(&object_store::path::Path::from(
                "apps/acme/shop/v0/manifest",
            ))
            .await
            .unwrap();
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 2);
        let keys = versions(&store).await;
        assert!(
            !keys
                .iter()
                .any(|k| k.contains("/v0/") || k.contains("/v1/")),
            "{keys:?}"
        );
        assert!(keys.contains(&"apps/acme/shop/v2/manifest".to_owned()));
    }

    #[tokio::test]
    async fn a_previous_that_is_not_a_version_id_ends_the_walk() {
        // A hand-edited manifest naming `/` as previous: following it would
        // address the whole app. The walk stops there instead.
        let (store, app) = lineage(8).await;
        let manifest = Manifest {
            assets: None,
            previous: Some("/".into()),
            classes: BTreeMap::from([("counter".into(), "01".into())]),
            front: None,
            access: BTreeMap::new(),
            config: BTreeMap::new(),
            requires: BTreeMap::new(),
            expire: BTreeMap::new(),
            batched: std::collections::BTreeSet::new(),
            limits: Default::default(),
        };
        put(&store, "apps/acme/shop/v1/manifest", &manifest.format()).await;
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 1);
        let keys = versions(&store).await;
        assert!(
            keys.contains(&"apps/acme/shop/current".to_owned()),
            "{keys:?}"
        );
        for i in 2..8 {
            assert!(
                keys.contains(&format!("apps/acme/shop/v{i}/manifest")),
                "v{i}: {keys:?}"
            );
        }
        assert!(
            keys.contains(&"apps/acme/shop/v0/manifest".to_owned()),
            "unreached: {keys:?}"
        );
    }

    #[tokio::test]
    async fn a_manifest_that_cannot_be_read_is_kept() {
        // Perhaps written by a newer WasmKeep: not deleted, and the walk does
        // not go past it.
        let (store, app) = lineage(8).await;
        put(
            &store,
            "apps/acme/shop/v1/manifest",
            "wasmkeep-manifest 9\n",
        )
        .await;
        assert_eq!(app.collect_versions(5, Duration::ZERO).await.unwrap(), 0);
        let keys = versions(&store).await;
        assert!(
            keys.contains(&"apps/acme/shop/v1/manifest".to_owned()),
            "{keys:?}"
        );
        assert!(
            keys.contains(&"apps/acme/shop/v0/manifest".to_owned()),
            "{keys:?}"
        );
    }

    #[test]
    fn only_plain_ids_are_followed() {
        assert!(is_version_id("a6d0b3dd3883b630") && is_version_id("v7"));
        for bad in [
            "secrets",
            "current",
            "",
            "/",
            "a/b",
            "..",
            "A6D0",
            "a b",
            &"a".repeat(65),
        ] {
            assert!(!is_version_id(bad), "{bad:?}");
        }
    }
}

#[cfg(test)]
#[path = "collect_assets_tests.rs"]
mod asset_tests;

impl AppRef {
    /// Collect old versions, then assets, under one bucket-clock marker.
    /// Returns (versions removed, asset objects removed).
    pub async fn collect(
        &self,
        keep_previous: usize,
        retention: Duration,
    ) -> Result<(usize, usize), DeployError> {
        self.collect_run(Some(keep_previous), retention).await
    }

    /// Collect unused asset objects without removing version manifests.
    pub async fn collect_assets(&self, retention: Duration) -> Result<usize, DeployError> {
        Ok(self.collect_run(None, retention).await?.1)
    }

    async fn collect_run(
        &self,
        keep: Option<usize>,
        retention: Duration,
    ) -> Result<(usize, usize), DeployError> {
        let current = self.current().await?;
        let has_assets = if let Some(c) = &current {
            self.manifest(&c.version).await?.assets.is_some()
        } else {
            false
        };
        let prefix = self.path("_assets");
        if !has_assets
            && self
                .store
                .list(Some(&prefix))
                .try_next()
                .await
                .map_err(bucket)?
                .is_none()
        {
            let versions = match keep {
                Some(n) => {
                    self.collect_versions_at(n, retention, crate::alarm::now_ms_i64(), None)
                        .await?
                }
                None => 0,
            };
            return Ok((versions, 0));
        }
        let key = self.path(&format!(
            "_assets/collecting/{}",
            wasmkeep_cluster::control::new_request_id()
        ));
        let start = tokio::time::Instant::now();
        self.store
            .put_opts(
                &key,
                Vec::new().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(bucket)?;
        let result = async {
            let tc = self
                .store
                .head(&key)
                .await
                .map_err(bucket)?
                .last_modified
                .timestamp_millis();
            let deploying: Vec<_> = self
                .store
                .list(Some(&self.path("_assets/deploying")))
                .try_collect()
                .await
                .map_err(bucket)?;
            let mut active = false;
            for marker in deploying {
                // Unknown marker keys are never deleted, and conservatively block.
                if direct_hex(&marker.location, &self.path("_assets/deploying"), 16)
                    && marker.last_modified.timestamp_millis() <= tc.saturating_sub(3_600_000)
                {
                    if !expired(start) {
                        // A concurrent renewal is a reason to stop, not evidence
                        // that the old listing still describes a dead deploy.
                        active |= !self.delete_candidate(&marker, start).await?;
                    }
                } else {
                    active = true;
                }
            }
            if active {
                eprintln!("assets not collected: a deploy is in progress");
                return Ok((0, 0));
            }
            let mut candidates = Vec::new();
            for kind in ["content", "index"] {
                let prefix = self.path(&format!("_assets/{kind}"));
                let objects: Vec<_> = self
                    .store
                    .list(Some(&prefix))
                    .try_collect()
                    .await
                    .map_err(bucket)?;
                candidates.extend(
                    objects
                        .into_iter()
                        .filter(|m| direct_hex(&m.location, &prefix, 64)),
                );
            }
            let prefix = self.path("_assets/collecting");
            let markers: Vec<_> = self
                .store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(bucket)?;
            for marker in markers {
                if marker.location != key
                    && direct_hex(&marker.location, &prefix, 16)
                    && marker.last_modified.timestamp_millis() <= tc.saturating_sub(900_000)
                    && !expired(start)
                {
                    self.delete_candidate(&marker, start).await?;
                }
            }
            if expired(start) {
                return Ok((0, 0));
            }
            let versions = match keep {
                Some(n) => {
                    self.collect_versions_at(n, retention, tc, Some(start))
                        .await?
                }
                None => 0,
            };
            let used = self.used_assets().await?;
            let cutoff =
                tc.saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
            let mut deleted = 0;
            for candidate in candidates {
                if expired(start) {
                    break;
                }
                if used.contains(&candidate.location)
                    || candidate.last_modified.timestamp_millis() > cutoff
                {
                    continue;
                }
                if deleted % 100 == 0 && self.current().await? != current {
                    break;
                }
                // Check again after reads: a slow HEAD must not extend the deadline.
                if self.delete_candidate(&candidate, start).await? {
                    deleted += 1;
                }
            }
            Ok((versions, deleted))
        }
        .await;
        let cleanup = self.store.delete(&key).await.map_err(bucket);
        match result {
            Err(e) => Err(e),
            Ok(value) => {
                cleanup?;
                Ok(value)
            }
        }
    }

    async fn used_assets(&self) -> Result<HashSet<object_store::path::Path>, DeployError> {
        let current = self.current().await?;
        let prefix = self.path("");
        let objects: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(bucket)?;
        let mut lineage = HashSet::new();
        let mut at = current.as_ref().map(|c| c.version.clone());
        while let Some(version) = at {
            if !is_version_id(&version) || !lineage.insert(version.clone()) {
                break;
            }
            at = match self.manifest(&version).await {
                Ok(manifest) => manifest.previous,
                // Collection can leave a missing ancestor after pruning.
                Err(DeployError::Corrupt(_)) => None,
                Err(e) => return Err(e),
            };
        }
        let mut used = HashSet::new();
        for object in objects {
            let Some(relative) = object.location.as_ref().strip_prefix(&format!("{prefix}/"))
            else {
                continue;
            };
            let Some(version) = relative.strip_suffix("/manifest") else {
                continue;
            };
            if version.contains('/') {
                continue;
            }
            // Read every version directory, even an unfamiliar id; never guess
            // that an unreadable manifest names no assets.
            let manifest = self.manifest(version).await?;
            if !lineage.contains(version) {
                eprintln!("assets retained by orphan directory {version}: manifest is present");
            }
            let Some(hash) = manifest.assets else {
                continue;
            };
            let index_key = self.path(&format!("_assets/index/{hash}"));
            match self.store.head(&index_key).await {
                Err(object_store::Error::NotFound { .. }) => {
                    if current.as_ref().is_some_and(|c| c.version == version) {
                        return Err(DeployError::Refused("assets not collected: the index of the current version is missing; run the deploy again to repair it".into()));
                    }
                    eprintln!("version {version} has lost its assets and cannot be rolled back to");
                    continue;
                }
                Err(e) => return Err(bucket(e)),
                Ok(_) => {}
            }
            let index = self.read_assets_index(&hash).await?;
            used.insert(index_key);
            for file in index.files.values() {
                for content in file.contents() {
                    used.insert(self.path(&format!("_assets/content/{}", content.digest)));
                }
            }
        }
        Ok(used)
    }

    async fn delete_candidate(
        &self,
        candidate: &object_store::ObjectMeta,
        start: tokio::time::Instant,
    ) -> Result<bool, DeployError> {
        match self.store.head(&candidate.location).await {
            Ok(now)
                if now.e_tag == candidate.e_tag
                    && now.size == candidate.size
                    && !expired(start) =>
            {
                // object_store 0.14.2 exposes no conditional delete. The marker
                // protocol is the safety boundary; this HEAD additionally keeps
                // replacements observed since listing. It is not an atomic CAS.
                self.store
                    .delete(&candidate.location)
                    .await
                    .map_err(bucket)?;
                Ok(true)
            }
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(bucket(e)),
        }
    }
}
fn expired(start: tokio::time::Instant) -> bool {
    start.elapsed() >= Duration::from_secs(300)
}
fn direct_hex(
    key: &object_store::path::Path,
    prefix: &object_store::path::Path,
    len: usize,
) -> bool {
    key.as_ref()
        .strip_prefix(&format!("{prefix}/"))
        .is_some_and(|tail| {
            tail.len() == len
                && tail
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}
